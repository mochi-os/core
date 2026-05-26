// Mochi server: per-DB row replication helpers
// Copyright Alistair Cunningham 2026
//
// Apply + emit helpers for the system DBs whose rows must replicate
// across paired hosts (users.db auth tables, sessions.db auth-flow
// tables, schedule.db). Each table type has its own apply and emit
// functions; they share the same shape (lookup by stable PK, CBOR-
// encode the row, replication_emit_internal). Per-table sections
// preserve the original file's header comment for table-specific
// rationale.

package main

import (
	"fmt"
	"strings"

	cbor "github.com/fxamacker/cbor/v2"
)

// ============================================================
// users.db rows
// ============================================================

// (users.db rationale below was: audit gap #2 V2)


// replication_users_users_mutable lists the columns of users.db.users
// that may be updated via the per-user row-replication path (delivered
// to every host in the user's host set, including per-user link
// partners). Other columns are either replicated on the pair-only path
// (username, role — see replication_emit_users_users_pair_set and
// replication_system_users_users_mutable) or stay strictly server-local
// (uid is the row key; role/username belong on the pair-only path
// because they're per-operator decisions, not per-user data).
var replication_users_users_mutable = map[string]bool{
	"methods": true,
	"status":  true,
}

// replication_emit_users_users_set emits a partial update for one row
// of users.db.users to the user's whole host set (operator-paired
// members AND per-user link partners). Use this for columns that
// represent the user's own choices and follow them across replicas —
// preferences, auth methods, account status. For per-operator columns
// (username, role) use replication_emit_users_users_pair_set instead.
func replication_emit_users_users_set(uid string, fields map[string]string) {
	if uid == "" || len(fields) == 0 {
		return
	}
	replication_emit_users_row(uid, &UsersRow{Table: "users", Cols: fields})
}

// replication_emit_users_users_pair_set emits a partial update for one
// row of users.db.users to the operator-paired hosts only — NOT to
// per-user link partners. Used for columns that are per-operator
// affordances rather than per-user data: username (each operator
// chooses their own per-host namespace) and role (admin authority is
// granted independently on each operator's servers; replicating it
// across operators would be a privilege-escalation footgun). Pair
// members share full identity state because they're the same
// operator's hosts.
func replication_emit_users_users_pair_set(uid string, fields map[string]string) {
	if uid == "" || len(fields) == 0 {
		return
	}
	replication_emit_system_row("users", "users", map[string]string{"uid": uid}, fields, false)
}

// replication_users_entities_mutable lists the columns of
// users.db.entities that may be updated via row replication. id is the
// row key. user is the owner FK (re-keyed from op.User on the receiver).
// private, fingerprint, parent, class are immutable post-create.
// published is per-host scheduling state (last directory republish ts)
// and intentionally stays off this list — each host re-republishes on
// its own cadence.
var replication_users_entities_mutable = map[string]bool{
	"name":    true,
	"privacy": true,
	"data":    true,
}

// replication_emit_users_entities_create replicates a new entity row.
// Called from entity_create after the local INSERT. Carries every
// column except the user FK (re-keyed on the receiver from op.User).
func replication_emit_users_entities_create(uid string, e *Entity) {
	if uid == "" || e == nil || e.ID == "" {
		return
	}
	replication_emit_users_row(uid, &UsersRow{
		Table: "entities",
		Cols: map[string]string{
			"id":          e.ID,
			"private":     e.Private,
			"fingerprint": e.Fingerprint,
			"parent":      e.Parent,
			"class":       e.Class,
			"name":        e.Name,
			"privacy":     e.Privacy,
			"data":        e.Data,
			"published":   fmt.Sprintf("%d", e.Published),
		},
	})
}

// replication_emit_users_entities_update replicates a partial column
// update against an existing entity row. The id is always included in
// fields so the receiver can identify the target row.
func replication_emit_users_entities_update(uid, id string, fields map[string]string) {
	if uid == "" || id == "" || len(fields) == 0 {
		return
	}
	cols := make(map[string]string, len(fields)+1)
	for k, v := range fields {
		cols[k] = v
	}
	cols["id"] = id
	replication_emit_users_row(uid, &UsersRow{Table: "entities", Cols: cols})
}

// replication_emit_users_entities_delete replicates a row delete.
func replication_emit_users_entities_delete(uid, id string) {
	if uid == "" || id == "" {
		return
	}
	replication_emit_users_row(uid, &UsersRow{
		Table:  "entities",
		Cols:   map[string]string{"id": id},
		Delete: true,
	})
}

// UsersRow is the wire payload for an insert / delete against a
// users.db auth table. Key and Cols carry the row's columns split
// by type; blob columns travel in KeyBytes / ColsBytes.
type UsersRow struct {
	Table     string            `cbor:"table"`
	Key       map[string]string `cbor:"key,omitempty"`
	KeyBytes  map[string][]byte `cbor:"key_bytes,omitempty"`
	Cols      map[string]string `cbor:"cols,omitempty"`
	ColsBytes map[string][]byte `cbor:"cols_bytes,omitempty"`
	Delete    bool              `cbor:"delete,omitempty"`
}

// replication_emit_users_row sends a row op for one of the users.db
// auth tables to every peer in the user's host set. user must be the
// uid of the row's owner — the op is signed by one of that user's
// entities so receivers can authenticate cross-host writes against
// the user's auth state.
func replication_emit_users_row(user string, r *UsersRow) {
	if user == "" || r.Table == "" {
		return
	}
	payload := cbor_encode(r)
	operation := "users-row.set"
	if r.Delete {
		operation = "users-row.delete"
	}
	replication_emit(user, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user,
		Database:  "users",
		Table:     r.Table,
		Operation: operation,
		Payload:   payload,
	})
}

// replication_users_row_apply lands a remote users-row op into the
// local users.db. Each table's apply path uses the natural PK so
// re-application is idempotent.
func replication_users_row_apply(userUID string, r *UsersRow) ApplyResult {
	if !user_exists(userUID) {
		return ApplyDeferred
	}
	udb := db_open("db/users.db")
	switch r.Table {
	case "credentials":
		return replication_users_credentials_apply(udb, userUID, r)
	case "recovery":
		return replication_users_recovery_apply(udb, userUID, r)
	case "tokens":
		return replication_users_tokens_apply(udb, userUID, r)
	case "totp":
		return replication_users_totp_apply(udb, userUID, r)
	case "users":
		return replication_users_users_apply(udb, userUID, r)
	case "entities":
		return replication_users_entities_apply(udb, userUID, r)
	}
	info("Replication users-row dropping: unsupported table %q", r.Table)
	return ApplyInvalid
}

// replication_users_entities_apply lands an INSERT / partial UPDATE /
// DELETE against users.db.entities. The op carries the entity's user
// FK in op.User; the apply path scopes every statement to
// (entities.id = ? AND entities.user = ?) so a misbehaving emitter
// can't touch another user's rows.
func replication_users_entities_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	id := r.Cols["id"]
	if id == "" {
		return ApplyInvalid
	}
	if r.Delete {
		udb.exec("delete from entities where id=? and user=?", id, userUID)
		return ApplyApplied
	}
	// A full-row create carries "private" (immutable post-create).
	if _, full := r.Cols["private"]; full {
		var published int64
		_, _ = fmt.Sscanf(r.Cols["published"], "%d", &published)
		udb.exec(`insert or ignore into entities
			(id, private, fingerprint, user, parent, class, name, privacy, data, published)
			values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, r.Cols["private"], r.Cols["fingerprint"], userUID,
			r.Cols["parent"], r.Cols["class"], r.Cols["name"],
			r.Cols["privacy"], r.Cols["data"], published)
		return ApplyApplied
	}
	// Partial update: whitelist mutable columns.
	sets := []string{}
	vals := []any{}
	for col, v := range r.Cols {
		if col == "id" || !replication_users_entities_mutable[col] {
			continue
		}
		if col == "published" {
			var pub int64
			_, _ = fmt.Sscanf(v, "%d", &pub)
			sets = append(sets, col+"=?")
			vals = append(vals, pub)
		} else {
			sets = append(sets, col+"=?")
			vals = append(vals, v)
		}
	}
	if len(sets) == 0 {
		return ApplyInvalid
	}
	vals = append(vals, id, userUID)
	udb.exec("update entities set "+strings.Join(sets, ", ")+" where id=? and user=?", vals...)
	return ApplyApplied
}

// replication_users_users_apply lands a partial UPDATE against
// users.db.users. Whitelists the changed columns and runs one UPDATE
// against the row identified by op.User. Skips silently if the user
// row isn't local yet — defer so a later keys-transfer lands first.
func replication_users_users_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	if r.Delete {
		// Cross-host user delete is a server-pair / link operation,
		// not a row op. Treat any incoming delete as a no-op so we
		// can't accidentally lose accounts via row replication.
		return ApplyApplied
	}
	sets := []string{}
	vals := []any{}
	for col, v := range r.Cols {
		if !replication_users_users_mutable[col] {
			continue
		}
		sets = append(sets, col+"=?")
		vals = append(vals, v)
	}
	if len(sets) == 0 {
		return ApplyInvalid
	}
	vals = append(vals, userUID)
	udb.exec("update users set "+strings.Join(sets, ", ")+" where uid=?", vals...)
	return ApplyApplied
}

func replication_users_credentials_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	id := r.KeyBytes["id"]
	if len(id) == 0 {
		return ApplyInvalid
	}
	if r.Delete {
		udb.exec("delete from credentials where id=? and user=?", id, userUID)
		return ApplyApplied
	}
	pk := r.ColsBytes["public_key"]
	var signCount, created int64
	_, _ = fmt.Sscanf(r.Cols["sign_count"], "%d", &signCount)
	_, _ = fmt.Sscanf(r.Cols["created"], "%d", &created)
	be, bs := 0, 0
	if r.Cols["backup_eligible"] == "1" {
		be = 1
	}
	if r.Cols["backup_state"] == "1" {
		bs = 1
	}
	udb.exec(`insert or replace into credentials
		(id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, userUID, pk, signCount, r.Cols["name"], r.Cols["transports"], be, bs, created)
	return ApplyApplied
}

func replication_users_recovery_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	hash := r.Cols["hash"]
	if hash == "" {
		return ApplyInvalid
	}
	if r.Delete {
		// Two delete shapes used by the source: by (user, hash) for one
		// code, or by user only when wiping all codes ahead of a fresh
		// regenerate. r.Cols["hash"] is empty for the second shape.
		if hash == "*" {
			udb.exec("delete from recovery where user=?", userUID)
		} else {
			udb.exec("delete from recovery where user=? and hash=?", userUID, hash)
		}
		return ApplyApplied
	}
	if exists, _ := udb.exists("select 1 from recovery where user=? and hash=?", userUID, hash); exists {
		return ApplyApplied
	}
	var created int64
	_, _ = fmt.Sscanf(r.Cols["created"], "%d", &created)
	udb.exec("insert into recovery (user, hash, created) values (?, ?, ?)", userUID, hash, created)
	return ApplyApplied
}

func replication_users_tokens_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	hash := r.Cols["hash"]
	if hash == "" {
		return ApplyInvalid
	}
	if r.Delete {
		udb.exec("delete from tokens where hash=?", hash)
		return ApplyApplied
	}
	var created, expires int64
	_, _ = fmt.Sscanf(r.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(r.Cols["expires"], "%d", &expires)
	udb.exec(`insert or replace into tokens
		(hash, user, app, name, scopes, created, expires)
		values (?, ?, ?, ?, ?, ?, ?)`,
		hash, userUID, r.Cols["app"], r.Cols["name"], r.Cols["scopes"], created, expires)
	return ApplyApplied
}

func replication_users_totp_apply(udb *DB, userUID string, r *UsersRow) ApplyResult {
	if r.Delete {
		udb.exec("delete from totp where user=?", userUID)
		return ApplyApplied
	}
	secret := r.Cols["secret"]
	if secret == "" {
		return ApplyInvalid
	}
	verified := 0
	if r.Cols["verified"] == "1" {
		verified = 1
	}
	var created int64
	_, _ = fmt.Sscanf(r.Cols["created"], "%d", &created)
	udb.exec(`insert or replace into totp (user, secret, verified, created) values (?, ?, ?, ?)`,
		userUID, secret, verified, created)
	return ApplyApplied
}

// users_row_decode_and_apply is the entry point called from
// replication_apply_op after dispatch detects a users-row op. Decodes
// the payload and runs the per-table apply.
func users_row_decode_and_apply(payload []byte, user string) ApplyResult {
	var r UsersRow
	if err := cbor.Unmarshal(payload, &r); err != nil {
		info("Replication users-row decode failed: %v", err)
		return ApplyInvalid
	}
	return replication_users_row_apply(user, &r)
}

// boolint01 returns "1" for true and "0" for false. Used at emit
// sites to fit a bool through UsersRow.Cols (map[string]string).
func boolint01(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// ============================================================
// schedule.db rows
// ============================================================

// (schedule.db rationale: paired hosts must agree on what's scheduled)


// ScheduleRow is the wire payload for one insert or delete against
// db/schedule.db. Key carries the natural composite identifier; Cols
// carries the mutable fields (omitted on delete). Wire shape mirrors
// UsersRow / SessionsRow.
type ScheduleRow struct {
	Key    map[string]string `cbor:"key"`
	Cols   map[string]string `cbor:"cols,omitempty"`
	Delete bool              `cbor:"delete,omitempty"`
}

// replication_emit_schedule_insert fans out a schedule insert to every
// host in the user's set (operator-paired members and per-user link
// partners both). Called from schedule_create after the local INSERT
// commits.
func replication_emit_schedule_insert(user, app string, due int64, event, data string, interval, created int64) {
	if user == "" || app == "" || event == "" {
		return
	}
	replication_emit_schedule_row(user, &ScheduleRow{
		Key: map[string]string{
			"user":    user,
			"app":     app,
			"event":   event,
			"created": fmt.Sprintf("%d", created),
		},
		Cols: map[string]string{
			"due":      fmt.Sprintf("%d", due),
			"data":     data,
			"interval": fmt.Sprintf("%d", interval),
		},
	})
}

// replication_emit_schedule_delete fans out a schedule delete keyed
// on the natural composite identifier. The caller looks up
// (user, app, event, created) before performing the local DELETE.
func replication_emit_schedule_delete(user, app, event string, created int64) {
	if user == "" || app == "" || event == "" {
		return
	}
	replication_emit_schedule_row(user, &ScheduleRow{
		Key: map[string]string{
			"user":    user,
			"app":     app,
			"event":   event,
			"created": fmt.Sprintf("%d", created),
		},
		Delete: true,
	})
}

// replication_emit_schedule_row is the underlying emit. Package-level
// variable so tests can stub.
var replication_emit_schedule_row = replication_emit_schedule_row_real

func replication_emit_schedule_row_real(user string, r *ScheduleRow) {
	if user == "" || r == nil {
		return
	}
	payload := cbor_encode(r)
	operation := "schedule-row.set"
	if r.Delete {
		operation = "schedule-row.delete"
	}
	replication_emit(user, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user,
		Database:  "schedule",
		Table:     "schedule",
		Operation: operation,
		Payload:   payload,
	})
}

// schedule_row_decode_and_apply decodes a ScheduleRow payload and
// dispatches to the apply path. Called from replication_apply_op.
func schedule_row_decode_and_apply(payload []byte, user string) ApplyResult {
	var r ScheduleRow
	if err := cbor.Unmarshal(payload, &r); err != nil {
		info("Replication schedule-row decode failed: %v", err)
		return ApplyInvalid
	}
	return replication_schedule_row_apply(user, &r)
}

// replication_schedule_row_apply lands a schedule insert or delete
// against db/schedule.db. Inserts use exists-check + INSERT (same
// pattern as the KeysSchedule bootstrap path) so a re-applied op or a
// row already imported by bootstrap is silently skipped. Deletes are
// unconditional on the natural key.
func replication_schedule_row_apply(userUID string, r *ScheduleRow) ApplyResult {
	if !user_exists(userUID) {
		return ApplyDeferred
	}
	user := r.Key["user"]
	app := r.Key["app"]
	event := r.Key["event"]
	created_raw := r.Key["created"]
	if user == "" || app == "" || event == "" || created_raw == "" {
		return ApplyInvalid
	}
	var created int64
	if _, err := fmt.Sscanf(created_raw, "%d", &created); err != nil {
		return ApplyInvalid
	}
	sdb := schedule_db()
	if r.Delete {
		sdb.exec("delete from schedule where user=? and app=? and event=? and created=?",
			user, app, event, created)
		return ApplyApplied
	}
	exists, _ := sdb.exists(
		"select 1 from schedule where user=? and app=? and event=? and created=?",
		user, app, event, created)
	if exists {
		return ApplyApplied
	}
	var due, interval int64
	_, _ = fmt.Sscanf(r.Cols["due"], "%d", &due)
	_, _ = fmt.Sscanf(r.Cols["interval"], "%d", &interval)
	sdb.exec(
		"insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, ?, ?, ?, ?)",
		user, app, due, event, r.Cols["data"], interval, created)
	schedule_notify()
	return ApplyApplied
}

// ============================================================
// sessions.db rows
// ============================================================

// (sessions.db rationale: closes audit gap #3 for auth-flow tables)


// SessionsRow is the wire payload for an insert / delete against one
// of the replicated sessions.db tables. Same shape as UsersRow; the
// receiver dispatches per-table by Table name.
type SessionsRow struct {
	Table  string            `cbor:"table"`
	Key    map[string]string `cbor:"key,omitempty"`
	Cols   map[string]string `cbor:"cols,omitempty"`
	Delete bool              `cbor:"delete,omitempty"`
}

// replication_emit_sessions_row sends a sessions-row op to every peer
// in the user's host set. user must be the uid of the row's owner.
// For codes (where the username — not uid — is the relevant identity
// before the user has actually logged in), look up the uid first;
// pass empty user to no-op the emit if the user isn't local.
func replication_emit_sessions_row(user string, r *SessionsRow) {
	if user == "" || r.Table == "" {
		return
	}
	payload := cbor_encode(r)
	operation := "sessions-row.set"
	if r.Delete {
		operation = "sessions-row.delete"
	}
	replication_emit(user, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user,
		Database:  "sessions",
		Table:     r.Table,
		Operation: operation,
		Payload:   payload,
	})
}

func replication_sessions_row_apply(userUID string, r *SessionsRow) ApplyResult {
	if !user_exists(userUID) {
		return ApplyDeferred
	}
	sdb := db_open("db/sessions.db")
	switch r.Table {
	case "partial":
		return replication_sessions_partial_apply(sdb, r)
	case "codes":
		return replication_sessions_codes_apply(sdb, r)
	}
	info("Replication sessions-row dropping: unsupported table %q", r.Table)
	return ApplyInvalid
}

func replication_sessions_partial_apply(sdb *DB, r *SessionsRow) ApplyResult {
	id := r.Key["id"]
	if id == "" {
		return ApplyInvalid
	}
	if r.Delete {
		sdb.exec("delete from partial where id=?", id)
		return ApplyApplied
	}
	var expires int64
	_, _ = fmt.Sscanf(r.Cols["expires"], "%d", &expires)
	sdb.exec(`insert or replace into partial (id, user, completed, remaining, expires) values (?, ?, ?, ?, ?)`,
		id, r.Cols["user"], r.Cols["completed"], r.Cols["remaining"], expires)
	return ApplyApplied
}

func replication_sessions_codes_apply(sdb *DB, r *SessionsRow) ApplyResult {
	code := r.Key["code"]
	username := r.Key["username"]
	if code == "" || username == "" {
		return ApplyInvalid
	}
	if r.Delete {
		sdb.exec("delete from codes where code=? and username=?", code, username)
		return ApplyApplied
	}
	var expires int64
	_, _ = fmt.Sscanf(r.Cols["expires"], "%d", &expires)
	sdb.exec("replace into codes (code, username, expires) values (?, ?, ?)",
		code, username, expires)
	return ApplyApplied
}

func sessions_row_decode_and_apply(payload []byte, user string) ApplyResult {
	var r SessionsRow
	if err := cbor.Unmarshal(payload, &r); err != nil {
		info("Replication sessions-row decode failed: %v", err)
		return ApplyInvalid
	}
	return replication_sessions_row_apply(user, &r)
}

// partial_create wraps the insert into sessions.db.partial + the
// cross-host emit. There are five call sites with the same shape;
// wrapping keeps the emit consistent and the insert SQL in one place.
func partial_create(sdb *DB, partialID, userUID, completed, remaining string, expires int64) {
	sdb.exec("insert into partial (id, user, completed, remaining, expires) values (?, ?, ?, ?, ?)",
		partialID, userUID, completed, remaining, expires)
	replication_emit_sessions_row(userUID, &SessionsRow{
		Table: "partial",
		Key:   map[string]string{"id": partialID},
		Cols: map[string]string{
			"user":      userUID,
			"completed": completed,
			"remaining": remaining,
			"expires":   fmt.Sprintf("%d", expires),
		},
	})
}

// partial_delete wraps the delete + cross-host emit. userUID is needed
// only for the emit's signer; the delete itself is by id only (the
// random 32-char id is globally unique).
func partial_delete(sdb *DB, partialID, userUID string) {
	sdb.exec("delete from partial where id=?", partialID)
	if userUID != "" {
		replication_emit_sessions_row(userUID, &SessionsRow{
			Table:  "partial",
			Key:    map[string]string{"id": partialID},
			Delete: true,
		})
	}
}
