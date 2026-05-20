// Mochi server: users.db row replication
// Copyright Alistair Cunningham 2026
//
// Closes audit gap #2 V2 (#78). The bulk-bootstrap snapshot copies
// users.db's auth tables (credentials, recovery, tokens, totp) at
// pair-join time, but any subsequent enrollment on one host doesn't
// reach the others. This file adds row-level emit + apply so a new
// passkey / token / TOTP setup / recovery code on host A lands on
// host B within an op round-trip.
//
// Wire format: UsersRow with both string and []byte Key/Cols maps,
// so credentials.id (BLOB PK) and credentials.public_key (BLOB) can
// travel safely. Per-table dispatch on the receiver knows which
// columns live in which map.
//
// Conflict resolution: simple last-applied-wins via INSERT OR REPLACE
// on the natural PK. Acceptable for these tables because (a) the
// PKs are externally-assigned (WebAuthn credential id, token hash,
// per-user TOTP secret) so two hosts can't generate distinct rows
// with the same PK, and (b) the user typically does enrollment on
// one host at a time.

package main

import (
	"fmt"
	"strings"

	cbor "github.com/fxamacker/cbor/v2"
)

// replication_users_users_mutable lists the columns of users.db.users
// that may be updated via row replication. Other columns (uid) are
// keys; columns added later that should NOT replicate stay off this
// list and are silently ignored by the apply path.
var replication_users_users_mutable = map[string]bool{
	"username": true,
	"role":     true,
	"methods":  true,
	"status":   true,
}

// replication_emit_users_users_set emits a partial update for one
// row of users.db.users. Callers pass only the columns they changed;
// the receiver runs UPDATE … SET <col>=?, … WHERE uid=?.
func replication_emit_users_users_set(uid string, fields map[string]string) {
	if uid == "" || len(fields) == 0 {
		return
	}
	replication_emit_users_row(uid, &UsersRow{Table: "users", Cols: fields})
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
