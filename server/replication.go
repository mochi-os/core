// Mochi server: Replication transport
// Copyright Alistair Cunningham 2026

package main

import (
	cbor "github.com/fxamacker/cbor/v2"
)

// Replication scope and op kind constants. See claude/plans/replication.md.
const (
	repl_scope_app  = "app"
	repl_scope_core = "core"
	repl_op_insert  = "insert"
	repl_op_update  = "update"
	repl_op_delete  = "delete"
)

// ReplicationOp is the wire format for a single replication operation sent
// between hosts in a user's host set. One ReplicationOp travels in the
// content/data segments of a `replication`/`op` message.
//
// Sequence is the origin peer's monotonic counter per (user, scope); the
// (peer, scope, user, sequence) tuple is the dedup key at the receiver.
// Schema (app-scope) is the sender's app schema version at the time of
// emit; receivers whose local schema is lower buffer the op in
// `replication.db.pending` until `database_upgrade` catches up.
type ReplicationOp struct {
	Scope    string `cbor:"scope"`
	User     string `cbor:"user,omitempty"`
	Database string `cbor:"db"`
	Table    string `cbor:"table"`
	Kind     string `cbor:"kind"`
	Payload  []byte `cbor:"payload"`
	Sequence int64  `cbor:"sequence"`
	Schema   int    `cbor:"schema,omitempty"`
}

// MembershipChange announces the new host set for a user. Sent by whichever
// host the user (or operator) initiated the change on; receivers replace
// their local view in `replication.db.hosts`. Sequence is a per-user
// monotonic counter from the originating host; older sequences are
// recorded as seen but do not overwrite a newer set.
type MembershipChange struct {
	User     string   `cbor:"user"`
	Hosts    []string `cbor:"hosts"`
	Sequence int64    `cbor:"sequence"`
}

// KeysTransfer carries a user's identity (the users-row fields plus every
// owned entity, including the private keys) from one host to another.
// Sent when a user opts in a new host or two servers pair.
//
// Username is the stable cross-host handle for the user (the integer
// users.id is per-host; the eventual UID lands in task #4). The message
// is signed by the user's identity entity, and the receiver checks that
// the signer (e.from) is one of the Entities being transferred — that
// signature is the entire authorisation. Only somebody who already holds
// the user's private keys can introduce the user to a new host.
type KeysTransfer struct {
	Username string       `cbor:"username"`
	Role     string       `cbor:"role,omitempty"`
	Methods  string       `cbor:"methods,omitempty"`
	Status   string       `cbor:"status,omitempty"`
	Entities []KeysEntity `cbor:"entities"`
}

// SessionInsert is the wire payload for a sessions.sessions insert op.
// Carried as the CBOR-encoded Payload of a ReplicationOp with
// Database="sessions" Table="sessions" Kind="insert". UserUID is the
// globally-stable user identifier (task #4); the receiver resolves it to
// a local users.id before inserting into sessions.db.
type SessionInsert struct {
	UserUID  string `cbor:"user_uid"`
	Code     string `cbor:"code"`
	Secret   string `cbor:"secret"`
	Expires  int64  `cbor:"expires"`
	Created  int64  `cbor:"created"`
	Accessed int64  `cbor:"accessed"`
	Address  string `cbor:"address"`
	Agent    string `cbor:"agent"`
}

// SessionDelete is the wire payload for a sessions.sessions delete op.
// Only the code is needed; deletion is unconditional at the receiver.
type SessionDelete struct {
	Code string `cbor:"code"`
}

// KeysEntity is one entity inside a KeysTransfer payload.
type KeysEntity struct {
	ID          string `cbor:"id"`
	Private     string `cbor:"private"`
	Fingerprint string `cbor:"fingerprint"`
	Parent      string `cbor:"parent,omitempty"`
	Class       string `cbor:"class"`
	Name        string `cbor:"name"`
	Privacy     string `cbor:"privacy"`
	Data        string `cbor:"data,omitempty"`
	Published   int64  `cbor:"published,omitempty"`
}

func init() {
	a := app("replication")
	a.service("replication")
	a.event("op", replication_op_event)
	a.event("snapshot-request", replication_snapshot_request_event)
	a.event("snapshot-chunk", replication_snapshot_chunk_event)
	a.event("membership-change", replication_membership_change_event)
	a.event("keys-transfer", replication_keys_transfer_event)
}

// replication_op_event receives a single replication op from a peer in the
// user's host set. The framework has already verified the signature against
// e.from (the originating user identity for app-scope ops). The handler
// dedups on (peer, scope, user, sequence) and records the op as applied.
//
// Op application itself — translating the payload into table writes — is
// done by the pattern-library helpers (PN-counter, LWW, append-only log,
// commit hook) and is wired up per-app. This handler is the transport-level
// landing point only.
func replication_op_event(e *Event) {
	var op ReplicationOp
	if !e.segment(&op) {
		info("Replication op dropping: cannot decode payload")
		return
	}

	db := db_open("db/replication.db")
	seen, _ := db.exists(
		"select 1 from seen where peer=? and scope=? and user=? and sequence=?",
		e.peer, op.Scope, op.User, op.Sequence)
	if seen {
		debug("Replication op duplicate: peer=%q scope=%q user=%q seq=%d",
			e.peer, op.Scope, op.User, op.Sequence)
		return
	}

	// TODO: schema coordination — if op.Schema > local app schema for
	// (user, app), buffer in `pending` until `database_upgrade` catches
	// up. See task #6.

	replication_apply_op(&op)

	db.exec(
		"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, ?, ?, ?, ?)",
		e.peer, op.Scope, op.User, op.Sequence, now())

	debug("Replication op applied: peer=%q scope=%q user=%q seq=%d db=%q table=%q kind=%q",
		e.peer, op.Scope, op.User, op.Sequence, op.Database, op.Table, op.Kind)
}

// replication_apply_op dispatches a verified, deduplicated op to the
// table-specific apply path. Most apps' tables will be handled by the
// pattern-library helpers (task #8); core-DB applies (sessions, etc.) are
// special-cased here.
func replication_apply_op(op *ReplicationOp) {
	switch {
	case op.Scope == repl_scope_app && op.Database == "sessions" && op.Table == "sessions":
		switch op.Kind {
		case repl_op_insert:
			var p SessionInsert
			if err := cbor.Unmarshal(op.Payload, &p); err != nil {
				info("Replication op sessions/insert: decode failed: %v", err)
				return
			}
			replication_session_apply_insert(&p)
		case repl_op_delete:
			var p SessionDelete
			if err := cbor.Unmarshal(op.Payload, &p); err != nil {
				info("Replication op sessions/delete: decode failed: %v", err)
				return
			}
			replication_session_apply_delete(&p)
		}
	}
}

// replication_session_apply_insert lands a replicated session insert into
// the local sessions.db. Resolves the user_uid to a local users.id; if
// the user doesn't exist locally yet (e.g. keys-transfer hasn't landed)
// the insert is skipped — the sender's queue will retry. `replace into`
// makes re-applies idempotent.
func replication_session_apply_insert(p *SessionInsert) {
	udb := db_open("db/users.db")
	row, _ := udb.row("select id from users where uid=?", p.UserUID)
	if row == nil {
		debug("Replication session-insert: user %q not local yet, skipping", p.UserUID)
		return
	}
	var localID int
	if v, ok := row["id"].(int64); ok {
		localID = int(v)
	}
	if localID == 0 {
		return
	}

	sdb := db_open("db/sessions.db")
	sdb.exec(
		"replace into sessions (user, code, secret, expires, created, accessed, address, agent) values (?, ?, ?, ?, ?, ?, ?, ?)",
		localID, p.Code, p.Secret, p.Expires, p.Created, p.Accessed, p.Address, p.Agent)
	debug("Replication session-insert applied: user_uid=%q code=%q", p.UserUID, p.Code)
}

// replication_session_apply_delete removes a session by code on the
// receiver. Unconditional — delete wins over a stale insert at the
// session-revocation layer.
func replication_session_apply_delete(p *SessionDelete) {
	sdb := db_open("db/sessions.db")
	sdb.exec("delete from sessions where code=?", p.Code)
	debug("Replication session-delete applied: code=%q", p.Code)
}

// replication_emit_session_insert fans out a session-insert op to every
// peer in the user's host set. Called by login_create after the local
// row is committed.
func replication_emit_session_insert(userUID, code, secret string, expires, created, accessed int64, address, agent string) {
	if userUID == "" {
		return
	}
	payload := cbor_encode(&SessionInsert{
		UserUID: userUID, Code: code, Secret: secret,
		Expires: expires, Created: created, Accessed: accessed,
		Address: address, Agent: agent,
	})
	replication_emit(userUID, &ReplicationOp{
		Scope:    repl_scope_app,
		User:     userUID,
		Database: "sessions",
		Table:    "sessions",
		Kind:     repl_op_insert,
		Payload:  payload,
	})
}

// replication_emit_session_delete fans out a session-delete op to every
// peer in the user's host set. Called by login_delete after the local
// row is removed.
func replication_emit_session_delete(userUID, code string) {
	if userUID == "" {
		return
	}
	payload := cbor_encode(&SessionDelete{Code: code})
	replication_emit(userUID, &ReplicationOp{
		Scope:    repl_scope_app,
		User:     userUID,
		Database: "sessions",
		Table:    "sessions",
		Kind:     repl_op_delete,
		Payload:  payload,
	})
}

// replication_snapshot_request_event: per-(user, scope) full state copy
// request. Used when a peer first joins a user's host set (per-user opt-in)
// or as a fallback for whole-server bootstrap. Not yet wired.
func replication_snapshot_request_event(e *Event) {
	debug("Replication snapshot-request not yet implemented (from peer %q)", e.peer)
}

// replication_snapshot_chunk_event: streamed reply to a snapshot-request.
// Not yet wired.
func replication_snapshot_chunk_event(e *Event) {
	debug("Replication snapshot-chunk not yet implemented (from peer %q)", e.peer)
}

// replication_membership_change_event applies a membership update from
// another host in the user's set. The framework has already verified the
// signature against e.from (the user's identity entity). Dedup on
// (peer, scope="membership", user, sequence); replace local hosts if the
// incoming sequence is the newest we've seen for the user.
//
// Older membership changes still go into `seen` so a slow peer's stale
// announcement doesn't keep re-applying after a newer state has landed.
func replication_membership_change_event(e *Event) {
	var mc MembershipChange
	if !e.segment(&mc) {
		info("Replication membership-change dropping: cannot decode payload")
		return
	}
	replication_membership_apply(e.peer, &mc)
}

// replication_membership_apply is the pure-DB half of the membership-change
// path, separated out for testing. Dedup on (peer, scope="membership", user,
// sequence); replace local hosts if the incoming sequence is the newest
// we've seen for the user. Older membership changes still go into `seen` so
// a slow peer's stale announcement doesn't keep re-applying after a newer
// state has landed.
func replication_membership_apply(originPeer string, mc *MembershipChange) {
	db := db_open("db/replication.db")

	if applied, _ := db.exists(
		"select 1 from seen where peer=? and scope='membership' and user=? and sequence=?",
		originPeer, mc.User, mc.Sequence); applied {
		return
	}

	var latest int64
	if row, err := db.row("select max(sequence) as seq from seen where scope='membership' and user=?", mc.User); err == nil && row != nil {
		if v, ok := row["seq"].(int64); ok {
			latest = v
		}
	}
	stale := mc.Sequence < latest

	if !stale {
		db.exec("delete from hosts where user=?", mc.User)
		for _, peer := range mc.Hosts {
			if peer == "" || peer == p2p_id {
				continue
			}
			db.exec("insert into hosts (user, peer, added, ack) values (?, ?, ?, 0)", mc.User, peer, now())
		}
	}

	db.exec(
		"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, 'membership', ?, ?, ?)",
		originPeer, mc.User, mc.Sequence, now())

	if stale {
		debug("Replication membership-change stale: user=%q seq=%d < latest=%d (from peer %q)",
			mc.User, mc.Sequence, latest, originPeer)
	} else {
		debug("Replication membership-change applied: user=%q seq=%d hosts=%v (from peer %q)",
			mc.User, mc.Sequence, mc.Hosts, originPeer)
	}
}

// replication_keys_transfer_event applies an inbound user-identity transfer
// from another host. The message is signed by one of the user's identity
// entities (the framework has already verified the signature); we further
// check that e.from is in the transferred entity set, which proves the
// sender holds the user's private keys and is authorised to introduce the
// user to this host. Once that holds, we insert (or reconcile) the
// users.db.users row and `insert or ignore` every entity.
//
// Idempotent: re-running the handler with the same payload is a no-op.
func replication_keys_transfer_event(e *Event) {
	var kt KeysTransfer
	if !e.segment(&kt) {
		info("Replication keys-transfer dropping: cannot decode payload")
		return
	}
	replication_keys_transfer_apply(e.from, e.peer, &kt)
}

// replication_keys_transfer_apply is the pure-DB half of the keys-transfer
// path, separated for testing. `signer` is the entity that signed the
// outer message (already verified by the framework); it must appear among
// the transferred entities, which is what authorises the transfer.
// Returns the number of entities newly inserted (0 on rejection or on a
// fully-duplicate transfer).
func replication_keys_transfer_apply(signer, originPeer string, kt *KeysTransfer) int {
	if kt.Username == "" {
		info("Replication keys-transfer dropping: empty username")
		return 0
	}

	senderOK := false
	for _, ent := range kt.Entities {
		if ent.ID == signer {
			senderOK = true
			break
		}
	}
	if !senderOK {
		info("Replication keys-transfer dropping: signer %q not in transferred entities (username=%q peer=%q)",
			signer, kt.Username, originPeer)
		return 0
	}

	udb := db_open("db/users.db")

	var localID int
	if row, err := udb.row("select id from users where username=?", kt.Username); err == nil && row != nil {
		if v, ok := row["id"].(int64); ok {
			localID = int(v)
		}
	}

	if localID == 0 {
		role := kt.Role
		if role == "" {
			role = "user"
		}
		methods := kt.Methods
		if methods == "" {
			methods = "email"
		}
		status := kt.Status
		if status == "" {
			status = "active"
		}

		result, err := udb.internal.Exec("insert into users (username, role, methods, status) values (?, ?, ?, ?)",
			kt.Username, role, methods, status)
		if err != nil {
			warn("Replication keys-transfer: failed to insert user %q: %v", kt.Username, err)
			return 0
		}
		id, _ := result.LastInsertId()
		localID = int(id)
	}

	inserted := 0
	for _, ent := range kt.Entities {
		if !valid(ent.ID, "entity") {
			continue
		}
		exists, _ := udb.exists("select 1 from entities where id=?", ent.ID)
		if exists {
			continue
		}
		udb.exec(`insert into entities
			(id, private, fingerprint, user, parent, class, name, privacy, data, published)
			values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			ent.ID, ent.Private, ent.Fingerprint, localID, ent.Parent, ent.Class, ent.Name, ent.Privacy, ent.Data, ent.Published)
		inserted++
	}

	debug("Replication keys-transfer applied: username=%q entities=%d inserted=%d (from peer %q)",
		kt.Username, len(kt.Entities), inserted, originPeer)
	return inserted
}

// replication_transfer_keys is the local side: read a user's row and every
// owned entity (including private keys) from users.db, package them into a
// KeysTransfer, and send to the target peer signed by the first entity.
// Returns true when the message was queued for delivery.
//
// The caller is the orchestration layer for per-user opt-in or pair
// creation — once the peer is in `replication.db.hosts` or
// `replication.db.pair`, call this to deliver the keys, then start fanning
// out ordinary replication ops.
func replication_transfer_keys(userID int, peer string) bool {
	if peer == "" || peer == p2p_id {
		return false
	}

	udb := db_open("db/users.db")

	var u User
	if !udb.scan(&u, "select id, uid, username, role, methods, status from users where id=?", userID) {
		warn("Replication transfer-keys: user %d not found", userID)
		return false
	}

	rows, err := udb.rows("select id, private, fingerprint, parent, class, name, privacy, data, published from entities where user=?", userID)
	if err != nil {
		warn("Replication transfer-keys: failed to read entities for user %d: %v", userID, err)
		return false
	}
	if len(rows) == 0 {
		warn("Replication transfer-keys: no entities for user %d", userID)
		return false
	}

	kt := KeysTransfer{
		Username: u.Username,
		Role:     u.Role,
		Methods:  u.Methods,
		Status:   u.Status,
	}
	for _, r := range rows {
		id, _ := r["id"].(string)
		if id == "" {
			continue
		}
		ent := KeysEntity{
			ID:          id,
			Private:     toString(r["private"]),
			Fingerprint: toString(r["fingerprint"]),
			Parent:      toString(r["parent"]),
			Class:       toString(r["class"]),
			Name:        toString(r["name"]),
			Privacy:     toString(r["privacy"]),
			Data:        toString(r["data"]),
		}
		if pub, ok := r["published"].(int64); ok {
			ent.Published = pub
		}
		kt.Entities = append(kt.Entities, ent)
	}
	if len(kt.Entities) == 0 {
		warn("Replication transfer-keys: user %d has no valid entities", userID)
		return false
	}

	from := kt.Entities[0].ID
	m := message(from, "", "replication", "keys-transfer")
	m.add(&kt)
	m.send_peer(peer)
	return true
}

// toString converts a SQLite map value to a string, handling both []byte
// and string cases. Returns "" for nil or unconvertible values.
func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	return ""
}

// replication_membership_update is the local side: bumps the user's
// membership sequence, replaces local hosts with the new set, and emits a
// membership-change announcement to every peer in the new set.
//
// `hosts` is the complete new host set excluding the local host. The
// local entry is recorded too, but is filtered out of the outbound peer
// list (we don't send messages to ourselves).
func replication_membership_update(user string, hosts []string) {
	seq := replication_sequence_next(user, "membership")

	db := db_open("db/replication.db")
	db.exec("delete from hosts where user=?", user)
	for _, peer := range hosts {
		if peer == "" || peer == p2p_id {
			continue
		}
		db.exec("insert into hosts (user, peer, added, ack) values (?, ?, ?, 0)", user, peer, now())
	}
	db.exec(
		"insert or ignore into seen (peer, scope, user, sequence, applied) values ('', 'membership', ?, ?, ?)",
		user, seq, now())

	mc := &MembershipChange{User: user, Hosts: hosts, Sequence: seq}

	udb := db_open("db/users.db")
	row, err := udb.row("select id from entities where user=? order by id limit 1", user)
	if err != nil || row == nil {
		warn("Replication membership-update: no signing entity for user %q: %v", user, err)
		return
	}
	from, _ := row["id"].(string)
	if from == "" {
		return
	}

	for _, peer := range hosts {
		if peer == "" || peer == p2p_id {
			continue
		}
		m := message(from, from, "replication", "membership-change")
		m.add(mc)
		m.send_peer(peer)
	}
}

// recipients returns the union of per-user opt-in hosts and server-pair
// members for the given user UID, excluding the local host.
//
// Per-user opt-ins live in `replication.db.hosts`; server-pair members in
// `replication.db.pair`. A user on a paired server who has also opted in to
// a third host gets all three; the local host is filtered out so emit() can
// fan out to the rest without redundant self-send.
func recipients(user string) []string {
	db := db_open("db/replication.db")
	set := map[string]bool{}

	if rows, err := db.rows("select peer from hosts where user=?", user); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" && p != p2p_id {
				set[p] = true
			}
		}
	}

	if rows, err := db.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" && p != p2p_id {
				set[p] = true
			}
		}
	}

	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	return out
}

// replication_sequence_next allocates and returns the next outbound sequence
// number for (user, scope). The counter advances on every call, including
// retries; the receiver dedups by (peer, scope, user, sequence) so re-sent
// sequences are idempotent against late-arriving duplicates.
func replication_sequence_next(user, scope string) int64 {
	db := db_open("db/replication.db")
	db.exec("insert or ignore into sequence (user, scope, next) values (?, ?, 0)", user, scope)
	db.exec("update sequence set next = next + 1 where user=? and scope=?", user, scope)
	if row, err := db.row("select next from sequence where user=? and scope=?", user, scope); err == nil && row != nil {
		if v, ok := row["next"].(int64); ok {
			return v
		}
	}
	return 0
}

// replication_emit sends a replication op to every peer in the user's host
// set. The caller has already applied the op locally; emit is fire-and-
// forget at the API level (delivery is at-least-once via queue.db).
//
// App-scope ops are signed with the user's identity key by the message
// framework. Core-scope ops need to be signed with the server's libp2p key
// (server_sign in p2p.go); that path isn't wired through emit yet and is
// owned by task #34 (whole-server core-DB replication).
//
// `user` is the user's UID. Per-user opt-in and whole-server pairing must
// have populated replication.db.hosts / replication.db.pair for emit to
// have any peers to send to; with no recipients, emit is a no-op.
func replication_emit(user string, op *ReplicationOp) {
	peers := recipients(user)
	if len(peers) == 0 {
		return
	}

	op.Sequence = replication_sequence_next(user, op.Scope)

	if op.Scope != repl_scope_app {
		// TODO: core-scope signing needs a parallel message type that
		// signs with p2p_private (server_sign) rather than entity_sign.
		// See task #34.
		debug("Replication emit core-scope not yet wired (db=%q table=%q)", op.Database, op.Table)
		return
	}

	// Pick any owned identity for this user as the signing entity. The
	// join is by user_uid (task #4), which the v51 trigger keeps in sync
	// with users.uid on insert / FK update.
	udb := db_open("db/users.db")
	row, err := udb.row("select id from entities where user_uid=? order by id limit 1", user)
	if err != nil || row == nil {
		warn("Replication emit: no signing entity for user %q: %v", user, err)
		return
	}
	from, _ := row["id"].(string)
	if from == "" {
		return
	}

	for _, peer := range peers {
		m := message(from, from, "replication", "op")
		m.add(op)
		m.send_peer(peer)
	}
}
