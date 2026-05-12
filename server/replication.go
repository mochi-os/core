// Mochi server: Replication transport
// Copyright Alistair Cunningham 2026

package main

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

func init() {
	a := app("replication")
	a.service("replication")
	a.event("op", replication_op_event)
	a.event("snapshot-request", replication_snapshot_request_event)
	a.event("snapshot-chunk", replication_snapshot_chunk_event)
	a.event("membership-change", replication_membership_change_event)
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
	// TODO: apply the op via the pattern-library helper that owns this
	// table. See task #8.

	db.exec(
		"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, ?, ?, ?, ?)",
		e.peer, op.Scope, op.User, op.Sequence, now())

	debug("Replication op applied: peer=%q scope=%q user=%q seq=%d db=%q table=%q kind=%q",
		e.peer, op.Scope, op.User, op.Sequence, op.Database, op.Table, op.Kind)
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

// replication_membership_change_event: a peer joined or left the user's
// host set. Receivers update their local view so future ops fan out to the
// correct set. Not yet wired.
func replication_membership_change_event(e *Event) {
	debug("Replication membership-change not yet implemented (from peer %q)", e.peer)
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

	// Pick any owned identity for this user as the signing entity. Once
	// task #4 lands the users.uid → entities join is by UID; until then
	// the caller's `user` argument should match whatever entities.user
	// currently stores.
	udb := db_open("db/users.db")
	row, err := udb.row("select id from entities where user=? order by id limit 1", user)
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
