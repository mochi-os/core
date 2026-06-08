// Mochi server: Replication transport
// Copyright Alistair Cunningham 2026

package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Replication scope and op constants. See claude/plans/replication.md.
const (
	repl_scope_app             = "app"
	repl_scope_core            = "core"
	repl_op_insert             = "insert"
	repl_op_update             = "update"
	repl_op_delete             = "delete"
	repl_op_exec               = "exec"
	repl_op_exec_app_system    = "exec-app-system"
	repl_op_exec_user_core     = "exec-user-core"
	repl_db_user_core_sentinel = "user"
)

// ApplyResult is returned by replication_apply_op to tell the caller
// whether the op landed cleanly, should be buffered for later, or should
// be dropped as unrecognised.
type ApplyResult int

const (
	ApplyApplied  ApplyResult = iota // wrote the change locally, mark seen
	ApplyDeferred                    // can't apply yet (waiting on user / schema); buffer in pending
	ApplyInvalid                     // unknown shape; drop silently
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
//
// LeaderScope / LeaderKey / Fence are filled in for leader-gated ops —
// the emitter holds a lease for ('LeaderScope', 'LeaderKey') with
// the given fence at emit time. Receivers compare the fence against
// `replication.db.fence_witness` and drop the op when a higher fence
// has already been seen (stale-leader output).
type ReplicationOp struct {
	Scope       string `cbor:"scope"`
	User        string `cbor:"user,omitempty"`
	Database    string `cbor:"db"`
	Table       string `cbor:"table,omitempty"`
	UID         string `cbor:"uid,omitempty"`
	Operation   string `cbor:"operation"`
	Payload     []byte `cbor:"payload"`
	Sequence    int64  `cbor:"sequence"`
	Prev        int64  `cbor:"prev,omitempty"`
	Schema      int    `cbor:"schema,omitempty"`
	LeaderScope string `cbor:"leader_scope,omitempty"`
	LeaderKey   string `cbor:"leader_key,omitempty"`
	Fence       int64  `cbor:"fence,omitempty"`
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
	// UID is the source's users.uid. The receiver re-uses it verbatim
	// so per-user filesystem paths (users/<uid>/<app>/files,
	// users/<uid>/<app>/db) and entity ownership FKs match the
	// bootstrap-copied data on disk. Without this, keys-transfer
	// created a fresh local uid for each user, and the bulk-bootstrap
	// file transfer left every per-user-app DB and file tree at the
	// source's path — invisible to the now-different local uid.
	// Caught live: pair-bootstrap put feeds.db at users/<source-uid>/
	// feeds/db/feeds.db (148k posts), but the user logged in on the
	// replica as the new local uid and saw no feeds. (Empty string
	// allowed for backwards compatibility with per-user link callers
	// that haven't been updated yet; receiver falls back to uid() in
	// that case.)
	UID      string       `cbor:"uid,omitempty"`
	Username string       `cbor:"username"`
	Role     string       `cbor:"role,omitempty"`
	Methods  string       `cbor:"methods,omitempty"`
	Status   string       `cbor:"status,omitempty"`
	Entities []KeysEntity `cbor:"entities"`
	// OAuth provider links the user has (GitHub, Google, etc.). Each
	// entry is a row from the source's users.db.oauth, minus the
	// per-host integer PK. The receiver re-keys to its own local
	// users.uid at apply time (the uid is per-host; (provider, subject)
	// is the cross-host stable lookup key). Without this, a user who
	// signed up via OAuth on the source can't log in on the replica:
	// oauth_login finds no row for (provider, subject), falls back to
	// the email-collision branch, and refuses the login with
	// "An account with that email already exists. Sign in first..." —
	// a dead end because the user has no other auth method to use.
	OAuth []KeysOauth `cbor:"oauth,omitempty"`
	// Auth-method state. Whole-server pair already gets these via
	// users.db bootstrap snapshot, but per-user link (one user moves
	// to a second host while their other-user data stays on the
	// source) needs them in the keys-transfer payload — otherwise
	// the user lands on the replica with username + entities only
	// and can't use any of their existing auth factors.
	Credentials []KeysCredential `cbor:"credentials,omitempty"`
	Recovery    []KeysRecovery   `cbor:"recovery,omitempty"`
	Tokens      []KeysToken      `cbor:"tokens,omitempty"`
	Totp        *KeysTotp        `cbor:"totp,omitempty"`
	// Scheduled events the user owns (db/schedule.db rows). Per-user
	// link otherwise leaves them on the source — lost if the source is
	// later dropped, and never present on the replica. Carried here so
	// the replica has them too. Note: once an event exists on >1 host,
	// its callback must be mochi.schedule.leader-gated to fire once.
	Schedule []KeysSchedule `cbor:"schedule,omitempty"`
	// Seeds is the source's replication tail for the user's non-file
	// streams (users, sessions) at the instant the source added this
	// peer to the host set. The replica seeds its in-order apply
	// cursors from these so the first live op on each stream chains.
	// File-bootstrapped streams seed per-file from the DB snapshot
	// instead — see claude/plans/replication-per-db-handoff.md piece 3.
	Seeds map[string]int64 `cbor:"seeds,omitempty"`
}

// KeysSchedule is one scheduled event for the user. Mirrors
// db/schedule.db.schedule minus the per-host autoincrement PK (the
// receiver re-allocates it). The cross-host identity for idempotent
// re-apply is (user, app, event, created).
type KeysSchedule struct {
	App      string `cbor:"app"`
	Due      int64  `cbor:"due"`
	Event    string `cbor:"event"`
	Data     string `cbor:"data,omitempty"`
	Interval int64  `cbor:"interval,omitempty"`
	Created  int64  `cbor:"created"`
}

// KeysOauth is one OAuth provider link inside a KeysTransfer payload.
// Mirrors the columns of users.db.oauth except the per-host integer PK
// (recreated on the receiver) and the user column (re-keyed to the
// receiver's local uid for the matching username).
type KeysOauth struct {
	Provider string `cbor:"provider"`
	Subject  string `cbor:"subject"`
	Email    string `cbor:"email,omitempty"`
	Verified bool   `cbor:"verified,omitempty"`
	Name     string `cbor:"name,omitempty"`
	Created  int64  `cbor:"created"`
}

// KeysCredential is one passkey / WebAuthn credential. Mirrors
// users.db.credentials minus the per-host user FK (re-keyed at apply
// time). The blob `id` is the WebAuthn credential ID (cross-host
// stable); `public_key` is the credential's signing key.
type KeysCredential struct {
	ID             []byte `cbor:"id"`
	PublicKey      []byte `cbor:"public_key"`
	SignCount      int64  `cbor:"sign_count,omitempty"`
	Name           string `cbor:"name,omitempty"`
	Transports     string `cbor:"transports,omitempty"`
	BackupEligible bool   `cbor:"backup_eligible,omitempty"`
	BackupState    bool   `cbor:"backup_state,omitempty"`
	Created        int64  `cbor:"created"`
}

// KeysRecovery is one recovery code (hashed) for the user. The
// receiver re-allocates the integer PK locally; the natural
// cross-host identity is (user, hash).
type KeysRecovery struct {
	Hash    string `cbor:"hash"`
	Created int64  `cbor:"created"`
}

// KeysToken is one API token row. The hash is the cross-host stable PK.
type KeysToken struct {
	Hash    string `cbor:"hash"`
	App     string `cbor:"app"`
	Name    string `cbor:"name,omitempty"`
	Scopes  string `cbor:"scopes,omitempty"`
	Created int64  `cbor:"created"`
	Expires int64  `cbor:"expires,omitempty"`
}

// KeysTotp is the per-user TOTP secret. Single row per user.
type KeysTotp struct {
	Secret   string `cbor:"secret"`
	Verified bool   `cbor:"verified,omitempty"`
	Created  int64  `cbor:"created"`
}

// WebpushDelivered is the wire payload for replicating a per-user
// webpush dedup row. Local marks of (endpoint, event_id) fan out to
// the user's host set so a replica that processes the same event after
// the local replica already delivered sees the row pre-populated and
// short-circuits its send. Closes the cross-replica race window left
// open by the local-only V1 dedup.
type WebpushDelivered struct {
	Endpoint string `cbor:"endpoint"`
	EventID  string `cbor:"event_id"`
	TS       int64  `cbor:"ts"`
}

// EmailDelivered mirrors WebpushDelivered for the email layer.
type EmailDelivered struct {
	Address string `cbor:"address"`
	EventID string `cbor:"event_id"`
	TS      int64  `cbor:"ts"`
}

// SessionInsert is the wire payload for a sessions.sessions insert op.
// Carried as the CBOR-encoded Payload of a ReplicationOp with
// Database="sessions" Table="sessions" Operation="insert". UserUID is the
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
	// User-scope ops carry the originating user-entity signature; the
	// rest of the events are server-to-server (no entity yet, or no
	// sensible signer) and ride the libp2p transport's per-stream
	// peer authentication — e.peer is the verified sender. Handlers
	// authorize via that peer-id (e.g. bootstrap chunks require the
	// peer be an active bootstrap source for the scope).
	a.event("sql/op", replication_op_event)
	a.event("host/membership/change", replication_membership_change_event)
	a.event("keys/transfer", replication_keys_transfer_event)
	// Live file replication (see file_push.go). Per-(user, peer) push;
	// any size; sha256-verified on the receiver before atomic rename.
	a.event("file/push", replication_file_push_event)
	a.event("file/delete", replication_file_delete_event)
	// Per-user link-request flow (see replication_link.go).
	// Server-to-server: B has no entity yet at link-request time;
	// A's response (link-approved/denied) is keyed on the placeholder
	// not on entity identity.
	a.event_anonymous("link/request", replication_link_request_event)
	a.event_anonymous("link/approved", replication_link_approved_event)
	a.event_anonymous("link/denied", replication_link_denied_event)
	a.event_anonymous("lookup/freshness", replication_freshness_probe_event)
	a.event_anonymous("lookup/user", replication_user_lookup_event)
	// Whole-server pair join-request flow (see replication_join.go).
	// Server-to-server: a fresh replica has no entities at all.
	a.event_anonymous("join/request", replication_join_request_event)
	a.event_anonymous("join/approved", replication_join_approved_event)
	a.event_anonymous("join/denied", replication_join_denied_event)
	a.event_anonymous("pair/membership/change", replication_pair_membership_change_event)
	// System-scope replication for core DBs (see replication_system.go).
	// Pair-scoped, libp2p-signed (no entity signer for settings/apps/
	// domains rows). Last-applier-by-arrival-order wins.
	a.event_anonymous("system/set", replication_system_set_event)
	a.event_anonymous("system/row", replication_system_row_event)
	// Bulk bootstrap protocol (see replication_bootstrap.go). Pair-scoped,
	// libp2p-signed; chunk handlers gate on bootstrap_is_active_source(scope,
	// e.peer) so an unauthorized peer can't inject data into our scope roots.
	// Every bootstrap-time exchange is a synchronous stream RPC: requester
	// opens a stream, writes the request, reads one or more response
	// segments on the same stream until done. No queue.db involvement, no
	// ID-matching dance — the response IS the ACK.
	a.event_anonymous("bootstrap/file/manifest", replication_bootstrap_file_manifest_event)
	a.event_anonymous("bootstrap/file/chunk/fetch", replication_bootstrap_file_chunk_fetch_event)
	a.event_anonymous("bootstrap/db/manifest", replication_bootstrap_db_manifest_event)
	a.event_anonymous("bootstrap/db/fetch", replication_bootstrap_db_fetch_event)
	a.event_anonymous("bootstrap/scope/done", replication_bootstrap_scope_done_event)
	// Cross-host leader election (see leader.go). The claim event is a
	// sync stream RPC: proposer opens, writes LeaderClaimRequest, reads
	// LeaderClaimResponse on the same stream. The granted event is a
	// fire-and-forget queue message: proposer pushes post-success so
	// other peers in the membership mirror the new lease in their
	// leadership row.
	a.event_anonymous("replica/leader/claim", replica_leader_claim_event)
	a.event_anonymous("replica/leader/granted", replica_leader_granted_event)
	// A peer telling us our replication relationship with it has gone
	// irreparable (broken past T_forget) so both sides' admins are
	// notified. See replication_irreparable.go.
	a.event_anonymous("replica/irreparable", replication_irreparable_event)
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
// repl_op_stream returns the per-physical-DB stream key the in-order
// gate chains an op on. Almost always op.Database, but app-system
// exec ops (mochi.access.* / attachments) and app data-DB exec ops
// both travel under op.Database = app.id while writing two distinct
// files (users/<u>/<app>/app.db vs users/<u>/<app>/db/<file>). They
// get independent streams — an "/app" suffix for the app-system one —
// so a stuck op in one doesn't block the other and the bootstrap
// cursor-seed stays per-file exact. The wire op.Database is unchanged;
// only the gate's notion of "which stream" is finer.
func repl_op_stream(op *ReplicationOp) string {
	if op.Operation == repl_op_exec_app_system {
		return op.Database + "/app"
	}
	return op.Database
}

func replication_op_event(e *Event) {
	var op ReplicationOp
	if !e.segment(&op) {
		info("Replication op dropping: cannot decode payload")
		return
	}
	replication_op_receive(e.peer, &op)
}

// replication_op_receive is the framework-layer entry point for an
// inbound op once it has been decoded. Production calls it from
// replication_op_event after CBOR-decoding the Event payload; the
// multi-master test harness calls it directly so the dedup / fence /
// per-stream gate / pending-buffer / cursor-advance machinery is
// exercised end-to-end.
func replication_op_receive(peer string, op *ReplicationOp) {
	db := db_open("db/replication.db")
	seen, _ := db.exists(
		"select 1 from seen where peer=? and scope=? and user=? and sequence=?",
		peer, op.Scope, op.User, op.Sequence)
	if seen {
		debug("Replication op duplicate: peer=%q scope=%q user=%q seq=%d",
			peer, op.Scope, op.User, op.Sequence)
		return
	}

	// Fence check before dispatch: if this op carries a leader-stamp
	// (op.LeaderScope/Key/Fence) and our witness for that lease has
	// already seen a higher fence, the emitter has been superseded and
	// we drop the op silently. Stamp-less ops pass through.
	if !replication_fence_observe(op.LeaderScope, op.LeaderKey, peer, op.Fence) {
		info("Replication op dropped: stale leader fence %d for scope=%q key=%q from peer=%q",
			op.Fence, op.LeaderScope, op.LeaderKey, peer)
		// Record as seen so the sender's queue drops it; further
		// retries with the same fence will just hit the same check.
		db.exec(
			"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, ?, ?, ?, ?)",
			peer, op.Scope, op.User, op.Sequence, now())
		return
	}

	// Per-db in-order gate. Each op chains onto its (peer, scope, user,
	// db) stream via op.Prev — the sequence of the previous op for the
	// same DB. Prev==0 starts (or restarts) the stream; Prev==cursor
	// extends it; Prev<cursor is already applied; Prev>cursor — or no
	// cursor yet — is a gap, buffered in `pending` until the link
	// arrives (claude/plans/replication-test.md Stage 19).
	stream := repl_op_stream(op)
	cursor, anchored := replication_cursor(db, peer, op.Scope, op.User, stream)
	switch {
	case op.Prev == 0:
		// Stream (re)start — apply unconditionally and anchor.
	case anchored && op.Prev == cursor:
		// Chains onto the cursor.
	case anchored && op.Prev < cursor:
		debug("Replication op below cursor: peer=%q scope=%q user=%q db=%q seq=%d prev=%d cursor=%d",
			peer, op.Scope, op.User, stream, op.Sequence, op.Prev, cursor)
		return
	default:
		replication_pending_buffer(db, peer, op)
		debug("Replication op buffered out-of-order: peer=%q scope=%q user=%q db=%q seq=%d prev=%d cursor=%d anchored=%v",
			peer, op.Scope, op.User, stream, op.Sequence, op.Prev, cursor, anchored)
		return
	}

	if replication_op_land(db, peer, op) != ApplyDeferred {
		replication_stream_drain(db, peer, op.Scope, op.User, stream)
	}
}

// replication_cursor returns the apply watermark for an inbound
// (peer, scope, user, db) stream and whether the stream is anchored.
// A cursor row is created by the gate's first Prev==0 op for the
// stream, and seeded by the bootstrap cursor-seed for a freshly-
// replicated user. A stream with no cursor row is un-anchored: a
// Prev==0 op anchors it; a Prev>0 op buffers until the seed lands.
func replication_cursor(db *DB, peer, scope, user, database string) (int64, bool) {
	row, err := db.row("select sequence from cursor where peer=? and scope=? and user=? and db=?", peer, scope, user, database)
	if err != nil || row == nil {
		return 0, false
	}
	seq, _ := row["sequence"].(int64)
	return seq, true
}

// replication_cursor_set advances the apply watermark for a db-stream.
// The cursor only moves forward — a Prev==0 restart carrying an older
// sequence (a straggler at the schema-67 seam) still applies but must
// not rewind a stream other ops have already advanced.
func replication_cursor_set(db *DB, peer, scope, user, database string, sequence int64) {
	db.exec(
		"insert into cursor (peer, scope, user, db, sequence) values (?, ?, ?, ?, ?) "+
			"on conflict(peer, scope, user, db) do update set sequence=max(sequence, excluded.sequence)",
		peer, scope, user, database, sequence)
}

// replication_pending_buffer stores an op that can't apply yet — a gap
// or a deferred op — in `pending`, keyed by its db-stream and Prev.
func replication_pending_buffer(db *DB, peer string, op *ReplicationOp) {
	db.exec(
		"insert or ignore into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		peer, op.Scope, op.User, repl_op_stream(op), op.Sequence, op.Prev, op.Schema, cbor_encode(op), now())
}

// replication_op_land applies one verified, fence-passed op, records it
// in `seen`, and on success advances the per-db cursor. A deferred op
// is buffered; an unrecognised op still advances the cursor so it
// can't wedge the stream.
func replication_op_land(db *DB, peer string, op *ReplicationOp) ApplyResult {
	res := replication_apply_op(op)
	switch res {
	case ApplyApplied:
		db.exec(
			"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, ?, ?, ?, ?)",
			peer, op.Scope, op.User, op.Sequence, now())
		replication_cursor_set(db, peer, op.Scope, op.User, repl_op_stream(op), op.Sequence)
		debug("Replication op applied: peer=%q scope=%q user=%q db=%q seq=%d prev=%d table=%q op=%q",
			peer, op.Scope, op.User, op.Database, op.Sequence, op.Prev, op.Table, op.Operation)
		commit_hook_fire(op.User, op.Database, op.Table, op.Operation, op.UID)
	case ApplyDeferred:
		replication_pending_buffer(db, peer, op)
		debug("Replication op deferred: peer=%q scope=%q user=%q db=%q seq=%d",
			peer, op.Scope, op.User, op.Database, op.Sequence)
	case ApplyInvalid:
		info("Replication op dropping: unrecognised shape peer=%q scope=%q db=%q table=%q op=%q",
			peer, op.Scope, op.Database, op.Table, op.Operation)
		replication_cursor_set(db, peer, op.Scope, op.User, repl_op_stream(op), op.Sequence)
	}
	return res
}

// replication_stream_drain applies buffered ops for one inbound
// db-stream in chain order: the op whose Prev equals the current
// cursor, then the op whose Prev is that op's sequence, and so on. A
// buffered stream-start (Prev==0 — a deferred fresh op) applies
// whether or not the stream is anchored. Stops at the first missing
// link or still-deferred op.
func replication_stream_drain(db *DB, peer, scope, user, database string) {
	for {
		cursor, anchored := replication_cursor(db, peer, scope, user, database)
		var row map[string]any
		if anchored {
			row, _ = db.row(
				"select sequence, payload from pending where peer=? and scope=? and user=? and db=? and prev=?",
				peer, scope, user, database, cursor)
		}
		if row == nil {
			row, _ = db.row(
				"select sequence, payload from pending where peer=? and scope=? and user=? and db=? and prev=0 limit 1",
				peer, scope, user, database)
		}
		if row == nil {
			return
		}
		seq, _ := row["sequence"].(int64)
		payload, _ := row["payload"].([]byte)
		if len(payload) == 0 {
			if s, ok := row["payload"].(string); ok {
				payload = []byte(s)
			}
		}
		var op ReplicationOp
		if err := cbor.Unmarshal(payload, &op); err != nil {
			info("Replication stream drain: malformed payload, dropping (peer=%q db=%q seq=%d): %v",
				peer, database, seq, err)
			db.exec("delete from pending where peer=? and scope=? and user=? and sequence=?", peer, scope, user, seq)
			return
		}
		if replication_op_land(db, peer, &op) == ApplyDeferred {
			replication_pending_kick(&op)
			return
		}
		db.exec("delete from pending where peer=? and scope=? and user=? and sequence=?", peer, scope, user, seq)
	}
}

// replication_apply_op dispatches a verified, deduplicated op to the
// right apply path based on Database + Table. Returns ApplyDeferred
// when the op can't be applied yet (waiting on a local user or app
// DB) so the caller buffers it in `pending` for a later retry;
// ApplyInvalid for unrecognised ops. File ops travel as their own
// events (file/push, file/delete), never through ReplicationOp.
func replication_apply_op(op *ReplicationOp) ApplyResult {
	return replication_apply_sql(op)
}

// replication_apply_sql dispatches a verified op to the right system-DB
// apply handler based on Database and Table. Per-app SQL command
// replication (the opt-out default) lands here via Operation == "exec".
func replication_apply_sql(op *ReplicationOp) ApplyResult {
	if op.Operation == repl_op_exec {
		return replication_apply_sql_command(op)
	}
	if op.Operation == repl_op_exec_app_system {
		return replication_apply_app_system_exec(op)
	}
	if op.Operation == repl_op_exec_user_core {
		return replication_apply_user_core_exec(op)
	}
	switch {
	case op.Scope == repl_scope_app && op.Database == "users" && (op.Operation == "users-row.set" || op.Operation == "users-row.delete"):
		return users_row_decode_and_apply(op.Payload, op.User)
	case op.Scope == repl_scope_app && op.Database == "sessions" && (op.Operation == "sessions-row.set" || op.Operation == "sessions-row.delete"):
		return sessions_row_decode_and_apply(op.Payload, op.User)
	case op.Scope == repl_scope_app && op.Database == "schedule" && (op.Operation == "schedule-row.set" || op.Operation == "schedule-row.delete"):
		return schedule_row_decode_and_apply(op.Payload, op.User)
	case op.Scope == repl_scope_app && op.Database == "notifications" && op.Table == "webpush_delivered":
		var w WebpushDelivered
		if err := cbor.Unmarshal(op.Payload, &w); err != nil {
			info("Replication op webpush_delivered: decode failed: %v", err)
			return ApplyInvalid
		}
		return replication_webpush_delivered_apply(op.User, &w)
	case op.Scope == repl_scope_app && op.Database == "notifications" && op.Table == "email_delivered":
		var em EmailDelivered
		if err := cbor.Unmarshal(op.Payload, &em); err != nil {
			info("Replication op email_delivered: decode failed: %v", err)
			return ApplyInvalid
		}
		return replication_email_delivered_apply(op.User, &em)
	case op.Scope == repl_scope_app && op.Database == "sessions" && op.Table == "sessions":
		switch op.Operation {
		case repl_op_insert:
			var p SessionInsert
			if err := cbor.Unmarshal(op.Payload, &p); err != nil {
				info("Replication op sessions/insert: decode failed: %v", err)
				return ApplyInvalid
			}
			return replication_session_apply_insert(&p)
		case repl_op_delete:
			var p SessionDelete
			if err := cbor.Unmarshal(op.Payload, &p); err != nil {
				info("Replication op sessions/delete: decode failed: %v", err)
				return ApplyInvalid
			}
			return replication_session_apply_delete(&p)
		}
	}
	return ApplyInvalid
}

// replication_session_apply_insert lands a replicated session insert into
// the local sessions.db. Defers when the user isn't yet local (keys-transfer
// hasn't landed). `replace into` makes re-applies idempotent.
func replication_session_apply_insert(p *SessionInsert) ApplyResult {
	if !user_exists(p.UserUID) {
		return ApplyDeferred
	}

	sdb := db_open("db/sessions.db")
	sdb.exec(
		"replace into sessions (user, code, secret, expires, created, accessed, address, agent) values (?, ?, ?, ?, ?, ?, ?, ?)",
		p.UserUID, p.Code, p.Secret, p.Expires, p.Created, p.Accessed, p.Address, p.Agent)
	debug("Replication session-insert applied: user_uid=%q code=%q", p.UserUID, p.Code)
	return ApplyApplied
}

// replication_session_apply_delete removes a session by code on the
// receiver. Unconditional — delete wins over a stale insert at the
// session-revocation layer.
func replication_session_apply_delete(p *SessionDelete) ApplyResult {
	sdb := db_open("db/sessions.db")
	sdb.exec("delete from sessions where code=?", p.Code)
	debug("Replication session-delete applied: code=%q", p.Code)
	return ApplyApplied
}

// web_replication_health serves /_/replication/health: a JSON snapshot of
// the local replication state suitable for LB consumption, operator
// dashboards, and the staged-rollout monitors in Phase H/I/J.
//
// Reports: this host's peer-id, configured server-pair members, per-user
// opt-in counts, pending-buffer depth (with the age of the oldest entry
// as a proxy for replication lag), and per-(user, scope) sequence
// counters for replication outbound flow. Read-only, cheap to call.
func web_replication_health(c *gin.Context) {
	db := db_open("db/replication.db")
	out := gin.H{"peer_id": net_id}

	// Server-pair members.
	pairs := []string{}
	if rows, err := db.rows("select peer from pair order by peer"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok {
				pairs = append(pairs, p)
			}
		}
	}
	out["pair"] = pairs

	// Per-user opt-in counts.
	out["hosts"] = db.integer("select count(*) from hosts")
	out["users_with_hosts"] = db.integer("select count(distinct user) from hosts")

	// Pending buffer.
	pending_count := db.integer("select count(*) from pending")
	out["pending"] = pending_count
	if pending_count > 0 {
		oldest := db.integer("select min(received) from pending")
		out["pending_oldest_age"] = now() - int64(oldest)
	}

	// Seen counts for diagnostics.
	out["seen_total"] = db.integer("select count(*) from seen")

	// Active leaderships held by this host (informational).
	out["leases_held"] = db.integer("select count(*) from leadership where peer=? and expires > ?", net_id, now())

	c.JSON(200, out)
}

// web_replication_progress reports the current bootstrap progress for
// the authenticated user. Used by /login/replicating to display "still
// syncing files (264 left)" instead of "Waiting for approval" once the
// approval has actually landed and bulk-transfer is underway.
//
// Response shape:
//
//	{
//	  "user": { "status": "pending-replication" | "active" },
//	  "approved": true|false,
//	  "scopes": [
//	    { "scope": "files",   "state": "...", "remaining": N, "failed": M },
//	    { "scope": "userdbs", "state": "...", "remaining": N, "failed": M }
//	  ]
//	}
//
// `approved` is true once the source has approved the link — detected
// by the presence of a replication.db.hosts row, which apply_keys
// inserts at approval time. It's the clean signal /login/replicating
// uses to switch from "Waiting for approval" to "Syncing your data",
// independent of how far the individual scopes have progressed (a
// freshly-approved link sits briefly in scope state 'queued', which
// must still read as "approved, syncing").
//
// Only the scopes relevant to this user's link (files + userdbs) are
// returned — apps/sysdbs are whole-server concerns and aren't part of
// the per-user activation gate.
func web_replication_progress(c *gin.Context) {
	u := web_auth(c)
	if u == nil {
		respond_error(c, http.StatusUnauthorized, "authentication_required", "errors.authentication_required", nil)
		return
	}
	rdb := db_open("db/replication.db")
	// Find the source peer for this user. If the user has no hosts row,
	// they're either a regular local user (post-replication or never
	// replicated) or the placeholder was just created and apply_keys
	// hasn't inserted the host row yet — report empty scopes so the
	// page still polls.
	peer := ""
	if row, _ := rdb.row("select peer from hosts where user=? order by added asc limit 1", u.UID); row != nil {
		peer, _ = row["peer"].(string)
	}
	scopes := []gin.H{}
	for _, scope := range []string{bootstrap_scope_files, bootstrap_scope_userdbs} {
		entry := gin.H{"scope": scope, "state": "", "remaining": 0, "failed": 0}
		if peer != "" {
			row, _ := rdb.row("select state, position, failed from bootstrap where scope=? and peer=?", scope, peer)
			if row != nil {
				state, _ := row["state"].(string)
				entry["state"] = state
				position_string, _ := row["position"].(string)
				if position_string != "" {
					remaining, _ := strconv.ParseInt(position_string, 10, 64)
					entry["remaining"] = remaining
				}
				entry["failed"] = row["failed"]
			}
		}
		scopes = append(scopes, entry)
	}
	c.JSON(200, gin.H{
		"user":     gin.H{"status": u.Status},
		"approved": peer != "",
		"scopes":   scopes,
	})
}

// replication_manager drives the periodic pending-buffer drain.
// Deferred ops not unblocked by a keys-transfer (e.g. an app schema
// upgrade that catches the local version up to a sender's, once the
// schema-coordination path lands) get retried on every tick. Also
// surfaces stalled streams to the log when their oldest pending op
// has been stuck longer than stall_warn_seconds, so operators see the
// drift before users notice missing data.
//
// Once per pending_gc_period_seconds (default 1h) it also runs the
// unfillable-pending GC so months of intermittent peer churn don't
// silently accumulate dropped-but-buffered rows in replication.db.pending.
// See replication_pending_gc.
func replication_manager() {
	last_gc := now()
	for range time.Tick(30 * time.Second) {
		replication_pending_drain()
		replication_pending_warn_stalled()
		replication_wiped_rebootstrap()
		if now()-last_gc >= pending_gc_period_seconds {
			replication_irreparable_scan()
			replication_offline_scan()
			replication_pending_gc()
			last_gc = now()
		}
	}
}

// pending_gc_period_seconds is the interval between automatic
// unfillable-pending GC passes. GC is cheap (scans the stalled-stream
// list, deletes per-row), but no point running it every 30s when the
// TTL is measured in days.
const pending_gc_period_seconds = 60 * 60

// stall_warn_seconds is how long a stream may keep a pending op before
// it gets a periodic warning in the log. Tuned to be loud enough for
// operators to notice without spamming on transient gaps that drain
// naturally within a minute.
const stall_warn_seconds = 15 * 60

func replication_pending_warn_stalled() {
	threshold := now() - stall_warn_seconds
	for _, s := range replication_pending_stalled() {
		if s.Oldest > threshold {
			continue
		}
		warn("Replication stream stalled: peer=%q scope=%q user=%q db=%q cursor=%d anchored=%v predecessor.minimum=%d predecessor.maximum=%d count=%d age=%ds",
			s.Peer, s.Scope, s.User, s.Database,
			s.Cursor, s.Anchored, s.Predecessor.Minimum, s.Predecessor.Maximum, s.Count,
			now()-s.Oldest)
	}
}

// replication_app_drain re-attempts pending-buffer drain for one
// post_migration_drain_async spawns the per-(user, app) drain in a
// background goroutine. Production points it at the spawning version;
// tests that mutate data_dir override it with a no-op so the
// goroutine doesn't race with host-switching. Assigned in init() to
// break a static-initialization cycle (replication_app_drain
// transitively references db_app which calls this var).
var post_migration_drain_async func(user, app_id string)

func init() {
	post_migration_drain_async = func(user, app_id string) {
		go replication_app_drain(user, app_id)
	}
}

// (user, app) tuple, called opportunistically when the app's per-user
// DB is opened (db_app). Covers the schema-just-upgraded case: if
// pending ops were deferred for schema-skew and the local app then
// migrated forward, the next request to the app triggers a fresh
// drain attempt without waiting for the 30 s manager tick.
func replication_app_drain(user, appID string) {
	if user == "" || appID == "" {
		return
	}
	db := db_open("db/replication.db")
	streams, err := db.rows(
		"select distinct peer, scope from pending where user=? and db=?",
		user, appID)
	if err != nil {
		return
	}
	for _, s := range streams {
		peer, _ := s["peer"].(string)
		scope, _ := s["scope"].(string)
		replication_stream_drain(db, peer, scope, user, appID)
	}
}

// replication_pending_kick is invoked when a buffered op stays
// deferred. Schema-skew (op.Schema > local) and "app not installed yet"
// are the two app-side reasons an op gets stuck; both are unstuck by
// app_check_install downloading the matching published version. We
// kick best-effort — the call is idempotent when the local version is
// already current, so a no-op cost when the deferral is for a different
// reason (e.g. unknown user awaiting keys-transfer).
func replication_pending_kick(op *ReplicationOp) {
	if op == nil {
		return
	}
	if op.Database == "" || !valid(op.Database, "entity") {
		// system-DB tables (users, sessions, notifications) or dev /
		// internal apps — no publisher download path applies.
		return
	}
	if !replication_pending_kick_due(op.Database) {
		return
	}
	go app_check_install(op.Database)
}

// replication_pending_kick_state tracks the last time
// replication_pending_kick fired for each app id, so a busy drain
// doesn't fan out duplicate app_check_install goroutines for the same
// stuck app every 30 seconds. The TTL is long compared to the drain
// interval but short compared to operator patience.
var (
	replication_pending_kick_last  = map[string]int64{}
	replication_pending_kick_mu    sync.Mutex
	replication_pending_kick_ttl_s = int64(300) // 5 minutes
)

func replication_pending_kick_due(appID string) bool {
	replication_pending_kick_mu.Lock()
	defer replication_pending_kick_mu.Unlock()
	now_ts := now()
	if last, ok := replication_pending_kick_last[appID]; ok && now_ts-last < replication_pending_kick_ttl_s {
		return false
	}
	replication_pending_kick_last[appID] = now_ts
	return true
}

// StalledStream describes one (peer, scope, user, db) stream whose
// pending buffer cannot drain because the next-applicable op's
// predecessor is missing (or the stream has never been anchored at
// all).
//
// Reported by replication_pending_stalled() for operator visibility -
// these stalls accumulate silently otherwise: writes on the sender
// succeed, ops arrive at the receiver, but the apply gate can't chain
// them past the gap. Recovery in V1 is operator-driven (the dropped
// predecessor ops aren't replayable from the sender's current state);
// future delta-bootstrap work will fill the gap by shipping current
// row state from the sender, advancing the cursor past the missing
// sequences and accepting that the intervening per-op deltas are lost.
type StalledStream struct {
	Peer        string           `json:"peer"`
	Scope       string           `json:"scope"`
	User        string           `json:"user"`
	Database    string           `json:"database"`
	Cursor      int64            `json:"cursor"`
	Anchored    bool             `json:"anchored"`
	Predecessor PredecessorRange `json:"predecessor"`
	Count       int64            `json:"count"`
	Oldest      int64            `json:"oldest"`
}

// PredecessorRange holds the minimum and maximum predecessor sequence
// values across a stalled stream's pending ops. The pending table's
// internal column is `prev`; the external surfaces spell it out so
// admin / mochictl callers don't have to know the abbreviation.
type PredecessorRange struct {
	Minimum int64 `json:"minimum"`
	Maximum int64 `json:"maximum"`
}

// replication_pending_stalled returns the (peer, scope, user, db)
// streams whose pending buffer cannot drain - either anchored with a
// gap (smallest prev > cursor) or unanchored with no Prev==0 op
// present. Streams whose pending will drain on the next tick (op with
// prev=cursor, or an unanchored stream-start with prev=0) are
// excluded.
func replication_pending_stalled() []StalledStream {
	db := db_open("db/replication.db")
	rows, err := db.rows(
		"select peer, scope, user, db, count(*) as count, min(prev) as min_prev, max(prev) as max_prev, min(received) as oldest from pending where db != '' group by peer, scope, user, db")
	if err != nil {
		return nil
	}
	var stalled []StalledStream
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		scope, _ := r["scope"].(string)
		user, _ := r["user"].(string)
		database, _ := r["db"].(string)
		min_prev, _ := r["min_prev"].(int64)
		max_prev, _ := r["max_prev"].(int64)
		count, _ := r["count"].(int64)
		oldest, _ := r["oldest"].(int64)
		cursor, anchored := replication_cursor(db, peer, scope, user, database)
		// Will drain naturally on the next tick: anchored chain has
		// the next op (prev=cursor) present, or the unanchored stream
		// has a Prev==0 start available.
		if anchored {
			if min_prev <= cursor {
				continue
			}
		} else {
			if min_prev == 0 {
				continue
			}
		}
		stalled = append(stalled, StalledStream{
			Peer:        peer,
			Scope:       scope,
			User:        user,
			Database:    database,
			Cursor:      cursor,
			Anchored:    anchored,
			Predecessor: PredecessorRange{Minimum: min_prev, Maximum: max_prev},
			Count:       count,
			Oldest:      oldest,
		})
	}
	return stalled
}

// pending_gc_default_ttl_days is the default age above which an
// unfillable pending row is purged. Overridable via setting
// `replication.pending.unfillable_ttl_days`. A month is conservative -
// operator intervention via `mochictl replication resync` or
// `mochictl replica reset` should have happened by then if the stuck
// stream mattered.
const pending_gc_default_ttl_days = 30

// replication_pending_gc walks the stalled-stream list (same classifier
// the warning loop uses) and deletes pending rows whose `received`
// timestamp is older than the configured TTL. Every drop emits an
// audit event with (peer, scope, user, db, sequence, age_seconds) so
// the operator can grep the audit log for "what did we lose".
//
// Returns the total number of rows dropped. Safe to call on demand
// (mochictl replication pending gc) as well as from the manager loop.
func replication_pending_gc() int {
	ttl_days := int64(pending_gc_default_ttl_days)
	if s := setting_get("replication.pending.unfillable_ttl_days", ""); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
			ttl_days = v
		}
	}
	cutoff := now() - ttl_days*86400
	stalled := replication_pending_stalled()
	if len(stalled) == 0 {
		return 0
	}
	db := db_open("db/replication.db")
	dropped := 0
	for _, s := range stalled {
		// Per-stream scan of just the aged rows. Cheap: pending is
		// bounded by the stalled-stream count and the rows we want are
		// the long-tail end of each.
		rows, err := db.rows(
			"select sequence, received from pending where peer=? and scope=? and user=? and db=? and received<?",
			s.Peer, s.Scope, s.User, s.Database, cutoff)
		if err != nil {
			continue
		}
		for _, r := range rows {
			sequence, _ := r["sequence"].(int64)
			received, _ := r["received"].(int64)
			db.exec(
				"delete from pending where peer=? and scope=? and user=? and db=? and sequence=?",
				s.Peer, s.Scope, s.User, s.Database, sequence)
			age := now() - received
			info("Replication pending GC: dropped peer=%q scope=%q user=%q db=%q seq=%d age=%ds",
				s.Peer, s.Scope, s.User, s.Database, sequence, age)
			audit_replication_pending_purged(s.Peer, s.Scope, s.User, s.Database, sequence, age)
			dropped++
		}
	}
	if dropped > 0 {
		info("Replication pending GC: dropped %d unfillable row(s) older than %d days", dropped, ttl_days)
	}
	return dropped
}

// replication_pending_drain re-evaluates buffered ops: each
// (peer, scope, user, db) stream drains in chain order from its
// cursor. Pre-schema-67 rows (no db/prev) drain once in arrival order.
//
// Called automatically after a keys-transfer (when a new user lands,
// pending ops for that user become applyable) and on a periodic
// background tick.
func replication_pending_drain() {
	db := db_open("db/replication.db")

	if streams, err := db.rows("select distinct peer, scope, user, db from pending where db != ''"); err == nil {
		for _, s := range streams {
			peer, _ := s["peer"].(string)
			scope, _ := s["scope"].(string)
			user, _ := s["user"].(string)
			database, _ := s["db"].(string)
			replication_stream_drain(db, peer, scope, user, database)
		}
	}

	// Legacy: ops buffered before the schema-67 per-db re-key carry no
	// db/prev — drain them in arrival order, the pre-67 behaviour.
	rows, err := db.rows(
		"select peer, scope, user, sequence, payload from pending where db='' order by received limit 100")
	if err != nil {
		return
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		scope, _ := r["scope"].(string)
		user_field, _ := r["user"].(string)
		sequence, _ := r["sequence"].(int64)
		payload, _ := r["payload"].([]byte)
		if len(payload) == 0 {
			if s, ok := r["payload"].(string); ok {
				payload = []byte(s)
			}
		}

		var op ReplicationOp
		if err := cbor.Unmarshal(payload, &op); err != nil {
			info("Replication pending drain: malformed payload, dropping (peer=%q seq=%d): %v", peer, sequence, err)
			db.exec("delete from pending where peer=? and scope=? and user=? and sequence=?", peer, scope, user_field, sequence)
			continue
		}

		switch replication_apply_op(&op) {
		case ApplyApplied:
			db.exec(
				"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, ?, ?, ?, ?)",
				peer, scope, user_field, sequence, now())
			db.exec("delete from pending where peer=? and scope=? and user=? and sequence=?", peer, scope, user_field, sequence)
			debug("Replication pending drain: applied (peer=%q scope=%q user=%q seq=%d)", peer, scope, user_field, sequence)
		case ApplyDeferred:
			// Still not ready — leave in pending. Kick auxiliary
			// progress where we know it might unblock the op:
			// an exec op referencing an entity-style app id whose
			// version isn't installed locally is unblocked by an
			// app_check_install side-effect that downloads the
			// app from the publisher. Without this, ops sit waiting
			// for the next 24-hour apps_manager tick.
			replication_pending_kick(&op)
		case ApplyInvalid:
			info("Replication pending drain: invalid op dropped (peer=%q seq=%d)", peer, sequence)
			db.exec("delete from pending where peer=? and scope=? and user=? and sequence=?", peer, scope, user_field, sequence)
		}
	}
}

// replication_webpush_delivered_apply lands a remote webpush_delivered
// row into the user's local notifications.db. Resolves user_uid to a
// local users.id; defers when the user isn't yet local. `insert or
// ignore` makes re-applies idempotent. The apply path only needs
// User.ID for the db_user() path lookup so it skips the full
// user_by_uid (which insists on a non-nil identity entity).
func replication_webpush_delivered_apply(userUID string, w *WebpushDelivered) ApplyResult {
	if w.Endpoint == "" || w.EventID == "" {
		return ApplyInvalid
	}

	if !user_exists(userUID) {
		return ApplyDeferred
	}
	u := &User{UID: userUID}

	db := webpush_dedup_db(u)
	db.exec("insert or ignore into webpush_delivered (endpoint, event_id, ts) values (?, ?, ?)", w.Endpoint, w.EventID, w.TS)
	debug("Replication webpush_delivered apply: user_uid=%q endpoint=%q event_id=%q", userUID, w.Endpoint, w.EventID)
	return ApplyApplied
}

// replication_email_delivered_apply lands a remote email_delivered row.
// Same shape as webpush.
func replication_email_delivered_apply(userUID string, em *EmailDelivered) ApplyResult {
	if em.Address == "" || em.EventID == "" {
		return ApplyInvalid
	}

	if !user_exists(userUID) {
		return ApplyDeferred
	}
	u := &User{UID: userUID}

	db := email_dedup_db(u)
	db.exec("insert or ignore into email_delivered (address, event_id, ts) values (?, ?, ?)", em.Address, em.EventID, em.TS)
	debug("Replication email_delivered apply: user_uid=%q address=%q event_id=%q", userUID, em.Address, em.EventID)
	return ApplyApplied
}

// user_exists returns true when the given user UID has a local users.db
// row. Used by replication apply handlers to decide whether a remote op
// should be applied now or deferred until a keys-transfer lands.
func user_exists(uid string) bool {
	if uid == "" {
		return false
	}
	udb := db_open("db/users.db")
	exists, _ := udb.exists("select 1 from users where uid=?", uid)
	return exists
}

// replication_emit_webpush_delivered fans out a webpush dedup row to
// the user's host set. Called from webpush_mark_delivered after the
// local insert. The op is keyed by (endpoint, event_id) — receivers
// `insert or ignore` so a concurrent same-replica race results in the
// row being present on every host rather than divergent state.
func replication_emit_webpush_delivered(userUID, endpoint, event_id string, ts int64) {
	if userUID == "" {
		return
	}
	payload := cbor_encode(&WebpushDelivered{Endpoint: endpoint, EventID: event_id, TS: ts})
	replication_emit(userUID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      userUID,
		Database:  "notifications",
		Table:     "webpush_delivered",
		Operation: repl_op_insert,
		Payload:   payload,
	})
}

// replication_emit_email_delivered: same as webpush_delivered but
// keyed by (address, event_id).
func replication_emit_email_delivered(userUID, address, event_id string, ts int64) {
	if userUID == "" {
		return
	}
	payload := cbor_encode(&EmailDelivered{Address: address, EventID: event_id, TS: ts})
	replication_emit(userUID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      userUID,
		Database:  "notifications",
		Table:     "email_delivered",
		Operation: repl_op_insert,
		Payload:   payload,
	})
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
		Scope:     repl_scope_app,
		User:      userUID,
		Database:  "sessions",
		Table:     "sessions",
		Operation: repl_op_insert,
		Payload:   payload,
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
		Scope:     repl_scope_app,
		User:      userUID,
		Database:  "sessions",
		Table:     "sessions",
		Operation: repl_op_delete,
		Payload:   payload,
	})
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
			if peer == "" || peer == net_id {
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

	sender_ok := false
	for _, ent := range kt.Entities {
		if ent.ID == signer {
			sender_ok = true
			break
		}
	}
	if !sender_ok {
		info("Replication keys-transfer dropping: signer %q not in transferred entities (username=%q peer=%q)",
			signer, kt.Username, originPeer)
		return 0
	}

	udb := db_open("db/users.db")

	var userUID string
	if row, err := udb.row("select uid from users where username=?", kt.Username); err == nil && row != nil {
		userUID, _ = row["uid"].(string)
	}

	if userUID == "" {
		role := kt.Role
		if role == "" {
			role = "user"
		}
		// Empty methods is the valid "any one factor signs you in" default;
		// preserve it rather than coercing to email-required.
		methods := kt.Methods
		status := kt.Status
		if status == "" {
			status = "active"
		}

		// Prefer the source's UID. The bootstrap file + DB transfer
		// copied this user's data into users/<source-uid>/...; if we
		// generated a fresh local uid here every per-user-app DB and
		// file tree would be invisible to the user after login.
		// Fall back to a freshly-minted uid only if the source didn't
		// supply one (older per-user-link callers).
		userUID = kt.UID
		if userUID == "" {
			userUID = uid()
		}
		if _, err := udb.internal.Exec("insert into users (uid, username, role, methods, status) values (?, ?, ?, ?, ?)",
			userUID, kt.Username, role, methods, status); err != nil {
			warn("Replication keys-transfer: failed to insert user %q: %v", kt.Username, err)
			return 0
		}
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
			ent.ID, ent.Private, ent.Fingerprint, userUID, ent.Parent, ent.Class, ent.Name, ent.Privacy, ent.Data, ent.Published)
		inserted++
	}

	// Replicate OAuth provider links. Re-keyed to the local userUID;
	// (provider, subject) is the unique constraint and is stable across
	// hosts, so the INSERT OR IGNORE makes a re-applied keys-transfer
	// idempotent. Without this, a user who signed up via OAuth on the
	// source can't log in on the replica (oauth_login lookup misses,
	// falls through to the email-collision refusal).
	for _, link := range kt.OAuth {
		if link.Provider == "" || link.Subject == "" {
			continue
		}
		udb.exec(`insert or ignore into oauth
			(user, provider, subject, email, verified, name, created)
			values (?, ?, ?, ?, ?, ?, ?)`,
			userUID, link.Provider, link.Subject, link.Email, boolint(link.Verified), link.Name, link.Created)
	}

	// Passkeys / WebAuthn credentials. Cross-host stable id is the
	// blob credential ID. INSERT OR IGNORE on the PK (id) makes
	// re-application idempotent.
	for _, c := range kt.Credentials {
		if len(c.ID) == 0 {
			continue
		}
		udb.exec(`insert or ignore into credentials
			(id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created)
			values (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			c.ID, userUID, c.PublicKey, c.SignCount, c.Name, c.Transports, boolint(c.BackupEligible), boolint(c.BackupState), c.Created)
	}

	// Recovery codes (hashed). The integer PK is per-host; the natural
	// cross-host identity is (user, hash). Skip if the receiver
	// already has the same hash for the same user.
	for _, r := range kt.Recovery {
		if r.Hash == "" {
			continue
		}
		if exists, _ := udb.exists("select 1 from recovery where user=? and hash=?", userUID, r.Hash); exists {
			continue
		}
		udb.exec(`insert into recovery (user, hash, created) values (?, ?, ?)`,
			userUID, r.Hash, r.Created)
	}

	// API tokens. Hash is the cross-host stable PK.
	for _, t := range kt.Tokens {
		if t.Hash == "" {
			continue
		}
		udb.exec(`insert or ignore into tokens
			(hash, user, app, name, scopes, created, expires)
			values (?, ?, ?, ?, ?, ?, ?)`,
			t.Hash, userUID, t.App, t.Name, t.Scopes, t.Created, t.Expires)
	}

	// TOTP secret (one per user).
	if kt.Totp != nil && kt.Totp.Secret != "" {
		udb.exec(`insert or ignore into totp
			(user, secret, verified, created)
			values (?, ?, ?, ?)`,
			userUID, kt.Totp.Secret, boolint(kt.Totp.Verified), kt.Totp.Created)
	}

	// Seed the in-order apply cursors for the user's non-file streams
	// (users, sessions) from the source's tail snapshot so live ops
	// chain instead of buffering forever on a fresh replica. cursor_set
	// is monotonic — a re-applied keys-transfer is idempotent.
	rdb := db_open("db/replication.db")
	for stream, seq := range kt.Seeds {
		replication_cursor_set(rdb, originPeer, repl_scope_app, userUID, stream, seq)
		replication_stream_drain(rdb, originPeer, repl_scope_app, userUID, stream)
	}

	debug("Replication keys-transfer applied: username=%q entities=%d inserted=%d oauth=%d credentials=%d recovery=%d tokens=%d totp=%v (from peer %q)",
		kt.Username, len(kt.Entities), inserted, len(kt.OAuth),
		len(kt.Credentials), len(kt.Recovery), len(kt.Tokens), kt.Totp != nil, originPeer)

	// A new user (or new entities) just landed — any session inserts
	// previously deferred while waiting on this user now have a fighting
	// chance. Drain the pending buffer.
	if inserted > 0 {
		replication_pending_drain()
	}

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
func replication_transfer_keys(userUID string, peer string) bool {
	if peer == "" || peer == net_id {
		return false
	}
	kt, ok := build_keys_transfer(userUID)
	if !ok {
		return false
	}
	from := kt.Entities[0].ID
	m := message(from, "", "replication", "keys/transfer")
	m.add(kt)
	m.send_peer(peer)
	return true
}

// build_keys_transfer assembles the KeysTransfer payload for a user
// without sending it. Split out from replication_transfer_keys so the
// multi-master bootstrap test (#57) can exercise the real payload-build
// and feed the result straight into replication_keys_transfer_apply on
// a receiver host - no wire mock, no duplicated SQL.
func build_keys_transfer(user_uid string) (*KeysTransfer, bool) {
	users := db_open("db/users.db")

	var u User
	if !users.scan(&u, "select uid, username, role, methods, status from users where uid=?", user_uid) {
		warn("Replication transfer-keys: user %q not found", user_uid)
		return nil, false
	}

	rows, err := users.rows("select id, private, fingerprint, parent, class, name, privacy, data, published from entities where user=?", user_uid)
	if err != nil {
		warn("Replication transfer-keys: failed to read entities for user %q: %v", user_uid, err)
		return nil, false
	}
	if len(rows) == 0 {
		warn("Replication transfer-keys: no entities for user %q", user_uid)
		return nil, false
	}

	kt := KeysTransfer{
		UID:      u.UID,
		Username: u.Username,
		Role:     u.Role,
		Methods:  u.Methods,
		Status:   u.Status,
	}
	// Snapshot the non-file streams' tails so the replica seeds its
	// in-order apply cursors — the caller has already added `peer` as
	// a recipient, so the first op it receives chains onto these.
	kt.Seeds = map[string]int64{
		"users":    replication_tail(user_uid, repl_scope_app, "users"),
		"sessions": replication_tail(user_uid, repl_scope_app, "sessions"),
	}
	if oauth_rows, err := users.rows("select provider, subject, email, verified, name, created from oauth where user=?", user_uid); err == nil {
		for _, or := range oauth_rows {
			link := KeysOauth{
				Provider: to_string(or["provider"]),
				Subject:  to_string(or["subject"]),
				Email:    to_string(or["email"]),
				Name:     to_string(or["name"]),
			}
			if v, ok := or["verified"].(int64); ok {
				link.Verified = v != 0
			}
			if v, ok := or["created"].(int64); ok {
				link.Created = v
			}
			if link.Provider == "" || link.Subject == "" {
				continue
			}
			kt.OAuth = append(kt.OAuth, link)
		}
	}
	if cred_rows, err := users.rows("select id, public_key, sign_count, name, transports, backup_eligible, backup_state, created from credentials where user=?", user_uid); err == nil {
		for _, cr := range cred_rows {
			c := KeysCredential{
				Name:       to_string(cr["name"]),
				Transports: to_string(cr["transports"]),
			}
			// db.rows() converts []byte to string defensively; use
			// to_bytes to recover the raw BLOB bytes for the
			// credential id + public key.
			c.ID = to_bytes(cr["id"])
			c.PublicKey = to_bytes(cr["public_key"])
			if v, ok := cr["sign_count"].(int64); ok {
				c.SignCount = v
			}
			if v, ok := cr["backup_eligible"].(int64); ok {
				c.BackupEligible = v != 0
			}
			if v, ok := cr["backup_state"].(int64); ok {
				c.BackupState = v != 0
			}
			if v, ok := cr["created"].(int64); ok {
				c.Created = v
			}
			if len(c.ID) == 0 {
				continue
			}
			kt.Credentials = append(kt.Credentials, c)
		}
	}
	if rec_rows, err := users.rows("select hash, created from recovery where user=?", user_uid); err == nil {
		for _, rr := range rec_rows {
			r := KeysRecovery{Hash: to_string(rr["hash"])}
			if v, ok := rr["created"].(int64); ok {
				r.Created = v
			}
			if r.Hash == "" {
				continue
			}
			kt.Recovery = append(kt.Recovery, r)
		}
	}
	if token_rows, err := users.rows("select hash, app, name, scopes, created, expires from tokens where user=?", user_uid); err == nil {
		for _, tr := range token_rows {
			t := KeysToken{
				Hash:   to_string(tr["hash"]),
				App:    to_string(tr["app"]),
				Name:   to_string(tr["name"]),
				Scopes: to_string(tr["scopes"]),
			}
			if v, ok := tr["created"].(int64); ok {
				t.Created = v
			}
			if v, ok := tr["expires"].(int64); ok {
				t.Expires = v
			}
			if t.Hash == "" {
				continue
			}
			kt.Tokens = append(kt.Tokens, t)
		}
	}
	if totp_row, err := users.row("select secret, verified, created from totp where user=?", user_uid); err == nil && totp_row != nil {
		t := &KeysTotp{Secret: to_string(totp_row["secret"])}
		if v, ok := totp_row["verified"].(int64); ok {
			t.Verified = v != 0
		}
		if v, ok := totp_row["created"].(int64); ok {
			t.Created = v
		}
		if t.Secret != "" {
			kt.Totp = t
		}
	}
	for _, r := range rows {
		id, _ := r["id"].(string)
		if id == "" {
			continue
		}
		ent := KeysEntity{
			ID:          id,
			Private:     to_string(r["private"]),
			Fingerprint: to_string(r["fingerprint"]),
			Parent:      to_string(r["parent"]),
			Class:       to_string(r["class"]),
			Name:        to_string(r["name"]),
			Privacy:     to_string(r["privacy"]),
			Data:        to_string(r["data"]),
		}
		if pub, ok := r["published"].(int64); ok {
			ent.Published = pub
		}
		kt.Entities = append(kt.Entities, ent)
	}
	if len(kt.Entities) == 0 {
		warn("Replication transfer-keys: user %q has no valid entities", user_uid)
		return nil, false
	}
	return &kt, true
}

// to_string converts a SQLite map value to a string, handling both []byte
// and string cases. Returns "" for nil or unconvertible values.
func to_string(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	return ""
}

// to_bytes is the symmetric helper for BLOB columns. db.rows() converts
// all []byte to string defensively (TEXT columns can come back as
// []byte from some SQLite paths, the catch-all simplifies consumers)
// so a callsite reading a real BLOB column sees `string` rather than
// `[]byte`. Go strings are byte-sequence-equivalent to []byte, so the
// round-trip preserves every byte; this helper just makes the
// conversion explicit at the BLOB-reading site. Returns nil for nil
// or unconvertible values.
func to_bytes(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	}
	return nil
}

// replication_membership_update is the local side: bumps the user's
// membership sequence, replaces local hosts with the new set, and emits a
// membership-change announcement to every peer in the new set.
//
// `hosts` is the host set as the caller knows it. Callers build it from
// `select peer from hosts` — the local hosts table, which by
// construction never lists this server itself. This function adds
// `net_id` so the *broadcast* set is complete: a server running a
// membership-update is, by definition, a host of that user (it holds
// their account and manages their host set), so it belongs in the set
// every replica is told about. Without this the emitted MembershipChange
// omitted the origin, and each replica's apply (which deletes + rewrites
// its hosts table from the payload) dropped the origin from its own host
// set — so the replica could no longer fan its writes back to the
// source. (Caught 2026-05-21: a per-user replica's "My hosts" listed a
// stale peer but not the source server.)
//
// The local-table rewrite still filters self out (a server isn't its
// own host), so adding net_id only affects the outbound set.
//
// Package-level alias so callers route through this hook; tests can
// replace it with a no-op to keep the send_peer goroutines (which write
// to queue.db) from outliving their setup tear-down.
var replication_membership_update = replication_membership_update_impl

// replication_membership_full_set returns the complete membership set
// for a user: this server (net_id) plus `hosts`, de-duplicated with
// blanks dropped and self first. Callers build `hosts` from the local
// `hosts` table, which by construction never lists this server — so
// without prepending net_id the broadcast membership set omits the
// origin, and replicas applying it drop the origin from their own host
// set. A server running a membership-update is always a host of that
// user, so it always belongs in the set.
func replication_membership_full_set(hosts []string) []string {
	seen := map[string]bool{}
	full := make([]string, 0, len(hosts)+1)
	for _, peer := range append([]string{net_id}, hosts...) {
		if peer == "" || seen[peer] {
			continue
		}
		seen[peer] = true
		full = append(full, peer)
	}
	return full
}

func replication_membership_update_impl(user string, hosts []string) {
	seq := replication_sequence_next(user, "membership")

	// The broadcast set must include this server (the origin); the
	// local-table rewrite below still filters self out.
	hosts = replication_membership_full_set(hosts)

	db := db_open("db/replication.db")
	db.exec("delete from hosts where user=?", user)
	for _, peer := range hosts {
		if peer == "" || peer == net_id {
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
		if peer == "" || peer == net_id {
			continue
		}
		m := message(from, from, "replication", "host/membership/change")
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
			if p, ok := r["peer"].(string); ok && p != "" && p != net_id {
				set[p] = true
			}
		}
	}

	if rows, err := db.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" && p != net_id {
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

// replication_emit_locks serialises per-(user, scope, db) emit
// critical sections. Bounded by the number of distinct streams the
// server has ever emitted on; locks are cheap (a sync.Mutex each)
// and the map grows monotonically, so we don't bother evicting.
var (
	replication_emit_locks_mu sync.Mutex
	replication_emit_locks    = map[string]*sync.Mutex{}
)

func replication_emit_lock(user, scope, database string) *sync.Mutex {
	tag := user + "|" + scope + "|" + database
	replication_emit_locks_mu.Lock()
	defer replication_emit_locks_mu.Unlock()
	mu, ok := replication_emit_locks[tag]
	if !ok {
		mu = &sync.Mutex{}
		replication_emit_locks[tag] = mu
	}
	return mu
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

// replication_tail_advance records `sequence` as the last op emitted
// for the (user, scope, database) db-stream and returns the previous
// last — the value stamped on the op as Prev. Returns 0 for the first
// op of a stream.
func replication_tail_advance(user, scope, database string, sequence int64) int64 {
	rdb := db_open("db/replication.db")
	var prev int64
	if row, err := rdb.row("select last from tail where user=? and scope=? and db=?", user, scope, database); err == nil && row != nil {
		prev, _ = row["last"].(int64)
	}
	rdb.exec(
		"insert into tail (user, scope, db, last) values (?, ?, ?, ?) "+
			"on conflict(user, scope, db) do update set last=excluded.last",
		user, scope, database, sequence)
	return prev
}

// replication_tail returns the last sequence emitted for the
// (user, scope, database) db-stream — the value a fresh replica seeds
// its apply cursor to. 0 if the stream has never emitted.
func replication_tail(user, scope, database string) int64 {
	rdb := db_open("db/replication.db")
	if row, err := rdb.row("select last from tail where user=? and scope=? and db=?", user, scope, database); err == nil && row != nil {
		last, _ := row["last"].(int64)
		return last
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
	replication_emit_to(user, op, nil)
}

// replication_emit_to_peer emits the op exclusively to one peer, used
// for pair-backfill where existing pair members already hold the row.
func replication_emit_to_peer(user string, op *ReplicationOp, peer string) {
	replication_emit_to(user, op, []string{peer})
}

// replication_emit_to is the package-level emit function variable so
// tests (notably the multi-master harness) can intercept every
// per-user-scope op and route via a deterministic in-memory wire model.
// Production points it at replication_emit_to_real.
var replication_emit_to = replication_emit_to_real

func replication_emit_to_real(user string, op *ReplicationOp, peers []string) {
	if peers == nil {
		peers = recipients(user)
	}
	// Withhold from any peer whose relationship is irreparable - it can't
	// apply until an operator re-bootstraps it, so emitting is wasted churn
	// (this is what piled 90k undeliverable ops onto the wiped mochi2).
	if len(peers) > 0 {
		kept := peers[:0:0]
		for _, p := range peers {
			if !irreparable_emit_skip(user, p) {
				kept = append(kept, p)
			}
		}
		peers = kept
	}
	if len(peers) == 0 {
		return
	}

	if op.Scope != repl_scope_app {
		// TODO: core-scope signing needs a parallel message type that
		// signs with net_private (server_sign) rather than entity_sign.
		// See task #34.
		debug("Replication emit core-scope not yet wired (db=%q table=%q)", op.Database, op.Table)
		return
	}

	// Pick any owned identity for this user as the signing entity. The
	// `user` column is now the TEXT uid (v53), so the join is direct.
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

	// Serialise sequence allocation + tail advance per
	// (user, scope, db). Both helpers use SELECT-then-UPDATE patterns
	// that race when two goroutines emit concurrently: both read the
	// same pre-update value, both write, both return the same prev
	// or sequence. Receiver applies one op cleanly and silently drops
	// the duplicate as "below cursor" - the stream chain corrupts and
	// every subsequent op buffers forever waiting for the lost link.
	// Surfaced on mochi2 as 668/272 entries stalled on feeds/projects;
	// see task #93. The mutex covers the visible bug; the helpers'
	// internal SELECT-then-UPDATE patterns should also be rewritten
	// atomically (sequence_next via UPSERT...RETURNING; tail_advance
	// via dual-column UPSERT...RETURNING the OLD value) as a follow-up
	// so callers outside this critical section also stay safe.
	stream := repl_op_stream(op)
	stream_mu := replication_emit_lock(user, op.Scope, stream)
	stream_mu.Lock()
	op.Sequence = replication_sequence_next(user, op.Scope)
	op.Prev = replication_tail_advance(user, op.Scope, stream, op.Sequence)
	stream_mu.Unlock()

	// Auto-fill the fence when the caller declared a leader scope/key
	// but didn't supply the fence explicitly. Receivers compare against
	// their fence_witness for (LeaderScope, LeaderKey) and drop ops
	// whose fence is strictly less than the highest seen.
	if op.LeaderScope != "" && op.LeaderKey != "" && op.Fence == 0 {
		op.Fence = replication_leader_fence(op.LeaderScope, op.LeaderKey)
	}

	for _, peer := range peers {
		m := message(from, from, "replication", "sql/op")
		m.add(op)
		m.send_peer(peer)
	}
}

// ============================================================
// Go-side internal exec replication
// ============================================================
//
// Go-side APIs (mochi.access.*, mochi.group.*, attachments helpers, etc)
// hit per-user databases via *DB.exec - the Starlark-side
// db_replicate_after_exec interception doesn't see them. The functions
// below emit + apply replicated SQL commands for those writes.
//
// Two flavours, one per file role:
//
//   - exec-app-system: writes to users/<uid>/<app>/app.db (access,
//     attachments). On the receiver, Database carries the app id;
//     the apply path resolves db_app_system(user, app).
//
//   - exec-user-core : writes to users/<uid>/user.db (groups,
//     accounts, interests, permissions, settings). No app context;
//     Database is the sentinel "user". The apply path opens
//     db_user(user, "user"), which is idempotent and creates tables
//     on first call so the receiver schema is always ready.
//
// Same SQLCommand payload as the Starlark exec path, same FK-defer
// behaviour, same parallel-queue serialization per (target, entity).

func replication_emit_app_system_exec(user *User, app *App, sql string, args []any) {
	if user == nil || user.UID == "" || app == nil {
		return
	}
	table := sql_target_table(sql)
	if table == "" {
		return
	}
	for _, prefix := range sql_default_excluded {
		if strings.HasPrefix(table, prefix) {
			return
		}
	}
	payload := cbor_encode(&SQLCommand{Statement: sql, Args: args})
	replication_emit(user.UID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user.UID,
		Database:  app.id,
		Table:     table,
		Operation: repl_op_exec_app_system,
		Payload:   payload,
	})
}

func replication_emit_user_core_exec(user *User, sql string, args []any) {
	if user == nil || user.UID == "" {
		return
	}
	table := sql_target_table(sql)
	if table == "" {
		return
	}
	for _, prefix := range sql_default_excluded {
		if strings.HasPrefix(table, prefix) {
			return
		}
	}
	payload := cbor_encode(&SQLCommand{Statement: sql, Args: args})
	replication_emit(user.UID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user.UID,
		Database:  repl_db_user_core_sentinel,
		Table:     table,
		Operation: repl_op_exec_user_core,
		Payload:   payload,
	})
}

func replication_apply_app_system_exec(op *ReplicationOp) ApplyResult {
	var cmd SQLCommand
	if err := cbor.Unmarshal(op.Payload, &cmd); err != nil {
		info("Replication exec-app-system: decode failed: %v", err)
		return ApplyInvalid
	}
	if cmd.Statement == "" {
		return ApplyInvalid
	}
	if !user_exists(op.User) {
		return ApplyDeferred
	}
	u := &User{UID: op.User}
	a := app_by_id(op.Database)
	if a == nil {
		return ApplyDeferred
	}
	db := db_app_system(u, a)
	if db == nil {
		return ApplyDeferred
	}
	if _, err := db.internal.Exec(cmd.Statement, cmd.Args...); err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			debug("Replication exec-app-system deferred (FK): user=%q app=%q table=%q sql=%q", op.User, op.Database, op.Table, cmd.Statement)
			return ApplyDeferred
		}
		warn("Replication exec-app-system failed on user=%q app=%q sql=%q: %v", op.User, op.Database, cmd.Statement, err)
		return ApplyApplied
	}
	return ApplyApplied
}

func replication_apply_user_core_exec(op *ReplicationOp) ApplyResult {
	var cmd SQLCommand
	if err := cbor.Unmarshal(op.Payload, &cmd); err != nil {
		info("Replication exec-user-core: decode failed: %v", err)
		return ApplyInvalid
	}
	if cmd.Statement == "" {
		return ApplyInvalid
	}
	if !user_exists(op.User) {
		return ApplyDeferred
	}
	u := &User{UID: op.User}
	db := db_user(u, "user")
	if db == nil {
		return ApplyDeferred
	}
	if _, err := db.internal.Exec(cmd.Statement, cmd.Args...); err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			debug("Replication exec-user-core deferred (FK): user=%q table=%q sql=%q", op.User, op.Table, cmd.Statement)
			return ApplyDeferred
		}
		warn("Replication exec-user-core failed on user=%q sql=%q: %v", op.User, cmd.Statement, err)
		return ApplyApplied
	}
	return ApplyApplied
}

// ============================================================
// mochi.replication.* Starlark API
// ============================================================
//
// In-Mochi consumers (the Pair page in apps/settings/system, the
// per-user "My hosts" page in apps/settings/user, mochictl's progress
// display) query replication state via this API instead of scraping
// /_/health.

// api_replication exposes mochi.replication.{status, links, hosts, joins}
// plus the link, host, join, pair, and bootstrap sub-namespaces. Two-word
// operations are sub-namespaced (host.remove), not glued (host_remove) —
// API names are single-word dotted segments.
var api_replication = sls.FromStringDict(sl.String("mochi.replication"), sl.StringDict{
	"status": sl.NewBuiltin("mochi.replication.status", api_replication_status),
	"links":  sl.NewBuiltin("mochi.replication.links", api_replication_links),
	"hosts":  sl.NewBuiltin("mochi.replication.hosts", api_replication_hosts),
	"joins":  sl.NewBuiltin("mochi.replication.joins", api_replication_joins),
	"link": sls.FromStringDict(sl.String("mochi.replication.link"), sl.StringDict{
		"approve": sl.NewBuiltin("mochi.replication.link.approve", api_replication_link_approve),
		"deny":    sl.NewBuiltin("mochi.replication.link.deny", api_replication_link_deny),
	}),
	"host": sls.FromStringDict(sl.String("mochi.replication.host"), sl.StringDict{
		"remove": sl.NewBuiltin("mochi.replication.host.remove", api_replication_host_remove),
	}),
	"join": sls.FromStringDict(sl.String("mochi.replication.join"), sl.StringDict{
		"approve": sl.NewBuiltin("mochi.replication.join.approve", api_replication_join_approve),
		"deny":    sl.NewBuiltin("mochi.replication.join.deny", api_replication_join_deny),
	}),
	"pair": sls.FromStringDict(sl.String("mochi.replication.pair"), sl.StringDict{
		"remove": sl.NewBuiltin("mochi.replication.pair.remove", api_replication_pair_remove),
	}),
	"bootstrap": sls.FromStringDict(sl.String("mochi.replication.bootstrap"), sl.StringDict{
		"progress": sl.NewBuiltin("mochi.replication.bootstrap.progress", api_replication_bootstrap_progress),
		"serving":  sl.NewBuiltin("mochi.replication.bootstrap.serving", api_replication_bootstrap_serving),
	}),
})

// api_replication_status returns a dict describing this server's
// replication state visible from the local DBs. Same data the
// /_/admin/replication/status endpoint returns to mochictl, exposed
// to Starlark callers so apps can render it directly.
//
// Returned shape:
//
//	{
//	  "peer":              "<this-peer-id>",
//	  "pair":              ["<peer-1>", "<peer-2>"],
//	  "irreparable":       ["<peer>"], // pair members broken past T_forget
//	  "hosts_count":       N,         // total per-user opt-in rows
//	  "links_pending":     N,         // pending per-user link-requests
//	  "joins_pending":     N,         // pending whole-server join-requests
//	  "bootstrap_pending": N,         // (scope, peer) rows still queued/active
//	}
//
// Read-only; no parameters; never returns an error.
func api_replication_status(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	rdb := db_open("db/replication.db")

	var pair []string
	if rows, err := rdb.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				pair = append(pair, p)
			}
		}
	}

	hosts_count := int64(0)
	if row, _ := rdb.row("select count(*) as c from hosts"); row != nil {
		if v, ok := row["c"].(int64); ok {
			hosts_count = v
		}
	}

	links_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from links"); row != nil {
		if v, ok := row["c"].(int64); ok {
			links_pending = v
		}
	}

	joins_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from joins"); row != nil {
		if v, ok := row["c"].(int64); ok {
			joins_pending = v
		}
	}

	// Bootstrap progress: count any (scope, peer) rows still in
	// queued/active. Zero means every active scope has reached
	// 'done' (or there are no peers in bootstrap at all).
	bootstrap_pending := int64(0)
	if row, _ := rdb.row("select count(*) as c from bootstrap where state != 'done'"); row != nil {
		if v, ok := row["c"].(int64); ok {
			bootstrap_pending = v
		}
	}

	// Whole-server pair members marked irreparable (broken past T_forget):
	// the Pair page badges these and offers Remove / re-bootstrap.
	var irreparable []string
	if rows, err := rdb.rows("select distinct peer from irreparable where scope=?", repl_scope_core); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				irreparable = append(irreparable, p)
			}
		}
	}

	// Pair members currently unreachable, with the time they went offline.
	// The Pair page renders an "Offline" badge once the duration crosses the
	// display threshold; the notification waits for offline_threshold (24h).
	offline_values := []sl.Value{}
	if rows, err := rdb.rows("select unreachable.peer as peer, unreachable.since as since from unreachable join pair on pair.peer = unreachable.peer"); err == nil {
		for _, r := range rows {
			p, _ := r["peer"].(string)
			if p == "" {
				continue
			}
			since, _ := r["since"].(int64)
			entry := sl.NewDict(2)
			_ = entry.SetKey(sl.String("peer"), sl.String(p))
			_ = entry.SetKey(sl.String("since"), sl.MakeInt64(since))
			offline_values = append(offline_values, entry)
		}
	}

	pair_values := make([]sl.Value, 0, len(pair))
	for _, p := range pair {
		pair_values = append(pair_values, sl.String(p))
	}
	irreparable_values := make([]sl.Value, 0, len(irreparable))
	for _, p := range irreparable {
		irreparable_values = append(irreparable_values, sl.String(p))
	}

	result := sl.NewDict(8)
	_ = result.SetKey(sl.String("peer"), sl.String(net_id))
	_ = result.SetKey(sl.String("pair"), sl.NewList(pair_values))
	_ = result.SetKey(sl.String("irreparable"), sl.NewList(irreparable_values))
	_ = result.SetKey(sl.String("offline"), sl.NewList(offline_values))
	_ = result.SetKey(sl.String("hosts_count"), sl.MakeInt64(hosts_count))
	_ = result.SetKey(sl.String("links_pending"), sl.MakeInt64(links_pending))
	_ = result.SetKey(sl.String("joins_pending"), sl.MakeInt64(joins_pending))
	_ = result.SetKey(sl.String("bootstrap_pending"), sl.MakeInt64(bootstrap_pending))
	return result, nil
}

// api_replication_links returns pending inbound link-requests for the
// calling user. Source-side display: "alice on B wants to replicate
// from A — Approve / Deny".
//
// Returned shape: list of dicts {peer, label, expires}.
func api_replication_links(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}

	rdb := db_open("db/replication.db")
	rows, err := rdb.rows(
		"select peer, label, expires from links where user=? order by received",
		u.UID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := sl.NewList(nil)
	for _, r := range rows {
		entry := sl.NewDict(3)
		_ = entry.SetKey(sl.String("peer"), sl.String(row_string(r, "peer")))
		_ = entry.SetKey(sl.String("label"), sl.String(row_string(r, "label")))
		_ = entry.SetKey(sl.String("expires"), sl.MakeInt64(row_int(r, "expires")))
		_ = out.Append(entry)
	}
	return out, nil
}

// api_replication_hosts returns the active per-user host set for the
// calling user — the peers that hold a copy of this user's data via
// the per-user opt-in flow.
//
// Returned shape: list of dicts {peer, added, ack}.
func api_replication_hosts(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}

	rdb := db_open("db/replication.db")
	rows, err := rdb.rows(
		"select peer, added, ack from hosts where user=? order by added",
		u.UID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := sl.NewList(nil)
	for _, r := range rows {
		peer := row_string(r, "peer")
		broken, _ := rdb.exists(
			"select 1 from irreparable where peer=? and scope=? and user=?",
			peer, repl_scope_app, u.UID)
		// Offline-since (0 when reachable): the My-hosts page badges a host
		// "Offline" once the duration crosses the display threshold.
		offline := int64(rdb.integer("select since from unreachable where peer=?", peer))
		entry := sl.NewDict(5)
		_ = entry.SetKey(sl.String("peer"), sl.String(peer))
		_ = entry.SetKey(sl.String("added"), sl.MakeInt64(row_int(r, "added")))
		_ = entry.SetKey(sl.String("ack"), sl.MakeInt64(row_int(r, "ack")))
		_ = entry.SetKey(sl.String("irreparable"), sl.Bool(broken))
		_ = entry.SetKey(sl.String("offline"), sl.MakeInt64(offline))
		_ = out.Append(entry)
	}
	return out, nil
}

// api_replication_link_approve approves an inbound link-request from
// `peer` targeting the calling user. Wraps replication_link_approve;
// the underlying handler runs the freshness probe, emits the
// keys-transfer, and updates membership.
//
// Returns "approved" on success, "already-approved" on the multi-tab
// loser. Errors surface as Starlark errors.
func api_replication_link_approve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}
	var peer string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "peer", &peer); err != nil {
		return sl_error(fn, "%v", err)
	}
	if peer == "" {
		return sl_error(fn, "invalid peer")
	}

	result, err := replication_link_approve(u.UID, peer)
	if err != nil {
		return sl_error(fn, "approve: %v", err)
	}
	return sl.String(result), nil
}

// api_replication_link_deny denies an inbound link-request from `peer`
// targeting the calling user. Wraps replication_link_deny; the
// underlying handler emits link-denied(reason=denied) to the
// destination.
//
// Returns "denied" on success, "already-handled" on the multi-tab
// loser. Never returns an error (the underlying call swallows DB
// failures with a warning).
func api_replication_link_deny(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}

	return sl.String(replication_link_deny(u.UID, peer)), nil
}

// api_replication_host_remove removes `peer` from the calling user's
// active per-user host set and emits a membership-change to the
// remaining peers (and the removed one) so the cluster converges on
// the smaller set. Returns "removed" or "not-found".
func api_replication_host_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}

	rdb := db_open("db/replication.db")
	exists, _ := rdb.exists(
		"select 1 from hosts where user=? and peer=?", u.UID, peer)
	if !exists {
		return sl.String("not-found"), nil
	}

	rows, err := rdb.rows("select peer from hosts where user=? and peer!=?", u.UID, peer)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	remaining := make([]string, 0, len(rows))
	for _, r := range rows {
		if p := row_string(r, "peer"); p != "" {
			remaining = append(remaining, p)
		}
	}

	// membership-update wipes & rewrites the local hosts table and
	// broadcasts the new set to every remaining host. The departing
	// peer learns it's out of the set when it next receives any op
	// for this user and sees itself missing from the membership list
	// (or when the periodic reconciler in #66's bootstrap protocol
	// confirms divergence).
	replication_membership_update(u.UID, remaining)
	audit_replication_host_removed(u.UID, peer)
	// Removing the relationship resolves any irreparable badge for it.
	rdb.exec("delete from irreparable where peer=? and scope=? and user=?", peer, repl_scope_app, u.UID)

	return sl.String("removed"), nil
}

// api_replication_joins returns pending inbound whole-server
// join-requests. Server-wide; the action wrapper must require_admin
// before calling. Returned shape: list of dicts {peer, label, expires}.
func api_replication_joins(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer, label, expires from joins order by received")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := sl.NewList(nil)
	for _, r := range rows {
		entry := sl.NewDict(3)
		_ = entry.SetKey(sl.String("peer"), sl.String(row_string(r, "peer")))
		_ = entry.SetKey(sl.String("label"), sl.String(row_string(r, "label")))
		_ = entry.SetKey(sl.String("expires"), sl.MakeInt64(row_int(r, "expires")))
		_ = out.Append(entry)
	}
	return out, nil
}

// api_replication_join_approve approves an inbound pair join-request
// from `peer`. Wraps replication_join_approve: replaces the local pair
// table with the new member set and emits join-approved + a
// pair-membership-change to existing members. Returns "approved" or
// "already-handled".
//
// Server-wide; the action wrapper must require_admin before calling.
func api_replication_join_approve(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil {
		return sl_error(fn, "no user")
	}
	var peer string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "peer", &peer); err != nil {
		return sl_error(fn, "%v", err)
	}
	if peer == "" {
		return sl_error(fn, "invalid peer")
	}

	result, err := replication_join_approve(peer)
	if err != nil {
		return sl_error(fn, "approve: %v", err)
	}
	return sl.String(result), nil
}

// api_replication_join_deny denies an inbound pair join-request from
// `peer`. Wraps replication_join_deny: emits join-denied(reason=denied)
// to the replica on the winner, no-op on the multi-tab loser. Returns
// "denied" or "already-handled".
//
// Server-wide; the action wrapper must require_admin before calling.
func api_replication_join_deny(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}
	return sl.String(replication_join_deny(peer)), nil
}

// api_replication_pair_remove drops `peer` from the local pair set and
// announces the new member set to every remaining pair member. Wraps
// replication_pair_remove (shared with the admin HTTP handler).
// Returns "removed" or "not-found".
//
// Server-wide; the action wrapper must require_admin before calling.
// The removed peer is intentionally not announced to — it learns of
// the change via gossip from a remaining member, matching the
// admin HTTP endpoint behavior.
func api_replication_pair_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "peer required")
	}
	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "invalid peer")
	}
	_, _, removed := replication_pair_remove(peer)
	if !removed {
		return sl.String("not-found"), nil
	}
	return sl.String("removed"), nil
}

// api_replication_bootstrap_progress returns the per-(peer, scope)
// bulk-bootstrap progress as a list of dicts. Each entry includes
// the peer, scope, state ('queued' | 'active' | 'done'), and a
// position cursor whose meaning depends on the state: for 'active'
// it's the count of items remaining; for 'done' it's empty.
//
// Optional argument: a single peer-id string filters to that peer's
// rows; omitted returns every peer's rows. Whole-server scope; no
// per-user filtering. Action wrappers gate to admin.
//
// Returned shape: list of dicts {peer, scope, state, position}.
func api_replication_bootstrap_progress(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var peerFilter string
	if len(args) > 0 && args[0] != sl.None {
		if p, ok := sl.AsString(args[0]); ok {
			peerFilter = p
		}
	}

	rdb := db_open("db/replication.db")
	var rows []map[string]any
	var err error
	if peerFilter != "" {
		rows, err = rdb.rows("select peer, scope, state, position from bootstrap where peer=? order by peer, scope", peerFilter)
	} else {
		rows, err = rdb.rows("select peer, scope, state, position from bootstrap order by peer, scope")
	}
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	out := sl.NewList(nil)
	for _, r := range rows {
		entry := sl.NewDict(4)
		_ = entry.SetKey(sl.String("peer"), sl.String(row_string(r, "peer")))
		_ = entry.SetKey(sl.String("scope"), sl.String(row_string(r, "scope")))
		_ = entry.SetKey(sl.String("state"), sl.String(row_string(r, "state")))
		_ = entry.SetKey(sl.String("position"), sl.String(row_string(r, "position")))
		_ = out.Append(entry)
	}
	return out, nil
}

// api_replication_bootstrap_serving returns the per-(peer, scope) rows
// from `bootstrap_served` — scopes this server is currently serving to
// a joined replica, with each row cleared when the replica acks the
// scope as done. Counterpart to bootstrap_progress: progress is the
// inbound view (this server consuming from a source), serving is the
// outbound view (this server feeding a replica). Both are needed for
// the operator UI to show symmetric Syncing/Synced status on both sides.
//
// Returned shape: list of dicts {peer, scope, started}.
func api_replication_bootstrap_serving(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer, scope, started from bootstrap_served order by peer, scope")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	out := sl.NewList(nil)
	for _, r := range rows {
		entry := sl.NewDict(3)
		_ = entry.SetKey(sl.String("peer"), sl.String(row_string(r, "peer")))
		_ = entry.SetKey(sl.String("scope"), sl.String(row_string(r, "scope")))
		_ = entry.SetKey(sl.String("started"), sl.MakeInt64(row_int(r, "started")))
		_ = out.Append(entry)
	}
	return out, nil
}

// row_string / row_int unpack scalar SQL row values defensively. The
// nil checks let api_replication_* return an empty list cleanly when
// a row was scanned with an unexpected column type instead of
// panicking the action.
func row_string(r map[string]any, key string) string {
	if v, ok := r[key].(string); ok {
		return v
	}
	return ""
}

// row_int extracts a numeric field from a map[string]any returned by
// a CBOR-decoded payload or a sqlite row scan. The cbor library
// decodes non-negative integers into uint64 (not int64) when the
// target is interface{}, so callers like the bootstrap chunk-fetch
// handler would see length=0 for every non-empty file because the
// uint64(903) case wasn't matched and fell through to the zero
// return. That broke file-chunk delivery — every file landed as a
// zero-byte file on the receiver, including 21,612 entity-id app
// files whose empty app.json then made the published-apps loader
// silently skip the entire installed-app set.
func row_int(r map[string]any, key string) int64 {
	switch v := r[key].(type) {
	case int64:
		return v
	case uint64:
		return int64(v)
	case int:
		return int64(v)
	case uint:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	}
	return 0
}

// ============================================================
// Per-app SQL command replication (envelope format)
// ============================================================

// SQLCommand is the wire payload for an opt-out app-DB SQL replication
// op. The receiver re-executes Statement against the same per-(user,
// app) DB using Args as bound parameters. Convergence is by re-execution:
// INSERTs dedup on PK conflict, UPDATE/DELETE follow last-write-wins by
// arrival order. Apps that need stronger semantics use the log-plus-
// derived-view pattern (see CLAUDE.md replication-safety bullets).
//
// Args are encoded as `any` so the same Go-side parameter types that
// went into the local exec round-trip through cbor and reach the
// replica's exec untouched (int64, string, []byte, nil).
type SQLCommand struct {
	Statement string `cbor:"sql"`
	Args      []any  `cbor:"args,omitempty"`
}

// sql_default_excluded names tables that never replicate, no matter
// what the app declares. SQLite's internal namespace plus the
// session-local commit log used by the future mochi.db.commit.hook
// drainer.
var sql_default_excluded = []string{
	"sqlite_",
	"_commit_log",
}

// sql_target_table extracts the target table from a mutating SQL
// statement. Returns "" for read-only statements (SELECT, PRAGMA …)
// and for schema statements (CREATE/DROP/ALTER) — neither replicates.
// The parser is intentionally simple: skip leading comments + whitespace,
// match the verb, then take the next identifier as the table name. CTE
// (WITH …) prefixes are not recognised and stay local; apps that need
// CTE writes to replicate should reshape to a plain INSERT/UPDATE/DELETE.
func sql_target_table(sql string) string {
	s := sql_strip_lead(sql)
	verb, rest := sql_take_word(s)
	switch strings.ToUpper(verb) {
	case "INSERT", "REPLACE":
		// INSERT [OR IGNORE|REPLACE|...] INTO <table>
		rest = sql_strip_lead(rest)
		if w, after := sql_take_word(rest); strings.ToUpper(w) == "OR" {
			_, after = sql_take_word(sql_strip_lead(after))
			rest = sql_strip_lead(after)
			w, after = sql_take_word(rest)
			if strings.ToUpper(w) != "INTO" {
				return ""
			}
			rest = sql_strip_lead(after)
		} else if strings.ToUpper(w) == "INTO" {
			rest = sql_strip_lead(after)
		} else {
			return ""
		}
		name, _ := sql_take_ident(rest)
		return name
	case "UPDATE":
		// UPDATE [OR ...] <table>
		rest = sql_strip_lead(rest)
		if w, after := sql_take_word(rest); strings.ToUpper(w) == "OR" {
			_, after = sql_take_word(sql_strip_lead(after))
			rest = sql_strip_lead(after)
		}
		name, _ := sql_take_ident(rest)
		return name
	case "DELETE":
		// DELETE FROM <table>
		rest = sql_strip_lead(rest)
		w, after := sql_take_word(rest)
		if strings.ToUpper(w) != "FROM" {
			return ""
		}
		name, _ := sql_take_ident(sql_strip_lead(after))
		return name
	}
	return ""
}

// sql_target_uid extracts the row identifier value bound to a
// mutating SQL statement, used as the row uid passed to
// commit_hook_fire on replication apply. App-side commit hooks then
// know which specific row a replicated write created or changed.
//
// Recognised shapes (cover the bulk of app SQL written under the
// Mochi single-word PK convention where the row id column is named
// "id" and bound as a string):
//
//	INSERT|REPLACE INTO <table> (id, ...) VALUES (?, ...)
//	INSERT|REPLACE INTO <table> VALUES (?, ...)
//	UPDATE <table> SET ... WHERE id = ?
//	DELETE FROM <table> WHERE id = ?
//
// Returns "" for any other shape. Apps whose row PK isn't "id", or
// whose WHERE clause carries more than a single id-equality, fall
// back to the empty-uid behaviour (commit hooks still fire on
// replication apply, just without a specific row identifier).
func sql_target_uid(sql string, args []any) string {
	s := sql_strip_lead(sql)
	verb, rest := sql_take_word(s)
	switch strings.ToUpper(verb) {
	case "INSERT", "REPLACE":
		rest = sql_strip_lead(rest)
		// Skip OR <conflict-action>.
		if word, after := sql_take_word(rest); strings.ToUpper(word) == "OR" {
			_, after = sql_take_word(sql_strip_lead(after))
			rest = sql_strip_lead(after)
		}
		word, after := sql_take_word(rest)
		if strings.ToUpper(word) != "INTO" {
			return ""
		}
		_, after = sql_take_ident(sql_strip_lead(after))
		after = sql_strip_lead(after)
		// Either an explicit "(id, ...) values (?, ...)" or the
		// implicit positional "values (?, ...)" - both have args[0]
		// bound to the row id.
		if strings.HasPrefix(after, "(") {
			column, _ := sql_take_ident(sql_strip_lead(after[1:]))
			if !strings.EqualFold(column, "id") {
				return ""
			}
		} else {
			keyword, _ := sql_take_word(after)
			if strings.ToUpper(keyword) != "VALUES" {
				return ""
			}
		}
		if len(args) == 0 {
			return ""
		}
		if uid, ok := args[0].(string); ok {
			return uid
		}
		return ""

	case "UPDATE", "DELETE":
		// Recognised only when the WHERE clause is exactly "id = ?".
		// The bound value is then the last entry in args.
		lower := strings.ToLower(sql)
		where := strings.LastIndex(lower, " where ")
		if where < 0 {
			return ""
		}
		clause := strings.TrimSpace(lower[where+len(" where "):])
		clause = strings.TrimRight(clause, " \t;")
		column, rest := sql_take_ident(clause)
		if !strings.EqualFold(column, "id") {
			return ""
		}
		rest = strings.TrimLeft(rest, " \t")
		if !strings.HasPrefix(rest, "=") {
			return ""
		}
		rest = strings.TrimLeft(rest[1:], " \t")
		if rest != "?" {
			return ""
		}
		if len(args) == 0 {
			return ""
		}
		if uid, ok := args[len(args)-1].(string); ok {
			return uid
		}
		return ""
	}
	return ""
}

// sql_strip_lead skips over leading whitespace and line / block comments.
func sql_strip_lead(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s, "*/"); i >= 0 {
				s = s[i+2:]
				continue
			}
			return ""
		}
		return s
	}
}

// sql_take_word reads the next contiguous run of letters as a single
// keyword. Stops at the first non-letter, returning the word and the
// remainder.
func sql_take_word(s string) (string, string) {
	i := 0
	for i < len(s) {
		c := s[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			break
		}
		i++
	}
	return s[:i], s[i:]
}

// sql_take_ident reads a SQL identifier: bare word, `"quoted"`, or
// `[bracketed]`. Returns the unquoted name plus the tail.
func sql_take_ident(s string) (string, string) {
	if s == "" {
		return "", s
	}
	if s[0] == '"' {
		if i := strings.IndexByte(s[1:], '"'); i >= 0 {
			return s[1 : 1+i], s[2+i:]
		}
		return "", ""
	}
	if s[0] == '[' {
		if i := strings.IndexByte(s[1:], ']'); i >= 0 {
			return s[1 : 1+i], s[2+i:]
		}
		return "", ""
	}
	if s[0] == '`' {
		if i := strings.IndexByte(s[1:], '`'); i >= 0 {
			return s[1 : 1+i], s[2+i:]
		}
		return "", ""
	}
	i := 0
	for i < len(s) {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			i++
			continue
		}
		break
	}
	return s[:i], s[i:]
}

// sql_table_excluded returns true when the given table is in either
// the core default-excluded set or the app's app.json exclude list.
func sql_table_excluded(av *AppVersion, table string) bool {
	if table == "" {
		return true
	}
	for _, prefix := range sql_default_excluded {
		if strings.HasPrefix(table, prefix) {
			return true
		}
	}
	if av == nil {
		return false
	}
	for _, t := range av.Database.Replicate.Exclude.Tables {
		if t == table {
			return true
		}
	}
	return false
}

// replication_emit_sql_command fans out a successful local app-DB write
// to the user's host set. Called from api_db_query (mochi.db.execute)
// after the local exec returns nil error, and from the deferred-emit
// flush at transaction commit. Skipped when the user has no UID
// (anonymous or pre-v51 row) or the table is excluded.
func replication_emit_sql_command(user *User, app *App, av *AppVersion, sql string, args []any) {
	if user == nil || user.UID == "" || app == nil || av == nil {
		return
	}
	table := sql_target_table(sql)
	if sql_table_excluded(av, table) {
		return
	}
	payload := cbor_encode(&SQLCommand{Statement: sql, Args: args})
	replication_emit(user.UID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user.UID,
		Database:  app.id,
		Table:     table,
		UID:       sql_target_uid(sql, args),
		Operation: repl_op_exec,
		Payload:   payload,
		Schema:    av.Database.Schema,
	})
}

// replication_apply_sql_command re-executes a replicated SQL statement
// on the local replica's per-(user, app) DB. Defers when the user or
// app isn't yet present, or when the receiver's schema is older than
// the sender's (the op will retry on database_upgrade landing).
func replication_apply_sql_command(op *ReplicationOp) ApplyResult {
	var cmd SQLCommand
	if err := cbor.Unmarshal(op.Payload, &cmd); err != nil {
		info("Replication exec: decode failed: %v", err)
		return ApplyInvalid
	}
	if cmd.Statement == "" {
		return ApplyInvalid
	}

	if !user_exists(op.User) {
		return ApplyDeferred
	}
	u := &User{UID: op.User}
	a := app_by_id(op.Database)
	if a == nil {
		return ApplyDeferred
	}
	av := a.active(u)
	if av == nil {
		return ApplyDeferred
	}
	if op.Schema > av.Database.Schema {
		// Receiver schema older than sender's. Defer; the
		// database_upgrade triggered by the next app activity
		// drives a pending-drain.
		return ApplyDeferred
	}

	db := db_app(u, a)
	if db == nil {
		return ApplyDeferred
	}

	if _, err := db.starlark.Exec(cmd.Statement, cmd.Args...); err != nil {
		// FK violations under out-of-order arrival (parallel-queue
		// send sends N ops to one peer concurrently; receiver applies
		// in arrival order). The parent row may arrive a fraction of
		// a second after the child — defer so the next pending-drain
		// tick retries once the parent has landed. Other errors
		// (schema drift, real bugs) log + advance dedup as before.
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			debug("Replication exec deferred (FK): user=%q app=%q table=%q sql=%q", op.User, op.Database, op.Table, cmd.Statement)
			return ApplyDeferred
		}
		warn("Replication exec failed on user=%q app=%q sql=%q: %v", op.User, op.Database, cmd.Statement, err)
		return ApplyApplied
	}
	debug("Replication exec apply: user=%q app=%q table=%q", op.User, op.Database, op.Table)
	return ApplyApplied
}

// ============================================================
// System-scope replication for core DBs
// ============================================================
//
// Pair-scope replication for operator-tuned tables that live in core
// DBs (not per-user): settings.db.settings, apps.db routing tables,
// domains.db domains / routes. Each pair member emits "this row
// changed" on local write; receivers replay via REPLACE INTO.

// SystemSet is the wire payload for a single field-level write to a
// core system DB. Database/Table identify the destination
// (e.g. "settings"/"settings"); Row is the primary-key value (e.g.
// the setting name); Field is the column being written; Value is the
// new value. Empty Value means "delete the row" (LWW-tombstone of
// the binding).
type SystemSet struct {
	Database string `cbor:"db"`
	Table    string `cbor:"table"`
	Row      string `cbor:"row"`
	Field    string `cbor:"field"`
	Value    string `cbor:"value"`
}

// replication_system_set_event is the receive handler. Decodes the
// payload and delegates to the apply function below.
func replication_system_set_event(e *Event) {
	var s SystemSet
	if !e.segment(&s) {
		info("Replication system-set dropping: cannot decode payload")
		return
	}
	replication_system_set_apply(e.peer, &s)
}

// replication_system_set_apply applies an incoming system-set write
// to the destination DB. Dispatches by (Database, Table); unknown
// destinations are silently dropped after a warn. Order-of-arrival
// determines the winner under concurrent writes — see the file
// header for the trade-off.
func replication_system_set_apply(originPeer string, s *SystemSet) {
	if s.Database == "" || s.Table == "" || s.Row == "" || s.Field == "" {
		info("Replication system-set dropping: missing key fields")
		return
	}
	switch s.Database + "." + s.Table {
	case "settings.settings":
		replication_system_set_apply_settings(originPeer, s)
	case "apps.classes", "apps.services", "apps.paths":
		replication_system_set_apply_apps_two_col(originPeer, s)
	case "apps.apps":
		replication_system_set_apply_apps_installs(originPeer, s)
	default:
		warn("Replication system-set: unsupported destination %q.%q (from peer %q)",
			s.Database, s.Table, originPeer)
	}
}

// replication_system_set_apply_settings handles settings.db.settings.
// Only the `value` field is replicated. Empty value deletes the row.
func replication_system_set_apply_settings(originPeer string, s *SystemSet) {
	if s.Field != "value" {
		info("Replication system-set settings.settings: unsupported field %q (from peer %q)", s.Field, originPeer)
		return
	}
	db := db_open("db/settings.db")
	if s.Value == "" {
		db.exec("delete from settings where name=?", s.Row)
	} else {
		db.exec("replace into settings (name, value) values (?, ?)", s.Row, s.Value)
	}
	debug("Replication system-set settings.settings applied: name=%q value=%q (from %q)",
		s.Row, s.Value, originPeer)
}

// replication_system_set_apply_apps_two_col handles classes / services
// / paths in apps.db. All three are (key, app) tables — the keying
// column varies per table. Empty value deletes the row.
func replication_system_set_apply_apps_two_col(originPeer string, s *SystemSet) {
	if s.Field != "app" {
		info("Replication system-set apps.%s: unsupported field %q (from peer %q)", s.Table, s.Field, originPeer)
		return
	}
	var keyCol string
	switch s.Table {
	case "classes":
		keyCol = "class"
	case "services":
		keyCol = "service"
	case "paths":
		keyCol = "path"
	default:
		return
	}
	db := db_apps()
	if s.Value == "" {
		db.exec(fmt.Sprintf("delete from %s where %s=?", s.Table, keyCol), s.Row)
	} else {
		db.exec(
			fmt.Sprintf("replace into %s (%s, app) values (?, ?)", s.Table, keyCol),
			s.Row, s.Value)
	}
	debug("Replication system-set apps.%s applied: %s=%q value=%q (from %q)",
		s.Table, keyCol, s.Row, s.Value, originPeer)
}

// replication_system_set_apply_apps_installs handles apps.db.apps —
// the install registry. Value carries the install timestamp. A bump
// (re-broadcast of a newer timestamp) means the source just installed
// or upgraded the app; the receiver needs the matching code on disk
// to keep the pair in sync, so app_check_install runs async to pull
// the latest version from the publisher.
func replication_system_set_apply_apps_installs(originPeer string, s *SystemSet) {
	if s.Field != "installed" {
		info("Replication system-set apps.apps: unsupported field %q (from peer %q)", s.Field, originPeer)
		return
	}
	db := db_apps()
	if s.Value == "" {
		db.exec("delete from apps where app=?", s.Row)
	} else {
		var installed int64
		_, _ = fmt.Sscanf(s.Value, "%d", &installed)
		if installed == 0 {
			installed = now()
		}
		db.exec("replace into apps (app, installed) values (?, ?)", s.Row, installed)
	}
	debug("Replication system-set apps.apps applied: app=%q value=%q (from %q)",
		s.Row, s.Value, originPeer)
	if s.Value != "" && valid(s.Row, "entity") {
		go app_check_install(s.Row)
	}
}

// replication_emit_system_set is the package-level emit function
// variable so tests can stub it. Production points it at
// replication_emit_system_set_real.
var replication_emit_system_set = replication_emit_system_set_real

// replication_emit_system_set_real emits a system-set write to every
// pair member. No-op when this server has no pair members.
func replication_emit_system_set_real(database, table, row, field, value string) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer from pair")
	if err != nil || len(rows) == 0 {
		return
	}
	payload := &SystemSet{
		Database: database, Table: table, Row: row, Field: field, Value: value,
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		if peer == "" || peer == net_id {
			continue
		}
		m := message("", "", "replication", "system/set")
		m.add(payload)
		m.send_peer(peer)
	}
}

// SystemRow is the row-level companion to SystemSet. Used for tables
// where field-level is awkward — multi-column rows, or rows with
// composite primary keys. Key carries the row's primary-key columns
// (1+ entries); Cols carries the remaining data columns being written.
// Delete=true signals "remove the row".
//
// Wire: replication/system-row. Same order-of-arrival semantics as
// SystemSet — no LWW conflict resolution.
type SystemRow struct {
	Database string            `cbor:"db"`
	Table    string            `cbor:"table"`
	Key      map[string]string `cbor:"key"`
	Cols     map[string]string `cbor:"cols"`
	Delete   bool              `cbor:"delete,omitempty"`
}

// replication_system_row_event is the receive handler for row-level
// ops.
func replication_system_row_event(e *Event) {
	var s SystemRow
	if !e.segment(&s) {
		info("Replication system-row dropping: cannot decode payload")
		return
	}
	replication_system_row_apply(e.peer, &s)
}

// replication_system_row_apply dispatches an inbound row-level op to
// its table-specific handler.
func replication_system_row_apply(originPeer string, s *SystemRow) {
	if s.Database == "" || s.Table == "" || len(s.Key) == 0 {
		info("Replication system-row dropping: missing key fields")
		return
	}
	switch s.Database + "." + s.Table {
	case "domains.domains":
		replication_system_row_apply_domains(originPeer, s)
	case "domains.routes":
		replication_system_row_apply_routes(originPeer, s)
	case "apps.versions":
		replication_system_row_apply_apps_versions(originPeer, s)
	case "apps.tracks":
		replication_system_row_apply_apps_tracks(originPeer, s)
	case "domains.delegations":
		replication_system_row_apply_delegations(originPeer, s)
	case "users.users":
		replication_system_row_apply_users_users(originPeer, s)
	case "settings.documents":
		replication_system_row_apply_settings_documents(originPeer, s)
	default:
		warn("Replication system-row: unsupported destination %q.%q (from peer %q)",
			s.Database, s.Table, originPeer)
	}
}

// replication_system_users_users_mutable is the column whitelist for
// pair-scope replication into users.db.users. Columns listed here flow
// between operator-paired hosts (sharing one operator's full identity
// state); other users.users columns either replicate via the per-user
// path (preferences, methods, status — see replication_users_users_mutable)
// or stay strictly local per host.
var replication_system_users_users_mutable = map[string]bool{
	"username": true,
	"role":     true,
}

// replication_system_row_apply_users_users handles users.db.users for
// the pair-scope columns (username, role). Per-user link partners must
// not receive these — username is a per-host namespace affordance and
// role is the local operator's authority decision — so the emit-side
// uses the pair-only system-row pipeline and this handler is the
// receiver counterpart. The per-user replication_users_users_apply path
// silently ignores these columns via its own narrower whitelist.
func replication_system_row_apply_users_users(originPeer string, s *SystemRow) {
	uid := s.Key["uid"]
	if uid == "" {
		info("Replication system-row users.users dropping: missing uid key (from peer %q)", originPeer)
		return
	}
	if s.Delete {
		// User deletion is a server-pair operation, not a row op —
		// no-op so an errant emitter can't lose accounts.
		return
	}
	sets := []string{}
	vals := []any{}
	for col, v := range s.Cols {
		if !replication_system_users_users_mutable[col] {
			continue
		}
		sets = append(sets, col+"=?")
		vals = append(vals, v)
	}
	if len(sets) == 0 {
		return
	}
	vals = append(vals, uid)
	db := db_open("db/users.db")
	// Use db.internal.Exec directly so a UNIQUE-constraint refusal
	// (the documented collision-at-apply case for username changes)
	// surfaces as a log line instead of panicking the receiver. The
	// local row stays at its pre-replication value; no data is
	// destroyed.
	if _, err := db.internal.Exec("update users set "+strings.Join(sets, ", ")+" where uid=?", vals...); err != nil {
		warn("Replication system-row users.users refused: uid=%q cols=%v err=%v (from %q)", uid, s.Cols, err, originPeer)
		return
	}
	debug("Replication system-row users.users applied: uid=%q cols=%v (from %q)", uid, s.Cols, originPeer)
}

// replication_system_row_apply_settings_documents handles
// settings.db.documents - operator-edited markdown overrides for the
// server's rules / terms / privacy pages. Composite key
// (name, language) with body + updated as the row data. LWW per
// (name, language): a later replace from any pair member wins.
func replication_system_row_apply_settings_documents(originPeer string, s *SystemRow) {
	name := s.Key["name"]
	language := s.Key["language"]
	if name == "" || language == "" {
		info("Replication system-row settings.documents dropping: missing key (from peer %q)", originPeer)
		return
	}
	db := db_open("db/settings.db")
	if s.Delete {
		db.exec("delete from documents where name=? and language=?", name, language)
		debug("Replication system-row settings.documents deleted: %q/%q (from %q)", name, language, originPeer)
		return
	}
	body := s.Cols["body"]
	var updated int64
	_, _ = fmt.Sscanf(s.Cols["updated"], "%d", &updated)
	db.exec("replace into documents (name, language, body, updated) values (?, ?, ?, ?)",
		name, language, body, updated)
	debug("Replication system-row settings.documents applied: %q/%q updated=%d (from %q)", name, language, updated, originPeer)
}

// replication_system_row_apply_domains handles domains.db.domains.
// Single-column key (domain) with multi-column row data.
func replication_system_row_apply_domains(originPeer string, s *SystemRow) {
	name := s.Key["domain"]
	if name == "" {
		return
	}
	db := db_open("db/domains.db")
	if s.Delete {
		db.exec("delete from domains where domain=?", name)
		debug("Replication system-row domains.domains deleted: %q (from %q)", name, originPeer)
		return
	}
	var verified, tls, created, updated int64
	_, _ = fmt.Sscanf(s.Cols["verified"], "%d", &verified)
	_, _ = fmt.Sscanf(s.Cols["tls"], "%d", &tls)
	_, _ = fmt.Sscanf(s.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(s.Cols["updated"], "%d", &updated)
	token := s.Cols["token"]
	db.exec(
		"replace into domains (domain, verified, token, tls, created, updated) values (?, ?, ?, ?, ?, ?)",
		name, verified, token, tls, created, updated)
	debug("Replication system-row domains.domains applied: %q (from %q)", name, originPeer)
}

// replication_system_row_apply_routes handles domains.db.routes —
// composite key (domain, path) carried via Key map.
func replication_system_row_apply_routes(originPeer string, s *SystemRow) {
	domain := s.Key["domain"]
	path := s.Key["path"]
	if domain == "" {
		info("Replication system-row domains.routes dropping: empty domain key (from peer %q)", originPeer)
		return
	}
	db := db_open("db/domains.db")
	if s.Delete {
		db.exec("delete from routes where domain=? and path=?", domain, path)
		debug("Replication system-row domains.routes deleted: %q+%q (from %q)", domain, path, originPeer)
		return
	}
	method := s.Cols["method"]
	target := s.Cols["target"]
	context := s.Cols["context"]
	owner := s.Cols["owner"]
	var priority, enabled, created, updated int64
	_, _ = fmt.Sscanf(s.Cols["priority"], "%d", &priority)
	_, _ = fmt.Sscanf(s.Cols["enabled"], "%d", &enabled)
	_, _ = fmt.Sscanf(s.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(s.Cols["updated"], "%d", &updated)
	db.exec(
		"replace into routes (domain, path, method, target, context, owner, priority, enabled, created, updated) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		domain, path, method, target, context, owner, priority, enabled, created, updated)
	debug("Replication system-row domains.routes applied: %q+%q (from %q)", domain, path, originPeer)
}

// replication_emit_system_row is the package-level emit function
// variable for the row-level ops; tests can stub it.
var replication_emit_system_row = replication_emit_system_row_real

// replication_emit_system_row_real emits a row-level op to every pair
// member.
func replication_emit_system_row_real(database, table string, key, cols map[string]string, del bool) {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer from pair")
	if err != nil || len(rows) == 0 {
		return
	}
	payload := &SystemRow{
		Database: database, Table: table,
		Key: key, Cols: cols, Delete: del,
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		if peer == "" || peer == net_id {
			continue
		}
		m := message("", "", "replication", "system/row")
		m.add(payload)
		m.send_peer(peer)
	}
}

// replication_system_row_apply_apps_versions handles apps.db.versions —
// (app primary key, version, track). Single-column key, two data
// columns. Empty row → delete.
func replication_system_row_apply_apps_versions(originPeer string, s *SystemRow) {
	app := s.Key["app"]
	if app == "" {
		return
	}
	db := db_apps()
	if s.Delete {
		db.exec("delete from versions where app=?", app)
		return
	}
	db.exec("replace into versions (app, version, track) values (?, ?, ?)",
		app, s.Cols["version"], s.Cols["track"])
	debug("Replication system-row apps.versions applied: %q (from %q)", app, originPeer)
	// A version row update for an entity-id app means the source
	// installed or upgraded a published app. The replica needs the
	// matching code on disk to actually serve requests against it;
	// fire app_check_install so the publisher download happens now
	// instead of waiting for the next 24-hour apps_manager tick.
	// Skip non-entity ids (dev / internal apps live on the local
	// filesystem and don't need downloading).
	if valid(app, "entity") {
		go app_check_install(app)
	}
}

// replication_system_row_apply_apps_tracks handles apps.db.tracks —
// composite key (app, track), single data column (version). Operator
// pinning a track to a new version means the source has (or wants)
// that version locally; the receiver needs it on disk to follow the
// pin, so app_check_install runs async — same pattern as the
// versions apply handler above.
func replication_system_row_apply_apps_tracks(originPeer string, s *SystemRow) {
	app := s.Key["app"]
	track := s.Key["track"]
	if app == "" || track == "" {
		return
	}
	db := db_apps()
	if s.Delete {
		db.exec("delete from tracks where app=? and track=?", app, track)
		return
	}
	db.exec("replace into tracks (app, track, version) values (?, ?, ?)",
		app, track, s.Cols["version"])
	debug("Replication system-row apps.tracks applied: %q+%q (from %q)", app, track, originPeer)
	if valid(app, "entity") {
		go app_check_install(app)
	}
}

// replication_system_row_apply_delegations handles domains.db.delegations —
// composite key (domain, path, owner), two data columns (created, updated).
// The id integer PK is local-only; receivers let SQLite assign on insert.
func replication_system_row_apply_delegations(originPeer string, s *SystemRow) {
	domain := s.Key["domain"]
	path := s.Key["path"]
	owner := s.Key["owner"]
	if domain == "" || owner == "" {
		return
	}
	db := db_open("db/domains.db")
	if s.Delete {
		db.exec("delete from delegations where domain=? and path=? and owner=?", domain, path, owner)
		return
	}
	// Insert if not present; the unique(domain, path, owner) index
	// keeps replays idempotent. An incoming op for an already-present
	// row updates the timestamps via ON CONFLICT DO UPDATE pattern —
	// but SQLite doesn't allow direct ON CONFLICT on a non-pk unique
	// index here. Use a DELETE-then-INSERT instead.
	var created, updated int64
	_, _ = fmt.Sscanf(s.Cols["created"], "%d", &created)
	_, _ = fmt.Sscanf(s.Cols["updated"], "%d", &updated)
	db.exec("delete from delegations where domain=? and path=? and owner=?", domain, path, owner)
	db.exec("insert into delegations (domain, path, owner, created, updated) values (?, ?, ?, ?, ?)",
		domain, path, owner, created, updated)
	debug("Replication system-row domains.delegations applied: %q+%q+%q (from %q)",
		domain, path, owner, originPeer)
}

// ============================================================
// Per-DB row replication helpers
// (users.db auth, sessions.db auth-flow, schedule.db)
// ============================================================

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
	"methods":  true,
	"disabled": true,
	"status":   true,
	"purge":    true,
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
	case "reauthentication":
		return replication_sessions_reauthentication_apply(sdb, r)
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

func replication_sessions_reauthentication_apply(sdb *DB, r *SessionsRow) ApplyResult {
	id := r.Key["id"]
	if id == "" {
		return ApplyInvalid
	}
	if r.Delete {
		sdb.exec("delete from reauthentication where id=?", id)
		return ApplyApplied
	}
	var expires int64
	_, _ = fmt.Sscanf(r.Cols["expires"], "%d", &expires)
	sdb.exec("insert or replace into reauthentication (id, user, methods, expires) values (?, ?, ?, ?)",
		id, r.Cols["user"], r.Cols["methods"], expires)
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
