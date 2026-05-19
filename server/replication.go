// Mochi server: Replication transport
// Copyright Alistair Cunningham 2026

package main

import (
	"sync"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	"github.com/gin-gonic/gin"
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
	Operation   string `cbor:"operation"`
	Payload     []byte `cbor:"payload"`
	Sequence    int64  `cbor:"sequence"`
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

	// Fence check before dispatch: if this op carries a leader-stamp
	// (op.LeaderScope/Key/Fence) and our witness for that lease has
	// already seen a higher fence, the emitter has been superseded and
	// we drop the op silently. Stamp-less ops pass through.
	if !replication_fence_observe(op.LeaderScope, op.LeaderKey, e.peer, op.Fence) {
		info("Replication op dropped: stale leader fence %d for scope=%q key=%q from peer=%q",
			op.Fence, op.LeaderScope, op.LeaderKey, e.peer)
		// Record as seen so the sender's queue drops it; further
		// retries with the same fence will just hit the same check.
		db.exec(
			"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, ?, ?, ?, ?)",
			e.peer, op.Scope, op.User, op.Sequence, now())
		return
	}

	switch replication_apply_op(&op) {
	case ApplyApplied:
		db.exec(
			"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, ?, ?, ?, ?)",
			e.peer, op.Scope, op.User, op.Sequence, now())
		debug("Replication op applied: peer=%q scope=%q user=%q seq=%d db=%q table=%q op=%q",
			e.peer, op.Scope, op.User, op.Sequence, op.Database, op.Table, op.Operation)
		commit_hook_fire(op.User, op.Database, op.Table, op.Operation, "")
	case ApplyDeferred:
		payload := cbor_encode(&op)
		db.exec(
			"insert or ignore into pending (peer, scope, user, sequence, schema, payload, received) values (?, ?, ?, ?, ?, ?, ?)",
			e.peer, op.Scope, op.User, op.Sequence, op.Schema, payload, now())
		debug("Replication op deferred: peer=%q scope=%q user=%q seq=%d db=%q table=%q op=%q",
			e.peer, op.Scope, op.User, op.Sequence, op.Database, op.Table, op.Operation)
	case ApplyInvalid:
		info("Replication op dropping: unrecognised shape peer=%q scope=%q db=%q table=%q op=%q",
			e.peer, op.Scope, op.Database, op.Table, op.Operation)
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
	out := gin.H{"peer_id": p2p_id}

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
	out["leases_held"] = db.integer("select count(*) from leadership where peer=? and expires > ?", p2p_id, now())

	c.JSON(200, out)
}

// replication_manager drives the periodic pending-buffer drain.
// Deferred ops not unblocked by a keys-transfer (e.g. an app schema
// upgrade that catches the local version up to a sender's, once the
// schema-coordination path lands) get retried on every tick.
func replication_manager() {
	for range time.Tick(30 * time.Second) {
		replication_pending_drain()
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

// replication_pending_drain walks `replication.db.pending` in arrival
// order and re-evaluates each buffered op against the current local
// state. Ops that now apply move to `seen`; ops that are still deferred
// stay in pending until the next drain.
//
// Called automatically after a keys-transfer (when a new user lands,
// pending session inserts for that user become applyable) and on a
// periodic background tick.
func replication_pending_drain() {
	db := db_open("db/replication.db")
	rows, err := db.rows("select peer, scope, user, sequence, payload from pending order by received limit 100")
	if err != nil {
		return
	}
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		scope, _ := r["scope"].(string)
		userField, _ := r["user"].(string)
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
			db.exec("delete from pending where peer=? and scope=? and user=? and sequence=?", peer, scope, userField, sequence)
			continue
		}

		switch replication_apply_op(&op) {
		case ApplyApplied:
			db.exec(
				"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, ?, ?, ?, ?)",
				peer, scope, userField, sequence, now())
			db.exec("delete from pending where peer=? and scope=? and user=? and sequence=?", peer, scope, userField, sequence)
			debug("Replication pending drain: applied (peer=%q scope=%q user=%q seq=%d)", peer, scope, userField, sequence)
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
			db.exec("delete from pending where peer=? and scope=? and user=? and sequence=?", peer, scope, userField, sequence)
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

	var userUID string
	if row, err := udb.row("select uid from users where username=?", kt.Username); err == nil && row != nil {
		userUID, _ = row["uid"].(string)
	}

	if userUID == "" {
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
	if peer == "" || peer == p2p_id {
		return false
	}

	udb := db_open("db/users.db")

	var u User
	if !udb.scan(&u, "select uid, username, role, methods, status from users where uid=?", userUID) {
		warn("Replication transfer-keys: user %q not found", userUID)
		return false
	}

	rows, err := udb.rows("select id, private, fingerprint, parent, class, name, privacy, data, published from entities where user=?", userUID)
	if err != nil {
		warn("Replication transfer-keys: failed to read entities for user %q: %v", userUID, err)
		return false
	}
	if len(rows) == 0 {
		warn("Replication transfer-keys: no entities for user %q", userUID)
		return false
	}

	kt := KeysTransfer{
		UID:      u.UID,
		Username: u.Username,
		Role:     u.Role,
		Methods:  u.Methods,
		Status:   u.Status,
	}
	if oauthRows, err := udb.rows("select provider, subject, email, verified, name, created from oauth where user=?", userUID); err == nil {
		for _, or := range oauthRows {
			link := KeysOauth{
				Provider: toString(or["provider"]),
				Subject:  toString(or["subject"]),
				Email:    toString(or["email"]),
				Name:     toString(or["name"]),
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
	if credRows, err := udb.rows("select id, public_key, sign_count, name, transports, backup_eligible, backup_state, created from credentials where user=?", userUID); err == nil {
		for _, cr := range credRows {
			c := KeysCredential{
				Name:       toString(cr["name"]),
				Transports: toString(cr["transports"]),
			}
			if id, ok := cr["id"].([]byte); ok {
				c.ID = id
			}
			if pk, ok := cr["public_key"].([]byte); ok {
				c.PublicKey = pk
			}
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
	if recRows, err := udb.rows("select hash, created from recovery where user=?", userUID); err == nil {
		for _, rr := range recRows {
			r := KeysRecovery{Hash: toString(rr["hash"])}
			if v, ok := rr["created"].(int64); ok {
				r.Created = v
			}
			if r.Hash == "" {
				continue
			}
			kt.Recovery = append(kt.Recovery, r)
		}
	}
	if tokRows, err := udb.rows("select hash, app, name, scopes, created, expires from tokens where user=?", userUID); err == nil {
		for _, tr := range tokRows {
			t := KeysToken{
				Hash:   toString(tr["hash"]),
				App:    toString(tr["app"]),
				Name:   toString(tr["name"]),
				Scopes: toString(tr["scopes"]),
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
	if totpRow, err := udb.row("select secret, verified, created from totp where user=?", userUID); err == nil && totpRow != nil {
		t := &KeysTotp{Secret: toString(totpRow["secret"])}
		if v, ok := totpRow["verified"].(int64); ok {
			t.Verified = v != 0
		}
		if v, ok := totpRow["created"].(int64); ok {
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
		warn("Replication transfer-keys: user %q has no valid entities", userUID)
		return false
	}

	from := kt.Entities[0].ID
	m := message(from, "", "replication", "keys/transfer")
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
//
// Package-level alias so callers route through this hook; tests can
// replace it with a no-op to keep the send_peer goroutines (which write
// to queue.db) from outliving their setup tear-down.
var replication_membership_update = replication_membership_update_impl

func replication_membership_update_impl(user string, hosts []string) {
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
	replication_emit_to(user, op, nil)
}

// replication_emit_to_peer emits the op exclusively to one peer, used
// for pair-backfill where existing pair members already hold the row.
func replication_emit_to_peer(user string, op *ReplicationOp, peer string) {
	replication_emit_to(user, op, []string{peer})
}

func replication_emit_to(user string, op *ReplicationOp, peers []string) {
	if peers == nil {
		peers = recipients(user)
	}
	if len(peers) == 0 {
		return
	}

	if op.Scope != repl_scope_app {
		// TODO: core-scope signing needs a parallel message type that
		// signs with p2p_private (server_sign) rather than entity_sign.
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

	op.Sequence = replication_sequence_next(user, op.Scope)

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
