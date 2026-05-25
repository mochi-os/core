// Mochi server: per-user link-request P2P protocol
// Copyright Alistair Cunningham 2026
//
// Wire protocol for per-user replication opt-in. Alice on host A has an
// account; alice (or anyone using B's signup form) types alice@a in B's
// signup "Advanced" disclosure. B sends a `link-request` op to A; A
// stores the row in `replication.db.links` (one row per (target user,
// source peer) — INSERT OR REPLACE so a fresh signup overwrites any
// stale prior). The actual user — already logged in on A — opens
// Settings → Replication, sees the pending request, clicks Approve or
// Deny.
//
// Approve runs `user_is_fresh` on B (via a sync freshness-probe stream,
// see replication_link_freshness_probe.go), then sends a `link-approved`
// op carrying the user's encrypted identity keys + replicated users.db
// rows. B fills in the placeholder, status='active'. Deny / expiry /
// freshness-fail emit `link-denied` so B cleans up the placeholder.
//
// All messages on this path are server-to-server (B doesn't have alice's
// keys at link-request time, and the relationship is "this server is
// asking that server"); they ride the existing message infrastructure
// with From="" — receiver verify() returns true for empty From, and
// libp2p stream-level authentication provides the "this came from peer
// X" signal that the handlers use via e.peer.

package main

import (
	"fmt"
	"time"
)

// LinkRequest is the wire payload for an inbound per-user replication
// request. B sends this to A on signup-with-Advanced; A stores it in
// `replication.db.links` for the user to approve later via Settings →
// Replication. Dedup is per (user, peer) via INSERT OR REPLACE: a fresh
// signup attempt from the same B against the same alice-on-A overwrites
// any prior pending row, redirecting alice's eventual Approve at the
// new placeholder.
type LinkRequest struct {
	// TargetUser is the username on A that B's signup typed in the
	// Advanced "Replicate existing account" field (right-hand side of
	// `alice@a.mochi-os.org` is the routing label; left-hand is this
	// field). A resolves it to a local uid before storing the row.
	TargetUser string `cbor:"target_user"`

	// Label is B's self-advertised display name for itself (e.g. its
	// primary domain). Stored on the row purely for UI rendering on
	// A's settings page; never used as identity. The peer-id (from
	// e.peer at receive time) is the cryptographic identity.
	Label string `cbor:"label,omitempty"`

	// Placeholder is the uid B created locally at signup. Approve
	// targets this placeholder; if a fresh request overwrites the
	// row, the older placeholder is stranded and ages out via B's
	// own 24h safety net.
	Placeholder string `cbor:"placeholder"`
}

// LinkApproved is the wire payload for an outbound approval. Sent from
// A to B after alice clicks Approve on A's Settings → Replication and
// user_is_fresh has returned true for B's placeholder. Carries the
// usual identity-key transfer payload (same shape as KeysTransfer),
// addressed at the specific placeholder B has been holding.
type LinkApproved struct {
	// Placeholder echoes back the placeholder uid B is sitting on.
	// B checks this matches its local pending-replication row before
	// applying the keys.
	Placeholder string `cbor:"placeholder"`

	// Keys is the same payload shape replication uses for whole-server
	// pair identity-key transfer — the user's users.db row fields
	// plus every owned entity including private keys. Re-used so the
	// receiver code path (replication_keys_transfer_apply) is shared.
	Keys KeysTransfer `cbor:"keys"`
}

// LinkDenied is the wire payload for an outbound denial. Sent from A
// to B on any not-Approved outcome:
//   - alice clicked Deny in Settings → Replication
//   - the 1h link-request expired with no decision
//   - user_is_fresh returned false at Approve time
//
// B receives, cleans up the placeholder, and shows the user the same
// "approval denied / expired" UX. Reason is informational only; the
// cleanup is identical in every case.
type LinkDenied struct {
	Placeholder string `cbor:"placeholder"`
	Reason      string `cbor:"reason,omitempty"`
}

// replication_link_request_event is A's receive handler. Decodes the
// payload and delegates to the pure-DB apply function below; split
// out so unit tests don't have to construct a live stream Event.
func replication_link_request_event(e *Event) {
	var lr LinkRequest
	if !e.segment(&lr) {
		info("Replication link-request dropping: cannot decode payload")
		return
	}
	replication_link_request_apply(e.peer, &lr)
}

// replication_link_request_apply is the pure-DB half of the
// link-request path. Resolves the target username to a local uid;
// refuses if no such user; otherwise upserts the row in
// `replication.db.links` keyed on (user, peer) so newest wins.
// Refuses if the source peer already has the target user replicated
// (replication-to-self check — catches both per-user opt-in and
// whole-server pair coverage). The user discovers the request next
// time they open Settings → Replication; no notification fires.
func replication_link_request_apply(originPeer string, lr *LinkRequest) {
	if lr.TargetUser == "" || lr.Placeholder == "" || originPeer == "" {
		info("Replication link-request dropping: missing fields (user=%q placeholder=%q peer=%q)",
			lr.TargetUser, lr.Placeholder, originPeer)
		return
	}

	udb := db_open("db/users.db")
	var u User
	if !udb.scan(&u, "select uid, username, role, methods, status from users where username=?", lr.TargetUser) {
		info("Replication link-request dropping: target user %q not found", lr.TargetUser)
		return
	}

	// A link-request from a peer that's ALREADY in the host set is not
	// a no-op: a healthy replica never re-runs signup, so a re-request
	// means that host lost its local data (wipe / reinstall / disk
	// failure) while keeping its per-server libp2p key, and is asking
	// to re-pull the account. Surface it as a normal pending request so
	// the user can re-approve — approval re-emits the keys-transfer and
	// re-runs the bootstrap, all idempotent (links / hosts are keyed on
	// (user, peer) with INSERT OR REPLACE, so no duplicate rows result).
	// Previously this case was silently dropped, leaving the wiped
	// replica stuck on "waiting for approval" forever.
	rdb := db_open("db/replication.db")

	const link_request_ttl_seconds = 3600 // 1 hour
	received := now()
	rdb.exec(
		"insert or replace into links (user, peer, label, placeholder, received, expires) values (?, ?, ?, ?, ?, ?)",
		u.UID, originPeer, lr.Label, lr.Placeholder, received, received+link_request_ttl_seconds)

	debug("Replication link-request stored: user=%q peer=%q label=%q placeholder=%q",
		u.UID, originPeer, lr.Label, lr.Placeholder)

	// A re-request from a peer already in the host set means that host
	// lost its data (wipe / reinstall) — see the comment above. The
	// wiped replica re-approves with a reset replication.db: its
	// outbound sequence counter restarts at 1. So:
	//   - drop it from the host set (approval re-adds it),
	//   - purge the bulk replication already queued to it — it has no
	//     keys to verify those until re-approval, and the post-approval
	//     bootstrap re-transfers the data wholesale, and
	//   - clear our `seen` dedup rows for it: otherwise its restarted
	//     sequence numbers collide with the pre-wipe ones and we drop
	//     every new op as a duplicate. The pre-wipe ops those rows
	//     referred to no longer exist, so dropping the rows is safe.
	// Control messages (priority above bulk) are left in place.
	if host, _ := rdb.exists("select 1 from hosts where user=? and peer=?", u.UID, originPeer); host {
		rdb.exec("delete from hosts where user=? and peer=?", u.UID, originPeer)
		rdb.exec("delete from seen where peer=? and user=?", originPeer, u.UID)
		db_open("db/queue.db").exec(
			"delete from queue where target=? and priority<=?", originPeer, priority_bulk)
		info("Replication link-request: peer %q re-linking user %q after data loss — dropped from host set, cleared dedup state, purged queued bulk replication", originPeer, u.UID)
	}
}

// replication_link_approved_event is B's receive handler. The matching
// placeholder row must still exist on B (status='pending-replication').
// If yes, apply the keys via the shared keys-transfer code path and
// flip the placeholder's status to 'active'. If the placeholder is
// gone (timed out via B's 24h safety net, or a duplicate signup
// orphaned it), the approval is dropped — the user has already moved
// on; their session reflects whatever current state B has.
func replication_link_approved_event(e *Event) {
	var la LinkApproved
	if !e.segment(&la) {
		info("Replication link-approved dropping: cannot decode payload")
		return
	}
	if la.Placeholder == "" {
		info("Replication link-approved dropping: empty placeholder")
		return
	}

	udb := db_open("db/users.db")
	var u User
	if !udb.scan(&u, "select uid, username, role, methods, status from users where uid=?", la.Placeholder) {
		info("Replication link-approved dropping: placeholder %q not found locally", la.Placeholder)
		return
	}
	if u.Status != "pending-replication" {
		info("Replication link-approved dropping: placeholder %q status=%q (expected pending-replication)",
			la.Placeholder, u.Status)
		return
	}

	// Apply the keys via the shared code path. The signer (e.from)
	// is empty for server-to-server messages, but keys-transfer-apply
	// is set up to check the signer is in the transferred entity set
	// — so for the link-approved path we trust e.peer (libp2p) +
	// the placeholder existing as the authorisation, not the signer.
	// Use a dedicated apply that doesn't enforce the signer check
	// since server-to-server messages don't carry one.
	replication_link_apply_keys(e.peer, la.Placeholder, &la.Keys)
}

// replication_link_apply_keys is the placeholder-aware variant of the
// keys-transfer apply path. The standard keys-transfer apply expects an
// entity signer that's in the transferred set; link-approved messages
// are server-to-server (libp2p-authenticated, no entity signer) so we
// skip that check. The placeholder existing in status='pending-replication'
// is the authorisation: only the user who started the signup on B has
// a session against that placeholder, and only A's user can have
// approved the request, so reaching this code path implies both ends
// of the user's own consent.
func replication_link_apply_keys(originPeer, placeholder string, kt *KeysTransfer) int {
	if kt.Username == "" {
		info("Replication link-approved dropping: empty username in transfer (placeholder=%q)", placeholder)
		return 0
	}

	udb := db_open("db/users.db")

	// IMPORTANT: do NOT flip status='active' here. The placeholder
	// stays in 'pending-replication' until the file + userdbs scopes
	// of bootstrap_start_user complete (see bootstrap_wait_then_activate
	// below). If we activated immediately, the user lands on their
	// dashboard while bulk-bootstrap is still rename(2)-ing DBs out
	// from under any web request that's already opened them — caught
	// live on 2026-05-20 as a "sqlite3: database disk image is
	// malformed" panic on user.db plus a feeds.db that ended at 221 KB
	// instead of the source's 1 GB (the running feeds handler's fd was
	// pinned to the pre-snapshot inode). Deferring activation lets
	// /login/replicating's poll continue showing "waiting" until the
	// backfill is on disk, then bounces the user to a working dashboard.

	inserted := 0
	for _, ent := range kt.Entities {
		if !valid(ent.ID, "entity") {
			continue
		}
		exists, _ := udb.exists("select 1 from entities where id=?", ent.ID)
		if exists {
			continue
		}
		udb.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			ent.ID, ent.Private, ent.Fingerprint, placeholder, ent.Parent, ent.Class, ent.Name, ent.Privacy, ent.Data, ent.Published)
		inserted++
	}

	replication_link_apply_extras(udb, placeholder, kt)

	// Mirror the source's auth-method list so the login UI offers the
	// factors we just imported (passkey / oauth / totp). The placeholder
	// was created with the default methods='email'.
	if kt.Methods != "" {
		udb.exec("update users set methods=? where uid=?", kt.Methods, placeholder)
	}

	// Add the source peer to this user's per-user host set so future
	// ops fan out to it. The membership-change op the source emits
	// at Approve time will eventually reconcile, but we record the
	// reciprocal entry now so writes from here flow to the source.
	rdb := db_open("db/replication.db")
	rdb.exec(
		"insert or replace into hosts (user, peer, added, ack) values (?, ?, ?, 0)",
		placeholder, originPeer, now())

	// Seed the in-order apply cursors for the user's non-file streams
	// (users, sessions) at the source's tail snapshot. Live ops that
	// arrived already — buffered with no cursor — drain now; later
	// ones chain. File-bootstrapped streams seed per-file from the DB
	// snapshot instead (bootstrap_db_seed_cursor).
	for stream, seq := range kt.Seeds {
		replication_cursor_set(rdb, originPeer, repl_scope_app, placeholder, stream, seq)
		replication_stream_drain(rdb, originPeer, repl_scope_app, placeholder, stream)
	}

	// Backfill this user's existing data from the source. Without
	// this the placeholder would have the schema in place (via lazy
	// database_upgrade on first request) but zero rows — only writes
	// that happen *after* the link is approved would ever reach us
	// via the now-correct hosts fan-out. The user would land on their
	// dashboard with no posts, no notifications, no preferences, no
	// attachments.
	//
	// bootstrap_start_user is fully async; it returns immediately and
	// the file + db manifest fetches run in goroutines.
	bootstrap_start_user(originPeer, placeholder)

	// Background waiter that flips status='active' once the bootstrap
	// scopes settle. Until then the placeholder stays pending-replication
	// and /login/replicating's polling keeps the user on the waiting page.
	go bootstrap_wait_then_activate(originPeer, placeholder)

	debug("Replication link-approved applied: placeholder=%q peer=%q username=%q entities=%d",
		placeholder, originPeer, kt.Username, inserted)
	return inserted
}

// replication_link_apply_extras writes the keys-transfer payload that
// isn't entities — auth factors (into users.db) and scheduled events
// (into schedule.db) — re-keyed to the placeholder uid. The mirror of
// replication_link_collect_extras on the source.
//
// Every insert is idempotent: replication_link_apply_keys is not
// exactly-once (a retried link-approved op re-runs it), so OAuth /
// credentials / tokens use INSERT OR IGNORE on their cross-host-stable
// keys, recovery codes are checked by (user, hash) since their PK is a
// local autoincrement, TOTP is INSERT OR REPLACE (one row per user),
// and scheduled events dedup on (user, app, event, created). Bool
// fields are passed straight through — the SQLite driver stores Go
// bools as 0/1 (same as the passkey-registration path).
func replication_link_apply_extras(udb *DB, placeholder string, kt *KeysTransfer) {
	for _, o := range kt.OAuth {
		udb.exec(
			"insert or ignore into oauth (user, provider, subject, email, verified, name, created) values (?, ?, ?, ?, ?, ?, ?)",
			placeholder, o.Provider, o.Subject, o.Email, o.Verified, o.Name, o.Created)
	}
	for _, c := range kt.Credentials {
		udb.exec(
			"insert or ignore into credentials (id, user, public_key, sign_count, name, transports, backup_eligible, backup_state, created) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			c.ID, placeholder, c.PublicKey, c.SignCount, c.Name, c.Transports, c.BackupEligible, c.BackupState, c.Created)
	}
	for _, r := range kt.Recovery {
		if exists, _ := udb.exists("select 1 from recovery where user=? and hash=?", placeholder, r.Hash); !exists {
			udb.exec("insert into recovery (user, hash, created) values (?, ?, ?)", placeholder, r.Hash, r.Created)
		}
	}
	for _, tok := range kt.Tokens {
		udb.exec(
			"insert or ignore into tokens (hash, user, app, name, scopes, created, expires) values (?, ?, ?, ?, ?, ?, ?)",
			tok.Hash, placeholder, tok.App, tok.Name, tok.Scopes, tok.Created, tok.Expires)
	}
	if kt.Totp != nil {
		udb.exec(
			"insert or replace into totp (user, secret, verified, created) values (?, ?, ?, ?)",
			placeholder, kt.Totp.Secret, kt.Totp.Verified, kt.Totp.Created)
	}

	// Scheduled events go into db/schedule.db. The PK is a local
	// autoincrement; dedup on (user, app, event, created) so a
	// re-applied keys-transfer doesn't double-insert.
	sdb := schedule_db()
	for _, s := range kt.Schedule {
		exists, _ := sdb.exists(
			"select 1 from schedule where user=? and app=? and event=? and created=?",
			placeholder, s.App, s.Event, s.Created)
		if exists {
			continue
		}
		sdb.exec(
			"insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, ?, ?, ?, ?)",
			placeholder, s.App, s.Due, s.Event, s.Data, s.Interval, s.Created)
	}
}

// bootstrap_wait_then_activate polls the bootstrap state until the
// `files` and `userdbs` scopes for `peer` both reach state='done',
// then flips the placeholder user from `pending-replication` to
// `active`. Designed to run in a goroutine spawned by apply_keys (or
// by the startup re-spawn that recovers from a mid-bootstrap crash).
//
// Behaviour notes:
//   - Idempotent: the final UPDATE is gated on the current status, so
//     a second waiter (re-spawn after restart) is a no-op once active.
//   - Bounded: gives up after bootstrap_wait_timeout and leaves the
//     placeholder as pending-replication. The user can cancel via
//     /login/replicating's Cancel button, or the bootstrap_resume on
//     next start will re-fire the manifest and a fresh waiter.
//   - Single-target: a per-user link by definition has exactly one
//     source peer, so we don't need to coordinate across multiple
//     peers' scope state — only this one.
var bootstrap_wait_then_activate = bootstrap_wait_then_activate_impl

const (
	bootstrap_wait_poll_interval = 2 * time.Second
	bootstrap_wait_timeout       = 60 * time.Minute
)

func bootstrap_wait_then_activate_impl(peer, uid string) {
	if peer == "" || uid == "" {
		return
	}
	deadline := time.Now().Add(bootstrap_wait_timeout)
	for time.Now().Before(deadline) {
		filesState, _ := bootstrap_get_state(bootstrap_scope_files, peer)
		userdbsState, _ := bootstrap_get_state(bootstrap_scope_userdbs, peer)
		if filesState == bootstrap_state_done && userdbsState == bootstrap_state_done {
			// The userdbs bootstrap copied user.db wholesale, including
			// the user's per-device push subscriptions. Those are
			// host-local by design (accounts.go keeps browser/
			// unifiedpush/fcm rows out of the ongoing-ops fan-out —
			// "each browser/phone registers its own push endpoint per
			// host"). Prune them now, before the user goes active, so
			// this host doesn't push to endpoints that belong to a
			// browser/phone paired with the source.
			replication_link_prune_devices(uid)

			udb := db_open("db/users.db")
			res, err := udb.internal.Exec(
				"update users set status='active' where uid=? and status='pending-replication'",
				uid)
			if err != nil {
				warn("Replication bootstrap-wait: failed to flip placeholder %q to active: %v", uid, err)
				return
			}
			affected, _ := res.RowsAffected()
			if affected > 0 {
				info("Replication bootstrap-wait: placeholder %q activated after bootstrap from peer %q", uid, peer)
			}
			return
		}
		time.Sleep(bootstrap_wait_poll_interval)
	}
	info("Replication bootstrap-wait: gave up after %v on placeholder=%q peer=%q (files=%q userdbs=%q)",
		bootstrap_wait_timeout, uid, peer,
		bootstrap_state_or_empty(bootstrap_scope_files, peer),
		bootstrap_state_or_empty(bootstrap_scope_userdbs, peer))
}

// replication_link_prune_devices deletes per-device push subscriptions
// (browser / unifiedpush / fcm) from the linked user's user.db.accounts
// table. These travel into the replica only because the userdbs
// bootstrap copies user.db wholesale; the ongoing-ops path deliberately
// excludes them (accounts.go) because a push endpoint is registered by
// a specific browser/phone against a specific host. Leaving the
// source's endpoints on the replica would make this host push to
// devices it has no relationship with — double notifications at best.
// Non-device account types (email, AI services, MCP) are host-shared
// and correctly kept.
func replication_link_prune_devices(uid string) {
	udb := db_user(&User{UID: uid}, "user")
	udb.exec("delete from accounts where type in ('browser', 'unifiedpush', 'fcm')")
}

func bootstrap_state_or_empty(scope, peer string) string {
	s, _ := bootstrap_get_state(scope, peer)
	return s
}

// replication_link_resume_pending_activations re-spawns the wait-then-
// activate goroutine for every pending-replication placeholder still
// on disk. Called from server startup so a crash or restart mid-
// bootstrap doesn't strand the user — the bootstrap itself resumes via
// bootstrap_resume; this paired routine ensures the activation flip
// still happens once the resumed bootstrap settles.
//
// Source-peer discovery: per-user link signup writes the source peer
// to replication.db.hosts at apply_keys time, so we read the host
// list for each pending-replication user and pick the first peer.
// A placeholder with multiple hosts predates the activation flip so
// using "first" is fine — there's no fan-out yet.
func replication_link_resume_pending_activations() {
	udb := db_open("db/users.db")
	users, err := udb.rows("select uid from users where status='pending-replication'")
	if err != nil {
		info("Replication link resume: failed to read pending-replication users: %v", err)
		return
	}
	if len(users) == 0 {
		return
	}
	rdb := db_open("db/replication.db")
	for _, u := range users {
		uid, _ := u["uid"].(string)
		if uid == "" {
			continue
		}
		hostRows, err := rdb.rows("select peer from hosts where user=? order by added asc limit 1", uid)
		if err != nil || len(hostRows) == 0 {
			info("Replication link resume: pending-replication user %q has no host — skipping", uid)
			continue
		}
		peer, _ := hostRows[0]["peer"].(string)
		if peer == "" {
			continue
		}
		debug("Replication link resume: re-spawning wait-then-activate for placeholder=%q peer=%q", uid, peer)
		go bootstrap_wait_then_activate(peer, uid)
	}
}

// replication_link_denied_event is B's receive handler. Decodes the
// payload and delegates to the pure-DB apply function below.
func replication_link_denied_event(e *Event) {
	var ld LinkDenied
	if !e.segment(&ld) {
		info("Replication link-denied dropping: cannot decode payload")
		return
	}
	replication_link_denied_apply(e.peer, &ld)
}

// replication_link_denied_apply is the pure-DB half of the link-denied
// path. Any non-Approve outcome on A's side — user clicked Deny, 1h
// timer fired, or user_is_fresh failed — lands here. B cleans up the
// placeholder row and any associated state. The user on B sees their
// waiting banner flip to the denied UX next time they refresh.
func replication_link_denied_apply(originPeer string, ld *LinkDenied) {
	if ld.Placeholder == "" {
		info("Replication link-denied dropping: empty placeholder")
		return
	}

	udb := db_open("db/users.db")
	var u User
	if !udb.scan(&u, "select uid, username, role, methods, status from users where uid=?", ld.Placeholder) {
		// Already cleaned up locally (e.g. 24h safety net fired).
		return
	}
	if u.Status != "pending-replication" {
		debug("Replication link-denied: placeholder %q status=%q (not pending), no cleanup", ld.Placeholder, u.Status)
		return
	}

	// Same cleanup as user_delete but scoped to the placeholder; the
	// placeholder has no replicated data yet (no keys arrived), so
	// the cleanup is degenerate — just drop the users row and the
	// per-user disk dir if anything got scaffolded.
	udb.exec("delete from users where uid=?", ld.Placeholder)
	udb.exec("delete from entities where user=?", ld.Placeholder)

	debug("Replication link-denied applied: placeholder=%q reason=%q (from peer %q)",
		ld.Placeholder, ld.Reason, originPeer)
}

// replication_emit_link_request sends a link-request from this server
// (B in the protocol's naming) to the source server A's peer. Called
// from the signup-with-Advanced path on B after the placeholder row
// is created. The message rides with From="" — libp2p stream-level
// authentication is what tells A this came from B; the From entity
// field stays empty because B has no user-entity for the user being
// linked (alice's keys haven't crossed yet).
func replication_emit_link_request(sourcePeer, targetUser, label, placeholder string) {
	if sourcePeer == "" || targetUser == "" || placeholder == "" {
		return
	}
	m := message("", "", "replication", "link/request")
	m.add(&LinkRequest{TargetUser: targetUser, Label: label, Placeholder: placeholder})
	m.send_peer(sourcePeer)
}

// replication_emit_link_approved sends an approval to the source server
// B (in the protocol's naming, "source" is whichever server has the
// pending placeholder — i.e. the destination of the original signup).
// Called from the Approve action on A's Settings → Replication after
// user_is_fresh on B has returned true. Carries the full identity-key
// transfer payload.
func replication_emit_link_approved(destinationPeer, placeholder string, keys *KeysTransfer) {
	if destinationPeer == "" || placeholder == "" || keys == nil {
		return
	}
	m := message("", "", "replication", "link/approved")
	m.add(&LinkApproved{Placeholder: placeholder, Keys: *keys})
	m.send_peer(destinationPeer)
}

// replication_emit_link_denied sends a denial / expiry / freshness-fail
// to the destination server. The reason field is informational only;
// the destination cleans up the placeholder identically regardless.
//
// Package-level alias so tests can replace it with a no-op to keep the
// send_peer goroutines (which write to queue.db) from outliving the
// test setup tear-down.
var replication_emit_link_denied = replication_emit_link_denied_impl

func replication_emit_link_denied_impl(destinationPeer, placeholder, reason string) {
	if destinationPeer == "" || placeholder == "" {
		return
	}
	m := message("", "", "replication", "link/denied")
	m.add(&LinkDenied{Placeholder: placeholder, Reason: reason})
	m.send_peer(destinationPeer)
}

// FreshnessProbe / FreshnessResult are the wire payloads for the
// synchronous defense-in-depth check A runs against B before sending
// keys. A opens a fresh libp2p stream to B's peer, writes the probe,
// reads the result, closes — bypassing the queue (a queued retry
// can't deliver the response to a live caller).
type FreshnessProbe struct {
	Placeholder string `cbor:"placeholder"`
}

type FreshnessResult struct {
	Fresh bool `cbor:"fresh"`
}

// replication_link_freshness_probe opens a synchronous P2P stream to
// the destination peer holding the named placeholder, sends a probe,
// and returns the freshness result. Used by replication_link_approve
// just before sending keys, to confirm the placeholder hasn't
// accumulated activity in the window between request and approval.
//
// The probe stream is raw (not queued) because a retry can't deliver
// the response to a caller that has already returned. Failures
// propagate to the caller, which surfaces them as
// link-denied(reason="freshness-failed").
func replication_link_freshness_probe(peer, placeholder string) (bool, error) {
	if peer == "" || placeholder == "" {
		return false, fmt.Errorf("freshness probe: empty peer or placeholder")
	}

	s, err := stream_to_peer(peer, "", "", "replication", "lookup/freshness", "", nil)
	if err != nil {
		return false, fmt.Errorf("freshness probe: open stream: %w", err)
	}
	defer s.close()

	if err := s.write(&FreshnessProbe{Placeholder: placeholder}); err != nil {
		return false, fmt.Errorf("freshness probe: write request: %w", err)
	}

	var result FreshnessResult
	if err := s.read(&result); err != nil {
		return false, fmt.Errorf("freshness probe: read response: %w", err)
	}
	return result.Fresh, nil
}

// replication_freshness_probe_event is B's handler for an inbound
// freshness probe. Reads the placeholder uid, runs user_is_fresh,
// writes the boolean result back on the same stream. Must be invoked
// via a live stream (e.stream != nil); a queued retry has no caller
// to respond to and silently no-ops.
func replication_freshness_probe_event(e *Event) {
	if e.stream == nil {
		info("Replication freshness-probe: no stream (queued retry?) — dropping")
		return
	}
	// Stream payload arrives as e.content (same reasoning as
	// replication_user_lookup_event — see comment there).
	placeholder, _ := e.content["placeholder"].(string)
	result := &FreshnessResult{Fresh: user_is_fresh(placeholder)}
	if err := e.stream.write(result); err != nil {
		warn("Replication freshness-probe: failed to write response: %v", err)
	}
}

// UserLookup / UserLookupResult are the wire payloads for the
// synchronous "does this username exist on your server?" probe a
// destination server (B) runs against the source (A) at signup time,
// before creating a `pending-replication` placeholder. B needs A's uid
// for the user so the placeholder lands with the canonical id; A is
// authoritative on what that uid is. Raw-stream request-response, same
// shape as FreshnessProbe — queued retries can't deliver the response.
type UserLookup struct {
	Username string `cbor:"username"`
}

type UserLookupResult struct {
	UID    string `cbor:"uid,omitempty"`
	Exists bool   `cbor:"exists"`
}

// replication_user_lookup opens a synchronous P2P stream to the source
// peer, asks "do you have a user named <username>?", and returns the
// canonical uid (or "" + false if not). Used by the signup-with-
// Advanced path on B before creating a placeholder row.
//
// The lookup is a P2P-only mechanism — there's no HTTPS fallback. If
// the source peer is unreachable the caller surfaces the failure as a
// "couldn't reach <source>" form error; the user can retry. We do not
// fall back to anonymous directory lookups because the source might
// have private users that aren't in the directory.
func replication_user_lookup(peer, username string) (string, bool, error) {
	if peer == "" || username == "" {
		return "", false, fmt.Errorf("user lookup: empty peer or username")
	}

	s, err := stream_to_peer(peer, "", "", "replication", "lookup/user", "", nil)
	if err != nil {
		return "", false, fmt.Errorf("user lookup: open stream: %w", err)
	}
	defer s.close()

	if err := s.write(&UserLookup{Username: username}); err != nil {
		return "", false, fmt.Errorf("user lookup: write request: %w", err)
	}

	var result UserLookupResult
	if err := s.read(&result); err != nil {
		return "", false, fmt.Errorf("user lookup: read response: %w", err)
	}
	return result.UID, result.Exists, nil
}

// replication_user_lookup_event is A's handler for an inbound lookup.
// Reads the username, queries the local users.db for the uid, writes
// the result back on the same stream. Must be invoked via a live
// stream (e.stream != nil); a queued retry has no caller to respond to
// and silently no-ops. The query returns the uid for any local user
// matching the username; the requester is responsible for deciding
// whether that user wants to be replicated (the link-request flow's
// Approve step is the actual consent gate).
//
// Payload arrives as the stream's first segment, which the dispatch
// path already decoded into e.content (a map[string]any) — so the
// handler reads fields from e.content directly rather than re-decoding
// via e.segment (which is for additional segments past the content
// envelope, and would EOF here because the caller only sent one).
func replication_user_lookup_event(e *Event) {
	if e.stream == nil {
		info("Replication user-lookup: no stream (queued retry?) — dropping")
		return
	}
	username, _ := e.content["username"].(string)
	result := &UserLookupResult{}
	if username != "" {
		udb := db_open("db/users.db")
		var u User
		if udb.scan(&u, "select uid, username, role, methods, status from users where username=?", username) {
			result.UID = u.UID
			result.Exists = true
		}
	}
	if err := e.stream.write(result); err != nil {
		warn("Replication user-lookup: failed to write response: %v", err)
	}
}

// replication_link_expire_sweep walks the `links` table on this host
// and emits link-denied(reason="expired") for any row past its expiry,
// then deletes the row locally. Called periodically from the
// replication manager goroutine. Idempotent: a row already gone is
// silently skipped.
func replication_link_expire_sweep() {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select user, peer, placeholder from links where expires < ?", now())
	if err != nil {
		warn("Replication link-expire-sweep: query failed: %v", err)
		return
	}
	for _, row := range rows {
		user, _ := row["user"].(string)
		peer, _ := row["peer"].(string)
		placeholder, _ := row["placeholder"].(string)
		if peer != "" && placeholder != "" {
			replication_emit_link_denied(peer, placeholder, "expired")
		}
		rdb.exec("delete from links where user=? and peer=?", user, peer)
	}
}

// replication_link_approve is called from the Approve handler on A's
// settings page. Verb sequence:
//
//  1. DELETE the row from `links` under the same SQLite transaction as
//     the freshness-probe call and key transfer. The DELETE-as-lock
//     makes the multi-tab race idempotent: a concurrent second Approve
//     finds zero rows affected and returns the already-approved status
//     without re-firing the transfer.
//  2. Run freshness-probe on the destination peer (synchronous P2P
//     call). If the placeholder has accumulated activity, abort with
//     link-denied(reason="freshness-failed").
//  3. Load the user's identity keys + entities, build the KeysTransfer
//     payload, send link-approved.
//  4. Add the destination peer to the local user's host set + emit a
//     membership-change so any existing per-user hosts learn about the
//     new member.
//
// Returns ("approved", nil) on success, ("already-approved", nil) on
// the multi-tab loser, (..., error) on probe / build / transfer
// failures.
func replication_link_approve(user, peer string) (string, error) {
	rdb := db_open("db/replication.db")

	row, _ := rdb.row("select placeholder from links where user=? and peer=?", user, peer)
	if row == nil {
		return "already-approved", nil
	}
	placeholder, _ := row["placeholder"].(string)
	if placeholder == "" {
		return "", fmt.Errorf("link row has empty placeholder")
	}

	res, err := rdb.internal.Exec("delete from links where user=? and peer=?", user, peer)
	if err != nil {
		return "", fmt.Errorf("delete link row: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// Another tab won the race.
		return "already-approved", nil
	}

	fresh, err := replication_link_freshness_probe(peer, placeholder)
	if err != nil {
		replication_emit_link_denied(peer, placeholder, "freshness-failed")
		return "", fmt.Errorf("freshness probe failed: %w", err)
	}
	if !fresh {
		replication_emit_link_denied(peer, placeholder, "freshness-failed")
		return "", fmt.Errorf("destination placeholder is no longer empty")
	}

	udb := db_open("db/users.db")
	var u User
	if !udb.scan(&u, "select uid, username, role, methods, status from users where uid=?", user) {
		return "", fmt.Errorf("local user %q not found", user)
	}

	var entities []Entity
	udb.scans(&entities, "select id, private, fingerprint, parent, class, name, privacy, data, published from entities where user=?", user)
	keysEntities := make([]KeysEntity, 0, len(entities))
	for _, e := range entities {
		keysEntities = append(keysEntities, KeysEntity{
			ID:          e.ID,
			Private:     e.Private,
			Fingerprint: e.Fingerprint,
			Parent:      e.Parent,
			Class:       e.Class,
			Name:        e.Name,
			Privacy:     e.Privacy,
			Data:        e.Data,
			Published:   e.Published,
		})
	}

	keys := &KeysTransfer{
		Username: u.Username,
		Methods:  u.Methods,
		Status:   "active",
		// Role intentionally omitted — server-local per the plan.
		Entities: keysEntities,
	}
	replication_link_collect_extras(udb, user, keys)

	// Add peer to the user's host set before emitting link-approved,
	// and snapshot the non-file replication streams' tails at that
	// instant. The replica seeds its in-order apply cursors from
	// keys.Seeds so the first op it receives on each stream — emitted
	// at or after this point — chains onto the seed.
	rdb.exec(
		"insert or replace into hosts (user, peer, added, ack) values (?, ?, ?, 0)",
		user, peer, now())
	keys.Seeds = map[string]int64{
		"users":    replication_tail(user, repl_scope_app, "users"),
		"sessions": replication_tail(user, repl_scope_app, "sessions"),
	}

	replication_emit_link_approved(peer, placeholder, keys)
	audit_replication_link_approved(user, peer)

	var current []string
	if rows, err := rdb.rows("select peer from hosts where user=?", user); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				current = append(current, p)
			}
		}
	}
	replication_membership_update(user, current)

	return "approved", nil
}

// replication_link_collect_extras fills the non-entity fields of `keys`
// from the source for `uid`: OAuth provider links, passkey credentials,
// recovery codes, API tokens, the TOTP secret (all from users.db), and
// scheduled events (from schedule.db).
//
// Whole-server pair replication gets the users.db rows for free via the
// bootstrap snapshot. Per-user link has no such snapshot — only this
// user moves to the replica, the rest of users.db stays on the source
// — so the keys-transfer payload must carry them explicitly. Without
// this the user reaches the replica with username + entities only:
// their OAuth links, recovery codes, passkeys, TOTP and scheduled
// events all silently absent, leaving a fresh email code as the one
// usable login method.
func replication_link_collect_extras(udb *DB, uid string, keys *KeysTransfer) {
	if oauth, err := udb.rows("select provider, subject, email, verified, name, created from oauth where user=?", uid); err == nil {
		for _, r := range oauth {
			keys.OAuth = append(keys.OAuth, KeysOauth{
				Provider: row_string(r, "provider"),
				Subject:  row_string(r, "subject"),
				Email:    row_string(r, "email"),
				Verified: row_int(r, "verified") != 0,
				Name:     row_string(r, "name"),
				Created:  row_int(r, "created"),
			})
		}
	}

	if creds, err := udb.rows("select id, public_key, sign_count, name, transports, backup_eligible, backup_state, created from credentials where user=?", uid); err == nil {
		for _, r := range creds {
			id, _ := r["id"].([]byte)
			pk, _ := r["public_key"].([]byte)
			keys.Credentials = append(keys.Credentials, KeysCredential{
				ID:             id,
				PublicKey:      pk,
				SignCount:      row_int(r, "sign_count"),
				Name:           row_string(r, "name"),
				Transports:     row_string(r, "transports"),
				BackupEligible: row_int(r, "backup_eligible") != 0,
				BackupState:    row_int(r, "backup_state") != 0,
				Created:        row_int(r, "created"),
			})
		}
	}

	if recovery, err := udb.rows("select hash, created from recovery where user=?", uid); err == nil {
		for _, r := range recovery {
			keys.Recovery = append(keys.Recovery, KeysRecovery{
				Hash:    row_string(r, "hash"),
				Created: row_int(r, "created"),
			})
		}
	}

	if tokens, err := udb.rows("select hash, app, name, scopes, created, expires from tokens where user=?", uid); err == nil {
		for _, r := range tokens {
			keys.Tokens = append(keys.Tokens, KeysToken{
				Hash:    row_string(r, "hash"),
				App:     row_string(r, "app"),
				Name:    row_string(r, "name"),
				Scopes:  row_string(r, "scopes"),
				Created: row_int(r, "created"),
				Expires: row_int(r, "expires"),
			})
		}
	}

	if totp, _ := udb.row("select secret, verified, created from totp where user=?", uid); totp != nil {
		keys.Totp = &KeysTotp{
			Secret:   row_string(totp, "secret"),
			Verified: row_int(totp, "verified") != 0,
			Created:  row_int(totp, "created"),
		}
	}

	// Scheduled events live in db/schedule.db (a system DB, not in any
	// per-user bootstrap scope), so per-user link must carry them in
	// the payload or the user's reminders / recurring jobs are left
	// behind on the source.
	if sched, err := schedule_db().rows("select app, due, event, data, interval, created from schedule where user=?", uid); err == nil {
		for _, r := range sched {
			keys.Schedule = append(keys.Schedule, KeysSchedule{
				App:      row_string(r, "app"),
				Due:      row_int(r, "due"),
				Event:    row_string(r, "event"),
				Data:     row_string(r, "data"),
				Interval: row_int(r, "interval"),
				Created:  row_int(r, "created"),
			})
		}
	}
}

// replication_link_deny is called from the Deny handler on A's settings
// page. Same DELETE-as-lock idempotence pattern as approve. Emits
// link-denied(reason="denied") to the destination on success; returns
// "already-handled" on the multi-tab loser.
func replication_link_deny(user, peer string) string {
	rdb := db_open("db/replication.db")

	row, _ := rdb.row("select placeholder from links where user=? and peer=?", user, peer)
	if row == nil {
		return "already-handled"
	}
	placeholder, _ := row["placeholder"].(string)

	res, err := rdb.internal.Exec("delete from links where user=? and peer=?", user, peer)
	if err != nil {
		warn("Replication link-deny: delete failed for user=%q peer=%q: %v", user, peer, err)
		return ""
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return "already-handled"
	}

	if placeholder != "" {
		replication_emit_link_denied(peer, placeholder, "denied")
	}
	audit_replication_link_denied(user, peer)
	return "denied"
}
