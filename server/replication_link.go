// Mochi server: replication invite Net protocols (link + join)
// Copyright Alistair Cunningham 2026
//
// This file contains two related-but-distinct wire protocols for
// growing the replication set:
//
//   - link  : per-user opt-in (alice@a → B, "Settings -> Replication"
//             on A). One link per (target user, source peer). The
//             user authorises; admin not involved.
//   - join  : whole-server pair growth (mochictl replica join from a
//             fresh B; admin on A approves). Adds B to pair, emits
//             pair-membership-change to existing members.
//
// Same shape (request -> store -> approve -> activate -> propagate),
// distinct semantics (per-user vs whole-server). Merged into one file
// because they're the same conceptual family and the per-protocol
// section headers below preserve the original file's rationale.

package main

import (
	"fmt"
	"time"
)

// ============================================================
// link: per-user opt-in invite protocol
// ============================================================
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
	// ops fan out to it. The join ops the source announces at Approve
	// time will eventually reconcile, but we record the reciprocal
	// entry now so writes from here flow to the source. Then assert our
	// own membership immediately so the other hosts' rows for us carry
	// a fresh seen + attestation without waiting for the hourly tick.
	rdb := db_open("db/replication.db")
	rdb.exec(
		"insert or replace into hosts (user, peer, added, ack, seen) values (?, ?, ?, 0, ?)",
		placeholder, originPeer, now(), now())
	replication_membership_assert(placeholder)

	// Seed the in-order apply cursors for the user's non-file streams
	// (users, sessions) at the source's tail snapshot. Live ops that
	// arrived already — buffered with no cursor — drain now; later
	// ones chain. File-bootstrapped streams seed per-file from the DB
	// snapshot instead (bootstrap_db_seed_cursor).
	for stream, seq := range kt.Seeds {
		// Seeds carries only system-row streams (users, sessions) under bare
		// logical keys; re-qualify to the class key the gate uses locally.
		key := repl_stream_key(repl_stream_class_system, stream)
		replication_cursor_set(rdb, originPeer, repl_scope_app, placeholder, key, seq)
		replication_stream_drain(rdb, originPeer, repl_scope_app, placeholder, key)
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
		files_state, _ := bootstrap_get_state(bootstrap_scope_files, peer)
		userdbs_state, _ := bootstrap_get_state(bootstrap_scope_userdbs, peer)
		if files_state == bootstrap_state_done && userdbs_state == bootstrap_state_done {
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
		host_rows, err := rdb.rows("select peer from hosts where user=? order by added asc limit 1", uid)
		if err != nil || len(host_rows) == 0 {
			info("Replication link resume: pending-replication user %q has no host — skipping", uid)
			continue
		}
		peer, _ := host_rows[0]["peer"].(string)
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

// replication_link_freshness_probe opens a synchronous Net stream to
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

// replication_user_lookup opens a synchronous Net stream to the source
// peer, asks "do you have a user named <username>?", and returns the
// canonical uid (or "" + false if not). Used by the signup-with-
// Advanced path on B before creating a placeholder row.
//
// The lookup is a Net-only mechanism — there's no HTTPS fallback. If
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
//  2. Run freshness-probe on the destination peer (synchronous Net
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
	keys_entities := make([]KeysEntity, 0, len(entities))
	for _, e := range entities {
		keys_entities = append(keys_entities, KeysEntity{
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
		Entities: keys_entities,
	}
	replication_link_collect_extras(udb, user, keys)

	// Add peer to the user's host set before emitting link-approved,
	// and snapshot the non-file replication streams' tails at that
	// instant. The replica seeds its in-order apply cursors from
	// keys.Seeds so the first op it receives on each stream — emitted
	// at or after this point — chains onto the seed.
	rdb.exec(
		"insert or replace into hosts (user, peer, added, ack, seen) values (?, ?, ?, 0, ?)",
		user, peer, now(), now())
	// Bare logical keys on the wire (stable format); values from the
	// class-qualified local tail. The receiver re-qualifies before seeding.
	keys.Seeds = map[string]int64{
		"users":    replication_tail(user, repl_scope_app, repl_stream_key(repl_stream_class_system, "users")),
		"sessions": replication_tail(user, repl_scope_app, repl_stream_key(repl_stream_class_system, "sessions")),
	}

	replication_emit_link_approved(peer, placeholder, keys)
	audit_replication_link_approved(user, peer)

	// Announce the user-authorised set as additive `join` ops, one per
	// member (existing hosts ∪ the new one). Every host — existing and new —
	// learns every member; join only adds, so this can never strip a host.
	var current []string
	if rows, err := rdb.rows("select peer from hosts where user=?", user); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				current = append(current, p)
			}
		}
	}
	replication_membership_announce(user, current)
	replication_membership_assert(user)

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

// ============================================================
// join: whole-server pair growth protocol
// ============================================================

// JoinRequest is the wire payload from a fresh replica asking to join
// a pair set. Label is the replica's self-advertised display name (UI
// hint only); the peer-id from e.peer at receive time is the
// cryptographic identity.
type JoinRequest struct {
	Label string `cbor:"label,omitempty"`
}

// JoinApproved is the wire payload from a source to a replica
// signalling that the operator has approved the join. Members is the
// new pair set including the replica; the replica writes these to its
// own `replication.db.pair` so it can immediately fan-out subsequent
// writes to the rest of the cluster.
type JoinApproved struct {
	Members []string `cbor:"members"`
}

// JoinDenied is the wire payload from a source to a replica signalling
// any non-Approve outcome: operator clicked Deny, the 10-minute expiry
// fired, or some defense-in-depth check failed. Reason is informational
// only; the replica treats every denial identically (mochictl exits
// non-zero with the reason for the operator).
type JoinDenied struct {
	Reason string `cbor:"reason,omitempty"`
}

// PairMembershipChange announces the new pair set to existing members
// when the set changes (a replica was approved or removed). Receivers
// replace their local `replication.db.pair` with Members (self-filtered).
// Sequence is monotonic per emitting peer so a stale message can't
// overwrite a newer one — same pattern as MembershipChange for per-user
// host sets.
type PairMembershipChange struct {
	Members  []string `cbor:"members"`
	Sequence int64    `cbor:"sequence"`
}

// replication_join_request_event is the source's receive handler.
// Decodes the payload and delegates to the pure-DB apply function.
func replication_join_request_event(e *Event) {
	var jr JoinRequest
	if !e.segment(&jr) {
		info("Replication join-request dropping: cannot decode payload")
		return
	}
	replication_join_request_apply(e.peer, &jr)
}

// replication_join_request_apply is the pure-DB half of the
// join-request path. Upserts a row in `replication.db.joins` keyed on
// the source peer; INSERT OR REPLACE makes the dedup mechanical (a
// repeat from the same replica overwrites the prior row).
//
// A join-request from a peer that is already in the pair set is
// accepted (not dropped) — this is the recovery flow for a replica
// that lost its disk and re-installed with the same p2p id. The
// admin's Approve action runs through the same code path either way:
// the pair INSERT OR REPLACE is a no-op for the already-present row,
// the join-approved emit makes the replica re-run bootstrap_start, and
// replication_pair_backfill re-seeds the sysdbs. Without this, the
// only operator path back is to remove the pair row on the source side
// first, which is fiddlier and unnecessary.
func replication_join_request_apply(originPeer string, jr *JoinRequest) {
	if originPeer == "" {
		info("Replication join-request dropping: empty peer")
		return
	}

	rdb := db_open("db/replication.db")

	const join_request_ttl_seconds = 600 // 10 minutes
	received := now()
	rdb.exec(
		"insert or replace into joins (peer, label, received, expires) values (?, ?, ?, ?)",
		originPeer, jr.Label, received, received+join_request_ttl_seconds)

	debug("Replication join-request stored: peer=%q label=%q", originPeer, jr.Label)
}

// replication_join_approved_event is the replica's receive handler.
// Records the announced pair set in `replication.db.pair`. The pair
// set received from the source is authoritative for the replica at
// join time — the replica had no prior pair state.
func replication_join_approved_event(e *Event) {
	var ja JoinApproved
	if !e.segment(&ja) {
		info("Replication join-approved dropping: cannot decode payload")
		return
	}
	replication_join_approved_apply(e.peer, &ja)
}

// replication_join_approved_apply applies an approved pair set on the
// replica side. Replaces the local pair table with Members (filtered
// to exclude self), then kicks off bulk bootstrap against the source
// peer (the one that approved us) so the replica's empty filesystem
// gets populated.
func replication_join_approved_apply(originPeer string, ja *JoinApproved) {
	rdb := db_open("db/replication.db")
	rdb.exec("delete from pair")
	for _, peer := range ja.Members {
		if peer == "" || peer == net_id {
			continue
		}
		rdb.exec("insert or replace into pair (peer, added, role) values (?, ?, '')", peer, now())
	}
	pair_membership_refresh()
	debug("Replication join-approved applied: members=%v (from peer %q)", ja.Members, originPeer)
	// Start file-tree bulk bootstrap against the approving source.
	// V5 handles file-scope only; DB-scope bootstrap arrives in a
	// follow-up that adds a bootstrap-db-manifest event.
	bootstrap_start(originPeer)
}

// replication_join_denied_event is the replica's receive handler for a
// denial. Decodes the payload and delegates to the pure-DB apply
// function so unit tests can exercise the settings-update path without
// constructing a live stream Event.
func replication_join_denied_event(e *Event) {
	var jd JoinDenied
	if !e.segment(&jd) {
		info("Replication join-denied dropping: cannot decode payload")
		return
	}
	replication_join_denied_apply(e.peer, jd.Reason)
}

// replication_join_denied_apply records the denial in settings.db if
// this denial matches the current pending peer. admin_replica_status
// reads the result to report "denied" the next time mochictl polls.
func replication_join_denied_apply(originPeer, reason string) {
	if pending := setting_get("replica.join.peer", ""); pending == originPeer {
		setting_set("replica.join.state", "denied")
		setting_set("replica.join.reason", reason)
	}
	debug("Replication join-denied received: reason=%q (from peer %q)", reason, originPeer)
}

// replication_pair_membership_change_event is the existing-member
// receive handler for pair-set changes (a new replica joined, an
// existing member was removed). Replaces the local pair table with
// the announced set; the sequence number is monotonic per origin
// peer so a stale message can't overwrite a newer one.
func replication_pair_membership_change_event(e *Event) {
	var pmc PairMembershipChange
	if !e.segment(&pmc) {
		info("Replication pair-membership-change dropping: cannot decode payload")
		return
	}
	replication_pair_membership_apply(e.peer, &pmc)
}

// replication_pair_membership_apply is the pure-DB half. Uses the
// generic per-peer `seen` table with scope="pair-membership" for
// dedup + staleness rejection, same pattern as the user-scope
// membership-apply path.
func replication_pair_membership_apply(originPeer string, pmc *PairMembershipChange) {
	db := db_open("db/replication.db")

	if applied, _ := db.exists(
		"select 1 from seen where peer=? and scope='pair-membership' and user='' and sequence=?",
		originPeer, pmc.Sequence); applied {
		return
	}

	var latest int64
	if row, err := db.row("select max(sequence) as seq from seen where scope='pair-membership' and user=''"); err == nil && row != nil {
		if v, ok := row["seq"].(int64); ok {
			latest = v
		}
	}
	stale := pmc.Sequence < latest

	if !stale {
		// If this receiver isn't in the announced set, treat as
		// "I've been kicked": empty the pair entirely. Otherwise
		// rebuild it as the announced set minus self.
		in_set := false
		for _, p := range pmc.Members {
			if p == net_id {
				in_set = true
				break
			}
		}

		// Compute newly-added peers (in announced \ current pair) BEFORE
		// rewriting pair. Those are fresh joiners — clear any stale
		// `seen` rows the same way the approver does in
		// replication_join_approve_core. Member-leave (peer in current
		// pair but not in announced) does NOT clear seen because a
		// future re-join goes through the approve path again.
		current := map[string]bool{}
		if rows, err := db.rows("select peer from pair"); err == nil {
			for _, r := range rows {
				if p, ok := r["peer"].(string); ok && p != "" {
					current[p] = true
				}
			}
		}

		db.exec("delete from pair")
		if in_set {
			for _, peer := range pmc.Members {
				if peer == "" || peer == net_id {
					continue
				}
				db.exec("insert or replace into pair (peer, added, role) values (?, ?, '')", peer, now())
				if !current[peer] {
					db.exec("delete from seen where peer=?", peer)
				}
			}
		}
	}

	db.exec(
		"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, 'pair-membership', '', ?, ?)",
		originPeer, pmc.Sequence, now())

	if !stale {
		pair_membership_refresh()
	}

	if stale {
		debug("Replication pair-membership-change stale: seq=%d < latest=%d (from peer %q)",
			pmc.Sequence, latest, originPeer)
	} else {
		debug("Replication pair-membership-change applied: members=%v (from peer %q)", pmc.Members, originPeer)
	}
}

// replication_emit_join_request sends a join-request from this server
// (the joining replica) to the source peer. Called from mochictl
// replica join after the zero-users gate passes.
func replication_emit_join_request(sourcePeer, label string) {
	if sourcePeer == "" {
		return
	}
	m := message("", "", "replication", "join/request")
	m.add(&JoinRequest{Label: label})
	m.send_peer(sourcePeer)
}

// replication_emit_join_approved sends an approval to the replica.
// Carries the full member set (including the new replica) so the
// replica can populate its own pair table.
func replication_emit_join_approved(replicaPeer string, members []string) {
	if replicaPeer == "" {
		return
	}
	m := message("", "", "replication", "join/approved")
	m.add(&JoinApproved{Members: members})
	m.send_peer(replicaPeer)
}

// replication_emit_join_denied sends a denial to the replica with an
// informational reason.
//
// Package-level alias so tests can replace it with a no-op to keep
// the send_peer goroutines (which write to queue.db) from outliving
// the test setup tear-down.
var replication_emit_join_denied = replication_emit_join_denied_impl

func replication_emit_join_denied_impl(replicaPeer, reason string) {
	if replicaPeer == "" {
		return
	}
	m := message("", "", "replication", "join/denied")
	m.add(&JoinDenied{Reason: reason})
	m.send_peer(replicaPeer)
}

// replication_emit_pair_membership_change propagates a pair-set change
// to every existing pair member. Called after Approve writes the new
// member to the pair table or after Remove deletes one.
func replication_emit_pair_membership_change(members, targets []string) {
	seq := replication_sequence_next("", "pair-membership")
	pmc := &PairMembershipChange{Members: members, Sequence: seq}
	for _, peer := range targets {
		if peer == "" || peer == net_id {
			continue
		}
		m := message("", "", "replication", "pair/membership/change")
		m.add(pmc)
		m.send_peer(peer)
	}
}

// replication_join_expire_sweep walks the `joins` table on this host
// and emits join-denied(reason="expired") for any row past its expiry,
// then deletes the row locally. Called periodically from the
// replication manager goroutine. Idempotent.
func replication_join_expire_sweep() {
	rdb := db_open("db/replication.db")
	rows, err := rdb.rows("select peer from joins where expires < ?", now())
	if err != nil {
		warn("Replication join-expire-sweep: query failed: %v", err)
		return
	}
	for _, row := range rows {
		peer, _ := row["peer"].(string)
		if peer != "" {
			replication_emit_join_denied(peer, "expired")
		}
		rdb.exec("delete from joins where peer=?", peer)
	}
}

// replication_join_approve_core is the DB-only half of the approve
// path; returns (status, fullMemberSet, existingPeersToNotify, error).
// Split so unit tests can exercise the DELETE-as-lock + pair-insert
// behaviour without triggering goroutine emits that outlive the test.
//
//  1. DELETE the row from `joins`. DELETE-as-lock makes the multi-tab
//     race idempotent — a concurrent second call finds zero rows
//     affected and returns "already-approved".
//  2. Insert into `replication.db.pair` for the new member.
//  3. Compute the full new member set and the existing-members subset
//     that needs the pair-membership-change broadcast.
//
// Returns ("already-approved", nil, nil, nil) on the multi-tab loser;
// ("approved", full, existing, nil) on the winner; (..., ..., ..., err)
// on DB failure.
func replication_join_approve_core(peer string) (string, []string, []string, error) {
	rdb := db_open("db/replication.db")

	res, err := rdb.internal.Exec("delete from joins where peer=?", peer)
	if err != nil {
		return "", nil, nil, fmt.Errorf("delete joins row: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return "already-approved", nil, nil, nil
	}

	rdb.exec("insert or replace into pair (peer, added, role) values (?, ?, '')", peer, now())
	pair_membership_refresh()

	// Joining replica is fresh by protocol contract — its sequence
	// counters restart at 0. Clear any stale `seen` rows from a prior
	// incarnation of the same libp2p host (preserved across reinstall
	// per the per-server-not-per-user host-key convention) so its new
	// ops at low sequence numbers don't get silently dropped as
	// duplicates by max-seen-sequence dedup.
	rdb.exec("delete from seen where peer=?", peer)

	// Pre-populate bootstrap_served with the scopes the new replica
	// is about to pull. Each gets cleared by a `bootstrap/scope/done`
	// ack from the replica when it flips that scope to done locally.
	// While a row exists, the operator UI shows this peer as "Syncing"
	// instead of "Synced", matching the replica's own status.
	ts := now()
	for _, scope := range []string{bootstrap_scope_files, bootstrap_scope_apps, bootstrap_scope_userdbs} {
		rdb.exec("insert or replace into bootstrap_served (peer, scope, started) values (?, ?, ?)", peer, scope, ts)
	}

	var members []string
	if rows, err := rdb.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				members = append(members, p)
			}
		}
	}
	// The full set the replica should know (everyone, including self
	// since the replica needs to fan-out to the source too).
	full := append([]string{net_id}, members...)

	// Existing pair members (everyone except the new joiner) get the
	// pair-membership-change announcement.
	var existing []string
	for _, m := range members {
		if m != peer {
			existing = append(existing, m)
		}
	}
	return "approved", full, existing, nil
}

// replication_join_approve is the high-level Approve handler. Calls
// the DB-only core then fires the join-approved op to the new replica
// and pair-membership-change to existing members.
func replication_join_approve(peer string) (string, error) {
	status, full, existing, err := replication_join_approve_core(peer)
	if err != nil || status != "approved" {
		return status, err
	}
	replication_emit_join_approved(peer, full)
	if len(existing) > 0 {
		replication_emit_pair_membership_change(full, existing)
	}
	// Backfill system DBs (users + settings + apps + domains) to the
	// new peer via the op channel. Replaces the sysdbs scope of bulk
	// bootstrap, which atomic-rename-replaced files the running
	// receiver had open and corrupted state. Async; returns immediately.
	replication_pair_backfill(peer)
	audit_replication_pair_join_approved(peer)
	return status, nil
}

// replication_join_deny_core is the DB-only half of the deny path.
// Returns "denied" on the winner, "already-handled" on the multi-tab
// loser. Split for the same testability reason as approve_core.
func replication_join_deny_core(peer string) string {
	rdb := db_open("db/replication.db")

	res, err := rdb.internal.Exec("delete from joins where peer=?", peer)
	if err != nil {
		warn("Replication join-deny: delete failed for peer=%q: %v", peer, err)
		return ""
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return "already-handled"
	}
	return "denied"
}

// replication_join_deny is the high-level Deny handler. Calls the
// DB-only core then fires join-denied to the replica on success.
func replication_join_deny(peer string) string {
	status := replication_join_deny_core(peer)
	if status == "denied" {
		replication_emit_join_denied(peer, "denied")
		audit_replication_pair_join_denied(peer)
	}
	return status
}

// admin_replication_emit_pair_membership is the package-level emit-function
// variable used by replication_pair_remove. Lives here (rather than alongside
// the admin HTTP handler) so the cross-platform Starlark API and tests can
// reach it on non-Linux builds. Tests stub it out, same pattern as
// admin_replica_emit_pair_membership.
var admin_replication_emit_pair_membership = replication_emit_pair_membership_change

// replication_pair_remove deletes `peer` from the local pair table and
// announces the resulting member set to every remaining member.
// Returns (removed-peer, remaining-members, ok). ok is false when the
// peer wasn't in the pair set. Shared by the admin HTTP handler and the
// mochi.replication.pair.remove Starlark API, so it lives here rather
// than in admin_replication.go.
func replication_pair_remove(peer string) (string, []string, bool) {
	rdb := db_open("db/replication.db")
	exists, _ := rdb.exists("select 1 from pair where peer=?", peer)
	if !exists {
		return "", nil, false
	}

	rdb.exec("delete from pair where peer=?", peer)
	pair_membership_refresh()
	// Removing the relationship resolves any irreparable badge for it.
	replication_irreparable_clear(peer, repl_scope_core)

	var remaining []string
	if rows, err := rdb.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				remaining = append(remaining, p)
			}
		}
	}

	// Announce the new pair set to remaining members AND the kicked
	// peer. The kicked peer receives Members that doesn't include it,
	// which the receiver treats as "I've been kicked" — empties its
	// pair table. Without notifying the kicked peer, an N=2 unpair
	// would leave the other side believing the pair still exists,
	// because there are no remaining members to forward the change.
	full := append([]string{net_id}, remaining...)
	recipients := append([]string{peer}, remaining...)
	admin_replication_emit_pair_membership(full, recipients)
	audit_replication_pair_removed(peer)

	return peer, remaining, true
}
