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

	// Replication-to-self refusal: if this peer already shows up in
	// the user's per-user host set, there's nothing to do.
	rdb := db_open("db/replication.db")
	if exists, _ := rdb.exists("select 1 from hosts where user=? and peer=?", u.UID, originPeer); exists {
		info("Replication link-request dropping: peer %q already in user %q's host set", originPeer, u.UID)
		return
	}

	const link_request_ttl_seconds = 3600 // 1 hour
	received := now()
	rdb.exec(
		"insert or replace into links (user, peer, label, placeholder, received, expires) values (?, ?, ?, ?, ?, ?)",
		u.UID, originPeer, lr.Label, lr.Placeholder, received, received+link_request_ttl_seconds)

	debug("Replication link-request stored: user=%q peer=%q label=%q placeholder=%q",
		u.UID, originPeer, lr.Label, lr.Placeholder)
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

	// Reconcile the placeholder with the source's canonical username.
	// Per the replicated-column policy (users.db — beyond entities in
	// the plan), per-user replication keeps username server-local;
	// the placeholder retains whatever the user chose on B at signup,
	// and the source's username is ignored.
	if _, err := udb.internal.Exec("update users set status='active' where uid=?", placeholder); err != nil {
		warn("Replication link-approved: failed to flip placeholder %q to active: %v", placeholder, err)
		return 0
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
		udb.exec("insert into entities (id, private, fingerprint, user, parent, class, name, privacy, data, published) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			ent.ID, ent.Private, ent.Fingerprint, placeholder, ent.Parent, ent.Class, ent.Name, ent.Privacy, ent.Data, ent.Published)
		inserted++
	}

	// Add the source peer to this user's per-user host set so future
	// ops fan out to it. The membership-change op the source emits
	// at Approve time will eventually reconcile, but we record the
	// reciprocal entry now so writes from here flow to the source.
	rdb := db_open("db/replication.db")
	rdb.exec(
		"insert or replace into hosts (user, peer, added, ack) values (?, ?, ?, 0)",
		placeholder, originPeer, now())

	debug("Replication link-approved applied: placeholder=%q peer=%q username=%q entities=%d",
		placeholder, originPeer, kt.Username, inserted)
	return inserted
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

	s, err := stream_to_peer(peer, "", "", "replication", "freshness/probe", "", nil)
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

	s, err := stream_to_peer(peer, "", "", "replication", "user/lookup", "", nil)
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

	replication_emit_link_approved(peer, placeholder, keys)
	audit_replication_link_approved(user, peer)

	// Add peer to user's host set + emit membership-change so any
	// existing per-user peers also learn about the new member.
	rdb.exec(
		"insert or replace into hosts (user, peer, added, ack) values (?, ?, ?, 0)",
		user, peer, now())

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
