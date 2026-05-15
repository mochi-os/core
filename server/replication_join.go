// Mochi server: whole-server pair join-request P2P protocol
// Copyright Alistair Cunningham 2026
//
// Wire protocol for whole-server pair growth. A fresh replica B (no
// admin account, no users) runs `mochictl replica join <source-id>`;
// mochictl emits a `join-request` op to the source's peer. The source
// stores it in `replication.db.joins` and renders the row on the Pair
// page (apps/settings/system/replication.star). The operator clicks
// Approve, which adds B to `replication.db.pair`, emits `join-approved`
// back to B, and propagates a `pair-membership-change` to any existing
// pair members. B then begins bulk bootstrap (see #66).
//
// All messages on this path are server-to-server (B has no admin or
// user-entity at join time); they ride the existing message infrastructure
// with From="" — receiver verify() returns true for empty From, and
// libp2p stream-level authentication provides the "this came from peer
// X" signal that handlers use via e.peer. Mirrors the per-user
// link-request protocol in replication_link.go.

package main

import (
	"fmt"
)

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
		if peer == "" || peer == p2p_id {
			continue
		}
		rdb.exec("insert or replace into pair (peer, added, role) values (?, ?, '')", peer, now())
	}
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
		db.exec("delete from pair")
		for _, peer := range pmc.Members {
			if peer == "" || peer == p2p_id {
				continue
			}
			db.exec("insert or replace into pair (peer, added, role) values (?, ?, '')", peer, now())
		}
	}

	db.exec(
		"insert or ignore into seen (peer, scope, user, sequence, applied) values (?, 'pair-membership', '', ?, ?)",
		originPeer, pmc.Sequence, now())

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
		if peer == "" || peer == p2p_id {
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
	full := append([]string{p2p_id}, members...)

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
// peer wasn't in the pair set. Shared by the admin HTTP handler
// (Linux-only) and the mochi.replication.pair_remove Starlark API
// (cross-platform), so it lives here rather than in admin_replication.go.
func replication_pair_remove(peer string) (string, []string, bool) {
	rdb := db_open("db/replication.db")
	exists, _ := rdb.exists("select 1 from pair where peer=?", peer)
	if !exists {
		return "", nil, false
	}

	rdb.exec("delete from pair where peer=?", peer)

	var remaining []string
	if rows, err := rdb.rows("select peer from pair"); err == nil {
		for _, r := range rows {
			if p, ok := r["peer"].(string); ok && p != "" {
				remaining = append(remaining, p)
			}
		}
	}

	full := append([]string{p2p_id}, remaining...)
	if len(remaining) > 0 {
		admin_replication_emit_pair_membership(full, remaining)
	}
	audit_replication_pair_removed(peer)

	return peer, remaining, true
}
