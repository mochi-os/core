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
// repeat from the same replica overwrites the prior row). Refuses if
// the source peer is already an active pair member — no point pending
// a join for someone already in the set.
func replication_join_request_apply(originPeer string, jr *JoinRequest) {
	if originPeer == "" {
		info("Replication join-request dropping: empty peer")
		return
	}

	rdb := db_open("db/replication.db")
	if exists, _ := rdb.exists("select 1 from pair where peer=?", originPeer); exists {
		info("Replication join-request dropping: peer %q already a pair member", originPeer)
		return
	}

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
// to exclude self).
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
}

// replication_join_denied_event is the replica's receive handler for a
// denial. Idempotent local cleanup — no state to remove on the replica
// side beyond the pending mochictl process which exits via its own
// polling.
func replication_join_denied_event(e *Event) {
	var jd JoinDenied
	if !e.segment(&jd) {
		info("Replication join-denied dropping: cannot decode payload")
		return
	}
	debug("Replication join-denied received: reason=%q (from peer %q)", jd.Reason, e.peer)
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
	m := message("", "", "replication", "join-request")
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
	m := message("", "", "replication", "join-approved")
	m.add(&JoinApproved{Members: members})
	m.send_peer(replicaPeer)
}

// replication_emit_join_denied sends a denial to the replica with an
// informational reason.
func replication_emit_join_denied(replicaPeer, reason string) {
	if replicaPeer == "" {
		return
	}
	m := message("", "", "replication", "join-denied")
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
		m := message("", "", "replication", "pair-membership-change")
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
	}
	return status
}
