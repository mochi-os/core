// Mochi server: whole-server pair join-request protocol unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// TestReplicationJoinRequestApplyStoresRow: a valid join-request from a
// new peer writes a row to `replication.db.joins` with the expected
// fields and a 10-minute expiry from receipt.
func TestReplicationJoinRequestApplyStoresRow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	replication_join_request_apply("peer-B", &JoinRequest{Label: "b.example.net"})

	rdb := db_open("db/replication.db")
	row, _ := rdb.row("select peer, label, received, expires from joins where peer='peer-B'")
	if row == nil {
		t.Fatal("expected joins row after join-request apply")
	}
	if got, _ := row["label"].(string); got != "b.example.net" {
		t.Errorf("label = %q, want %q", got, "b.example.net")
	}
	received, _ := row["received"].(int64)
	expires, _ := row["expires"].(int64)
	if expires-received != 600 {
		t.Errorf("expires - received = %d, want 600 (10m)", expires-received)
	}
}

// TestReplicationJoinRequestApplyReplacesOnSecond: a second request from
// the same replica overwrites the first row (INSERT OR REPLACE).
func TestReplicationJoinRequestApplyReplacesOnSecond(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	replication_join_request_apply("peer-B", &JoinRequest{Label: "old"})
	replication_join_request_apply("peer-B", &JoinRequest{Label: "new"})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select label from joins where peer='peer-B'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after second join-request, got %d", len(rows))
	}
	if l, _ := rows[0]["label"].(string); l != "new" {
		t.Errorf("label after replace = %q, want %q", l, "new")
	}
}

// TestReplicationJoinRequestApplyAcceptsExistingMember: a join-request
// from a peer that's already in the pair set is accepted (recovery
// flow for a replica that lost its disk and re-installed with the
// same p2p id). The admin's Approve action handles the re-pair through
// the existing code path (pair INSERT OR REPLACE, fresh join-approved,
// pair-backfill).
func TestReplicationJoinRequestApplyAcceptsExistingMember(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")

	replication_join_request_apply("peer-B", &JoinRequest{Label: "recovery"})

	row, _ := rdb.row("select label from joins where peer='peer-B'")
	if row == nil {
		t.Fatal("join-request from existing pair member should be stored (recovery path)")
	}
	if got := row["label"].(string); got != "recovery" {
		t.Errorf("joins row label = %q, want %q", got, "recovery")
	}
}

// TestReplicationJoinRequestApplyRejectsEmptyPeer: empty peer silently
// no-ops.
func TestReplicationJoinRequestApplyRejectsEmptyPeer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	replication_join_request_apply("", &JoinRequest{Label: "x"})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select 1 from joins")
	if len(rows) != 0 {
		t.Errorf("expected 0 rows after empty-peer join-request, got %d", len(rows))
	}
}

// TestReplicationJoinApprovedApplyReplacesPair: receiving a join-approved
// replaces the local pair table with the announced members (self-filtered).
func TestReplicationJoinApprovedApplyReplacesPair(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('old', 0, '')")

	replication_join_approved_apply("peer-A", &JoinApproved{
		Members: []string{"peer-A", "peer-C", "self"},
	})

	// p2p_id is "self" in setup_replication_test — must be filtered.
	rows, _ := rdb.rows("select peer from pair order by peer")
	if len(rows) != 2 {
		t.Fatalf("expected 2 pair rows after join-approved, got %d", len(rows))
	}
	if peers := []string{rows[0]["peer"].(string), rows[1]["peer"].(string)}; peers[0] != "peer-A" || peers[1] != "peer-C" {
		t.Errorf("pair set = %v, want [peer-A peer-C]", peers)
	}
}

// TestReplicationPairMembershipApplyFresh: a pair-membership-change op
// with a newer sequence than anything seen replaces the local pair
// table. The receiver must be in the announced Members set; otherwise
// the op is treated as "you've been kicked" (see ApplyKickedReceiver
// test below).
func TestReplicationPairMembershipApplyFresh(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('stale', 0, '')")

	// p2p_id is "self" in setup_replication_test — must be in Members.
	replication_pair_membership_apply("peer-A", &PairMembershipChange{
		Members:  []string{"peer-A", "peer-B", "self"},
		Sequence: 1,
	})

	rows, _ := rdb.rows("select peer from pair order by peer")
	if len(rows) != 2 {
		t.Fatalf("expected 2 pair rows, got %d", len(rows))
	}
	if rows[0]["peer"].(string) != "peer-A" || rows[1]["peer"].(string) != "peer-B" {
		t.Errorf("pair after apply has wrong members: %v", rows)
	}
}

// TestReplicationPairMembershipApplyKickedReceiver: a membership-change
// whose Members list does NOT include the receiver is interpreted as
// "I've been removed from the pair set" — the receiver clears its pair
// table entirely. This closes the N=2 unpair loop: the kicked peer
// learns it was removed even though there are no remaining members
// left to forward the change.
func TestReplicationPairMembershipApplyKickedReceiver(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")

	// Members announces a set that does NOT include "self" — peer-A
	// is telling us we've been removed.
	replication_pair_membership_apply("peer-A", &PairMembershipChange{
		Members:  []string{"peer-A"},
		Sequence: 1,
	})

	count := rdb.integer("select count(*) from pair")
	if count != 0 {
		t.Errorf("kicked receiver should have empty pair; got %d rows", count)
	}
}

// TestReplicationPairMembershipApplyStaleIgnored: a pair-membership-change
// with sequence less than what we've already seen is recorded as seen
// but does not overwrite the pair table.
func TestReplicationPairMembershipApplyStaleIgnored(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Apply newer first (receiver "self" is in Members so this is the
	// normal "I'm in the pair" path, not a kick).
	replication_pair_membership_apply("peer-A", &PairMembershipChange{
		Members:  []string{"peer-A", "peer-B", "self"},
		Sequence: 5,
	})
	// Now stale older.
	replication_pair_membership_apply("peer-A", &PairMembershipChange{
		Members:  []string{"peer-A", "self"},
		Sequence: 3,
	})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select peer from pair order by peer")
	if len(rows) != 2 {
		t.Errorf("stale apply should not have shrunk pair set; got %d rows", len(rows))
	}
}

// TestReplicationPairMembershipApplyDuplicateIgnored: re-applying the
// same sequence is a no-op.
func TestReplicationPairMembershipApplyDuplicateIgnored(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	replication_pair_membership_apply("peer-A", &PairMembershipChange{
		Members:  []string{"peer-A", "peer-B", "self"},
		Sequence: 1,
	})
	rdb := db_open("db/replication.db")
	rdb.exec("delete from pair where peer='peer-A'")

	// Re-apply same sequence: should not re-insert peer-A.
	replication_pair_membership_apply("peer-A", &PairMembershipChange{
		Members:  []string{"peer-A", "peer-B", "self"},
		Sequence: 1,
	})

	exists, _ := rdb.exists("select 1 from pair where peer='peer-A'")
	if exists {
		t.Error("duplicate sequence should not re-apply the membership change")
	}
}

// TestReplicationJoinApproveCoreIdempotent: a concurrent second approve
// finds the joins row already gone and returns "already-approved" with
// no re-firing of side effects. Tests the DB-only core to avoid the
// emit goroutines that would otherwise outlive the test.
func TestReplicationJoinApproveCoreIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	status, _, _, err := replication_join_approve_core("peer-B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "already-approved" {
		t.Errorf("status = %q, want %q", status, "already-approved")
	}
}

// TestReplicationJoinApproveCoreAddsToPair: a valid approve writes the
// new member into the pair table and returns the full member set + the
// existing-members subset that should be notified.
func TestReplicationJoinApproveCoreAddsToPair(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-B', 'b', 0, 9999999999)")
	rdb.exec("insert into pair (peer, added, role) values ('peer-C', 0, '')") // existing member

	status, full, existing, err := replication_join_approve_core("peer-B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "approved" {
		t.Errorf("status = %q, want %q", status, "approved")
	}
	exists, _ := rdb.exists("select 1 from pair where peer='peer-B'")
	if !exists {
		t.Error("approved peer should be in pair table")
	}
	exists, _ = rdb.exists("select 1 from joins where peer='peer-B'")
	if exists {
		t.Error("joins row should be deleted after approve")
	}
	// full set = [self, peer-B, peer-C] (some order)
	want_full := map[string]bool{"self": true, "peer-B": true, "peer-C": true}
	if len(full) != 3 {
		t.Errorf("full = %v, want 3 members", full)
	}
	for _, m := range full {
		if !want_full[m] {
			t.Errorf("full has unexpected member %q", m)
		}
	}
	// existing = [peer-C] (peer-B was the joiner; self is the source not in `existing`)
	if len(existing) != 1 || existing[0] != "peer-C" {
		t.Errorf("existing = %v, want [peer-C]", existing)
	}
}

// TestReplicationJoinApproveCoreClearsSeenForJoiner: approving a fresh
// join clears any stale `seen` rows for the joining peer so its
// post-reinstall ops at low sequence numbers aren't silently dropped
// as duplicates. This is the bug we hit when mochi2 was wiped and
// re-paired: its libp2p host key survived the wipe (per-server, not
// per-user), so its peer ID stayed the same, but its sequence
// counters restarted from 0 and collided with mochi1's historical
// seen rows.
func TestReplicationJoinApproveCoreClearsSeenForJoiner(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-B', 'b', 0, 9999999999)")
	// Plant stale seen rows from a previous incarnation of peer-B.
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-B', 'app', 'u1', 5000, 0)")
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-B', 'app', 'u2', 12345, 0)")
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-B', 'system', '', 7, 0)")
	// Plant unrelated rows that must survive (different peer).
	rdb.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-X', 'app', 'u1', 99, 0)")

	if _, _, _, err := replication_join_approve_core("peer-B"); err != nil {
		t.Fatalf("approve: %v", err)
	}

	if n := rdb.integer("select count(*) from seen where peer='peer-B'"); n != 0 {
		t.Errorf("seen rows for joining peer not cleared: got %d, want 0", n)
	}
	if n := rdb.integer("select count(*) from seen where peer='peer-X'"); n != 1 {
		t.Errorf("seen rows for unrelated peer must survive: got %d, want 1", n)
	}
}

// TestReplicationPairMembershipApplyClearsSeenForNewMembers: when an
// existing member receives a pair-membership-change announcement, it
// also clears `seen` for any peer that's newly in the announced set
// (i.e. was just added to the pair). Peers already in the local pair
// are unaffected, and a member-leave doesn't clear anything (the
// leaver's seen rows stay so subsequent re-pair goes through approve).
func TestReplicationPairMembershipApplyClearsSeenForNewMembers(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	// Existing pair from this receiver's perspective: peer-C is already paired.
	db.exec("insert into pair (peer, added, role) values ('peer-C', 0, '')")
	// Stale seen rows: peer-B (newly joining) has stale entries that
	// should be cleared; peer-C (already paired) and peer-X (unrelated)
	// must survive.
	db.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-B', 'app', 'u1', 9999, 0)")
	db.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-C', 'app', 'u1', 50, 0)")
	db.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-X', 'app', 'u1', 1, 0)")

	pmc := &PairMembershipChange{
		Members:  []string{p2p_id, "peer-B", "peer-C"}, // includes self + the new joiner
		Sequence: 1,
	}
	replication_pair_membership_apply("peer-Z" /* origin = some other member */, pmc)

	if n := db.integer("select count(*) from seen where peer='peer-B'"); n != 0 {
		t.Errorf("seen for newly-joined peer-B not cleared: got %d, want 0", n)
	}
	if n := db.integer("select count(*) from seen where peer='peer-C'"); n != 1 {
		t.Errorf("seen for already-paired peer-C must survive: got %d, want 1", n)
	}
	if n := db.integer("select count(*) from seen where peer='peer-X'"); n != 1 {
		t.Errorf("seen for unrelated peer-X must survive: got %d, want 1", n)
	}
}

// TestReplicationPairMembershipApplyLeaveDoesNotClearSeen: when a peer
// leaves the pair (drops out of the announced set), its `seen` rows
// stay — a re-pair later goes through the approve path which clears
// them. Without this rule, every member shrink would wipe history we
// might still need.
func TestReplicationPairMembershipApplyLeaveDoesNotClearSeen(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")
	db.exec("insert into pair (peer, added, role) values ('peer-C', 0, '')")
	db.exec("insert into seen (peer, scope, user, sequence, applied) values ('peer-B', 'app', 'u1', 50, 0)")

	// Announce a new membership without peer-B (peer-B left).
	pmc := &PairMembershipChange{
		Members:  []string{p2p_id, "peer-C"},
		Sequence: 1,
	}
	replication_pair_membership_apply("peer-C", pmc)

	if n := db.integer("select count(*) from seen where peer='peer-B'"); n != 1 {
		t.Errorf("seen for leaving peer must survive: got %d, want 1", n)
	}
}

// TestReplicationJoinDenyCoreIdempotent: deny with no row returns
// "already-handled".
func TestReplicationJoinDenyCoreIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	if status := replication_join_deny_core("peer-B"); status != "already-handled" {
		t.Errorf("deny with no row = %q, want %q", status, "already-handled")
	}
}

// TestReplicationJoinDenyCoreDeletesRow: a valid deny deletes the row
// and returns "denied".
func TestReplicationJoinDenyCoreDeletesRow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-B', 'b', 0, 9999999999)")

	if status := replication_join_deny_core("peer-B"); status != "denied" {
		t.Errorf("status = %q, want %q", status, "denied")
	}
	exists, _ := rdb.exists("select 1 from joins where peer='peer-B'")
	if exists {
		t.Error("joins row should be deleted after deny")
	}
}
