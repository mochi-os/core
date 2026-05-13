// Mochi server: per-user link-request protocol unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// TestReplicationLinkRequestApplyStoresRow: a valid link-request from a
// new peer for a known user writes a row to `replication.db.links` with
// the expected fields and a 1h expiry from receipt.
func TestReplicationLinkRequestApplyStoresRow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "alice",
		Label:       "b.example.net",
		Placeholder: "ph-1",
	})

	rdb := db_open("db/replication.db")
	row, _ := rdb.row("select user, peer, label, placeholder, expires, received from links where user='u-alice' and peer='peer-B'")
	if row == nil {
		t.Fatal("expected one row in links after link-request apply")
	}
	if got, _ := row["label"].(string); got != "b.example.net" {
		t.Errorf("label = %q, want %q", got, "b.example.net")
	}
	if got, _ := row["placeholder"].(string); got != "ph-1" {
		t.Errorf("placeholder = %q, want %q", got, "ph-1")
	}
	received, _ := row["received"].(int64)
	expires, _ := row["expires"].(int64)
	if expires-received != 3600 {
		t.Errorf("expires - received = %d, want 3600 (1h)", expires-received)
	}
}

// TestReplicationLinkRequestApplyReplacesOnSecond: INSERT OR REPLACE
// keyed on (user, peer) — a second request from the same peer for the
// same user overwrites the first, redirecting eventual Approve at the
// fresh placeholder.
func TestReplicationLinkRequestApplyReplacesOnSecond(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "alice",
		Label:       "b.example.net",
		Placeholder: "ph-old",
	})
	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "alice",
		Label:       "b.example.net",
		Placeholder: "ph-new",
	})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select placeholder from links where user='u-alice' and peer='peer-B'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after second link-request, got %d", len(rows))
	}
	if ph, _ := rows[0]["placeholder"].(string); ph != "ph-new" {
		t.Errorf("placeholder after replace = %q, want %q", ph, "ph-new")
	}
}

// TestReplicationLinkRequestApplyDifferentPeersDistinct: two source peers
// can each have a pending request against the same target user — the
// dedup is per (user, peer), not per user.
func TestReplicationLinkRequestApplyDifferentPeersDistinct(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	replication_link_request_apply("peer-B", &LinkRequest{TargetUser: "alice", Placeholder: "ph-B"})
	replication_link_request_apply("peer-C", &LinkRequest{TargetUser: "alice", Placeholder: "ph-C"})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select peer from links where user='u-alice' order by peer")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for alice from different peers, got %d", len(rows))
	}
}

// TestReplicationLinkRequestApplyRefusesReplicationToSelf: if the source
// peer is already in the target user's hosts set (per-user opt-in or
// whole-server pair already covers it), the link-request is silently
// refused — no row created.
func TestReplicationLinkRequestApplyRefusesReplicationToSelf(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	rdb := db_open("db/replication.db")
	rdb.exec("insert into hosts (user, peer, added) values ('u-alice', 'peer-B', 0)")

	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "alice",
		Placeholder: "ph-1",
	})

	exists, _ := rdb.exists("select 1 from links where user='u-alice' and peer='peer-B'")
	if exists {
		t.Error("link-request from already-hosting peer should be refused (no row written)")
	}
}

// TestReplicationLinkRequestApplyRefusesUnknownUser: targeting a username
// that doesn't exist on this server silently drops — there's no row to
// store the request against.
func TestReplicationLinkRequestApplyRefusesUnknownUser(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	replication_link_request_apply("peer-B", &LinkRequest{
		TargetUser:  "nobody",
		Placeholder: "ph-1",
	})

	rdb := db_open("db/replication.db")
	rows, _ := rdb.rows("select 1 from links")
	if len(rows) != 0 {
		t.Errorf("expected no rows for unknown user, got %d", len(rows))
	}
}

// TestReplicationLinkRequestApplyRejectsMissingFields: empty user,
// placeholder, or peer all silently no-op.
func TestReplicationLinkRequestApplyRejectsMissingFields(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	cases := []struct {
		name string
		peer string
		lr   LinkRequest
	}{
		{"empty user", "peer-B", LinkRequest{Placeholder: "ph"}},
		{"empty placeholder", "peer-B", LinkRequest{TargetUser: "alice"}},
		{"empty peer", "", LinkRequest{TargetUser: "alice", Placeholder: "ph"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			replication_link_request_apply(c.peer, &c.lr)
			rdb := db_open("db/replication.db")
			rows, _ := rdb.rows("select 1 from links")
			if len(rows) != 0 {
				t.Errorf("expected 0 rows after %s, got %d", c.name, len(rows))
			}
		})
	}
}

// TestReplicationLinkApproveIdempotent: the DELETE-as-lock pattern in
// replication_link_approve makes a concurrent second call return
// "already-approved" without re-firing the freshness probe or transfer.
// We simulate the race by calling approve twice on a row that no longer
// exists after the first call.
func TestReplicationLinkApproveIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	rdb := db_open("db/replication.db")
	rdb.exec(
		"insert into links (user, peer, label, placeholder, received, expires) values ('u-alice', 'peer-B', 'b', 'ph-1', 0, 9999999999)")

	// Simulate the second tab's call to approve after the first already
	// deleted the row. The second call should return "already-approved"
	// rather than error.
	rdb.exec("delete from links where user='u-alice' and peer='peer-B'")

	status, err := replication_link_approve("u-alice", "peer-B")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != "already-approved" {
		t.Errorf("status = %q, want %q", status, "already-approved")
	}
}

// TestReplicationLinkDenyIdempotent: same DELETE-as-lock pattern. After
// the row is already gone, deny returns "already-handled".
func TestReplicationLinkDenyIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")

	// Row never existed → already-handled.
	if status := replication_link_deny("u-alice", "peer-B"); status != "already-handled" {
		t.Errorf("deny with no row = %q, want %q", status, "already-handled")
	}
}

// TestReplicationLinkDeniedApplyCleansPlaceholder: receiving a denied op
// for a placeholder in 'pending-replication' status wipes the users row
// and any entity rows that might have been scaffolded.
func TestReplicationLinkDeniedApplyCleansPlaceholder(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('ph-1', 'placeholder-name', 'pending-replication')")

	replication_link_denied_apply("peer-A", &LinkDenied{Placeholder: "ph-1", Reason: "denied"})

	exists, _ := udb.exists("select 1 from users where uid='ph-1'")
	if exists {
		t.Error("denied placeholder row should be deleted")
	}
}

// TestReplicationLinkDeniedApplyLeavesActiveAccountAlone: an active
// (post-Approve) user row must NOT be touched by a stray late-arriving
// link-denied op.
func TestReplicationLinkDeniedApplyLeavesActiveAccountAlone(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice', 'active')")

	replication_link_denied_apply("peer-A", &LinkDenied{Placeholder: "u-alice"})

	exists, _ := udb.exists("select 1 from users where uid='u-alice'")
	if !exists {
		t.Error("active user must not be deleted by a stray link-denied op")
	}
}
