// Mochi server: stream / RPC peer-failover ordering
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// add_directory_location_for_failover ensures the locations table
// exists and inserts a (entity, peer, seen) row. Used by failover
// tests to set up controlled directory state.
func add_directory_location_for_failover(t *testing.T, entity, peer string, seen int64) {
	t.Helper()
	db := db_open("db/directory.db")
	db.exec("create table if not exists locations (entity text not null, peer text not null, seen integer not null, primary key (entity, peer))")
	db.exec("insert or replace into locations (entity, peer, seen) values (?, ?, ?)", entity, peer, seen)
}

// TestEntityPeersFailoverActiveFirst — active peers (seen within the
// active window) come before stale ones, ordered oldest-seen first
// within the active set so the most stably-running replica gets the
// initial attempt.
func TestEntityPeersFailoverActiveFirst(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	entity := "uid-failover-test"
	now_ts := now()
	// Active set (within 2h):
	add_directory_location_for_failover(t, entity, "peer-recent", now_ts-60)        // 1 min ago
	add_directory_location_for_failover(t, entity, "peer-old-but-active", now_ts-3000) // 50 min ago
	add_directory_location_for_failover(t, entity, "peer-mid", now_ts-1800)         // 30 min ago
	// Stale (older than 2h but within 30d):
	add_directory_location_for_failover(t, entity, "peer-stale", now_ts-3*86400)    // 3 days ago

	got := entity_peers_failover(entity)
	want := []string{"peer-old-but-active", "peer-mid", "peer-recent", "peer-stale"}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d, want %d (got=%v)", len(got), len(want), got)
	}
	for i, p := range want {
		if got[i] != p {
			t.Errorf("got[%d]=%q, want %q (full: %v)", i, got[i], p, got)
		}
	}
}

// TestEntityPeersFailoverAllStale — when no peer is in the active
// window, stale peers are returned (most-recent first) as a last
// resort. Without this, an entity whose all locations missed a
// recent republish would be unreachable until directory cleanup.
func TestEntityPeersFailoverAllStale(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	entity := "uid-all-stale"
	now_ts := now()
	add_directory_location_for_failover(t, entity, "peer-A", now_ts-10*86400) // 10 days ago
	add_directory_location_for_failover(t, entity, "peer-B", now_ts-3*86400)  // 3 days ago — more recent
	add_directory_location_for_failover(t, entity, "peer-C", now_ts-20*86400) // 20 days ago

	got := entity_peers_failover(entity)
	want := []string{"peer-B", "peer-A", "peer-C"} // stale tier, most-recent first
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d, want %d (got=%v)", len(got), len(want), got)
	}
	for i, p := range want {
		if got[i] != p {
			t.Errorf("got[%d]=%q, want %q (full: %v)", i, got[i], p, got)
		}
	}
}

// TestEntityPeersFailoverAgedOut — peers older than 30 days are
// excluded entirely (they've aged past directory retention).
func TestEntityPeersFailoverAgedOut(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	entity := "uid-aged-out"
	now_ts := now()
	add_directory_location_for_failover(t, entity, "peer-fresh", now_ts-300)
	add_directory_location_for_failover(t, entity, "peer-ancient", now_ts-100*86400) // 100 days ago

	got := entity_peers_failover(entity)
	if len(got) != 1 || got[0] != "peer-fresh" {
		t.Errorf("aged-out peer must be excluded; got %v, want [peer-fresh]", got)
	}
}

// TestEntityPeersFailoverEmpty — no directory rows means an empty
// slice. Callers should fall back to broadcast/directory-request.
func TestEntityPeersFailoverEmpty(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	got := entity_peers_failover("uid-unknown")
	if len(got) != 0 {
		t.Errorf("unknown entity: want empty, got %v", got)
	}
}

// TestEntityPeersFailoverLocalShortCircuit — when the entity is local
// (in users.db.entities), short-circuit to [self] without hitting the
// directory.
func TestEntityPeersFailoverLocalShortCircuit(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-local', 'u@example.com')")
	udb.exec("insert into entities (id, private, fingerprint, user, class, name) values ('uid-local-entity', 'p', 'fp', 'u-local', 'feed', 'F')")

	got := entity_peers_failover("uid-local-entity")
	if len(got) != 1 || got[0] != net_id {
		t.Errorf("local entity: want [%s], got %v", net_id, got)
	}
}

// TestEntityPeersFailoverIncludesEveryPeer — explicit guarantee that
// callers tracking through the returned list reach every known
// (non-aged-out) peer, even when none are recently active. Closes the
// "stream fails when all replicas missed a republish" hole.
func TestEntityPeersFailoverIncludesEveryPeer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	entity := "uid-mixed-staleness"
	now_ts := now()
	// Mix of active, stale, and inactive-but-not-aged-out peers.
	add_directory_location_for_failover(t, entity, "fresh-A", now_ts-300)
	add_directory_location_for_failover(t, entity, "stale-B", now_ts-12*3600) // 12 h ago, well past 2h window
	add_directory_location_for_failover(t, entity, "stale-C", now_ts-5*86400) // 5 days ago

	got := entity_peers_failover(entity)
	present := map[string]bool{}
	for _, p := range got {
		present[p] = true
	}
	for _, want := range []string{"fresh-A", "stale-B", "stale-C"} {
		if !present[want] {
			t.Errorf("expected %q in failover list (was: %v)", want, got)
		}
	}
}
