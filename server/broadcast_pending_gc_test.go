// Mochi server: broadcast pending GC tests
// Copyright Alistair Cunningham 2026
//
// Coverage for the stuck-stream classifier (task #101). The full
// broadcast_pending_gc loop requires user/app registration globals;
// here we exercise the classifier directly because that's where the
// "is this gap unfillable" decision lives. The skip-and-drain step
// itself is just broadcast_advance_local, which has its own tests.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func setup_broadcast_pending_gc_test(t *testing.T) (string, func()) {
	t.Helper()
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_gc")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	orig := data_dir
	data_dir = tmp_dir
	return tmp_dir, func() {
		data_dir = orig
		os.RemoveAll(tmp_dir)
	}
}

// stage_stalled_stream creates a per-app DB at users/<uid>/<app>/app.db
// in the current data_dir, populates pending + received so the
// (peer, key) stream looks stuck: received.last < min(pending.seq) - 1.
// Returns the relative db path the GC walker would use.
func stage_stalled_stream(t *testing.T, uid, app, peer, key string, last, min_pending, count int64, oldest int64) string {
	t.Helper()
	rel := filepath.Join("users", uid, app, "app.db")
	abs := filepath.Join(data_dir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	db := db_open(rel)
	if db == nil {
		t.Fatal("db_open")
	}
	broadcast_pending_table_create(db)
	broadcast_received_table_create(db)
	db.exec("insert into received (sender, key, last) values (?, ?, ?)", peer, key, last)
	for i := int64(0); i < count; i++ {
		db.exec(`insert into pending
			(peer, key, sequence, source, target, service, event, content, received)
			values (?, ?, ?, '', '', '', '', ?, ?)`,
			peer, key, min_pending+i, []byte{1, 2}, oldest)
	}
	return rel
}

// TestBroadcastPendingStalledDBClassifiesGap confirms the per-DB
// classifier marks a stream stuck when min(pending.seq) > last+1.
// This is the load-bearing decision: every stream the GC operates on
// passes through this filter.
func TestBroadcastPendingStalledDBClassifiesGap(t *testing.T) {
	_, cleanup := setup_broadcast_pending_gc_test(t)
	defer cleanup()
	rel := stage_stalled_stream(t, "u1", "appA", "peer1", "key1",
		1555, 4255, 50, now()-100)
	got := broadcast_pending_stalled_db("u1", "appA", rel)
	if len(got) != 1 {
		t.Fatalf("expected 1 stalled stream, got %d", len(got))
	}
	s := got[0]
	if s.Peer != "peer1" || s.Key != "key1" {
		t.Errorf("wrong identity: peer=%q key=%q", s.Peer, s.Key)
	}
	if s.Last != 1555 {
		t.Errorf("last: got %d, want 1555", s.Last)
	}
	if s.MinPending != 4255 {
		t.Errorf("min_pending: got %d, want 4255", s.MinPending)
	}
	if s.Count != 50 {
		t.Errorf("count: got %d, want 50", s.Count)
	}
}

// TestBroadcastPendingStalledDBSkipsContiguous confirms a stream that
// would drain naturally (min(pending.seq) == last+1) is NOT flagged.
// Without this guard the GC would treat every transient out-of-order
// arrival as an unfillable gap and start skipping events that were
// about to apply on the next tick.
func TestBroadcastPendingStalledDBSkipsContiguous(t *testing.T) {
	_, cleanup := setup_broadcast_pending_gc_test(t)
	defer cleanup()
	rel := stage_stalled_stream(t, "u2", "appA", "peer1", "key1",
		1555, 1556, 50, now()-100)
	got := broadcast_pending_stalled_db("u2", "appA", rel)
	if len(got) != 0 {
		t.Errorf("expected 0 stalled streams (chain is contiguous), got %d", len(got))
	}
}

// TestBroadcastPendingStalledDBNoTables confirms an app DB without
// the broadcast tables returns empty - apps that don't use the
// subsystem must be silently skipped, not crash the walker.
func TestBroadcastPendingStalledDBNoTables(t *testing.T) {
	tmp_dir, cleanup := setup_broadcast_pending_gc_test(t)
	defer cleanup()
	rel := filepath.Join("users", "u3", "appB", "app.db")
	abs := filepath.Join(tmp_dir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	db := db_open(rel)
	if db == nil {
		t.Fatal("db_open")
	}
	// Don't create any broadcast tables.
	got := broadcast_pending_stalled_db("u3", "appB", rel)
	if len(got) != 0 {
		t.Errorf("expected 0 stalled streams from app without broadcast, got %d", len(got))
	}
}

// TestBroadcastPendingStalledWalkFindsMultipleApps confirms the
// per-user-app walk discovers streams across separate DBs. Replicates
// the production case where one user has multiple subscribed apps
// each with their own stuck (peer, key).
func TestBroadcastPendingStalledWalkFindsMultipleApps(t *testing.T) {
	_, cleanup := setup_broadcast_pending_gc_test(t)
	defer cleanup()
	stage_stalled_stream(t, "u1", "appA", "peer1", "key1", 100, 500, 10, now()-100)
	stage_stalled_stream(t, "u1", "appB", "peer2", "key2", 200, 600, 10, now()-100)
	stage_stalled_stream(t, "u2", "appA", "peer1", "key1", 300, 700, 10, now()-100)
	got := broadcast_pending_stalled()
	if len(got) != 3 {
		t.Errorf("expected 3 stalled streams across users/apps, got %d", len(got))
	}
}

// TestBroadcastAdvanceSkipsAndDrains confirms the actual unstick:
// after broadcast_advance_local jumps received.last past the gap, the
// chain-drain picks up the now-contiguous tail. This is the core of
// what broadcast_pending_gc does; testing it against a real DB
// without the user/app globals proves the SQL is right.
func TestBroadcastAdvanceSkipsAndDrains(t *testing.T) {
	_, cleanup := setup_broadcast_pending_gc_test(t)
	defer cleanup()

	// Stub the dispatcher so chain-drain has something to call. We
	// just record which sequences ran and report success.
	original := broadcast_pending_dispatch
	defer func() { broadcast_pending_dispatch = original }()
	var dispatched []int64
	broadcast_pending_dispatch = func(row *broadcast_pending_row, db *DB) bool {
		dispatched = append(dispatched, row.Sequence)
		return true
	}

	rel := stage_stalled_stream(t, "u1", "appA", "peer1", "key1",
		1555, 4255, 5, now()-100)
	db := db_open(rel)
	if db == nil {
		t.Fatal("db_open")
	}
	// Sanity check: chain at 1556 finds nothing (gap below pending).
	broadcast_pending_drain_chain(db, "peer1", "key1")
	if len(dispatched) != 0 {
		t.Fatalf("pre-skip drain should be empty (chain is below pending min); got %v", dispatched)
	}

	// Skip the gap. broadcast_advance_local advances received.last to
	// min(pending.seq)-1 = 4254, then drain_chain picks up seq=4255
	// and continues through the contiguous tail (5 rows: 4255..4259).
	broadcast_advance_local(db, "peer1", "key1", 4254)
	if len(dispatched) != 5 {
		t.Errorf("expected 5 dispatches after skip (chain seq 4255..4259), got %d: %v",
			len(dispatched), dispatched)
	}
	got_last := broadcast_received_get(db, "peer1", "key1")
	if got_last != 4259 {
		t.Errorf("received.last after drain: got %d, want 4259", got_last)
	}
}
