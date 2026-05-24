// Mochi server: Broadcast log + acknowledged tests
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"testing"
)

// setup_broadcast_log_test gives a temp dir + DB scoped to the test.
func setup_broadcast_log_test(t *testing.T) (*DB, func()) {
	t.Helper()
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_log")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	orig := data_dir
	data_dir = tmp_dir
	db := db_open("db/test.db")
	return db, func() {
		data_dir = orig
		os.RemoveAll(tmp_dir)
	}
}

// TestBroadcastLogAppend — log_append allocates sequences monotonically
// for (key, peer) and writes one row per call.
func TestBroadcastLogAppend(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	s1 := broadcast_log_append(db, "k1", "peerA", "event/a", []byte(`{"x":1}`))
	s2 := broadcast_log_append(db, "k1", "peerA", "event/b", []byte(`{"x":2}`))
	if s1 != 1 || s2 != 2 {
		t.Errorf("sequences: got %d, %d; want 1, 2", s1, s2)
	}

	count := db.integer("select count(*) from _log where key='k1' and peer='peerA'")
	if count != 2 {
		t.Errorf("log rows: got %d, want 2", count)
	}

	row, _ := db.row("select event, data from _log where key='k1' and peer='peerA' and sequence=2")
	if e, _ := row["event"].(string); e != "event/b" {
		t.Errorf("event at seq 2: got %q, want event/b", e)
	}
	if d, _ := row["data"].(string); d != `{"x":2}` {
		t.Errorf("data at seq 2: got %q", d)
	}
}

// TestBroadcastLogPerPeerSequence — under multi-host, host A and host B
// each write to their own (key, peer) slot. No PK collision.
func TestBroadcastLogPerPeerSequence(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	if s := broadcast_log_append(db, "k1", "peerA", "e", []byte(`{}`)); s != 1 {
		t.Errorf("peerA first: got %d", s)
	}
	if s := broadcast_log_append(db, "k1", "peerB", "e", []byte(`{}`)); s != 1 {
		t.Errorf("peerB first (independent of peerA): got %d", s)
	}
	if s := broadcast_log_append(db, "k1", "peerA", "e", []byte(`{}`)); s != 2 {
		t.Errorf("peerA second: got %d", s)
	}

	// Both peers' rows coexist under the same key.
	count := db.integer("select count(*) from _log where key='k1'")
	if count != 3 {
		t.Errorf("total log rows for k1: got %d, want 3", count)
	}
}

// TestBroadcastLogAgeTrim — rows older than broadcast_log_age are dropped.
func TestBroadcastLogAgeTrim(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	broadcast_log_table_create(db)

	// Insert two rows: one old, one fresh. broadcast_log_append uses
	// now() so go behind its back for the old one.
	db.exec("insert into _log (key, peer, sequence, event, data, created) values ('k', 'p', 1, 'e', '', ?)", now()-broadcast_log_age-100)
	db.exec("insert into _log (key, peer, sequence, event, data, created) values ('k', 'p', 2, 'e', '', ?)", now())

	broadcast_log_age_trim(db, "k", "p")
	count := db.integer("select count(*) from _log where key='k' and peer='p'")
	if count != 1 {
		t.Errorf("after age trim: got %d rows, want 1 (fresh row only)", count)
	}

	// The remaining row should be the fresh one (sequence 2).
	row, _ := db.row("select sequence from _log where key='k' and peer='p'")
	if s, _ := row["sequence"].(int64); s != 2 {
		t.Errorf("remaining row sequence: got %d, want 2", s)
	}
}

// TestBroadcastLogAckTrim — after subscribers acknowledge, rows below
// the min ack across all subscribers are dropped.
func TestBroadcastLogAckTrim(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	for i := int64(1); i <= 10; i++ {
		broadcast_log_append(db, "k", "p", "e", []byte(`{}`))
		_ = i
	}

	broadcast_acknowledged_table_create(db)
	// Two subscribers; one has acked through 7, the other through 5.
	db.exec("insert into _acknowledged (key, peer, subscriber, last) values ('k', 'p', 'subA', 7)")
	db.exec("insert into _acknowledged (key, peer, subscriber, last) values ('k', 'p', 'subB', 5)")

	broadcast_log_ack_trim(db, "k", "p")
	// min(7, 5) = 5 → keep rows >= 5, drop 1..4.
	count := db.integer("select count(*) from _log where key='k' and peer='p'")
	if count != 6 {
		t.Errorf("after ack trim: got %d, want 6 (sequences 5..10)", count)
	}

	low := db.integer("select min(sequence) from _log where key='k' and peer='p'")
	if low != 5 {
		t.Errorf("min remaining sequence: got %d, want 5", low)
	}
}

// TestBroadcastLogAckTrimNoSubscribers — ack trim is a no-op when no
// _acknowledged rows exist (avoid wiping the log just because nobody
// has acked yet).
func TestBroadcastLogAckTrimNoSubscribers(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		broadcast_log_append(db, "k", "p", "e", []byte(`{}`))
	}
	broadcast_acknowledged_table_create(db)

	broadcast_log_ack_trim(db, "k", "p")
	count := db.integer("select count(*) from _log where key='k' and peer='p'")
	if count != 3 {
		t.Errorf("no-subscribers trim must be a no-op: got %d, want 3", count)
	}
}

// TestBroadcastReplayQuery — the underlying replay query returns rows
// in sequence order, filtered by (key, peer) and after.
func TestBroadcastReplayQuery(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	for i := 0; i < 5; i++ {
		broadcast_log_append(db, "k", "peerA", "e", []byte(`{}`))
	}
	// Different peer should not pollute results.
	for i := 0; i < 3; i++ {
		broadcast_log_append(db, "k", "peerB", "e", []byte(`{}`))
	}

	rows, _ := db.rows("select sequence from _log where key=? and peer=? and sequence > ? order by sequence limit ?", "k", "peerA", int64(2), 100)
	if len(rows) != 3 {
		t.Fatalf("replay rows after seq 2 from peerA: got %d, want 3", len(rows))
	}
	want := []int64{3, 4, 5}
	for i, r := range rows {
		s, _ := r["sequence"].(int64)
		if s != want[i] {
			t.Errorf("row[%d].sequence: got %d, want %d", i, s, want[i])
		}
	}
}

// TestBroadcastResyncThrottle — same (user, peer, key) within 60s
// blocks; different tags pass through.
func TestBroadcastResyncThrottle(t *testing.T) {
	// Reset the global cache between subtests.
	broadcast_resync_lock.Lock()
	broadcast_resync_last = map[string]int64{}
	broadcast_resync_lock.Unlock()

	if !broadcast_resync_throttle("u1", "peerA", "k") {
		t.Errorf("first call should pass")
	}
	if broadcast_resync_throttle("u1", "peerA", "k") {
		t.Errorf("immediate second call should be throttled")
	}
	if !broadcast_resync_throttle("u1", "peerB", "k") {
		t.Errorf("different peer should pass")
	}
	if !broadcast_resync_throttle("u2", "peerA", "k") {
		t.Errorf("different user should pass")
	}
	if !broadcast_resync_throttle("u1", "peerA", "k2") {
		t.Errorf("different key should pass")
	}
}

// TestBroadcastResyncJitter — values stay within [0, maximum), spread
// across the interval across many calls (not a stuck constant).
func TestBroadcastResyncJitter(t *testing.T) {
	const samples = 1000
	seen := make(map[int64]int)
	for i := 0; i < samples; i++ {
		jitter := broadcast_resync_jitter()
		if jitter < 0 {
			t.Errorf("jitter must be non-negative, got %v", jitter)
		}
		if jitter >= broadcast_resync_jitter_maximum {
			t.Errorf("jitter must be < %v, got %v", broadcast_resync_jitter_maximum, jitter)
		}
		// Bucket by 500ms slice so we don't expect every nanosecond.
		seen[int64(jitter/(500*1_000_000))]++
	}
	// With 1000 samples and 5s/500ms = 10 buckets, every bucket
	// should see at least a few hits. Guard against the rand path
	// returning a stuck constant.
	if len(seen) < 5 {
		t.Errorf("jitter not spread across the interval: only %d distinct 500ms buckets across %d samples (%v)",
			len(seen), samples, seen)
	}
}
