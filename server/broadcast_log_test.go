// Mochi server: Broadcast log + acknowledged tests
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"sync"
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

	count := db.integer("select count(*) from log where key='k1' and peer='peerA'")
	if count != 2 {
		t.Errorf("log rows: got %d, want 2", count)
	}

	row, _ := db.row("select event, data from log where key='k1' and peer='peerA' and sequence=2")
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
	if s := broadcast_log_append(db, "k1", "peer_b", "e", []byte(`{}`)); s != 1 {
		t.Errorf("peer_b first (independent of peerA): got %d", s)
	}
	if s := broadcast_log_append(db, "k1", "peerA", "e", []byte(`{}`)); s != 2 {
		t.Errorf("peerA second: got %d", s)
	}

	// Both peers' rows coexist under the same key.
	count := db.integer("select count(*) from log where key='k1'")
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
	db.exec("insert into log (key, peer, sequence, event, data, created) values ('k', 'p', 1, 'e', '', ?)", now()-broadcast_log_age-100)
	db.exec("insert into log (key, peer, sequence, event, data, created) values ('k', 'p', 2, 'e', '', ?)", now())

	broadcast_log_age_trim(db, "k", "p")
	count := db.integer("select count(*) from log where key='k' and peer='p'")
	if count != 1 {
		t.Errorf("after age trim: got %d rows, want 1 (fresh row only)", count)
	}

	// The remaining row should be the fresh one (sequence 2).
	row, _ := db.row("select sequence from log where key='k' and peer='p'")
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
	db.exec("insert into acknowledged (key, peer, subscriber, last) values ('k', 'p', 'subA', 7)")
	db.exec("insert into acknowledged (key, peer, subscriber, last) values ('k', 'p', 'subB', 5)")

	broadcast_log_ack_trim(db, "k", "p")
	// min(7, 5) = 5 → keep rows >= 5, drop 1..4.
	count := db.integer("select count(*) from log where key='k' and peer='p'")
	if count != 6 {
		t.Errorf("after ack trim: got %d, want 6 (sequences 5..10)", count)
	}

	low := db.integer("select min(sequence) from log where key='k' and peer='p'")
	if low != 5 {
		t.Errorf("min remaining sequence: got %d, want 5", low)
	}
}

// TestBroadcastLogAckTrimNoSubscribers — ack trim is a no-op when no
// acknowledged rows exist (avoid wiping the log just because nobody
// has acked yet).
func TestBroadcastLogAckTrimNoSubscribers(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	for i := 0; i < 3; i++ {
		broadcast_log_append(db, "k", "p", "e", []byte(`{}`))
	}
	broadcast_acknowledged_table_create(db)

	broadcast_log_ack_trim(db, "k", "p")
	count := db.integer("select count(*) from log where key='k' and peer='p'")
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
		broadcast_log_append(db, "k", "peer_b", "e", []byte(`{}`))
	}

	rows, _ := db.rows("select sequence from log where key=? and peer=? and sequence > ? order by sequence limit ?", "k", "peerA", int64(2), 100)
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

// TestBroadcastNextLocalConcurrentNoDuplicates is the regression
// test for the race surfaced on wasabi 2026-05-24..26 (468
// event_ai_tag panics: "UNIQUE constraint failed: log.key, log.peer,
// log.sequence"). The previous UPSERT-then-SELECT pair let two
// goroutines both read the higher of two interleaved updates and
// emit the same sequence number. Fix uses RETURNING so each call
// sees its own atomic post-update value. Test fires N goroutines,
// collects every returned sequence, asserts:
//   - all sequences are unique (no duplicates -> no log UNIQUE
//     violation)
//   - the set of sequences is exactly {1..N}
//   - the final sequence.last equals N
func TestBroadcastNextLocalConcurrentNoDuplicates(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	const N = 200
	results := make([]int64, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = broadcast_next_local(db, "k", "p")
		}(i)
	}
	wg.Wait()

	seen := map[int64]int{}
	for _, s := range results {
		seen[s]++
	}
	for seq, count := range seen {
		if count > 1 {
			t.Errorf("sequence %d emitted %d times (race regression)", seq, count)
		}
	}
	if len(seen) != N {
		t.Errorf("got %d distinct sequences, want %d (some lost to race)", len(seen), N)
	}
	for seq := int64(1); seq <= int64(N); seq++ {
		if seen[seq] == 0 {
			t.Errorf("sequence %d missing from allocation set", seq)
			break
		}
	}
	if last := db.integer("select last from sequence where key='k' and peer='p'"); last != N {
		t.Errorf("final sequence.last = %d, want %d", last, N)
	}
}

// TestBroadcastResyncThrottle — same (user, peer, key) with a
// resync already in flight blocks; different tags pass through.
// Updated for task #81: the gate is now per-in-flight rather than
// time-based. Independent-tags coverage is duplicated in
// broadcast_test.go's TestBroadcastResyncThrottleIndependentTags but
// kept here too so the legacy test name still surfaces in a grep.
func TestBroadcastResyncThrottle(t *testing.T) {
	// Reset the global cache between subtests.
	broadcast_resync_lock.Lock()
	broadcast_resync_inflight = map[string]int64{}
	broadcast_resync_lock.Unlock()

	if !broadcast_resync_throttle("u1", "peerA", "k") {
		t.Errorf("first call should pass")
	}
	if broadcast_resync_throttle("u1", "peerA", "k") {
		t.Errorf("immediate second call should be blocked while previous is in flight")
	}
	if !broadcast_resync_throttle("u1", "peer_b", "k") {
		t.Errorf("different peer should pass")
	}
	if !broadcast_resync_throttle("u2", "peerA", "k") {
		t.Errorf("different user should pass")
	}
	if !broadcast_resync_throttle("u1", "peerA", "k2") {
		t.Errorf("different key should pass")
	}
}

// broadcast_acknowledge_reset_for_test clears the pending-ack map so any
// in-flight coalesce-window AfterFunc timers scheduled by a test find no
// entry and return at broadcast_acknowledge_flush's nil-check, before the
// user_by_uid -> db_open call that reads the data_dir global. Registered via
// t.Cleanup by tests that enqueue acks, so a leaked 250 ms timer can't race
// a later test's data_dir reset.
func broadcast_acknowledge_reset_for_test() {
	broadcast_acknowledge_lock.Lock()
	broadcast_acknowledge_pending_map = map[string]*broadcast_acknowledge_pending{}
	broadcast_acknowledge_lock.Unlock()
}

// TestBroadcastAcknowledgeCoalesce — burst enqueues for the same
// (user, key, peer) tuple bump the pending sequence in place rather
// than queuing N separate flushes. Different tuples track
// independently. Inspects the in-memory pending map directly to keep
// the test fast and avoid the (real) 250 ms timer wait.
func TestBroadcastAcknowledgeCoalesce(t *testing.T) {
	// Reset the global state between subtests.
	broadcast_acknowledge_lock.Lock()
	broadcast_acknowledge_pending_map = map[string]*broadcast_acknowledge_pending{}
	broadcast_acknowledge_lock.Unlock()
	// Drain the pending map at test end so the 250 ms AfterFunc timers this
	// test schedules fire into an empty map — broadcast_acknowledge_flush
	// no-ops on a missing tag instead of calling user_by_uid -> db_open,
	// which reads the data_dir global and would race a later test's
	// data_dir reset (pre-existing flaky -race).
	t.Cleanup(broadcast_acknowledge_reset_for_test)

	// First enqueue for (u1, k, p1) creates an entry at seq=5.
	broadcast_acknowledge_enqueue("u1", "app1", "from1", "to1", "k", "p1", 5)

	// Three more enqueues for the same tuple should bump the
	// pending sequence; no second entry should be created.
	broadcast_acknowledge_enqueue("u1", "app1", "from1", "to1", "k", "p1", 7)
	broadcast_acknowledge_enqueue("u1", "app1", "from1", "to1", "k", "p1", 6)
	broadcast_acknowledge_enqueue("u1", "app1", "from1", "to1", "k", "p1", 9)

	// A different (key) tuple is independent.
	broadcast_acknowledge_enqueue("u1", "app1", "from1", "to1", "k2", "p1", 3)
	// A different (peer) tuple is independent.
	broadcast_acknowledge_enqueue("u1", "app1", "from1", "to1", "k", "p2", 4)

	broadcast_acknowledge_lock.Lock()
	defer broadcast_acknowledge_lock.Unlock()

	if got := len(broadcast_acknowledge_pending_map); got != 3 {
		t.Errorf("expected 3 pending entries (one per distinct tuple), got %d", got)
	}
	first := broadcast_acknowledge_pending_map["u1|k|p1"]
	if first == nil {
		t.Fatalf("missing pending entry for u1|k|p1")
	}
	if first.sequence != 9 {
		t.Errorf("expected pending sequence bumped to 9 (max of 5,7,6,9), got %d", first.sequence)
	}
	second := broadcast_acknowledge_pending_map["u1|k2|p1"]
	if second == nil || second.sequence != 3 {
		t.Errorf("expected u1|k2|p1 sequence 3, got %v", second)
	}
	third := broadcast_acknowledge_pending_map["u1|k|p2"]
	if third == nil || third.sequence != 4 {
		t.Errorf("expected u1|k|p2 sequence 4, got %v", third)
	}
}

// TestBroadcastAcknowledgeFlush — flushing a tag clears it from the
// pending map. Subsequent enqueues for the same tag re-create a fresh
// pending entry.
func TestBroadcastAcknowledgeFlush(t *testing.T) {
	broadcast_acknowledge_lock.Lock()
	broadcast_acknowledge_pending_map = map[string]*broadcast_acknowledge_pending{}
	broadcast_acknowledge_lock.Unlock()
	// See TestBroadcastAcknowledgeCoalesce: drain so leaked coalesce-window
	// timers can't run db_open after the test and race data_dir.
	t.Cleanup(broadcast_acknowledge_reset_for_test)

	broadcast_acknowledge_enqueue("u1", "app1", "from1", "to1", "k", "p1", 5)
	// Cannot call broadcast_acknowledge_flush directly without a
	// real user/app in the registries (the send path would panic on
	// nil user_by_uid). Simulate by deleting the entry, the same
	// terminal state the flush leaves.
	broadcast_acknowledge_lock.Lock()
	delete(broadcast_acknowledge_pending_map, "u1|k|p1")
	broadcast_acknowledge_lock.Unlock()

	// Next enqueue with same tag creates a fresh entry, not bumps.
	broadcast_acknowledge_enqueue("u1", "app1", "from1", "to1", "k", "p1", 12)
	broadcast_acknowledge_lock.Lock()
	defer broadcast_acknowledge_lock.Unlock()
	pending := broadcast_acknowledge_pending_map["u1|k|p1"]
	if pending == nil {
		t.Fatalf("expected fresh pending entry after flush + re-enqueue")
	}
	if pending.sequence != 12 {
		t.Errorf("expected fresh entry sequence 12, got %d", pending.sequence)
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

// TestBroadcastInfraTableEnsure — a receiver re-executing a replicated
// broadcast row op must first see the infra table created, because the
// sender's CREATE never crosses the wire (schema statements don't
// replicate). Reproduces the "no such table: log" replication failure:
// before the ensure the row write fails; after it, it succeeds. Covers
// all three replicated infra tables and confirms unrelated names no-op.
func TestBroadcastInfraTableEnsure(t *testing.T) {
	cases := []struct {
		table string
		write string
	}{
		{"log", "insert into log (key, peer, sequence, event, data, created) values ('k', 'p', 1, 'e', '', 0)"},
		{"sequence", "insert into sequence (key, peer, last) values ('k', 'p', 1)"},
		{"acknowledged", "insert into acknowledged (key, peer, subscriber, last) values ('k', 'p', 's', 1)"},
	}
	for _, c := range cases {
		db, cleanup := setup_broadcast_log_test(t)

		// Fresh DB has no broadcast tables: the replicated write fails
		// exactly as it did on the broken partner host.
		if _, err := db.internal.Exec(c.write); err == nil {
			t.Errorf("%s: expected write to fail on a DB lacking the table", c.table)
		}

		broadcast_infra_table_ensure(db, c.table)

		if _, err := db.internal.Exec(c.write); err != nil {
			t.Errorf("%s: write failed after ensure: %v", c.table, err)
		}
		cleanup()
	}

	// An unrelated table name must not create anything.
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()
	broadcast_infra_table_ensure(db, "messages")
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='messages'"); exists {
		t.Error("ensure created a table for a non-broadcast name")
	}
}
