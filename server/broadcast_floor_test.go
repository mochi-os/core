// Mochi server: unfillable-gap escape hatch regressions.
//
// The 2026-07 News feed wedge exposed three healing failures: the pending GC
// skipped one hole per hourly pass (never converging against a sparse
// buffer), the replay log age-trimmed rows a wedged subscriber still needed
// (making the gap permanently unfillable), and a resync request below the
// log floor replayed useless far-future events instead of saying so. These
// tests cover the fixes: the looping skip, the ack-floor-aware trim, and
// the broadcast/floor skip handler.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// TestBroadcastSkipEscalation — a single unfillable skip is a log line
// only; the operator email fires on recurrence (the same stream again
// inside broadcast_skip_recurrence) and on breadth (broadcast_skip_breadth
// distinct streams inside a day), and records past the recurrence window
// are pruned in passing.
func TestBroadcastSkipEscalation(t *testing.T) {
	breadth := broadcast_skip_breadth
	broadcast_skip_breadth = 3
	defer func() {
		broadcast_skip_breadth = breadth
		broadcast_skip_breadth_warned.Delete("breadth")
		broadcast_skip_state.Range(func(k, _ any) bool {
			if strings.HasPrefix(k.(string), "u-skip|") {
				broadcast_skip_state.Delete(k)
			}
			return true
		})
	}()

	record := func(id string) broadcast_skip_record {
		t.Helper()
		v, ok := broadcast_skip_state.Load(id)
		if !ok {
			t.Fatalf("expected a skip record for %q", id)
		}
		return v.(broadcast_skip_record)
	}

	// First skip: recorded, no recurrence warn.
	broadcast_skip_warn("u-skip", "feeds", "p1", "k1", 2, 3)
	if r := record("u-skip|feeds|p1|k1"); r.warned != 0 {
		t.Fatal("a first skip must not warn")
	}

	// A repeat inside the recurrence window warns.
	broadcast_skip_warn("u-skip", "feeds", "p1", "k1", 4, 5)
	first := record("u-skip|feeds|p1|k1").warned
	if first == 0 {
		t.Fatal("a repeat skip inside the recurrence window must warn")
	}

	// Same-day repeats keep the stamp: one recurrence warn per day.
	broadcast_skip_warn("u-skip", "feeds", "p1", "k1", 6, 7)
	if record("u-skip|feeds|p1|k1").warned != first {
		t.Error("recurrence warn must throttle to one per day")
	}

	// A prior skip past the recurrence window does not escalate.
	stale := "u-skip|feeds|p2|k2"
	broadcast_skip_state.Store(stale, broadcast_skip_record{last: now() - broadcast_skip_recurrence - 10})
	broadcast_skip_warn("u-skip", "feeds", "p2", "k2", 2, 2)
	if record(stale).warned != 0 {
		t.Error("a skip after the recurrence window must not warn")
	}

	// Breadth: a third distinct stream within a day trips the aggregate.
	broadcast_skip_breadth_warned.Delete("breadth")
	broadcast_skip_warn("u-skip", "feeds", "p3", "k3", 2, 2)
	if _, ok := broadcast_skip_breadth_warned.Load("breadth"); !ok {
		t.Fatal("breadth threshold must warn")
	}

	// Records older than the recurrence window are pruned in passing.
	dead := "u-skip|feeds|p4|k4"
	broadcast_skip_state.Store(dead, broadcast_skip_record{last: now() - broadcast_skip_recurrence - 10})
	broadcast_skip_warn("u-skip", "feeds", "p5", "k5", 2, 2)
	if _, ok := broadcast_skip_state.Load(dead); ok {
		t.Error("stale records must be pruned")
	}
}

// TestBroadcastPendingSkipStreamConverges — a sparse buffer with many holes
// unsticks in ONE pass: the loop skips each hole, the drain applies the run
// behind it, and the stream ends at the buffer's top.
func TestBroadcastPendingSkipStreamConverges(t *testing.T) {
	_, cleanup := setup_broadcast_pending_gc_test(t)
	defer cleanup()

	original := broadcast_pending_dispatch
	defer func() { broadcast_pending_dispatch = original }()
	var dispatched []int64
	broadcast_pending_dispatch = func(row *broadcast_pending_row, db *DB) bool {
		dispatched = append(dispatched, row.Sequence)
		return true
	}

	rel := stage_stalled_stream(t, "u1", "appA", "peer1", "key1", 5, 10, 1, now()-1000)
	db := db_open(rel)
	// Add more buffered rows with holes between them: 10 (staged), 12, 15, 16, 20.
	for _, seq := range []int64{12, 15, 16, 20} {
		db.exec("insert into pending (peer, key, sequence, source, target, service, event, content, received) values ('peer1', 'key1', ?, '', '', '', '', x'01', ?)", seq, now()-1000)
	}

	last := broadcast_pending_skip_stream(db, "u1", "appA", "peer1", "key1", 5, now()-10, false)
	if last != 20 {
		t.Errorf("converged watermark: got %d, want 20", last)
	}
	if len(dispatched) != 5 {
		t.Errorf("dispatched %d buffered events, want 5: %v", len(dispatched), dispatched)
	}
	if n := db.integer("select count(*) from pending where peer='peer1' and key='key1'"); n != 0 {
		t.Errorf("pending rows remaining: %d, want 0", n)
	}
}

// TestBroadcastPendingSkipStreamRespectsAge — holes younger than the cutoff
// are left to wait out their own TTL (they may still fill via resync).
func TestBroadcastPendingSkipStreamRespectsAge(t *testing.T) {
	_, cleanup := setup_broadcast_pending_gc_test(t)
	defer cleanup()

	original := broadcast_pending_dispatch
	defer func() { broadcast_pending_dispatch = original }()
	broadcast_pending_dispatch = func(row *broadcast_pending_row, db *DB) bool { return true }

	rel := stage_stalled_stream(t, "u2", "appA", "peer1", "key1", 5, 10, 1, now())
	db := db_open(rel)

	// Cutoff is in the past; the staged row was received just now, so the
	// hole is too young to skip.
	last := broadcast_pending_skip_stream(db, "u2", "appA", "peer1", "key1", 5, now()-3600, false)
	if last != 5 {
		t.Errorf("young hole must not skip: got %d, want 5", last)
	}

	// force bypasses the age gate.
	last = broadcast_pending_skip_stream(db, "u2", "appA", "peer1", "key1", 5, now()-3600, true)
	if last != 10 {
		t.Errorf("force skip: got %d, want 10", last)
	}
}

// TestBroadcastLogAgeTrimRespectsAckFloor — aged rows a lagging subscriber
// still needs survive the normal trim and fall only at the hard cap.
func TestBroadcastLogAgeTrimRespectsAckFloor(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	broadcast_log_table_create(db)
	broadcast_acknowledged_table_create(db)
	// Ten aged rows; the laggard subscriber has acked through 3.
	for i := int64(1); i <= 10; i++ {
		db.exec("insert into log (key, peer, sequence, event, data, created) values ('k', 'p', ?, 'e', '', ?)", i, now()-broadcast_log_age-100)
	}
	db.exec("insert into acknowledged (key, peer, subscriber, last) values ('k', 'p', 'laggard', 3)")

	broadcast_log_age_trim(db, "k", "p")
	if low := db.integer("select min(sequence) from log where key='k' and peer='p'"); low != 4 {
		t.Errorf("after floor-aware trim: min sequence %d, want 4 (rows the laggard needs survive)", low)
	}

	// Rows past the hard cap fall regardless of the floor.
	db.exec("update log set created=? where key='k' and peer='p' and sequence <= 6", now()-broadcast_log_age_maximum-100)
	broadcast_log_age_trim(db, "k", "p")
	if low := db.integer("select min(sequence) from log where key='k' and peer='p'"); low != 7 {
		t.Errorf("after hard cap: min sequence %d, want 7", low)
	}
}

// TestBroadcastFloorSkips — a broadcast/floor event from the stream's own
// peer advances the watermark to floor-1; one from any other peer is
// refused (only the origin is authoritative about its own log).
func TestBroadcastFloorSkips(t *testing.T) {
	_, cleanup := setup_broadcast_pending_gc_test(t)
	defer cleanup()

	rel := stage_stalled_stream(t, "u4", "appA", "peer1", "key1", 5, 100, 1, now()-1000)
	db := db_open(rel)

	e := &Event{
		from:    "subscriber-entity",
		peer:    "peer1",
		user:    &User{UID: "u4"},
		db:      db,
		content: map[string]any{"key": "key1", "peer": "peer1", "floor": int64(50)},
	}
	if err := e.broadcast_floor(&App{id: "appA"}); err != nil {
		t.Fatalf("broadcast_floor: %v", err)
	}
	if last := broadcast_received_get(db, "peer1", "key1"); last != 49 {
		t.Errorf("watermark after floor skip: got %d, want 49", last)
	}

	// A floor event arriving from a different peer than it names is refused.
	forged := &Event{
		from:    "subscriber-entity",
		peer:    "peer2",
		user:    &User{UID: "u4"},
		db:      db,
		content: map[string]any{"key": "key1", "peer": "peer1", "floor": int64(90)},
	}
	if err := forged.broadcast_floor(&App{id: "appA"}); err == nil {
		t.Error("floor event from a mismatched peer must be refused")
	}
	if last := broadcast_received_get(db, "peer1", "key1"); last != 49 {
		t.Errorf("watermark after forged floor: got %d, want 49 (unchanged)", last)
	}

	// A floor at or below the watermark is a no-op.
	e.content["floor"] = int64(30)
	if err := e.broadcast_floor(&App{id: "appA"}); err != nil {
		t.Fatalf("broadcast_floor below watermark: %v", err)
	}
	if last := broadcast_received_get(db, "peer1", "key1"); last != 49 {
		t.Errorf("watermark after stale floor: got %d, want 49 (unchanged)", last)
	}

	// A floor past the buffered rows sweeps them: the skipped events are
	// stale duplicates the chain-drain never dispatches, and without the
	// sweep they linger below the cursor forever.
	e.content["floor"] = int64(150)
	if err := e.broadcast_floor(&App{id: "appA"}); err != nil {
		t.Fatalf("broadcast_floor sweep: %v", err)
	}
	if n := db.integer("select count(*) from pending where peer='peer1' and key='key1'"); n != 0 {
		t.Errorf("floor skip must sweep below-cursor pending rows; %d remain", n)
	}
}
