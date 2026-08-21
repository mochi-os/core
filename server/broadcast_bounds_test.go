// Mochi server: the gap path's state is bounded against an unsigned key.
//
// The pending table, broadcast_stalls and broadcast_resync_inflight are all
// written before any app handler runs, from a stream key nothing signs.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"testing"
	"time"
)

// broadcast_bounds_reset empties both maps so a test starts from a known size.
func broadcast_bounds_reset() {
	broadcast_stall_lock.Lock()
	broadcast_stalls = map[string]*broadcast_stall{}
	broadcast_stall_lock.Unlock()

	broadcast_resync_lock.Lock()
	broadcast_resync_inflight = map[string]int64{}
	broadcast_resync_lock.Unlock()
}

func broadcast_stall_size() int {
	broadcast_stall_lock.Lock()
	defer broadcast_stall_lock.Unlock()
	return len(broadcast_stalls)
}

func broadcast_resync_size() int {
	broadcast_resync_lock.Lock()
	defer broadcast_resync_lock.Unlock()
	return len(broadcast_resync_inflight)
}

// TestBroadcastStallMapIsBounded. The key is attacker-chosen, so the ceiling
// is what stands between one peer and a map that grows for the life of the
// process.
func TestBroadcastStallMapIsBounded(t *testing.T) {
	broadcast_bounds_reset()
	defer broadcast_bounds_reset()

	for i := 0; i < broadcast_stall_maximum+500; i++ {
		broadcast_stall_note("u1", "feeds", "peer-x", fmt.Sprintf("invented-%d", i), 100, 500)
	}
	if n := broadcast_stall_size(); n > broadcast_stall_maximum {
		t.Errorf("stall map holds %d entries, above the ceiling of %d", n, broadcast_stall_maximum)
	}
}

// TestBroadcastStallCeilingKeepsTrackedStreams. Refusing past the ceiling
// must only refuse NEW streams. A stream already being tracked has to keep
// getting its watermark reset, or a flood would freeze a real stall's state
// at a stale watermark and warn about a stream that has since moved.
func TestBroadcastStallCeilingKeepsTrackedStreams(t *testing.T) {
	broadcast_bounds_reset()
	defer broadcast_bounds_reset()

	broadcast_stall_note("u1", "feeds", "peer-real", "key-real", 100, 500)
	for i := 0; i < broadcast_stall_maximum+500; i++ {
		broadcast_stall_note("u1", "feeds", "peer-x", fmt.Sprintf("invented-%d", i), 100, 500)
	}

	broadcast_stall_note("u1", "feeds", "peer-real", "key-real", 175, 600)
	broadcast_stall_lock.Lock()
	stall := broadcast_stalls["u1|feeds|peer-real|key-real"]
	broadcast_stall_lock.Unlock()
	if stall == nil {
		t.Fatal("the tracked stream was evicted by the flood")
	}
	if stall.watermark != 175 {
		t.Errorf("tracked stream's watermark is %d, want the reset to 175", stall.watermark)
	}
}

// TestBroadcastStallSweepDropsOnlyIdleEntries - the sweep is on last-note, not
// stall age: sweeping on age would evict exactly the long stall it warns about.
func TestBroadcastStallSweepDropsOnlyIdleEntries(t *testing.T) {
	broadcast_bounds_reset()
	defer broadcast_bounds_reset()

	broadcast_stall_note("u1", "feeds", "peer-x", "idle", 100, 500)
	broadcast_stall_note("u1", "feeds", "peer-x", "live", 100, 500)

	broadcast_stall_lock.Lock()
	broadcast_stalls["u1|feeds|peer-x|idle"].seen = now() - broadcast_stall_idle - 1
	// A long-running stall: first is old, but it is still being re-noted.
	broadcast_stalls["u1|feeds|peer-x|live"].first = now() - 9*86400
	broadcast_stall_lock.Unlock()

	broadcast_stall_sweep()

	broadcast_stall_lock.Lock()
	_, idle := broadcast_stalls["u1|feeds|peer-x|idle"]
	_, live := broadcast_stalls["u1|feeds|peer-x|live"]
	broadcast_stall_lock.Unlock()

	if idle {
		t.Error("an entry idle past broadcast_stall_idle survived the sweep")
	}
	if !live {
		t.Error("a stream still being re-noted was swept; a nine-day stall is what this map exists to report")
	}
}

// TestBroadcastAdvanceClearsStallTracking drives the real heal path rather
// than calling the clear directly: broadcast_advance_local is the only place
// that knows a stream moved, and it has to reach the map through db.app,
// which nothing read before.
func TestBroadcastAdvanceClearsStallTracking(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	broadcast_bounds_reset()
	defer broadcast_bounds_reset()

	user := &User{UID: "u-heal"}
	app := &App{id: "feeds"}
	db := db_app_system(user, app)
	if db == nil {
		t.Fatal("no system database")
	}
	defer db.close()

	broadcast_stall_note("u-heal", "feeds", "peer-h", "key-h", 100, 500)
	if broadcast_stall_size() != 1 {
		t.Fatal("stall was not tracked")
	}

	broadcast_advance_local(db, "peer-h", "key-h", 101)

	if n := broadcast_stall_size(); n != 0 {
		t.Errorf("%d stall entr(ies) survived an advance; a healed stream must not be retained", n)
	}
}

// TestBroadcastResyncInflightIsBounded. Past the ceiling a new tag is refused
// rather than admitted-but-untracked: admitting would send a resync back to
// the peer for every gap event, which is the amplification the throttle
// exists to prevent.
func TestBroadcastResyncInflightIsBounded(t *testing.T) {
	broadcast_bounds_reset()
	defer broadcast_bounds_reset()

	refused := 0
	for i := 0; i < broadcast_resync_maximum+500; i++ {
		if !broadcast_resync_throttle("u1", "peer-x", fmt.Sprintf("invented-%d", i)) {
			refused++
		}
	}
	if n := broadcast_resync_size(); n > broadcast_resync_maximum {
		t.Errorf("in-flight map holds %d entries, above the ceiling of %d", n, broadcast_resync_maximum)
	}
	if refused == 0 {
		t.Error("no request was refused past the ceiling, so every invented key still drew a resync")
	}
}

// TestBroadcastResyncSweepDropsExpired. An entry past the timeout is already
// ignored by the throttle, so reclaiming it changes no decision - but nothing
// reclaimed it, because the delete only ran on an advance a deliberately
// gapped stream never makes.
func TestBroadcastResyncSweepDropsExpired(t *testing.T) {
	broadcast_bounds_reset()
	defer broadcast_bounds_reset()

	timeout := int64(broadcast_resync_timeout / time.Second)
	broadcast_resync_lock.Lock()
	broadcast_resync_inflight["u1|peer-x|expired"] = time.Now().Unix() - timeout - 1
	broadcast_resync_inflight["u1|peer-x|fresh"] = time.Now().Unix()
	broadcast_resync_lock.Unlock()

	broadcast_resync_sweep()

	broadcast_resync_lock.Lock()
	_, expired := broadcast_resync_inflight["u1|peer-x|expired"]
	_, fresh := broadcast_resync_inflight["u1|peer-x|fresh"]
	broadcast_resync_lock.Unlock()

	if expired {
		t.Error("an entry past the timeout survived the sweep")
	}
	if !fresh {
		t.Error("a live in-flight entry was swept, so its stream would re-request immediately")
	}
}

// TestBroadcastPendingStreamCap. broadcast_pending_maximum is per (peer, key), so
// it bounds nothing while the key is chosen by the sender: each invented key
// draws its own thousand-row budget.
func TestBroadcastPendingStreamCap(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/bounds.db")
	if db == nil {
		t.Fatal("no database")
	}
	defer db.close()

	insert := func(key string, sequence int64) bool {
		return broadcast_pending_insert(db, "peer-x", key, sequence,
			"from", "to", "service", "event", "", "", "", []byte("{}"))
	}

	for i := 0; i < broadcast_pending_streams_maximum; i++ {
		if !insert(fmt.Sprintf("stream-%d", i), 5) {
			t.Fatalf("stream %d was refused below the cap", i)
		}
	}
	if insert("one-too-many", 5) {
		t.Errorf("a %dth distinct stream from one peer was buffered", broadcast_pending_streams_maximum+1)
	}
	// An established stream is unaffected: the cap gates opening a stream,
	// not appending to one, so a real subscriber at the cap still catches up.
	if !insert("stream-0", 6) {
		t.Error("an already-buffered stream was refused; the cap must gate new streams only")
	}
	// And a different peer has its own budget - one flooding peer must not
	// deny buffering to everyone else on the same app database.
	if !broadcast_pending_insert(db, "peer-other", "stream-0", 5,
		"from", "to", "service", "event", "", "", "", []byte("{}")) {
		t.Error("a different peer was refused; the cap must be per peer")
	}
}
