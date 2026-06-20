// Mochi server: peer send-progress (stall) cache + whole-target defer tests.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"testing"
)

// TestPeerStallThresholdAndWindow: the progress-stall cache trips after
// peer_stall_threshold inflight-timeouts, an ack clears it, and once the
// trial window passes peer_is_stalled reopens so a parked backlog gets a
// trial send.
func TestPeerStallThresholdAndWindow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	peer_progress = map[string]PeerProgress{}

	peer := "peer-stall-test"
	if peer_is_stalled(peer) {
		t.Fatal("a fresh peer must not be stalled")
	}
	for i := 1; i < peer_stall_threshold; i++ {
		peer_mark_no_progress(peer)
		if peer_is_stalled(peer) {
			t.Fatalf("stalled after %d timeouts; threshold is %d", i, peer_stall_threshold)
		}
	}
	peer_mark_no_progress(peer) // crosses the threshold
	if !peer_is_stalled(peer) {
		t.Fatalf("not stalled after %d timeouts (threshold %d)", peer_stall_threshold, peer_stall_threshold)
	}

	// An ack clears the stall.
	peer_mark_progress(peer)
	if peer_is_stalled(peer) {
		t.Fatal("an ack must clear the stall")
	}

	// Re-stall, then expire the trial window: peer_is_stalled reopens so
	// the deferred backlog gets its trial.
	for i := 0; i < peer_stall_threshold; i++ {
		peer_mark_no_progress(peer)
	}
	if !peer_is_stalled(peer) {
		t.Fatal("must re-stall after threshold timeouts")
	}
	peer_progress_lock.Lock()
	e := peer_progress[peer]
	e.StalledUntil = now() - 1
	peer_progress[peer] = e
	peer_progress_lock.Unlock()
	if peer_is_stalled(peer) {
		t.Fatal("past the trial window, peer_is_stalled must reopen for a trial")
	}
}

// TestQueueDeferTargetParksWholeBacklog: queue_defer_target pushes every
// pending row for a target forward in one shot, so queue_select stops
// returning that target — the fix for the per-tick re-scan spin. A
// different peer's rows are untouched, and queue_resurrect_peer brings the
// parked backlog back.
func TestQueueDeferTargetParksWholeBacklog(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db := queue_test_table()

	for i := 0; i < 8; i++ {
		queue_test_insert_target(db, fmt.Sprintf("stuck-%d", i), "peer-stuck", priority_bulk)
	}
	queue_test_insert_target(db, "live-1", "peer-live", priority_interactive)

	// Both peers due -> one row per peer.
	if got := len(queue_select(db)); got != 2 {
		t.Fatalf("before defer: queue_select returned %d, want 2 (one per peer)", got)
	}

	// Park the stuck peer's whole backlog in one UPDATE.
	queue_defer_target("peer-stuck", now()+1000)

	// Only the live peer remains; the stuck peer is out of the scan
	// entirely (no per-tick re-scan of its pile).
	entries := queue_select(db)
	if len(entries) != 1 {
		t.Fatalf("after defer: queue_select returned %d, want 1 (stuck peer parked)", len(entries))
	}
	if entries[0].Target != "peer-live" {
		t.Errorf("remaining entry target = %q, want peer-live", entries[0].Target)
	}

	// Recovery brings the parked backlog back due.
	queue_resurrect_peer("peer-stuck")
	if got := len(queue_select(db)); got != 2 {
		t.Fatalf("after resurrect: queue_select returned %d, want 2", got)
	}
}
