// Mochi server: peer send-progress (stall) cache + whole-target defer tests.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"testing"
)

// peer_progress_age backdates a peer's no-progress evidence by `age` seconds,
// as if it had begun that long ago.
func peer_progress_age(id string, age int64) {
	peer_progress_lock.Lock()
	p := peer_progress[id]
	p.Since = now() - age
	peer_progress[id] = p
	peer_progress_lock.Unlock()
}

// peer_stall_test stalls a peer the way time does: no-progress evidence that
// has run for peer_stall_after with no ack.
func peer_stall_test(id string) {
	peer_mark_no_progress(id)
	peer_progress_age(id, peer_stall_after())
	peer_mark_no_progress(id)
}

// TestPeerStallThresholdAndWindow: the progress-stall cache trips once
// no-progress evidence has run for peer_stall_after with no ack, an ack clears
// it, and once the trial window passes peer_is_stalled reopens so a parked
// backlog gets a trial send - which, failing, re-stalls at once.
func TestPeerStallThresholdAndWindow(t *testing.T) {
	setup_replication_test(t)
	peer_progress = map[string]PeerProgress{}

	peer := "peer-stall-test"
	if peer_is_stalled(peer) {
		t.Fatal("a fresh peer must not be stalled")
	}
	peer_mark_no_progress(peer)
	peer_progress_age(peer, peer_stall_after()-1)
	peer_mark_no_progress(peer)
	if peer_is_stalled(peer) {
		t.Fatal("stalled before the evidence had run for peer_stall_after")
	}
	peer_progress_age(peer, peer_stall_after())
	peer_mark_no_progress(peer)
	if !peer_is_stalled(peer) {
		t.Fatal("not stalled once the evidence had run for peer_stall_after")
	}

	// An ack clears the stall.
	peer_mark_progress(peer)
	if peer_is_stalled(peer) {
		t.Fatal("an ack must clear the stall")
	}

	// Re-stall, then expire the trial window: peer_is_stalled reopens so
	// the deferred backlog gets its trial.
	peer_stall_test(peer)
	if !peer_is_stalled(peer) {
		t.Fatal("must re-stall")
	}
	peer_progress_lock.Lock()
	e := peer_progress[peer]
	e.StalledUntil = now() - 1
	peer_progress[peer] = e
	peer_progress_lock.Unlock()
	if peer_is_stalled(peer) {
		t.Fatal("past the trial window, peer_is_stalled must reopen for a trial")
	}
	// The trial times out too, with no ack since: stalled again at once.
	peer_mark_no_progress(peer)
	if !peer_is_stalled(peer) {
		t.Error("a failed trial did not re-stall the peer")
	}
}

// TestPeerStallIgnoresABurstOfTimeouts is #158: messages written in successive
// seconds that wait behind one slow handler on the far side time out in
// successive sweeps. Counted by sweep, three of them stalled a working peer for
// an hour; the late acks that followed did not clear it, as only an ack for a
// message still in flight counted.
func TestPeerStallIgnoresABurstOfTimeouts(t *testing.T) {
	setup_replication_test(t)
	peer_progress = map[string]PeerProgress{}

	peer := "peer-burst-test"
	for i := 0; i < 10; i++ {
		peer_mark_no_progress(peer)
	}
	if peer_is_stalled(peer) {
		t.Error("ten timeouts in the same moment stalled the peer: one slow handler on the far side defers its backlog for an hour")
	}

	// A late ack, for a message the sweeper had already timed out, clears the
	// evidence, so later timeouts start afresh.
	s := &Sender{peer: peer, inflight: map[string]*pending{}}
	s.handle_inbound(&Frame{Type: frame_type_ack, Replies: []string{"timed-out-already"}})
	peer_progress_lock.Lock()
	_, kept := peer_progress[peer]
	peer_progress_lock.Unlock()
	if kept {
		t.Error("an ack for a message already timed out left the evidence standing")
	}
}

func TestQueueDeferTargetParksWholeBacklog(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()

	for i := 0; i < 8; i++ {
		queue_test_insert_target(db, fmt.Sprintf("stuck-%d", i), "peer-stuck", priority_interactive)
	}
	queue_test_insert_target(db, "live-1", "peer-live", priority_interactive)

	// Both peers due -> one row per peer.
	if got := len(queue_test_select(db)); got != 2 {
		t.Fatalf("before defer: queue_select returned %d, want 2 (one per peer)", got)
	}

	// Park the stuck peer's whole backlog in one UPDATE.
	queue_defer_target("peer-stuck", now()+1000)

	// Only the live peer remains; the stuck peer is out of the scan
	// entirely (no per-tick re-scan of its pile).
	entries := queue_test_select(db)
	if len(entries) != 1 {
		t.Fatalf("after defer: queue_select returned %d, want 1 (stuck peer parked)", len(entries))
	}
	if entries[0].Target != "peer-live" {
		t.Errorf("remaining entry target = %q, want peer-live", entries[0].Target)
	}

	// Recovery brings the parked backlog back due.
	queue_resurrect_peer("peer-stuck")
	if got := len(queue_test_select(db)); got != 2 {
		t.Fatalf("after resurrect: queue_select returned %d, want 2", got)
	}
}
