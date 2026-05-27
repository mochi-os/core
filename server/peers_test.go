// Mochi server: peers subsystem unit tests
// Copyright Alistair Cunningham 2026
//
// Tests covering the in-memory peer-silence fast-fail (task #100).
// The gate sits in peer_stream so every outbound send path
// (queue_send_direct, file_push, directory, messages) gets the
// benefit; the tests exercise the gate directly to keep the
// libp2p layer out of the picture.

package main

import (
	"testing"
)

// reset_peer_reachability is a test helper that clears the in-memory
// map and any state left by an earlier test. The reachability map is
// a package-level var so test order would otherwise pollute later
// runs.
func reset_peer_reachability(t *testing.T) {
	t.Helper()
	peer_reachability_lock.Lock()
	peer_reachability = map[string]PeerReachability{}
	peer_reachability_lock.Unlock()
}

// TestPeerIsSilentEmptyMap confirms a brand-new peer (never recorded
// in the map) is NEVER silent. This is the cold-start case after a
// server restart - every peer gets a fresh trial.
func TestPeerIsSilentEmptyMap(t *testing.T) {
	reset_peer_reachability(t)
	if peer_is_silent("12D3KooWFakePeerForTest1") {
		t.Error("unknown peer must not be silent (cold-start should allow first attempt)")
	}
}

// TestPeerIsSilentBelowThreshold confirms a peer that has failed
// fewer times than the threshold is not silenced. The threshold
// exists to ignore transient blips.
func TestPeerIsSilentBelowThreshold(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest2"
	for i := 0; i < peer_silent_failure_threshold-1; i++ {
		peer_mark_send_failed(id)
	}
	if peer_is_silent(id) {
		t.Errorf("peer with %d failures (below threshold %d) must not be silent",
			peer_silent_failure_threshold-1, peer_silent_failure_threshold)
	}
}

// TestPeerIsSilentAtThreshold confirms a peer that has hit the
// threshold IS silenced - this is the load-bearing gate. Three
// consecutive failures inside the skip window means the next caller
// fast-fails without going near libp2p.
func TestPeerIsSilentAtThreshold(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest3"
	for i := 0; i < peer_silent_failure_threshold; i++ {
		peer_mark_send_failed(id)
	}
	if !peer_is_silent(id) {
		t.Errorf("peer with %d consecutive failures must be silent", peer_silent_failure_threshold)
	}
}

// TestPeerIsSilentSuccessResetsCounter confirms a success clears the
// silent state. This is how a returning peer recovers - the trial
// send after the skip window succeeds and the counter resets to 0.
func TestPeerIsSilentSuccessResetsCounter(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest4"
	for i := 0; i < peer_silent_failure_threshold; i++ {
		peer_mark_send_failed(id)
	}
	if !peer_is_silent(id) {
		t.Fatal("peer must be silent after threshold failures")
	}
	peer_mark_send_success(id)
	if peer_is_silent(id) {
		t.Error("peer must not be silent after a successful send (success clears counter)")
	}
}

// TestPeerIsSilentSkipWindowExpiry confirms the silence lapses once
// the skip window passes - the next caller IS allowed to try again
// even if the consecutive failures count is still above threshold.
// This is the "trial attempt after silence" mechanism.
//
// We exercise this by writing a sufficiently-old LastAttempt directly
// instead of waiting peer_silent_skip_window seconds in the test.
func TestPeerIsSilentSkipWindowExpiry(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest5"
	peer_reachability_lock.Lock()
	peer_reachability[id] = PeerReachability{
		ConsecutiveFailures: peer_silent_failure_threshold + 5,
		LastAttempt:         now() - peer_silent_skip_window - 1,
	}
	peer_reachability_lock.Unlock()
	if peer_is_silent(id) {
		t.Error("peer with stale LastAttempt must not be silent (skip window has passed; trial attempt allowed)")
	}
}

// TestPeerIsSilentBootstrap confirms bootstrap peers are never
// silenced regardless of failure count. Bootstrap is our default
// publisher / fallback - silencing it would brick the whole pubsub
// mesh when it goes down briefly.
func TestPeerIsSilentBootstrap(t *testing.T) {
	reset_peer_reachability(t)
	id := peers_bootstrap[0].ID
	for i := 0; i < peer_silent_failure_threshold*10; i++ {
		peer_mark_send_failed(id)
	}
	if peer_is_silent(id) {
		t.Error("bootstrap peer must never be silent regardless of failure count")
	}
}

// TestPeerIsSilentSelf confirms self-loop is never silent. The
// in-process pipe in peer_stream() can't fail in the way the libp2p
// connect can; marking self as silent would break all local fan-out.
func TestPeerIsSilentSelf(t *testing.T) {
	reset_peer_reachability(t)
	// net_id may be empty in tests where p2p hasn't initialised;
	// peer_is_silent's id=="" guard covers that case. Test the
	// explicit net_id==id path with a fake-but-equal value.
	saved := net_id
	net_id = "12D3KooWFakeSelf"
	defer func() { net_id = saved }()
	for i := 0; i < peer_silent_failure_threshold*10; i++ {
		peer_mark_send_failed(net_id)
	}
	if peer_is_silent(net_id) {
		t.Error("self peer must never be silent")
	}
}
