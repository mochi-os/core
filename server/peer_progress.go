// Mochi server: Peer send-progress ("stalled") cache. peer_reachability.go
// tracks libp2p connect failures; this tracks a peer whose connection is up but
// which makes no progress: frames time out without an ack, a stream will not
// open, the hello never comes. A stalled target's whole backlog is deferred for
// peer_stall_window, then gets one trial send that clears or re-stalls it. Fed
// from the per-peer Sender and its opening.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import "sync"

const (
	// peer_stall_multiple is how many inflight timeouts' worth of time sends to
	// a peer must go on timing out, with no ack from it in between, before the
	// peer counts as stalled. Messages written in successive seconds that all
	// wait behind one slow handler on the far side time out in successive
	// sweeps: measured in time they are one timeout, not a peer that stopped.
	peer_stall_multiple = 3

	// How long a stalled target's backlog is deferred before the next trial
	// send. A genuinely-dead target retries at most this often (no spin); a
	// recovered one resumes on the next trial.
	peer_stall_window = 3600 // 1 hour
)

type PeerProgress struct {
	Since        int64 // first evidence of no progress since the peer last acked anything
	StalledUntil int64 // end of the current stall's trial window; 0 when not stalled
}

var (
	peer_progress      = map[string]PeerProgress{}
	peer_progress_lock = &sync.Mutex{}
)

// peer_stall_after is how long no-progress evidence must run without an ack
// before a peer is stalled.
func peer_stall_after() int64 {
	return int64(peer_stall_multiple * peer_inflight_timeout())
}

// peer_is_stalled reports whether sends to this peer are timing out
// without acks and the trial window hasn't reopened yet. Bootstrap and
// self are never stalled.
func peer_is_stalled(id string) bool {
	if id == "" || id == net_id || peer_is_bootstrap(id) {
		return false
	}
	peer_progress_lock.Lock()
	defer peer_progress_lock.Unlock()
	p, ok := peer_progress[id]
	return ok && p.StalledUntil > 0 && now() < p.StalledUntil
}

// peer_stall_until returns the time a stalled target's backlog should be
// deferred to (its current trial-window end), or 0 if not stalled.
func peer_stall_until(id string) int64 {
	peer_progress_lock.Lock()
	defer peer_progress_lock.Unlock()
	return peer_progress[id].StalledUntil
}

// peer_mark_progress clears any stall and the evidence towards one - the peer
// acked something, so it is applying. Called for every ack frame from the
// Sender read loop, those for messages the sweeper has already timed out
// included: they are the acks that show a slow peer was never stuck.
// Resurrects the deferred backlog only on the stalled->recovered transition (a
// cheap no-op for the common, never-stalled peer).
func peer_mark_progress(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_progress_lock.Lock()
	p, ok := peer_progress[id]
	stalled := ok && p.StalledUntil > 0
	if ok {
		delete(peer_progress, id)
	}
	peer_progress_lock.Unlock()
	if stalled {
		queue_resurrect_peer(id)
	}
}

// peer_mark_no_progress records evidence that a peer whose connection is up is
// not making progress: an inflight frame timed out without an ack, a stream
// would not open on the live connection, or the far side withheld its hello.
// Once such evidence has run for peer_stall_after with no ack in between, it
// opens a stall window; past that window the first new evidence re-stalls at
// once, which is the trial failing.
func peer_mark_no_progress(id string) {
	if id == "" || id == net_id || peer_is_bootstrap(id) {
		return
	}
	t := now()
	peer_progress_lock.Lock()
	p := peer_progress[id]
	if p.Since == 0 {
		p.Since = t
	}
	if t-p.Since >= peer_stall_after() {
		p.StalledUntil = t + peer_stall_window
	}
	peer_progress[id] = p
	peer_progress_lock.Unlock()
}
