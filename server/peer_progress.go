// Mochi server: Peer send-progress ("stalled") cache. peer_reachability.go
// tracks libp2p connect failures; this tracks a peer whose /mochi/2 stream
// opens but whose frames time out without an ack. A stalled target's whole
// backlog is deferred for peer_stall_window, then gets one trial send that
// clears or re-stalls it. Fed from the per-peer Sender.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import "sync"

const (
	// Consecutive sweep-observed inflight timeouts (no intervening ack)
	// before a target is treated as stalled. Mirrors
	// peer_silent_failure_threshold; conservative so a single slow ack
	// doesn't stall a working peer.
	peer_stall_threshold = 3

	// How long a stalled target's backlog is deferred before the next trial
	// send. A genuinely-dead target retries at most this often (no spin); a
	// recovered one resumes on the next trial.
	peer_stall_window = 3600 // 1 hour
)

type PeerProgress struct {
	Timeouts     int
	StalledUntil int64
}

var (
	peer_progress      = map[string]PeerProgress{}
	peer_progress_lock = &sync.Mutex{}
)

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
	return ok && p.Timeouts >= peer_stall_threshold && now() < p.StalledUntil
}

// peer_stall_until returns the time a stalled target's backlog should be
// deferred to (its current trial-window end), or 0 if not stalled.
func peer_stall_until(id string) int64 {
	peer_progress_lock.Lock()
	defer peer_progress_lock.Unlock()
	p, ok := peer_progress[id]
	if !ok || p.Timeouts < peer_stall_threshold {
		return 0
	}
	return p.StalledUntil
}

// peer_mark_progress clears any stall — an ack arrived, so the peer is
// applying and acking. Resurrects the deferred backlog only on the
// stalled->recovered transition (a cheap no-op for the common,
// never-stalled peer). Called per ack frame from the Sender read loop.
func peer_mark_progress(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_progress_lock.Lock()
	p, ok := peer_progress[id]
	stalled := ok && p.Timeouts >= peer_stall_threshold
	if ok {
		delete(peer_progress, id)
	}
	peer_progress_lock.Unlock()
	// The peer just acked, so it is reachable again: clear the offline mark.
	if stalled {
		queue_resurrect_peer(id)
	}
}

// peer_mark_no_progress records that an inflight frame to this peer timed
// out without an ack. On crossing the threshold it opens a stall window.
// Called once per sweep per peer that had stale inflight.
func peer_mark_no_progress(id string) {
	if id == "" || id == net_id || peer_is_bootstrap(id) {
		return
	}
	peer_progress_lock.Lock()
	p := peer_progress[id]
	p.Timeouts++
	if p.Timeouts >= peer_stall_threshold {
		p.StalledUntil = now() + peer_stall_window
	}
	peer_progress[id] = p
	peer_progress_lock.Unlock()
}
