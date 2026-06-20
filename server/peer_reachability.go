// Mochi server: Peer reachability silent-cache.
//
// Without this, every queue_process goroutine and every /mochi/2/messages
// sender_open call would independently pay the unbounded libp2p connect
// timeout on `net_me.Connect()` when the target peer is offline. Three
// consecutive stream-open failures mean "skip the libp2p attempt and
// return nil immediately" — the queue row stays pending, Sender open
// returns errSenderUnreachable.
//
// Silence is DURABLE: once the counter crosses the threshold, the peer
// stays silent until peer_mark_reachable / peer_mark_send_success
// clears it. There is no time-based "trial probe" window.
//
// Recovery is event-driven via peer_reconnect_manager:
//   - peer_mark_send_failed schedules the peer for periodic reconnect
//     probing the first time it crosses the threshold (via
//     peer_schedule_reconnect — same path used by libp2p disconnect).
//   - peer_reconnect_manager runs probes with exponential backoff
//     (10s..300s).
//   - On success, peer_connect's success path calls peer_mark_reachable
//     (clears silence) and peer_reconnected (removes from
//     peer_reconnects), and queue_resurrect_peer wakes any deferred
//     queue.db rows for that peer.
//
// Why durable instead of a 60s window?
//
// The earlier design had peer_is_silent return false after 60s of no
// attempts. Every 60s, the next batch of queue_process picks would
// trigger fresh libp2p connect attempts to unreachable peers, each
// blocking for the full ~10s libp2p timeout. queue_process's WaitGroup
// then waited for those goroutines, dragging the whole tick out to
// ~10s. Self-loop drain, unreachable peers, and first-time
// sender_open for newly-back peers all starved because they were in
// the same batch as the stalled offline-peer goroutines.
//
// The durable model centralises the probing in peer_reconnect_manager
// (which is designed for it, with backoff and a concurrency cap) and
// keeps queue_process's tick latency bounded by the actual fast
// goroutines.
//
// The /mochi/2 send path (peer_protocol_open) feeds this cache.
//
// Not persisted: a server restart starts every peer with zero failures
// recorded, so every peer gets a fresh trial. The map is bounded by the
// number of distinct peers we've ever queued for in this process; GC
// pressure is negligible (a few thousand entries at most on a busy
// server).
//
// Why not libp2p's ConnectionGater?
//
// ConnectionGater.InterceptPeerDial would let us short-circuit silenced
// peers at the libp2p dial-time entry point — cheaper than our top-of-
// function map lookup. We don't, because the gater would also block
// libp2p internals that legitimately want to keep trying: DHT
// findpeer/Provide, identify-push, mDNS discovery, relay reservations.
// A peer that's "silent" from our application's send perspective may
// be perfectly reachable via a fresh address libp2p just learned —
// blocking it at the gater would prevent us from ever finding out.
// Silent-cache is a send-path optimisation, not a network policy, so
// it stays at the send-path entry point (peer_protocol_open).
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "sync"

// peer_silent_failure_threshold is the number of consecutive failed
// stream-opens before peer_is_silent starts returning true. Three is
// conservative — transient blips (one missed packet, a router reboot,
// an in-progress reconnect) don't silence the peer.
const peer_silent_failure_threshold = 3

type PeerReachability struct {
	ConsecutiveFailures int
	LastAttempt         int64
}

var (
	peer_reachability      = map[string]PeerReachability{}
	peer_reachability_lock = &sync.Mutex{}
)

// peer_is_silent returns true when the peer has crossed the failure
// threshold and the caller should fast-fail without attempting a
// libp2p connect. Bootstrap peers are always trusted infrastructure;
// never silenced. Self never silenced (in-process pipe can't fail).
//
// Silence is durable: it stays true until peer_mark_reachable or
// peer_mark_send_success clears the counter. peer_reconnect_manager
// drives the trial probes that eventually trigger that clearing.
func peer_is_silent(id string) bool {
	if id == "" || id == net_id || peer_is_bootstrap(id) {
		return false
	}
	peer_reachability_lock.Lock()
	defer peer_reachability_lock.Unlock()
	r, ok := peer_reachability[id]
	return ok && r.ConsecutiveFailures >= peer_silent_failure_threshold
}

// peer_mark_send_success clears any silent state. Called when an
// outbound libp2p stream opens cleanly via peer_protocol_open. The
// libp2p layer being alive is what matters here; whether the eventual
// app-level ACK arrives is a separate concern.
func peer_mark_send_success(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_reachability_lock.Lock()
	defer peer_reachability_lock.Unlock()
	peer_reachability[id] = PeerReachability{ConsecutiveFailures: 0, LastAttempt: now()}
}

// peer_mark_send_failed records one stream-open failure. Called from
// the peer_connect=false branch in peer_protocol_open when the libp2p
// connect itself fails — that's the "peer is unreachable" signal we
// want to silence on.
// Transient stream-open errors after a successful connect don't count
// (the peer IS reachable; the failure is application- or
// protocol-level, handled separately).
//
// On the failure that first crosses the threshold, schedules the peer
// for periodic reconnect probing via peer_reconnect_manager. Without
// this, a peer we discovered via DHT but never successfully connected
// to would stay silent forever (peer_reconnects[] is otherwise only
// populated by libp2p disconnect events).
func peer_mark_send_failed(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_reachability_lock.Lock()
	r := peer_reachability[id]
	r.ConsecutiveFailures++
	r.LastAttempt = now()
	peer_reachability[id] = r
	crossed := r.ConsecutiveFailures == peer_silent_failure_threshold
	peer_reachability_lock.Unlock()
	if crossed {
		peer_schedule_reconnect(id)
		// Persist "unreachable since" for a replication member we can't even
		// connect to (powered off / partitioned) — the companion to the
		// inflight-timeout stamp in peer_mark_no_progress and the
		// disconnect-event stamp in peer_disconnected.
		replication_member_unreachable(id)
	}
}

// peer_mark_reachable resets the silent cache without recording a
// stream-open success. Called from peer_connect's libp2p-success path
// so that rows woken by queue_resurrect_peer can actually attempt
// their sends rather than fast-failing on a stale silent flag.
//
// Without this, a peer that came back during the silence period would
// stay fast-failed on every outbound until peer_mark_send_success
// fired — and peer_mark_send_success only fires on actual stream open,
// which queue_resurrect_peer's woken rows can't reach if peer_is_silent
// blocks them. Calling peer_mark_reachable from peer_connect's
// success path breaks the chicken-and-egg.
func peer_mark_reachable(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_reachability_lock.Lock()
	defer peer_reachability_lock.Unlock()
	delete(peer_reachability, id)
}
