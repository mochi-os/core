// Mochi server: Peer reachability silent-cache. Three consecutive stream-open
// failures make peer_protocol_open fast-fail rather than pay the libp2p connect
// timeout for an offline peer. Silence is DURABLE - there is no time-based
// trial window, because that put a ~10s dial on whatever queue_process
// goroutine picked the row. Recovery is event-driven through
// peer_reconnect_manager. Not persisted.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

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

// peer_is_silent reports that the caller should fast-fail without a libp2p
// connect. Durable: cleared only by peer_mark_reachable or
// peer_mark_send_success.
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

// peer_mark_send_failed records one connect-level stream-open failure; errors
// after a successful connect do not count. The failure that first crosses the
// threshold enrolls the peer for reconnect probing - its only recovery path.
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
	}
}

// peer_mark_reachable clears the silent cache without a stream-open success.
// peer_connect's success path calls it because peer_mark_send_success only
// fires on a stream open, which a silenced peer's resurrected rows can never
// reach.
func peer_mark_reachable(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_reachability_lock.Lock()
	defer peer_reachability_lock.Unlock()
	delete(peer_reachability, id)
}
