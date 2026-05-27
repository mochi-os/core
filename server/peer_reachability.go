// Mochi server: Peer reachability silent-cache.
//
// Without this, every queue_process goroutine and every /mochi/2/messages
// sender_open call would independently pay the unbounded libp2p connect
// timeout on `net_me.Connect()` when the target peer is offline. Three
// consecutive failures within the skip window mean "skip the libp2p
// attempt and return nil immediately" — the queue row stays pending
// under the usual exponential backoff, Sender open returns
// errSenderUnreachable, the pull_loop just polls until silence ages out.
//
// Both /mochi/1 (peer_stream) and /mochi/2 (peer_protocol_open) feed
// this cache; one reachability oracle covers both protocols.
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
// it stays at the send-path entry points (peer_protocol_open and
// peer_stream).
//
// Copyright Alistair Cunningham 2024-2026

package main

import "sync"

// peer_silent_failure_threshold is the number of consecutive failed
// stream-opens before peer_is_silent starts returning true. Three is
// conservative — transient blips (one missed packet, a router reboot,
// an in-progress reconnect) don't silence the peer.
const peer_silent_failure_threshold = 3

// peer_silent_skip_window is how long after a failed attempt the peer
// stays silent before we trial another send. Short enough that a
// reconnecting peer drains its backlog within a couple of minutes;
// long enough that wasted libp2p timeouts are amortised.
const peer_silent_skip_window = 60

type PeerReachability struct {
	ConsecutiveFailures int
	LastAttempt         int64
}

var (
	peer_reachability      = map[string]PeerReachability{}
	peer_reachability_lock = &sync.Mutex{}
)

// peer_is_silent returns true when the peer has been failing recently
// and the caller should fast-fail without attempting a libp2p connect.
// Bootstrap peers are always trusted infrastructure; never silenced.
// Self never silenced (in-process pipe can't fail).
func peer_is_silent(id string) bool {
	if id == "" || id == net_id || peer_is_bootstrap(id) {
		return false
	}
	peer_reachability_lock.Lock()
	defer peer_reachability_lock.Unlock()
	r, ok := peer_reachability[id]
	if !ok || r.ConsecutiveFailures < peer_silent_failure_threshold {
		return false
	}
	return now()-r.LastAttempt < peer_silent_skip_window
}

// peer_mark_send_success clears any silent state. Called when an
// outbound libp2p stream opens cleanly — by peer_stream on the
// /mochi/1 path and by peer_protocol_open on the /mochi/2 path. The
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
// the peer_connect=false branches in peer_stream (/mochi/1) and
// peer_protocol_open (/mochi/2) when the libp2p connect itself fails
// — that's the "peer is unreachable" signal we want to silence on.
// Transient stream-open errors after a successful connect don't count
// (the peer IS reachable; the failure is application- or
// protocol-level, handled separately).
func peer_mark_send_failed(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_reachability_lock.Lock()
	defer peer_reachability_lock.Unlock()
	r := peer_reachability[id]
	r.ConsecutiveFailures++
	r.LastAttempt = now()
	peer_reachability[id] = r
}

// peer_mark_reachable resets the silent cache without recording a
// stream-open success. Called from peer_connect's libp2p-success path
// so that rows woken by queue_resurrect_peer can actually attempt
// their sends rather than fast-failing on a stale silent flag.
//
// Without this, a peer that returned during the 60s silence window
// stays fast-failed on every outbound until peer_silent_skip_window
// lapses — even though queue_resurrect_peer has already pulled all
// deferred rows forward to now(). Each of those resurrected rows
// then sees peer_is_silent==true and returns errSenderUnreachable
// without ever opening the stream, so the backlog stalls for up to
// peer_silent_skip_window after the peer has demonstrably reconnected.
func peer_mark_reachable(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_reachability_lock.Lock()
	defer peer_reachability_lock.Unlock()
	delete(peer_reachability, id)
}
