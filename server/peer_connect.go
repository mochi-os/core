// Mochi server: Peer connection lifecycle.
//
// Owns peer_connect / peer_disconnected and everything they need:
// the libp2p-state machine transitions (via Peer.state in peers.go),
// the reconnect backoff manager, the disconnect-hook registry that
// /mochi/2 and future subsystems plug into, the publish/request
// pubsub plumbing for peer-discovery announcements, and the shutdown
// bye-and-drain sequence.
//
// The Peer registry itself (identity, addresses, peers.db
// persistence) lives in peers.go; the reachability silent-cache lives
// in peer_reachability.go.
//
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"math/rand/v2"
	"sync"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
)

func init() {
	a := app("peers")
	a.service("peers")
	a.event_anonymous("request", peer_request_event) // Unsigned pubsub broadcast
	a.event_anonymous("publish", peer_publish_event) // Unsigned pubsub broadcast
}

// Reconnection state for a disconnected peer.
type PeerReconnect struct {
	NextRetry int64
	Attempts  int
}

var (
	peer_reconnects     = map[string]PeerReconnect{}
	peer_reconnect_lock = &sync.Mutex{}

	peer_publish_chan = make(chan bool, 1) // buffer-1 so peer_request_event doesn't block on a slow publisher
)

// peer_disconnect_hooks fires once per disconnect, in registration
// order. Subsystems with per-peer state (the /mochi/2 protocol cache,
// the /mochi/2 Sender registry, future caches) self-register via
// peer_register_disconnect_hook in their init() so this file stays
// ignorant of /mochi/2 internals.
var (
	peer_disconnect_hooks      []func(string)
	peer_disconnect_hooks_lock sync.Mutex
)

// peer_register_disconnect_hook adds a callback that runs each time
// peer_disconnected fires. Hooks run synchronously in registration
// order. Use this for "tear down my per-peer state on disconnect" —
// typical examples: cache invalidation, in-flight goroutine shutdown,
// metric counters.
//
// Hooks must be cheap (they all run synchronously on the libp2p
// disconnect event dispatch path); offload anything expensive.
func peer_register_disconnect_hook(fn func(string)) {
	if fn == nil {
		return
	}
	peer_disconnect_hooks_lock.Lock()
	defer peer_disconnect_hooks_lock.Unlock()
	peer_disconnect_hooks = append(peer_disconnect_hooks, fn)
}

// Connect to a peer if possible. Call peer_add_known(),
// peer_discovered(), or peer_discovered_address() first.
//
// Uses the peer_state machine to prevent concurrent connect attempts
// for the same peer racing onto net_connect. A caller that finds the
// peer already in `connecting` returns false immediately rather than
// piling on; the in-flight goroutine will resolve the state. Callers
// that need a "wait for the connect" semantic must loop and retry.
func peer_connect(id string) bool {
	if id == net_id {
		return true
	}

	peers_lock.Lock()
	p, found := peers[id]
	if !found {
		peers_lock.Unlock()
		return false
	}
	switch p.state {
	case peer_state_connected:
		peers_lock.Unlock()
		return true
	case peer_state_connecting:
		peers_lock.Unlock()
		return false // another caller has it; don't race onto net_connect
	}
	p.state = peer_state_connecting
	peers[id] = p
	addrs := peer_address_strings(p.addresses)
	peers_lock.Unlock()

	ok := net_connect(id, addrs)

	peers_lock.Lock()
	p = peers[id]
	if ok {
		p.state = peer_state_connected
	} else {
		p.state = peer_state_disconnected
	}
	peers[id] = p
	peers_lock.Unlock()

	if ok {
		peer_refresh_connected_address(id)
		peer_reconnected(id)
		// Clear the silent-cache BEFORE resurrecting deferred rows.
		// queue_resurrect_peer pulls rows forward to now() so they
		// run on the next queue_process tick, but peer_protocol_open's
		// peer_is_silent fast-fail would short-circuit each one for
		// up to peer_silent_skip_window seconds (60s) after the
		// reconnect. Resetting reachability lets the resurrected rows
		// actually trial the new connection.
		peer_mark_reachable(id)
		// Any queue rows deferred by queue_process's silent-peer
		// pre-filter (1h next_retry push when peer_is_silent) become
		// ready immediately. Without this the backlog waits out the
		// deferral despite the peer being back.
		queue_resurrect_peer(id)
	}

	return ok
}

// Refresh the timestamp of the address we actually connected on.
func peer_refresh_connected_address(id string) {
	pid, err := p2p_peer.Decode(id)
	if err != nil {
		return
	}

	conns := net_me.Network().ConnsToPeer(pid)
	if len(conns) == 0 {
		return
	}

	t := now()
	addr := conns[0].RemoteMultiaddr().String() + "/p2p/" + id

	peers_lock.Lock()
	if p, found := peers[id]; found {
		for i, a := range p.addresses {
			if a.Address == addr {
				p.addresses[i].Updated = t
				peers[id] = p
				break
			}
		}
	}
	peers_lock.Unlock()

	db := db_open("db/peers.db")
	db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, addr, t)
}

// Peer has become disconnected.
func peer_disconnected(id string) {
	if id == "" {
		return
	}
	debug("Peer %q disconnected", id)

	peers_lock.Lock()
	if p, found := peers[id]; found {
		p.state = peer_state_disconnected
		peers[id] = p
	}
	peers_lock.Unlock()

	peer_disconnect_hooks_lock.Lock()
	hooks := peer_disconnect_hooks
	peer_disconnect_hooks_lock.Unlock()
	for _, fn := range hooks {
		fn(id)
	}

	peer_schedule_reconnect(id)
}

// peer_schedule_reconnect adds id to peer_reconnects[] with an initial
// retry delay if not already scheduled. Two callers:
//
//   - peer_disconnected (above): libp2p reports a peer we were
//     connected to has gone away.
//   - peer_mark_send_failed (peer_reachability.go) when crossing the
//     silent-failure threshold: a peer we couldn't open a stream to
//     enough times in a row is treated the same as one that
//     disconnected, so peer_reconnect_manager probes it periodically.
//
// Without the second path, a peer we discovered via DHT but never
// successfully connected to would stay silent forever — peer_is_silent
// is durable (no time-based lapse), and only peer_reconnect_manager's
// successful probe (which goes through peer_connect → peer_mark_reachable)
// can clear silence. Self and empty id are no-ops.
func peer_schedule_reconnect(id string) {
	if id == "" || id == net_id {
		return
	}
	peer_reconnect_lock.Lock()
	if _, scheduled := peer_reconnects[id]; !scheduled {
		delay := int64(10) + rand.Int64N(5) // 10-14 seconds initial delay with jitter
		peer_reconnects[id] = PeerReconnect{NextRetry: now() + delay, Attempts: 0}
	}
	peer_reconnect_lock.Unlock()
}

// Clear reconnection state for a peer (called when peer connects by any means).
func peer_reconnected(id string) {
	peer_reconnect_lock.Lock()
	delete(peer_reconnects, id)
	peer_reconnect_lock.Unlock()
}

// peer_reconnect_parallel caps how many reconnect attempts can run
// concurrently. Each attempt can block for the full libp2p TCP-connect
// timeout (~10s) on an unreachable peer, so serial-3-per-tick (the
// previous limit) is too slow at scale — 100 disconnected peers take
// 5+ minutes to retry each. 20 parallel attempts × 10s timeout =
// 100 attempts/minute worst case, still bounded by libp2p resource
// limits.
const peer_reconnect_parallel = 20

// Reconnect to disconnected peers with exponential backoff. Per-tick:
// scan peer_reconnects for entries whose NextRetry is due, fire each
// in its own goroutine through a semaphore. Goroutines update
// per-peer backoff state on failure under peer_reconnect_lock.
func peer_reconnect_manager() {
	sem := make(chan struct{}, peer_reconnect_parallel)
	for range time.Tick(10 * time.Second) {
		t := now()
		var ready []string

		peer_reconnect_lock.Lock()
		for id, r := range peer_reconnects {
			if r.NextRetry <= t {
				ready = append(ready, id)
			}
		}
		peer_reconnect_lock.Unlock()

		for _, id := range ready {
			sem <- struct{}{}
			go func(id string) {
				defer func() { <-sem }()
				if peer_connect(id) {
					debug("Peer %q reconnected successfully", id)
					return
				}
				// Backoff: 10s, 20s, 40s, 80s, 160s, 300s (capped).
				peer_reconnect_lock.Lock()
				r := peer_reconnects[id]
				r.Attempts++
				delay := int64(10) << min(r.Attempts, 5)
				if delay > 300 {
					delay = 300
				}
				delay += rand.Int64N(delay/4 + 1) // 0-25% jitter
				r.NextRetry = now() + delay
				peer_reconnects[id] = r
				peer_reconnect_lock.Unlock()
			}(id)
		}
	}
}

// Publish our own information to the pubsub regularly or when requested.
func peers_publish() {
	for {
		message("", "", "peers", "publish").publish(false)

		select {
		case <-peer_publish_chan:
			debug("Peer publish requested")
		case <-time.After(time.Hour):
			debug("Peer routine publish")
		}
	}
}

// Received a peer publish event from another server. We don't need to
// do anything here because the pubsub manager already marked the peer as
// discovered on receipt.
func peer_publish_event(e *Event) {
}

// Reply to a peer request if for us. Non-blocking — if a publish is
// already pending the second request collapses with it.
func peer_request_event(e *Event) {
	if e.get("id", "") != net_id {
		return
	}
	select {
	case peer_publish_chan <- true:
	default:
	}
}

// Notify peers of shutdown (best effort). Every open /mochi/2/messages
// Sender gets a `bye` frame on its existing stream, then we wait for
// in-flight to drain (capped by peers_shutdown_drain_timeout) before
// forcing the close.
func peers_shutdown() {
	senders_bye_all(peers_shutdown_drain_timeout)
}

// peers_shutdown_drain_timeout — how long peers_shutdown waits for
// senders' inflight to drain on bye. Long enough for most inflight to
// ack on a healthy link; short enough not to delay shutdown noticeably.
var peers_shutdown_drain_timeout = 5 * time.Second
