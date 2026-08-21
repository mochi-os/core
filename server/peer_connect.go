// Mochi server: Peer connection lifecycle - connect/disconnect, the reconnect
// backoff manager, the disconnect-hook registry, and the peer-discovery pubsub.
// The registry is peers.go; the reachability silent-cache is
// peer_reachability.go.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// peers_init registers the built-in peers app and its handlers. Called from
// main_serve, not a package init(): registration has a defined startup
// position.
func peers_init() {
	a := app("peers")
	a.service("peers")
	a.event_anonymous("request", peer_request_event) // Unsigned pubsub broadcast
	a.event_anonymous("publish", peer_publish_event) // Unsigned pubsub broadcast
	a.event_anonymous("record", peer_record_event)   // Unsigned pubsub broadcast; record self-certifies
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

// peer_disconnect_hooks fires once per disconnect, in registration order.
// Subsystems with per-peer state self-register via
// peer_register_disconnect_hook.
var (
	peer_disconnect_hooks      []func(string)
	peer_disconnect_hooks_lock sync.Mutex
)

// peer_register_disconnect_hook adds a callback run on each peer_disconnected.
// Hooks run synchronously on the libp2p disconnect dispatch path, so keep them
// cheap and offload anything expensive.
func peer_register_disconnect_hook(fn func(string)) {
	if fn == nil {
		return
	}
	peer_disconnect_hooks_lock.Lock()
	defer peer_disconnect_hooks_lock.Unlock()
	peer_disconnect_hooks = append(peer_disconnect_hooks, fn)
}

// Connect to a peer if possible. Call peer_add_known(), peer_discovered() or
// peer_discovered_address() first. Returns false at once if a connect is
// already in flight, so a caller needing "wait for the connect" must loop and
// retry.
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

	if !ok {
		peer_addresses_failed(id)
	}

	if ok {
		peer_refresh_connected_address(id)
		peer_reconnected(id)
		// Clear the silent cache before resurrecting deferred rows, or peer_is_silent
		// short-circuits every resurrected row for up to peer_silent_skip_window.
		peer_mark_reachable(id)
		// Any queue rows deferred by queue_process's silent-peer
		// pre-filter (1h next_retry push when peer_is_silent) become
		// ready immediately. Without this the backlog waits out the
		// deferral despite the peer being back.
		queue_resurrect_peer(id)
	}

	return ok
}

// peer_connect_retry dials a peer and, on failure, enrolls it in the reconnect
// manager's backoff probes. Startup dials need this: every other enrollment
// trigger requires having already reached the peer.
func peer_connect_retry(id string) {
	if !peer_connect(id) {
		peer_schedule_reconnect(id)
	}
}

// Refresh the address we actually connected on, recording the success —
// the evidence that protects it from cap eviction and early pruning.
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
		peer_address_insert(&p, addr, t)
		for i, a := range p.addresses {
			if a.Address == addr {
				p.addresses[i].Success = t
				p.addresses[i].Failure = 0
				break
			}
		}
		peers[id] = p
	}
	peers_lock.Unlock()

	db := db_open("db/peers.db")
	db.exec("insert into peers ( id, address, updated, success ) values ( ?, ?, ?, ? ) on conflict ( id, address ) do update set updated=excluded.updated, success=excluded.success, failure=0", id, addr, t, t)
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

	// A replication member dropping at the libp2p level is the event-driven
	// "offline" signal that fires even when there is no traffic to it - the
	// gap the send-failure stamp alone misses for an idle member.

	peer_schedule_reconnect(id)
}

// peer_schedule_reconnect adds id to peer_reconnects[] with an initial retry
// delay if not already scheduled; self and empty id are no-ops. Silence is
// durable, so enrollment here is the only path back for a peer never reached.
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
	// Reachable again at the libp2p level: clear the offline mark so an idle
	// member that reconnects without resuming traffic doesn't keep showing
	// the offline badge.
}

// peer_reconnect_parallel caps concurrent reconnect attempts. Each can block
// for the full libp2p connect timeout (~10s), so serial retries do not scale.
const peer_reconnect_parallel = 20

// Reconnect to disconnected peers with exponential backoff. Per-tick:
// scan peer_reconnects for entries whose NextRetry is due, fire each
// in its own goroutine through a semaphore. Goroutines update
// per-peer backoff state on failure under peer_reconnect_lock.
func peer_reconnect_manager() {
	semaphore := make(chan struct{}, peer_reconnect_parallel)
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
			semaphore <- struct{}{}
			go func(id string) {
				defer func() { <-semaphore }()
				if peer_connect(id) {
					debug("Peer %q reconnected successfully", id)
					// Re-ship any retained journal ops the peer missed while
					// it was gone (#23). Receiver dedups what it already has.
					return
				}
				// The peer may be unreachable because our addresses for
				// it are stale (or were never known); ask the mesh for
				// fresh ones alongside the backoff probe.
				peer_request_addresses(id)
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

// peers_publish_minimum_interval throttles how often the publish loop
// fires: a flood of peers/request broadcasts naming this server (or a
// burst of local address changes) collapses into one publish per
// interval instead of one per request.
const peers_publish_minimum_interval = 30 * time.Second

// peers_publish_addresses_maximum caps how many addresses one publish carries
// and how many a receiver applies, bounding what a hostile publisher can push
// into receivers' peers.db.
const peers_publish_addresses_maximum = 16

// Publish our identity and dialable addresses to the pubsub: regularly, on
// request, and when our address set changes (net_watch_addresses). This is how
// a server that knows us only by peer id learns where to dial.
func peers_publish() {
	for {
		m := message("", "", "peers", "publish")
		if addresses := net_addresses(); len(addresses) > 0 {
			if len(addresses) > peers_publish_addresses_maximum {
				addresses = addresses[:peers_publish_addresses_maximum]
			}
			m.set("addresses", strings.Join(addresses, ","))
		}
		// The signed record is the authoritative, replay-protected,
		// relayable form of the same addresses; the plain list above
		// stays for receivers that predate it.
		if record := peer_record_announce(); record != "" {
			m.set("record", record)
		}
		// Advertise that we relay so NAT'd peers can reserve a slot, plus
		// our reservation load (0-100) so they prefer a relay with headroom.
		if relay_enabled() {
			m.set("relay", "true")
			m.set("relay/load", itoa(relay_load_percent()))
		}
		if name := peer_names_announce(); name != "" {
			m.set("name", name)
		}
		m.publish(false)

		select {
		case <-peer_publish_chan:
			debug("Peer publish requested")
		case <-time.After(time.Hour):
			debug("Peer routine publish")
		}
		time.Sleep(peers_publish_minimum_interval)
	}
}

// peers_publish_request nudges the publish loop. Non-blocking — if a
// publish is already pending the second request collapses with it.
func peers_publish_request() {
	select {
	case peer_publish_chan <- true:
	default:
	}
}

// Merge an announcing peer's addresses, preferring its signed record
// (self-certifying, replay-protected) over the plain list (trusted only via the
// StrictSign-verified origin). An empty origin is a direct-stream spoof.
func peer_publish_event(e *Event) {
	if e.origin == "" || e.origin == net_id {
		return
	}

	// Names apply independently of addresses: a publish with no claims from a peer
	// that previously claimed one clears it.
	var names []string
	if n := strings.ToLower(strings.TrimSpace(e.get("name", ""))); n != "" && peer_name_valid(n) {
		names = append(names, n)
	}
	peer_names_apply(e.origin, names)

	// Note whether this peer offers relay, for AutoRelay candidate
	// selection. Not security-sensitive: a false claim just fails to
	// grant a reservation.
	if e.get("relay", "") == "true" {
		peer_relay_seen(e.origin, int(atoi(e.get("relay/load", "0"), 0)))
	}

	// Prefer the signed record: self-certifying and replay-protected.
	// Fall back to the plain address list when absent or unverifiable,
	// so peers that predate signed records still publish addresses.
	addresses, ok := peer_record_apply(e.origin, e.get("record", ""))
	if !ok {
		announced := e.get("addresses", "")
		if announced == "" {
			return
		}
		addresses = strings.Split(announced, ",")
	}
	peer_apply_addresses(e.origin, addresses)
}

// peer_apply_addresses merges discovered addresses through the receive-side
// hygiene shared by direct announcements and relayed records.
func peer_apply_addresses(id string, addresses []string) {
	applied := 0
	for _, address := range addresses {
		if applied >= peers_publish_addresses_maximum {
			break
		}
		address = strings.TrimSpace(address)
		ma, err := multiaddr.NewMultiaddr(address)
		if err != nil {
			continue
		}
		information, err := p2p_peer.AddrInfoFromP2pAddr(ma)
		if err != nil || information.ID.String() != id {
			continue
		}
		// A circuit address relaying through ourselves is dead weight: we hold the
		// direct connection it reserved. It stays valid for every other peer.
		if net_id != "" && strings.Contains(address, "/p2p/"+net_id+"/p2p-circuit") {
			continue
		}
		debug("Peer %q discovered at address %q", id, address)
		peer_discovered_address(id, address)
		applied++
	}
}

// Reply to a peers/request: republish ourselves if it names us, else relay that
// peer's signed record if we hold one. Check the origin first - peers/* only
// arrives over pubsub, so an unverified event must not trigger our announce.
func peer_request_event(e *Event) {
	if e.origin == "" || e.origin == net_id {
		return
	}
	id := e.get("id", "")
	if id == "" {
		return
	}
	if id == net_id {
		peers_publish_request()
		return
	}
	peer_record_relay(id)
}

// A relayed signed record vouches for a third party's addresses. Trust is in
// the record's own signature, so the carrier's identity is irrelevant.
func peer_record_event(e *Event) {
	id, addresses, sequence, data, ok := peer_record_verify(e.get("record", ""))
	if !ok || id == net_id {
		return
	}
	// Only a non-stale answer suppresses our relay: signed records never expire,
	// so a replayed old one would otherwise silence every legitimate relay.
	if peer_record_current(id, sequence) {
		peer_record_seen(id)
	}
	if !peer_record_store(id, sequence, data) {
		return
	}
	peer_apply_addresses(id, addresses)
}

// peer_request_addresses broadcasts a peers/request asking a peer to publish
// itself - the recovery path for a peer known only by id or at stale addresses.
// Rate limited per target. Returns whether a request was broadcast.
func peer_request_addresses(id string) bool {
	if id == "" || id == net_id {
		return false
	}
	if !rate_limit_peer_request.allow(id) {
		return false
	}
	debug("Peer %q addresses unknown or unreachable; requesting publish", id)
	message("", "", "peers", "request").set("id", id).publish(false)
	return true
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
