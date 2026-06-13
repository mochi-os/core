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
	"strings"
	"sync"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

func init() {
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

	if !ok {
		peer_addresses_failed(id)
	}

	if ok {
		peer_refresh_connected_address(id)
		peer_reconnected(id)
		// Fresh authenticated evidence: re-verify stale name claims now.
		peer_names_connected(id)
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

// peer_connect_retry dials a peer and, on failure, enrolls it in the
// reconnect manager's backoff probes. Startup dials (the bootstrap list
// and the peers.db restore) use this instead of bare peer_connect: a
// server that boots before its network is ready fails every initial
// dial, and without enrollment nothing ever retries — the reconnect
// machinery's other triggers (a libp2p disconnect, the silent-failure
// threshold) both require having reached the peer or having traffic for
// it, so a never-connected idle server stays isolated until restart.
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
	replication_member_unreachable(id)

	peer_schedule_reconnect(id)
}

// peer_schedule_reconnect adds id to peer_reconnects[] with an initial
// retry delay if not already scheduled. Three callers:
//
//   - peer_disconnected (above): libp2p reports a peer we were
//     connected to has gone away.
//   - peer_mark_send_failed (peer_reachability.go) when crossing the
//     silent-failure threshold: a peer we couldn't open a stream to
//     enough times in a row is treated the same as one that
//     disconnected, so peer_reconnect_manager probes it periodically.
//   - peer_connect_retry (above): a startup dial failed, typically
//     because the server booted before its network was ready.
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
	// Reachable again at the libp2p level: clear the offline mark so an idle
	// member that reconnects without resuming traffic doesn't keep showing
	// the offline badge.
	replication_member_reachable(id)
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

// peers_publish_addresses_maximum caps how many addresses one publish
// carries and how many a receiver applies from one event. Generous —
// a host with several interfaces plus observed and relay addresses
// stays comfortably under it — while bounding what a hostile publisher
// can push into receivers' peers.db.
const peers_publish_addresses_maximum = 16

// Publish our own information — identity plus dialable addresses — to
// the pubsub regularly, when another server requests it, or when our
// address set changes (net_watch_addresses). The addresses are how a
// server that knows this server only by peer id (mochictl replica join,
// any bare-peer-id send) becomes able to dial it: receivers verify the
// pubsub envelope names us as originator and merge the addresses into
// their peer registry.
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
		// Advertise that we relay so NAT'd peers can reserve a slot.
		if relay_enabled() {
			m.set("relay", "true")
		}
		if name, domains := peer_names_announce(); name != "" || domains != "" {
			if name != "" {
				m.set("name", name)
			}
			if domains != "" {
				m.set("domains", domains)
			}
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

// Received a peer publish event from another server: merge the
// originator's announced addresses into the peer registry. Two trust
// roots, preferred in order: a signed peer record (self-certifying —
// its own libp2p signature proves the addresses, with sequence-based
// replay rejection), then the plain address list (trusted via the
// GossipSub envelope, which StrictSign-verifies the originating peer
// into e.origin). A direct-stream message spoofing this event has no
// origin and is ignored, as is any address whose /p2p/ suffix names a
// different peer.
func peer_publish_event(e *Event) {
	if e.origin == "" || e.origin == net_id {
		return
	}

	// Claimed names apply (or clear) independently of addresses: a
	// publish with no claims from a peer that previously claimed names
	// means its operator turned announcements off — honor it.
	var names []string
	if n := strings.ToLower(strings.TrimSpace(e.get("name", ""))); n != "" && peer_name_valid(n) {
		names = append(names, n)
	}
	for _, d := range strings.Split(e.get("domains", ""), ",") {
		if len(names) >= peer_names_maximum {
			break
		}
		d = strings.ToLower(strings.TrimSpace(d))
		if d != "" && peer_name_valid(d) {
			names = append(names, d)
		}
	}
	peer_names_apply(e.origin, names)

	// Note whether this peer offers relay, for AutoRelay candidate
	// selection. Not security-sensitive: a false claim just fails to
	// grant a reservation.
	if e.get("relay", "") == "true" {
		peer_relay_seen(e.origin)
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

// peer_apply_addresses merges discovered addresses for a peer through
// the receive-side hygiene shared by direct announcements and relayed
// records: cap the count, drop entries whose /p2p/ suffix names a
// different peer, and reject loopback or unspecified addresses (junk for
// every receiver — the same-host peers they'd be valid for learn them
// over mDNS, not the mesh).
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
		info, err := p2p_peer.AddrInfoFromP2pAddr(ma)
		if err != nil || info.ID.String() != id {
			continue
		}
		if net_unroutable(ma) {
			continue
		}
		debug("Peer %q discovered at address %q", id, address)
		peer_discovered_address(id, address)
		applied++
	}
}

// Reply to a peers/request. If it names us, republish ourselves
// (non-blocking — a pending publish collapses with it). If it names a
// peer we are not, and we hold that peer's signed record, relay it on
// their behalf — the address-book-exchange path that lets a server find
// a peer that is offline or never heard the request.
func peer_request_event(e *Event) {
	id := e.get("id", "")
	if id == "" {
		return
	}
	if id == net_id {
		peers_publish_request()
		return
	}
	if e.origin == "" || e.origin == net_id {
		return
	}
	peer_record_relay(id)
}

// Received a relayed signed record: some server is vouching for a
// third party's addresses by carrying that peer's record. Trust is in
// the record's own signature, not the carrier, so the relayer's
// identity is irrelevant — we verify the record, apply it to the peer
// it names (with the same replay and hygiene guards as a direct
// announcement), and note the answer so our own relay suppresses.
func peer_record_event(e *Event) {
	id, addresses, sequence, data, ok := peer_record_verify(e.get("record", ""))
	if !ok || id == net_id {
		return
	}
	peer_record_seen(id)
	if !peer_record_store(id, sequence, data) {
		return
	}
	peer_apply_addresses(id, addresses)
}

// peer_request_addresses broadcasts a peers/request asking the named
// peer to publish itself — the recovery path for sending to a peer we
// know only by id (a replica joining a pair source it has never met) or
// whose stored addresses have gone stale. The target answers with a
// peers/publish carrying its addresses; peer_publish_event applies them
// and the queued messages deliver on the next wake. Rate limited per
// target so the queue retrying an unreachable peer doesn't flood the
// mesh. Returns whether a request was broadcast.
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
