// Mochi server: Peers
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

type Peer struct {
	ID        string
	Updated   int64 // throttles peers.db saves to at most once per hour per peer
	addresses []PeerAddress
	connected bool
}

// PeerAddress tracks a peer's address with a last-seen timestamp
type PeerAddress struct {
	Address string
	Updated int64
}

// peer_row is the sqlx scan target for `select id, address, updated
// from peers ...` — one row per (peer, address) tuple. Kept separate
// from Peer so the in-memory Peer struct doesn't carry a singular
// `Address` field that's only meaningful during SQL scanning.
type peer_row struct {
	ID      string
	Address string
	Updated int64
}

// peer_address_strings extracts address strings from a slice of PeerAddress
func peer_address_strings(addrs []PeerAddress) []string {
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.Address
	}
	return out
}

const (
	peers_minimum          = 1
	peer_maximum_addresses = 20
)

// Reconnection state for a disconnected peer
type PeerReconnect struct {
	NextRetry int64
	Attempts  int
}

// PeerReachability is the in-memory fast-fail signal for outbound
// sends. Without it every queue_process goroutine and every
// /mochi/2/messages sender_open call independently pays the unbounded
// libp2p connect timeout on `net_me.Connect()` when the target peer is
// offline. Three consecutive failures within the skip window mean
// "skip the libp2p attempt and return nil immediately" — the queue row
// stays pending under the usual exponential backoff, the Sender open
// returns errSenderUnreachable, and the pull_loop just polls until the
// silence ages out.
//
// Both /mochi/1 (peer_stream) and /mochi/2 (peer_protocol_open) feed
// this cache; one reachability oracle covers both protocols.
//
// Not persisted: a server restart starts every peer with zero failures
// recorded, so every peer gets a fresh trial. The map is bounded by
// the number of distinct peers we've ever queued for in this process;
// GC pressure is negligible (a few thousand entries at most on a busy
// server).
type PeerReachability struct {
	ConsecutiveFailures int
	LastAttempt         int64
}

// peer_silent_failure_threshold is the number of consecutive failed
// stream-opens before peer_is_silent starts returning true. Three is
// conservative — transient blips (one missed packet, a router
// reboot, an in-progress reconnect) don't silence the peer.
const peer_silent_failure_threshold = 3

// peer_silent_skip_window is how long after a failed attempt the peer
// stays silent before we'll trial another send. Short enough that a
// reconnecting peer drains its backlog within a couple of minutes;
// long enough that wasted libp2p timeouts are amortised.
const peer_silent_skip_window = 60

// peer_default_publisher_hardcoded is the fallback publisher peer ID
// when mochi.conf doesn't override [publisher] peer. Wasabi's libp2p
// id; serves the published-app catalogue.
const peer_default_publisher_hardcoded = "12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH"

// bootstrap_addresses_hardcoded is the fallback list of bootstrap
// multiaddresses when mochi.conf doesn't override [bootstrap]
// addresses. Comma-separated multiaddrs; each includes /p2p/<peer-id>
// so the peer identity is recoverable. Out-of-the-box installs need
// at least one reachable bootstrap to discover the wider network.
const bootstrap_addresses_hardcoded = "/ip4/217.182.75.108/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /ip6/2001:41d0:601:1100::61f7/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH"

var (
	// peer_default_publisher + peers_bootstrap start at the hardcoded
	// defaults so package init() and tests that read them before
	// net_start see usable values. peers_bootstrap_load() reruns at
	// startup and replaces them with whatever mochi.conf specifies (or
	// the same hardcoded defaults if unset).
	peer_default_publisher = peer_default_publisher_hardcoded
	peers_bootstrap        = bootstrap_addresses_parse(bootstrap_addresses_hardcoded)
	peers                  map[string]Peer = map[string]Peer{}
	peer_publish_chan                      = make(chan bool, 1) // buffer-1 so peer_request_event doesn't block on a slow publisher
	peers_lock                             = &sync.Mutex{}
	peer_reconnects                        = map[string]PeerReconnect{}
	peer_reconnect_lock                    = &sync.Mutex{}
	peer_reachability                      = map[string]PeerReachability{}
	peer_reachability_lock                 = &sync.Mutex{}
)

// peer_is_silent returns true when the peer has been failing recently
// and the caller should fast-fail without attempting a libp2p
// connect. Bootstrap peers are always trusted infrastructure; never
// silenced. Self never silenced (in-process pipe can't fail).
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

func init() {
	a := app("peers")
	a.service("peers")
	a.event_anonymous("request", peer_request_event) // Unsigned pubsub broadcast
	a.event_anonymous("publish", peer_publish_event) // Unsigned pubsub broadcast
}

// bootstrap_addresses_parse turns a comma-separated list of multiaddrs
// (each carrying its /p2p/<id> suffix) into a slice of Peer entries,
// grouping addresses that share a peer id. Invalid entries log a
// warning and are skipped. Caller is responsible for shuffling.
func bootstrap_addresses_parse(list string) []Peer {
	parts := strings.Split(list, ",")
	grouped := map[string][]PeerAddress{}
	order := []string{}
	for _, entry := range parts {
		addr := strings.TrimSpace(entry)
		if addr == "" {
			continue
		}
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			warn("Bootstrap: invalid multiaddress %q: %v", addr, err)
			continue
		}
		info, err := p2p_peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			warn("Bootstrap: cannot extract peer id from %q: %v", addr, err)
			continue
		}
		id := info.ID.String()
		if _, seen := grouped[id]; !seen {
			order = append(order, id)
		}
		grouped[id] = append(grouped[id], PeerAddress{Address: addr})
	}
	out := make([]Peer, 0, len(order))
	for _, id := range order {
		out = append(out, Peer{ID: id, addresses: grouped[id]})
	}
	return out
}

// peers_bootstrap_load reloads peers_bootstrap + peer_default_publisher
// from mochi.conf, falling back to the hardcoded defaults. Called from
// net_start after ini_load so operator overrides take effect; the
// package-level `var` form is the same defaults so anything that reads
// peers_bootstrap before net_start (tests, package init) still sees a
// working list.
//
// Each entry in [bootstrap] addresses is a full libp2p multiaddress
// including /p2p/<id>, so the single config option carries both
// address and identity. Same multi-address-per-peer grouping as the
// hardcoded form. [publisher] peer overrides the default publisher
// peer id used by app version checks.
func peers_bootstrap_load() {
	peer_default_publisher = ini_string("publisher", "peer", peer_default_publisher_hardcoded)

	raw := ini_strings_commas("bootstrap", "addresses")
	list := bootstrap_addresses_hardcoded
	if len(raw) > 0 {
		list = strings.Join(raw, ",")
	}
	peers_bootstrap = bootstrap_addresses_parse(list)

	// Shuffle so different servers attempt bootstrap peers in different
	// orders, spreading the initial-connect load across them.
	rand.Shuffle(len(peers_bootstrap), func(i, j int) {
		peers_bootstrap[i], peers_bootstrap[j] = peers_bootstrap[j], peers_bootstrap[i]
	})
}

// peer_is_bootstrap returns true if the peer ID is a bootstrap peer
func peer_is_bootstrap(id string) bool {
	for _, p := range peers_bootstrap {
		if p.ID == id {
			return true
		}
	}
	return false
}

// peer_is_pair returns true if the peer ID is in the local pair set.
// Pair members are our own infrastructure (whole-server replication
// partners we explicitly chose to pair with) — the inbound stream
// rate limit is anti-DoS for unknown peers and shouldn't throttle
// them. During bulk bootstrap the file-scope driver can legitimately
// fire >100 chunk-fetch streams per second on a fast local network,
// and rate-limiting them stalls the bootstrap with a flood of
// "Net rate limited peer" log lines.
func peer_is_pair(id string) bool {
	if id == "" {
		return false
	}
	rdb := db_open("db/replication.db")
	exists, _ := rdb.exists("select 1 from pair where peer=?", id)
	return exists
}

// Add some peers we already know about from the database
func peers_add_from_db(limit int) {
	var ps []peer_row
	db := db_open("db/peers.db")
	err := db.scans(&ps, "select id from peers group by id order by updated desc limit ?", limit)
	if err != nil {
		warn("Database error loading peers: %v", err)
		return
	}
	for _, p := range ps {
		var addresses []string
		var as []peer_row
		err := db.scans(&as, "select address from peers where id=?", p.ID)
		if err != nil {
			warn("Database error loading addresses for peer %q: %v", p.ID, err)
			continue
		}
		for _, a := range as {
			addresses = append(addresses, a.Address)
		}
		debug("Adding database peer %q at %v", p.ID, addresses)
		peer_add_known(p.ID, addresses)
		go peer_connect(p.ID)
	}
}

// Add already known peer to memory, merging any new addresses with
// the existing entry. Previously this returned early when the peer was
// already in memory, silently dropping any newly-discovered addresses
// (e.g. a known peer reachable on a new IP via a different discovery
// path). Now matches peer_discovered_work's merge semantics — caps the
// per-peer address list at peer_maximum_addresses, replacing the
// oldest when full.
func peer_add_known(id string, addresses []string) {
	peers_lock.Lock()
	defer peers_lock.Unlock()

	t := now()
	p, found := peers[id]
	if !found {
		p = Peer{ID: id, connected: false}
	}
	for _, addr := range addresses {
		exists := false
		for i, a := range p.addresses {
			if a.Address == addr {
				exists = true
				p.addresses[i].Updated = t
				break
			}
		}
		if exists {
			continue
		}
		pa := PeerAddress{Address: addr, Updated: t}
		if len(p.addresses) < peer_maximum_addresses {
			p.addresses = append(p.addresses, pa)
			continue
		}
		oldest := 0
		for i, a := range p.addresses {
			if a.Updated < p.addresses[oldest].Updated {
				oldest = i
			}
		}
		p.addresses[oldest] = pa
	}
	peers[id] = p
}

// Get details of a peer, either from memory, or from database
func peer_by_id(id string) *Peer {
	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()
	if found {
		return &p
	}

	// Load from database
	p = Peer{ID: id, connected: false}

	var ps []peer_row
	db := db_open("db/peers.db")
	err := db.scans(&ps, "select * from peers where id=?", id)
	if err != nil {
		warn("Database error looking up peer %q: %v", id, err)
		return nil
	}
	if len(ps) == 0 {
		//debug("Peer %q not found in database", id)
		return nil
	}
	for _, a := range ps {
		debug("Peer %q adding address %q from database", id, a.Address)
		p.addresses = append(p.addresses, PeerAddress{Address: a.Address, Updated: a.Updated})
	}

	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	debug("Adding database peer %q at %v", id, p.addresses)
	return &p
}

// Connect to a peer if possible
// Call peer_add_known(), peer_discovered(), or peer_discovered_address() before calling peer_connect()
func peer_connect(id string) bool {
	if id == net_id {
		return true
	}

	peers_lock.Lock()
	p, found := peers[id]
	peers_lock.Unlock()

	if !found {
		return false
	}

	if p.connected {
		return true
	}
	p.connected = net_connect(id, peer_address_strings(p.addresses))

	// Refresh the timestamp of the address we actually connected on
	if p.connected {
		peer_refresh_connected_address(id)
		peer_reconnected(id)
		// Any queue rows deferred by queue_process's silent-peer
		// pre-filter (1h next_retry push when peer_is_silent) become
		// ready immediately. Without this the backlog waits out the
		// deferral despite the peer being back.
		queue_resurrect_peer(id)
	}

	peers_lock.Lock()
	peers[id] = p
	peers_lock.Unlock()

	return p.connected
}

// Refresh the timestamp of the address we actually connected on
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

// peer_disconnect_hooks fires once per disconnect, in registration
// order. Subsystems with per-peer state (the /mochi/2 protocol cache,
// the /mochi/2 Sender registry, future caches) self-register via
// peer_register_disconnect_hook in their init() so peers.go doesn't
// have to know about them.
var (
	peer_disconnect_hooks      []func(string)
	peer_disconnect_hooks_lock sync.Mutex
)

// peer_register_disconnect_hook adds a callback that runs each time
// peer_disconnected fires. Hooks run synchronously in the order they
// were registered. Use this for "tear down my per-peer state on
// disconnect" — typical examples: cache invalidation, in-flight
// goroutine shutdown, metric counters.
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

// Peer has become disconnected
func peer_disconnected(id string) {
	if id == "" {
		return
	}
	debug("Peer %q disconnected", id)

	peers_lock.Lock()
	if p, found := peers[id]; found {
		p.connected = false
		peers[id] = p
	}
	peers_lock.Unlock()

	peer_disconnect_hooks_lock.Lock()
	hooks := peer_disconnect_hooks
	peer_disconnect_hooks_lock.Unlock()
	for _, fn := range hooks {
		fn(id)
	}

	// Schedule reconnection if not already scheduled
	peer_reconnect_lock.Lock()
	if _, scheduled := peer_reconnects[id]; !scheduled {
		delay := int64(10) + rand.Int64N(5) // 10-14 seconds initial delay with jitter
		peer_reconnects[id] = PeerReconnect{NextRetry: now() + delay, Attempts: 0}
		//debug("Peer %q scheduled for reconnection in %ds", id, delay)
	}
	peer_reconnect_lock.Unlock()
}

// Clear reconnection state for a peer (called when peer connects by any means)
func peer_reconnected(id string) {
	peer_reconnect_lock.Lock()
	delete(peer_reconnects, id)
	peer_reconnect_lock.Unlock()
}

// New or existing peer discovered or re-discovered at unknown address
func peer_discovered(id string) {
	p, err := p2p_peer.Decode(id)
	if err != nil {
		return
	}

	for _, a := range net_me.Peerstore().Addrs(p) {
		peer_discovered_work(id, a.String()+"/p2p/"+id)
	}

	go queue_check_peer(id)
}

// New or existing peer discovered or re-discovered at known address
func peer_discovered_address(id string, address string) {
	peer_discovered_work(id, address)
	go queue_check_peer(id)
}

// Do the work for the above two functions
func peer_discovered_work(id string, address string) {
	t := now()
	save := false

	peers_lock.Lock()
	p, found := peers[id]

	if found {
		exists := false
		for i, a := range p.addresses {
			if a.Address == address {
				exists = true
				p.addresses[i].Updated = t
				break
			}
		}
		if !exists {
			pa := PeerAddress{Address: address, Updated: t}
			if len(p.addresses) < peer_maximum_addresses {
				p.addresses = append(p.addresses, pa)
			} else {
				// Replace the oldest address
				oldest := 0
				for i, a := range p.addresses {
					if a.Updated < p.addresses[oldest].Updated {
						oldest = i
					}
				}
				p.addresses[oldest] = pa
			}
		}

		if p.Updated < t-int64(3600) {
			save = true
			p.Updated = t
		}
	} else {
		p = Peer{ID: id, addresses: []PeerAddress{{Address: address, Updated: t}}, Updated: t}
		save = true
	}

	peers[id] = p
	peers_lock.Unlock()

	if save {
		db := db_open("db/peers.db")
		db.exec("replace into peers ( id, address, updated ) values ( ?, ?, ? )", id, address, t)
	}
}

// Clean up stale peers
func peers_manager() {
	for range time.Tick(24 * time.Hour) {
		expiry := now() - 14*86400

		// Prune stale addresses from the database
		db := db_open("db/peers.db")
		db.exec("delete from peers where updated<?", expiry)

		// Prune stale addresses from memory
		peers_lock.Lock()
		for id, p := range peers {
			// Collect bootstrap addresses for this peer so we never prune them
			bootstrap := map[string]bool{}
			for _, bp := range peers_bootstrap {
				if bp.ID == id {
					for _, a := range bp.addresses {
						bootstrap[a.Address] = true
					}
					break
				}
			}

			// Keep addresses that are recent or are bootstrap hardcoded
			kept := []PeerAddress{}
			for _, a := range p.addresses {
				if a.Updated >= expiry || bootstrap[a.Address] {
					kept = append(kept, a)
				}
			}
			p.addresses = kept

			// Remove peer from memory if no addresses remain and not connected
			if len(p.addresses) == 0 && !p.connected {
				delete(peers, id)
			} else {
				peers[id] = p
			}
		}
		peers_lock.Unlock()
	}
}

// Reconnect to disconnected peers with exponential backoff
func peer_reconnect_manager() {
	for range time.Tick(10 * time.Second) {
		t := now()
		var ready []string

		// Collect peers ready for reconnection (max 3 per tick)
		peer_reconnect_lock.Lock()
		for id, r := range peer_reconnects {
			if r.NextRetry <= t {
				ready = append(ready, id)
				if len(ready) >= 3 {
					break
				}
			}
		}
		peer_reconnect_lock.Unlock()

		for _, id := range ready {
			if peer_connect(id) {
				debug("Peer %q reconnected successfully", id)
				continue
			}

			// Backoff: 10s, 20s, 40s, 80s, 160s, 300s (capped at 5 minutes)
			peer_reconnect_lock.Lock()
			r := peer_reconnects[id]
			r.Attempts++
			delay := int64(10) << min(r.Attempts, 5) // 20, 40, 80, 160, 320
			if delay > 300 {
				delay = 300
			}
			delay += rand.Int64N(delay/4 + 1) // 0-25% jitter
			r.NextRetry = now() + delay
			peer_reconnects[id] = r
			peer_reconnect_lock.Unlock()
			//debug("Peer %q reconnect attempt %d failed, next retry in %ds", id, r.Attempts, delay)
		}
	}
}

// Publish our own information to the pubsub regularly or when requested
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

// Received a peer publish event from another server
// We don't need to do anything here because we've already
// marked the peer as discovered in net_pubsubs()
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

// Get a reader and writer to a peer, connecting if necessary
func peer_stream(id string) *Stream {
	if id == "" {
		return nil
	}

	if id == net_id {
		r1, w1 := io.Pipe()
		r2, w2 := io.Pipe()
		go stream_receive(&Stream{id: stream_id(), reader: &pipe_reader{PipeReader: r1}, writer: &pipe_writer{PipeWriter: w2}}, 1, net_id)
		return &Stream{id: stream_id(), reader: &pipe_reader{PipeReader: r2}, writer: &pipe_writer{PipeWriter: w1}}
	}

	// Fast-fail for recently-silent peers. Without this every
	// queue_process tick re-attempts the libp2p connect for a peer
	// known to be unreachable, blocking that bucket for the full
	// connect timeout (tens of seconds). The skip lasts
	// peer_silent_skip_window; after that we trial one attempt and
	// either clear the silence (peer back) or re-arm it (still gone).
	if peer_is_silent(id) {
		return nil
	}

	p := peer_by_id(id)
	if p == nil {
		// In a future version, rate limit this
		//debug("Peer %q unknown, sending pubsub request for it", id)
		message("", "", "peers", "request").set("id", id).publish(false)
		peer_mark_send_failed(id)
		return nil
	}

	if !peer_connect(id) {
		peer_mark_send_failed(id)
		return nil
	}

	s := net_stream(id)
	if s == nil {
		return nil
	}
	peer_mark_send_success(id)
	return s
}

// Check whether we have enough peers in the pubsub mesh to send broadcast messages to
func peers_sufficient() bool {
	return len(net_pubsub_1.ListPeers()) >= peers_minimum
}

// Notify peers of shutdown (best effort).
//
// Two paths: peers that already have an open /mochi/2/messages stream
// get a `bye` frame on the existing Sender (preserves the in-flight
// drain semantics). Peers we haven't talked to via /mochi/2 yet — or
// /mochi/1-only peers — get the legacy fresh-stream bye on /mochi/1.
//
// peers_shutdown_drain_timeout caps how long we'll wait for the
// senders' inflight to empty before forcing the close.
func peers_shutdown() {
	// First, drain every open /mochi/2/messages Sender via bye + wait.
	senders_bye_all(peers_shutdown_drain_timeout)

	peers_lock.Lock()
	connected := []string{}
	for id, p := range peers {
		if p.connected {
			connected = append(connected, id)
		}
	}
	peers_lock.Unlock()

	// Then send the legacy bye to every still-connected peer that
	// didn't have a /mochi/2 Sender. peers with both will receive
	// two bye frames — that's harmless; both paths treat it as
	// "stop sending us new work".
	info("Notifying %d connected peers of shutdown", len(connected))
	for _, id := range connected {
		s := net_stream(id)
		if s != nil && s.writer != nil {
			s.write(Headers{Type: "bye"})
			s.writer.Close()
		}
	}
}

// peers_shutdown_drain_timeout — how long peers_shutdown waits for
// senders' inflight to drain on bye. Long enough for most inflight to
// ack on a healthy link; short enough not to delay shutdown noticeably.
var peers_shutdown_drain_timeout = 5 * time.Second
