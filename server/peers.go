// Mochi server: Peer registry — identity, addresses, persistence.
//
// This file owns the in-memory `peers` map of known libp2p peers and
// the on-disk peers.db that backs it. Connection lifecycle
// (peer_connect, peer_disconnected, reconnect manager, shutdown bye)
// lives in peer_connect.go; the silent-cache fast-fail logic lives in
// peer_reachability.go.
//
// Why not libp2p's Peerstore for addresses?
//
// Peerstore (AddAddrs / Addrs) covers the in-memory side, but its TTL
// model is push-driven (Addresses expire when their TTL elapses) and
// it's unbounded. We need three things Peerstore doesn't give us:
// (a) a hard cap on addresses per peer (peer_address_max=20) so a
// noisy multiaddr-broadcasting peer can't blow our footprint up,
// (b) on-disk persistence across restarts (peers.db) so a freshly-
// started server has somewhere to dial before bootstrap+DHT discovery
// fills in, (c) explicit last-seen timestamps for the 14-day pruning
// sweep. We do read from Peerstore where libp2p has already
// populated it (peer_refresh_connected_address uses the connection's
// remote multiaddr), but the authoritative store is this map.
//
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"strings"
	"sync"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// peer_state is the current libp2p-level connection state for a peer.
// Transitions are gated by peers_lock; the connecting → connected /
// disconnected transition happens after a synchronous net_connect call
// outside the lock (libp2p connect can block for the full TCP
// timeout). The connecting state prevents two concurrent callers from
// racing onto net_connect for the same peer.
type peer_state int

const (
	peer_state_disconnected peer_state = iota
	peer_state_connecting
	peer_state_connected
)

type Peer struct {
	ID        string
	Updated   int64 // throttles peers.db saves to at most once per hour per peer
	addresses []PeerAddress
	state     peer_state
}

// PeerAddress tracks a peer's address with usefulness evidence: when it
// was last seen (announced, discovered, or refreshed), when a
// connection last succeeded on it, and how many whole-peer dial rounds
// have failed since that success. Dialing hands every address to libp2p
// at once, so failures only accrue peer-wide (no address worked);
// per-address differentiation comes from successes.
type PeerAddress struct {
	Address string
	Updated int64
	Success int64 // last successful connection on this address; 0 = never proven
	Failure int64 // failed dial rounds since the last success
}

// peer_row is the sqlx scan target for peers.db rows — one row per
// (peer, address) tuple. Kept separate from Peer so the in-memory Peer
// struct doesn't carry a singular `Address` field that's only
// meaningful during SQL scanning.
type peer_row struct {
	ID      string
	Address string
	Updated int64
	Success int64
	Failure int64
}

// peer_address_strings extracts address strings from a slice of PeerAddress.
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
	peer_expiry            = 14 * 86400 // addresses unseen this long prune
	peer_unproven          = 3 * 86400  // never-successful addresses prune sooner
)

// peer_default_publisher_hardcoded is the fallback publisher peer ID
// when mochi.conf doesn't override [publisher] peer. Yuzu's libp2p
// id; serves the published-app catalogue.
const peer_default_publisher_hardcoded = "12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR"

// bootstrap_addresses_hardcoded is the fallback list of bootstrap
// multiaddresses when mochi.conf doesn't override [bootstrap]
// addresses. Comma-separated multiaddrs; each includes /p2p/<peer-id>
// so the peer identity is recoverable. Out-of-the-box installs need
// at least one reachable bootstrap to discover the wider network.
//
// The 1443 entries are the normal path. The 443 entries (QUIC over
// UDP, WSS over TCP on the mochi-os.org name the certificate covers)
// are the hostile-network fallback: a firewall that blocks 1443 but
// allows the web cannot tell WSS from HTTPS. They activate once the
// bootstrap runs with [p2p] https enabled; until then dialling them
// fails harmlessly while 1443 carries the connection.
const bootstrap_addresses_hardcoded = "/ip4/51.178.97.142/tcp/1443/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR, /ip6/2001:41d0:30f:8e00::1/tcp/1443/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR, /ip4/51.178.97.142/udp/443/quic-v1/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR, /ip6/2001:41d0:30f:8e00::1/udp/443/quic-v1/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR, /ip4/217.182.75.108/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /ip6/2001:41d0:601:1100::61f7/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /ip4/217.182.75.108/udp/443/quic-v1/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /ip6/2001:41d0:601:1100::61f7/udp/443/quic-v1/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /dns4/mochi-os.org/tcp/443/tls/ws/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH"

var (
	// peer_default_publisher + peers_bootstrap start at the hardcoded
	// defaults so package init() and tests that read them before
	// net_start see usable values. peers_bootstrap_load() reruns at
	// startup and replaces them with whatever mochi.conf specifies (or
	// the same hardcoded defaults if unset).
	peer_default_publisher                 = peer_default_publisher_hardcoded
	peers_bootstrap                        = bootstrap_addresses_parse(bootstrap_addresses_hardcoded)
	peers                  map[string]Peer = map[string]Peer{}
	peers_lock                             = &sync.Mutex{}
)

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
	// Order is preserved as priority order — the first entry is the
	// primary bootstrap, later entries are backups. bootstrap_manager
	// connects to the most-preferred reachable one (no shuffle: we want a
	// deterministic primary, not load-spread across equals).
	peers_bootstrap = bootstrap_addresses_parse(list)
}

// bootstrap_recheck is how often bootstrap_manager re-evaluates which
// bootstrap to hold a connection to, so the primary is preferred again
// promptly once it recovers from an outage.
const bootstrap_recheck = 30 * time.Second

// bootstrap_manager maintains a connection to the most-preferred reachable
// bootstrap. peers_bootstrap is in priority order (primary first); the
// manager makes every bootstrap's addresses known up front, then dials
// down the list and stops at the first that is or becomes connected — so a
// backup (e.g. wasabi) is dialled only while every higher-priority
// bootstrap (e.g. yuzu) is unreachable, and the primary is preferred again
// the moment it recovers. Replaces dialling every bootstrap at once.
func bootstrap_manager() {
	for _, p := range peers_bootstrap {
		if p.ID != net_id {
			peer_add_known(p.ID, peer_address_strings(p.addresses))
		}
	}
	bootstrap_connect_preferred()
	for range time.Tick(bootstrap_recheck) {
		bootstrap_connect_preferred()
	}
}

// bootstrap_connect_preferred ensures a connection to the highest-priority
// reachable bootstrap: it walks peers_bootstrap in priority order and stops
// at the first that connects (peer_connect returns true for an already-open
// connection), leaving lower-priority backups untouched while a higher one
// holds.
func bootstrap_connect_preferred() {
	for _, p := range peers_bootstrap {
		if p.ID == net_id {
			continue
		}
		if peer_connect(p.ID) {
			return
		}
	}
}

// peer_addresses_normalise validates operator-supplied multiaddresses
// for a peer and returns them in registry form (each carrying the
// /p2p/<id> suffix). An entry may omit the suffix; one that carries it
// must name the expected peer. Returns the normalised list and the
// first rejected input ("" when all were valid).
func peer_addresses_normalise(id string, addresses []string) ([]string, string) {
	var out []string
	for _, address := range addresses {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		ma, err := multiaddr.NewMultiaddr(address)
		if err != nil {
			return nil, address
		}
		if info, err := p2p_peer.AddrInfoFromP2pAddr(ma); err == nil {
			if info.ID.String() != id {
				return nil, address
			}
			out = append(out, address)
			continue
		}
		with := address + "/p2p/" + id
		if _, err := multiaddr.NewMultiaddr(with); err != nil {
			return nil, address
		}
		out = append(out, with)
	}
	return out, ""
}

// peer_addresses_count returns how many addresses the registry holds
// for a peer — zero for a peer we know only by id (or not at all).
func peer_addresses_count(id string) int {
	peers_lock.Lock()
	defer peers_lock.Unlock()
	return len(peers[id].addresses)
}

// peer_bootstrap_addresses returns the configured bootstrap addresses
// for a peer — the entries eviction and pruning must never remove.
func peer_bootstrap_addresses(id string) map[string]bool {
	out := map[string]bool{}
	for _, bp := range peers_bootstrap {
		if bp.ID == id {
			for _, a := range bp.addresses {
				out[a.Address] = true
			}
			break
		}
	}
	return out
}

// peer_address_insert merges one address into a peer's list, refreshing
// the timestamp when already present and evicting the least useful
// entry when the cap is reached. Returns whether the address was new.
// Eviction never removes a bootstrap address and drops never-proven
// entries before ones a connection has succeeded on (oldest success
// first, then oldest seen) — so a roaming peer's churn of dead LAN
// addresses cannot push out the one address that works. Caller holds
// peers_lock.
func peer_address_insert(p *Peer, address string, t int64) bool {
	for i, a := range p.addresses {
		if a.Address == address {
			p.addresses[i].Updated = t
			return false
		}
	}
	pa := PeerAddress{Address: address, Updated: t}
	if len(p.addresses) < peer_maximum_addresses {
		p.addresses = append(p.addresses, pa)
		return true
	}
	bootstrap := peer_bootstrap_addresses(p.ID)
	victim := -1
	for i, a := range p.addresses {
		if bootstrap[a.Address] {
			continue
		}
		if victim < 0 {
			victim = i
			continue
		}
		v := p.addresses[victim]
		if a.Success != v.Success {
			if a.Success < v.Success {
				victim = i
			}
			continue
		}
		if a.Updated < v.Updated {
			victim = i
		}
	}
	if victim < 0 {
		return false // every slot is a bootstrap address; drop the newcomer
	}
	p.addresses[victim] = pa
	return true
}

// peer_is_bootstrap returns true if the peer ID is a bootstrap peer.
func peer_is_bootstrap(id string) bool {
	for _, p := range peers_bootstrap {
		if p.ID == id {
			return true
		}
	}
	return false
}

// Add some peers we already know about from the database, restoring
// each address's success/failure evidence.
func peers_add_from_db(limit int) {
	var ps []peer_row
	db := db_open("db/peers.db")
	err := db.scans(&ps, "select id from peers group by id order by updated desc limit ?", limit)
	if err != nil {
		warn("Database error loading peers: %v", err)
		return
	}
	for _, p := range ps {
		var as []peer_row
		err := db.scans(&as, "select address, updated, success, failure from peers where id=?", p.ID)
		if err != nil {
			warn("Database error loading addresses for peer %q: %v", p.ID, err)
			continue
		}
		t := now()
		peers_lock.Lock()
		entry, found := peers[p.ID]
		if !found {
			entry = Peer{ID: p.ID}
		}
		for _, a := range as {
			peer_address_insert(&entry, a.Address, t)
			for i := range entry.addresses {
				if entry.addresses[i].Address == a.Address {
					entry.addresses[i].Success = a.Success
					entry.addresses[i].Failure = a.Failure
					break
				}
			}
		}
		addresses := peer_address_strings(entry.addresses)
		peers[p.ID] = entry
		peers_lock.Unlock()
		debug("Adding database peer %q at %v", p.ID, addresses)
		go peer_connect_retry(p.ID)
	}
}

// Add already known peer to memory, merging any new addresses with the
// existing entry via peer_address_insert's cap and eviction rules.
func peer_add_known(id string, addresses []string) {
	peers_lock.Lock()
	defer peers_lock.Unlock()

	t := now()
	p, found := peers[id]
	if !found {
		p = Peer{ID: id}
	}
	for _, addr := range addresses {
		peer_address_insert(&p, addr, t)
	}
	peers[id] = p
}

// New or existing peer discovered or re-discovered at unknown address.
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

// New or existing peer discovered or re-discovered at known address.
func peer_discovered_address(id string, address string) {
	peer_discovered_work(id, address)
	go queue_check_peer(id)
}

// Do the work for the above two functions.
func peer_discovered_work(id string, address string) {
	t := now()
	save := false

	peers_lock.Lock()
	p, found := peers[id]
	if !found {
		p = Peer{ID: id}
	}
	peer_address_insert(&p, address, t)
	if !found || p.Updated < t-int64(3600) {
		save = true
		p.Updated = t
	}
	peers[id] = p
	peers_lock.Unlock()

	if save {
		// Upsert, not replace: a replace would wipe the row's
		// success/failure evidence.
		db := db_open("db/peers.db")
		db.exec("insert into peers ( id, address, updated ) values ( ?, ?, ? ) on conflict ( id, address ) do update set updated=excluded.updated", id, address, t)
	}
}

// peer_addresses_failed counts a failed whole-peer dial round against
// every address, in memory and on disk. Dialing hands all addresses to
// libp2p at once, so a failure means none of them worked.
func peer_addresses_failed(id string) {
	peers_lock.Lock()
	if p, found := peers[id]; found {
		for i := range p.addresses {
			p.addresses[i].Failure++
		}
		peers[id] = p
	}
	peers_lock.Unlock()

	db := db_open("db/peers.db")
	db.exec("update peers set failure=failure+1 where id=?", id)
}

// Clean up stale peers.
func peers_manager() {
	for range time.Tick(24 * time.Hour) {
		peers_prune()

		// Announced names age out with the same expiry. Signed records
		// and relay flags age out too.
		expiry := now() - peer_expiry
		peer_names_sweep(expiry)
		peer_records_sweep(expiry)
		peer_relays_sweep()
	}
}

// peers_prune drops stale addresses: anything unseen for peer_expiry,
// and never-proven addresses unseen for peer_unproven — the junk a
// roaming machine accumulates (other networks' LAN addresses) dies in
// days instead of weeks, while addresses a connection has succeeded on
// get the full window. Live peers' addresses never age: every hourly
// announcement refreshes them. Bootstrap addresses never prune.
func peers_prune() {
	t := now()
	expiry := t - peer_expiry
	unproven := t - peer_unproven

	// Prune stale addresses from the database
	db := db_open("db/peers.db")
	db.exec("delete from peers where updated<? or ( success=0 and updated<? )", expiry, unproven)

	// Prune stale addresses from memory
	peers_lock.Lock()
	for id, p := range peers {
		bootstrap := peer_bootstrap_addresses(id)
		kept := []PeerAddress{}
		for _, a := range p.addresses {
			if bootstrap[a.Address] || a.Updated >= unproven || (a.Success > 0 && a.Updated >= expiry) {
				kept = append(kept, a)
			}
		}
		p.addresses = kept

		// Remove peer from memory if no addresses remain and not connected
		if len(p.addresses) == 0 && p.state != peer_state_connected {
			delete(peers, id)
		} else {
			peers[id] = p
		}
	}
	peers_lock.Unlock()
}

// peers_purge_self_relay drops every stored address that relays through
// this server itself — a circuit address with our own peer ID in the relay
// slot. We can never use our own relay to reach the peer (we hold a direct
// reservation connection to it), so the address is dead weight: registry
// bloat and a wasted dial. Called once at startup to shed any accumulated
// before this filter existed; peer_apply_addresses keeps new ones out. The
// address stays valid for every other peer, who keep advertising it.
func peers_purge_self_relay() {
	if net_id == "" {
		return
	}
	marker := "/p2p/" + net_id + "/p2p-circuit"

	db_open("db/peers.db").exec("delete from peers where address like ?", "%"+marker+"%")

	peers_lock.Lock()
	for id, p := range peers {
		kept := p.addresses[:0]
		for _, a := range p.addresses {
			if !strings.Contains(a.Address, marker) {
				kept = append(kept, a)
			}
		}
		p.addresses = kept
		peers[id] = p
	}
	peers_lock.Unlock()
}

// Check whether we have enough peers in the pubsub mesh to send broadcast
// messages to. Nil before net_start (unit tests).
func peers_sufficient() bool {
	return net_pubsub != nil && len(net_pubsub.ListPeers()) >= peers_minimum
}
