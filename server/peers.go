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
	"math/rand/v2"
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

// PeerAddress tracks a peer's address with a last-seen timestamp.
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
)

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
	peers_bootstrap = bootstrap_addresses_parse(list)

	// Shuffle so different servers attempt bootstrap peers in different
	// orders, spreading the initial-connect load across them.
	rand.Shuffle(len(peers_bootstrap), func(i, j int) {
		peers_bootstrap[i], peers_bootstrap[j] = peers_bootstrap[j], peers_bootstrap[i]
	})
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

// Add some peers we already know about from the database.
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
		p = Peer{ID: id}
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

// Clean up stale peers.
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
			if len(p.addresses) == 0 && p.state != peer_state_connected {
				delete(peers, id)
			} else {
				peers[id] = p
			}
		}
		peers_lock.Unlock()
	}
}

// Check whether we have enough peers in the pubsub mesh to send broadcast
// messages to. Nil before net_start (unit tests).
func peers_sufficient() bool {
	return net_pubsub != nil && len(net_pubsub.ListPeers()) >= peers_minimum
}
