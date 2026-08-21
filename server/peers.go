// Mochi server: Peer registry - identity, addresses, and the peers.db that
// backs the in-memory map. Connection lifecycle is peer_connect.go; the
// silent-cache is peer_reachability.go. libp2p's Peerstore is not the store of
// record: we need a hard per-peer address cap, on-disk persistence, and
// last-seen for pruning.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"sync"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// peer_state is the libp2p-level connection state, gated by peers_lock. The
// connecting state stops two callers racing onto net_connect, which runs
// outside the lock because a libp2p connect can block for the full TCP timeout.
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

// PeerAddress tracks an address with usefulness evidence. Dialing hands every
// address to libp2p at once, so failures only accrue peer-wide; per-address
// differentiation comes from successes.
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
// when mochi.conf doesn't override [publisher] peer. Yuzu's libp2p id;
// serves the published-app catalogue.
const peer_default_publisher_hardcoded = "12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR" // yuzu

// bootstrap_addresses_hardcoded is the fallback bootstrap list when mochi.conf
// does not set [bootstrap] addresses: comma-separated multiaddrs, each carrying
// /p2p/<peer-id>, in priority order. The 443 entries are the hostile-network
// fallback - QUIC over UDP/443 and WSS over TCP/443 on the project's own name,
// which a firewall cannot tell from HTTPS.
const bootstrap_addresses_hardcoded = "/ip4/51.178.97.142/tcp/1443/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR, /ip6/2001:41d0:30f:8e00::1/tcp/1443/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR, /ip4/51.178.97.142/udp/443/quic-v1/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR, /ip6/2001:41d0:30f:8e00::1/udp/443/quic-v1/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR, /ip4/217.182.75.108/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /ip6/2001:41d0:601:1100::61f7/tcp/1443/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /ip4/217.182.75.108/udp/443/quic-v1/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /ip6/2001:41d0:601:1100::61f7/udp/443/quic-v1/p2p/12D3KooWRbpjpRmFiK7v6wRXA6yvAtTXXfvSE6xjbHVFFSaxN8SH, /dns/mochi-os.org/tcp/443/tls/ws/p2p/12D3KooWELMRq3U9TrJE2FJs8pcXSQotDrtXwhajTNV2CN7fWdyR"

var (
	// Hardcoded defaults so anything reading these before net_start (tests,
	// package init) sees a usable list; peers_bootstrap_load() replaces them at
	// startup.
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
		information, err := p2p_peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			warn("Bootstrap: cannot extract peer id from %q: %v", addr, err)
			continue
		}
		id := information.ID.String()
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

// peers_bootstrap_load reloads peers_bootstrap and peer_default_publisher from
// mochi.conf after ini_load, falling back to the hardcoded defaults. Each
// [bootstrap] addresses entry carries its own /p2p/<id>: address and identity.
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

// bootstrap_manager holds a connection to the most-preferred reachable
// bootstrap. peers_bootstrap is priority order: it makes every bootstrap's
// addresses known, then dials down the list and stops at the first connected.
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

// bootstrap_connect_preferred walks peers_bootstrap in priority order and stops
// at the first that connects (peer_connect is true for an already-open
// connection).
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

// mesh_isolation_recheck is how often mesh_isolation_manager samples the
// broadcast mesh while watching for, and recovering from, isolation.
const mesh_isolation_recheck = 15 * time.Second

// mesh_isolation_confirm is how many consecutive empty samples confirm
// isolation before remediation starts, so ordinary GossipSub churn (a peer
// briefly leaving the mesh) doesn't trigger a re-dial round on its own.
const mesh_isolation_confirm = 2

// mesh_isolation_alert_after is how long confirmed isolation must persist
// before the operator is warned: long enough that a bootstrap restart or a
// transient partition reconverges unnoticed, short enough that a genuinely
// cut-off server is reported within the hour.
const mesh_isolation_alert_after = 10 * time.Minute

// mesh_isolation_state carries mesh_isolation_step's memory between
// samples: consecutive empty samples seen, the unix time isolation was
// confirmed (0 when not isolated), and whether the operator alert has
// fired for this episode.
type mesh_isolation_state struct {
	empty   int
	since   int64
	alerted bool
}

// mesh_isolation_step folds one mesh sample into the isolation state and
// reports what the manager should do this tick. Pure (no I/O, no clock) so the
// confirm / stand-down / alert logic is unit-testable.
func mesh_isolation_step(s mesh_isolation_state, peers int, t int64) (next mesh_isolation_state, remediate, alert, recovered bool) {
	if peers > 0 {
		// A peer is in the mesh: stand down, resetting all memory. Report
		// recovery only if we had escalated to an alert this episode.
		return mesh_isolation_state{}, false, false, s.since != 0 && s.alerted
	}

	s.empty++
	if s.empty < mesh_isolation_confirm {
		return s, false, false, false
	}
	if s.since == 0 {
		s.since = t
	}
	if !s.alerted && t-s.since >= int64(mesh_isolation_alert_after/time.Second) {
		s.alerted = true
		return s, true, true, false
	}
	return s, true, false, false
}

// mesh_isolation_manager watches the GossipSub mesh and, while it is empty,
// re-dials every bootstrap, resets known-peer reconnect backoffs, and requests
// fresh addresses. Warns the operator once if isolation outlasts
// mesh_isolation_alert_after.
func mesh_isolation_manager() {
	var state mesh_isolation_state
	for range time.Tick(mesh_isolation_recheck) {
		if net_pubsub == nil {
			continue
		}
		previous := state.since
		var remediate, alert, recovered bool
		state, remediate, alert, recovered = mesh_isolation_step(state, len(net_pubsub.ListPeers()), now())
		if recovered {
			info("Mesh isolation cleared after %s; broadcast mesh recovered", time.Duration(now()-previous)*time.Second)
		}
		if remediate {
			mesh_isolation_remediate()
		}
		if alert {
			warn("Broadcast mesh isolated for %s: no GossipSub peers. Re-dialling all bootstraps and known peers.", time.Duration(now()-state.since)*time.Second)
		}
	}
}

// mesh_isolation_remediate makes one aggressive attempt to rejoin the mesh.
func mesh_isolation_remediate() {
	for _, p := range peers_bootstrap {
		if p.ID != net_id {
			go peer_connect(p.ID)
		}
	}

	peer_reconnect_lock.Lock()
	for id, r := range peer_reconnects {
		r.NextRetry = now()
		peer_reconnects[id] = r
	}
	peer_reconnect_lock.Unlock()

	peers_publish_request()
}

// peer_addresses_normalise validates operator-supplied multiaddresses and
// returns them with the /p2p/<id> suffix. An entry may omit the suffix; one
// that carries it must name this peer. Returns the list and the first rejected
// input.
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
		if information, err := p2p_peer.AddrInfoFromP2pAddr(ma); err == nil {
			if information.ID.String() != id {
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

// peer_address_insert merges one address into a peer's list, evicting the least
// useful entry at the cap: never a bootstrap address, never-proven before
// proven. Returns whether the address was new. Caller holds peers_lock.
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

	peer_addresses_arrived(id)

	if save {
		// Upsert, not replace: a replace would wipe the row's
		// success/failure evidence.
		db := db_open("db/peers.db")
		db.exec("insert into peers ( id, address, updated ) values ( ?, ?, ? ) on conflict ( id, address ) do update set updated=excluded.updated", id, address, t)
	}
}

// Waiters blocked in peer_await_addresses, keyed by peer id. Signalled
// by peer_addresses_arrived each time a discovered address lands in the
// peers map.
var (
	peer_waiters      = map[string][]chan struct{}{}
	peer_waiters_lock sync.Mutex
)

// peer_await_addresses blocks until a discovered address lands for id or
// timeout elapses, reporting which - the bounded inline wait for synchronous
// paths (remote_reach). The queue recovers through its retry loop instead.
func peer_await_addresses(id string, timeout time.Duration) bool {
	arrival := make(chan struct{})
	peer_waiters_lock.Lock()
	peer_waiters[id] = append(peer_waiters[id], arrival)
	peer_waiters_lock.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-arrival:
		return true
	case <-timer.C:
		peer_waiters_lock.Lock()
		waiters := peer_waiters[id]
		for i, w := range waiters {
			if w == arrival {
				peer_waiters[id] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(peer_waiters[id]) == 0 {
			delete(peer_waiters, id)
		}
		peer_waiters_lock.Unlock()
		// An arrival may race the timeout: signalled after the timer
		// fired but before deregistration. Report it either way.
		select {
		case <-arrival:
			return true
		default:
			return false
		}
	}
}

// peer_addresses_arrived wakes every peer_await_addresses call blocked
// on `id`. Companion to peer_addresses_failed.
func peer_addresses_arrived(id string) {
	peer_waiters_lock.Lock()
	waiters := peer_waiters[id]
	delete(peer_waiters, id)
	peer_waiters_lock.Unlock()
	for _, w := range waiters {
		close(w)
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

// peer_address_drop removes one stale address from id, in memory and peers.db.
// `bare` is the multiaddr as a swarm dial error reports it, without the
// /p2p/<id> suffix the stored form carries, so match either shape. See #48.
func peer_address_drop(id, bare string) {
	peers_lock.Lock()
	if p, found := peers[id]; found {
		kept := make([]PeerAddress, 0, len(p.addresses))
		for _, a := range p.addresses {
			if a.Address == bare || strings.HasPrefix(a.Address, bare+"/p2p/") {
				continue
			}
			kept = append(kept, a)
		}
		p.addresses = kept
		peers[id] = p
	}
	peers_lock.Unlock()

	db := db_open("db/peers.db")
	db.exec("delete from peers where id=? and ( address=? or address like ? )", id, bare, bare+"/p2p/%")
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

// peers_prune drops addresses unseen for peer_expiry, and never-proven ones
// unseen for peer_unproven, so a roaming machine's dead LAN addresses die in
// days while proven ones get the full window. Bootstrap addresses never prune.
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

// peers_purge_self_relay drops stored addresses that relay through this server:
// we hold the direct connection the reservation was made on, so our own relay
// can never reach the peer. Startup sweep; peer_apply_addresses keeps new ones
// out.
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
