// Mochi server: peer address discovery unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// The peers/publish + peers/request path (#424): a server that knows another
// only by peer id learns its addresses from a signed pubsub announcement, and
// the send path can solicit one.

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	p2p "github.com/libp2p/go-libp2p"
	p2p_host "github.com/libp2p/go-libp2p/core/host"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// peer_addresses_count returns how many addresses the registry holds
// for a peer — zero for a peer we know only by id (or not at all).
func peer_addresses_count(id string) int {
	peers_lock.Lock()
	defer peers_lock.Unlock()
	return len(peers[id].addresses)
}

// setup_peer_discovery_test gives a fresh data_dir (via the replication
// harness), a peers.db with schema, and an empty in-memory peer
// registry. Returns a cleanup that restores the prior registry.
func setup_peer_discovery_test(t *testing.T) {
	t.Helper()
	setup_replication_test(t)

	pdb := db_open("db/peers.db")
	pdb.exec("create table if not exists peers ( id text not null, address text not null, updated integer not null, success integer not null default 0, failure integer not null default 0, primary key ( id, address ) )")
	pdb.exec("create table if not exists records ( id text not null primary key, record blob not null, sequence integer not null, updated integer not null )")

	peers_lock.Lock()
	saved := peers
	peers = map[string]Peer{}
	peers_lock.Unlock()

	t.Cleanup(func() {
		peers_lock.Lock()
		peers = saved
		peers_lock.Unlock()
	})
}

// publish_event builds the Event shape pubsub_receive hands to
// peer_publish_event: origin from the verified GossipSub envelope,
// announced addresses in content.
func publish_event(origin, addresses string) *Event {
	return &Event{origin: origin, content: map[string]any{"addresses": addresses}}
}

func TestPeerPublishEventAppliesAddresses(t *testing.T) {
	setup_peer_discovery_test(t)

	origin, _ := test_host(t)
	announced := "/ip4/192.0.2.10/tcp/1443/p2p/" + origin + ",/ip4/192.0.2.10/udp/1443/quic-v1/p2p/" + origin
	peer_publish_event(publish_event(origin, announced))

	if n := peer_addresses_count(origin); n != 2 {
		t.Errorf("addresses applied = %d, want 2", n)
	}
}

// TestPeerPublishEventRequiresOrigin: a direct-stream message spoofing
// the peers/publish event has no GossipSub-verified origin and must be
// ignored.
func TestPeerPublishEventRequiresOrigin(t *testing.T) {
	setup_peer_discovery_test(t)

	origin, _ := test_host(t)
	peer_publish_event(publish_event("", "/ip4/192.0.2.10/tcp/1443/p2p/"+origin))

	if n := peer_addresses_count(origin); n != 0 {
		t.Errorf("addresses applied = %d, want 0 (no origin)", n)
	}
}

// TestPeerPublishEventRejectsForeignSuffix: an announcement may only
// carry the originator's own addresses — a /p2p/ suffix naming a
// different peer is address poisoning and must be dropped.
func TestPeerPublishEventRejectsForeignSuffix(t *testing.T) {
	setup_peer_discovery_test(t)

	origin, _ := test_host(t)
	victim, _ := test_host(t)
	peer_publish_event(publish_event(origin, "/ip4/203.0.113.66/tcp/1443/p2p/"+victim))

	if n := peer_addresses_count(victim); n != 0 {
		t.Errorf("victim addresses applied = %d, want 0", n)
	}
	if n := peer_addresses_count(origin); n != 0 {
		t.Errorf("origin addresses applied = %d, want 0", n)
	}
}

func TestPeerPublishEventSkipsInvalid(t *testing.T) {
	setup_peer_discovery_test(t)

	origin, _ := test_host(t)
	announced := "not-a-multiaddr,/ip4/192.0.2.20/tcp/1443/p2p/" + origin
	peer_publish_event(publish_event(origin, announced))

	if n := peer_addresses_count(origin); n != 1 {
		t.Errorf("addresses applied = %d, want 1 (invalid entry skipped)", n)
	}
}

func TestPeerPublishEventCapsAddresses(t *testing.T) {
	setup_peer_discovery_test(t)

	origin, _ := test_host(t)
	var announced []string
	for i := 0; i < peers_publish_addresses_maximum+4; i++ {
		announced = append(announced, fmt.Sprintf("/ip4/10.9.0.%d/tcp/1443/p2p/%s", i+1, origin))
	}
	peer_publish_event(publish_event(origin, strings.Join(announced, ",")))

	if n := peer_addresses_count(origin); n != peers_publish_addresses_maximum {
		t.Errorf("addresses applied = %d, want cap %d", n, peers_publish_addresses_maximum)
	}
}

// TestPeerRequestAddressesRateLimit: the send path may solicit a
// publish at most once per window per target; self and empty ids are
// never solicited. The broadcast itself is a no-op in unit tests
// (peers_sufficient is false without a live pubsub).
func TestPeerRequestAddressesRateLimit(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	if !peer_request_addresses(id) {
		t.Error("first request should broadcast")
	}
	if peer_request_addresses(id) {
		t.Error("second request inside the window should be suppressed")
	}
	if peer_request_addresses("") {
		t.Error("empty id should never broadcast")
	}
	if peer_request_addresses(net_id) {
		t.Error("self should never broadcast")
	}
}

// TestPeerConnectRetryEnrollsFailedDial: the other enrollment triggers require
// having reached the peer, so without this a server that boots before its
// network is ready stays isolated until restart.
func TestPeerConnectRetryEnrollsFailedDial(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	peer_add_known(id, []string{"/ip4/192.0.2.50/tcp/1443/p2p/" + id})

	// net_me is nil in unit tests, so the dial fails like an
	// unreachable network would.
	peer_connect_retry(id)

	peer_reconnect_lock.Lock()
	_, scheduled := peer_reconnects[id]
	delete(peer_reconnects, id)
	peer_reconnect_lock.Unlock()
	if !scheduled {
		t.Error("failed startup dial did not schedule a reconnect probe")
	}
}

// TestPeerReconnectLetsGoWhatNothingNeeds: probing never ended, so a server
// that had gone was probed every five minutes until restart, each probe
// broadcasting an address request. A peer is let go after
// peer_reconnect_attempts_maximum failures only when it is no bootstrap peer,
// hosts no directory entries and has nothing queued. Holding its addresses is
// no need: the address an inbound stream came from is held for any peer that
// opened one.
func TestPeerReconnectLetsGoWhatNothingNeeds(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_directory_test_schema()
	reset_peer_reachability(t)

	gone, _ := test_host(t)
	early, _ := test_host(t)
	queued, _ := test_host(t)
	hosting, _ := test_host(t)
	addressed, _ := test_host(t)
	bootstrap, _ := test_host(t)

	db_open("db/queue.db").exec("insert into queue ( id, target, from_entity, to_entity, service, event, next_retry, created ) values ( 'row', ?, 'from', 'to', 'feeds', 'update', 0, 0 )", queued)
	db_open("db/directory.db").exec("insert into entries ( entity, peer, name, class, created, seen ) values ( 'entity', ?, 'Name', 'person', 0, 0 )", hosting)
	peer_add_known(addressed, []string{"/ip4/192.0.2.51/tcp/1443/p2p/" + addressed})
	saved := peers_bootstrap
	peers_bootstrap = append([]Peer{{ID: bootstrap}}, saved...)
	defer func() { peers_bootstrap = saved }()

	all := []string{gone, early, queued, hosting, addressed, bootstrap}
	for _, id := range all {
		for i := 0; i < peer_silent_failure_threshold; i++ {
			peer_mark_send_failed(id)
		}
		attempts := peer_reconnect_attempts_maximum - 1
		if id == early {
			attempts = peer_reconnect_attempts_maximum - 2
		}
		peer_reconnect_lock.Lock()
		peer_reconnects[id] = PeerReconnect{NextRetry: now(), Attempts: attempts}
		peer_reconnect_lock.Unlock()
		peer_reconnect_failed(id)
	}

	scheduled := map[string]bool{}
	peer_reconnect_lock.Lock()
	for _, id := range all {
		_, scheduled[id] = peer_reconnects[id]
		delete(peer_reconnects, id)
	}
	peer_reconnect_lock.Unlock()

	if scheduled[gone] {
		t.Error("a peer nothing needs is still probed after its last allowed failure")
	}
	if peer_is_silent(gone) {
		t.Error("a peer let go keeps its send failures, so a delivery queued for it later would never enroll it again")
	}
	if scheduled[addressed] {
		t.Error("a peer needed only because its addresses are held is still probed after its last allowed failure")
	}
	for id, why := range map[string]string{
		early:     "a peer short of its allowed failures",
		queued:    "a peer with a delivery queued",
		hosting:   "a peer hosting directory entries",
		bootstrap: "a bootstrap peer",
	} {
		if !scheduled[id] {
			t.Errorf("%s was let go", why)
		}
	}
}

// TestPeerPublishEventDropsSelfRelay: a circuit address that relays
// through ourselves is dead weight (we reach the peer directly over its
// reservation), so it is dropped on apply; a third-party relay and a
// direct address are kept.
func TestPeerPublishEventDropsSelfRelay(t *testing.T) {
	setup_peer_discovery_test(t)

	self, _ := test_host(t)
	saved := net_id
	net_id = self
	defer func() { net_id = saved }()

	origin, _ := test_host(t)
	other, _ := test_host(t)
	direct := "/ip4/192.0.2.10/udp/1443/quic-v1/p2p/" + origin
	self_relay := "/ip4/198.51.100.1/udp/1443/quic-v1/p2p/" + self + "/p2p-circuit/p2p/" + origin
	other_relay := "/ip4/203.0.113.5/udp/1443/quic-v1/p2p/" + other + "/p2p-circuit/p2p/" + origin
	peer_publish_event(publish_event(origin, strings.Join([]string{direct, self_relay, other_relay}, ",")))

	if n := peer_addresses_count(origin); n != 2 {
		t.Errorf("addresses applied = %d, want 2 (self-relay dropped)", n)
	}
	peers_lock.Lock()
	p := peers[origin]
	peers_lock.Unlock()
	for _, a := range p.addresses {
		if strings.Contains(a.Address, "/p2p/"+self+"/p2p-circuit") {
			t.Errorf("self-relay address was stored: %q", a.Address)
		}
	}
}

// TestPeersPurgeSelfRelay: the startup purge sheds self-relay addresses
// already in the registry (accumulated before the apply-time filter).
func TestPeersPurgeSelfRelay(t *testing.T) {
	setup_peer_discovery_test(t)

	self, _ := test_host(t)
	saved := net_id
	net_id = self
	defer func() { net_id = saved }()

	origin, _ := test_host(t)
	direct := "/ip4/192.0.2.10/udp/1443/quic-v1/p2p/" + origin
	self_relay := "/ip4/198.51.100.1/udp/1443/quic-v1/p2p/" + self + "/p2p-circuit/p2p/" + origin
	// Seed both directly, bypassing the apply-time filter.
	peer_discovered_address(origin, direct)
	peer_discovered_address(origin, self_relay)
	if n := peer_addresses_count(origin); n != 2 {
		t.Fatalf("seeded addresses = %d, want 2", n)
	}

	peers_purge_self_relay()

	if n := peer_addresses_count(origin); n != 1 {
		t.Errorf("after purge = %d, want 1 (self-relay removed)", n)
	}
	if ok, _ := db_open("db/peers.db").exists("select 1 from peers where address like ?", "%/p2p/"+self+"/p2p-circuit%"); ok {
		t.Error("self-relay address survived purge in peers.db")
	}
}

// TestBootstrapConnectPreferred: the walk holds the highest-priority
// reachable bootstrap and leaves lower-priority backups untouched while it
// is connected — so the backup (wasabi) is dialled only when the primary
// (yuzu) is unavailable.
func TestBootstrapConnectPreferred(t *testing.T) {
	setup_peer_discovery_test(t)

	primary, _ := test_host(t)
	backup, _ := test_host(t)

	saved := peers_bootstrap
	peers_bootstrap = []Peer{{ID: primary}, {ID: backup}}
	defer func() { peers_bootstrap = saved }()

	// Primary already connected; backup merely known (disconnected).
	peers_lock.Lock()
	peers[primary] = Peer{ID: primary, state: peer_state_connected}
	peers[backup] = Peer{ID: backup, state: peer_state_disconnected}
	peers_lock.Unlock()

	bootstrap_connect_preferred()

	// The connected primary short-circuits the walk, so the backup is
	// never dialled — it stays disconnected (not even connecting).
	peers_lock.Lock()
	state := peers[backup].state
	peers_lock.Unlock()
	if state != peer_state_disconnected {
		t.Errorf("backup state = %v, want disconnected (must not be dialled while primary is connected)", state)
	}
}

// TestPeerAddressEviction: when the per-peer cap forces a choice, the
// victim is the least useful address — never-proven before proven,
// oldest first — so a roaming peer's churn cannot push out the address
// connections actually succeed on.
func TestPeerAddressEviction(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	proven := fmt.Sprintf("/ip4/198.51.100.1/tcp/1443/p2p/%s", id)

	peers_lock.Lock()
	p := Peer{ID: id}
	for i := 0; i < peer_maximum_addresses-1; i++ {
		p.addresses = append(p.addresses, PeerAddress{Address: fmt.Sprintf("/ip4/10.0.0.%d/tcp/1443/p2p/%s", i+1, id), Updated: int64(100 + i)})
	}
	// The proven address is the OLDEST seen — the old rule would evict it.
	p.addresses = append(p.addresses, PeerAddress{Address: proven, Updated: 50, Success: 99})
	newcomer := peer_address_insert(&p, "/ip4/203.0.113.99/tcp/1443/p2p/"+id, now())
	peers_lock.Unlock()

	if !newcomer {
		t.Fatal("newcomer not inserted")
	}
	kept := false
	evicted := true
	for _, a := range p.addresses {
		if a.Address == proven {
			kept = true
		}
		if a.Address == fmt.Sprintf("/ip4/10.0.0.1/tcp/1443/p2p/%s", id) {
			evicted = false
		}
	}
	if !kept {
		t.Error("eviction removed the proven address")
	}
	if !evicted {
		t.Error("eviction kept the oldest never-proven address")
	}
}

// TestPeerAddressesFailed: a failed dial round counts against every
// address, in memory and in peers.db.
func TestPeerAddressesFailed(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	address := "/ip4/192.0.2.40/tcp/1443/p2p/" + id
	peer_discovered_address(id, address)

	peer_addresses_failed(id)
	peer_addresses_failed(id)

	peers_lock.Lock()
	failure := peers[id].addresses[0].Failure
	peers_lock.Unlock()
	if failure != 2 {
		t.Errorf("memory failure count = %d, want 2", failure)
	}
	var rows []peer_row
	db_open("db/peers.db").scans(&rows, "select id, address, updated, success, failure from peers where id=?", id)
	if len(rows) != 1 || rows[0].Failure != 2 {
		t.Errorf("database failure count = %+v, want 2", rows)
	}
}

// TestPeersPrune: never-proven addresses prune after peer_unproven,
// proven ones get the full peer_expiry window, and bootstrap addresses
// never prune.
func TestPeersPrune(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	t0 := now()
	stale := t0 - peer_unproven - 10
	proven := PeerAddress{Address: "/ip4/198.51.100.2/tcp/1443/p2p/" + id, Updated: stale, Success: stale}
	junk := PeerAddress{Address: "/ip4/10.0.0.9/tcp/1443/p2p/" + id, Updated: stale}
	ancient := PeerAddress{Address: "/ip4/198.51.100.3/tcp/1443/p2p/" + id, Updated: t0 - peer_expiry - 10, Success: t0 - peer_expiry - 10}

	db := db_open("db/peers.db")
	for _, a := range []PeerAddress{proven, junk, ancient} {
		db.exec("insert into peers ( id, address, updated, success ) values ( ?, ?, ?, ? )", id, a.Address, a.Updated, a.Success)
	}
	peers_lock.Lock()
	peers[id] = Peer{ID: id, addresses: []PeerAddress{proven, junk, ancient}}
	peers_lock.Unlock()

	peers_prune()

	peers_lock.Lock()
	remaining := peer_address_strings(peers[id].addresses)
	peers_lock.Unlock()
	if len(remaining) != 1 || remaining[0] != proven.Address {
		t.Errorf("memory prune kept %v, want only the proven address", remaining)
	}
	var rows []peer_row
	db.scans(&rows, "select id, address, updated, success, failure from peers where id=?", id)
	if len(rows) != 1 || rows[0].Address != proven.Address {
		t.Errorf("database prune kept %+v, want only the proven address", rows)
	}
}

// TestNetContainerInterface: tool-generated bridge names match; real
// interfaces and deliberately-named bridges don't.
func TestNetContainerInterface(t *testing.T) {
	matching := []string{"docker0", "br-1a2b3c4d", "virbr0", "veth12ab", "cni0", "podman1", "flannel.1", "lxcbr0", "lxdbr0", "kube-bridge"}
	for _, name := range matching {
		if !net_container_interface(name) {
			t.Errorf("net_container_interface(%q) = false, want true", name)
		}
	}
	plain := []string{"eth0", "wlp3s0", "enp4s0", "br0", "bridge0", "lo", "tailscale0", "wg0"}
	for _, name := range plain {
		if net_container_interface(name) {
			t.Errorf("net_container_interface(%q) = true, want false", name)
		}
	}
}

// peer_discovery_fill holds the unproven population at its ceiling for the
// rest of the test: never-connected peers, the shape a flood of minted ids
// leaves behind.
func peer_discovery_fill(t *testing.T) {
	t.Helper()
	peers_lock.Lock()
	for i := 0; i < peer_unproven_maximum; i++ {
		id := fmt.Sprintf("unproven-peer-%d", i)
		peers[id] = Peer{ID: id, addresses: []PeerAddress{{Address: "/ip4/10.0.0.1/tcp/1/p2p/" + id}}}
	}
	peers_lock.Unlock()
}

// TestPeerPublishEventAdmitsTheOrigin is #128. An origin is only a keypair, and
// one connected peer can publish for any number it mints: each one stored a
// name, a relay flag, a record and addresses with no admission check, kept for
// days. At the ceiling a new origin's publish leaves nothing behind.
func TestPeerPublishEventAdmitsTheOrigin(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_peer_names_test(t)
	peer_discovery_fill(t)

	origin, key := test_host(t)
	record := test_signed_record(t, key, origin, []string{"/ip4/192.0.2.10/tcp/1443"}, 1)
	peer_publish_event(&Event{origin: origin, content: map[string]any{
		"record": record, "addresses": "/ip4/192.0.2.11/tcp/1443/p2p/" + origin,
		"name": "minted.example", "relay": "true",
	}})

	if peer_held(origin) {
		t.Error("THE POINT: a minted origin was taken into the registry at the ceiling")
	}
	if peer_name(origin) != "" {
		t.Error("a minted origin's name was kept at the ceiling")
	}
	for _, id := range peer_relays_fresh() {
		if id == origin {
			t.Error("a minted origin's relay flag was kept at the ceiling")
		}
	}
	if peer_record_envelope(origin) != nil {
		t.Error("a minted origin's record was kept at the ceiling")
	}

	// A peer already held is still updated.
	held, _ := test_host(t)
	peers_lock.Lock()
	peers[held] = Peer{ID: held, addresses: []PeerAddress{{Address: "/ip4/10.0.0.2/tcp/1/p2p/" + held}}}
	peers_lock.Unlock()
	peer_publish_event(publish_event(held, "/ip4/192.0.2.12/tcp/1443/p2p/"+held))
	if n := peer_addresses_count(held); n != 2 {
		t.Errorf("a held peer's announcement applied %d addresses, want 2: updates for known peers must not be collateral", n)
	}
}

// TestPeerPublishEventKeepsNothingForAnUnheldOrigin: a publish that carries no
// usable address leaves its origin out of the registry, and so keeps none of
// its name, relay flag or record either - those would otherwise grow with no
// count against the ceiling.
func TestPeerPublishEventKeepsNothingForAnUnheldOrigin(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_peer_names_test(t)

	origin, _ := test_host(t)
	peer_publish_event(&Event{origin: origin, content: map[string]any{
		"addresses": "/ip4/192.0.2.13/tcp/1443", // no /p2p suffix: refused
		"name":      "addressless.example", "relay": "true",
	}})
	if peer_held(origin) {
		t.Fatal("control: an origin with no usable address was registered")
	}
	if peer_name(origin) != "" {
		t.Error("a name was kept for an origin the registry does not hold")
	}
	for _, id := range peer_relays_fresh() {
		if id == origin {
			t.Error("a relay flag was kept for an origin the registry does not hold")
		}
	}
}

// TestPeerApplyAddressesBoundsTheLength is #128's second half: a multiaddr
// parses at any length (a /dns name has no limit of its own), so one 1 MiB
// announcement left about 1 MiB in memory under the peer.
func TestPeerApplyAddressesBoundsTheLength(t *testing.T) {
	setup_peer_discovery_test(t)

	origin, _ := test_host(t)
	long := "/dns/" + strings.Repeat("a", peer_address_maximum) + ".example/tcp/1443/p2p/" + origin
	short := "/dns/mochi.example/tcp/1443/p2p/" + origin
	peer_apply_addresses(origin, []string{long, short})
	addresses := peer_stored_addresses(origin)
	if len(addresses) != 1 || addresses[0] != short {
		t.Errorf("stored %d addresses, want only the one within peer_address_maximum", len(addresses))
	}
}

// TestPeerDiscoveredSavesEveryAddress is #153. The save throttle was one
// timestamp per peer, and the save wrote only the address of the call that
// passed it, so a peer discovered with several addresses at once had its first
// written and the rest held in memory only - pruned from disk, and forgotten at
// a restart.
func TestPeerDiscoveredSavesEveryAddress(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	first := "/ip4/192.0.2.20/tcp/1443/p2p/" + id
	second := "/ip4/192.0.2.20/udp/443/quic-v1/p2p/" + id
	peer_discovered_address(id, first)
	peer_discovered_address(id, second)

	db := db_open("db/peers.db")
	if n := db.integer("select count(*) from peers where id=?", id); n != 2 {
		t.Fatalf("THE POINT: peers.db holds %d of the peer's 2 addresses", n)
	}

	// Seen again within the hour: not rewritten. Over the hour: rewritten.
	db.exec("update peers set updated=1 where id=?", id)
	peer_discovered_address(id, first)
	if n := db.integer("select updated from peers where address=?", first); n != 1 {
		t.Error("an address seen again within peer_save_interval was rewritten")
	}
	peers_lock.Lock()
	p := peers[id]
	for i := range p.addresses {
		p.addresses[i].Saved -= peer_save_interval + 1
	}
	peers[id] = p
	peers_lock.Unlock()
	peer_discovered_address(id, second)
	if n := db.integer("select updated from peers where address=?", second); n <= 1 {
		t.Error("an address whose last write is over peer_save_interval old was not rewritten, so the prune deletes a row the registry uses")
	}
}

// TestPeerDiscoveryAdmits is #154. The stream handlers, the pubsub neighbour
// and the mDNS answer each registered a peer with no admission check; every
// discovery now passes the one rule.
func TestPeerDiscoveryAdmits(t *testing.T) {
	setup_peer_discovery_test(t)
	peer_discovery_fill(t)

	stranger, _ := test_host(t)
	peer_discovered_address(stranger, "/ip4/192.0.2.30/tcp/1443/p2p/"+stranger)
	if peer_held(stranger) {
		t.Error("THE POINT: a discovery registered a new peer at the ceiling")
	}

	answered, _ := test_host(t)
	pid, err := p2p_peer.Decode(answered)
	if err != nil {
		t.Fatal(err)
	}
	(&mdns_notifee{}).HandlePeerFound(p2p_peer.AddrInfo{ID: pid, Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/192.0.2.31/tcp/1443")}})
	if peer_held(answered) {
		t.Error("an mDNS answer registered a new peer at the ceiling")
	}

	// Freeing a slot admits the next.
	peers_lock.Lock()
	delete(peers, "unproven-peer-0")
	peers_lock.Unlock()
	peer_discovered_address(stranger, "/ip4/192.0.2.30/tcp/1443/p2p/"+stranger)
	if !peer_held(stranger) {
		t.Error("a discovery under the ceiling was refused")
	}
}

// TestPeerDiscoveryWakesOnlyOnChange is #155: each discovery woke the queue - a
// pass and a claim from every open Sender - and one runs for the relaying
// neighbour of every pubsub message, almost always a peer known at an address
// known.
func TestPeerDiscoveryWakesOnlyOnChange(t *testing.T) {
	setup_peer_discovery_test(t)

	woke := func() bool {
		time.Sleep(50 * time.Millisecond) // the wake runs on its own goroutine
		select {
		case <-queue_wake_ch:
			return true
		default:
			return false
		}
	}
	woke()

	id, _ := test_host(t)
	address := "/ip4/192.0.2.40/tcp/1443/p2p/" + id
	peer_discovered_address(id, address)
	if !woke() {
		t.Error("a new peer did not wake the queue")
	}
	peer_discovered_address(id, address)
	if woke() {
		t.Error("THE POINT: rediscovering a known peer at a known address woke the queue")
	}
	peer_discovered_address(id, "/ip4/192.0.2.41/tcp/1443/p2p/"+id)
	if !woke() {
		t.Error("a new address did not wake the queue")
	}
}

// TestPeerAddKnownRequiresTheSuffix is #159: an address a remote host's
// /_/p2p/info supplied was kept with no /p2p suffix, or one naming another
// peer, and failed at every dial with a warning mailed to the administrator.
func TestPeerAddKnownRequiresTheSuffix(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	other, _ := test_host(t)
	good := "/ip4/192.0.2.50/tcp/1443/p2p/" + id
	peer_add_known(id, []string{
		"/ip4/192.0.2.50/tcp/1443",                                 // no suffix
		"/ip4/192.0.2.51/tcp/1443/p2p/" + other,                    // another peer's
		"/ip4/0.0.0.0/tcp/1443/p2p/" + id,                          // unspecified
		"/dns/" + strings.Repeat("b", 600) + "/tcp/1443/p2p/" + id, // over the length bound
		good,
	})
	addresses := peer_stored_addresses(id)
	if len(addresses) != 1 || addresses[0] != good {
		t.Errorf("kept %v, want only %q", addresses, good)
	}
}

// TestPeersAddFromDatabaseRestoresEveryPeer is #172: startup loaded 100 peers,
// picked by an arbitrary row's time, and nothing consulted the table for the
// rest, so a server knowing more could not dial them after a restart. Every
// stored peer is restored; the most recently seen are dialled.
func TestPeersAddFromDatabaseRestoresEveryPeer(t *testing.T) {
	setup_peer_discovery_test(t)
	db := db_open("db/peers.db")
	ids := make([]string, 150)
	for i := range ids {
		ids[i], _ = test_host(t)
		db.exec("insert into peers ( id, address, updated, success, failure ) values ( ?, ?, ?, 7, 2 )", ids[i], "/ip4/192.0.2.60/tcp/1443/p2p/"+ids[i], 1000+i)
	}
	// A stale second row must not decide a peer's rank: its newest does.
	db.exec("insert into peers ( id, address, updated ) values ( ?, ?, 1 )", ids[0], "/ip4/192.0.2.61/tcp/1443/p2p/"+ids[0])
	db.exec("update peers set updated=5000 where id=? and updated>1", ids[0])

	peer_reconnect_lock.Lock()
	peer_reconnects = map[string]PeerReconnect{}
	peer_reconnect_lock.Unlock()
	peers_add_from_db(2)

	for _, id := range ids {
		if !peer_held(id) {
			t.Fatalf("THE POINT: stored peer %q was not restored", id)
		}
	}
	peers_lock.Lock()
	restored := peers[ids[1]].addresses[0]
	peers_lock.Unlock()
	if restored.Success != 7 || restored.Failure != 2 || restored.Saved != 1001 || restored.Updated != 1001 {
		t.Errorf("restored address evidence = %+v, want the row's", restored)
	}

	// The two most recently seen are dialled; with no host the dials fail and
	// enroll them for probing, which is how the test sees which were dialled.
	want := map[string]bool{ids[0]: true, ids[149]: true}
	var dialled map[string]bool
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		dialled = map[string]bool{}
		peer_reconnect_lock.Lock()
		for id := range peer_reconnects {
			dialled[id] = true
		}
		peer_reconnect_lock.Unlock()
		if len(dialled) >= 2 {
			break
		}
	}
	if len(dialled) != 2 || !dialled[ids[0]] || !dialled[ids[149]] {
		t.Errorf("dialled %v, want the two most recently seen %v", dialled, want)
	}
	peer_reconnect_lock.Lock()
	peer_reconnects = map[string]PeerReconnect{}
	peer_reconnect_lock.Unlock()
}

// TestPeerDisconnectedProbesOnlyWhatItNeeds is #138: every peer libp2p
// reported gone was enrolled for reconnect probing - a dial and a mesh-wide
// address request every few minutes, for days, for a throwaway identity that
// only ever dialled in.
func TestPeerDisconnectedProbesOnlyWhatItNeeds(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_directory_test_schema()
	reset_peer_reachability(t)

	inbound, _ := test_host(t)
	dialled, _ := test_host(t)
	queued, _ := test_host(t)
	peers_lock.Lock()
	peers[inbound] = Peer{ID: inbound, state: peer_state_connected}
	peers[dialled] = Peer{ID: dialled, state: peer_state_connected, dialled: true}
	peers[queued] = Peer{ID: queued, state: peer_state_connected}
	peers_lock.Unlock()
	db_open("db/queue.db").exec("insert into queue ( id, target, from_entity, to_entity, service, event, next_retry, created ) values ( 'row', ?, 'from', 'to', 'feeds', 'update', 0, 0 )", queued)

	peer_reconnect_lock.Lock()
	peer_reconnects = map[string]PeerReconnect{}
	peer_reconnect_lock.Unlock()
	for _, id := range []string{inbound, dialled, queued} {
		peer_disconnected(id)
	}
	peer_reconnect_lock.Lock()
	_, probe_inbound := peer_reconnects[inbound]
	_, probe_dialled := peer_reconnects[dialled]
	_, probe_queued := peer_reconnects[queued]
	peer_reconnects = map[string]PeerReconnect{}
	peer_reconnect_lock.Unlock()

	if probe_inbound {
		t.Error("THE POINT: a peer that only dialled in, and that nothing needs, was enrolled for probing")
	}
	if !probe_dialled {
		t.Error("a peer this host dialled was not probed when it dropped")
	}
	if !probe_queued {
		t.Error("a peer with deliveries queued was not probed when it dropped")
	}
}

// TestPeerReconnectProbeAsksOnlyForWantedPeers: a failed probe broadcast a
// peers/request for whatever was probed. One is sent only for a peer this host
// has a reason to reach.
func TestPeerReconnectProbeAsksOnlyForWantedPeers(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_directory_test_schema()
	reset_peer_reachability(t)

	unwanted, _ := test_host(t)
	wanted, _ := test_host(t)
	peers_lock.Lock()
	peers[wanted] = Peer{ID: wanted, dialled: true}
	peers_lock.Unlock()
	peer_reconnect_lock.Lock()
	peer_reconnects[unwanted] = PeerReconnect{NextRetry: now()}
	peer_reconnects[wanted] = PeerReconnect{NextRetry: now()}
	peer_reconnect_lock.Unlock()
	t.Cleanup(func() {
		peer_reconnect_lock.Lock()
		delete(peer_reconnects, unwanted)
		delete(peer_reconnects, wanted)
		peer_reconnect_lock.Unlock()
	})

	peer_reconnect_probe(unwanted)
	peer_reconnect_probe(wanted)
	if !rate_limit_peer_request.allow(unwanted) {
		t.Error("THE POINT: a probe of a peer nothing wants broadcast an address request for it")
	}
	if rate_limit_peer_request.allow(wanted) {
		t.Error("a probe of a peer this host dialled asked the mesh for nothing")
	}
}

// TestPeerReconnectProbeRecoversAConnectedPeer is #168's second half: a probe
// that found the peer connected returned at once, clearing nothing, so a peer
// silenced while its connection stayed up stayed silent and was probed every
// ten seconds.
func TestPeerReconnectProbeRecoversAConnectedPeer(t *testing.T) {
	setup_peer_discovery_test(t)
	reset_peer_reachability(t)

	id, _ := test_host(t)
	peers_lock.Lock()
	peers[id] = Peer{ID: id, state: peer_state_connected}
	peers_lock.Unlock()
	for i := 0; i < peer_silent_failure_threshold; i++ {
		peer_mark_send_failed(id)
	}
	if !peer_is_silent(id) {
		t.Fatal("control: the peer is not silent")
	}

	peer_reconnect_probe(id)
	peer_reconnect_lock.Lock()
	_, scheduled := peer_reconnects[id]
	delete(peer_reconnects, id)
	peer_reconnect_lock.Unlock()
	if peer_is_silent(id) {
		t.Error("THE POINT: a probe that found the peer connected left it silent")
	}
	if scheduled {
		t.Error("a probe that found the peer connected left it enrolled, to be probed again in ten seconds")
	}
}

// TestPeerWritesSurviveADatabaseFault is #171: the peer cache's writes used
// DB.exec, which panics on any SQLite error, on goroutines with no recover - a
// full disk or a lock held past the busy timeout ended the whole server for a
// database the code treats as a rebuildable cache.
func TestPeerWritesSurviveADatabaseFault(t *testing.T) {
	setup_peer_discovery_test(t)
	pdb := db_open("db/peers.db")
	pdb.exec("drop table peers")
	pdb.exec("drop table records")
	pdb.exec("drop table if exists names")

	id, _ := test_host(t)
	peer_names_lock.Lock()
	peer_names[id] = []PeerName{{Name: "faulted.example", Updated: now()}}
	peer_names_lock.Unlock()
	t.Cleanup(func() {
		peer_names_lock.Lock()
		delete(peer_names, id)
		peer_names_lock.Unlock()
	})
	for name, write := range map[string]func(){
		"peer_discovered_address": func() { peer_discovered_address(id, "/ip4/192.0.2.70/tcp/1443/p2p/"+id) },
		"peer_addresses_failed":   func() { peer_addresses_failed(id) },
		"peer_address_drop":       func() { peer_address_drop(id, "/ip4/192.0.2.70/tcp/1443") },
		"peers_prune":             func() { peers_prune() },
		"peer_names_save":         func() { peer_names_save(id) },
		"peer_names_sweep":        func() { peer_names_sweep(now()) },
		"peer_record_save":        func() { peer_record_save(id, []byte("record"), 1) },
		"peer_records_sweep":      func() { peer_records_sweep(now()) },
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("THE POINT: %s panicked on a peers.db fault, which ends the process on a goroutine with no recover: %v", name, r)
				}
			}()
			write()
		}()
	}
}

// TestInboundNeighbourStaysUnproven is #2112: a successful connect stamped
// Success on whatever connection to the peer came first, an inbound one's
// remote address included, so any node that dialled in and relayed one
// announcement was proven and escaped the unproven ceiling admission is held
// to. Only an address this host dialled and reached is stamped.
func TestInboundNeighbourStaysUnproven(t *testing.T) {
	setup_peer_discovery_test(t)
	host := func() p2p_host.Host {
		h, err := p2p.New(p2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("test host: %v", err)
		}
		t.Cleanup(func() { h.Close() })
		return h
	}
	near, inbound, outbound := host(), host(), host()
	saved := net_me
	net_me = near
	t.Cleanup(func() { net_me = saved })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := inbound.Connect(ctx, p2p_peer.AddrInfo{ID: near.ID(), Addrs: near.Addrs()}); err != nil {
		t.Fatalf("connect in: %v", err)
	}
	if err := near.Connect(ctx, p2p_peer.AddrInfo{ID: outbound.ID(), Addrs: outbound.Addrs()}); err != nil {
		t.Fatalf("connect out: %v", err)
	}
	peers_lock.Lock()
	for _, h := range []p2p_host.Host{inbound, outbound} {
		peers[h.ID().String()] = Peer{ID: h.ID().String(), state: peer_state_connected}
	}
	peers_lock.Unlock()

	proven := func(id string) bool {
		peers_lock.Lock()
		defer peers_lock.Unlock()
		for _, a := range peers[id].addresses {
			if a.Success > 0 {
				return true
			}
		}
		return false
	}
	peer_refresh_connected_address(inbound.ID().String())
	peer_refresh_connected_address(outbound.ID().String())
	if proven(inbound.ID().String()) {
		t.Error("THE POINT: a neighbour that only dialled in was recorded as proven")
	}
	if !proven(outbound.ID().String()) {
		t.Error("control: a peer this host dialled and reached was not recorded as proven")
	}
}

// TestPeerConnectLeavesAPrunedPeerOut is #2113: a peer pruned while
// peer_connect dialled it was put back as an empty Peer, with no addresses,
// counting against the registry with nothing to dial.
func TestPeerConnectLeavesAPrunedPeerOut(t *testing.T) {
	setup_peer_discovery_test(t)
	id, _ := test_host(t)
	peers_lock.Lock()
	peers[id] = Peer{ID: id, addresses: []PeerAddress{{Address: "/ip4/192.0.2.7/tcp/1443/p2p/" + id}}}
	peers_lock.Unlock()
	dialling := make(chan struct{})
	release := make(chan struct{})
	saved := peer_dial
	peer_dial = func(string, []string) bool {
		close(dialling)
		<-release
		return false
	}
	t.Cleanup(func() { peer_dial = saved })

	done := make(chan struct{})
	go func() {
		peer_connect(id)
		close(done)
	}()
	<-dialling
	peers_lock.Lock()
	delete(peers, id) // pruned mid-dial
	peers_lock.Unlock()
	close(release)
	<-done

	peers_lock.Lock()
	p, held := peers[id]
	peers_lock.Unlock()
	if held {
		t.Errorf("THE POINT: a peer pruned while it was dialled was put back: %+v", p)
	}
}
