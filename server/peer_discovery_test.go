// Mochi server: peer address discovery unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Tests for the peers/publish + peers/request address-discovery path
// (ticket #424): a server that knows another only by peer id must be
// able to learn its addresses from a signed pubsub announcement, and
// the send path must be able to solicit one. The original mechanism
// was disabled in v0.1.6 when wildcard binds removed the single
// self-evident listen address; these tests pin the revived behaviour.

package main

import (
	"fmt"
	"strings"
	"testing"
)

// setup_peer_discovery_test gives a fresh data_dir (via the replication
// harness), a peers.db with schema, and an empty in-memory peer
// registry. Returns a cleanup that restores the prior registry.
func setup_peer_discovery_test(t *testing.T) func() {
	t.Helper()
	cleanup := setup_replication_test(t)

	pdb := db_open("db/peers.db")
	pdb.exec("create table if not exists peers ( id text not null, address text not null, updated integer not null, success integer not null default 0, failure integer not null default 0, primary key ( id, address ) )")
	pdb.exec("create table if not exists records ( id text not null primary key, record blob not null, sequence integer not null, updated integer not null )")

	peers_lock.Lock()
	saved := peers
	peers = map[string]Peer{}
	peers_lock.Unlock()

	return func() {
		peers_lock.Lock()
		peers = saved
		peers_lock.Unlock()
		cleanup()
	}
}

// publish_event builds the Event shape pubsub_receive hands to
// peer_publish_event: origin from the verified GossipSub envelope,
// announced addresses in content.
func publish_event(origin, addresses string) *Event {
	return &Event{origin: origin, content: map[string]any{"addresses": addresses}}
}

func TestPeerPublishEventAppliesAddresses(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	announced := "not-a-multiaddr,/ip4/192.0.2.20/tcp/1443/p2p/" + origin
	peer_publish_event(publish_event(origin, announced))

	if n := peer_addresses_count(origin); n != 1 {
		t.Errorf("addresses applied = %d, want 1 (invalid entry skipped)", n)
	}
}

func TestPeerPublishEventCapsAddresses(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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

// TestPeerConnectRetryEnrollsFailedDial: a failed startup dial must
// enroll the peer in the reconnect manager's backoff probes. The other
// enrollment triggers (libp2p disconnect, silent-failure threshold)
// require having reached the peer or having traffic for it, so without
// this a server that boots before its network is ready stays isolated
// until restart (observed live: hotel network blocking the bootstrap
// port left instances out of the mesh indefinitely).
func TestPeerConnectRetryEnrollsFailedDial(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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


// TestPeerPublishEventDropsSelfRelay: a circuit address that relays
// through ourselves is dead weight (we reach the peer directly over its
// reservation), so it is dropped on apply; a third-party relay and a
// direct address are kept.
func TestPeerPublishEventDropsSelfRelay(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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


func TestPeerAddressesNormalise(t *testing.T) {
	id, _ := test_host(t)
	other, _ := test_host(t)

	bare := "/ip4/198.51.100.3/tcp/1443"
	suffixed := bare + "/p2p/" + id

	out, bad := peer_addresses_normalise(id, []string{bare, suffixed, " ", ""})
	if bad != "" {
		t.Fatalf("unexpected rejection %q", bad)
	}
	if len(out) != 2 || out[0] != suffixed || out[1] != suffixed {
		t.Errorf("normalised = %v, want two copies of %q", out, suffixed)
	}

	if _, bad := peer_addresses_normalise(id, []string{bare + "/p2p/" + other}); bad == "" {
		t.Error("address suffixed with a different peer must be rejected")
	}
	if _, bad := peer_addresses_normalise(id, []string{"junk"}); bad == "" {
		t.Error("unparseable address must be rejected")
	}
}
