// Mochi server: peer address discovery unit tests
// Copyright Alistair Cunningham 2026
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
	pdb.exec("create table if not exists peers ( id text not null, address text not null, updated integer not null, primary key ( id, address ) )")

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
