// Mochi server: #48 — a dial that fails because the address now answers as a
// DIFFERENT peer (an identity rotation — re-paired/rebuilt server) must drop
// that stale address so the reconnect manager stops dialing the defunct id
// forever (the peer-id-mismatch CPU churn).
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	p2p_sec "github.com/libp2p/go-libp2p/core/sec"
	p2p_swarm "github.com/libp2p/go-libp2p/p2p/net/swarm"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// TestNetDropsRotatedAddress: a peer-id mismatch on a dialled address drops it.
func TestNetDropsRotatedAddress(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	stale, _ := test_host(t)   // defunct identity we still hold an address for
	current, _ := test_host(t) // who that address actually belongs to now
	bare := "/ip4/127.0.0.1/udp/1446/quic-v1"
	peer_add_known(stale, []string{bare + "/p2p/" + stale})
	if n := peer_addresses_count(stale); n != 1 {
		t.Fatalf("setup: stale has %d addresses, want 1", n)
	}

	ma, err := multiaddr.NewMultiaddr(bare)
	if err != nil {
		t.Fatal(err)
	}
	staleID, _ := p2p_peer.Decode(stale)
	curID, _ := p2p_peer.Decode(current)
	dialErr := &p2p_swarm.DialError{
		Peer: staleID,
		DialErrors: []p2p_swarm.TransportError{
			{Address: ma, Cause: p2p_sec.ErrPeerIDMismatch{Expected: staleID, Actual: curID}},
		},
	}

	net_drop_rotated_addresses(stale, dialErr)

	if n := peer_addresses_count(stale); n != 0 {
		t.Fatalf("rotated address not dropped: %d remain", n)
	}
}

// TestNetKeepsAddressOnPlainFailure: an ordinary unreachable dial (no mismatch)
// must NOT drop the address — the peer may just be transiently offline.
func TestNetKeepsAddressOnPlainFailure(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	id, _ := test_host(t)
	bare := "/ip4/192.0.2.9/udp/1443/quic-v1"
	peer_add_known(id, []string{bare + "/p2p/" + id})

	idDec, _ := p2p_peer.Decode(id)
	ma, _ := multiaddr.NewMultiaddr(bare)
	dialErr := &p2p_swarm.DialError{
		Peer:       idDec,
		DialErrors: []p2p_swarm.TransportError{{Address: ma, Cause: p2p_swarm.ErrNoTransport}},
	}

	net_drop_rotated_addresses(id, dialErr)

	if n := peer_addresses_count(id); n != 1 {
		t.Fatalf("plain dial failure wrongly dropped the address: %d remain", n)
	}
}
