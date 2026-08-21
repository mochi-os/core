// Mochi server: #48 - a dial that fails because the address now answers as a
// different peer (an identity rotation) must drop that stale address, or the
// reconnect manager dials the defunct id forever.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

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
	stale_i_d, _ := p2p_peer.Decode(stale)
	cur_i_d, _ := p2p_peer.Decode(current)
	dial_error := &p2p_swarm.DialError{
		Peer: stale_i_d,
		DialErrors: []p2p_swarm.TransportError{
			{Address: ma, Cause: p2p_sec.ErrPeerIDMismatch{Expected: stale_i_d, Actual: cur_i_d}},
		},
	}

	net_drop_rotated_addresses(stale, dial_error)

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

	id_dec, _ := p2p_peer.Decode(id)
	ma, _ := multiaddr.NewMultiaddr(bare)
	dial_error := &p2p_swarm.DialError{
		Peer:       id_dec,
		DialErrors: []p2p_swarm.TransportError{{Address: ma, Cause: p2p_swarm.ErrNoTransport}},
	}

	net_drop_rotated_addresses(id, dial_error)

	if n := peer_addresses_count(id); n != 1 {
		t.Fatalf("plain dial failure wrongly dropped the address: %d remain", n)
	}
}
