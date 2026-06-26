// Mochi server: #47 — peer_protocol_open must not hang forever when a stale
// "Connected" peer forces net_me.NewStream to dial an address it can't reach.
// Before the fix, NewStream was called with net_context (no deadline) and the
// open looped indefinitely (the 13h prod reseed hang). Now it is bounded by
// peer_stream_open_timeout.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"

	p2p "github.com/libp2p/go-libp2p"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// TestPeerProtocolOpenTimesOut reproduces the #47 hang shape: Mochi believes it
// is connected to a peer (peer_connect short-circuits to NewStream) but the only
// libp2p address is a black-holed TEST-NET one, so NewStream must dial and can
// never complete. With the bounded context the open returns an error within the
// (lowered) timeout; without it the test's own 5s guard trips on the hang.
func TestPeerProtocolOpenTimesOut(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	// A real libp2p host so NewStream genuinely attempts a dial.
	h, err := p2p.New(p2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"))
	if err != nil {
		t.Fatalf("test host: %v", err)
	}
	defer h.Close()
	saved_me := net_me
	net_me = h
	defer func() { net_me = saved_me }()

	// Peer Mochi thinks is connected (stale state) with only a black-holed
	// address in the libp2p peerstore — the dial can never succeed.
	id, _ := test_host(t)
	pid, err := p2p_peer.Decode(id)
	if err != nil {
		t.Fatal(err)
	}
	blackhole, err := multiaddr.NewMultiaddr("/ip4/192.0.2.50/udp/1443/quic-v1")
	if err != nil {
		t.Fatal(err)
	}
	h.Peerstore().AddAddr(pid, blackhole, time.Hour)
	peers_lock.Lock()
	peers[id] = Peer{ID: id, state: peer_state_connected}
	peers_lock.Unlock()

	saved_timeout := peer_stream_open_timeout
	peer_stream_open_timeout = 300 * time.Millisecond
	defer func() { peer_stream_open_timeout = saved_timeout }()

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, e := peer_protocol_open(id, protocol_stream)
		done <- e
	}()

	select {
	case e := <-done:
		if e == nil {
			t.Fatal("expected an error opening a stream to a black-holed peer")
		}
		if d := time.Since(start); d > 3*time.Second {
			t.Fatalf("open returned in %v — NewStream not bounded by peer_stream_open_timeout", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer_protocol_open HUNG (>5s) — #47 NewStream deadline not applied")
	}
}
