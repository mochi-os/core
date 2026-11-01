// Mochi server: peers subsystem unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// The in-memory peer-silence fast-fail. The gate lives in peer_protocol_open;
// those tests drive it directly, keeping the libp2p layer out of the picture.
// The broadcast mesh's forgotten-subscription repair runs two real hosts.

package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	p2p "github.com/libp2p/go-libp2p"
	p2p_pubsub "github.com/libp2p/go-libp2p-pubsub"
	p2p_host "github.com/libp2p/go-libp2p/core/host"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
)

// reset_peer_reachability is a test helper that clears the in-memory
// map and any state left by an earlier test. The reachability map is
// a package-level var so test order would otherwise pollute later
// runs.
func reset_peer_reachability(t *testing.T) {
	t.Helper()
	peer_reachability_lock.Lock()
	peer_reachability = map[string]PeerReachability{}
	peer_reachability_lock.Unlock()
}

// TestPeerIsSilentEmptyMap confirms a brand-new peer (never recorded
// in the map) is NEVER silent. This is the cold-start case after a
// server restart - every peer gets a fresh trial.
func TestPeerIsSilentEmptyMap(t *testing.T) {
	reset_peer_reachability(t)
	if peer_is_silent("12D3KooWFakePeerForTest1") {
		t.Error("unknown peer must not be silent (cold-start should allow first attempt)")
	}
}

// TestPeerIsSilentBelowThreshold confirms a peer that has failed
// fewer times than the threshold is not silenced. The threshold
// exists to ignore transient blips.
func TestPeerIsSilentBelowThreshold(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest2"
	for i := 0; i < peer_silent_failure_threshold-1; i++ {
		peer_mark_send_failed(id)
	}
	if peer_is_silent(id) {
		t.Errorf("peer with %d failures (below threshold %d) must not be silent",
			peer_silent_failure_threshold-1, peer_silent_failure_threshold)
	}
}

// TestPeerIsSilentAtThreshold confirms a peer that has hit the
// threshold IS silenced - this is the load-bearing gate. Three
// consecutive failures inside the skip window means the next caller
// fast-fails without going near libp2p.
func TestPeerIsSilentAtThreshold(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest3"
	for i := 0; i < peer_silent_failure_threshold; i++ {
		peer_mark_send_failed(id)
	}
	if !peer_is_silent(id) {
		t.Errorf("peer with %d consecutive failures must be silent", peer_silent_failure_threshold)
	}
}

// TestPeerIsSilentSuccessResetsCounter confirms a success clears the
// silent state. This is how a returning peer recovers - the trial
// send after the skip window succeeds and the counter resets to 0.
func TestPeerIsSilentSuccessResetsCounter(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest4"
	for i := 0; i < peer_silent_failure_threshold; i++ {
		peer_mark_send_failed(id)
	}
	if !peer_is_silent(id) {
		t.Fatal("peer must be silent after threshold failures")
	}
	peer_mark_send_success(id)
	if peer_is_silent(id) {
		t.Error("peer must not be silent after a successful send (success clears counter)")
	}
}

// TestPeerIsSilentIsDurable: silence stays true however old the last attempt
// is. Only peer_mark_reachable or peer_mark_send_success clears it.
func TestPeerIsSilentIsDurable(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest5"
	peer_reachability_lock.Lock()
	peer_reachability[id] = PeerReachability{
		ConsecutiveFailures: peer_silent_failure_threshold + 5,
		LastAttempt:         now() - 86400, // a day ago — old enough to expose any time-based lapse
	}
	peer_reachability_lock.Unlock()
	if !peer_is_silent(id) {
		t.Error("peer with stale LastAttempt must STILL be silent (durable until peer_mark_reachable / peer_mark_send_success clears)")
	}
}

// TestPeerCrossingThresholdSchedulesReconnect: crossing the threshold is the
// only enrollment path for a peer never libp2p-disconnected from, so without it
// silence is permanent.
func TestPeerCrossingThresholdSchedulesReconnect(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForReconnectSchedule"

	// Drain any pre-existing schedule entry.
	peer_reconnect_lock.Lock()
	delete(peer_reconnects, id)
	peer_reconnect_lock.Unlock()

	// Below threshold: no schedule.
	for i := 0; i < peer_silent_failure_threshold-1; i++ {
		peer_mark_send_failed(id)
	}
	peer_reconnect_lock.Lock()
	_, scheduled_below := peer_reconnects[id]
	peer_reconnect_lock.Unlock()
	if scheduled_below {
		t.Errorf("peer below silent threshold was scheduled for reconnect; want unscheduled")
	}

	// Crossing the threshold schedules it.
	peer_mark_send_failed(id)
	peer_reconnect_lock.Lock()
	_, scheduled_after := peer_reconnects[id]
	peer_reconnect_lock.Unlock()
	if !scheduled_after {
		t.Error("peer at silent threshold was not scheduled for reconnect; peer_reconnect_manager will never probe it and silence will be permanent")
	}

	// Cleanup so other tests aren't affected.
	peer_reconnect_lock.Lock()
	delete(peer_reconnects, id)
	peer_reconnect_lock.Unlock()
}

// TestPeerMarkReachableClearsSilence: silence must clear immediately, so rows
// woken by queue_resurrect_peer can trial the new connection.
func TestPeerMarkReachableClearsSilence(t *testing.T) {
	reset_peer_reachability(t)
	id := "12D3KooWFakePeerForTest6"
	for i := 0; i < peer_silent_failure_threshold; i++ {
		peer_mark_send_failed(id)
	}
	if !peer_is_silent(id) {
		t.Fatal("peer must be silent after threshold failures (precondition)")
	}
	peer_mark_reachable(id)
	if peer_is_silent(id) {
		t.Error("peer_mark_reachable must immediately clear silence; queue_resurrect_peer needs the next send to actually trial the connection")
	}
}

// TestPeerIsSilentBootstrap confirms bootstrap peers are never
// silenced regardless of failure count. Bootstrap is our default
// publisher / fallback - silencing it would brick the whole pubsub
// mesh when it goes down briefly.
func TestPeerIsSilentBootstrap(t *testing.T) {
	reset_peer_reachability(t)
	id := peers_bootstrap[0].ID
	for i := 0; i < peer_silent_failure_threshold*10; i++ {
		peer_mark_send_failed(id)
	}
	if peer_is_silent(id) {
		t.Error("bootstrap peer must never be silent regardless of failure count")
	}
}

// TestPeerIsSilentSelf confirms self-loop is never silent. The
// in-process pipe can't fail in the way the libp2p connect can;
// marking self as silent would break all local fan-out.
func TestPeerIsSilentSelf(t *testing.T) {
	reset_peer_reachability(t)
	// net_id may be empty in tests where p2p hasn't initialised;
	// peer_is_silent's id=="" guard covers that case. Test the
	// explicit net_id==id path with a fake-but-equal value.
	saved := net_id
	net_id = "12D3KooWFakeSelf"
	defer func() { net_id = saved }()
	for i := 0; i < peer_silent_failure_threshold*10; i++ {
		peer_mark_send_failed(net_id)
	}
	if peer_is_silent(net_id) {
		t.Error("self peer must never be silent")
	}
}

// TestPeerAdmitBoundsUnprovenPeers is #600. peer_record_event applies any
// validly self-signed record, and minting a keypair is free, so without a
// ceiling a flood of invented ids grows `peers` and peers.db at the inbound
// pubsub rate - and the rows survive until the unproven prune days later.
func TestPeerAdmitBoundsUnprovenPeers(t *testing.T) {
	peers_lock.Lock()
	saved := peers
	peers = map[string]Peer{}
	peers_lock.Unlock()
	t.Cleanup(func() {
		peers_lock.Lock()
		peers = saved
		peers_lock.Unlock()
	})

	if !peer_admit("a-brand-new-peer") {
		t.Fatal("a new peer was refused with the table empty")
	}

	// Fill to the ceiling with never-connected peers, the shape a record flood
	// produces: an address, no success.
	peers_lock.Lock()
	for i := 0; i < peer_unproven_maximum; i++ {
		id := fmt.Sprintf("unproven-peer-%d", i)
		peers[id] = Peer{ID: id, addresses: []PeerAddress{{Address: "/ip4/10.0.0.1/tcp/1"}}}
	}
	peers_lock.Unlock()

	if peer_admit("one-peer-too-many") {
		t.Error("a new unproven peer was admitted at the ceiling: the table grows at the inbound record rate")
	}

	// A peer already held keeps receiving address updates - those are for
	// something this host has a reason to know about.
	if !peer_admit("unproven-peer-0") {
		t.Error("an already-held peer was refused: address updates for known peers must not be collateral")
	}

	// And a peer that has actually connected does not count toward the ceiling,
	// so proving one frees a slot for a new arrival.
	peers_lock.Lock()
	proven := peers["unproven-peer-0"]
	proven.addresses = []PeerAddress{{Address: "/ip4/10.0.0.1/tcp/1", Success: 1}}
	peers["unproven-peer-0"] = proven
	peers_lock.Unlock()
	if !peer_admit("a-different-new-peer") {
		t.Error("proving a peer did not free a slot: the ceiling counts connected peers, which it must not")
	}
}

// TestPeersBootstrapIsolated: a server isolated in development has no
// bootstrap peers, so a test suite's server never meets the Mochi network.
func TestPeersBootstrapIsolated(t *testing.T) {
	saved, list := net_isolated, peers_bootstrap
	t.Cleanup(func() { net_isolated, peers_bootstrap = saved, list })
	net_isolated = false
	peers_bootstrap_load()
	if len(peers_bootstrap) == 0 {
		t.Fatal("CONTROL: a server not isolated has the bootstrap peers")
	}
	net_isolated = true
	peers_bootstrap_load()
	if len(peers_bootstrap) != 0 {
		t.Errorf("THE POINT: an isolated server has no bootstrap peers: %v", peers_bootstrap)
	}
}

// TestMeshForgottenStep: a peer relaying to the topic while missing from its
// members is answered for reconnection at the next sample it is still missing,
// then left alone for the cooldown.
func TestMeshForgottenStep(t *testing.T) {
	cooldown := int64(mesh_forgotten_cooldown / time.Second)
	none := map[string]bool{}
	var s mesh_forgotten_state
	var lost []string

	s, lost = mesh_forgotten_step(s, map[string]bool{"a": true, "b": true}, map[string]bool{"b": true}, 100)
	if len(lost) != 0 {
		t.Fatalf("a sender is only a suspect at its first sample: %v", lost)
	}
	if !s.suspects["a"] || s.suspects["b"] {
		t.Fatalf("THE POINT: the sender missing from the members is the suspect, the member is not: %v", s.suspects)
	}

	s, lost = mesh_forgotten_step(s, none, none, 115)
	if !slices.Equal(lost, []string{"a"}) {
		t.Fatalf("THE POINT: a suspect still missing at the next sample is answered, sending or not: %v", lost)
	}

	s, lost = mesh_forgotten_step(s, map[string]bool{"a": true}, none, 130)
	s, lost = mesh_forgotten_step(s, map[string]bool{"a": true}, none, 145)
	if len(lost) != 0 || s.suspects["a"] {
		t.Fatalf("a peer just reconnected is left alone for the cooldown: lost %v, suspects %v", lost, s.suspects)
	}

	s, _ = mesh_forgotten_step(s, map[string]bool{"a": true}, none, 115+cooldown)
	s, lost = mesh_forgotten_step(s, none, none, 130+cooldown)
	if !slices.Equal(lost, []string{"a"}) {
		t.Fatalf("after the cooldown the peer is answered again: %v", lost)
	}

	s, _ = mesh_forgotten_step(mesh_forgotten_state{}, map[string]bool{"c": true}, none, 200)
	s, lost = mesh_forgotten_step(s, none, map[string]bool{"c": true}, 215)
	if len(lost) != 0 {
		t.Fatalf("a suspect that has since joined the members is not answered: %v", lost)
	}
}

// TestMeshForgottenSubscriptionRecovers reproduces, with two real hosts, the
// GossipSub state yuzu was found in: a's outbound pubsub stream to b dies with
// the connection left open, so a drops b from the topic and never hears its
// subscription again, while b goes on relaying to a. a's broadcasts then reach
// no one, and dialling b again changes nothing since it is connected. The
// repair notes b as a sender, answers it at the next sample and closes the
// connection; the reconnect brings b back as a member and a's broadcasts with
// it.
func TestMeshForgottenSubscriptionRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host := func() p2p_host.Host {
		h, err := p2p.New(p2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("test host: %v", err)
		}
		t.Cleanup(func() { h.Close() })
		return h
	}
	a, b := host(), host()

	saved_me, saved_id := net_me, net_id
	net_me, net_id = a, a.ID().String()
	t.Cleanup(func() { net_me, net_id = saved_me, saved_id })
	mesh_senders_take()

	received := make(chan []byte, 64)
	join := func(h p2p_host.Host, validate bool, out chan []byte) *p2p_pubsub.Topic {
		gs, err := p2p_pubsub.NewGossipSub(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
		if validate {
			if err := gs.RegisterTopicValidator(pubsub_topic, pubsub_validate); err != nil {
				t.Fatal(err)
			}
		}
		topic, err := gs.Join(pubsub_topic)
		if err != nil {
			t.Fatal(err)
		}
		subscription, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for {
				m, err := subscription.Next(ctx)
				if err != nil {
					return
				}
				if out != nil && m.ReceivedFrom != h.ID() {
					out <- m.Data
				}
			}
		}()
		return topic
	}
	ta := join(a, true, nil)
	tb := join(b, false, received)

	member := func(topic *p2p_pubsub.Topic, id p2p_peer.ID) bool {
		return slices.Contains(topic.ListPeers(), id)
	}
	wait := func(what string, condition func() bool) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			if condition() {
				return
			}
		}
		t.Fatalf("timed out waiting until %s", what)
	}
	reaches := func(message string) bool {
		for len(received) > 0 {
			<-received
		}
		if err := ta.Publish(ctx, []byte(message)); err != nil {
			t.Fatalf("a publishes: %v", err)
		}
		select {
		case data := <-received:
			return string(data) == message
		case <-time.After(time.Second):
			return false
		}
	}
	dial := func() {
		if err := a.Connect(ctx, p2p_peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}); err != nil {
			t.Fatalf("a dials b: %v", err)
		}
	}

	dial()
	wait("each host lists the other", func() bool { return member(ta, b.ID()) && member(tb, a.ID()) })
	wait("CONTROL: a's broadcast reaches b while each lists the other", func() bool { return reaches("before") })

	// a's outbound pubsub stream to b dies, the connection left open, as
	// closing a duplicate connection does.
	for _, c := range a.Network().ConnsToPeer(b.ID()) {
		for _, s := range c.GetStreams() {
			if s.Stat().Direction == p2p_network.DirOutbound && strings.HasPrefix(string(s.Protocol()), "/meshsub/") {
				s.Reset()
			}
		}
	}
	wait("a drops b from the topic", func() bool { return !member(ta, b.ID()) })
	mesh_senders_take()
	wait("b relays to a, which does not list it", func() bool {
		if err := tb.Publish(ctx, []byte("from b")); err != nil {
			t.Fatalf("b publishes: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		return mesh_senders_take()[b.ID().String()]
	})
	dial()
	time.Sleep(time.Second)
	if member(ta, b.ID()) || !member(tb, a.ID()) || reaches("forgotten") {
		t.Fatal("CONTROL: with b's subscription forgotten, dialling the connected b leaves a's broadcasts reaching no one")
	}

	// Two samples of the repair, b relaying before each.
	members := func() map[string]bool {
		out := map[string]bool{}
		for _, id := range ta.ListPeers() {
			out[id.String()] = true
		}
		return out
	}
	var state mesh_forgotten_state
	var lost []string
	for sample := range 2 {
		if err := tb.Publish(ctx, []byte("from b")); err != nil {
			t.Fatalf("b publishes: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		state, lost = mesh_forgotten_step(state, mesh_senders_take(), members(), now()+int64(sample)*15)
	}
	if !slices.Equal(lost, []string{b.ID().String()}) {
		t.Fatalf("THE POINT: b, relaying to a without being listed, is answered for reconnection: %v", lost)
	}
	for _, id := range lost {
		mesh_forgotten_reconnect(id)
	}
	wait("the connection closes and b drops a", func() bool {
		return a.Network().Connectedness(b.ID()) != p2p_network.Connected && !member(tb, a.ID())
	})

	// The reconnect peer_disconnected schedules, ten seconds on in a server.
	time.Sleep(500 * time.Millisecond)
	dial()
	wait("THE POINT: a lists b again", func() bool { return member(ta, b.ID()) })
	wait("THE POINT: a's broadcasts reach b again", func() bool { return reaches("after") })
}
