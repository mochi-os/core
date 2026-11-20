// Mochi server: circuit-relay participation unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	p2p "github.com/libp2p/go-libp2p"
	p2p_host "github.com/libp2p/go-libp2p/core/host"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	pbv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/pb"
)

// TestRelayAddrinfo: a relay candidate's dial target keeps only direct
// addresses — the /p2p/ suffix is stripped and circuit addresses (a
// relay reached through another relay) are dropped.
func TestRelayAddrinfo(t *testing.T) {
	id, _ := test_host(t)
	hop, _ := test_host(t)

	ai := relay_addrinfo(id, []string{
		"/ip4/198.51.100.9/tcp/1443/p2p/" + id,                          // direct, kept
		"/ip4/192.0.2.1/tcp/1443/p2p/" + hop + "/p2p-circuit/p2p/" + id, // circuit, dropped
		"garbage", // unparseable, dropped
	})
	if ai.ID.String() != id {
		t.Fatalf("id = %q, want %q", ai.ID, id)
	}
	if len(ai.Addrs) != 1 {
		t.Errorf("kept %d addresses, want 1 (direct only)", len(ai.Addrs))
	}
}

// TestPeerRelaysExpiry: a relay flag is a candidate only within the
// freshness window, and the sweep drops stale ones.
func TestPeerRelaysExpiry(t *testing.T) {
	setup_peer_discovery_test(t)

	fresh, _ := test_host(t)
	stale, _ := test_host(t)
	peer_relay_seen(fresh, 0)
	peer_relays_lock.Lock()
	peer_relays[stale] = peer_relay_record{seen: now() - peer_relay_maximum_age - 10}
	peer_relays_lock.Unlock()

	got := map[string]bool{}
	for _, id := range peer_relays_fresh() {
		got[id] = true
	}
	if !got[fresh] {
		t.Error("fresh relay flag not a candidate")
	}
	if got[stale] {
		t.Error("stale relay flag still a candidate")
	}

	peer_relays_sweep()
	peer_relays_lock.Lock()
	_, present := peer_relays[stale]
	peer_relays_lock.Unlock()
	if present {
		t.Error("sweep did not drop the stale flag")
	}
}

// TestPeerRelaySeenIgnoresSelf: we never list ourselves as a relay
// candidate.
func TestPeerRelaySeenIgnoresSelf(t *testing.T) {
	setup_peer_discovery_test(t)

	saved := net_id
	id, _ := test_host(t)
	net_id = id
	defer func() { net_id = saved }()

	peer_relay_seen(id, 0)
	for _, f := range peer_relays_fresh() {
		if f == id {
			t.Error("own id recorded as a relay")
		}
	}
}

// TestPeerPublishEventRelayFlag: the relay flag in a publish is
// recorded for AutoRelay candidate selection.
func TestPeerPublishEventRelayFlag(t *testing.T) {
	setup_peer_discovery_test(t)

	id, key := test_host(t)
	encoded := test_signed_record(t, key, id, []string{"/ip4/192.0.2.5/tcp/1443"}, 1)
	peer_publish_event(&Event{origin: id, content: map[string]any{"record": encoded, "relay": "true"}})

	found := false
	for _, f := range peer_relays_fresh() {
		if f == id {
			found = true
		}
	}
	if !found {
		t.Error("relay flag from publish not recorded")
	}

	// A publish without the flag must not mark a peer as a relay.
	plain, _ := test_host(t)
	peer_publish_event(publish_event(plain, "/ip4/192.0.2.6/tcp/1443/p2p/"+plain))
	for _, f := range peer_relays_fresh() {
		if f == plain {
			t.Error("peer marked as relay without announcing it")
		}
	}
}

// TestNetRelayCandidates: the AutoRelay source offers bootstrap relays
// plus flagged peers with a direct address, skips self and circuit-only
// peers, and honours the count cap.
func TestNetRelayCandidates(t *testing.T) {
	setup_peer_discovery_test(t)

	boot, _ := test_host(t)
	saved := peers_bootstrap
	peers_bootstrap = []Peer{{ID: boot, addresses: []PeerAddress{{Address: "/ip4/198.51.100.1/tcp/1443/p2p/" + boot}}}}
	defer func() { peers_bootstrap = saved }()

	flagged, _ := test_host(t)
	peer_discovered_address(flagged, "/ip4/203.0.113.5/tcp/1443/p2p/"+flagged)
	peer_relay_seen(flagged, 0)

	hop, _ := test_host(t)
	circuit, _ := test_host(t)
	peer_discovered_address(circuit, "/ip4/192.0.2.1/tcp/1443/p2p/"+hop+"/p2p-circuit/p2p/"+circuit)
	peer_relay_seen(circuit, 0)

	got := map[string]bool{}
	for ai := range net_relay_candidates(context.Background(), 16) {
		got[ai.ID.String()] = true
	}
	if !got[boot] {
		t.Error("bootstrap relay not offered")
	}
	if !got[flagged] {
		t.Error("flagged relay with a direct address not offered")
	}
	if got[circuit] {
		t.Error("circuit-only peer offered as a relay candidate")
	}

	// The count cap stops the source.
	count := 0
	for range net_relay_candidates(context.Background(), 1) {
		count++
	}
	if count != 1 {
		t.Errorf("count cap yielded %d candidates, want 1", count)
	}
}

// TestPeerPublishEventRelayLoad: a relay's advertised load is recorded so
// candidate selection can prefer relays with headroom, and updates on
// re-announcement.
func TestPeerPublishEventRelayLoad(t *testing.T) {
	setup_peer_discovery_test(t)

	id, key := test_host(t)
	rec := test_signed_record(t, key, id, []string{"/ip4/192.0.2.7/tcp/1443"}, 1)
	peer_publish_event(&Event{origin: id, content: map[string]any{"record": rec, "relay": "true", "relay/load": "95"}})
	if relay_candidate_load(id) != 95 {
		t.Errorf("relay/load from publish = %d, want 95", relay_candidate_load(id))
	}

	rec2 := test_signed_record(t, key, id, []string{"/ip4/192.0.2.7/tcp/1443"}, 2)
	peer_publish_event(&Event{origin: id, content: map[string]any{"record": rec2, "relay": "true", "relay/load": "10"}})
	if relay_candidate_load(id) != 10 {
		t.Errorf("relay/load not updated, = %d want 10", relay_candidate_load(id))
	}
}

// TestNetRelayCandidatesPrefersLessLoaded: a heavily-loaded relay (higher
// tier) is offered after a comfortable one.
func TestNetRelayCandidatesPrefersLessLoaded(t *testing.T) {
	setup_peer_discovery_test(t)

	saved := peers_bootstrap
	peers_bootstrap = nil
	defer func() { peers_bootstrap = saved }()

	loaded, _ := test_host(t)
	peer_discovered_address(loaded, "/ip4/203.0.113.1/tcp/1443/p2p/"+loaded)
	peer_relay_seen(loaded, 100) // full tier

	free, _ := test_host(t)
	peer_discovered_address(free, "/ip4/203.0.113.2/tcp/1443/p2p/"+free)
	peer_relay_seen(free, 10) // comfortable tier

	var order []string
	for ai := range net_relay_candidates(context.Background(), 16) {
		order = append(order, ai.ID.String())
	}
	if len(order) != 2 || order[0] != free || order[1] != loaded {
		t.Errorf("order = %v, want free (%s) before loaded (%s)", order, free, loaded)
	}
}

// TestRelayOffered: relay participation is on by default and the setting
// is the opt-out.
func TestRelayOffered(t *testing.T) {
	setup_peer_discovery_test(t)
	db_open("db/settings.db").exec("create table if not exists settings ( name text primary key, value text not null )")

	if !relay_offered() {
		t.Error("relay should be offered by default")
	}
	setting_set("relay", "false")
	if relay_offered() {
		t.Error("opt-out not honoured")
	}
}

// TestRelayMetricsTracer: the tracer keeps live reservation/circuit gauges
// and a cumulative refusal count, and relay_utilization clamps and caps.
func TestRelayMetricsTracer(t *testing.T) {
	relay_reservations.Store(0)
	relay_circuits.Store(0)
	relay_rejected.Store(0)

	m := relay_metrics{}

	// Reservations: opens count, renewals don't, closes decrement by count.
	m.ReservationAllowed(false)
	m.ReservationAllowed(false)
	m.ReservationAllowed(true) // renewal — no change
	m.ReservationClosed(1)
	if got := relay_reservations.Load(); got != 1 {
		t.Errorf("reservations = %d, want 1", got)
	}

	// Circuits: opens and closes.
	m.ConnectionOpened()
	m.ConnectionOpened()
	m.ConnectionClosed(time.Second)
	if got := relay_circuits.Load(); got != 1 {
		t.Errorf("circuits = %d, want 1", got)
	}

	// Rejected: only non-OK reservation requests.
	m.ReservationRequestHandled(pbv2.Status_OK)
	m.ReservationRequestHandled(pbv2.Status_RESERVATION_REFUSED)
	m.ReservationRequestHandled(pbv2.Status_RESOURCE_LIMIT_EXCEEDED)
	if got := relay_rejected.Load(); got != 2 {
		t.Errorf("rejected = %d, want 2", got)
	}

	// relay_utilization clamps a negative gauge to 0 and reports the cap.
	relay_reservations.Store(-5)
	u := relay_utilization()
	res := u["reservations"].(map[string]any)
	if res["held"].(int64) != 0 {
		t.Errorf("held = %v, want 0 (clamped)", res["held"])
	}
	if res["maximum"].(int64) != int64(relay_reservations_default) {
		t.Errorf("maximum = %v, want %d", res["maximum"], relay_reservations_default)
	}
}

// TestRelayResources: the relay runs with raised reservation caps and an
// unbounded per-connection limit by default (so relayed transfers aren't
// truncated), and a constrained relay can cap them via [relay] config.
func TestRelayResources(t *testing.T) {
	// Default — no config: raised caps, no per-connection Data/Duration limit.
	rc := relay_resources()
	if rc.MaxReservations != relay_reservations_default {
		t.Errorf("MaxReservations = %d, want %d", rc.MaxReservations, relay_reservations_default)
	}
	if rc.MaxReservationsPerIP != 64 || rc.MaxReservationsPerASN != 256 {
		t.Errorf("per-IP/ASN = %d/%d, want 64/256", rc.MaxReservationsPerIP, rc.MaxReservationsPerASN)
	}
	if rc.Limit != nil {
		t.Errorf("Limit = %+v, want nil (unbounded transfer)", rc.Limit)
	}

	// A constrained relay caps reservations and per-connection data.
	t.Setenv("MOCHI_RELAY_RESERVATIONS", "256")
	t.Setenv("MOCHI_RELAY_DATA", "1048576") // 1 MiB
	rc = relay_resources()
	if rc.MaxReservations != 256 {
		t.Errorf("configured MaxReservations = %d, want 256", rc.MaxReservations)
	}
	if rc.Limit == nil || rc.Limit.Data != 1048576 {
		t.Errorf("Limit.Data = %v, want 1048576", rc.Limit)
	}
	// Duration left unset → bounded only by data (effectively unbounded).
	if rc.Limit.Duration < time.Hour {
		t.Errorf("Limit.Duration = %v, want effectively unbounded", rc.Limit.Duration)
	}
}

// TestRelaySaturationAlert (#58): the saturation alert fires only on NEW
// refusals while the relay is actually near its global capacity. A refusal at
// low utilisation — the real wasabi case, 1 refused with 3 of 2048 held — is a
// per-peer/per-IP sub-limit, not the pool filling up, and must stay silent.
func TestRelaySaturationAlert(t *testing.T) {
	const maximum = 2048 // threshold is 0.8*2048 = 1638.4
	cases := []struct {
		name                             string
		rejected, alerted, held, maximum int64
		want                             bool
	}{
		{"wasabi case: low utilisation refusal", 1, 0, 3, maximum, false},
		{"no new refusals", 5, 5, maximum, maximum, false},
		{"new refusal but half full", 6, 5, maximum / 2, maximum, false},
		{"new refusal just below threshold", 6, 5, 1638, maximum, false},
		{"new refusal at/above threshold", 6, 5, 1639, maximum, true},
		{"new refusal while full", 10, 5, maximum, maximum, true},
		{"maximum unset", 1, 0, 0, 0, false},
	}
	for _, c := range cases {
		if got := relay_saturation_alert(c.rejected, c.alerted, c.held, c.maximum); got != c.want {
			t.Errorf("%s: relay_saturation_alert(%d,%d,%d,%d) = %v, want %v",
				c.name, c.rejected, c.alerted, c.held, c.maximum, got, c.want)
		}
	}
}

// relay_test_greetings gives the test an empty greetings record.
func relay_test_greetings(t *testing.T) {
	t.Helper()
	peer_greetings_lock.Lock()
	saved := peer_greetings
	peer_greetings = map[string]int64{}
	peer_greetings_lock.Unlock()
	t.Cleanup(func() {
		peer_greetings_lock.Lock()
		peer_greetings = saved
		peer_greetings_lock.Unlock()
	})
}

// TestRelayAccessAdmitsOnlyGreetedPeers is #591 and #135. libp2p installs no
// ACL by default, so every circuit-v2 client on the internet could reserve a
// slot and tunnel third-party traffic over the operator's bandwidth - with
// rc.Limit nil, unmetered and for as long as it liked. Membership of peers was
// the first gate, and any libp2p node joined it by opening one stream: the
// relay admits only peers that completed a /mochi/2 hello with this host.
func TestRelayAccessAdmitsOnlyGreetedPeers(t *testing.T) {
	setup_peer_discovery_test(t)
	relay_test_greetings(t)
	saved := net_me
	net_me = nil // no host to probe from: admission rests on the record alone
	t.Cleanup(func() { net_me = saved })

	greeted := p2p_peer.ID("a-peer-that-said-hello")
	other := p2p_peer.ID("a-second-peer-that-said-hello")
	held := p2p_peer.ID("a-peer-the-registry-holds")
	stranger := p2p_peer.ID("some-random-libp2p-node")
	peer_greet(greeted.String())
	peer_greet(other.String())
	peer_discovered_address(held.String(), "/ip4/192.0.2.9/tcp/1443/p2p/"+held.String())

	acl := relay_access{}
	if !acl.AllowReserve(greeted, nil) {
		t.Error("a peer that completed a hello was refused a reservation: relaying for Mochi peers is the point of the service")
	}
	if acl.AllowReserve(held, nil) {
		t.Error("THE POINT: a peer the registry merely holds reserved a slot; one stream opened and closed puts any libp2p node there")
	}
	if acl.AllowReserve(stranger, nil) {
		t.Error("an unknown peer reserved a slot: the server is an open relay for the whole internet")
	}
	if !acl.AllowConnect(greeted, nil, other) {
		t.Error("a connection between two greeted peers was refused")
	}
	if acl.AllowConnect(greeted, nil, held) {
		t.Error("a greeted peer was allowed to tunnel to a peer that never said hello")
	}
	if acl.AllowConnect(stranger, nil, greeted) {
		t.Error("an unknown peer was allowed to tunnel to a greeted destination")
	}

	// A greeting older than peer_expiry no longer counts.
	peer_greetings_lock.Lock()
	peer_greetings[greeted.String()] = now() - peer_expiry - 1
	peer_greetings_lock.Unlock()
	if acl.AllowReserve(greeted, nil) {
		t.Error("a greeting older than peer_expiry still admitted the peer")
	}
}

// TestPeerGreetingsAreBounded: ids are free to mint and a hello costs only a
// connection, so the record drops its oldest entry at the bound rather than
// growing.
func TestPeerGreetingsAreBounded(t *testing.T) {
	relay_test_greetings(t)
	peer_greetings_lock.Lock()
	for i := 0; i < peer_greetings_maximum; i++ {
		peer_greetings[fmt.Sprintf("greeted-%d", i)] = now() - int64(peer_greetings_maximum-i)
	}
	peer_greetings_lock.Unlock()

	peer_greet("one-more")
	peer_greetings_lock.Lock()
	size := len(peer_greetings)
	_, oldest := peer_greetings["greeted-0"]
	_, newest := peer_greetings["one-more"]
	peer_greetings_lock.Unlock()
	if size != peer_greetings_maximum {
		t.Errorf("the record holds %d greetings, want its bound %d", size, peer_greetings_maximum)
	}
	if oldest {
		t.Error("the oldest greeting survived a new one at the bound")
	}
	if !newest {
		t.Error("the new greeting was not recorded")
	}
}

// TestRelayProbeAdmitsAMochiServer: a peer with no greeting on record is asked
// for a hello over its connection when it asks the relay for something. A
// Mochi server answers and is admitted; a libp2p node that does not speak
// /mochi/2 is refused.
func TestRelayProbeAdmitsAMochiServer(t *testing.T) {
	setup_peer_discovery_test(t)
	relay_test_greetings(t)
	host := func() p2p_host.Host {
		h, err := p2p.New(p2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("test host: %v", err)
		}
		t.Cleanup(func() { h.Close() })
		return h
	}
	relay, mochi, plain := host(), host(), host()
	mochi.SetStreamHandler(protocol_messages, receive_messages)

	saved_me, saved_id := net_me, net_id
	net_me, net_id = relay, relay.ID().String()
	t.Cleanup(func() { net_me, net_id = saved_me, saved_id })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, h := range []p2p_host.Host{mochi, plain} {
		if err := relay.Connect(ctx, p2p_peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}); err != nil {
			t.Fatalf("connect: %v", err)
		}
	}

	acl := relay_access{}
	if !acl.AllowReserve(mochi.ID(), nil) {
		t.Fatal("a Mochi server that answers the probe was refused a reservation")
	}
	if !peer_greeted(mochi.ID().String()) {
		t.Error("the probe's hello was not recorded, so the server is probed on every request")
	}
	if acl.AllowReserve(plain.ID(), nil) {
		t.Error("a libp2p node that does not speak /mochi/2 was admitted")
	}
}

// TestNetRelayCandidatesPrefersConfiguredRelays is #160. AutoRelay reserves on
// candidates picked at random, so while enough configured relays are reachable
// and announce a relay with room, an announced relay must not be offered at
// all; one is offered only to make up for a configured relay that is full,
// unreachable or running no relay.
func TestNetRelayCandidatesPrefersConfiguredRelays(t *testing.T) {
	setup_peer_discovery_test(t)
	reset_peer_reachability(t)

	first, _ := test_host(t)
	second, _ := test_host(t)
	saved := peers_bootstrap
	peers_bootstrap = []Peer{
		{ID: first, addresses: []PeerAddress{{Address: "/ip4/198.51.100.11/tcp/1443/p2p/" + first}}},
		{ID: second, addresses: []PeerAddress{{Address: "/ip4/198.51.100.12/tcp/1443/p2p/" + second}}},
	}
	t.Cleanup(func() { peers_bootstrap = saved })
	peer_relay_seen(first, 0)
	peer_relay_seen(second, 0)
	var announced []string
	for i := 0; i < 3; i++ {
		id, _ := test_host(t)
		peer_discovered_address(id, fmt.Sprintf("/ip4/203.0.113.%d/tcp/1443/p2p/%s", 20+i, id))
		peer_relay_seen(id, 0)
		announced = append(announced, id)
	}

	offered := func() map[string]bool {
		got := map[string]bool{}
		for ai := range net_relay_candidates(context.Background(), relay_candidates_maximum) {
			got[ai.ID.String()] = true
		}
		return got
	}

	got := offered()
	if !got[first] || !got[second] {
		t.Errorf("the configured relays were not both offered: %v", got)
	}
	for _, id := range announced {
		if got[id] {
			t.Errorf("THE POINT: an announced relay was offered while both configured relays had room")
		}
	}

	// One configured relay full: exactly one announced relay makes up for it.
	peer_relay_seen(second, 100)
	got = offered()
	extra := 0
	for _, id := range announced {
		if got[id] {
			extra++
		}
	}
	if !got[first] || extra != 1 || len(got) != 2 {
		t.Errorf("with one configured relay full, offered %v: want the other configured relay and one announced", got)
	}

	// A configured relay running no relay this host has heard of does not
	// count as one with room either.
	peer_relays_lock.Lock()
	delete(peer_relays, second)
	peer_relays_lock.Unlock()
	got = offered()
	extra = 0
	for _, id := range announced {
		if got[id] {
			extra++
		}
	}
	if !got[first] || extra != 1 || len(got) != 2 {
		t.Errorf("with one configured relay announcing none, offered %v: want the other configured relay and one announced", got)
	}

	// The other configured relay unreachable as well: two announced relays.
	peers_lock.Lock()
	peers[first] = Peer{ID: first, addresses: []PeerAddress{{Address: "/ip4/198.51.100.11/tcp/1443/p2p/" + first, Failure: 3}}}
	peers_lock.Unlock()
	got = offered()
	extra = 0
	for _, id := range announced {
		if got[id] {
			extra++
		}
	}
	if extra != relay_wanted || len(got) != relay_wanted {
		t.Errorf("with neither configured relay usable, offered %v: want %d announced", got, relay_wanted)
	}
}
