// Mochi server: circuit-relay participation unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"context"
	"testing"
	"time"

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	fresh, _ := test_host(t)
	stale, _ := test_host(t)
	peer_relay_seen(fresh)
	peer_relays_lock.Lock()
	peer_relays[stale] = now() - peer_relay_maximum_age - 10
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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	saved := net_id
	id, _ := test_host(t)
	net_id = id
	defer func() { net_id = saved }()

	peer_relay_seen(id)
	for _, f := range peer_relays_fresh() {
		if f == id {
			t.Error("own id recorded as a relay")
		}
	}
}

// TestPeerPublishEventRelayFlag: the relay flag in a publish is
// recorded for AutoRelay candidate selection.
func TestPeerPublishEventRelayFlag(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

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
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	boot, _ := test_host(t)
	saved := peers_bootstrap
	peers_bootstrap = []Peer{{ID: boot, addresses: []PeerAddress{{Address: "/ip4/198.51.100.1/tcp/1443/p2p/" + boot}}}}
	defer func() { peers_bootstrap = saved }()

	flagged, _ := test_host(t)
	peer_discovered_address(flagged, "/ip4/203.0.113.5/tcp/1443/p2p/"+flagged)
	peer_relay_seen(flagged)

	hop, _ := test_host(t)
	circuit, _ := test_host(t)
	peer_discovered_address(circuit, "/ip4/192.0.2.1/tcp/1443/p2p/"+hop+"/p2p-circuit/p2p/"+circuit)
	peer_relay_seen(circuit)

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

// TestRelayOffered: relay participation is on by default and the setting
// is the opt-out.
func TestRelayOffered(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()
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
