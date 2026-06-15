// Mochi server: signed peer record unit tests
// Copyright Alistair Cunningham 2026
//
// Pins the trust and replay properties of signed peer records: a record
// must carry its own valid libp2p signature, name the GossipSub-verified
// sender, and advance the stored sequence; a replay or a record for the
// wrong peer is rejected; and the sequence survives a restart so an
// attacker cannot roll a peer's addresses back across one.

package main

import (
	"encoding/base64"
	"strings"
	"testing"

	p2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	p2p_record "github.com/libp2p/go-libp2p/core/record"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// test_signed_record seals a peer record for id, signed by key, and
// returns it base64-encoded as the publish payload carries it.
func test_signed_record(t *testing.T, key p2p_crypto.PrivKey, id string, addresses []string, sequence uint64) string {
	t.Helper()
	pid, err := p2p_peer.Decode(id)
	if err != nil {
		t.Fatalf("decode id: %v", err)
	}
	var addrs []multiaddr.Multiaddr
	for _, a := range addresses {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			t.Fatalf("multiaddr %q: %v", a, err)
		}
		addrs = append(addrs, ma)
	}
	rec := &p2p_peer.PeerRecord{PeerID: pid, Addrs: addrs, Seq: sequence}
	env, err := p2p_record.Seal(rec, key)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	data, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func peer_stored_addresses(id string) []string {
	peers_lock.Lock()
	defer peers_lock.Unlock()
	return peer_address_strings(peers[id].addresses)
}

func TestPeerRecordRoundTrip(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	id, key := test_host(t)
	encoded := test_signed_record(t, key, id, []string{"/ip4/192.0.2.10/tcp/1443", "/ip4/192.0.2.10/udp/1443/quic-v1"}, 100)

	addresses, ok := peer_record_apply(id, encoded)
	if !ok {
		t.Fatal("valid record rejected")
	}
	if len(addresses) != 2 {
		t.Fatalf("returned %d addresses, want 2", len(addresses))
	}
	for _, a := range addresses {
		info, err := p2p_peer.AddrInfoFromP2pAddr(multiaddr.StringCast(a))
		if err != nil || info.ID.String() != id {
			t.Errorf("address %q not suffixed for %q", a, id)
		}
	}
	if peer_record_get(id) == nil {
		t.Error("record not cached for relay")
	}
}

// TestPeerRecordFiltersUnroutable: a signed record carries whatever the
// signer advertised, including its loopback WebSocket listener; the apply
// path must drop undialable addresses (loopback, link-local, unspecified)
// the same way the plain-list path does, so a remote peer's
// /ip4/127.0.0.1/.../ws never lands in the registry or on the status page.
func TestPeerRecordFiltersUnroutable(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	id, key := test_host(t)
	encoded := test_signed_record(t, key, id, []string{
		"/ip4/192.0.2.10/tcp/1443",        // routable, keep
		"/ip4/127.0.0.1/tcp/36336/ws",     // loopback WS, drop
		"/ip6/::1/tcp/1443",               // loopback, drop
		"/ip6/fe80::1/tcp/1443",           // link-local, drop
		"/ip4/192.0.2.10/udp/443/quic-v1", // routable, keep
	}, 100)

	addresses, ok := peer_record_apply(id, encoded)
	if !ok {
		t.Fatal("valid record rejected")
	}
	if len(addresses) != 2 {
		t.Fatalf("returned %d addresses, want 2 (routable only): %v", len(addresses), addresses)
	}
	joined := strings.Join(addresses, " ")
	for _, bad := range []string{"127.0.0.1", "/ws", "::1", "fe80"} {
		if strings.Contains(joined, bad) {
			t.Errorf("undialable address (%s) survived filtering: %v", bad, addresses)
		}
	}
}

// TestPeerRecordRejectsWrongOrigin: a record validly signed for peer A
// must not apply when the GossipSub sender is peer B — the direct-
// announcement origin binding.
func TestPeerRecordRejectsWrongOrigin(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	a, akey := test_host(t)
	b, _ := test_host(t)
	encoded := test_signed_record(t, akey, a, []string{"/ip4/192.0.2.20/tcp/1443"}, 1)

	if _, ok := peer_record_apply(b, encoded); ok {
		t.Error("record for A applied under origin B")
	}
}

// TestPeerRecordRejectsForgedKey: a record whose signing key does not
// derive to the claimed PeerID must be rejected (ConsumeEnvelope binds
// the signature to env.PublicKey; we bind that key to the PeerID).
func TestPeerRecordRejectsForgedKey(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	victim, _ := test_host(t)
	_, attacker := test_host(t)
	// Attacker seals a record claiming the victim's PeerID with the
	// attacker's own key.
	pid, _ := p2p_peer.Decode(victim)
	ma, _ := multiaddr.NewMultiaddr("/ip4/203.0.113.7/tcp/1443")
	rec := &p2p_peer.PeerRecord{PeerID: pid, Addrs: []multiaddr.Multiaddr{ma}, Seq: 1}
	env, err := p2p_record.Seal(rec, attacker)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	data, _ := env.Marshal()
	encoded := base64.StdEncoding.EncodeToString(data)

	if _, ok := peer_record_apply(victim, encoded); ok {
		t.Error("record signed by the wrong key was accepted")
	}
}

// TestPeerRecordReplayRejected: only a strictly newer sequence applies.
func TestPeerRecordReplayRejected(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	id, key := test_host(t)

	if _, ok := peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.30/tcp/1443"}, 100)); !ok {
		t.Fatal("first record rejected")
	}
	if _, ok := peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.31/tcp/1443"}, 100)); ok {
		t.Error("same-sequence replay accepted")
	}
	if _, ok := peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.32/tcp/1443"}, 50)); ok {
		t.Error("older-sequence rollback accepted")
	}
	if _, ok := peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.33/tcp/1443"}, 101)); !ok {
		t.Error("newer sequence rejected")
	}
}

func TestPeerRecordRejectsGarbage(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	id, _ := test_host(t)
	for _, bad := range []string{"", "not base64 !!!", base64.StdEncoding.EncodeToString([]byte("random bytes not an envelope"))} {
		if _, ok := peer_record_apply(id, bad); ok {
			t.Errorf("garbage %q accepted", bad)
		}
	}
}

// TestPeerPublishEventPrefersRecord: when a publish carries both a
// signed record and a plain list, the record's addresses win.
func TestPeerPublishEventPrefersRecord(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	id, key := test_host(t)
	encoded := test_signed_record(t, key, id, []string{"/ip4/192.0.2.40/tcp/1443"}, 1)
	e := &Event{origin: id, content: map[string]any{
		"record":    encoded,
		"addresses": "/ip4/203.0.113.9/tcp/1443/p2p/" + id, // would-be plain entry, must be ignored
	}}
	peer_publish_event(e)

	stored := peer_stored_addresses(id)
	if len(stored) != 1 || stored[0] != "/ip4/192.0.2.40/tcp/1443/p2p/"+id {
		t.Errorf("stored %v, want only the record's address", stored)
	}
}

// TestPeerPublishEventFallsBackToList: a publish with no record still
// applies its plain address list (older senders).
func TestPeerPublishEventFallsBackToList(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	id, _ := test_host(t)
	peer_publish_event(publish_event(id, "/ip4/198.51.100.7/tcp/1443/p2p/"+id))
	if n := peer_addresses_count(id); n != 1 {
		t.Errorf("plain-list fallback applied %d addresses, want 1", n)
	}
}

// TestPeerRecordsLoad: the stored sequence survives a restart, so a
// rollback is still rejected after reload.
func TestPeerRecordsLoad(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	id, key := test_host(t)
	if _, ok := peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.50/tcp/1443"}, 200)); !ok {
		t.Fatal("record rejected")
	}

	// Simulate a restart: drop the in-memory cache, reload from disk.
	peer_records_lock.Lock()
	peer_records = map[string]SignedRecord{}
	peer_records_lock.Unlock()
	peer_records_load()

	if _, ok := peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.51/tcp/1443"}, 150)); ok {
		t.Error("rollback accepted after reload — sequence not persisted")
	}
}

// TestPeerRecordEventAppliesRelayed: a relayed record applies to the
// peer it names, regardless of who carried it — the origin-independence
// that makes address-book exchange work. The record self-certifies, so
// the carrier's identity is irrelevant.
func TestPeerRecordEventAppliesRelayed(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	subject, subjectKey := test_host(t)
	encoded := test_signed_record(t, subjectKey, subject, []string{"/ip4/192.0.2.60/tcp/1443"}, 10)

	// No origin set: a relay isn't bound to its carrier.
	peer_record_event(&Event{content: map[string]any{"record": encoded}})

	if n := peer_addresses_count(subject); n != 1 {
		t.Errorf("relayed record applied %d addresses, want 1", n)
	}
	if peer_record_get(subject) == nil {
		t.Error("relayed record not cached")
	}
}

// TestPeerRecordEventReplayGuard: a relayed record with a stale sequence
// does not roll the peer's addresses back.
func TestPeerRecordEventReplayGuard(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	subject, subjectKey := test_host(t)
	peer_record_event(&Event{content: map[string]any{"record": test_signed_record(t, subjectKey, subject, []string{"/ip4/192.0.2.70/tcp/1443"}, 100)}})
	peer_record_event(&Event{content: map[string]any{"record": test_signed_record(t, subjectKey, subject, []string{"/ip4/192.0.2.71/tcp/1443"}, 50)}})

	// Only the seq=100 address should be present.
	addresses := peer_stored_addresses(subject)
	if len(addresses) != 1 || addresses[0] != "/ip4/192.0.2.70/tcp/1443/p2p/"+subject {
		t.Errorf("stored %v, want only the newer record's address", addresses)
	}
}

// TestPeerRecordEventIgnoresOwn: a relay of our own record is ignored
// (we are authoritative for ourselves).
func TestPeerRecordEventIgnoresOwn(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	saved := net_id
	id, key := test_host(t)
	net_id = id
	defer func() { net_id = saved }()

	peer_record_event(&Event{content: map[string]any{"record": test_signed_record(t, key, id, []string{"/ip4/192.0.2.80/tcp/1443"}, 1)}})
	if peer_record_get(id) != nil {
		t.Error("our own relayed record was stored")
	}
}

// TestPeerRecordRelayable: only a held, fresh record for another peer
// is relayable.
func TestPeerRecordRelayable(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	fresh, _ := test_host(t)
	stale, _ := test_host(t)
	unknown, _ := test_host(t)

	peer_records_lock.Lock()
	peer_records[fresh] = SignedRecord{Envelope: []byte("x"), Sequence: 1, Updated: now()}
	peer_records[stale] = SignedRecord{Envelope: []byte("x"), Sequence: 1, Updated: now() - peer_record_relay_maximum_age - 10}
	peer_records_lock.Unlock()

	if !peer_record_relayable(fresh) {
		t.Error("fresh held record not relayable")
	}
	if peer_record_relayable(stale) {
		t.Error("stale record relayable")
	}
	if peer_record_relayable(unknown) {
		t.Error("unknown peer relayable")
	}

	saved := net_id
	net_id = fresh
	if peer_record_relayable(fresh) {
		t.Error("our own record relayable")
	}
	net_id = saved
}

// TestPeerRequestEventRelaysThirdParty: a request naming a peer we hold
// a fresh record for consumes the relay rate limit (the relay path
// fires); a request naming us does not.
func TestPeerRequestEventRelaysThirdParty(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	subject, subjectKey := test_host(t)
	asker, _ := test_host(t)
	encoded := test_signed_record(t, subjectKey, subject, []string{"/ip4/192.0.2.90/tcp/1443"}, 1)
	if _, ok := peer_record_apply(subject, encoded); !ok {
		t.Fatal("seed record rejected")
	}

	// A request for the subject, carried by the asker, triggers a relay
	// (observable via the rate-limit token it consumes).
	peer_request_event(&Event{origin: asker, content: map[string]any{"id": subject}})
	if rate_limit_record_relay.allow(subject) {
		t.Error("relay did not consume the rate-limit token for the subject")
	}
}

func TestDbUpgrade84(t *testing.T) {
	cleanup := setup_peer_discovery_test(t)
	defer cleanup()

	db := db_open("db/peers.db")
	db.exec("drop table records")
	db_upgrade_84()
	db_upgrade_84() // idempotent
	db.exec("insert into records ( id, record, sequence, updated ) values ('x', x'00', 1, 1)")
	if ok, _ := db.exists("select 1 from records where id='x'"); !ok {
		t.Error("records table not usable after upgrade")
	}
}
