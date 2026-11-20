// Mochi server: signed peer record unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// Trust and replay properties: a record must carry its own valid libp2p
// signature, name the GossipSub-verified sender, and advance a stored sequence
// that survives a restart.

package main

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	p2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	p2p_record "github.com/libp2p/go-libp2p/core/record"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// peer_record_envelope reads a stored record's envelope bytes, or nil when
// none is held. Production reads the map directly in peer_record_relay; this
// is the assertion form, and lives here because only tests need it.
func peer_record_envelope(id string) []byte {
	peer_records_lock.Lock()
	defer peer_records_lock.Unlock()
	if r, found := peer_records[id]; found {
		return r.Envelope
	}
	return nil
}

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
	setup_peer_discovery_test(t)

	id, key := test_host(t)
	encoded := test_signed_record(t, key, id, []string{"/ip4/192.0.2.10/tcp/1443", "/ip4/192.0.2.10/udp/1443/quic-v1"}, 100)

	if !peer_record_apply(id, encoded) {
		t.Fatal("valid record rejected")
	}
	addresses := peer_stored_addresses(id)
	if len(addresses) != 2 {
		t.Fatalf("applied %d addresses, want 2", len(addresses))
	}
	for _, a := range addresses {
		information, err := p2p_peer.AddrInfoFromP2pAddr(multiaddr.StringCast(a))
		if err != nil || information.ID.String() != id {
			t.Errorf("address %q not suffixed for %q", a, id)
		}
	}
	if peer_record_envelope(id) == nil {
		t.Error("record not cached for relay")
	}
}

// TestPeerRecordRejectsWrongOrigin: a record validly signed for peer A
// must not apply when the GossipSub sender is peer B — the direct-
// announcement origin binding.
func TestPeerRecordRejectsWrongOrigin(t *testing.T) {
	setup_peer_discovery_test(t)

	a, akey := test_host(t)
	b, _ := test_host(t)
	encoded := test_signed_record(t, akey, a, []string{"/ip4/192.0.2.20/tcp/1443"}, 1)

	if peer_record_apply(b, encoded) {
		t.Error("record for A applied under origin B")
	}
}

// TestPeerRecordRejectsForgedKey: a record whose signing key does not
// derive to the claimed PeerID must be rejected (ConsumeEnvelope binds
// the signature to env.PublicKey; we bind that key to the PeerID).
func TestPeerRecordRejectsForgedKey(t *testing.T) {
	setup_peer_discovery_test(t)

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

	if peer_record_apply(victim, encoded) {
		t.Error("record signed by the wrong key was accepted")
	}
}

// TestPeerRecordReplayRejected: only a strictly newer sequence applies.
func TestPeerRecordReplayRejected(t *testing.T) {
	setup_peer_discovery_test(t)

	id, key := test_host(t)

	if !peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.30/tcp/1443"}, 100)) {
		t.Fatal("first record rejected")
	}
	if peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.31/tcp/1443"}, 100)) {
		t.Error("same-sequence replay accepted")
	}
	if peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.32/tcp/1443"}, 50)) {
		t.Error("older-sequence rollback accepted")
	}
	if !peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.33/tcp/1443"}, 101)) {
		t.Error("newer sequence rejected")
	}
}

func TestPeerRecordRejectsGarbage(t *testing.T) {
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	for _, bad := range []string{"", "not base64 !!!", base64.StdEncoding.EncodeToString([]byte("random bytes not an envelope"))} {
		if peer_record_apply(id, bad) {
			t.Errorf("garbage %q accepted", bad)
		}
	}
}

// TestPeerPublishEventPrefersRecord: when a publish carries both a
// signed record and a plain list, the record's addresses win.
func TestPeerPublishEventPrefersRecord(t *testing.T) {
	setup_peer_discovery_test(t)

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
	setup_peer_discovery_test(t)

	id, _ := test_host(t)
	peer_publish_event(publish_event(id, "/ip4/198.51.100.7/tcp/1443/p2p/"+id))
	if n := peer_addresses_count(id); n != 1 {
		t.Errorf("plain-list fallback applied %d addresses, want 1", n)
	}
}

// TestPeerRecordsLoad: the stored sequence survives a restart, so a
// rollback is still rejected after reload.
func TestPeerRecordsLoad(t *testing.T) {
	setup_peer_discovery_test(t)

	id, key := test_host(t)
	if !peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.50/tcp/1443"}, 200)) {
		t.Fatal("record rejected")
	}

	// Simulate a restart: drop the in-memory cache, reload from disk.
	peer_records_lock.Lock()
	peer_records = map[string]SignedRecord{}
	peer_records_lock.Unlock()
	peer_records_load()

	if peer_record_apply(id, test_signed_record(t, key, id, []string{"/ip4/192.0.2.51/tcp/1443"}, 150)) {
		t.Error("rollback accepted after reload — sequence not persisted")
	}
}

// TestPeerRecordEventAppliesRelayed: a relayed record applies to the
// peer it names, regardless of who carried it — the origin-independence
// that makes address-book exchange work. The record self-certifies, so
// the carrier's identity is irrelevant.
func TestPeerRecordEventAppliesRelayed(t *testing.T) {
	setup_peer_discovery_test(t)

	subject, subject_key := test_host(t)
	encoded := test_signed_record(t, subject_key, subject, []string{"/ip4/192.0.2.60/tcp/1443"}, 10)

	// A carrier that is not the subject: a relay isn't bound to its carrier.
	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": encoded}})

	if n := peer_addresses_count(subject); n != 1 {
		t.Errorf("relayed record applied %d addresses, want 1", n)
	}
	if peer_record_envelope(subject) == nil {
		t.Error("relayed record not cached")
	}
}

// TestPeerRecordEventReplayGuard: a relayed record with a stale sequence
// does not roll the peer's addresses back.
func TestPeerRecordEventReplayGuard(t *testing.T) {
	setup_peer_discovery_test(t)

	subject, subject_key := test_host(t)
	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": test_signed_record(t, subject_key, subject, []string{"/ip4/192.0.2.70/tcp/1443"}, 100)}})
	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": test_signed_record(t, subject_key, subject, []string{"/ip4/192.0.2.71/tcp/1443"}, 50)}})

	// Only the seq=100 address should be present.
	addresses := peer_stored_addresses(subject)
	if len(addresses) != 1 || addresses[0] != "/ip4/192.0.2.70/tcp/1443/p2p/"+subject {
		t.Errorf("stored %v, want only the newer record's address", addresses)
	}
}

// TestPeerRecordEventIgnoresOwn: a relay of our own record is ignored
// (we are authoritative for ourselves).
func TestPeerRecordEventIgnoresOwn(t *testing.T) {
	setup_peer_discovery_test(t)

	saved := net_id
	id, key := test_host(t)
	net_id = id
	defer func() { net_id = saved }()

	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": test_signed_record(t, key, id, []string{"/ip4/192.0.2.80/tcp/1443"}, 1)}})
	if peer_record_envelope(id) != nil {
		t.Error("our own relayed record was stored")
	}
}

// TestPeerRecordRelayable: only a held, fresh record for another peer
// is relayable.
func TestPeerRecordRelayable(t *testing.T) {
	setup_peer_discovery_test(t)

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
	setup_peer_discovery_test(t)

	subject, subject_key := test_host(t)
	asker, _ := test_host(t)
	encoded := test_signed_record(t, subject_key, subject, []string{"/ip4/192.0.2.90/tcp/1443"}, 1)
	if !peer_record_apply(subject, encoded) {
		t.Fatal("seed record rejected")
	}

	// A request for the subject, carried by the asker, triggers a relay
	// (observable via the rate-limit token it consumes).
	peer_request_event(&Event{origin: asker, content: map[string]any{"id": subject}})
	if rate_limit_record_relay.allow(subject) {
		t.Error("relay did not consume the rate-limit token for the subject")
	}
}

// TestPeerRecordEventAdmitsBeforeStoring is #127. A relayed record was stored
// in memory and peers.db, and stamped in the relay-suppression map, before the
// admission ceiling was asked - the ceiling whose own comment names this flood
// - so each minted id left a record behind for fourteen days.
func TestPeerRecordEventAdmitsBeforeStoring(t *testing.T) {
	setup_peer_discovery_test(t)
	peer_discovery_fill(t)

	subject, key := test_host(t)
	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{
		"record": test_signed_record(t, key, subject, []string{"/ip4/192.0.2.80/tcp/1443"}, 1),
	}})
	if peer_record_envelope(subject) != nil {
		t.Error("THE POINT: a record for a peer refused admission was kept in memory")
	}
	if found, _ := db_open("db/peers.db").exists("select 1 from records where id=?", subject); found {
		t.Error("a record for a peer refused admission was written to peers.db")
	}
	if record_answered(subject) {
		t.Error("a record for a peer refused admission took an entry in the suppression map")
	}
	if peer_held(subject) {
		t.Error("a peer refused admission was registered")
	}
}

// TestPeerRecordEventRequiresOrigin: peers/record is broadcast only, and a
// direct stream with an empty From carried one with no rate limit at all.
func TestPeerRecordEventRequiresOrigin(t *testing.T) {
	setup_peer_discovery_test(t)

	subject, key := test_host(t)
	peer_record_event(&Event{content: map[string]any{
		"record": test_signed_record(t, key, subject, []string{"/ip4/192.0.2.81/tcp/1443"}, 1),
	}})
	if peer_held(subject) || peer_record_envelope(subject) != nil {
		t.Error("a record that did not arrive over pubsub was applied")
	}
}

// TestPeerRecordVerifyBoundsTheRecord: a record with thousands of addresses
// verified like any other and was kept whole. Real records are about 1 KB with
// at most a few dozen addresses.
func TestPeerRecordVerifyBoundsTheRecord(t *testing.T) {
	id, key := test_host(t)
	var many []string
	for i := 0; i <= peer_record_addresses_maximum; i++ {
		many = append(many, fmt.Sprintf("/ip4/10.1.%d.%d/tcp/1443", i/256, i%256))
	}
	if _, _, _, _, ok := peer_record_verify(test_signed_record(t, key, id, many, 1)); ok {
		t.Errorf("a record with %d addresses verified", len(many))
	}
	if _, _, _, _, ok := peer_record_verify(test_signed_record(t, key, id, many[:peer_record_addresses_maximum], 1)); !ok {
		t.Errorf("a record with %d addresses, the bound, was refused", peer_record_addresses_maximum)
	}
	long := []string{"/dns/" + strings.Repeat("c", 250) + "/tcp/1443"}
	for len(long)*260 <= peer_record_maximum {
		long = append(long, long[0])
	}
	if _, _, _, _, ok := peer_record_verify(test_signed_record(t, key, id, long, 1)); ok {
		t.Error("a record over peer_record_maximum verified")
	}
}

// TestPeerRecordEventRestoresAnUnheldPeer is #2105: a relayed record as new as
// the one held was refused before its addresses were applied, so a peer whose
// record survived on disk but who had left the registry could not be dialled
// until it published again.
func TestPeerRecordEventRestoresAnUnheldPeer(t *testing.T) {
	setup_peer_discovery_test(t)

	subject, key := test_host(t)
	record := test_signed_record(t, key, subject, []string{"/ip4/192.0.2.82/tcp/1443"}, 5)
	if !peer_record_apply(subject, record) {
		t.Fatal("seed record rejected")
	}
	peers_lock.Lock()
	delete(peers, subject)
	peers_lock.Unlock()

	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": record}})
	if addresses := peer_stored_addresses(subject); len(addresses) != 1 {
		t.Errorf("THE POINT: the relayed record restored %d addresses, want 1", len(addresses))
	}

	// A stale record still restores nothing.
	peers_lock.Lock()
	delete(peers, subject)
	peers_lock.Unlock()
	stale := test_signed_record(t, key, subject, []string{"/ip4/192.0.2.83/tcp/1443"}, 4)
	peer_record_event(&Event{origin: "12D3KooWSomeCarrier", content: map[string]any{"record": stale}})
	if peer_held(subject) {
		t.Error("a stale relayed record restored its addresses")
	}
}
