// Mochi server: directory unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/fxamacker/cbor/v2"
	p2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
)

// The directory is one row per (entity, peer), each asserted by the peer it
// names and self-verifying: entry_store checks the entity's content
// signature and the host's claim attestation from the payload before
// anything is stored. These tests mint real ed25519 entity keys and real
// libp2p host identities so the verification paths run for real.

func setup_directory_test(t *testing.T) func() {
	cleanup := setup_replication_test(t) // sets data_dir + net_id="self"
	protocol2_init()                     // canonical_encoder for the signables
	// entry_store and entity resolution are strict about the ownership
	// check since the 2026-07 fail-safe: an errored check (no entities
	// table) refuses the row instead of falling through.
	setup_users_test_schema()
	db := db_open("db/directory.db")
	db.exec("create table entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, signature text not null default '', attestation text not null default '', primary key ( entity, peer ) )")
	return cleanup
}

// test_identity mints an entity keypair. The entity id IS the public key.
func test_identity(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("entity keygen: %v", err)
	}
	return base58_encode(public), private
}

// test_host mints a libp2p host identity whose peer id self-certifies.
func test_host(t *testing.T) (string, p2p_crypto.PrivKey) {
	t.Helper()
	private, _, err := p2p_crypto.GenerateKeyPairWithReader(p2p_crypto.Ed25519, 256, rand.Reader)
	if err != nil {
		t.Fatalf("host keygen: %v", err)
	}
	id, err := p2p_peer.IDFromPrivateKey(private)
	if err != nil {
		t.Fatalf("host id: %v", err)
	}
	return id.String(), private
}

// test_entry builds a fully-signed row: content signed with the entity key,
// claim attested with the host key.
func test_entry(t *testing.T, entity string, key ed25519.PrivateKey, peer string, host p2p_crypto.PrivKey, name string, version, created, seen int64) *Entry {
	t.Helper()
	signable, err := entry_signable(entity, name, "person", "x", version)
	if err != nil {
		t.Fatalf("entry_signable: %v", err)
	}
	attestable, err := entry_attest_signable(entity, peer, version, created, seen)
	if err != nil {
		t.Fatalf("entry_attest_signable: %v", err)
	}
	attestation, err := host.Sign(attestable)
	if err != nil {
		t.Fatalf("host sign: %v", err)
	}
	return &Entry{
		Entity:      entity,
		Peer:        peer,
		Name:        name,
		Class:       "person",
		Data:        "x",
		Version:     version,
		Created:     created,
		Seen:        seen,
		Signature:   base58_encode(ed25519.Sign(key, signable)),
		Attestation: base58_encode(attestation),
	}
}

// add_entry inserts a bare row directly — for cleanup/routing tests that
// exercise paths which read rows but never verify them.
func add_entry(t *testing.T, entity, peer string, seen int64) {
	t.Helper()
	db := db_open("db/directory.db")
	db.exec("insert or replace into entries (entity, peer, name, class, version, created, seen) values (?, ?, 'n', 'person', 1, 1, ?)", entity, peer, seen)
}

// --- entry_store: verification gate ---

// TestEntryStoreVerified: a fully-signed row from another peer is stored
// with its fields intact and the fingerprint derived locally.
func TestEntryStoreVerified(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	entity, ek := test_identity(t)
	peer, hk := test_host(t)
	en := test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, now())

	if !entry_store(en, "test") {
		t.Fatal("verified row was not stored")
	}
	db := db_open("db/directory.db")
	row, _ := db.row("select * from entries where entity=? and peer=?", entity, peer)
	if row == nil {
		t.Fatal("no row stored")
	}
	if n, _ := row["name"].(string); n != "Alice" {
		t.Errorf("name = %q, want Alice", n)
	}
	if fp, _ := row["fingerprint"].(string); fp != fingerprint(entity) {
		t.Errorf("fingerprint = %q, want locally-derived %q", fp, fingerprint(entity))
	}
}

// TestEntryStoreRejectsSelf: this host is authoritative for its own rows;
// a replayed copy of our row must not be stored by the receive path.
func TestEntryStoreRejectsSelf(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	entity, ek := test_identity(t)
	_, hk := test_host(t)
	en := test_entry(t, entity, ek, net_id, hk, "Alice", 100, 50, now())

	if entry_store(en, "test") {
		t.Error("row naming net_id was stored; want refused")
	}
}

// TestEntryStoreRejectsTamperedContent: flipping a content fact after
// signing must fail the entity signature.
func TestEntryStoreRejectsTamperedContent(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	entity, ek := test_identity(t)
	peer, hk := test_host(t)
	en := test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, now())
	en.Name = "Mallory"

	if entry_store(en, "test") {
		t.Error("tampered content was stored; want content-signature failure")
	}
}

// TestEntryStoreRejectsTamperedAttestation: flipping the claim freshness
// after attestation must fail the host signature.
func TestEntryStoreRejectsTamperedAttestation(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	entity, ek := test_identity(t)
	peer, hk := test_host(t)
	en := test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, now()-600)
	en.Seen = now() // claim fresher than attested

	if entry_store(en, "test") {
		t.Error("tampered attestation was stored; want attestation failure")
	}
}

// TestEntryStoreRejectsWrongPeer: an attestation signed by one host cannot
// be presented as another peer's row — the peer id is the verification key.
func TestEntryStoreRejectsWrongPeer(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	entity, ek := test_identity(t)
	peer, hk := test_host(t)
	other, _ := test_host(t)
	en := test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, now())
	en.Peer = other // claim grafted onto a different peer

	if entry_store(en, "test") {
		t.Error("row with another peer's attestation was stored; want refused")
	}
}

// --- entry_store: ordering ---

// TestEntryStoreOrdering: content LWW by version; equal version with newer
// seen is an attestation refresh and replaces; equal version and seen is a
// re-flood and is dropped; older version is always dropped.
func TestEntryStoreOrdering(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	entity, ek := test_identity(t)
	peer, hk := test_host(t)
	db := db_open("db/directory.db")
	base := now() - 100

	if !entry_store(test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, base), "test") {
		t.Fatal("seed row not stored")
	}
	if !entry_store(test_entry(t, entity, ek, peer, hk, "Alice Smith", 200, 50, base+1), "test") {
		t.Error("newer version was not stored")
	}
	if entry_store(test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, base+2), "test") {
		t.Error("older version was stored; want dropped")
	}
	if entry_store(test_entry(t, entity, ek, peer, hk, "Different", 200, 50, base+1), "test") {
		t.Error("equal version + equal seen was stored; want dropped")
	}
	if !entry_store(test_entry(t, entity, ek, peer, hk, "Alice Smith", 200, 50, base+60), "test") {
		t.Error("attestation refresh (same version, newer seen) was not stored")
	}
	row, _ := db.row("select name, seen from entries where entity=? and peer=?", entity, peer)
	if n, _ := row["name"].(string); n != "Alice Smith" {
		t.Errorf("final name = %q, want Alice Smith", n)
	}
	if s := row_int(row, "seen"); s != base+60 {
		t.Errorf("final seen = %d, want %d", s, base+60)
	}
}

// --- delete event ---

// dir_delete_event builds a directory/delete event with a host-key
// attestation, as directory_delete_event receives it.
func dir_delete_event(t *testing.T, entity, peer string, host p2p_crypto.PrivKey, when int64) *Event {
	t.Helper()
	signable, err := entry_delete_signable(entity, peer, when)
	if err != nil {
		t.Fatalf("entry_delete_signable: %v", err)
	}
	sig, err := host.Sign(signable)
	if err != nil {
		t.Fatalf("host sign: %v", err)
	}
	return &Event{
		service: "directory",
		event:   "delete",
		content: map[string]any{
			"entity":      entity,
			"peer":        peer,
			"time":        i64toa(when),
			"attestation": base58_encode(sig),
		},
	}
}

// TestDirectoryDeleteEvent: a host-signed withdrawal removes that peer's
// row when the row's seen <= the delete time, keeps a fresher row, never
// touches other peers' rows, and rejects an attestation by the wrong host.
func TestDirectoryDeleteEvent(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	entity, ek := test_identity(t)
	peer, hk := test_host(t)
	other, ohk := test_host(t)
	db := db_open("db/directory.db")
	base := now() - 100

	entry_store(test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, base), "test")
	entry_store(test_entry(t, entity, ek, other, ohk, "Alice", 100, 50, base), "test")

	// Wrong key: attestation by `other` claiming to withdraw `peer`.
	directory_delete_event(dir_delete_event(t, entity, peer, ohk, base+1))
	if n := db.integer("select count(*) from entries where entity=? and peer=?", entity, peer); n != 1 {
		t.Error("withdrawal with another host's key removed the row; want kept")
	}

	// Correct withdrawal removes only peer's row.
	directory_delete_event(dir_delete_event(t, entity, peer, hk, base+1))
	if n := db.integer("select count(*) from entries where entity=? and peer=?", entity, peer); n != 0 {
		t.Error("valid withdrawal did not remove the row")
	}
	if n := db.integer("select count(*) from entries where entity=? and peer=?", entity, other); n != 1 {
		t.Error("withdrawal removed another peer's row")
	}

	// A row fresher than the delete time survives (the peer re-joined).
	entry_store(test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, base+50), "test")
	directory_delete_event(dir_delete_event(t, entity, peer, hk, base+10))
	if n := db.integer("select count(*) from entries where entity=? and peer=?", entity, peer); n != 1 {
		t.Error("stale withdrawal removed a fresher row; want kept")
	}
}

// --- dead-peer cleanup ---

// setup_directory_cleanup_test extends setup_directory_test with the
// queue + peers schemas that directory_forget_peer touches.
func setup_directory_cleanup_test(t *testing.T) func() {
	cleanup := setup_directory_test(t)
	queue_test_table()
	pdb := db_open("db/peers.db")
	pdb.exec("create table if not exists peers ( id text not null, address text not null, updated integer not null, primary key (id, address) )")
	return cleanup
}

// reset_caches clears the in-memory peer caches between tests; they're
// package-level so test order would otherwise leak state.
func reset_caches(t *testing.T) {
	t.Helper()
	peer_reachability_lock.Lock()
	peer_reachability = map[string]PeerReachability{}
	peer_reachability_lock.Unlock()
	peer_reconnect_lock.Lock()
	peer_reconnects = map[string]PeerReconnect{}
	peer_reconnect_lock.Unlock()
	peers_lock.Lock()
	peers = map[string]Peer{}
	peers_lock.Unlock()
}

// TestDirectoryForgetPeerClearsAllStores: directory_forget_peer must
// remove the dead peer's rows from directory.db.entries, queue.db
// (target=peer), peers.db (id=peer), AND the three in-memory caches.
// One peer's removal must not touch other peers' rows.
func TestDirectoryForgetPeerClearsAllStores(t *testing.T) {
	cleanup := setup_directory_cleanup_test(t)
	defer cleanup()
	defer reset_caches(t)

	dead := "12D3KooWFakeDeadPeerForForgetTest"
	live := "12D3KooWFakeLivePeerKeptAfterCleanup"

	ddb := db_open("db/directory.db")
	qdb := db_open("db/queue.db")
	pdb := db_open("db/peers.db")

	// Two dead-peer rows, one live-peer row in each table.
	for _, e := range []string{"ent-A", "ent-B"} {
		add_entry(t, e, dead, now()-86400)
	}
	add_entry(t, "ent-C", live, now())
	for i, target := range []string{dead, dead, live} {
		qdb.exec(`insert into queue (id, type, target, from_entity, to_entity, service, event, next_retry, created, priority)
			values (?, 'direct', ?, '', '', 't', 'm', ?, ?, ?)`,
			fmt.Sprintf("q-%d", i), target, now()-1, now()-100, priority_interactive)
	}
	pdb.exec("insert into peers (id, address, updated) values (?, ?, ?)", dead, "/ip4/1.1.1.1/tcp/1443", now())
	pdb.exec("insert into peers (id, address, updated) values (?, ?, ?)", dead, "/ip4/2.2.2.2/tcp/1443", now())
	pdb.exec("insert into peers (id, address, updated) values (?, ?, ?)", live, "/ip4/3.3.3.3/tcp/1443", now())
	peer_reachability_lock.Lock()
	peer_reachability[dead] = PeerReachability{ConsecutiveFailures: 99, LastAttempt: now()}
	peer_reachability_lock.Unlock()
	peer_reconnect_lock.Lock()
	peer_reconnects[dead] = PeerReconnect{NextRetry: now()}
	peer_reconnect_lock.Unlock()
	peers_lock.Lock()
	peers[dead] = Peer{ID: dead}
	peers_lock.Unlock()

	directory_forget_peer(dead)

	if n := ddb.integer("select count(*) from entries where peer=?", dead); n != 0 {
		t.Errorf("entries rows for dead peer after forget = %d, want 0", n)
	}
	if n := qdb.integer("select count(*) from queue where target=?", dead); n != 0 {
		t.Errorf("queue rows for dead peer after forget = %d, want 0", n)
	}
	if n := pdb.integer("select count(*) from peers where id=?", dead); n != 0 {
		t.Errorf("peers rows for dead peer after forget = %d, want 0", n)
	}
	if n := ddb.integer("select count(*) from entries where peer=?", live); n != 1 {
		t.Errorf("entries rows for live peer after forget = %d, want 1 (must not touch other peers)", n)
	}
	if n := qdb.integer("select count(*) from queue where target=?", live); n != 1 {
		t.Errorf("queue rows for live peer after forget = %d, want 1", n)
	}
	if n := pdb.integer("select count(*) from peers where id=?", live); n != 1 {
		t.Errorf("peers rows for live peer after forget = %d, want 1", n)
	}
	if peer_is_silent(dead) {
		t.Error("silent-cache still marks dead peer; peer_mark_reachable should have cleared it")
	}
	peer_reconnect_lock.Lock()
	_, sched := peer_reconnects[dead]
	peer_reconnect_lock.Unlock()
	if sched {
		t.Error("peer_reconnects still has dead peer scheduled")
	}
	peers_lock.Lock()
	_, in_peers := peers[dead]
	peers_lock.Unlock()
	if in_peers {
		t.Error("peers map still has dead peer")
	}
}

// TestDirectoryCleanupDeadPeersSkipsFreshSeen: a peer whose latest
// `seen` is recent must NOT be forgotten even if peer_is_silent says
// it's unreachable. The two criteria together prevent forgetting a
// peer that's only briefly offline.
func TestDirectoryCleanupDeadPeersSkipsFreshSeen(t *testing.T) {
	cleanup := setup_directory_cleanup_test(t)
	defer cleanup()
	defer reset_caches(t)

	silent_but_recent := "12D3KooWFakeSilentButRecentlySeenPeer"
	add_entry(t, "ent-X", silent_but_recent, now()-3600) // 1h ago

	// Mark silent (cache positive), but seen is fresh.
	peer_reachability_lock.Lock()
	peer_reachability[silent_but_recent] = PeerReachability{
		ConsecutiveFailures: peer_silent_failure_threshold + 10,
		LastAttempt:         now(),
	}
	peer_reachability_lock.Unlock()

	directory_cleanup_dead_peers()

	ddb := db_open("db/directory.db")
	if n := ddb.integer("select count(*) from entries where peer=?", silent_but_recent); n != 1 {
		t.Errorf("recently-seen but silent peer was forgotten (rows now=%d); want kept", n)
	}
}

// TestDirectoryCleanupDeadPeersSkipsLiveCache: a peer whose latest
// `seen` is ancient but is NOT in the silent-cache (e.g. server just
// restarted, cache empty) must NOT be forgotten yet — the next hourly
// sweep will re-evaluate once the silent-cache rebuilds.
func TestDirectoryCleanupDeadPeersSkipsLiveCache(t *testing.T) {
	cleanup := setup_directory_cleanup_test(t)
	defer cleanup()
	defer reset_caches(t)

	stale_but_unsilenced := "12D3KooWFakeStaleButNotYetSilencedPeer"
	add_entry(t, "ent-X", stale_but_unsilenced, now()-directory_location_max_age-1)

	// silent-cache is empty (cold).
	directory_cleanup_dead_peers()

	ddb := db_open("db/directory.db")
	if n := ddb.integer("select count(*) from entries where peer=?", stale_but_unsilenced); n != 1 {
		t.Errorf("stale-but-unsilenced peer was forgotten (rows now=%d); want kept until silent-cache confirms unreachable", n)
	}
}

// TestDirectoryCleanupDeadPeersForgetsStaleAndSilent: a peer that
// meets BOTH criteria — seen > max_age old AND silent-cache positive —
// must be forgotten.
func TestDirectoryCleanupDeadPeersForgetsStaleAndSilent(t *testing.T) {
	cleanup := setup_directory_cleanup_test(t)
	defer cleanup()
	defer reset_caches(t)

	dead := "12D3KooWFakeStaleSilentDeadPeer"
	add_entry(t, "ent-X", dead, now()-directory_location_max_age-1)

	peer_reachability_lock.Lock()
	peer_reachability[dead] = PeerReachability{
		ConsecutiveFailures: peer_silent_failure_threshold + 10,
		LastAttempt:         now(),
	}
	peer_reachability_lock.Unlock()

	directory_cleanup_dead_peers()

	ddb := db_open("db/directory.db")
	if n := ddb.integer("select count(*) from entries where peer=?", dead); n != 0 {
		t.Errorf("stale+silent peer was NOT forgotten (rows now=%d); want 0", n)
	}
}

// TestDirectoryCleanupDeadPeersSkipsBootstrap: bootstrap peers are
// trusted infrastructure and must never be forgotten regardless of
// silent/stale state.
func TestDirectoryCleanupDeadPeersSkipsBootstrap(t *testing.T) {
	cleanup := setup_directory_cleanup_test(t)
	defer cleanup()
	defer reset_caches(t)

	bootstrap_id := peers_bootstrap[0].ID
	add_entry(t, "ent-X", bootstrap_id, now()-directory_location_max_age-1)

	peer_reachability_lock.Lock()
	peer_reachability[bootstrap_id] = PeerReachability{
		ConsecutiveFailures: peer_silent_failure_threshold + 99,
		LastAttempt:         now(),
	}
	peer_reachability_lock.Unlock()

	directory_cleanup_dead_peers()

	ddb := db_open("db/directory.db")
	if n := ddb.integer("select count(*) from entries where peer=?", bootstrap_id); n != 1 {
		t.Errorf("bootstrap peer was forgotten (rows now=%d); want kept regardless of silent/stale state", n)
	}
}

// TestDirectoryTtlSweep: the daily sweep removes rows whose seen is past
// the 30-day retention and keeps fresher ones.
func TestDirectoryTtlSweep(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	add_entry(t, "ent-old", "peerY", now()-31*86400)
	add_entry(t, "ent-new", "peerY", now()-86400)

	db := db_open("db/directory.db")
	db.exec("delete from entries where seen<?", now()-30*86400)

	if n := db.integer("select count(*) from entries where entity='ent-old'"); n != 0 {
		t.Errorf("expired row survived the sweep")
	}
	if n := db.integer("select count(*) from entries where entity='ent-new'"); n != 1 {
		t.Errorf("fresh row removed by the sweep")
	}
}

// --- directory push: reliable self-row delivery to sync peers ---

// TestDirectoryPushRowsSelectsOwnNewerRows: only this host's own rows
// past the watermark are selected, oldest first.
func TestDirectoryPushRowsSelectsOwnNewerRows(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	t0 := now()
	add_entry(t, "ent-a", net_id, t0-100) // own, below watermark
	add_entry(t, "ent-b", net_id, t0-50)  // own, above watermark
	add_entry(t, "ent-c", net_id, t0-10)  // own, above watermark
	add_entry(t, "ent-d", "peerZ", t0)    // foreign: never pushed

	rows := directory_push_rows(t0 - 60)
	if len(rows) != 2 {
		t.Fatalf("selected %d rows, want 2", len(rows))
	}
	if rows[0].Entity != "ent-b" || rows[1].Entity != "ent-c" {
		t.Errorf("rows out of order: %q, %q; want ent-b, ent-c", rows[0].Entity, rows[1].Entity)
	}
	for _, r := range rows {
		if r.Peer != net_id {
			t.Errorf("selected foreign row %q peer=%q", r.Entity, r.Peer)
		}
	}
}

// TestDirectoryPushRowsWatermarkExcludesDelivered: a watermark at the
// newest row's seen yields nothing — the steady-state no-op between
// hourly re-attests.
func TestDirectoryPushRowsWatermarkExcludesDelivered(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	t0 := now()
	add_entry(t, "ent-a", net_id, t0-100)
	add_entry(t, "ent-b", net_id, t0-50)

	if rows := directory_push_rows(t0 - 50); len(rows) != 0 {
		t.Errorf("selected %d rows past an up-to-date watermark, want 0", len(rows))
	}
	if rows := directory_push_rows(0); len(rows) != 2 {
		t.Errorf("zero watermark selected %d rows, want all 2", len(rows))
	}
}

// TestDirectoryPushEventStoresVerifiedRows: the push receiver runs every
// streamed row through the entry_store gate — verified rows land, a
// tampered row is dropped, and the loop survives it.
func TestDirectoryPushEventStoresVerifiedRows(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	entity, ek := test_identity(t)
	entity2, ek2 := test_identity(t)
	peer, hk := test_host(t)
	good := test_entry(t, entity, ek, peer, hk, "Alice", 100, 50, now())
	bad := test_entry(t, entity2, ek2, peer, hk, "Mallory", 100, 50, now())
	bad.Name = "Tampered" // breaks the content signature

	r, w := io.Pipe()
	go func() {
		enc := cbor.NewEncoder(w)
		_ = enc.Encode(good)
		_ = enc.Encode(bad)
		w.Close()
	}()

	directory_push_event(&Event{peer: peer, stream: &Stream{reader: r}})

	db := db_open("db/directory.db")
	if n := db.integer("select count(*) from entries where entity=?", entity); n != 1 {
		t.Errorf("verified pushed row not stored")
	}
	if n := db.integer("select count(*) from entries where entity=?", entity2); n != 0 {
		t.Errorf("tampered pushed row was stored")
	}
}
