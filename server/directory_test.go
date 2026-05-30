// Mochi server: directory routing-table unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"fmt"
	"testing"
)

// directory_record_location is the shared helper that both the live
// directory-publish handler and the bulk directory-download loop use to
// populate the `locations` routing table. The bulk download previously
// skipped `locations` entirely, so a freshly-wiped server knew every
// entity (in `entities`) but could route to almost none — caught
// 2026-05-21 as a market app reporting "Comptroller is not available".

func setup_directory_test(t *testing.T) func() {
	cleanup := setup_replication_test(t) // sets data_dir + net_id="self"
	db := db_open("db/directory.db")
	db.exec("create table locations (entity text not null, peer text not null, seen integer not null, primary key (entity, peer))")
	return cleanup
}

// TestDirectoryRecordLocationStoresClaim: a well-formed "p2p/<peer>"
// location is recorded into `locations` with the supplied `seen`
// timestamp verbatim (not now()), so the failover tiering can judge
// freshness honestly.
func TestDirectoryRecordLocationStoresClaim(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	db := db_open("db/directory.db")
	directory_record_location(db, "ent-1", "p2p/peer-X", 1779000000)

	row, _ := db.row("select peer, seen from locations where entity='ent-1'")
	if row == nil {
		t.Fatal("expected a locations row for ent-1")
	}
	if p, _ := row["peer"].(string); p != "peer-X" {
		t.Errorf("peer = %q, want peer-X", p)
	}
	if s := row_int(row, "seen"); s != 1779000000 {
		t.Errorf("seen = %d, want 1779000000 (the passed value, not now())", s)
	}
}

// TestDirectoryRecordLocationSkipsSelf: a location claim pointing at
// this server is not recorded — a server isn't a routable remote peer
// for its own entities.
func TestDirectoryRecordLocationSkipsSelf(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	db := db_open("db/directory.db")
	directory_record_location(db, "ent-self", "p2p/self", 1779000000)

	if n := db.integer("select count(*) from locations where entity='ent-self'"); n != 0 {
		t.Errorf("self-claim recorded %d rows, want 0", n)
	}
}

// TestDirectoryRecordLocationSkipsEmpty: an empty / prefix-only
// location yields no peer and is skipped rather than inserting a
// blank-peer row.
func TestDirectoryRecordLocationSkipsEmpty(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	db := db_open("db/directory.db")
	directory_record_location(db, "ent-empty", "", 1779000000)
	directory_record_location(db, "ent-prefix-only", "p2p/", 1779000000)

	if n := db.integer("select count(*) from locations"); n != 0 {
		t.Errorf("empty/prefix-only locations recorded %d rows, want 0", n)
	}
}

// TestDirectoryRecordLocationReplaces: a second claim for the same
// (entity, peer) replaces the first — INSERT OR REPLACE on the
// (entity, peer) primary key, so a re-download refreshes `seen` in
// place rather than duplicating.
func TestDirectoryRecordLocationReplaces(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()

	db := db_open("db/directory.db")
	directory_record_location(db, "ent-1", "p2p/peer-X", 1000)
	directory_record_location(db, "ent-1", "p2p/peer-X", 2000)

	if n := db.integer("select count(*) from locations where entity='ent-1'"); n != 1 {
		t.Errorf("rows for (ent-1, peer-X) = %d, want 1", n)
	}
	row, _ := db.row("select seen from locations where entity='ent-1' and peer='peer-X'")
	if row == nil || row_int(row, "seen") != 2000 {
		t.Errorf("seen after re-record = %v, want 2000", row)
	}
}

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
// remove the dead peer's rows from directory.db.locations, queue.db
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
		directory_record_location(ddb, e, "p2p/"+dead, now()-86400)
	}
	directory_record_location(ddb, "ent-C", "p2p/"+live, now())
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

	if n := ddb.integer("select count(*) from locations where peer=?", dead); n != 0 {
		t.Errorf("locations rows for dead peer after forget = %d, want 0", n)
	}
	if n := qdb.integer("select count(*) from queue where target=?", dead); n != 0 {
		t.Errorf("queue rows for dead peer after forget = %d, want 0", n)
	}
	if n := pdb.integer("select count(*) from peers where id=?", dead); n != 0 {
		t.Errorf("peers rows for dead peer after forget = %d, want 0", n)
	}
	if n := ddb.integer("select count(*) from locations where peer=?", live); n != 1 {
		t.Errorf("locations rows for live peer after forget = %d, want 1 (must not touch other peers)", n)
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
	ddb := db_open("db/directory.db")
	directory_record_location(ddb, "ent-X", "p2p/"+silent_but_recent, now()-3600) // 1h ago

	// Mark silent (cache positive), but seen is fresh.
	peer_reachability_lock.Lock()
	peer_reachability[silent_but_recent] = PeerReachability{
		ConsecutiveFailures: peer_silent_failure_threshold + 10,
		LastAttempt:         now(),
	}
	peer_reachability_lock.Unlock()

	directory_cleanup_dead_peers()

	if n := ddb.integer("select count(*) from locations where peer=?", silent_but_recent); n != 1 {
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
	ddb := db_open("db/directory.db")
	directory_record_location(ddb, "ent-X", "p2p/"+stale_but_unsilenced, now()-directory_location_max_age-1)

	// silent-cache is empty (cold).
	directory_cleanup_dead_peers()

	if n := ddb.integer("select count(*) from locations where peer=?", stale_but_unsilenced); n != 1 {
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
	ddb := db_open("db/directory.db")
	directory_record_location(ddb, "ent-X", "p2p/"+dead, now()-directory_location_max_age-1)

	peer_reachability_lock.Lock()
	peer_reachability[dead] = PeerReachability{
		ConsecutiveFailures: peer_silent_failure_threshold + 10,
		LastAttempt:         now(),
	}
	peer_reachability_lock.Unlock()

	directory_cleanup_dead_peers()

	if n := ddb.integer("select count(*) from locations where peer=?", dead); n != 0 {
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
	ddb := db_open("db/directory.db")
	directory_record_location(ddb, "ent-X", "p2p/"+bootstrap_id, now()-directory_location_max_age-1)

	peer_reachability_lock.Lock()
	peer_reachability[bootstrap_id] = PeerReachability{
		ConsecutiveFailures: peer_silent_failure_threshold + 99,
		LastAttempt:         now(),
	}
	peer_reachability_lock.Unlock()

	directory_cleanup_dead_peers()

	if n := ddb.integer("select count(*) from locations where peer=?", bootstrap_id); n != 1 {
		t.Errorf("bootstrap peer was forgotten (rows now=%d); want kept regardless of silent/stale state", n)
	}
}

// --- Version-based last-write-wins (directory_publish_event) ---
//
// Pubsub floods duplicate, reorder, and (within the freshness window)
// replay announcements, so directory_publish_event keeps the
// highest-versioned description rather than trusting arrival order. The
// version is the announcing host's last-edit time, carried in the
// announcement's `version` content field. The location refresh is
// independent — it must run on every announcement so routing/failover
// freshness stays honest even when the description write is dropped.

// setup_directory_entities_test extends setup_directory_test with the
// directory.db `entities` table (carrying the version column) plus a
// minimal users.db `entities` table, so directory_publish_event's
// description write and its local-entity check have somewhere to land.
func setup_directory_entities_test(t *testing.T) func() {
	cleanup := setup_directory_test(t)
	ddb := db_open("db/directory.db")
	ddb.exec("create table entities ( id text not null primary key, name text not null, class text not null, location text not null default '', data text not null default '', fingerprint text not null default '', created integer not null, updated integer not null, version integer not null default 0 )")
	udb := db_open("db/users.db")
	udb.exec("create table if not exists entities (id text not null primary key, user text not null default '')")
	return cleanup
}

// dir_publish_event builds a signed directory/publish event (from == the
// announced entity), mirroring what pubsub_receive hands the handler
// after signature verification.
func dir_publish_event(id, name, location string, version int64, peer string) *Event {
	return &Event{
		from:    id,
		service: "directory",
		event:   "publish",
		peer:    peer,
		content: map[string]any{
			"id":       id,
			"name":     name,
			"class":    "person",
			"location": location,
			"data":     "x",
			"version":  i64toa(version),
		},
	}
}

// dir_entity reads back the stored name + version for an entity.
func dir_entity(t *testing.T, db *DB, id string) (string, int64) {
	t.Helper()
	row, _ := db.row("select name, version from entities where id=?", id)
	if row == nil {
		t.Fatalf("no entities row for %s", id)
	}
	name, _ := row["name"].(string)
	return name, row_int(row, "version")
}

// TestDirectoryPublishNewerVersionWins: a higher-versioned announcement
// overwrites the stored description.
func TestDirectoryPublishNewerVersionWins(t *testing.T) {
	cleanup := setup_directory_entities_test(t)
	defer cleanup()
	ddb := db_open("db/directory.db")
	id := test_entity_id('a')

	directory_publish_event(dir_publish_event(id, "Alice", "p2p/peerY", 100, "peerY"))
	if name, ver := dir_entity(t, ddb, id); name != "Alice" || ver != 100 {
		t.Fatalf("after seed: name=%q version=%d, want Alice/100", name, ver)
	}

	directory_publish_event(dir_publish_event(id, "Alice Smith", "p2p/peerY", 200, "peerY"))
	if name, ver := dir_entity(t, ddb, id); name != "Alice Smith" || ver != 200 {
		t.Errorf("after newer announce: name=%q version=%d, want Alice Smith/200", name, ver)
	}
}

// TestDirectoryPublishOlderVersionDropped: a reordered or replayed older
// announcement must NOT overwrite a newer stored description — the core
// bug version-LWW fixes.
func TestDirectoryPublishOlderVersionDropped(t *testing.T) {
	cleanup := setup_directory_entities_test(t)
	defer cleanup()
	ddb := db_open("db/directory.db")
	id := test_entity_id('b')

	directory_publish_event(dir_publish_event(id, "Alice Smith", "p2p/peerY", 200, "peerY"))
	directory_publish_event(dir_publish_event(id, "Alice", "p2p/peerY", 100, "peerY")) // stale replay

	if name, ver := dir_entity(t, ddb, id); name != "Alice Smith" || ver != 200 {
		t.Errorf("stale announce clobbered newer record: name=%q version=%d, want Alice Smith/200", name, ver)
	}
}

// TestDirectoryPublishEqualVersionDropped: an equal-version announcement
// (a re-flood of the same state) leaves the record untouched.
func TestDirectoryPublishEqualVersionDropped(t *testing.T) {
	cleanup := setup_directory_entities_test(t)
	defer cleanup()
	ddb := db_open("db/directory.db")
	id := test_entity_id('c')

	directory_publish_event(dir_publish_event(id, "Alice Smith", "p2p/peerY", 200, "peerY"))
	directory_publish_event(dir_publish_event(id, "Different", "p2p/peerY", 200, "peerY"))

	if name, ver := dir_entity(t, ddb, id); name != "Alice Smith" || ver != 200 {
		t.Errorf("equal-version announce changed record: name=%q version=%d, want Alice Smith/200", name, ver)
	}
}

// TestDirectoryPublishStaleStillRefreshesLocation: when an older-version
// announcement is dropped for the description, its location claim must
// STILL be recorded — a stale description doesn't mean a stale host, and
// routing/failover needs to learn the peer hosts the entity.
func TestDirectoryPublishStaleStillRefreshesLocation(t *testing.T) {
	cleanup := setup_directory_entities_test(t)
	defer cleanup()
	ddb := db_open("db/directory.db")
	id := test_entity_id('d')

	directory_publish_event(dir_publish_event(id, "Alice Smith", "p2p/peerY", 200, "peerY"))
	// Older announce relayed via a different host peerZ: description dropped,
	// location for peerZ still recorded.
	directory_publish_event(dir_publish_event(id, "Alice", "p2p/peerZ", 100, "peerZ"))

	if name, _ := dir_entity(t, ddb, id); name != "Alice Smith" {
		t.Errorf("stale announce clobbered description: name=%q, want Alice Smith", name)
	}
	if n := ddb.integer("select count(*) from locations where entity=? and peer='peerZ'", id); n != 1 {
		t.Errorf("location for peerZ not recorded on a version-dropped announce: %d rows, want 1", n)
	}
}

// TestDirectoryPublishVersionlessDoesNotClobberVersioned: a pre-version
// (versionless) announcement must not overwrite a record we already hold
// at a known version — protects upgraded receivers during rollout.
func TestDirectoryPublishVersionlessDoesNotClobberVersioned(t *testing.T) {
	cleanup := setup_directory_entities_test(t)
	defer cleanup()
	ddb := db_open("db/directory.db")
	id := test_entity_id('e')

	directory_publish_event(dir_publish_event(id, "Alice Smith", "p2p/peerY", 200, "peerY"))
	ev := dir_publish_event(id, "Alicia", "p2p/peerY", 0, "peerY")
	delete(ev.content, "version") // truly absent, as an old sender would emit
	directory_publish_event(ev)

	if name, ver := dir_entity(t, ddb, id); name != "Alice Smith" || ver != 200 {
		t.Errorf("versionless announce clobbered versioned record: name=%q version=%d, want Alice Smith/200", name, ver)
	}
}

// TestDirectoryPublishVersionlessAmongVersionlessApplies: two versionless
// announcements (an un-upgraded owner) keep the prior unconditional
// behaviour — the second still updates the first.
func TestDirectoryPublishVersionlessAmongVersionlessApplies(t *testing.T) {
	cleanup := setup_directory_entities_test(t)
	defer cleanup()
	ddb := db_open("db/directory.db")
	id := test_entity_id('f')

	ev1 := dir_publish_event(id, "Alice", "p2p/peerY", 0, "peerY")
	delete(ev1.content, "version")
	directory_publish_event(ev1)

	ev2 := dir_publish_event(id, "Alicia", "p2p/peerY", 0, "peerY")
	delete(ev2.content, "version")
	directory_publish_event(ev2)

	if name, _ := dir_entity(t, ddb, id); name != "Alicia" {
		t.Errorf("versionless update did not apply: name=%q, want Alicia", name)
	}
}
