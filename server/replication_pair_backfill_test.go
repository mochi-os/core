// Mochi server: pair-backfill unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// TestPairBackfillEmitsForEveryReplicatedRow: seed every replicated
// system table with a row, fire replication_pair_backfill_impl
// (bypassing the test-setup stub), and confirm a system-set or
// system-row emit fires for every row.
func TestPairBackfillEmitsForEveryReplicatedRow(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	// Source-side seed data.
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, status) values ('u-alice', 'alice@example.org', 'active')")
	udb.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, ?, ?, ?, ?, ?)",
		test_entity_id('a'), "private-key", "fp", "u-alice", "identity", "Alice")

	sdb := db_open("db/settings.db")
	sdb.exec("create table if not exists settings (name text primary key, value text not null)")
	sdb.exec("insert into settings (name, value) values ('server_name', 'test-server')")
	sdb.exec("insert into settings (name, value) values ('locale', 'en')")

	adb := db_open("db/apps.db")
	adb.exec("create table if not exists classes (class text primary key, app text not null)")
	adb.exec("create table if not exists services (service text primary key, app text not null)")
	adb.exec("create table if not exists paths (path text primary key, app text not null)")
	adb.exec("create table if not exists apps (app text primary key, installed integer not null default 0)")
	adb.exec("create table if not exists versions (app text primary key, version text, track text)")
	adb.exec("create table if not exists tracks (app text not null, track text not null, version text not null, primary key (app, track))")
	adb.exec("insert into classes (class, app) values ('wiki', 'app-wiki-123')")
	adb.exec("insert into services (service, app) values ('feed', 'app-feed-456')")
	adb.exec("insert into paths (path, app) values ('feed', 'app-feed-456')")
	adb.exec("insert into apps (app, installed) values ('app-wiki-123', 100)")
	adb.exec("insert into versions (app, version, track) values ('app-wiki-123', '1.2', 'stable')")
	adb.exec("insert into tracks (app, track, version) values ('app-wiki-123', 'stable', '1.2')")

	ddb := db_open("db/domains.db")
	ddb.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 0, created integer not null default 0, updated integer not null default 0)")
	ddb.exec("create table if not exists routes (domain text not null, path text not null, method text not null default '', target text not null default '', context text not null default '', owner text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null default 0, updated integer not null default 0, primary key (domain, path))")
	ddb.exec("create table if not exists delegations (domain text not null, path text not null, owner text not null, created integer not null default 0, primary key (domain, path, owner))")
	ddb.exec("insert into domains (domain, verified, token, tls, created, updated) values ('example.org', 1, 'tok', 1, 100, 100)")
	ddb.exec("insert into routes (domain, path, method, target, owner) values ('example.org', '/', 'GET', 'app-feed-456', 'u-alice')")
	ddb.exec("insert into delegations (domain, path, owner, created) values ('example.org', '/sub', 'u-alice', 100)")

	// Capture every emit.
	var setEmits []struct{ db, table, row, field, value string }
	var rowEmits []struct {
		db, table string
		key       map[string]string
		cols      map[string]string
		del       bool
	}
	var transferred []string

	origSet := replication_system_set_to_peer_var
	origRow := replication_system_row_to_peer_var
	origTransfer := replication_transfer_keys_var
	replication_system_set_to_peer_var = func(peer, db, table, row, field, value string) {
		setEmits = append(setEmits, struct{ db, table, row, field, value string }{db, table, row, field, value})
	}
	replication_system_row_to_peer_var = func(peer, db, table string, key, cols map[string]string, del bool) {
		rowEmits = append(rowEmits, struct {
			db, table string
			key       map[string]string
			cols      map[string]string
			del       bool
		}{db, table, key, cols, del})
	}
	replication_transfer_keys_var = func(uid, peer string) bool {
		transferred = append(transferred, uid)
		return true
	}
	defer func() {
		replication_system_set_to_peer_var = origSet
		replication_system_row_to_peer_var = origRow
		replication_transfer_keys_var = origTransfer
	}()

	replication_pair_backfill_impl("peer-NEW")

	// 1 user transferred
	if len(transferred) != 1 || transferred[0] != "u-alice" {
		t.Errorf("users transferred = %v, want [u-alice]", transferred)
	}

	// system-set emits: 2 settings + 1 class + 1 service + 1 path + 1 install = 6
	if len(setEmits) != 6 {
		t.Errorf("system-set emits = %d, want 6 (2 settings + 3 two-col + 1 install)", len(setEmits))
	}
	// system-row emits: 1 version + 1 track + 1 domain + 1 route + 1 delegation = 5
	if len(rowEmits) != 5 {
		t.Errorf("system-row emits = %d, want 5 (1 version + 1 track + 1 domain + 1 route + 1 delegation)", len(rowEmits))
	}

	// Spot-check a couple of the emit shapes.
	foundSettings := false
	for _, e := range setEmits {
		if e.db == "settings" && e.table == "settings" && e.row == "server_name" && e.field == "value" && e.value == "test-server" {
			foundSettings = true
		}
	}
	if !foundSettings {
		t.Errorf("settings.server_name emit missing from %+v", setEmits)
	}

	foundRoute := false
	for _, e := range rowEmits {
		if e.db == "domains" && e.table == "routes" && e.key["domain"] == "example.org" && e.key["path"] == "/" {
			foundRoute = true
		}
	}
	if !foundRoute {
		t.Errorf("domains.routes emit missing from %+v", rowEmits)
	}
}

// TestPairBackfillSkipsEmptyPeer: empty peer is a no-op.
func TestPairBackfillSkipsEmptyPeer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	called := false
	origTransfer := replication_transfer_keys_var
	replication_transfer_keys_var = func(uid, peer string) bool { called = true; return true }
	defer func() { replication_transfer_keys_var = origTransfer }()

	replication_pair_backfill_impl("")
	if called {
		t.Error("backfill ran on empty peer; should be no-op")
	}
}
