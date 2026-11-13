// Mochi server: databases are laid and changed only by their versioned steps.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// versioning_has reports whether a database holds a table or index of a name.
func versioning_has(db *DB, name string) bool {
	found, _ := db.exists("select 1 from sqlite_master where name=?", name)
	return found
}

// versioning_column reports whether a table has a column.
func versioning_column(db *DB, table, column string) bool {
	found, _ := db.exists("select 1 from pragma_table_info(?) where name=?", table, column)
	return found
}

// TestOpenersLayNothing: opening the server-wide databases makes no table; a
// fresh install's db_create and the numbered steps lay them.
func TestOpenersLayNothing(t *testing.T) {
	test_bare_directory(t)
	for name, db := range map[string]*DB{
		"networks.db": networks_database(),
		"apps.db":     db_apps(),
	} {
		if count := db.integer("select count(*) from sqlite_master"); count != 0 {
			t.Errorf("THE POINT: opening %s laid %d schema objects", name, count)
		}
	}
}

// TestFreshInstallLaysTheNetworkDatabases: db_create lays the databases whose
// openers used to lay them, as they are now, and records the current schema.
func TestFreshInstallLaysTheNetworkDatabases(t *testing.T) {
	test_bare_directory(t)
	db_create()
	if schema := atoi(setting_get("schema", ""), 0); schema != schema_version {
		t.Errorf("a fresh install records schema %d, want %d", schema, schema_version)
	}
	networks := networks_database()
	for _, name := range []string{"deliveries", "actors", "identities", "firehose", "websub", "webmentions", "reports", "pictrs", "copies"} {
		if !versioning_has(networks, name) {
			t.Errorf("THE POINT: a fresh networks.db has no %s", name)
		}
	}
	if !versioning_column(networks, "deliveries", "address") {
		t.Error("a fresh deliveries table has no address")
	}
	// The Matrix and XMPP engines' tables are networks.db's, and neither has
	// a file of its own.
	for _, name := range []string{"outbox", "rooms", "events", "devices", "messages", "changes", "stream", "presence", "media", "standins", "pushers", "sent"} {
		if !versioning_has(networks, name) {
			t.Errorf("THE POINT: a fresh networks.db has no Matrix %s", name)
		}
	}
	// What the client interface keeps of one account is its owner's.
	for _, name := range []string{"settings", "rules", "notifications", "filters", "directs", "pending", "notices"} {
		if versioning_has(networks, name) {
			t.Errorf("THE POINT: a fresh networks.db lays Matrix %s, which is each owner's own", name)
		}
	}
	if versioning_column(networks, "pushers", "body") {
		t.Error("THE POINT: a fresh networks.db keeps pushers' bodies, not only their apps and keys")
	}
	if !versioning_column(networks, "outbox", "origin") || !versioning_column(networks, "changes", "gone") {
		t.Error("a fresh networks.db lacks outbox.origin or changes.gone")
	}
	for _, name := range []string{"conferences", "uploads", "chatlog_archive"} {
		if !versioning_has(networks, name) {
			t.Errorf("THE POINT: a fresh networks.db has no XMPP %s", name)
		}
	}
	for _, name := range []string{"roster", "offline", "seen"} {
		if versioning_has(networks, name) {
			t.Errorf("THE POINT: a fresh networks.db lays %s, which is each user's own", name)
		}
	}
	for _, name := range []string{"matrix.db", "xmpp.db"} {
		if file_exists(filepath.Join(data_dir, "db", name)) {
			t.Errorf("THE POINT: a fresh install lays %s", name)
		}
	}
	if !versioning_has(db_open("db/world.db"), "worlds") {
		t.Error("THE POINT: a fresh world.db has no worlds table")
	}
	if !versioning_has(db_apps(), "classes") {
		t.Error("a fresh apps.db has no classes table")
	}
}

// TestSchema18BringsAnInstallUp: an install at schema 17, whose openers laid
// their tables as they were then, is brought to the current schema once by
// db_upgrade_18: the columns its openers added, the stream rebuild, the legacy
// table dropped, the client interface's backfill, the databases it never had,
// and the commit-hook log left in an app's data database.
func TestSchema18BringsAnInstallUp(t *testing.T) {
	directory := test_bare_directory(t)
	settings := db_open("db/settings.db")
	settings.exec("create table settings ( name text not null primary key, value text not null )")
	settings.exec("insert into settings ( name, value ) values ( 'schema', '17' )")

	networks := networks_database()
	networks.exec("create table deliveries (id text not null primary key, user text not null, account text not null, network text not null, inbox text not null, host text not null, body text not null, attempts integer not null default 0, next integer not null, claimed integer not null default 0, created integer not null)")
	networks.exec("create table firehose (sequence integer primary key, did text not null, kind text not null, body blob not null, created integer not null)")
	networks.exec("insert into firehose (sequence, did, kind, body, created) values (7, 'did:plc:a', 'commit', x'00', 1)")
	networks.exec("create table received (id text not null primary key, created integer not null)")

	matrix := networks
	matrix.exec("create table outbox (id integer primary key autoincrement, destination text not null, kind text not null, body text not null, created integer not null)")
	matrix.exec("create table changes (id integer primary key autoincrement, user text not null, viewer text not null default '', created integer not null)")
	rooms_tables(matrix)
	matrix.exec("insert into events (id, room, type, key, sender, depth, body, received) values ('$a', '!r', 'm.room.message', null, '@x:y', 1, '{}', 1)")
	matrix.exec("create table data (user text not null, type text not null, content text not null, primary key (user, type))")
	matrix.exec("insert into data (user, type, content) values ('@a:b', 'm.secret', '{}')")
	// @a:b is u1's hosted account, whose data schema 25 moves into u1's own
	// networks.db.
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', disabled text not null default '', status text not null default 'active')")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("insert into users (uid, username) values ('u1', 'a@b')")
	users.exec("insert into entities (id, fingerprint, user, class, name) values ('e-u1', 'fp-u1abc', 'u1', 'person', 'A')")
	// The handle registry, as every install since step 12 has it.
	db_upgrade_12()
	networks.exec("create table actors (id text not null primary key, user text not null, domain text not null, handle text not null, network text not null, created integer not null, deleted integer not null default 0, key text not null default '')")
	networks.exec("insert into actors (id, user, domain, handle, network, created) values ('m-a', 'u1', 'b', 'a', 'matrix', 1)")

	app := filepath.Join(directory, "users", "u1", "feeds", "db")
	if err := os.MkdirAll(app, 0755); err != nil {
		t.Fatal(err)
	}
	data := db_open("users/u1/feeds/db/feeds.db")
	data.exec("create table _commit_log (id integer primary key)")
	data.exec("create table posts (id text)")
	// One no handle holds when the walk reaches it, which the walk must close.
	db_open("users/u2/chat/db/chat.db").exec("create table _commit_log (id integer primary key)")
	db_purge_prefix(filepath.Join("users", "u2"))

	db_upgrade()

	if db_cached(filepath.Join(directory, "users", "u2", "chat", "db", "chat.db")) {
		t.Error("THE POINT: the walk over the apps' data databases left one open")
	}
	if versioning_has(db_open("users/u2/chat/db/chat.db"), "_commit_log") {
		t.Error("the commit-hook log survived in a database the walk opened itself")
	}

	if schema := atoi(setting_get("schema", ""), 0); schema != schema_version {
		t.Fatalf("schema after the upgrade is %d, want %d", schema, schema_version)
	}
	if !versioning_column(networks, "deliveries", "address") {
		t.Error("THE POINT: deliveries did not gain address")
	}
	if versioning_has(networks, "received") {
		t.Error("the server-wide received table survived")
	}
	row, _ := networks.row("select sql from sqlite_master where name='firehose'")
	if !strings.Contains(strings.ToLower(row_string(row, "sql")), "autoincrement") {
		t.Error("THE POINT: the stream table was not rebuilt with AUTOINCREMENT")
	}
	if networks.integer("select count(*) from firehose where sequence=7") != 1 {
		t.Error("the rebuild lost the stream's events")
	}
	if !versioning_has(networks, "actors") || !versioning_has(networks, "pictrs") {
		t.Error("THE POINT: networks.db's newer tables were not laid")
	}
	if !versioning_column(matrix, "outbox", "origin") || !versioning_column(matrix, "changes", "gone") {
		t.Error("THE POINT: the Matrix tables did not gain outbox.origin and changes.gone")
	}
	if matrix.integer("select count(*) from stream where event='$a'") != 1 {
		t.Error("THE POINT: the rooms' timelines were not taken into the client order")
	}
	owner := user_by_uid("u1")
	if owner == nil {
		t.Fatal("the account's owner did not load")
	}
	if db_user_brief(owner, "networks").integer("select count(*) from settings where user='@a:b' and type='m.secret'") != 1 {
		t.Error("THE POINT: the key store's account data was not copied into settings, and on into its owner's networks.db")
	}
	if versioning_has(matrix, "settings") {
		t.Error("THE POINT: the server-wide settings table outlived the move into the owners' networks.db")
	}
	if !versioning_has(networks, "conferences") || !versioning_has(db_open("db/world.db"), "worlds") {
		t.Error("THE POINT: the XMPP tables and world.db were not laid")
	}
	if versioning_has(data, "_commit_log") {
		t.Error("THE POINT: the old commit-hook log survived in the app's data database")
	}
	if !versioning_has(data, "posts") {
		t.Error("the app's own table went with the commit-hook log")
	}
}

// TestMatrixClientTablesMoveToTheirOwners: schema 25 moves what the Matrix
// client interface kept of each hosted account in the server's networks.db
// into its owner's - a suspended owner's too - with filters as sieves, and
// each owner's positions going on from the server's so no client's token goes
// back. The server keeps only pushers' apps and keys and what other servers'
// senders and the chat bridge sent; the rows of an account gone, or of a user
// gone, are dropped.
func TestMatrixClientTablesMoveToTheirOwners(t *testing.T) {
	test_bare_directory(t)
	networks := networks_database()
	networks_create(networks)
	db_upgrade_18_federation(networks)
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', disabled text not null default '', status text not null default 'active')")
	users.exec("insert into users (uid, username, status) values ('u-alice', 'alice@example.com', 'active'), ('u-sus', 'sus@example.com', 'suspended'), ('u-bob', 'bob@example.com', 'active'), ('u-carol', 'carol@example.com', 'active'), ('u-dave', 'dave@example.com', 'active')")
	db_upgrade_12()
	// The suspended owner's actor went at a start before (actors_rebuild);
	// their account and handle are theirs still. The taken address's actor
	// still names alice, though its handle and account are now bob's.
	for _, actor := range [][]any{{"m-alice", "u-alice", "alice", 0}, {"m-old", "u-alice", "old", 1}, {"m-ghost", "u-ghost", "ghost", 0}, {"m-stale", "u-alice", "taken", 0}} {
		networks.exec("insert into actors (id, user, domain, handle, network, created, deleted) values (?, ?, 'example.com', ?, 'matrix', 1, ?)", actor...)
	}
	for _, owner := range [][]string{{"u-sus", "m-sus", "sus"}, {"u-bob", "m-taken", "taken"}, {"u-dave", "m-dave", "dave"}} {
		users.exec("insert into handles (domain, name, user, created) values ('example.com', ?, ?, 1)", owner[2], owner[0])
		own := db_open("users/" + owner[0] + "/user.db")
		own.exec("create table if not exists accounts (id text not null primary key, type text not null, data text not null default '')")
		own.exec("insert into accounts (id, type, data) values (?, 'matrix', ?)", owner[1], `{"hosted":true,"domain":"example.com","handle":"`+owner[2]+`"}`)
	}
	for _, address := range []string{"@alice:example.com", "@old:example.com", "@sus:example.com", "@ghost:example.com", "@taken:example.com"} {
		networks.exec("insert into settings (user, type, content) values (?, 'm.direct', '{}')", address)
		networks.exec("insert into rules (user, kind, id, body) values (?, 'override', 'mine', '{}')", address)
		networks.exec("insert into pushers (user, app, key, kind, body, created) values (?, 'org.example', ?, 'http', '{}', 1)", address, "key"+address)
	}
	networks.exec("insert into notifications (user, room, event, position, actions, created) values ('@alice:example.com', '!r:example.com', '$e', 1, '[]', 1)")
	networks.exec("insert into filters (id, user, body) values (7, '@alice:example.com', '{\"room\":{}}')")
	networks.exec("insert into directs (user, room, person) values ('@alice:example.com', '!r:example.com', '@carol:remote.example')")
	networks.exec("insert into pending (room, user, kind, created) values ('!k:remote.example', '@alice:example.com', 'knock', 1)")
	networks.exec("insert into notices (user, account, room, created) values ('u-alice', 'm-alice', '!n:example.com', 1), ('u-alice', 'l-alice', '!ln:example.com', 1), ('u-bob', 'l-bob', '!lb:example.com', 1), ('u-carol', 'l-carol', '!lc:example.com', 1), ('u-ghost', 'l-ghost', '!lg:example.com', 1)")
	for _, row := range [][]string{{"m-alice", "client|PHONE|t1"}, {"@alice:example.com", "todevice|PHONE|t2"}, {"@carol:remote.example", "todevice|remote.example|m1"}, {"!bridged:example.com", "bridge|k1"}, {"@old:example.com", "todevice|PHONE|t3"}} {
		networks.exec("insert into sent (account, key, event, created) values (?, ?, '$s', 1)", row[0], row[1])
	}
	for _, table := range []string{"settings", "rules", "pending", "filters", "notifications"} {
		networks.exec("update sqlite_sequence set seq=500 where name=?", table)
	}

	db_upgrade_25()

	own := db_open("users/u-alice/networks.db")
	for table, want := range map[string]int{"settings": 1, "rules": 1, "pushers": 1, "notifications": 1, "sieves": 1, "directs": 1, "pending": 1} {
		if got := own.integer("select count(*) from " + table + " where user='@alice:example.com'"); got != want {
			t.Errorf("THE POINT: alice's networks.db holds %d of her %s rows, want %d", got, table, want)
		}
		if got := own.integer("select count(*) from " + table + " where user='@old:example.com'"); got != 0 {
			t.Errorf("THE POINT: %d %s rows of her account already gone were moved, not dropped", got, table)
		}
	}
	if own.integer("select count(*) from sieves where id=7") != 1 {
		t.Error("the filter's id, which her clients hold, did not survive the move")
	}
	if own.integer("select count(*) from notices where account='m-alice' and room='!n:example.com'") != 1 {
		t.Error("THE POINT: her notices room did not move")
	}
	if own.integer("select count(*) from notices where account='l-alice' and room='!ln:example.com'") != 1 || db_open("users/u-bob/networks.db").integer("select count(*) from notices where account='l-bob'") != 1 {
		t.Error("THE POINT: the notices rooms of linked accounts, which have no actor here, were dropped")
	}
	if db_open("users/u-carol/networks.db").integer("select count(*) from notices where account='l-carol'") != 1 {
		t.Error("THE POINT: the notices room of a user with no Matrix account here was dropped")
	}
	if db_open("users/u-dave/networks.db").integer("select coalesce((select seq from sqlite_sequence where name='settings'), 0)") < 500 {
		t.Error("THE POINT: an owner with nothing to move starts behind the server's positions, which their clients' tokens hold")
	}
	if own.integer("select count(*) from settings where user='@taken:example.com'") != 0 || db_open("users/u-bob/networks.db").integer("select count(*) from settings where user='@taken:example.com'") != 1 {
		t.Error("THE POINT: an address went by a stale actor row, not to the user whose handle and account it is")
	}
	if own.integer("select count(*) from sent where account in ('m-alice', '@alice:example.com')") != 2 {
		t.Error("THE POINT: what her clients sent, and the to-device messages she sent, did not move")
	}
	for _, table := range []string{"settings", "rules", "pending", "sieves", "notifications"} {
		if seq := own.integer("select coalesce((select seq from sqlite_sequence where name=?), 0)", table); seq < 500 {
			t.Errorf("THE POINT: her %s positions start at %d, behind the server's 500, so her clients' tokens go back", table, seq)
		}
	}
	suspended := db_open("users/u-sus/networks.db")
	if suspended.integer("select count(*) from settings where user='@sus:example.com'") != 1 {
		t.Error("THE POINT: a suspended owner whose actor went at an earlier start lost their account data")
	}
	if suspended.integer("select coalesce((select seq from sqlite_sequence where name='settings'), 0)") < 500 {
		t.Error("the suspended owner's positions start behind the server's")
	}
	for _, table := range []string{"settings", "rules", "notifications", "filters", "directs", "pending", "notices"} {
		if versioning_has(networks, table) {
			t.Errorf("THE POINT: the server's networks.db still has %s", table)
		}
	}
	if versioning_column(networks, "pushers", "body") {
		t.Error("THE POINT: the server's pushers keep more than their apps and keys")
	}
	if got := networks.integer("select count(*) from pushers"); got != 3 || networks.integer("select count(*) from pushers where user='@alice:example.com'") != 1 || networks.integer("select count(*) from pushers where user='@sus:example.com'") != 1 {
		t.Errorf("the server keeps %d pushers' keys, want alice's, bob's and the suspended owner's", got)
	}
	if got := networks.integer("select count(*) from sent"); got != 2 || networks.integer("select count(*) from sent where account in ('@carol:remote.example', '!bridged:example.com')") != 2 {
		t.Errorf("THE POINT: the server's sent holds %d rows, want only another server's sender's and the bridge's", got)
	}
}

// matrix_move_test_server lays a server at schema 24 holding one hosted
// Matrix account's client rows, alice's, in its networks.db.
func matrix_move_test_server(t *testing.T) *DB {
	t.Helper()
	test_bare_directory(t)
	networks := networks_database()
	networks_create(networks)
	db_upgrade_18_federation(networks)
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', disabled text not null default '', status text not null default 'active')")
	users.exec("insert into users (uid, username) values ('u-alice', 'alice@example.com')")
	db_upgrade_12()
	networks.exec("insert into actors (id, user, domain, handle, network, created) values ('m-alice', 'u-alice', 'example.com', 'alice', 'matrix', 1)")
	networks.exec("insert into settings (user, type, content) values ('@alice:example.com', 'm.direct', '{}')")
	networks.exec("insert into pushers (user, app, key, kind, body, created) values ('@alice:example.com', 'org.example', 'phone', 'http', '{}', 1)")
	networks.exec("insert into sent (account, key, event, created) values ('@alice:example.com', 'todevice|PHONE|t1', '$s', 1)")
	return networks
}

// TestMatrixClientTablesMoveSurvivesAStartCutShort: a start stopped part-way
// through step 25 leaves the server's tables whole, so the next start runs
// the step again and finishes it, moving nothing twice.
func TestMatrixClientTablesMoveSurvivesAStartCutShort(t *testing.T) {
	networks := matrix_move_test_server(t)
	networks.exec("create trigger cut before delete on sent begin select raise(abort, 'cut short'); end")
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the step finished through the cut")
			}
		}()
		db_upgrade_25()
	}()
	for _, table := range []string{"settings", "rules", "notifications", "filters", "directs", "pending", "notices"} {
		if !versioning_has(networks, table) {
			t.Errorf("THE POINT: a start cut short dropped the server's %s, so the step is never run again", table)
		}
	}
	if !versioning_column(networks, "pushers", "body") || networks.integer("select count(*) from pushers") != 1 {
		t.Error("THE POINT: a start cut short left the server's pushers half rebuilt")
	}
	networks.exec("drop trigger cut")
	db_upgrade_25()
	own := db_open("users/u-alice/networks.db")
	if own.integer("select count(*) from settings where user='@alice:example.com'") != 1 || own.integer("select count(*) from pushers") != 1 || own.integer("select count(*) from sent") != 1 {
		t.Error("the step run again did not leave alice's rows once each")
	}
	if versioning_has(networks, "settings") || networks.integer("select count(*) from pushers where app='org.example' and key='phone'") != 1 || networks.integer("select count(*) from sent") != 0 {
		t.Error("the step run again did not finish")
	}
}

// TestMatrixClientTablesMoveRefusesADamagedFile: an owner's networks.db that
// cannot take their rows stops the start, naming the file, with nothing
// dropped, so nothing is lost while the file is seen to.
func TestMatrixClientTablesMoveRefusesADamagedFile(t *testing.T) {
	networks := matrix_move_test_server(t)
	path := filepath.Join(data_dir, "users", "u-alice", "networks.db")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("not a database ", 512)), 0600); err != nil {
		t.Fatal(err)
	}
	refused := ""
	func() {
		defer func() { refused = fmt.Sprint(recover()) }()
		db_upgrade_25()
	}()
	if !strings.Contains(refused, filepath.Join("users", "u-alice", "networks.db")) || !strings.Contains(refused, "nothing has been dropped") {
		t.Errorf("THE POINT: the start was not stopped naming the damaged file: %q", refused)
	}
	if !versioning_has(networks, "settings") || networks.integer("select count(*) from settings where user='@alice:example.com'") != 1 || !versioning_column(networks, "pushers", "body") {
		t.Error("THE POINT: alice's rows were dropped though her file could not take them")
	}
}

// TestMatrixClientTablesMoveOpensOnlyWhatItMust: an owner with nothing to
// move and no positions to carry on has no networks.db opened or made, so
// a server whose tables are empty cannot be stopped by one.
func TestMatrixClientTablesMoveOpensOnlyWhatItMust(t *testing.T) {
	networks := matrix_move_test_server(t)
	networks.exec("delete from settings")
	networks.exec("delete from pushers")
	networks.exec("delete from sent")
	networks.exec("delete from sqlite_sequence")
	db_upgrade_25()
	if file_exists(filepath.Join(data_dir, "users", "u-alice", "networks.db")) {
		t.Error("THE POINT: a networks.db was made for an owner with nothing to take")
	}
	if versioning_has(networks, "settings") {
		t.Error("the step did not finish")
	}
}

// TestUserDatabaseIsVersioned: user.db is brought to its version once, its
// legacy shapes changed then, and the changes are not made again on a later
// open; a new user's file is laid whole.
func TestUserDatabaseIsVersioned(t *testing.T) {
	test_data_directory(t)
	alice := &User{UID: "alice"}
	legacy := db_open("users/alice/user.db")
	legacy.exec("create table accounts (id integer primary key, type text not null, label text not null default '', identifier text not null default '', data text not null default '', created integer not null, verified integer not null default 0, enabled integer not null default 1, \"default\" text not null default '')")
	legacy.exec("insert into accounts (id, type, identifier, data, created) values (3, 'fediverse', 'alice@example.com', '{\"hosted\":true}', 1)")
	db_purge_prefix(filepath.Join("users", "alice"))

	db := db_user(alice, "user")
	if version := db.integer("pragma user_version"); version != len(db_user_steps["user"]) {
		t.Fatalf("THE POINT: user.db is at version %d, want %d", version, len(db_user_steps["user"]))
	}
	if !versioning_column(db, "accounts", "device") || !versioning_column(db, "accounts", "last_delivered") {
		t.Error("THE POINT: accounts did not gain its later columns")
	}
	if kind, _ := db.exists("select 1 from pragma_table_info('accounts') where name='id' and type='text' collate nocase"); !kind {
		t.Error("accounts ids were not moved to text")
	}
	if identifier, _ := db.row("select identifier from accounts where id='3'"); row_string(identifier, "identifier") != "@alice@example.com" {
		t.Errorf("the hosted address became %q, want @alice@example.com", row_string(identifier, "identifier"))
	}
	for _, name := range []string{"preferences", "permissions", "groups", "keys", "objects", "repositories", "directory", "apps"} {
		if !versioning_has(db, name) {
			t.Errorf("user.db has no %s", name)
		}
	}
	// What is the user's on the other networks - the client interfaces'
	// tables, written on nearly every request a client makes, the activities
	// each account has taken, and the XMPP accounts' rosters, queues and
	// rooms - is their networks.db's, never user.db's.
	networks := db_user(alice, "networks")
	for _, name := range []string{"snowflakes", "snapshots", "markers", "filters", "subscriptions", "alerts", "numbers", "seen", "roster", "offline", "pushes", "nodes", "items", "joined"} {
		// user.db's nodes are its Bluesky repository's (repository.go).
		if name != "nodes" && versioning_has(db, name) {
			t.Errorf("THE POINT: user.db lays %s", name)
		}
		if !versioning_has(networks, name) {
			t.Errorf("THE POINT: the user's networks.db has no %s", name)
		}
	}
	if networks.integer("pragma user_version") != len(db_user_steps["networks"]) {
		t.Error("the user's networks.db was not versioned")
	}

	// Written after the file reached its version: a later open must not
	// rewrite it, as the old open path did every time.
	db.exec("insert into accounts (id, type, identifier, data, created) values ('later', 'fediverse', 'kept@example.com', '{\"hosted\":true}', 2)")
	db_purge_prefix(filepath.Join("users", "alice"))
	again := db_user(alice, "user")
	if identifier, _ := again.row("select identifier from accounts where id='later'"); row_string(identifier, "identifier") != "kept@example.com" {
		t.Errorf("THE POINT: a later open rewrote an account: %q", row_string(identifier, "identifier"))
	}

	fresh := db_user(&User{UID: "bob"}, "user")
	if !versioning_has(fresh, "accounts") || fresh.integer("pragma user_version") != len(db_user_steps["user"]) {
		t.Error("a new user's user.db was not laid and versioned")
	}
	notifications := db_user(&User{UID: "bob"}, "notifications")
	if !versioning_has(notifications, "email_delivered") || notifications.integer("pragma user_version") != 1 {
		t.Error("notifications.db was not laid and versioned")
	}
}

// TestUserDatabaseGivesUpClientTables: a user.db laid by a 2.0.12-2.0.16
// core, as an export of one restores it, still holds the other networks'
// client tables; its next open moves their rows to the user's networks.db,
// with their ids and counters, and drops them from user.db.
func TestUserDatabaseGivesUpClientTables(t *testing.T) {
	test_data_directory(t)
	alice := &User{UID: "alice"}
	old := db_open("users/alice/user.db")
	if err := user_upgrade_1(old); err != nil {
		t.Fatal(err)
	}
	// As 2.0.16 laid them, at user.db version 1.
	for _, table := range []string{
		"create table alerts (id integer primary key autoincrement, account text not null, row text not null, event text not null default '', answer text not null, created integer not null, unique (account, row, event))",
		"create table filters (id integer primary key autoincrement, account text not null, title text not null, context text not null, action text not null default 'warn', keywords text not null default '[]', expires integer not null default 0, created integer not null)",
		"create table markers (account text not null, timeline text not null, position text not null, version integer not null default 0, updated integer not null, primary key (account, timeline))",
		"create table numbers (number integer primary key autoincrement, kind text not null, id text not null, created integer not null, unique (kind, id))",
		"create table snapshots (kind text not null, key text not null, document text not null, created integer not null, updated integer not null, primary key (kind, key))",
		"create table snowflakes (number integer primary key, kind text not null, key text not null, unique (kind, key))",
		"create table subscriptions (token text not null primary key, account text not null, endpoint text not null, auth text not null, key text not null, alerts text not null default '{}', policy text not null default 'all', standard integer not null default 0, created integer not null)",
	} {
		old.exec(table)
	}
	old.exec("insert into alerts (id, account, row, event, answer, created) values (5, 'f1', 'r5', 'e', '{\"type\":\"follow\"}', 10), (9, 'f1', 'r9', 'e', '{\"type\":\"mention\"}', 20), (12, 'f1', 'r12', 'e', '{}', 30)")
	old.exec("delete from alerts where id=12")
	old.exec("insert into filters (id, account, title, context, created) values (3, 'f1', 'No spoilers', '[\"home\"]', 1)")
	old.exec("insert into markers (account, timeline, position, updated) values ('f1', 'home', '42', 1)")
	old.exec("insert into numbers (number, kind, id, created) values (7, 'notification', 'alert:5', 10)")
	old.exec("insert into snapshots (kind, key, document, created, updated) values ('status', 'note:x', '{}', 1, 1)")
	old.exec("insert into snowflakes (number, kind, key) values (99, 'status', 'x')")
	old.exec("insert into subscriptions (token, account, endpoint, auth, key, created) values ('t', 'f1', 'https://push.example', 'a', 'k', 1)")
	old.exec("pragma user_version=1")
	db_purge_prefix(filepath.Join("users", "alice"))

	db := db_user(alice, "user")
	if version := db.integer("pragma user_version"); version != len(db_user_steps["user"]) {
		t.Fatalf("THE POINT: user.db is at version %d, want %d", version, len(db_user_steps["user"]))
	}
	networks := db_user(alice, "networks")
	for table, want := range map[string]int{"alerts": 2, "filters": 1, "markers": 1, "numbers": 1, "snapshots": 1, "snowflakes": 1, "subscriptions": 1} {
		if versioning_has(db, table) {
			t.Errorf("THE POINT: user.db still holds %s", table)
		}
		if got := networks.integer("select count(*) from " + table); got != want {
			t.Errorf("THE POINT: the user's networks.db holds %d rows of %s, want %d", got, table, want)
		}
	}
	if found, _ := networks.exists("select 1 from alerts where id=9 and row='r9' and answer like '%mention%'"); !found {
		t.Error("THE POINT: a notification did not keep its id")
	}
	if found, _ := networks.exists("select 1 from filters where id=3 and title='No spoilers'"); !found {
		t.Error("THE POINT: a filter did not keep its id")
	}
	// The counter past the removed row: a new notification is never given an
	// id a client already saw.
	networks.exec("insert into alerts (account, row, event, answer, created) values ('f1', 'new', 'e', '{}', 40)")
	if id, _ := networks.row("select id from alerts where row='new'"); row_int(id, "id") != 13 {
		t.Errorf("THE POINT: a new notification after the move is %d, want 13", row_int(id, "id"))
	}

	// A user.db laid as now is left as it is.
	fresh := db_user(&User{UID: "bob"}, "user")
	if fresh.integer("pragma user_version") != len(db_user_steps["user"]) || versioning_has(fresh, "alerts") {
		t.Error("a new user's user.db was not laid as now")
	}
}

// TestNotificationsKnowTheirPerson: a user's networks.db laid before the
// notifications kept whom they are from has each one filled in on its next
// open, as the client interface keys the person: by id, else by identity, and
// none for a notification naming no one.
func TestNotificationsKnowTheirPerson(t *testing.T) {
	test_data_directory(t)
	alice := &User{UID: "alice"}
	old := db_open("users/alice/networks.db")
	if err := networks_upgrade_1(old); err != nil {
		t.Fatal(err)
	}
	old.exec("pragma user_version=1")
	old.exec("insert into alerts (account, row, event, answer, created, number, type) values ('f1', 'a', 'e', '{\"type\":\"follow\",\"person\":{\"id\":\"https://remote.example/users/bob\"}}', 1, 1, 'follow'), ('f1', 'b', 'e', '{\"type\":\"mention\",\"person\":{\"id\":\"\",\"identity\":\"abc\"}}', 2, 2, 'mention'), ('f1', 'c', 'e', '{\"type\":\"mention\"}', 3, 3, 'mention')")
	db_purge_prefix(filepath.Join("users", "alice"))

	networks := db_user(alice, "networks")
	if version := networks.integer("pragma user_version"); version != len(db_user_steps["networks"]) {
		t.Fatalf("THE POINT: the user's networks.db is at version %d, want %d", version, len(db_user_steps["networks"]))
	}
	for row, want := range map[string]any{"a": "https://remote.example/users/bob", "b": "identity:abc", "c": nil} {
		kept, _ := networks.row("select person from alerts where row=?", row)
		if got := kept["person"]; fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("THE POINT: notification %s is from %v, want %v", row, got, want)
		}
	}
	if found, _ := networks.exists("select 1 from sqlite_master where type='index' and name='alerts_person'"); !found {
		t.Error("THE POINT: the notifications have no index of persons")
	}
}

// TestSystemDatabaseIsVersioned: an app.db is laid whole when new, and one
// laid before the columns it gained since is brought up to date.
func TestSystemDatabaseIsVersioned(t *testing.T) {
	test_data_directory(t)
	alice := &User{UID: "alice"}
	fresh := db_app_system(alice, &App{id: "fresh"})
	for _, name := range []string{"access", "sequence", "received", "log", "acknowledged", "subscribed", "pending", "commits", "idempotency"} {
		if !versioning_has(fresh, name) {
			t.Errorf("THE POINT: a new app.db has no %s", name)
		}
	}
	if version := fresh.integer("pragma user_version"); version != len(db_system_steps) {
		t.Errorf("a new app.db is at version %d, want %d", version, len(db_system_steps))
	}

	legacy := db_open("users/alice/old/app.db")
	legacy.exec("create table received (sender text not null, key text not null, last integer not null default 0, primary key (sender, key))")
	legacy.exec("create table commits (seq integer primary key autoincrement, name text not null, kind text not null, row_uid text not null default '', ts integer not null, fired integer not null default 0)")
	legacy.exec("create table pending (peer text not null, key text not null, sequence integer not null, source text not null, target text not null, service text not null, event text not null, msg_id text not null default '', sender_app text not null default '', sender_services text not null default '', content blob not null, received integer not null, primary key (peer, key, sequence))")
	db_purge_prefix(filepath.Join("users", "alice", "old"))
	db := db_app_system(alice, &App{id: "old"})
	if !versioning_column(db, "received", "seen") || !versioning_column(db, "commits", "attempts") || !versioning_column(db, "pending", "message") {
		t.Error("THE POINT: a legacy app.db was not brought up to date")
	}
}

// TestEncryptionDatabaseIsVersioned: encryption.db is laid by its step.
func TestEncryptionDatabaseIsVersioned(t *testing.T) {
	test_data_directory(t)
	db := encryption_database(&User{UID: "alice"})
	if !versioning_has(db, "devices") || !versioning_has(db, "omemo") || db.integer("pragma user_version") != 1 {
		t.Error("THE POINT: encryption.db was not laid and versioned")
	}
}

// TestUpgradeFileStepFailure: a step that fails leaves the version at the last
// step that worked, is not tried again on that handle, and is tried again on
// the next one.
func TestUpgradeFileStepFailure(t *testing.T) {
	test_bare_directory(t)
	calls := 0
	failing := true
	steps := []func(*DB) error{
		func(db *DB) error { db.exec("create table one (a)"); return nil },
		func(db *DB) error {
			calls++
			if failing {
				return errors.New("not yet")
			}
			db.exec("create table two (a)")
			return nil
		},
	}
	db := db_open("db/steps.db")
	db_upgrade_file(db, "db/steps.db", steps)
	db_upgrade_file(db, "db/steps.db", steps)
	if version := db.integer("pragma user_version"); version != 1 {
		t.Errorf("THE POINT: a failed second step left version %d, want 1", version)
	}
	if calls != 1 {
		t.Errorf("the failing step ran %d times on one handle, want 1", calls)
	}
	failing = false
	db_purge_prefix("db")
	again := db_open("db/steps.db")
	db_upgrade_file(again, "db/steps.db", steps)
	if version := again.integer("pragma user_version"); version != 2 || !versioning_has(again, "two") {
		t.Errorf("the next handle did not finish the steps: version %d", version)
	}
}

// TestUpgradeFileHoldsOpenersUntilLaid: an opener arriving while the steps run
// waits for them, rather than reading a file still being laid.
func TestUpgradeFileHoldsOpenersUntilLaid(t *testing.T) {
	test_bare_directory(t)
	steps := []func(*DB) error{
		func(db *DB) error {
			time.Sleep(100 * time.Millisecond)
			db.exec("create table slow (a)")
			return nil
		},
	}
	db := db_open("db/slow.db")
	var group sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			db_upgrade_file(db, "db/slow.db", steps)
			if _, err := db.exists("select 1 from slow"); err != nil {
				failures <- err
			}
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("THE POINT: an opener read the file before its steps finished: %v", err)
	}
}

// TestWipedRebuildableDatabasesAreLaidAgain: a directory.db or world.db an
// operator deleted makes the next start lay it again, through db_create.
func TestWipedRebuildableDatabasesAreLaidAgain(t *testing.T) {
	directory := test_bare_directory(t)
	db_create()
	db_purge_prefix("db")
	if missing := db_heal_transient(); len(missing) != 0 {
		t.Fatalf("a healthy install asks for %v to be laid", missing)
	}
	for _, name := range []string{"directory", "world"} {
		path := filepath.Join(directory, "db", name+".db")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if missing := db_heal_transient(); !slices.Equal(missing, []string{name}) {
			t.Errorf("THE POINT: a deleted %s.db makes the start lay %v", name, missing)
		}
		db_prepare(false)
		db_purge_prefix("db")
	}
	if !versioning_has(db_open("db/directory.db"), "entries") || !versioning_has(db_open("db/world.db"), "worlds") {
		t.Error("the start did not lay the deleted databases again")
	}
}

// TestAMissingDatabaseOnAnOlderInstallIsLaidAlone: an install at a schema
// before db_upgrade_15 added tokens.refresh, missing its directory.db, starts:
// only the missing file is laid fresh, and the upgrade brings the rest up.
// Laying every database first met users.db with the index on tokens.refresh
// before the column existed, and the server could not start.
func TestAMissingDatabaseOnAnOlderInstallIsLaidAlone(t *testing.T) {
	directory := test_bare_directory(t)
	db_create()
	users := db_open("db/users.db")
	users.exec("drop index tokens_refresh")
	users.exec("alter table tokens drop column refresh")
	setting_set("schema", "14")
	if err := os.Remove(filepath.Join(directory, "db", "directory.db")); err != nil {
		t.Fatal(err)
	}
	db_purge_prefix("db")

	func() {
		defer func() {
			if failure := recover(); failure != nil {
				t.Fatalf("THE POINT: the start failed: %v", failure)
			}
		}()
		db_prepare(false)
	}()
	if !versioning_column(db_open("db/users.db"), "tokens", "refresh") {
		t.Error("the upgrade did not bring users.db up")
	}
	if !versioning_has(db_open("db/directory.db"), "entries") {
		t.Error("the missing directory.db was not laid")
	}
	if schema := atoi(setting_get("schema", ""), 0); schema != schema_version {
		t.Errorf("schema after the start is %d, want %d", schema, schema_version)
	}
}

// TestActionAccessRequireReadsTheSystemDatabase: a.access.require answers from
// the access rules in the app's system database, where every grant is written.
func TestActionAccessRequireReadsTheSystemDatabase(t *testing.T) {
	test_data_directory(t)
	owner := &User{UID: "owner"}
	app := &App{id: "notes"}
	db_app_system(owner, app).access_set("alice", "notebook", "read", true, "owner")

	thread := &sl.Thread{}
	thread.SetLocal("app", app)
	thread.SetLocal("owner", owner)
	access := &ActionAccess{action: &Action{user: &User{UID: "a", Role: "user", Identity: &Entity{ID: "alice"}}}}
	require := sl.NewBuiltin("require", access.sl_require)
	if _, err := access.sl_require(thread, require, sl.Tuple{sl.String("notebook"), sl.String("read")}, nil); err != nil {
		t.Errorf("THE POINT: a granted operation was refused: %v", err)
	}
	if _, err := access.sl_require(thread, require, sl.Tuple{sl.String("notebook"), sl.String("write")}, nil); err == nil {
		t.Error("an operation never granted was allowed")
	}
}

// versioning_schema is a database's tables, indexes, triggers and views, one
// a line, sorted, as a step's fixture records them.
func versioning_schema(db *DB) string {
	rows, _ := db.rows("select type, name, coalesce(sql, '') as sql from sqlite_master where name not like 'sqlite_%' order by type, name")
	var lines []string
	for _, row := range rows {
		lines = append(lines, row_string(row, "type")+" "+row_string(row, "name")+": "+strings.Join(strings.Fields(row_string(row, "sql")), " "))
	}
	return strings.Join(lines, "\n") + "\n"
}

// TestVersionedStepsKeepTheirSchema: what each released step lays on an empty
// database is pinned in testdata/steps. A step runs on every install and file
// made before it, so once released it never changes: a change is a new step,
// and a function a step shares with db_create is copied into the step as it
// was before db_create's copy changes. A step added since records its fixture
// once with MOCHI_STEP_FIXTURES=write; an existing fixture is never rewritten.
func TestVersionedStepsKeepTheirSchema(t *testing.T) {
	check := func(name, got string) {
		t.Helper()
		path := filepath.Join("testdata", "steps", name+".sql")
		want, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			if os.Getenv("MOCHI_STEP_FIXTURES") != "write" {
				t.Errorf("%s has no fixture: record it once with MOCHI_STEP_FIXTURES=write", name)
				return
			}
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(got), 0644); err != nil {
				t.Fatal(err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if string(want) != got {
			t.Errorf("THE POINT: %s lays a different schema from %s. A released step runs on every install and file made before it, so it never changes: make the change a new step, and if a function the step shares with db_create changed, give the step a copy of it as it was.\n--- want\n%s--- got\n%s", name, path, want, got)
		}
	}

	test_bare_directory(t)
	db_upgrade_18()
	for _, file := range []string{"networks", "world"} {
		check("db_upgrade_18."+file, versioning_schema(db_open("db/"+file+".db")))
	}
	db_upgrade_19()
	check("db_upgrade_19.networks", versioning_schema(db_open("db/networks.db")))
	db_upgrade_20()
	check("db_upgrade_20.networks", versioning_schema(db_open("db/networks.db")))
	// queue.db as schema 20 left it, for db_upgrade_21 to change.
	queue := db_open("db/queue.db")
	queue.exec("create table queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20, claimed integer not null default 0 )")
	queue.exec("create index queue_status_priority_retry on queue (status, priority, next_retry)")
	queue.exec("create index queue_target on queue (target)")
	queue.exec("create index queue_target_priority_retry on queue (target, priority desc, next_retry)")
	db_upgrade_21()
	check("db_upgrade_21.queue", versioning_schema(queue))
	db_upgrade_22()
	check("db_upgrade_22.networks", versioning_schema(db_open("db/networks.db")))
	db_upgrade_23()
	check("db_upgrade_23.queue", versioning_schema(queue))
	// Step 25 reads the handle registry, which step 12 laid long before.
	db_upgrade_12()
	db_upgrade_25()
	check("db_upgrade_25.networks", versioning_schema(db_open("db/networks.db")))

	for name, steps := range map[string][]func(*DB) error{
		"user":          db_user_steps["user"],
		"notifications": db_user_steps["notifications"],
		"networks":      db_user_steps["networks"],
		"encryption":    encryption_steps,
		"system":        db_system_steps,
	} {
		for k := range steps {
			db := db_open(fmt.Sprintf("db/steps-%s-%d.db", name, k+1))
			for _, step := range steps[:k+1] {
				if err := step(db); err != nil {
					t.Fatalf("%s step %d: %v", name, k+1, err)
				}
			}
			check(fmt.Sprintf("%s_upgrade_%d", name, k+1), versioning_schema(db))
		}
	}
}

// TestUpgradeFileSurvivesAPanickingStep: a step that panics is a failed step:
// the opener goes on, the version stays, and the handle tries again once the
// retry delay has passed, not on every open before then.
func TestUpgradeFileSurvivesAPanickingStep(t *testing.T) {
	test_bare_directory(t)
	panicking := true
	steps := []func(*DB) error{func(db *DB) error {
		if panicking {
			panic("a damaged file")
		}
		db.exec("create table laid (a)")
		return nil
	}}
	db := db_open("db/panics.db")
	func() {
		defer func() {
			if failure := recover(); failure != nil {
				t.Fatalf("THE POINT: a panicking step took the opener down: %v", failure)
			}
		}()
		db_upgrade_file(db, "db/panics.db", steps)
	}()
	if db.versioned || db.integer("pragma user_version") != 0 {
		t.Error("THE POINT: a panicking step marked the file done")
	}
	panicking = false
	db_upgrade_file(db, "db/panics.db", steps)
	if versioning_has(db, "laid") {
		t.Error("the step was tried again before the retry delay")
	}
	db.retry = 0
	db_upgrade_file(db, "db/panics.db", steps)
	if !versioning_has(db, "laid") || db.integer("pragma user_version") != 1 {
		t.Error("THE POINT: the step was not tried again after the retry delay")
	}
}

// TestACraftedSystemDatabaseDoesNotStopTheServer: an app.db whose access table
// has another shape, as a crafted restore bundle may carry, fails its step and
// is reported; the restore and the startup sweep that bring it up go on.
func TestACraftedSystemDatabaseDoesNotStopTheServer(t *testing.T) {
	test_data_directory(t)
	db_open("users/mallory/notes/app.db").exec("create table access (x text)")
	db_purge_prefix("users")
	func() {
		defer func() {
			if failure := recover(); failure != nil {
				t.Fatalf("THE POINT: a crafted app.db took the server down: %v", failure)
			}
		}()
		db_app_system_upgrade("mallory")
	}()
	if version := db_open("users/mallory/notes/app.db").integer("pragma user_version"); version != 0 {
		t.Errorf("a failed step moved the version to %d", version)
	}
}

// TestAppSystemSweepClosesWhatItOpens: the startup sweep runs before anything
// can evict a handle, so it closes each app.db it opened rather than holding
// its descriptors.
func TestAppSystemSweepClosesWhatItOpens(t *testing.T) {
	directory := test_data_directory(t)
	names := []string{"one", "two", "three"}
	for _, name := range names {
		db_open("users/alice/" + name + "/app.db").exec("create table other (a)")
	}
	db_purge_prefix("users")
	db_app_system_sweep()
	for _, name := range names {
		path := filepath.Join(directory, "users", "alice", name, "app.db")
		if db_cached(path) {
			t.Errorf("THE POINT: the sweep left %s open", path)
		}
		if version := db_open("users/alice/" + name + "/app.db").integer("pragma user_version"); version != len(db_system_steps) {
			t.Errorf("%s is at version %d after the sweep", name, version)
		}
	}
}

// TestRestoreGuardJudgesOnlyAppDatabases: the restore guard compares an app's
// own database, in its db directory, with the app's schema, and leaves core's
// per-user files alone though they share an app's file name and keep versions
// of their own.
func TestRestoreGuardJudgesOnlyAppDatabases(t *testing.T) {
	test_bare_directory(t)
	av := &AppVersion{Version: "1.0", Label: "guarded"}
	av.Database.File = "notifications.db"
	av.Database.Schema = 5
	app := &App{id: "guarded", internal: av}
	av.app = app
	apps_lock.Lock()
	saved := apps
	apps = map[string]*App{"guarded": app}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = saved
		apps_lock.Unlock()
	})

	bundle := t.TempDir()
	write := func(relative string, version int) {
		path := filepath.Join(bundle, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		d, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if _, err := d.Exec(fmt.Sprintf("create table t (a); pragma user_version=%d", version)); err != nil {
			t.Fatal(err)
		}
	}
	write("notifications.db", 9)
	if refused, _ := restore_schema_guard(bundle); refused != "" {
		t.Errorf("THE POINT: core's own notifications.db was judged as the app's, refusing %q", refused)
	}
	write("guarded/db/notifications.db", 9)
	if refused, _ := restore_schema_guard(bundle); refused == "" {
		t.Error("an app database newer than its app was not refused")
	}
}

// actors_plans answers how networks.db plans each lookup of a hosted account
// by its address, keyed by the function that makes it.
func actors_plans(t *testing.T) map[string]string {
	t.Helper()
	queries := map[string]string{
		"federation_local": "select 1 from actors where network='matrix' and domain=? and deleted=0",
		"federation_user":  "select * from actors where network='matrix' and domain=? and handle=? and deleted=0",
		"xmpp_served":      "select 1 from actors where network='xmpp' and domain=? and deleted=0",
		"reports":          "select 1 from actors where domain=? and deleted=0",
	}
	out := map[string]string{}
	for name, query := range queries {
		values := []any{}
		for range strings.Count(query, "?") {
			values = append(values, "a")
		}
		rows, err := networks_database().internal.Query("explain query plan "+query, values...)
		if err != nil {
			t.Fatal(err)
		}
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, detail)
		}
		rows.Close()
		out[name] = strings.Join(plan, "; ")
	}
	return out
}

// TestActorsAreIndexedByDomain: the Matrix engine looks a hosted account up by
// domain, and by domain and handle, for every room member on every event, and
// XMPP for every stanza. Without an index each lookup reads the whole table,
// which at a few thousand accounts made one event in a 250-member room cost
// most of a second. A fresh install lays the index, and step 19 adds it to an
// install made before it.
func TestActorsAreIndexedByDomain(t *testing.T) {
	judge := func(how string) {
		t.Helper()
		for name, plan := range actors_plans(t) {
			if !strings.Contains(plan, "USING INDEX actors_domain") {
				t.Errorf("THE POINT: on %s, %s's lookup of an account by its address does not use the domain index: %s", how, name, plan)
			}
		}
	}

	test_data_directory(t)
	judge("a fresh install")

	test_bare_directory(t)
	db_upgrade_18()
	for _, plan := range actors_plans(t) {
		if strings.Contains(plan, "actors_domain") {
			t.Fatalf("step 18 already lays the index, so the upgrade below proves nothing: %s", plan)
		}
	}
	db_upgrade_19()
	judge("an install brought up by step 19")
}
