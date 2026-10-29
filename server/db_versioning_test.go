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
		"matrix.db":   federation_database(),
		"xmpp.db":     xmpp_database(),
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
	for _, name := range []string{"deliveries", "actors", "identities", "stream", "websub", "webmentions", "reports", "pictrs"} {
		if !versioning_has(networks, name) {
			t.Errorf("THE POINT: a fresh networks.db has no %s", name)
		}
	}
	if !versioning_column(networks, "deliveries", "address") {
		t.Error("a fresh deliveries table has no address")
	}
	matrix := federation_database()
	for _, name := range []string{"outbox", "rooms", "events", "clients", "changes", "stream", "settings", "presence", "media", "standins", "notices"} {
		if !versioning_has(matrix, name) {
			t.Errorf("THE POINT: a fresh matrix.db has no %s", name)
		}
	}
	if !versioning_column(matrix, "outbox", "origin") || !versioning_column(matrix, "changes", "gone") {
		t.Error("a fresh matrix.db lacks outbox.origin or changes.gone")
	}
	if !versioning_has(xmpp_database(), "roster") || !versioning_has(xmpp_database(), "chatlog_archive") {
		t.Error("THE POINT: a fresh xmpp.db has no roster or chatlog index")
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
	networks.exec("create table stream (sequence integer primary key, did text not null, kind text not null, body blob not null, created integer not null)")
	networks.exec("insert into stream (sequence, did, kind, body, created) values (7, 'did:plc:a', 'commit', x'00', 1)")
	networks.exec("create table received (id text not null primary key, created integer not null)")

	matrix := federation_database()
	matrix.exec("create table outbox (id integer primary key autoincrement, destination text not null, kind text not null, body text not null, created integer not null)")
	matrix.exec("create table changes (id integer primary key autoincrement, user text not null, viewer text not null default '', created integer not null)")
	rooms_tables(matrix)
	matrix.exec("insert into events (id, room, type, key, sender, depth, body, received) values ('$a', '!r', 'm.room.message', null, '@x:y', 1, '{}', 1)")
	matrix.exec("create table data (user text not null, type text not null, content text not null, primary key (user, type))")
	matrix.exec("insert into data (user, type, content) values ('@a:b', 'm.secret', '{}')")

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

	if schema := atoi(setting_get("schema", ""), 0); schema != 18 {
		t.Fatalf("schema after the upgrade is %d, want 18", schema)
	}
	if !versioning_column(networks, "deliveries", "address") {
		t.Error("THE POINT: deliveries did not gain address")
	}
	if versioning_has(networks, "received") {
		t.Error("the server-wide received table survived")
	}
	row, _ := networks.row("select sql from sqlite_master where name='stream'")
	if !strings.Contains(strings.ToLower(row_string(row, "sql")), "autoincrement") {
		t.Error("THE POINT: the stream table was not rebuilt with AUTOINCREMENT")
	}
	if networks.integer("select count(*) from stream where sequence=7") != 1 {
		t.Error("the rebuild lost the stream's events")
	}
	if !versioning_has(networks, "actors") || !versioning_has(networks, "pictrs") {
		t.Error("THE POINT: networks.db's newer tables were not laid")
	}
	if !versioning_column(matrix, "outbox", "origin") || !versioning_column(matrix, "changes", "gone") {
		t.Error("THE POINT: matrix.db did not gain outbox.origin and changes.gone")
	}
	if matrix.integer("select count(*) from stream where event='$a'") != 1 {
		t.Error("THE POINT: the rooms' timelines were not taken into the client order")
	}
	if matrix.integer("select count(*) from settings where user='@a:b' and type='m.secret'") != 1 {
		t.Error("THE POINT: the key store's account data was not copied into settings")
	}
	if !versioning_has(xmpp_database(), "roster") || !versioning_has(db_open("db/world.db"), "worlds") {
		t.Error("THE POINT: xmpp.db and world.db were not laid")
	}
	if versioning_has(data, "_commit_log") {
		t.Error("THE POINT: the old commit-hook log survived in the app's data database")
	}
	if !versioning_has(data, "posts") {
		t.Error("the app's own table went with the commit-hook log")
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
	for _, name := range []string{"preferences", "permissions", "groups", "keys", "objects", "repositories", "numbers", "snowflakes", "directory", "apps"} {
		if !versioning_has(db, name) {
			t.Errorf("user.db has no %s", name)
		}
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
	for _, file := range []string{"networks", "matrix", "xmpp", "world"} {
		check("db_upgrade_18."+file, versioning_schema(db_open("db/"+file+".db")))
	}
	db_upgrade_19()
	check("db_upgrade_19.networks", versioning_schema(db_open("db/networks.db")))

	for name, steps := range map[string][]func(*DB) error{
		"user":          db_user_steps["user"],
		"notifications": db_user_steps["notifications"],
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
		rows, err := networks_database().internal.Query("explain query plan "+query, "a", "b")
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
