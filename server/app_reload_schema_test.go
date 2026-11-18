// Mochi server: a development app whose reloaded manifest raises its database
// schema is migrated on the next open, not at the next restart.
//
// A development server serves an app's new code as soon as its files change,
// but a handle already open had been marked ready at the old schema, so every
// later open took the fast path and the new code queried columns the upgrade
// had not yet added (wasabi, 2026-11-17: feeds schema 24, "no such column:
// notified" and "upkeep" until the restart). A raise is migrated only once the
// app's files have settled, so a checkout that writes app.json before the
// Starlark beside it does not run the step from the old code.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// schema_source is a development app at either schema: version 2 adds a
// column, and tick writes it, as Feeds' scheduled poll wrote notified.
const schema_source = `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key)")

def database_upgrade(version):
    if version == 2:
        mochi.db.execute("alter table alpha add column extra text not null default ''")

def tick(e):
    mochi.db.execute("insert into alpha (extra) values ('ran')")
`

// schema_older is the same app before its version 2 step was written.
const schema_older = `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key)")

def database_upgrade(version):
    pass
`

// schema_manifest is the app's app.json at the given schema.
func schema_manifest(schema int) string {
	return `{"version":"1.0","label":"schema","architecture":{"engine":"starlark","version":` +
		itoa(app_version_maximum) + `},"execute":["app.star"],"database":{"file":"schema.db","schema":` +
		itoa(schema) + `,"create":{"function":"database_create"},"upgrade":{"function":"database_upgrade"}}}`
}

// schema_settled is a moment the app's files were written long enough ago to
// have settled, after the one it was loaded at.
func schema_settled() time.Time {
	return time.Now().Add(-2 * starlark_settle)
}

// schema_app is a development app loaded at schema 1 from its own directory,
// on a server with development reload on, whose manifest and source the test
// can change. Its manifest has been read once, as the loader does at startup,
// so a reload is gated on the file changing.
func schema_app(t *testing.T, id, source string) (*App, *AppVersion, string) {
	t.Helper()
	// The runtime state starlark_configure sets at startup, as
	// lifecycle_test_app sets it.
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 32)
		starlark_default_timeout = 90 * time.Second
	}
	saved := dev_reload
	dev_reload = true
	t.Cleanup(func() { dev_reload = saved })

	base := t.TempDir()
	loaded := time.Now().Add(-time.Minute)
	schema_file(t, filepath.Join(base, "app.star"), source, loaded)
	manifest := filepath.Join(base, "app.json")
	schema_file(t, manifest, schema_manifest(1), loaded)

	av := &AppVersion{base: base, Version: "1.0"}
	a := &App{id: id, development: true, versions: map[string]*AppVersion{"1.0": av}, latest: av}
	av.app = a
	reload_now(av)
	if av.Database.Schema != 1 || len(av.Execute) != 1 {
		t.Fatalf("the app did not load from its manifest: schema %d, execute %v", av.Database.Schema, av.Execute)
	}
	t.Cleanup(func() { db_purge_prefix("users") })
	return a, av, manifest
}

// schema_file writes one of the app's files, dated when.
func schema_file(t *testing.T, path, content string, when time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("dating %s: %v", path, err)
	}
}

// schema_column reports whether the version 2 step's column is on the file.
func schema_column(db *DB) bool {
	return db.integer("select count(*) from pragma_table_info('alpha') where name='extra'") == 1
}

// TestReloadedSchemaMigratesOnTheNextOpen is the defect: the handle open at
// schema 1 stayed ready after the reload raised the schema to 2.
func TestReloadedSchemaMigratesOnTheNextOpen(t *testing.T) {
	test_data_directory(t)
	a, av, manifest := schema_app(t, "schemareload", schema_source)
	u := &User{UID: "schemareloaduser"}

	if db := db_app(u, a); db == nil || db.integer("pragma user_version") != 1 {
		t.Fatal("the database was not created at schema 1")
	}

	schema_file(t, manifest, schema_manifest(2), schema_settled())
	av.reload()
	if av.Database.Schema != 2 {
		t.Fatalf("the reload did not take the raised schema: %d", av.Database.Schema)
	}

	db := db_app(u, a)
	if db == nil {
		t.Fatal("db_app returned nil after the reload")
	}
	if v := db.integer("pragma user_version"); v != 2 {
		t.Errorf("user_version = %d after the reload raised the schema, want 2: the handle kept the fast path, so the new code runs against the old schema until a restart", v)
	}
	if !schema_column(db) {
		t.Error("the upgrade's column is missing after the reload")
	}
}

// TestUnsettledRaiseWaitsForItsCode: a checkout writes app.json before the
// Starlark beside it. The raise is seen while the old code, with no version 2
// step, is still on disk; run then, the step does nothing, is stamped done and
// never runs again, and the column is missing for good. The open waits for the
// files to settle instead, and the step runs from the code that declares it.
func TestUnsettledRaiseWaitsForItsCode(t *testing.T) {
	test_data_directory(t)
	a, av, manifest := schema_app(t, "schemaunsettled", schema_older)
	u := &User{UID: "schemaunsettleduser"}

	if db := db_app(u, a); db == nil || db.integer("pragma user_version") != 1 {
		t.Fatal("the database was not created at schema 1")
	}

	schema_file(t, manifest, schema_manifest(2), time.Now())
	av.reload()
	if av.Database.Schema != 2 {
		t.Fatalf("the reload did not take the raised schema: %d", av.Database.Schema)
	}

	// The checkout writes the Starlark a moment after app.json, while the
	// open below is already under way.
	star := filepath.Join(av.base, "app.star")
	written := make(chan struct{})
	go func() {
		defer close(written)
		time.Sleep(starlark_settle / 4)
		if err := os.WriteFile(star, []byte(schema_source), 0o600); err != nil {
			t.Errorf("writing app.star: %v", err)
		}
	}()
	db := db_app(u, a)
	<-written
	if v := db.integer("pragma user_version"); v != 2 {
		t.Errorf("user_version = %d after the open, want 2", v)
	}
	if !schema_column(db) {
		t.Error("the version 2 column is missing: the step was used up by the code that came before it")
	}
}

// TestFreshDatabaseWaitsForTheFiles: a user's first open of an app whose files
// are still being written gets its tables. A handle handed back without them
// would let an action run halfway, as Feeds' create makes its entity before
// its first insert.
func TestFreshDatabaseWaitsForTheFiles(t *testing.T) {
	test_data_directory(t)
	a, av, _ := schema_app(t, "schemafresh", schema_source)
	if err := os.Chtimes(filepath.Join(av.base, "app.star"), time.Now(), time.Now()); err != nil {
		t.Fatalf("dating app.star: %v", err)
	}

	db := db_app(&User{UID: "schemafreshuser"}, a)
	if db == nil {
		t.Fatal("db_app returned nil")
	}
	if n := db.integer("select count(*) from sqlite_master where type='table' and name='alpha'"); n != 1 {
		t.Error("the first open returned a handle with no tables while the files settled")
	}
}

// TestReloadDuringCreateLeavesTheLowerSchema: a reload landing while the
// database is being created must not have the handle marked ready at the
// raised schema, which its file never reached.
func TestReloadDuringCreateLeavesTheLowerSchema(t *testing.T) {
	app, av := lifecycle_test_app(t, `
def database_upgrade(version):
    if version == 2:
        mochi.db.execute("alter table alpha add column extra text not null default ''")
`)
	av.Database.Create.Function = ""
	av.Database.Upgrade.Function = "database_upgrade"
	av.Database.create_function = func(db *DB) {
		db.exec("create table alpha (id integer primary key)")
		apps_lock.Lock()
		av.Database.Schema = 2
		apps_lock.Unlock()
	}
	u := &User{UID: "schemaraceuser"}

	if db := db_app(u, app); db == nil {
		t.Fatal("db_app returned nil on creation")
	}
	db := db_app(u, app)
	if v := db.integer("pragma user_version"); v != 2 {
		t.Errorf("user_version = %d on the open after the raise, want 2", v)
	}
	if !schema_column(db) {
		t.Error("the version 2 column is missing: the handle was marked ready at schema 2 while its file was at 1")
	}
}

// TestScheduledEventReadsTheDevelopmentManifest: a scheduled event is the
// other way in. Web requests and P2P events reread a development app's
// manifest; a scheduled run did not, so its new code met the old schema even
// with the handle fixed.
func TestScheduledEventReadsTheDevelopmentManifest(t *testing.T) {
	create_test_users_db(t)
	db_open("db/users.db").exec("create table if not exists entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	db_apps()
	route_owner_user(t, "schedulereloaduser", "user", "")
	route_owner_identity(t, "schedulereloaduser")
	u := user_by_uid("schedulereloaduser")
	if u == nil {
		t.Fatal("the test user did not resolve")
	}

	a, _, manifest := schema_app(t, "schedulereload", schema_source)
	original := apps
	apps = map[string]*App{a.id: a}
	t.Cleanup(func() { apps = original; resolution_invalidate() })
	resolution_invalidate()

	if db := db_app(u, a); db == nil || db.integer("pragma user_version") != 1 {
		t.Fatal("the database was not created at schema 1")
	}

	schema_file(t, manifest, schema_manifest(2), schema_settled())
	schedule_run_event(&ScheduledEvent{User: u.UID, App: a.id, Event: "tick"})

	db := db_app(u, a)
	if v := db.integer("pragma user_version"); v != 2 {
		t.Errorf("user_version = %d after a scheduled run of the raised app, want 2: the raised schema was not migrated before the handler ran", v)
	}
	if n := db.integer("select count(*) from alpha where extra='ran'"); n != 1 {
		t.Error("the scheduled handler's write is missing: it queried the column before the upgrade added it")
	}
}
