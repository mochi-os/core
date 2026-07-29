// Mochi server: scope of built-in attachment event dispatch.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"testing"
)

// TestAttachmentEventsSkipDatabaselessApp checks who core will run built-in
// _attachment/* handling for. The handlers store other people's content in the
// app's storage, so an app that declares no database - and therefore does no
// attachment work at all - must not be a destination for them.
//
// attachments_setup() runs before the handler reads its payload, so the table
// appearing in the app's system database is proof the handler executed.
func TestAttachmentEventsSkipDatabaselessApp(t *testing.T) {
	if attachment_dispatch_reaches(t, "") {
		t.Error("built-in attachment handling ran for an app that declares no database: a peer can have content stored under an app that does no attachment work")
	}
}

// TestAttachmentEventsReachDatabaseApp is the other half, and the one that
// stops the guard above from being satisfied by simply refusing everything:
// an app that does attachment work must still receive them.
func TestAttachmentEventsReachDatabaseApp(t *testing.T) {
	if !attachment_dispatch_reaches(t, "myapp.db") {
		t.Error("built-in attachment handling did not run for an app that declares a database: legitimate attachment sync is broken")
	}
}

// attachment_dispatch_reaches routes a stranger's _attachment/create at an app
// declaring the given database file, and reports whether the handler ran.
func attachment_dispatch_reaches(t *testing.T, database string) bool {
	t.Helper()

	// Distinct id per case: db_open caches handles by data_dir-relative path,
	// so two cases sharing one would hand the second the first's handle,
	// pointing into an already-removed temp directory.
	// Distinct id and service per case: db_open caches handles by
	// data_dir-relative path and app_for_service caches by (user, service), so
	// two cases sharing either would hand the second the first's app.
	app_id, service := "filelike-nodatabase", "publish-nodatabase"
	if database != "" {
		app_id, service = "filelike-database", "publish-database"
	}
	tmp, err := os.MkdirTemp("", "mochi_attachment_scope")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	original := data_dir
	data_dir = tmp
	defer func() { data_dir = original }()
	defer os.RemoveAll(tmp)

	victim := "uid-victim"
	udb := db_open("db/users.db")
	udb.exec(`create table if not exists users (id integer primary key, uid text not null default '', username text unique not null,
		role text not null default 'user', methods text not null default '', disabled text not null default '',
		status text not null default '', restore_source text not null default '', restore_passkeys integer not null default 0,
		timezone text not null default 'UTC', created integer not null default 0, updated integer not null default 0)`)
	udb.exec(`create table if not exists entities (id text not null primary key, private text not null default '',
		fingerprint text not null default '', user text not null, parent text not null default '', class text not null,
		name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)`)
	udb.exec("insert into users (uid, username) values (?, ?)", victim, "victim")
	// user_by_uid resolves an identity entity, so the victim needs one.
	udb.exec(`insert into entities (id, fingerprint, user, class, name, privacy) values (?, ?, ?, 'person', 'Victim', 'public')`,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "bbbbbbbbb", victim)

	// An app shaped like files: one service, no declared database.
	av := &AppVersion{Version: "1", Services: []string{service}}
	av.Database.File = database
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	app := &App{id: app_id, versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = app
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps[app_id] = app
	apps_lock.Unlock()
	defer func() {
		apps_lock.Lock()
		delete(apps, app_id)
		apps_lock.Unlock()
	}()

	// A stranger's event: the service claim rides on the frame, so it costs
	// the sender nothing to assert it.
	e := &Event{
		id:              event_id(),
		from:            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		service:         service,
		event:           "_attachment/create",
		sender_services: []string{service},
		user:            user_by_uid(victim),
		content:         map[string]any{},
	}
	if e.user == nil {
		t.Fatal("fixture user not resolvable")
	}

	e.route()

	// Checked before opening: db_open creates the file, which would both
	// invent the evidence and leave a stray database behind.
	path := "users/" + victim + "/" + app_id + "/app.db"
	if _, err := os.Stat(data_dir + "/" + path); err != nil {
		return false
	}
	db := db_open(path)
	if db == nil {
		return false
	}
	row, _ := db.row("select name from sqlite_master where type='table' and name='attachments'")
	return row != nil
}
