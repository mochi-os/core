// Shared test fixture: a temp data_dir with a registered user and app whose
// per-(user, app) DB feature tests can exec against.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"testing"
	"time"
)

// setup_sql_replication_test wires up just enough server state for an
// apply-side test: a temp data_dir, a registered user, a registered app
// pointing at a per-(user, app) DB the apply path will exec against,
// and a fresh schema in that DB.
func setup_sql_replication_test(t *testing.T) (cleanup func(), user_uid, app_id string) {
	t.Helper()
	tmp, err := os.MkdirTemp("", "mochi_sql_repl")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	orig := data_dir
	data_dir = tmp

	udb := db_open("db/users.db")
	udb.exec(`create table if not exists users (id integer primary key, uid text not null unique, username text not null unique)`)
	user_uid = "uid-test-sql"
	udb.exec("insert into users (uid, username) values (?, ?)", user_uid, "alice")

	app_id = "myapp"
	av := &AppVersion{Version: "1"}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	av.Database.File = "myapp.db"
	av.Database.Schema = 1
	av.Database.create_function = func(db *DB) {
		db.exec(`create table posts (id text primary key, title text not null)`)
	}
	a := &App{id: app_id, versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = a
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps[app_id] = a
	apps_lock.Unlock()

	cleanup = func() {
		apps_lock.Lock()
		delete(apps, app_id)
		apps_lock.Unlock()
		data_dir = orig
		os.RemoveAll(tmp)
	}
	return
}

// 50-character pseudo-entity-id used in tests where valid("entity") needs
// to pass (49-51 word chars). The first character varies so different
// fixtures produce distinct IDs.
func test_entity_id(prefix byte) string {
	out := make([]byte, 50)
	out[0] = prefix
	for i := 1; i < 50; i++ {
		out[i] = 'a'
	}
	return string(out)
}

// setup_replication_test survives the replication removal as the shared
// "temp server state" fixture many feature tests grew on: a fresh data_dir,
// net_id set to "self", and a queue.db schema to absorb async send_peer
// writes. The name is historical.
func setup_replication_test(t *testing.T) func() {
	tmp_dir, err := os.MkdirTemp("", "mochi_repl_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	orig_data_dir := data_dir
	data_dir = tmp_dir
	orig_p2p_id := net_id
	net_id = "self"

	// queue.db is touched by Message.send_work via send_peer goroutines —
	// tests that fire emits asynchronously would otherwise panic on a missing
	// table. No actual delivery happens in unit tests; rows just accumulate
	// and are torn down with the temp dir.
	queue := db_open("db/queue.db")
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20 )")

	return func() {
		// Drain any /mochi/2 self-loop workers spawned by this test before we
		// mutate global state (data_dir, net_id) — otherwise a worker
		// mid-handler would race the assignments below.
		workers_drain_test(500 * time.Millisecond)
		data_dir = orig_data_dir
		net_id = orig_p2p_id
		os.RemoveAll(tmp_dir)
	}
}
// setup_users_test_schema creates a minimal users.db schema for tests that
// exercise the keys-transfer or session-replication apply paths. Mirrors
// the v53 schema: uid is the PK on users, FKs reference users(uid).
func setup_users_test_schema() {
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', disabled text not null default '', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")
	users.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create index entities_user on entities(user)")
	// Auth-factor tables — mirrors db.go's uid-keyed schema. Needed by
	// the per-user link keys-transfer tests (auth factors travel in the
	// payload).
	users.exec("create table credentials (id blob primary key, user text not null references users(uid) on delete cascade, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create table recovery (id integer primary key, user text not null references users(uid) on delete cascade, hash text not null, created integer not null)")
	users.exec("create table totp (user text primary key references users(uid) on delete cascade, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("create table oauth (id integer primary key, user text not null references users(uid) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create table tokens (hash text primary key not null, user text not null references users(uid) on delete cascade, app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")
}

// setup_sessions_test_schema creates the sessions table for tests that
// exercise session-replication apply paths.
func setup_sessions_test_schema() {
	sessions := db_open("db/sessions.db")
	sessions.exec("create table sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("create unique index sessions_code on sessions(code)")
}
