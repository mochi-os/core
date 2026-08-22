// Shared test fixture: a temp data_dir with a registered user and app whose
// per-(user, app) DB feature tests can exec against.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

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
func setup_replication_test(t *testing.T) {
	t.Helper()
	test_data_directory(t)

	previous_id := net_id
	net_id = "self"

	// Registered after the data_dir restore so it runs before it: any
	// /mochi/2 self-loop worker this test spawned has to be drained while the
	// globals it reads still hold the values it started under.
	t.Cleanup(func() {
		workers_drain_test(500 * time.Millisecond)
		net_id = previous_id
	})

	// queue.db is touched by Message.send_work via send_peer goroutines —
	// tests that fire emits asynchronously would otherwise panic on a missing
	// table. No actual delivery happens in unit tests; rows just accumulate
	// and are torn down with the temp dir.
	setup_queue_test_schema().exec(health_schema)
}

// setup_users_test_schema creates a minimal users.db schema, mirroring db.go's
// uid-keyed tables.
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
	users.exec("create table totp (user text primary key references users(uid) on delete cascade, secret text not null, verified integer not null default 0, pending text not null default '', created integer not null)")
	users.exec("create table oauth (id integer primary key, user text not null references users(uid) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create table tokens (hash text primary key not null, user text not null references users(uid) on delete cascade, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0)")
}

// setup_sessions_test_schema creates the sessions tables: the sessions
// themselves plus the short-lived rows the login flows write while a sign-in
// is still in progress.
func setup_sessions_test_schema() *DB {
	sessions := db_open("db/sessions.db")
	sessions.exec("create table sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("create unique index sessions_code on sessions(code)")
	sessions.exec("create table codes (code text not null, username text not null, expires integer not null, primary key (code, username))")
	sessions.exec("create table ceremonies (id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create table partial (id text primary key, user text not null, completed text not null default '', remaining text not null, expires integer not null)")
	sessions.exec("create table reauthentication (id text primary key, user text not null, methods text not null default '', expires integer not null)")
	return sessions
}

// test_data_directory points data_dir at a fresh temporary directory for the
// rest of the test and restores the previous value when it ends. t.TempDir
// removes the tree itself, and the restore is registered after it so it runs
// first, which is why callers need no cleanup of their own.
func test_data_directory(t testing.TB) string {
	t.Helper()
	previous := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = previous })
	return data_dir
}

// setup_settings_test_schema creates the settings table many features read
// their configuration from.
func setup_settings_test_schema() *DB {
	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings (name text primary key, value text not null)")
	return settings
}

// setup_domains_test_schema creates domains.db as web.go and domains.go expect
// to find it: the domains themselves, the routes that hang off them, and the
// per-path delegations.
func setup_domains_test_schema() *DB {
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner integer not null default 0, priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists routes_domain on routes(domain)")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner integer not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("create index if not exists delegations_domain on delegations(domain)")
	domains.exec("create index if not exists delegations_owner on delegations(owner)")
	return domains
}

// setup_repositories_test_schema creates the repositories table.
func setup_repositories_test_schema() *DB {
	db := db_open("db/repositories.db")
	db.exec(`create table if not exists repositories (
		id text primary key not null,
		name text not null default '',
		description text not null default '',
		default_branch text not null default 'main',
		size integer not null default 0,
		created text not null default '',
		updated text not null default ''
	)`)
	db.exec("create index if not exists repositories_name on repositories(name)")
	return db
}

// setup_queue_test_schema creates queue.db with the column set queue.go's
// `select * from queue` scans into, so a test row round-trips the same way a
// real one does.
func setup_queue_test_schema() *DB {
	queue := db_open("db/queue.db")
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20, claimed integer not null default 0 )")
	return queue
}

// setup_directory_test_schema creates the learned-directory entries table.
func setup_directory_test_schema() *DB {
	directory := db_open("db/directory.db")
	directory.exec(`create table if not exists entries (
		entity text not null, peer text not null, name text not null default '',
		class text not null default '', data text not null default '',
		fingerprint text not null default '', version integer not null default 0,
		created integer not null default 0, seen integer not null default 0,
		message text not null default '', expires text not null default '',
		signature text not null default '', primary key (entity, peer))`)
	return directory
}
