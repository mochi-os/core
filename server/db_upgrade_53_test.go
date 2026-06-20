// Mochi server: db_upgrade_53 migration test
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDbUpgrade53FullRebuild exercises the v53 migration end-to-end:
// users gets rebuilt with uid as the TEXT primary key, every FK table is
// rebuilt with `user TEXT REFERENCES users(uid)`, parallel `user_uid`
// columns and v51 triggers go away, sessions/domains/schedule integer
// `user`/`owner` columns retype to TEXT and remap their values through
// the int-id → uid map, and per-user data directories rename from
// `users/<int>/` to `users/<uid>/`.
func TestDbUpgrade53FullRebuild(t *testing.T) {
	tmp, _ := os.MkdirTemp("", "mochi_v53")
	defer os.RemoveAll(tmp)
	orig := data_dir
	data_dir = tmp
	defer func() { data_dir = orig }()

	// Simulate post-v52 state: users with uid backfilled, entities with
	// user_uid populated, FK tables with parallel user_uid, and
	// sessions/domains/schedule still on integer user/owner.
	users := db_open("db/users.db")
	users.exec("create table users (id integer primary key, uid text not null default '', username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")
	users.exec("create unique index users_uid on users (uid)")
	users.exec(`create trigger users_uid_insert after insert on users
		when new.uid is null or new.uid = ''
		begin
			update users set uid = lower(hex(randomblob(16))) where id = new.id;
		end`)
	users.exec("insert into users (id, uid, username) values (1, 'uid-alice', 'alice@example.com')")
	users.exec("insert into users (id, uid, username) values (2, 'uid-bob', 'bob@example.com')")

	users.exec(`create table entities (
		id text not null primary key, private text not null, fingerprint text not null,
		user references users(id), user_uid text not null default '',
		parent text not null default '', class text not null, name text not null,
		privacy text not null default 'public', data text not null default '', published integer not null default 0
	)`)
	users.exec("insert into entities (id, private, fingerprint, user, user_uid, class, name) values ('e-alice', 'priv', 'fp-a', 1, 'uid-alice', 'person', 'Alice')")
	users.exec("insert into entities (id, private, fingerprint, user, user_uid, class, name) values ('e-bob', 'priv', 'fp-b', 2, 'uid-bob', 'person', 'Bob')")

	users.exec("create table credentials (id blob primary key, user integer not null, user_uid text not null default '', public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("insert into credentials (id, user, user_uid, public_key, created) values (x'01', 1, 'uid-alice', x'42', 0)")

	users.exec("create table recovery (id integer primary key, user integer not null, user_uid text not null default '', hash text not null, created integer not null)")
	users.exec("create table totp (user integer primary key, user_uid text not null default '', secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("create table oauth (id integer primary key, user integer not null, user_uid text not null default '', provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create table tokens (hash text primary key not null, user integer not null, user_uid text not null default '', app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")

	sessions := db_open("db/sessions.db")
	sessions.exec("create table sessions (user integer not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("insert into sessions (user, code, expires) values (1, 'sess-1', 999)")
	sessions.exec("create table ceremonies (id text primary key, type text not null, user integer, challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create table partial (id text primary key, user integer not null, completed text not null default '', remaining text not null, expires integer not null)")
	sessions.exec("create table logins (user integer primary key, last integer not null)")
	sessions.exec("insert into logins (user, last) values (1, 100)")
	sessions.exec("create table accesses (hash text primary key not null, user integer not null, used integer not null default 0)")
	sessions.exec("create table passkeys (credential blob primary key, user integer not null, last integer not null default 0)")
	sessions.exec("create table verifications (oauth integer primary key, user integer not null, last integer not null default 0)")

	domains := db_open("db/domains.db")
	domains.exec("create table domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("create table routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner integer not null default 0, priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("insert into domains (domain, created, updated) values ('example.com', 0, 0)")
	domains.exec("insert into routes (domain, path, method, target, owner, created, updated) values ('example.com', '/', 'app', 'wikis', 1, 0, 0)")
	domains.exec("create table delegations (id integer primary key, domain text not null, path text not null, owner integer not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("insert into delegations (domain, path, owner, created, updated) values ('example.com', '/api', 2, 0, 0)")

	schedule := db_open("db/schedule.db")
	schedule.exec("create table schedule (id integer primary key, user int not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	schedule.exec("insert into schedule (user, app, due, event, data, interval, created) values (1, 'wikis', 0, 'evt', '', 0, 0)")

	os.MkdirAll(filepath.Join(tmp, "users", "1", "wikis"), 0755)
	os.MkdirAll(filepath.Join(tmp, "users", "2"), 0755)

	db_upgrade_53()

	// Users: uid is the PK, no integer id.
	col_uid_pk := false
	rows, _ := users.rows("select name, pk from pragma_table_info('users')")
	for _, r := range rows {
		name, _ := r["name"].(string)
		pk, _ := r["pk"].(int64)
		if name == "uid" && pk > 0 {
			col_uid_pk = true
		}
		if name == "id" {
			t.Errorf("users.id should no longer exist after v53")
		}
	}
	if !col_uid_pk {
		t.Errorf("users.uid should be the PK after v53")
	}

	// entities.user is TEXT and references users(uid); user_uid dropped.
	if row, _ := users.row("select user from entities where id='e-alice'"); row != nil {
		if user, _ := row["user"].(string); user != "uid-alice" {
			t.Errorf("entities.user expected 'uid-alice', got %q", user)
		}
	} else {
		t.Error("entities row missing")
	}
	rows, _ = users.rows("select name from pragma_table_info('entities')")
	for _, r := range rows {
		if name, _ := r["name"].(string); name == "user_uid" {
			t.Errorf("entities.user_uid should have been dropped")
		}
	}

	// credentials.user remapped.
	if row, _ := users.row("select user from credentials"); row != nil {
		if user, _ := row["user"].(string); user != "uid-alice" {
			t.Errorf("credentials.user expected 'uid-alice', got %q", user)
		}
	}

	// sessions / logins / domains / schedule all remapped.
	if row, _ := sessions.row("select user from sessions where code='sess-1'"); row != nil {
		if user, _ := row["user"].(string); user != "uid-alice" {
			t.Errorf("sessions.user expected 'uid-alice', got %q", user)
		}
	}
	if row, _ := sessions.row("select user from logins"); row != nil {
		if user, _ := row["user"].(string); user != "uid-alice" {
			t.Errorf("logins.user expected 'uid-alice', got %q", user)
		}
	}
	if row, _ := domains.row("select owner from routes"); row != nil {
		if owner, _ := row["owner"].(string); owner != "uid-alice" {
			t.Errorf("routes.owner expected 'uid-alice', got %q", owner)
		}
	}
	if row, _ := domains.row("select owner from delegations"); row != nil {
		if owner, _ := row["owner"].(string); owner != "uid-bob" {
			t.Errorf("delegations.owner expected 'uid-bob', got %q", owner)
		}
	}
	if row, _ := schedule.row("select user from schedule"); row != nil {
		if user, _ := row["user"].(string); user != "uid-alice" {
			t.Errorf("schedule.user expected 'uid-alice', got %q", user)
		}
	}

	// Per-user disk directories renamed to uid-named.
	if _, err := os.Stat(filepath.Join(tmp, "users", "uid-alice")); os.IsNotExist(err) {
		t.Error("users/uid-alice/ should exist after rename")
	}
	if _, err := os.Stat(filepath.Join(tmp, "users", "uid-bob")); os.IsNotExist(err) {
		t.Error("users/uid-bob/ should exist after rename")
	}
	if _, err := os.Stat(filepath.Join(tmp, "users", "1")); !os.IsNotExist(err) {
		t.Error("users/1/ should no longer exist after rename")
	}
	if _, err := os.Stat(filepath.Join(tmp, "users", "uid-alice", "wikis")); os.IsNotExist(err) {
		t.Error("nested data should have moved during rename")
	}
}
