// Mochi server: db_upgrade_74 migration test
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"testing"
)

// TestDbUpgrade74RelaxDefaultEmail checks that the v74 migration relaxes the
// historical methods='email' default to '' for every account still on that
// untouched default — regardless of whether a third-party provider is linked —
// while leaving accounts that chose a stricter set or a different factor alone.
// This aligns existing users with the new "any registered factor signs you in"
// default and un-breaks OAuth login after the OAuth/email decoupling.
func TestDbUpgrade74RelaxDefaultEmail(t *testing.T) {
	tmp, _ := os.MkdirTemp("", "mochi_v74")
	defer os.RemoveAll(tmp)
	orig := data_dir
	data_dir = tmp
	defer func() { data_dir = orig }()

	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default '', disabled text not null default '', status text not null default 'active', restore_source text not null default '')")
	users.exec("create table oauth (id integer primary key, user text not null references users(uid) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")

	// a: plain default 'email', no provider -> relaxed to ''.
	users.exec("insert into users (uid, username, methods) values ('a', 'a@example.com', 'email')")
	// b: default 'email' with an OAuth provider linked -> relaxed to '' (linkage is irrelevant now).
	users.exec("insert into users (uid, username, methods) values ('b', 'b@example.com', 'email')")
	users.exec("insert into oauth (user, provider, subject, created) values ('b', 'google', 'sub-b', 0)")
	// c: a deliberately stricter set -> unchanged.
	users.exec("insert into users (uid, username, methods) values ('c', 'c@example.com', 'email,totp')")
	// d: a different sole factor -> unchanged.
	users.exec("insert into users (uid, username, methods) values ('d', 'd@example.com', 'passkey')")
	// e: already '' -> unchanged (idempotent).
	users.exec("insert into users (uid, username, methods) values ('e', 'e@example.com', '')")

	db_upgrade_74()

	want := map[string]string{"a": "", "b": "", "c": "email,totp", "d": "passkey", "e": ""}
	for u, expected := range want {
		row, _ := users.row("select methods from users where uid=?", u)
		if got := row_string(row, "methods"); got != expected {
			t.Errorf("user %s: methods = %q, want %q", u, got, expected)
		}
	}
}
