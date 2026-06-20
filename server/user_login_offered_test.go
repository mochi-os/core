// Mochi server: login factor-offering test
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// TestUserLoginOffered checks that the login screen offers only factors that
// can actually complete the login: when the account requires specific factors,
// usable-but-not-required ones are dropped (they can't substitute); when
// nothing is required, every usable factor is offered (any one suffices). The
// system email floor is folded in like the login bar itself.
func TestUserLoginOffered(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	users := db_open("db/users.db")
	users.exec("create table credentials (id blob primary key, user text not null, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("create table oauth (id integer primary key, user text not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	settings := db_open("db/settings.db")
	settings.exec("create table settings (name text primary key, value text not null)")
	// Register a passkey and a verified authenticator so both count as usable.
	users.exec("insert into credentials (id, user, public_key, created) values (x'01', 'u1', x'00', 1)")
	users.exec("insert into totp (user, secret, verified, created) values ('u1', 's', 1, 1)")

	got := func(methods string) string {
		users.exec("delete from users")
		users.exec("insert into users (uid, username, methods) values ('u1', 'a@example.com', ?)", methods)
		var u User
		users.scan(&u, "select uid, username, role, methods, disabled, status from users where uid='u1'")
		return strings.Join(user_login_offered(&u), ",")
	}

	// Nothing required: any one usable factor suffices, so offer them all.
	if g := got(""); g != "email,passkey,totp" {
		t.Errorf("no required: offered = %q, want email,passkey,totp", g)
	}
	// Email required: only email can complete the login; drop the rest.
	if g := got("email"); g != "email" {
		t.Errorf("email required: offered = %q, want email", g)
	}
	// Two required: offer exactly those, still dropping non-required passkey.
	if g := got("email,totp"); g != "email,totp" {
		t.Errorf("email+totp required: offered = %q, want email,totp", g)
	}
	// System email floor applies even when the user requires nothing.
	setting_set("auth_email", "required")
	if g := got(""); g != "email" {
		t.Errorf("system email floor: offered = %q, want email", g)
	}
}
