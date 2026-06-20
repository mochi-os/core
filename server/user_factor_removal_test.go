// Mochi server: last-sign-in-factor removal guard test
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// TestUserFactorRemovalBlocked covers the guard shared by the passkey-delete
// and authenticator-disable paths: removing a factor's last credential is
// refused when the factor is still required, or when it's the user's only
// remaining way to sign in (the case the old "is it in methods" substring
// check missed once methods could be empty and email could be disabled).
func TestUserFactorRemovalBlocked(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	users := db_open("db/users.db")
	users.exec("create table credentials (id blob primary key, user text not null, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("create table oauth (id integer primary key, user text not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null)")
	settings := db_open("db/settings.db")
	settings.exec("create table settings (name text primary key, value text not null)")

	load := func(methods, disabled string) *User {
		users.exec("delete from users")
		users.exec("insert into users (uid, username, methods, disabled) values ('u1', 'a@example.com', ?, ?)", methods, disabled)
		var u User
		users.scan(&u, "select uid, username, role, methods, disabled, status from users where uid='u1'")
		return &u
	}
	reset := func() { users.exec("delete from credentials"); users.exec("delete from totp") }
	passkey := func() { users.exec("insert into credentials (id, user, public_key, created) values (x'01', 'u1', x'00', 1)") }
	totp := func() { users.exec("insert into totp (user, secret, verified, created) values ('u1', 's', 1, 1)") }

	cases := []struct {
		name              string
		methods, disabled string
		setup             func()
		factor, want      string
	}{
		{"delete passkey, email available", "", "", func() { reset(); passkey() }, "passkey", ""},
		{"delete only passkey, email disabled", "", "email", func() { reset(); passkey() }, "passkey", "last"},
		{"delete passkey while required", "passkey", "", func() { reset(); passkey() }, "passkey", "required"},
		{"delete passkey, totp fallback, email disabled", "", "email", func() { reset(); passkey(); totp() }, "passkey", ""},
		{"disable only totp, email disabled", "", "email", func() { reset(); totp() }, "totp", "last"},
		{"disable totp, email available", "", "", func() { reset(); totp() }, "totp", ""},
		{"disable totp while required", "totp", "", func() { reset(); totp() }, "totp", "required"},
	}
	for _, c := range cases {
		u := load(c.methods, c.disabled)
		c.setup()
		if got := user_factor_removal_blocked(u, c.factor); got != c.want {
			t.Errorf("%s: blocked = %q, want %q", c.name, got, c.want)
		}
	}
}
