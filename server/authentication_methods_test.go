// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestUserMethodsConfigure covers the per-method tri-state setter: valid
// transitions (required <-> allowed <-> disabled), the operator policy
// floor/ceiling, credential availability, and the at-least-one-factor guard
// that stops a lockout. It also checks the derived per-method state and that
// auth_remaining_methods tracks only the required set.
func TestUserMethodsConfigure(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	users := db_open("db/users.db")
	// create_test_users_db now includes the disabled column; add the credential
	// tables that availability checks read.
	users.exec("create table credentials (id blob primary key, user text not null, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, created integer not null)")

	settings := db_open("db/settings.db")
	settings.exec("create table settings (name text primary key, value text not null)")

	users.exec("insert into users (uid, username, methods) values ('u1', 'a@example.com', 'email')")

	// load re-reads u1 so each step sees the persisted methods/disabled.
	load := func() *User {
		var u User
		if !users.scan(&u, "select uid, username, role, methods, disabled, status from users where uid='u1'") {
			t.Fatal("reload failed")
		}
		return &u
	}

	// Default: email is required.
	if s := user_method_state(load(), "email"); s != "required" {
		t.Errorf("email state = %q, want required", s)
	}

	// Disabling email while it's the only factor is refused (lockout guard).
	if code := user_methods_configure(load(), "email", "disabled"); code != "last" {
		t.Errorf("disable last factor = %q, want last", code)
	}

	// Register a passkey, allow it, then email can drop to allowed.
	users.exec("insert into credentials (id, user, public_key, created) values (x'01', 'u1', x'00', 1)")
	if code := user_methods_configure(load(), "passkey", "allowed"); code != "" {
		t.Errorf("allow passkey = %q, want ok", code)
	}
	if code := user_methods_configure(load(), "email", "allowed"); code != "" {
		t.Errorf("relax email to allowed = %q, want ok", code)
	}
	u := load()
	if u.Methods != "" {
		t.Errorf("methods = %q, want empty (nothing required)", u.Methods)
	}
	if got := user_login_factors(u); len(got) != 2 || got[0] != "email" || got[1] != "passkey" {
		t.Errorf("login factors = %v, want [email passkey]", got)
	}

	// Require passkey -> it AND-s into login; auth_remaining tracks it.
	if code := user_methods_configure(load(), "passkey", "required"); code != "" {
		t.Errorf("require passkey = %q, want ok", code)
	}
	if rem := auth_remaining_methods(load(), "email"); len(rem) != 1 || rem[0] != "passkey" {
		t.Errorf("remaining after email = %v, want [passkey]", rem)
	}
	if rem := auth_remaining_methods(load(), "passkey"); len(rem) != 0 {
		t.Errorf("remaining after passkey = %v, want none", rem)
	}

	// Requiring/allowing a factor with no credential is refused.
	if code := user_methods_configure(load(), "totp", "required"); code != "credential" {
		t.Errorf("require totp without secret = %q, want credential", code)
	}

	// Recovery can never be required. OAuth can be required now that it's an
	// independent factor, but only with a linked provider - none is linked here,
	// so it's gated on the credential rather than hard-rejected.
	if code := user_methods_configure(load(), "recovery", "required"); code != "invalid" {
		t.Errorf("require recovery = %q, want invalid", code)
	}
	if code := user_methods_configure(load(), "oauth", "required"); code != "credential" {
		t.Errorf("require oauth without a linked provider = %q, want credential", code)
	}

	// Unknown method / state are rejected.
	if code := user_methods_configure(load(), "sms", "allowed"); code != "invalid" {
		t.Errorf("unknown method = %q, want invalid", code)
	}
	if code := user_methods_configure(load(), "email", "sometimes"); code != "invalid" {
		t.Errorf("unknown state = %q, want invalid", code)
	}

	// Disabling a method is recorded and reflected in the state.
	users.exec("update users set methods='email', disabled='' where uid='u1'")
	if code := user_methods_configure(load(), "passkey", "disabled"); code != "" {
		t.Errorf("disable passkey = %q, want ok", code)
	}
	if s := user_method_state(load(), "passkey"); s != "disabled" {
		t.Errorf("passkey state after disable = %q, want disabled", s)
	}
	if user_method_usable(load(), "passkey") {
		t.Error("disabled passkey reported usable")
	}

	// Operator floor: system email=required forbids lowering email.
	setting_set("auth_email", "required")
	if code := user_methods_configure(load(), "email", "allowed"); code != "blocked" {
		t.Errorf("lower system-required email = %q, want blocked", code)
	}
	// Operator ceiling: system passkey=disabled forbids enabling passkey.
	setting_set("auth_passkey", "disabled")
	if code := user_methods_configure(load(), "passkey", "allowed"); code != "blocked" {
		t.Errorf("enable system-disabled passkey = %q, want blocked", code)
	}
}

// TestUserMethodStateOperatorClamp covers the operator-policy clamp in
// user_method_state (the settings grid's displayed state): a server-disabled
// method reads "disabled" and server-required email reads "required", whatever
// the user set or has registered.
func TestUserMethodStateOperatorClamp(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	users := db_open("db/users.db")
	users.exec("create table credentials (id blob primary key, user text not null, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, created integer not null)")
	settings := db_open("db/settings.db")
	settings.exec("create table settings (name text primary key, value text not null)")
	users.exec("insert into users (uid, username, methods) values ('u1', 'a@example.com', '')")
	// A registered passkey: without the clamp this would read "allowed".
	users.exec("insert into credentials (id, user, public_key, created) values (x'01', 'u1', x'00', 1)")

	load := func() *User {
		var u User
		users.scan(&u, "select uid, username, role, methods, disabled, status from users where uid='u1'")
		return &u
	}

	setting_set("auth_passkey", "disabled")
	if s := user_method_state(load(), "passkey"); s != "disabled" {
		t.Errorf("passkey state with auth_passkey=disabled = %q, want disabled", s)
	}
	setting_set("auth_email", "required")
	if s := user_method_state(load(), "email"); s != "required" {
		t.Errorf("email state with auth_email=required = %q, want required", s)
	}
}

// TestPartialContinue verifies MFA factors converge onto one partial in any
// completion order: a factor completed with a live login_partial cookie folds
// into that partial instead of minting a fresh one (which forgot the factors
// already done and made accounts requiring a code factor plus passkey or
// OAuth impossible to sign in to), a satisfied partial is deleted, and
// another user's cookie is never merged.
func TestPartialContinue(t *testing.T) {
	cleanup := create_test_sessions_db(t)
	defer cleanup()

	sessions := db_open("db/sessions.db")
	request := func(cookie string) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/_/auth/verify", nil)
		if cookie != "" {
			c.Request.AddCookie(&http.Cookie{Name: "login_partial", Value: cookie})
		}
		return c
	}

	// First factor without a cookie: a fresh partial holding just it.
	alice := &User{UID: "u-alice", Username: "alice@example.com", Methods: "email,passkey"}
	partial, remaining := partial_continue(request(""), alice, "email")
	if partial == "" || len(remaining) != 1 || remaining[0] != "passkey" {
		t.Fatalf("first factor: partial=%q remaining=%v, want fresh partial remaining [passkey]", partial, remaining)
	}

	// Second factor with the cookie: folds in, satisfies, deletes the partial.
	done, left := partial_continue(request(partial), alice, "passkey")
	if done != "" || left != nil {
		t.Errorf("second factor: partial=%q remaining=%v, want completion", done, left)
	}
	if exists, _ := sessions.exists("select 1 from partial where id=?", partial); exists {
		t.Error("satisfied partial should be deleted")
	}

	// Three factors: the middle completion updates the same partial in place,
	// storing the completed set in canonical order.
	carol := &User{UID: "u-carol", Username: "carol@example.com", Methods: "email,passkey,totp"}
	first, left := partial_continue(request(""), carol, "passkey")
	if first == "" || len(left) != 2 {
		t.Fatalf("carol first factor: partial=%q remaining=%v", first, left)
	}
	second, left := partial_continue(request(first), carol, "email")
	if second != first {
		t.Errorf("second factor minted a new partial %q, want in-place update of %q", second, first)
	}
	if len(left) != 1 || left[0] != "totp" {
		t.Errorf("carol remaining = %v, want [totp]", left)
	}
	row, _ := sessions.row("select completed from partial where id=?", first)
	if row == nil || row["completed"].(string) != "email,passkey" {
		t.Errorf("completed = %v, want canonical email,passkey", row)
	}

	// A cookie naming another user's partial is never merged: bob gets his
	// own fresh partial and carol's row stays untouched.
	bob := &User{UID: "u-bob", Username: "bob@example.com", Methods: "email,totp"}
	other, left := partial_continue(request(first), bob, "email")
	if other == "" || other == first {
		t.Errorf("cross-user cookie merged: got %q (carol's %q)", other, first)
	}
	if len(left) != 1 || left[0] != "totp" {
		t.Errorf("bob remaining = %v, want [totp]", left)
	}
	row, _ = sessions.row("select completed from partial where id=?", first)
	if row == nil || row["completed"].(string) != "email,passkey" {
		t.Errorf("carol's partial mutated by bob's completion: %v", row)
	}
}

// TestAccountRateLimit verifies the per-account attempt limit on guessable
// factors: hammering TOTP against one account answers 429 after the limit
// regardless of source address, while other accounts stay unaffected.
func TestAccountRateLimit(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	users := db_open("db/users.db")
	users.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("insert into users (uid, username, methods) values ('u-limit', 'limit@example.com', 'totp')")
	users.exec("insert into totp (user, secret, verified, created) values ('u-limit', 'JBSWY3DPEHPK3PXP', 1, 1)")
	users.exec("insert into users (uid, username, methods) values ('u-other', 'other@example.com', 'totp')")
	users.exec("insert into totp (user, secret, verified, created) values ('u-other', 'JBSWY3DPEHPK3PXP', 1, 1)")
	defer rate_limit_account.reset("u-limit")
	defer rate_limit_account.reset("u-other")

	attempt := func(email string) int {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/_/auth/totp", strings.NewReader(`{"email":"`+email+`","code":"000000"}`))
		c.Request.Header.Set("Content-Type", "application/json")
		web_auth_totp(c)
		return w.Code
	}

	for i := 1; i <= rate_limit_account.limit; i++ {
		if code := attempt("limit@example.com"); code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: got %d, want 401", i, code)
		}
	}
	if code := attempt("limit@example.com"); code != http.StatusTooManyRequests {
		t.Errorf("over-limit attempt: got %d, want 429", code)
	}
	// The limit is per account, not global: another account still verifies.
	if code := attempt("other@example.com"); code != http.StatusUnauthorized {
		t.Errorf("other account: got %d, want 401", code)
	}
}
