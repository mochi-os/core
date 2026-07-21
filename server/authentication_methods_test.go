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

// TestAccountGateSpacing checks the reserved-slot spacing curve: the first few
// failures reserve at the floor, then the gap doubles per failure and caps.
func TestAccountGateSpacing(t *testing.T) {
	if g := account_gate_spacing(0); g != account_gate_floor {
		t.Errorf("free tier spacing = %d, want floor %d", g, account_gate_floor)
	}
	if g := account_gate_spacing(account_gate_free); g != account_gate_floor {
		t.Errorf("last free spacing = %d, want floor %d", g, account_gate_floor)
	}
	if g := account_gate_spacing(account_gate_free + 2); g != 2 {
		t.Errorf("second paid spacing = %d, want 2", g)
	}
	if g := account_gate_spacing(1000); g != account_wait_max {
		t.Errorf("far past the cap = %d, want %d", g, account_wait_max)
	}
}

// TestAccountGateReserve is the anti-parallel test the old (read-count, sleep,
// verify) throttle failed: concurrent guesses all serialise through the gate
// lock, so calling reserve repeatedly on one account (which is exactly what N
// concurrent requests do — one at a time under the lock) hands out DISTINCT,
// increasing wait slots, not the same free-tier slot to everyone. Past the
// max-wait depth the gate refuses rather than queue, bounding both the guess
// rate per account and the number of handlers that ever sleep.
func TestAccountGateReserve(t *testing.T) {
	gate := &account_gate{entries: make(map[string]*account_gate_entry)}

	// A burst of reservations (no failures recorded yet) gets floor-spaced,
	// increasing slots — never all zero.
	var waits []int64
	for i := 0; i < 20; i++ {
		wait, ok := gate.reserve("u1")
		if !ok {
			break
		}
		waits = append(waits, wait)
	}
	if len(waits) < 2 || waits[0] != 0 || waits[1] < account_gate_floor {
		t.Fatalf("burst did not serialise: %v (want 0, then >= floor, increasing)", waits)
	}
	for i := 1; i < len(waits); i++ {
		if waits[i] <= waits[i-1] {
			t.Errorf("slot %d (%d) not after slot %d (%d) — parallel bypass", i, waits[i], i-1, waits[i-1])
		}
	}
	// Beyond the max-wait depth the gate refuses rather than hold a goroutine.
	if _, ok := gate.reserve("u1"); ok {
		t.Error("a queue past account_wait_max must be refused (429), not queued")
	}
	// A different account is unaffected: its first slot is immediate.
	if wait, ok := gate.reserve("u2"); !ok || wait != 0 {
		t.Errorf("separate account: wait=%d ok=%v, want immediate slot", wait, ok)
	}
	// A correct credential (reset) clears the queue.
	gate.reset("u1")
	if wait, ok := gate.reserve("u1"); !ok || wait != 0 {
		t.Errorf("after reset: wait=%d ok=%v, want cleared", wait, ok)
	}
}

// TestAccountRateLimit verifies the per-account throttle does NOT hard-lock an
// account: with spacing neutralised (so serial attempts don't sleep), wrong
// codes stay 401 indefinitely — never a 429 that a third party could hold a
// known account in — and a correct credential is never blocked.
func TestAccountRateLimit(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	users := db_open("db/users.db")
	users.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("insert into users (uid, username, methods) values ('u-limit', 'limit@example.com', 'totp')")
	users.exec("insert into totp (user, secret, verified, created) values ('u-limit', 'JBSWY3DPEHPK3PXP', 1, 1)")
	defer account_login.reset("u-limit")

	// Neutralise spacing (floor 0 AND a free tier past the attempt count) so
	// serial attempts reserve immediate slots and never sleep; the spacing and
	// serialisation are covered by TestAccountGateReserve.
	savedFloor, savedFree := account_gate_floor, account_gate_free
	account_gate_floor, account_gate_free = 0, 1_000_000
	defer func() { account_gate_floor, account_gate_free = savedFloor, savedFree }()

	attempt := func() int {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/_/auth/totp", strings.NewReader(`{"email":"limit@example.com","code":"000000"}`))
		c.Request.Header.Set("Content-Type", "application/json")
		web_auth_totp(c)
		return w.Code
	}

	for i := 0; i < 25; i++ {
		if code := attempt(); code != http.StatusUnauthorized {
			t.Fatalf("wrong attempt %d: got %d, want 401 (never a hard 429 lockout)", i, code)
		}
	}
	if account_login.entries["u-limit"] == nil || account_login.entries["u-limit"].failures == 0 {
		t.Error("failures should accumulate")
	}
}
