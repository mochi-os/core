package main

import "testing"

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
