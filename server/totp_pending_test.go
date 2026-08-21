// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for enrolling a TOTP authenticator without disturbing the current one.
// The new secret waits in pending until a code proves it: overwriting the row
// would reset verified and drop TOTP out of the user's usable factors.

package main

import (
	"testing"
	"time"

	"github.com/pquerna/otp/totp"
	sl "go.starlark.net/starlark"
)

// totp_user prepares a user with a verified authenticator and returns its
// secret, plus the Starlark thread the API builtins read their context from.
func totp_user(t *testing.T) (*sl.Thread, string) {
	t.Helper()
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username, methods) values ('u-totp', 'totp@example.com', 'email')")

	key, err := totp.Generate(totp.GenerateOpts{Issuer: "Mochi", AccountName: "totp@example.com"})
	if err != nil {
		t.Fatalf("generating a secret: %v", err)
	}
	users.exec("insert into totp (user, secret, verified, pending, created) values ('u-totp', ?, 1, '', ?)",
		key.Secret(), now())

	// An internal app: require_permission bypasses the grant check for those,
	// which keeps these tests on the TOTP behaviour rather than on permission
	// plumbing (that is covered in permissions_test.go).
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: "u-totp", Username: "totp@example.com"})
	thread.SetLocal("app", &App{id: "test-app", internal: &AppVersion{}})
	return thread, key.Secret()
}

func totp_row(t *testing.T) (secret string, verified int64, pending string) {
	t.Helper()
	row, _ := db_open("db/users.db").row("select secret, verified, pending from totp where user='u-totp'")
	if row == nil {
		t.Fatal("no totp row")
	}
	secret, _ = row["secret"].(string)
	verified, _ = row["verified"].(int64)
	pending, _ = row["pending"].(string)
	return
}

// TestTotpSetupLeavesTheLiveAuthenticatorAlone is the defect: beginning an
// enrolment must not touch the secret the user is currently logging in with,
// nor its verified flag.
func TestTotpSetupLeavesTheLiveAuthenticatorAlone(t *testing.T) {
	defer setup_replication_test(t)()
	setup_users_test_schema()
	thread, original := totp_user(t)

	builtin := sl.NewBuiltin("mochi.user.totp.setup", api_user_totp_setup)
	if _, err := api_user_totp_setup(thread, builtin, nil, nil); err != nil {
		t.Fatalf("setup: %v", err)
	}

	secret, verified, pending := totp_row(t)
	if secret != original {
		t.Error("starting an enrolment replaced the live secret; the user's existing authenticator no longer works")
	}
	if verified != 1 {
		t.Error("starting an enrolment cleared verified; TOTP drops out of the user's usable factors and login falls back to an email code")
	}
	if pending == "" || pending == original {
		t.Error("the new secret was not parked in pending")
	}
}

// TestTotpAbandonedEnrolmentKeepsLoginWorking states the same property the way
// a user meets it: they open the setup page, never scan the code, and their
// authenticator still signs them in.
func TestTotpAbandonedEnrolmentKeepsLoginWorking(t *testing.T) {
	defer setup_replication_test(t)()
	setup_users_test_schema()
	thread, original := totp_user(t)

	builtin := sl.NewBuiltin("mochi.user.totp.setup", api_user_totp_setup)
	if _, err := api_user_totp_setup(thread, builtin, nil, nil); err != nil {
		t.Fatalf("setup: %v", err)
	}

	code, err := totp.GenerateCode(original, time.Now())
	if err != nil {
		t.Fatalf("generating a code: %v", err)
	}
	if !totp_verify("u-totp", code) {
		t.Error("the existing authenticator no longer validates after an abandoned enrolment")
	}
	if !user_method_available(&User{UID: "u-totp"}, "totp") {
		t.Error("totp is no longer an available factor after an abandoned enrolment")
	}
}

// TestTotpVerifyPromotesThePendingSecret: completing the enrolment swaps the
// new secret in and retires the old one.
func TestTotpVerifyPromotesThePendingSecret(t *testing.T) {
	defer setup_replication_test(t)()
	setup_users_test_schema()
	thread, original := totp_user(t)

	setup := sl.NewBuiltin("mochi.user.totp.setup", api_user_totp_setup)
	if _, err := api_user_totp_setup(thread, setup, nil, nil); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, _, pending := totp_row(t)

	code, err := totp.GenerateCode(pending, time.Now())
	if err != nil {
		t.Fatalf("generating a code: %v", err)
	}
	verify := sl.NewBuiltin("mochi.user.totp.verify", api_user_totp_verify)
	result, err := api_user_totp_verify(thread, verify, sl.Tuple{sl.String(code)}, nil)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if result != sl.True {
		t.Fatalf("verify returned %v for a valid pending code, want True", result)
	}

	secret, verified, still_pending := totp_row(t)
	if secret != pending {
		t.Error("the proven secret was not promoted")
	}
	if secret == original {
		t.Error("the old secret is still in place after a completed enrolment")
	}
	if verified != 1 {
		t.Error("the promoted authenticator is not marked verified")
	}
	if still_pending != "" {
		t.Error("pending was not cleared after promotion")
	}
}

// TestTotpVerifyWithTheOldCodeStillStepsUp: while an enrolment is in flight the
// existing authenticator must still satisfy a step-up, or a user who starts an
// enrolment locks themselves out of the very flows that guard it.
func TestTotpVerifyWithTheOldCodeStillStepsUp(t *testing.T) {
	defer setup_replication_test(t)()
	setup_users_test_schema()
	setup_sessions_test_schema()
	// reauthentication_result records the accrual here; the shared sessions
	// fixture does not carry this table.
	db_open("db/sessions.db").exec("create table reauthentication (id text primary key, user text not null, methods text not null default '', expires integer not null)")
	thread, original := totp_user(t)

	setup := sl.NewBuiltin("mochi.user.totp.setup", api_user_totp_setup)
	if _, err := api_user_totp_setup(thread, setup, nil, nil); err != nil {
		t.Fatalf("setup: %v", err)
	}

	code, err := totp.GenerateCode(original, time.Now())
	if err != nil {
		t.Fatalf("generating a code: %v", err)
	}
	verify := sl.NewBuiltin("mochi.user.totp.verify", api_user_totp_verify)
	result, err := api_user_totp_verify(thread, verify, sl.Tuple{sl.String(code)}, nil)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if result == sl.None {
		t.Error("the existing authenticator was refused for step-up while an enrolment was pending")
	}

	// And the pending enrolment survives: a step-up is not a completion.
	if _, _, pending := totp_row(t); pending == "" {
		t.Error("a step-up with the old code cleared the pending enrolment")
	}
}

// TestTotpUpgradeAddsPendingColumn covers the migration on an existing install,
// including the idempotence db_upgrade relies on.
func TestTotpUpgradeAddsPendingColumn(t *testing.T) {
	defer setup_replication_test(t)()
	users := db_open("db/users.db")
	users.exec("create table totp (user text primary key, secret text not null, verified integer not null default 0, created integer not null)")

	db_upgrade_6()
	if have, _ := users.exists("select 1 from pragma_table_info('totp') where name='pending'"); !have {
		t.Fatal("db_upgrade_6 did not add the pending column")
	}

	// Runs again without error: db_upgrade re-walks versions on a server that
	// failed partway, and every migration has to tolerate that.
	db_upgrade_6()
}
