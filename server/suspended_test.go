// Mochi server: a suspended account is told it is suspended.
//
// user_by_uid returns nil for a suspended user, deliberately - it is the "get
// the acting user" lookup and a suspended user must not act. Five login paths
// then checked user.Status == "suspended" AFTER that lookup, so the check could
// never run and the suspended user got whatever the nil branch said instead:
// a 500 on recovery login, "user not found" on MFA verify and passkey login,
// and "provider_error" on both OAuth paths - which blames the identity provider
// for what is purely the local account's state.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"regexp"
	"strings"
	"testing"
)

// suspended_env creates users.db with one active and one suspended account,
// each carrying a person entity so user_by_uid's OTHER nil - no identity -
// cannot be mistaken for the suspension filter.
func suspended_env(t *testing.T) func() {
	t.Helper()
	cleanup := create_web_test_env(t)
	users := db_open("db/users.db")
	// The shared fixture's tables predate these columns, and its entities
	// table carries a user_uid the Entity struct has no field for, so `select
	// *` in identity() fails to scan against it. Build the real schema here.
	users.exec("drop table if exists entities")
	users.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	for _, column := range []string{
		"methods text not null default ''",
		"disabled text not null default ''",
		"status text not null default 'active'",
	} {
		users.exec("alter table users add column " + column)
	}
	for _, account := range []struct{ uid, username, status string }{
		{"u-suspended", "suspended@example.com", "suspended"},
		{"u-active", "active@example.com", "active"},
	} {
		users.exec("insert into users (uid, username, role, status, created, updated) values (?, ?, 'user', ?, ?, ?)",
			account.uid, account.username, account.status, now(), now())
		users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, ?, 'person', ?)",
			"entity-"+account.uid, "fingerprint-"+account.uid, account.uid, account.username)
	}
	return cleanup
}

// TestUserByUidHidesASuspendedAccount is the premise every claim below rests
// on. If this ever stops being true, the checks that were removed were not
// dead after all and their removal is a security regression.
func TestUserByUidHidesASuspendedAccount(t *testing.T) {
	defer suspended_env(t)()

	if user_by_uid("u-suspended") != nil {
		t.Fatal("user_by_uid returned a suspended user; every caller treats a non-nil result as an account allowed to act")
	}
	// The same lookup on an otherwise identical active account must succeed,
	// or the nil above proves nothing about the status filter.
	if user_by_uid("u-active") == nil {
		t.Fatal("user_by_uid returned nil for an active account too, so the nil above is not evidence that suspension is what hides a user")
	}
}

// TestUserSuspendedTellsTheReasonsApart: the nil above conflates three
// outcomes - no such user, suspended, no identity - and only the middle one
// may be reported as a suspension.
func TestUserSuspendedTellsTheReasonsApart(t *testing.T) {
	defer suspended_env(t)()

	if !user_suspended("u-suspended") {
		t.Error("a suspended account is not reported as suspended, so its login paths cannot explain the refusal")
	}
	if user_suspended("u-active") {
		t.Error("an active account is reported as suspended")
	}
	if user_suspended("u-nonexistent") {
		t.Error("an unknown uid is reported as suspended, which would turn every not-found into a suspension notice")
	}
	if user_suspended("") {
		t.Error("an empty uid is reported as suspended")
	}
}

// TestNoSuspendedCheckFollowsUserByUid is the dead code itself. A status check
// placed after the lookup cannot run, and its presence is what made the paths
// look correct on inspection for as long as they did.
func TestNoSuspendedCheckFollowsUserByUid(t *testing.T) {
	for file, functions := range map[string][]string{
		"authentication.go": {"func web_auth_mfa(", "func web_recovery_login("},
		"passkeys.go":       {"func web_passkey_login_finish("},
		"oauth.go":          {"func oauth_login(", "func oauth_mobile_login("},
	} {
		for _, function := range functions {
			body := function_body(t, file, function)
			lookup := strings.Index(body, "user_by_uid(")
			if lookup < 0 {
				t.Errorf("%s %s no longer looks a user up by uid; this test is checking the wrong function", file, function)
				continue
			}
			if strings.Contains(body[lookup:], `Status == "suspended"`) {
				t.Errorf("%s %s checks the status AFTER user_by_uid, which returns nil for a suspended account - the check cannot run", file, function)
			}
		}
	}
}

// TestEveryLoginPathExplainsSuspension is the other half: removing the dead
// checks must not simply drop the behaviour they were reaching for.
func TestEveryLoginPathExplainsSuspension(t *testing.T) {
	for file, functions := range map[string][]string{
		"authentication.go": {"func web_auth_mfa(", "func web_recovery_login("},
		"passkeys.go":       {"func web_passkey_login_finish("},
		"oauth.go":          {"func oauth_login(", "func oauth_mobile_login("},
	} {
		for _, function := range functions {
			body := function_body(t, file, function)
			if !strings.Contains(body, "user_suspended(") {
				t.Errorf("%s %s cannot tell a suspended account from a missing one, so a suspended user is told the wrong thing", file, function)
			}
		}
	}
}

// TestSuspensionIsCheckedBeforeTheFallback: the order is the whole point. The
// generic answer - 500, "user not found", "provider_error" - has to come
// second, or the suspension branch is unreachable for a different reason.
func TestSuspensionIsCheckedBeforeTheFallback(t *testing.T) {
	for file, pairs := range map[string][][2]string{
		"authentication.go": {
			{"func web_auth_mfa(", "user_not_found"},
			{"func web_recovery_login(", "user_error"},
		},
		"passkeys.go": {{"func web_passkey_login_finish(", "user_not_found"}},
		"oauth.go": {
			{"func oauth_login(", "oauth_user_missing"},
			{"func oauth_mobile_login(", "oauth_user_missing"},
		},
	} {
		for _, pair := range pairs {
			function, fallback := pair[0], pair[1]
			body := function_body(t, file, function)
			suspended := strings.Index(body, "user_suspended(")
			generic := strings.Index(body, fallback)
			if suspended < 0 || generic < 0 {
				t.Errorf("%s %s: could not find both the suspension check and the %q fallback", file, function, fallback)
				continue
			}
			if suspended > generic {
				t.Errorf("%s %s answers %q before checking for a suspension, so the suspension branch never runs", file, function, fallback)
			}
		}
	}
}

// TestLiveSuspendedChecksAreLeftAlone: user_by_username does NOT filter, so
// the check after it is reachable and correct. A sweep that removed every
// post-lookup status check would break the TOTP login path silently.
func TestLiveSuspendedChecksAreLeftAlone(t *testing.T) {
	defer suspended_env(t)()

	user := user_by_username("suspended@example.com")
	if user == nil {
		t.Fatal("user_by_username hid a suspended account; the status check in web_auth_totp is now dead too")
	}
	if user.Status != "suspended" {
		t.Errorf("user_by_username returned status %q, want suspended", user.Status)
	}

	body := function_body(t, "authentication.go", "func web_auth_totp(")
	if !strings.Contains(body, `Status == "suspended"`) {
		t.Error("web_auth_totp no longer checks the status, but its lookup does not filter suspended accounts, so one can now complete TOTP login")
	}
}

// TestSuspendedLookupCostsNothingOnTheSuccessPath: the extra read is only
// justified because it happens on a failure branch. Called unconditionally it
// would add a query to every login.
func TestSuspendedLookupCostsNothingOnTheSuccessPath(t *testing.T) {
	for file, functions := range map[string][]string{
		"authentication.go": {"func web_auth_mfa(", "func web_recovery_login("},
		"passkeys.go":       {"func web_passkey_login_finish("},
		"oauth.go":          {"func oauth_login(", "func oauth_mobile_login("},
	} {
		for _, function := range functions {
			body := function_body(t, file, function)
			call := regexp.MustCompile(`(?m)^(\s*)if user_suspended\(`).FindStringSubmatch(body)
			if call == nil {
				t.Errorf("%s %s does not guard on user_suspended at all", file, function)
				continue
			}
			// Two tabs or more means it sits inside the enclosing `if user ==
			// nil` block rather than at the function's top level.
			if len(call[1]) < 2 {
				t.Errorf("%s %s calls user_suspended at the top level, so every login pays for the extra read", file, function)
			}
		}
	}
}
