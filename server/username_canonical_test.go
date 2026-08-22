// Mochi server: one mailbox is one account.
//
// email_valid is mail.ParseAddress, which accepts "Alice <a@b.com>", " a@b.com
// " and "A@B.com" for one mailbox. The unique index on users.username does not
// merge them: SQLite compares TEXT with BINARY collation, so they are two rows.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// username_variants are the spellings of one mailbox that mail.ParseAddress
// accepts. Each must reach the same account as the plain form.
var username_variants = []string{
	"Alice <alice@example.com>",
	" alice@example.com ",
	"Alice@Example.com",
	"ALICE@EXAMPLE.COM",
	"<alice@example.com>",
}

// username_test_setup gives a test its own users.db holding one account, and
// the sessions.db table code_send writes to.
func username_test_setup(t *testing.T) {
	t.Helper()
	create_test_users_db(t)
	db_open("db/users.db").exec("insert into users (uid, username) values ('u1', 'alice@example.com')")
	db_open("db/sessions.db").exec(
		"create table if not exists codes ( code text not null primary key, username text not null, expires integer not null )")
}

// TestUserByUsernameFindsEveryVariant. The lookup is what decides whether a
// login continues into an existing account or falls through to signup.
func TestUserByUsernameFindsEveryVariant(t *testing.T) {
	username_test_setup(t)

	for _, variant := range username_variants {
		u := user_by_username(variant)
		if u == nil {
			t.Errorf("user_by_username(%q) found nobody; it is the same mailbox as alice@example.com, so the login falls through to signup", variant)
			continue
		}
		if u.UID != "u1" {
			t.Errorf("user_by_username(%q) returned uid %q, want u1", variant, u.UID)
		}
	}
}

// TestUserByUsernameRejectsUnparseable. Canonicalising must not turn a
// nonsense argument into a match on some other account.
func TestUserByUsernameRejectsUnparseable(t *testing.T) {
	username_test_setup(t)

	for _, value := range []string{"", "   ", "not an address", "@example.com"} {
		if u := user_by_username(value); u != nil {
			t.Errorf("user_by_username(%q) returned uid %q, want nobody", value, u.UID)
		}
	}
}

// TestCodeSendStoresCanonicalUsername is the defect. The codes row's username
// becomes the account's username when the code is redeemed, so storing the
// spelling the caller typed is what creates the second account.
func TestCodeSendStoresCanonicalUsername(t *testing.T) {
	username_test_setup(t)

	for _, variant := range username_variants {
		db_open("db/sessions.db").exec("delete from codes")
		// One bucket for the whole loop, since the limiter is already keyed on
		// the canonical address.
		rate_limit_code.reset("alice@example.com")
		if reason := code_send(variant, nil); reason != "" {
			t.Errorf("code_send(%q) refused: %q", variant, reason)
			continue
		}
		row, _ := db_open("db/sessions.db").row("select username from codes limit 1")
		if row == nil {
			t.Errorf("code_send(%q) wrote no code row", variant)
			continue
		}
		if stored := row["username"].(string); stored != "alice@example.com" {
			t.Errorf("code_send(%q) stored username %q; redeeming that code creates a second account for the one mailbox", variant, stored)
		}
	}
}

// TestCodeSendRefusesUnparseable. The canonical form is also the validation,
// so an address that cannot be reduced must not reach the codes table.
func TestCodeSendRefusesUnparseable(t *testing.T) {
	username_test_setup(t)

	if reason := code_send("not an address", nil); reason != "invalid_email" {
		t.Errorf("code_send on an unparseable address returned %q, want invalid_email", reason)
	}
	if count := db_open("db/users.db").integer("select count(*) from codes"); count != 0 {
		t.Errorf("%d code row(s) written for an unparseable address", count)
	}
}

// TestCodeConsumeMatchesAcrossSpellings. Restore issues its code through
// code_send and consumes it here; if the two canonicalise differently, a code
// the user genuinely received is refused.
func TestCodeConsumeMatchesAcrossSpellings(t *testing.T) {
	username_test_setup(t)

	for _, variant := range username_variants {
		db_open("db/sessions.db").exec("delete from codes")
		rate_limit_code.reset("alice@example.com")
		if reason := code_send("alice@example.com", nil); reason != "" {
			t.Fatalf("code_send refused: %q", reason)
		}
		row, _ := db_open("db/sessions.db").row("select code from codes limit 1")
		code := row["code"].(string)

		if !code_consume_email(variant, code) {
			t.Errorf("code_consume_email(%q, ...) refused a code issued to the same mailbox", variant)
		}
	}
}

// TestUserCreateStoresCanonicalUsername. The last gate before an address
// becomes an account, and the one every signup route shares - email code,
// OAuth and restore all arrive here.
func TestUserCreateStoresCanonicalUsername(t *testing.T) {
	username_test_setup(t)

	user, reason := user_create("Bob <BOB@Example.com>")
	if user == nil {
		t.Fatalf("user_create refused: %q", reason)
	}
	if user.Username != "bob@example.com" {
		t.Errorf("user_create stored username %q, want bob@example.com", user.Username)
	}

}

// TestLoginCodeVariantSignsIntoExistingAccount is the defect end to end, over
// the path a user actually walks: ask for a code with the address spelled a
// second way, redeem it, and land in the account that already exists rather
// than a new empty one.
func TestLoginCodeVariantSignsIntoExistingAccount(t *testing.T) {
	username_test_setup(t)

	for _, variant := range username_variants {
		db_open("db/sessions.db").exec("delete from codes")
		rate_limit_code.reset("alice@example.com")
		if reason := code_send(variant, nil); reason != "" {
			t.Fatalf("code_send(%q) refused: %q", variant, reason)
		}
		row, _ := db_open("db/sessions.db").row("select code from codes limit 1")
		if row == nil {
			t.Fatalf("code_send(%q) wrote no code row", variant)
		}

		user, reason := user_from_code(row["code"].(string))
		if user == nil {
			t.Errorf("redeeming a code sent to %q returned %q", variant, reason)
			continue
		}
		if user.UID != "u1" {
			t.Errorf("redeeming a code sent to %q signed in as uid %q, want u1: the user got a second, empty account", variant, user.UID)
		}
		if count := db_open("db/users.db").integer("select count(*) from users"); count != 1 {
			t.Errorf("%d accounts exist after a login as %q, want 1", count, variant)
		}
	}
}

// TestUserCreateRejectsUnparseable. Refusing beats inserting an account whose
// username no lookup can ever canonicalise to.
func TestUserCreateRejectsUnparseable(t *testing.T) {
	username_test_setup(t)

	if user, reason := user_create("not an address"); user != nil || reason != "invalid" {
		t.Errorf("user_create on an unparseable address returned (%v, %q), want (nil, invalid)", user, reason)
	}
}

// TestSchemaNineCanonicalisesExistingUsernames. Rows written before the login
// paths canonicalised would become unreachable once the lookups do, so the
// migration has to bring them along.
func TestSchemaNineCanonicalisesExistingUsernames(t *testing.T) {
	create_test_users_db(t)
	users := db_open("db/users.db")
	users.exec("create table if not exists settings ( name text not null primary key, value text not null )")
	users.exec("insert into users (uid, username) values ('u1', 'Carol <CAROL@Example.com>')")
	users.exec("insert into users (uid, username) values ('u2', 'Dave@Example.com')")
	users.exec("insert into users (uid, username) values ('u3', 'erin@example.com')")

	db_upgrade_9()

	for id, want := range map[string]string{"u1": "carol@example.com", "u2": "dave@example.com", "u3": "erin@example.com"} {
		row, _ := users.row("select username from users where uid=?", id)
		if row == nil {
			t.Errorf("%s vanished", id)
			continue
		}
		if got := row["username"].(string); got != want {
			t.Errorf("%s username is %q after the migration, want %q", id, got, want)
		}
	}
}

// TestSchemaNineLeavesCollidingAccountsAlone. Two rows reducing to one address
// are two accounts, each belonging to whoever completed that signup. Rewriting
// either would either fail the unique index or, worse, hand one person the
// other's account.
func TestSchemaNineLeavesCollidingAccountsAlone(t *testing.T) {
	create_test_users_db(t)
	users := db_open("db/users.db")
	users.exec("create table if not exists settings ( name text not null primary key, value text not null )")
	users.exec("insert into users (uid, username) values ('u1', 'frank@example.com')")
	users.exec("insert into users (uid, username) values ('u2', 'Frank@Example.com')")

	db_upgrade_9()

	for id, want := range map[string]string{"u1": "frank@example.com", "u2": "Frank@Example.com"} {
		row, _ := users.row("select username from users where uid=?", id)
		if row == nil {
			t.Fatalf("%s vanished; the migration must never remove an account", id)
		}
		if got := row["username"].(string); got != want {
			t.Errorf("%s username is %q, want %q left untouched", id, got, want)
		}
	}
}

// TestApiUserCreateStoresCanonicalUsername. mochi.user.create is an
// administrator route into the same table, so it needs the same reduction or
// it reintroduces the split from the Starlark side.
func TestApiUserCreateStoresCanonicalUsername(t *testing.T) {
	username_test_setup(t)

	// An internal app, so require_permission's users/write gate (which reads
	// grants from a database this fixture does not build) is not what the test
	// ends up measuring.
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: "u1", Username: "alice@example.com", Role: "administrator"})
	thread.SetLocal("app", &App{id: "settings", internal: &AppVersion{}})

	_, err := api_user_create(thread, sl.NewBuiltin("mochi.user.create", nil),
		sl.Tuple{sl.String("Grace <GRACE@Example.com>")}, nil)
	if err != nil {
		t.Fatalf("api_user_create: %v", err)
	}
	if exists, _ := db_open("db/users.db").exists("select 1 from users where username=?", "grace@example.com"); !exists {
		row, _ := db_open("db/users.db").row("select username from users where uid<>'u1' limit 1")
		t.Errorf("mochi.user.create stored %v, want grace@example.com", row)
	}
}

// TestApiUserGetFindsEveryVariant. mochi.user.get.username is how an app looks an
// account up by address; a miss there reads as "no such user".
func TestApiUserGetUsernameFindsEveryVariant(t *testing.T) {
	username_test_setup(t)

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: "u1", Username: "alice@example.com", Role: "administrator"})
	thread.SetLocal("app", &App{id: "settings", internal: &AppVersion{}})

	for _, variant := range username_variants {
		value, err := api_user_get_username(thread, sl.NewBuiltin("mochi.user.get.username", nil), sl.Tuple{sl.String(variant)}, nil)
		if err != nil {
			t.Errorf("mochi.user.get.username(%q): %v", variant, err)
			continue
		}
		if value == sl.None {
			t.Errorf("mochi.user.get.username(%q) found nobody; it is the same mailbox as alice@example.com", variant)
		}
	}
}

// username_canonical_sources are the files whose username handling this change
// covers. A site added later that binds a caller's spelling straight into a
// users query, or compares one to a stored username, puts the split back.
var username_canonical_sources = []string{
	"users.go", "authentication.go", "oauth.go", "auth_restore.go", "web.go",
}

// TestUsernameQueriesAreCanonical is the gate: every query keyed on
// users.username must bind a value that has been through email_address, and
// every comparison against a stored username must reduce the other side.
func TestUsernameQueriesAreCanonical(t *testing.T) {
	// Bare identifiers that are already canonical where they are bound: a value
	// read back out of the users table, or one the enclosing function reduced
	// at entry. Anything else has to be wrapped in email_address at the call.
	reduced := map[string]bool{
		"address":    true, // code_send, reduced at entry
		"username":   true, // reduced by the enclosing function
		"canonical":  true, // db_upgrade_9
		"email":      true, // auth_restore and code_consume_email, reduced at entry
		"c.Username": true, // read back out of the codes row
	}

	pattern := regexp.MustCompile(`users where username=\?",(.*)$`)
	argument := regexp.MustCompile(`^[^,)]*`)
	for _, name := range username_canonical_sources {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		for _, line := range strings.Split(string(body), "\n") {
			match := pattern.FindStringSubmatch(line)
			if match == nil {
				continue
			}
			bound := strings.TrimSpace(match[1])
			if strings.HasPrefix(bound, "email_address(") {
				continue
			}
			if reduced[argument.FindString(bound)] {
				continue
			}
			t.Errorf("%s: a users lookup binds %q, which nothing reduces with email_address; "+
				"one mailbox spelled two ways becomes two accounts", name, bound)
		}
	}

	// The one comparison rather than query: OAuth started from the email-entry
	// login flow checks the typed address against the linked account, and a
	// raw comparison there refuses a legitimate login as an account mismatch.
	oauth, err := os.ReadFile("oauth.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(oauth), "u.Username == email_address(expect_email)") {
		t.Error("oauth.go compares a stored username against the typed address without reducing it; " +
			"a user who typed their address a second way is refused with oauth_account_mismatch")
	}
}

// Restore's "already taken" check is what stops a second account for an address
// that has one, and on an empty server restore can also make that account an
// administrator. The taken check runs before the code check, so no code is
// needed.
func TestRestoreSeesAnExistingAccountAcrossSpellings(t *testing.T) {
	create_test_users_db(t)
	restore_tables_create(t)
	db_open("db/users.db").exec("insert into users (uid, username) values ('u1', 'frank@example.com')")

	for _, variant := range []string{"Frank <frank@example.com>", "Frank@Example.com", " frank@example.com "} {
		status, reason, _ := restore_post(t, variant, "")
		if status != http.StatusConflict || reason != "username_taken" {
			t.Errorf("restore as %q: got %d/%q, want 409/username_taken; it is the same mailbox as the existing account",
				variant, status, reason)
		}
	}
}
