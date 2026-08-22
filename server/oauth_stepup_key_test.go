// Mochi server: an OAuth step-up proof is keyed on a server-generated id, not
// on the caller's challenge, so two accounts may hold the same challenge.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// stepup_env builds the users and sessions tables the step-up callback writes,
// plus two accounts each with the same provider linked.
func stepup_env(t *testing.T) {
	t.Helper()
	create_web_test_env(t)

	users := db_open("db/users.db")
	users.exec("drop table if exists users")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default '', disabled text not null default '', status text not null default 'active', restore_source text not null default '', restore_passkeys integer not null default 0, purge integer not null default 0)")
	users.exec("create table if not exists oauth (id integer primary key, user text not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")

	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists ceremonies (id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create table if not exists reauthentication (id text primary key, user text not null, methods text not null default '', expires integer not null)")
	sessions.exec("create table if not exists verifications (oauth integer primary key, user text not null, last integer not null default 0)")

	for _, account := range []struct{ uid, subject string }{
		{"u-attacker", "subject-attacker"},
		{"u-victim", "subject-victim"},
	} {
		users.exec("insert into users (uid, username) values (?, ?)", account.uid, account.uid+"@example.com")
		users.exec("insert into oauth (user, provider, subject, created) values (?, 'github', ?, ?)",
			account.uid, account.subject, now())
	}
}

// stepup_callback runs the real OAuth step-up callback for one account.
func stepup_callback(t *testing.T, uid, subject, challenge string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/_/auth/oauth/github/callback", nil)

	user := &User{UID: uid, Username: uid + "@example.com"}
	oauth_reauthenticate(c, "github", &oauth_profile{
		Subject: subject, Email: uid + "@example.com", Verified: true, Name: uid,
	}, user, challenge)
}

// stepup_challenge is what the client sends: base64url(sha256(verifier)).
func stepup_challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// stepup_proofs counts the stored proofs for one account.
func stepup_proofs(t *testing.T, uid string) int64 {
	t.Helper()
	row, err := db_open("db/sessions.db").row(
		"select count(*) as total from ceremonies where type='reauthentication_oauth' and user=?", uid)
	if err != nil || row == nil {
		t.Fatalf("counting proofs: %v", err)
	}
	total, _ := row["total"].(int64)
	return total
}

// TestASquattedChallengeDoesNotBlockAnotherUser is the defect. The attacker
// stores a proof under a challenge first; the victim's callback must still
// store theirs.
func TestASquattedChallengeDoesNotBlockAnotherUser(t *testing.T) {
	stepup_env(t)

	shared := stepup_challenge("the-victims-verifier")

	stepup_callback(t, "u-attacker", "subject-attacker", shared)
	if got := stepup_proofs(t, "u-attacker"); got != 1 {
		t.Fatalf("the attacker stored %d proofs, want 1; this test is not exercising the path it thinks it is", got)
	}

	// Under the original keying this is a primary-key violation, and db.exec
	// panics on one - so the failure was a 500 in the victim's popup, not a
	// quiet miss.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("the victim's callback panicked on a challenge another account had already used: %v", r)
		}
	}()
	stepup_callback(t, "u-victim", "subject-victim", shared)

	if got := stepup_proofs(t, "u-victim"); got != 1 {
		t.Errorf("the victim stored %d proofs, want 1; their step-up cannot complete", got)
	}
}

// TestTheChallengeIsStoredInItsOwnColumn: the row must be findable by the
// value the client will present, and the id must be the server's.
func TestTheChallengeIsStoredInItsOwnColumn(t *testing.T) {
	stepup_env(t)

	challenge := stepup_challenge("a-verifier")
	stepup_callback(t, "u-victim", "subject-victim", challenge)

	row, err := db_open("db/sessions.db").row(
		"select id, challenge from ceremonies where type='reauthentication_oauth' and user='u-victim'")
	if err != nil || row == nil {
		t.Fatalf("no proof row was stored: %v", err)
	}
	if stored := as_string(row["challenge"]); stored != challenge {
		t.Errorf("the challenge column holds %q, want the caller's %q; verify.finish looks the row up by this value", stored, challenge)
	}
	if id := as_string(row["id"]); id == challenge {
		t.Error("the primary key is still the caller's challenge, so one account can occupy another's")
	}
}

// TestEachUserReadsTheirOwnProof: two rows may share a challenge, so the user
// filter is the only thing keeping each account's proof separate.
func TestEachUserReadsTheirOwnProof(t *testing.T) {
	stepup_env(t)

	shared := stepup_challenge("the-shared-verifier")
	stepup_callback(t, "u-attacker", "subject-attacker", shared)
	stepup_callback(t, "u-victim", "subject-victim", shared)

	db := db_open("db/sessions.db")
	for _, uid := range []string{"u-attacker", "u-victim"} {
		row, err := db.row("select user from ceremonies where challenge=? and type='reauthentication_oauth' and user=? and expires>?",
			[]byte(shared), uid, now())
		if err != nil || row == nil {
			t.Fatalf("%s cannot find their own proof: %v", uid, err)
		}
		if owner := as_string(row["user"]); owner != uid {
			t.Errorf("%s read a proof belonging to %q", uid, owner)
		}
	}
}

// TestFinishScopesOnUserAndChallenge pins the read shape. A lookup on the
// challenge alone would now return whichever row came first.
func TestFinishScopesOnUserAndChallenge(t *testing.T) {
	body := function_body(t, "oauth.go", "func api_user_oauth_verify_finish(")

	if !strings.Contains(body, "where challenge=?") {
		t.Error("verify.finish no longer looks the proof up by the challenge column")
	}
	if !strings.Contains(body, "user=?") {
		t.Error("verify.finish does not scope its lookup to the calling user; two accounts may now hold the same challenge")
	}
	if strings.Contains(body, "where id=? and type='reauthentication_oauth'") {
		t.Error("verify.finish still treats the caller's challenge as the row id")
	}
	if strings.Contains(body, `delete from ceremonies where challenge=?`) {
		t.Error("verify.finish consumes by challenge, so one user's step-up deletes another's stored proof")
	}
	if !strings.Contains(body, `delete from ceremonies where id=?`) {
		t.Error("verify.finish does not delete the row it read by id, so the proof is not reliably single-use")
	}
}

func TestTwoAccountsCoexistUnderOneChallenge(t *testing.T) {
	stepup_env(t)

	shared := stepup_challenge("shared-verifier")
	stepup_callback(t, "u-attacker", "subject-attacker", shared)
	stepup_callback(t, "u-victim", "subject-victim", shared)

	row, err := db_open("db/sessions.db").row(
		"select count(*) as total from ceremonies where challenge=? and type='reauthentication_oauth'", []byte(shared))
	if err != nil || row == nil {
		t.Fatalf("counting rows for the shared challenge: %v", err)
	}
	if total, _ := row["total"].(int64); total != 2 {
		t.Errorf("%d rows share the challenge, want 2; the second account could not store its proof", total)
	}
}
