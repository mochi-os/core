// Mochi server: an app token is bound to the session that signed it.
//
// jwt_verify loaded the session row to get its signing secret and then returned
// the user named inside the TOKEN, never comparing it with s.User. A valid
// signature therefore proved only "signed with this session's secret", and
// web.go fed the returned uid straight to user_by_uid - so one session secret
// authenticated as any account a token it signed happened to name.
// auth_create_app_token had the same gap from the issuing side.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

// session_binding_env creates the sessions table and returns the cleanup.
func session_binding_env(t *testing.T) func() {
	t.Helper()
	cleanup := create_web_test_env(t)
	db := db_open("db/sessions.db")
	db.exec("create table if not exists sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	db.exec("create unique index if not exists sessions_code on sessions(code)")
	return cleanup
}

// session_binding_add inserts one live session.
func session_binding_add(user, code, secret string) {
	n := now()
	db_open("db/sessions.db").exec(
		"insert into sessions (user, code, secret, expires, created, address, agent) values (?, ?, ?, ?, ?, '127.0.0.1', 'test')",
		user, code, secret, n+86400, n)
}

// session_binding_forge signs a token naming whatever user it is told to,
// with whatever secret it is told to. This is what the issuer refuses to do
// and therefore the only way to reach jwt_verify's own check.
func session_binding_forge(user, app, kid, secret string) (string, error) {
	claims := mochi_claims{
		User: user,
		App:  app,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Unix(now()+jwt_expiry, 0)),
			IssuedAt:  jwt.NewNumericDate(time.Unix(now(), 0)),
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	t.Header["kid"] = kid
	return t.SignedString([]byte(secret))
}

// TestAppTokenRefusesAUserTheSessionDoesNotOwn is the issuing half of the
// defect: the session was looked up by code and its secret used, while the
// user came from the argument and was never checked against it.
func TestAppTokenRefusesAUserTheSessionDoesNotOwn(t *testing.T) {
	defer session_binding_env(t)()
	session_binding_add("attacker", "attacker-session", "attacker-secret-32-chars-abcdefgh")

	if token := auth_create_app_token("victim", "attacker-session", "feeds"); token != "" {
		t.Error("a session owned by one account minted a token naming another, so its secret signs for the whole user table")
	}
}

// TestAppTokenStillIssuesForTheSessionOwner keeps the check from being a
// blanket refusal - the only reason the endpoint exists is the matched pair.
func TestAppTokenStillIssuesForTheSessionOwner(t *testing.T) {
	defer session_binding_env(t)()
	session_binding_add("owner", "owner-session", "owner-secret-32-characters-abcd")

	token := auth_create_app_token("owner", "owner-session", "feeds")
	if token == "" {
		t.Fatal("the session's own user was refused a token")
	}
	user, app, err := jwt_verify(token)
	if err != nil {
		t.Fatalf("a token issued to the session's own user does not verify: %v", err)
	}
	if user != "owner" || app != "feeds" {
		t.Errorf("verified as user %q app %q, want owner/feeds", user, app)
	}
}

// TestJwtVerifyConfinesALeakedSecretToItsOwnAccount is the point of the whole
// change. Both sessions are live and each has its own secret; holding one of
// them must not authenticate as the other's owner.
func TestJwtVerifyConfinesALeakedSecretToItsOwnAccount(t *testing.T) {
	defer session_binding_env(t)()
	session_binding_add("attacker", "attacker-session", "attacker-secret-32-chars-abcdefgh")
	session_binding_add("victim", "victim-session", "victim-secret-32-characters-abc")

	forged, err := session_binding_forge("victim", "feeds", "attacker-session", "attacker-secret-32-chars-abcdefgh")
	if err != nil {
		t.Fatalf("signing the forged token: %v", err)
	}

	user, _, err := jwt_verify(forged)
	if err == nil {
		t.Errorf("a token naming %q, signed with the attacker's session secret, verified - the secret is a credential over every account", user)
	}
	if user != "" {
		t.Errorf("jwt_verify returned user %q alongside its error; web.go passes this to user_by_uid", user)
	}

	// The victim's own session is untouched by the check.
	genuine := auth_create_app_token("victim", "victim-session", "feeds")
	if genuine == "" {
		t.Fatal("the victim can no longer obtain a token for their own session")
	}
	if user, _, err := jwt_verify(genuine); err != nil || user != "victim" {
		t.Errorf("the victim's own token verified as %q, err %v", user, err)
	}
}

// TestJwtVerifyChecksTheSignatureBeforeTheBinding: a caller presenting a
// mismatched user with a bad signature must be told the signature is bad, not
// that the pair does not match. Otherwise the error distinguishes "this
// account exists on this session" for someone who cannot sign at all.
func TestJwtVerifyChecksTheSignatureBeforeTheBinding(t *testing.T) {
	defer session_binding_env(t)()
	session_binding_add("attacker", "attacker-session", "attacker-secret-32-chars-abcdefgh")

	forged, err := session_binding_forge("victim", "feeds", "attacker-session", "not-the-sessions-secret-at-all")
	if err != nil {
		t.Fatalf("signing the forged token: %v", err)
	}

	_, _, err = jwt_verify(forged)
	if err == nil {
		t.Fatal("a token signed with the wrong secret verified")
	}
	if strings.Contains(err.Error(), "does not match the session") {
		t.Errorf("an unsignable token was refused for the user mismatch rather than the signature (%v), which reports the binding to a caller who never held the secret", err)
	}
}
