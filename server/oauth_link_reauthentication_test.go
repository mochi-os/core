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

// begin_link drives POST /_/auth/oauth/github/begin as the given session,
// returning the status code.
func begin_link(session string, body string) int {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/oauth/github/begin", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	c.Params = gin.Params{{Key: "provider", Value: "github"}}
	web_oauth_begin(c)
	return w.Code
}

// TestOauthLinkRequiresReauthentication covers the step-up gate on OAuth
// linking. Linking ADDS a way to sign in and outlives every later passphrase,
// passkey and TOTP change, so a session cookie alone must not be enough - the
// same rule the settings app already applies to passkey registration, TOTP
// setup, recovery regeneration and unlink. Before this gate a stolen session
// could attach an attacker-controlled provider as a durable backdoor.
func TestOauthLinkRequiresReauthentication(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()

	sessions := db_open("db/sessions.db")
	sessions.exec("create table sessions (user text not null, code text not null primary key, secret text not null, expires integer not null, created integer not null, accessed integer not null, address text not null default '', agent text not null default '')")
	sessions.exec("create table ceremonies (id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create table reauthentication (id text primary key, user text not null, methods text not null default '', expires integer not null)")
	db_open("db/settings.db").exec("create table settings (name text primary key, value text not null default '')")

	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("insert into users (uid, username, methods) values ('u-link', 'link@example.com', 'email')")

	// github has static endpoints, so beginning a ceremony needs no network.
	setting_set("oauth_github_client_id", "test-client")
	setting_set("oauth_github_client_secret", "test-secret")

	session := login_create("u-link", "", "")

	ceremonies := func() int {
		row, _ := sessions.row("select count(*) as n from ceremonies where type='oauth'")
		n, _ := row["n"].(int64)
		return int(n)
	}

	// A session with no proof is the stolen-cookie case: refused, and no
	// ceremony is left behind for the callback to complete.
	if code := begin_link(session, `{"link":true}`); code != http.StatusForbidden {
		t.Errorf("link with no proof: status = %d, want 403", code)
	}
	if n := ceremonies(); n != 0 {
		t.Errorf("link with no proof started %d ceremonies, want 0", n)
	}

	// A token that was never issued is not a proof either.
	if code := begin_link(session, `{"link":true,"token":"not-a-real-token"}`); code != http.StatusForbidden {
		t.Errorf("link with a forged proof: status = %d, want 403", code)
	}
	if n := ceremonies(); n != 0 {
		t.Errorf("link with a forged proof started %d ceremonies, want 0", n)
	}

	// A genuine proof lets the link through.
	proof := uid()
	sessions.exec("insert into reauthentication (id, user, methods, expires) values (?, 'u-link', 'email', ?)", proof, now()+300)
	if code := begin_link(session, `{"link":true,"token":"`+proof+`"}`); code != http.StatusOK {
		t.Errorf("link with a valid proof: status = %d, want 200", code)
	}
	if n := ceremonies(); n != 1 {
		t.Errorf("link with a valid proof started %d ceremonies, want 1", n)
	}
	row, _ := sessions.row("select user from ceremonies where type='oauth'")
	if u, _ := row["user"].(string); u != "u-link" {
		t.Errorf("ceremony user = %q, want u-link", u)
	}

	// The proof is spent: replaying it does not authorise a second link.
	if code := begin_link(session, `{"link":true,"token":"`+proof+`"}`); code != http.StatusForbidden {
		t.Errorf("replayed proof: status = %d, want 403", code)
	}
	if n := ceremonies(); n != 1 {
		t.Errorf("replayed proof started another ceremony (%d total)", n)
	}

	// A proof belonging to somebody else is not usable here.
	users.exec("insert into users (uid, username, methods) values ('u-other', 'other@example.com', 'email')")
	other := uid()
	sessions.exec("insert into reauthentication (id, user, methods, expires) values (?, 'u-other', 'email', ?)", other, now()+300)
	if code := begin_link(session, `{"link":true,"token":"`+other+`"}`); code != http.StatusForbidden {
		t.Errorf("another user's proof: status = %d, want 403", code)
	}

	// An expired proof is refused.
	stale := uid()
	sessions.exec("insert into reauthentication (id, user, methods, expires) values (?, 'u-link', 'email', ?)", stale, now()-1)
	if code := begin_link(session, `{"link":true,"token":"`+stale+`"}`); code != http.StatusForbidden {
		t.Errorf("expired proof: status = %d, want 403", code)
	}

	// Signing in with OAuth is not a credential change and stays ungated: the
	// caller is anonymous there, so requiring a proof would lock everyone out.
	sessions.exec("delete from ceremonies")
	if code := begin_link("", `{"link":false}`); code != http.StatusOK {
		t.Errorf("login flow: status = %d, want 200", code)
	}
	if n := ceremonies(); n != 1 {
		t.Errorf("login flow started %d ceremonies, want 1", n)
	}
}
