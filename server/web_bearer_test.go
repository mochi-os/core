// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Regression tests for the app-token gate in web_action.
//
// A non-public app action requires a Bearer token bound to that app: a session
// cookie is ambient authority over every app the user has installed, so the
// per-app boundary rests entirely on the token being app-specific.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// web_bearer_env builds the minimum a web_action auth decision needs: a user
// with a session, and an app carrying one non-public action.
func web_bearer_env(t *testing.T) (*App, *User, string) {
	t.Helper()
	create_web_test_env(t)

	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("create unique index if not exists sessions_code on sessions(code)")

	// user_by_login selects uid, username, role, methods, disabled and status,
	// so the table needs those columns or cookie authentication silently fails
	// and the request is refused before it ever reaches the token gate this
	// test is about.
	users := db_open("db/users.db")
	users.exec("drop table if exists users")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default '', disabled text not null default '', status text not null default 'active')")
	users.exec("insert into users (uid, username, role, status) values ('bearer-user', 'bearer@example.com', 'user', 'active')")

	// A person entity: web_action refuses an authenticated user with no identity
	// before the token check. identity() does a select * into the Entity struct,
	// so a differing column set fails the scan and the user reads as
	// identity-less.
	users.exec("drop table if exists entities")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'private', data text not null default '', published integer not null default 0)")
	identity := test_entity_id('b')
	users.exec("insert into entities (id, fingerprint, user, class, name, privacy) values (?, ?, 'bearer-user', 'person', 'Bearer Tester', 'private')",
		identity, fingerprint(identity))

	n := now()
	sessions.exec("insert into sessions (user, code, secret, expires, created, address, agent) values (?, ?, 'bearer-test-secret-0123456789', ?, ?, '127.0.0.1', 'test')",
		"bearer-user", "bearer-session", n+86400, n)

	version := &AppVersion{
		Actions: map[string]AppAction{
			// Non-public and function-backed: neither aa.Public nor
			// shell_static can excuse it from the token requirement.
			"-/private": {Function: "action_private"},
			// A public action skips the app-token block entirely, and still
			// runs AS the user any credential names.
			"-/open": {Function: "action_open", Public: true},
		},
	}
	app := &App{id: "bearertest", latest: version, versions: map[string]*AppVersion{"1.0": version}}
	version.app = app

	user := &User{UID: "bearer-user", Username: "bearer@example.com", Role: "user"}
	return app, user, "bearer-session"
}

// web_bearer_status runs one request through web_action and reports the status
// and body. Assert the gate's own refusal, not "not 200": a bypass surfaces as
// a 500 from this fixture's function-less action, which "not 200" would pass.
func web_bearer_status(t *testing.T, app *App, session string, authorization string) (int, string) {
	t.Helper()
	return web_bearer_method(t, app, session, authorization, "GET")
}

// web_bearer_method is the same for a chosen method, which is what separates a
// read-only token from an ordinary one.
func web_bearer_method(t *testing.T, app *App, session string, authorization string, method string) (int, string) {
	t.Helper()
	return web_bearer_action(t, app, session, authorization, method, "-/private")
}

// web_bearer_action is the same for a chosen action, so the public one can be
// driven too.
func web_bearer_action(t *testing.T, app *App, session string, authorization string, method string, action string) (int, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, "/bearertest/"+action, nil)
	c.Request.Header.Set("Accept", "application/json")
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	if authorization != "" {
		c.Request.Header.Set("Authorization", authorization)
	}
	web_action(c, app, action, nil, routing_class)
	return w.Code, w.Body.String()
}

func TestBearerGateRejectsUnverifiedToken(t *testing.T) {
	app, _, session := web_bearer_env(t)

	cases := []struct {
		name          string
		authorization string
	}{
		// The exact string that returned 200 against a live server.
		{"garbage", "Bearer notarealtoken"},
		// JWT-shaped but unsigned by us: proves the check is verification,
		// not a guess at the token's format.
		{"malformed JWT", "Bearer aaa.bbb.ccc"},
		// An empty token is still a "Bearer " prefix.
		{"empty", "Bearer "},
	}

	for _, tt := range cases {
		status, body := web_bearer_status(t, app, session, tt.authorization)
		if status != 403 || !strings.Contains(body, "app_token_required") {
			t.Errorf("%s: session cookie plus %q got %d %s; want 403 app_token_required - an unverified token must not satisfy the app-token gate",
				tt.name, tt.authorization, status, body)
		}
	}
}

func TestBearerGateRejectsCookieAlone(t *testing.T) {
	app, _, session := web_bearer_env(t)

	status, body := web_bearer_status(t, app, session, "")
	if status != 403 || !strings.Contains(body, "app_token_required") {
		t.Errorf("session cookie alone got %d %s; want 403 app_token_required - the cookie is ambient over every installed app", status, body)
	}
}

// The asset token is the one that travels in an image or attachment URL, so a
// copied link hands it over. It must read and nothing else, whether it arrives
// in the query string or, as an app's own fetch would send it, in the header.
func TestBearerGateHoldsAnAssetTokenToReads(t *testing.T) {
	app, _, session := web_bearer_env(t)

	asset := auth_create_asset_token("bearer-user", session, app.id)
	ordinary := auth_create_app_token("bearer-user", session, app.id)
	if asset == "" || ordinary == "" {
		t.Fatal("could not mint the tokens")
	}

	// A refusal here is the gate's own. The fixture's action names a function
	// that does not exist, so a request that PASSES the gate ends in a 500 -
	// which is what distinguishes "allowed through" from "refused".
	status, body := web_bearer_method(t, app, session, "Bearer "+asset, "GET")
	if status == 403 {
		t.Errorf("asset token on a read got 403 %s; a read is the whole point of it", body)
	}

	status, body = web_bearer_method(t, app, session, "Bearer "+asset, "POST")
	if status != 403 || !strings.Contains(body, "app_token_read_only") {
		t.Errorf("asset token on a write got %d %s; want 403 app_token_read_only", status, body)
	}

	// Control: the same write with the ordinary app token is not refused, so
	// the refusal above measures the purpose and not the method.
	status, body = web_bearer_method(t, app, session, "Bearer "+ordinary, "POST")
	if status == 403 {
		t.Errorf("app token on a write got 403 %s; only the asset token is read-only", body)
	}

	// And on a PUBLIC action, which skips the whole app-token block: it still
	// runs as whoever the credential names, so a leaked asset token would post
	// as them. Found live - the check sat inside that block at first, and a
	// POST carrying an asset token answered 200.
	status, body = web_bearer_action(t, app, session, "Bearer "+asset, "POST", "-/open")
	if status != 403 || !strings.Contains(body, "app_token_read_only") {
		t.Errorf("asset token writing to a public action got %d %s; want 403 app_token_read_only", status, body)
	}
	status, body = web_bearer_action(t, app, session, "Bearer "+ordinary, "POST", "-/open")
	if status == 403 {
		t.Errorf("app token on the public action got 403 %s; the refusal must be about the purpose", body)
	}
}
