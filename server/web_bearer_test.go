// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Regression tests for the app-token gate in web_action.
//
// A non-public app action requires a Bearer token bound to that app; the
// session cookie alone is not enough, because a cookie is ambient authority
// over every app the user has installed and the whole per-app boundary rests
// on the token being app-specific.
//
// The gate was defeatable twice over. has_bearer was set from the "Bearer "
// prefix before the token was validated, so the literal string "Bearer x"
// satisfied it; and an unverified token leaves the app claim empty, which the
// app match then skipped as having nothing to compare. Together a session
// cookie plus any nonsense header reached every app's actions - confirmed
// against a running server, where the cookie alone answered 403 and the cookie
// with a garbage token answered 200.

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
func web_bearer_env(t *testing.T) (*App, *User, string, func()) {
	t.Helper()
	cleanup := create_web_test_env(t)

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

	// A person entity, because web_action refuses an authenticated user with
	// no identity before it reaches the token check. Columns follow the
	// entities table create_web_test_env already made; user carries the uid
	// because that is what User.identity() matches on.
	// Recreated to match the Entity struct: identity() does a select * and
	// scans into it, so a column set that differs from the struct fails the
	// scan and the user reads as having no identity.
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
		},
	}
	app := &App{id: "bearertest", latest: version, versions: map[string]*AppVersion{"1.0": version}}
	version.app = app

	user := &User{UID: "bearer-user", Username: "bearer@example.com", Role: "user"}
	return app, user, "bearer-session", cleanup
}

// web_bearer_status runs one request through web_action and reports the status
// and body the auth decision produced.
//
// The assertions below check for the gate's own refusal rather than "not 200".
// A bypass here does not surface as a 200: it surfaces as the request reaching
// the handler, which for this fixture's function-less action is a 500. Testing
// for "not 200" would pass against the very bug this file exists for.
func web_bearer_status(t *testing.T, app *App, session string, authorization string) (int, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/bearertest/-/private", nil)
	c.Request.Header.Set("Accept", "application/json")
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	if authorization != "" {
		c.Request.Header.Set("Authorization", authorization)
	}
	web_action(c, app, "-/private", nil, routing_class)
	return w.Code, w.Body.String()
}

func TestBearerGateRejectsUnverifiedToken(t *testing.T) {
	app, _, session, cleanup := web_bearer_env(t)
	defer cleanup()

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
	app, _, session, cleanup := web_bearer_env(t)
	defer cleanup()

	status, body := web_bearer_status(t, app, session, "")
	if status != 403 || !strings.Contains(body, "app_token_required") {
		t.Errorf("session cookie alone got %d %s; want 403 app_token_required - the cookie is ambient over every installed app", status, body)
	}
}
