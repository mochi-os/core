// Mochi server: which app a websocket connection is tagged with.
//
// Delivery is scoped by sending app, so the tag decides everything a socket
// can hear. The browser sends the session cookie on every same-origin
// handshake; when the cookie was read first, a top-window page's socket was
// tagged with no app however it presented its token, and no app's writes could
// reach it. A presented, valid token must therefore win the tagging.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func websocket_authenticate_attempt(t *testing.T, session string, query string, bearer string) (*User, string, bool) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/websocket"+query, nil)
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	if bearer != "" {
		c.Request.Header.Set("Authorization", "Bearer "+bearer)
	}
	return websocket_authenticate(c)
}

func TestWebsocketTokenTagsTheConnection(t *testing.T) {
	// One data directory for both databases: create_test_sessions_db would
	// mint a second TempDir and orphan the users database made by the first.
	create_test_users_db(t)
	sessions_schema := db_open("db/sessions.db")
	sessions_schema.exec("create table sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")

	users := db_open("db/users.db")
	// user_by_uid loads the user's identity entity alongside the row.
	users.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("insert into users (uid, username) values ('u1', 'one@example.com')")
	users.exec("insert into users (uid, username) values ('u2', 'two@example.com')")
	// user_by_uid refuses a user with no person entity.
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e1', '', 'fp1', 'u1', 'person', 'One')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e2', '', 'fp2', 'u2', 'person', 'Two')")
	sessions := db_open("db/sessions.db")
	sessions.exec("insert into sessions (user, code, secret, expires) values ('u1', 'login1', 'secret-one-1234567890', ?)", now()+3600)
	sessions.exec("insert into sessions (user, code, secret, expires) values ('u2', 'login2', 'secret-two-1234567890', ?)", now()+3600)

	own := auth_create_app_token("u1", "login1", "app-entity-1")
	other := auth_create_app_token("u2", "login2", "app-entity-2")
	if own == "" || other == "" {
		t.Fatal("could not mint test tokens")
	}

	// Cookie alone authenticates but tags no app.
	u, app, token_auth := websocket_authenticate_attempt(t, "login1", "?key=notifications", "")
	if u == nil || u.UID != "u1" || app != "" || token_auth {
		t.Errorf("cookie-only: got user %v app %q token_auth %v, want u1 with no app", u, app, token_auth)
	}

	// A token alone authenticates and tags, from the header or the query.
	u, app, token_auth = websocket_authenticate_attempt(t, "", "?key=notifications", own)
	if u == nil || u.UID != "u1" || app != "app-entity-1" || !token_auth {
		t.Errorf("bearer-only: got user %v app %q token_auth %v, want u1 tagged app-entity-1", u, app, token_auth)
	}
	u, app, token_auth = websocket_authenticate_attempt(t, "", "?key=notifications&token="+own, "")
	if u == nil || u.UID != "u1" || app != "app-entity-1" || !token_auth {
		t.Errorf("query-token-only: got user %v app %q token_auth %v, want u1 tagged app-entity-1", u, app, token_auth)
	}

	// The defect: cookie plus token. The token must tag the connection even
	// though the cookie authenticates - the browser always sends it.
	u, app, token_auth = websocket_authenticate_attempt(t, "login1", "?key=notifications&token="+own, "")
	if u == nil || u.UID != "u1" || app != "app-entity-1" || !token_auth {
		t.Errorf("cookie+token: got user %v app %q token_auth %v, want u1 tagged app-entity-1", u, app, token_auth)
	}

	// Another user's token is not adopted over the cookie's session.
	u, app, token_auth = websocket_authenticate_attempt(t, "login1", "?key=notifications&token="+other, "")
	if u == nil || u.UID != "u1" || app != "" || token_auth {
		t.Errorf("cookie+foreign token: got user %v app %q token_auth %v, want cookie's u1 with no app", u, app, token_auth)
	}

	// A garbage token falls back to the cookie rather than failing the
	// handshake outright.
	u, app, token_auth = websocket_authenticate_attempt(t, "login1", "?key=notifications&token=not-a-jwt", "")
	if u == nil || u.UID != "u1" || app != "" || token_auth {
		t.Errorf("cookie+garbage token: got user %v app %q token_auth %v, want cookie's u1 with no app", u, app, token_auth)
	}

	// Nothing at all stays unauthenticated.
	u, _, _ = websocket_authenticate_attempt(t, "", "?key=notifications", "")
	if u != nil {
		t.Errorf("no credentials: got user %v, want nil", u)
	}
}
