// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// identity_token_setup builds the tables /_/identity touches and one user with
// an identity, and returns a minter for tokens of a given action binding.
func identity_token_setup(t *testing.T) func(action, entity, scopes string) string {
	create_test_users_db(t)

	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create table tokens (hash text primary key not null, user text not null, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0)")
	users.exec("insert into users (uid, username, methods) values ('u-token', 'owner@example.com', 'email')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-token', '', 'fp-token', 'u-token', 'person', 'Owner')")

	db_open("db/sessions.db").exec("create table accesses (hash text primary key not null, user text not null, used integer not null default 0)")

	mint := func(action, entity, scopes string) string {
		token := token_generate()
		users.exec("insert into tokens (hash, user, app, name, scopes, action, entity, created) values (?, 'u-token', 'feeds', 'test', ?, ?, ?, ?)",
			token_hash(token), scopes, action, entity, now())
		return token
	}
	return mint
}

// identity_with_token drives GET /_/identity carrying a Bearer token.
func identity_with_token(token string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/identity", nil)
	c.Request.Header.Set("Authorization", "Bearer "+token)
	web_identity_get(c)
	return w
}

// A bound token names one action on one entity and is minted to be handed out -
// an RSS token lives in the feed URL, its reader's server and every proxy log
// between. /_/identity must not answer it with the owner's email, status and
// identity.
func TestIdentityRefusesBoundToken(t *testing.T) {
	mint := identity_token_setup(t)

	// The exact shape apps/feeds mints for a feed's RSS URL.
	rss := mint(":feed/-/rss", "feed-123", `["rss"]`)
	w := identity_with_token(rss)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("bound RSS token: status = %d, want 401 (body %s)", w.Code, w.Body.String())
	}
	if body := w.Body.String(); strings.Contains(body, "owner@example.com") {
		t.Errorf("bound RSS token was answered with the owner's email: %s", body)
	}

	// The class-level variant, bound to an action but no entity - the shape
	// feeds, forums, wikis and notifications mint for a whole-app feed.
	class := mint("-/rss", "", `["rss"]`)
	if w := identity_with_token(class); w.Code != http.StatusUnauthorized {
		t.Errorf("action-bound token with no entity: status = %d, want 401", w.Code)
	}

	// A git credential is bound the same way and travels the same way.
	git := mint(":repository/git/*path", "", `["git"]`)
	if w := identity_with_token(git); w.Code != http.StatusUnauthorized {
		t.Errorf("bound git token: status = %d, want 401", w.Code)
	}
}

// TestIdentityAcceptsUnboundToken is the other half, and the reason the check
// is on the binding rather than on scopes: a user-created API token carries no
// action, and asking which account it belongs to is what it is for. Refusing
// it would break every general-purpose token.
func TestIdentityAcceptsUnboundToken(t *testing.T) {
	mint := identity_token_setup(t)

	for _, test := range []struct {
		name   string
		scopes string
	}{
		{"no scopes recorded", ""},
		{"empty scope list", `[]`},
		{"scoped but unbound", `["read"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := identity_with_token(mint("", "", test.scopes))
			if w.Code != http.StatusOK {
				t.Fatalf("unbound token: status = %d, want 200 (body %s)", w.Code, w.Body.String())
			}
			var body struct {
				User struct {
					Email string `json:"email"`
				} `json:"user"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("response: %v", err)
			}
			if body.User.Email != "owner@example.com" {
				t.Errorf("email = %q, want owner@example.com", body.User.Email)
			}
		})
	}
}

// TestTokenUnbound pins the rule itself, since it is the whole gate.
func TestTokenUnbound(t *testing.T) {
	if token_unbound(nil) {
		t.Error("a nil token is not unbound; it is no token at all")
	}
	if !token_unbound(&Token{}) {
		t.Error("a token with no action binding is unbound")
	}
	if token_unbound(&Token{Action: "-/rss"}) {
		t.Error("a token naming an action is bound")
	}
	// The entity alone does not decide it: the action is what a binding is,
	// and every app that binds sets the action whether or not it sets an
	// entity (feeds sets both, repositories sets only the action).
	if token_unbound(&Token{Action: ":feed/-/rss", Entity: "feed-1"}) {
		t.Error("a token naming an action and an entity is bound")
	}
}
