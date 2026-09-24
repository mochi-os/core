// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
package main

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// oauth_start_request presents a ceremony's state at /start with a session,
// or none when the session is empty.
func oauth_start_request(state string, session string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/auth/oauth/github/start?state="+state, nil)
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	c.Params = gin.Params{{Key: "provider", Value: "github"}}
	web_oauth_start(c)
	return w
}

// oauth_consent_from_hop follows the hop /begin answered with the session
// given and returns the consent URL the provider would show, failing unless
// the hop sends the browser off the site.
func oauth_consent_from_hop(t *testing.T, hop string, session string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(hop)
	if err != nil || parsed.Host != "" || parsed.Path != "/_/auth/oauth/github/start" {
		t.Fatalf("begin answered %q, want the same-origin hop", hop)
	}
	w := oauth_start_request(parsed.Query().Get("state"), session)
	location, err := url.Parse(w.Header().Get("Location"))
	if w.Code != http.StatusFound || err != nil || location.Host != "github.com" {
		t.Fatalf("start with the session that began: %d %q, want a redirect to the provider", w.Code, w.Header().Get("Location"))
	}
	return location
}

// TestOauthLinkBeginsAtTheSameOriginHop: a link ceremony begins in the shell's
// sandboxed iframe, which may hand the top window only a same-origin URL, so
// /begin answers /start rather than the provider's URL; the hop sends the
// session that began the ceremony on to the consent the provider URL would
// have carried, leaving the row for the callback, and nobody else.
func TestOauthLinkBeginsAtTheSameOriginHop(t *testing.T) {
	oauth_binding_setup(t)
	sessions := db_open("db/sessions.db")
	link_session := login_create("u-link", "", "")
	other_session := login_create("u-other", "", "")

	proof := uid()
	sessions.exec("insert into reauthentication (id, user, session, methods, expires) values (?, 'u-link', ?, 'email', ?)", proof, link_session, now()+300)
	begun := oauth_begin_request(link_session, `{"link":true,"token":"`+proof+`","target":"/settings/user/accounts"}`)
	if begun.Code != http.StatusOK {
		t.Fatalf("begin link: %d", begun.Code)
	}
	var answer struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(begun.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	state, st := oauth_only_ceremony(t)
	if answer.URL != "/_/auth/oauth/github/start?state="+state {
		t.Fatalf("begin link answered %q, want the hop for ceremony %s", answer.URL, state)
	}

	// The wrong browser: no session, another user's session, a state that
	// names no ceremony. None reaches the provider, and the row survives for
	// the right browser.
	for _, wrong := range []struct{ name, state, session string }{
		{"no session", state, ""},
		{"another user's session", state, other_session},
		{"unknown state", "nonsense", link_session},
	} {
		w := oauth_start_request(wrong.state, wrong.session)
		if w.Code != http.StatusFound || !strings.Contains(w.Header().Get("Location"), "oauth_error=state_invalid") {
			t.Errorf("start with %s: %d %q, want state_invalid", wrong.name, w.Code, w.Header().Get("Location"))
		}
	}
	if rows, _ := sessions.rows("select 1 from ceremonies where type='oauth'"); len(rows) != 1 {
		t.Fatalf("refused hops left %d ceremonies, want the one", len(rows))
	}

	// The session that began it: sent to the provider with the ceremony's own
	// state and PKCE challenge, and the row is still there for the callback.
	consent := oauth_consent_from_hop(t, answer.URL, link_session)
	query := consent.Query()
	if query.Get("state") != state || query.Get("code_challenge") != oauth_challenge(st.Verifier) || query.Get("client_id") != "test-client" {
		t.Errorf("consent = %v, want the ceremony's state, challenge and client", query)
	}
	if !strings.Contains(query.Get("redirect_uri"), "/_/auth/oauth/github/callback") {
		t.Errorf("redirect_uri = %q", query.Get("redirect_uri"))
	}
	if rows, _ := sessions.rows("select 1 from ceremonies where type='oauth'"); len(rows) != 1 {
		t.Errorf("the hop consumed the ceremony; the callback needs it")
	}
	cb, _ := oauth_callback_context(state, &http.Cookie{Name: "session", Value: link_session})
	if _, link_user, ok := oauth_callback_ceremony(cb, "github", state); !ok || link_user != "u-link" {
		t.Errorf("callback after the hop: ok=%v user=%q", ok, link_user)
	}
}

// TestOauthHopOnlyForSessionBoundCeremonies: a web login runs top-window and
// a mobile ceremony runs in the system browser, neither holding a session the
// hop could check, so /begin answers them the provider's URL, and a login
// ceremony's state presented at /start is refused even with a session.
func TestOauthHopOnlyForSessionBoundCeremonies(t *testing.T) {
	oauth_binding_setup(t)
	sessions := db_open("db/sessions.db")
	link_session := login_create("u-link", "", "")

	for _, kind := range []struct{ name, body string }{
		{"web login", `{}`},
		{"mobile login", `{"mode":"mobile","scheme":"mochi","challenge":"` + strings.Repeat("c", 43) + `"}`},
	} {
		sessions.exec("delete from ceremonies")
		begun := oauth_begin_request("", kind.body)
		if begun.Code != http.StatusOK {
			t.Fatalf("%s begin: %d", kind.name, begun.Code)
		}
		var answer struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(begun.Body.Bytes(), &answer); err != nil {
			t.Fatal(err)
		}
		if parsed, err := url.Parse(answer.URL); err != nil || parsed.Host != "github.com" {
			t.Errorf("%s begin answered %q, want the provider's URL", kind.name, answer.URL)
		}
		state, _ := oauth_only_ceremony(t)
		w := oauth_start_request(state, link_session)
		if !strings.Contains(w.Header().Get("Location"), "oauth_error=state_invalid") {
			t.Errorf("start with a %s ceremony: %q, want state_invalid", kind.name, w.Header().Get("Location"))
		}
	}

	// A mobile link is bound by its verifier at /exchange, and its browser
	// holds no session: the provider's URL, not the hop.
	sessions.exec("delete from ceremonies")
	proof := uid()
	sessions.exec("insert into reauthentication (id, user, session, methods, expires) values (?, 'u-link', ?, 'email', ?)", proof, link_session, now()+300)
	begun := oauth_begin_request(link_session, `{"mode":"mobile","scheme":"mochi","challenge":"`+strings.Repeat("c", 43)+`","link":true,"token":"`+proof+`"}`)
	if begun.Code != http.StatusOK {
		t.Fatalf("mobile link begin: %d", begun.Code)
	}
	var answer struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(begun.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	if parsed, err := url.Parse(answer.URL); err != nil || parsed.Host != "github.com" {
		t.Errorf("mobile link begin answered %q, want the provider's URL", answer.URL)
	}
}
