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

// oauth_binding_setup creates the tables the OAuth begin and callback handlers
// touch, two users, and a configured github provider. github's endpoints are
// static, so no ceremony here needs the network.
func oauth_binding_setup(t *testing.T) func() {
	cleanup := create_test_users_db(t)

	sessions := db_open("db/sessions.db")
	sessions.exec("create table sessions (user text not null, code text not null primary key, secret text not null, expires integer not null, created integer not null, accessed integer not null, address text not null default '', agent text not null default '')")
	sessions.exec("create table ceremonies (id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null)")
	sessions.exec("create table reauthentication (id text primary key, user text not null, methods text not null default '', expires integer not null)")
	db_open("db/settings.db").exec("create table settings (name text primary key, value text not null default '')")

	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("insert into users (uid, username, methods) values ('u-link', 'link@example.com', 'email')")
	users.exec("insert into users (uid, username, methods) values ('u-other', 'other@example.com', 'email')")

	setting_set("oauth_github_client_id", "test-client")
	setting_set("oauth_github_client_secret", "test-secret")
	return cleanup
}

// oauth_begin_request drives POST /_/auth/oauth/github/begin and returns the
// recorder, so the caller can read both the body and any cookie set.
func oauth_begin_request(session string, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/oauth/github/begin", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	c.Params = gin.Params{{Key: "provider", Value: "github"}}
	web_oauth_begin(c)
	return w
}

// oauth_binding_from_response returns the binding cookie a /begin response
// set, or nil.
func oauth_binding_from_response(w *httptest.ResponseRecorder) *http.Cookie {
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == oauth_binding_cookie {
			return cookie
		}
	}
	return nil
}

// oauth_only_ceremony returns the id and stored state of the single oauth
// ceremony row, failing the test if there is not exactly one.
func oauth_only_ceremony(t *testing.T) (string, oauth_state) {
	t.Helper()
	rows, _ := db_open("db/sessions.db").rows("select id, data from ceremonies where type='oauth'")
	if len(rows) != 1 {
		t.Fatalf("ceremony rows = %d, want 1", len(rows))
	}
	var st oauth_state
	if err := json.Unmarshal([]byte(rows[0]["data"].(string)), &st); err != nil {
		t.Fatalf("ceremony data: %v", err)
	}
	return rows[0]["id"].(string), st
}

// oauth_callback_context builds a callback GET carrying the given cookies.
func oauth_callback_context(state string, cookies ...*http.Cookie) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/auth/oauth/github/callback?state="+state+"&code=provider-code", nil)
	for _, cookie := range cookies {
		if cookie != nil {
			c.Request.AddCookie(cookie)
		}
	}
	c.Params = gin.Params{{Key: "provider", Value: "github"}}
	return c, w
}

// TestOauthLoginCeremonyBoundToBrowser covers the login-CSRF gap: before the
// binding, the callback accepted any browser that presented a live state, so
// an attacker who completed the provider consent themselves and captured the
// callback URL could have a victim open it and be signed in to the attacker's
// account. Now /begin sets a cookie whose value is stored in the ceremony, and
// the callback requires it back.
func TestOauthLoginCeremonyBoundToBrowser(t *testing.T) {
	defer oauth_binding_setup(t)()
	sessions := db_open("db/sessions.db")

	// /begin sets the cookie and stores its value in the row.
	w := oauth_begin_request("", `{"link":false}`)
	if w.Code != http.StatusOK {
		t.Fatalf("begin: status = %d, want 200", w.Code)
	}
	cookie := oauth_binding_from_response(w)
	if cookie == nil {
		t.Fatal("begin set no binding cookie")
	}
	if cookie.Value == "" || !cookie.HttpOnly || cookie.Path != oauth_binding_path || cookie.SameSite != http.SameSiteLaxMode {
		t.Errorf("binding cookie = %+v; want a non-empty HttpOnly Lax cookie on %s", cookie, oauth_binding_path)
	}
	state, st := oauth_only_ceremony(t)
	if st.Binding == "" || st.Binding != cookie.Value {
		t.Errorf("row binding = %q, cookie = %q; want equal and non-empty", st.Binding, cookie.Value)
	}

	// A callback from a browser without the cookie is refused, and the
	// ceremony is consumed so it cannot be retried in the right browser.
	c, w := oauth_callback_context(state)
	web_oauth_callback(c)
	if w.Code != http.StatusFound || !strings.Contains(w.Header().Get("Location"), "oauth_error=state_invalid") {
		t.Errorf("callback without cookie: status = %d, location = %q; want 302 to state_invalid", w.Code, w.Header().Get("Location"))
	}
	if n, _ := sessions.rows("select 1 from ceremonies where type='oauth'"); len(n) != 0 {
		t.Errorf("callback without cookie left %d ceremonies, want 0 (consumed)", len(n))
	}

	// A callback with a different browser's cookie value is refused too.
	oauth_begin_request("", `{"link":false}`)
	state, _ = oauth_only_ceremony(t)
	c, w = oauth_callback_context(state, &http.Cookie{Name: oauth_binding_cookie, Value: "not-the-value-that-was-set"})
	web_oauth_callback(c)
	if w.Code != http.StatusFound || !strings.Contains(w.Header().Get("Location"), "oauth_error=state_invalid") {
		t.Errorf("callback with wrong cookie: status = %d, location = %q; want 302 to state_invalid", w.Code, w.Header().Get("Location"))
	}

	// The browser that began the ceremony passes the binding check, and the
	// cookie is cleared on the way through. The full handler would go on to
	// exchange the code with the provider, so only the resolution step runs.
	w = oauth_begin_request("", `{"link":false}`)
	cookie = oauth_binding_from_response(w)
	state, _ = oauth_only_ceremony(t)
	c, w = oauth_callback_context(state, cookie)
	resolved, link_user, ok := oauth_callback_ceremony(c, "github", state)
	if !ok || resolved == nil {
		t.Fatalf("callback with the right cookie: rejected (location %q)", w.Header().Get("Location"))
	}
	if link_user != "" {
		t.Errorf("login ceremony resolved link_user = %q, want empty", link_user)
	}
	if resolved.Binding != cookie.Value {
		t.Errorf("resolved binding = %q, want %q", resolved.Binding, cookie.Value)
	}
	cleared := oauth_binding_from_response(w)
	if cleared == nil || cleared.Value != "" || cleared.MaxAge >= 0 {
		t.Errorf("binding cookie after callback = %+v; want cleared", cleared)
	}
	if n, _ := sessions.rows("select 1 from ceremonies where type='oauth'"); len(n) != 0 {
		t.Errorf("accepted callback left %d ceremonies, want 0 (consumed)", len(n))
	}
}

// TestOauthLinkCeremonyBoundToSession covers the link-hijack variant of the
// same gap: a link ceremony names the account it will attach the provider
// identity to, so a victim tricked into completing it in their own browser
// would hand their provider identity to that account. The callback must arrive
// with the session of the user who authorised the link. The settings app
// begins the link from the sandboxed shell iframe, whose responses cannot set
// a cookie, so the session - which the top-level callback navigation does
// carry - is the binding here rather than the login cookie.
func TestOauthLinkCeremonyBoundToSession(t *testing.T) {
	defer oauth_binding_setup(t)()
	sessions := db_open("db/sessions.db")

	link_session := login_create("u-link", "", "")
	other_session := login_create("u-other", "", "")

	begin := func() (string, *http.Cookie) {
		t.Helper()
		proof := uid()
		sessions.exec("insert into reauthentication (id, user, methods, expires) values (?, 'u-link', 'email', ?)", proof, now()+300)
		w := oauth_begin_request(link_session, `{"link":true,"token":"`+proof+`"}`)
		if w.Code != http.StatusOK {
			t.Fatalf("begin link: status = %d, want 200", w.Code)
		}
		state, st := oauth_only_ceremony(t)
		if st.Binding != "" {
			t.Errorf("link ceremony carries a browser binding %q; the session is its binding", st.Binding)
		}
		return state, oauth_binding_from_response(w)
	}

	// No session at the callback: refused and consumed.
	state, cookie := begin()
	if cookie != nil {
		t.Errorf("begin link set a binding cookie; it cannot reach the sandboxed caller and is not the binding")
	}
	c, w := oauth_callback_context(state)
	if _, _, ok := oauth_callback_ceremony(c, "github", state); ok {
		t.Error("link callback with no session was accepted")
	}
	if !strings.Contains(w.Header().Get("Location"), "oauth_error=state_invalid") {
		t.Errorf("link callback with no session: location = %q, want state_invalid", w.Header().Get("Location"))
	}
	if n, _ := sessions.rows("select 1 from ceremonies where type='oauth'"); len(n) != 0 {
		t.Errorf("refused link callback left %d ceremonies, want 0", len(n))
	}

	// Somebody else's session: refused. This is the victim's browser.
	state, _ = begin()
	c, _ = oauth_callback_context(state, &http.Cookie{Name: "session", Value: other_session})
	if _, _, ok := oauth_callback_ceremony(c, "github", state); ok {
		t.Error("link callback with another user's session was accepted")
	}

	// The authorising user's session: accepted, and the row's user comes back
	// for oauth_link to attach the identity to.
	state, _ = begin()
	c, _ = oauth_callback_context(state, &http.Cookie{Name: "session", Value: link_session})
	_, link_user, ok := oauth_callback_ceremony(c, "github", state)
	if !ok {
		t.Fatal("link callback with the authorising session was rejected")
	}
	if link_user != "u-link" {
		t.Errorf("link_user = %q, want u-link", link_user)
	}
}

// TestOauthMobileCeremonyNotBrowserBound: the app calls /begin from its own
// HTTP client and the provider opens in a system browser, so no cookie can tie
// the two - and none is needed, because the result is released only at
// /exchange to the holder of the PKCE verifier. The callback must therefore
// pass with no cookie and no session.
func TestOauthMobileCeremonyNotBrowserBound(t *testing.T) {
	defer oauth_binding_setup(t)()

	w := oauth_begin_request("", `{"mode":"mobile","scheme":"mochi","challenge":"`+strings.Repeat("c", 43)+`"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("begin mobile: status = %d, want 200", w.Code)
	}
	if cookie := oauth_binding_from_response(w); cookie != nil {
		t.Errorf("mobile begin set a binding cookie %+v; the browser is not the party being bound", cookie)
	}
	state, st := oauth_only_ceremony(t)
	if st.Binding != "" {
		t.Errorf("mobile ceremony carries a browser binding %q", st.Binding)
	}
	c, _ := oauth_callback_context(state)
	resolved, _, ok := oauth_callback_ceremony(c, "github", state)
	if !ok || resolved == nil || resolved.Mode != "mobile" {
		t.Errorf("mobile callback with no cookie or session was rejected")
	}
}

// TestOauthStepupCeremonyNotSessionBound: the step-up ceremony is bound to the
// account being re-proved (oauth_reauthenticate accepts only a provider
// identity already linked to it, and releases the proof to that user alone),
// and Android completes it in a browser that holds no server session - so the
// callback must pass without one.
func TestOauthStepupCeremonyNotSessionBound(t *testing.T) {
	defer oauth_binding_setup(t)()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/settings/-/user/account/oauth/verify/begin", nil)
	provider := oauth_providers()["github"]
	if _, _, err := oauth_begin_ceremony(c, provider, "github", "u-link", "", "reauthentication", "", "stepup-challenge", ""); err != nil {
		t.Fatalf("begin step-up: %v", err)
	}
	if cookie := oauth_binding_from_response(w); cookie != nil {
		t.Errorf("step-up begin set a binding cookie %+v", cookie)
	}
	state, st := oauth_only_ceremony(t)
	if st.Binding != "" {
		t.Errorf("step-up ceremony carries a browser binding %q", st.Binding)
	}
	c, _ = oauth_callback_context(state)
	resolved, link_user, ok := oauth_callback_ceremony(c, "github", state)
	if !ok || resolved == nil || resolved.Mode != "reauthentication" || link_user != "u-link" {
		t.Errorf("step-up callback with no session was rejected (ok=%v link_user=%q)", ok, link_user)
	}
}
