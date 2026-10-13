// Mochi server: Signing in with an account on another network - tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// network_login_env is the linked-account environment with the tables a
// login touches: the identities that sign in and the record of logins.
func network_login_env(t *testing.T) *User {
	t.Helper()
	alice := create_linked_test_env(t)
	db_open("db/users.db").exec("create table if not exists oauth (id integer primary key, user text not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	db_open("db/sessions.db").exec("create table if not exists logins (user text primary key, last integer not null)")
	db_open("db/sessions.db").exec("create table if not exists partial (id text primary key, user text not null, completed text not null default '', remaining text not null, expires integer not null)")
	db_open("db/sessions.db").exec("create table if not exists codes (code text not null, username text not null, expires integer not null, primary key (code, username))")
	setting_set("signup_enabled", "true")
	gin.SetMode(gin.TestMode)
	return alice
}

// network_login_begin_request posts to the begin endpoint as the login page
// does and returns the recorder.
func network_login_begin_request(provider string, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "http://example.com/_/auth/oauth/"+provider+"/begin", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = gin.Params{{Key: "provider", Value: provider}}
	web_oauth_begin(c)
	return w
}

// network_login_callback drives the callback with the cookies the browser
// holds and returns the recorder.
func network_login_callback(provider string, query string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "http://example.com/_/auth/oauth/"+provider+"/callback?"+query, nil)
	for _, cookie := range cookies {
		c.Request.AddCookie(cookie)
	}
	c.Params = gin.Params{{Key: "provider", Value: provider}}
	web_oauth_callback(c)
	return w
}

// cookie_named finds a cookie a response set, or nil.
func cookie_named(w *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	return nil
}

// TestNetworkLoginMastodon: the login page begins a sign-in with a Mastodon
// account by naming the server, the consent is the server's, and the callback
// is bound to the browser. An identity nobody holds yet waits on an email: the
// browser goes to the login page with a pending cookie, and the login that
// follows joins the identity to the user it proves, as a linked account and
// as a way to sign in. The same account then signs that user straight in.
// With sign-up closed a new identity is refused.
func TestNetworkLoginMastodon(t *testing.T) {
	alice := network_login_env(t)
	fake := create_mastodon_fake(t)
	host := fake_host(fake.server)

	// The methods list offers Mastodon; Bluesky only with a configured domain.
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "http://example.com/_/auth/methods", nil)
	web_auth_methods(c)
	var methods struct {
		OAuth map[string]bool `json:"oauth"`
	}
	json.Unmarshal(w.Body.Bytes(), &methods)
	if !methods.OAuth["mastodon"] {
		t.Error("the methods do not offer Mastodon")
	}
	if methods.OAuth["bluesky"] != (bluesky_client_base() != "") {
		t.Errorf("bluesky offered = %v with client base %q", methods.OAuth["bluesky"], bluesky_client_base())
	}

	begin := func() (string, *http.Cookie) {
		w := network_login_begin_request("mastodon", `{"server":"`+host+`","target":"/feeds/"}`)
		if w.Code != http.StatusOK {
			t.Fatalf("begin: %d %s", w.Code, w.Body.String())
		}
		var answer struct {
			URL string `json:"url"`
		}
		json.Unmarshal(w.Body.Bytes(), &answer)
		consent, err := url.Parse(answer.URL)
		if err != nil || consent.Host != host || consent.Path != "/oauth/authorize" {
			t.Fatalf("consent = %q", answer.URL)
		}
		q := consent.Query()
		if q.Get("client_id") != fake.client || q.Get("state") == "" || q.Get("code_challenge_method") != "S256" {
			t.Fatalf("consent query = %v", q)
		}
		fake.lock.Lock()
		fake.challenge = q.Get("code_challenge")
		fake.lock.Unlock()
		binding := cookie_named(w, oauth_binding_cookie)
		if binding == nil {
			t.Fatal("begin set no binding cookie")
		}
		return q.Get("state"), binding
	}

	// A callback in a browser without the binding is refused.
	state, _ := begin()
	if r := network_login_callback("mastodon", "state="+state+"&code=code-1"); !strings.Contains(r.Header().Get("Location"), "oauth_error=state_invalid") {
		t.Errorf("callback without the binding: %q", r.Header().Get("Location"))
	}

	// A new identity waits on an email address.
	state, binding := begin()
	r := network_login_callback("mastodon", "state="+state+"&code=code-1", binding)
	if r.Code != http.StatusFound || r.Header().Get("Location") != "/login/?network=mastodon" {
		t.Fatalf("callback for a new identity: %d %q", r.Code, r.Header().Get("Location"))
	}
	pending := cookie_named(r, network_login_cookie)
	if pending == nil || pending.Value == "" {
		t.Fatal("no pending cookie")
	}
	if has, _ := db_open("db/users.db").exists("select 1 from oauth where provider='mastodon'"); has {
		t.Error("an identity was recorded before any login")
	}
	if len(accounts_network(alice, "mastodon")) != 0 {
		t.Error("an account was linked before any login")
	}

	// With sign-up closed the identity is refused instead.
	setting_set("signup_enabled", "false")
	state, binding = begin()
	if r := network_login_callback("mastodon", "state="+state+"&code=code-1", binding); !strings.Contains(r.Header().Get("Location"), "oauth_error=signup_disabled") {
		t.Errorf("callback with sign-up closed: %q", r.Header().Get("Location"))
	}
	setting_set("signup_enabled", "true")

	// The login that follows joins the identity to the user it proves.
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "http://example.com/_/auth/verify", nil)
	c.Request.AddCookie(pending)
	auth_establish_session(c, alice)
	row, _ := db_open("db/users.db").row("select user, subject, email, verified from oauth where provider='mastodon'")
	if row == nil || row_string(row, "user") != alice.UID || row_string(row, "subject") != fake.server.URL+"/@alice" || row_string(row, "email") != alice.Username || row_int(row, "verified") != 1 {
		t.Fatalf("identity after the login = %v", row)
	}
	linked := accounts_network(alice, "mastodon")
	if len(linked) != 1 || account_data(linked[0])["token"] != "mtok" || linked[0]["identifier"] != "@alice@"+host {
		t.Fatalf("linked account after the login = %v", linked)
	}
	if cleared := cookie_named(w, network_login_cookie); cleared == nil || cleared.Value != "" {
		t.Error("the pending cookie was not cleared")
	}
	if left, _ := db_open("db/sessions.db").exists("select 1 from ceremonies where type='network_login'"); left {
		t.Error("the pending identity was not spent")
	}
	// Presenting the spent cookie again changes nothing.
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "http://example.com/_/auth/verify", nil)
	c.Request.AddCookie(pending)
	auth_establish_session(c, alice)
	if n, _ := db_open("db/users.db").row("select count(*) as n from oauth"); row_int(n, "n") != 1 {
		t.Errorf("identities after a replayed cookie = %v", n)
	}

	// The account now signs alice in. Her required factors still apply as
	// MFA: with email required the browser goes on to the codes page.
	state, binding = begin()
	r = network_login_callback("mastodon", "state="+state+"&code=code-1", binding)
	if r.Code != http.StatusFound || r.Header().Get("Location") != "/login/codes?redirect=%2Ffeeds%2F" {
		t.Fatalf("callback for a known identity with email required: %d %q", r.Code, r.Header().Get("Location"))
	}
	// With no other factor required she is signed straight in, to the target.
	db_open("db/users.db").exec("update users set methods='oauth' where uid=?", alice.UID)
	state, binding = begin()
	r = network_login_callback("mastodon", "state="+state+"&code=code-1", binding)
	if r.Code != http.StatusFound || r.Header().Get("Location") != "/feeds/" {
		t.Fatalf("callback for a known identity: %d %q", r.Code, r.Header().Get("Location"))
	}
	if session := cookie_named(r, "session"); session == nil || session.Value == "" {
		t.Error("the sign-in set no session")
	}
	if len(accounts_network(alice, "mastodon")) != 1 {
		t.Error("the sign-in made a second linked account")
	}
	fake.lock.Lock()
	registrations := fake.registrations
	fake.lock.Unlock()
	if registrations != 1 {
		t.Errorf("%d registrations on the server, want 1", registrations)
	}

	// Without the development switch the provider is unknown to the sign-in.
	networks_development = false
	if w := network_login_begin_request("mastodon", `{"server":"`+host+`"}`); w.Code != http.StatusNotFound {
		t.Errorf("begin with the switch off: %d", w.Code)
	}
	networks_development = true
	// A link or a native sign-in is not begun here.
	if w := network_login_begin_request("mastodon", `{"server":"`+host+`","link":true}`); w.Code != http.StatusNotFound {
		t.Errorf("begin as a link: %d", w.Code)
	}
	if w := network_login_begin_request("mastodon", `{"server":"`+host+`","mode":"mobile","scheme":"mochi","challenge":"`+strings.Repeat("c", 43)+`"}`); w.Code != http.StatusNotFound {
		t.Errorf("begin as a native app: %d", w.Code)
	}
}

// TestTokenListByKind: the Sessions page lists a user's sign-ins and minted
// passwords from every app under the sessions permission, each with its
// client and account named, and revokes one under the same permission.
func TestTokenListByKind(t *testing.T) {
	alice := create_linked_test_env(t)
	db_open("db/sessions.db").exec("create table if not exists accesses (hash text primary key, user text not null, used integer not null)")
	db_open("db/users.db").exec("create table if not exists tokens (hash text primary key not null, user text not null, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0, kind text not null default '', client text not null default '', account text not null default '', network text not null default '', jkt text not null default '', refresh text not null default '', device text not null default '', scram text not null default '')")
	db_open("db/networks.db").exec("insert into applications (id, secret, name, website, redirects, scopes, network, created) values ('cl-1', '', 'Ivory', 'https://ivory.example', '[]', 'read write', 'fediverse', 1)")
	users := db_open("db/users.db")
	users.exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires, kind, client, account, network, jkt, refresh, device) values ('h-signin', ?, '', 'Ivory', '[\"read\",\"write\"]', '', '', 10, 0, 'signin', 'cl-1', 'f1', 'fediverse', '', '', 'phone')", alice.UID)
	users.exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires, kind, account, network) values ('h-password', ?, 'settings', 'Laptop', '[]', '', '', 20, 0, 'password', 'f1', 'fediverse')", alice.UID)
	users.exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires) values ('h-plain', ?, 'app1', 'DAV', '[\"dav\"]', '', '', 30, 0)", alice.UID)

	app := create_external_app("app1")
	thread := create_test_thread(alice, app)
	list := sl.NewBuiltin("mochi.token.list", api_token_list)
	remove := sl.NewBuiltin("mochi.token.delete", api_token_delete)
	listed := func(kwargs []sl.Tuple) []any {
		value, err := api_token_list(thread, list, nil, kwargs)
		if err != nil {
			t.Fatal(err)
		}
		return sl_decode(value).([]any)
	}
	if _, err := api_token_list(thread, list, nil, linked_kwargs("kind", "signin")); err == nil {
		t.Error("the sign-ins were listed without user/sessions/read")
	}
	permission_grant(alice, app.id, "user/sessions/read")
	signins := listed(linked_kwargs("kind", "signin"))
	if len(signins) != 1 {
		t.Fatalf("sign-ins = %v", signins)
	}
	signin := signins[0].(map[string]any)
	client, _ := signin["client"].(map[string]any)
	if signin["hash"] != "h-signin" || client["name"] != "Ivory" || client["website"] != "https://ivory.example" || signin["address"] != "alice@example.com" || signin["device"] != "phone" {
		t.Errorf("sign-in = %v", signin)
	}
	passwords := listed(linked_kwargs("kind", "password"))
	if len(passwords) != 1 || passwords[0].(map[string]any)["name"] != "Laptop" {
		t.Errorf("passwords = %v", passwords)
	}
	if _, err := api_token_list(thread, list, nil, linked_kwargs("kind", "dav")); err == nil {
		t.Error("an unknown kind was accepted")
	}
	// The app's own list is unchanged by the kinds.
	if own := listed(nil); len(own) != 1 || own[0].(map[string]any)["hash"] != "h-plain" {
		t.Errorf("own tokens = %v", own)
	}

	// Revoking a sign-in takes user/sessions/write; a plain token of another
	// app is still refused.
	if _, err := api_token_delete(thread, remove, sl.Tuple{sl.String("h-signin")}, nil); err == nil {
		t.Error("a sign-in was revoked without user/sessions/write")
	}
	permission_grant(alice, app.id, "user/sessions/write")
	if _, err := api_token_delete(thread, remove, sl.Tuple{sl.String("h-signin")}, nil); err != nil {
		t.Errorf("revoking a sign-in: %v", err)
	}
	if has, _ := users.exists("select 1 from tokens where hash='h-signin'"); has {
		t.Error("the sign-in was not revoked")
	}
	if _, err := api_token_delete(thread, remove, sl.Tuple{sl.String("h-password")}, nil); err != nil {
		t.Errorf("revoking a password: %v", err)
	}
	users.exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires) values ('h-other', ?, 'other', 'Other', '[]', '', '', 40, 0)", alice.UID)
	if _, err := api_token_delete(thread, remove, sl.Tuple{sl.String("h-other")}, nil); err == nil {
		t.Error("another app's plain token was revoked")
	}
}
