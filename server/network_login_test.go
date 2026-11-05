// Mochi server: Signing in with an account on another network - tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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

// TestNetworkLoginAfterLink: a Mastodon or Bluesky account linked in
// Settings is also a way to sign in, as a sign-in with one is also a link, so
// the first sign-in with it goes straight in with no email asked for. An
// account on another network is not, and an identity another user already
// signs in with stays theirs.
func TestNetworkLoginAfterLink(t *testing.T) {
	alice := network_login_env(t)
	fake := create_mastodon_fake(t)
	host := fake_host(fake.server)
	subject := fake.server.URL + "/@alice"
	users := db_open("db/users.db")

	linked_store(alice, "mastodon", &linked_profile{ID: subject, Address: "@alice@" + host, Name: "Alice", Server: host}, map[string]any{"token": "mtok"})
	row, _ := users.row("select user, email, verified from oauth where provider='mastodon' and subject=?", subject)
	if row == nil || row_string(row, "user") != alice.UID || row_string(row, "email") != alice.Username || row_int(row, "verified") != 1 {
		t.Fatalf("THE POINT: the link made no way to sign in: %v", row)
	}
	// Linking again keeps the one.
	linked_store(alice, "mastodon", &linked_profile{ID: subject, Address: "@alice@" + host, Name: "Alice", Server: host}, map[string]any{"token": "mtok"})
	if n, _ := users.row("select count(*) as n from oauth where provider='mastodon'"); row_int(n, "n") != 1 {
		t.Errorf("identities after a second link = %v", n)
	}

	// The first sign-in with the account goes straight in: no pending
	// identity, no email page.
	db_open("db/users.db").exec("update users set methods='oauth' where uid=?", alice.UID)
	w := network_login_begin_request("mastodon", `{"server":"`+host+`","target":"/feeds/"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("begin: %d %s", w.Code, w.Body.String())
	}
	var answer struct {
		URL string `json:"url"`
	}
	json.Unmarshal(w.Body.Bytes(), &answer)
	consent, err := url.Parse(answer.URL)
	if err != nil {
		t.Fatalf("consent = %q", answer.URL)
	}
	fake.lock.Lock()
	fake.challenge = consent.Query().Get("code_challenge")
	fake.lock.Unlock()
	r := network_login_callback("mastodon", "state="+consent.Query().Get("state")+"&code=code-1", cookie_named(w, oauth_binding_cookie))
	if r.Code != http.StatusFound || r.Header().Get("Location") != "/feeds/" {
		t.Fatalf("THE POINT: the first sign-in after the link: %d %q", r.Code, r.Header().Get("Location"))
	}
	if pending := cookie_named(r, network_login_cookie); pending != nil && pending.Value != "" {
		t.Error("the sign-in kept a pending identity")
	}

	// A Bluesky account is one too; a Lemmy account is not a way to sign in.
	linked_store(alice, "bluesky", &linked_profile{ID: "did:plc:alice", Address: "alice.bsky.social", Name: "Alice"}, map[string]any{"token": "btok"})
	if held, _ := users.exists("select 1 from oauth where user=? and provider='bluesky' and subject='did:plc:alice'", alice.UID); !held {
		t.Error("THE POINT: a linked Bluesky account is no way to sign in")
	}
	linked_store(alice, "lemmy", &linked_profile{ID: "https://lemmy.example/u/alice", Address: "@alice@lemmy.example", Name: "Alice"}, map[string]any{"token": "ltok"})
	if held, _ := users.exists("select 1 from oauth where provider='lemmy'"); held {
		t.Error("a linked Lemmy account became a way to sign in")
	}

	// An identity another user signs in with stays theirs.
	users.exec("insert into oauth (user, provider, subject, email, verified, name, created) values ('someone', 'mastodon', 'https://elsewhere.example/@bob', 'bob@example.com', 1, 'Bob', 1)")
	linked_store(alice, "mastodon", &linked_profile{ID: "https://elsewhere.example/@bob", Address: "@bob@elsewhere.example", Name: "Bob"}, map[string]any{"token": "mtok"})
	if row, _ := users.row("select user from oauth where provider='mastodon' and subject='https://elsewhere.example/@bob'"); row_string(row, "user") != "someone" {
		t.Errorf("THE POINT: another user's identity moved: %v", row)
	}
}

// TestNetworkLoginForeignAccount: a server that names an account on another
// server as the one signing in is refused, as the address is the identity a
// sign-in is known by: it would otherwise sign in as whoever holds that
// account's sign-in here. A link through such a server is refused too, and
// makes no way to sign in.
func TestNetworkLoginForeignAccount(t *testing.T) {
	alice := network_login_env(t)
	fake := create_mastodon_fake(t)
	host := fake_host(fake.server)
	users := db_open("db/users.db")
	users.exec("update users set methods='oauth' where uid=?", alice.UID)
	users.exec("insert into oauth (user, provider, subject, email, verified, name, created) values (?, 'mastodon', 'https://mastodon.example/@alice', ?, 1, 'Alice', 1)", alice.UID, alice.Username)
	fake.lock.Lock()
	fake.address = "https://mastodon.example/@alice"
	fake.lock.Unlock()

	w := network_login_begin_request("mastodon", `{"server":"`+host+`","target":"/feeds/"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("begin: %d %s", w.Code, w.Body.String())
	}
	var answer struct {
		URL string `json:"url"`
	}
	json.Unmarshal(w.Body.Bytes(), &answer)
	consent, err := url.Parse(answer.URL)
	if err != nil {
		t.Fatalf("consent = %q", answer.URL)
	}
	fake.lock.Lock()
	fake.challenge = consent.Query().Get("code_challenge")
	fake.lock.Unlock()
	r := network_login_callback("mastodon", "state="+consent.Query().Get("state")+"&code=code-1", cookie_named(w, oauth_binding_cookie))
	if !strings.Contains(r.Header().Get("Location"), "oauth_error=provider_error") {
		t.Errorf("THE POINT: a server naming another server's account signed in: %d %q", r.Code, r.Header().Get("Location"))
	}
	if session := cookie_named(r, "session"); session != nil && session.Value != "" {
		t.Error("THE POINT: the sign-in set a session")
	}
	if len(accounts_network(alice, "mastodon")) != 0 {
		t.Error("the sign-in linked the server's account")
	}

	// A link through the server is refused as well.
	if _, err := mastodon_profile(context.Background(), &linked{user: alice, kind: "mastodon", data: map[string]any{"server": host, "token": "mtok"}}); err == nil {
		t.Error("THE POINT: a link read another server's account as the server's own")
	}
	if n, _ := users.row("select count(*) as n from oauth"); row_int(n, "n") != 1 {
		t.Errorf("identities after the refusals = %v, want 1", n)
	}
}

// TestSchema24SignsInLinkedAccounts: an install at schema 23 has its linked
// Mastodon and Bluesky accounts made ways to sign in, once, by db_upgrade_24. A
// hosted account, an account on another network and an identity another user
// signs in with are left alone, as is a Mastodon account whose address is not
// on the server it was linked at, and a user with no user.db gains none.
func TestSchema24SignsInLinkedAccounts(t *testing.T) {
	directory := test_bare_directory(t)
	settings := db_open("db/settings.db")
	settings.exec("create table settings ( name text not null primary key, value text not null )")
	settings.exec("insert into settings ( name, value ) values ( 'schema', '23' )")
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null)")
	users.exec("create table oauth (id integer primary key, user text not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("insert into users (uid, username) values ('u1', 'alice@example.com'), ('u2', 'bob@example.com'), ('u3', 'carol@example.com')")
	users.exec("insert into oauth (user, provider, subject, email, verified, name, created) values ('u2', 'mastodon', 'https://example.social/@bob', 'bob@example.com', 1, 'Bob', 1)")
	if err := os.MkdirAll(filepath.Join(directory, "users", "u1"), 0755); err != nil {
		t.Fatal(err)
	}
	accounts := db_open("users/u1/user.db")
	accounts.exec("create table accounts (id text not null primary key, type text not null, label text not null default '', identifier text not null default '', data text not null default '', created integer not null)")
	for _, account := range [][]string{
		{"a1", "mastodon", "Alice", `{"id":"https://example.social/@alice","name":"Alice","server":"example.social"}`},
		{"a2", "bluesky", "Alice", `{"id":"did:plc:alice","name":""}`},
		{"a3", "bluesky", "Hosted", `{"id":"did:plc:hosted","hosted":true}`},
		{"a4", "lemmy", "Alice", `{"id":"https://lemmy.example/u/alice"}`},
		{"a5", "mastodon", "Bob", `{"id":"https://example.social/@bob","server":"example.social"}`},
		{"a7", "mastodon", "Carol", `{"id":"https://example.social/@carol","server":"evil.example"}`},
		{"a6", "mastodon", "Empty", `{}`},
	} {
		accounts.exec("insert into accounts (id, type, label, data, created) values (?, ?, ?, ?, 1)", account[0], account[1], account[2], account[3])
	}
	// No handle holds it when the walk reaches it, which the walk must close.
	db_purge_prefix(filepath.Join("users", "u1"))

	db_upgrade()

	if schema := atoi(setting_get("schema", ""), 0); schema != schema_version {
		t.Fatalf("schema after the upgrade is %d, want %d", schema, schema_version)
	}
	row, _ := users.row("select user, email, verified, name from oauth where provider='mastodon' and subject='https://example.social/@alice'")
	if row == nil || row_string(row, "user") != "u1" || row_string(row, "email") != "alice@example.com" || row_int(row, "verified") != 1 || row_string(row, "name") != "Alice" {
		t.Errorf("THE POINT: the linked Mastodon account is no way to sign in: %v", row)
	}
	if row, _ := users.row("select user, name from oauth where provider='bluesky' and subject='did:plc:alice'"); row == nil || row_string(row, "user") != "u1" || row_string(row, "name") != "Alice" {
		t.Errorf("THE POINT: the linked Bluesky account is no way to sign in, named by its label: %v", row)
	}
	if held, _ := users.exists("select 1 from oauth where subject='did:plc:hosted'"); held {
		t.Error("a hosted account became a way to sign in")
	}
	if held, _ := users.exists("select 1 from oauth where provider='lemmy'"); held {
		t.Error("a Lemmy account became a way to sign in")
	}
	if held, _ := users.exists("select 1 from oauth where subject='https://example.social/@carol'"); held {
		t.Error("THE POINT: a Mastodon account naming another server's address became a way to sign in")
	}
	if row, _ := users.row("select user from oauth where subject='https://example.social/@bob'"); row_string(row, "user") != "u2" {
		t.Errorf("THE POINT: another user's identity moved: %v", row)
	}
	if n, _ := users.row("select count(*) as n from oauth"); row_int(n, "n") != 3 {
		t.Errorf("identities after the upgrade = %v, want 3", n)
	}
	if file_exists(filepath.Join(directory, "users", "u3", "user.db")) {
		t.Error("the upgrade made a user.db for a user who had none")
	}
	if db_cached(filepath.Join(directory, "users", "u1", "user.db")) {
		t.Error("the upgrade left a user.db it opened itself open")
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
	if signin["hash"] != "h-signin" || client["name"] != "Ivory" || client["website"] != "https://ivory.example" || signin["address"] != "@alice@example.com" || signin["device"] != "phone" {
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
