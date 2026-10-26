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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	git "github.com/go-git/go-git/v5"
	"github.com/tailscale/hujson"
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
			// Marked asset: the only actions an asset token reaches. One
			// private and function-backed, one public, so the read-only rule
			// is measured on both sides of the app-token block.
			"-/asset":  {Function: "action_asset", Asset: true},
			"-/avatar": {Function: "action_avatar", Public: true, Asset: true},
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

// The asset token is the one a page puts in an image or attachment URL, so a
// copied link hands it over. It reaches only the actions a manifest marks
// asset, and on those it reads and nothing else, whether it arrives in the
// query string or, as an app's own fetch would send it, in the header.
// Anywhere else it is set aside before the gates, and the request goes on as
// the cookie alone.
func TestBearerGateHoldsAnAssetTokenToReads(t *testing.T) {
	app, _, session := web_bearer_env(t)

	asset := auth_create_asset_token("bearer-user", session, app.id)
	ordinary := auth_create_app_token("bearer-user", session, app.id)
	if asset == "" || ordinary == "" {
		t.Fatal("could not mint the tokens")
	}

	// A refusal here is the gate's own. The fixture's actions name functions
	// that do not exist, so a request that PASSES the gate ends in a 500 -
	// which is what distinguishes "allowed through" from "refused".
	status, body := web_bearer_action(t, app, session, "Bearer "+asset, "GET", "-/asset")
	if status == 403 {
		t.Errorf("asset token on a read of an asset action got 403 %s; a read is the whole point of it", body)
	}

	status, body = web_bearer_action(t, app, session, "Bearer "+asset, "POST", "-/asset")
	if status != 403 || !strings.Contains(body, "app_token_read_only") {
		t.Errorf("asset token on a write got %d %s; want 403 app_token_read_only", status, body)
	}

	// Control: the same write with the ordinary app token is not refused, so
	// the refusal above measures the purpose and not the method.
	status, body = web_bearer_action(t, app, session, "Bearer "+ordinary, "POST", "-/asset")
	if status == 403 {
		t.Errorf("app token on a write got 403 %s; only the asset token is read-only", body)
	}

	// And on a PUBLIC asset action, which skips the whole app-token block: it
	// still runs as whoever the credential names, so a leaked asset token
	// would post as them.
	status, body = web_bearer_action(t, app, session, "Bearer "+asset, "POST", "-/avatar")
	if status != 403 || !strings.Contains(body, "app_token_read_only") {
		t.Errorf("asset token writing to a public asset action got %d %s; want 403 app_token_read_only", status, body)
	}
	status, body = web_bearer_action(t, app, session, "Bearer "+ordinary, "POST", "-/avatar")
	if status == 403 {
		t.Errorf("app token on the public asset action got 403 %s; the refusal must be about the purpose", body)
	}

	// On an action without the mark the token is set aside, so the request is
	// the cookie alone and the app-token gate refuses it as such: a copied
	// image link reads nothing a page does not embed.
	status, body = web_bearer_method(t, app, session, "Bearer "+asset, "GET")
	if status != 403 || !strings.Contains(body, "app_token_required") {
		t.Errorf("asset token on an unmarked action got %d %s; want 403 app_token_required, the token set aside", status, body)
	}
}

// web_bearer_caller builds a Starlark app whose actions answer whether they
// ran as a user, on the bearer fixture's account and session. The app-token
// gate is skipped for public actions, so what matters there is who the action
// runs as, which only a real handler can report.
func web_bearer_caller(t *testing.T) (*App, string) {
	t.Helper()
	_, _, session := web_bearer_env(t)

	// Starlark.call needs the runtime state the server sets up at startup: a
	// concurrency semaphore (a nil channel blocks for ever) and a compute
	// timeout (zero cancels at once).
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 32)
	}
	original := starlark_default_timeout
	starlark_default_timeout = 30 * time.Second
	t.Cleanup(func() { starlark_default_timeout = original })

	star := filepath.Join(t.TempDir(), "app.star")
	source := `
def action_who(a):
    return {"signed": a.user != None, "user": a.user.uid if a.user else ""}

def action_session(a):
    return {
        "current": len([r for r in mochi.user.session.list() if r["current"]]),
        "presence": mochi.presence.set("busy"),
    }

def action_storage(a):
    return {"user": a.user.uid if a.user else "", "size": mochi.cache.size("probe")}

def action_write(a):
    return {"written": mochi.cache.write("written", "bytes")}

def action_logout(a):
    a.logout()
    return {"done": True}
`
	if err := os.WriteFile(star, []byte(source), 0o600); err != nil {
		t.Fatalf("writing the test app: %v", err)
	}
	av := &AppVersion{Version: "1.0", Execute: []string{star}, Actions: map[string]AppAction{
		"-/open":    {Function: "action_who", Public: true},
		"-/picture": {Function: "action_who", Public: true, Asset: true},
		"-/avatar":  {Function: "action_who", Public: true, Asset: true, Shared: true},
		"-/private": {Function: "action_who"},
		"-/session": {Function: "action_session"},
		"-/storage": {Function: "action_storage", Public: true},
		"-/write":   {Function: "action_write", Public: true},
		"-/logout":  {Function: "action_logout"},
	}}
	av.Architecture.Engine = "starlark"
	app := &App{id: "bearertest", internal: av}
	av.app = app
	if _, found := av.starlark().globals["action_who"]; !found {
		t.Fatal("the test app did not load; action_who is not defined")
	}
	return app, session
}

// web_bearer_target runs one request through web_action at a full target, so a
// query-string token can be carried.
func web_bearer_target(t *testing.T, app *App, session string, authorization string, method string, target string) (int, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, "/bearertest/"+target, nil)
	c.Request.Header.Set("Accept", "application/json")
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	if authorization != "" {
		c.Request.Header.Set("Authorization", authorization)
	}
	action, _, _ := strings.Cut(target, "?")
	web_action(c, app, action, nil, routing_class)
	return w.Code, w.Body.String()
}

// Any app's page holds its own app token and can send it to any URL on the
// server, so a token minted for another app must not act as the user on a
// public action, which the app-token gate does not cover. The requests carry
// no cookie, as a sandboxed app frame's do.
func TestBearerGateSetsAsideAnotherAppsToken(t *testing.T) {
	app, session := web_bearer_caller(t)
	own := auth_create_app_token("bearer-user", session, app.id)
	other := auth_create_app_token("bearer-user", session, "otherapp")
	asset := auth_create_asset_token("bearer-user", session, "otherapp")
	if own == "" || other == "" || asset == "" {
		t.Fatal("could not mint the tokens")
	}
	signed := `"signed":true`
	unsigned := `"signed":false`

	// Control: the app's own token runs a public action as the user.
	status, body := web_bearer_target(t, app, "", "Bearer "+own, "GET", "-/open")
	if status != 200 || !strings.Contains(body, signed) {
		t.Fatalf("own token on a public action got %d %s; want it to run as the user", status, body)
	}

	// Another app's token, in the header or the query string, is set aside.
	status, body = web_bearer_target(t, app, "", "Bearer "+other, "GET", "-/open")
	if status != 200 || !strings.Contains(body, unsigned) {
		t.Errorf("another app's token on a public action got %d %s; want it to run anonymously", status, body)
	}
	status, body = web_bearer_target(t, app, "", "", "GET", "-/open?token="+other)
	if status != 200 || !strings.Contains(body, unsigned) {
		t.Errorf("another app's token in the query string got %d %s; want it to run anonymously", status, body)
	}

	// Set aside, the request goes on as the cookie says: the shell page, which
	// has the cookie, keeps its user.
	status, body = web_bearer_target(t, app, session, "Bearer "+other, "GET", "-/open")
	if status != 200 || !strings.Contains(body, signed) {
		t.Errorf("another app's token with the session cookie got %d %s; want the cookie's user", status, body)
	}

	// A read of an asset action other apps embed - marked shared, as a
	// person's avatar is - keeps it, so the avatar still loads as the user; a
	// write there does not.
	for _, token := range []string{other, asset} {
		status, body = web_bearer_target(t, app, "", "Bearer "+token, "GET", "-/avatar")
		if status != 200 || !strings.Contains(body, signed) {
			t.Errorf("another app's token reading a shared asset action got %d %s; want it to run as the user", status, body)
		}
	}
	status, body = web_bearer_target(t, app, "", "Bearer "+other, "POST", "-/avatar")
	if status != 200 || !strings.Contains(body, unsigned) {
		t.Errorf("another app's token writing to a shared asset action got %d %s; want it to run anonymously", status, body)
	}

	// Any other asset action - an attachment, a photo - serves whatever id it
	// is asked for, so a read sets the token aside too: kept, any app's page
	// holding an id could read the user's private ones.
	for _, token := range []string{other, asset} {
		status, body = web_bearer_target(t, app, "", "Bearer "+token, "GET", "-/picture")
		if status != 200 || !strings.Contains(body, unsigned) {
			t.Errorf("another app's token reading an unshared asset action got %d %s; want it to run anonymously", status, body)
		}
	}

	// Set aside means wholly: another app's asset token writing to an asset
	// action is anonymous, not refused as the read-only credential it no longer
	// is.
	status, body = web_bearer_target(t, app, "", "Bearer "+asset, "POST", "-/avatar")
	if status != 200 || !strings.Contains(body, unsigned) {
		t.Errorf("another app's asset token writing to a shared asset action got %d %s; want it to run anonymously", status, body)
	}

	// Control: the app's own token reads its unshared asset as the user.
	status, body = web_bearer_target(t, app, "", "Bearer "+own, "GET", "-/picture")
	if status != 200 || !strings.Contains(body, signed) {
		t.Errorf("own token reading an unshared asset action got %d %s; want it to run as the user", status, body)
	}

	// A private action still refuses it outright.
	status, body = web_bearer_target(t, app, "", "Bearer "+other, "GET", "-/private")
	if status != 403 || !strings.Contains(body, "app_token_mismatch") {
		t.Errorf("another app's token on a private action got %d %s; want 403 app_token_mismatch", status, body)
	}
}

// web_bearer_two_users adds a second account to the caller fixture, and mints
// tokens for both: the victim's as a copied link would carry them, the
// caller's from the caller's own session.
func web_bearer_two_users(t *testing.T) (*App, string, map[string]string, map[string]string) {
	t.Helper()
	app, session := web_bearer_caller(t)

	users := db_open("db/users.db")
	users.exec("insert into users (uid, username, role, status) values ('other-user', 'other@example.com', 'user', 'active')")
	identity := test_entity_id('o')
	users.exec("insert into entities (id, fingerprint, user, class, name, privacy) values (?, ?, 'other-user', 'person', 'Other Tester', 'private')",
		identity, fingerprint(identity))
	users.exec("create table if not exists tokens (hash text primary key not null, user text not null, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0, kind text not null default '', client text not null default '', account text not null default '', network text not null default '', jkt text not null default '', refresh text not null default '', device text not null default '', scram text not null default '')")
	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists accesses (hash text primary key not null, user text not null, used integer not null default 0)")
	n := now()
	sessions.exec("insert into sessions (user, code, secret, expires, created, address, agent) values ('other-user', 'other-session', 'other-test-secret-0123456789', ?, ?, '127.0.0.1', 'test')", n+86400, n)

	victim := map[string]string{
		"asset": auth_create_asset_token("bearer-user", session, app.id),
		"app":   auth_create_app_token("bearer-user", session, app.id),
		// An app token for another app, as an image link copied before asset
		// tokens carried one.
		"elsewhere": auth_create_app_token("bearer-user", session, "otherapp"),
		"api":       token_create("bearer-user", app.id, "api", nil, 0, "", ""),
	}
	caller := map[string]string{
		"app": auth_create_app_token("other-user", "other-session", app.id),
		"api": token_create("other-user", app.id, "api", nil, 0, "", ""),
	}
	for _, tokens := range []map[string]string{victim, caller} {
		for kind, token := range tokens {
			if token == "" {
				t.Fatalf("could not mint the %s token", kind)
			}
		}
	}
	return app, session, victim, caller
}

// A header token decides the app and purpose the gates judge, so it must name
// the user the request runs as. The header is set aside whenever the URL token
// or the cookie has bound another user, and the request is judged on that
// credential alone.
func TestBearerGateSetsAsideAnotherUsersToken(t *testing.T) {
	app, session, victim, caller := web_bearer_two_users(t)
	as_victim := `"user":"bearer-user"`
	as_caller := `"user":"other-user"`

	refused := []struct {
		name          string
		session       string
		authorization string
		target        string
		want          string
	}{
		// The victim's asset token from an image link, beside the caller's
		// app token: the app token's claims would lift the read-only asset
		// onto the app's private actions.
		{"asset token in the URL, caller's app token", "", "Bearer " + caller["app"], "-/private?token=" + victim["asset"], "authentication_required"},
		// The victim's old app token for another app, beside the caller's API
		// token for this one: the API token would carry it across apps.
		{"another app's token in the URL, caller's API token", "", "Bearer " + caller["api"], "-/private?token=" + victim["elsewhere"], "app_token_mismatch"},
		// The cookie binds the victim; the header names the caller.
		{"cookie, caller's app token", session, "Bearer " + caller["app"], "-/private", "app_token_required"},
		{"cookie, caller's API token", session, "Bearer " + caller["api"], "-/private", "app_token_required"},
	}
	for _, tt := range refused {
		status, body := web_bearer_target(t, app, tt.session, tt.authorization, "GET", tt.target)
		if strings.Contains(body, as_victim) || !strings.Contains(body, tt.want) {
			t.Errorf("%s: got %d %s; want %s, the header set aside", tt.name, status, body, tt.want)
		}
	}

	// Controls: a header naming the same user is still used, beside either
	// binding, and a header alone binds its own user.
	allowed := []struct {
		name          string
		session       string
		authorization string
		target        string
		want          string
	}{
		{"asset token in the URL, own app token", "", "Bearer " + victim["app"], "-/private?token=" + victim["asset"], as_victim},
		{"cookie, own app token", session, "Bearer " + victim["app"], "-/private", as_victim},
		{"cookie, own API token", session, "Bearer " + victim["api"], "-/private", as_victim},
		{"the caller's app token alone", "", "Bearer " + caller["app"], "-/private", as_caller},
		{"the caller's API token alone", "", "Bearer " + caller["api"], "-/private", as_caller},
	}
	for _, tt := range allowed {
		status, body := web_bearer_target(t, app, tt.session, tt.authorization, "GET", tt.target)
		if status != 200 || !strings.Contains(body, tt.want) {
			t.Errorf("%s: got %d %s; want 200 with %s", tt.name, status, body, tt.want)
		}
	}
}

// The session an action sees is the caller's own: mochi.user.session.list marks
// it current, and mochi.presence.set changes only its connections, or every
// connection of the user when the request has no session. A URL token binds
// its user without the cookie, so neither the cookie nor a header set aside
// may hand the action another account's session.
func TestBearerGateSessionBelongsToTheUser(t *testing.T) {
	app, session, victim, caller := web_bearer_two_users(t)

	// Two connections of the victim, each from its own login session.
	presence_lock.Lock()
	presence_connections["bearer-user"] = map[string]*presence_connection{
		"here":      {ID: "here", Session: session, Status: "online"},
		"elsewhere": {ID: "elsewhere", Session: "bearer-elsewhere", Status: "online"},
	}
	presence_lock.Unlock()
	t.Cleanup(func() {
		presence_lock.Lock()
		delete(presence_connections, "bearer-user")
		presence_lock.Unlock()
	})

	own := `"current":1,"presence":1`
	none := `"current":0,"presence":2`
	cases := []struct {
		name          string
		session       string
		authorization string
		target        string
		want          string
	}{
		{"cookie", session, "Bearer " + victim["app"], "-/session", own},
		{"own app token", "", "Bearer " + victim["app"], "-/session", own},
		{"app token in the URL", "", "", "-/session?token=" + victim["app"], none},
		// The caller's header is set aside, and its session with it.
		{"app token in the URL, caller's app token", "", "Bearer " + caller["app"], "-/session?token=" + victim["app"], none},
		// The URL token binds the victim before the cookie is read.
		{"app token in the URL, caller's cookie", "other-session", "", "-/session?token=" + victim["app"], none},
	}
	for _, tt := range cases {
		status, body := web_bearer_target(t, app, tt.session, tt.authorization, "GET", tt.target)
		if status != 200 || !strings.Contains(body, tt.want) {
			t.Errorf("%s: got %d %s; want 200 with %s", tt.name, status, body, tt.want)
		}
	}
}

// web_bearer_cache points the cache at a fresh directory and gives the victim
// one entry, so whose storage an action reaches can be read off its size.
func web_bearer_cache(t *testing.T, app *App) string {
	t.Helper()
	saved := cache_dir
	cache_dir = t.TempDir()
	t.Cleanup(func() { cache_dir = saved })
	victim := filepath.Join(cache_dir, "apps", "bearer-user", app.id)
	if err := os.MkdirAll(victim, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(victim, "probe"), []byte("victim-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	return victim
}

// A token set aside binds nothing of its user: not the storage an anonymous
// request falls back to, which the owner names, and not an API token sent
// beside it. The owner used to be chosen before the asset token was set
// aside, so a copied image link read and wrote the victim's storage.
func TestBearerGateSetAsideTokenBindsNothing(t *testing.T) {
	app, _, victim, _ := web_bearer_two_users(t)
	storage := web_bearer_cache(t, app)
	of_victim := `"size":13`

	// Control: the victim's own credential reaches the victim's storage, and
	// an anonymous request reaches none.
	status, body := web_bearer_target(t, app, "", "Bearer "+victim["app"], "GET", "-/storage")
	if status != 200 || !strings.Contains(body, of_victim) {
		t.Fatalf("victim's own token got %d %s; want the victim's storage", status, body)
	}
	anonymous, _ := web_bearer_target(t, app, "", "", "GET", "-/storage")

	status, body = web_bearer_target(t, app, "", "", "GET", "-/storage?token="+victim["asset"])
	if strings.Contains(body, of_victim) || status != anonymous {
		t.Errorf("anonymous read with the victim's asset token got %d %s; want %d as with no token", status, body, anonymous)
	}

	web_bearer_target(t, app, "", "Bearer "+victim["asset"], "POST", "-/write")
	if _, err := os.Stat(filepath.Join(storage, "written")); err == nil {
		t.Error("an anonymous write with the victim's asset token landed in the victim's storage")
	}

	// Signed in as someone else on a hosted route with a context, where the
	// owner names the storage a signed-in caller reaches: the owner must not
	// be the victim the set-aside token named.
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/storage?token="+victim["asset"], nil)
	c.Request.Header.Set("Accept", "application/json")
	c.Request.AddCookie(&http.Cookie{Name: "session", Value: "other-session"})
	c.Set("domain_route", &route{Context: "context"})
	c.Set("domain_context", "context")
	c.Set("domain_owner", "")
	web_action(c, app, "-/storage", nil, routing_hosted)
	if strings.Contains(w.Body.String(), of_victim) {
		t.Errorf("another user's cookie with the victim's asset token on a hosted route got %d %s; want none of the victim's storage", w.Code, w.Body.String())
	}

	// An API token of the victim's sent beside the asset token goes with it.
	status, body = web_bearer_target(t, app, "other-session", "Bearer "+victim["api"], "GET", "-/private?token="+victim["asset"])
	if status != 403 || !strings.Contains(body, "app_token_required") {
		t.Errorf("another user's cookie with the victim's asset and API tokens got %d %s; want 403 app_token_required", status, body)
	}
}

// The account gates judge whoever the request runs as once credentials are
// set aside, not the user a set-aside token named.
func TestBearerGateAccountGatesFollowTheSetAside(t *testing.T) {
	app, _, victim, _ := web_bearer_two_users(t)
	users := db_open("db/users.db")

	// A closing account's cookie, beside an active user's asset token in the
	// URL: the token is set aside, and the request is the closing account's.
	users.exec("update users set status='closing' where uid='other-user'")
	status, body := web_bearer_target(t, app, "other-session", "", "GET", "-/open?token="+victim["asset"])
	if status != 403 || !strings.Contains(body, "account_closing") {
		t.Errorf("a closing account's cookie with another user's asset token got %d %s; want 403 account_closing", status, body)
	}
	users.exec("update users set status='active' where uid='other-user'")

	// A closing account's token for another app, set aside on a public
	// action, leaves an anonymous request, which the gate does not refuse.
	users.exec("update users set status='closing' where uid='bearer-user'")
	status, body = web_bearer_target(t, app, "", "Bearer "+victim["elsewhere"], "GET", "-/open")
	if status != 200 || !strings.Contains(body, `"signed":false`) {
		t.Errorf("a closing account's token for another app got %d %s; want it set aside and the request anonymous", status, body)
	}
	// Control: the closing account's own token is refused.
	status, body = web_bearer_target(t, app, "", "Bearer "+victim["app"], "GET", "-/open")
	if status != 403 || !strings.Contains(body, "account_closing") {
		t.Errorf("a closing account's own token got %d %s; want 403 account_closing", status, body)
	}
}

// The version is chosen for whoever the request runs as: a token set aside
// leaves the default, not its user's preference.
func TestBearerGateVersionFollowsTheSetAside(t *testing.T) {
	_, session, victim, _ := web_bearer_two_users(t)

	versions := map[string]*AppVersion{}
	app := &App{id: "bearertest", versions: map[string]*AppVersion{}}
	for _, version := range []string{"1", "2"} {
		star := filepath.Join(t.TempDir(), "app.star")
		source := "def action_version(a):\n    return {\"version\": \"" + version + "\"}\n"
		if err := os.WriteFile(star, []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
		av := &AppVersion{Version: version, Execute: []string{star}, Actions: map[string]AppAction{
			"-/version": {Function: "action_version", Public: true},
			// Public only in the victim's version.
			"-/gated": {Function: "action_version", Public: version == "2"},
		}}
		// A DAV route only in the victim's version, which answers OPTIONS
		// itself; elsewhere OPTIONS gets the preflight.
		if version == "2" {
			av.Actions["-/dav"] = AppAction{Feature: "carddav", Public: true}
		} else {
			av.Actions["-/dav"] = AppAction{Function: "action_version", Public: true}
		}
		av.Architecture.Engine = "starlark"
		av.app = app
		versions[version] = av
		app.versions[version] = av
	}
	app.latest = versions["1"]

	// The victim prefers version 2; anyone else gets 1.
	apps_lock.Lock()
	resolution_version_put(resolution_key{"bearer-user", app.id}, versions["2"])
	resolution_version_put(resolution_key{"", app.id}, versions["1"])
	apps_lock.Unlock()
	t.Cleanup(resolution_invalidate)

	status, body := web_bearer_target(t, app, session, "", "GET", "-/version")
	if status != 200 || !strings.Contains(body, `"version":"2"`) {
		t.Fatalf("the victim's own request got %d %s; want version 2", status, body)
	}
	status, body = web_bearer_target(t, app, "", "Bearer "+victim["elsewhere"], "GET", "-/version")
	if status != 200 || !strings.Contains(body, `"version":"1"`) {
		t.Errorf("another app's token, set aside, got %d %s; want the default version 1", status, body)
	}
	// The action is found again in that version, where it is not public.
	status, body = web_bearer_target(t, app, "", "Bearer "+victim["elsewhere"], "GET", "-/gated")
	if status != 401 || !strings.Contains(body, "authentication_required") {
		t.Errorf("another app's token, set aside, on an action public only in its user's version got %d %s; want 401 authentication_required", status, body)
	}
	status, body = web_bearer_target(t, app, "", "Bearer "+victim["elsewhere"], "OPTIONS", "-/dav")
	if status != 204 {
		t.Errorf("OPTIONS with another app's token, set aside, on a DAV route only in its user's version got %d %s; want the 204 preflight", status, body)
	}
}

// web_bearer_repository gives the victim a private repository in the test app,
// with an empty bare repository on disk and the access rules git checks, and
// the routes a repositories app declares for it.
func web_bearer_repository(t *testing.T, app *App) *Entity {
	t.Helper()
	app.internal.Actions[":repository/*path"] = AppAction{File: "index.html", Public: true}
	app.internal.Actions[":repository/git/*path"] = AppAction{Feature: "git", Public: true}
	app.internal.Actions[":repository/-/storage"] = AppAction{Function: "action_storage", Public: true}

	repository := test_entity_id('r')
	db_open("db/users.db").exec("insert into entities (id, fingerprint, user, class, name, privacy) values (?, ?, 'bearer-user', 'repository', 'Private', 'private')",
		repository, fingerprint(repository))
	owner := user_by_uid("bearer-user")
	path := git_repo_path(owner, app, repository)
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := git.PlainInit(path, true); err != nil {
		t.Fatal(err)
	}
	system := db_app_system(owner, app)
	system.access_set(owner.identity().ID, "repository/"+repository, "*", true, "test")
	system.close()
	e := entity_by_any(repository)
	if e == nil {
		t.Fatal("the repository entity did not load")
	}
	return e
}

// web_bearer_domain runs one request under the entity's own domain.
func web_bearer_domain(t *testing.T, app *App, e *Entity, method string, name string, authorization string) (int, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, "/"+name, nil)
	c.Request.Header.Set("Accept", "application/json")
	if strings.HasPrefix(name, "git-") {
		c.Request.Header.Set("Content-Type", "application/x-"+name+"-request")
	}
	if authorization != "" {
		c.Request.Header.Set("Authorization", authorization)
	}
	web_action(c, app, name, e, routing_domain)
	return w.Code, w.Body.String()
}

// Git under a repository's own domain is answered without the action routing
// chose, the repository's page, which is a static file an asset token may
// load. The token must not reach git: a copied image link would push.
func TestBearerGateDomainGitSetsAsideAssetToken(t *testing.T) {
	app, _, victim, _ := web_bearer_two_users(t)
	e := web_bearer_repository(t, app)
	push := func(authorization string) (int, string) {
		return web_bearer_domain(t, app, e, "POST", "git-receive-pack", authorization)
	}

	anonymous, _ := push("")
	if anonymous != 401 {
		t.Fatalf("a push with no credential got %d; want 401", anonymous)
	}
	// Control: the app's own token reaches the push, which then finds no
	// commands in the empty body.
	if status, body := push("Bearer " + victim["app"]); status == 401 {
		t.Fatalf("a push with the app's own token got %d %s; want it past authentication", status, body)
	}
	if status, body := push("Bearer " + victim["asset"]); status != 401 {
		t.Errorf("a push with the victim's asset token got %d %s; want 401 as with no credential", status, body)
	}
}

// a.logout ends the session of the user the action runs as. A token in the
// URL binds its user before the cookie is read, so the cookie may be another
// account's, and that session is not this caller's to end.
func TestBearerGateLogoutEndsOnlyTheCallersSession(t *testing.T) {
	app, session, victim, _ := web_bearer_two_users(t)

	status, body := web_bearer_target(t, app, "other-session", "", "GET", "-/logout?token="+victim["app"])
	if status != 200 {
		t.Fatalf("logout as the URL token's user got %d %s", status, body)
	}
	if session_user("other-session") != "other-user" {
		t.Error("logout as the URL token's user ended the cookie's session, another account's")
	}

	// Control: the user's own session ends.
	status, body = web_bearer_target(t, app, session, "Bearer "+victim["app"], "GET", "-/logout")
	if status != 200 {
		t.Fatalf("logout with the user's own cookie got %d %s", status, body)
	}
	if session_user(session) != "" {
		t.Error("logout with the user's own cookie left the session live")
	}
}

// Mid-restore an account's files are renamed underneath anything holding them.
// Its own requests are refused, and so are anyone else's that would run with
// its storage: an anonymous request to a public action on its entity, and git
// on its repositories.
func TestWebActionRefusesARestoringOwner(t *testing.T) {
	app, _, _, _ := web_bearer_two_users(t)
	web_bearer_cache(t, app)
	e := web_bearer_repository(t, app)

	// Controls: while the owner is active, the public action runs with the
	// owner's storage, and anonymous git is asked for a credential.
	status, body := web_bearer_domain(t, app, e, "GET", "-/storage", "")
	if status != 200 || !strings.Contains(body, `"size":13`) {
		t.Fatalf("an anonymous request to the active owner's public action got %d %s; want the owner's storage", status, body)
	}
	if status, body := web_bearer_domain(t, app, e, "POST", "git-upload-pack", ""); status != 401 {
		t.Fatalf("anonymous git on the active owner's repository got %d %s; want 401", status, body)
	}

	db_open("db/users.db").exec("update users set status='pending-restore' where uid='bearer-user'")
	status, body = web_bearer_domain(t, app, e, "GET", "-/storage", "")
	if status != 503 || !strings.Contains(body, "restore_in_progress") || strings.Contains(body, `"size"`) {
		t.Errorf("an anonymous request to a restoring owner's public action got %d %s; want 503 restore_in_progress", status, body)
	}
	if status, body := web_bearer_domain(t, app, e, "POST", "git-upload-pack", ""); status != 503 {
		t.Errorf("anonymous git on a restoring owner's repository got %d %s; want 503", status, body)
	}

	// A visitor's browser gets the message itself, not JSON.
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/-/storage", nil)
	c.Request.Header.Set("Accept", "text/html")
	web_action(c, app, "-/storage", e, routing_domain)
	if w.Code != 503 || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") || w.Body.Len() == 0 || strings.HasPrefix(w.Body.String(), "{") {
		t.Errorf("a browser visiting a restoring owner's page got %d %q %s; want 503 with the message as text", w.Code, w.Header().Get("Content-Type"), w.Body.String())
	}
}

// Every app's page shows a person's avatar and style through the people app,
// with its own token. Only the routes the people manifest marks shared keep
// that token, so a missing mark turns every other app's avatars anonymous, and
// a person this server knows only through the user's directory then fails to
// load. Read from the people app's source, skipped where it is absent.
func TestPeopleEmbeddedRoutesAreShared(t *testing.T) {
	base := "../../apps/people"
	if _, err := os.Stat(base + "/app.json"); err != nil {
		t.Skipf("apps/people source absent: %v", err)
	}
	// Parsed as app_read parses it, without the checks that need a server
	// version and the app's files.
	raw, err := os.ReadFile(base + "/app.json")
	if err != nil {
		t.Fatal(err)
	}
	data, err := hujson.Standardize(raw)
	if err != nil {
		t.Fatalf("the people manifest: %v", err)
	}
	var av AppVersion
	if err := json.Unmarshal(data, &av); err != nil {
		t.Fatalf("the people manifest: %v", err)
	}
	for _, route := range []string{":person/-/avatar", ":person/-/style"} {
		action, found := av.Actions[route]
		if !found {
			t.Errorf("the people manifest has no %s", route)
			continue
		}
		if !action.Public || !action.Asset || !action.Shared {
			t.Errorf("%s is public=%v asset=%v shared=%v; other apps embed it, so it must be all three", route, action.Public, action.Asset, action.Shared)
		}
	}
}
