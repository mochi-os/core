// Mochi server: provider accounts - the identity a sign-in link names, the
// grants it gathers, and the ceremony that gathers them
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
	sl "go.starlark.net/starlark"
	"golang.org/x/oauth2"
)

// TestAccountProvidersOfferConfiguredOauthOnly: an OAuth provider the server
// has no client for cannot be connected, so it is not offered; a configured
// one is, with the oauth flow and its capabilities.
func TestAccountProvidersOfferConfiguredOauthOnly(t *testing.T) {
	create_test_users_db(t)
	setup_settings_test_schema()
	load_core_labels()
	list := func() map[string]map[string]any {
		out, err := api_account_providers(&sl.Thread{}, sl.NewBuiltin("mochi.account.providers", api_account_providers), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		by := map[string]map[string]any{}
		for _, p := range sl_decode(out).([]any) {
			pm := p.(map[string]any)
			by[pm["type"].(string)] = pm
		}
		return by
	}
	before := list()
	if _, offered := before["google"]; offered {
		t.Error("google offered with no client configured")
	}
	if p := before["caldav"]; p == nil || p["flow"] != "form" {
		t.Errorf("caldav = %v, want a form provider", p)
	}
	if p := before["apple"]; p == nil || len(p["fields"].([]any)) != 3 {
		t.Errorf("apple = %v, want three fields", p)
	}
	setting_set("oauth_google_client_id", "id")
	setting_set("oauth_google_client_secret", "secret")
	after := list()
	p := after["google"]
	if p == nil || p["flow"] != "oauth" {
		t.Fatalf("google = %v, want an oauth provider", p)
	}
	capabilities := p["capabilities"].([]any)
	if len(capabilities) != 2 || capabilities[0] != "login" || capabilities[1] != "calendar" {
		t.Errorf("google capabilities = %v", capabilities)
	}
}

// TestAccountLinkMakesTheAccount: linking an identity for sign-in creates
// the connected account it is, holding the sign-in capability; unlinking
// removes it unless a grant still lives on it.
func TestAccountLinkMakesTheAccount(t *testing.T) {
	oauth_binding_setup(t)
	user := user_by_uid("u-link")
	if user == nil {
		t.Fatal("no test user")
	}
	if !oauth_link_apply("github", &oauth_profile{Subject: "sub-1", Email: "link@example.com", Name: "Link"}, "u-link") {
		t.Fatal("link refused")
	}
	db := db_user(user, "user")
	row, _ := db.row("select id, type, label, identifier, data from accounts where type='github'")
	if row == nil || row["identifier"] != "sub-1" || row["label"] != "link@example.com" {
		t.Fatalf("account after link = %v", row)
	}
	if granted := account_granted(user, row); len(granted) != 1 || granted[0] != "login" {
		t.Errorf("granted = %v, want [login]", granted)
	}
	// Linking again finds the row rather than making a second.
	oauth_link_apply("github", &oauth_profile{Subject: "sub-1", Email: "link@example.com"}, "u-link")
	if rows, _ := db.rows("select id from accounts where type='github'"); len(rows) != 1 {
		t.Errorf("accounts after a second link = %d, want 1", len(rows))
	}
	// Another user's link to the same identity makes nothing here.
	if oauth_link_apply("github", &oauth_profile{Subject: "sub-1"}, "u-other") {
		t.Error("an identity linked to one user was linked to another")
	}

	// Unlinked with no grant: the row goes.
	account_oauth_unlinked(user, "github")
	if exists, _ := db.exists("select 1 from accounts where type='github'"); exists {
		t.Error("account survived the unlink with no grant on it")
	}
	// Unlinked with a grant: the row stays, and the sign-in is no longer granted.
	db.exec("insert into accounts (id, type, label, identifier, data, created) values ('g1', 'github', '', 'sub-2', ?, 1)", json_encode(map[string]any{"refresh": "r", "scopes": []string{"repo"}}))
	account_oauth_unlinked(user, "github")
	row, _ = db.row("select id, type, identifier, data from accounts where id='g1'")
	if row == nil {
		t.Fatal("account with a grant did not survive the unlink")
	}
	if granted := account_granted(user, row); len(granted) != 0 {
		t.Errorf("granted after unlink = %v, want none", granted)
	}
}

// TestOauthGrantCeremony: a grant asks the provider for the capability's
// scopes beside the identity, offline so a refresh token comes back, on a
// consent the provider shows; the callback routes it to the grant, bound to
// the user's session like a link; and the web begin endpoint cannot ask for it.
func TestOauthGrantCeremony(t *testing.T) {
	oauth_binding_setup(t)
	previous := oauth_grants["github"]
	oauth_grants["github"] = map[string][]string{"calendar": {"repo"}}
	t.Cleanup(func() { oauth_grants["github"] = previous })

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/calendars/-/calendars/grant", nil)
	provider := oauth_providers()["github"]
	session := login_create("u-link", "", "")
	hop, _, err := oauth_begin_ceremony(c, provider, "github", "u-link", "/calendars/", "grant", "", "", "", &oauth_grant{capability: "calendar"})
	if err != nil {
		t.Fatal(err)
	}
	// The wizard runs in the shell's iframe, so the grant answers the
	// same-origin hop; the consent is what the hop sends the session to.
	query := oauth_consent_from_hop(t, hop, session).Query()
	if !strings.Contains(query.Get("scope"), "repo") || !strings.Contains(query.Get("scope"), "read:user") {
		t.Errorf("scope = %q, want the identity scopes and the capability's", query.Get("scope"))
	}
	if query.Get("access_type") != "offline" || query.Get("include_granted_scopes") != "true" || query.Get("prompt") != "consent select_account" {
		t.Errorf("grant parameters = %v", query)
	}
	state, st := oauth_only_ceremony(t)
	if st.Mode != "grant" || st.Capability != "calendar" || st.Target != "/calendars/" {
		t.Errorf("ceremony = %+v", st)
	}
	if got := oauth_callback_destination(&st, "u-link"); got != oauth_destination_grant {
		t.Errorf("destination = %q, want grant", got)
	}
	if got := oauth_callback_destination(&st, ""); got != oauth_destination_login {
		t.Errorf("a grant ceremony with no user routes to %q, want login", got)
	}
	// Bound to the session that began it, like a link.
	cb, _ := oauth_callback_context(state)
	if _, _, ok := oauth_callback_ceremony(cb, "github", state); ok {
		t.Error("grant callback with no session was accepted")
	}

	// A re-consent of a known account steers the provider to it.
	w2 := httptest.NewRecorder()
	c2, _ := gin.CreateTestContext(w2)
	c2.Request = httptest.NewRequest("POST", "/calendars/-/calendars/grant", nil)
	db_open("db/sessions.db").exec("delete from ceremonies")
	hop, _, err = oauth_begin_ceremony(c2, provider, "github", "u-link", "/calendars/", "grant", "", "", "", &oauth_grant{capability: "calendar", hint: "sub-9"})
	if err != nil {
		t.Fatal(err)
	}
	if q := oauth_consent_from_hop(t, hop, session).Query(); q.Get("login_hint") != "sub-9" || q.Get("prompt") != "consent" {
		t.Errorf("re-consent parameters = %v", q)
	}

	// The web endpoint reduces an asked-for grant mode to a plain login.
	db_open("db/sessions.db").exec("delete from ceremonies")
	begun := oauth_begin_request("", `{"mode":"grant"}`)
	if begun.Code != http.StatusOK {
		t.Fatalf("begin: %d", begun.Code)
	}
	_, st = oauth_only_ceremony(t)
	if st.Mode != "" || st.Capability != "" {
		t.Errorf("web begin minted a %q ceremony with capability %q", st.Mode, st.Capability)
	}
}

// TestOauthGrantApplyStoresTheToken: the callback lands the refresh token and
// the granted scopes on the account the identity is, made when new, and sends
// the browser back to the app naming the capability and the account; a
// consent that withheld the scopes grants nothing.
func TestOauthGrantApplyStoresTheToken(t *testing.T) {
	oauth_binding_setup(t)
	previous := oauth_grants["github"]
	oauth_grants["github"] = map[string][]string{"calendar": {"repo"}}
	t.Cleanup(func() { oauth_grants["github"] = previous })
	user := user_by_uid("u-link")
	db := db_user(user, "user")
	profile := &oauth_profile{Subject: "sub-1", Email: "link@example.com"}
	st := &oauth_state{Target: "/calendars/?view=month", Capability: "calendar"}

	apply := func(token *oauth2.Token) string {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/_/auth/oauth/github/callback", nil)
		oauth_grant_apply(c, "github", profile, token, st, "u-link")
		return w.Header().Get("Location")
	}

	// Withheld scope: nothing stored.
	location := apply((&oauth2.Token{RefreshToken: "r1"}).WithExtra(map[string]any{"scope": "read:user"}))
	if !strings.Contains(location, "grant_error=denied") {
		t.Errorf("withheld scope: location = %q", location)
	}
	if exists, _ := db.exists("select 1 from accounts where type='github'"); exists {
		t.Error("a denied grant stored an account")
	}
	// No refresh token: nothing stored.
	location = apply((&oauth2.Token{AccessToken: "a"}).WithExtra(map[string]any{"scope": "read:user repo"}))
	if !strings.Contains(location, "grant_error=provider_error") {
		t.Errorf("no refresh token: location = %q", location)
	}

	// Granted: the account is made with the token and scopes, and the browser
	// returns to the target with the capability and the account.
	location = apply((&oauth2.Token{RefreshToken: "r1"}).WithExtra(map[string]any{"scope": "read:user repo"}))
	row, _ := db.row("select id, type, identifier, label, data, verified from accounts where type='github'")
	if row == nil || row["identifier"] != "sub-1" || row["label"] != "link@example.com" {
		t.Fatalf("account after grant = %v", row)
	}
	var data struct {
		Refresh string   `json:"refresh"`
		Scopes  []string `json:"scopes"`
	}
	json.Unmarshal([]byte(row["data"].(string)), &data)
	if data.Refresh != "r1" || len(data.Scopes) != 2 {
		t.Errorf("data = %+v", data)
	}
	if want := "/calendars/?view=month&granted=calendar&account=" + row_string(row, "id"); location != want {
		t.Errorf("location = %q, want %q", location, want)
	}
	// github offers no calendar capability of its own, so the grant is read
	// through the scope check the capability list would consult.
	if !oauth_account_granted(user, row, "calendar") {
		t.Error("the granted scopes do not cover the capability")
	}
	if granted := account_granted(user, row); len(granted) != 0 {
		t.Errorf("granted before the link = %v, want none", granted)
	}

	// Linked for sign-in afterwards: the same row, now holding both.
	oauth_link_apply("github", profile, "u-link")
	if rows, _ := db.rows("select id from accounts where type='github'"); len(rows) != 1 {
		t.Fatalf("accounts after link = %d, want 1", len(rows))
	}
	row, _ = db.row("select id, type, identifier, data from accounts where type='github'")
	if granted := account_granted(user, row); len(granted) != 1 || granted[0] != "login" {
		t.Errorf("granted after link = %v, want [login]", granted)
	}
	if !oauth_account_granted(user, row, "calendar") {
		t.Error("the link dropped the grant")
	}

	// A second consent adds scopes and replaces the token, on the same row.
	apply((&oauth2.Token{RefreshToken: "r2"}).WithExtra(map[string]any{"scope": "read:user repo gist"}))
	row, _ = db.row("select id, data from accounts where type='github'")
	json.Unmarshal([]byte(row["data"].(string)), &data)
	if data.Refresh != "r2" || len(data.Scopes) != 3 {
		t.Errorf("data after a second consent = %+v", data)
	}
}

// TestAccountRemoveKeepsTheSignIn: removing an OAuth account revokes its
// grants; while its identity is linked for sign-in the row stays, holding the
// sign-in alone, and the login page's unlink is what ends that.
func TestAccountRemoveKeepsTheSignIn(t *testing.T) {
	oauth_binding_setup(t)
	user := user_by_uid("u-link")
	db := db_user(user, "user")
	oauth_link_apply("github", &oauth_profile{Subject: "sub-1", Email: "link@example.com"}, "u-link")
	row, _ := db.row("select id from accounts where type='github'")
	db.account_set(row_string(row, "id"), map[string]any{"data": json_encode(map[string]any{"refresh": "r", "scopes": []string{"repo"}})})

	thread := &sl.Thread{}
	thread.SetLocal("app", &App{id: "internal", internal: &AppVersion{}})
	thread.SetLocal("user", user)
	fn := sl.NewBuiltin("mochi.account.remove", api_account_remove)
	out, err := api_account_remove(thread, fn, sl.Tuple{sl.String(row_string(row, "id"))}, nil)
	if err != nil || out != sl.True {
		t.Fatalf("remove = %v, %v", out, err)
	}
	kept, _ := db.row("select id, data from accounts where type='github'")
	if kept == nil {
		t.Fatal("removing a linked account deleted the sign-in's row")
	}
	if kept["data"] != "{}" {
		t.Errorf("grant not revoked: data = %v", kept["data"])
	}

	// Without the sign-in link, the row goes.
	db_open("db/users.db").exec("delete from oauth where provider='github'")
	out, err = api_account_remove(thread, fn, sl.Tuple{sl.String(row_string(row, "id"))}, nil)
	if err != nil || out != sl.True {
		t.Fatalf("second remove = %v, %v", out, err)
	}
	if exists, _ := db.exists("select 1 from accounts where type='github'"); exists {
		t.Error("an unlinked account survived its removal")
	}
}

// TestAccountAddCalendarProviders: the two password-backed calendar
// providers keep their credentials in the data column, iCloud at Apple's
// fixed address, and an OAuth type is not added by form.
func TestAccountAddCalendarProviders(t *testing.T) {
	oauth_binding_setup(t)
	user := user_by_uid("u-link")
	thread := &sl.Thread{}
	thread.SetLocal("app", &App{id: "internal", internal: &AppVersion{}})
	thread.SetLocal("user", user)
	fn := sl.NewBuiltin("mochi.account.add", api_account_add)
	add := func(ptype string, fields map[string]string) (map[string]any, error) {
		var kwargs []sl.Tuple
		for k, v := range fields {
			kwargs = append(kwargs, sl.Tuple{sl.String(k), sl.String(v)})
		}
		out, err := api_account_add(thread, fn, sl.Tuple{sl.String(ptype)}, kwargs)
		if err != nil {
			return nil, err
		}
		return sl_decode(out).(map[string]any), nil
	}
	if _, err := add("google", map[string]string{"label": "x"}); err == nil {
		t.Error("an oauth type was added by form")
	}
	if _, err := add("caldav", map[string]string{"url": "ftp://x", "username": "u", "password": "p"}); err == nil {
		t.Error("a non-http server address was accepted")
	}
	out, err := add("caldav", map[string]string{"url": "https://dav.example.com/", "username": "u", "password": "p", "label": "Work"})
	if err != nil {
		t.Fatal(err)
	}
	db := db_user(user, "user")
	row, _ := db.row("select identifier, data from accounts where id=?", out["id"])
	var data map[string]any
	json.Unmarshal([]byte(row["data"].(string)), &data)
	if row["identifier"] != "https://dav.example.com/" || data["username"] != "u" || data["password"] != "p" {
		t.Errorf("caldav account = %v %v", row, data)
	}
	out, err = add("apple", map[string]string{"username": "me@icloud.com", "password": "abcd-efgh"})
	if err != nil {
		t.Fatal(err)
	}
	row, _ = db.row("select identifier, data from accounts where id=?", out["id"])
	json.Unmarshal([]byte(row["data"].(string)), &data)
	if row["identifier"] != "me@icloud.com" || data["url"] != dav_client_apple_root {
		t.Errorf("apple account = %v %v", row, data)
	}
	if _, err := add("apple", map[string]string{"username": "not an id", "password": "x"}); err == nil {
		t.Error("an Apple ID that is not an address was accepted")
	}
}

// TestOauthSignInKeepsItsAccountRow: an identity linked before accounts held
// sign-ins has no account row, and signing in with it makes the row, so the
// accounts page and the calendars wizard see the sign-in from then on; a
// later sign-in, web or mobile, leaves the row and its label alone.
func TestOauthSignInKeepsItsAccountRow(t *testing.T) {
	oauth_binding_setup(t)
	users := db_open("db/users.db")
	users.exec("update users set methods='oauth' where uid='u-link'")
	db_open("db/sessions.db").exec("create table logins (user text primary key, last integer not null)")
	users.exec("insert into oauth (user, provider, subject, email, name, created) values ('u-link', 'github', 'sub-1', 'link@example.com', 'Link', 1)")
	user := user_by_uid("u-link")
	db := db_user(user, "user")
	profile := &oauth_profile{Subject: "sub-1", Email: "link@example.com", Name: "Link"}

	web := func() {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/_/auth/oauth/github/callback", nil)
		oauth_login(c, "github", profile, "/", "")
		if w.Code != http.StatusFound || strings.Contains(w.Header().Get("Location"), "oauth_error") {
			t.Fatalf("web sign-in: %d %q", w.Code, w.Header().Get("Location"))
		}
	}
	mobile := func() {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/_/auth/oauth/github/callback", nil)
		oauth_mobile_login(c, "github", profile, &oauth_state{Provider: "github", Mode: "mobile", Scheme: "mochi", Challenge: strings.Repeat("c", 43)})
		if w.Code != http.StatusFound || strings.Contains(w.Header().Get("Location"), "error=") {
			t.Fatalf("mobile sign-in: %d %q", w.Code, w.Header().Get("Location"))
		}
	}
	account := func(want string) {
		t.Helper()
		rows, _ := db.rows("select id, label, identifier from accounts where type='github'")
		if len(rows) != 1 || row_string(rows[0], "identifier") != "sub-1" || row_string(rows[0], "label") != want {
			t.Fatalf("github accounts = %v, want one for sub-1 labelled %q", rows, want)
		}
	}

	web()
	account("link@example.com")
	// A label the user gave survives, and no second row appears.
	row, _ := db.row("select id from accounts where type='github'")
	db.account_set(row_string(row, "id"), map[string]any{"label": "Work"})
	web()
	account("Work")

	db.exec("delete from accounts where type='github'")
	mobile()
	account("link@example.com")
	mobile()
	account("link@example.com")
}
