// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	sessions.exec("create table verifications (oauth integer not null, user text not null, last integer not null, primary key (oauth, user))")
	db_open("db/settings.db").exec("create table settings (name text primary key, value text not null default '')")

	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create table oauth (id integer primary key, user text not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("insert into users (uid, username, methods) values ('u-link', 'link@example.com', 'email')")
	users.exec("insert into users (uid, username, methods) values ('u-other', 'other@example.com', 'email')")
	// user_by_uid, which the Bearer path at /begin goes through, returns nil
	// for a user with no person entity.
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-link', '', 'fp-link', 'u-link', 'person', 'Link')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-other', '', 'fp-other', 'u-other', 'person', 'Other')")

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

// oauth_mobile_link_begin starts a mobile LINK ceremony for u-link and returns
// the ceremony state id and the app's PKCE verifier.
func oauth_mobile_link_begin(t *testing.T, session string) (string, string) {
	t.Helper()
	sessions := db_open("db/sessions.db")
	proof := uid()
	sessions.exec("insert into reauthentication (id, user, methods, expires) values (?, 'u-link', 'email', ?)", proof, now()+300)

	verifier := random_alphanumeric(64)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])

	token := auth_create_app_token("u-link", session, "settings")
	if token == "" {
		t.Fatal("could not mint an app token")
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/oauth/github/begin",
		strings.NewReader(`{"link":true,"mode":"mobile","scheme":"mochi","target":"mochi:oauth-link-return","challenge":"`+challenge+`","token":"`+proof+`"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("Authorization", "Bearer "+token)
	c.Params = gin.Params{{Key: "provider", Value: "github"}}
	web_oauth_begin(c)
	if w.Code != http.StatusOK {
		t.Fatalf("begin mobile link: status = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	state, _ := oauth_only_ceremony(t)
	return state, verifier
}

// oauth_exchange_request drives POST /_/auth/oauth/exchange.
func oauth_exchange_request(code, verifier, bearer string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/oauth/exchange",
		strings.NewReader(`{"code":"`+code+`","verifier":"`+verifier+`"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		c.Request.Header.Set("Authorization", "Bearer "+bearer)
	}
	web_oauth_exchange(c)
	return w
}

// oauth_linked_owner returns the user a (provider, subject) is linked to.
func oauth_linked_owner(provider, subject string) string {
	row, _ := db_open("db/users.db").row("select user from oauth where provider=? and subject=?", provider, subject)
	if row == nil {
		return ""
	}
	owner, _ := row["user"].(string)
	return owner
}

// TestOauthMobileLinkCompletesInTheApp is the mobile half of the link-hijack
// defence. A link the app began used to be written by whatever browser
// presented the callback - on Android a Custom Tab carrying no session, so
// nothing in that request identified the app or the user. An attacker who
// began a link on their own device could hand the provider URL to a victim and
// collect the victim's provider identity against the attacker's account.
//
// Now the callback only stashes the profile and deep-links the app; the link
// is written at /exchange, where the verifier proves the app instance and the
// Bearer proves the user.
func TestOauthMobileLinkCompletesInTheApp(t *testing.T) {
	defer oauth_binding_setup(t)()
	sessions := db_open("db/sessions.db")

	link_session := login_create("u-link", "", "")
	other_session := login_create("u-other", "", "")
	link_token := auth_create_app_token("u-link", link_session, "settings")
	other_token := auth_create_app_token("u-other", other_session, "settings")

	profile := &oauth_profile{Subject: "gh-subject-1", Email: "link@example.com", Verified: true, Name: "Link User"}

	// The callback writes NO link and sends the app a deep link, not a web
	// redirect. This is the browser the attacker would have handed to a victim.
	state, verifier := oauth_mobile_link_begin(t, link_session)
	callback, _ := oauth_callback_context(state)
	st, link_user, ok := oauth_callback_ceremony(callback, "github", state)
	if !ok {
		t.Fatal("mobile link callback was rejected")
	}
	// The routing itself is the security property: reaching the plain link
	// destination is the browser writing a link it cannot authorise.
	if d := oauth_callback_destination(st, link_user); d != oauth_destination_mobile_link {
		t.Fatalf("mobile link ceremony routes to %q, want %q", d, oauth_destination_mobile_link)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_/auth/oauth/github/callback", nil)
	oauth_mobile_link(c, "github", profile, st, link_user)

	if owner := oauth_linked_owner("github", profile.Subject); owner != "" {
		t.Errorf("the callback wrote the link (owner %q); it must wait for the app's exchange", owner)
	}
	location := w.Header().Get("Location")
	if !strings.HasPrefix(location, "mochi:oauth-link-return?") {
		t.Errorf("callback location = %q, want a mochi:oauth-link-return deep link", location)
	}
	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("callback location: %v", err)
	}
	// Go puts an opaque URI's query in RawQuery, not in Opaque.
	query := parsed.Query()
	code := query.Get("code")
	if code == "" {
		t.Fatal("callback deep link carries no exchange code")
	}
	if query.Get("nonce") == "" {
		t.Error("callback deep link carries no return nonce")
	}

	// Another user's token cannot complete it, even holding the verifier -
	// this is the attack, with the attacker's own app presenting its token.
	if w := oauth_exchange_request(code, verifier, other_token); w.Code != http.StatusForbidden {
		t.Errorf("exchange with another user's token: status = %d, want 403", w.Code)
	}
	if owner := oauth_linked_owner("github", profile.Subject); owner != "" {
		t.Errorf("a refused exchange linked the identity to %q", owner)
	}

	// No token at all is refused too. The exchange row is consumed on the
	// first attempt, so re-stash for each case.
	restash := func() string {
		t.Helper()
		code, err := oauth_mobile_store(st.Challenge, map[string]any{
			"link": true, "user": link_user, "provider": "github",
			"profile": map[string]any{"subject": profile.Subject, "email": profile.Email, "verified": true, "name": profile.Name},
		})
		if err != nil {
			t.Fatalf("restash: %v", err)
		}
		return code
	}
	if w := oauth_exchange_request(restash(), verifier, ""); w.Code != http.StatusUnauthorized {
		t.Errorf("exchange with no token: status = %d, want 401", w.Code)
	}

	// The wrong verifier is refused even with the right token: the app that
	// began the ceremony is a separate claim from the user completing it.
	if w := oauth_exchange_request(restash(), random_alphanumeric(64), link_token); w.Code != http.StatusUnauthorized {
		t.Errorf("exchange with a wrong verifier: status = %d, want 401", w.Code)
	}
	if owner := oauth_linked_owner("github", profile.Subject); owner != "" {
		t.Errorf("a refused exchange linked the identity to %q", owner)
	}

	// The app that began it, acting for the user it names: the link lands.
	w2 := oauth_exchange_request(restash(), verifier, link_token)
	if w2.Code != http.StatusOK {
		t.Fatalf("exchange with the right verifier and token: status = %d, want 200 (%s)", w2.Code, w2.Body.String())
	}
	if owner := oauth_linked_owner("github", profile.Subject); owner != "u-link" {
		t.Errorf("linked owner = %q, want u-link", owner)
	}
	if n, _ := sessions.rows("select 1 from ceremonies where type='oauth_exchange'"); len(n) != 0 {
		t.Errorf("%d exchange rows left, want 0 (single-use)", len(n))
	}
}

// TestOauthMobileLinkRefusesAnotherUsersIdentity: the identity is already
// somebody else's, so the exchange reports the conflict rather than moving it.
func TestOauthMobileLinkRefusesAnotherUsersIdentity(t *testing.T) {
	defer oauth_binding_setup(t)()

	db_open("db/users.db").exec(
		"insert into oauth (user, provider, subject, email, verified, name, created) values ('u-other', 'github', 'gh-taken', '', 1, '', ?)", now())

	link_session := login_create("u-link", "", "")
	link_token := auth_create_app_token("u-link", link_session, "settings")
	state, verifier := oauth_mobile_link_begin(t, link_session)
	callback, _ := oauth_callback_context(state)
	st, link_user, ok := oauth_callback_ceremony(callback, "github", state)
	if !ok {
		t.Fatal("mobile link callback was rejected")
	}
	code, err := oauth_mobile_store(st.Challenge, map[string]any{
		"link": true, "user": link_user, "provider": "github",
		"profile": map[string]any{"subject": "gh-taken", "email": "", "verified": true, "name": ""},
	})
	if err != nil {
		t.Fatalf("stash: %v", err)
	}
	if w := oauth_exchange_request(code, verifier, link_token); w.Code != http.StatusConflict {
		t.Errorf("exchange for another user's identity: status = %d, want 409", w.Code)
	}
	if owner := oauth_linked_owner("github", "gh-taken"); owner != "u-other" {
		t.Errorf("identity owner = %q, want u-other (unmoved)", owner)
	}
}

// TestOauthCallbackDestinations pins the routing table the callback switches
// on. Each row is a ceremony shape and the completion it must reach; the
// mobile/browser split is what keeps a link the app began from being written
// by a browser that cannot prove who asked for it.
func TestOauthCallbackDestinations(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     oauth_state
		link_user string
		want      string
	}{
		{"web login", oauth_state{}, "", oauth_destination_login},
		{"web link", oauth_state{}, "u-link", oauth_destination_link},
		{"mobile login", oauth_state{Mode: "mobile"}, "", oauth_destination_mobile_login},
		{"mobile link", oauth_state{Mode: "mobile"}, "u-link", oauth_destination_mobile_link},
		{"step-up", oauth_state{Mode: "reauthentication"}, "u-link", oauth_destination_reauthentication},
		{"step-up without a user", oauth_state{Mode: "reauthentication"}, "", oauth_destination_login},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := oauth_callback_destination(&test.state, test.link_user); got != test.want {
				t.Errorf("destination = %q, want %q", got, test.want)
			}
		})
	}
}
