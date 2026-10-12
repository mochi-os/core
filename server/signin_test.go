// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	"mellium.im/sasl"
)

// create_signin_test_env is create_linked_test_env with alice's handle in the
// registry, hosted Matrix and XMPP accounts beside her fediverse and Bluesky
// ones, and the login app stood in by app "login1".
func create_signin_test_env(t *testing.T) *User {
	t.Helper()
	alice := create_linked_test_env(t)
	users := db_open("db/users.db")
	users.exec("create table if not exists tokens (hash text primary key not null, user text not null, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0, kind text not null default '', client text not null default '', account text not null default '', network text not null default '', jkt text not null default '', refresh text not null default '', device text not null default '', scram text not null default '')")
	db_open("db/sessions.db").exec("create table if not exists accesses (hash text primary key not null, user text not null, used integer not null default 0)")
	users.exec("insert into handles (domain, name, user, kind, created) values ('example.com', 'alice', 'u-alice', 'handle', 1)")
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('m1', 'matrix', '', '@alice:example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('x1', 'xmpp', '', 'alice@example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	saved := oauth_consent_login
	oauth_consent_login = func(a *App) bool { return a != nil && a.id == "login1" }
	t.Cleanup(func() { oauth_consent_login = saved })
	return alice
}

// login_thread is the login app's thread, granted the consent permission.
func login_thread(alice *User) *sl.Thread {
	app := create_external_app("login1")
	permission_grant(alice, app.id, "oauth/consent")
	return create_test_thread(alice, app)
}

// signin_call drives one request to the sign-in server at example.com.
func signin_call(method string, path string, body string, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
		c.Request = httptest.NewRequest(method, "http://example.com"+path, reader)
		if strings.HasPrefix(body, "{") {
			c.Request.Header.Set("Content-Type", "application/json")
		} else {
			c.Request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	} else {
		c.Request = httptest.NewRequest(method, "http://example.com"+path, nil)
	}
	for name, value := range headers {
		c.Request.Header.Set(name, value)
	}
	signin_route(c)
	var answer map[string]any
	json.Unmarshal(w.Body.Bytes(), &answer)
	return w, answer
}

// consent_id reads the request id from the redirect to the consent page.
func consent_id(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	location := w.Header().Get("Location")
	if w.Code != http.StatusFound || !strings.HasPrefix(location, "/login/oauth/consent?request=") {
		t.Fatalf("authorize answered %d %q, want the consent page", w.Code, location)
	}
	return strings.TrimPrefix(location, "/login/oauth/consent?request=")
}

// approve answers a request for an account through the login app and returns
// the code from the client's redirect.
func approve(t *testing.T, alice *User, id string, account string, feed string) (string, *url.URL) {
	t.Helper()
	thread := login_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	kwargs := []sl.Tuple{}
	if feed != "" {
		kwargs = linked_kwargs("feed", feed)
	}
	result := decoded(t)(api_oauth_approve(thread, fn, sl.Tuple{sl.String(id), sl.String(account)}, kwargs))
	location, err := url.Parse(result["url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	return location.Query().Get("code"), location
}

// validate drives signin_validate for a request carrying a token.
func validate(token string, scheme string, proof string) (*Token, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "http://example.com/api/v1/accounts/verify_credentials", nil)
	c.Request.Header.Set("Authorization", scheme+" "+token)
	if proof != "" {
		c.Request.Header.Set("DPoP", proof)
	}
	return signin_validate(c), w
}

// TestSigninMastodonClient: a Mastodon app registers, asks for a sign-in and
// is sent to the consent page; the login app reads the request and approves
// it for an account, choosing the home feed; the code becomes a lasting token
// bound to the client and the account, which the engines accept and the app
// routes do not; a wrong secret, verifier, redirect or reused code is refused;
// a refusal and a revocation go where they should; and the app's own token is
// minted with its credentials.
func TestSigninMastodonClient(t *testing.T) {
	alice := create_signin_test_env(t)
	fn := sl.NewBuiltin("test", nil)

	w, registered := signin_call("POST", "/api/v1/apps", "client_name=Tusky&redirect_uris=https%3A%2F%2Ftusky.example%2Fcb&scopes=read+write&website=https%3A%2F%2Ftusky.example", nil)
	if w.Code != 200 || registered["client_id"] == nil || registered["client_secret"] == nil {
		t.Fatalf("registration: %d %v", w.Code, registered)
	}
	client, secret := registered["client_id"].(string), registered["client_secret"].(string)
	if _, bad := signin_call("POST", "/api/v1/apps", "client_name=&redirect_uris=x", nil); bad["error"] == nil {
		t.Error("a registration without a name or a valid redirect was taken")
	}

	verifier := strings.Repeat("v", 43)
	authorize := "/oauth/authorize?client_id=" + client + "&redirect_uri=https%3A%2F%2Ftusky.example%2Fcb&response_type=code&scope=read+write&state=s1&code_challenge=" + oauth_challenge(verifier) + "&code_challenge_method=S256"
	w, _ = signin_call("GET", authorize, "", nil)
	id := consent_id(t, w)
	if w, answer := signin_call("GET", "/oauth/authorize?client_id="+client+"&redirect_uri=https%3A%2F%2Fevil.example%2Fcb&response_type=code", "", nil); w.Code != 400 || answer["error"] != "invalid_request" {
		t.Errorf("an unregistered redirect: %d %v", w.Code, answer)
	}
	if w, _ := signin_call("GET", "/oauth/authorize?client_id="+client+"&redirect_uri=https%3A%2F%2Ftusky.example%2Fcb&response_type=token", "", nil); w.Code != 302 || !strings.Contains(w.Header().Get("Location"), "tusky.example/cb?") || !strings.Contains(w.Header().Get("Location"), "error=invalid_request") {
		t.Errorf("a wrong response type at a registered redirect: %d %q", w.Code, w.Header().Get("Location"))
	}
	if w, answer := signin_call("GET", "/oauth/authorize?client_id=nobody&redirect_uri=https%3A%2F%2Ftusky.example%2Fcb&response_type=code", "", nil); w.Code != 400 || answer["error"] != "invalid_client" {
		t.Errorf("an unknown client: %d %v", w.Code, answer)
	}

	// The consent page reads the request.
	request := decoded(t)(api_oauth_request(login_thread(alice), fn, sl.Tuple{sl.String(id)}, nil))
	if cl, _ := request["client"].(map[string]any); cl["name"] != "Tusky" || request["network"] != "fediverse" || request["writes"] != true || request["scope"] != "read write" {
		t.Errorf("request = %v", request)
	}
	if accounts, _ := request["accounts"].([]any); len(accounts) != 1 || accounts[0].(map[string]any)["id"] != "f1" || accounts[0].(map[string]any)["home"] != "" {
		t.Errorf("accounts = %v", request["accounts"])
	}
	other := create_test_thread(alice, create_external_app("other"))
	permission_grant(alice, "other", "oauth/consent")
	if _, err := api_oauth_request(other, fn, sl.Tuple{sl.String(id)}, nil); err == nil || !strings.Contains(err.Error(), "login app") {
		t.Errorf("another app read the request: %v", err)
	}
	if _, err := api_oauth_approve(login_thread(alice), fn, sl.Tuple{sl.String(id), sl.String("b1")}, nil); err == nil || !strings.Contains(err.Error(), "fediverse") {
		t.Errorf("approving for an account on another network: %v", err)
	}
	feed := strings.Repeat("f", 50)
	code, location := approve(t, alice, id, "f1", feed)
	if location.Host != "tusky.example" || location.Query().Get("state") != "s1" || location.Query().Get("iss") != "http://example.com" || code == "" {
		t.Fatalf("approval redirect = %v", location)
	}
	if home := account_data(account_hosted_get(alice, "f1"))["home"]; home != feed {
		t.Errorf("home feed = %v, want %s", home, feed)
	}
	if value, err := api_oauth_request(login_thread(alice), fn, sl.Tuple{sl.String(id)}, nil); err != nil || value != sl.None {
		t.Errorf("an answered request still reads: %v %v", value, err)
	}

	// The token exchange.
	form := "grant_type=authorization_code&code=" + code + "&redirect_uri=https%3A%2F%2Ftusky.example%2Fcb&client_id=" + client + "&client_secret="
	if w, answer := signin_call("POST", "/oauth/token", form+"wrong&code_verifier="+verifier, nil); w.Code != 401 || answer["error"] != "invalid_client" {
		t.Errorf("wrong secret: %d %v", w.Code, answer)
	}
	if w, answer := signin_call("POST", "/oauth/token", form+secret+"&code_verifier=wrong", nil); w.Code != 400 || answer["error"] != "invalid_grant" {
		t.Errorf("wrong verifier: %d %v", w.Code, answer)
	}
	if _, answer := signin_call("POST", "/oauth/token", form+secret+"&code_verifier="+verifier, nil); answer["error"] != "invalid_grant" {
		t.Errorf("a code refused once is dead, but: %v", answer)
	}
	// The verifier mismatch spent the code: run the sign-in again for a token.
	w, _ = signin_call("GET", authorize, "", nil)
	code, _ = approve(t, alice, consent_id(t, w), "f1", "")
	w, token := signin_call("POST", "/oauth/token", "grant_type=authorization_code&code="+code+"&redirect_uri=https%3A%2F%2Ftusky.example%2Fcb&client_id="+client+"&client_secret="+secret+"&code_verifier="+verifier, nil)
	if w.Code != 200 || token["token_type"] != "Bearer" || token["scope"] != "read write" || token["expires_in"] != nil || token["refresh_token"] != nil {
		t.Fatalf("token = %d %v", w.Code, token)
	}
	access := token["access_token"].(string)
	if _, answer := signin_call("POST", "/oauth/token", "grant_type=authorization_code&code="+code+"&client_id="+client+"&client_secret="+secret+"&code_verifier="+verifier, nil); answer["error"] != "invalid_grant" {
		t.Errorf("a code used twice: %v", answer)
	}

	// The engines take the token; the app routes do not.
	held, _ := validate(access, "Bearer", "")
	if held == nil || held.Kind != token_signin || held.Account != "f1" || held.Client != client || held.User != alice.UID || held.Network != "fediverse" || !signin_scopes_check(held, "write:statuses") || signin_scopes_check(held, "push") {
		t.Fatalf("validated token = %+v", held)
	}
	if token_unbound(token_validate(access)) {
		t.Error("a sign-in token passes as an app token")
	}
	listed := token_list(alice.UID, "", "")
	if len(listed) != 1 || listed[0]["kind"] != token_signin || listed[0]["client"] != client || listed[0]["account"] != "f1" {
		t.Errorf("listed tokens = %v", listed)
	}
	if _, w := validate("mochi-nonsense", "Bearer", ""); w.Code != 401 {
		t.Errorf("an unknown token: %d", w.Code)
	}

	// A refusal goes back to the client.
	w, _ = signin_call("GET", authorize, "", nil)
	refused := decoded(t)(api_oauth_refuse(login_thread(alice), fn, sl.Tuple{sl.String(consent_id(t, w))}, nil))
	if !strings.Contains(refused["url"].(string), "error=access_denied") || !strings.Contains(refused["url"].(string), "state=s1") {
		t.Errorf("refusal = %v", refused)
	}

	// The app's own token, and revocation.
	w, own := signin_call("POST", "/oauth/token", "grant_type=client_credentials&client_id="+client+"&client_secret="+secret+"&scope=read", nil)
	if w.Code != 200 || own["access_token"] == nil {
		t.Errorf("client credentials: %d %v", w.Code, own)
	}
	if w, _ := signin_call("POST", "/oauth/revoke", "token="+access+"&client_id="+client+"&client_secret="+secret, nil); w.Code != 200 {
		t.Errorf("revoke: %d", w.Code)
	}
	if held, _ := validate(access, "Bearer", ""); held != nil {
		t.Error("a revoked token still validates")
	}
}

// TestSigninMatrixClient: a Matrix client registers dynamically as a public
// client, must use PKCE, and signs in to a Matrix account for an expiring
// token with a refresh token and a device; refreshing rotates both and ends
// the old; another client cannot refresh it; and the revocation takes the
// refresh token too.
func TestSigninMatrixClient(t *testing.T) {
	alice := create_signin_test_env(t)
	w, registered := signin_call("POST", "/oauth/register", `{"client_name":"Element X","client_uri":"https://element.example","redirect_uris":["https://element.example/cb"],"token_endpoint_auth_method":"none","grant_types":["authorization_code","refresh_token"],"response_types":["code"],"scope":"urn:matrix:org.matrix.msc2967.client:api:* urn:matrix:org.matrix.msc2967.client:device:ABCDEF"}`, nil)
	if w.Code != 201 || registered["client_id"] == nil || registered["client_secret"] != nil || registered["token_endpoint_auth_method"] != "none" {
		t.Fatalf("registration: %d %v", w.Code, registered)
	}
	client := registered["client_id"].(string)
	if w, answer := signin_call("POST", "/oauth/register", `{"client_name":"X","redirect_uris":["https://x.example/cb"],"grant_types":["implicit"]}`, nil); w.Code != 400 || answer["error"] != "invalid_client_metadata" {
		t.Errorf("an unsupported grant type: %d %v", w.Code, answer)
	}

	base := "/oauth/authorize?client_id=" + client + "&redirect_uri=https%3A%2F%2Felement.example%2Fcb&response_type=code&scope=urn%3Amatrix%3Aorg.matrix.msc2967.client%3Aapi%3A*&state=m1"
	if w, _ := signin_call("GET", base, "", nil); w.Code != 302 || !strings.Contains(w.Header().Get("Location"), "error=invalid_request") {
		t.Errorf("a public client without PKCE: %d %q", w.Code, w.Header().Get("Location"))
	}
	verifier := strings.Repeat("w", 43)
	w, _ = signin_call("GET", base+"&code_challenge="+oauth_challenge(verifier)+"&code_challenge_method=S256", "", nil)
	code, _ := approve(t, alice, consent_id(t, w), "m1", "")
	w, token := signin_call("POST", "/oauth/token", "grant_type=authorization_code&code="+code+"&redirect_uri=https%3A%2F%2Felement.example%2Fcb&client_id="+client+"&code_verifier="+verifier, nil)
	if w.Code != 200 || token["refresh_token"] == nil || token["expires_in"] == nil || token["device_id"] == nil || token["token_type"] != "Bearer" {
		t.Fatalf("token = %d %v", w.Code, token)
	}
	access, refresh, device := token["access_token"].(string), token["refresh_token"].(string), token["device_id"].(string)
	if held, _ := validate(access, "Bearer", ""); held == nil || held.Network != "matrix" || held.Device != device || held.Account != "m1" {
		t.Fatalf("validated = %+v", held)
	}

	// Another client cannot refresh it; the client can, once.
	w, other := signin_call("POST", "/oauth/register", `{"client_name":"Other","redirect_uris":["https://other.example/cb"]}`, nil)
	if w, answer := signin_call("POST", "/oauth/token", "grant_type=refresh_token&refresh_token="+refresh+"&client_id="+other["client_id"].(string), nil); w.Code != 400 || answer["error"] != "invalid_grant" {
		t.Errorf("another client's refresh: %d %v", w.Code, answer)
	}
	w, renewed := signin_call("POST", "/oauth/token", "grant_type=refresh_token&refresh_token="+refresh+"&client_id="+client, nil)
	if w.Code != 200 || renewed["access_token"] == access || renewed["refresh_token"] == refresh || renewed["device_id"] != device {
		t.Fatalf("refresh = %d %v", w.Code, renewed)
	}
	if held, _ := validate(access, "Bearer", ""); held != nil {
		t.Error("the old access token survived the refresh")
	}
	if _, answer := signin_call("POST", "/oauth/token", "grant_type=refresh_token&refresh_token="+refresh+"&client_id="+client, nil); answer["error"] != "invalid_grant" {
		t.Errorf("the old refresh token still works: %v", answer)
	}
	if held, _ := validate(renewed["access_token"].(string), "Bearer", ""); held == nil || held.Device != device {
		t.Error("the new access token does not validate")
	}
	if w, _ := signin_call("POST", "/oauth/revoke", "token="+renewed["refresh_token"].(string)+"&client_id="+client, nil); w.Code != 200 {
		t.Errorf("revoke by refresh token: %d", w.Code)
	}
	if held, _ := validate(renewed["access_token"].(string), "Bearer", ""); held != nil {
		t.Error("revoking by the refresh token left the access token")
	}
}

// bluesky_client_fake serves an AT Protocol client's document and makes its
// proofs.
type bluesky_client_fake struct {
	server *httptest.Server
	key    *ecdsa.PrivateKey
	id     string
}

func create_bluesky_client_fake(t *testing.T) *bluesky_client_fake {
	t.Helper()
	f := &bluesky_client_fake{}
	_, private, _ := key_generate(key_p256)
	f.key, _ = key_p256_private(private)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /client-metadata.json", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"client_id": f.id, "client_name": "Skies", "client_uri": f.server.URL, "redirect_uris": []string{f.server.URL + "/cb"}, "scope": bluesky_scope, "dpop_bound_access_tokens": true, "token_endpoint_auth_method": "none", "grant_types": []string{"authorization_code", "refresh_token"}, "response_types": []string{"code"}})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	f.id = f.server.URL + "/client-metadata.json"
	return f
}

// proof makes a DPoP proof for a request to the sign-in server with the nonce
// it last gave.
func (f *bluesky_client_fake) proof(t *testing.T, method string, path string, token string) string {
	t.Helper()
	proof, err := bluesky_proof(f.key, method, "http://example.com"+path, token)
	if err != nil {
		t.Fatal(err)
	}
	return proof
}

// TestSigninBlueskyClient: an AT Protocol client is known by its document,
// pushes its request with a proof after taking up the server's nonce, sends
// the user to the consent page by the request's URI, and exchanges the code
// with the same key for tokens bound to it; a request made with a proof by
// another key, or without the token's hash, is refused; and the refresh is
// bound too.
func TestSigninBlueskyClient(t *testing.T) {
	alice := create_signin_test_env(t)
	f := create_bluesky_client_fake(t)
	verifier := strings.Repeat("b", 43)
	form := "client_id=" + url.QueryEscape(f.id) + "&redirect_uri=" + url.QueryEscape(f.server.URL+"/cb") + "&response_type=code&scope=" + url.QueryEscape(bluesky_scope) + "&state=bs&code_challenge=" + oauth_challenge(verifier) + "&code_challenge_method=S256&login_hint=alice.example.com"

	// Without a proof the push is refused; without the nonce it is asked for.
	if w, answer := signin_call("POST", "/oauth/par", form, nil); w.Code != 400 || answer["error"] != "invalid_dpop_proof" {
		t.Errorf("push without a proof: %d %v", w.Code, answer)
	}
	bluesky_nonce_set("example.com", "")
	w, answer := signin_call("POST", "/oauth/par", form, map[string]string{"DPoP": f.proof(t, "POST", "/oauth/par", "")})
	if w.Code != 400 || answer["error"] != "use_dpop_nonce" || w.Header().Get("DPoP-Nonce") == "" {
		t.Fatalf("push without the nonce: %d %v", w.Code, answer)
	}
	bluesky_nonce_set("example.com", w.Header().Get("DPoP-Nonce"))
	w, pushed := signin_call("POST", "/oauth/par", form, map[string]string{"DPoP": f.proof(t, "POST", "/oauth/par", "")})
	if w.Code != 201 || pushed["request_uri"] == nil {
		t.Fatalf("push: %d %v", w.Code, pushed)
	}
	if w, answer := signin_call("POST", "/oauth/par", form, map[string]string{"DPoP": f.proof(t, "POST", "/oauth/token", "")}); w.Code != 400 || answer["error"] != "invalid_dpop_proof" {
		t.Errorf("a proof for another path: %d %v", w.Code, answer)
	}

	w, _ = signin_call("GET", "/oauth/authorize?client_id="+url.QueryEscape(f.id)+"&request_uri="+url.QueryEscape(pushed["request_uri"].(string)), "", nil)
	id := consent_id(t, w)
	if w, answer := signin_call("GET", "/oauth/authorize?request_uri="+url.QueryEscape(pushed["request_uri"].(string)), "", nil); w.Code != 400 || answer["error"] != "invalid_request" {
		t.Errorf("a pushed request taken twice: %d %v", w.Code, answer)
	}
	request := decoded(t)(api_oauth_request(login_thread(alice), sl.NewBuiltin("test", nil), sl.Tuple{sl.String(id)}, nil))
	accounts, _ := request["accounts"].([]any)
	if cl, _ := request["client"].(map[string]any); cl["name"] != "Skies" || request["network"] != "bluesky" || len(accounts) != 1 || accounts[0].(map[string]any)["hinted"] != true {
		t.Errorf("request = %v", request)
	}
	code, location := approve(t, alice, id, "b1", "")
	if location.Host != fake_host(f.server) || location.Query().Get("iss") != "http://example.com" {
		t.Fatalf("approval redirect = %v", location)
	}

	// The exchange needs the same key.
	exchange := "grant_type=authorization_code&code=" + code + "&redirect_uri=" + url.QueryEscape(f.server.URL+"/cb") + "&client_id=" + url.QueryEscape(f.id) + "&code_verifier=" + verifier
	other := create_bluesky_client_fake(t)
	if w, answer := signin_call("POST", "/oauth/token", exchange, map[string]string{"DPoP": other.proof(t, "POST", "/oauth/token", "")}); w.Code != 400 || answer["error"] != "invalid_dpop_proof" {
		t.Fatalf("exchange with another key: %d %v", w.Code, answer)
	}
	// That refusal spent the code; sign in again.
	w, pushed = signin_call("POST", "/oauth/par", form, map[string]string{"DPoP": f.proof(t, "POST", "/oauth/par", "")})
	w, _ = signin_call("GET", "/oauth/authorize?client_id="+url.QueryEscape(f.id)+"&request_uri="+url.QueryEscape(pushed["request_uri"].(string)), "", nil)
	code, _ = approve(t, alice, consent_id(t, w), "b1", "")
	exchange = "grant_type=authorization_code&code=" + code + "&redirect_uri=" + url.QueryEscape(f.server.URL+"/cb") + "&client_id=" + url.QueryEscape(f.id) + "&code_verifier=" + verifier
	w, token := signin_call("POST", "/oauth/token", exchange, map[string]string{"DPoP": f.proof(t, "POST", "/oauth/token", "")})
	if w.Code != 200 || token["token_type"] != "DPoP" || token["refresh_token"] == nil || token["sub"] != "https://example.com/~b1" || token["expires_in"] == nil {
		t.Fatalf("token = %d %v", w.Code, token)
	}
	access := token["access_token"].(string)
	thumbprint := jwk_thumbprint(bluesky_jwk(&f.key.PublicKey))

	// A request proves possession with the token's hash.
	held, _ := validate(access, "DPoP", f.proof(t, "GET", "/api/v1/accounts/verify_credentials", access))
	if held == nil || held.JKT != thumbprint || held.Account != "b1" || held.Network != "bluesky" || !signin_scopes_check(held, "repo:app.bsky.feed.post") {
		t.Fatalf("validated = %+v", held)
	}
	if held, w := validate(access, "DPoP", ""); held != nil || w.Code != 400 {
		t.Error("a bound token without a proof was accepted")
	}
	if held, _ := validate(access, "DPoP", other.proof(t, "GET", "/api/v1/accounts/verify_credentials", access)); held != nil {
		t.Error("a proof by another key was accepted")
	}
	if held, _ := validate(access, "DPoP", f.proof(t, "GET", "/api/v1/accounts/verify_credentials", "")); held != nil {
		t.Error("a proof without the token's hash was accepted")
	}
	if held, _ := validate(access, "DPoP", f.proof(t, "POST", "/api/v1/accounts/verify_credentials", access)); held != nil {
		t.Error("a proof for another method was accepted")
	}
	replay := f.proof(t, "GET", "/api/v1/accounts/verify_credentials", access)
	if held, _ := validate(access, "DPoP", replay); held == nil {
		t.Error("a fresh proof was refused")
	}
	if held, _ := validate(access, "DPoP", replay); held != nil {
		t.Error("a replayed proof was accepted")
	}

	// The refresh is bound to the key too.
	refresh := "grant_type=refresh_token&refresh_token=" + token["refresh_token"].(string) + "&client_id=" + url.QueryEscape(f.id)
	if w, answer := signin_call("POST", "/oauth/token", refresh, map[string]string{"DPoP": other.proof(t, "POST", "/oauth/token", "")}); w.Code != 400 || answer["error"] != "invalid_dpop_proof" {
		t.Errorf("refresh with another key: %d %v", w.Code, answer)
	}
	w, renewed := signin_call("POST", "/oauth/token", refresh, map[string]string{"DPoP": f.proof(t, "POST", "/oauth/token", "")})
	if w.Code != 200 || renewed["token_type"] != "DPoP" || renewed["sub"] != "https://example.com/~b1" {
		t.Fatalf("refresh = %d %v", w.Code, renewed)
	}
	if held, _ := validate(renewed["access_token"].(string), "DPoP", f.proof(t, "GET", "/api/v1/accounts/verify_credentials", renewed["access_token"].(string))); held == nil || held.JKT != thumbprint {
		t.Error("the renewed token is not bound to the key")
	}
}

// TestSigninDocuments: the server's documents name its endpoints on the host
// asked; another host, no domain or the switch off answer not found; and
// browsers from anywhere may call.
func TestSigninDocuments(t *testing.T) {
	create_signin_test_env(t)
	w, document := signin_call("GET", "/.well-known/oauth-authorization-server", "", nil)
	if w.Code != 200 || document["issuer"] != "http://example.com" || document["token_endpoint"] != "http://example.com/oauth/token" || document["pushed_authorization_request_endpoint"] != "http://example.com/oauth/par" || document["client_id_metadata_document_supported"] != true {
		t.Errorf("document = %d %v", w.Code, document)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("no open CORS")
	}
	w, resource := signin_call("GET", "/.well-known/oauth-protected-resource", "", nil)
	if w.Code != 200 || resource["resource"] != "http://example.com" {
		t.Errorf("resource = %d %v", w.Code, resource)
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "http://other.example/.well-known/oauth-authorization-server", nil)
	signin_route(c)
	if rec.Code != 404 {
		t.Errorf("another host: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("OPTIONS", "http://example.com/oauth/token", nil)
	signin_route(c)
	if rec.Code != 204 || rec.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Errorf("preflight: %d", rec.Code)
	}
	networks_development = false
	if w, _ := signin_call("GET", "/.well-known/oauth-authorization-server", "", nil); w.Code != 404 {
		t.Errorf("with the switch off: %d", w.Code)
	}
	networks_development = true
	server_domain_name = ""
	if w, _ := signin_call("POST", "/oauth/token", "grant_type=client_credentials", nil); w.Code != 404 {
		t.Errorf("without a domain: %d", w.Code)
	}
	server_domain_name = "Example.com"
}

// TestPasswords: a minted password opens the account its address names on
// each network and no other, carries the device's name, keeps the SCRAM forms
// an XMPP server needs, is listed and revoked as a token of the app that
// minted it, and is refused on a wrong password, another account or a
// non-hosted one.
func TestPasswords(t *testing.T) {
	alice := create_signin_test_env(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	permission_grant(alice, "app1", "tokens/create")
	mint := func(account string, name string) string {
		value, err := api_account_password(thread, fn, sl.Tuple{sl.String(account), sl.String(name)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return string(value.(sl.String))
	}
	phone := mint("f1", "Phone")
	laptop := mint("x1", "Laptop")
	if !strings.HasPrefix(phone, "mochi-") || phone == laptop {
		t.Fatalf("passwords = %q %q", phone, laptop)
	}
	if _, err := api_account_password(thread, fn, sl.Tuple{sl.String("f1"), sl.String("")}, nil); err == nil {
		t.Error("a password without a device name was minted")
	}
	if _, err := api_account_password(thread, fn, sl.Tuple{sl.String("nope"), sl.String("x")}, nil); err == nil {
		t.Error("a password for no account was minted")
	}

	if user, row, name := password_check("fediverse", "@Alice@Example.com", phone); user == nil || row["id"] != "f1" || name != "Phone" {
		t.Errorf("fediverse check = %v %v %q", user, row, name)
	}
	if user, _, _ := password_check("fediverse", "alice@example.com", laptop); user != nil {
		t.Error("another account's password opened the fediverse account")
	}
	if user, _, _ := password_check("fediverse", "alice@example.com", "mochi-wrong"); user != nil {
		t.Error("a wrong password opened the account")
	}
	if user, _, _ := password_check("fediverse", "bob@example.com", phone); user != nil {
		t.Error("an unknown handle opened an account")
	}
	if user, row, _ := password_check("xmpp", "xmpp:alice@example.com", laptop); user == nil || row["id"] != "x1" {
		t.Error("the XMPP account did not open")
	}
	matrix := mint("m1", "Tablet")
	if user, row, _ := password_check("matrix", "@alice:example.com", matrix); user == nil || row["id"] != "m1" {
		t.Error("the Matrix account did not open")
	}
	sky := mint("b1", "App")
	if user, row, _ := password_check("bluesky", "Alice.Example.com", sky); user == nil || row["id"] != "b1" {
		t.Error("the Bluesky account did not open by handle")
	}
	db_user(alice, "user").account_set("b1", map[string]any{"data": `{"hosted":true,"handle":"alice","domain":"example.com","did":"did:plc:alice"}`})
	if user, row, _ := password_check("bluesky", "did:plc:alice", sky); user == nil || row["id"] != "b1" {
		t.Error("the Bluesky account did not open by DID")
	}

	forms := password_scram("xmpp", "alice@example.com", "sha256")
	if len(forms) != 1 {
		t.Fatalf("%d SCRAM forms, want 1", len(forms))
	}
	salt, _ := forms[0][0].([]byte)
	iterations, _ := forms[0][1].(int)
	key, _ := forms[0][2].([]byte)
	if want := sasl.SCRAMSaltPassword(sha256.New, []byte(laptop), salt, iterations); iterations != password_scram_iterations || string(key) != string(want) || len(salt) == 0 {
		t.Error("the SCRAM form is not the salted password")
	}
	if forms := password_scram("xmpp", "nobody@example.com", "sha256"); len(forms) != 0 {
		t.Error("SCRAM forms for an unknown address")
	}

	listed := token_list(alice.UID, "app1", "")
	if len(listed) != 4 || listed[0]["kind"] != token_password || listed[0]["account"] == nil {
		t.Errorf("listed = %v", listed)
	}
	if token_unbound(token_lookup(phone)) {
		t.Error("a password passes as an app token")
	}
	if held, _ := validate(phone, "Bearer", ""); held != nil {
		t.Error("a password passes as a sign-in token")
	}
	if value, err := api_token_delete(thread, fn, sl.Tuple{sl.String(phone)}, nil); err != nil || value != sl.True {
		t.Fatalf("delete = %v %v", value, err)
	}
	if user, _, _ := password_check("fediverse", "alice@example.com", phone); user != nil {
		t.Error("a revoked password opened the account")
	}
}

// TestNumbers: an item gets one number per user, kept, and a number names
// its item of the kind asked and no other.
func TestNumbers(t *testing.T) {
	alice := create_signin_test_env(t)
	first := number_for(alice, "post", "https://example.com/~f1/abc")
	if first == 0 || number_for(alice, "post", "https://example.com/~f1/abc") != first {
		t.Fatalf("the first number = %d, and did not keep", first)
	}
	second := number_for(alice, "post", "https://example.com/~f1/def")
	if second != first+1 {
		t.Errorf("the second number = %d, want %d", second, first+1)
	}
	if number_for(alice, "comment", "https://example.com/~f1/abc") == first {
		t.Error("a comment took a post's number")
	}
	if number_identifier(alice, "post", second) != "https://example.com/~f1/def" || number_identifier(alice, "comment", second) != "" || number_identifier(alice, "post", 99) != "" {
		t.Error("numbers do not name their items")
	}
}
