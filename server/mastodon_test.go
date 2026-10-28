// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// A mastodon_fake is a Mastodon server: registration, the token exchange, the
// account and instance documents, statuses, timelines, notifications and media.
type mastodon_fake struct {
	server        *httptest.Server
	lock          sync.Mutex
	registrations int
	client        string
	secret        string
	redirect      string
	challenge     string
	keys          []string
	polls         int
}

func create_mastodon_fake(t *testing.T) *mastodon_fake {
	t.Helper()
	f := &mastodon_fake{}
	authorized := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Authorization") != "Bearer mtok" {
			answer(w, 401, map[string]any{"error": "The access token is invalid"})
			return false
		}
		return true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/apps", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.Form.Get("client_name") != "Mochi" || r.Form.Get("scopes") != mastodon_scopes || r.Form.Get("redirect_uris") == "" {
			answer(w, 422, map[string]any{"error": "bad registration"})
			return
		}
		f.lock.Lock()
		f.registrations++
		f.client = "cid-" + strings.Repeat("x", f.registrations)
		f.secret = "sec-" + f.client
		f.redirect = r.Form.Get("redirect_uris")
		f.lock.Unlock()
		answer(w, 200, map[string]any{"id": "1", "client_id": f.client, "client_secret": f.secret})
	})
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		f.lock.Lock()
		defer f.lock.Unlock()
		if r.Form.Get("grant_type") != "authorization_code" || r.Form.Get("code") != "code-1" || r.Form.Get("client_id") != f.client || r.Form.Get("client_secret") != f.secret || r.Form.Get("redirect_uri") != f.redirect || oauth_challenge(r.Form.Get("code_verifier")) != f.challenge {
			answer(w, 400, map[string]any{"error": "invalid_grant", "error_description": "the code or the verifier is wrong"})
			return
		}
		answer(w, 200, map[string]any{"access_token": "mtok", "token_type": "Bearer", "scope": "read write", "created_at": 1})
	})
	mux.HandleFunc("GET /api/v1/accounts/verify_credentials", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		answer(w, 200, map[string]any{"id": "1", "username": "alice", "acct": "alice", "display_name": "Alice", "url": f.server.URL + "/@alice", "avatar": f.server.URL + "/a.png"})
	})
	mux.HandleFunc("GET /api/v2/instance", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"domain": fake_host(f.server), "configuration": map[string]any{
			"statuses":          map[string]any{"max_characters": 500, "max_media_attachments": 4},
			"media_attachments": map[string]any{"supported_mime_types": []string{"image/png"}, "image_size_limit": 16777216, "video_size_limit": 103809024, "description_limit": 1500},
		}})
	})
	mux.HandleFunc("POST /api/v1/statuses", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		body := json_body(r)
		f.lock.Lock()
		f.keys = append(f.keys, r.Header.Get("Idempotency-Key"))
		f.lock.Unlock()
		answer(w, 200, map[string]any{"id": "101", "url": f.server.URL + "/@alice/101", "content": body["status"], "visibility": body["visibility"]})
	})
	mux.HandleFunc("/api/v1/statuses/", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/statuses/")
		switch {
		case id == "101" && r.Method == "PUT":
			answer(w, 200, map[string]any{"id": "101", "url": f.server.URL + "/@alice/101", "content": json_body(r)["status"]})
		case id == "101" && r.Method == "DELETE":
			answer(w, 200, map[string]any{"id": "101"})
		default:
			answer(w, 404, map[string]any{"error": "Record not found"})
		}
	})
	mux.HandleFunc("GET /api/v1/timelines/home", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		if r.URL.Query().Get("limit") != "40" {
			answer(w, 400, map[string]any{"error": "limit"})
			return
		}
		if r.URL.Query().Get("max_id") == "" {
			w.Header().Set("Link", "<"+f.server.URL+"/api/v1/timelines/home?max_id=8>; rel=\"next\", <"+f.server.URL+"/api/v1/timelines/home?min_id=9>; rel=\"prev\"")
			answer(w, 200, []any{map[string]any{"id": "9"}, map[string]any{"id": "8"}})
			return
		}
		answer(w, 200, []any{})
	})
	mux.HandleFunc("GET /api/v1/accounts/1/statuses", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		if r.URL.Query().Get("exclude_reblogs") != "true" {
			answer(w, 400, map[string]any{"error": "reblogs"})
			return
		}
		answer(w, 200, []any{map[string]any{"id": "101"}})
	})
	mux.HandleFunc("GET /api/v1/notifications", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		if r.URL.Query().Get("limit") != "30" {
			answer(w, 400, map[string]any{"error": "limit"})
			return
		}
		answer(w, 200, []any{map[string]any{"id": "n1", "type": "mention"}})
	})
	mux.HandleFunc("POST /api/v2/media", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		r.ParseMultipartForm(1 << 20)
		file, header, err := r.FormFile("file")
		if err != nil || header.Filename != "cat.png" || header.Header.Get("Content-Type") != "image/png" || r.FormValue("description") != "A cat" {
			answer(w, 422, map[string]any{"error": "bad upload"})
			return
		}
		content, _ := io.ReadAll(file)
		if string(content) != "PNG" {
			answer(w, 422, map[string]any{"error": "bad content"})
			return
		}
		answer(w, 202, map[string]any{"id": "m1", "type": "image", "url": nil})
	})
	mux.HandleFunc("GET /api/v1/media/m1", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		f.lock.Lock()
		f.polls++
		polls := f.polls
		f.lock.Unlock()
		if polls < 2 {
			w.WriteHeader(206)
			return
		}
		answer(w, 200, map[string]any{"id": "m1", "type": "image", "url": f.server.URL + "/media/m1.png"})
	})
	mux.HandleFunc("GET /api/v1/bookmarks", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		answer(w, 200, []any{map[string]any{"id": "b1"}})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// oauth_hop follows the same-origin hop a link answered, with the session, and
// returns the consent URL the browser is sent to.
func oauth_hop(t *testing.T, provider string, hop string, session string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(hop)
	if err != nil || parsed.Host != "" || parsed.Path != "/_/auth/oauth/"+provider+"/start" {
		t.Fatalf("link answered %q, want the same-origin hop", hop)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", hop, nil)
	c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	c.Params = gin.Params{{Key: "provider", Value: provider}}
	web_oauth_start(c)
	location, err := url.Parse(w.Header().Get("Location"))
	if w.Code != http.StatusFound || err != nil {
		t.Fatalf("start answered %d %q", w.Code, w.Header().Get("Location"))
	}
	return location
}

// link_return drives the callback of a link with the query given and returns
// where the browser is sent.
func link_return(provider string, query string, session string) (int, string) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "http://example.com/_/auth/oauth/"+provider+"/callback?"+query, nil)
	if session != "" {
		c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	}
	c.Params = gin.Params{{Key: "provider", Value: provider}}
	web_oauth_callback(c)
	return w.Code, w.Header().Get("Location")
}

// TestMastodonLink: a Mastodon account links by the server's consent after
// Mochi registers itself there once; the ceremony is bound to the session; the
// token, the account and the server's limits land on the row; a refused
// consent and a callback in another browser both fail safely; the sign-in
// endpoints do not offer the provider; and the account's functions post with
// the idempotency key, edit, delete, read, upload and request.
func TestMastodonLink(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_mastodon_fake(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	host := fake_host(fake.server)
	session := login_create(alice.UID, "", "")

	// A server named wrongly is a refusal for the user, not a server fault.
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("mastodon")}, linked_kwargs("server", "not a server", "target", "/settings/networks")); !refused(err, "errors.link_failed", "invalid server") {
		t.Errorf("THE POINT: a server named wrongly: %v", err)
	}

	begun := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("mastodon")}, linked_kwargs("server", "https://"+host+"/", "target", "/settings/networks")))
	hop, _ := begun["url"].(string)
	row, _ := db_open("db/networks.db").row("select id, redirect from clients where host=? and network='mastodon'", host)
	if row == nil || row_string(row, "redirect") != "http://example.com/_/auth/oauth/mastodon/callback" {
		t.Fatalf("registration row = %v", row)
	}
	consent := oauth_hop(t, "mastodon", hop, session)
	q := consent.Query()
	if consent.Host != host || consent.Path != "/oauth/authorize" || q.Get("client_id") != fake.client || q.Get("redirect_uri") != fake.redirect || q.Get("scope") != mastodon_scopes || q.Get("code_challenge_method") != "S256" || q.Get("state") == "" {
		t.Fatalf("consent = %v", consent)
	}
	fake.lock.Lock()
	fake.challenge = q.Get("code_challenge")
	fake.lock.Unlock()
	state := q.Get("state")

	// A callback presented by a browser without the session is refused, and
	// the ceremony is spent.
	other := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("mastodon")}, linked_kwargs("server", host, "target", "/settings/networks")))
	other_state := oauth_hop(t, "mastodon", other["url"].(string), session).Query().Get("state")
	if code, location := link_return("mastodon", "state="+other_state+"&code=code-1", ""); code != http.StatusFound || !strings.Contains(location, "oauth_error=state_invalid") {
		t.Errorf("callback without the session: %d %q", code, location)
	}
	if code, location := link_return("mastodon", "state="+other_state+"&code=code-1", session); !strings.Contains(location, "state_invalid") {
		t.Errorf("spent ceremony: %d %q", code, location)
	}
	fake.lock.Lock()
	registrations := fake.registrations
	fake.lock.Unlock()
	if registrations != 1 {
		t.Errorf("%d registrations for two ceremonies, want 1", registrations)
	}

	code, location := link_return("mastodon", "state="+state+"&code=code-1", session)
	if code != http.StatusFound || !strings.HasPrefix(location, "/settings/networks?linked=") {
		t.Fatalf("callback: %d %q", code, location)
	}
	id := strings.TrimPrefix(location, "/settings/networks?linked=")
	l := linked_get(alice, id)
	if l == nil || l.get("token") != "mtok" || l.get("server") != host || l.get("account") != "1" || l.identifier != "@alice@"+host || l.label != "Alice" {
		t.Fatalf("linked account = %+v", l)
	}
	limits, _ := l.data["limits"].(map[string]any)
	if limits["characters"] != float64(500) || limits["attachments"] != float64(4) || limits["description"] != float64(1500) {
		t.Errorf("limits = %v", limits)
	}
	present := decoded(t)(api_account_get(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if present["hosted"] != false || present["url"] != fake.server.URL+"/@alice" || present["limits"] == nil || present["token"] != nil {
		t.Errorf("present = %v", present)
	}

	// A refused consent goes back to the target.
	refused := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("mastodon")}, linked_kwargs("server", host, "target", "/settings/networks?tab=accounts")))
	refused_state := oauth_hop(t, "mastodon", refused["url"].(string), session).Query().Get("state")
	if _, location := link_return("mastodon", "state="+refused_state+"&error=access_denied", session); location != "/settings/networks?tab=accounts&link_error=access_denied" {
		t.Errorf("refused consent: %q", location)
	}

	// A sign-in with a network provider names the account's server
	// (network_login_test.go); without one there is nothing to begin.
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/oauth/mastodon/begin", strings.NewReader("{}"))
	c.Params = gin.Params{{Key: "provider", Value: "mastodon"}}
	web_oauth_begin(c)
	if w.Code != http.StatusBadRequest {
		t.Errorf("begin with a network provider and no server: %d", w.Code)
	}

	// A registration whose redirect has changed is made afresh.
	db_open("db/networks.db").exec("update clients set redirect='http://old/callback' where host=?", host)
	decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("mastodon")}, linked_kwargs("server", host, "target", "/settings/networks")))
	fake.lock.Lock()
	registrations = fake.registrations
	fake.lock.Unlock()
	if registrations != 2 {
		t.Errorf("%d registrations after the redirect changed, want 2", registrations)
	}

	// The account's functions.
	created := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object_dict("status", "Hello", "visibility", "public")}, linked_kwargs("key", "post-1")))
	if created["id"] != "101" || created["url"] != fake.server.URL+"/@alice/101" || created["data"].(map[string]any)["content"] != "Hello" {
		t.Errorf("create = %v", created)
	}
	decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object_dict("status", "Hello")}, linked_kwargs("key", "post-1")))
	fake.lock.Lock()
	keys := append([]string{}, fake.keys...)
	fake.lock.Unlock()
	if len(keys) != 1 || keys[0] != "post-1" {
		t.Errorf("statuses posted = %v, want one with the key", keys)
	}
	updated := decoded(t)(api_account_object_update(thread, fn, sl.Tuple{sl.String(id), sl.String("101"), object_dict("status", "Edited")}, nil))
	if updated["id"] != "101" || updated["data"].(map[string]any)["content"] != "Edited" {
		t.Errorf("update = %v", updated)
	}
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String(id), sl.String("101")}, nil); err != nil {
		t.Errorf("delete: %v", err)
	}
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String(id), sl.String("404")}, nil); err != nil {
		t.Errorf("delete of a status already gone: %v", err)
	}
	timeline := decoded(t)(api_account_timeline(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if items, _ := timeline["items"].([]any); len(items) != 2 || timeline["cursor"] != "8" {
		t.Errorf("timeline = %v", timeline)
	}
	if more := decoded(t)(api_account_timeline(thread, fn, sl.Tuple{sl.String(id)}, linked_kwargs("cursor", "8"))); more["cursor"] != "" {
		t.Errorf("timeline end = %v", more)
	}
	if posts := decoded(t)(api_account_posts(thread, fn, sl.Tuple{sl.String(id)}, nil)); len(posts["items"].([]any)) != 1 || posts["cursor"] != "101" {
		t.Errorf("posts = %v", posts)
	}
	if notifications := decoded(t)(api_account_notifications(thread, fn, sl.Tuple{sl.String(id)}, nil)); len(notifications["items"].([]any)) != 1 {
		t.Errorf("notifications = %v", notifications)
	}
	saved := mastodon_upload_wait
	mastodon_upload_wait = 5 * 1e9
	defer func() { mastodon_upload_wait = saved }()
	upload := decoded(t)(api_account_upload(thread, fn, sl.Tuple{sl.String(id), sl.Bytes("PNG"), sl.String("image/png")}, linked_kwargs("name", "cat.png", "description", "A cat")))
	if upload["id"] != "m1" || upload["url"] != fake.server.URL+"/media/m1.png" {
		t.Errorf("upload = %v", upload)
	}
	requested := decoded(t)(api_account_request(thread, fn, sl.Tuple{sl.String(id), sl.String("GET"), sl.String("v1/bookmarks")}, nil))
	if data, _ := requested["data"].([]any); len(data) != 1 {
		t.Errorf("request = %v", requested)
	}
	if tested := decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil)); tested["success"] != true {
		t.Errorf("test = %v", tested)
	}
}

// TestMastodonMobileLink: a native app's link runs the consent in the system
// browser and lands the account at the exchange, with nothing stored before.
func TestMastodonMobileLink(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_mastodon_fake(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	host := fake_host(fake.server)
	verifier := strings.Repeat("v", 43)

	begun := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("mastodon")}, linked_kwargs("server", host, "target", "/", "scheme", "mochi", "challenge", oauth_challenge(verifier))))
	consent, err := url.Parse(begun["url"].(string))
	if err != nil || consent.Host != host || begun["nonce"] == "" {
		t.Fatalf("mobile link answered %v", begun)
	}
	q := consent.Query()
	fake.lock.Lock()
	fake.challenge = q.Get("code_challenge")
	fake.lock.Unlock()
	code, location := link_return("mastodon", "state="+q.Get("state")+"&code=code-1", "")
	if code != http.StatusFound || !strings.HasPrefix(location, "mochi:oauth-link-return?") {
		t.Fatalf("mobile callback: %d %q", code, location)
	}
	returned, _ := url.Parse(location)
	if returned.Query().Get("nonce") != begun["nonce"] || returned.Query().Get("code") == "" {
		t.Errorf("deep link = %q", location)
	}
	if rows, _ := db_user(alice, "user").rows("select id from accounts where type='mastodon'"); len(rows) != 0 {
		t.Error("the account was stored before the exchange")
	}
	stash, _ := db_open("db/sessions.db").row("select data from ceremonies where id=? and type='oauth_exchange'", returned.Query().Get("code"))
	if stash == nil || !strings.Contains(row_string(stash, "data"), `"linked":true`) {
		t.Errorf("stash = %v", stash)
	}
}
