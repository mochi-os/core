// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.mau.fi/util/dbutil"
	sl "go.starlark.net/starlark"
)

// create_linked_test_env is create_delivery_test_env with the sessions tables
// a link ceremony needs, a plain scheme to the test servers, and fresh
// session, server and nonce registries. It stops every session it started and
// restores what it changed.
func create_linked_test_env(t *testing.T) *User {
	t.Helper()
	alice := create_delivery_test_env(t)
	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists sessions (user text not null, code text not null primary key, secret text not null, expires integer not null, created integer not null, accessed integer not null, address text not null default '', agent text not null default '')")
	sessions.exec("create table if not exists ceremonies (id text primary key, type text not null, user text not null default '', challenge blob not null, data text not null default '', expires integer not null)")

	saved_scheme, saved_sessions, saved_servers, saved_nonces, saved_inbound := network_scheme, linked_sessions, bluesky_servers, bluesky_nonces, linked_inbound
	network_scheme = "http://"
	linked_sessions = map[string]*linked_session{}
	bluesky_servers = map[string]*bluesky_server{}
	bluesky_nonces = map[string]string{}
	t.Cleanup(func() {
		linked_lock.Lock()
		var running []*linked_session
		for _, s := range linked_sessions {
			running = append(running, s)
		}
		linked_sessions = map[string]*linked_session{}
		linked_lock.Unlock()
		// The sessions end before their databases go with the test.
		linked_end(running, 5*time.Second)
		network_scheme, linked_sessions, bluesky_servers, bluesky_nonces, linked_inbound = saved_scheme, saved_sessions, saved_servers, saved_nonces, saved_inbound
		encryption_reset()
	})
	return alice
}

// encryption_reset waits for the devices' work under way and forgets the
// devices held, whose databases go with the test.
func encryption_reset() {
	done := make(chan struct{})
	go func() {
		encryption_running.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
	encryption_lock.Lock()
	for _, d := range encryption_matrices {
		d.end()
	}
	encryption_matrices = map[string]*encryption_matrix{}
	encryption_stores = map[string]*dbutil.Database{}
	encryption_lock.Unlock()
	omemo_lock.Lock()
	omemo_devices = map[string]*omemo_device{}
	omemo_lock.Unlock()
	omemo_occupants_lock.Lock()
	omemo_occupants = map[string]map[string]string{}
	omemo_occupants_lock.Unlock()
	encryption_made.Clear()
}

// linked_thread is a thread of an external app granted the account
// permissions, with a request context for a ceremony to begin in.
func linked_thread(alice *User) *sl.Thread {
	app := create_external_app("app1")
	for _, permission := range []string{"accounts/read", "accounts/write", "account:*"} {
		permission_grant(alice, app.id, permission)
	}
	thread := create_test_thread(alice, app)
	thread.SetLocal("context", context.Background())
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "http://example.com/settings/-/networks/link", nil)
	thread.SetLocal("action", &Action{web: c})
	return thread
}

// linked_kwargs builds Starlark keyword arguments from pairs.
func linked_kwargs(pairs ...string) []sl.Tuple {
	var kwargs []sl.Tuple
	for i := 0; i+1 < len(pairs); i += 2 {
		kwargs = append(kwargs, sl.Tuple{sl.String(pairs[i]), sl.String(pairs[i+1])})
	}
	return kwargs
}

// decoded answers a function that decodes a Starlark answer into a map,
// failing the test on an error: decoded(t)(api_account_get(...)).
func decoded(t *testing.T) func(sl.Value, error) map[string]any {
	return func(value sl.Value, err error) map[string]any {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		result, _ := sl_decode(value).(map[string]any)
		if result == nil {
			t.Fatalf("answer %v is not a dict", value)
		}
		return result
	}
}

// An inbound_log records what linked_inbound was handed.
type inbound_log struct {
	lock   sync.Mutex
	events []map[string]any
}

func (log *inbound_log) install(t *testing.T) {
	t.Helper()
	linked_inbound = func(user *User, account string, network string, events []map[string]any) {
		log.lock.Lock()
		for _, event := range events {
			event["account"] = account
			event["network"] = network
			log.events = append(log.events, event)
		}
		log.lock.Unlock()
	}
}

// find answers the first event of a kind matching the condition, waiting for
// it.
func (log *inbound_log) find(t *testing.T, kind string, condition func(map[string]any) bool) map[string]any {
	t.Helper()
	var found map[string]any
	wait(t, "a "+kind+" event", func() bool {
		log.lock.Lock()
		defer log.lock.Unlock()
		for _, event := range log.events {
			if event["kind"] == kind && (condition == nil || condition(event)) {
				found = event
				return true
			}
		}
		return false
	})
	return found
}

// wait polls until the condition holds or two seconds pass.
func wait(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waited in vain for %s", what)
}

// json_body decodes a request body.
func json_body(r *http.Request) map[string]any {
	var body map[string]any
	data, _ := io.ReadAll(r.Body)
	json.Unmarshal(data, &body)
	return body
}

// answer writes a JSON answer.
func answer(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

// TestLinkedProviders: the network providers are offered with how an existing
// account is linked while the development switch is on, and not at all with it
// off; Bluesky links by OAuth with a configured domain and by app password
// without; and a hosted account is listed under the network, a linked one
// under its social or chat capability.
func TestLinkedProviders(t *testing.T) {
	alice := create_linked_test_env(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)

	providers := func() map[string]map[string]any {
		value, err := api_account_providers(thread, fn, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]map[string]any{}
		for _, item := range sl_decode(value).([]any) {
			p := item.(map[string]any)
			out[p["type"].(string)] = p
		}
		return out
	}
	listed := providers()
	mastodon, _ := listed["mastodon"]["link"].(map[string]any)
	if mastodon == nil || mastodon["flow"] != "oauth" || listed["mastodon"]["flow"] != "link" {
		t.Errorf("mastodon = %v", listed["mastodon"])
	}
	lemmy, _ := listed["lemmy"]["link"].(map[string]any)
	if lemmy == nil || lemmy["flow"] != "form" || len(lemmy["fields"].([]any)) != 4 {
		t.Errorf("lemmy = %v", listed["lemmy"])
	}
	if fields := lemmy["fields"].([]any); fields[0].(map[string]any)["name"] != "server" || fields[0].(map[string]any)["label"] == "" {
		t.Errorf("lemmy field = %v", fields[0])
	}
	bluesky, _ := listed["bluesky"]["link"].(map[string]any)
	if bluesky == nil || bluesky["flow"] != "oauth" || listed["bluesky"]["flow"] != "handle" {
		t.Errorf("bluesky with a domain = %v", listed["bluesky"])
	}
	for _, kind := range []string{"matrix", "xmpp"} {
		link, _ := listed[kind]["link"].(map[string]any)
		if link == nil || link["flow"] != "form" {
			t.Errorf("%s = %v", kind, listed[kind])
		}
	}
	if _, listed := listed["fediverse"]["link"]; listed {
		t.Error("a hosted fediverse account offers a link")
	}

	saved := server_domain_name
	server_domain_name = ""
	bluesky, _ = providers()["bluesky"]["link"].(map[string]any)
	server_domain_name = saved
	if bluesky == nil || bluesky["flow"] != "form" || len(bluesky["fields"].([]any)) != 2 {
		t.Errorf("bluesky without a domain = %v", bluesky)
	}

	networks_development = false
	off := providers()
	networks_development = true
	for _, kind := range []string{"mastodon", "lemmy", "bluesky", "matrix", "xmpp", "fediverse"} {
		if _, found := off[kind]; found {
			t.Errorf("%s offered with the development switch off", kind)
		}
	}
	if _, found := off["email"]; !found {
		t.Error("email missing with the development switch off")
	}

	hosted := map[string]any{"type": "bluesky", "data": `{"hosted":true}`}
	linked := map[string]any{"type": "bluesky", "data": `{"did":"did:plc:x"}`}
	if got := account_capabilities(hosted); len(got) != 1 || got[0] != "network" {
		t.Errorf("hosted capabilities = %v", got)
	}
	if got := account_capabilities(linked); len(got) != 1 || got[0] != "social" {
		t.Errorf("linked capabilities = %v", got)
	}
	if got := account_capabilities(map[string]any{"type": "email"}); len(got) != 1 || got[0] != "notify" {
		t.Errorf("email capabilities = %v", got)
	}
}

// A lemmy_fake is a Lemmy server on its 0.19 interface.
type lemmy_fake struct {
	server  *httptest.Server
	lock    sync.Mutex
	posts   []map[string]any
	revoked bool
	uploads int
}

func create_lemmy_fake(t *testing.T) *lemmy_fake {
	t.Helper()
	f := &lemmy_fake{}
	authorized := func(r *http.Request) bool {
		f.lock.Lock()
		defer f.lock.Unlock()
		return !f.revoked && r.Header.Get("Authorization") == "Bearer tok1"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v3/user/login", func(w http.ResponseWriter, r *http.Request) {
		body := json_body(r)
		if body["username_or_email"] == "alice" && body["password"] == "secret" && (body["totp_2fa_token"] == nil || body["totp_2fa_token"] == "123456") {
			answer(w, 200, map[string]any{"jwt": "tok1"})
			return
		}
		answer(w, 400, map[string]any{"error": "incorrect_login"})
	})
	mux.HandleFunc("GET /api/v3/site", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r) {
			answer(w, 401, map[string]any{"error": "not_logged_in"})
			return
		}
		answer(w, 200, map[string]any{"my_user": map[string]any{"local_user_view": map[string]any{"person": map[string]any{
			"id": 7, "name": "alice", "display_name": "Alice", "actor_id": f.server.URL + "/u/alice", "avatar": f.server.URL + "/a.png",
		}}}})
	})
	mux.HandleFunc("/api/v3/post", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r) {
			answer(w, 401, map[string]any{"error": "not_logged_in"})
			return
		}
		body := json_body(r)
		f.lock.Lock()
		defer f.lock.Unlock()
		switch r.Method {
		case "POST":
			f.posts = append(f.posts, body)
			answer(w, 200, map[string]any{"post_view": map[string]any{"post": map[string]any{"id": 42, "name": body["name"], "ap_id": f.server.URL + "/post/42"}}})
		case "PUT":
			answer(w, 200, map[string]any{"post_view": map[string]any{"post": map[string]any{"id": body["post_id"], "name": body["name"], "ap_id": f.server.URL + "/post/42"}}})
		}
	})
	mux.HandleFunc("POST /api/v3/post/delete", func(w http.ResponseWriter, r *http.Request) {
		body := json_body(r)
		if body["deleted"] != true {
			answer(w, 400, map[string]any{"error": "not deleted"})
			return
		}
		answer(w, 200, map[string]any{"post_view": map[string]any{"post": map[string]any{"id": body["post_id"], "deleted": true}}})
	})
	mux.HandleFunc("GET /api/v3/post/list", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r) {
			answer(w, 401, map[string]any{"error": "not_logged_in"})
			return
		}
		q := r.URL.Query()
		if q.Get("type_") != "Subscribed" || q.Get("limit") != "40" {
			answer(w, 400, map[string]any{"error": "bad query " + r.URL.RawQuery})
			return
		}
		if q.Get("page_cursor") == "" {
			answer(w, 200, map[string]any{"posts": []any{map[string]any{"post": map[string]any{"id": 1}}, map[string]any{"post": map[string]any{"id": 2}}}, "next_page": "P2"})
			return
		}
		answer(w, 200, map[string]any{"posts": []any{}})
	})
	mux.HandleFunc("GET /api/v3/user", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("person_id") != "7" {
			answer(w, 400, map[string]any{"error": "bad person"})
			return
		}
		if r.URL.Query().Get("page") == "1" {
			answer(w, 200, map[string]any{"posts": []any{map[string]any{"post": map[string]any{"id": 42}}}, "comments": []any{map[string]any{"comment": map[string]any{"id": 9}}}})
			return
		}
		answer(w, 200, map[string]any{"posts": []any{}, "comments": []any{}})
	})
	mux.HandleFunc("GET /api/v3/user/replies", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"replies": []any{map[string]any{"comment_reply": map[string]any{"id": 1}}}})
	})
	mux.HandleFunc("GET /api/v3/user/mention", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"mentions": []any{map[string]any{"person_mention": map[string]any{"id": 2}}}})
	})
	mux.HandleFunc("GET /api/v3/community/list", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"communities": []any{map[string]any{"community": map[string]any{"id": 5, "name": r.URL.Query().Get("sort")}}}})
	})
	mux.HandleFunc("POST /pictrs/image", func(w http.ResponseWriter, r *http.Request) {
		cookie, _ := r.Cookie("jwt")
		if cookie == nil || cookie.Value != "tok1" || !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
			answer(w, 401, map[string]any{"error": "no cookie"})
			return
		}
		r.ParseMultipartForm(1 << 20)
		file, header, err := r.FormFile("images[]")
		if err != nil {
			answer(w, 400, map[string]any{"error": err.Error()})
			return
		}
		content, _ := io.ReadAll(file)
		if header.Filename != "cat.png" || string(content) != "PNG" {
			answer(w, 400, map[string]any{"error": "bad file"})
			return
		}
		f.lock.Lock()
		f.uploads++
		f.lock.Unlock()
		answer(w, 201, map[string]any{"files": []any{map[string]any{"file": "abc.png", "delete_token": "d"}}})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// fake_host is the host of a test server.
func fake_host(server *httptest.Server) string {
	u, _ := url.Parse(server.URL)
	return u.Host
}

// TestLinkedLemmy: a Lemmy account links by a form, the token is kept and the
// password is not; the account's functions post once per key, edit, delete,
// read the subscribed posts, the account's own, replies and mentions, upload an
// image with the cookie the image store wants, and send the app's own
// requests; a wrong password and a blocked server are refused; the test
// reports a sign-in the server stopped accepting.
// TestLemmyAddress pins where an app's path on a linked Lemmy server goes:
// under the 0.19 interface, or under /api/ for one of Lemmy 1.0's own routes.
func TestLemmyAddress(t *testing.T) {
	l := &linked{kind: "lemmy", data: map[string]any{"server": "lemmy.example"}}
	if got := lemmy_api(l, "user/export_settings", nil); got != "https://lemmy.example/api/v3/user/export_settings" {
		t.Errorf("0.19 path = %s", got)
	}
	if got := lemmy_api(l, "v4/account/settings/export", map[string]string{"page": "1"}); got != "https://lemmy.example/api/v4/account/settings/export?page=1" {
		t.Errorf("1.0 path = %s", got)
	}
}

func TestLinkedLemmy(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_lemmy_fake(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	host := fake_host(fake.server)

	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", host, "username", "alice", "password", "wrong")); err == nil || !strings.Contains(err.Error(), "did not accept") {
		t.Errorf("wrong password: %v", err)
	}
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", host, "username", "alice")); err == nil || !strings.Contains(err.Error(), "required") {
		t.Errorf("missing password: %v", err)
	}
	block_add(db_user(alice, "user"), "127.0.0.1")
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", host, "username", "alice", "password", "secret")); err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Errorf("blocked server: %v", err)
	}
	block_remove(db_user(alice, "user"), "127.0.0.1")

	account := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", host, "username", "alice", "password", "secret", "code", "123456")))
	id, _ := account["id"].(string)
	if id == "" || account["type"] != "lemmy" || account["identifier"] != "@alice@"+host || account["label"] != "Alice" || account["hosted"] != false || account["server"] != host || account["url"] != fake.server.URL+"/u/alice" {
		t.Fatalf("linked account = %v", account)
	}
	if granted, _ := account["granted"].([]any); len(granted) != 1 || granted[0] != "social" {
		t.Errorf("granted = %v", account["granted"])
	}
	l := linked_get(alice, id)
	if l == nil || l.get("token") != "tok1" || l.get("password") != "" || l.get("person") != "7" {
		t.Fatalf("stored data = %v", l)
	}
	if listed := decoded(t)(api_account_get(thread, fn, sl.Tuple{sl.String(id)}, nil)); listed["hosted"] != false || listed["token"] != nil {
		t.Errorf("get = %v", listed)
	}

	// Linking again renews the row rather than making a second one.
	again := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", host, "username", "alice", "password", "secret")))
	if again["id"] != id {
		t.Errorf("a second link made %v, want %s", again["id"], id)
	}

	object := object_dict("kind", "post", "name", "Hello")
	created := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object}, linked_kwargs("key", "k1")))
	if created["id"] != "post/42" || created["url"] != fake.server.URL+"/post/42" {
		t.Errorf("create = %v", created)
	}
	decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object}, linked_kwargs("key", "k1")))
	fake.lock.Lock()
	posts := len(fake.posts)
	fake.lock.Unlock()
	if posts != 1 {
		t.Errorf("%d posts made for one key, want 1", posts)
	}
	decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object}, linked_kwargs("key", "k2")))
	fake.lock.Lock()
	posts = len(fake.posts)
	fake.lock.Unlock()
	if posts != 2 {
		t.Errorf("%d posts made for two keys, want 2", posts)
	}
	updated := decoded(t)(api_account_object_update(thread, fn, sl.Tuple{sl.String(id), sl.String("post/42"), object_dict("name", "Edited")}, nil))
	if data, _ := updated["data"].(map[string]any); updated["id"] != "post/42" || data["post"].(map[string]any)["name"] != "Edited" {
		t.Errorf("update = %v", updated)
	}
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String(id), sl.String("post/42")}, nil); err != nil {
		t.Errorf("delete: %v", err)
	}
	if value, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String(id), sl.String("nonsense")}, nil); err != nil || value != sl.None {
		t.Errorf("delete of a malformed id succeeded: %v, %v", value, err)
	}

	timeline := decoded(t)(api_account_timeline(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if items, _ := timeline["items"].([]any); len(items) != 2 || timeline["cursor"] != "P2" {
		t.Errorf("timeline = %v", timeline)
	}
	more := decoded(t)(api_account_timeline(thread, fn, sl.Tuple{sl.String(id)}, linked_kwargs("cursor", "P2")))
	if items, _ := more["items"].([]any); len(items) != 0 || more["cursor"] != "" {
		t.Errorf("timeline end = %v", more)
	}
	own := decoded(t)(api_account_posts(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if items, _ := own["items"].([]any); len(items) != 2 || own["cursor"] != "2" {
		t.Errorf("posts = %v", own)
	}
	notifications := decoded(t)(api_account_notifications(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if items, _ := notifications["items"].([]any); len(items) != 2 || items[0].(map[string]any)["kind"] != "reply" || items[1].(map[string]any)["kind"] != "mention" {
		t.Errorf("notifications = %v", notifications)
	}
	upload := decoded(t)(api_account_upload(thread, fn, sl.Tuple{sl.String(id), sl.Bytes("PNG"), sl.String("image/png")}, linked_kwargs("name", "cat.png")))
	if upload["id"] != "abc.png" || upload["url"] != fake.server.URL+"/pictrs/image/abc.png" {
		t.Errorf("upload = %v", upload)
	}
	query := sl.NewDict(1)
	query.SetKey(sl.String("sort"), sl.String("Hot"))
	requested := decoded(t)(api_account_request(thread, fn, sl.Tuple{sl.String(id), sl.String("get"), sl.String("community/list")}, []sl.Tuple{{sl.String("query"), query}}))
	if data, _ := requested["data"].(map[string]any); requested["status"] != int64(200) && requested["status"] != 200 || data == nil || data["communities"].([]any)[0].(map[string]any)["community"].(map[string]any)["name"] != "Hot" {
		t.Errorf("request = %v", requested)
	}

	tested := decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if tested["success"] != true {
		t.Errorf("test = %v", tested)
	}
	fake.lock.Lock()
	fake.revoked = true
	fake.lock.Unlock()
	tested = decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if message, _ := tested["message"].(string); tested["success"] != false || !strings.Contains(strings.ToLower(message), "authentication") {
		t.Errorf("test after revocation = %v", tested)
	}
	if _, err := api_account_timeline(thread, fn, sl.Tuple{sl.String(id)}, nil); err == nil {
		t.Error("timeline after revocation succeeded")
	}
}

// TestLinkedGuards: the linked functions refuse a hosted account, a missing
// permission, a bad method or path, a chat message with no room, and
// everything with the development switch off; follow stays hosted-only.
func TestLinkedGuards(t *testing.T) {
	alice := create_linked_test_env(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('m1', 'matrix', 'Alice', '@alice:example.org', '{\"id\":\"@alice:example.org\",\"server\":\"example.org\",\"base\":\"http://example.org\",\"token\":\"x\"}', 1, 1)")

	if _, err := api_account_timeline(thread, fn, sl.Tuple{sl.String("f1")}, nil); err == nil || !strings.Contains(err.Error(), "hosted") {
		t.Errorf("timeline of a hosted account: %v", err)
	}
	if _, err := api_account_request(thread, fn, sl.Tuple{sl.String("m1"), sl.String("TRACE"), sl.String("client/v3/x")}, nil); err == nil || !strings.Contains(err.Error(), "method") {
		t.Errorf("bad method: %v", err)
	}
	if _, err := api_account_request(thread, fn, sl.Tuple{sl.String("m1"), sl.String("GET"), sl.String("../x")}, nil); err == nil || !strings.Contains(err.Error(), "path") {
		t.Errorf("bad path: %v", err)
	}
	if value, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), object_dict("body", "hi")}, nil); err != nil || value != sl.None {
		t.Errorf("a message with no room on a chat account: %v, %v", value, err)
	}
	if _, err := api_account_follow(thread, fn, sl.Tuple{sl.String("m1"), sl.String("@bob:example.org")}, nil); err == nil || !strings.Contains(err.Error(), "request") {
		t.Errorf("follow on a linked account: %v", err)
	}
	other := create_test_thread(alice, create_external_app("app2"))
	other.SetLocal("context", context.Background())
	if _, err := api_account_timeline(other, fn, sl.Tuple{sl.String("m1")}, nil); err == nil || !strings.Contains(err.Error(), "permission") {
		t.Errorf("no permission: %v", err)
	}
	if _, err := api_account_link(other, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", "x", "username", "a", "password", "b")); err == nil || !strings.Contains(err.Error(), "permission") {
		t.Errorf("link without permission: %v", err)
	}
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("email")}, nil); err == nil || !strings.Contains(err.Error(), "unknown provider") {
		t.Errorf("link of a non-network provider: %v", err)
	}
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", "not a host", "username", "a", "password", "b")); err == nil || !strings.Contains(err.Error(), "invalid server") {
		t.Errorf("invalid server: %v", err)
	}

	networks_development = false
	defer func() { networks_development = true }()
	if _, err := api_account_timeline(thread, fn, sl.Tuple{sl.String("m1")}, nil); err == nil || !strings.Contains(err.Error(), "not available") {
		t.Errorf("timeline with the switch off: %v", err)
	}
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", "x", "username", "a", "password", "b")); err == nil || !strings.Contains(err.Error(), "not available") {
		t.Errorf("link with the switch off: %v", err)
	}
	linked_start(linked_get(alice, "m1"))
	if _, running := linked_sessions[linked_session_key(alice.UID, "m1")]; running {
		t.Error("a session started with the switch off")
	}
}

// TestLinkedRequestLimiter: a linked account's requests are counted against
// their own budget, not the host's, and a refusal reaches the app.
func TestLinkedRequestLimiter(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_lemmy_fake(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	host := fake_host(fake.server)
	account := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("lemmy")}, linked_kwargs("server", host, "username", "alice", "password", "secret")))
	id := account["id"].(string)

	rate_limit_host = &rate_limiter{entries: make(map[string]*rate_limit_entry), limit: 1, window: 60}
	if _, err := api_account_timeline(thread, fn, sl.Tuple{sl.String(id)}, nil); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if _, err := api_account_timeline(thread, fn, sl.Tuple{sl.String(id)}, nil); err == nil || !strings.Contains(err.Error(), "rate limit") {
		t.Errorf("second request past the budget: %v", err)
	}
	// The host's own budget is untouched: a plain fetch of the same host passes.
	if err := host_allowed(alice, "", "127.0.0.1"); err != nil || !rate_limit_host.allow("127.0.0.1") {
		t.Error("the host's budget was charged for the account's requests")
	}
}

// A matrix_fake is a Matrix home server for one account.
type matrix_fake struct {
	server   *httptest.Server
	lock     sync.Mutex
	token    string
	syncs    int
	expired  bool
	refreshs int
	logouts  int
}

func create_matrix_fake(t *testing.T) *matrix_fake {
	t.Helper()
	f := &matrix_fake{token: "mt1"}
	authorized := func(w http.ResponseWriter, r *http.Request) bool {
		f.lock.Lock()
		defer f.lock.Unlock()
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if f.expired && token == "mt1" {
			answer(w, 401, map[string]any{"errcode": "M_UNKNOWN_TOKEN", "error": "expired", "soft_logout": true})
			return false
		}
		if token != f.token {
			answer(w, 401, map[string]any{"errcode": "M_UNKNOWN_TOKEN", "error": "unknown"})
			return false
		}
		return true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/matrix/client", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"m.homeserver": map[string]any{"base_url": f.server.URL + "/"}})
	})
	mux.HandleFunc("GET /_matrix/client/v3/login", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"flows": []any{map[string]any{"type": "m.login.sso"}, map[string]any{"type": "m.login.password"}}})
	})
	mux.HandleFunc("POST /_matrix/client/v3/login", func(w http.ResponseWriter, r *http.Request) {
		body := json_body(r)
		identifier, _ := body["identifier"].(map[string]any)
		if body["type"] != "m.login.password" || identifier["user"] != "@alice:"+fake_host(f.server) || body["password"] != "secret" || body["refresh_token"] != true || !strings.HasPrefix(body["initial_device_display_name"].(string), "Mochi") {
			answer(w, 403, map[string]any{"errcode": "M_FORBIDDEN", "error": "Invalid password"})
			return
		}
		answer(w, 200, map[string]any{"user_id": identifier["user"], "access_token": "mt1", "device_id": "DEV1", "refresh_token": "mr1", "expires_in_ms": 3600000})
	})
	mux.HandleFunc("GET /_matrix/client/v3/profile/", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"displayname": "Alice", "avatar_url": "mxc://" + fake_host(f.server) + "/abc"})
	})
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		f.lock.Lock()
		f.syncs++
		n := f.syncs
		f.lock.Unlock()
		since := r.URL.Query().Get("since")
		if since == "" {
			answer(w, 200, map[string]any{"next_batch": "s1", "rooms": map[string]any{"join": map[string]any{"!room:" + fake_host(f.server): map[string]any{"timeline": map[string]any{"events": []any{map[string]any{"type": "m.room.message", "event_id": "$e1", "sender": "@bob:" + fake_host(f.server), "origin_server_ts": 1700000000000, "content": map[string]any{"msgtype": "m.text", "body": "hi"}}}}}}}})
			return
		}
		// Nothing new: a long poll that ends quickly here.
		time.Sleep(50 * time.Millisecond)
		answer(w, 200, map[string]any{"next_batch": "s" + strings.Repeat("1", n%3+1), "rooms": map[string]any{}})
	})
	mux.HandleFunc("POST /_matrix/client/v3/refresh", func(w http.ResponseWriter, r *http.Request) {
		if json_body(r)["refresh_token"] != "mr1" {
			answer(w, 401, map[string]any{"errcode": "M_UNKNOWN_TOKEN"})
			return
		}
		f.lock.Lock()
		f.refreshs++
		f.token = "mt2"
		f.lock.Unlock()
		answer(w, 200, map[string]any{"access_token": "mt2", "refresh_token": "mr2", "expires_in_ms": 3600000})
	})
	mux.HandleFunc("GET /_matrix/client/v3/account/whoami", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		answer(w, 200, map[string]any{"user_id": "@alice:" + fake_host(f.server), "device_id": "DEV1"})
	})
	mux.HandleFunc("GET /_matrix/client/v3/joined_rooms", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		answer(w, 200, map[string]any{"joined_rooms": []any{"!room:" + fake_host(f.server)}})
	})
	mux.HandleFunc("POST /_matrix/client/v3/logout", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		f.lock.Lock()
		f.logouts++
		f.lock.Unlock()
		answer(w, 200, map[string]any{})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// TestLinkedMatrix: a Matrix account links by a form through the server's
// well-known document, as a device of its own with a refresh token; the
// manager's sync loop hands what arrives to linked_inbound and keeps its
// position; an expired token is renewed and the loop carries on; the test
// checks the token; and unlinking logs the device out and stops the loop.
func TestLinkedMatrix(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_matrix_fake(t)
	log := &inbound_log{}
	log.install(t)
	saved := matrix_sync_timeout
	matrix_sync_timeout = 200 * time.Millisecond
	defer func() { matrix_sync_timeout = saved }()
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	host := fake_host(fake.server)

	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("matrix")}, linked_kwargs("username", "@alice:"+host, "password", "wrong")); err == nil || !strings.Contains(err.Error(), "did not accept") {
		t.Errorf("wrong password: %v", err)
	}
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("matrix")}, linked_kwargs("username", "alice", "password", "secret")); err == nil || !strings.Contains(err.Error(), "server") {
		t.Errorf("bare name without a server: %v", err)
	}
	account := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("matrix")}, linked_kwargs("username", "alice", "server", host, "password", "secret")))
	id := account["id"].(string)
	if account["identifier"] != "@alice:"+host || account["label"] != "Alice" || account["server"] != host || account["avatar"] != fake.server.URL+"/_matrix/client/v1/media/download/"+host+"/abc" {
		t.Fatalf("linked account = %v", account)
	}
	if granted, _ := account["granted"].([]any); len(granted) != 2 || granted[0] != "chat" || granted[1] != "notify" {
		t.Errorf("granted = %v", account["granted"])
	}
	l := linked_get(alice, id)
	if l.get("token") != "mt1" || l.get("refresh") != "mr1" || l.get("device") != "DEV1" || l.get("base") != fake.server.URL || l.get("password") != "" {
		t.Fatalf("stored data = %v", l.data)
	}

	wait(t, "the sync loop to connect", func() bool { connected, _ := linked_connected(alice.UID, id); return connected })
	log.find(t, "session", nil)
	event := log.find(t, "message", nil)
	if event["network"] != "matrix" || event["account"] != id || event["conversation"] != "!room:"+host || event["body"] != "hi" || event["from"] != "@bob:"+host || event["id"] != "$e1" || event["created"] != int64(1700000000) {
		t.Errorf("inbound event = %v", event)
	}
	wait(t, "the position to be kept", func() bool { return linked_get(alice, id).get("since") != "" })
	if present := decoded(t)(api_account_get(thread, fn, sl.Tuple{sl.String(id)}, nil)); present["connected"] != true {
		t.Errorf("present = %v", present)
	}

	fake.lock.Lock()
	fake.expired = true
	fake.lock.Unlock()
	wait(t, "the token to be renewed", func() bool { return linked_get(alice, id).get("token") == "mt2" })
	wait(t, "syncing to continue", func() bool {
		fake.lock.Lock()
		defer fake.lock.Unlock()
		return fake.refreshs == 1 && fake.token == "mt2" && fake.syncs > 2
	})
	if connected, reason := linked_connected(alice.UID, id); !connected {
		t.Errorf("not connected after the renewal: %s", reason)
	}
	tested := decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil))
	if tested["success"] != true {
		t.Errorf("test = %v", tested)
	}
	requested := decoded(t)(api_account_request(thread, fn, sl.Tuple{sl.String(id), sl.String("GET"), sl.String("client/v3/joined_rooms")}, nil))
	if data, _ := requested["data"].(map[string]any); data == nil || len(data["joined_rooms"].([]any)) != 1 {
		t.Errorf("request = %v", requested)
	}

	if value, err := api_account_remove(thread, fn, sl.Tuple{sl.String(id)}, nil); err != nil || value != sl.True {
		t.Fatalf("remove = %v, %v", value, err)
	}
	fake.lock.Lock()
	logouts := fake.logouts
	fake.lock.Unlock()
	if logouts != 1 {
		t.Errorf("%d logouts, want 1", logouts)
	}
	if _, running := linked_sessions[linked_session_key(alice.UID, id)]; running {
		t.Error("the session survived the unlink")
	}
	if linked_get(alice, id) != nil {
		t.Error("the row survived the unlink")
	}
}

// TestLinkedManagerStartsAtBoot: the manager starts a session for every
// enabled linked chat account of every active user, and disabling an account
// stops its session.
func TestLinkedManagerStartsAtBoot(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_matrix_fake(t)
	saved := matrix_sync_timeout
	matrix_sync_timeout = 200 * time.Millisecond
	defer func() { matrix_sync_timeout = saved }()
	host := fake_host(fake.server)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m1', 'matrix', 'Alice', ?, ?, 1, 1, 1)", "@alice:"+host, json_encode(map[string]any{"id": "@alice:" + host, "server": host, "base": fake.server.URL, "token": "mt1", "refresh": "mr1"}))
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m2', 'matrix', 'Alice', ?, ?, 1, 1, 0)", "@alice:"+host, json_encode(map[string]any{"id": "@alice:" + host, "server": host, "base": fake.server.URL, "token": "mt1"}))
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('h1', 'matrix', 'Alice', ?, ?, 1, 1, 1)", "@alice:example.com", json_encode(map[string]any{"hosted": true, "handle": "alice", "domain": "example.com"}))

	linked_resume()
	wait(t, "the enabled account's session", func() bool { connected, _ := linked_connected(alice.UID, "m1"); return connected })
	if _, running := linked_sessions[linked_session_key(alice.UID, "m2")]; running {
		t.Error("a disabled account got a session")
	}
	if _, running := linked_sessions[linked_session_key(alice.UID, "h1")]; running {
		t.Error("a hosted account got a session")
	}

	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String("m1")}, []sl.Tuple{{sl.String("enabled"), sl.False}}); err != nil {
		t.Fatal(err)
	}
	if _, running := linked_sessions[linked_session_key(alice.UID, "m1")]; running {
		t.Error("the session survived disabling the account")
	}
	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String("m1")}, []sl.Tuple{{sl.String("enabled"), sl.True}}); err != nil {
		t.Fatal(err)
	}
	wait(t, "the session to start again", func() bool { connected, _ := linked_connected(alice.UID, "m1"); return connected })
}

// TestLinkedStopUserWaits: stopping a user's sessions waits for them to end,
// since what they write goes in the databases a purge then removes.
func TestLinkedStopUserWaits(t *testing.T) {
	create_linked_test_env(t)
	ended := make(chan struct{})
	s := &linked_session{ended: ended, cancel: func() {
		go func() {
			time.Sleep(200 * time.Millisecond)
			close(ended)
		}()
	}}
	linked_lock.Lock()
	linked_sessions[linked_session_key("u-gone", "l1")] = s
	linked_lock.Unlock()
	linked_stop_user("u-gone")
	select {
	case <-ended:
	default:
		t.Error("THE POINT: the session ended before its user's stop returned")
	}
}
