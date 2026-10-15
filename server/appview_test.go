// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// An appview_fake stands in for Bluesky's index: it takes a request only with
// a token signed by the account's signing key, naming it and the method, and
// keeps what it was asked.
type appview_fake struct {
	server   *httptest.Server
	lock     sync.Mutex
	key      string // the signing key's did:key the token must verify against
	did      string
	calls    []map[string]any
	refused  int
	labelers []string
}

func create_appview_fake(t *testing.T) *appview_fake {
	t.Helper()
	f := &appview_fake{}
	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/", func(w http.ResponseWriter, r *http.Request) {
		method := strings.TrimPrefix(r.URL.Path, "/xrpc/")
		f.lock.Lock()
		defer f.lock.Unlock()
		if err := f.verify(r, method); err != nil {
			f.refused++
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]any{"error": "AuthMissing", "message": err.Error()})
			return
		}
		f.labelers = strings.Split(r.Header.Get("Atproto-Accept-Labelers"), ", ")
		var body map[string]any
		if r.Body != nil {
			data, _ := io.ReadAll(r.Body)
			json.Unmarshal(data, &body)
		}
		f.calls = append(f.calls, map[string]any{"method": method, "query": r.URL.Query().Encode(), "body": body})
		w.Header().Set("Content-Type", "application/json")
		switch method {
		case "app.bsky.notification.listNotifications":
			json.NewEncoder(w).Encode(map[string]any{"cursor": "n2", "notifications": []any{
				map[string]any{"uri": "at://did:plc:bobbobbobbobbobbobbobbob/app.bsky.feed.like/1", "cid": "bafy1", "reason": "like", "reasonSubject": "at://" + f.did + "/app.bsky.feed.post/p1", "author": map[string]any{"did": "did:plc:bobbobbobbobbobbobbobbob", "handle": "bob.example", "displayName": "Bob"}},
			}})
		case "app.bsky.feed.getTimeline":
			json.NewEncoder(w).Encode(map[string]any{"feed": []any{map[string]any{"post": map[string]any{"uri": "at://did:plc:bobbobbobbobbobbobbobbob/app.bsky.feed.post/t1", "cid": "bafyt1", "author": map[string]any{"did": "did:plc:bobbobbobbobbobbobbobbob", "handle": "bob.example"}, "record": map[string]any{"text": "From the timeline", "createdAt": "2026-10-15T00:00:00Z"}, "labels": []any{map[string]any{"src": "did:plc:ar7c4by46qjdydhdevvrndac", "val": "spam"}}}}}})
		case "app.bsky.feed.getFeed":
			json.NewEncoder(w).Encode(map[string]any{"feed": []any{map[string]any{"post": map[string]any{"uri": "at://did:plc:bobbobbobbobbobbobbobbob/app.bsky.feed.post/g1", "cid": "bafyg1", "author": map[string]any{"did": "did:plc:bobbobbobbobbobbobbobbob", "handle": "bob.example"}, "record": map[string]any{"text": "From a custom feed", "createdAt": "2026-10-15T00:00:00Z"}}}}})
		case "app.bsky.graph.muteActor", "app.bsky.graph.unmuteActor":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotImplemented)
			json.NewEncoder(w).Encode(map[string]any{"error": "MethodNotImplemented"})
		}
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// verify checks the request's token: three parts, ES256, signed by the key,
// issued by the account, for the index and the method, not expired.
func (f *appview_fake) verify(r *http.Request, method string) error {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	parts := strings.Split(token, ".")
	if token == "" || len(parts) != 3 {
		return fmt.Errorf("no token")
	}
	var header struct{ Alg, Typ string }
	data, _ := base64.RawURLEncoding.DecodeString(parts[0])
	json.Unmarshal(data, &header)
	if header.Alg != "ES256" {
		return fmt.Errorf("alg %q", header.Alg)
	}
	var claims struct {
		Iss, Aud, Lxm, Jti string
		Exp, Iat           int64
	}
	data, _ = base64.RawURLEncoding.DecodeString(parts[1])
	json.Unmarshal(data, &claims)
	if claims.Iss != f.did || claims.Aud != bluesky_appview_did || claims.Lxm != method || claims.Exp <= now() || claims.Jti == "" {
		return fmt.Errorf("claims %+v", claims)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("signature not base64url")
	}
	public, err := atcrypto.ParsePublicDIDKey(f.key)
	if err != nil {
		return err
	}
	if err := public.HashAndVerify([]byte(parts[0]+"."+parts[1]), signature); err != nil {
		return fmt.Errorf("signature does not verify")
	}
	return nil
}

// create_appview_test_env makes alice's hosted Bluesky account with its
// identity and repository, the fake index in place of the real one, and bob
// as a person on Bluesky whose documents resolve.
func create_appview_test_env(t *testing.T) (*User, *appview_fake, *plc_fake) {
	t.Helper()
	alice, plc := create_atproto_test_env(t)
	if err := atproto_account_create(context.Background(), alice, "b1"); err != nil {
		t.Fatal(err)
	}
	row := account_hosted_get(alice, "b1")
	f := create_appview_fake(t)
	f.did = atproto_did(row)
	f.key = keypair_get(alice, "b1", key_signing).Public
	saved_appview, saved_fetch := bluesky_appview, resolve_fetch
	bluesky_appview = f.server.URL
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		switch {
		case strings.HasSuffix(address, "/did:plc:bobbobbobbobbobbobbobbob"):
			return []byte(`{"id":"did:plc:bobbobbobbobbobbobbobbob","alsoKnownAs":["at://bob.example"],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://pds.example"}]}`), nil
		case strings.HasPrefix(address, "https://pds.example/xrpc/com.atproto.repo.getRecord"):
			return []byte(`{"uri":"at://did:plc:bobbobbobbobbobbobbobbob/app.bsky.actor.profile/self","cid":"bafyprofile","value":{"displayName":"Bob"}}`), nil
		case strings.HasSuffix(address, "bob.example/.well-known/atproto-did"):
			return []byte("did:plc:bobbobbobbobbobbobbobbob"), nil
		}
		return nil, &resolve_refusal{Status: 404}
	}
	saved_lookup := bluesky_lookup
	bluesky_lookup = func(ctx context.Context, name string) ([]string, error) { return nil, fmt.Errorf("no DNS in tests") }
	t.Cleanup(func() { bluesky_appview, resolve_fetch, bluesky_lookup = saved_appview, saved_fetch, saved_lookup })
	return alice, f, plc
}

// TestAppviewForwarding: a hosted account's notifications, timeline and the
// index's methods go to the index with a token signed by its key, naming the
// labellers; a method that is not the index's is refused; the account's own
// posts come from its repository.
func TestAppviewForwarding(t *testing.T) {
	alice, f, _ := create_appview_test_env(t)
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	did := f.did

	saved := setting_effective("labelers")
	setting_set("labelers", "did:plc:ownerlabelownerlabelowne")
	t.Cleanup(func() { setting_set("labelers", saved) })
	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String("b1")}, []sl.Tuple{{sl.String("labelers"), sl.NewList([]sl.Value{sl.String("did:plc:mylabelermylabelermylabe")})}}); err != nil {
		t.Fatal(err)
	}
	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String("b1")}, []sl.Tuple{{sl.String("labelers"), sl.NewList([]sl.Value{sl.String("not-a-did")})}}); err == nil {
		t.Error("a labeller that is not an identifier was accepted")
	}

	value, err := api_account_notifications(thread, fn, sl.Tuple{sl.String("b1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	page := sl_decode_map(value)
	items, _ := page["items"].([]any)
	if len(items) != 1 || page["cursor"] != "n2" {
		t.Errorf("notifications = %v", page)
	}
	if f.refused != 0 {
		t.Fatalf("the index refused %d requests", f.refused)
	}
	if want := []string{"did:plc:ar7c4by46qjdydhdevvrndac", "did:plc:ownerlabelownerlabelowne", "did:plc:mylabelermylabelermylabe"}; strings.Join(f.labelers, ",") != strings.Join(want, ",") {
		t.Errorf("labelers header = %v, want %v", f.labelers, want)
	}

	value, err = api_account_timeline(thread, fn, sl.Tuple{sl.String("b1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if items, _ := sl_decode_map(value)["items"].([]any); len(items) != 1 {
		t.Errorf("timeline = %v", sl_decode_map(value))
	}

	value, err = api_account_request(thread, fn, sl.Tuple{sl.String("b1"), sl.String("GET"), sl.String("app.bsky.feed.getFeed")}, []sl.Tuple{{sl.String("query"), object_dict("feed", "at://did:plc:bobbobbobbobbobbobbobbob/app.bsky.feed.generator/cool")}})
	if err != nil {
		t.Fatal(err)
	}
	answer := sl_decode_map(value)
	if answer["status"] != int64(200) {
		t.Errorf("request = %v", answer)
	}
	last := f.calls[len(f.calls)-1]
	if last["method"] != "app.bsky.feed.getFeed" || !strings.Contains(last["query"].(string), "feed=at%3A%2F%2Fdid%3Aplc%3Abob") {
		t.Errorf("forwarded call = %v", last)
	}
	if _, err := api_account_request(thread, fn, sl.Tuple{sl.String("b1"), sl.String("GET"), sl.String("com.atproto.repo.getRecord")}, nil); err == nil || !strings.Contains(err.Error(), "not forwarded") {
		t.Errorf("a repository method was forwarded: %v", err)
	}

	// The account's own posts: from its repository, in the index's shape.
	r := repository_get(alice, "b1")
	if _, _, err := repository_write(context.Background(), r, "app.bsky.feed.post", "own1", map[string]any{"$type": "app.bsky.feed.post", "text": "Mine", "createdAt": "2026-10-15T00:00:00Z"}, false); err != nil {
		t.Fatal(err)
	}
	value, err = api_account_posts(thread, fn, sl.Tuple{sl.String("b1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	posts, _ := sl_decode_map(value)["items"].([]any)
	if len(posts) != 1 {
		t.Fatalf("posts = %v", posts)
	}
	post, _ := posts[0].(map[string]any)["post"].(map[string]any)
	if post["uri"] != "at://"+did+"/app.bsky.feed.post/own1" || post["record"].(map[string]any)["text"] != "Mine" {
		t.Errorf("post = %v", post)
	}
	calls := len(f.calls)
	api_account_posts(thread, fn, sl.Tuple{sl.String("b1")}, nil)
	if len(f.calls) != calls {
		t.Error("reading the account's own posts asked the index")
	}
}

// TestAppviewToken: a token for another service names it and the method and
// is signed by the account's key; a tampered token does not verify.
func TestAppviewToken(t *testing.T) {
	alice, f, _ := create_appview_test_env(t)
	row := account_hosted_get(alice, "b1")
	token, err := atproto_token(alice, row, bluesky_appview_did, "app.bsky.feed.getTimeline")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("GET", "https://api.example/xrpc/app.bsky.feed.getTimeline", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	if err := f.verify(request, "app.bsky.feed.getTimeline"); err != nil {
		t.Errorf("token does not verify: %v", err)
	}
	if err := f.verify(request, "app.bsky.feed.getFeed"); err == nil {
		t.Error("a token for one method passed for another")
	}
	parts := strings.Split(token, ".")
	request.Header.Set("Authorization", "Bearer "+parts[0]+"."+parts[1]+"."+strings.Repeat("A", len(parts[2])))
	if err := f.verify(request, "app.bsky.feed.getTimeline"); err == nil {
		t.Error("a tampered token verified")
	}
}

// TestAtprotoGraph: following a person on Bluesky writes a follow record and
// the following row at once; following again writes nothing more;
// unfollowing deletes both. A block is a record on each account; lifting it
// deletes the record; a mute goes to the index as the account.
func TestAtprotoGraph(t *testing.T) {
	alice, f, _ := create_appview_test_env(t)
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	db := db_user(alice, "user")
	records := func(collection string) int {
		items, _ := repository_records(alice, "b1", collection, 100, "", false)
		return len(items)
	}

	value, err := api_account_follow(thread, fn, sl.Tuple{sl.String("b1"), sl.String("bob.example")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	answer := sl_decode_map(value)
	if answer["id"] != "did:plc:bobbobbobbobbobbobbobbob" || answer["accepted"] != true || answer["locked"] != false {
		t.Errorf("follow = %v", answer)
	}
	if records("app.bsky.graph.follow") != 1 {
		t.Fatalf("%d follow records, want 1", records("app.bsky.graph.follow"))
	}
	row, _ := db.row("select activity, accepted from following where account='b1' and actor='did:plc:bobbobbobbobbobbobbobbob'")
	if row == nil || !strings.HasPrefix(row_string(row, "activity"), "at://"+f.did+"/app.bsky.graph.follow/") || row_int(row, "accepted") == 0 {
		t.Errorf("following row = %v", row)
	}
	event := atproto_commit_event(t)
	if len(event.Ops) != 1 || !strings.HasPrefix(event.Ops[0].Path, "app.bsky.graph.follow/") {
		t.Errorf("follow commit ops = %+v", event.Ops)
	}
	if _, err := api_account_follow(thread, fn, sl.Tuple{sl.String("b1"), sl.String("did:plc:bobbobbobbobbobbobbobbob")}, nil); err != nil {
		t.Fatal(err)
	}
	if records("app.bsky.graph.follow") != 1 {
		t.Errorf("following again made %d records", records("app.bsky.graph.follow"))
	}
	following, err := api_account_following(thread, fn, sl.Tuple{sl.String("b1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if list, _ := sl_decode(following).([]any); len(list) != 1 {
		t.Errorf("following list = %v", sl_decode(following))
	}

	if _, err := api_account_unfollow(thread, fn, sl.Tuple{sl.String("b1"), sl.String("did:plc:bobbobbobbobbobbobbobbob")}, nil); err != nil {
		t.Fatal(err)
	}
	if records("app.bsky.graph.follow") != 0 {
		t.Errorf("%d follow records after unfollowing", records("app.bsky.graph.follow"))
	}
	if found, _ := db.exists("select 1 from following where account='b1'"); found {
		t.Error("the following row stayed")
	}
	event = atproto_commit_event(t)
	if len(event.Ops) != 1 || event.Ops[0].Action != "delete" {
		t.Errorf("unfollow commit ops = %+v", event.Ops)
	}

	// Blocks.
	if _, err := api_account_block_add(thread, fn, sl.Tuple{sl.String("did:plc:bobbobbobbobbobbobbobbob")}, nil); err != nil {
		t.Fatal(err)
	}
	if records("app.bsky.graph.block") != 1 {
		t.Fatalf("%d block records, want 1", records("app.bsky.graph.block"))
	}
	if !actor_blocked(alice, "did:plc:bobbobbobbobbobbobbobbob") {
		t.Error("bob is not blocked")
	}
	if _, err := api_account_block_remove(thread, fn, sl.Tuple{sl.String("did:plc:bobbobbobbobbobbobbobbob")}, nil); err != nil {
		t.Fatal(err)
	}
	if records("app.bsky.graph.block") != 0 || actor_blocked(alice, "did:plc:bobbobbobbobbobbobbobbob") {
		t.Error("the block stayed")
	}

	// Mutes go to the index.
	calls := len(f.calls)
	if _, err := api_account_mute_add(thread, fn, sl.Tuple{sl.String("did:plc:bobbobbobbobbobbobbobbob")}, nil); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != calls+1 || f.calls[calls]["method"] != "app.bsky.graph.muteActor" || f.calls[calls]["body"].(map[string]any)["actor"] != "did:plc:bobbobbobbobbobbobbobbob" {
		t.Errorf("mute calls = %v", f.calls[calls:])
	}
	if !actor_muted(alice, "did:plc:bobbobbobbobbobbobbobbob") {
		t.Error("bob is not muted here")
	}
	if _, err := api_account_mute_remove(thread, fn, sl.Tuple{sl.String("did:plc:bobbobbobbobbobbobbobbob")}, nil); err != nil {
		t.Fatal(err)
	}
	if last := f.calls[len(f.calls)-1]; last["method"] != "app.bsky.graph.unmuteActor" {
		t.Errorf("unmute call = %v", last)
	}
}

// TestAtprotoTakedown: the owner takes an account down: its repository is not
// served, its status says so, the stream tells the relays; restoring undoes
// each; the handle registry shows the account and its status.
func TestAtprotoTakedown(t *testing.T) {
	alice, f, _ := create_appview_test_env(t)
	root := user_by_uid("u-root")
	app := create_external_app("app3")
	permission_grant(root, app.id, "settings/write")
	permission_grant(root, app.id, "server/read")
	thread := create_test_thread(root, app)
	thread.SetLocal("context", context.Background())
	fn := sl.NewBuiltin("test", nil)

	if _, err := api_server_account_takedown(thread, fn, sl.Tuple{sl.String(f.did)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, status := atproto_json(t, "/xrpc/com.atproto.sync.getRepoStatus?did="+f.did); status["active"] != false || status["status"] != "takendown" {
		t.Errorf("status = %v", status)
	}
	if w, answer := atproto_json(t, "/xrpc/com.atproto.sync.getRepo?did="+f.did); w.Code != 400 || answer["error"] != "RepoTakendown" {
		t.Errorf("getRepo of a taken-down account: %d %v", w.Code, answer)
	}
	if w, _ := atproto_json(t, "/xrpc/com.atproto.repo.listRecords?repo="+f.did+"&collection=app.bsky.feed.post"); w.Code != 400 {
		t.Errorf("listRecords of a taken-down account: %d", w.Code)
	}
	all := stream_events(t, 0)
	final := all[len(all)-1].RepoAccount
	if final == nil || final.Active || final.Status == nil || *final.Status != "takendown" {
		t.Errorf("last event = %+v", all[len(all)-1])
	}
	registry, err := api_handle_registry(thread, fn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range sl_decode(registry).([]any) {
		handle, _ := item.(map[string]any)
		for _, account := range handle["accounts"].([]any) {
			entry, _ := account.(map[string]any)
			if entry["did"] == f.did {
				found = true
				if entry["status"] != "takendown" || entry["network"] != "bluesky" {
					t.Errorf("registry entry = %v", entry)
				}
			}
		}
	}
	if !found {
		t.Error("the registry does not list the account")
	}

	if _, err := api_server_account_takedown(thread, fn, sl.Tuple{sl.String(f.did)}, []sl.Tuple{{sl.String("down"), sl.False}}); err != nil {
		t.Fatal(err)
	}
	if _, status := atproto_json(t, "/xrpc/com.atproto.sync.getRepoStatus?did="+f.did); status["active"] != true {
		t.Errorf("status after restoring = %v", status)
	}
	if w, _ := atproto_json(t, "/xrpc/com.atproto.sync.getRepo?did="+f.did); w.Code != 200 {
		t.Errorf("getRepo after restoring: %d", w.Code)
	}
	all = stream_events(t, 0)
	if restored := all[len(all)-1].RepoAccount; restored == nil || !restored.Active {
		t.Errorf("last event after restoring = %+v", all[len(all)-1])
	}
	if _, err := api_server_account_takedown(thread, fn, sl.Tuple{sl.String("did:plc:nobody")}, nil); err == nil {
		t.Error("an unknown identity was taken down")
	}
	alice_thread := atproto_thread(alice)
	if _, err := api_server_account_takedown(alice_thread, fn, sl.Tuple{sl.String(f.did)}, nil); err == nil {
		t.Error("a user without settings/write took an account down")
	}
}

// TestAtprotoGenerator: a generator record published by an app makes this
// server a feed generator: its did:web document, the feeds it describes, and
// a skeleton the app fills from the record's feed.
func TestAtprotoGenerator(t *testing.T) {
	alice, f, _ := create_appview_test_env(t)
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	value, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("b1"), sl_encode(map[string]any{"$type": "app.bsky.feed.generator", "did": "did:web:example.com", "displayName": "Alice's picks", "createdAt": "2026-10-15T00:00:00Z"})}, nil)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := sl_decode_map(value)["id"].(string)
	if !strings.HasPrefix(uri, "at://"+f.did+"/app.bsky.feed.generator/") {
		t.Fatalf("generator id = %q", uri)
	}
	// A record naming no service is given this server.
	value, err = api_account_object_create(thread, fn, sl.Tuple{sl.String("b1"), sl_encode(map[string]any{"$type": "app.bsky.feed.generator", "displayName": "Unnamed service"})}, nil)
	if err != nil {
		t.Fatal(err)
	}
	unnamed := sl_decode_map(value)["id"].(string)
	items, _ := repository_records(alice, "b1", "app.bsky.feed.generator", 100, "", false)
	filled := false
	for _, item := range items {
		record, _ := item["value"].(map[string]any)
		if record["displayName"] == "Unnamed service" {
			filled = record["did"] == "did:web:example.com"
		}
	}
	if !filled {
		t.Errorf("a generator record naming no service was not given this server: %v", items)
	}
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String("b1"), sl.String(unnamed)}, nil); err != nil {
		t.Fatal(err)
	}

	saved_call := activitypub_call
	activitypub_call = func(user *User, a *App, function string, args Map) (any, error) {
		if function != "atproto/skeleton" || args["feed"] != uri || args["account"] != "b1" {
			t.Errorf("call %s %v", function, args)
		}
		if args["cursor"] == "more" {
			return map[string]any{"posts": []any{"at://" + f.did + "/app.bsky.feed.post/p3"}}, nil
		}
		return map[string]any{"posts": []any{"at://" + f.did + "/app.bsky.feed.post/p1", "at://" + f.did + "/app.bsky.feed.post/p2"}, "cursor": "more"}, nil
	}
	t.Cleanup(func() { activitypub_call = saved_call })
	apps_lock.Lock()
	saved_apps := apps
	apps = map[string]*App{"feeds": {id: "feeds", internal: &AppVersion{Functions: map[string]AppFunction{"atproto/skeleton": {}}}}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = saved_apps
		apps_lock.Unlock()
	})

	_, document := atproto_json(t, "/.well-known/did.json")
	services, _ := document["service"].([]any)
	if document["id"] != "did:web:example.com" || len(services) != 1 || services[0].(map[string]any)["type"] != "BskyFeedGenerator" || services[0].(map[string]any)["serviceEndpoint"] != "https://example.com" {
		t.Errorf("did document = %v", document)
	}
	_, described := atproto_json(t, "/xrpc/app.bsky.feed.describeFeedGenerator")
	if feeds, _ := described["feeds"].([]any); len(feeds) != 1 || feeds[0].(map[string]any)["uri"] != uri || described["did"] != "did:web:example.com" {
		t.Errorf("describeFeedGenerator = %v", described)
	}
	w, skeleton := atproto_json(t, "/xrpc/app.bsky.feed.getFeedSkeleton?feed="+uri)
	page, _ := skeleton["feed"].([]any)
	if w.Code != 200 || len(page) != 2 || page[0].(map[string]any)["post"] != "at://"+f.did+"/app.bsky.feed.post/p1" || skeleton["cursor"] != "more" {
		t.Errorf("skeleton = %d %v", w.Code, skeleton)
	}
	if _, more := atproto_json(t, "/xrpc/app.bsky.feed.getFeedSkeleton?feed="+uri+"&cursor=more"); len(more["feed"].([]any)) != 1 || more["cursor"] != nil {
		t.Errorf("second page = %v", more)
	}
	if w, answer := atproto_json(t, "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://"+f.did+"/app.bsky.feed.generator/nothing"); w.Code != 400 || answer["error"] != "UnknownFeed" {
		t.Errorf("unknown feed: %d %v", w.Code, answer)
	}
}

// TestAtprotoSubject: a client signing in to a hosted Bluesky account is told
// the account's identifier as the subject of its tokens, which is how an AT
// Protocol client, and Mochi's own sign-in provider, knows the account.
func TestAtprotoSubject(t *testing.T) {
	alice, f, _ := create_appview_test_env(t)
	db_open("db/users.db").exec("create table if not exists tokens (hash text primary key not null, user text not null, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0, kind text not null default '', client text not null default '', account text not null default '', network text not null default '', jkt text not null default '', refresh text not null default '', device text not null default '', scram text not null default '')")
	db_open("db/sessions.db").exec("create table if not exists accesses (hash text primary key not null, user text not null, used integer not null default 0)")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "https://example.com/oauth/token", nil)
	signin_issue(c, &signin_client{ID: "https://app.example/client-metadata.json", Name: "Skies", Network: "bluesky", Method: "none"}, alice, "b1", bluesky_scope, "jkt1", "")
	var answer map[string]any
	json.Unmarshal(w.Body.Bytes(), &answer)
	if w.Code != 200 || answer["sub"] != f.did || answer["token_type"] != "DPoP" {
		t.Errorf("token answer = %d %v", w.Code, answer)
	}
}
