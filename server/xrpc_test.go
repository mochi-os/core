// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// An xrpc_feeds fake stands in for the feeds app: it records each write it
// is told of.
type xrpc_feeds struct {
	lock    sync.Mutex
	written []Map
}

func (f *xrpc_feeds) call(s *xrpc_session, a *App, function string, args Map) (any, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if function == "atproto/written" {
		f.written = append(f.written, args)
	}
	return map[string]any{}, nil
}

// last is the last write the fake was told of.
func (f *xrpc_feeds) last(t *testing.T) Map {
	t.Helper()
	f.lock.Lock()
	defer f.lock.Unlock()
	if len(f.written) == 0 {
		t.Fatal("no write was mirrored")
	}
	return f.written[len(f.written)-1]
}

// count is how many writes the fake was told of.
func (f *xrpc_feeds) count() int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return len(f.written)
}

// create_xrpc_test_env is alice with her hosted Bluesky account b1, its
// identity and repository, the home feed "feed1", the sign-in tables, the
// fake feeds app, and the fake index.
func create_xrpc_test_env(t *testing.T) (*User, *xrpc_feeds, *appview_fake, *plc_fake) {
	t.Helper()
	alice, view, plc := create_appview_test_env(t)
	users := db_open("db/users.db")
	users.exec("create table if not exists tokens (hash text primary key not null, user text not null, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0, kind text not null default '', client text not null default '', account text not null default '', network text not null default '', jkt text not null default '', refresh text not null default '', device text not null default '', scram text not null default '')")
	db_open("db/sessions.db").exec("create table if not exists accesses (hash text primary key not null, user text not null, used integer not null default 0)")
	statuses_tables(db_user(alice, "user"))
	row := account_hosted_get(alice, "b1")
	data := account_data(row)
	data["home"] = "feed1"
	db_user(alice, "user").account_set("b1", map[string]any{"data": json_encode(data)})

	fake := &xrpc_feeds{}
	saved_caller, saved_notify := xrpc_caller, xrpc_notify
	xrpc_caller = fake.call
	// Two apps declare the function, as a development copy of feeds beside
	// the installed one does; one of them keeps the posts.
	apps_lock.Lock()
	apps["feeds1"] = &App{id: "feeds1", internal: &AppVersion{Functions: map[string]AppFunction{"atproto/written": {Function: "function_atproto_written"}}}}
	apps["feeds2"] = &App{id: "feeds2", internal: &AppVersion{Functions: map[string]AppFunction{"atproto/written": {Function: "function_atproto_written"}}}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		xrpc_caller, xrpc_notify = saved_caller, saved_notify
		apps_lock.Lock()
		delete(apps, "feeds1")
		delete(apps, "feeds2")
		apps_lock.Unlock()
	})
	return alice, fake, view, plc
}

// xrpc_call drives one request to the data server at example.com, with a
// bearer token where one is given, and decodes the JSON answer.
func xrpc_call(t *testing.T, method string, name string, token string, body any, headers map[string]string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	switch b := body.(type) {
	case nil:
		reader = bytes.NewReader(nil)
	case []byte:
		reader = bytes.NewReader(b)
	default:
		reader = bytes.NewReader([]byte(json_encode(b)))
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, "https://example.com/xrpc/"+name, reader)
	c.Request.Host = "example.com"
	if _, raw := body.([]byte); !raw && body != nil {
		c.Request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		c.Request.Header.Set("Authorization", "Bearer "+token)
	}
	for key, value := range headers {
		c.Request.Header.Set(key, value)
	}
	if !atproto_route(c) {
		t.Fatalf("%s %s was not answered", method, name)
	}
	var answer map[string]any
	json.Unmarshal(w.Body.Bytes(), &answer)
	return w, answer
}

// xrpc_signin signs alice's account in the old way, with a minted password.
func xrpc_signin(t *testing.T, alice *User) (string, string) {
	t.Helper()
	password := password_mint(alice, "", "b1", "bluesky", "Phone")
	w, answer := xrpc_call(t, "POST", "com.atproto.server.createSession", "", map[string]any{"identifier": "alice.example.com", "password": password}, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("createSession = %d %s", w.Code, w.Body.String())
	}
	access, _ := answer["accessJwt"].(string)
	refresh, _ := answer["refreshJwt"].(string)
	return access, refresh
}

// xrpc_claims decodes a JWT's claims.
func xrpc_claims(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token %q is not a JWT", token)
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(data, &claims); err != nil {
		t.Fatal(err)
	}
	return claims
}

// TestXrpcSession: an app signs in with the account's handle and a minted
// password, and is given tokens a client reads as JWTs naming the account
// and when they expire; the access token reads the session, a wrong password
// is refused, refreshing ends the old tokens and gives new ones, and deleting
// the session ends it.
func TestXrpcSession(t *testing.T) {
	alice, _, _, _ := create_xrpc_test_env(t)
	did := atproto_did(account_hosted_get(alice, "b1"))

	if w, answer := xrpc_call(t, "POST", "com.atproto.server.createSession", "", map[string]any{"identifier": "alice.example.com", "password": "mochi-wrong"}, nil); w.Code != http.StatusUnauthorized || answer["error"] != "AuthenticationRequired" {
		t.Errorf("a wrong password = %d %v", w.Code, answer)
	}
	access, refresh := xrpc_signin(t, alice)
	claims := xrpc_claims(t, access)
	if claims["sub"] != did || claims["scope"] != "com.atproto.access" || claims["aud"] != "did:web:example.com" {
		t.Errorf("access claims = %v", claims)
	}
	if exp, _ := claims["exp"].(float64); int64(exp) <= now() || int64(exp) > now()+xrpc_access_lifetime {
		t.Errorf("access exp = %v", claims["exp"])
	}
	if xrpc_claims(t, refresh)["scope"] != "com.atproto.refresh" {
		t.Error("the refresh token is not one")
	}

	w, answer := xrpc_call(t, "GET", "com.atproto.server.getSession", access, nil, nil)
	if w.Code != http.StatusOK || answer["did"] != did || answer["handle"] != "alice.example.com" || answer["active"] != true {
		t.Fatalf("getSession = %d %v", w.Code, answer)
	}
	document, _ := answer["didDoc"].(map[string]any)
	if document == nil || document["id"] != did {
		t.Errorf("didDoc = %v", answer["didDoc"])
	}
	if w, _ := xrpc_call(t, "GET", "com.atproto.server.getSession", refresh, nil, nil); w.Code == http.StatusOK {
		t.Error("the refresh token reads the session")
	}

	w, answer = xrpc_call(t, "POST", "com.atproto.server.refreshSession", refresh, nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("refreshSession = %d %s", w.Code, w.Body.String())
	}
	renewed, _ := answer["accessJwt"].(string)
	again, _ := answer["refreshJwt"].(string)
	if renewed == "" || renewed == access || again == refresh {
		t.Fatalf("refreshSession answered %v", answer)
	}
	if w, _ := xrpc_call(t, "GET", "com.atproto.server.getSession", access, nil, nil); w.Code == http.StatusOK {
		t.Error("the old access token outlived the refresh")
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.refreshSession", refresh, nil, nil); w.Code == http.StatusOK {
		t.Error("the old refresh token refreshed again")
	}
	if w, _ := xrpc_call(t, "GET", "com.atproto.server.getSession", renewed, nil, nil); w.Code != http.StatusOK {
		t.Errorf("the renewed token = %d", w.Code)
	}

	// An expired token is told apart from one never given.
	db_open("db/users.db").exec("update tokens set expires=? where hash=?", now()-1, token_hash(renewed))
	if w, answer := xrpc_call(t, "GET", "com.atproto.server.getSession", renewed, nil, nil); w.Code != http.StatusBadRequest || answer["error"] != "ExpiredToken" {
		t.Errorf("an expired token = %d %v", w.Code, answer)
	}
	db_open("db/users.db").exec("update tokens set expires=? where hash=?", now()+60, token_hash(renewed))

	// A handle is kept for good, and the app is told so, not sent to Mochi.
	if w, answer := xrpc_call(t, "POST", "com.atproto.identity.updateHandle", renewed, map[string]any{"handle": "other.example.com"}, nil); w.Code != http.StatusBadRequest || answer["message"] != "A handle on this server cannot be changed" {
		t.Errorf("THE POINT: a handle change = %d %v", w.Code, answer)
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.deleteSession", again, nil, nil); w.Code != http.StatusOK {
		t.Errorf("deleteSession = %d", w.Code)
	}
	if w, _ := xrpc_call(t, "GET", "com.atproto.server.getSession", renewed, nil, nil); w.Code == http.StatusOK {
		t.Error("the session outlived its deletion")
	}

	// A handle on a domain the user runs signs in as one on the server's.
	db_open("db/users.db").exec("insert into handles (domain, name, user, kind, created) values ('own.example', 'sky', ?, 'handle', 1)", alice.UID)
	db_user(alice, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('b9', 'bluesky', '', 'sky.own.example', ?, 1, 1)", json_encode(map[string]any{"hosted": true, "handle": "sky", "domain": "own.example", "did": "did:plc:skyskyskyskyskyskyskysky"}))
	password := password_mint(alice, "", "b9", "bluesky", "Tablet")
	if w, answer := xrpc_call(t, "POST", "com.atproto.server.createSession", "", map[string]any{"identifier": "sky.own.example", "password": password}, nil); w.Code != http.StatusOK || answer["did"] != "did:plc:skyskyskyskyskyskyskysky" {
		t.Errorf("signing in on the user's own domain = %d %v", w.Code, answer)
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.createSession", "", map[string]any{"identifier": "alice.example.com", "password": password}, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("one account's password signed in to another: %d", w.Code)
	}
}

// TestXrpcRecords: an app writes, changes and deletes records of any type in
// the account's repository, each in a commit relays can verify, and each told
// to the feeds app with the record (a deleted one's last content); a write
// to another repository, a record of the wrong type, a stale swap or a
// token that may not write is refused; applyWrites makes several writes in
// one commit; a follow record keeps the following row the apps read.
func TestXrpcRecords(t *testing.T) {
	alice, feeds, _, _ := create_xrpc_test_env(t)
	did := atproto_did(account_hosted_get(alice, "b1"))
	access, _ := xrpc_signin(t, alice)

	post := map[string]any{"$type": "app.bsky.feed.post", "text": "From an app", "createdAt": "2026-10-20T00:00:00Z"}
	w, answer := xrpc_call(t, "POST", "com.atproto.repo.createRecord", access, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "record": post}, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("createRecord = %d %s", w.Code, w.Body.String())
	}
	uri, _ := answer["uri"].(string)
	rkey := uri[strings.LastIndex(uri, "/")+1:]
	if !strings.HasPrefix(uri, "at://"+did+"/app.bsky.feed.post/") || answer["cid"] == "" {
		t.Fatalf("createRecord answered %v", answer)
	}
	if id, record := repository_record(alice, "b1", "app.bsky.feed.post", rkey); id != answer["cid"] || record["text"] != "From an app" {
		t.Errorf("the record held = %s %v", id, record)
	}
	commit := atproto_commit_event(t)
	if len(commit.Ops) != 1 || commit.Ops[0].Action != "create" || commit.Ops[0].Path != "app.bsky.feed.post/"+rkey {
		t.Errorf("commit ops = %+v", commit.Ops)
	}
	told := feeds.last(t)
	if told["action"] != "create" || told["collection"] != "app.bsky.feed.post" || told["rkey"] != rkey || told["uri"] != uri || told["home"] != "feed1" || told["account"] != "b1" {
		t.Errorf("mirrored = %v", told)
	}
	if feeds.count() != 1 {
		t.Errorf("the record was mirrored %d times, want once", feeds.count())
	}
	if record, _ := told["record"].(map[string]any); record["text"] != "From an app" {
		t.Errorf("mirrored record = %v", told["record"])
	}

	// A record of any type is kept, and one that is not the collection's
	// type is refused.
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.createRecord", access, map[string]any{"repo": did, "collection": "com.example.note", "record": map[string]any{"body": "Anything"}}, nil); w.Code != http.StatusOK {
		t.Errorf("a record of another type = %d %s", w.Code, w.Body.String())
	}
	before := feeds.count()
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.createRecord", access, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "record": map[string]any{"$type": "app.bsky.feed.like"}}, nil); w.Code != http.StatusBadRequest {
		t.Errorf("a record of the wrong type = %d", w.Code)
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.createRecord", access, map[string]any{"repo": "did:plc:bobbobbobbobbobbobbobbob", "collection": "app.bsky.feed.post", "record": post}, nil); w.Code != http.StatusBadRequest {
		t.Errorf("a write to another repository = %d", w.Code)
	}
	if feeds.count() != before {
		t.Error("a refused write was mirrored")
	}

	// A token that may only read cannot write.
	reader := "mochi-" + uid()
	db_open("db/users.db").exec("insert into tokens (hash, user, app, name, scopes, created, kind, account, network) values (?, ?, '', 'Reader', ?, ?, ?, 'b1', 'bluesky')", token_hash(reader), alice.UID, json_encode([]string{"atproto"}), now(), token_signin)
	if w, answer := xrpc_call(t, "POST", "com.atproto.repo.createRecord", reader, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "record": post}, nil); w.Code != http.StatusForbidden || answer["error"] != "InsufficientScope" {
		t.Errorf("a read-only token wrote: %d %v", w.Code, answer)
	}
	if w, _ := xrpc_call(t, "GET", "com.atproto.server.getSession", reader, nil, nil); w.Code != http.StatusOK {
		t.Errorf("a read-only token cannot read: %d", w.Code)
	}

	// Changing the record: a stale swap is refused, the right one writes.
	changed := map[string]any{"$type": "app.bsky.feed.post", "text": "Changed in the app", "createdAt": "2026-10-20T00:00:00Z"}
	if w, answer := xrpc_call(t, "POST", "com.atproto.repo.putRecord", access, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "rkey": rkey, "record": changed, "swapRecord": "bafystale"}, nil); w.Code != http.StatusBadRequest || answer["error"] != "InvalidSwap" {
		t.Errorf("a stale swap = %d %v", w.Code, answer)
	}
	cid, _ := repository_record(alice, "b1", "app.bsky.feed.post", rkey)
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.putRecord", access, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "rkey": rkey, "record": changed, "swapRecord": cid}, nil); w.Code != http.StatusOK {
		t.Fatalf("putRecord = %d %s", w.Code, w.Body.String())
	}
	if _, record := repository_record(alice, "b1", "app.bsky.feed.post", rkey); record["text"] != "Changed in the app" {
		t.Errorf("the changed record = %v", record)
	}
	if commit := atproto_commit_event(t); len(commit.Ops) != 1 || commit.Ops[0].Action != "update" {
		t.Errorf("update commit ops = %+v", commit.Ops)
	}
	if told := feeds.last(t); told["action"] != "update" || told["uri"] != uri {
		t.Errorf("mirrored update = %v", told)
	}

	// Deleting it tells the feeds app what the record said.
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.deleteRecord", access, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "rkey": rkey}, nil); w.Code != http.StatusOK {
		t.Fatalf("deleteRecord = %d %s", w.Code, w.Body.String())
	}
	if id, _ := repository_record(alice, "b1", "app.bsky.feed.post", rkey); id != "" {
		t.Error("the deleted record is still held")
	}
	if commit := atproto_commit_event(t); len(commit.Ops) != 1 || commit.Ops[0].Action != "delete" {
		t.Errorf("delete commit ops = %+v", commit.Ops)
	}
	told = feeds.last(t)
	if record, _ := told["record"].(map[string]any); told["action"] != "delete" || record["text"] != "Changed in the app" {
		t.Errorf("mirrored delete = %v", told)
	}
	before = feeds.count()
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.deleteRecord", access, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "rkey": rkey}, nil); w.Code != http.StatusOK || feeds.count() != before {
		t.Errorf("deleting it again = %d, mirrored %d", w.Code, feeds.count()-before)
	}

	// Several writes in one commit.
	events := len(stream_events(t, 0))
	w, answer = xrpc_call(t, "POST", "com.atproto.repo.applyWrites", access, map[string]any{"repo": did, "writes": []any{
		map[string]any{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.feed.post", "value": map[string]any{"text": "First", "createdAt": "2026-10-20T00:00:00Z"}},
		map[string]any{"$type": "com.atproto.repo.applyWrites#create", "collection": "app.bsky.graph.follow", "rkey": "3follow", "value": map[string]any{"subject": "did:plc:bobbobbobbobbobbobbobbob", "createdAt": "2026-10-20T00:00:00Z"}},
	}}, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("applyWrites = %d %s", w.Code, w.Body.String())
	}
	if results, _ := answer["results"].([]any); len(results) != 2 || answer["commit"] == nil {
		t.Errorf("applyWrites answered %v", answer)
	}
	if after := len(stream_events(t, 0)); after != events+1 {
		t.Errorf("%d events for one applyWrites, want 1", after-events)
	}
	if commit := atproto_commit_event(t); len(commit.Ops) != 2 {
		t.Errorf("applyWrites commit ops = %+v", commit.Ops)
	}
	if told := feeds.last(t); told["collection"] != "app.bsky.graph.follow" {
		t.Errorf("the follow was not mirrored: %v", told)
	}
	following := func() map[string]any {
		row, _ := db_user(alice, "user").row("select * from following where account='b1' and actor='did:plc:bobbobbobbobbobbobbobbob'")
		return row
	}
	if row := following(); row == nil || row_string(row, "activity") != "at://"+did+"/app.bsky.graph.follow/3follow" || row_int(row, "accepted") == 0 {
		t.Errorf("following after the follow record = %v", row)
	}
	// Following again through Mochi finds the app's record and writes none,
	// giving a record without its row the row.
	db_user(alice, "user").exec("delete from following where account='b1'")
	if uri, err := atproto_follow(context.Background(), alice, account_hosted_get(alice, "b1"), "did:plc:bobbobbobbobbobbobbobbob"); err != nil || uri != "at://"+did+"/app.bsky.graph.follow/3follow" {
		t.Errorf("following again = %q %v", uri, err)
	}
	if items, _ := repository_records(alice, "b1", "app.bsky.graph.follow", 10, "", false); len(items) != 1 {
		t.Errorf("%d follow records after following again, want 1", len(items))
	}
	if row := following(); row == nil {
		t.Error("following again left the record without its row")
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.deleteRecord", access, map[string]any{"repo": did, "collection": "app.bsky.graph.follow", "rkey": "3follow"}, nil); w.Code != http.StatusOK {
		t.Fatalf("deleting the follow = %d", w.Code)
	}
	if row := following(); row != nil {
		t.Errorf("following after the follow was deleted = %v", row)
	}
}

// TestXrpcUpload: a blob an app uploads is kept for the account and answered
// as a record names it; a post naming it is told to the feeds app with the
// image's bytes and description.
func TestXrpcUpload(t *testing.T) {
	alice, feeds, _, _ := create_xrpc_test_env(t)
	did := atproto_did(account_hosted_get(alice, "b1"))
	access, _ := xrpc_signin(t, alice)
	image := []byte("\x89PNG\r\n\x1a\n not really an image")
	w, answer := xrpc_call(t, "POST", "com.atproto.repo.uploadBlob", access, image, map[string]string{"Content-Type": "image/png"})
	if w.Code != http.StatusOK {
		t.Fatalf("uploadBlob = %d %s", w.Code, w.Body.String())
	}
	blob, _ := answer["blob"].(map[string]any)
	id := bluesky_blob_cid(blob)
	if blob["$type"] != "blob" || blob["mimeType"] != "image/png" || id == "" {
		t.Fatalf("blob = %v", answer["blob"])
	}
	if kind, data, ok := repository_blob(alice, "b1", id); !ok || kind != "image/png" || !bytes.Equal(data, image) {
		t.Error("the blob is not kept")
	}
	post := map[string]any{"$type": "app.bsky.feed.post", "text": "A picture", "createdAt": "2026-10-20T00:00:00Z", "embed": map[string]any{"$type": "app.bsky.embed.images", "images": []any{map[string]any{"alt": "A bird", "image": blob}}}}
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.createRecord", access, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "record": post}, nil); w.Code != http.StatusOK {
		t.Fatalf("createRecord = %d %s", w.Code, w.Body.String())
	}
	media, _ := feeds.last(t)["media"].([]any)
	if len(media) != 1 {
		t.Fatalf("media = %v", feeds.last(t)["media"])
	}
	item, _ := media[0].(map[string]any)
	if data, _ := item["data"].([]byte); item["description"] != "A bird" || item["type"] != "image/png" || !bytes.Equal(data, image) || item["name"] != id+".png" {
		t.Errorf("media item = %v", item)
	}
}

// TestXrpcForward: the methods of the network an app asks for go to the
// index, or the service the app names, with a token signed by the account's
// key for the service and the method, and the answer comes back; an unknown
// service is refused; preferences are kept here.
func TestXrpcForward(t *testing.T) {
	alice, _, view, _ := create_xrpc_test_env(t)
	access, _ := xrpc_signin(t, alice)

	w, answer := xrpc_call(t, "GET", "app.bsky.feed.getTimeline?limit=5", access, nil, nil)
	if w.Code != http.StatusOK || view.refused != 0 {
		t.Fatalf("getTimeline = %d %s (refused %d)", w.Code, w.Body.String(), view.refused)
	}
	if feed, _ := answer["feed"].([]any); len(feed) != 1 {
		t.Errorf("timeline = %v", answer)
	}
	view.lock.Lock()
	last := view.calls[len(view.calls)-1]
	view.lock.Unlock()
	if last["method"] != "app.bsky.feed.getTimeline" || last["query"] != "limit=5" {
		t.Errorf("forwarded = %v", last)
	}
	if w, _ := xrpc_call(t, "GET", "app.bsky.feed.getTimeline", "", nil, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("an anonymous forward = %d", w.Code)
	}

	// Named with the proxy header.
	if w, _ := xrpc_call(t, "POST", "app.bsky.graph.muteActor", access, map[string]any{"actor": "did:plc:bobbobbobbobbobbobbobbob"}, map[string]string{"Atproto-Proxy": "did:web:api.bsky.app#bsky_appview"}); w.Code != http.StatusOK {
		t.Errorf("muteActor through the proxy header = %d %s", w.Code, w.Body.String())
	}
	before := len(view.calls)
	if w, _ := xrpc_call(t, "GET", "app.bsky.feed.getTimeline", access, nil, map[string]string{"Atproto-Proxy": "did:plc:nobodynobodynobodynobody#bsky_appview"}); w.Code != http.StatusBadRequest || len(view.calls) != before {
		t.Errorf("an unknown service = %d", w.Code)
	}
	if w, _ := xrpc_call(t, "GET", "app.bsky.feed.getTimeline", access, nil, map[string]string{"Atproto-Proxy": "did:web:api.bsky.app"}); w.Code != http.StatusBadRequest {
		t.Errorf("a proxy naming no service = %d", w.Code)
	}

	// Preferences are the account's own, kept here.
	if w, answer := xrpc_call(t, "GET", "app.bsky.actor.getPreferences", access, nil, nil); w.Code != http.StatusOK || len(answer["preferences"].([]any)) != 0 {
		t.Errorf("getPreferences before = %d %v", w.Code, answer)
	}
	preferences := []any{map[string]any{"$type": "app.bsky.actor.defs#adultContentPref", "enabled": false}}
	if w, _ := xrpc_call(t, "POST", "app.bsky.actor.putPreferences", access, map[string]any{"preferences": preferences}, nil); w.Code != http.StatusOK {
		t.Fatalf("putPreferences = %d", w.Code)
	}
	before = len(view.calls)
	w, answer = xrpc_call(t, "GET", "app.bsky.actor.getPreferences", access, nil, nil)
	if list, _ := answer["preferences"].([]any); w.Code != http.StatusOK || len(list) != 1 || len(view.calls) != before {
		t.Errorf("getPreferences after = %d %v", w.Code, answer)
	}
}

// TestXrpcServiceAuth: a token for another service is signed by the
// account's key, names the account, the service and the method, and lasts
// a minute or up to an hour where asked; longer is refused.
func TestXrpcServiceAuth(t *testing.T) {
	alice, _, view, _ := create_xrpc_test_env(t)
	access, _ := xrpc_signin(t, alice)
	w, answer := xrpc_call(t, "GET", "com.atproto.server.getServiceAuth?aud=did:web:other.example&lxm=com.atproto.server.createAccount", access, nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("getServiceAuth = %d %s", w.Code, w.Body.String())
	}
	token, _ := answer["token"].(string)
	claims := xrpc_claims(t, token)
	if claims["iss"] != view.did || claims["aud"] != "did:web:other.example" || claims["lxm"] != "com.atproto.server.createAccount" {
		t.Errorf("claims = %v", claims)
	}
	parts := strings.Split(token, ".")
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
	public, _ := atcrypto.ParsePublicDIDKey(view.key)
	if err := public.HashAndVerify([]byte(parts[0]+"."+parts[1]), signature); err != nil {
		t.Errorf("the token does not verify: %v", err)
	}
	exp := now() + 1800
	w, answer = xrpc_call(t, "GET", "com.atproto.server.getServiceAuth?aud=did:web:other.example&exp="+strconv.FormatInt(exp, 10), access, nil, nil)
	if got, _ := xrpc_claims(t, answer["token"].(string))["exp"].(float64); w.Code != http.StatusOK || int64(got) != exp {
		t.Errorf("a half-hour token = %d exp %v", w.Code, got)
	}
	if w, answer := xrpc_call(t, "GET", "com.atproto.server.getServiceAuth?aud=did:web:other.example&exp="+strconv.FormatInt(now()+7200, 10), access, nil, nil); w.Code != http.StatusBadRequest || answer["error"] != "BadExpiration" {
		t.Errorf("a two-hour token = %d %v", w.Code, answer)
	}
	if w, _ := xrpc_call(t, "GET", "com.atproto.server.getServiceAuth?aud=other.example", access, nil, nil); w.Code != http.StatusBadRequest {
		t.Errorf("an audience that is not a DID = %d", w.Code)
	}
}

// TestXrpcMoveOut: moving the account away - the code is sent to the user as
// a notification, the change of identity is signed with this server's
// rotation key only with that code, and the directory takes it; deactivated,
// the account's repository answers as deactivated and publishes nothing.
func TestXrpcMoveOut(t *testing.T) {
	alice, _, view, plc := create_xrpc_test_env(t)
	did := view.did
	access, _ := xrpc_signin(t, alice)
	var sent []Map
	xrpc_notify = func(user *User, args Map) error {
		sent = append(sent, args)
		return nil
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.identity.requestPlcOperationSignature", access, nil, nil); w.Code != http.StatusOK || len(sent) != 1 {
		t.Fatalf("requestPlcOperationSignature = %d, %d sent", w.Code, len(sent))
	}
	event, _ := sent[0]["event"].(string)
	code := strings.TrimPrefix(event, "identity:")
	if len(code) != 11 || code[5] != '-' || sent[0]["topic"] != "bluesky/identity" || sent[0]["object"] != "b1" || sent[0]["title"] == "" || sent[0]["body"] == "" {
		t.Fatalf("notification = %v", sent[0])
	}

	// The other server's recommended identity.
	other, _ := atcrypto.GeneratePrivateKeyK256()
	public, _ := other.PublicKey()
	change := map[string]any{
		"rotationKeys":        []string{public.DIDKey()},
		"verificationMethods": map[string]any{"atproto": public.DIDKey()},
		"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://other.example"}},
		"alsoKnownAs":         []string{"at://alice.other.example"},
	}
	wrong := map[string]any{"token": "AAAAA-AAAAA"}
	for key, value := range change {
		wrong[key] = value
	}
	if w, answer := xrpc_call(t, "POST", "com.atproto.identity.signPlcOperation", access, wrong, nil); w.Code != http.StatusBadRequest || answer["error"] != "InvalidToken" {
		t.Errorf("signing with a wrong code = %d %v", w.Code, answer)
	}
	change["token"] = code
	w, answer := xrpc_call(t, "POST", "com.atproto.identity.signPlcOperation", access, change, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("signPlcOperation = %d %s", w.Code, w.Body.String())
	}
	op, _ := answer["operation"].(map[string]any)
	if err := plc_submit(context.Background(), did, op); err != nil {
		t.Fatalf("the directory refused the signed change: %v", err)
	}
	if last := plc.last(did); object_strings(last["rotationKeys"])[0] != public.DIDKey() {
		t.Errorf("the directory's last operation = %v", last)
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.identity.signPlcOperation", access, change, nil); w.Code != http.StatusBadRequest {
		t.Error("the code signed a second change")
	}

	// Deactivated, the repository is no longer the account's live one.
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.deactivateAccount", access, nil, nil); w.Code != http.StatusOK {
		t.Fatalf("deactivateAccount = %d", w.Code)
	}
	all := stream_events(t, 0)
	if event := all[len(all)-1].RepoAccount; event == nil || event.Active || event.Status == nil || *event.Status != "deactivated" {
		t.Errorf("the last event = %+v", all[len(all)-1])
	}
	if _, status := atproto_json(t, "/xrpc/com.atproto.sync.getRepoStatus?did="+did); status["active"] != false || status["status"] != "deactivated" {
		t.Errorf("getRepoStatus = %v", status)
	}
	if w, answer := atproto_json(t, "/xrpc/com.atproto.sync.getRepo?did="+did); w.Code != http.StatusBadRequest || answer["error"] != "RepoDeactivated" {
		t.Errorf("getRepo = %d %v", w.Code, answer)
	}
	row := account_hosted_get(alice, "b1")
	if _, err := atproto_create(context.Background(), alice, row, map[string]any{"text": "After moving"}, "", false); err == nil {
		t.Error("a deactivated account published")
	}
	if w, answer := xrpc_call(t, "POST", "com.atproto.repo.createRecord", access, map[string]any{"repo": did, "collection": "app.bsky.feed.post", "record": map[string]any{"text": "After moving"}}, nil); w.Code != http.StatusBadRequest || answer["error"] != "AccountDeactivated" {
		t.Errorf("an app wrote to a deactivated account: %d %v", w.Code, answer)
	}
	if w, answer := xrpc_call(t, "GET", "com.atproto.server.getSession", access, nil, nil); answer["active"] != false || answer["status"] != "deactivated" {
		t.Errorf("getSession of the deactivated account = %d %v", w.Code, answer)
	}
}

// TestXrpcMoveIn: an account arrives from another server - here root's c1,
// standing in for it - into alice's account made ready for it: the moving
// app proves the identity with a token the old server signed, imports the
// repository, uploads the blobs it names, has the old server sign the change
// of identity this server recommends, submits it, and activates the account,
// which is refused until the identity names this server and its key.
func TestXrpcMoveIn(t *testing.T) {
	alice, _, _, plc := create_xrpc_test_env(t)
	ctx := context.Background()
	root := user_by_uid("u-root")

	// The old server's account, with a post naming an image.
	if _, err := handle_reserve(root, "", "carol", "handle"); err != nil {
		t.Fatal(err)
	}
	db_user(root, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('c1', 'bluesky', '', 'carol.example.com', '{\"hosted\":true,\"handle\":\"carol\",\"domain\":\"example.com\"}', 1, 1)")
	if err := keypairs_create(root, "c1", "bluesky"); err != nil {
		t.Fatal(err)
	}
	actor_register(root, "c1", "bluesky", "example.com", "carol")
	if err := atproto_account_create(ctx, root, "c1"); err != nil {
		t.Fatal(err)
	}
	old := account_hosted_get(root, "c1")
	did := atproto_did(old)
	image := []byte("\x89PNG\r\n\x1a\n carried across")
	uploaded, err := atproto_upload(root, old, image, "image/png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := atproto_create(ctx, root, old, map[string]any{"text": "Before moving", "embed": map[string]any{"$type": "app.bsky.embed.images", "images": []any{map[string]any{"alt": "", "image": uploaded["data"]}}}}, "", false); err != nil {
		t.Fatal(err)
	}
	archive := atproto_request(t, "example.com", "/xrpc/com.atproto.sync.getRepo?did="+did).Body.Bytes()
	authorization, err := atproto_token(root, old, "did:web:example.com", "com.atproto.server.createAccount")
	if err != nil {
		t.Fatal(err)
	}
	// From here the old server is elsewhere.
	networks_database().exec("delete from identities where did=?", did)

	// Alice makes her account ready in Settings, on a second handle.
	saved := setting_effective("handles")
	setting_set("handles", "2")
	t.Cleanup(func() { setting_set("handles", saved) })
	if _, err := handle_reserve(alice, "", "arrival", "handle"); err != nil {
		t.Fatal(err)
	}
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	value, err := api_account_add(thread, fn, sl.Tuple{sl.String("bluesky")}, []sl.Tuple{{sl.String("handle"), sl.String("arrival")}, {sl.String("arriving"), sl.String("true")}})
	if err != nil {
		t.Fatal(err)
	}
	ready := sl_decode_map(value)
	id, _ := ready["id"].(string)
	if row := account_hosted_get(alice, id); row == nil || atproto_did(row) != "" || !atproto_deactivated(row) {
		t.Fatalf("the ready account = %v", row)
	}
	password := password_mint(alice, "", id, "bluesky", "Moving")

	body := map[string]any{"handle": "arrival.example.com", "did": did, "password": password}
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.createAccount", "", body, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("createAccount with no token from the old server = %d", w.Code)
	}
	// A token naming the identity but signed by a key it does not name.
	encode := func(v map[string]any) string { return base64.RawURLEncoding.EncodeToString([]byte(json_encode(v))) }
	unsigned := encode(map[string]any{"typ": "JWT", "alg": "ES256"}) + "." + encode(map[string]any{"iss": did, "aud": "did:web:example.com", "exp": now() + 60, "iat": now(), "jti": uid(), "lxm": "com.atproto.server.createAccount"})
	signature, err := key_sign_p256(keypair_get(alice, "b1", key_signing).Private, []byte(unsigned))
	if err != nil {
		t.Fatal(err)
	}
	forged := unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.createAccount", forged, body, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("createAccount with a token another identity signed = %d", w.Code)
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.createAccount", authorization, map[string]any{"handle": "arrival.example.com", "did": did, "password": "mochi-wrong"}, nil); w.Code != http.StatusBadRequest {
		t.Errorf("createAccount with a wrong password = %d", w.Code)
	}
	// An account not waiting for one keeps its own identity.
	live := password_mint(alice, "", "b1", "bluesky", "Live")
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.createAccount", authorization, map[string]any{"handle": "alice.example.com", "did": did, "password": live}, nil); w.Code != http.StatusBadRequest || atproto_did(account_hosted_get(alice, "b1")) == did {
		t.Errorf("createAccount took over an account not waiting for one: %d", w.Code)
	}
	w, answer := xrpc_call(t, "POST", "com.atproto.server.createAccount", authorization, body, nil)
	if w.Code != http.StatusOK || answer["did"] != did {
		t.Fatalf("createAccount = %d %s", w.Code, w.Body.String())
	}
	access, _ := answer["accessJwt"].(string)
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.createAccount", authorization, body, nil); w.Code == http.StatusOK {
		t.Error("the account arrived twice")
	}

	// The repository, then the blobs it names.
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.importRepo", access, archive, map[string]string{"Content-Type": "application/vnd.ipld.car"}); w.Code != http.StatusOK {
		t.Fatalf("importRepo = %d %s", w.Code, w.Body.String())
	}
	if items, _ := repository_records(alice, id, "app.bsky.feed.post", 10, "", false); len(items) != 1 || items[0]["value"].(map[string]any)["text"] != "Before moving" {
		t.Errorf("the imported posts = %v", items)
	}
	if _, profile := repository_record(alice, id, "app.bsky.actor.profile", "self"); profile == nil {
		t.Error("the imported profile is missing")
	}
	w, answer = xrpc_call(t, "GET", "com.atproto.repo.listMissingBlobs", access, nil, nil)
	if blobs, _ := answer["blobs"].([]any); w.Code != http.StatusOK || len(blobs) != 1 || blobs[0].(map[string]any)["cid"] != uploaded["id"] {
		t.Fatalf("listMissingBlobs = %d %v", w.Code, answer)
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.repo.uploadBlob", access, image, map[string]string{"Content-Type": "image/png"}); w.Code != http.StatusOK {
		t.Fatalf("uploadBlob = %d %s", w.Code, w.Body.String())
	}
	if _, answer := xrpc_call(t, "GET", "com.atproto.repo.listMissingBlobs", access, nil, nil); len(answer["blobs"].([]any)) != 0 {
		t.Errorf("missing after the upload = %v", answer)
	}
	_, status := xrpc_call(t, "GET", "com.atproto.server.checkAccountStatus", access, nil, nil)
	if status["activated"] != false || status["importedBlobs"] != float64(1) || status["expectedBlobs"] != float64(1) || status["indexedRecords"] == float64(0) {
		t.Errorf("checkAccountStatus = %v", status)
	}

	// Not yet: the identity names the old server's key.
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.activateAccount", access, nil, nil); w.Code != http.StatusBadRequest {
		t.Errorf("activating before the identity moved = %d", w.Code)
	}
	_, recommended := xrpc_call(t, "GET", "com.atproto.identity.getRecommendedDidCredentials", access, nil, nil)
	signing := keypair_get(alice, id, key_signing)
	methods, _ := recommended["verificationMethods"].(map[string]any)
	if methods["atproto"] != signing.Public || len(object_strings(recommended["rotationKeys"])) == 0 {
		t.Fatalf("recommended = %v", recommended)
	}

	// The old server signs the change; the moving app submits it here.
	_, previous, err := plc_last(ctx, did)
	if err != nil {
		t.Fatal(err)
	}
	_, private, _ := plc_keys(root, "c1")
	op := map[string]any{"type": "plc_operation", "prev": previous, "rotationKeys": recommended["rotationKeys"], "verificationMethods": recommended["verificationMethods"], "alsoKnownAs": recommended["alsoKnownAs"], "services": recommended["services"]}
	if err := plc_sign(op, private); err != nil {
		t.Fatal(err)
	}
	stranger := map[string]any{"type": "plc_operation", "prev": previous, "rotationKeys": recommended["rotationKeys"], "verificationMethods": map[string]any{"atproto": "did:key:zQ3shunBKsXixLxKtC5qeSG9E4J5RkGN57im31pcTzbNQnm5w"}, "alsoKnownAs": recommended["alsoKnownAs"], "services": recommended["services"]}
	plc_sign(stranger, private)
	if w, _ := xrpc_call(t, "POST", "com.atproto.identity.submitPlcOperation", access, map[string]any{"operation": stranger}, nil); w.Code != http.StatusBadRequest {
		t.Errorf("submitting a change that names another key = %d", w.Code)
	}
	if w, _ := xrpc_call(t, "POST", "com.atproto.identity.submitPlcOperation", access, map[string]any{"operation": op}, nil); w.Code != http.StatusOK {
		t.Fatalf("submitPlcOperation = %d %s", w.Code, w.Body.String())
	}
	if methods, _ := plc.last(did)["verificationMethods"].(map[string]any); methods["atproto"] != signing.Public {
		t.Error("the directory did not take the change")
	}

	events := len(stream_events(t, 0))
	if w, _ := xrpc_call(t, "POST", "com.atproto.server.activateAccount", access, nil, nil); w.Code != http.StatusOK {
		t.Fatalf("activateAccount = %d %s", w.Code, w.Body.String())
	}
	row := account_hosted_get(alice, id)
	if atproto_deactivated(row) || atproto_did(row) != did {
		t.Errorf("the arrived account = %v", row)
	}
	all := stream_events(t, 0)
	if len(all) != events+3 || all[events].RepoIdentity == nil || all[events+1].RepoAccount == nil || !all[events+1].RepoAccount.Active || all[events+2].RepoCommit == nil {
		t.Errorf("%d events on activating", len(all)-events)
	}
	atproto_commit_event(t)
	if _, status := atproto_json(t, "/xrpc/com.atproto.sync.getRepoStatus?did="+did); status["active"] != true {
		t.Errorf("getRepoStatus = %v", status)
	}
	if user, account, _ := atproto_identity(did); user == nil || row_string(account, "id") != id {
		t.Error("the identity is not registered to the arrived account")
	}
}

// TestXrpcHome: a hosted fediverse or Bluesky account's home feed is set,
// shown with the account and cleared; another account has none, and an
// identifier that is not a feed's is refused.
func TestXrpcHome(t *testing.T) {
	alice, _, _, _ := create_xrpc_test_env(t)
	thread, fn := atproto_thread(alice), sl.NewBuiltin("test", nil)
	permission_grant(alice, "app2", "account:*")
	update := func(id string, home string) error {
		_, err := api_account_update(thread, fn, sl.Tuple{sl.String(id)}, []sl.Tuple{{sl.String("home"), sl.String(home)}})
		return err
	}
	shown := func(id string) any {
		row, _ := db_user(alice, "user").row("select id, type, label, identifier, data, created, verified, enabled, \"default\", device from accounts where id=?", id)
		return account_present(alice, row)["home"]
	}
	feed := "1" + strings.Repeat("a", 49)
	if err := update("b1", feed); err != nil {
		t.Fatal(err)
	}
	if shown("b1") != feed {
		t.Errorf("home shown = %v", shown("b1"))
	}
	if err := update("b1", "not a feed"); err == nil {
		t.Error("an identifier that is not a feed's was taken")
	}
	db_user(alice, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('x1', 'xmpp', '', 'alice@example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	if err := update("x1", feed); err == nil {
		t.Error("an XMPP account was given a home feed")
	}
	if shown("x1") != nil {
		t.Errorf("an XMPP account shows a home feed: %v", shown("x1"))
	}
	if err := update("b1", ""); err != nil {
		t.Fatal(err)
	}
	if shown("b1") != "" {
		t.Errorf("home after clearing = %v", shown("b1"))
	}
}
