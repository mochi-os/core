// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// create_activitypub_test_env is create_delivery_test_env with alice's handle
// reserved and her account f1 in the actors table, as creating the account
// would leave it, and a remote person bob at remote.example whose documents
// resolve_fetch answers and whose key signs the requests the tests send.
type remote_person struct {
	id      string
	key_id  string
	inbox   string
	public  string
	private string
}

func create_activitypub_test_env(t *testing.T) (*User, *remote_person) {
	t.Helper()
	alice := create_delivery_test_env(t)
	if _, err := handle_reserve(alice, "", "alice", "handle"); err != nil {
		t.Fatal(err)
	}
	actor_register(alice, "f1", "fediverse", "example.com", "alice")
	gin.SetMode(gin.TestMode)

	public, private, err := key_generate(key_rsa)
	if err != nil {
		t.Fatal(err)
	}
	bob := &remote_person{id: "https://remote.example/u/bob", key_id: "https://remote.example/u/bob#main-key", inbox: "https://remote.example/u/bob/inbox", public: public, private: private}
	saved_fetch, saved_keys := resolve_fetch, signature_keys
	signature_keys = map[string]signature_key_entry{}
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if address == bob.id {
			return []byte(`{"id":"` + bob.id + `","type":"Person","preferredUsername":"bob","inbox":"` + bob.inbox + `","endpoints":{"sharedInbox":"https://remote.example/inbox"},"publicKey":{"id":"` + bob.key_id + `","owner":"` + bob.id + `","publicKeyPem":` + json_encode(bob.public) + `}}`), nil
		}
		return nil, &resolve_refusal{Status: 404}
	}
	t.Cleanup(func() { resolve_fetch, signature_keys = saved_fetch, saved_keys })
	return alice, bob
}

// activitypub_request drives the engine with one request and returns the
// recorder.
func activitypub_request(t *testing.T, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = r
	if !activitypub_route(c) {
		t.Fatal("activitypub_route did not answer")
	}
	return w
}

// activitypub_get fetches a path as a fediverse server would.
func activitypub_get(t *testing.T, path string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest("GET", "https://example.com"+path, nil)
	r.Header.Set("Accept", activitypub_type)
	w := activitypub_request(t, r)
	var document map[string]any
	json.Unmarshal(w.Body.Bytes(), &document)
	return w, document
}

// signed_post posts an activity to a path, signed as the remote person, with
// the body given or the activity encoded.
func signed_post(t *testing.T, remote *remote_person, path string, activity map[string]any, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	if body == nil {
		body = []byte(json_encode(activity))
	}
	r := httptest.NewRequest("POST", "https://example.com"+path, bytes.NewReader(body))
	r.Header.Set("Content-Type", activitypub_type)
	if err := signature_sign(r, remote.key_id, remote.private, body); err != nil {
		t.Fatal(err)
	}
	return activitypub_request(t, r)
}

func follow_activity(remote *remote_person, id string) map[string]any {
	return map[string]any{"@context": activitystreams, "id": id, "type": "Follow", "actor": remote.id, "object": "https://example.com/~f1"}
}

// TestActivityPubDiscovery: WebFinger answers the handle, the id and the page
// with the actor's self link and the subscribe template, host-meta names
// WebFinger, NodeInfo names the software, and an unknown name is not found.
func TestActivityPubDiscovery(t *testing.T) {
	create_activitypub_test_env(t)
	for _, resource := range []string{"acct:alice@example.com", "acct:Alice@Example.com", "https://example.com/~f1", "https://example.com/@alice"} {
		r := httptest.NewRequest("GET", "https://example.com/.well-known/webfinger?resource="+resource, nil)
		w := activitypub_request(t, r)
		if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), activitypub_jrd) {
			t.Fatalf("webfinger %s: %d %s", resource, w.Code, w.Header().Get("Content-Type"))
		}
		var jrd struct {
			Subject string
			Links   []map[string]string
		}
		json.Unmarshal(w.Body.Bytes(), &jrd)
		if jrd.Subject != "acct:alice@example.com" {
			t.Errorf("webfinger %s subject %q", resource, jrd.Subject)
		}
		self, template := "", ""
		for _, link := range jrd.Links {
			if link["rel"] == "self" {
				self = link["href"]
			}
			if link["rel"] == activitypub_subscribe {
				template = link["template"]
			}
		}
		if self != "https://example.com/~f1" || template != "https://example.com/~/authorize?uri={uri}" {
			t.Errorf("webfinger %s links = %v", resource, jrd.Links)
		}
	}
	for _, resource := range []string{"acct:carol@example.com", "acct:alice@other.example", "https://other.example/~f1", "", "acct:alice"} {
		w := activitypub_request(t, httptest.NewRequest("GET", "https://example.com/.well-known/webfinger?resource="+resource, nil))
		if w.Code != http.StatusNotFound {
			t.Errorf("webfinger %q: %d, want 404", resource, w.Code)
		}
	}

	w := activitypub_request(t, httptest.NewRequest("GET", "https://example.com/.well-known/host-meta", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `template="https://example.com/.well-known/webfinger?resource={uri}"`) {
		t.Errorf("host-meta: %d %s", w.Code, w.Body.String())
	}

	w = activitypub_request(t, httptest.NewRequest("GET", "https://example.com/.well-known/nodeinfo", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "https://example.com/nodeinfo/2.1") {
		t.Errorf("nodeinfo links: %d %s", w.Code, w.Body.String())
	}
	setting_set("description", "A test server")
	w = activitypub_request(t, httptest.NewRequest("GET", "https://example.com/nodeinfo/2.1", nil))
	var nodeinfo struct {
		Software  map[string]any
		Protocols []string
		Metadata  map[string]any
		Usage     map[string]map[string]any
	}
	json.Unmarshal(w.Body.Bytes(), &nodeinfo)
	if w.Code != http.StatusOK || nodeinfo.Software["name"] != "mochi" || len(nodeinfo.Protocols) != 1 || nodeinfo.Protocols[0] != "activitypub" || nodeinfo.Metadata["nodeDescription"] != "A test server" {
		t.Errorf("nodeinfo: %d %s", w.Code, w.Body.String())
	}
	if nodeinfo.Usage["users"]["total"] != float64(2) {
		t.Errorf("nodeinfo users = %v, want 2", nodeinfo.Usage["users"]["total"])
	}
}

// TestActivityPubActor: the actor document carries the id, the handle, the
// key, the collections and the shared inbox; a Service when automated; the
// profile the people app gives; a browser is sent to the person's page, as
// from the handle address; a server asking for the handle is sent to the id.
func TestActivityPubActor(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	w, document := activitypub_get(t, "/~f1")
	if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), activitypub_type) {
		t.Fatalf("actor: %d %s", w.Code, w.Header().Get("Content-Type"))
	}
	key := keypair_get(alice, "f1", key_signing)
	public, _ := document["publicKey"].(map[string]any)
	endpoints, _ := document["endpoints"].(map[string]any)
	if document["id"] != "https://example.com/~f1" || document["type"] != "Person" || document["preferredUsername"] != "alice" || document["name"] != "Alice" ||
		document["inbox"] != "https://example.com/~f1/inbox" || document["outbox"] != "https://example.com/~f1/outbox" || document["followers"] != "https://example.com/~f1/followers" ||
		document["url"] != "https://example.com/@alice" || endpoints["sharedInbox"] != "https://example.com/~/inbox" ||
		public["id"] != "https://example.com/~f1#main-key" || public["owner"] != "https://example.com/~f1" || public["publicKeyPem"] != key.Public ||
		document["manuallyApprovesFollowers"] != false || document["discoverable"] != true {
		t.Errorf("actor document = %s", w.Body.String())
	}

	// The people app describes the person; the account's label names it.
	saved := activitypub_call
	activitypub_call = func(user *User, a *App, function string, args Map) (any, error) {
		if function != "activitypub/profile" || args["account"] != "f1" {
			t.Errorf("profile call %s %v", function, args)
		}
		return map[string]any{"summary": "<p>Hi</p>", "avatar": "https://example.com/people/fp-alice1/-/avatar", "links": []any{map[string]any{"name": "Mastodon", "url": "https://mastodon.example/@alice"}}}, nil
	}
	t.Cleanup(func() { activitypub_call = saved })
	apps_lock.Lock()
	saved_apps := apps
	apps = map[string]*App{"people": {id: "people", internal: &AppVersion{Functions: map[string]AppFunction{"activitypub/profile": {Function: "function_activitypub_profile"}}}}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = saved_apps
		apps_lock.Unlock()
	})
	db := db_user(alice, "user")
	db.exec("update accounts set label='Alice in Wonderland', data='{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\",\"automated\":true}' where id='f1'")
	_, document = activitypub_get(t, "/~f1")
	icon, _ := document["icon"].(map[string]any)
	attachments, _ := document["attachment"].([]any)
	if document["type"] != "Service" || document["name"] != "Alice in Wonderland" || document["summary"] != "<p>Hi</p>" || icon["url"] != "https://example.com/people/fp-alice1/-/avatar" || len(attachments) != 1 {
		t.Errorf("actor with profile = %v", document)
	}
	if field, _ := attachments[0].(map[string]any); field["type"] != "PropertyValue" || field["name"] != "Mastodon" || !strings.Contains(field["value"].(string), `rel="me nofollow noopener noreferrer"`) || !strings.Contains(field["value"].(string), "https://mastodon.example/@alice") {
		t.Errorf("attachment = %v", attachments[0])
	}

	// Collections answer their counts.
	db.exec("insert into followers (account, actor, inbox, shared, created) values ('f1', 'https://remote.example/u/bob', 'https://remote.example/u/bob/inbox', '', 1)")
	_, document = activitypub_get(t, "/~f1/followers")
	if document["type"] != "OrderedCollection" || document["totalItems"] != float64(1) {
		t.Errorf("followers = %v", document)
	}

	// A browser lands on the person's page from both addresses; a server
	// asking for the handle is sent to the id.
	for _, path := range []string{"/~f1", "/@alice", "/@Alice"} {
		r := httptest.NewRequest("GET", "https://example.com"+path, nil)
		r.Header.Set("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")
		w := activitypub_request(t, r)
		if w.Code != http.StatusFound || w.Header().Get("Location") != "/people/fp-alice1" {
			t.Errorf("%s for a browser: %d %s", path, w.Code, w.Header().Get("Location"))
		}
	}
	w, _ = activitypub_get(t, "/@alice")
	if w.Code != http.StatusFound || w.Header().Get("Location") != "https://example.com/~f1" {
		t.Errorf("/@alice for a server: %d %s", w.Code, w.Header().Get("Location"))
	}
	for _, path := range []string{"/~nobody", "/@carol", "/~f1/nothing/here", "/~b1"} {
		w, _ := activitypub_get(t, path)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s: %d, want 404", path, w.Code)
		}
	}

	// The subscribe landing hands the address to the feeds lookup.
	w = activitypub_request(t, httptest.NewRequest("GET", "https://example.com/~/authorize?uri=acct%3Abob%40remote.example", nil))
	if w.Code != http.StatusFound || w.Header().Get("Location") != "/feeds/find?link=acct%3Abob%40remote.example" {
		t.Errorf("authorize: %d %s", w.Code, w.Header().Get("Location"))
	}
}

// TestActivityPubSeenPerAccount: an activity reaching the server through two
// accounts' inboxes - as one sent to a person and to a follower on another of
// the server's domains does - is given to both; a replay to either is dropped.
func TestActivityPubSeenPerAccount(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('f2', 'fediverse', '', '@second@example.com', '{\"hosted\":true,\"handle\":\"second\",\"domain\":\"example.com\"}', 1, 1)")
	if err := keypairs_create(alice, "f2", "fediverse"); err != nil {
		t.Fatal(err)
	}
	actor_register(alice, "f2", "fediverse", "example.com", "second")
	create := map[string]any{"@context": activitystreams, "id": bob.id + "/notes/1/activity", "type": "Create", "actor": bob.id,
		"object": map[string]any{"id": bob.id + "/notes/1", "type": "Note", "attributedTo": bob.id, "content": "<p>Hello</p>"}}
	queued := func(account string) int64 {
		n, _ := networks_database().row("select count(*) as n from inbound where account=?", account)
		return row_int(n, "n")
	}
	for _, path := range []string{"/~f1/inbox", "/~f2/inbox", "/~f1/inbox"} {
		if w := signed_post(t, bob, path, create, nil); w.Code != http.StatusAccepted {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
	if queued("f1") != 1 || queued("f2") != 1 {
		t.Errorf("queued f1 %d, f2 %d; want the activity once for each", queued("f1"), queued("f2"))
	}
}

// TestActivityPubFollow: a signed Follow records the follower with their inbox
// and shared inbox and queues an Accept to them; the same activity again is
// dropped; an Undo forgets them; a Follow of someone else is ignored.
func TestActivityPubFollow(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	db := db_user(alice, "user")

	w := signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/1"), nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("follow: %d %s", w.Code, w.Body.String())
	}
	row, _ := db.row("select actor, inbox, shared from followers where account='f1'")
	if row == nil || row["actor"] != bob.id || row["inbox"] != bob.inbox || row["shared"] != "https://remote.example/inbox" {
		t.Fatalf("followers row = %v", row)
	}
	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["inbox"] != bob.inbox {
		t.Fatalf("deliveries = %v, want the Accept to bob's inbox", rows)
	}
	_, accept := delivery_body(t)
	object, _ := accept["object"].(map[string]any)
	if accept["type"] != "Accept" || accept["actor"] != "https://example.com/~f1" || object["type"] != "Follow" || object["id"] != "https://remote.example/follows/1" || object["actor"] != bob.id || object["object"] != "https://example.com/~f1" {
		t.Errorf("Accept = %v", accept)
	}

	// A replay is taken and dropped: no second Accept.
	w = signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/1"), nil)
	if w.Code != http.StatusAccepted || len(delivery_rows(t)) != 1 {
		t.Errorf("replay: %d, %d deliveries", w.Code, len(delivery_rows(t)))
	}

	// A Follow of some other actor is nothing to this one.
	other := follow_activity(bob, "https://remote.example/follows/2")
	other["object"] = "https://example.com/~b1"
	signed_post(t, bob, "/~f1/inbox", other, nil)
	if n, _ := db.row("select count(*) as n from followers where account='f1'"); row_int(n, "n") != 1 {
		t.Errorf("a Follow of another actor recorded a follower")
	}

	// Undo of the Follow forgets the follower.
	undo := map[string]any{"@context": activitystreams, "id": "https://remote.example/follows/1/undo", "type": "Undo", "actor": bob.id,
		"object": map[string]any{"id": "https://remote.example/follows/1", "type": "Follow", "actor": bob.id, "object": "https://example.com/~f1"}}
	w = signed_post(t, bob, "/~f1/inbox", undo, nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("undo: %d", w.Code)
	}
	if found, _ := db.exists("select 1 from followers where account='f1'"); found {
		t.Error("follower kept after Undo")
	}
	// The Follow was handed to the apps once it was accepted, for a
	// notification; the replay, the Follow of another actor and the Undo were
	// not.
	queued, _ := networks_database().rows("select type from inbound")
	if len(queued) != 1 || queued[0]["type"] != "Follow" {
		t.Errorf("queued for the apps: %v, want the one Follow", queued)
	}

	// The shared inbox finds the account from the Follow's object.
	w = signed_post(t, bob, "/~/inbox", follow_activity(bob, "https://remote.example/follows/3"), nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("shared inbox: %d %s", w.Code, w.Body.String())
	}
	if found, _ := db.exists("select 1 from followers where account='f1' and actor=?", bob.id); !found {
		t.Error("the shared inbox did not record the follower")
	}
}

// TestActivityPubInboxRefusals: an unsigned request, a tampered body, a key
// from another host, a body too large or malformed, and an actor that is not a
// fediverse id are refused; a blocked host is taken and dropped.
func TestActivityPubInboxRefusals(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	db := db_user(alice, "user")
	before := len(delivery_rows(t))

	r := httptest.NewRequest("POST", "https://example.com/~f1/inbox", bytes.NewReader([]byte(json_encode(follow_activity(bob, "https://remote.example/follows/u1")))))
	if w := activitypub_request(t, r); w.Code != http.StatusUnauthorized {
		t.Errorf("unsigned: %d", w.Code)
	}

	body := []byte(json_encode(follow_activity(bob, "https://remote.example/follows/u2")))
	r = httptest.NewRequest("POST", "https://example.com/~f1/inbox", bytes.NewReader(append(body, ' ')))
	signature_sign(r, bob.key_id, bob.private, body)
	if w := activitypub_request(t, r); w.Code != http.StatusUnauthorized {
		t.Errorf("tampered body: %d", w.Code)
	}

	// Bob's key signing an activity claimed by someone on another host.
	claimed := follow_activity(bob, "https://remote.example/follows/u3")
	claimed["actor"] = "https://other.example/u/mallory"
	if w := signed_post(t, bob, "/~f1/inbox", claimed, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("key of another host: %d", w.Code)
	}

	if w := signed_post(t, bob, "/~f1/inbox", nil, []byte("not json")); w.Code != http.StatusBadRequest {
		t.Errorf("malformed: %d", w.Code)
	}
	bad := follow_activity(bob, "https://remote.example/follows/u4")
	bad["actor"] = "not an id"
	if w := signed_post(t, bob, "/~f1/inbox", bad, nil); w.Code != http.StatusBadRequest {
		t.Errorf("actor not an id: %d", w.Code)
	}
	huge := follow_activity(bob, "https://remote.example/follows/u5")
	huge["content"] = strings.Repeat("x", activitypub_body_maximum)
	if w := signed_post(t, bob, "/~f1/inbox", huge, nil); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized: %d", w.Code)
	}

	block_add(db, "remote.example")
	if w := signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/u6"), nil); w.Code != http.StatusAccepted {
		t.Errorf("blocked host: %d, want a silent 202", w.Code)
	}
	block_remove(db, "remote.example")

	if found, _ := db.exists("select 1 from followers where account='f1'"); found {
		t.Error("a refused Follow recorded a follower")
	}
	if len(delivery_rows(t)) != before {
		t.Error("a refused Follow queued a delivery")
	}

	// A deleted actor nobody here knows is taken without a fetch.
	fetches := 0
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		fetches++
		return saved(ctx, address, accept)
	}
	t.Cleanup(func() { resolve_fetch = saved })
	gone := map[string]any{"@context": activitystreams, "id": "https://gone.example/u/x#delete", "type": "Delete", "actor": "https://gone.example/u/x", "object": "https://gone.example/u/x"}
	r = httptest.NewRequest("POST", "https://example.com/~f1/inbox", bytes.NewReader([]byte(json_encode(gone))))
	if w := activitypub_request(t, r); w.Code != http.StatusAccepted || fetches != 0 {
		t.Errorf("delete of an unknown actor: %d after %d fetches", w.Code, fetches)
	}
}

// TestActivityPubLoopbackRemote: under the development switch a stand-in
// server on loopback acts over http, as the harness's does; outside it an
// http actor is refused.
func TestActivityPubLoopbackRemote(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	public, private, _ := key_generate(key_rsa)
	local := &remote_person{id: "http://127.0.0.1:9999/users/carol", key_id: "http://127.0.0.1:9999/users/carol#main-key", inbox: "http://127.0.0.1:9999/inbox", public: public, private: private}
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if address == local.id {
			return []byte(`{"id":"` + local.id + `","type":"Person","preferredUsername":"carol","inbox":"` + local.inbox + `","publicKey":{"id":"` + local.key_id + `","owner":"` + local.id + `","publicKeyPem":` + json_encode(public) + `}}`), nil
		}
		return saved(ctx, address, accept)
	}
	t.Cleanup(func() { resolve_fetch = saved })

	w := signed_post(t, local, "/~f1/inbox", follow_activity(local, "http://127.0.0.1:9999/follows/1"), nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("loopback follow: %d %s", w.Code, w.Body.String())
	}
	if found, _ := db_user(alice, "user").exists("select 1 from followers where account='f1' and actor=?", local.id); !found {
		t.Error("the loopback follower was not recorded")
	}

	url_allow_private = false
	w = signed_post(t, local, "/~f1/inbox", follow_activity(local, "http://127.0.0.1:9999/follows/2"), nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("http actor without the switch: %d, want 400", w.Code)
	}
	url_allow_private = true
}

// TestActivityPubQueue: an activity the engine does not handle itself is queued
// for the account and handed to the apps declaring activitypub/receive in
// batches, each account in turn, and the rows go once offered.
func TestActivityPubQueue(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	like := map[string]any{"@context": activitystreams, "id": "https://remote.example/likes/1", "type": "Like", "actor": bob.id, "object": "https://example.com/~f1/post1"}
	if w := signed_post(t, bob, "/~f1/inbox", like, nil); w.Code != http.StatusAccepted {
		t.Fatalf("like: %d", w.Code)
	}
	db := networks_database()
	row, _ := db.row("select user, account, actor, type from inbound")
	if row == nil || row["user"] != alice.UID || row["account"] != "f1" || row["actor"] != bob.id || row["type"] != "Like" {
		t.Fatalf("inbound row = %v", row)
	}
	// A second account's rows, so the pass takes one batch from each.
	for i := 0; i < 3; i++ {
		inbound_add(alice, "f2", bob.id, "Like", []byte(`{"type":"Like"}`))
	}

	var calls []string
	saved_call, saved_batch := activitypub_call, inbound_batch
	inbound_batch = 2
	activitypub_call = func(user *User, a *App, function string, args Map) (any, error) {
		activities, _ := args["activities"].([]any)
		calls = append(calls, a.id+":"+args["account"].(string)+":"+itoa(len(activities)))
		if function != "activitypub/receive" || user.UID != alice.UID {
			t.Errorf("call %s as %s", function, user.UID)
		}
		if first, _ := activities[0].(map[string]any); first["type"] != "Like" || first["actor"] != bob.id {
			t.Errorf("activity = %v", first)
		}
		return nil, nil
	}
	t.Cleanup(func() { activitypub_call, inbound_batch = saved_call, saved_batch })
	apps_lock.Lock()
	saved_apps := apps
	apps = map[string]*App{"feeds": {id: "feeds", internal: &AppVersion{Functions: map[string]AppFunction{"activitypub/receive": {Function: "function_receive"}}}}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = saved_apps
		apps_lock.Unlock()
	})

	if n := inbound_process(); n != 3 {
		t.Errorf("first pass handled %d rows, want 3 (one for f1, a batch of two for f2)", n)
	}
	if strings.Join(calls, " ") != "feeds:f1:1 feeds:f2:2" {
		t.Errorf("calls = %v", calls)
	}
	if n := inbound_process(); n != 1 {
		t.Errorf("second pass handled %d rows, want the last of f2", n)
	}
	if n, _ := db.row("select count(*) as n from inbound"); row_int(n, "n") != 0 {
		t.Error("rows left in the queue after they were offered")
	}
	if n := inbound_process(); n != 0 {
		t.Errorf("an empty queue handled %d", n)
	}
}

// TestActivityPubObjects: an object the account published is served from its
// app under its id, a deleted one as a tombstone, the outbox lists them newest
// first with a page boundary, and a browser is sent to the object's page.
func TestActivityPubObjects(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	db := db_user(alice, "user")
	for i, id := range []string{"o1", "o2", "o3"} {
		db.exec("insert into objects (id, account, app, type, addressing, created) values (?, 'f1', 'feeds', 'Note', ?, ?)", id, json_encode(map[string]any{"to": []string{activitystreams_public}, "cc": []string{"https://example.com/~f1/followers"}}), 1000+i)
	}
	db.exec("update objects set deleted=2000 where id='o2'")
	saved_call := activitypub_call
	activitypub_call = func(user *User, a *App, function string, args Map) (any, error) {
		if function != "activitypub/object" || a.id != "feeds" || args["account"] != "f1" {
			t.Errorf("call %s to %s with %v", function, a.id, args)
		}
		id, _ := args["id"].(string)
		if strings.HasSuffix(id, "/o3") {
			return nil, nil
		}
		return map[string]any{"type": "Note", "content": "<p>" + id + "</p>", "url": "https://example.com/feeds/abc/" + strings.TrimPrefix(id, "https://example.com/~f1/")}, nil
	}
	t.Cleanup(func() { activitypub_call = saved_call })
	apps_lock.Lock()
	saved_apps := apps
	apps = map[string]*App{"feeds": {id: "feeds", internal: &AppVersion{}}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = saved_apps
		apps_lock.Unlock()
	})

	w, object := activitypub_get(t, "/~f1/o1")
	if w.Code != http.StatusOK || object["id"] != "https://example.com/~f1/o1" || object["attributedTo"] != "https://example.com/~f1" || object["content"] != "<p>https://example.com/~f1/o1</p>" || object["published"] == nil {
		t.Errorf("object = %d %v", w.Code, object)
	}
	if to, _ := object["to"].([]any); len(to) != 1 || to[0] != activitystreams_public {
		t.Errorf("object addressing = %v", object["to"])
	}
	// The Create the object went out in is served at its id, so a server
	// told of it by a group's Announce can fetch it; a deleted object's is gone.
	w, create := activitypub_get(t, "/~f1/o1/activity")
	if inner, _ := create["object"].(map[string]any); w.Code != http.StatusOK || create["type"] != "Create" || create["id"] != "https://example.com/~f1/o1/activity" || create["actor"] != "https://example.com/~f1" || inner["id"] != "https://example.com/~f1/o1" {
		t.Errorf("create activity = %d %v", w.Code, create)
	}
	if w, _ := activitypub_get(t, "/~f1/o2/activity"); w.Code != http.StatusNotFound {
		t.Errorf("a deleted object's create activity: %d", w.Code)
	}
	w, object = activitypub_get(t, "/~f1/o2")
	if w.Code != http.StatusGone || object["type"] != "Tombstone" || object["formerType"] != "Note" {
		t.Errorf("deleted object = %d %v", w.Code, object)
	}
	if w, _ := activitypub_get(t, "/~f1/o3"); w.Code != http.StatusNotFound {
		t.Errorf("object the app no longer has: %d", w.Code)
	}
	r := httptest.NewRequest("GET", "https://example.com/~f1/o1", nil)
	r.Header.Set("Accept", "text/html")
	if w := activitypub_request(t, r); w.Code != http.StatusFound || w.Header().Get("Location") != "https://example.com/feeds/abc/o1" {
		t.Errorf("object for a browser: %d %s", w.Code, w.Header().Get("Location"))
	}

	w, outbox := activitypub_get(t, "/~f1/outbox")
	if w.Code != http.StatusOK || outbox["totalItems"] != float64(2) || outbox["first"] != "https://example.com/~f1/outbox?page=true" {
		t.Errorf("outbox = %v", outbox)
	}
	_, page := activitypub_get(t, "/~f1/outbox?page=true")
	items, _ := page["orderedItems"].([]any)
	if page["type"] != "OrderedCollectionPage" || len(items) != 1 {
		t.Fatalf("outbox page = %v", page)
	}
	if create, _ := items[0].(map[string]any); create["type"] != "Create" || create["actor"] != "https://example.com/~f1" || create["object"].(map[string]any)["id"] != "https://example.com/~f1/o1" {
		t.Errorf("outbox item = %v", items[0])
	}
}

// TestActivityPubRemoval: removing a hosted account sends Delete of the actor
// to its followers, signed with the key the tombstone keeps, after which the
// id answers gone and WebFinger no longer names it.
func TestActivityPubRemoval(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	box := create_inbox(t, alice)
	key := keypair_get(alice, "f1", key_signing)
	db := db_user(alice, "user")
	db.exec("insert into followers (account, actor, inbox, shared, created) values ('f1', 'https://remote.example/u/bob', ?, ?, 1)", box.server.URL+"/u/bob/inbox", box.server.URL+"/inbox")
	db.exec("insert into followers (account, actor, inbox, shared, created) values ('f1', 'https://remote.example/u/carol', ?, ?, 1)", box.server.URL+"/u/carol/inbox", box.server.URL+"/inbox")

	activitypub_account_remove(alice, "f1")
	keypairs_delete(db, "f1")
	db.row_remove(reg_accounts, map[string]any{"id": "f1"})

	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["inbox"] != box.server.URL+"/inbox" {
		t.Fatalf("deliveries = %v, want one Delete to the shared inbox", rows)
	}
	_, activity := delivery_body(t)
	if activity["type"] != "Delete" || activity["actor"] != "https://example.com/~f1" || activity["object"] != "https://example.com/~f1" {
		t.Errorf("Delete = %v", activity)
	}
	delivery_process()
	if box.count() != 1 {
		t.Fatalf("%d requests delivered, want 1", box.count())
	}
	if box.errors[0] != nil {
		t.Errorf("the Delete's signature did not verify against the kept key: %v", box.errors[0])
	}
	w, document := activitypub_get(t, "/~f1")
	if w.Code != http.StatusGone || document["type"] != "Tombstone" {
		t.Errorf("removed actor: %d %v", w.Code, document)
	}
	if public, _ := document["publicKey"].(map[string]any); public["publicKeyPem"] != key.Public {
		t.Errorf("the tombstone does not carry the key the Delete was signed with: %v", document["publicKey"])
	}
	if w, _ := activitypub_get(t, "/~f1/inbox"); w.Code != http.StatusGone {
		t.Errorf("removed actor's inbox: %d", w.Code)
	}
	w = activitypub_request(t, httptest.NewRequest("GET", "https://example.com/.well-known/webfinger?resource=acct:alice@example.com", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("webfinger after removal: %d", w.Code)
	}
	if found, _ := db.exists("select 1 from followers where account='f1'"); found {
		t.Error("followers kept after removal")
	}

	// The tombstone is dropped after its time.
	networks_database().exec("update actors set deleted=1 where id='f1'")
	activitypub_cleanup()
	if actor_get("f1") != nil {
		t.Error("tombstone kept past its age")
	}
}

// TestActivityPubRebuild: the actors table is rebuilt from the registry and
// the accounts, so a server whose networks database was lost finds its
// accounts again, and a row for an account that is gone is dropped.
func TestActivityPubRebuild(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	db := networks_database()
	db.exec("delete from actors")
	db.exec("insert into actors (id, user, domain, handle, network, created) values ('stale', ?, 'example.com', 'alice', 'fediverse', 1)", alice.UID)
	actors_rebuild()
	if a := actor_get("f1"); a == nil || a.User != alice.UID || a.Domain != "example.com" || a.Handle != "alice" || a.Network != "fediverse" {
		t.Errorf("rebuilt actor = %v", a)
	}
	if actor_get("b1") == nil {
		t.Error("the Bluesky account on the handle was not rebuilt")
	}
	if actor_get("stale") != nil {
		t.Error("a row for an account that is gone was kept")
	}
}

// TestActivityPubEngineOff: with the fediverse setting off the engine's paths
// are not found, as before it existed.
func TestActivityPubEngineOff(t *testing.T) {
	create_activitypub_test_env(t)
	engines_test_reset(t)
	engine_register(activitypub_engine())
	setting_set("fediverse", "false")
	t.Cleanup(func() { setting_set("fediverse", "true") })
	for _, path := range []string{"/.well-known/webfinger?resource=acct:alice@example.com", "/~f1", "/@alice", "/nodeinfo/2.1"} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "https://example.com"+path, nil)
		if !engine_route(c) || w.Code != http.StatusNotFound {
			t.Errorf("%s with the fediverse off: %d", path, w.Code)
		}
	}
}

// TestActivityPubSignedLookup: a server that gives its documents to signed
// requests only, as GoToSocial does, still has its people resolved by the
// engine, which signs the lookup as the account; a failure remembered from an
// unsigned lookup does not stand in the way.
func TestActivityPubSignedLookup(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	saved := resolve_fetch
	signed := 0
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if address != bob.id {
			return nil, &resolve_refusal{Status: 404}
		}
		request, _ := http.NewRequestWithContext(ctx, "GET", address, nil)
		if sign := resolve_signed_by(ctx); sign != nil {
			sign(request)
		}
		if request.Header.Get("Signature") == "" {
			return nil, &resolve_refusal{Status: 401}
		}
		signed++
		return saved(ctx, address, accept)
	}
	t.Cleanup(func() { resolve_fetch = saved })

	// An unsigned lookup fails and is remembered.
	if _, err := author_resolve(context.Background(), bob.id); err == nil {
		t.Fatal("an unsigned lookup of a secure server succeeded")
	}
	w := signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/s1"), nil)
	if w.Code != http.StatusAccepted {
		t.Fatalf("follow: %d %s", w.Code, w.Body.String())
	}
	if found, _ := db_user(alice, "user").exists("select 1 from followers where account='f1' and actor=?", bob.id); !found {
		t.Error("the follower on a secure server was not recorded")
	}
	if signed == 0 {
		t.Error("no lookup was signed")
	}
	if rows := delivery_rows(t); len(rows) != 1 || rows[0]["inbox"] != bob.inbox {
		t.Errorf("deliveries = %v, want the Accept to bob's inbox", rows)
	}
}
