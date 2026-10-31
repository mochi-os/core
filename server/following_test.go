// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// following_documents answers the documents of the people the following
// tests know: bob and carol at remote.example, carol naming bob among her
// own identities, bob's outbox with a post, a reply and a boost.
func following_documents(t *testing.T, bob *remote_person) {
	t.Helper()
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		switch {
		case strings.Contains(address, "webfinger") && strings.Contains(address, "carol"):
			return []byte(`{"subject":"acct:carol@remote.example","links":[{"rel":"self","type":"application/activity+json","href":"https://remote.example/u/carol"}]}`), nil
		case address == bob.id:
			return []byte(`{"id":"` + bob.id + `","type":"Person","preferredUsername":"bob","inbox":"` + bob.inbox + `","outbox":"` + bob.id + `/outbox","endpoints":{"sharedInbox":"https://remote.example/inbox"},"publicKey":{"id":"` + bob.key_id + `","owner":"` + bob.id + `","publicKeyPem":` + json_encode(bob.public) + `}}`), nil
		case address == "https://remote.example/u/carol":
			return []byte(`{"id":"https://remote.example/u/carol","type":"Person","preferredUsername":"carol","inbox":"https://remote.example/u/carol/inbox","alsoKnownAs":["` + bob.id + `","https://example.com/~f1"],"manuallyApprovesFollowers":true}`), nil
		case address == "https://remote.example/u/dave":
			return []byte(`{"id":"https://remote.example/u/dave","type":"Person","preferredUsername":"dave","inbox":"https://remote.example/u/dave/inbox"}`), nil
		case address == bob.id+"/outbox":
			return []byte(`{"id":"` + bob.id + `/outbox","type":"OrderedCollection","totalItems":3,"first":"` + bob.id + `/outbox?page=true"}`), nil
		case address == bob.id+"/outbox?page=true":
			return []byte(`{"type":"OrderedCollectionPage","next":"` + bob.id + `/outbox?page=true&max_id=1","orderedItems":[` +
				`{"type":"Create","actor":"` + bob.id + `","object":{"id":"` + bob.id + `/notes/3","type":"Note","attributedTo":"` + bob.id + `","content":"<p>Three</p>","to":["` + activitystreams_public + `"]}},` +
				`{"type":"Announce","actor":"` + bob.id + `","object":"https://other.example/u/eve/notes/9","published":"2026-10-14T10:00:00Z"},` +
				`{"type":"Create","actor":"` + bob.id + `","object":{"id":"` + bob.id + `/notes/2","type":"Note","attributedTo":"` + bob.id + `","inReplyTo":"https://other.example/u/eve/notes/8","content":"<p>A reply</p>"}}]}`), nil
		case address == "https://remote.example/u/bob/notes/3":
			return []byte(`{"id":"https://remote.example/u/bob/notes/3","type":"Note","attributedTo":"` + bob.id + `","content":"<p>Three</p>"}`), nil
		case address == "https://remote.example/u/bob/notes/4":
			return []byte(`{"id":"https://other.example/u/eve/notes/4","type":"Note","content":"<p>Served under another host's id</p>"}`), nil
		}
		return nil, &resolve_refusal{Status: 404}
	}
	t.Cleanup(func() { resolve_fetch = saved })
}

// TestActivityPubInboxSigning: the key of an activity at the shared inbox is
// fetched signed, as its server may insist: as the first account here with a
// user that the activity is for, or as the server's own account when it is
// for nobody here. Every activity is checked alike, so a forged one is refused
// whether or not anyone follows its sender, and a key on another host than
// the sender's is refused before anything is fetched.
func TestActivityPubInboxSigning(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	following_documents(t, bob)
	saved := resolve_fetch
	var fetches []string
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		signer := ""
		if sign := resolve_signed_by(ctx); sign != nil {
			r := httptest.NewRequest("GET", address, nil)
			sign(r)
			signer = signature_parameters(r.Header.Get("Signature"))["keyid"]
		}
		fetches = append(fetches, address+" "+signer)
		if signer == "" {
			return nil, &resolve_refusal{Status: http.StatusUnauthorized}
		}
		return saved(ctx, address, accept)
	}
	t.Cleanup(func() { resolve_fetch = saved })
	fresh := func() { fetches, signature_keys = nil, map[string]signature_key_entry{} }
	create := func(id string, cc ...string) map[string]any {
		return map[string]any{"@context": activitystreams, "id": id, "type": "Create", "actor": bob.id, "to": []string{activitystreams_public}, "cc": cc,
			"object": map[string]any{"id": id + "/note", "type": "Note", "attributedTo": bob.id, "content": "<p>Hello</p>", "to": []string{activitystreams_public}, "cc": cc}}
	}
	queued := func() int64 {
		n, _ := networks_database().row("select count(*) as n from inbound where account='f1' and type='Create'")
		return row_int(n, "n")
	}

	// For nobody here: checked, the key fetched as the server's own account.
	fresh()
	if w := signed_post(t, bob, "/~/inbox", create("https://remote.example/creates/9", bob.id+"/followers"), nil); w.Code != http.StatusAccepted {
		t.Errorf("an activity for nobody here: %d %s", w.Code, w.Body.String())
	}
	if len(fetches) == 0 || fetches[0] != bob.id+" https://example.com/~/actor#main-key" {
		t.Errorf("fetches for an activity for nobody here = %v", fetches)
	}
	if queued() != 0 {
		t.Error("an activity for nobody here was queued")
	}

	// Forged and for nobody here: refused, as one for someone here is.
	fresh()
	forger := *bob
	forger.private = activitypub_server_private()
	if w := signed_post(t, &forger, "/~/inbox", create("https://remote.example/creates/10", bob.id+"/followers"), nil); w.Code != http.StatusUnauthorized {
		t.Errorf("a forged activity for nobody here: %d", w.Code)
	}

	// A key on another host: refused, nothing fetched from it.
	fresh()
	elsewhere := *bob
	elsewhere.key_id = "https://other.example/keys/1"
	if w := signed_post(t, &elsewhere, "/~/inbox", create("https://remote.example/creates/11", "https://example.com/~f1"), nil); w.Code != http.StatusUnauthorized {
		t.Errorf("a key on another host: %d", w.Code)
	}
	for _, fetch := range fetches {
		if strings.Contains(fetch, "other.example") {
			t.Errorf("a key on another host was fetched: %s", fetch)
		}
	}

	// For three accounts here, the first with no user and the second with no
	// key: the third signs.
	actor_register(&User{UID: "u9"}, "f2", "fediverse", "example.com", "ghost")
	db_user(alice, "user").exec(`insert into accounts (id, type, label, identifier, data, created, verified) values ('f3', 'fediverse', '', '@carl@example.com', '{"hosted":true,"handle":"carl","domain":"example.com"}', 1, 1)`)
	actor_register(alice, "f3", "fediverse", "example.com", "carl")
	fresh()
	if w := signed_post(t, bob, "/~/inbox", create("https://remote.example/creates/12", "https://example.com/~f2", "https://example.com/~f3", "https://example.com/~f1"), nil); w.Code != http.StatusAccepted {
		t.Errorf("an activity for an account here: %d %s", w.Code, w.Body.String())
	}
	if len(fetches) == 0 || !strings.HasSuffix(fetches[0], "/~f1#main-key") {
		t.Errorf("fetches for an activity for an account here = %v", fetches)
	}
	if queued() != 1 {
		t.Errorf("queued %d Creates for the account addressed, want 1", queued())
	}
}

// TestActivityPubDeletes: a Delete names only the public. A reply from
// someone the account does not follow is kept as a copy, on its author's host
// and of a sensible length, through which the reply's Delete reaches the
// account; no one else's Delete of it does, nor removes the copy, and two
// senders' copies of one object stand apart. A followed person's post needs no
// copy. A follower deleting themselves is forgotten: through their Delete
// while their key can be read, and once their server, asked as the server's
// own account, answers that they are gone (410) when it cannot, though what
// they sent is not handed to the apps unchecked. A 404 is not gone, and a
// self-Delete that cannot be checked is taken and dropped, forged or not.
func TestActivityPubDeletes(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	db := db_user(alice, "user")
	queue := networks_database()
	carol := &remote_person{id: "https://remote.example/u/carol", key_id: "https://remote.example/u/carol#main-key", inbox: "https://remote.example/u/carol/inbox", public: bob.public, private: bob.private}
	saved := resolve_fetch
	answer := 0
	asked := ""
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		switch {
		case address == bob.id && answer != 0:
			if sign := resolve_signed_by(ctx); sign != nil {
				r := httptest.NewRequest("GET", address, nil)
				sign(r)
				asked = signature_parameters(r.Header.Get("Signature"))["keyid"]
			}
			return nil, &resolve_refusal{Status: answer}
		case address == carol.id:
			return []byte(`{"id":"` + carol.id + `","type":"Person","preferredUsername":"carol","inbox":"` + carol.inbox + `","publicKey":{"id":"` + carol.key_id + `","owner":"` + carol.id + `","publicKeyPem":` + json_encode(carol.public) + `}}`), nil
		}
		return saved(ctx, address, accept)
	}
	t.Cleanup(func() { resolve_fetch = saved })
	queued := func(kind string) int64 {
		n, _ := queue.row("select count(*) as n from inbound where account='f1' and type=?", kind)
		return row_int(n, "n")
	}
	copies := func(object string) int64 {
		n, _ := queue.row("select count(*) as n from copies where account='f1' and object=?", object)
		return row_int(n, "n")
	}
	create := func(by *remote_person, id string, object string) map[string]any {
		return map[string]any{"@context": activitystreams, "id": id, "type": "Create", "actor": by.id, "to": []string{activitystreams_public}, "cc": []string{"https://example.com/~f1"},
			"object": map[string]any{"id": object, "type": "Note", "attributedTo": by.id, "inReplyTo": "https://example.com/~f1/objects/1", "content": "<p>A reply</p>", "to": []string{activitystreams_public}, "cc": []string{"https://example.com/~f1"}}}
	}
	remove := func(by *remote_person, id string, object any, cc ...string) map[string]any {
		activity := map[string]any{"@context": activitystreams, "id": id, "type": "Delete", "actor": by.id, "to": []string{activitystreams_public}, "object": object}
		if len(cc) > 0 {
			activity["cc"] = cc
		}
		return activity
	}
	post := func(by *remote_person, activity map[string]any) {
		t.Helper()
		if w := signed_post(t, by, "/~/inbox", activity, nil); w.Code != http.StatusAccepted {
			t.Fatalf("%s: %d %s", activity["id"], w.Code, w.Body.String())
		}
	}

	note := "https://remote.example/u/bob/notes/7"
	post(bob, create(bob, note+"/activity", note))
	if copies(note) != 1 {
		t.Fatal("no copy kept of a reply from someone not followed")
	}
	post(bob, create(bob, note+"/elsewhere/activity", "https://other.example/notes/7"))
	post(bob, create(bob, note+"/long/activity", note+"/"+strings.Repeat("x", copy_length)))
	if n, _ := queue.row("select count(*) as n from copies where account='f1'"); row_int(n, "n") != 1 {
		t.Error("a copy was kept of an object on another host, or with an id past any server's")
	}
	tombstone := map[string]any{"id": note, "type": "Tombstone"}
	post(carol, remove(carol, carol.id+"/deletes/1", tombstone))
	if queued("Delete") != 0 {
		t.Error("someone else's Delete of the reply reached the account through its copy")
	}
	post(carol, remove(carol, carol.id+"/deletes/2", tombstone, "https://example.com/~f1"))
	if copies(note) != 1 {
		t.Error("someone else's Delete of the reply removed its copy")
	}
	post(carol, create(carol, carol.id+"/creates/1", note))
	if copies(note) != 2 {
		t.Errorf("%d copies of one object from two senders, want 2", copies(note))
	}
	deletes := queued("Delete")
	post(bob, remove(bob, note+"#delete", tombstone))
	if queued("Delete") != deletes+1 {
		t.Error("the Delete of a reply from someone not followed did not reach the account")
	}
	if found, _ := queue.exists("select 1 from copies where object=? and actor=?", note, bob.id); found {
		t.Error("the copy outlived its Delete")
	}

	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values ('f1', ?, ?, 'https://example.com/~f1/follows/y', 1, 1)", bob.id, bob.inbox)
	post(bob, create(bob, note+"/8/activity", note+"/8"))
	if copies(note+"/8") != 0 {
		t.Error("a copy was kept of a followed person's post")
	}
	db.exec("delete from following where account='f1'")

	follower := func() bool {
		found, _ := db.exists("select 1 from followers where account='f1' and actor=?", bob.id)
		return found
	}
	follow := func() {
		db.exec("insert or replace into followers (account, actor, inbox, shared, created) values ('f1', ?, ?, '', 1)", bob.id, bob.inbox)
	}
	follow()
	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values ('f1', ?, ?, 'https://example.com/~f1/follows/z', 1, 1)", bob.id, bob.inbox)
	queue.exec("insert into copies (object, account, actor, created) values (?, 'f1', ?, 1)", note+"/9", bob.id)
	deletes = queued("Delete")
	post(bob, remove(bob, bob.id+"#delete-1", bob.id))
	if follower() {
		t.Error("a follower who deleted themselves is still a follower")
	}
	if found, _ := db.exists("select 1 from following where account='f1' and actor=?", bob.id); found || copies(note+"/9") != 0 {
		t.Error("someone deleted is still followed, or their copies kept")
	}
	if queued("Delete") != deletes+1 {
		t.Error("a follower's checked Delete of themselves was not handed to the apps")
	}

	// Their key gone with them: a 404 is not proof, a 410 is.
	forger := *bob
	forger.private = activitypub_server_private()
	for _, status := range []int{http.StatusNotFound, http.StatusGone} {
		follow()
		signature_keys = map[string]signature_key_entry{}
		answer, asked = status, ""
		deletes = queued("Delete")
		post(&forger, remove(bob, fmt.Sprintf("%s#delete-%d", bob.id, status), bob.id))
		if follower() != (status == http.StatusNotFound) {
			t.Errorf("after a self-Delete that cannot be checked and a %d, follower = %v", status, follower())
		}
		if asked != "https://example.com/~/actor#main-key" {
			t.Errorf("asked whether they are gone as %q, want the server's own account", asked)
		}
		if queued("Delete") != deletes {
			t.Error("a Delete that could not be checked was handed to the apps")
		}
	}

	// Alive, a forged Delete of them is dropped and they stay.
	follow()
	signature_keys = map[string]signature_key_entry{}
	answer = 0
	post(&forger, remove(bob, bob.id+"#delete-forged", bob.id))
	if !follower() {
		t.Error("a forged Delete removed a follower")
	}
}

// TestActivityPubFollowing: a post by someone the account follows reaches the
// shared inbox addressed to their followers and is queued for the account;
// one by a stranger is not. A Move by the followed person sends the follow on
// to where they went, once their new identity names the old one, and the apps
// hear of it; a Move to an identity that does not name them is ignored.
func TestActivityPubFollowing(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	following_documents(t, bob)
	db := db_user(alice, "user")
	queue := networks_database()
	queued := func(kind string) int64 {
		n, _ := queue.row("select count(*) as n from inbound where account='f1' and type=?", kind)
		return row_int(n, "n")
	}
	post := func(id string) map[string]any {
		return map[string]any{"@context": activitystreams, "id": id, "type": "Create", "actor": bob.id,
			"to": []string{activitystreams_public}, "cc": []string{bob.id + "/followers"},
			"object": map[string]any{"id": id + "/note", "type": "Note", "attributedTo": bob.id, "content": "<p>Hello followers</p>", "to": []string{activitystreams_public}, "cc": []string{bob.id + "/followers"}}}
	}

	// Not following: a post to the followers collection names no account here.
	if w := signed_post(t, bob, "/~/inbox", post("https://remote.example/creates/1"), nil); w.Code != http.StatusAccepted {
		t.Fatalf("shared inbox: %d %s", w.Code, w.Body.String())
	}
	if queued("Create") != 0 {
		t.Fatal("a stranger's post to their followers was queued")
	}

	// Following: the same post is for the account.
	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values ('f1', ?, ?, 'https://example.com/~f1/follows/x', 1, 1)", bob.id, bob.inbox)
	if w := signed_post(t, bob, "/~/inbox", post("https://remote.example/creates/2"), nil); w.Code != http.StatusAccepted {
		t.Fatalf("shared inbox: %d %s", w.Code, w.Body.String())
	}
	if queued("Create") != 1 {
		t.Fatalf("queued %d Creates for the following account, want 1", queued("Create"))
	}
	row, _ := queue.row("select actor from inbound where account='f1' and type='Create'")
	if row == nil || row["actor"] != bob.id {
		t.Errorf("queued row = %v", row)
	}

	// A Move to someone who does not name bob among their identities is
	// ignored: the follow stays where it was.
	before := len(delivery_rows(t))
	move := map[string]any{"@context": activitystreams, "id": "https://remote.example/moves/1", "type": "Move", "actor": bob.id, "object": bob.id, "target": "https://remote.example/u/dave"}
	if w := signed_post(t, bob, "/~/inbox", move, nil); w.Code != http.StatusAccepted {
		t.Fatalf("move: %d", w.Code)
	}
	if found, _ := db.exists("select 1 from following where account='f1' and actor=?", bob.id); !found || len(delivery_rows(t)) != before || queued("Move") != 0 {
		t.Error("a Move to an identity that does not name the person was acted on")
	}

	// A Move to carol, who names bob: the follow goes to carol, pending, and
	// the apps hear of the Move.
	move = map[string]any{"@context": activitystreams, "id": "https://remote.example/moves/2", "type": "Move", "actor": bob.id, "object": bob.id, "target": "https://remote.example/u/carol"}
	if w := signed_post(t, bob, "/~/inbox", move, nil); w.Code != http.StatusAccepted {
		t.Fatalf("move: %d", w.Code)
	}
	if found, _ := db.exists("select 1 from following where account='f1' and actor=?", bob.id); found {
		t.Error("the follow of the old identity stayed")
	}
	row, _ = db.row("select inbox, accepted from following where account='f1' and actor='https://remote.example/u/carol'")
	if row == nil || row["inbox"] != "https://remote.example/u/carol/inbox" || row_int(row, "accepted") != 0 {
		t.Fatalf("following row after the move = %v", row)
	}
	rows := delivery_rows(t)
	if len(rows) != before+1 || rows[len(rows)-1]["inbox"] != "https://remote.example/u/carol/inbox" {
		t.Fatalf("deliveries after the move = %v", rows)
	}
	_, follow := delivery_body(t)
	if follow["type"] != "Follow" || follow["object"] != "https://remote.example/u/carol" || follow["actor"] != "https://example.com/~f1" {
		t.Errorf("Follow after the move = %v", follow)
	}
	if queued("Move") != 1 {
		t.Errorf("queued %d Moves for the apps, want 1", queued("Move"))
	}
}

// TestActivityPubFeatured: the featured collection holds the objects the apps
// name as pinned, as documents, and nothing an app does not name.
func TestActivityPubFeatured(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	db := db_user(alice, "user")
	for i, id := range []string{"p1", "p2"} {
		db.exec("insert into objects (id, account, app, type, addressing, created) values (?, 'f1', 'feeds', 'Note', ?, ?)", id, json_encode(map[string]any{"to": []string{activitystreams_public}}), 1000+i)
	}
	saved_call := activitypub_call
	activitypub_call = func(user *User, a *App, function string, args Map) (any, error) {
		switch function {
		case "activitypub/featured":
			return []any{"https://example.com/~f1/p2", "https://example.com/~f1/missing"}, nil
		case "activitypub/object":
			return map[string]any{"type": "Note", "content": "<p>" + args["id"].(string) + "</p>"}, nil
		}
		t.Errorf("call %s", function)
		return nil, nil
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

	w, featured := activitypub_get(t, "/~f1/featured")
	items, _ := featured["orderedItems"].([]any)
	if w.Code != http.StatusOK || featured["type"] != "OrderedCollection" || featured["totalItems"] != float64(1) || len(items) != 1 {
		t.Fatalf("featured = %d %v", w.Code, featured)
	}
	if object, _ := items[0].(map[string]any); object["id"] != "https://example.com/~f1/p2" || object["attributedTo"] != "https://example.com/~f1" || object["content"] != "<p>https://example.com/~f1/p2</p>" {
		t.Errorf("featured item = %v", items[0])
	}
}

// TestAccountFollowing: the follow API answers whether the person is locked
// and the follow accepted, the following list shows the follow pending until
// the Accept arrives, and the list is behind the account permission.
func TestAccountFollowing(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	following_documents(t, bob)
	fn := sl.NewBuiltin("test", nil)
	thread := create_acting_thread(alice)

	value, err := api_account_follow(thread, fn, sl.Tuple{sl.String("f1"), sl.String("@carol@remote.example")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	answer := sl_decode(value).(map[string]any)
	if answer["id"] != "https://remote.example/u/carol" || answer["locked"] != true || answer["accepted"] != false {
		t.Errorf("follow answered %v", answer)
	}
	plain := create_test_thread(alice, create_external_app("app6"))
	if _, err := api_account_following(plain, fn, sl.Tuple{sl.String("f1")}, nil); err == nil {
		t.Error("the following list was read without the account permission")
	}
	value, err = api_account_following(thread, fn, sl.Tuple{sl.String("f1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	list := sl_decode(value).([]any)
	if len(list) != 1 || list[0].(map[string]any)["actor"] != "https://remote.example/u/carol" || list[0].(map[string]any)["accepted"] != int64(0) {
		t.Fatalf("following = %v", list)
	}

	// Carol's Accept settles it. Her server signs as her; bob's key stands in
	// for hers, with her id on the key, which the engine checks against the
	// owner the document names.
	db := db_user(alice, "user")
	db.exec("update following set accepted=? where account='f1' and actor='https://remote.example/u/carol'", now())
	value, _ = api_account_following(thread, fn, sl.Tuple{sl.String("f1")}, []sl.Tuple{{sl.String("actor"), sl.String("https://remote.example/u/carol")}})
	if list := sl_decode(value).([]any); len(list) != 1 || list[0].(map[string]any)["accepted"].(int64) == 0 {
		t.Errorf("following after the Accept = %v", list)
	}
	value, _ = api_account_following(thread, fn, sl.Tuple{sl.String("f1")}, []sl.Tuple{{sl.String("actor"), sl.String(bob.id)}})
	if list := sl_decode(value).([]any); len(list) != 0 {
		t.Errorf("someone not followed listed: %v", list)
	}
}

// TestAccountMove: aliases name the person's other identities in the actor
// document; moving the account to one that names it records where it went,
// says so in the document and sends a Move to the followers' servers; moving
// to one that does not name it is refused.
func TestAccountMove(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	following_documents(t, bob)
	fn := sl.NewBuiltin("test", nil)
	// Settings holds accounts/write and no account's publishing permission.
	thread := create_test_thread(alice, create_external_app("app2"))
	thread.SetLocal("context", context.Background())
	permission_grant(alice, "app2", "accounts/write")
	db := db_user(alice, "user")

	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String("f1")}, []sl.Tuple{{sl.String("aliases"), sl.NewList([]sl.Value{sl.String("@carol@remote.example")})}}); err != nil {
		t.Fatal(err)
	}
	_, document := activitypub_get(t, "/~f1")
	if aliases, _ := document["alsoKnownAs"].([]any); len(aliases) != 1 || aliases[0] != "https://remote.example/u/carol" {
		t.Errorf("alsoKnownAs = %v", document["alsoKnownAs"])
	}
	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String("f1")}, []sl.Tuple{{sl.String("aliases"), sl.NewList([]sl.Value{sl.String("@nobody@nowhere.invalid")})}}); err == nil {
		t.Error("an alias that does not resolve was accepted")
	}

	// A follower to tell.
	signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/mv"), nil)
	before := len(delivery_rows(t))

	// An app publishing through the account cannot move it.
	_, err := api_account_move(create_acting_thread(alice), fn, sl.Tuple{sl.String("f1"), sl.String("@carol@remote.example")}, nil)
	var permission *PermissionError
	if !errors.As(err, &permission) || permission.Permission != "accounts/write" {
		t.Errorf("a move with only the account's publishing permission: %v", err)
	}
	// Someone else viewing alice's page, as an OpenGraph render or a context
	// route has it, with their own grant: the account is looked for among
	// theirs, not hers.
	mallory := &User{UID: "u9", Username: "mallory", Role: "user", Preferences: map[string]string{}}
	viewer := create_test_thread(mallory, create_external_app("app3"))
	viewer.SetLocal("storage", alice)
	viewer.SetLocal("context", context.Background())
	permission_grant(mallory, "app3", "accounts/write")
	if _, err := api_account_move(viewer, fn, sl.Tuple{sl.String("f1"), sl.String("@carol@remote.example")}, nil); err == nil {
		t.Error("someone viewing alice's page moved her account")
	}
	if moved, _ := account_data(account_hosted_get(alice, "f1"))["moved"].(string); moved != "" {
		t.Errorf("alice's account moved to %q by another user", moved)
	}

	if _, err := api_account_move(thread, fn, sl.Tuple{sl.String("f1"), sl.String("https://remote.example/u/dave")}, nil); !refused(err, "errors.move_not_named", "") {
		t.Errorf("a move to an account that does not name this one was allowed: %v", err)
	}
	if _, err := api_account_move(thread, fn, sl.Tuple{sl.String("f1"), sl.String("@nobody@nowhere.invalid")}, nil); !refused(err, "errors.move_unresolved", "") {
		t.Errorf("a move to an account that cannot be found: %v", err)
	}
	// The account's own document, as its server answers it.
	fetch := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if address == "https://example.com/~f1" {
			return []byte(`{"id":"https://example.com/~f1","type":"Person","preferredUsername":"alice","inbox":"https://example.com/~f1/inbox","alsoKnownAs":["https://example.com/~f1"]}`), nil
		}
		return fetch(ctx, address, accept)
	}
	if _, err := api_account_move(thread, fn, sl.Tuple{sl.String("f1"), sl.String("https://example.com/~f1")}, nil); !refused(err, "errors.move_to_itself", "") {
		t.Errorf("a move to itself: %v", err)
	}
	resolve_fetch = fetch
	if _, err := api_account_move(thread, fn, sl.Tuple{sl.String("f1"), sl.String("@carol@remote.example")}, nil); err != nil {
		t.Fatal(err)
	}
	row := account_hosted_get(alice, "f1")
	if moved, _ := account_data(row)["moved"].(string); moved != "https://remote.example/u/carol" {
		t.Errorf("moved = %q", moved)
	}
	_, document = activitypub_get(t, "/~f1")
	if document["movedTo"] != "https://remote.example/u/carol" {
		t.Errorf("movedTo = %v", document["movedTo"])
	}
	rows := delivery_rows(t)
	if len(rows) != before+1 || rows[len(rows)-1]["inbox"] != "https://remote.example/inbox" {
		t.Fatalf("deliveries after the move = %v", rows)
	}
	_, move := delivery_body(t)
	if move["type"] != "Move" || move["object"] != "https://example.com/~f1" || move["target"] != "https://remote.example/u/carol" {
		t.Errorf("Move = %v", move)
	}
	if found, _ := db.exists("select 1 from followers where account='f1'"); !found {
		t.Error("the followers were dropped by the move; their servers drop them")
	}
}

// TestObjectRead: an object on another server is read as the account; one
// whose id names another host, or on a blocked host, is not.
func TestObjectRead(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	following_documents(t, bob)
	fn := sl.NewBuiltin("test", nil)
	thread := create_acting_thread(alice)

	value, err := api_account_object_read(thread, fn, sl.Tuple{sl.String("f1"), sl.String(bob.id + "/notes/3")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	object, _ := sl_decode(value).(map[string]any)
	if object == nil || object["content"] != "<p>Three</p>" || object["attributedTo"] != bob.id {
		t.Errorf("object = %v", sl_decode(value))
	}
	if value, _ := api_account_object_read(thread, fn, sl.Tuple{sl.String("f1"), sl.String(bob.id + "/notes/4")}, nil); value != sl.None {
		t.Errorf("an object served under another host's id was read: %v", value)
	}
	if value, _ := api_account_object_read(thread, fn, sl.Tuple{sl.String("f1"), sl.String(bob.id + "/notes/9")}, nil); value != sl.None {
		t.Errorf("an object the server does not have was read: %v", value)
	}
	if _, err := api_account_object_read(thread, fn, sl.Tuple{sl.String("f1"), sl.String("not an id")}, nil); err == nil {
		t.Error("a bare string was read as an id")
	}
	block_add(db_user(alice, "user"), "remote.example")
	if value, _ := api_account_object_read(thread, fn, sl.Tuple{sl.String("f1"), sl.String(bob.id + "/notes/3")}, nil); value != sl.None {
		t.Error("an object on a blocked host was read")
	}
}

// TestFediversePosts: a fediverse person's outbox is read as their posts: the
// Creates of posts, not of replies, each Announce as the boosted object's id,
// and the next page as the cursor; a stranger's handle reads the same way,
// and a server that does not answer is None.
func TestFediversePosts(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	following_documents(t, bob)
	fn := sl.NewBuiltin("test", nil)
	thread := create_acting_thread(alice)

	value, err := api_account_posts(thread, fn, sl.Tuple{sl.String(bob.id)}, []sl.Tuple{{sl.String("signer"), sl.String("f1")}})
	if err != nil {
		t.Fatal(err)
	}
	page, _ := sl_decode(value).(map[string]any)
	items, _ := page["items"].([]any)
	if len(items) != 2 || page["cursor"] != bob.id+"/outbox?page=true&max_id=1" {
		t.Fatalf("page = %v", page)
	}
	if note, _ := items[0].(map[string]any); note["id"] != bob.id+"/notes/3" || note["content"] != "<p>Three</p>" {
		t.Errorf("first item = %v", items[0])
	}
	if boost, _ := items[1].(map[string]any); boost["type"] != "Announce" || boost["object"] != "https://other.example/u/eve/notes/9" || boost["actor"] != bob.id {
		t.Errorf("second item = %v", items[1])
	}
	if value, _ := api_account_posts(thread, fn, sl.Tuple{sl.String("https://remote.example/u/nobody")}, nil); value != sl.None {
		t.Errorf("an outbox the server does not have was read: %v", value)
	}
}

// TestObjectAnnounce: a boost goes out as an Announce of the object to the
// public, the followers and the author, is listed in the outbox as the
// account's own activity, and is taken back with an Undo through
// object.delete.
func TestObjectAnnounce(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	following_documents(t, bob)
	fn := sl.NewBuiltin("test", nil)
	thread := create_acting_thread(alice)
	signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/an"), nil)
	before := len(delivery_rows(t))

	value, err := api_account_announce(thread, fn, sl.Tuple{sl.String("f1"), sl.String(bob.id + "/notes/3")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	answer := sl_decode(value).(map[string]any)
	announce, _ := answer["id"].(string)
	if !strings.HasPrefix(announce, "https://example.com/~f1/") || answer["url"] != bob.id+"/notes/3" {
		t.Fatalf("announce answered %v", answer)
	}
	rows := delivery_rows(t)
	if len(rows) <= before {
		t.Fatal("nothing delivered for the Announce")
	}
	_, activity := delivery_body(t)
	cc, _ := activity["cc"].([]any)
	if activity["type"] != "Announce" || activity["id"] != announce || activity["object"] != bob.id+"/notes/3" || len(cc) != 2 || cc[1] != bob.id {
		t.Errorf("Announce = %v", activity)
	}
	_, page := activitypub_get(t, "/~f1/outbox?page=true")
	items, _ := page["orderedItems"].([]any)
	if len(items) != 1 {
		t.Fatalf("outbox page = %v", page)
	}
	if item, _ := items[0].(map[string]any); item["type"] != "Announce" || item["id"] != announce || item["object"] != bob.id+"/notes/3" {
		t.Errorf("outbox item = %v", items[0])
	}
	// Fetched by its id, the boost is the Announce, never asked of the app
	// that made it, which would answer the boosted post as the account's own.
	fetched, document := activitypub_get(t, strings.TrimPrefix(announce, "https://example.com"))
	if fetched.Code != 200 || document["type"] != "Announce" || document["id"] != announce || document["actor"] != "https://example.com/~f1" || document["object"] != bob.id+"/notes/3" {
		t.Errorf("Announce fetched = %d %v", fetched.Code, document)
	}

	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String("f1"), sl.String(announce)}, nil); err != nil {
		t.Fatal(err)
	}
	_, undo := delivery_body(t)
	undone, _ := undo["object"].(map[string]any)
	if undo["type"] != "Undo" || undone["type"] != "Announce" || undone["id"] != announce || undone["object"] != bob.id+"/notes/3" {
		t.Errorf("Undo = %v", undo)
	}
	_, outbox := activitypub_get(t, "/~f1/outbox")
	if outbox["totalItems"] != float64(0) {
		t.Errorf("outbox after the undo = %v", outbox)
	}
}

// TestResolveSignedByHostedAccount: a user with a hosted fediverse account
// looks people up signed as it, so a server that answers signed requests only
// gives the person's document; a user with none looks up unsigned and gets
// nothing from such a server.
func TestResolveSignedByHostedAccount(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	saved := resolve_fetch
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
		return saved(ctx, address, accept)
	}
	t.Cleanup(func() { resolve_fetch = saved })
	fn := sl.NewBuiltin("test", nil)

	// Root has no hosted account: the lookup is unsigned and refused.
	root := user_by_uid("u-root")
	plain := create_test_thread(root, create_external_app("app7"))
	plain.SetLocal("context", context.Background())
	if value, _ := api_account_resolve(plain, fn, sl.Tuple{sl.String(bob.id)}, nil); value != sl.None {
		t.Errorf("an unsigned lookup of a secure server answered %v", value)
	}

	thread := create_acting_thread(alice)
	value, err := api_account_resolve(thread, fn, sl.Tuple{sl.String(bob.id)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	found, _ := sl_decode(value).(map[string]any)
	if found == nil || found["name"] != "bob" || found["id"] != bob.id {
		t.Errorf("signed lookup = %v", sl_decode(value))
	}
}
