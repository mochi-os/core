// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	sl "go.starlark.net/starlark"
)

// create_acting_thread is a thread of an external app granted account:f1 by
// alice, with a request context.
func create_acting_thread(alice *User) *sl.Thread {
	app := create_external_app("app1")
	permission_grant(alice, app.id, "account:f1")
	thread := create_test_thread(alice, app)
	thread.SetLocal("context", context.Background())
	return thread
}

// object_dict makes a Starlark dict of strings.
func object_dict(pairs ...string) *sl.Dict {
	d := sl.NewDict(len(pairs) / 2)
	for i := 0; i+1 < len(pairs); i += 2 {
		d.SetKey(sl.String(pairs[i]), sl.String(pairs[i+1]))
	}
	return d
}

// delivery_body decodes the body of the newest queued delivery.
func delivery_body(t *testing.T) (string, map[string]any) {
	t.Helper()
	row, _ := db_open("db/networks.db").row("select inbox, body from deliveries order by created desc, rowid desc limit 1")
	if row == nil {
		t.Fatal("nothing queued")
	}
	var activity map[string]any
	if err := json.Unmarshal([]byte(row["body"].(string)), &activity); err != nil {
		t.Fatal(err)
	}
	return row["inbox"].(string), activity
}

// TestObjectCreateUpdateDelete: an object published as a hosted account gets its
// id under the account, attributedTo, published and the public addressing;
// Create goes to the followers' inboxes; Update keeps the id and published and
// adds updated; Delete sends a tombstone and keeps the id.
func TestObjectCreateUpdateDelete(t *testing.T) {
	alice := create_delivery_test_env(t)
	box := create_inbox(t, alice)
	thread := create_acting_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	db := db_user(alice, "user")
	db.exec("insert into followers (account, actor, inbox, shared, created) values ('f1', 'https://remote.example/u/bob', ?, '', 1)", box.server.URL+"/u/bob/inbox")
	db.exec("insert into followers (account, actor, inbox, shared, created) values ('f1', 'https://remote.example/u/carol', ?, ?, 1)", box.server.URL+"/u/carol/inbox", box.server.URL+"/inbox")

	value, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("f1"), object_dict("type", "Note", "content", "<p>Hello</p>")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	remote := string(value.(sl.String))
	if !strings.HasPrefix(remote, "https://example.com/~f1/") {
		t.Fatalf("object id %q", remote)
	}
	row, _ := db.row("select app, type, addressing, deleted from objects where account='f1'")
	if row == nil || row["app"] != "app1" || row["type"] != "Note" {
		t.Errorf("objects row = %v", row)
	}
	rows := delivery_rows(t)
	if len(rows) != 2 {
		t.Fatalf("%d deliveries queued, want one per follower inbox: %v", len(rows), rows)
	}
	_, activity := delivery_body(t)
	object, _ := activity["object"].(map[string]any)
	if activity["type"] != "Create" || activity["actor"] != "https://example.com/~f1" || activity["id"] != remote+"/activity" {
		t.Errorf("Create activity = %v", activity)
	}
	if object["id"] != remote || object["attributedTo"] != "https://example.com/~f1" || object["published"] == nil || object["content"] != "<p>Hello</p>" {
		t.Errorf("Create object = %v", object)
	}
	to, _ := object["to"].([]any)
	cc, _ := object["cc"].([]any)
	if len(to) != 1 || to[0] != activitystreams_public || len(cc) != 1 || cc[0] != "https://example.com/~f1/followers" {
		t.Errorf("addressing to=%v cc=%v", to, cc)
	}
	delivery_process()
	if box.count() != 2 {
		t.Errorf("%d requests delivered, want 2", box.count())
	}
	for _, err := range box.errors {
		if err != nil {
			t.Errorf("delivery signature: %v", err)
		}
	}

	value, err = api_account_object_update(thread, fn, sl.Tuple{sl.String("f1"), sl.String(remote), object_dict("type", "Note", "content", "<p>Edited</p>")}, nil)
	if err != nil || string(value.(sl.String)) != remote {
		t.Fatalf("update = %v, %v", value, err)
	}
	_, activity = delivery_body(t)
	object, _ = activity["object"].(map[string]any)
	if activity["type"] != "Update" || object["id"] != remote || object["content"] != "<p>Edited</p>" || object["updated"] == nil || object["published"] == nil {
		t.Errorf("Update = %v", activity)
	}
	if cc, _ := object["cc"].([]any); len(cc) != 1 || cc[0] != "https://example.com/~f1/followers" {
		t.Errorf("Update kept no addressing: %v", object)
	}
	row, _ = db.row("select updated from objects where account='f1'")
	if row["updated"].(int64) == 0 {
		t.Error("objects row not marked updated")
	}

	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String("f1"), sl.String(remote)}, nil); err != nil {
		t.Fatal(err)
	}
	_, activity = delivery_body(t)
	object, _ = activity["object"].(map[string]any)
	if activity["type"] != "Delete" || object["id"] != remote || object["type"] != "Tombstone" {
		t.Errorf("Delete = %v", activity)
	}
	row, _ = db.row("select deleted from objects where account='f1'")
	if row == nil || row["deleted"].(int64) == 0 {
		t.Error("objects row not kept as deleted")
	}
	queued := len(delivery_rows(t))
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String("f1"), sl.String(remote)}, nil); err != nil {
		t.Errorf("second delete = %v", err)
	}
	if len(delivery_rows(t)) != queued {
		t.Error("a second delete queued more")
	}
	if _, err := api_account_object_update(thread, fn, sl.Tuple{sl.String("f1"), sl.String(remote), object_dict("type", "Note")}, nil); err == nil {
		t.Error("update of a deleted object allowed")
	}
	if _, err := api_account_object_update(thread, fn, sl.Tuple{sl.String("f1"), sl.String("https://example.com/~f1/nothing"), object_dict("type", "Note")}, nil); err == nil {
		t.Error("update of an unknown object allowed")
	}
}

// TestObjectAddressing: addressing the object carries is kept, a mentioned
// person's inbox is found through their actor document, and the public gets
// no delivery.
func TestObjectAddressing(t *testing.T) {
	alice := create_delivery_test_env(t)
	thread := create_acting_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		return []byte(`{"id":"https://remote.example/u/bob","type":"Person","preferredUsername":"bob","inbox":"https://remote.example/u/bob/inbox","endpoints":{"sharedInbox":"https://remote.example/inbox"}}`), nil
	}
	t.Cleanup(func() { resolve_fetch = saved })

	object := object_dict("type", "Note", "content", "<p>@bob</p>", "to", "https://remote.example/u/bob")
	object.SetKey(sl.String("cc"), sl.NewList([]sl.Value{sl.String(activitystreams_public)}))
	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("f1"), object}, nil); err != nil {
		t.Fatal(err)
	}
	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["inbox"] != "https://remote.example/u/bob/inbox" {
		t.Errorf("deliveries = %v, want bob's inbox alone", rows)
	}
	_, activity := delivery_body(t)
	if to, _ := activity["to"].([]any); len(to) != 1 || to[0] != "https://remote.example/u/bob" {
		t.Errorf("to = %v", activity["to"])
	}
}

// TestObjectRefusals: publishing needs the account permission, the account must
// be the user's own and on the fediverse, and the object a type.
func TestObjectRefusals(t *testing.T) {
	alice := create_delivery_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	app := create_external_app("app2")
	thread := create_test_thread(alice, app)
	thread.SetLocal("context", context.Background())

	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("f1"), object_dict("type", "Note")}, nil); err == nil || !strings.Contains(err.Error(), "permission") {
		t.Errorf("ungranted app = %v, want a permission error", err)
	}
	permission_grant(alice, app.id, "account:*")
	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("b1"), object_dict("type", "Note")}, nil); err == nil || !strings.Contains(err.Error(), "cannot publish yet") {
		t.Errorf("bluesky account = %v", err)
	}
	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("nope"), object_dict("type", "Note")}, nil); err == nil || !strings.Contains(err.Error(), "no such account") {
		t.Errorf("unknown account = %v", err)
	}
	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("f1"), object_dict("content", "x")}, nil); err == nil || !strings.Contains(err.Error(), "type") {
		t.Errorf("object without a type = %v", err)
	}
	setting_set("fediverse", "false")
	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String("f1"), object_dict("type", "Note")}, nil); err == nil || !strings.Contains(err.Error(), "off") {
		t.Errorf("network off = %v", err)
	}
	setting_set("fediverse", "true")
	if n := len(delivery_rows(t)); n != 0 {
		t.Errorf("%d deliveries queued by refused calls", n)
	}
}

// TestFollowUnfollow: following resolves the handle to the person's id and
// inbox, records the follow as pending and sends Follow; unfollowing sends Undo
// of that Follow and forgets it.
func TestFollowUnfollow(t *testing.T) {
	alice := create_delivery_test_env(t)
	thread := create_acting_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if strings.Contains(address, "webfinger") {
			return []byte(`{"subject":"acct:bob@remote.example","links":[{"rel":"self","type":"application/activity+json","href":"https://remote.example/u/bob"}]}`), nil
		}
		return []byte(`{"id":"https://remote.example/u/bob","type":"Person","preferredUsername":"bob","inbox":"https://remote.example/u/bob/inbox"}`), nil
	}
	t.Cleanup(func() { resolve_fetch = saved })
	db := db_user(alice, "user")

	if _, err := api_account_follow(thread, fn, sl.Tuple{sl.String("f1"), sl.String("@bob@remote.example")}, nil); err != nil {
		t.Fatal(err)
	}
	row, _ := db.row("select actor, inbox, activity, accepted from following where account='f1'")
	if row == nil || row["actor"] != "https://remote.example/u/bob" || row["inbox"] != "https://remote.example/u/bob/inbox" || row["accepted"].(int64) != 0 {
		t.Fatalf("following row = %v", row)
	}
	activity := row["activity"].(string)
	if !strings.HasPrefix(activity, "https://example.com/~f1/follows/") {
		t.Errorf("follow activity id %q", activity)
	}
	inbox, body := delivery_body(t)
	if inbox != "https://remote.example/u/bob/inbox" || body["type"] != "Follow" || body["id"] != activity || body["actor"] != "https://example.com/~f1" || body["object"] != "https://remote.example/u/bob" {
		t.Errorf("Follow = %v to %s", body, inbox)
	}

	// Following again re-sends the same Follow rather than a second one.
	api_account_follow(thread, fn, sl.Tuple{sl.String("f1"), sl.String("https://remote.example/u/bob")}, nil)
	if _, body := delivery_body(t); body["id"] != activity {
		t.Errorf("a second follow made a new activity %v", body["id"])
	}

	if _, err := api_account_unfollow(thread, fn, sl.Tuple{sl.String("f1"), sl.String("@bob@remote.example")}, nil); err != nil {
		t.Fatal(err)
	}
	if row, _ := db.row("select 1 from following where account='f1'"); row != nil {
		t.Error("following row kept after unfollow")
	}
	_, body = delivery_body(t)
	undone, _ := body["object"].(map[string]any)
	if body["type"] != "Undo" || body["id"] != activity+"/undo" || undone["id"] != activity || undone["type"] != "Follow" || undone["object"] != "https://remote.example/u/bob" {
		t.Errorf("Undo = %v", body)
	}
	if _, err := api_account_unfollow(thread, fn, sl.Tuple{sl.String("f1"), sl.String("@bob@remote.example")}, nil); err == nil {
		t.Error("unfollowing someone not followed allowed")
	}

	// A person on a domain the user blocks cannot be followed.
	block_add(db, "remote.example")
	if _, err := api_account_follow(thread, fn, sl.Tuple{sl.String("f1"), sl.String("@bob@remote.example")}, nil); err == nil {
		t.Error("following on a blocked domain allowed")
	}
}

// TestMedia: a file is fetched as the account with a signed GET and returned
// with its status and type; one over the cap, or on a blocked host, is not.
func TestMedia(t *testing.T) {
	alice := create_delivery_test_env(t)
	thread := create_acting_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	var signed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Signature") != "" {
			signed.Add(1)
		}
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("PNGDATA"))
	}))
	t.Cleanup(server.Close)

	value, err := api_account_media(thread, fn, sl.Tuple{sl.String("f1"), sl.String(server.URL + "/a.png")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := value.(*sl.Dict)
	if !ok {
		t.Fatalf("media = %v, want a dict", value)
	}
	status, _, _ := result.Get(sl.String("status"))
	kind, _, _ := result.Get(sl.String("type"))
	data, _, _ := result.Get(sl.String("data"))
	if status != sl.MakeInt(200) || kind != sl.String("image/png") || data != sl.Bytes("PNGDATA") {
		t.Errorf("media = status %v type %v data %v", status, kind, data)
	}
	if signed.Load() != 1 {
		t.Error("the fetch was not signed")
	}

	saved := media_maximum
	media_maximum = 3
	t.Cleanup(func() { media_maximum = saved })
	if value, _ := api_account_media(thread, fn, sl.Tuple{sl.String("f1"), sl.String(server.URL + "/a.png")}, nil); value != sl.None {
		t.Errorf("media over the cap = %v, want None", value)
	}
	media_maximum = saved

	host, _, _ := strings.Cut(strings.TrimPrefix(server.URL, "http://"), ":")
	block_add(db_user(alice, "user"), host)
	if value, _ := api_account_media(thread, fn, sl.Tuple{sl.String("f1"), sl.String(server.URL + "/a.png")}, nil); value != sl.None {
		t.Errorf("media from a blocked host = %v, want None", value)
	}
	if signed.Load() != 2 {
		t.Errorf("%d fetches reached the server, want 2", signed.Load())
	}
}

// TestBlocks: a user's blocks and the owner's are kept apart, each under its own
// permission, listed and removed, and a block covers the domains under it.
func TestBlocks(t *testing.T) {
	alice := create_delivery_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	app := create_external_app("app3")
	thread := create_test_thread(alice, app)

	if _, err := api_account_block_add(thread, fn, sl.Tuple{sl.String("Spam.example")}, nil); err == nil {
		t.Error("block without accounts/write allowed")
	}
	permission_grant(alice, app.id, "accounts/write")
	permission_grant(alice, app.id, "accounts/read")
	if _, err := api_account_block_add(thread, fn, sl.Tuple{sl.String("Spam.example")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := api_account_block_add(thread, fn, sl.Tuple{sl.String("not a domain")}, nil); err == nil {
		t.Error("an invalid domain was blocked")
	}
	value, err := api_account_block_list(thread, fn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	list := sl_decode(value).([]any)
	if len(list) != 1 || list[0].(map[string]any)["domain"] != "spam.example" {
		t.Errorf("user blocks = %v", list)
	}
	if err := host_allowed(alice, "fediverse", "a.b.spam.example"); err == nil {
		t.Error("a domain under the block is allowed")
	}
	if err := host_allowed(alice, "fediverse", "notspam.example"); err != nil {
		t.Errorf("a sibling domain is refused: %v", err)
	}
	if err := host_allowed(nil, "fediverse", "spam.example"); err != nil {
		t.Errorf("the user's block applied to the server: %v", err)
	}
	if _, err := api_account_block_remove(thread, fn, sl.Tuple{sl.String("spam.example")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := host_allowed(alice, "fediverse", "spam.example"); err != nil {
		t.Errorf("removed block still applies: %v", err)
	}

	// The owner's blocks need settings/write, and apply to everyone.
	if _, err := api_server_block_add(thread, fn, sl.Tuple{sl.String("bad.example")}, nil); err == nil {
		t.Error("server block by an ordinary user allowed")
	}
	admin := create_test_admin(t, "u-root")
	admin_thread := create_test_thread(admin, app)
	permission_grant(admin, app.id, "settings/write")
	permission_grant(admin, app.id, "server/read")
	if _, err := api_server_block_add(admin_thread, fn, sl.Tuple{sl.String("bad.example")}, nil); err != nil {
		t.Fatal(err)
	}
	value, err = api_server_block_list(admin_thread, fn, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if list := sl_decode(value).([]any); len(list) != 1 || list[0].(map[string]any)["domain"] != "bad.example" {
		t.Errorf("server blocks = %v", list)
	}
	if err := host_allowed(alice, "fediverse", "bad.example"); err == nil {
		t.Error("the owner's block did not apply to a user")
	}
	if _, err := api_server_block_remove(admin_thread, fn, sl.Tuple{sl.String("bad.example")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := host_allowed(alice, "fediverse", "bad.example"); err != nil {
		t.Errorf("removed server block still applies: %v", err)
	}
}

// TestResolveHonoursPolicy: mochi.account.resolve answers None for a person on
// a domain the user blocks or a network that is off, without a fetch.
func TestResolveHonoursPolicy(t *testing.T) {
	alice := create_delivery_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	thread := create_test_thread(alice, create_external_app("app4"))
	var fetches atomic.Int32
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		fetches.Add(1)
		return []byte(`{"id":"https://remote.example/u/bob","type":"Person","preferredUsername":"bob","inbox":"https://remote.example/u/bob/inbox"}`), nil
	}
	t.Cleanup(func() { resolve_fetch = saved })

	block_add(db_user(alice, "user"), "remote.example")
	if value, _ := api_account_resolve(thread, fn, sl.Tuple{sl.String("https://remote.example/u/bob")}, nil); value != sl.None {
		t.Errorf("resolve on a blocked domain = %v", value)
	}
	block_remove(db_user(alice, "user"), "remote.example")
	setting_set("fediverse", "false")
	if value, _ := api_account_resolve(thread, fn, sl.Tuple{sl.String("@bob@remote.example")}, nil); value != sl.None {
		t.Errorf("resolve with the network off = %v", value)
	}
	setting_set("fediverse", "true")
	if fetches.Load() != 0 {
		t.Errorf("%d fetches made for refused lookups", fetches.Load())
	}
	if value, _ := api_account_resolve(thread, fn, sl.Tuple{sl.String("https://remote.example/u/bob")}, nil); value == sl.None {
		t.Error("an allowed lookup answered None")
	}
}
