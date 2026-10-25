// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestActivityPubBlockedMuted: a blocked person's signed Follow is taken and
// dropped, with no follower recorded and nothing queued; a muted person's
// Follow is accepted and handed to the apps, while their Like is taken and
// dropped; unmuting lets the next Like through.
func TestActivityPubBlockedMuted(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	app := create_external_app("app4")
	thread := create_test_thread(alice, app)
	permission_grant(alice, app.id, "accounts/write")
	permission_grant(alice, app.id, "accounts/read")
	db := db_user(alice, "user")
	queue := networks_database()
	queued := func() int64 {
		n, _ := queue.row("select count(*) as n from inbound")
		return row_int(n, "n")
	}

	if _, err := api_account_block_add(thread, fn, sl.Tuple{sl.String(bob.id)}, nil); err != nil {
		t.Fatal(err)
	}
	value, _ := api_account_block_list(thread, fn, nil, nil)
	if list := sl_decode(value).([]any); len(list) != 1 || list[0].(map[string]any)["actor"] != bob.id {
		t.Errorf("blocks = %v", list)
	}
	if w := signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/b1"), nil); w.Code != http.StatusAccepted {
		t.Fatalf("blocked follow: %d", w.Code)
	}
	if found, _ := db.exists("select 1 from followers where account='f1'"); found {
		t.Error("a blocked person was recorded as a follower")
	}
	if queued() != 0 || len(delivery_rows(t)) != 0 {
		t.Error("a blocked person's Follow was queued or accepted")
	}
	if _, err := api_account_block_remove(thread, fn, sl.Tuple{sl.String(bob.id)}, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := api_account_mute_add(thread, fn, sl.Tuple{sl.String(bob.id)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := api_account_mute_add(thread, fn, sl.Tuple{sl.String("not an actor")}, nil); err == nil {
		t.Error("a bare name was muted")
	}
	value, _ = api_account_mute_list(thread, fn, nil, nil)
	if list := sl_decode(value).([]any); len(list) != 1 || list[0].(map[string]any)["actor"] != bob.id {
		t.Errorf("mutes = %v", list)
	}
	if w := signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/m1"), nil); w.Code != http.StatusAccepted {
		t.Fatalf("muted follow: %d", w.Code)
	}
	if found, _ := db.exists("select 1 from followers where account='f1' and actor=?", bob.id); !found {
		t.Error("a muted person could not follow")
	}
	if queued() != 1 {
		t.Errorf("queued %d after a muted person's Follow, want the Follow for the apps", queued())
	}
	like := map[string]any{"@context": activitystreams, "id": "https://remote.example/likes/m1", "type": "Like", "actor": bob.id, "object": "https://example.com/~f1/post1"}
	if w := signed_post(t, bob, "/~f1/inbox", like, nil); w.Code != http.StatusAccepted {
		t.Fatalf("muted like: %d", w.Code)
	}
	if queued() != 1 {
		t.Error("a muted person's Like reached the queue")
	}
	if _, err := api_account_mute_remove(thread, fn, sl.Tuple{sl.String(bob.id)}, nil); err != nil {
		t.Fatal(err)
	}
	like["id"] = "https://remote.example/likes/m2"
	signed_post(t, bob, "/~f1/inbox", like, nil)
	if queued() != 2 {
		t.Error("an unmuted person's Like did not reach the queue")
	}

	// Blocking a follower forgets them.
	if _, err := api_account_block_add(thread, fn, sl.Tuple{sl.String(bob.id)}, nil); err != nil {
		t.Fatal(err)
	}
	if found, _ := db.exists("select 1 from followers where account='f1' and actor=?", bob.id); found {
		t.Error("a blocked person stayed a follower")
	}
}

// TestAccountFollowers: the followers of a hosted account are listed, one
// person is found by their id, and the list is behind the account permission.
func TestAccountFollowers(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	signed_post(t, bob, "/~f1/inbox", follow_activity(bob, "https://remote.example/follows/l1"), nil)

	plain := create_test_thread(alice, create_external_app("app5"))
	if _, err := api_account_followers(plain, fn, sl.Tuple{sl.String("f1")}, nil); err == nil {
		t.Error("followers listed without the account permission")
	}
	thread := create_acting_thread(alice)
	value, err := api_account_followers(thread, fn, sl.Tuple{sl.String("f1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	list := sl_decode(value).([]any)
	if len(list) != 1 || list[0].(map[string]any)["actor"] != bob.id || list[0].(map[string]any)["inbox"] != bob.inbox {
		t.Errorf("followers = %v", list)
	}
	value, _ = api_account_followers(thread, fn, sl.Tuple{sl.String("f1")}, []sl.Tuple{{sl.String("actor"), sl.String(bob.id)}})
	if list := sl_decode(value).([]any); len(list) != 1 {
		t.Errorf("follower by id = %v", list)
	}
	value, _ = api_account_followers(thread, fn, sl.Tuple{sl.String("f1")}, []sl.Tuple{{sl.String("actor"), sl.String("https://remote.example/u/carol")}})
	if list := sl_decode(value).([]any); len(list) != 0 {
		t.Errorf("a stranger listed as a follower: %v", list)
	}
}

// TestAccountReport: a report goes out as a Flag to the person's shared
// inbox, naming them and the objects complained of, from the server's own
// account on the reporting account's domain and queued as that account, so
// their server never learns who reported; an object on another server, or a
// person who does not resolve, is refused.
func TestAccountReport(t *testing.T) {
	alice, bob := create_activitypub_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	thread := create_acting_thread(alice)
	// The lookups made in forwarding, by whose key they are signed.
	var keys []string
	saved := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if sign := resolve_signed_by(ctx); sign != nil {
			r := httptest.NewRequest("GET", address, nil)
			sign(r)
			keys = append(keys, r.Header.Get("Signature"))
		}
		return saved(ctx, address, accept)
	}
	t.Cleanup(func() { resolve_fetch = saved })
	objects := sl.NewList([]sl.Value{sl.String("https://remote.example/notes/9")})
	if _, err := api_account_report(thread, fn, sl.Tuple{sl.String("f1"), sl.String(bob.id)}, []sl.Tuple{{sl.String("objects"), objects}, {sl.String("content"), sl.String("Spam")}}); err != nil {
		t.Fatal(err)
	}
	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["inbox"] != "https://remote.example/inbox" {
		t.Fatalf("deliveries = %v, want the Flag at the shared inbox", rows)
	}
	_, flag := delivery_body(t)
	named := object_strings(flag["object"])
	if flag["type"] != "Flag" || flag["actor"] != "https://example.com/~/actor" || flag["content"] != "Spam" || len(named) != 2 || named[0] != bob.id || named[1] != "https://remote.example/notes/9" {
		t.Errorf("THE POINT: Flag = %v", flag)
	}
	if id, _ := flag["id"].(string); !strings.HasPrefix(id, "https://example.com/~/actor#flags/") {
		t.Errorf("THE POINT: Flag id = %q", id)
	}
	if row, _ := db_open("db/networks.db").row("select account from deliveries"); row_string(row, "account") != "server:example.com" {
		t.Errorf("THE POINT: the Flag is queued as %v, not the server's account", row)
	}
	if strings.Contains(json_encode(flag), "~f1") {
		t.Errorf("THE POINT: the Flag names the reporting account: %v", flag)
	}
	if len(keys) == 0 || !strings.Contains(strings.Join(keys, " "), `keyId="https://example.com/~/actor#main-key"`) || strings.Contains(strings.Join(keys, " "), "~f1") {
		t.Errorf("THE POINT: the lookups were not signed as the server's account: %v", keys)
	}

	elsewhere := sl.NewList([]sl.Value{sl.String("https://other.example/notes/1")})
	if _, err := api_account_report(thread, fn, sl.Tuple{sl.String("f1"), sl.String(bob.id)}, []sl.Tuple{{sl.String("objects"), elsewhere}}); err == nil {
		t.Error("an object on another server was reported")
	}
	if _, err := api_account_report(thread, fn, sl.Tuple{sl.String("f1"), sl.String("https://remote.example/u/nobody")}, nil); err == nil {
		t.Error("a person who does not resolve was reported")
	}
}

// TestTextClean: HTML from elsewhere keeps its text formatting and links and
// loses scripts, handlers and styles.
func TestTextClean(t *testing.T) {
	fn := sl.NewBuiltin("test", nil)
	value, err := api_text_clean(nil, fn, sl.Tuple{sl.String(`<p>Hi <a href="https://example.com/" onclick="x()">there</a><script>alert(1)</script><style>p{}</style> <b>bold</b></p>`)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := string(value.(sl.String))
	if strings.Contains(out, "script") || strings.Contains(out, "onclick") || strings.Contains(out, "style") || !strings.Contains(out, `<a href="https://example.com/"`) || !strings.Contains(out, "<b>bold</b>") {
		t.Errorf("clean = %q", out)
	}
}
