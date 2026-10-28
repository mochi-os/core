// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// create_community_test_env makes alice's community "alice_books" - a name
// under her handle, which is free of the limit - as a group
// account, with bob following it, and a thread whose app may act for it.
func create_community_test_env(t *testing.T) (*User, *remote_person, string, *sl.Thread) {
	t.Helper()
	alice, bob := create_activitypub_test_env(t)
	if _, err := handle_reserve(alice, "", "alice_books", "community"); err != nil {
		t.Fatal(err)
	}
	app := create_external_app("forums")
	permission_grant(alice, app.id, "accounts/write")
	permission_grant(alice, app.id, "accounts/read")
	thread := create_test_thread(alice, app)
	thread.SetLocal("context", context.Background())
	fn := sl.NewBuiltin("test", nil)
	value, err := api_account_add(thread, fn, sl.Tuple{sl.String("fediverse")}, []sl.Tuple{{sl.String("handle"), sl.String("alice_books")}, {sl.String("kind"), sl.String("group")}, {sl.String("label"), sl.String("Books")}})
	if err != nil {
		t.Fatal(err)
	}
	id, _ := sl_decode_map(value)["id"].(string)
	if id == "" {
		t.Fatal("no group id")
	}
	permission_grant(alice, app.id, "account:"+id)
	db_user(alice, "user").exec("insert into followers (account, actor, inbox, shared, created) values (?, ?, ?, ?, ?)", id, bob.id, bob.inbox, "https://remote.example/inbox", now())
	return alice, bob, id, thread
}

// TestCommunityAccount: a group is made on a community name only, and only on
// the fediverse; it is listed under "group" and never among the person's
// network accounts; its document is a Group naming its page, its moderators
// collection, whether posting is restricted and whether it is sensitive;
// WebFinger says it is a Group; a browser is sent to its page.
func TestCommunityAccount(t *testing.T) {
	alice, _, id, thread := create_community_test_env(t)
	fn := sl.NewBuiltin("test", nil)

	if _, err := api_account_add(thread, fn, sl.Tuple{sl.String("fediverse")}, []sl.Tuple{{sl.String("handle"), sl.String("alice")}, {sl.String("kind"), sl.String("group")}}); !refused(err, "errors.handle_not_community", "") {
		t.Errorf("a group on a person's handle: %v", err)
	}
	if _, err := api_account_add(thread, fn, sl.Tuple{sl.String("fediverse")}, []sl.Tuple{{sl.String("handle"), sl.String("alice_books")}}); !refused(err, "errors.handle_not_held", "") {
		t.Errorf("a person's account on a community name: %v", err)
	}
	if _, err := api_account_add(thread, fn, sl.Tuple{sl.String("fediverse")}, []sl.Tuple{{sl.String("handle"), sl.String("alice_books")}, {sl.String("kind"), sl.String("club")}}); err == nil || !strings.Contains(err.Error(), "invalid kind") {
		t.Errorf("an unknown kind: %v", err)
	}

	for capability, want := range map[string]bool{"network": false, "group": true} {
		value, err := api_account_list(thread, fn, sl.Tuple{sl.String(capability)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, item := range sl_decode(value).([]any) {
			row, _ := item.(map[string]any)
			if row["id"] == id {
				found = true
				if row["kind"] != "group" || row["hosted"] != true {
					t.Errorf("listed group = %v", row)
				}
			}
		}
		if found != want {
			t.Errorf("listed under %q: %v, want %v", capability, found, want)
		}
	}

	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String(id)}, []sl.Tuple{
		{sl.String("summary"), sl.String("Books & reading")},
		{sl.String("page"), sl.String("https://example.com/forums/abc")},
		{sl.String("moderators"), sl.NewList([]sl.Value{sl.String("https://example.com/~f1")})},
		{sl.String("restricted"), sl.True},
		{sl.String("sensitive"), sl.True},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String("f1")}, []sl.Tuple{{sl.String("moderators"), sl.NewList(nil)}}); err == nil {
		t.Error("a person's account took moderators")
	}
	if _, err := api_account_update(thread, fn, sl.Tuple{sl.String(id)}, []sl.Tuple{{sl.String("moderators"), sl.NewList([]sl.Value{sl.String("not an id")})}}); err == nil {
		t.Error("a moderator that is not an id was taken")
	}

	w, document := activitypub_get(t, "/~"+id)
	key := keypair_get(alice, id, key_signing)
	public, _ := document["publicKey"].(map[string]any)
	if w.Code != http.StatusOK || document["type"] != "Group" || document["preferredUsername"] != "alice_books" || document["name"] != "Books" ||
		document["url"] != "https://example.com/forums/abc" || document["attributedTo"] != "https://example.com/~"+id+"/moderators" ||
		document["postingRestrictedToMods"] != true || document["sensitive"] != true || document["summary"] != "<p>Books &amp; reading</p>" ||
		document["inbox"] != "https://example.com/~"+id+"/inbox" || document["followers"] != "https://example.com/~"+id+"/followers" ||
		public["publicKeyPem"] != key.Public {
		t.Errorf("group document = %s", w.Body.String())
	}
	w, moderators := activitypub_get(t, "/~"+id+"/moderators")
	if items, _ := moderators["orderedItems"].([]any); w.Code != http.StatusOK || moderators["type"] != "OrderedCollection" || len(items) != 1 || items[0] != "https://example.com/~f1" {
		t.Errorf("moderators = %d %v", w.Code, moderators)
	}
	if w, _ := activitypub_get(t, "/~f1/moderators"); w.Code != http.StatusNotFound {
		t.Errorf("a person's moderators: %d", w.Code)
	}

	r := httptest.NewRequest("GET", "https://example.com/.well-known/webfinger?resource=acct:alice_books@example.com", nil)
	w = activitypub_request(t, r)
	var jrd struct {
		Subject string
		Links   []map[string]any
	}
	json.Unmarshal(w.Body.Bytes(), &jrd)
	if w.Code != http.StatusOK || jrd.Subject != "acct:alice_books@example.com" || len(jrd.Links) == 0 || jrd.Links[0]["href"] != "https://example.com/~"+id {
		t.Fatalf("webfinger = %d %s", w.Code, w.Body.String())
	}
	if properties, _ := jrd.Links[0]["properties"].(map[string]any); properties[activitystreams+"#type"] != "Group" {
		t.Errorf("webfinger properties = %v", jrd.Links[0]["properties"])
	}
	r = httptest.NewRequest("GET", "https://example.com/.well-known/webfinger?resource=acct:alice@example.com", nil)
	w = activitypub_request(t, r)
	if strings.Contains(w.Body.String(), "#type") {
		t.Error("a person's WebFinger carries a type property")
	}

	r = httptest.NewRequest("GET", "https://example.com/~"+id, nil)
	r.Header.Set("Accept", "text/html")
	w = activitypub_request(t, r)
	if w.Code != http.StatusFound || w.Header().Get("Location") != "https://example.com/forums/abc" {
		t.Errorf("browser at the group: %d %s", w.Code, w.Header().Get("Location"))
	}
	r = httptest.NewRequest("GET", "https://example.com/@alice_books", nil)
	r.Header.Set("Accept", "text/html")
	w = activitypub_request(t, r)
	if w.Code != http.StatusFound || w.Header().Get("Location") != "https://example.com/forums/abc" {
		t.Errorf("browser at the community's handle: %d %s", w.Code, w.Header().Get("Location"))
	}
}

// TestCommunityAnnounce: the group announces a member's activity whole to its
// followers, with the activity's actor told, and lists the Announce in its
// outbox; an activity whose id is not the one announced is refused; a pinned
// post of a member's is named by id in the featured collection.
func TestCommunityAnnounce(t *testing.T) {
	alice, bob, id, thread := create_community_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	create := map[string]any{
		"@context": activitystreams,
		"id":       "https://member.example/~m1/post1/activity",
		"type":     "Create",
		"actor":    "https://member.example/~m1",
		"to":       []string{activitystreams_public},
		"cc":       []string{"https://example.com/~" + id},
		"object":   map[string]any{"id": "https://member.example/~m1/post1", "type": "Page", "name": "A book"},
	}
	if _, err := api_account_announce(thread, fn, sl.Tuple{sl.String(id), sl.String("https://member.example/~m1/other")}, []sl.Tuple{{sl.String("activity"), sl_encode(create)}}); err == nil {
		t.Error("an activity under another id was announced")
	}
	value, err := api_account_announce(thread, fn, sl.Tuple{sl.String(id), sl.String("https://member.example/~m1/post1/activity")}, []sl.Tuple{{sl.String("activity"), sl_encode(create)}})
	if err != nil {
		t.Fatal(err)
	}
	announced, _ := sl_decode_map(value)["id"].(string)
	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["inbox"] != "https://remote.example/inbox" {
		t.Fatalf("deliveries = %v, want the Announce to bob's shared inbox", rows)
	}
	_, announce := delivery_body(t)
	inner, _ := announce["object"].(map[string]any)
	if announce["type"] != "Announce" || announce["actor"] != "https://example.com/~"+id || announce["id"] != announced || inner["id"] != create["id"] || inner["type"] != "Create" {
		t.Errorf("Announce = %v", announce)
	}
	if cc := object_strings(announce["cc"]); len(cc) != 2 || cc[0] != "https://example.com/~"+id+"/followers" || cc[1] != "https://member.example/~m1" {
		t.Errorf("Announce cc = %v", cc)
	}
	if _, set := inner["@context"]; set {
		t.Error("the inner activity kept its context")
	}
	_ = bob

	saved_call := activitypub_call
	activitypub_call = func(user *User, a *App, function string, args Map) (any, error) {
		if function == "activitypub/featured" {
			return []any{"https://member.example/~m1/post1"}, nil
		}
		return nil, nil
	}
	t.Cleanup(func() { activitypub_call = saved_call })
	apps_lock.Lock()
	saved_apps := apps
	apps = map[string]*App{"forums": {id: "forums", internal: &AppVersion{}}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = saved_apps
		apps_lock.Unlock()
	})

	w, outbox := activitypub_get(t, "/~"+id+"/outbox?page=true")
	items, _ := outbox["orderedItems"].([]any)
	if w.Code != http.StatusOK || len(items) != 1 || items[0].(map[string]any)["type"] != "Announce" || items[0].(map[string]any)["object"] != create["id"] {
		t.Errorf("outbox = %v", outbox)
	}
	w, featured := activitypub_get(t, "/~"+id+"/featured")
	if pinned, _ := featured["orderedItems"].([]any); w.Code != http.StatusOK || len(pinned) != 1 || pinned[0] != "https://member.example/~m1/post1" {
		t.Errorf("featured = %v", featured)
	}
	_ = alice
}

// TestCommunityActivity: an account sends an activity of the allowed kinds
// whole, addressed to the public and its followers unless it says otherwise,
// and answered at its id; its outbox lists it only when it is addressed to
// the public; a Create is refused, as is an activity with no object.
func TestCommunityActivity(t *testing.T) {
	alice, _, id, thread := create_community_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	for _, bad := range []map[string]any{{"type": "Create", "object": "https://x.example/1"}, {"type": "Lock"}} {
		if _, err := api_account_activity(thread, fn, sl.Tuple{sl.String(id), sl_encode(bad)}, nil); err == nil {
			t.Errorf("activity %v was sent", bad)
		}
	}
	value, err := api_account_activity(thread, fn, sl.Tuple{sl.String(id), sl_encode(map[string]any{"@context": activitystreams, "type": "Lock", "object": "https://member.example/~m1/post1", "cc": []string{"https://example.com/~" + id + "/followers", "https://member.example/~m1"}})}, nil)
	if err != nil {
		t.Fatal(err)
	}
	remote, _ := sl_decode_map(value)["id"].(string)
	if !strings.HasPrefix(remote, "https://example.com/~"+id+"/") {
		t.Fatalf("activity id = %q", remote)
	}
	rows := delivery_rows(t)
	if len(rows) != 1 || rows[0]["inbox"] != "https://remote.example/inbox" {
		t.Fatalf("deliveries = %v, want the Lock to bob's shared inbox", rows)
	}
	_, lock := delivery_body(t)
	if lock["type"] != "Lock" || lock["actor"] != "https://example.com/~"+id || lock["id"] != remote || lock["object"] != "https://member.example/~m1/post1" || lock["@context"] == nil {
		t.Errorf("Lock = %v", lock)
	}
	if to := object_strings(lock["to"]); len(to) != 0 {
		t.Errorf("an activity with its own cc gained to = %v", to)
	}
	w, served := activitypub_get(t, strings.TrimPrefix(remote, "https://example.com"))
	if w.Code != http.StatusOK || served["type"] != "Lock" || served["id"] != remote {
		t.Errorf("served activity = %d %v", w.Code, served)
	}
	value, err = api_account_activity(thread, fn, sl.Tuple{sl.String(id), sl_encode(map[string]any{"@context": activitystreams, "type": "Lock", "object": "https://member.example/~m1/post2"})}, nil)
	if err != nil {
		t.Fatal(err)
	}
	public, _ := sl_decode_map(value)["id"].(string)
	w, outbox := activitypub_get(t, "/~"+id+"/outbox?page=true")
	if items, _ := outbox["orderedItems"].([]any); w.Code != http.StatusOK || len(items) != 1 || items[0].(map[string]any)["id"] != public {
		t.Errorf("THE POINT: the outbox lists the public Lock alone, not the one to followers and a member: %v", outbox)
	}
	_ = alice
}

// TestAccountAddRefused: each reason a hosted account is refused for is a
// refusal with its own label and status, which the action answers to the user
// rather than a server error (web_action_error).
func TestAccountAddRefused(t *testing.T) {
	_, _, _, thread := create_community_test_env(t)
	fn := sl.NewBuiltin("test", nil)
	add := func(pairs ...string) error {
		var kwargs []sl.Tuple
		for i := 0; i+1 < len(pairs); i += 2 {
			kwargs = append(kwargs, sl.Tuple{sl.String(pairs[i]), sl.String(pairs[i+1])})
		}
		_, err := api_account_add(thread, fn, sl.Tuple{sl.String("fediverse")}, kwargs)
		return err
	}
	status := func(err error) int {
		var refusal *Refusal
		if errors.As(err, &refusal) {
			return refusal.Status
		}
		return 0
	}

	if err := add("handle", "nobody"); !refused(err, "errors.handle_not_held", "") || status(err) != http.StatusForbidden {
		t.Errorf("THE POINT: a handle the user does not hold: %v", err)
	}
	err := add("handle", "alice_books", "kind", "group")
	var refusal *Refusal
	if !refused(err, "errors.account_exists", "") || status(err) != http.StatusConflict || !errors.As(err, &refusal) || refusal.Names["network"] != "account.display.fediverse" {
		t.Errorf("THE POINT: a second account on the handle, named by its network: %v", err)
	}
	setting_set("fediverse", "false")
	defer setting_set("fediverse", "true")
	if err := add("handle", "alice"); !refused(err, "errors.network_unavailable", "") || status(err) != http.StatusForbidden {
		t.Errorf("THE POINT: a network switched off: %v", err)
	}
}
