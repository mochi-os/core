// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"nhooyr.io/websocket"
)

// A mastodon_feeds stands in for the feeds app's mastodon/* functions: posts
// and comments kept in memory, each call recorded.
type mastodon_feeds struct {
	lock     sync.Mutex
	posts    []map[string]any
	comments []map[string]any
	lists    []map[string]any
	members  map[string][]string
	calls    []string
	args     []Map
	sequence int64
	// The chat app's conversations: each message with its chat, and who
	// the chat is with.
	messages []map[string]any
	chats    map[string][]any
	read     map[string]int64
}

// sent is what the last call of a function was given.
func (f *mastodon_feeds) sent(function string) Map {
	f.lock.Lock()
	defer f.lock.Unlock()
	for i := len(f.calls) - 1; i >= 0; i-- {
		if f.calls[i] == function {
			return f.args[i]
		}
	}
	return nil
}

// direct answers the chat app's direct/* functions from the fake's
// conversations.
func (f *mastodon_feeds) direct(function string, args Map) (any, error) {
	text := func(key string) string { v, _ := args[key].(string); return v }
	if f.chats == nil {
		f.chats, f.read = map[string][]any{}, map[string]int64{}
	}
	shaped := func(m map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range m {
			out[k] = v
		}
		out["read"] = m["mine"] == true || mastodon_integer(m, "created") <= f.read[mastodon_string(m, "chat")]
		out["people"] = f.chats[mastodon_string(m, "chat")]
		return out
	}
	find := func(id string) map[string]any {
		for _, m := range f.messages {
			if m["id"] == id && m["deleted"] != true {
				return m
			}
		}
		return nil
	}
	switch function {
	case "direct/send":
		chat := text("chat")
		if chat == "" {
			people, _ := args["people"].([]any)
			for id, with := range f.chats {
				if len(with) == len(people) && len(people) > 0 && mastodon_map(with[0])["id"] == people[0] {
					chat = id
				}
			}
			if chat == "" {
				chat = "chat" + strconv.Itoa(len(f.chats)+1)
				for _, p := range people {
					f.chats[chat] = append(f.chats[chat], map[string]any{"id": p, "name": "Bob"})
				}
			}
		}
		if _, held := f.chats[chat]; !held {
			return map[string]any{"error": "Chat not found"}, nil
		}
		f.sequence++
		m := map[string]any{"id": "m" + strconv.FormatInt(f.sequence, 10), "chat": chat, "member": "e-alice", "name": "Alice", "body": text("body"), "created": now() - 1000 + f.sequence, "edited": int64(0), "remote": "", "mine": true}
		f.messages = append(f.messages, m)
		return shaped(m), nil
	case "direct/message":
		if m := find(text("message")); m != nil {
			return shaped(m), nil
		}
		return nil, nil
	case "direct/messages":
		out := []any{}
		for i := len(f.messages) - 1; i >= 0; i-- {
			m := f.messages[i]
			if m["deleted"] == true || (text("chat") != "" && m["chat"] != text("chat")) {
				continue
			}
			if person := text("person"); person != "" && (len(f.chats[mastodon_string(m, "chat")]) == 0 || mastodon_map(f.chats[mastodon_string(m, "chat")][0])["id"] != person) {
				continue
			}
			out = append(out, shaped(m))
		}
		return out, nil
	case "direct/conversations":
		out := []any{}
		for id, with := range f.chats {
			var last map[string]any
			for _, m := range f.messages {
				if m["chat"] == id && m["deleted"] != true && (last == nil || mastodon_integer(m, "created") > mastodon_integer(last, "created")) {
					last = m
				}
			}
			item := map[string]any{"id": id, "name": "Bob", "updated": now(), "kind": "", "people": with, "last": nil, "unread": false}
			if last != nil {
				shapedlast := shaped(last)
				item["last"], item["updated"], item["unread"] = shapedlast, last["created"], shapedlast["read"] != true
			}
			out = append(out, item)
		}
		return out, nil
	case "direct/read":
		chat := text("chat")
		if m := find(text("message")); m != nil {
			chat = mastodon_string(m, "chat")
		}
		f.read[chat] = now() + 1000
		return map[string]any{"read": f.read[chat]}, nil
	case "direct/edit":
		m := find(text("message"))
		if m == nil || m["mine"] != true {
			return map[string]any{"error": "Message not found"}, nil
		}
		m["body"], m["edited"] = text("body"), now()
		return shaped(m), nil
	case "direct/delete":
		m := find(text("message"))
		if m == nil || m["mine"] != true {
			return map[string]any{"error": "Message not found"}, nil
		}
		m["deleted"] = true
		return map[string]any{"deleted": []any{text("message")}}, nil
	}
	return nil, fmt.Errorf("the fake has no %s", function)
}

func (f *mastodon_feeds) call(s *mastodon_session, a *App, function string, args Map) (any, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.calls = append(f.calls, function)
	f.args = append(f.args, args)
	text := func(key string) string { v, _ := args[key].(string); return v }
	find := func(id string) map[string]any {
		for _, p := range f.posts {
			if p["id"] == id {
				return p
			}
		}
		return nil
	}
	comment := func(id string) map[string]any {
		for _, c := range f.comments {
			if c["id"] == id {
				out := map[string]any{}
				for k, v := range c {
					out[k] = v
				}
				out["post"] = find(c["post"].(string))
				return out
			}
		}
		return nil
	}
	switch function {
	case "mastodon/posts":
		var out []any
		kind := text("kind")
		var wanted map[string]bool
		if ids, ok := args["ids"].([]any); ok {
			wanted = map[string]bool{}
			for _, id := range ids {
				wanted[fmt.Sprint(id)] = true
			}
		}
		sorted := append([]map[string]any(nil), f.posts...)
		sort.SliceStable(sorted, func(i, j int) bool { return sorted[i]["created"].(int64) > sorted[j]["created"].(int64) })
		var before, after int64
		if p := find(text("before")); p != nil {
			before = p["created"].(int64)
		}
		if p := find(text("after")); p != nil {
			after = p["created"].(int64)
		}
		limit := args["limit"]
		for _, p := range sorted {
			switch kind {
			case "ids":
				if !wanted[p["id"].(string)] {
					continue
				}
			case "remote":
				if !wanted[fmt.Sprint(p["remote"])] {
					continue
				}
			case "list":
				if p["feed"] != text("feed") {
					continue
				}
			case "tag":
				if !strings.Contains(fmt.Sprint(p["tags"]), text("tag")) {
					continue
				}
			case "favourites":
				if p["reaction"] == "" {
					continue
				}
			case "bookmarks":
				if p["saved"] != true {
					continue
				}
			case "own":
				if p["owned"] != true {
					continue
				}
			}
			if before > 0 && p["created"].(int64) >= before {
				continue
			}
			if after > 0 && p["created"].(int64) <= after {
				continue
			}
			out = append(out, p)
			if l, ok := limit.(int); ok && len(out) >= l {
				break
			}
		}
		return out, nil
	case "mastodon/create":
		if text("home") == "" {
			return map[string]any{"error": "The feed this app posts to is gone"}, nil
		}
		f.sequence++
		if reply := text("reply"); reply != "" {
			parent, post := "", reply
			if id, ok := strings.CutPrefix(reply, "c:"); ok {
				c := comment(id)
				parent, post = id, c["post"].(map[string]any)["id"].(string)
			}
			c := map[string]any{"id": fmt.Sprintf("c%d", f.sequence), "post": post, "parent": parent, "subscriber": "identity-alice", "name": "Alice", "body": text("text"), "format": "text", "created": now() + f.sequence, "mine": true, "reaction": "", "likes": int64(0), "replies": int64(0)}
			f.comments = append(f.comments, c)
			return map[string]any{"comment": comment(c["id"].(string))}, nil
		}
		var attachments []any
		media, _ := args["media"].([]any)
		for i, m := range media {
			item := m.(map[string]any)
			attachments = append(attachments, map[string]any{"id": fmt.Sprintf("a%d-%d", f.sequence, i), "name": item["name"], "type": item["type"], "size": len(item["data"].([]byte)), "url": "https://example.com/feeds/fp1/-/attachments/a" + strconv.Itoa(i), "description": item["description"]})
		}
		data := map[string]any{}
		if poll, ok := args["poll"].(map[string]any); ok {
			var options []any
			for _, o := range poll["options"].([]any) {
				options = append(options, map[string]any{"name": o, "votes": int64(0)})
			}
			data["poll"] = map[string]any{"options": options, "multiple": poll["multiple"], "voters": int64(0)}
		}
		p := map[string]any{"id": fmt.Sprintf("p%d", f.sequence), "feed": "feed1", "feed_name": "Home", "fingerprint": "fp1", "owned": true, "created": now() + f.sequence, "edited": int64(0),
			"body": text("text"), "format": "text", "data": data, "author": "identity-alice", "remote": "", "visibility": text("visibility"), "warning": text("warning"), "sensitive": args["sensitive"],
			"language": text("language"), "pinned": int64(0), "link": fmt.Sprintf("https://example.com/feeds/fp1/p%d", f.sequence), "attachments": attachments,
			"comments": int64(0), "likes": int64(0), "boosts": int64(0), "reaction": "", "saved": false, "copies": []any{}, "tags": []any{}}
		f.posts = append(f.posts, p)
		return map[string]any{"post": p}, nil
	case "mastodon/edit":
		p := find(text("id"))
		if p == nil {
			return map[string]any{"error": "No such post"}, nil
		}
		p["body"] = text("text")
		p["edited"] = now()
		return map[string]any{"post": p}, nil
	case "mastodon/delete":
		for i, p := range f.posts {
			if p["id"] == text("id") {
				f.posts = append(f.posts[:i], f.posts[i+1:]...)
				return map[string]any{"deleted": true}, nil
			}
		}
		return map[string]any{"error": "No such post"}, nil
	case "mastodon/react", "mastodon/save", "mastodon/pin":
		id := text("id")
		if cid, ok := strings.CutPrefix(id, "c:"); ok {
			for _, c := range f.comments {
				if c["id"] == cid {
					c["reaction"] = map[bool]string{true: "like", false: ""}[args["on"] == true]
				}
			}
			return map[string]any{"comment": comment(cid)}, nil
		}
		p := find(id)
		if p == nil {
			return map[string]any{"error": "No such post"}, nil
		}
		switch function {
		case "mastodon/react":
			p["reaction"] = map[bool]string{true: "like", false: ""}[args["on"] == true]
		case "mastodon/pin":
			p["pinned"] = map[bool]int64{true: 1, false: 0}[args["on"] == true]
		default:
			p["saved"] = args["on"] == true
		}
		return map[string]any{"post": p}, nil
	case "mastodon/comments":
		var out []any
		for _, c := range f.comments {
			if c["post"] == text("post") {
				out = append(out, comment(c["id"].(string)))
			}
		}
		return out, nil
	case "mastodon/comment":
		if c := comment(text("id")); c != nil {
			return c, nil
		}
		return nil, nil
	case "mastodon/follow":
		return map[string]any{"followed": args["on"]}, nil
	case "mastodon/lists":
		var out []any
		for _, l := range f.lists {
			out = append(out, l)
		}
		return out, nil
	case "mastodon/list":
		switch text("action") {
		case "create":
			l := map[string]any{"id": fmt.Sprintf("list%d", len(f.lists)+1), "name": text("name"), "created": now()}
			f.lists = append(f.lists, l)
			return map[string]any{"list": l}, nil
		case "delete":
			return map[string]any{"error": "Delete the feed in Feeds"}, nil
		case "accounts":
			var out []any
			for _, m := range f.members[text("id")] {
				out = append(out, m)
			}
			return out, nil
		case "add":
			f.members[text("id")] = append(f.members[text("id")], text("address"))
			return map[string]any{}, nil
		}
		return map[string]any{}, nil
	case "mastodon/notifications":
		var out []any
		for _, raw := range args["rows"].([]any) {
			row := raw.(map[string]any)
			// As feeds sends them: rolled up on the feed's row, the item in
			// the event.
			item := strings.TrimPrefix(fmt.Sprint(row["event"]), fmt.Sprint(row["topic"])+":")
			switch row["topic"] {
			case "comment/mine":
				out = append(out, map[string]any{"type": "mention", "comment": comment(item)})
			case "reaction/mine":
				out = append(out, map[string]any{"type": "favourite", "post": find(strings.SplitN(item, ":", 2)[0]), "person": map[string]any{"id": "https://remote.example/users/bob", "name": "Bob", "address": "@bob@remote.example"}})
			case "account":
				out = append(out, map[string]any{"type": "follow", "person": map[string]any{"id": "https://remote.example/users/carol", "name": "Carol", "address": "@carol@remote.example"}})
			default:
				out = append(out, nil)
			}
		}
		return out, nil
	}
	if strings.HasPrefix(function, "direct/") {
		return f.direct(function, args)
	}
	return nil, fmt.Errorf("the fake has no %s", function)
}

// create_mastodon_test_env is alice with her hosted fediverse account
// @alice@example.com ("f1"), its home feed "feed1", the sign-in tables, and
// the fake standing in for the feeds app.
func create_mastodon_test_env(t *testing.T) (*User, *mastodon_feeds) {
	t.Helper()
	alice := create_signin_test_env(t)
	setup_vapid(t)
	db := db_user(alice, "user")
	db.exec("update accounts set enabled=1, data=? where id='f1'", json_encode(map[string]any{"hosted": true, "handle": "alice", "domain": "example.com", "home": "feed1"}))
	actor_register(alice, "f1", "fediverse", "example.com", "alice")
	fake := &mastodon_feeds{members: map[string][]string{}}
	saved_caller, saved_rows := mastodon_caller, mastodon_notification_rows
	mastodon_caller = fake.call
	t.Cleanup(func() { mastodon_caller, mastodon_notification_rows = saved_caller, saved_rows })
	functions := map[string]AppFunction{}
	for _, name := range []string{"posts", "comments", "comment", "create", "edit", "delete", "react", "save", "pin", "boost", "hold", "vote", "follow", "lists", "list", "notifications"} {
		functions["mastodon/"+name] = AppFunction{Function: "function_mastodon_" + name}
	}
	direct := map[string]AppFunction{}
	for _, name := range []string{"conversations", "messages", "message", "send", "edit", "delete", "read"} {
		direct["direct/"+name] = AppFunction{Function: "function_direct_" + name}
	}
	apps_lock.Lock()
	apps["feeds1"] = &App{id: "feeds1", internal: &AppVersion{Functions: functions}}
	apps["chat1"] = &App{id: "chat1", internal: &AppVersion{Functions: direct}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, "feeds1")
		delete(apps, "chat1")
		apps_lock.Unlock()
		mastodon_streams_lock.Lock()
		mastodon_streams = map[string]map[*mastodon_stream]bool{}
		mastodon_streams_lock.Unlock()
	})
	return alice, fake
}

// mastodon_token is a Mastodon app's token for alice's account, with the
// scopes given.
func mastodon_token(t *testing.T, alice *User, scope string) string {
	t.Helper()
	m := signin_mint(alice, "", "Test", "fediverse", "f1", scope, "", "", false)
	if m == nil {
		t.Fatal("no token")
	}
	return m.token
}

// mastodon_request sends one request to the interface as example.com, with
// a token, answering the recorder and the decoded body.
func mastodon_request(t *testing.T, token string, method string, path string, body any, headers ...string) (*httptest.ResponseRecorder, any) {
	t.Helper()
	var reader io.Reader = http.NoBody
	kind := ""
	switch b := body.(type) {
	case nil:
	case url.Values:
		reader, kind = strings.NewReader(b.Encode()), "application/x-www-form-urlencoded"
	case *bytes.Buffer:
		reader = b
	default:
		data, _ := json.Marshal(b)
		reader, kind = bytes.NewReader(data), "application/json"
	}
	request := httptest.NewRequest(method, "http://example.com"+path, reader)
	if kind != "" {
		request.Header.Set("Content-Type", kind)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		request.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = request
	if !mastodon_route(c) {
		t.Fatalf("%s %s was not answered", method, path)
	}
	var answer any
	json.Unmarshal(w.Body.Bytes(), &answer)
	return w, answer
}

// mastodon_items is an answer as a list of maps.
func mastodon_items(v any) []map[string]any {
	items, _ := v.([]any)
	var out []map[string]any
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// TestMastodonAuthentication: a request without a token, with another
// network's, or with one lacking the scope a request needs is refused; a
// read token reads.
func TestMastodonAuthentication(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	if w, body := mastodon_request(t, "", "GET", "/api/v1/accounts/verify_credentials", nil); w.Code != 401 || mastodon_map(body)["error"] == nil {
		t.Errorf("THE POINT: no token: %d %v", w.Code, body)
	}
	matrix := signin_mint(alice, "", "Matrix", "matrix", "m1", "urn:matrix:client:api:*", "", "", false)
	if w, _ := mastodon_request(t, matrix.token, "GET", "/api/v1/accounts/verify_credentials", nil); w.Code != 401 {
		t.Errorf("THE POINT: a Matrix token: %d", w.Code)
	}
	other := signin_mint(alice, "", "Other", "fediverse", "m1", "read", "", "", false)
	if w, _ := mastodon_request(t, other.token, "GET", "/api/v1/accounts/verify_credentials", nil); w.Code != 401 {
		t.Errorf("THE POINT: a token for another network's account: %d", w.Code)
	}
	read := mastodon_token(t, alice, "read")
	if w, body := mastodon_request(t, read, "GET", "/api/v1/accounts/verify_credentials", nil); w.Code != 200 || mastodon_map(body)["username"] != "alice" {
		t.Errorf("a read token reads: %d %v", w.Code, body)
	}
	if w, _ := mastodon_request(t, read, "POST", "/api/v1/statuses", map[string]any{"status": "hello"}); w.Code != 403 {
		t.Errorf("THE POINT: a read token posted: %d", w.Code)
	}
	if w, _ := mastodon_request(t, read, "POST", "/api/v1/accounts/1/follow", nil); w.Code != 403 {
		t.Errorf("THE POINT: a read token followed: %d", w.Code)
	}
	if w, _ := mastodon_request(t, mastodon_token(t, alice, "read write"), "POST", "/api/v1/push/subscription", nil); w.Code != 403 {
		t.Errorf("a token without push subscribed: %d", w.Code)
	}
	scoped := mastodon_token(t, alice, "read:statuses")
	if w, _ := mastodon_request(t, scoped, "GET", "/api/v1/timelines/home", nil); w.Code != 200 {
		t.Errorf("a token with read:statuses read the home timeline: %d", w.Code)
	}
}

// TestMastodonInstance: the instance is described as Mastodon 4.3 with the
// server's push key, and an app checks its token.
func TestMastodonInstance(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	_, v2 := mastodon_request(t, "", "GET", "/api/v2/instance", nil)
	instance := mastodon_map(v2)
	configuration := mastodon_map(instance["configuration"])
	if key, _ := mastodon_map(configuration["vapid"])["public_key"].(string); instance["domain"] != "example.com" || !strings.HasPrefix(fmt.Sprint(instance["version"]), "4.3.0") || key == "" {
		t.Errorf("THE POINT: the instance = %v", instance)
	}
	if _, v1 := mastodon_request(t, "", "GET", "/api/v1/instance", nil); mastodon_map(v1)["uri"] != "example.com" {
		t.Errorf("the old instance = %v", v1)
	}
	w, registered := signin_call("POST", "/api/v1/apps", "client_name=Ivory&redirect_uris=ivory%3A%2F%2Fcb&scopes=read+write", nil)
	if key, _ := registered["vapid_key"].(string); w.Code != 200 || key == "" {
		t.Fatalf("THE POINT: registration = %d %v", w.Code, registered)
	}
	m := signin_mint(alice, registered["client_id"].(string), "Ivory", "fediverse", "f1", "read write", "", "", false)
	w, app := signin_call("GET", "/api/v1/apps/verify_credentials", "", map[string]string{"Authorization": "Bearer " + m.token})
	if w.Code != 200 || app["name"] != "Ivory" {
		t.Errorf("THE POINT: the app checking its token = %d %v", w.Code, app)
	}
}

// TestMastodonInstanceAbout: the instance carries the owner's description
// and contact address, and the rules: each section of the rules document that
// is a list of points, in the reader's language, as plain text.
func TestMastodonInstanceAbout(t *testing.T) {
	create_mastodon_test_env(t)
	// The owner's documents, as documents_test.go lays them, and the core
	// labels, which are what makes a language one the server speaks.
	db_open("db/settings.db").exec("create table if not exists documents ( name text not null, language text not null, body text not null, updated integer not null, primary key ( name, language ) )")
	load_core_labels()
	setting_set("description", "Wasabi test server")
	setting_set("operator_email", "admin@example.com")
	setting_set("operator_jurisdiction", "Estonia")
	rules := func(value any) []map[string]any {
		var out []map[string]any
		list, _ := value.([]any)
		for _, rule := range list {
			out = append(out, mastodon_map(rule))
		}
		return out
	}

	_, v2 := mastodon_request(t, "", "GET", "/api/v2/instance", nil)
	instance := mastodon_map(v2)
	if instance["description"] != "Wasabi test server" || mastodon_map(instance["contact"])["email"] != "admin@example.com" {
		t.Errorf("THE POINT: the instance's description %v and contact %v", instance["description"], instance["contact"])
	}
	listed := rules(instance["rules"])
	var titles []string
	for _, rule := range listed {
		titles = append(titles, fmt.Sprint(rule["text"]))
	}
	if strings.Join(titles, "|") != "Be respectful|Stay legal|Don't spam or deceive|Respect privacy" {
		t.Errorf("THE POINT: the rules are the document's lists, got %q", titles)
	}
	if len(listed) == 4 {
		legal, _ := listed[1]["hint"].(string)
		if listed[0]["id"] != "1" || listed[1]["id"] != "2" || !strings.HasPrefix(legal, "No content that is illegal under the law of Estonia.\n") || strings.Contains(legal, "**") || strings.Contains(legal, "- ") {
			t.Errorf("THE POINT: a rule's points, as plain text = %q (%v)", legal, listed[1]["id"])
		}
	}

	_, v1 := mastodon_request(t, "", "GET", "/api/v1/instance", nil)
	old := mastodon_map(v1)
	if old["description"] != "Wasabi test server" || old["short_description"] != "Wasabi test server" || old["email"] != "admin@example.com" || len(rules(old["rules"])) != 4 {
		t.Errorf("THE POINT: the old instance = %v %v %v, %d rules", old["description"], old["short_description"], old["email"], len(rules(old["rules"])))
	}
	if _, answer := mastodon_request(t, "", "GET", "/api/v1/instance/rules", nil); len(rules(answer)) != 4 || rules(answer)[2]["text"] != "Don't spam or deceive" {
		t.Errorf("THE POINT: the rules on their own = %v", answer)
	}

	german := strings.SplitN(document_get("rules", "de"), "\n## ", 2)[1]
	german = german[:strings.IndexByte(german, '\n')]
	if _, answer := mastodon_request(t, "", "GET", "/api/v1/instance/rules", nil, "Accept-Language", "de"); len(rules(answer)) != 4 || rules(answer)[0]["text"] != german {
		t.Errorf("THE POINT: the rules in German open with %q, got %v", german, answer)
	}

	if err := document_set("rules", "en", "# Rules\n\nBe kind.\n\n## Be __kind__\n\n- No [spam](https://example.org/spam) at all\n* No `bots`\n\n## Changes\n\nWe may change these."); err != nil {
		t.Fatal(err)
	}
	_, answer := mastodon_request(t, "", "GET", "/api/v1/instance/rules", nil)
	if listed := rules(answer); len(listed) != 1 || listed[0]["text"] != "Be kind" || listed[0]["hint"] != "No spam at all\nNo bots" {
		t.Errorf("THE POINT: the owner's own rules = %v", listed)
	}
}

// TestMastodonAccounts: the signed-in account shows its followers and those
// it follows as core keeps them; its name changes; a relationship says who
// follows whom and what is blocked or muted; and blocking and muting are
// the user's.
func TestMastodonAccounts(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write follow")
	db := db_user(alice, "user")
	db.exec("insert into followers (account, actor, inbox, created) values ('f1', 'https://remote.example/users/bob', 'https://remote.example/inbox', 1)")
	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values ('f1', 'https://remote.example/users/bob', 'https://remote.example/inbox', 'x', 1, 2)")
	db.exec("insert into following (account, actor, inbox, activity, accepted, created) values ('f1', 'https://remote.example/users/carol', 'https://remote.example/inbox', 'y', 0, 3)")
	_, me := mastodon_request(t, token, "GET", "/api/v1/accounts/verify_credentials", nil)
	self := mastodon_map(me)
	if self["acct"] != "alice" || fmt.Sprint(self["followers_count"]) != "1" || fmt.Sprint(self["following_count"]) != "2" || self["source"] == nil || self["uri"] != "https://example.com/~f1" {
		t.Errorf("THE POINT: the account = %v", self)
	}
	if _, changed := mastodon_request(t, token, "PATCH", "/api/v1/accounts/update_credentials", url.Values{"display_name": {"Alice Two"}}); mastodon_map(changed)["display_name"] != "Alice Two" {
		t.Errorf("THE POINT: the name was not changed: %v", changed)
	}
	if _, found := mastodon_request(t, token, "GET", "/api/v1/accounts/lookup?acct=alice", nil); mastodon_map(found)["id"] != self["id"] {
		t.Errorf("looking the account up = %v", found)
	}
	bob := mastodon_number(alice, "account", "https://remote.example/users/bob", 0)
	carol := mastodon_number(alice, "account", "https://remote.example/users/carol", 0)
	_, relations := mastodon_request(t, token, "GET", "/api/v1/accounts/relationships?id[]="+bob+"&id[]="+carol, nil)
	list := mastodon_items(relations)
	if len(list) != 2 || list[0]["following"] != true || list[0]["followed_by"] != true || list[1]["following"] != false || list[1]["requested"] != true {
		t.Errorf("THE POINT: the relationships = %v", list)
	}
	if _, blocked := mastodon_request(t, token, "POST", "/api/v1/accounts/"+bob+"/block", nil); mastodon_map(blocked)["blocking"] != true || !actor_blocked(alice, "https://remote.example/users/bob") {
		t.Errorf("THE POINT: the block = %v", blocked)
	}
	if found, _ := db.exists("select 1 from followers where actor='https://remote.example/users/bob'"); found {
		t.Error("a blocked person is still a follower")
	}
	if _, muted := mastodon_request(t, token, "POST", "/api/v1/accounts/"+carol+"/mute", nil); mastodon_map(muted)["muting"] != true {
		t.Errorf("THE POINT: the mute = %v", muted)
	}
	if _, unmuted := mastodon_request(t, token, "POST", "/api/v1/accounts/"+carol+"/unmute", nil); mastodon_map(unmuted)["muting"] != false || actor_muted(alice, "https://remote.example/users/carol") {
		t.Errorf("the unmute = %v", unmuted)
	}
	mastodon_request(t, token, "POST", "/api/v1/domain_blocks", url.Values{"domain": {"spam.example"}})
	if _, domains := mastodon_request(t, token, "GET", "/api/v1/domain_blocks", nil); fmt.Sprint(domains) != "[spam.example]" {
		t.Errorf("THE POINT: the domain blocks = %v", domains)
	}
	if w, _ := mastodon_request(t, token, "GET", "/api/v1/accounts/999", nil); w.Code != 404 {
		t.Errorf("an unknown account: %d", w.Code)
	}
}

// TestMastodonStatuses: a status posted is a post the feeds app makes in
// the home feed, with its visibility and files; a client retrying with the
// same key posts once; statuses carry numbers that sort as they were made
// and page by them; a favourite, a bookmark and a reply go to the app; a
// post's comments are its context; and a delete answers the text, for a
// client to draft again.
func TestMastodonStatuses(t *testing.T) {
	alice, fake := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	if w, _ := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": strings.Repeat("x", 5001)}); w.Code != 422 {
		t.Errorf("THE POINT: a status past the limit: %d", w.Code)
	}
	// An upload, then a status taking it.
	var form bytes.Buffer
	writer := multipart.NewWriter(&form)
	part, _ := writer.CreateFormFile("file", "cat.png")
	part.Write([]byte("\x89PNG\r\n\x1a\nfake picture"))
	writer.WriteField("description", "A cat")
	writer.Close()
	w, uploaded := mastodon_request(t, token, "POST", "/api/v2/media", &form, "Content-Type", writer.FormDataContentType())
	if w.Code != 200 || mastodon_map(uploaded)["type"] != "image" {
		t.Fatalf("upload = %d %v", w.Code, uploaded)
	}
	media := mastodon_map(uploaded)["id"].(string)
	if w, _ := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "x", "media_ids": []string{"12345"}}); w.Code != 422 {
		t.Errorf("THE POINT: a file the account did not upload was taken: %d", w.Code)
	}
	planted := mastodon_number(alice, "media", "planted", 0)
	os.MkdirAll(filepath.Dir(mastodon_upload_path(alice, "planted")), 0o700)
	os.WriteFile(mastodon_upload_path(alice, "planted"), []byte("\x89PNG\r\n\x1a\nplanted"), 0o600)
	if w, _ := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "x", "media_ids": []string{planted}}); w.Code != 422 {
		t.Errorf("THE POINT: a file kept without its upload record was taken: %d", w.Code)
	}
	w, posted := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "first", "visibility": "private", "spoiler_text": "careful", "media_ids": []string{media}}, "Idempotency-Key", "k1")
	first := mastodon_map(posted)
	if w.Code != 200 || first["content"] != "<p>first</p>" || first["visibility"] != "private" || first["spoiler_text"] != "careful" || first["sensitive"] != true {
		t.Fatalf("THE POINT: the status = %d %v", w.Code, first)
	}
	args := fake.args[len(fake.args)-1]
	files, _ := args["media"].([]any)
	if args["visibility"] != "followers" || args["home"] != "feed1" || len(files) != 1 || string(files[0].(map[string]any)["data"].([]byte)) != "\x89PNG\r\n\x1a\nfake picture" || files[0].(map[string]any)["description"] != "A cat" {
		t.Errorf("THE POINT: what the app was asked to post = %v", args)
	}
	if account := mastodon_map(first["account"]); account["acct"] != "alice" || len(mastodon_items(first["media_attachments"])) != 1 {
		t.Errorf("the status's account and media = %v %v", account, first["media_attachments"])
	}
	creates := 0
	for _, call := range fake.calls {
		if call == "mastodon/create" {
			creates++
		}
	}
	if _, again := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "first"}, "Idempotency-Key", "k1"); mastodon_map(again)["id"] != first["id"] {
		t.Errorf("THE POINT: a retried status was posted again: %v", again)
	}
	for _, call := range fake.calls {
		if call == "mastodon/create" {
			creates--
		}
	}
	if creates != 0 {
		t.Errorf("THE POINT: the retry reached the app")
	}
	_, second := mastodon_request(t, token, "POST", "/api/v1/statuses", url.Values{"status": {"second"}})
	_, third := mastodon_request(t, token, "POST", "/api/v1/statuses", url.Values{"status": {"third"}})
	a, _ := strconv.ParseInt(first["id"].(string), 10, 64)
	b, _ := strconv.ParseInt(mastodon_map(second)["id"].(string), 10, 64)
	c, _ := strconv.ParseInt(mastodon_map(third)["id"].(string), 10, 64)
	if !(a < b && b < c) || a < (now()-100)*1000<<16 {
		t.Errorf("THE POINT: the numbers do not sort as the statuses were made: %d %d %d", a, b, c)
	}

	// The home timeline, newest first, paged by number.
	w, home := mastodon_request(t, token, "GET", "/api/v1/timelines/home?limit=2", nil)
	page := mastodon_items(home)
	if len(page) != 2 || page[0]["content"] != "<p>third</p>" || page[1]["content"] != "<p>second</p>" || !strings.Contains(w.Header().Get("Link"), "max_id="+mastodon_map(second)["id"].(string)) {
		t.Fatalf("THE POINT: the home timeline = %v %q", page, w.Header().Get("Link"))
	}
	if _, older := mastodon_request(t, token, "GET", "/api/v1/timelines/home?max_id="+mastodon_map(second)["id"].(string), nil); len(mastodon_items(older)) != 1 || mastodon_items(older)[0]["id"] != first["id"] {
		t.Errorf("THE POINT: the page after = %v", older)
	}
	if _, newer := mastodon_request(t, token, "GET", "/api/v1/timelines/home?since_id="+mastodon_map(second)["id"].(string), nil); len(mastodon_items(newer)) != 1 || mastodon_items(newer)[0]["content"] != "<p>third</p>" {
		t.Errorf("the page before = %v", newer)
	}

	// A favourite and a bookmark go to the app.
	if _, liked := mastodon_request(t, token, "POST", "/api/v1/statuses/"+first["id"].(string)+"/favourite", nil); mastodon_map(liked)["favourited"] != true {
		t.Errorf("THE POINT: the favourite = %v", liked)
	}
	if _, marked := mastodon_request(t, token, "POST", "/api/v1/statuses/"+first["id"].(string)+"/bookmark", nil); mastodon_map(marked)["bookmarked"] != true {
		t.Errorf("THE POINT: the bookmark = %v", marked)
	}
	if _, bookmarks := mastodon_request(t, token, "GET", "/api/v1/bookmarks", nil); len(mastodon_items(bookmarks)) != 1 {
		t.Errorf("the bookmarks = %v", bookmarks)
	}
	// A pin goes to the app, which pins the post as the user does there.
	if _, pinned := mastodon_request(t, token, "POST", "/api/v1/statuses/"+first["id"].(string)+"/pin", nil); mastodon_map(pinned)["pinned"] != true || fake.args[len(fake.args)-1]["on"] != true {
		t.Errorf("THE POINT: the pin = %v, asked %v", pinned, fake.args[len(fake.args)-1])
	}
	if _, unpinned := mastodon_request(t, token, "POST", "/api/v1/statuses/"+first["id"].(string)+"/unpin", nil); mastodon_map(unpinned)["pinned"] != false {
		t.Errorf("THE POINT: the unpin = %v", unpinned)
	}
	// A reply is a comment, and the post's context.
	_, reply := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "a reply", "in_reply_to_id": first["id"]})
	if mastodon_map(reply)["in_reply_to_id"] != first["id"] || fake.args[len(fake.args)-1]["reply"] != "p1" {
		t.Errorf("THE POINT: the reply = %v, asked %v", reply, fake.args[len(fake.args)-1])
	}
	_, deeper := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "deeper", "in_reply_to_id": mastodon_map(reply)["id"]})
	if mastodon_map(deeper)["in_reply_to_id"] != mastodon_map(reply)["id"] {
		t.Errorf("the reply to the reply = %v", deeper)
	}
	if w, _ := mastodon_request(t, token, "POST", "/api/v1/statuses/"+mastodon_map(reply)["id"].(string)+"/pin", nil); w.Code != http.StatusUnprocessableEntity {
		t.Errorf("THE POINT: pinning a comment = %d, want 422", w.Code)
	}
	_, context := mastodon_request(t, token, "GET", "/api/v1/statuses/"+first["id"].(string)+"/context", nil)
	if descendants := mastodon_items(mastodon_map(context)["descendants"]); len(descendants) != 2 {
		t.Errorf("THE POINT: the post's context = %v", context)
	}
	_, inner := mastodon_request(t, token, "GET", "/api/v1/statuses/"+mastodon_map(deeper)["id"].(string)+"/context", nil)
	if ancestors := mastodon_items(mastodon_map(inner)["ancestors"]); len(ancestors) != 2 || ancestors[0]["id"] != first["id"] {
		t.Errorf("THE POINT: a comment's ancestors = %v", inner)
	}
	// A delete answers the text.
	if w, deleted := mastodon_request(t, token, "DELETE", "/api/v1/statuses/"+mastodon_map(third)["id"].(string), nil); w.Code != 200 || mastodon_map(deleted)["text"] != "third" {
		t.Errorf("THE POINT: the delete = %d %v", w.Code, deleted)
	}
	if w, _ := mastodon_request(t, token, "GET", "/api/v1/statuses/"+mastodon_map(third)["id"].(string), nil); w.Code != 404 {
		t.Errorf("the deleted status is still there: %d", w.Code)
	}
	// With no home feed, posting fails with the app's reason.
	db_user(alice, "user").exec("update accounts set data=? where id='f1'", json_encode(map[string]any{"hosted": true, "handle": "alice", "domain": "example.com"}))
	if w, failed := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "lost"}); w.Code != 422 || !strings.Contains(fmt.Sprint(failed), "gone") {
		t.Errorf("THE POINT: posting with no home feed = %d %v", w.Code, failed)
	}
}

// TestMastodonNotifications: the notifications the feeds app accounts for -
// a reply, a favourite, a follow - are the account's, newest first, of the
// kinds asked, grouped for newer clients; one dismissed is gone, and a
// clear empties the list; the unread count follows the read position.
func TestMastodonNotifications(t *testing.T) {
	alice, fake := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	_, posted := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "mine"})
	_, reply := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "a reply", "in_reply_to_id": mastodon_map(posted)["id"]})
	_ = reply
	mastodon_notification_rows = func(s *mastodon_session) []map[string]any {
		return []map[string]any{
			{"id": "n1", "app": "feeds1", "topic": "comment/mine", "object": "feed1", "created": now() - 30, "event": "feeds1:comment/mine:" + fake.comments[0]["id"].(string)},
			{"id": "n2", "app": "feeds1", "topic": "reaction/mine", "object": "feed1", "created": now() - 20, "event": "feeds1:reaction/mine:p1:bob:like"},
			{"id": "n3", "app": "feeds1", "topic": "account", "object": "f1", "created": now() - 10, "event": "feeds1:account:follow:https://remote.example/users/carol"},
			{"id": "n4", "app": "calendar1", "topic": "reminder", "object": "x", "created": now(), "event": ""},
		}
	}
	_, listed := mastodon_request(t, token, "GET", "/api/v1/notifications", nil)
	items := mastodon_items(listed)
	if len(items) != 3 || items[0]["type"] != "follow" || items[1]["type"] != "favourite" || items[2]["type"] != "mention" {
		t.Fatalf("THE POINT: the notifications = %v", items)
	}
	if status := mastodon_map(items[2]["status"]); status == nil || status["in_reply_to_id"] != mastodon_map(posted)["id"] {
		t.Errorf("the reply's status = %v", items[2]["status"])
	}
	if who := mastodon_map(items[1]["account"]); who["acct"] != "bob@remote.example" {
		t.Errorf("who favourited = %v", who)
	}
	if _, follows := mastodon_request(t, token, "GET", "/api/v1/notifications?types[]=follow", nil); len(mastodon_items(follows)) != 1 {
		t.Errorf("THE POINT: the follows alone = %v", follows)
	}
	_, grouped := mastodon_request(t, token, "GET", "/api/v2/notifications", nil)
	if groups := mastodon_items(mastodon_map(grouped)["notification_groups"]); len(groups) != 3 || len(mastodon_items(mastodon_map(grouped)["accounts"])) < 2 {
		t.Errorf("THE POINT: the grouped notifications = %v", grouped)
	}
	mastodon_request(t, token, "POST", "/api/v1/markers", map[string]any{"notifications": map[string]any{"last_read_id": items[1]["id"]}})
	if _, count := mastodon_request(t, token, "GET", "/api/v1/notifications/unread_count", nil); fmt.Sprint(mastodon_map(count)["count"]) != "1" {
		t.Errorf("THE POINT: the unread count = %v", count)
	}
	if _, markers := mastodon_request(t, token, "GET", "/api/v1/markers?timeline[]=notifications", nil); mastodon_map(mastodon_map(markers)["notifications"])["last_read_id"] != items[1]["id"] {
		t.Errorf("the marker = %v", markers)
	}
	mastodon_request(t, token, "POST", "/api/v1/notifications/"+items[0]["id"].(string)+"/dismiss", nil)
	if _, after := mastodon_request(t, token, "GET", "/api/v1/notifications", nil); len(mastodon_items(after)) != 2 {
		t.Errorf("THE POINT: the dismissed notification stayed: %v", after)
	}
	mastodon_request(t, token, "POST", "/api/v1/notifications/clear", nil)
	if _, cleared := mastodon_request(t, token, "GET", "/api/v1/notifications", nil); len(mastodon_items(cleared)) != 0 {
		t.Errorf("THE POINT: the cleared notifications = %v", cleared)
	}
}

// mastodon_alert_add keeps a notification for alice's account with no number
// or kind on its row, as one copied from where they were kept before, or one
// whose recording stopped before it was numbered: answering its row id.
func mastodon_alert_add(t *testing.T, alice *User, row string, created int64, answer map[string]any) string {
	t.Helper()
	db := db_user(alice, "networks")
	defer db.close()
	db.exec("insert into alerts (account, row, event, answer, created) values ('f1', ?, 'e', ?, ?)", row, json_encode(answer), created)
	kept, _ := db.row("select id from alerts where account='f1' and row=?", row)
	return strconv.FormatInt(row_int(kept, "id"), 10)
}

// mastodon_follow_alert is a follow by u<i>@remote.example.
func mastodon_follow_alert(i int) map[string]any {
	return map[string]any{"type": "follow", "person": map[string]any{"id": fmt.Sprintf("https://remote.example/users/u%d", i), "address": fmt.Sprintf("@u%d@remote.example", i), "name": fmt.Sprintf("U%d", i)}}
}

// mastodon_notifications_walk pages through the notifications a path asks
// for by each page's Link, answering who each one is from, in order.
func mastodon_notifications_walk(t *testing.T, token string, path string) []string {
	t.Helper()
	next := regexp.MustCompile(`[?&]max_id=(\d+)[^>]*>; rel="next"`)
	var names []string
	for cursor, pages := "", 0; pages < 1000; pages++ {
		address := path
		if cursor != "" {
			address += "&max_id=" + cursor
		}
		w, body := mastodon_request(t, token, "GET", address, nil)
		items := mastodon_items(body)
		for _, item := range items {
			names = append(names, strings.TrimSuffix(fmt.Sprint(mastodon_map(item["account"])["acct"]), "@remote.example"))
		}
		m := next.FindStringSubmatch(w.Header().Get("Link"))
		if m == nil || len(items) == 0 {
			break
		}
		cursor = m[1]
	}
	return names
}

// TestMastodonNotificationsWalk: paging through notifications lists each one
// once, in the order of their numbers, the numbers clients page by: those
// arriving in one second newest first, and those numbered before in whatever
// order they were given. min_id pages from the oldest side, since_id from the
// newest.
func TestMastodonNotificationsWalk(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	for i := 0; i < 10; i++ {
		mastodon_alert_add(t, alice, fmt.Sprintf("r%d", i), 1_700_000_000+int64(i/5), mastodon_follow_alert(i))
	}
	// Two numbered by an earlier server, the newer one first.
	ten := mastodon_alert_add(t, alice, "r10", 1_700_000_002, mastodon_follow_alert(10))
	eleven := mastodon_alert_add(t, alice, "r11", 1_700_000_002, mastodon_follow_alert(11))
	mastodon_number(alice, "notification", "alert:"+eleven, 1_700_000_002)
	numbered := mastodon_number(alice, "notification", "alert:"+ten, 1_700_000_002)
	// Asked for by its number before any list has read it: the row is given
	// the number its client holds.
	if _, one := mastodon_request(t, token, "GET", "/api/v1/notifications/"+numbered, nil); mastodon_map(one)["id"] != numbered || mastodon_map(mastodon_map(one)["account"])["acct"] != "u10@remote.example" {
		t.Errorf("THE POINT: the notification asked for by its number = %v", one)
	}

	want := "u10 u11 u9 u8 u7 u6 u5 u4 u3 u2 u1 u0"
	if got := strings.Join(mastodon_notifications_walk(t, token, "/api/v1/notifications?limit=3"), " "); got != want {
		t.Errorf("THE POINT: a walk of 12 notifications = %s, want %s", got, want)
	}
	_, all := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=40", nil)
	items := mastodon_items(all)
	if len(items) != 12 {
		t.Fatalf("the notifications = %v", items)
	}
	numbers := map[string]string{}
	for _, item := range items {
		numbers[strings.TrimSuffix(fmt.Sprint(mastodon_map(item["account"])["acct"]), "@remote.example")] = item["id"].(string)
	}
	names := func(body any) string {
		var out []string
		for _, item := range mastodon_items(body) {
			out = append(out, strings.TrimSuffix(fmt.Sprint(mastodon_map(item["account"])["acct"]), "@remote.example"))
		}
		return strings.Join(out, " ")
	}
	if _, newer := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=3&min_id="+numbers["u3"], nil); names(newer) != "u6 u5 u4" {
		t.Errorf("THE POINT: the three just newer than u3 = %s", names(newer))
	}
	if _, newest := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=3&since_id="+numbers["u3"], nil); names(newest) != "u10 u11 u9" {
		t.Errorf("THE POINT: the newest three since u3 = %s", names(newest))
	}
	who := mastodon_map(items[4]["account"])["id"].(string)
	if _, from := mastodon_request(t, token, "GET", "/api/v1/notifications?account_id="+who, nil); names(from) != "u7" {
		t.Errorf("THE POINT: the notifications from u7 = %s", names(from))
	}

	// One recorded as it comes, after those kept before were numbered, is
	// numbered then and listed first.
	if !mastodon_alert_record(mastodon_session_for(alice, account_hosted_get(alice, "f1")), map[string]any{"id": "r12", "event": "e", "created": int64(1_700_000_003)}, mastodon_follow_alert(12)) {
		t.Fatal("the new notification was not kept")
	}
	if _, newest := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=2", nil); names(newest) != "u12 u10" {
		t.Errorf("THE POINT: the newest two after one more came = %s", names(newest))
	}
	// One whose recording stopped between keeping and numbering it is
	// numbered by the next request and listed.
	mastodon_alert_add(t, alice, "r13", 1_700_000_004, mastodon_follow_alert(13))
	if _, newest := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=2", nil); names(newest) != "u13 u12" {
		t.Errorf("THE POINT: the newest two after one kept without its number = %s", names(newest))
	}
	// Two in one second, the later told to a streaming client before any
	// list reads them: numbered as they came, so the later is the newer.
	session := mastodon_session_for(alice, account_hosted_get(alice, "f1"))
	mastodon_alert_record(session, map[string]any{"id": "r14", "event": "e", "created": int64(1_700_000_005)}, mastodon_follow_alert(14))
	mastodon_alert_record(session, map[string]any{"id": "r15", "event": "e", "created": int64(1_700_000_005)}, mastodon_follow_alert(15))
	told, _ := db_user(alice, "networks").row("select * from alerts where row='r15'")
	mastodon_alert(nil, session, told)
	if _, newest := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=2", nil); names(newest) != "u15 u14" {
		t.Errorf("THE POINT: two of one second, the later told first = %s", names(newest))
	}
}

// TestMastodonNotificationsQueryReadsTheNumbers: a page is read as a range of
// the account's index of numbers - or of kinds and numbers, for one kind -
// never by sorting all the account keeps.
func TestMastodonNotificationsQueryReadsTheNumbers(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	db := db_user(alice, "networks")
	defer db.close()
	plan := func(w mastodon_wanted, maximum int64, minimum int64) string {
		query, args := mastodon_alerts_query("f1", w, maximum, minimum, 20)
		rows, err := db.rows("explain query plan "+query, args...)
		if err != nil {
			t.Fatal(err)
		}
		var details []string
		for _, row := range rows {
			details = append(details, row_string(row, "detail"))
		}
		return strings.Join(details, "\n")
	}
	for _, c := range []struct {
		name             string
		w                mastodon_wanted
		maximum, minimum int64
		index            string
	}{
		{"every kind", mastodon_wanted{limit: 20}, 0, 0, "alerts_number"},
		{"every kind below a number", mastodon_wanted{limit: 20}, 1 << 60, 0, "alerts_number"},
		{"every kind from the oldest side", mastodon_wanted{limit: 20, oldest: true}, 0, 1 << 50, "alerts_number"},
		{"all but follows", mastodon_wanted{limit: 20, excluded: []string{"follow"}}, 1 << 60, 0, "alerts_number"},
		{"one person's", mastodon_wanted{limit: 20, key: "https://remote.example/users/bob"}, 1 << 60, 0, "alerts_number"},
		{"mentions", mastodon_wanted{limit: 20, types: []string{"mention"}}, 1 << 60, 0, "alerts_type"},
	} {
		if got := plan(c.w, c.maximum, c.minimum); !strings.HasPrefix(got, "SEARCH alerts USING INDEX "+c.index+" (account=? AND") || strings.Contains(got, "TEMP B-TREE") {
			t.Errorf("THE POINT: a page of %s is not read as a range of %s:\n%s", c.name, c.index, got)
		}
	}
}

// TestMastodonNotificationsPageReadsItsRows: a page renders the rows it
// shows, not every row kept - none of the older ones, whose statuses this
// server has nothing of and would fetch - and fills past rows that render
// nothing and rows of kinds not asked for.
func TestMastodonNotificationsPageReadsItsRows(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	var fetched atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetched.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(remote.Close)
	saved_private, saved_cache := url_allow_private, resolve_cache
	url_allow_private, resolve_cache = true, map[string]resolve_entry{}
	t.Cleanup(func() { url_allow_private, resolve_cache = saved_private, saved_cache })

	for i := 0; i < 50; i++ {
		mastodon_alert_add(t, alice, fmt.Sprintf("old%d", i), 1_600_000_000+int64(i), map[string]any{"type": "favourite", "remote": fmt.Sprintf("%s/notes/%d", remote.URL, i), "person": mastodon_follow_alert(i)["person"]})
	}
	held := remote.URL + "/notes/held"
	mastodon_snapshot(alice, "status", "note:"+held, map[string]any{"id": "1", "uri": held, "content": "held"})
	mastodon_alert_add(t, alice, "mention", 1_650_000_000, map[string]any{"type": "mention", "remote": held, "person": mastodon_follow_alert(99)["person"]})
	for i := 0; i < 10; i++ {
		mastodon_alert_add(t, alice, fmt.Sprintf("follow%d", i), 1_700_000_000+int64(i), mastodon_follow_alert(i))
	}
	// Newest of all, three that name nothing and render as nothing.
	for i := 0; i < 3; i++ {
		mastodon_alert_add(t, alice, fmt.Sprintf("gone%d", i), 1_800_000_000+int64(i), map[string]any{"type": "mention"})
	}

	_, page := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=5", nil)
	if items := mastodon_items(page); len(items) != 5 || items[0]["type"] != "follow" {
		t.Errorf("THE POINT: a page under three that render nothing = %v", page)
	}
	if fetched.Load() != 0 {
		t.Errorf("THE POINT: a page of five fetched %d statuses of older notifications", fetched.Load())
	}
	if _, mentions := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=1&types[]=mention", nil); len(mastodon_items(mentions)) != 1 || mastodon_items(mentions)[0]["type"] != "mention" {
		t.Errorf("THE POINT: the mentions under ten follows = %v", mentions)
	}
	if _, others := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=1&exclude_types[]=follow", nil); len(mastodon_items(others)) != 1 || mastodon_items(others)[0]["type"] != "mention" {
		t.Errorf("THE POINT: the first that is not a follow = %v", others)
	}
	if fetched.Load() != 0 {
		t.Errorf("THE POINT: pages of one fetched %d statuses of older notifications", fetched.Load())
	}
	// From one person: the rows naming others are not read.
	_, mentions := mastodon_request(t, token, "GET", "/api/v1/notifications?limit=1&types[]=mention", nil)
	person := mastodon_map(mastodon_items(mentions)[0]["account"])["id"].(string)
	if _, from := mastodon_request(t, token, "GET", "/api/v1/notifications?account_id="+person, nil); len(mastodon_items(from)) != 1 || mastodon_items(from)[0]["type"] != "mention" {
		t.Errorf("THE POINT: the notifications from the one who mentioned = %v", from)
	}
	if fetched.Load() != 0 {
		t.Errorf("THE POINT: the notifications from one person fetched %d statuses of others'", fetched.Load())
	}
	if _, nobody := mastodon_request(t, token, "GET", "/api/v1/notifications?account_id=12345", nil); len(mastodon_items(nobody)) != 0 || fetched.Load() != 0 {
		t.Errorf("THE POINT: the notifications from no one known = %v, after %d fetches", nobody, fetched.Load())
	}
}

// TestMastodonNotificationsReadingLimit: one request reads at most so many
// kept notifications - each rendered, and a mention naming no person, as the
// feeds app keeps them, must be rendered to learn whom it is from, fetching a
// post its server no longer answers for every time - and its Link goes on
// from the last it read, so the next request reads the next.
func TestMastodonNotificationsReadingLimit(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	var fetched atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetched.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(remote.Close)
	saved_private, saved_cache, saved_reading := url_allow_private, resolve_cache, mastodon_alerts_reading
	url_allow_private, resolve_cache, mastodon_alerts_reading = true, map[string]resolve_entry{}, 30
	t.Cleanup(func() {
		url_allow_private, resolve_cache, mastodon_alerts_reading = saved_private, saved_cache, saved_reading
	})

	mastodon_alert_add(t, alice, "follow", 1_600_000_000, mastodon_follow_alert(1))
	for i := 0; i < 50; i++ {
		mastodon_alert_add(t, alice, fmt.Sprintf("mention%d", i), 1_700_000_000+int64(i), map[string]any{"type": "mention", "remote": fmt.Sprintf("%s/notes/%d", remote.URL, i)})
	}
	_, follows := mastodon_request(t, token, "GET", "/api/v1/notifications?types[]=follow", nil)
	if items := mastodon_items(follows); len(items) != 1 || fetched.Load() != 0 {
		t.Fatalf("the follows = %v, after %d fetches", follows, fetched.Load())
	}
	person := mastodon_map(mastodon_items(follows)[0]["account"])["id"].(string)

	next := regexp.MustCompile(`[?&]max_id=(\d+)[^>]*>; rel="next"`)
	w, body := mastodon_request(t, token, "GET", "/api/v1/notifications?account_id="+person, nil)
	m := next.FindStringSubmatch(w.Header().Get("Link"))
	if len(mastodon_items(body)) != 0 || fetched.Load() != 30 || m == nil {
		t.Fatalf("THE POINT: a request over 50 mentions naming no one = %d items, %d fetches, Link %q", len(mastodon_items(body)), fetched.Load(), w.Header().Get("Link"))
	}
	w, body = mastodon_request(t, token, "GET", "/api/v1/notifications?account_id="+person+"&max_id="+m[1], nil)
	if items := mastodon_items(body); len(items) != 1 || items[0]["type"] != "follow" || fetched.Load() != 50 {
		t.Errorf("THE POINT: the request going on from the last read = %v, after %d fetches", body, fetched.Load())
	}
}

// TestMastodonNumberUnderConcurrency: items numbered at once, many in one
// second, each get a number of their own, never "0" - a notification without
// one is never listed.
func TestMastodonNumberUnderConcurrency(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	var wg sync.WaitGroup
	numbers := make([][]string, 8)
	for g := range numbers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				numbers[g] = append(numbers[g], mastodon_number(alice, "notification", fmt.Sprintf("alert:%d", g*1000+i), 1_700_000_000))
			}
		}()
	}
	wg.Wait()
	seen, lost := map[string]bool{}, 0
	for _, list := range numbers {
		for _, n := range list {
			if n == "0" {
				lost++
			}
			seen[n] = true
		}
	}
	if lost > 0 || len(seen) != 400 {
		t.Errorf("THE POINT: 400 items numbered at once: %d given no number, %d different numbers", lost, len(seen))
	}
}

// TestMastodonNotificationsAllReachable: every kept notification can be paged
// to, past the newest 400, and the unread count counts to the limit Mastodon
// takes, 100 unless asked, of the 400 one request reads.
func TestMastodonNotificationsAllReachable(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	for i := 0; i < 1005; i++ {
		mastodon_alert_add(t, alice, fmt.Sprintf("r%d", i), 1_700_000_000+int64(i), mastodon_follow_alert(i))
	}
	names := mastodon_notifications_walk(t, token, "/api/v1/notifications?limit=40")
	seen := map[string]bool{}
	for _, name := range names {
		seen[name] = true
	}
	if len(names) != 1005 || len(seen) != 1005 {
		t.Errorf("THE POINT: a walk of 1,005 notifications listed %d, %d of them different", len(names), len(seen))
	}
	for _, c := range []struct {
		query string
		want  string
	}{{"", "100"}, {"?limit=300", "300"}, {"?limit=5000", "400"}} {
		if _, count := mastodon_request(t, token, "GET", "/api/v1/notifications/unread_count"+c.query, nil); fmt.Sprint(mastodon_map(count)["count"]) != c.want {
			t.Errorf("THE POINT: the unread count%s = %v, want %s", c.query, count, c.want)
		}
	}
}

// TestMastodonFilters: a filter's keyword marks the statuses that hold it,
// and one that hides keeps them out of the timeline.
func TestMastodonFilters(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "spoilers ahead"})
	mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "nothing here"})
	_, made := mastodon_request(t, token, "POST", "/api/v2/filters", map[string]any{"title": "No spoilers", "context": []string{"home"}, "filter_action": "warn", "keywords_attributes": []any{map[string]any{"keyword": "spoilers", "whole_word": true}}})
	filter := mastodon_map(made)
	if filter["title"] != "No spoilers" || len(mastodon_items(filter["keywords"])) != 1 {
		t.Fatalf("filter = %v", made)
	}
	_, home := mastodon_request(t, token, "GET", "/api/v1/timelines/home", nil)
	items := mastodon_items(home)
	if len(items) != 2 || len(items[1]["filtered"].([]any)) != 1 || len(items[0]["filtered"].([]any)) != 0 {
		t.Errorf("THE POINT: the filtered statuses = %v", items)
	}
	mastodon_request(t, token, "PUT", "/api/v2/filters/"+filter["id"].(string), map[string]any{"filter_action": "hide"})
	if _, hidden := mastodon_request(t, token, "GET", "/api/v1/timelines/home", nil); len(mastodon_items(hidden)) != 1 {
		t.Errorf("THE POINT: a hiding filter left the status: %v", hidden)
	}
	mastodon_request(t, token, "DELETE", "/api/v2/filters/"+filter["id"].(string), nil)
	if _, filters := mastodon_request(t, token, "GET", "/api/v2/filters", nil); len(mastodon_items(filters)) != 0 {
		t.Errorf("the deleted filter = %v", filters)
	}
}

// TestMastodonLists: the user's feeds are lists, a list's timeline is the
// feed's, and deleting one is refused, since the feed takes its posts with
// it.
func TestMastodonLists(t *testing.T) {
	alice, fake := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	_, made := mastodon_request(t, token, "POST", "/api/v1/lists", map[string]any{"title": "Friends"})
	list := mastodon_map(made)
	if list["title"] != "Friends" {
		t.Fatalf("list = %v", made)
	}
	mastodon_request(t, token, "GET", "/api/v1/timelines/list/"+list["id"].(string), nil)
	if args := fake.args[len(fake.args)-1]; args["kind"] != "list" || args["feed"] != "list1" {
		t.Errorf("THE POINT: the list's timeline asked = %v", args)
	}
	if w, _ := mastodon_request(t, token, "DELETE", "/api/v1/lists/"+list["id"].(string), nil); w.Code != 422 {
		t.Errorf("THE POINT: a list was deleted: %d", w.Code)
	}
	if _, lists := mastodon_request(t, token, "GET", "/api/v1/lists", nil); len(mastodon_items(lists)) != 1 {
		t.Errorf("the lists = %v", lists)
	}
}

// TestMastodonPush: a push subscription is kept per token with the kinds it
// asks for, a new notification is pushed to it as a Web Push, and a push
// service saying the subscription is gone ends it.
func TestMastodonPush(t *testing.T) {
	alice, fake := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write push")
	pushed := make(chan *http.Request, 4)
	var status int = 201
	var lock sync.Mutex
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		lock.Lock()
		code := status
		lock.Unlock()
		w.WriteHeader(code)
		pushed <- r
	}))
	t.Cleanup(endpoint.Close)
	if w, _ := mastodon_request(t, token, "POST", "/api/v1/push/subscription", map[string]any{"subscription": map[string]any{"endpoint": "http://plain.example/push", "keys": map[string]any{"p256dh": "x", "auth": "y"}}}); w.Code != 422 {
		t.Errorf("THE POINT: a push endpoint over plain HTTP: %d", w.Code)
	}
	// The subscription goes to the test server's https address as the push
	// service; the guard sends it to the loopback server in its place.
	private, _ := ecdh.P256().GenerateKey(rand.Reader)
	key := base64.RawURLEncoding.EncodeToString(private.PublicKey().Bytes())
	secret := make([]byte, 16)
	rand.Read(secret)
	auth := base64.RawURLEncoding.EncodeToString(secret)
	w, made := mastodon_request(t, token, "POST", "/api/v1/push/subscription", map[string]any{"subscription": map[string]any{"endpoint": "https://push.example/send/abc", "keys": map[string]any{"p256dh": key, "auth": auth}}, "data": map[string]any{"alerts": map[string]any{"mention": true, "favourite": false}}})
	alerts := mastodon_map(mastodon_map(made)["alerts"])
	if w.Code != 200 || alerts["mention"] != true || alerts["favourite"] != false || mastodon_map(made)["server_key"] == "" {
		t.Fatalf("THE POINT: the subscription = %d %v", w.Code, made)
	}
	_, read := mastodon_request(t, token, "GET", "/api/v1/push/subscription", nil)
	if mastodon_map(read)["endpoint"] != "https://push.example/send/abc" {
		t.Errorf("the subscription read back = %v", read)
	}
	// The push service is the test server.
	db_user(alice, "networks").exec("update subscriptions set endpoint=?", endpoint.URL+"/send/abc")
	mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "mine"})
	mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "a reply", "in_reply_to_id": mastodon_number(alice, "status", "post:p1", 0)})
	rows := []map[string]any{{"id": "n1", "app": "feeds1", "topic": "comment/mine", "object": "feed1", "created": now(), "event": "feeds1:comment/mine:" + fake.comments[0]["id"].(string), "read": int64(0)}}
	mastodon_notification_rows = func(s *mastodon_session) []map[string]any { return rows }
	mastodon_heard_notification(alice, "n1")
	select {
	case r := <-pushed:
		if r.Header.Get("Content-Encoding") != "aes128gcm" || !strings.HasPrefix(r.Header.Get("Authorization"), "vapid ") || !strings.HasSuffix(r.Header.Get("Authorization"), ", k="+webpush_public) {
			t.Errorf("THE POINT: the push = %v", r.Header)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: the notification was not pushed")
	}
	// A favourite is not one of the kinds asked for.
	rows = []map[string]any{{"id": "n2", "app": "feeds1", "topic": "reaction/mine", "object": "feed1", "created": now(), "event": "feeds1:reaction/mine:p1:bob:like", "read": int64(0)}}
	mastodon_heard_notification(alice, "n2")
	select {
	case <-pushed:
		t.Error("THE POINT: a kind not asked for was pushed")
	case <-time.After(300 * time.Millisecond):
	}
	// A push service the user has blocked is not sent to: pushes go through the guard.
	mastodon_request(t, token, "POST", "/api/v1/domain_blocks", url.Values{"domain": {"127.0.0.1"}})
	rows = []map[string]any{{"id": "n9", "app": "feeds1", "topic": "comment/mine", "object": "feed1", "created": now(), "event": "feeds1:comment/mine:" + fake.comments[0]["id"].(string), "read": int64(0)}}
	mastodon_heard_notification(alice, "n9")
	select {
	case <-pushed:
		t.Error("THE POINT: a push went round the guard")
	case <-time.After(300 * time.Millisecond):
	}
	mastodon_request(t, token, "DELETE", "/api/v1/domain_blocks?domain=127.0.0.1", nil)
	// The same event again, as a refreshed row is announced, is not pushed.
	rows = []map[string]any{{"id": "n1", "app": "feeds1", "topic": "comment/mine", "object": "feed1", "created": now(), "event": "feeds1:comment/mine:" + fake.comments[0]["id"].(string), "read": int64(0)}}
	mastodon_heard_notification(alice, "n1")
	select {
	case <-pushed:
		t.Error("THE POINT: an event already pushed was pushed again")
	case <-time.After(300 * time.Millisecond):
	}
	// A subscription the push service has forgotten ends.
	lock.Lock()
	status = 410
	lock.Unlock()
	mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "another reply", "in_reply_to_id": mastodon_number(alice, "status", "post:p1", 0)})
	rows = []map[string]any{{"id": "n1", "app": "feeds1", "topic": "comment/mine", "object": "feed1", "created": now() + 1, "event": "feeds1:comment/mine:" + fake.comments[1]["id"].(string), "read": int64(0)}}
	mastodon_heard_notification(alice, "n1")
	<-pushed
	if found, _ := db_user(alice, "networks").exists("select 1 from subscriptions"); found {
		t.Error("THE POINT: the forgotten subscription stayed")
	}
}

// TestMastodonStreaming: a client streaming its user stream is told of a
// post made in a feed it follows, as the feeds app announces it, and of a
// new notification; a client streaming nothing is told nothing.
func TestMastodonStreaming(t *testing.T) {
	alice, fake := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Host = "example.com"
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		mastodon_route(c)
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/streaming?access_token="+url.QueryEscape(token), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	ws.Write(ctx, websocket.MessageText, []byte(`{"type":"subscribe","stream":"user"}`))
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		streams := mastodon_streams_of(alice.UID)
		if len(streams) == 1 {
			streams[0].lock.Lock()
			subscribed := streams[0].streams["user"]
			streams[0].lock.Unlock()
			if subscribed {
				break
			}
		}
	}
	mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "live"})
	// With the networks switched off, an app's write tells no stream.
	networks_development = false
	mastodon_heard(alice, apps["feeds1"], "fp1", map[string]any{"type": "post/create", "feed": "feed1", "post": "p1"})
	networks_development = true
	quiet, stop := context.WithTimeout(ctx, 500*time.Millisecond)
	_, early, err := ws.Read(quiet)
	stop()
	if err == nil {
		t.Fatalf("THE POINT: a write with the networks off reached the stream: %s", early)
	}
	ws, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/api/v1/streaming?access_token="+url.QueryEscape(token), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	ws.Write(ctx, websocket.MessageText, []byte(`{"type":"subscribe","stream":"user"}`))
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		streams := mastodon_streams_of(alice.UID)
		if len(streams) == 1 {
			streams[0].lock.Lock()
			subscribed := streams[0].streams["user"]
			streams[0].lock.Unlock()
			if subscribed {
				break
			}
		}
	}
	mastodon_heard(alice, apps["feeds1"], "fp1", map[string]any{"type": "post/create", "feed": "feed1", "post": "p1"})
	_, message, err := ws.Read(ctx)
	if err != nil {
		t.Fatalf("THE POINT: the stream was not told of the post: %v", err)
	}
	var event struct {
		Stream  []string `json:"stream"`
		Event   string   `json:"event"`
		Payload string   `json:"payload"`
	}
	json.Unmarshal(message, &event)
	var status map[string]any
	json.Unmarshal([]byte(event.Payload), &status)
	if event.Event != "update" || status["content"] != "<p>live</p>" || event.Stream[0] != "user" {
		t.Errorf("THE POINT: the stream's update = %s", message)
	}
	_ = fake
	mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "a reply", "in_reply_to_id": mastodon_number(alice, "status", "post:p1", 0)})
	mastodon_notification_rows = func(s *mastodon_session) []map[string]any {
		return []map[string]any{{"id": "n1", "app": "feeds1", "topic": "comment/mine", "object": "feed1", "created": now(), "event": "feeds1:comment/mine:" + fake.comments[0]["id"].(string), "read": int64(0)}}
	}
	db_open("db/users.db").exec("update tokens set network='fediverse' where hash=?", token_hash(token))
	mastodon_heard(alice, apps["feeds1"], "notifications", map[string]any{"type": "new", "id": "n1", "read": int64(0)})
	_, message, err = ws.Read(ctx)
	if err != nil {
		t.Fatalf("THE POINT: the stream was not told of the notification: %v", err)
	}
	json.Unmarshal(message, &event)
	if event.Event != "notification" || !strings.Contains(event.Payload, `"type":"mention"`) {
		t.Errorf("THE POINT: the stream's notification = %s", message)
	}
}

// TestMastodonConversations: a direct status is a message of a conversation in chat,
// never a post: sent to the people it mentions, or into the conversation of
// the direct status it answers; the conversations list holds it, unread
// until read, and a deleted one comes back with its next message; the
// user's own message is edited and deleted through chat, someone else's is
// not; another person's message is a mention among the notifications.
func TestMastodonConversations(t *testing.T) {
	alice, fake := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	resolve_lock.Lock()
	resolve_cache["@bob@remote.example"] = resolve_entry{value: &resolved{Network: "fediverse", Type: "person", ID: "https://remote.example/users/bob", Username: "bob", Host: "remote.example", Name: "Bob", URL: "https://remote.example/@bob"}, expires: time.Now().Add(time.Hour)}
	resolve_lock.Unlock()

	sends := func() int {
		n := 0
		for _, call := range fake.calls {
			if call == "direct/send" {
				n++
			}
		}
		return n
	}
	if w, _ := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "Nobody named", "visibility": "direct"}); w.Code != http.StatusUnprocessableEntity || sends() != 0 {
		t.Errorf("a direct status naming nobody = %d", w.Code)
	}
	w, sent := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "@bob@remote.example Hello", "visibility": "direct"})
	for _, call := range fake.calls {
		if call == "mastodon/create" {
			t.Fatal("THE POINT: a direct status was posted to a feed")
		}
	}
	status := mastodon_map(sent)
	if w.Code != http.StatusOK || status["visibility"] != "direct" || !strings.Contains(fmt.Sprint(status["content"]), "Hello") {
		t.Fatalf("THE POINT: the direct status = %d %v", w.Code, sent)
	}
	if people, _ := fake.sent("direct/send")["people"].([]any); len(people) != 1 || people[0] != "https://remote.example/users/bob" {
		t.Errorf("sent to %v", fake.sent("direct/send"))
	}
	if mentions := mastodon_items(status["mentions"]); len(mentions) != 1 || mentions[0]["acct"] != "bob@remote.example" {
		t.Errorf("mentions = %v", status["mentions"])
	}
	if _, read := mastodon_request(t, token, "GET", "/api/v1/statuses/"+fmt.Sprint(status["id"]), nil); mastodon_map(read)["visibility"] != "direct" {
		t.Errorf("reading the direct status = %v", read)
	}

	_, listed := mastodon_request(t, token, "GET", "/api/v1/conversations", nil)
	conversations := mastodon_items(listed)
	if len(conversations) != 1 || conversations[0]["unread"] != false || mastodon_map(conversations[0]["last_status"])["id"] != status["id"] {
		t.Fatalf("THE POINT: the conversations = %v", listed)
	}
	if accounts := mastodon_items(conversations[0]["accounts"]); len(accounts) != 1 || accounts[0]["acct"] != "bob@remote.example" {
		t.Errorf("with = %v", conversations[0]["accounts"])
	}
	conversation := fmt.Sprint(conversations[0]["id"])

	// Bob answers: the conversation is unread, and his message a mention.
	fake.lock.Lock()
	fake.messages = append(fake.messages, map[string]any{"id": "mbob", "chat": "chat1", "member": "https://remote.example/users/bob", "name": "Bob", "body": "Hi Alice", "created": now() - 900, "edited": int64(0), "remote": "https://remote.example/notes/1", "mine": false})
	fake.lock.Unlock()
	_, listed = mastodon_request(t, token, "GET", "/api/v1/conversations", nil)
	conversations = mastodon_items(listed)
	last := mastodon_map(conversations[0]["last_status"])
	if conversations[0]["unread"] != true || mastodon_map(last["account"])["acct"] != "bob@remote.example" {
		t.Fatalf("after bob's answer = %v", listed)
	}
	if _, read := mastodon_request(t, token, "POST", "/api/v1/conversations/"+conversation+"/read", nil); mastodon_map(read)["unread"] != false {
		t.Errorf("THE POINT: the conversation read = %v", read)
	}
	mastodon_notification_rows = func(s *mastodon_session) []map[string]any {
		return []map[string]any{{"id": "d1", "app": "chat1", "topic": "message", "object": "chat1", "created": now(), "event": "chat1:message:mbob"}}
	}
	_, notified := mastodon_request(t, token, "GET", "/api/v1/notifications", nil)
	if items := mastodon_items(notified); len(items) != 1 || items[0]["type"] != "mention" || mastodon_map(items[0]["status"])["visibility"] != "direct" || mastodon_map(items[0]["account"])["acct"] != "bob@remote.example" {
		t.Errorf("THE POINT: bob's message among the notifications = %v", notified)
	}

	// Answering his message goes into the same conversation.
	if _, answered := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "Good to hear", "visibility": "direct", "in_reply_to_id": last["id"]}); mastodon_map(answered)["visibility"] != "direct" {
		t.Fatalf("the answer = %v", answered)
	}
	if fake.sent("direct/send")["chat"] != "chat1" {
		t.Errorf("THE POINT: the answer went to %v", fake.sent("direct/send"))
	}
	_, public := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "A post"})
	before := sends()
	if w, _ := mastodon_request(t, token, "POST", "/api/v1/statuses", map[string]any{"status": "Privately", "visibility": "direct", "in_reply_to_id": mastodon_map(public)["id"]}); w.Code != http.StatusUnprocessableEntity || sends() != before {
		t.Errorf("a direct answer to a post = %d", w.Code)
	}

	// The user's own message is edited and deleted; bob's is not.
	if _, edited := mastodon_request(t, token, "PUT", "/api/v1/statuses/"+fmt.Sprint(status["id"]), map[string]any{"status": "Hello again"}); !strings.Contains(fmt.Sprint(mastodon_map(edited)["content"]), "Hello again") || mastodon_map(edited)["edited_at"] == nil {
		t.Errorf("THE POINT: the edit = %v", edited)
	}
	if w, _ := mastodon_request(t, token, "PUT", "/api/v1/statuses/"+fmt.Sprint(last["id"]), map[string]any{"status": "Not mine"}); w.Code != http.StatusForbidden {
		t.Errorf("editing bob's message = %d", w.Code)
	}
	if w, _ := mastodon_request(t, token, "POST", "/api/v1/statuses/"+fmt.Sprint(status["id"])+"/favourite", nil); w.Code != http.StatusUnprocessableEntity {
		t.Errorf("favouriting a direct status = %d", w.Code)
	}
	if _, deleted := mastodon_request(t, token, "DELETE", "/api/v1/statuses/"+fmt.Sprint(status["id"]), nil); mastodon_map(deleted)["text"] != "Hello again" {
		t.Errorf("THE POINT: the deletion = %v", deleted)
	}
	if w, _ := mastodon_request(t, token, "GET", "/api/v1/statuses/"+fmt.Sprint(status["id"]), nil); w.Code != http.StatusNotFound {
		t.Errorf("the deleted message = %d", w.Code)
	}

	// Taken off the list, the conversation stays off until a new message.
	mastodon_request(t, token, "DELETE", "/api/v1/conversations/"+conversation, nil)
	if _, listed := mastodon_request(t, token, "GET", "/api/v1/conversations", nil); len(mastodon_items(listed)) != 0 {
		t.Errorf("THE POINT: a removed conversation is listed: %v", listed)
	}
	fake.lock.Lock()
	fake.messages = append(fake.messages, map[string]any{"id": "mbob2", "chat": "chat1", "member": "https://remote.example/users/bob", "name": "Bob", "body": "Still there?", "created": now() + 5, "edited": int64(0), "remote": "https://remote.example/notes/2", "mine": false})
	fake.lock.Unlock()
	if _, listed := mastodon_request(t, token, "GET", "/api/v1/conversations", nil); len(mastodon_items(listed)) != 1 {
		t.Errorf("a removed conversation with a new message = %v", listed)
	}
}

// TestMastodonReports: a report of someone on another server goes to their
// server's moderators as a Flag from the server's own account, naming the
// statuses of theirs complained of and the comment; the user cannot report
// themselves.
func TestMastodonReports(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read write")
	resolve_lock.Lock()
	resolve_cache["https://remote.example/users/bob"] = resolve_entry{value: &resolved{Network: "fediverse", Type: "person", ID: "https://remote.example/users/bob", Username: "bob", Host: "remote.example", Inbox: "https://remote.example/users/bob/inbox", Shared: "https://remote.example/inbox"}, expires: time.Now().Add(time.Hour)}
	resolve_lock.Unlock()
	db_open("db/networks.db").exec("delete from deliveries")
	bob := mastodon_number(alice, "account", "https://remote.example/users/bob", 0)
	mastodon_snapshot(alice, "status", "note:https://remote.example/notes/9", map[string]any{"id": "x"})
	note := mastodon_number(alice, "status", "note:https://remote.example/notes/9", 0)
	w, answer := mastodon_request(t, token, "POST", "/api/v1/reports", map[string]any{"account_id": bob, "status_ids": []string{note}, "comment": "Spam, again"})
	if w.Code != http.StatusOK || mastodon_map(answer)["forwarded"] != true {
		t.Fatalf("THE POINT: the report = %d %v", w.Code, answer)
	}
	row, _ := db_open("db/networks.db").row("select inbox, body from deliveries")
	if row == nil || row_string(row, "inbox") != "https://remote.example/inbox" {
		t.Fatalf("THE POINT: the Flag was not sent to bob's server: %v", row)
	}
	var flag map[string]any
	json.Unmarshal([]byte(row_string(row, "body")), &flag)
	objects := object_strings(flag["object"])
	if flag["type"] != "Flag" || flag["actor"] != activitypub_server_actor("example.com") || flag["content"] != "Spam, again" || len(objects) != 2 || objects[0] != "https://remote.example/users/bob" || objects[1] != "https://remote.example/notes/9" {
		t.Errorf("THE POINT: the Flag = %v", flag)
	}
	self := mastodon_number(alice, "account", "identity:e-alice", 0)
	if w, refused := mastodon_request(t, token, "POST", "/api/v1/reports", map[string]any{"account_id": self, "comment": "x"}); w.Code != http.StatusUnprocessableEntity || mastodon_map(refused)["error"] != "Validation failed: You cannot report yourself" {
		t.Errorf("reporting oneself = %d %v", w.Code, refused)
	}
}

// mastodon_walk walks the signed-in account's followers or following page by
// page, by the cursor each page's Link header gives, answering the pages.
func mastodon_walk(t *testing.T, token string, which string, limit int) [][]map[string]any {
	t.Helper()
	_, me := mastodon_request(t, token, "GET", "/api/v1/accounts/verify_credentials", nil)
	path := fmt.Sprintf("/api/v1/accounts/%v/%s?limit=%d", mastodon_map(me)["id"], which, limit)
	next := regexp.MustCompile(`[?&]max_id=(\d+)[^>]*>; rel="next"`)
	var pages [][]map[string]any
	for cursor := ""; len(pages) < 1000; {
		address := path
		if cursor != "" {
			address += "&max_id=" + cursor
		}
		w, body := mastodon_request(t, token, "GET", address, nil)
		if w.Code != http.StatusOK {
			t.Fatalf("%s = %d %s", address, w.Code, w.Body.String())
		}
		pages = append(pages, mastodon_items(body))
		m := next.FindStringSubmatch(w.Header().Get("Link"))
		if m == nil {
			break
		}
		cursor = m[1]
	}
	return pages
}

// mastodon_people_add records people as following alice's account, or as
// followed by it, at the times given, in the order given.
func mastodon_people_add(alice *User, which string, actors []string, created []int64) {
	db := db_user(alice, "user")
	defer db.close()
	for i, actor := range actors {
		if which == "followers" {
			db.exec("insert into followers (account, actor, inbox, created) values ('f1', ?, ?, ?)", actor, actor+"/inbox", created[i])
		} else {
			db.exec("insert into following (account, actor, inbox, activity, accepted, created) values ('f1', ?, ?, ?, 1, ?)", actor, actor+"/inbox", actor+"/follow", created[i])
		}
	}
}

// mastodon_people_stub has resolve_fetch describe the people asked for by
// answer, which says what to do with each address; the cache starts empty and
// so does the record of lookups under way.
func mastodon_people_stub(t *testing.T, answer func(address string) error) {
	t.Helper()
	saved_fetch, saved_cache := resolve_fetch, resolve_cache
	resolve_lock.Lock()
	resolve_cache = map[string]resolve_entry{}
	resolve_lock.Unlock()
	mastodon_lookups_lock.Lock()
	mastodon_lookups = map[string]*mastodon_pending{}
	mastodon_lookups_lock.Unlock()
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		if err := answer(address); err != nil {
			return nil, err
		}
		return mastodon_people_document(address), nil
	}
	t.Cleanup(func() {
		resolve_lock.Lock()
		resolve_fetch, resolve_cache = saved_fetch, saved_cache
		resolve_lock.Unlock()
	})
}

// mastodon_people_document is the actor document a person's server gives in
// these tests: named "Described <name>", so an account their server described
// is told apart from one given by its address alone, whose name is the
// address's last segment.
func mastodon_people_document(address string) []byte {
	name := address[strings.LastIndexByte(address, '/')+1:]
	return []byte(`{"id":"` + address + `","type":"Person","preferredUsername":"` + name + `","name":"Described ` + name + `","inbox":"` + address + `/inbox"}`)
}

// TestMastodonPeopleListEachPersonOnce: walking the followers or following
// list page by page answers everyone exactly once, newest follow first, though
// a fifth of them were numbered earlier from elsewhere and seven follows share
// each second, so pages end inside a second.
func TestMastodonPeopleListEachPersonOnce(t *testing.T) {
	for _, which := range []string{"followers", "following"} {
		t.Run(which, func(t *testing.T) {
			alice, _ := create_mastodon_test_env(t)
			token := mastodon_token(t, alice, "read")
			const n = 200
			var actors []string
			var created []int64
			for i := 0; i < n; i++ {
				// Recorded out of time order, so the row ids do not follow it.
				j := i * 73 % n
				actors = append(actors, fmt.Sprintf("https://remote.example/users/u%d", j))
				created = append(created, 1_000_000+int64(j/7))
			}
			mastodon_people_add(alice, which, actors, created)
			for i, actor := range actors {
				mastodon_snapshot(alice, "account", actor, map[string]any{"id": "x", "url": actor, "acct": fmt.Sprintf("u%d@remote.example", i)})
				if i%5 == 0 {
					// Seen in a status first, from another time.
					mastodon_number(alice, "account", actor, 1_000_000+int64(i*31%n))
				}
			}
			// The expected order: newest follow first, and of follows in one
			// second the one recorded last.
			expected := make([]int, n)
			for i := range expected {
				expected[i] = i
			}
			sort.SliceStable(expected, func(a, b int) bool {
				if created[expected[a]] != created[expected[b]] {
					return created[expected[a]] > created[expected[b]]
				}
				return expected[a] > expected[b]
			})
			var got []string
			for _, page := range mastodon_walk(t, token, which, 40) {
				for _, item := range page {
					got = append(got, fmt.Sprint(item["url"]))
				}
			}
			if len(got) != n {
				t.Fatalf("THE POINT: a walk answered %d items for %d people", len(got), n)
			}
			for i, index := range expected {
				if got[i] != actors[index] {
					t.Fatalf("THE POINT: item %d is %s where %s is next by follow time", i, got[i], actors[index])
				}
			}
		})
	}
}

// TestMastodonPeoplePageReadsOnlyItsRows: a page reads and numbers only its
// own rows, not every row before the cursor.
func TestMastodonPeoplePageReadsOnlyItsRows(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	var actors []string
	var created []int64
	for i := 0; i < 200; i++ {
		actors = append(actors, fmt.Sprintf("https://remote.example/users/u%d", i))
		created = append(created, 2_000_000-int64(i))
	}
	mastodon_people_add(alice, "followers", actors, created)
	for _, actor := range actors {
		mastodon_snapshot(alice, "account", actor, map[string]any{"id": "x", "url": actor})
	}
	pages := mastodon_walk(t, token, "followers", 40)
	if len(pages) < 3 {
		t.Fatalf("a walk of 200 at 40 a page gave %d pages", len(pages))
	}
	// The third page again, with every number forgotten: only its own people
	// may be numbered again.
	_, me := mastodon_request(t, token, "GET", "/api/v1/accounts/verify_credentials", nil)
	self := fmt.Sprint(mastodon_map(me)["id"])
	w, _ := mastodon_request(t, token, "GET", "/api/v1/accounts/"+self+"/followers?limit=40", nil)
	w, _ = mastodon_request(t, token, "GET", "/api/v1/accounts/"+self+"/followers?limit=40&max_id="+regexp.MustCompile(`max_id=(\d+)`).FindStringSubmatch(w.Header().Get("Link"))[1], nil)
	cursor := regexp.MustCompile(`max_id=(\d+)`).FindStringSubmatch(w.Header().Get("Link"))[1]
	db := db_user(alice, "networks")
	defer db.close()
	db.exec("delete from snowflakes where kind='account' and key like 'https://remote.example/%'")
	w, body := mastodon_request(t, token, "GET", "/api/v1/accounts/"+self+"/followers?limit=40&max_id="+cursor, nil)
	if items := mastodon_items(body); w.Code != http.StatusOK || len(items) != 40 || items[0]["url"] != actors[80] {
		t.Fatalf("the third page = %d, %d items", w.Code, len(items))
	}
	for _, actor := range actors[:80] {
		if found, _ := db.exists("select 1 from snowflakes where kind='account' and key=?", actor); found {
			t.Fatalf("THE POINT: the third page numbered %s, on an earlier page", actor)
		}
	}
}

// TestMastodonPeopleNeverLeftOut: a follower whose lookup the limiter refuses
// is listed by their address, and not kept so, so the next page asks again
// and lists them as their server describes them.
func TestMastodonPeopleNeverLeftOut(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	var actors []string
	var created []int64
	for i := 0; i < 10; i++ {
		actors = append(actors, fmt.Sprintf("https://remote.example/users/u%d", i))
		created = append(created, 3_000_000-int64(i))
	}
	mastodon_people_add(alice, "followers", actors, created)
	var refuse atomic.Bool
	refuse.Store(true)
	mastodon_people_stub(t, func(address string) error {
		if refuse.Load() && strings.HasSuffix(address, "1") || refuse.Load() && strings.HasSuffix(address, "3") {
			return &RateLimitError{Retry: 60}
		}
		return nil
	})
	pages := mastodon_walk(t, token, "followers", 40)
	if len(pages) != 1 || len(pages[0]) != 10 {
		t.Fatalf("THE POINT: the list holds %v for 10 followers", pages)
	}
	for i, item := range pages[0] {
		refused := i == 1 || i == 3
		if item["uri"] != actors[i] || item["id"] == nil || item["id"] == "" {
			t.Errorf("item %d = %v", i, item)
		}
		if item["username"] != fmt.Sprintf("u%d", i) || item["acct"] != fmt.Sprintf("u%d@remote.example", i) {
			t.Errorf("THE POINT: item %d is named %v, %v", i, item["username"], item["acct"])
		}
		if refused && item["display_name"] != fmt.Sprintf("u%d", i) {
			t.Errorf("item %d, refused, = %v", i, item["display_name"])
		}
		if !refused && item["display_name"] != fmt.Sprintf("Described u%d", i) {
			t.Errorf("item %d, described, = %v", i, item["display_name"])
		}
		if held := mastodon_snapshot_get(alice, "account", actors[i]); (held != nil) == refused {
			t.Errorf("THE POINT: item %d, refused %v, kept %v", i, refused, held)
		}
	}
	refuse.Store(false)
	pages = mastodon_walk(t, token, "followers", 40)
	for _, i := range []int{1, 3} {
		if pages[0][i]["display_name"] != fmt.Sprintf("Described u%d", i) {
			t.Errorf("THE POINT: once their server answers, item %d = %v", i, pages[0][i]["display_name"])
		}
	}
}

// TestMastodonPeopleAskedTogether: the servers of a page's people with nothing
// kept are asked several at a time, never more than the bound.
func TestMastodonPeopleAskedTogether(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	var actors []string
	var created []int64
	for i := 0; i < 24; i++ {
		actors = append(actors, fmt.Sprintf("https://remote.example/users/u%d", i))
		created = append(created, 4_000_000-int64(i))
	}
	mastodon_people_add(alice, "followers", actors, created)
	var open, most atomic.Int64
	mastodon_people_stub(t, func(address string) error {
		now := open.Add(1)
		for {
			seen := most.Load()
			if now <= seen || most.CompareAndSwap(seen, now) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		open.Add(-1)
		return nil
	})
	pages := mastodon_walk(t, token, "followers", 40)
	if len(pages[0]) != 24 {
		t.Fatalf("the list holds %d of 24", len(pages[0]))
	}
	for i, item := range pages[0] {
		if item["display_name"] != fmt.Sprintf("Described u%d", i) {
			t.Errorf("item %d = %v", i, item["display_name"])
		}
	}
	if most.Load() < 2 {
		t.Errorf("THE POINT: the servers were asked one at a time")
	}
	if most.Load() > mastodon_people_concurrency {
		t.Errorf("THE POINT: %d servers asked at once, past the bound of %d", most.Load(), mastodon_people_concurrency)
	}
}

// TestMastodonPeopleDeadline: a page waits for its people's servers only until
// its deadline, giving the rest by address; lookups already sent run on and
// their answers serve the next page, and those not yet sent are not sent.
func TestMastodonPeopleDeadline(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	saved := mastodon_people_deadline
	mastodon_people_deadline = time.Second
	t.Cleanup(func() { mastodon_people_deadline = saved })
	n := mastodon_people_concurrency + 4
	var actors []string
	var created []int64
	for i := 0; i < n; i++ {
		actors = append(actors, fmt.Sprintf("https://remote.example/users/u%d", i))
		created = append(created, 5_000_000-int64(i))
	}
	mastodon_people_add(alice, "followers", actors, created)
	release := make(chan struct{})
	var sent, done atomic.Int64
	saved_fetch, saved_cache := resolve_fetch, resolve_cache
	resolve_lock.Lock()
	resolve_cache = map[string]resolve_entry{}
	resolve_lock.Unlock()
	mastodon_lookups_lock.Lock()
	mastodon_lookups = map[string]*mastodon_pending{}
	mastodon_lookups_lock.Unlock()
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		sent.Add(1)
		defer done.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return mastodon_people_document(address), nil
	}
	t.Cleanup(func() {
		resolve_lock.Lock()
		resolve_fetch, resolve_cache = saved_fetch, saved_cache
		resolve_lock.Unlock()
	})
	// The first page's request ends when its answer is written, as one
	// net/http serves does.
	_, me := mastodon_request(t, token, "GET", "/api/v1/accounts/verify_credentials", nil)
	ended, end := context.WithCancel(context.Background())
	request := httptest.NewRequest("GET", fmt.Sprintf("http://example.com/api/v1/accounts/%v/followers?limit=40", mastodon_map(me)["id"]), nil).WithContext(ended)
	request.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = request
	start := time.Now()
	mastodon_route(c)
	end()
	if waited := time.Since(start); waited > 4*time.Second {
		t.Errorf("THE POINT: the page waited %s for servers that did not answer", waited)
	}
	var first any
	json.Unmarshal(w.Body.Bytes(), &first)
	pages := [][]map[string]any{mastodon_items(first)}
	if len(pages[0]) != n {
		t.Fatalf("THE POINT: the list holds %d of %d", len(pages[0]), n)
	}
	for i, item := range pages[0] {
		if item["display_name"] != fmt.Sprintf("u%d", i) || item["uri"] != actors[i] {
			t.Errorf("item %d = %v", i, item)
		}
	}
	close(release)
	for wait := time.Now(); done.Load() < int64(mastodon_people_concurrency) && time.Since(wait) < 5*time.Second; {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if sent.Load() != int64(mastodon_people_concurrency) {
		t.Errorf("THE POINT: %d lookups were sent; only the %d under way when the page stopped waiting should be", sent.Load(), mastodon_people_concurrency)
	}
	pages = mastodon_walk(t, token, "followers", 40)
	for i, item := range pages[0] {
		if item["display_name"] != fmt.Sprintf("Described u%d", i) {
			t.Errorf("THE POINT: on the next page item %d = %v", i, item["display_name"])
		}
	}
	if sent.Load() != int64(n) {
		t.Errorf("THE POINT: the next page sent %d lookups in all; the answers of those under way were not kept", sent.Load())
	}
}

// TestMastodonPeopleLeaveOutBlockedHosts: people on a host the user blocks
// are left out of the list whether or not they were kept, and every page but
// the last is still full.
func TestMastodonPeopleLeaveOutBlockedHosts(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	var actors []string
	var created []int64
	for i := 0; i < 50; i++ {
		host := "remote.example"
		if i%5 == 2 {
			host = "blocked.example"
		}
		actors = append(actors, fmt.Sprintf("https://%s/users/u%d", host, i))
		created = append(created, 6_000_000-int64(i))
	}
	mastodon_people_add(alice, "followers", actors, created)
	for _, actor := range actors {
		mastodon_snapshot(alice, "account", actor, map[string]any{"id": "x", "url": actor})
	}
	db_user_brief(alice, "user").exec("insert into blocks (domain, created) values ('blocked.example', 1)")
	pages := mastodon_walk(t, token, "followers", 20)
	total := 0
	for p, page := range pages {
		for _, item := range page {
			if strings.Contains(fmt.Sprint(item["url"]), "blocked.example") {
				t.Errorf("THE POINT: %v, on a blocked host, is listed", item["url"])
			}
		}
		if p < len(pages)-1 && len(page) != 20 {
			t.Errorf("THE POINT: page %d holds %d of 20", p, len(page))
		}
		total += len(page)
	}
	if total != 40 {
		t.Errorf("THE POINT: the walk listed %d of the 40 people on allowed hosts", total)
	}
}

// TestMastodonFollowKeptForTheLists: someone who follows a hosted account, or
// whom it follows, is kept as clients would see them when the follow is
// recorded, for a user a Mastodon app is signed in for, so the lists ask no
// server; for a user with no Mastodon app nothing is kept.
func TestMastodonFollowKeptForTheLists(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	carol := "https://remote.example/users/carol"
	dave := "https://remote.example/users/dave"
	erin := "https://remote.example/users/erin"
	var asked atomic.Int64
	mastodon_people_stub(t, func(address string) error {
		asked.Add(1)
		return nil
	})
	followed := func(person string) {
		t.Helper()
		activity := map[string]any{"id": person + "/follows/1", "type": "Follow", "actor": person, "object": "https://example.com/~f1"}
		if err := activitypub_dispatch(context.Background(), alice, actor_get("f1"), person, "Follow", activity, []byte(json_encode(activity))); err != nil {
			t.Fatal(err)
		}
	}
	follows := func(person string) {
		t.Helper()
		r, err := author_resolve(context.Background(), person)
		if err != nil {
			t.Fatal(err)
		}
		account_follow(alice, "f1", "https://example.com/~f1", r)
	}
	// Dave follows and is followed before any Mastodon app is signed in.
	followed(dave)
	follows(dave)
	if held := mastodon_snapshot_get(alice, "account", dave); held != nil {
		t.Errorf("THE POINT: kept for a user with no Mastodon app: %v", held)
	}
	token := mastodon_token(t, alice, "read")
	followed(carol)
	follows(erin)
	for _, person := range []string{carol, erin} {
		name := person[strings.LastIndexByte(person, '/')+1:]
		if held := mastodon_snapshot_get(alice, "account", person); held == nil || held["username"] != name || held["acct"] != name+"@remote.example" || held["uri"] != person {
			t.Errorf("THE POINT: %s as kept = %v", name, held)
		}
	}
	resolve_lock.Lock()
	resolve_cache = map[string]resolve_entry{}
	resolve_lock.Unlock()
	asked.Store(0)
	for which, person := range map[string]string{"followers": carol, "following": erin} {
		found := false
		for _, item := range mastodon_walk(t, token, which, 40)[0] {
			found = found || item["uri"] == person
		}
		if !found {
			t.Errorf("%s does not list %s", which, person)
		}
	}
	// Only dave is asked about, once: the first list keeps him for the other.
	if asked.Load() != 1 {
		t.Errorf("THE POINT: the lists asked %d times; only dave's server, once, should be", asked.Load())
	}
}

// TestMastodonPeopleLookupPanic: a lookup that panics over what a server sent
// leaves that person given by their address, and the page and the server
// answer on.
func TestMastodonPeopleLookupPanic(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	actors := []string{"https://remote.example/users/u0", "https://remote.example/users/u1"}
	mastodon_people_add(alice, "followers", actors, []int64{7_000_001, 7_000_000})
	mastodon_people_stub(t, func(address string) error {
		if strings.HasSuffix(address, "u0") {
			panic("a hostile document")
		}
		return nil
	})
	pages := mastodon_walk(t, token, "followers", 40)
	if len(pages[0]) != 2 || pages[0][0]["display_name"] != "u0" || pages[0][1]["display_name"] != "Described u1" {
		t.Fatalf("THE POINT: the page = %v", pages[0])
	}
}

// mastodon_people_blocking has resolve_fetch hold every lookup until release
// is called or the lookup's context ends, counting those sent and those under
// way. At the test's end it releases them and waits for every lookup under way
// to finish, so none outlives the test into the next.
func mastodon_people_blocking(t *testing.T) (release func(), sent *atomic.Int64, open *atomic.Int64) {
	t.Helper()
	released := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(released) }) }
	sent, open = &atomic.Int64{}, &atomic.Int64{}
	mastodon_people_stub(t, func(address string) error { return nil })
	stubbed := resolve_fetch
	resolve_fetch = func(ctx context.Context, address string, accept string) ([]byte, error) {
		sent.Add(1)
		open.Add(1)
		defer open.Add(-1)
		select {
		case <-released:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return stubbed(ctx, address, accept)
	}
	t.Cleanup(func() {
		release()
		for wait := time.Now(); time.Since(wait) < 5*time.Second; time.Sleep(10 * time.Millisecond) {
			mastodon_lookups_lock.Lock()
			left := len(mastodon_lookups)
			mastodon_lookups_lock.Unlock()
			if left == 0 && open.Load() == 0 {
				return
			}
		}
		t.Error("lookups were still under way at the test's end")
	})
	return
}

// mastodon_people_followers records n followers at remote.example with
// nothing kept of them, newest first.
func mastodon_people_followers(alice *User, n int, base int64) []string {
	var actors []string
	var created []int64
	for i := 0; i < n; i++ {
		actors = append(actors, fmt.Sprintf("https://remote.example/users/u%d", i))
		created = append(created, base-int64(i))
	}
	mastodon_people_add(alice, "followers", actors, created)
	return actors
}

// TestMastodonPeopleLookupsShared: a page asking for people whose lookups an
// earlier page left under way waits for those, and sends none of its own.
func TestMastodonPeopleLookupsShared(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	saved := mastodon_people_deadline
	mastodon_people_deadline = 300 * time.Millisecond
	t.Cleanup(func() { mastodon_people_deadline = saved })
	mastodon_people_followers(alice, mastodon_people_concurrency, 8_000_000)
	_, sent, _ := mastodon_people_blocking(t)
	mastodon_walk(t, token, "followers", 40)
	first := sent.Load()
	mastodon_walk(t, token, "followers", 40)
	mastodon_walk(t, token, "followers", 40)
	if first != int64(mastodon_people_concurrency) || sent.Load() != first {
		t.Errorf("THE POINT: three pages sent %d lookups for %d people, the first %d", sent.Load(), mastodon_people_concurrency, first)
	}
}

// TestMastodonPeopleLookupsBounded: with the server's bound of lookups under
// way reached, a page sends no more, and gives the rest by their address.
func TestMastodonPeopleLookupsBounded(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	saved_deadline, saved_maximum := mastodon_people_deadline, mastodon_lookups_maximum
	mastodon_people_deadline, mastodon_lookups_maximum = 300*time.Millisecond, 3
	t.Cleanup(func() { mastodon_people_deadline, mastodon_lookups_maximum = saved_deadline, saved_maximum })
	actors := mastodon_people_followers(alice, 8, 8_100_000)
	_, sent, _ := mastodon_people_blocking(t)
	pages := mastodon_walk(t, token, "followers", 40)
	if len(pages[0]) != len(actors) {
		t.Fatalf("the list holds %d of %d", len(pages[0]), len(actors))
	}
	if sent.Load() != 3 {
		t.Errorf("THE POINT: %d lookups sent with a bound of 3", sent.Load())
	}
}

// TestMastodonPeopleLookupsEnd: a lookup the page stopped waiting for ends at
// its own timeout when the server never answers, and leaves nothing under
// way.
func TestMastodonPeopleLookupsEnd(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	saved_deadline, saved_timeout := mastodon_people_deadline, mastodon_lookup_timeout
	mastodon_people_deadline, mastodon_lookup_timeout = 100*time.Millisecond, 400*time.Millisecond
	t.Cleanup(func() { mastodon_people_deadline, mastodon_lookup_timeout = saved_deadline, saved_timeout })
	mastodon_people_followers(alice, 4, 8_200_000)
	_, sent, open := mastodon_people_blocking(t)
	mastodon_walk(t, token, "followers", 40)
	if sent.Load() != 4 {
		t.Fatalf("%d lookups sent for 4 people", sent.Load())
	}
	for wait := time.Now(); open.Load() > 0 && time.Since(wait) < 5*time.Second; {
		time.Sleep(10 * time.Millisecond)
	}
	mastodon_lookups_lock.Lock()
	left := len(mastodon_lookups)
	mastodon_lookups_lock.Unlock()
	if open.Load() != 0 || left != 0 {
		t.Errorf("THE POINT: %d lookups still waiting on a silent server, %d recorded as under way, after their timeout", open.Load(), left)
	}
}

// TestMastodonPeopleQueryUsesTheIndex: a page of the followers or following
// list reads a range of the index on when each follow was recorded, sorting
// nothing, so its cost does not grow with the list.
func TestMastodonPeopleQueryUsesTheIndex(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	db := db_user(alice, "user")
	defer db.close()
	for _, table := range []string{"followers", "following"} {
		for _, bounded := range []bool{false, true} {
			args := []any{"f1", 40}
			want := table + " USING INDEX " + table + "_created (account=?)"
			if bounded {
				args = []any{"f1", 100, 100, 5, 40}
				want = table + " USING INDEX " + table + "_created (account=? AND created<?)"
			}
			rows, err := db.rows("explain query plan "+mastodon_people_query(table, bounded), args...)
			if err != nil {
				t.Fatal(err)
			}
			var plan []string
			for _, row := range rows {
				plan = append(plan, row_string(row, "detail"))
			}
			joined := strings.Join(plan, "\n")
			if !strings.Contains(joined, want) || strings.Contains(joined, "TEMP B-TREE") {
				t.Errorf("THE POINT: %s, bounded %v, is not read as a range of its index:\n%s", table, bounded, joined)
			}
		}
	}
}

// TestMastodonPeopleAcrossABlockedRun: a page that meets a run of people on a
// blocked host longer than a page reads past it to a full page, and the next
// page goes on from where the first ended.
func TestMastodonPeopleAcrossABlockedRun(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	var actors []string
	var created []int64
	for i := 0; i < 300; i++ {
		host := "remote.example"
		// The run ends inside a batch, so the page fills part-way through one.
		if i >= 15 && i < 211 {
			host = "blocked.example"
		}
		actors = append(actors, fmt.Sprintf("https://%s/users/u%d", host, i))
		created = append(created, 9_000_000-int64(i))
	}
	mastodon_people_add(alice, "followers", actors, created)
	for _, actor := range actors {
		mastodon_snapshot(alice, "account", actor, map[string]any{"id": "x", "url": actor})
	}
	db_user_brief(alice, "user").exec("insert into blocks (domain, created) values ('blocked.example', 1)")
	var got []string
	pages := mastodon_walk(t, token, "followers", 40)
	for p, page := range pages {
		if p < len(pages)-1 && len(page) != 40 {
			t.Errorf("THE POINT: page %d holds %d of 40", p, len(page))
		}
		for _, item := range page {
			got = append(got, fmt.Sprint(item["url"]))
		}
	}
	var want []string
	for _, actor := range actors {
		if !strings.Contains(actor, "blocked.example") {
			want = append(want, actor)
		}
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("THE POINT: the walk listed %d people, %d expected, or out of order", len(got), len(want))
	}
}

// TestMastodonKeepNotTheAccountItself: a follow of the account's own address
// keeps nothing over the account's own form.
func TestMastodonKeepNotTheAccountItself(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	token := mastodon_token(t, alice, "read")
	_, me := mastodon_request(t, token, "GET", "/api/v1/accounts/verify_credentials", nil)
	own := account_url("example.com", "f1")
	before := mastodon_snapshot_get(alice, "account", own)
	if before == nil || before["username"] != "alice" {
		t.Fatalf("the account's own form = %v (%v)", before, me)
	}
	mastodon_keep(alice, actor_get("f1"), &resolved{Network: "fediverse", ID: own, Host: "example.com"})
	if after := mastodon_snapshot_get(alice, "account", own); after == nil || after["username"] != "alice" || after["acct"] != "alice" {
		t.Errorf("THE POINT: the account's own form, after a follow of itself, = %v", after)
	}
}

// TestMastodonAddressName: someone given by their address alone is named by
// its last segment where that is a name, and not otherwise.
func TestMastodonAddressName(t *testing.T) {
	for address, want := range map[string]string{
		"https://mastodon.example/users/alice":    "alice",
		"https://lemmy.example/u/bob":             "bob",
		"https://peertube.example/accounts/carol": "carol",
		"https://akkoma.example/@dave":            "dave",
		"https://pleroma.example/users/e.f-g_h":   "e.f-g_h",
		"https://misskey.example/users/9abc123":   "9abc123",
		"https://remote.example/":                 "",
		"https://remote.example/users/a%20b":      "",
		"https://remote.example/users/":           "",
		"not an address":                          "",
	} {
		if got := mastodon_address_name(address); got != want {
			t.Errorf("THE POINT: %s is named %q, want %q", address, got, want)
		}
	}
}

// TestMastodonForgetOnUndo: a follower who undoes their follow, or is deleted,
// leaves nothing kept of them, unless an account of the user's still follows
// them.
func TestMastodonForgetOnUndo(t *testing.T) {
	alice, _ := create_mastodon_test_env(t)
	mastodon_token(t, alice, "read")
	mastodon_people_stub(t, func(address string) error { return nil })
	undo := func(person string) {
		t.Helper()
		follow := map[string]any{"id": person + "/follows/1", "type": "Follow", "actor": person, "object": "https://example.com/~f1"}
		if err := activitypub_dispatch(context.Background(), alice, actor_get("f1"), person, "Follow", follow, []byte(json_encode(follow))); err != nil {
			t.Fatal(err)
		}
		activity := map[string]any{"id": person + "/undo/1", "type": "Undo", "actor": person, "object": follow}
		if err := activitypub_dispatch(context.Background(), alice, actor_get("f1"), person, "Undo", activity, []byte(json_encode(activity))); err != nil {
			t.Fatal(err)
		}
	}
	gone := "https://remote.example/users/gone"
	undo(gone)
	if held := mastodon_snapshot_get(alice, "account", gone); held != nil {
		t.Errorf("THE POINT: an undone follower is still kept: %v", held)
	}
	// Followed by the account as well: kept.
	followed := "https://remote.example/users/followed"
	r, err := author_resolve(context.Background(), followed)
	if err != nil {
		t.Fatal(err)
	}
	account_follow(alice, "f1", "https://example.com/~f1", r)
	undo(followed)
	if mastodon_snapshot_get(alice, "account", followed) == nil {
		t.Error("THE POINT: someone the account follows was forgotten when they stopped following it")
	}
	// Deleted: forgotten with the follow.
	deleted := "https://remote.example/users/deleted"
	follow := map[string]any{"id": deleted + "/follows/1", "type": "Follow", "actor": deleted, "object": "https://example.com/~f1"}
	activitypub_dispatch(context.Background(), alice, actor_get("f1"), deleted, "Follow", follow, []byte(json_encode(follow)))
	activitypub_forget(alice, "f1", deleted)
	if held := mastodon_snapshot_get(alice, "account", deleted); held != nil {
		t.Errorf("THE POINT: a deleted follower is still kept: %v", held)
	}
}
