// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// A lemmy_forums fake stands in for the forums app's lemmy/* functions: one
// community, the posts and comments given, and what it was asked.
type lemmy_forums struct {
	lock     sync.Mutex
	calls    []string
	args     []Map
	posts    []map[string]any
	comments []map[string]any
	replies  []string
}

func lemmy_fake_community() map[string]any {
	return map[string]any{"id": "forum1", "name": "books", "title": "Books", "description": "About books", "url": "", "local": true,
		"subscribed": "yes", "members": int64(3), "posts": int64(1), "comments": int64(2), "moderator": true,
		"moderators": []any{map[string]any{"id": "e-alice", "name": "alice", "display": "Alice", "mine": true}}}
}

func (f *lemmy_forums) call(s *lemmy_session, a *App, function string, args Map) (any, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.calls = append(f.calls, function)
	f.args = append(f.args, args)
	text := func(key string) string { v, _ := args[key].(string); return v }
	post := func(id string) map[string]any {
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
				return c
			}
		}
		return nil
	}
	switch function {
	case "lemmy/communities":
		return []any{lemmy_fake_community()}, nil
	case "lemmy/community", "lemmy/follow", "lemmy/moderator":
		if text("community") != "forum1" && text("name") != "books" {
			return map[string]any{"error": "couldnt_find_community"}, nil
		}
		community := lemmy_fake_community()
		if function == "lemmy/follow" && args["follow"] == false {
			community["subscribed"] = "no"
		}
		return community, nil
	case "lemmy/posts":
		out := []any{}
		for _, p := range f.posts {
			out = append(out, p)
		}
		return out, nil
	case "lemmy/post", "lemmy/post/vote", "lemmy/post/save", "lemmy/post/edit", "lemmy/post/lock":
		p := post(text("post"))
		if p == nil {
			return map[string]any{"error": "couldnt_find_post"}, nil
		}
		switch function {
		case "lemmy/post/vote":
			score, _ := args["score"].(int64)
			p["vote"] = score
		case "lemmy/post/save":
			p["saved"] = args["save"]
		case "lemmy/post/edit":
			if name, set := args["name"]; set {
				p["title"] = name
			}
		}
		return p, nil
	case "lemmy/post/create":
		p := map[string]any{"id": "p" + strconv.Itoa(len(f.posts)+1), "community": lemmy_fake_community(), "creator": map[string]any{"id": "e-alice", "mine": true}, "title": text("name"), "body": text("body"), "url": text("url"), "created": now(), "up": int64(1), "down": int64(0), "vote": int64(1)}
		f.posts = append(f.posts, p)
		return p, nil
	case "lemmy/comments":
		out := []any{}
		for _, c := range f.comments {
			if text("post") == "" || mastodon_string(c["post"].(map[string]any), "id") == text("post") {
				out = append(out, c)
			}
		}
		return out, nil
	case "lemmy/comment", "lemmy/comment/vote":
		c := comment(text("comment"))
		if c == nil {
			return map[string]any{"error": "couldnt_find_comment"}, nil
		}
		return c, nil
	case "lemmy/comment/create":
		p := post(text("post"))
		c := map[string]any{"id": "c" + strconv.Itoa(len(f.comments)+1), "post": p, "creator": map[string]any{"id": "e-alice", "mine": true}, "body": text("content"), "parent": text("parent"), "created": now()}
		if parent := text("parent"); parent != "" {
			c["path"] = append(append([]any{}, comment(parent)["path"].([]any)...), parent)
		} else {
			c["path"] = []any{}
		}
		f.comments = append(f.comments, c)
		return c, nil
	case "lemmy/replies":
		out := []any{}
		for _, id := range f.replies {
			out = append(out, comment(id))
		}
		return out, nil
	case "lemmy/mentions", "lemmy/reports", "lemmy/modlog":
		return []any{}, nil
	case "lemmy/post/report":
		return map[string]any{"id": "r1", "kind": "post", "reason": text("reason"), "post": post(text("post")), "reporter": map[string]any{"id": "e-alice", "mine": true}, "created": now()}, nil
	case "lemmy/search":
		return map[string]any{"posts": []any{f.posts[0]}, "comments": []any{}, "communities": []any{lemmy_fake_community()}}, nil
	case "lemmy/resolve":
		if text("query") == "!books@example.com" {
			return map[string]any{"community": lemmy_fake_community()}, nil
		}
		return map[string]any{"error": "couldnt_find_object"}, nil
	}
	return nil, fmt.Errorf("the fake has no %s", function)
}

// sent is what the last call of a function was given.
func (f *lemmy_forums) sent(function string) Map {
	f.lock.Lock()
	defer f.lock.Unlock()
	for i := len(f.calls) - 1; i >= 0; i-- {
		if f.calls[i] == function {
			return f.args[i]
		}
	}
	return nil
}

// create_lemmy_test_env is the Mastodon test environment - alice's hosted
// fediverse account f1, the sign-in tables and chat's fake - with the
// forums app's lemmy functions faked, one post of bob's and a reply to it,
// and a password minted for a Lemmy app.
func create_lemmy_test_env(t *testing.T) (*User, *lemmy_forums, *mastodon_feeds, string) {
	t.Helper()
	alice, chat := create_mastodon_test_env(t)
	bob := map[string]any{"id": "https://remote.example/users/bob", "name": "bob", "display": "Bob", "url": "https://remote.example/users/bob"}
	post := map[string]any{"id": "p1", "community": lemmy_fake_community(), "creator": bob, "title": "A book", "body": "Read it", "url": "", "created": now() - 100, "up": int64(5), "down": int64(2), "comments": int64(1), "vote": int64(0)}
	first := map[string]any{"id": "c1", "post": post, "creator": map[string]any{"id": "e-alice", "mine": true}, "body": "I will", "parent": "", "path": []any{}, "created": now() - 50}
	answer := map[string]any{"id": "c2", "post": post, "creator": bob, "body": "Good", "parent": "c1", "path": []any{"c1"}, "created": now() - 10}
	fake := &lemmy_forums{posts: []map[string]any{post}, comments: []map[string]any{first, answer}, replies: []string{"c2"}}
	functions := map[string]AppFunction{}
	for _, name := range []string{"communities", "community", "follow", "block", "posts", "post", "post/create", "post/edit", "post/delete", "post/remove", "post/lock", "post/feature", "post/vote", "post/save", "comments", "comment", "comment/create", "comment/edit", "comment/delete", "comment/remove", "comment/vote", "comment/save", "post/report", "comment/report", "reports", "report/resolve", "modlog", "ban", "moderator", "search", "resolve", "person", "replies", "mentions"} {
		functions["lemmy/"+name] = AppFunction{Function: "function_lemmy_" + strings.ReplaceAll(name, "/", "_")}
	}
	apps_lock.Lock()
	apps["forums1"] = &App{id: "forums1", internal: &AppVersion{Functions: functions}}
	apps_lock.Unlock()
	saved := lemmy_caller
	lemmy_caller = func(s *lemmy_session, a *App, function string, args Map) (any, error) {
		if strings.HasPrefix(function, "direct/") {
			return chat.call(nil, a, function, args)
		}
		return fake.call(s, a, function, args)
	}
	t.Cleanup(func() {
		lemmy_caller = saved
		apps_lock.Lock()
		delete(apps, "forums1")
		apps_lock.Unlock()
	})
	return alice, fake, chat, password_mint(alice, "", "f1", "fediverse", "Phone")
}

// lemmy_client_request sends one request to the interface as example.com, with a
// token where one is given, answering the recorder and the decoded body.
func lemmy_client_request(t *testing.T, token string, method string, path string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var reader io.Reader = http.NoBody
	kind := ""
	switch b := body.(type) {
	case nil:
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
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = request
	if !lemmy_route(c) {
		t.Fatalf("%s %s was not answered", method, path)
	}
	var answer map[string]any
	json.Unmarshal(w.Body.Bytes(), &answer)
	return w, answer
}

// lemmy_client_login signs a Lemmy app in as alice, answering its token.
func lemmy_client_login(t *testing.T, password string) string {
	t.Helper()
	w, answer := lemmy_client_request(t, "", "POST", "/api/v3/user/login", map[string]any{"username_or_email": "alice", "password": password})
	jwt, _ := answer["jwt"].(string)
	if w.Code != http.StatusOK || jwt == "" {
		t.Fatalf("login = %d %v", w.Code, answer)
	}
	return jwt
}

// TestLemmySession: a Lemmy app signs in with the account's handle and a
// minted password and is given a JWT naming the account's person by its
// number; the site names the account for a signed-in app alone; a wrong
// password, registration and administration are refused; logging out ends
// the token.
func TestLemmySession(t *testing.T) {
	alice, _, _, password := create_lemmy_test_env(t)
	if w, answer := lemmy_client_request(t, "", "POST", "/api/v3/user/login", map[string]any{"username_or_email": "alice", "password": "mochi-wrong"}); w.Code != http.StatusBadRequest || answer["error"] != "incorrect_login" {
		t.Errorf("a wrong password = %d %v", w.Code, answer)
	}
	jwt := lemmy_client_login(t, password)
	parts := strings.Split(jwt, ".")
	data, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims map[string]any
	json.Unmarshal(data, &claims)
	self := number_for(alice, "person", account_url("example.com", "f1"))
	if claims["sub"] != strconv.FormatInt(self, 10) {
		t.Errorf("THE POINT: the JWT's subject = %v, want %d", claims["sub"], self)
	}
	if _, site := lemmy_client_request(t, "", "GET", "/api/v3/site", nil); site["my_user"] != nil || site["version"] != lemmy_version {
		t.Errorf("the site anonymously = %v", site)
	}
	_, site := lemmy_client_request(t, jwt, "GET", "/api/v3/site", nil)
	person := mastodon_map(mastodon_map(mastodon_map(site["my_user"])["local_user_view"])["person"])
	if person["name"] != "alice" || person["id"] != float64(self) {
		t.Fatalf("THE POINT: the site's user = %v", site["my_user"])
	}
	if follows := mastodon_items(mastodon_map(site["my_user"])["follows"]); len(follows) != 1 || mastodon_map(follows[0]["community"])["name"] != "books" {
		t.Errorf("follows = %v", mastodon_map(site["my_user"])["follows"])
	}
	if w, answer := lemmy_client_request(t, jwt, "GET", "/api/v3/user/validate_auth", nil); w.Code != http.StatusOK || answer["success"] != true {
		t.Errorf("validate_auth = %d %v", w.Code, answer)
	}
	if _, answer := lemmy_client_request(t, "", "POST", "/api/v3/user/register", map[string]any{"username": "x"}); answer["error"] != "registration_closed" {
		t.Errorf("registration = %v", answer)
	}
	if _, answer := lemmy_client_request(t, jwt, "POST", "/api/v3/admin/add", nil); answer["error"] != "not_an_admin" {
		t.Errorf("administration = %v", answer)
	}
	if w, answer := lemmy_client_request(t, "", "GET", "/api/v3/post/list", nil); w.Code != http.StatusUnauthorized || answer["error"] != "not_logged_in" {
		t.Errorf("an anonymous listing = %d %v", w.Code, answer)
	}
	lemmy_client_request(t, jwt, "POST", "/api/v3/user/logout", nil)
	if w, _ := lemmy_client_request(t, jwt, "GET", "/api/v3/user/validate_auth", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("THE POINT: the token outlived logging out: %d", w.Code)
	}
}

// TestLemmyItems: communities, posts and comments come from forums with
// numbers the app keeps, the same each time; the numbers the app sends back
// reach forums as its own ids; counts, votes and paths are Lemmy's; an
// unknown number is refused before forums is asked.
func TestLemmyItems(t *testing.T) {
	alice, fake, _, password := create_lemmy_test_env(t)
	jwt := lemmy_client_login(t, password)
	_, listed := lemmy_client_request(t, jwt, "GET", "/api/v3/community/list?type_=Local", nil)
	communities := mastodon_items(listed["communities"])
	if len(communities) != 1 || communities[0]["subscribed"] != "Subscribed" {
		t.Fatalf("communities = %v", listed)
	}
	community := mastodon_map(communities[0]["community"])
	if community["actor_id"] != "https://example.com/c/books" || community["id"] != float64(number_for(alice, "community", "forum1")) {
		t.Errorf("THE POINT: the community = %v", community)
	}
	cid := fmt.Sprint(community["id"])
	if _, again := lemmy_client_request(t, jwt, "GET", "/api/v3/community?id="+cid, nil); mastodon_map(mastodon_map(again["community_view"])["community"])["id"] != community["id"] || len(mastodon_items(again["moderators"])) != 1 {
		t.Errorf("the community by its number = %v", again)
	}
	if w, answer := lemmy_client_request(t, jwt, "GET", "/api/v3/community?id=999999", nil); w.Code != http.StatusNotFound || answer["error"] != "couldnt_find_community" {
		t.Errorf("an unknown community = %d %v", w.Code, answer)
	}

	_, posts := lemmy_client_request(t, jwt, "GET", "/api/v3/post/list?community_id="+cid+"&limit=1", nil)
	views := mastodon_items(posts["posts"])
	if len(views) != 1 || posts["next_page"] != "Pa2" {
		t.Fatalf("posts = %v", posts)
	}
	if fake.sent("lemmy/posts")["community"] != "forum1" {
		t.Errorf("THE POINT: forums was asked for %v", fake.sent("lemmy/posts"))
	}
	counts := mastodon_map(views[0]["counts"])
	if counts["score"] != float64(3) || counts["upvotes"] != float64(5) || views[0]["my_vote"] != nil {
		t.Errorf("counts = %v, my_vote %v", counts, views[0]["my_vote"])
	}
	if creator := mastodon_map(views[0]["creator"]); creator["actor_id"] != "https://remote.example/users/bob" || creator["name"] != "bob" {
		t.Errorf("the creator = %v", creator)
	}
	pid := fmt.Sprint(mastodon_map(views[0]["post"])["id"])
	if _, voted := lemmy_client_request(t, jwt, "POST", "/api/v3/post/like", map[string]any{"post_id": json.Number(pid), "score": 1}); mastodon_map(voted["post_view"])["my_vote"] != float64(1) {
		t.Errorf("the vote = %v", voted)
	}
	if fake.sent("lemmy/post/vote")["post"] != "p1" || fake.sent("lemmy/post/vote")["score"] != int64(1) {
		t.Errorf("THE POINT: the vote reached forums as %v", fake.sent("lemmy/post/vote"))
	}
	if w, answer := lemmy_client_request(t, jwt, "POST", "/api/v3/post/like", map[string]any{"post_id": 999999, "score": 1}); w.Code != http.StatusNotFound || answer["error"] != "couldnt_find_post" {
		t.Errorf("an unknown post = %d %v", w.Code, answer)
	}
	before := len(fake.calls)
	lemmy_client_request(t, jwt, "POST", "/api/v3/post/like", map[string]any{"post_id": 999999, "score": 1})
	if len(fake.calls) != before {
		t.Error("forums was asked about a number it never gave")
	}

	_, made := lemmy_client_request(t, jwt, "POST", "/api/v3/post", map[string]any{"community_id": json.Number(cid), "name": "New one", "body": "Text", "language_id": 1})
	if mastodon_map(mastodon_map(made["post_view"])["post"])["name"] != "New one" || fake.sent("lemmy/post/create")["community"] != "forum1" || fake.sent("lemmy/post/create")["language"] != "en" {
		t.Errorf("THE POINT: the new post = %v, sent %v", made, fake.sent("lemmy/post/create"))
	}

	_, comments := lemmy_client_request(t, jwt, "GET", "/api/v3/comment/list?post_id="+pid, nil)
	list := mastodon_items(comments["comments"])
	if len(list) != 2 {
		t.Fatalf("comments = %v", comments)
	}
	first := mastodon_map(list[0]["comment"])
	second := mastodon_map(list[1]["comment"])
	if second["path"] != "0."+fmt.Sprint(first["id"])+"."+fmt.Sprint(second["id"]) {
		t.Errorf("THE POINT: the answer's path = %v", second["path"])
	}
	if _, answered := lemmy_client_request(t, jwt, "POST", "/api/v3/comment", map[string]any{"post_id": json.Number(pid), "parent_id": json.Number(fmt.Sprint(second["id"])), "content": "Thanks"}); answered["comment_view"] == nil || fake.sent("lemmy/comment/create")["parent"] != "c2" {
		t.Errorf("THE POINT: the reply reached forums as %v", fake.sent("lemmy/comment/create"))
	}
	if _, found := lemmy_client_request(t, jwt, "GET", "/api/v3/post?comment_id="+fmt.Sprint(first["id"]), nil); mastodon_map(mastodon_map(found["post_view"])["post"])["name"] != "A book" {
		t.Errorf("a post by its comment = %v", found)
	}
	if _, resolved := lemmy_client_request(t, jwt, "GET", "/api/v3/resolve_object?q=%21books%40example.com", nil); resolved["community"] == nil {
		t.Errorf("resolving the community = %v", resolved)
	}
	if _, searched := lemmy_client_request(t, jwt, "GET", "/api/v3/search?q=book", nil); len(mastodon_items(searched["posts"])) != 1 || len(mastodon_items(searched["communities"])) != 1 {
		t.Errorf("search = %v", searched)
	}
	if _, reported := lemmy_client_request(t, jwt, "POST", "/api/v3/post/report", map[string]any{"post_id": json.Number(pid), "reason": "Spam"}); mastodon_map(mastodon_map(reported["post_report_view"])["post_report"])["reason"] != "Spam" {
		t.Errorf("the report = %v", reported)
	}
	if _, log := lemmy_client_request(t, jwt, "GET", "/api/v3/modlog", nil); log["removed_posts"] == nil || log["banned_from_community"] == nil {
		t.Errorf("the modlog = %v", log)
	}
}

// TestLemmyInbox: replies are unread until marked, one at a time or all
// together, and the unread count follows; private messages are the
// account's conversations in chat, a new one sent as Lemmy's.
func TestLemmyInbox(t *testing.T) {
	_, _, chat, password := create_lemmy_test_env(t)
	jwt := lemmy_client_login(t, password)
	_, counted := lemmy_client_request(t, jwt, "GET", "/api/v3/user/unread_count", nil)
	if counted["replies"] != float64(1) {
		t.Fatalf("unread = %v", counted)
	}
	_, replies := lemmy_client_request(t, jwt, "GET", "/api/v3/user/replies?unread_only=true", nil)
	items := mastodon_items(replies["replies"])
	if len(items) != 1 || mastodon_map(items[0]["comment_reply"])["read"] != false {
		t.Fatalf("replies = %v", replies)
	}
	reply := fmt.Sprint(mastodon_map(items[0]["comment_reply"])["id"])
	lemmy_client_request(t, jwt, "POST", "/api/v3/comment/mark_as_read", map[string]any{"comment_reply_id": json.Number(reply), "read": true})
	if _, counted := lemmy_client_request(t, jwt, "GET", "/api/v3/user/unread_count", nil); counted["replies"] != float64(0) {
		t.Errorf("THE POINT: unread after marking the reply = %v", counted)
	}
	if _, replies := lemmy_client_request(t, jwt, "GET", "/api/v3/user/replies?unread_only=true", nil); len(mastodon_items(replies["replies"])) != 0 {
		t.Errorf("unread replies after marking = %v", replies)
	}
	lemmy_client_request(t, jwt, "POST", "/api/v3/comment/mark_as_read", map[string]any{"comment_reply_id": json.Number(reply), "read": false})
	if _, counted := lemmy_client_request(t, jwt, "GET", "/api/v3/user/unread_count", nil); counted["replies"] != float64(1) {
		t.Errorf("unread after marking it unread = %v", counted)
	}
	lemmy_client_request(t, jwt, "POST", "/api/v3/user/mark_all_as_read", nil)
	if _, counted := lemmy_client_request(t, jwt, "GET", "/api/v3/user/unread_count", nil); counted["replies"] != float64(0) {
		t.Errorf("unread after marking all = %v", counted)
	}

	// A private message to bob, on a Mastodon server, by the number his post
	// gave him.
	lemmy_softwares_lock.Lock()
	lemmy_softwares["remote.example"] = struct {
		name    string
		expires time.Time
	}{"mastodon", time.Now().Add(time.Hour)}
	lemmy_softwares_lock.Unlock()
	t.Cleanup(func() {
		lemmy_softwares_lock.Lock()
		delete(lemmy_softwares, "remote.example")
		lemmy_softwares_lock.Unlock()
	})
	_, posts := lemmy_client_request(t, jwt, "GET", "/api/v3/post/list", nil)
	bob := fmt.Sprint(mastodon_map(mastodon_items(posts["posts"])[0]["creator"])["id"])
	w, sent := lemmy_client_request(t, jwt, "POST", "/api/v3/private_message", map[string]any{"recipient_id": json.Number(bob), "content": "Hello Bob"})
	message := mastodon_map(mastodon_map(sent["private_message_view"])["private_message"])
	if w.Code != http.StatusOK || message["content"] != "Hello Bob" || fmt.Sprint(message["recipient_id"]) != bob {
		t.Fatalf("THE POINT: the private message = %d %v", w.Code, sent)
	}
	if args := chat.sent("direct/send"); args["kind"] != "" || len(args["people"].([]any)) != 1 || args["people"].([]any)[0] != "https://remote.example/users/bob" {
		t.Errorf("THE POINT: sent through chat as %v", args)
	}
	// To someone on a Lemmy server, it is Lemmy's ChatMessage.
	lemmy_softwares_lock.Lock()
	lemmy_softwares["remote.example"] = struct {
		name    string
		expires time.Time
	}{"lemmy", time.Now().Add(time.Hour)}
	lemmy_softwares_lock.Unlock()
	lemmy_client_request(t, jwt, "POST", "/api/v3/private_message", map[string]any{"recipient_id": json.Number(bob), "content": "On Lemmy"})
	if args := chat.sent("direct/send"); args["kind"] != "ChatMessage" {
		t.Errorf("THE POINT: to a Lemmy server sent as %v", args)
	}
	chat.lock.Lock()
	chat.messages = append(chat.messages, map[string]any{"id": "mbob", "chat": "chat1", "member": "https://remote.example/users/bob", "name": "Bob", "body": "Hi Alice", "created": now() - 5, "edited": int64(0), "remote": "https://remote.example/m/1", "mine": false})
	chat.lock.Unlock()
	if _, counted := lemmy_client_request(t, jwt, "GET", "/api/v3/user/unread_count", nil); counted["private_messages"] != float64(1) {
		t.Errorf("unread messages = %v", counted)
	}
	_, listed := lemmy_client_request(t, jwt, "GET", "/api/v3/private_message/list?unread_only=true", nil)
	messages := mastodon_items(listed["private_messages"])
	if len(messages) != 1 || fmt.Sprint(mastodon_map(messages[0]["private_message"])["creator_id"]) != bob {
		t.Fatalf("THE POINT: bob's message = %v", listed)
	}
	theirs := fmt.Sprint(mastodon_map(messages[0]["private_message"])["id"])
	if _, read := lemmy_client_request(t, jwt, "POST", "/api/v3/private_message/mark_as_read", map[string]any{"private_message_id": json.Number(theirs), "read": true}); mastodon_map(mastodon_map(read["private_message_view"])["private_message"])["read"] != true {
		t.Errorf("marking it read = %v", read)
	}
	if _, counted := lemmy_client_request(t, jwt, "GET", "/api/v3/user/unread_count", nil); counted["private_messages"] != float64(0) {
		t.Errorf("unread messages after reading = %v", counted)
	}
	mine := fmt.Sprint(message["id"])
	if _, edited := lemmy_client_request(t, jwt, "PUT", "/api/v3/private_message", map[string]any{"private_message_id": json.Number(mine), "content": "Hello again"}); mastodon_map(mastodon_map(edited["private_message_view"])["private_message"])["content"] != "Hello again" {
		t.Errorf("the edit = %v", edited)
	}
	if _, deleted := lemmy_client_request(t, jwt, "POST", "/api/v3/private_message/delete", map[string]any{"private_message_id": json.Number(mine), "deleted": true}); mastodon_map(mastodon_map(deleted["private_message_view"])["private_message"])["deleted"] != true {
		t.Errorf("the deletion = %v", deleted)
	}
	if _, listed := lemmy_client_request(t, jwt, "GET", "/api/v3/private_message/list", nil); len(mastodon_items(listed["private_messages"])) != 2 {
		t.Errorf("THE POINT: the deleted message is still listed: %v", listed)
	}
}

// TestLemmyPictrs: an image a signed-in app uploads is served to anyone at
// its name, and goes with the token it was given; anything that is not an
// image is refused, as is an upload with no sign-in.
func TestLemmyPictrs(t *testing.T) {
	_, _, _, password := create_lemmy_test_env(t)
	jwt := lemmy_client_login(t, password)
	png := append([]byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"), bytes.Repeat([]byte{0}, 64)...)
	upload := func(token string, data []byte) (*httptest.ResponseRecorder, map[string]any) {
		var body bytes.Buffer
		form := multipart.NewWriter(&body)
		part, _ := form.CreateFormFile("images[]", "image.png")
		part.Write(data)
		form.Close()
		request := httptest.NewRequest("POST", "http://example.com/pictrs/image", &body)
		request.Header.Set("Content-Type", form.FormDataContentType())
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = request
		lemmy_route(c)
		var answer map[string]any
		json.Unmarshal(w.Body.Bytes(), &answer)
		return w, answer
	}
	if w, _ := upload("", png); w.Code != http.StatusUnauthorized {
		t.Errorf("an anonymous upload = %d", w.Code)
	}
	if w, answer := upload(jwt, []byte("<html><script>alert(1)</script></html>")); w.Code != http.StatusBadRequest || answer["error"] != "invalid_image" {
		t.Errorf("a page as an image = %d %v", w.Code, answer)
	}
	w, answer := upload(jwt, png)
	files := mastodon_items(answer["files"])
	if w.Code != http.StatusCreated || len(files) != 1 {
		t.Fatalf("THE POINT: the upload = %d %v", w.Code, answer)
	}
	name, token := fmt.Sprint(files[0]["file"]), fmt.Sprint(files[0]["delete_token"])
	served, _ := lemmy_client_request(t, "", "GET", "/pictrs/image/"+name, nil)
	if served.Code != http.StatusOK || served.Header().Get("Content-Type") != "image/png" || !bytes.Equal(served.Body.Bytes(), png) {
		t.Errorf("THE POINT: served = %d %s", served.Code, served.Header().Get("Content-Type"))
	}
	if w, _ := lemmy_client_request(t, "", "GET", "/pictrs/image/delete/wrong/"+name, nil); w.Code != http.StatusNotFound {
		t.Errorf("deleting with the wrong token = %d", w.Code)
	}
	lemmy_client_request(t, "", "GET", "/pictrs/image/delete/"+token+"/"+name, nil)
	if w, _ := lemmy_client_request(t, "", "GET", "/pictrs/image/"+name, nil); w.Code != http.StatusNotFound {
		t.Errorf("THE POINT: the deleted image = %d", w.Code)
	}
	if found, _ := networks_database().exists("select 1 from pictrs where file=?", name); found {
		t.Error("THE POINT: the deleted image's record stayed")
	}
	if _, err := os.Stat(filepath.Join(lemmy_pictrs_directory(), name)); !os.IsNotExist(err) {
		t.Error("THE POINT: the deleted image's file stayed")
	}
	if found, _ := networks_database().exists("select 1 from pictrs where file=?", name); found {
		t.Error("THE POINT: the deleted image's record stayed")
	}
	if _, err := os.Stat(filepath.Join(lemmy_pictrs_directory(), name)); !os.IsNotExist(err) {
		t.Error("THE POINT: the deleted image's file stayed")
	}
}
