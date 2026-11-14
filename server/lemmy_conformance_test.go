// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// A lemmy_schema is what a Lemmy app's client library parses, extracted from
// it by claude/scripts/lemmy-schema.py into testdata/lemmy/schema.json: each
// route's answer type, each type's fields, and the aliases and enums they
// name. The library is the one Jerboa uses, with the types it parses a 0.19.11
// or later server with; it ignores keys it does not know and nothing else.
type lemmy_schema struct {
	Routes  map[string]string                        `json:"routes"`
	Types   map[string]map[string]lemmy_schema_field `json:"types"`
	Aliases map[string]string                        `json:"aliases"`
	Enums   map[string][]string                      `json:"enums"`
}

type lemmy_schema_field struct {
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Required bool   `json:"required"`
}

var lemmy_schema_list = regexp.MustCompile(`^List<(.+)>$`)

// check reports where a value is not what the library parses as kind: a
// field with no default left out, a null where the type allows none, a value
// of the wrong kind, an enum value it does not know.
func (s *lemmy_schema) check(kind string, value any, at string, problems *[]string) {
	for {
		alias, ok := s.Aliases[kind]
		if !ok {
			break
		}
		kind = alias
	}
	fail := func(format string, args ...any) {
		*problems = append(*problems, at+": "+fmt.Sprintf(format, args...))
	}
	if m := lemmy_schema_list.FindStringSubmatch(kind); m != nil {
		items, ok := value.([]any)
		if !ok {
			fail("a list of %s is %T", m[1], value)
			return
		}
		for i, item := range items {
			s.check(strings.TrimSuffix(m[1], "?"), item, fmt.Sprintf("%s[%d]", at, i), problems)
		}
		return
	}
	switch kind {
	case "String":
		if _, ok := value.(string); !ok {
			fail("a string is %T %v", value, value)
		}
	case "Boolean":
		if _, ok := value.(bool); !ok {
			fail("a boolean is %T %v", value, value)
		}
	case "Int", "Long":
		number, ok := value.(float64)
		if !ok || number != math.Trunc(number) || (kind == "Int" && math.Abs(number) > math.MaxInt32) {
			fail("an %s is %T %v", kind, value, value)
		}
	case "Double", "Float":
		if _, ok := value.(float64); !ok {
			fail("a number is %T %v", value, value)
		}
	case "Unit", "JsonElement", "JsonObject", "Any":
	default:
		if values, ok := s.Enums[kind]; ok {
			text, _ := value.(string)
			for _, v := range values {
				if v == text {
					return
				}
			}
			fail("%v is no %s", value, kind)
			return
		}
		fields, ok := s.Types[kind]
		if !ok {
			fail("the schema has no type %s", kind)
			return
		}
		object, ok := value.(map[string]any)
		if !ok {
			fail("a %s is %T", kind, value)
			return
		}
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			field := fields[name]
			v, present := object[name]
			switch {
			case !present && field.Required:
				fail("%s.%s is missing", kind, name)
			case !present:
			case v == nil && !field.Nullable:
				fail("%s.%s is null", kind, name)
			case v == nil:
			default:
				s.check(field.Type, v, at+"."+name, problems)
			}
		}
	}
}

// TestLemmyConformance: every answer Mochi's Lemmy interface gives an app is
// one Jerboa's client library parses for the Lemmy version the site reports.
// Each route Mochi serves that the library knows is sent through the interface
// with what an app sends, and its answer checked against the library's type
// for it. A route answered only with a refusal (registration, passwords,
// account deletion, the site's administration, distinguishing a comment,
// reporting a private message) is left out: the library reads a refusal as
// an error.
func TestLemmyConformance(t *testing.T) {
	data, err := os.ReadFile("testdata/lemmy/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema lemmy_schema
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	_, fake, _, password := create_lemmy_test_env(t)
	fake.mentions = []string{"c2"}
	jwt := lemmy_client_login(t, password)

	_, site := lemmy_client_request(t, jwt, "GET", "/api/v3/site", nil)
	me := fmt.Sprint(mastodon_map(mastodon_map(mastodon_map(site["my_user"])["local_user_view"])["person"])["id"])
	_, listed := lemmy_client_request(t, jwt, "GET", "/api/v3/community/list", nil)
	community := fmt.Sprint(mastodon_map(mastodon_items(listed["communities"])[0]["community"])["id"])
	_, posts := lemmy_client_request(t, jwt, "GET", "/api/v3/post/list?community_id="+community, nil)
	post := fmt.Sprint(mastodon_map(mastodon_items(posts["posts"])[0]["post"])["id"])
	creator := fmt.Sprint(mastodon_map(mastodon_items(posts["posts"])[0]["creator"])["id"])
	_, comments := lemmy_client_request(t, jwt, "GET", "/api/v3/comment/list?post_id="+post, nil)
	comment := fmt.Sprint(mastodon_map(mastodon_items(comments["comments"])[0]["comment"])["id"])
	_, replies := lemmy_client_request(t, jwt, "GET", "/api/v3/user/replies", nil)
	reply := fmt.Sprint(mastodon_map(mastodon_items(replies["replies"])[0]["comment_reply"])["id"])
	_, mentions := lemmy_client_request(t, jwt, "GET", "/api/v3/user/mention", nil)
	mention := fmt.Sprint(mastodon_map(mastodon_items(mentions["mentions"])[0]["person_mention"])["id"])
	_, sent := lemmy_client_request(t, jwt, "POST", "/api/v3/private_message", map[string]any{"content": "Hello", "recipient_id": json.Number(creator)})
	message := fmt.Sprint(mastodon_map(mastodon_map(sent["private_message_view"])["private_message"])["id"])
	_, reported := lemmy_client_request(t, jwt, "POST", "/api/v3/post/report", map[string]any{"post_id": json.Number(post), "reason": "Spam"})
	report := fmt.Sprint(mastodon_map(mastodon_map(reported["post_report_view"])["post_report"])["id"])
	_, flagged := lemmy_client_request(t, jwt, "POST", "/api/v3/comment/report", map[string]any{"comment_id": json.Number(comment), "reason": "Spam"})
	flag := fmt.Sprint(mastodon_map(mastodon_map(flagged["comment_report_view"])["comment_report"])["id"])

	number := func(id string) json.Number { return json.Number(id) }
	requests := []struct {
		route string
		path  string
		body  any
	}{
		{"POST user/login", "/api/v3/user/login", map[string]any{"username_or_email": "alice", "password": password}},
		{"GET site", "/api/v3/site", nil},
		{"GET federated_instances", "/api/v3/federated_instances", nil},
		{"GET user/validate_auth", "/api/v3/user/validate_auth", nil},
		{"GET user/unread_count", "/api/v3/user/unread_count", nil},
		{"GET user/report_count", "/api/v3/user/report_count", nil},
		{"GET user", "/api/v3/user?person_id=" + me, nil},
		{"GET user", "/api/v3/user?person_id=" + creator, nil},
		{"GET user/replies", "/api/v3/user/replies", nil},
		{"GET user/mention", "/api/v3/user/mention", nil},
		{"POST comment/mark_as_read", "/api/v3/comment/mark_as_read", map[string]any{"comment_reply_id": number(reply), "read": true}},
		{"POST user/mention/mark_as_read", "/api/v3/user/mention/mark_as_read", map[string]any{"person_mention_id": number(mention), "read": true}},
		{"POST user/mark_all_as_read", "/api/v3/user/mark_all_as_read", map[string]any{}},
		{"POST user/block", "/api/v3/user/block", map[string]any{"person_id": number(creator), "block": false}},
		{"GET community/list", "/api/v3/community/list", nil},
		{"GET community", "/api/v3/community?id=" + community, nil},
		{"POST community/follow", "/api/v3/community/follow", map[string]any{"community_id": number(community), "follow": true}},
		{"POST community/block", "/api/v3/community/block", map[string]any{"community_id": number(community), "block": false}},
		{"POST community/ban_user", "/api/v3/community/ban_user", map[string]any{"community_id": number(community), "person_id": number(creator), "ban": false}},
		{"POST community/mod", "/api/v3/community/mod", map[string]any{"community_id": number(community), "person_id": number(creator), "added": false}},
		{"GET post/list", "/api/v3/post/list?community_id=" + community, nil},
		{"GET post", "/api/v3/post?id=" + post, nil},
		{"POST post", "/api/v3/post", map[string]any{"community_id": number(community), "name": "New one", "body": "Text"}},
		{"PUT post", "/api/v3/post", map[string]any{"post_id": number(post), "name": "A book, edited"}},
		{"POST post/delete", "/api/v3/post/delete", map[string]any{"post_id": number(post), "deleted": false}},
		{"POST post/remove", "/api/v3/post/remove", map[string]any{"post_id": number(post), "removed": false}},
		{"POST post/lock", "/api/v3/post/lock", map[string]any{"post_id": number(post), "locked": false}},
		{"POST post/feature", "/api/v3/post/feature", map[string]any{"post_id": number(post), "featured": false, "feature_type": "Community"}},
		{"POST post/like", "/api/v3/post/like", map[string]any{"post_id": number(post), "score": 1}},
		{"PUT post/save", "/api/v3/post/save", map[string]any{"post_id": number(post), "save": true}},
		{"POST post/mark_as_read", "/api/v3/post/mark_as_read", map[string]any{"post_ids": []any{number(post)}, "read": true}},
		{"POST post/hide", "/api/v3/post/hide", map[string]any{"post_ids": []any{number(post)}, "hide": false}},
		{"POST post/report", "/api/v3/post/report", map[string]any{"post_id": number(post), "reason": "Spam"}},
		{"GET post/site_metadata", "/api/v3/post/site_metadata?url=https%3A%2F%2Fexample.org%2F", nil},
		{"GET comment/list", "/api/v3/comment/list?post_id=" + post, nil},
		{"GET comment", "/api/v3/comment?id=" + comment, nil},
		{"POST comment", "/api/v3/comment", map[string]any{"post_id": number(post), "content": "Thanks"}},
		{"PUT comment", "/api/v3/comment", map[string]any{"comment_id": number(comment), "content": "Edited"}},
		{"POST comment/delete", "/api/v3/comment/delete", map[string]any{"comment_id": number(comment), "deleted": false}},
		{"POST comment/remove", "/api/v3/comment/remove", map[string]any{"comment_id": number(comment), "removed": false}},
		{"POST comment/like", "/api/v3/comment/like", map[string]any{"comment_id": number(comment), "score": 1}},
		{"PUT comment/save", "/api/v3/comment/save", map[string]any{"comment_id": number(comment), "save": true}},
		{"POST comment/report", "/api/v3/comment/report", map[string]any{"comment_id": number(comment), "reason": "Spam"}},
		{"GET post/report/list", "/api/v3/post/report/list", nil},
		{"GET comment/report/list", "/api/v3/comment/report/list", nil},
		{"PUT post/report/resolve", "/api/v3/post/report/resolve", map[string]any{"report_id": number(report), "resolved": true}},
		{"PUT comment/report/resolve", "/api/v3/comment/report/resolve", map[string]any{"report_id": number(flag), "resolved": true}},
		{"GET private_message/report/list", "/api/v3/private_message/report/list", nil},
		{"GET modlog", "/api/v3/modlog", nil},
		{"GET search", "/api/v3/search?q=book", nil},
		{"GET resolve_object", "/api/v3/resolve_object?q=%21books%40example.com", nil},
		{"GET private_message/list", "/api/v3/private_message/list", nil},
		{"POST private_message", "/api/v3/private_message", map[string]any{"content": "Hello again", "recipient_id": number(creator)}},
		{"PUT private_message", "/api/v3/private_message", map[string]any{"private_message_id": number(message), "content": "Edited"}},
		{"POST private_message/mark_as_read", "/api/v3/private_message/mark_as_read", map[string]any{"private_message_id": number(message), "read": true}},
		{"POST private_message/delete", "/api/v3/private_message/delete", map[string]any{"private_message_id": number(message), "deleted": true}},
	}
	covered := map[string]bool{}
	for _, r := range requests {
		kind, known := schema.Routes[r.route]
		if !known {
			t.Errorf("%s is not a route the library knows", r.route)
			continue
		}
		covered[r.route] = true
		method, _, _ := strings.Cut(r.route, " ")
		w, _ := lemmy_client_request(t, jwt, method, r.path, r.body)
		if w.Code != http.StatusOK {
			t.Errorf("%s %s: %d %s", method, r.path, w.Code, w.Body.String())
			continue
		}
		var answer any
		if err := json.Unmarshal(w.Body.Bytes(), &answer); err != nil && kind != "Unit" {
			t.Errorf("%s: not JSON: %v", r.route, err)
			continue
		}
		problems := []string{}
		schema.check(kind, answer, r.route, &problems)
		for _, p := range problems {
			t.Errorf("THE POINT: %s", p)
		}
	}
	refused := map[string]bool{"POST user/register": true, "GET user/get_captcha": true, "POST user/password_reset": true, "POST user/password_change": true, "POST user/verify_email": true,
		"POST user/delete_account": true, "POST user/totp/generate": true, "POST user/totp/update": true, "POST user/import_settings": true, "POST user/logout": true,
		"POST community": true, "PUT community": true, "POST community/delete": true, "POST community/remove": true, "POST community/transfer": true, "POST site": true,
		"PUT user/save_user_settings": true, "PUT user/change_password": true, "PUT site": true,
		"POST comment/distinguish": true, "POST private_message/report": true}
	for route := range schema.Routes {
		if !covered[route] && !refused[route] {
			t.Errorf("%s is in the schema but not checked", route)
		}
	}
}
