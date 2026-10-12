// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Lemmy client
//
// A linked Lemmy account, signed in with the user's name and password on the
// 0.19 interface (/api/v3): the token the server issues is kept, the password
// is not. Posts and comments go out through the account, the subscribed
// communities' posts are its timeline, its own posts and comments are its
// posts, and replies and mentions are its notifications. A post is made once
// per key by the record linked_create keeps, since the interface has no
// idempotency of its own.

package main

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// lemmy_api is the address of a path under the server's /api/v3/.
func lemmy_api(l *linked, path string, query map[string]string) string {
	address := network_scheme + l.get("server") + "/api/v3/" + path
	if len(query) > 0 {
		values := url.Values{}
		for key, value := range query {
			values.Set(key, value)
		}
		address += "?" + values.Encode()
	}
	return address
}

// lemmy_link signs in to a server with the fields of the form - server,
// username, password and the one-time code of a second factor - and answers
// the account and what to store.
func lemmy_link(ctx context.Context, user *User, fields map[string]string) (*linked_profile, map[string]any, error) {
	host, err := mastodon_host(fields["server"])
	if err != nil {
		return nil, nil, err
	}
	if err := host_allowed(user, "", host); err != nil {
		return nil, nil, err
	}
	l := &linked{user: user, kind: "lemmy", data: map[string]any{"server": host}}
	login := map[string]any{"username_or_email": fields["username"], "password": fields["password"]}
	if fields["code"] != "" {
		login["totp_2fa_token"] = fields["code"]
	}
	var answer struct {
		Token string `json:"jwt"`
	}
	status, body, err := l.json(ctx, "POST", lemmy_api(l, "user/login", nil), login, &answer)
	if err != nil {
		return nil, nil, err
	}
	if status == 400 || status == 401 {
		return nil, nil, fmt.Errorf("the server did not accept the sign-in: %v", linked_failure(status, body))
	}
	if status < 200 || status > 299 || answer.Token == "" {
		return nil, nil, fmt.Errorf("the sign-in failed: %v", linked_failure(status, body))
	}
	l.data["token"] = answer.Token
	profile, err := lemmy_profile(ctx, l)
	if err != nil {
		return nil, nil, err
	}
	return profile, l.data, nil
}

// lemmy_profile reads the person behind the token from the site document.
func lemmy_profile(ctx context.Context, l *linked) (*linked_profile, error) {
	var site struct {
		User *struct {
			View struct {
				Person struct {
					ID     int64  `json:"id"`
					Name   string `json:"name"`
					Label  string `json:"display_name"`
					Actor  string `json:"actor_id"`
					Avatar string `json:"avatar"`
				} `json:"person"`
			} `json:"local_user_view"`
		} `json:"my_user"`
	}
	status, body, err := l.json(ctx, "GET", lemmy_api(l, "site", nil), nil, &site)
	if err != nil {
		return nil, err
	}
	if status == 401 || status == 403 {
		return nil, linked_signed_out
	}
	if status < 200 || status > 299 || site.User == nil || site.User.View.Person.Actor == "" {
		if status >= 200 && status <= 299 {
			return nil, linked_signed_out
		}
		return nil, fmt.Errorf("the account could not be read: %v", linked_failure(status, body))
	}
	person := site.User.View.Person
	l.set("person", strconv.FormatInt(person.ID, 10))
	name := person.Label
	if name == "" {
		name = person.Name
	}
	return &linked_profile{
		ID:      person.Actor,
		Address: "@" + person.Name + "@" + l.get("server"),
		Name:    name,
		Avatar:  person.Avatar,
		URL:     person.Actor,
		Server:  l.get("server"),
	}, nil
}

// lemmy_view reads the post or comment view a mutation answers with: the
// item's numeric id, its page and the view.
func lemmy_view(answer map[string]any, kind string) map[string]any {
	view, _ := answer[kind+"_view"].(map[string]any)
	item, _ := view[kind].(map[string]any)
	id := ""
	if number, ok := item["id"].(float64); ok {
		id = kind + "/" + strconv.FormatInt(int64(number), 10)
	}
	page, _ := item["ap_id"].(string)
	return map[string]any{"id": id, "url": page, "data": view}
}

// lemmy_identifier reads an object id of the form post/<n> or comment/<n>.
func lemmy_identifier(id string) (string, int64, error) {
	kind, number, found := strings.Cut(id, "/")
	n, err := strconv.ParseInt(number, 10, 64)
	if !found || err != nil || (kind != "post" && kind != "comment") {
		return "", 0, fmt.Errorf("invalid id %q: post/<number> or comment/<number>", id)
	}
	return kind, n, nil
}

// lemmy_create posts a post or a comment: the object is the create form with
// kind, "post" (name, community_id, url, body, nsfw, language_id) or
// "comment" (content, post_id, parent_id, language_id).
func lemmy_create(ctx context.Context, l *linked, object map[string]any) (map[string]any, error) {
	kind, _ := object["kind"].(string)
	if kind == "" {
		kind = "post"
	}
	if kind != "post" && kind != "comment" {
		return nil, fmt.Errorf("kind must be post or comment")
	}
	form := map[string]any{}
	for key, value := range object {
		if key != "kind" {
			form[key] = value
		}
	}
	var answer map[string]any
	status, body, err := l.json(ctx, "POST", lemmy_api(l, kind, nil), form, &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	return lemmy_view(answer, kind), nil
}

// lemmy_update edits a post or a comment: the object is the edit form, with
// the id added.
func lemmy_update(ctx context.Context, l *linked, id string, object map[string]any) (map[string]any, error) {
	kind, number, err := lemmy_identifier(id)
	if err != nil {
		return nil, err
	}
	form := map[string]any{}
	for key, value := range object {
		if key != "kind" {
			form[key] = value
		}
	}
	form[kind+"_id"] = number
	var answer map[string]any
	status, body, err := l.json(ctx, "PUT", lemmy_api(l, kind, nil), form, &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	return lemmy_view(answer, kind), nil
}

// lemmy_delete deletes a post or a comment.
func lemmy_delete(ctx context.Context, l *linked, id string) error {
	kind, number, err := lemmy_identifier(id)
	if err != nil {
		return err
	}
	status, body, err := l.json(ctx, "POST", lemmy_api(l, kind+"/delete", nil), map[string]any{kind + "_id": number, "deleted": true}, nil)
	if err != nil {
		return err
	}
	if status == 404 || (status >= 200 && status <= 299) {
		return nil
	}
	return linked_failure(status, body)
}

// lemmy_timeline reads the subscribed communities' posts, newest first, by the
// server's page cursor.
func lemmy_timeline(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	if limit > 50 {
		limit = 50
	}
	query := map[string]string{"type_": "Subscribed", "sort": "New", "limit": strconv.Itoa(limit)}
	if cursor != "" {
		query["page_cursor"] = cursor
	}
	var answer struct {
		Posts []any  `json:"posts"`
		Next  string `json:"next_page"`
	}
	status, body, err := l.json(ctx, "GET", lemmy_api(l, "post/list", query), nil, &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	page := &linked_page{Items: answer.Posts}
	if len(answer.Posts) > 0 {
		page.Cursor = answer.Next
	}
	return page, nil
}

// lemmy_pages turns a page number cursor into the number, 1 at the start.
func lemmy_pages(cursor string) int {
	page, err := strconv.Atoi(cursor)
	if err != nil || page < 1 {
		return 1
	}
	return page
}

// lemmy_posts reads the account's own posts and comments, a page at a time.
func lemmy_posts(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	if limit > 50 {
		limit = 50
	}
	person := l.get("person")
	if person == "" {
		if _, err := lemmy_profile(ctx, l); err != nil {
			return nil, err
		}
		person = l.get("person")
		l.save()
	}
	number := lemmy_pages(cursor)
	var answer struct {
		Posts    []any `json:"posts"`
		Comments []any `json:"comments"`
	}
	status, body, err := l.json(ctx, "GET", lemmy_api(l, "user", map[string]string{"person_id": person, "sort": "New", "limit": strconv.Itoa(limit), "page": strconv.Itoa(number)}), nil, &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	page := &linked_page{Items: append(answer.Posts, answer.Comments...)}
	if len(page.Items) > 0 {
		page.Cursor = strconv.Itoa(number + 1)
	}
	return page, nil
}

// lemmy_notifications reads replies and mentions, each item marked with its
// kind, a page at a time.
func lemmy_notifications(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	if limit > 50 {
		limit = 50
	}
	number := lemmy_pages(cursor)
	page := &linked_page{}
	for _, source := range []struct{ path, field, kind string }{{"user/replies", "replies", "reply"}, {"user/mention", "mentions", "mention"}} {
		var answer map[string][]map[string]any
		status, body, err := l.json(ctx, "GET", lemmy_api(l, source.path, map[string]string{"sort": "New", "limit": strconv.Itoa(limit), "page": strconv.Itoa(number)}), nil, &answer)
		if err != nil {
			return nil, err
		}
		if status < 200 || status > 299 {
			return nil, linked_failure(status, body)
		}
		for _, item := range answer[source.field] {
			item["kind"] = source.kind
			page.Items = append(page.Items, item)
		}
	}
	if len(page.Items) > 0 {
		page.Cursor = strconv.Itoa(number + 1)
	}
	return page, nil
}

// lemmy_upload uploads an image to the server's image store, which a post
// names by its URL.
func lemmy_upload(ctx context.Context, l *linked, content []byte, kind string, name string) (map[string]any, error) {
	var answer struct {
		Files []struct {
			File string `json:"file"`
		} `json:"files"`
	}
	status, body, err := l.multipart(ctx, network_scheme+l.get("server")+"/pictrs/image", "images[]", name, kind, content, nil, &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 || len(answer.Files) == 0 {
		return nil, linked_failure(status, body)
	}
	file := answer.Files[0].File
	var data any
	json_decode(body, &data)
	return map[string]any{"id": file, "url": network_scheme + l.get("server") + "/pictrs/image/" + file, "data": data}, nil
}

// lemmy_request sends an app's own request on the server's interface.
func lemmy_request(ctx context.Context, l *linked, method string, path string, query map[string]string, body any) (int, []byte, error) {
	status, data, err := l.json(ctx, method, lemmy_api(l, path, query), body, nil)
	return status, data, err
}
