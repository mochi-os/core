// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Mastodon client
//
// A linked Mastodon account. Mochi registers itself as an application on the
// user's server, once per server, and runs that server's OAuth consent; the
// token lands on the account and every request after carries it. The
// functions an app calls map onto the server's REST interface: statuses for
// posting, with the idempotency key the server honours, the home timeline
// and the account's own statuses for reading, notifications, and media for
// uploads. The server's limits are read from its instance document when the
// account is linked and refreshed daily (decision 24).

package main

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// mastodon_scopes is what Mochi asks a server for: reading and writing as the
// account.
const mastodon_scopes = "read write"

// mastodon_host_re is the shape of a server name a user types, with a port
// where a test server has one.
var mastodon_host_re = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+(:[0-9]+)?$`)

// mastodon_host reads the server a user named, as a host: with any scheme or
// path they pasted removed.
func mastodon_host(server string) (string, error) {
	server = strings.ToLower(strings.TrimSpace(server))
	server = strings.TrimPrefix(strings.TrimPrefix(server, "https://"), "http://")
	if i := strings.IndexAny(server, "/?#"); i >= 0 {
		server = server[:i]
	}
	server = strings.TrimPrefix(server, "@")
	if !mastodon_host_re.MatchString(server) {
		return "", fmt.Errorf("invalid server")
	}
	return server, nil
}

// mastodon_client is Mochi's registration on a server, made on first use and
// kept in networks.db for every user; a registration whose redirect no longer
// matches is made afresh, since the server checks the redirect against it.
func mastodon_client(ctx context.Context, user *User, host string, redirect string) (string, string, error) {
	db := db_open("db/networks.db")
	row, _ := db.row("select id, secret, redirect from clients where host=? and network='mastodon'", host)
	if row != nil && row_string(row, "redirect") == redirect {
		return row_string(row, "id"), row_string(row, "secret"), nil
	}
	l := &linked{user: user, id: "", kind: "", data: map[string]any{}}
	var answer struct {
		ID     string `json:"client_id"`
		Secret string `json:"client_secret"`
	}
	status, body, err := l.form(ctx, network_scheme+host+"/api/v1/apps", url.Values{
		"client_name":   {"Mochi"},
		"redirect_uris": {redirect},
		"scopes":        {mastodon_scopes},
		"website":       {"https://mochi-os.org"},
	}, &answer)
	if err != nil {
		return "", "", err
	}
	if status < 200 || status > 299 || answer.ID == "" || answer.Secret == "" {
		return "", "", fmt.Errorf("the server did not register Mochi: %v", linked_failure(status, body))
	}
	db.exec("insert into clients (host, network, id, secret, redirect, created) values (?, 'mastodon', ?, ?, ?, ?) on conflict(host, network) do update set id=excluded.id, secret=excluded.secret, redirect=excluded.redirect, created=excluded.created", host, answer.ID, answer.Secret, redirect, now())
	return answer.ID, answer.Secret, nil
}

// mastodon_prepare readies a link ceremony: the server is checked against the
// host policy, Mochi is registered on it, and the consent URL is set.
func mastodon_prepare(ctx context.Context, user *User, st *oauth_state, server string) error {
	host, err := mastodon_host(server)
	if err != nil {
		return err
	}
	if err := host_allowed(user, "", host); err != nil {
		return err
	}
	client, _, err := mastodon_client(ctx, user, host, st.Redirect)
	if err != nil {
		return err
	}
	st.Server = host
	st.Client = client
	st.Consent = network_scheme + host + "/oauth/authorize?" + url.Values{
		"response_type":         {"code"},
		"client_id":             {client},
		"redirect_uri":          {st.Redirect},
		"scope":                 {mastodon_scopes},
		"state":                 {st.State},
		"code_challenge":        {oauth_challenge(st.Verifier)},
		"code_challenge_method": {"S256"},
	}.Encode()
	return nil
}

// mastodon_callback completes a link: the code is exchanged for the token,
// the account and its server's limits are read, and what to store is
// answered.
func mastodon_callback(ctx context.Context, user *User, st *oauth_state, code string) (*linked_profile, map[string]any, error) {
	host := st.Server
	row, _ := db_open("db/networks.db").row("select id, secret from clients where host=? and network='mastodon'", host)
	if row == nil || row_string(row, "id") != st.Client {
		return nil, nil, fmt.Errorf("no registration on %s", host)
	}
	l := &linked{user: user, kind: "mastodon", data: map[string]any{"server": host}}
	var token struct {
		Token string `json:"access_token"`
		Scope string `json:"scope"`
	}
	status, body, err := l.form(ctx, network_scheme+host+"/oauth/token", url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"client_id":     {st.Client},
		"client_secret": {row_string(row, "secret")},
		"redirect_uri":  {st.Redirect},
		"scope":         {mastodon_scopes},
		"code_verifier": {st.Verifier},
	}, &token)
	if err != nil {
		return nil, nil, err
	}
	if status < 200 || status > 299 || token.Token == "" {
		return nil, nil, fmt.Errorf("the token exchange failed: %v", linked_failure(status, body))
	}
	l.data["token"] = token.Token
	l.data["scopes"] = strings.Fields(token.Scope)
	profile, err := mastodon_profile(ctx, l)
	if err != nil {
		return nil, nil, err
	}
	l.data["limits"] = mastodon_limits(ctx, l)
	l.data["refreshed"] = now()
	return profile, l.data, nil
}

// mastodon_address is the account's address, @user@host.
func mastodon_address(acct string, host string) string {
	acct = strings.TrimPrefix(acct, "@")
	if !strings.Contains(acct, "@") {
		acct += "@" + host
	}
	return "@" + acct
}

// mastodon_profile reads the account behind the token.
func mastodon_profile(ctx context.Context, l *linked) (*linked_profile, error) {
	var account struct {
		ID     string `json:"id"`
		Acct   string `json:"acct"`
		Name   string `json:"display_name"`
		URL    string `json:"url"`
		Avatar string `json:"avatar"`
	}
	status, body, err := l.json(ctx, "GET", network_scheme+l.get("server")+"/api/v1/accounts/verify_credentials", nil, &account)
	if err != nil {
		return nil, err
	}
	if status == 401 || status == 403 {
		return nil, linked_signed_out
	}
	if status < 200 || status > 299 || account.URL == "" {
		return nil, fmt.Errorf("the account could not be read: %v", linked_failure(status, body))
	}
	l.set("account", account.ID)
	if account.Name == "" {
		account.Name = account.Acct
	}
	return &linked_profile{
		ID:      account.URL,
		Address: mastodon_address(account.Acct, l.get("server")),
		Name:    account.Name,
		Avatar:  account.Avatar,
		URL:     account.URL,
		Server:  l.get("server"),
	}, nil
}

// mastodon_limits reads the server's limits for a post: characters,
// attachments and their sizes and types, and the length of alternative text.
// A server that does not answer leaves them unknown, and the app applies
// Mastodon's defaults.
func mastodon_limits(ctx context.Context, l *linked) map[string]any {
	var instance struct {
		Configuration struct {
			Statuses struct {
				Characters  int `json:"max_characters"`
				Attachments int `json:"max_media_attachments"`
			} `json:"statuses"`
			Media struct {
				Types       []string `json:"supported_mime_types"`
				Image       int64    `json:"image_size_limit"`
				Video       int64    `json:"video_size_limit"`
				Description int      `json:"description_limit"`
			} `json:"media_attachments"`
		} `json:"configuration"`
	}
	status, _, err := l.json(ctx, "GET", network_scheme+l.get("server")+"/api/v2/instance", nil, &instance)
	if err != nil || status != 200 {
		return nil
	}
	s, m := instance.Configuration.Statuses, instance.Configuration.Media
	if s.Characters == 0 {
		return nil
	}
	return map[string]any{
		"characters":  s.Characters,
		"attachments": s.Attachments,
		"image":       m.Image,
		"video":       m.Video,
		"types":       m.Types,
		"description": m.Description,
	}
}

// mastodon_api is the address of a path under the server's /api/.
func mastodon_api(l *linked, path string, query map[string]string) string {
	address := network_scheme + l.get("server") + "/api/" + path
	if len(query) > 0 {
		values := url.Values{}
		for key, value := range query {
			values.Set(key, value)
		}
		address += "?" + values.Encode()
	}
	return address
}

// mastodon_result shapes a status for an app: its id and page, and the status
// as the server answered it.
func mastodon_result(status map[string]any) map[string]any {
	id, _ := status["id"].(string)
	page, _ := status["url"].(string)
	return map[string]any{"id": id, "url": page, "data": status}
}

// mastodon_create posts a status: the object is the status form (status,
// media_ids, visibility, spoiler_text, language, in_reply_to_id, poll), sent
// with the key as the idempotency key.
func mastodon_create(ctx context.Context, l *linked, object map[string]any, key string) (map[string]any, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	if key != "" {
		headers["Idempotency-Key"] = key
	}
	status, body, _, err := l.request(ctx, "POST", mastodon_api(l, "v1/statuses", nil), headers, []byte(json_encode(object)))
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	var answer map[string]any
	if err := json_decode(body, &answer); err != nil {
		return nil, err
	}
	return mastodon_result(answer), nil
}

// mastodon_update edits a status.
func mastodon_update(ctx context.Context, l *linked, id string, object map[string]any) (map[string]any, error) {
	var answer map[string]any
	status, body, err := l.json(ctx, "PUT", mastodon_api(l, "v1/statuses/"+url.PathEscape(id), nil), object, &answer)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	return mastodon_result(answer), nil
}

// mastodon_delete deletes a status. One already gone is deleted.
func mastodon_delete(ctx context.Context, l *linked, id string) error {
	status, body, err := l.json(ctx, "DELETE", mastodon_api(l, "v1/statuses/"+url.PathEscape(id), nil), nil, nil)
	if err != nil {
		return err
	}
	if status == 404 || (status >= 200 && status <= 299) {
		return nil
	}
	return linked_failure(status, body)
}

// mastodon_page reads one page of a list endpoint: the items, and the next
// page's max_id from the Link header, or the last item's id, or nothing.
func mastodon_page(ctx context.Context, l *linked, path string, query map[string]string, cursor string, limit int, maximum int) (*linked_page, error) {
	if limit > maximum {
		limit = maximum
	}
	query["limit"] = strconv.Itoa(limit)
	if cursor != "" {
		query["max_id"] = cursor
	}
	status, body, header, err := l.request(ctx, "GET", mastodon_api(l, path, query), nil, nil)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	var items []any
	if err := json_decode(body, &items); err != nil {
		return nil, err
	}
	page := &linked_page{Items: items}
	if len(items) > 0 {
		for _, link := range strings.Split(header.Get("Link"), ",") {
			if strings.Contains(link, `rel="next"`) {
				if i := strings.Index(link, "max_id="); i >= 0 {
					page.Cursor = strings.TrimRight(link[i+7:], ">; ")
					if j := strings.IndexAny(page.Cursor, "&>"); j >= 0 {
						page.Cursor = page.Cursor[:j]
					}
				}
			}
		}
		if page.Cursor == "" {
			if last, ok := items[len(items)-1].(map[string]any); ok {
				page.Cursor, _ = last["id"].(string)
			}
		}
	}
	return page, nil
}

// mastodon_timeline reads the home timeline.
func mastodon_timeline(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	return mastodon_page(ctx, l, "v1/timelines/home", map[string]string{}, cursor, limit, 40)
}

// mastodon_posts reads the account's own statuses, replies included.
func mastodon_posts(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	account := l.get("account")
	if account == "" {
		if _, err := mastodon_profile(ctx, l); err != nil {
			return nil, err
		}
		account = l.get("account")
		l.save()
	}
	return mastodon_page(ctx, l, "v1/accounts/"+url.PathEscape(account)+"/statuses", map[string]string{"exclude_reblogs": "true"}, cursor, limit, 40)
}

// mastodon_notifications reads the account's notifications.
func mastodon_notifications(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	return mastodon_page(ctx, l, "v1/notifications", map[string]string{}, cursor, limit, 30)
}

// mastodon_upload_wait is how long an upload is given to be processed before
// its reference is answered anyway. A variable so tests can lower it.
var mastodon_upload_wait = 15 * time.Second

// mastodon_upload uploads a file as a media attachment with its alternative
// text, waiting a while for the server to process it, since a status cannot
// carry an attachment still processing.
func mastodon_upload(ctx context.Context, l *linked, content []byte, kind string, name string, description string) (map[string]any, error) {
	var media map[string]any
	status, body, err := l.multipart(ctx, mastodon_api(l, "v2/media", nil), "file", name, kind, content, map[string]string{"description": description}, &media)
	if err != nil {
		return nil, err
	}
	if status < 200 || status > 299 {
		return nil, linked_failure(status, body)
	}
	id, _ := media["id"].(string)
	deadline := time.Now().Add(mastodon_upload_wait)
	for (status == 202 || status == 206) && id != "" && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
		var processed map[string]any
		status, _, err = l.json(ctx, "GET", mastodon_api(l, "v1/media/"+url.PathEscape(id), nil), nil, &processed)
		if err != nil {
			return nil, err
		}
		if status == 200 {
			media = processed
		}
	}
	page, _ := media["url"].(string)
	return map[string]any{"id": id, "url": page, "data": media}, nil
}

// mastodon_refresh_limits reads a server's limits again once a day, for every
// linked Mastodon account on it.
func mastodon_refresh_limits(ctx context.Context, l *linked) {
	refreshed, _ := l.data["refreshed"].(float64)
	if now()-int64(refreshed) < 86400 {
		return
	}
	if limits := mastodon_limits(ctx, l); limits != nil {
		l.set("limits", limits)
	}
	l.set("refreshed", now())
	l.save()
}
