// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Lemmy apps
//
// What a Lemmy app (Jerboa, Voyager, Thunder and the rest, on the 0.19
// interface under /api/v3) asks of a server, answered for a hosted fediverse
// account (the federation plan, phase 28). The app signs in with the
// account's handle and a password minted in Settings (decision 7). Its
// communities, posts, comments, votes, saved items, reports and moderation
// are the forums app's, through the functions it declares (lemmy/*); its
// private messages are the account's conversations in chat (direct/*). Every
// item has a whole number, kept per user (decision 8). The Local and All
// listings show what the user takes part in (decision 6). Registration,
// captchas and administration answer as a closed server's do. Images are
// kept by the server under /pictrs, as Lemmy's image store keeps them.

package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// lemmy_version is the interface version the engine answers as.
const lemmy_version = "0.19.13"

// lemmy_lifetime is how long a sign-in lasts unused.
const lemmy_lifetime = 90 * 86400

// lemmy_image_maximum bounds an image an app uploads.
const lemmy_image_maximum = 20 << 20

func lemmy_engine() *engine {
	return &engine{
		name:     "lemmy-client",
		prefixes: []string{"/api/v3/", "/pictrs/"},
		setting:  "fediverse",
		body:     lemmy_image_maximum + 1<<20,
		handler:  lemmy_route,
	}
}

// lemmy_tables makes the image store's table in networks.db.
func lemmy_tables(db *DB) {
	db.exec("create table if not exists pictrs (file text not null primary key, user text not null, token text not null, type text not null, created integer not null)")
}

// === Errors ===

// lemmy_error answers a Lemmy error: its name, as Lemmy's apps read it.
func lemmy_error(c *gin.Context, status int, name string) {
	c.JSON(status, map[string]any{"error": name}) // i18n-ok: Lemmy's errors are protocol, read by apps
}

// === Sessions ===

// A lemmy_session is a request of a Lemmy app's: its token, the user and the
// hosted fediverse account it is signed in to.
type lemmy_session struct {
	token   *Token
	user    *User
	row     map[string]any
	account string
	handle  string
	domain  string
	actor   string
}

// lemmy_raw is the token a request carries: as a bearer, in the jwt cookie,
// or in the auth field older apps send.
func lemmy_raw(c *gin.Context) string {
	if scheme, value, found := strings.Cut(c.GetHeader("Authorization"), " "); found && strings.EqualFold(scheme, "Bearer") {
		return strings.TrimSpace(value)
	}
	if cookie, err := c.Cookie("jwt"); err == nil && cookie != "" {
		return cookie
	}
	return c.Query("auth")
}

// lemmy_token is a Lemmy sign-in's token by its hash: shaped as Lemmy's JWT,
// which apps read their person's number from.
func lemmy_token(raw string) *Token {
	if strings.Count(raw, ".") != 2 {
		return nil
	}
	var t Token
	if !db_open("db/users.db").scan(&t, "select hash, user, app, name, scopes, action, entity, created, expires, kind, client, account, network, jkt, refresh, device, scram from tokens where hash=? and kind=? and network='fediverse' and client='lemmy'", token_hash(raw), token_signin) {
		return nil
	}
	if t.Expires > 0 && now() > t.Expires {
		return nil
	}
	json.Unmarshal([]byte(t.ScopesDB), &t.Scopes)
	return &t
}

// lemmy_session_of is the session a token names, or nil.
func lemmy_session_of(raw string) *lemmy_session {
	t := lemmy_token(raw)
	if t == nil {
		return nil
	}
	user := user_by_uid(t.User)
	if user == nil {
		return nil
	}
	row := account_hosted_get(user, t.Account)
	if row == nil || row["type"] != "fediverse" || account_group(row) {
		return nil
	}
	data := account_data(row)
	handle, _ := data["handle"].(string)
	domain, _ := data["domain"].(string)
	if handle == "" || domain == "" {
		return nil
	}
	// A sign-in in use lasts; one unused for its lifetime ends.
	db_open("db/users.db").exec("update tokens set expires=? where hash=?", now()+lemmy_lifetime, t.Hash)
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?) on conflict(hash) do update set used=excluded.used", t.Hash, t.User, now())
	return &lemmy_session{token: t, user: user, row: row, account: t.Account, handle: handle, domain: strings.ToLower(domain), actor: account_url(domain, t.Account)}
}

// lemmy_authenticate is the request's session, or writes the refusal.
func lemmy_authenticate(c *gin.Context) *lemmy_session {
	s := lemmy_session_of(lemmy_raw(c))
	if s == nil {
		lemmy_error(c, http.StatusUnauthorized, "not_logged_in")
	}
	return s
}

// lemmy_jwt is a sign-in's token: a JWT naming the person by their number
// and this server, as Lemmy's are, known to the server by its hash.
func lemmy_jwt(person int64, issuer string) string {
	encode := func(v map[string]any) string {
		return base64.RawURLEncoding.EncodeToString([]byte(json_encode(v)))
	}
	signature := make([]byte, 32)
	rand.Read(signature)
	return encode(map[string]any{"typ": "JWT", "alg": "HS256"}) + "." + encode(map[string]any{"sub": strconv.FormatInt(person, 10), "iss": issuer, "iat": now()}) + "." + base64.RawURLEncoding.EncodeToString(signature)
}

// lemmy_login signs an app in: the account's handle, or its address, and a
// password minted for it.
func lemmy_login(c *gin.Context) {
	in := lemmy_read(c)
	name := strings.TrimPrefix(strings.TrimSpace(in.text("username_or_email")), "@")
	if name == "" || in.text("password") == "" {
		lemmy_error(c, http.StatusBadRequest, "incorrect_login")
		return
	}
	if !strings.Contains(name, "@") {
		name += "@" + strings.ToLower(activitypub_host(c))
	}
	user, row, device := password_check("fediverse", name, in.text("password"))
	if user == nil || row == nil || account_group(row) {
		lemmy_error(c, http.StatusBadRequest, "incorrect_login")
		return
	}
	id, _ := row["id"].(string)
	data := account_data(row)
	domain, _ := data["domain"].(string)
	jwt := lemmy_jwt(number_for(user, "person", account_url(domain, id)), "https://"+strings.ToLower(activitypub_host(c)))
	db_open("db/users.db").exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires, kind, client, account, network, jkt, refresh, device) values (?, ?, '', ?, ?, '', '', ?, ?, ?, 'lemmy', ?, 'fediverse', '', '', '')",
		token_hash(jwt), user.UID, device, json_encode([]string{"read", "write", "follow"}), now(), now()+lemmy_lifetime, token_signin, id)
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?)", token_hash(jwt), user.UID, now())
	c.JSON(http.StatusOK, map[string]any{"jwt": jwt, "registration_created": false, "verify_email_sent": false})
}

// === Input ===

// A lemmy_input is a request's fields: its query, and its JSON body.
type lemmy_input struct {
	values map[string]any
}

// lemmy_read reads a request's fields.
func lemmy_read(c *gin.Context) *lemmy_input {
	in := &lemmy_input{values: map[string]any{}}
	for key, values := range c.Request.URL.Query() {
		if len(values) > 0 {
			in.values[key] = values[0]
		}
	}
	if c.Request.Method != "GET" && c.Request.Body != nil {
		data, _ := io.ReadAll(io.LimitReader(c.Request.Body, 1<<20))
		var body map[string]any
		if json.Unmarshal(data, &body) == nil {
			for key, value := range body {
				in.values[key] = value
			}
		}
	}
	return in
}

func (in *lemmy_input) text(key string) string {
	switch v := in.values[key].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case bool:
		return strconv.FormatBool(v)
	case nil:
		return ""
	}
	return fmt.Sprint(in.values[key])
}

func (in *lemmy_input) has(key string) bool {
	v, found := in.values[key]
	return found && v != nil
}

func (in *lemmy_input) number(key string) int64 {
	n, _ := strconv.ParseInt(in.text(key), 10, 64)
	return n
}

func (in *lemmy_input) flag(key string) bool {
	return in.text(key) == "true" || in.text(key) == "1"
}

// page is the page asked for, from 1, and its size.
func (in *lemmy_input) page(fallback int) (int, int) {
	page := int(in.number("page"))
	if cursor := in.text("page_cursor"); strings.HasPrefix(cursor, "Pa") {
		page, _ = strconv.Atoi(strings.TrimPrefix(cursor, "Pa"))
	}
	if page < 1 {
		page = 1
	}
	limit := int(in.number("limit"))
	if limit <= 0 {
		limit = fallback
	}
	return page, min(limit, 50)
}

// === Numbers ===

// lemmy_key is what a number names for the user, of a kind, or empty.
func lemmy_key(s *lemmy_session, kind string, number int64) string {
	if number <= 0 {
		return ""
	}
	return number_identifier(s.user, kind, number)
}

// lemmy_number is an item's number for the user.
func lemmy_number(s *lemmy_session, kind string, key string) int64 {
	if key == "" {
		return 0
	}
	return number_for(s.user, kind, key)
}

// lemmy_time is a time as Lemmy writes one.
func lemmy_time(unix int64) string {
	if unix <= 0 {
		unix = now()
	}
	return time.Unix(unix, 0).UTC().Format("2006-01-02T15:04:05.000000Z")
}

// lemmy_optional_time is a time, or nil where there is none.
func lemmy_optional_time(unix int64) any {
	if unix <= 0 {
		return nil
	}
	return lemmy_time(unix)
}

// lemmy_optional is a text, or nil where it is empty.
func lemmy_optional(text string) any {
	if text == "" {
		return nil
	}
	return text
}

// === Routing ===

func lemmy_route(c *gin.Context) bool {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type")
	if c.Request.Method == "OPTIONS" {
		c.AbortWithStatus(http.StatusNoContent)
		return true
	}
	if signin_issuer(c) == "" {
		lemmy_error(c, http.StatusNotFound, "not_found")
		return true
	}
	path := strings.TrimSuffix(c.Request.URL.Path, "/")
	if rest, ok := strings.CutPrefix(path, "/pictrs/image"); ok {
		lemmy_pictrs(c, strings.Trim(rest, "/"))
		return true
	}
	route := c.Request.Method + " " + strings.TrimPrefix(path, "/api/v3/")
	if lemmy_public(c, route) {
		return true
	}
	s := lemmy_authenticate(c)
	if s == nil {
		return true
	}
	if !lemmy_dispatch(c, s, route) {
		lemmy_error(c, http.StatusNotFound, "unknown")
	}
	return true
}

// lemmy_public answers what needs no sign-in: logging in, the site, and the
// closed doors of registration, reporting whether it did.
func lemmy_public(c *gin.Context, route string) bool {
	switch route {
	case "POST user/login":
		lemmy_login(c)
	case "POST user/register":
		lemmy_error(c, http.StatusBadRequest, "registration_closed")
	case "GET user/get_captcha":
		c.JSON(http.StatusOK, map[string]any{})
	case "POST user/password_reset", "POST user/password_change", "POST user/verify_email", "POST user/resend_verification_email":
		lemmy_error(c, http.StatusBadRequest, "registration_closed")
	case "GET site":
		lemmy_site(c, lemmy_session_of(lemmy_raw(c)))
	case "GET federated_instances":
		c.JSON(http.StatusOK, map[string]any{"federated_instances": map[string]any{"linked": []any{}, "allowed": []any{}, "blocked": []any{}}})
	case "POST user/logout":
		if raw := lemmy_raw(c); raw != "" {
			db_open("db/users.db").exec("delete from tokens where hash=? and client='lemmy'", token_hash(raw))
		}
		c.JSON(http.StatusOK, map[string]any{"success": true})
	default:
		return false
	}
	return true
}

// lemmy_dispatch answers a signed-in app's request, reporting whether it
// knew the method.
func lemmy_dispatch(c *gin.Context, s *lemmy_session, route string) bool {
	in := lemmy_read(c)
	switch route {
	case "GET user/validate_auth":
		c.JSON(http.StatusOK, map[string]any{"success": true})
	case "GET user/unread_count":
		lemmy_unread(c, s)
	case "GET user/report_count":
		lemmy_report_count(c, s, in)
	case "PUT user/save_user_settings":
		c.JSON(http.StatusOK, map[string]any{"success": true})
	case "PUT user/change_password", "POST user/delete_account", "POST user/totp/generate", "POST user/totp/update", "POST user/export_settings", "POST user/import_settings":
		lemmy_error(c, http.StatusBadRequest, "not_available")
	case "GET user":
		lemmy_person(c, s, in)
	case "GET user/replies", "GET user/mention":
		lemmy_inbox(c, s, in, route == "GET user/mention")
	case "POST comment/mark_as_read", "POST user/mention/mark_as_read":
		lemmy_inbox_read(c, s, in, route == "POST user/mention/mark_as_read")
	case "POST user/mark_all_as_read":
		lemmy_inbox_all_read(c, s)
	case "POST user/block":
		lemmy_person_block(c, s, in)
	case "GET community/list":
		lemmy_communities(c, s, in)
	case "GET community":
		lemmy_community(c, s, in)
	case "POST community/follow":
		lemmy_community_change(c, s, "follow", Map{"community": lemmy_key(s, "community", in.number("community_id")), "follow": in.flag("follow")})
	case "POST community/block":
		lemmy_community_change(c, s, "block", Map{"community": lemmy_key(s, "community", in.number("community_id")), "block": in.flag("block")})
	case "POST community/ban_user":
		lemmy_ban(c, s, in)
	case "POST community/mod":
		lemmy_moderator(c, s, in)
	case "POST community", "PUT community", "POST community/delete", "POST community/remove", "POST community/transfer", "POST community/hide":
		lemmy_error(c, http.StatusBadRequest, "not_available")
	case "GET post/list":
		lemmy_posts_page(c, s, in)
	case "GET post":
		lemmy_post_get(c, s, in)
	case "POST post":
		lemmy_post_write(c, s, "post/create", Map{"community": lemmy_key(s, "community", in.number("community_id")), "name": in.text("name"), "body": in.text("body"), "url": in.text("url"), "nsfw": in.flag("nsfw"), "language": lemmy_language_code(in.number("language_id"))})
	case "PUT post":
		args := Map{"post": lemmy_key(s, "post", in.number("post_id"))}
		for _, key := range []string{"name", "body", "url"} {
			if in.has(key) {
				args[key] = in.text(key)
			}
		}
		if in.has("nsfw") {
			args["nsfw"] = in.flag("nsfw")
		}
		if in.has("language_id") {
			args["language"] = lemmy_language_code(in.number("language_id"))
		}
		lemmy_post_write(c, s, "post/edit", args)
	case "POST post/delete":
		lemmy_post_write(c, s, "post/delete", Map{"post": lemmy_key(s, "post", in.number("post_id")), "deleted": in.flag("deleted")})
	case "POST post/remove":
		lemmy_post_write(c, s, "post/remove", Map{"post": lemmy_key(s, "post", in.number("post_id")), "removed": in.flag("removed"), "reason": in.text("reason")})
	case "POST post/lock":
		lemmy_post_write(c, s, "post/lock", Map{"post": lemmy_key(s, "post", in.number("post_id")), "locked": in.flag("locked")})
	case "POST post/feature":
		lemmy_post_write(c, s, "post/feature", Map{"post": lemmy_key(s, "post", in.number("post_id")), "featured": in.flag("featured")})
	case "POST post/like":
		lemmy_post_write(c, s, "post/vote", Map{"post": lemmy_key(s, "post", in.number("post_id")), "score": in.number("score")})
	case "PUT post/save":
		lemmy_post_write(c, s, "post/save", Map{"post": lemmy_key(s, "post", in.number("post_id")), "save": in.flag("save")})
	case "POST post/mark_as_read", "POST post/hide":
		c.JSON(http.StatusOK, map[string]any{"success": true})
	case "POST post/report":
		lemmy_report_create(c, s, "post", Map{"post": lemmy_key(s, "post", in.number("post_id")), "reason": in.text("reason")})
	case "GET post/site_metadata":
		c.JSON(http.StatusOK, map[string]any{"metadata": map[string]any{}})
	case "GET comment/list":
		lemmy_comments(c, s, in)
	case "GET comment":
		lemmy_comment_get(c, s, in)
	case "POST comment":
		parent := ""
		if in.has("parent_id") {
			parent = lemmy_key(s, "comment", in.number("parent_id"))
		}
		lemmy_comment_write(c, s, "comment/create", Map{"post": lemmy_key(s, "post", in.number("post_id")), "parent": parent, "content": in.text("content")})
	case "PUT comment":
		lemmy_comment_write(c, s, "comment/edit", Map{"comment": lemmy_key(s, "comment", in.number("comment_id")), "content": in.text("content")})
	case "POST comment/delete":
		lemmy_comment_write(c, s, "comment/delete", Map{"comment": lemmy_key(s, "comment", in.number("comment_id")), "deleted": in.flag("deleted")})
	case "POST comment/remove":
		lemmy_comment_write(c, s, "comment/remove", Map{"comment": lemmy_key(s, "comment", in.number("comment_id")), "removed": in.flag("removed"), "reason": in.text("reason")})
	case "POST comment/like":
		lemmy_comment_write(c, s, "comment/vote", Map{"comment": lemmy_key(s, "comment", in.number("comment_id")), "score": in.number("score")})
	case "PUT comment/save":
		lemmy_comment_write(c, s, "comment/save", Map{"comment": lemmy_key(s, "comment", in.number("comment_id")), "save": in.flag("save")})
	case "POST comment/distinguish":
		lemmy_error(c, http.StatusBadRequest, "not_available")
	case "POST comment/report":
		lemmy_report_create(c, s, "comment", Map{"comment": lemmy_key(s, "comment", in.number("comment_id")), "reason": in.text("reason")})
	case "GET post/report/list", "GET comment/report/list":
		lemmy_reports(c, s, in, strings.TrimSuffix(strings.TrimPrefix(route, "GET "), "/report/list"))
	case "PUT post/report/resolve", "PUT comment/report/resolve":
		lemmy_report_resolve(c, s, in, strings.TrimSuffix(strings.TrimPrefix(route, "PUT "), "/report/resolve"))
	case "GET private_message/report/list":
		c.JSON(http.StatusOK, map[string]any{"private_message_reports": []any{}})
	case "GET modlog":
		lemmy_modlog(c, s, in)
	case "GET search":
		lemmy_search(c, s, in)
	case "GET resolve_object":
		lemmy_resolve(c, s, in)
	case "GET private_message/list":
		lemmy_messages(c, s, in)
	case "POST private_message":
		lemmy_message_send(c, s, in)
	case "PUT private_message":
		lemmy_message_change(c, s, "edit", Map{"message": lemmy_key(s, "message", in.number("private_message_id")), "body": in.text("content")})
	case "POST private_message/delete":
		lemmy_message_change(c, s, "delete", Map{"message": lemmy_key(s, "message", in.number("private_message_id"))})
	case "POST private_message/mark_as_read":
		lemmy_message_change(c, s, "read", Map{"message": lemmy_key(s, "message", in.number("private_message_id"))})
	case "POST private_message/report":
		lemmy_error(c, http.StatusBadRequest, "not_available")
	default:
		if strings.HasPrefix(route, "GET admin/") || strings.HasPrefix(route, "POST admin/") || strings.HasPrefix(route, "PUT admin/") || strings.HasPrefix(route, "POST site") || strings.HasPrefix(route, "PUT site") {
			lemmy_error(c, http.StatusBadRequest, "not_an_admin")
			return true
		}
		return false
	}
	return true
}

// === Calling the apps ===

// lemmy_caller runs an app's function as the user; a test stands its own in.
var lemmy_caller = func(s *lemmy_session, a *App, function string, args Map) (any, error) {
	return app_call_as_server(s.user, a, function, args)
}

// lemmy_call calls a function of the app that keeps the communities, or of
// the one that keeps conversations for direct/*, as the account.
func lemmy_call(s *lemmy_session, function string, args Map) (any, error) {
	name := function
	if !strings.HasPrefix(function, "direct/") {
		name = "lemmy/" + function
	}
	found := apps_for_function(s.user, name)
	if len(found) == 0 {
		return nil, fmt.Errorf("no app here keeps it")
	}
	args["account"] = s.account
	return lemmy_caller(s, found[0], name, args)
}

// lemmy_ask calls a function and answers what it answered, as lemmy_answer.
func lemmy_ask(c *gin.Context, s *lemmy_session, function string, args Map) map[string]any {
	result, err := lemmy_call(s, function, args)
	return lemmy_answer(c, result, err)
}

// lemmy_answer is a function's answer as a map, or writes the refusal: its
// error, which the apps say is a Lemmy error's name where it is one.
func lemmy_answer(c *gin.Context, result any, err error) map[string]any {
	m, _ := result.(map[string]any)
	if err != nil || m == nil {
		lemmy_error(c, http.StatusBadRequest, "couldnt_find_object")
		return nil
	}
	if failure := mastodon_string(m, "error"); failure != "" {
		lemmy_error(c, http.StatusBadRequest, failure)
		return nil
	}
	return m
}

// === People ===

// lemmy_person_of is someone as Lemmy's person: the user's account, or one
// the forums app names, with the number the user's apps know them by.
func lemmy_person_of(s *lemmy_session, p map[string]any) map[string]any {
	if p == nil || p["mine"] == true {
		return lemmy_self(s)
	}
	key := mastodon_string(p, "id")
	name := mastodon_string(p, "name")
	if name == "" {
		name = mastodon_slug(mastodon_string(p, "display"))
	}
	// Apps read the server a person is on from their actor's address, so
	// someone known only here is given one on this server.
	url := mastodon_string(p, "url")
	if !strings.HasPrefix(url, "https://") && !strings.HasPrefix(url, "http://") {
		url = "https://" + s.domain + "/u/" + name
	}
	return map[string]any{
		"id":           lemmy_number(s, "person", key),
		"name":         name,
		"display_name": lemmy_optional(mastodon_string(p, "display")),
		"avatar":       lemmy_optional(mastodon_string(p, "avatar")),
		"banned":       p["banned"] == true,
		"published":    lemmy_time(mastodon_integer(p, "created")),
		"actor_id":     url,
		"local":        p["local"] == true,
		"deleted":      false,
		"bot_account":  p["bot"] == true,
		"instance_id":  1,
	}
}

// lemmy_self is the account as Lemmy's person.
func lemmy_self(s *lemmy_session) map[string]any {
	label, _ := s.row["label"].(string)
	return map[string]any{
		"id":           lemmy_number(s, "person", s.actor),
		"name":         s.handle,
		"display_name": lemmy_optional(label),
		"avatar":       nil,
		"banned":       false,
		"published":    lemmy_time(row_int(s.row, "created")),
		"actor_id":     s.actor,
		"local":        true,
		"deleted":      false,
		"bot_account":  false,
		"instance_id":  1,
	}
}

// lemmy_person_view is someone with their counts, as Lemmy's lists show a
// person.
func lemmy_person_view(person map[string]any, posts int64, comments int64) map[string]any {
	return map[string]any{"person": person, "counts": map[string]any{"person_id": person["id"], "post_count": posts, "comment_count": comments}, "is_admin": false}
}

// === The site ===

// lemmy_site answers the site: this server as a closed Lemmy instance, and
// for a signed-in app the account with what it follows, moderates and
// blocks.
func lemmy_site(c *gin.Context, s *lemmy_session) {
	host := strings.ToLower(activitypub_host(c))
	name := setting_effective("name")
	if name == "" {
		name = host
	}
	site := map[string]any{
		"id": 1, "name": name, "published": lemmy_time(1), "actor_id": "https://" + host + "/", "last_refreshed_at": lemmy_time(now()),
		"inbox_url": "https://" + host + "/inbox", "public_key": "", "instance_id": 1,
	}
	local := map[string]any{
		"id": 1, "site_id": 1, "site_setup": true, "enable_downvotes": true, "enable_nsfw": true, "community_creation_admin_only": true,
		"require_email_verification": false, "private_instance": false, "default_theme": "browser", "default_post_listing_type": "Subscribed",
		"hide_modlog_mod_names": false, "application_email_admins": false, "actor_name_max_length": 20, "federation_enabled": true,
		"captcha_enabled": false, "captcha_difficulty": "medium", "published": lemmy_time(1), "registration_mode": "Closed",
		"reports_email_admins": false, "federation_signed_fetch": true, "default_post_listing_mode": "List", "default_sort_type": "Active",
	}
	limits := map[string]any{"local_site_id": 1, "message": 180, "message_per_second": 60, "post": 6, "post_per_second": 600, "register": 3, "register_per_second": 3600,
		"image": 6, "image_per_second": 3600, "comment": 6, "comment_per_second": 600, "search": 60, "search_per_second": 600, "published": lemmy_time(1),
		"import_user_settings": 1, "import_user_settings_per_second": 86400}
	answer := map[string]any{
		"site_view": map[string]any{"site": site, "local_site": local, "local_site_rate_limit": limits,
			"counts": map[string]any{"site_id": 1, "users": 0, "posts": 0, "comments": 0, "communities": 0, "users_active_day": 0, "users_active_week": 0, "users_active_month": 0, "users_active_half_year": 0}},
		"admins": []any{}, "version": lemmy_version, "all_languages": lemmy_languages(), "discussion_languages": []any{},
		"taglines": []any{}, "custom_emojis": []any{}, "blocked_urls": []any{},
	}
	if s != nil {
		answer["my_user"] = lemmy_my_user(s)
	}
	c.JSON(http.StatusOK, answer)
}

// lemmy_my_user is the signed-in account as Lemmy describes its user: its
// settings, and the communities it follows, moderates and blocks.
func lemmy_my_user(s *lemmy_session) map[string]any {
	self := lemmy_self(s)
	follows, moderates, blocks := []any{}, []any{}, []any{}
	result, err := lemmy_call(s, "communities", Map{"type": "Subscribed", "page": 1, "limit": 500})
	if err == nil {
		for _, community := range mastodon_list(result) {
			view := lemmy_community_of(s, community)
			if mastodon_string(community, "subscribed") == "yes" {
				follows = append(follows, map[string]any{"community": view, "follower": self})
			}
			if community["moderator"] == true {
				moderates = append(moderates, map[string]any{"community": view, "moderator": self})
			}
			if community["blocked"] == true {
				blocks = append(blocks, map[string]any{"community": view, "person": self})
			}
		}
	}
	people := []any{}
	for _, actor := range lemmy_blocked_people(s) {
		people = append(people, map[string]any{"person": self, "target": lemmy_person_of(s, map[string]any{"id": actor, "url": actor, "name": mastodon_slug(strings.TrimPrefix(actor, "https://"))})})
	}
	local := map[string]any{
		"id": self["id"], "person_id": self["id"], "show_nsfw": true, "theme": "browser", "default_sort_type": "Active", "default_listing_type": "Subscribed",
		"interface_language": "browser", "show_avatars": true, "send_notifications_to_email": false, "show_scores": true, "show_bot_accounts": true,
		"show_read_posts": true, "email_verified": true, "accepted_application": true, "open_links_in_new_tab": false, "blur_nsfw": true, "auto_expand": false,
		"infinite_scroll_enabled": true, "admin": false, "post_listing_mode": "List", "totp_2fa_enabled": false, "enable_keyboard_navigation": false,
		"enable_animated_images": true, "collapse_bot_comments": false,
	}
	return map[string]any{
		"local_user_view": map[string]any{"local_user": local, "local_user_vote_display_mode": map[string]any{"local_user_id": self["id"], "score": true, "upvotes": true, "downvotes": true, "upvote_percentage": false},
			"person": self, "counts": map[string]any{"person_id": self["id"], "post_count": 0, "comment_count": 0}},
		"follows": follows, "moderates": moderates, "community_blocks": blocks, "instance_blocks": []any{}, "person_blocks": people, "discussion_languages": []any{},
	}
}

// lemmy_languages are the languages a post may be marked with, by Lemmy's
// numbers: undetermined first.
var lemmy_language_codes = []string{"und", "en", "de", "fr", "es", "it", "nl", "pt", "pl", "ru", "uk", "sv", "da", "nb", "fi", "cs", "hu", "ro", "tr", "el", "he", "ar", "fa", "hi", "ja", "ko", "zh", "id", "vi", "th", "eo"}

func lemmy_languages() []any {
	out := []any{}
	for i, code := range lemmy_language_codes {
		out = append(out, map[string]any{"id": i, "code": code, "name": code})
	}
	return out
}

// lemmy_language_code is a language's code by its number, or empty.
func lemmy_language_code(number int64) string {
	if number <= 0 || int(number) >= len(lemmy_language_codes) {
		return ""
	}
	return lemmy_language_codes[number]
}

// lemmy_language_number is a language's number by its code; 0 for none.
func lemmy_language_number(code string) int {
	code = strings.ToLower(strings.SplitN(code, "-", 2)[0])
	for i, held := range lemmy_language_codes {
		if held == code && i > 0 {
			return i
		}
	}
	return 0
}

// === Communities ===

// lemmy_community_of is a community as Lemmy's.
func lemmy_community_of(s *lemmy_session, community map[string]any) map[string]any {
	key := mastodon_string(community, "id")
	// A forum not published as a community is given an address on this
	// server, which apps read the server from.
	actor := mastodon_string(community, "url")
	if actor == "" {
		actor = "https://" + s.domain + "/c/" + mastodon_string(community, "name")
	}
	return map[string]any{
		"id":                         lemmy_number(s, "community", key),
		"name":                       mastodon_string(community, "name"),
		"title":                      mastodon_string(community, "title"),
		"description":                lemmy_optional(mastodon_string(community, "description")),
		"removed":                    false,
		"published":                  lemmy_time(mastodon_integer(community, "created")),
		"updated":                    lemmy_optional_time(mastodon_integer(community, "updated")),
		"deleted":                    false,
		"nsfw":                       community["nsfw"] == true,
		"actor_id":                   actor,
		"local":                      community["local"] == true,
		"icon":                       lemmy_optional(mastodon_string(community, "icon")),
		"banner":                     lemmy_optional(mastodon_string(community, "banner")),
		"hidden":                     false,
		"posting_restricted_to_mods": community["restricted"] == true,
		"instance_id":                1,
		"visibility":                 "Public",
	}
}

// lemmy_community_view is a community with the user's standing in it and its
// counts.
func lemmy_community_view(s *lemmy_session, community map[string]any) map[string]any {
	view := lemmy_community_of(s, community)
	subscribed := "NotSubscribed"
	switch mastodon_string(community, "subscribed") {
	case "yes":
		subscribed = "Subscribed"
	case "pending":
		subscribed = "Pending"
	}
	return map[string]any{
		"community":  view,
		"subscribed": subscribed,
		"blocked":    community["blocked"] == true,
		"counts": map[string]any{
			"community_id": view["id"], "subscribers": mastodon_integer(community, "members"), "subscribers_local": mastodon_integer(community, "members"),
			"posts": mastodon_integer(community, "posts"), "comments": mastodon_integer(community, "comments"), "published": view["published"],
			"users_active_day": 0, "users_active_week": 0, "users_active_month": 0, "users_active_half_year": 0,
		},
		"banned_from_community": community["banned"] == true,
	}
}

// lemmy_moderators are a community's moderators, as Lemmy lists them.
func lemmy_moderators(s *lemmy_session, community map[string]any) []any {
	view := lemmy_community_of(s, community)
	out := []any{}
	for _, person := range mastodon_list(community["moderators"]) {
		out = append(out, map[string]any{"community": view, "moderator": lemmy_person_of(s, person)})
	}
	return out
}

func lemmy_communities(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	page, limit := in.page(20)
	result, err := lemmy_call(s, "communities", Map{"type": in.text("type_"), "sort": in.text("sort"), "page": page, "limit": limit})
	if err != nil {
		lemmy_error(c, http.StatusBadRequest, "couldnt_find_community")
		return
	}
	out := []any{}
	for _, community := range mastodon_list(result) {
		out = append(out, lemmy_community_view(s, community))
	}
	c.JSON(http.StatusOK, map[string]any{"communities": out})
}

func lemmy_community(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	args := Map{}
	if in.has("id") {
		args["community"] = lemmy_key(s, "community", in.number("id"))
		if args["community"] == "" {
			lemmy_error(c, http.StatusNotFound, "couldnt_find_community")
			return
		}
	} else {
		args["name"] = in.text("name")
	}
	community := lemmy_ask(c, s, "community", args)
	if community == nil {
		return
	}
	c.JSON(http.StatusOK, map[string]any{"community_view": lemmy_community_view(s, community), "moderators": lemmy_moderators(s, community), "discussion_languages": []any{}})
}

// lemmy_community_change follows, leaves, blocks or unblocks a community.
func lemmy_community_change(c *gin.Context, s *lemmy_session, function string, args Map) {
	if args["community"] == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_community")
		return
	}
	community := lemmy_ask(c, s, function, args)
	if community == nil {
		return
	}
	answer := map[string]any{"community_view": lemmy_community_view(s, community), "discussion_languages": []any{}}
	if function == "block" {
		answer["blocked"] = community["blocked"] == true
	}
	c.JSON(http.StatusOK, answer)
}

func lemmy_ban(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	person := lemmy_key(s, "person", in.number("person_id"))
	community := lemmy_key(s, "community", in.number("community_id"))
	if person == "" || community == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_person")
		return
	}
	expires := in.number("expires")
	answer := lemmy_ask(c, s, "ban", Map{"community": community, "person": person, "ban": in.flag("ban"), "reason": in.text("reason"), "expires": expires, "remove": in.flag("remove_data")})
	if answer == nil {
		return
	}
	who, _ := answer["person"].(map[string]any)
	c.JSON(http.StatusOK, map[string]any{"person_view": lemmy_person_view(lemmy_person_of(s, who), 0, 0), "banned": in.flag("ban")})
}

func lemmy_moderator(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	person := lemmy_key(s, "person", in.number("person_id"))
	community := lemmy_key(s, "community", in.number("community_id"))
	if person == "" || community == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_person")
		return
	}
	answer := lemmy_ask(c, s, "moderator", Map{"community": community, "person": person, "added": in.flag("added")})
	if answer == nil {
		return
	}
	c.JSON(http.StatusOK, map[string]any{"moderators": lemmy_moderators(s, answer)})
}

// === Posts ===

// lemmy_post_of is a post as Lemmy's.
func lemmy_post_of(s *lemmy_session, post map[string]any) map[string]any {
	key := mastodon_string(post, "id")
	community, _ := post["community"].(map[string]any)
	creator, _ := post["creator"].(map[string]any)
	ap := mastodon_string(post, "remote")
	if ap == "" {
		ap = mastodon_string(post, "link")
	}
	return map[string]any{
		"id":                 lemmy_number(s, "post", key),
		"name":               mastodon_string(post, "title"),
		"url":                lemmy_optional(mastodon_string(post, "url")),
		"body":               lemmy_optional(mastodon_string(post, "body")),
		"creator_id":         lemmy_person_of(s, creator)["id"],
		"community_id":       lemmy_number(s, "community", mastodon_string(community, "id")),
		"removed":            post["removed"] == true,
		"locked":             post["locked"] == true,
		"published":          lemmy_time(mastodon_integer(post, "created")),
		"updated":            lemmy_optional_time(mastodon_integer(post, "edited")),
		"deleted":            post["deleted"] == true,
		"nsfw":               post["nsfw"] == true,
		"embed_title":        lemmy_optional(mastodon_string(post, "embed_title")),
		"embed_description":  lemmy_optional(mastodon_string(post, "embed_description")),
		"thumbnail_url":      lemmy_optional(mastodon_string(post, "thumbnail")),
		"ap_id":              ap,
		"local":              community["local"] == true,
		"language_id":        lemmy_language_number(mastodon_string(post, "language")),
		"featured_community": post["pinned"] == true,
		"featured_local":     false,
	}
}

// lemmy_vote is a vote as Lemmy's score, or nil for none.
func lemmy_vote(item map[string]any) any {
	switch mastodon_integer(item, "vote") {
	case 1:
		return 1
	case -1:
		return -1
	}
	return nil
}

// lemmy_post_view is a post with its creator, community, counts and the
// user's vote and save.
func lemmy_post_view(s *lemmy_session, post map[string]any) map[string]any {
	community, _ := post["community"].(map[string]any)
	creator, _ := post["creator"].(map[string]any)
	view := lemmy_post_of(s, post)
	up, down := mastodon_integer(post, "up"), mastodon_integer(post, "down")
	answer := map[string]any{
		"post":                          view,
		"creator":                       lemmy_person_of(s, creator),
		"community":                     lemmy_community_of(s, community),
		"creator_banned_from_community": creator["banned"] == true,
		"banned_from_community":         community["banned"] == true,
		"creator_is_moderator":          creator["moderator"] == true,
		"creator_is_admin":              false,
		"counts": map[string]any{"post_id": view["id"], "comments": mastodon_integer(post, "comments"), "score": up - down, "upvotes": up, "downvotes": down,
			"published": view["published"], "newest_comment_time": lemmy_time(max(mastodon_integer(post, "latest"), mastodon_integer(post, "created")))},
		"subscribed":      "NotSubscribed",
		"saved":           post["saved"] == true,
		"read":            false,
		"hidden":          false,
		"creator_blocked": false,
		"unread_comments": 0,
	}
	if community["subscribed"] == "yes" {
		answer["subscribed"] = "Subscribed"
	}
	if vote := lemmy_vote(post); vote != nil {
		answer["my_vote"] = vote
	}
	return answer
}

func lemmy_posts_page(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	page, limit := in.page(20)
	args := Map{"type": in.text("type_"), "sort": in.text("sort"), "page": page, "limit": limit, "saved": in.flag("saved_only"), "liked": in.flag("liked_only")}
	if in.has("community_id") {
		args["community"] = lemmy_key(s, "community", in.number("community_id"))
		if args["community"] == "" {
			lemmy_error(c, http.StatusNotFound, "couldnt_find_community")
			return
		}
	} else if in.has("community_name") {
		args["name"] = in.text("community_name")
	}
	result, err := lemmy_call(s, "posts", args)
	if err != nil {
		lemmy_error(c, http.StatusBadRequest, "couldnt_get_posts")
		return
	}
	out := []any{}
	for _, post := range mastodon_list(result) {
		out = append(out, lemmy_post_view(s, post))
	}
	answer := map[string]any{"posts": out}
	if len(out) == limit {
		answer["next_page"] = "Pa" + strconv.Itoa(page+1)
	}
	c.JSON(http.StatusOK, answer)
}

func lemmy_post_get(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	key := lemmy_key(s, "post", in.number("id"))
	if key == "" && in.has("comment_id") {
		if comment := lemmy_ask(c, s, "comment", Map{"comment": lemmy_key(s, "comment", in.number("comment_id"))}); comment != nil {
			post, _ := comment["post"].(map[string]any)
			key = mastodon_string(post, "id")
		} else {
			return
		}
	}
	if key == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_post")
		return
	}
	post := lemmy_ask(c, s, "post", Map{"post": key})
	if post == nil {
		return
	}
	community, _ := post["community"].(map[string]any)
	c.JSON(http.StatusOK, map[string]any{"post_view": lemmy_post_view(s, post), "community_view": lemmy_community_view(s, community), "moderators": lemmy_moderators(s, community), "cross_posts": []any{}})
}

// lemmy_post_write makes, changes or acts on a post, and answers it as it
// stands.
func lemmy_post_write(c *gin.Context, s *lemmy_session, function string, args Map) {
	if key, set := args["post"]; set && key == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_post")
		return
	}
	if key, set := args["community"]; set && key == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_community")
		return
	}
	post := lemmy_ask(c, s, function, args)
	if post == nil {
		return
	}
	c.JSON(http.StatusOK, map[string]any{"post_view": lemmy_post_view(s, post)})
}

// === Comments ===

// lemmy_comment_of is a comment as Lemmy's: its path the numbers of the
// comments above it, from the root.
func lemmy_comment_of(s *lemmy_session, comment map[string]any) map[string]any {
	key := mastodon_string(comment, "id")
	post, _ := comment["post"].(map[string]any)
	creator, _ := comment["creator"].(map[string]any)
	number := lemmy_number(s, "comment", key)
	path := []string{"0"}
	for _, above := range mastodon_list_strings(comment["path"]) {
		path = append(path, strconv.FormatInt(lemmy_number(s, "comment", above), 10))
	}
	path = append(path, strconv.FormatInt(number, 10))
	ap := mastodon_string(comment, "remote")
	if ap == "" {
		ap = mastodon_string(post, "link") + "#" + key
	}
	community, _ := post["community"].(map[string]any)
	return map[string]any{
		"id":            number,
		"creator_id":    lemmy_person_of(s, creator)["id"],
		"post_id":       lemmy_number(s, "post", mastodon_string(post, "id")),
		"content":       mastodon_string(comment, "body"),
		"removed":       comment["removed"] == true,
		"published":     lemmy_time(mastodon_integer(comment, "created")),
		"updated":       lemmy_optional_time(mastodon_integer(comment, "edited")),
		"deleted":       comment["deleted"] == true,
		"ap_id":         ap,
		"local":         community["local"] == true,
		"path":          strings.Join(path, "."),
		"distinguished": false,
		"language_id":   0,
	}
}

// lemmy_comment_view is a comment with its creator, post, community, counts
// and the user's vote and save.
func lemmy_comment_view(s *lemmy_session, comment map[string]any) map[string]any {
	post, _ := comment["post"].(map[string]any)
	community, _ := post["community"].(map[string]any)
	creator, _ := comment["creator"].(map[string]any)
	view := lemmy_comment_of(s, comment)
	up, down := mastodon_integer(comment, "up"), mastodon_integer(comment, "down")
	answer := map[string]any{
		"comment":                       view,
		"creator":                       lemmy_person_of(s, creator),
		"post":                          lemmy_post_of(s, post),
		"community":                     lemmy_community_of(s, community),
		"counts":                        map[string]any{"comment_id": view["id"], "score": up - down, "upvotes": up, "downvotes": down, "published": view["published"], "child_count": mastodon_integer(comment, "children")},
		"creator_banned_from_community": creator["banned"] == true,
		"banned_from_community":         community["banned"] == true,
		"creator_is_moderator":          creator["moderator"] == true,
		"creator_is_admin":              false,
		"subscribed":                    "NotSubscribed",
		"saved":                         comment["saved"] == true,
		"creator_blocked":               false,
	}
	if community["subscribed"] == "yes" {
		answer["subscribed"] = "Subscribed"
	}
	if vote := lemmy_vote(comment); vote != nil {
		answer["my_vote"] = vote
	}
	return answer
}

func lemmy_comments(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	page, limit := in.page(50)
	args := Map{"type": in.text("type_"), "sort": in.text("sort"), "page": page, "limit": limit, "saved": in.flag("saved_only"), "liked": in.flag("liked_only"), "depth": in.number("max_depth")}
	if in.has("post_id") {
		args["post"] = lemmy_key(s, "post", in.number("post_id"))
		if args["post"] == "" {
			lemmy_error(c, http.StatusNotFound, "couldnt_find_post")
			return
		}
	}
	if in.has("parent_id") {
		args["parent"] = lemmy_key(s, "comment", in.number("parent_id"))
	}
	if in.has("community_id") {
		args["community"] = lemmy_key(s, "community", in.number("community_id"))
	}
	result, err := lemmy_call(s, "comments", args)
	if err != nil {
		lemmy_error(c, http.StatusBadRequest, "couldnt_get_comments")
		return
	}
	out := []any{}
	for _, comment := range mastodon_list(result) {
		out = append(out, lemmy_comment_view(s, comment))
	}
	c.JSON(http.StatusOK, map[string]any{"comments": out})
}

func lemmy_comment_get(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	key := lemmy_key(s, "comment", in.number("id"))
	if key == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_comment")
		return
	}
	comment := lemmy_ask(c, s, "comment", Map{"comment": key})
	if comment == nil {
		return
	}
	c.JSON(http.StatusOK, map[string]any{"comment_view": lemmy_comment_view(s, comment), "recipient_ids": []any{}})
}

// lemmy_comment_write makes, changes or acts on a comment, and answers it as
// it stands.
func lemmy_comment_write(c *gin.Context, s *lemmy_session, function string, args Map) {
	if key, set := args["comment"]; set && key == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_comment")
		return
	}
	if key, set := args["post"]; set && key == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_post")
		return
	}
	comment := lemmy_ask(c, s, function, args)
	if comment == nil {
		return
	}
	c.JSON(http.StatusOK, map[string]any{"comment_view": lemmy_comment_view(s, comment), "recipient_ids": []any{}})
}

// === Reports ===

// lemmy_report_view is a report as Lemmy's, of a post or a comment.
func lemmy_report_view(s *lemmy_session, report map[string]any) (string, map[string]any) {
	kind := mastodon_string(report, "kind")
	reporter, _ := report["reporter"].(map[string]any)
	resolver, _ := report["resolver"].(map[string]any)
	key := mastodon_string(report, "id")
	common := map[string]any{
		"id":          lemmy_number(s, "report", key),
		"creator_id":  lemmy_person_of(s, reporter)["id"],
		"reason":      mastodon_string(report, "reason"),
		"resolved":    report["resolved"] == true,
		"resolver_id": nil,
		"published":   lemmy_time(mastodon_integer(report, "created")),
		"updated":     lemmy_optional_time(mastodon_integer(report, "updated")),
	}
	if resolver != nil {
		common["resolver_id"] = lemmy_person_of(s, resolver)["id"]
	}
	view := map[string]any{"creator": lemmy_person_of(s, reporter), "creator_is_moderator": false, "creator_is_admin": false, "creator_blocked": false, "subscribed": "NotSubscribed", "saved": false}
	if resolver != nil {
		view["resolver"] = lemmy_person_of(s, resolver)
	}
	if kind == "comment" {
		comment, _ := report["comment"].(map[string]any)
		cv := lemmy_comment_view(s, comment)
		common["comment_id"] = mastodon_integer(cv["comment"].(map[string]any), "id")
		common["original_comment_text"] = mastodon_string(comment, "body")
		view["comment_report"], view["comment"], view["post"], view["community"] = common, cv["comment"], cv["post"], cv["community"]
		view["comment_creator"], view["counts"] = cv["creator"], cv["counts"]
		view["creator_banned_from_community"] = false
		return "comment_report_view", view
	}
	post, _ := report["post"].(map[string]any)
	pv := lemmy_post_view(s, post)
	common["post_id"] = mastodon_integer(pv["post"].(map[string]any), "id")
	common["original_post_name"] = mastodon_string(post, "title")
	common["original_post_url"] = lemmy_optional(mastodon_string(post, "url"))
	common["original_post_body"] = lemmy_optional(mastodon_string(post, "body"))
	view["post_report"], view["post"], view["community"] = common, pv["post"], pv["community"]
	view["post_creator"], view["counts"] = pv["creator"], pv["counts"]
	view["creator_banned_from_community"] = false
	return "post_report_view", view
}

func lemmy_report_create(c *gin.Context, s *lemmy_session, kind string, args Map) {
	if args[kind] == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_"+kind)
		return
	}
	report := lemmy_ask(c, s, kind+"/report", args)
	if report == nil {
		return
	}
	name, view := lemmy_report_view(s, report)
	c.JSON(http.StatusOK, map[string]any{name: view})
}

func lemmy_reports(c *gin.Context, s *lemmy_session, in *lemmy_input, kind string) {
	page, limit := in.page(20)
	args := Map{"kind": kind, "unresolved": in.flag("unresolved_only"), "page": page, "limit": limit}
	if in.has("community_id") {
		args["community"] = lemmy_key(s, "community", in.number("community_id"))
	}
	result, err := lemmy_call(s, "reports", args)
	if err != nil {
		lemmy_error(c, http.StatusBadRequest, "couldnt_find_object")
		return
	}
	out := []any{}
	for _, report := range mastodon_list(result) {
		_, view := lemmy_report_view(s, report)
		out = append(out, view)
	}
	c.JSON(http.StatusOK, map[string]any{kind + "_reports": out})
}

func lemmy_report_resolve(c *gin.Context, s *lemmy_session, in *lemmy_input, kind string) {
	key := lemmy_key(s, "report", in.number("report_id"))
	if key == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_object")
		return
	}
	report := lemmy_ask(c, s, "report/resolve", Map{"report": key, "resolved": in.flag("resolved")})
	if report == nil {
		return
	}
	name, view := lemmy_report_view(s, report)
	c.JSON(http.StatusOK, map[string]any{name: view})
}

func lemmy_report_count(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	args := Map{"kind": "", "unresolved": true, "page": 1, "limit": 500}
	if in.has("community_id") {
		args["community"] = lemmy_key(s, "community", in.number("community_id"))
	}
	result, _ := lemmy_call(s, "reports", args)
	posts, comments := 0, 0
	for _, report := range mastodon_list(result) {
		if mastodon_string(report, "kind") == "comment" {
			comments++
		} else {
			posts++
		}
	}
	answer := map[string]any{"comment_reports": comments, "post_reports": posts, "private_message_reports": 0}
	if in.has("community_id") {
		answer["community_id"] = in.number("community_id")
	}
	c.JSON(http.StatusOK, answer)
}

// lemmy_modlog is the moderation log of the communities the user moderates,
// or of one, in Lemmy's sections.
func lemmy_modlog(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	page, limit := in.page(20)
	args := Map{"page": page, "limit": limit}
	if in.has("community_id") {
		args["community"] = lemmy_key(s, "community", in.number("community_id"))
	}
	result, _ := lemmy_call(s, "modlog", args)
	sections := map[string][]any{}
	for _, name := range []string{"removed_posts", "locked_posts", "featured_posts", "removed_comments", "removed_communities", "banned_from_community", "banned", "added_to_community", "transferred_to_community", "added", "admin_purged_persons", "admin_purged_communities", "admin_purged_posts", "admin_purged_comments", "hidden_communities"} {
		sections[name] = []any{}
	}
	for _, entry := range mastodon_list(result) {
		moderator, _ := entry["moderator"].(map[string]any)
		community, _ := entry["community"].(map[string]any)
		number := lemmy_number(s, "modlog", mastodon_string(entry, "id"))
		when := lemmy_time(mastodon_integer(entry, "created"))
		reason := lemmy_optional(mastodon_string(entry, "reason"))
		undone := entry["undone"] == true
		base := map[string]any{"moderator": lemmy_person_of(s, moderator), "community": lemmy_community_of(s, community)}
		switch mastodon_string(entry, "kind") {
		case "post/remove", "post/lock", "post/feature":
			post, _ := entry["post"].(map[string]any)
			view := lemmy_post_of(s, post)
			record := map[string]any{"id": number, "mod_person_id": base["moderator"].(map[string]any)["id"], "post_id": view["id"], "when_": when}
			name := "removed_posts"
			switch mastodon_string(entry, "kind") {
			case "post/remove":
				record["reason"], record["removed"] = reason, !undone
			case "post/lock":
				name, record["locked"] = "locked_posts", !undone
			case "post/feature":
				name, record["featured"], record["is_featured_community"] = "featured_posts", !undone, true
			}
			base["post"] = view
			base[map[string]string{"removed_posts": "mod_remove_post", "locked_posts": "mod_lock_post", "featured_posts": "mod_feature_post"}[name]] = record
			sections[name] = append(sections[name], base)
		case "comment/remove":
			comment, _ := entry["comment"].(map[string]any)
			cv := lemmy_comment_view(s, comment)
			base["mod_remove_comment"] = map[string]any{"id": number, "mod_person_id": base["moderator"].(map[string]any)["id"], "comment_id": mastodon_integer(cv["comment"].(map[string]any), "id"), "reason": reason, "removed": !undone, "when_": when}
			base["comment"], base["commenter"], base["post"] = cv["comment"], cv["creator"], cv["post"]
			sections["removed_comments"] = append(sections["removed_comments"], base)
		case "ban":
			person, _ := entry["person"].(map[string]any)
			banned := lemmy_person_of(s, person)
			base["mod_ban_from_community"] = map[string]any{"id": number, "mod_person_id": base["moderator"].(map[string]any)["id"], "other_person_id": banned["id"], "community_id": base["community"].(map[string]any)["id"], "reason": reason, "banned": !undone, "expires": lemmy_optional_time(mastodon_integer(entry, "expires")), "when_": when}
			base["banned_person"] = banned
			sections["banned_from_community"] = append(sections["banned_from_community"], base)
		case "moderator":
			person, _ := entry["person"].(map[string]any)
			added := lemmy_person_of(s, person)
			base["mod_add_community"] = map[string]any{"id": number, "mod_person_id": base["moderator"].(map[string]any)["id"], "other_person_id": added["id"], "community_id": base["community"].(map[string]any)["id"], "removed": undone, "when_": when}
			base["modded_person"] = added
			sections["added_to_community"] = append(sections["added_to_community"], base)
		}
	}
	answer := map[string]any{}
	for name, items := range sections {
		answer[name] = items
	}
	c.JSON(http.StatusOK, answer)
}

// === Search ===

func lemmy_search(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	page, limit := in.page(20)
	kind := in.text("type_")
	if kind == "" {
		kind = "All"
	}
	args := Map{"query": in.text("q"), "type": kind, "page": page, "limit": limit}
	if in.has("community_id") {
		args["community"] = lemmy_key(s, "community", in.number("community_id"))
	}
	answer := map[string]any{"type_": kind, "comments": []any{}, "posts": []any{}, "communities": []any{}, "users": []any{}}
	result, err := lemmy_call(s, "search", args)
	found := mastodon_map(result)
	if err != nil || found == nil {
		c.JSON(http.StatusOK, answer)
		return
	}
	posts, comments, communities := []any{}, []any{}, []any{}
	for _, post := range mastodon_list(found["posts"]) {
		posts = append(posts, lemmy_post_view(s, post))
	}
	for _, comment := range mastodon_list(found["comments"]) {
		comments = append(comments, lemmy_comment_view(s, comment))
	}
	for _, community := range mastodon_list(found["communities"]) {
		communities = append(communities, lemmy_community_view(s, community))
	}
	answer["posts"], answer["comments"], answer["communities"] = posts, comments, communities
	c.JSON(http.StatusOK, answer)
}

// lemmy_resolve finds what an address or a link names: a community, held or
// fetched from its server, a post or comment the user's communities hold,
// or a person.
func lemmy_resolve(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	query := strings.TrimSpace(in.text("q"))
	if query == "" {
		lemmy_error(c, http.StatusBadRequest, "couldnt_find_object")
		return
	}
	result, err := lemmy_call(s, "resolve", Map{"query": query})
	found := mastodon_map(result)
	if err == nil && found != nil && mastodon_string(found, "error") == "" {
		switch {
		case found["community"] != nil:
			c.JSON(http.StatusOK, map[string]any{"community": lemmy_community_view(s, mastodon_map(found["community"]))})
			return
		case found["post"] != nil:
			c.JSON(http.StatusOK, map[string]any{"post": lemmy_post_view(s, mastodon_map(found["post"]))})
			return
		case found["comment"] != nil:
			c.JSON(http.StatusOK, map[string]any{"comment": lemmy_comment_view(s, mastodon_map(found["comment"]))})
			return
		}
	}
	// A person is found as the account finds anyone.
	if strings.HasPrefix(query, "@") || strings.HasPrefix(query, "https://") {
		ctx := activitypub_signing(c.Request.Context(), s.user, s.account)
		if r, err := author_resolve(ctx, query); err == nil && r != nil && r.Network == "fediverse" && r.Type != "group" && host_allowed(s.user, "fediverse", identifier_host(r.ID)) == nil {
			person := lemmy_person_of(s, map[string]any{"id": r.ID, "name": r.Username, "display": r.Name, "avatar": r.Avatar, "url": r.ID, "bot": r.Type == "service"})
			c.JSON(http.StatusOK, map[string]any{"person": lemmy_person_view(person, 0, 0)})
			return
		}
	}
	lemmy_error(c, http.StatusBadRequest, "couldnt_find_object")
}

// === People ===

// lemmy_person answers someone's page: the account's own with its posts and
// comments, or another's with theirs in the user's communities.
func lemmy_person(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	page, limit := in.page(20)
	key := lemmy_key(s, "person", in.number("person_id"))
	if key == "" && in.has("username") {
		name := strings.TrimPrefix(in.text("username"), "@")
		if name == s.handle || strings.EqualFold(name, s.handle+"@"+s.domain) {
			key = s.actor
		} else {
			ctx := activitypub_signing(c.Request.Context(), s.user, s.account)
			if !strings.Contains(name, "@") {
				name += "@" + s.domain
			}
			if r, err := author_resolve(ctx, "@"+name); err == nil && r != nil {
				key = r.ID
			}
		}
	}
	if key == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_person")
		return
	}
	result, err := lemmy_call(s, "person", Map{"person": key, "self": key == s.actor, "page": page, "limit": limit, "saved": in.flag("saved_only"), "sort": in.text("sort")})
	found := mastodon_map(result)
	if err != nil || found == nil {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_person")
		return
	}
	posts, comments, moderates := []any{}, []any{}, []any{}
	for _, post := range mastodon_list(found["posts"]) {
		posts = append(posts, lemmy_post_view(s, post))
	}
	for _, comment := range mastodon_list(found["comments"]) {
		comments = append(comments, lemmy_comment_view(s, comment))
	}
	var person map[string]any
	if key == s.actor {
		person = lemmy_self(s)
	} else {
		who := mastodon_map(found["person"])
		if who == nil {
			who = map[string]any{"id": key, "url": key}
		}
		person = lemmy_person_of(s, who)
	}
	for _, community := range mastodon_list(found["moderates"]) {
		moderates = append(moderates, map[string]any{"community": lemmy_community_of(s, community), "moderator": person})
	}
	c.JSON(http.StatusOK, map[string]any{"person_view": lemmy_person_view(person, mastodon_integer(found, "post_count"), mastodon_integer(found, "comment_count")), "moderates": moderates, "posts": posts, "comments": comments})
}

// lemmy_blocked_people are the people the user has blocked on the fediverse.
func lemmy_blocked_people(s *lemmy_session) []string {
	rows, _ := db_user(s.user, "user").rows("select actor from blocked where actor!='' order by created")
	out := []string{}
	for _, row := range rows {
		out = append(out, row_string(row, "actor"))
	}
	return out
}

// lemmy_person_block blocks or unblocks someone, as the user's other blocks
// on the fediverse.
func lemmy_person_block(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	key := lemmy_key(s, "person", in.number("person_id"))
	if key == "" || !strings.HasPrefix(key, "https://") {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_person")
		return
	}
	block := in.flag("block")
	if block {
		blocked_add(s.user, key)
	} else {
		blocked_remove(s.user, key)
	}
	person := lemmy_person_of(s, map[string]any{"id": key, "url": key, "name": mastodon_slug(strings.TrimPrefix(key, "https://"))})
	c.JSON(http.StatusOK, map[string]any{"person_view": lemmy_person_view(person, 0, 0), "blocked": block})
}

// === The inbox ===

// lemmy_read_set are the replies or mentions the user's apps have marked
// read: kept by the engine, as Lemmy's server keeps them.
func lemmy_read_set(s *lemmy_session, kind string) map[string]bool {
	held := mastodon_snapshot_get(s.user, "lemmy", kind+"/"+s.account)
	out := map[string]bool{}
	for _, id := range mastodon_list_strings(held["read"]) {
		out[id] = true
	}
	return out
}

// lemmy_read_mark marks replies or mentions read, or unread, by comment.
func lemmy_read_mark(s *lemmy_session, kind string, comments []string, read bool) {
	set := lemmy_read_set(s, kind)
	for _, id := range comments {
		if read {
			set[id] = true
		} else {
			delete(set, id)
		}
	}
	var list []string
	for id := range set {
		list = append(list, id)
	}
	// The oldest marks go first once there are many; a reply that old is
	// past the inbox's pages.
	if len(list) > 5000 {
		list = list[len(list)-5000:]
	}
	mastodon_snapshot(s.user, "lemmy", kind+"/"+s.account, map[string]any{"read": list})
}

// lemmy_inbox_items are the comments answering the user's posts and comments,
// or mentioning them.
func lemmy_inbox_items(s *lemmy_session, mentions bool, page int, limit int) []map[string]any {
	kind := "replies"
	if mentions {
		kind = "mentions"
	}
	result, err := lemmy_call(s, kind, Map{"page": page, "limit": limit})
	if err != nil {
		return nil
	}
	return mastodon_list(result)
}

func lemmy_inbox(c *gin.Context, s *lemmy_session, in *lemmy_input, mentions bool) {
	page, limit := in.page(20)
	kind, record, list := "replies", "comment_reply", "replies"
	if mentions {
		kind, record, list = "mentions", "person_mention", "mentions"
	}
	read := lemmy_read_set(s, kind)
	self := lemmy_self(s)
	out := []any{}
	for _, comment := range lemmy_inbox_items(s, mentions, page, limit) {
		id := mastodon_string(comment, "id")
		if in.flag("unread_only") && read[id] {
			continue
		}
		view := lemmy_comment_view(s, comment)
		view[record] = map[string]any{"id": lemmy_number(s, kind, id), "recipient_id": self["id"], "comment_id": mastodon_integer(view["comment"].(map[string]any), "id"), "read": read[id], "published": mastodon_string(view["comment"].(map[string]any), "published")}
		view["recipient"] = self
		out = append(out, view)
	}
	c.JSON(http.StatusOK, map[string]any{list: out})
}

func lemmy_inbox_read(c *gin.Context, s *lemmy_session, in *lemmy_input, mentions bool) {
	kind, field, record := "replies", "comment_reply_id", "comment_reply_view"
	if mentions {
		kind, field, record = "mentions", "person_mention_id", "person_mention_view"
	}
	id := lemmy_key(s, kind, in.number(field))
	if id == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_object")
		return
	}
	lemmy_read_mark(s, kind, []string{id}, in.flag("read"))
	comment := lemmy_ask(c, s, "comment", Map{"comment": id})
	if comment == nil {
		return
	}
	view := lemmy_comment_view(s, comment)
	self := lemmy_self(s)
	name := "comment_reply"
	if mentions {
		name = "person_mention"
	}
	view[name] = map[string]any{"id": in.number(field), "recipient_id": self["id"], "comment_id": mastodon_integer(view["comment"].(map[string]any), "id"), "read": in.flag("read"), "published": mastodon_string(view["comment"].(map[string]any), "published")}
	view["recipient"] = self
	c.JSON(http.StatusOK, map[string]any{record: view})
}

func lemmy_inbox_all_read(c *gin.Context, s *lemmy_session) {
	for _, mentions := range []bool{false, true} {
		kind := "replies"
		if mentions {
			kind = "mentions"
		}
		var ids []string
		for _, comment := range lemmy_inbox_items(s, mentions, 1, 500) {
			ids = append(ids, mastodon_string(comment, "id"))
		}
		lemmy_read_mark(s, kind, ids, true)
	}
	result, _ := lemmy_call(s, "direct/conversations", Map{"limit": 100})
	for _, chat := range mastodon_list(result) {
		if chat["unread"] == true {
			lemmy_call(s, "direct/read", Map{"chat": mastodon_string(chat, "id")})
		}
	}
	c.JSON(http.StatusOK, map[string]any{"replies": []any{}})
}

func lemmy_unread(c *gin.Context, s *lemmy_session) {
	counts := map[string]int{}
	for _, mentions := range []bool{false, true} {
		kind := "replies"
		if mentions {
			kind = "mentions"
		}
		read := lemmy_read_set(s, kind)
		for _, comment := range lemmy_inbox_items(s, mentions, 1, 500) {
			if !read[mastodon_string(comment, "id")] {
				counts[kind]++
			}
		}
	}
	result, _ := lemmy_call(s, "direct/conversations", Map{"limit": 100})
	for _, chat := range mastodon_list(result) {
		if chat["unread"] == true {
			counts["messages"]++
		}
	}
	c.JSON(http.StatusOK, map[string]any{"replies": counts["replies"], "mentions": counts["mentions"], "private_messages": counts["messages"]})
}

// === Private messages ===

// lemmy_message_view is a message of a conversation as Lemmy's private
// message: from its sender to the account, or from the account to the one
// it is with.
func lemmy_message_view(s *lemmy_session, message map[string]any) map[string]any {
	self := lemmy_self(s)
	other := self
	if people := mastodon_list(message["people"]); len(people) > 0 {
		other = lemmy_person_of(s, map[string]any{"id": mastodon_string(people[0], "id"), "url": mastodon_string(people[0], "id"), "display": mastodon_string(people[0], "name"), "name": mastodon_slug(mastodon_string(people[0], "name"))})
	}
	creator, recipient := self, other
	if message["mine"] != true {
		creator = lemmy_person_of(s, map[string]any{"id": mastodon_string(message, "member"), "url": mastodon_string(message, "member"), "display": mastodon_string(message, "name"), "name": mastodon_slug(mastodon_string(message, "name"))})
		recipient = self
	}
	ap := mastodon_string(message, "remote")
	if ap == "" {
		ap = "https://" + s.domain + "/chat/" + mastodon_string(message, "chat")
	}
	return map[string]any{
		"private_message": map[string]any{
			"id": lemmy_number(s, "message", mastodon_string(message, "id")), "creator_id": creator["id"], "recipient_id": recipient["id"],
			"content": mastodon_string(message, "body"), "deleted": false, "read": message["read"] == true,
			"published": lemmy_time(mastodon_integer(message, "created")), "updated": lemmy_optional_time(mastodon_integer(message, "edited")),
			"ap_id": ap, "local": false,
		},
		"creator":   creator,
		"recipient": recipient,
	}
}

func lemmy_messages(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	page, limit := in.page(20)
	args := Map{"limit": limit * page}
	if in.has("creator_id") {
		person := lemmy_key(s, "person", in.number("creator_id"))
		if person == "" {
			c.JSON(http.StatusOK, map[string]any{"private_messages": []any{}})
			return
		}
		args["person"] = person
	}
	result, err := lemmy_call(s, "direct/messages", args)
	out := []any{}
	if err == nil {
		messages := mastodon_list(result)
		start := (page - 1) * limit
		chats := map[string][]any{}
		for i, message := range messages {
			if i < start {
				continue
			}
			if in.flag("unread_only") && message["read"] == true {
				continue
			}
			chat := mastodon_string(message, "chat")
			if _, held := chats[chat]; !held {
				chats[chat] = lemmy_people_of_chat(s, chat)
			}
			message["people"] = chats[chat]
			out = append(out, lemmy_message_view(s, message))
		}
	}
	c.JSON(http.StatusOK, map[string]any{"private_messages": out})
}

// lemmy_people_of_chat are who a conversation is with.
func lemmy_people_of_chat(s *lemmy_session, chat string) []any {
	result, _ := lemmy_call(s, "direct/conversations", Map{"limit": 100})
	for _, held := range mastodon_list(result) {
		if mastodon_string(held, "id") == chat {
			people, _ := held["people"].([]any)
			return people
		}
	}
	return nil
}

// lemmy_softwares are the servers' software by host, read from their
// nodeinfo, for a day.
var lemmy_softwares = map[string]struct {
	name    string
	expires time.Time
}{}
var lemmy_softwares_lock sync.Mutex

// lemmy_software is the software a server runs, as its nodeinfo names it, or
// empty where it says nothing.
func lemmy_software(c *gin.Context, host string) string {
	lemmy_softwares_lock.Lock()
	if held, found := lemmy_softwares[host]; found && time.Now().Before(held.expires) {
		lemmy_softwares_lock.Unlock()
		return held.name
	}
	lemmy_softwares_lock.Unlock()
	name := ""
	var index struct {
		Links []struct {
			Href string `json:"href"`
		} `json:"links"`
	}
	if resolve_json(c.Request.Context(), "https://"+host+"/.well-known/nodeinfo", "application/json", &index) == nil {
		for _, link := range index.Links {
			if identifier_host(link.Href) != host {
				continue
			}
			var document struct {
				Software struct {
					Name string `json:"name"`
				} `json:"software"`
			}
			if resolve_json(c.Request.Context(), link.Href, "application/json", &document) == nil && document.Software.Name != "" {
				name = strings.ToLower(document.Software.Name)
				break
			}
		}
	}
	lemmy_softwares_lock.Lock()
	lemmy_softwares[host] = struct {
		name    string
		expires time.Time
	}{name, time.Now().Add(24 * time.Hour)}
	lemmy_softwares_lock.Unlock()
	return name
}

func lemmy_message_send(c *gin.Context, s *lemmy_session, in *lemmy_input) {
	recipient := lemmy_key(s, "person", in.number("recipient_id"))
	if recipient == "" || !strings.HasPrefix(recipient, "https://") {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_person")
		return
	}
	// A private message to someone on Lemmy is its ChatMessage; others,
	// Mastodon's among them, take a direct Note and ignore a ChatMessage.
	kind := ""
	if lemmy_software(c, identifier_host(recipient)) == "lemmy" {
		kind = "ChatMessage"
	}
	message := lemmy_ask(c, s, "direct/send", Map{"body": in.text("content"), "people": []any{recipient}, "kind": kind})
	if message == nil {
		return
	}
	full, _ := lemmy_call(s, "direct/message", Map{"message": mastodon_string(message, "id")})
	if held := mastodon_map(full); held != nil {
		message = held
	}
	c.JSON(http.StatusOK, map[string]any{"private_message_view": lemmy_message_view(s, message)})
}

// lemmy_message_change edits, deletes or marks read a message.
func lemmy_message_change(c *gin.Context, s *lemmy_session, change string, args Map) {
	if args["message"] == "" {
		lemmy_error(c, http.StatusNotFound, "couldnt_find_private_message")
		return
	}
	held := lemmy_ask(c, s, "direct/message", Map{"message": args["message"]})
	if held == nil {
		return
	}
	if lemmy_ask(c, s, "direct/"+change, args) == nil {
		return
	}
	view := lemmy_message_view(s, held)
	if full := mastodon_map(func() any { r, _ := lemmy_call(s, "direct/message", Map{"message": args["message"]}); return r }()); full != nil {
		view = lemmy_message_view(s, full)
	}
	if change == "delete" {
		view["private_message"].(map[string]any)["deleted"] = true
	}
	c.JSON(http.StatusOK, map[string]any{"private_message_view": view})
}

// === Images ===

// lemmy_pictrs_directory is where the image store keeps its files.
func lemmy_pictrs_directory() string {
	return filepath.Join(data_dir, "pictrs")
}

// lemmy_pictrs answers the image store: an upload from a signed-in app, an
// image to anyone, and a deletion by the token the upload was given.
func lemmy_pictrs(c *gin.Context, rest string) {
	db := networks_database()
	lemmy_tables(db)
	switch {
	case c.Request.Method == "POST" && rest == "":
		s := lemmy_authenticate(c)
		if s == nil {
			return
		}
		form, err := c.MultipartForm()
		if err != nil {
			lemmy_error(c, http.StatusBadRequest, "no_image")
			return
		}
		files := []any{}
		for _, headers := range form.File {
			for _, header := range headers {
				if header.Size > lemmy_image_maximum {
					lemmy_error(c, http.StatusBadRequest, "image_too_large")
					return
				}
				f, err := header.Open()
				if err != nil {
					continue
				}
				data, _ := io.ReadAll(io.LimitReader(f, lemmy_image_maximum+1))
				f.Close()
				kind := http.DetectContentType(data)
				if !strings.HasPrefix(kind, "image/") && !strings.HasPrefix(kind, "video/") {
					lemmy_error(c, http.StatusBadRequest, "invalid_image")
					return
				}
				name := random_alphanumeric(24) + xrpc_extension(kind)
				token := random_alphanumeric(24)
				os.MkdirAll(lemmy_pictrs_directory(), 0o700)
				if err := os.WriteFile(filepath.Join(lemmy_pictrs_directory(), name), data, 0o600); err != nil {
					lemmy_error(c, http.StatusInternalServerError, "couldnt_upload_image")
					return
				}
				db.exec("insert into pictrs (file, user, token, type, created) values (?, ?, ?, ?, ?)", name, s.user.UID, token, kind, now())
				files = append(files, map[string]any{"file": name, "delete_token": token})
			}
		}
		if len(files) == 0 {
			lemmy_error(c, http.StatusBadRequest, "no_image")
			return
		}
		c.JSON(http.StatusCreated, map[string]any{"msg": "ok", "files": files})
	case c.Request.Method == "GET" && strings.HasPrefix(rest, "delete/"):
		token, name, _ := strings.Cut(strings.TrimPrefix(rest, "delete/"), "/")
		row, _ := db.row("select file from pictrs where file=? and token=?", name, token)
		if row == nil {
			lemmy_error(c, http.StatusNotFound, "not_found")
			return
		}
		os.Remove(filepath.Join(lemmy_pictrs_directory(), filepath.Base(name)))
		db.exec("delete from pictrs where file=?", name)
		c.Status(http.StatusNoContent)
	case c.Request.Method == "GET" && rest != "" && !strings.Contains(rest, "/"):
		row, _ := db.row("select type from pictrs where file=?", rest)
		if row == nil {
			lemmy_error(c, http.StatusNotFound, "not_found")
			return
		}
		data, err := os.ReadFile(filepath.Join(lemmy_pictrs_directory(), filepath.Base(rest)))
		if err != nil {
			lemmy_error(c, http.StatusNotFound, "not_found")
			return
		}
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Header("Content-Security-Policy", "default-src 'none'")
		c.Header("X-Content-Type-Options", "nosniff")
		c.Data(http.StatusOK, row_string(row, "type"), data)
	default:
		lemmy_error(c, http.StatusNotFound, "not_found")
	}
}
