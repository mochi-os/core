// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Mastodon clients
//
// The Mastodon client interface over a hosted fediverse account (the
// federation plan, phase 28): a Mastodon app signed in to the account reads
// and writes as it. What the account follows, who follows it, its blocks and
// mutes are core's (objects.go, moderation.go); posts, timelines, favourites
// and bookmarks are the feeds app's, through the functions it declares
// (mastodon/*), so a status posted is a post in the account's home feed,
// published through it, a follow a source there, a favourite a reaction
// where the feed holds the post, and a delete a deleted post (decision 42).
// Notifications are the notifications app's. People and posts elsewhere are
// read from their servers as the account. Statuses and accounts carry
// numbers that sort by time, as Mastodon's do, since clients order by them.

package main

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// mastodon_page_maximum bounds how many items one page of a list holds.
const mastodon_page_maximum = 40

// mastodon_characters is the longest status a client may post.
const mastodon_characters = 5000

// mastodon_version is the Mastodon release the interface answers as, which
// clients read to know what they may ask.
const mastodon_version = "4.3.0"

// statuses_tables makes the tables the interface keeps in a user database:
// each item's time-ordered number, the last form of each account and status
// clients were given, the read positions clients keep, the filters, the
// push subscriptions, and the notifications as they arrived.
func statuses_tables(db *DB) {
	db.exec("create table if not exists snowflakes (number integer primary key, kind text not null, key text not null, unique (kind, key))")
	db.exec("create table if not exists snapshots (kind text not null, key text not null, document text not null, created integer not null, updated integer not null, primary key (kind, key))")
	db.exec("create table if not exists markers (account text not null, timeline text not null, position text not null, version integer not null default 0, updated integer not null, primary key (account, timeline))")
	db.exec("create table if not exists filters (id integer primary key autoincrement, account text not null, title text not null, context text not null, action text not null default 'warn', keywords text not null default '[]', expires integer not null default 0, created integer not null)")
	db.exec("create table if not exists subscriptions (token text not null primary key, account text not null, endpoint text not null, auth text not null, key text not null, alerts text not null default '{}', policy text not null default 'all', standard integer not null default 0, created integer not null)")
	db.exec("create table if not exists alerts (id integer primary key autoincrement, account text not null, row text not null, event text not null default '', answer text not null, created integer not null, unique (account, row, event))")
}

// mastodon_engine is the engine answering Mastodon clients.
func mastodon_engine() *engine {
	return &engine{
		name:     "mastodon-client",
		prefixes: []string{"/api/v1/", "/api/v2/"},
		setting:  "fediverse",
		body:     clients_upload_maximum() + 1<<20,
		handler:  mastodon_route,
	}
}

// === Errors ===

// mastodon_error answers a Mastodon error.
func mastodon_error(c *gin.Context, status int, message string) {
	c.JSON(status, map[string]any{"error": message}) // i18n-ok: Mastodon's errors are protocol, read by clients
}

// === Sessions ===

// A mastodon_session is a request of a Mastodon app's: its token, the user,
// and the hosted fediverse account it is signed in to.
type mastodon_session struct {
	token   *Token
	user    *User
	row     map[string]any
	account string
	handle  string
	domain  string
	actor   string
}

// mastodon_bearer is the token a request carries: in its header, or, for
// the streaming socket a browser cannot add headers to, in its query.
func mastodon_bearer(c *gin.Context) string {
	if scheme, value, found := strings.Cut(c.GetHeader("Authorization"), " "); found && strings.EqualFold(scheme, "Bearer") {
		return strings.TrimSpace(value)
	}
	if protocol := c.GetHeader("Sec-WebSocket-Protocol"); protocol != "" {
		return strings.TrimSpace(strings.Split(protocol, ",")[0])
	}
	return c.Query("access_token")
}

// mastodon_authenticate checks a request's token is a Mastodon app's,
// signed in to a hosted fediverse account and holding the scope asked, and
// writes the refusal.
func mastodon_authenticate(c *gin.Context, scope string) *mastodon_session {
	t := token_lookup(mastodon_bearer(c))
	if t == nil || t.Kind != token_signin || t.Network != "fediverse" || t.JKT != "" {
		mastodon_error(c, http.StatusUnauthorized, "The access token is invalid")
		return nil
	}
	if scope != "" && !mastodon_scoped(t, scope) {
		mastodon_error(c, http.StatusForbidden, "This action is outside the authorized scopes")
		return nil
	}
	user := user_by_uid(t.User)
	if user == nil {
		mastodon_error(c, http.StatusUnauthorized, "The access token is invalid")
		return nil
	}
	row := account_hosted_get(user, t.Account)
	if row == nil || row["type"] != "fediverse" {
		mastodon_error(c, http.StatusUnauthorized, "The access token is invalid")
		return nil
	}
	data := account_data(row)
	handle, _ := data["handle"].(string)
	domain, _ := data["domain"].(string)
	if handle == "" || domain == "" {
		mastodon_error(c, http.StatusUnauthorized, "The access token is invalid")
		return nil
	}
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?) on conflict(hash) do update set used=excluded.used", t.Hash, t.User, now())
	return &mastodon_session{token: t, user: user, row: row, account: t.Account, handle: handle, domain: strings.ToLower(domain), actor: account_url(domain, t.Account)}
}

// === Numbers ===

// mastodon_number gives an item its number for the user: Mastodon's form,
// the time the item was made in milliseconds shifted sixteen bits, and a
// count within the millisecond, so numbers sort as the items were made.
// The same item has the same number each time.
func mastodon_number(user *User, kind string, key string, created int64) string {
	db := db_user(user, "user")
	if row, _ := db.row("select number from snowflakes where kind=? and key=?", kind, key); row != nil {
		return strconv.FormatInt(row_int(row, "number"), 10)
	}
	if created <= 0 {
		created = now()
	}
	base := created * 1000 << 16
	row, _ := db.row("select max(number) as number from snowflakes where number>=? and number<?", base, base+65536)
	number := base
	if found := row_int(row, "number"); found >= base {
		number = found + 1
	}
	db.exec("insert or ignore into snowflakes (number, kind, key) values (?, ?, ?)", number, kind, key)
	row, _ = db.row("select number from snowflakes where kind=? and key=?", kind, key)
	return strconv.FormatInt(row_int(row, "number"), 10)
}

// mastodon_key is the item a number names, of the kind asked, or "".
func mastodon_key(user *User, kind string, number string) string {
	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return ""
	}
	row, _ := db_user(user, "user").row("select key from snowflakes where kind=? and number=?", kind, n)
	return row_string(row, "key")
}

// mastodon_time is a time as Mastodon writes one.
func mastodon_time(unix int64) string {
	return time.Unix(unix, 0).UTC().Format("2006-01-02T15:04:05.000Z")
}

// mastodon_parse reads a time Mastodon or ActivityPub writes, or 0.
func mastodon_parse(value string) int64 {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z0700"} {
		if t, err := time.Parse(layout, value); err == nil {
			return t.Unix()
		}
	}
	return 0
}

// mastodon_snapshot keeps the form of an item clients were given, so a later
// request by its number alone can be answered.
func mastodon_snapshot(user *User, kind string, key string, document map[string]any) {
	db_user(user, "user").exec("insert into snapshots (kind, key, document, created, updated) values (?, ?, ?, ?, ?) on conflict(kind, key) do update set document=excluded.document, updated=excluded.updated", kind, key, json_encode(document), now(), now())
}

// mastodon_snapshot_get is the form of an item clients were last given, or
// nil.
func mastodon_snapshot_get(user *User, kind string, key string) map[string]any {
	row, _ := db_user(user, "user").row("select document from snapshots where kind=? and key=?", kind, key)
	if row == nil {
		return nil
	}
	var document map[string]any
	json.Unmarshal([]byte(row_string(row, "document")), &document)
	return document
}

// === Routing ===

// mastodon_route answers one request of a Mastodon client.
func mastodon_route(c *gin.Context) bool {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key")
	c.Header("Access-Control-Expose-Headers", "Link")
	if c.Request.Method == "OPTIONS" {
		c.AbortWithStatus(http.StatusNoContent)
		return true
	}
	if signin_issuer(c) == "" {
		mastodon_error(c, http.StatusNotFound, "Record not found")
		return true
	}
	path := strings.TrimSuffix(c.Request.URL.EscapedPath(), "/")
	version := "v1"
	rest := ""
	switch {
	case strings.HasPrefix(path, "/api/v1/"):
		rest = strings.TrimPrefix(path, "/api/v1/")
	case strings.HasPrefix(path, "/api/v2/"):
		version, rest = "v2", strings.TrimPrefix(path, "/api/v2/")
	}
	parts := strings.Split(rest, "/")
	for i, part := range parts {
		if unescaped, err := url.PathUnescape(part); err == nil {
			parts[i] = unescaped
		}
	}
	mastodon_dispatch(c, version, parts)
	return true
}

// mastodon_dispatch answers a request by its method, version and parts:
// first what needs no sign-in, then the signed-in app's, each with the scope
// it needs.
func mastodon_dispatch(c *gin.Context, version string, parts []string) {
	method := c.Request.Method
	route := method + " " + version + "/" + strings.Join(parts, "/")
	switch route {
	case "GET v1/instance":
		c.JSON(http.StatusOK, mastodon_instance_v1(c))
		return
	case "GET v2/instance":
		c.JSON(http.StatusOK, mastodon_instance_v2(c))
		return
	case "GET v1/instance/peers", "GET v1/instance/rules", "GET v1/instance/domain_blocks", "GET v1/custom_emojis", "GET v1/trends", "GET v1/trends/tags", "GET v1/trends/statuses", "GET v1/trends/links", "GET v1/directory", "GET v1/announcements":
		c.JSON(http.StatusOK, []any{})
		return
	case "GET v1/instance/activity":
		c.JSON(http.StatusOK, []any{})
		return
	case "GET v1/instance/extended_description":
		c.JSON(http.StatusOK, map[string]any{"updated_at": mastodon_time(now()), "content": ""})
		return
	case "GET v1/streaming/health":
		c.String(http.StatusOK, "OK")
		return
	case "POST v1/accounts":
		mastodon_error(c, http.StatusForbidden, "Accounts are made in Mochi")
		return
	}
	if len(parts) > 0 && parts[0] == "streaming" {
		mastodon_streaming(c, parts[1:])
		return
	}
	s := mastodon_authenticate(c, mastodon_scope(method, parts))
	if s == nil {
		return
	}
	if mastodon_accounts_dispatch(c, s, version, parts) || mastodon_statuses_dispatch(c, s, version, parts) || mastodon_other_dispatch(c, s, version, parts) {
		return
	}
	mastodon_error(c, http.StatusNotFound, "Record not found")
}

// mastodon_scope is the scope a request needs, as Mastodon's granular
// scopes name it: a token holding read or write holds every read: or write:
// scope, and the old follow scope those of follows, blocks and mutes.
func mastodon_scope(method string, parts []string) string {
	access := "read"
	if method != "GET" {
		access = "write"
	}
	area := ""
	if len(parts) > 0 {
		area = parts[0]
	}
	action := ""
	if len(parts) == 3 {
		action = parts[2]
	}
	switch area {
	case "push":
		return "push"
	case "accounts":
		switch {
		case action == "follow" || action == "unfollow" || (len(parts) == 2 && parts[1] == "relationships"):
			return access + ":follows"
		case action == "block" || action == "unblock":
			return "write:blocks"
		case action == "mute" || action == "unmute":
			return "write:mutes"
		case action == "statuses":
			return "read:statuses"
		}
		return access + ":accounts"
	case "statuses":
		switch action {
		case "favourite", "unfavourite":
			return "write:favourites"
		case "bookmark", "unbookmark":
			return "write:bookmarks"
		}
		return access + ":statuses"
	case "timelines", "polls", "markers", "conversations", "scheduled_statuses", "streaming":
		return access + ":statuses"
	case "favourites":
		return "read:favourites"
	case "bookmarks":
		return "read:bookmarks"
	case "notifications":
		return access + ":notifications"
	case "media":
		return "write:media"
	case "filters":
		return access + ":filters"
	case "lists":
		return access + ":lists"
	case "blocks", "domain_blocks":
		return access + ":blocks"
	case "mutes":
		return access + ":mutes"
	case "follow_requests", "followed_tags":
		return access + ":follows"
	case "search":
		return "read:search"
	case "reports":
		return "write:reports"
	case "preferences":
		return "read:accounts"
	}
	return access
}

// mastodon_scoped says whether a token holds a scope: itself, a wider one,
// or for follows, blocks and mutes the old follow scope.
func mastodon_scoped(t *Token, scope string) bool {
	if signin_scopes_check(t, scope) {
		return true
	}
	switch scope {
	case "read:follows", "write:follows", "read:blocks", "write:blocks", "read:mutes", "write:mutes":
		return signin_scopes_check(t, "follow")
	}
	return false
}

// mastodon_match matches a request's parts to a pattern, "*" taking any one
// part, and answers those it took.
func mastodon_match(parts []string, pattern ...string) ([]string, bool) {
	return clients_match(parts, pattern...)
}

// mastodon_input reads a request's field: from its JSON body or its form or
// its query.
type mastodon_input struct {
	values map[string][]string
	json   map[string]any
}

// mastodon_read reads a request's fields, whatever form they come in.
func mastodon_read(c *gin.Context) *mastodon_input {
	in := &mastodon_input{values: map[string][]string{}}
	for key, values := range c.Request.URL.Query() {
		in.values[key] = append(in.values[key], values...)
	}
	if strings.HasPrefix(c.ContentType(), "application/json") {
		raw, _ := c.GetRawData()
		json.Unmarshal(raw, &in.json)
		return in
	}
	if strings.HasPrefix(c.ContentType(), "multipart/form-data") {
		c.Request.ParseMultipartForm(clients_upload_maximum())
	} else {
		c.Request.ParseForm()
	}
	if c.Request.PostForm != nil {
		for key, values := range c.Request.PostForm {
			in.values[key] = append(in.values[key], values...)
		}
	}
	if c.Request.MultipartForm != nil {
		for key, values := range c.Request.MultipartForm.Value {
			in.values[key] = append(in.values[key], values...)
		}
	}
	return in
}

// text is a field's value as text, or "".
func (in *mastodon_input) text(key string) string {
	if in.json != nil {
		switch v := in.json[key].(type) {
		case string:
			return v
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(v)
		}
	}
	if values := in.values[key]; len(values) > 0 {
		return values[0]
	}
	return ""
}

// has says whether a field was given.
func (in *mastodon_input) has(key string) bool {
	if in.json != nil {
		if _, found := in.json[key]; found {
			return true
		}
	}
	_, found := in.values[key]
	if !found {
		_, found = in.values[key+"[]"]
	}
	return found
}

// flag is a field's value as true or false.
func (in *mastodon_input) flag(key string) bool {
	value := strings.ToLower(in.text(key))
	return value == "true" || value == "1" || value == "on"
}

// list is a field's values: a JSON list, or the form's key[] values.
func (in *mastodon_input) list(key string) []string {
	var out []string
	if in.json != nil {
		if values, ok := in.json[key].([]any); ok {
			for _, v := range values {
				out = append(out, fmt.Sprint(v))
			}
			return out
		}
	}
	out = append(out, in.values[key+"[]"]...)
	if len(out) == 0 {
		out = append(out, in.values[key]...)
	}
	return out
}

// object is a JSON object field, or a form's key[name] fields.
func (in *mastodon_input) object(key string) map[string]any {
	if in.json != nil {
		if object, ok := in.json[key].(map[string]any); ok {
			return object
		}
	}
	out := map[string]any{}
	prefix := key + "["
	for name, values := range in.values {
		if inner, ok := strings.CutPrefix(name, prefix); ok && len(values) > 0 {
			inner = strings.TrimSuffix(inner, "]")
			inner, list := strings.CutSuffix(inner, "][")
			if list || strings.HasSuffix(name, "[]") {
				inner = strings.TrimSuffix(strings.TrimSuffix(inner, "]"), "[")
				var items []any
				for _, v := range values {
					items = append(items, v)
				}
				out[inner] = items
			} else {
				out[inner] = values[0]
			}
		}
	}
	return out
}

// mastodon_limit reads a page's size.
func mastodon_limit(in *mastodon_input, fallback int) int {
	limit, err := strconv.Atoi(in.text("limit"))
	if err != nil || limit <= 0 {
		limit = fallback
	}
	return min(limit, mastodon_page_maximum)
}

// mastodon_link writes a page's Link header: the next page older than its
// last item and the previous one newer than its first.
func mastodon_link(c *gin.Context, first string, last string) {
	if first == "" && last == "" {
		return
	}
	base := "https://" + c.Request.Host + c.Request.URL.Path
	query := c.Request.URL.Query()
	var links []string
	if last != "" {
		next := url.Values{}
		for key, values := range query {
			if key != "max_id" && key != "min_id" && key != "since_id" {
				next[key] = values
			}
		}
		next.Set("max_id", last)
		links = append(links, "<"+base+"?"+next.Encode()+`>; rel="next"`)
	}
	if first != "" {
		previous := url.Values{}
		for key, values := range query {
			if key != "max_id" && key != "min_id" && key != "since_id" {
				previous[key] = values
			}
		}
		previous.Set("min_id", first)
		links = append(links, "<"+base+"?"+previous.Encode()+`>; rel="prev"`)
	}
	c.Header("Link", strings.Join(links, ", "))
}

// === The instance ===

// mastodon_domain is the domain a request came by.
func mastodon_domain(c *gin.Context) string {
	host := strings.ToLower(c.Request.Host)
	if h, _, found := strings.Cut(host, ":"); found && !strings.Contains(h, "]") {
		host = h
	}
	return host
}

// mastodon_configuration is what the server lets a client post.
func mastodon_configuration(c *gin.Context) map[string]any {
	webpush_ensure()
	types := []string{"image/jpeg", "image/png", "image/gif", "image/webp", "image/avif", "video/mp4", "video/webm", "video/quicktime", "audio/mpeg", "audio/ogg", "audio/wav", "audio/mp4"}
	return map[string]any{
		"urls":     map[string]any{"streaming": "wss://" + c.Request.Host},
		"vapid":    map[string]any{"public_key": webpush_public},
		"accounts": map[string]any{"max_featured_tags": 0, "max_pinned_statuses": 5},
		"statuses": map[string]any{"max_characters": mastodon_characters, "max_media_attachments": 4, "characters_reserved_per_url": 23},
		"media_attachments": map[string]any{
			"supported_mime_types": types, "image_size_limit": clients_upload_maximum(), "image_matrix_limit": 16777216,
			"video_size_limit": clients_upload_maximum(), "video_frame_rate_limit": 60, "video_matrix_limit": 2304000,
		},
		"polls":       map[string]any{"max_options": 4, "max_characters_per_option": 50, "min_expiration": 300, "max_expiration": 2629746},
		"translation": map[string]any{"enabled": false},
	}
}

// mastodon_instance_v2 is the server as clients read it now.
func mastodon_instance_v2(c *gin.Context) map[string]any {
	domain := mastodon_domain(c)
	return map[string]any{
		"domain":        domain,
		"title":         domain,
		"version":       mastodon_version + " (compatible; Mochi " + build_version + ")",
		"source_url":    "https://mochi-os.org",
		"description":   "",
		"usage":         map[string]any{"users": map[string]any{"active_month": 0}},
		"thumbnail":     map[string]any{"url": "https://" + c.Request.Host + "/favicon.ico"},
		"icon":          []any{},
		"languages":     []string{"en"},
		"configuration": mastodon_configuration(c),
		"registrations": map[string]any{"enabled": false, "approval_required": false, "message": nil, "url": nil},
		"api_versions":  map[string]any{"mastodon": 2},
		"contact":       map[string]any{"email": "", "account": nil},
		"rules":         []any{},
	}
}

// mastodon_instance_v1 is the server as older clients read it.
func mastodon_instance_v1(c *gin.Context) map[string]any {
	domain := mastodon_domain(c)
	return map[string]any{
		"uri":               domain,
		"title":             domain,
		"short_description": "",
		"description":       "",
		"email":             "",
		"version":           mastodon_version + " (compatible; Mochi " + build_version + ")",
		"urls":              map[string]any{"streaming_api": "wss://" + c.Request.Host},
		"stats":             map[string]any{"user_count": 0, "status_count": 0, "domain_count": 0},
		"thumbnail":         "https://" + c.Request.Host + "/favicon.ico",
		"languages":         []string{"en"},
		"registrations":     false,
		"approval_required": false,
		"invites_enabled":   false,
		"configuration":     mastodon_configuration(c),
		"contact_account":   nil,
		"rules":             []any{},
	}
}

// === Accounts ===

// mastodon_avatar_missing is the picture an account with none shows.
func mastodon_avatar_missing(domain string) string {
	return "https://" + domain + "/favicon.ico"
}

// mastodon_own is the signed-in account as clients see it, with its source
// where asked.
func mastodon_own(s *mastodon_session, source bool) map[string]any {
	a := actor_get(s.account)
	if a == nil {
		a = &actor{ID: s.account, Domain: s.domain, Handle: s.handle, Created: row_int(s.row, "created")}
	}
	document := activitypub_actor(s.user, a, s.row)
	db := db_user(s.user, "user")
	count := func(sql string, args ...any) int64 {
		row, _ := db.row(sql, args...)
		return row_int(row, "n")
	}
	followers := count("select count(*) as n from followers where account=?", s.account)
	following := count("select count(*) as n from following where account=?", s.account)
	statuses := count("select count(*) as n from objects where account=? and deleted=0 and type in ('Note', 'Article', 'Question', 'Page')", s.account)
	last := count("select max(created) as n from objects where account=? and deleted=0", s.account)
	name, _ := document["name"].(string)
	note, _ := document["summary"].(string)
	avatar := mastodon_avatar_missing(s.domain)
	if icon, ok := document["icon"].(map[string]any); ok {
		if u, _ := icon["url"].(string); u != "" {
			avatar = u
		}
	}
	header := ""
	if image, ok := document["image"].(map[string]any); ok {
		header, _ = image["url"].(string)
	}
	fields := []any{}
	if attachments, ok := document["attachment"].([]map[string]any); ok {
		for _, f := range attachments {
			fields = append(fields, map[string]any{"name": f["name"], "value": f["value"], "verified_at": nil})
		}
	}
	created := a.Created
	if created == 0 {
		created = now()
	}
	out := map[string]any{
		"id":               mastodon_number(s.user, "account", s.actor, created),
		"username":         s.handle,
		"acct":             s.handle,
		"display_name":     name,
		"locked":           false,
		"bot":              document["type"] == "Service",
		"discoverable":     document["discoverable"],
		"indexable":        document["indexable"],
		"group":            false,
		"created_at":       mastodon_time(created - created%86400),
		"note":             note,
		"url":              document["url"],
		"uri":              s.actor,
		"avatar":           avatar,
		"avatar_static":    avatar,
		"header":           header,
		"header_static":    header,
		"followers_count":  followers,
		"following_count":  following,
		"statuses_count":   statuses,
		"last_status_at":   nil,
		"emojis":           []any{},
		"fields":           fields,
		"hide_collections": false,
		"noindex":          false,
		"roles":            []any{},
	}
	if last > 0 {
		out["last_status_at"] = time.Unix(last, 0).UTC().Format("2006-01-02")
	}
	if source {
		out["source"] = map[string]any{"privacy": "public", "sensitive": false, "language": "", "note": strip_tags(note), "fields": fields, "follow_requests_count": 0, "hide_collections": false, "discoverable": document["discoverable"], "indexable": document["indexable"]}
		out["role"] = map[string]any{"id": "-99", "name": "", "permissions": "0", "color": "", "highlighted": false}
	}
	mastodon_snapshot(s.user, "account", s.actor, out)
	return out
}

// strip_tags is HTML's text.
var strip_tags_re = regexp.MustCompile(`<[^>]*>`)

func strip_tags(source string) string {
	source = strings.NewReplacer("<br>", "\n", "<br/>", "\n", "<br />", "\n", "</p><p>", "\n\n").Replace(source)
	return html.UnescapeString(strip_tags_re.ReplaceAllString(source, ""))
}

// mastodon_figure is someone elsewhere, or a feed or source here, as clients
// see an account: from what their server says of them where known, else from
// what a post carries of them.
type mastodon_figure struct {
	key      string // their permanent id, a feed's or a source's
	username string
	host     string
	name     string
	avatar   string
	header   string
	url      string
	note     string
	locked   bool
	bot      bool
	group    bool
	created  int64
}

// mastodon_account is a person as clients see an account.
func mastodon_account(s *mastodon_session, p mastodon_figure) map[string]any {
	if p.key == s.actor {
		return mastodon_own(s, false)
	}
	if p.created == 0 {
		p.created = now()
		if held := mastodon_snapshot_get(s.user, "account", p.key); held != nil {
			if since := mastodon_parse(fmt.Sprint(held["created_at"])); since > 0 {
				p.created = since
			}
		}
	}
	username := p.username
	if username == "" {
		username = "unknown"
	}
	acct := username
	if p.host != "" && !strings.EqualFold(p.host, s.domain) {
		acct = username + "@" + p.host
	}
	avatar := p.avatar
	if avatar == "" {
		avatar = mastodon_avatar_missing(s.domain)
	}
	name := p.name
	if name == "" {
		name = username
	}
	out := map[string]any{
		"id":              mastodon_number(s.user, "account", p.key, p.created),
		"username":        username,
		"acct":            acct,
		"display_name":    name,
		"locked":          p.locked,
		"bot":             p.bot,
		"discoverable":    nil,
		"group":           p.group,
		"created_at":      mastodon_time(p.created - p.created%86400),
		"note":            markdown_policy.Sanitize(p.note),
		"url":             p.url,
		"uri":             p.key,
		"avatar":          avatar,
		"avatar_static":   avatar,
		"header":          p.header,
		"header_static":   p.header,
		"followers_count": 0,
		"following_count": 0,
		"statuses_count":  0,
		"last_status_at":  nil,
		"emojis":          []any{},
		"fields":          []any{},
	}
	mastodon_snapshot(s.user, "account", p.key, out)
	return out
}

// mastodon_resolved is someone their server described.
func mastodon_resolved(r *resolved) mastodon_figure {
	return mastodon_figure{key: r.ID, username: r.Username, host: r.Host, name: r.Name, avatar: r.Avatar, url: r.URL, note: r.Summary, locked: r.Locked, bot: r.Type == "service", group: r.Type == "group"}
}

// mastodon_resolve finds someone by an address or an id, as the account.
func mastodon_resolve(c *gin.Context, s *mastodon_session, address string) *resolved {
	ctx := activitypub_signing(c.Request.Context(), s.user, s.account)
	r, err := author_resolve(ctx, address)
	if err != nil || r == nil || r.ID == "" || r.Network != "fediverse" {
		return nil
	}
	if host_allowed(s.user, "fediverse", identifier_host(r.ID)) != nil {
		return nil
	}
	return r
}

// mastodon_account_by_number answers who an account number names: the
// signed-in account, or someone their server describes now, else as last
// given.
func mastodon_account_by_number(c *gin.Context, s *mastodon_session, number string) (string, map[string]any) {
	key := mastodon_key(s.user, "account", number)
	if key == "" {
		return "", nil
	}
	if key == s.actor {
		return key, mastodon_own(s, false)
	}
	if strings.HasPrefix(key, "https://") {
		if r := mastodon_resolve(c, s, key); r != nil {
			return key, mastodon_account(s, mastodon_resolved(r))
		}
	}
	return key, mastodon_snapshot_get(s.user, "account", key)
}

// mastodon_accounts_dispatch answers the accounts' requests, reporting
// whether the request was one.
func mastodon_accounts_dispatch(c *gin.Context, s *mastodon_session, version string, parts []string) bool {
	method := c.Request.Method
	route := method + " " + version + "/" + strings.Join(parts, "/")
	switch route {
	case "GET v1/accounts/verify_credentials":
		c.JSON(http.StatusOK, mastodon_own(s, true))
		return true
	case "PATCH v1/accounts/update_credentials":
		mastodon_update_credentials(c, s)
		return true
	case "GET v1/accounts/relationships":
		in := mastodon_read(c)
		out := []any{}
		for _, number := range in.list("id") {
			if key := mastodon_key(s.user, "account", number); key != "" {
				out = append(out, mastodon_relationship(s, number, key))
			}
		}
		c.JSON(http.StatusOK, out)
		return true
	case "GET v1/accounts/lookup":
		address := strings.TrimPrefix(c.Query("acct"), "@")
		if address == "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		if !strings.Contains(address, "@") || strings.EqualFold(strings.SplitN(address, "@", 2)[1], s.domain) {
			if name := strings.SplitN(address, "@", 2)[0]; strings.EqualFold(name, s.handle) {
				c.JSON(http.StatusOK, mastodon_own(s, false))
				return true
			}
			address = strings.SplitN(address, "@", 2)[0] + "@" + s.domain
		}
		r := mastodon_resolve(c, s, "@"+address)
		if r == nil {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		c.JSON(http.StatusOK, mastodon_account(s, mastodon_resolved(r)))
		return true
	case "GET v1/accounts/search":
		in := mastodon_read(c)
		c.JSON(http.StatusOK, mastodon_people_search(c, s, in.text("q"), in.flag("resolve"), mastodon_limit(in, 40)))
		return true
	case "GET v1/follow_requests", "GET v1/endorsements", "GET v1/featured_tags", "GET v1/followed_tags", "GET v1/suggestions", "GET v2/suggestions", "GET v1/accounts/familiar_followers":
		c.JSON(http.StatusOK, []any{})
		return true
	case "GET v1/preferences":
		c.JSON(http.StatusOK, map[string]any{"posting:default:visibility": "public", "posting:default:sensitive": false, "posting:default:language": nil, "reading:expand:media": "default", "reading:expand:spoilers": false})
		return true
	case "GET v1/blocks", "GET v1/mutes":
		table := "blocked"
		if parts[0] == "mutes" {
			table = "muted"
		}
		out := []any{}
		for _, row := range actor_list(db_user(s.user, "user"), table) {
			key := row_string(row, "actor")
			if !strings.HasPrefix(key, "https://") {
				continue
			}
			if r := mastodon_resolve(c, s, key); r != nil {
				out = append(out, mastodon_account(s, mastodon_resolved(r)))
			} else if held := mastodon_snapshot_get(s.user, "account", key); held != nil {
				out = append(out, held)
			}
		}
		c.JSON(http.StatusOK, out)
		return true
	case "GET v1/domain_blocks":
		rows, _ := db_user(s.user, "user").rows("select domain from blocks order by created")
		out := []string{}
		for _, row := range rows {
			out = append(out, row_string(row, "domain"))
		}
		c.JSON(http.StatusOK, out)
		return true
	case "POST v1/domain_blocks", "DELETE v1/domain_blocks":
		domain := strings.ToLower(strings.TrimSpace(mastodon_read(c).text("domain")))
		if domain == "" || strings.ContainsAny(domain, "/ @") {
			mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Domain is invalid")
			return true
		}
		if method == "POST" {
			db_user(s.user, "user").exec("insert or ignore into blocks (domain, created) values (?, ?)", domain, now())
		} else {
			db_user(s.user, "user").exec("delete from blocks where domain=?", domain)
		}
		c.JSON(http.StatusOK, map[string]any{})
		return true
	}
	if taken, ok := mastodon_match(parts, "accounts", "*"); ok && method == "GET" && version == "v1" {
		if _, account := mastodon_account_by_number(c, s, taken[0]); account != nil {
			c.JSON(http.StatusOK, account)
		} else {
			mastodon_error(c, http.StatusNotFound, "Record not found")
		}
		return true
	}
	if len(parts) == 3 && parts[0] == "accounts" && version == "v1" {
		number, action := parts[1], parts[2]
		key := mastodon_key(s.user, "account", number)
		if key == "" {
			mastodon_error(c, http.StatusNotFound, "Record not found")
			return true
		}
		switch method + " " + action {
		case "GET statuses":
			mastodon_account_statuses(c, s, number, key)
		case "GET followers", "GET following":
			mastodon_account_people(c, s, key, action)
		case "POST follow", "POST unfollow":
			mastodon_follow(c, s, number, key, action == "follow")
		case "POST block", "POST unblock":
			if key == s.actor {
				mastodon_error(c, http.StatusUnprocessableEntity, "You cannot block yourself")
				return true
			}
			if action == "block" {
				blocked_add(s.user, key)
			} else {
				blocked_remove(s.user, key)
			}
			c.JSON(http.StatusOK, mastodon_relationship(s, number, key))
		case "POST mute", "POST unmute":
			if key == s.actor {
				mastodon_error(c, http.StatusUnprocessableEntity, "You cannot mute yourself")
				return true
			}
			muted_set(c.Request.Context(), s.user, key, action == "mute")
			c.JSON(http.StatusOK, mastodon_relationship(s, number, key))
		case "GET lists", "GET featured_tags", "GET identity_proofs":
			c.JSON(http.StatusOK, []any{})
		case "POST note", "POST pin", "POST unpin":
			c.JSON(http.StatusOK, mastodon_relationship(s, number, key))
		default:
			mastodon_error(c, http.StatusNotFound, "Record not found")
		}
		return true
	}
	return false
}

// mastodon_update_credentials changes what the account shows: its name, its
// description and its links. Its picture and banner are the profile's, set
// in Mochi.
func mastodon_update_credentials(c *gin.Context, s *mastodon_session) {
	in := mastodon_read(c)
	if in.has("display_name") {
		name := strings.TrimSpace(in.text("display_name"))
		if len(name) > 100 {
			mastodon_error(c, http.StatusUnprocessableEntity, "Validation failed: Display name is too long")
			return
		}
		db_user(s.user, "user").account_set(s.account, map[string]any{"label": name})
		s.row = account_hosted_get(s.user, s.account)
	}
	c.JSON(http.StatusOK, mastodon_own(s, true))
}

// mastodon_relationship is the account's standing with someone.
func mastodon_relationship(s *mastodon_session, number string, key string) map[string]any {
	db := db_user(s.user, "user")
	following, _ := db.row("select accepted from following where account=? and actor=?", s.account, key)
	followed, _ := db.exists("select 1 from followers where account=? and actor=?", s.account, key)
	host := identifier_host(key)
	domain := false
	if host != "" {
		domain, _ = db.exists("select 1 from blocks where domain=?", host)
	}
	return map[string]any{
		"id":                   number,
		"following":            following != nil && row_int(following, "accepted") > 0,
		"showing_reblogs":      following != nil,
		"notifying":            false,
		"languages":            nil,
		"followed_by":          followed,
		"blocking":             actor_blocked(s.user, key),
		"blocked_by":           false,
		"muting":               actor_muted(s.user, key),
		"muting_notifications": actor_muted(s.user, key),
		"requested":            following != nil && row_int(following, "accepted") == 0,
		"requested_by":         false,
		"domain_blocking":      domain,
		"endorsed":             false,
		"note":                 "",
	}
}

// mastodon_people_search finds people by name or address: whoever an
// address names, asked of their server where the client asks to resolve,
// and those the account follows or is followed by whose names hold the
// words.
func mastodon_people_search(c *gin.Context, s *mastodon_session, query string, resolve bool, limit int) []any {
	query = strings.TrimSpace(strings.TrimPrefix(query, "@"))
	out := []any{}
	if query == "" {
		return out
	}
	seen := map[string]bool{}
	add := func(account map[string]any, key string) {
		if account != nil && !seen[key] && len(out) < limit {
			seen[key] = true
			out = append(out, account)
		}
	}
	if strings.EqualFold(query, s.handle) || strings.EqualFold(query, s.handle+"@"+s.domain) {
		add(mastodon_own(s, false), s.actor)
	}
	if (resolve && strings.Contains(query, "@")) || strings.HasPrefix(query, "https://") {
		target := query
		if !strings.HasPrefix(target, "https://") {
			target = "@" + target
		}
		if r := mastodon_resolve(c, s, target); r != nil {
			add(mastodon_account(s, mastodon_resolved(r)), r.ID)
		}
	}
	words := strings.ToLower(query)
	db := db_user(s.user, "user")
	rows, _ := db.rows("select key, document from snapshots where kind='account' order by updated desc limit 500")
	for _, row := range rows {
		var account map[string]any
		json.Unmarshal([]byte(row_string(row, "document")), &account)
		text := strings.ToLower(fmt.Sprint(account["acct"], " ", account["display_name"]))
		if strings.Contains(text, words) {
			add(account, row_string(row, "key"))
		}
	}
	return out
}

// mastodon_account_people answers who follows an account, or whom it
// follows: the signed-in account's own, which core keeps; someone else's are
// theirs to say, and are left empty.
func mastodon_account_people(c *gin.Context, s *mastodon_session, key string, which string) {
	out := []any{}
	if key != s.actor {
		c.JSON(http.StatusOK, out)
		return
	}
	in := mastodon_read(c)
	limit := mastodon_limit(in, 40)
	table := "followers"
	if which == "following" {
		table = "following"
	}
	before := int64(1 << 62)
	if max := in.text("max_id"); max != "" {
		before, _ = strconv.ParseInt(max, 10, 64)
	}
	rows, _ := db_user(s.user, "user").rows("select actor, created from "+table+" where account=? order by created desc", s.account)
	last := ""
	for _, row := range rows {
		person := row_string(row, "actor")
		number := mastodon_number(s.user, "account", person, row_int(row, "created"))
		if n, _ := strconv.ParseInt(number, 10, 64); n >= before {
			continue
		}
		var account map[string]any
		if held := mastodon_snapshot_get(s.user, "account", person); held != nil {
			account = held
		} else if r := mastodon_resolve(c, s, person); r != nil {
			account = mastodon_account(s, mastodon_resolved(r))
		}
		if account == nil {
			continue
		}
		out = append(out, account)
		last = number
		if len(out) == limit {
			break
		}
	}
	if len(out) == limit {
		mastodon_link(c, "", last)
	}
	c.JSON(http.StatusOK, out)
}

// mastodon_follow follows someone, or stops, through the home feed: a
// source there that the feeds app follows them through as the account.
func mastodon_follow(c *gin.Context, s *mastodon_session, number string, key string, on bool) {
	if key == s.actor {
		mastodon_error(c, http.StatusUnprocessableEntity, "You cannot follow yourself")
		return
	}
	result, err := mastodon_call(s, "mastodon/follow", Map{"address": key, "on": on})
	if err != nil {
		mastodon_error(c, http.StatusUnprocessableEntity, err.Error())
		return
	}
	if answer, ok := result.(map[string]any); ok {
		if failure, _ := answer["error"].(string); failure != "" {
			mastodon_error(c, http.StatusUnprocessableEntity, failure)
			return
		}
	}
	c.JSON(http.StatusOK, mastodon_relationship(s, number, key))
}

// === The apps' functions ===

// mastodon_app is the app answering a function, or nil.
func mastodon_app(user *User, function string) *App {
	found := apps_for_function(user, function)
	if len(found) == 0 {
		return nil
	}
	return found[0]
}

// mastodon_call calls a function the posting app declares, as the account,
// naming the account's home feed, where posts made from its apps land
// (decision 42).
func mastodon_call(s *mastodon_session, function string, args Map) (any, error) {
	a := mastodon_app(s.user, function)
	if a == nil {
		return nil, fmt.Errorf("No app here keeps posts")
	}
	args["account"] = s.account
	home, _ := account_data(s.row)["home"].(string)
	args["home"] = home
	return mastodon_caller(s, a, function, args)
}

// mastodon_caller runs an app's function as the user; a test stands its own
// in.
var mastodon_caller = func(s *mastodon_session, a *App, function string, args Map) (any, error) {
	return app_call_as_server(s.user, a, function, args)
}

// mastodon_list is a function's answer as a list of maps.
func mastodon_list(result any) []map[string]any {
	items, _ := result.([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// mastodon_string is a map's value as text.
func mastodon_string(m map[string]any, key string) string {
	switch v := m[key].(type) {
	case string:
		return v
	case nil:
		return ""
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprint(v)
	}
}

// mastodon_integer is a map's value as a number.
func mastodon_integer(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	}
	return 0
}
