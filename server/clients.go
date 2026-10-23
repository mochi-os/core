// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix clients
//
// Other Matrix clients signing in to a hosted Matrix account (the federation
// plan, phase 28): Element X first, signing in through the sign-in server's
// OAuth login and syncing by sliding sync, then the apps that still sign in
// the old way, with a password minted for the device, and sync by /sync.
// Each client is a device of the account beside core's own: its keys in the
// key store (keyring.go), the rooms it sees those the engine holds
// (rooms.go), and what it sends taken into them as the account, so chat and
// the client see the same conversations.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// clients_api_scopes are the scopes that let a client use the whole client
// interface: the stable one and MSC2967's.
var clients_api_scopes = []string{"urn:matrix:client:api:*", "urn:matrix:org.matrix.msc2967.client:api:*"}

// clients_tables makes what the client interface keeps in matrix.db beside
// the engine's rooms: the order events entered the rooms' timelines, which
// sync's positions count in; read receipts; the accounts' data, global and
// per room; filters; push rules, pushers and the notifications they made;
// room aliases and the room directory.
func clients_tables(db *DB) {
	db.exec("create table if not exists stream (position integer primary key autoincrement, room text not null, event text not null unique)")
	db.exec("create index if not exists stream_room on stream(room, position)")
	// A server that kept rooms before the client interface takes their
	// timelines into the order once, in the order they arrived.
	db.exec("insert or ignore into stream (room, event) select room, id from events where outlier=0 and rejected=0 and not exists (select 1 from stream) order by received, rowid")
	db.exec("create table if not exists receipts (position integer primary key autoincrement, room text not null, user text not null, type text not null, thread text not null default '', event text not null, ts integer not null, unique (room, user, type, thread))")
	db.exec("create table if not exists settings (position integer primary key autoincrement, user text not null, room text not null default '', type text not null, content text not null, unique (user, room, type))")
	// What the key store kept as account data before the client interface
	// kept it here, with a position for sync.
	db.exec("insert or ignore into settings (user, room, type, content) select user, '', type, content from data")
	db.exec("create table if not exists filters (id integer primary key autoincrement, user text not null, body text not null)")
	db.exec("create table if not exists rules (position integer primary key autoincrement, user text not null, kind text not null, id text not null, priority integer not null default 0, body text not null, enabled integer not null default 1, unique (user, kind, id))")
	db.exec("create table if not exists pushers (user text not null, app text not null, key text not null, device text not null default '', kind text not null, body text not null, created integer not null, primary key (app, key, user))")
	db.exec("create table if not exists notifications (id integer primary key autoincrement, user text not null, room text not null, event text not null, position integer not null, actions text not null, highlight integer not null default 0, read integer not null default 0, created integer not null, unique (user, event))")
	db.exec("create index if not exists notifications_user on notifications(user, room, read)")
	db.exec("create index if not exists sent_event on sent(event)")
	db.exec("create index if not exists events_member on events(key, type)")
	db.exec("create table if not exists aliases (alias text not null primary key, room text not null, creator text not null, created integer not null)")
	db.exec("create table if not exists published (room text not null primary key, created integer not null)")
	db.exec("create table if not exists pending (position integer primary key autoincrement, room text not null, user text not null, kind text not null, server text not null default '', state text not null default '', created integer not null, unique (room, user))")
}

// clients_upload_maximum bounds a file a client uploads, as mochi.account.upload
// bounds one an app does.
func clients_upload_maximum() int64 {
	return int64(media_maximum)
}

// clients_engine answers the client interface's paths and the client's
// discovery document, on the server's Matrix domains.
func clients_engine() *engine {
	return &engine{
		name:     "matrix-client",
		prefixes: []string{"/_matrix/client/", "/_matrix/media/", "/.well-known/matrix/client"},
		setting:  "matrix",
		body:     clients_upload_maximum() + 1<<20,
		handler:  clients_route,
	}
}

// === Errors and answers ===

// clients_error answers a Matrix error.
func clients_error(c *gin.Context, status int, code string, message string) {
	c.JSON(status, map[string]any{"errcode": code, "error": message}) // i18n-ok: Matrix error codes and their text are protocol, read by clients
}

// clients_answer writes a key store's answer.
func clients_answer(c *gin.Context, answer keyring_answer) {
	c.JSON(answer.status, answer.body)
}

// clients_body reads a request's JSON body, an empty one as {}.
func clients_body(c *gin.Context, into any) bool {
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, keyring_body_maximum+1))
	if err != nil || len(raw) > keyring_body_maximum {
		clients_error(c, http.StatusRequestEntityTooLarge, "M_TOO_LARGE", "The request is too large")
		return false
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		raw = []byte("{}")
	}
	if json.Unmarshal(raw, into) != nil {
		clients_error(c, http.StatusBadRequest, "M_NOT_JSON", "The content is not JSON")
		return false
	}
	return true
}

// clients_raw reads a request's body as it came, for the key store.
func clients_raw(c *gin.Context) (json.RawMessage, bool) {
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, keyring_body_maximum+1))
	if err != nil || len(raw) > keyring_body_maximum {
		clients_error(c, http.StatusRequestEntityTooLarge, "M_TOO_LARGE", "The request is too large")
		return nil, false
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		raw = []byte("{}")
	}
	return raw, true
}

// === Waking ===
//
// A client's sync waits for something new; whatever is new wakes every sync
// waiting, and each looks again whether it concerns it.

var clients_waking = make(chan struct{})
var clients_waking_lock sync.Mutex

// clients_wake wakes every sync waiting.
func clients_wake() {
	clients_waking_lock.Lock()
	close(clients_waking)
	clients_waking = make(chan struct{})
	clients_waking_lock.Unlock()
}

// clients_waiter is what a sync waits on until the next wake.
func clients_waiter() chan struct{} {
	clients_waking_lock.Lock()
	defer clients_waking_lock.Unlock()
	return clients_waking
}

// === Sessions ===

// A clients_session is a request's signed-in device: its token, the user,
// the hosted account and the device.
type clients_session struct {
	token  *Token
	user   *User
	h      *matrix_hosted
	device string
}

// clients_authenticate reads a request's token - the Authorization header or
// the old access_token parameter - and answers its session, or writes the
// refusal. A token past its life is refused as a soft logout, which tells
// the client to refresh.
func clients_authenticate(c *gin.Context) *clients_session {
	raw := ""
	if scheme, value, found := strings.Cut(c.GetHeader("Authorization"), " "); found && strings.EqualFold(scheme, "Bearer") {
		raw = strings.TrimSpace(value)
	}
	if raw == "" {
		raw = c.Query("access_token")
	}
	if raw == "" {
		clients_error(c, http.StatusUnauthorized, "M_MISSING_TOKEN", "Missing access token")
		return nil
	}
	refuse := func(soft bool) *clients_session {
		c.JSON(http.StatusUnauthorized, map[string]any{"errcode": "M_UNKNOWN_TOKEN", "error": "Unknown or expired access token", "soft_logout": soft}) // i18n-ok: Matrix error codes and their text are protocol, read by clients
		return nil
	}
	t := token_lookup(raw)
	if t == nil {
		// One that expired is still in the store: the client refreshes it.
		var expired Token
		if db_open("db/users.db").scan(&expired, "select hash, kind from tokens where hash=?", token_hash(raw)) && expired.Kind == token_signin {
			return refuse(true)
		}
		return refuse(false)
	}
	if t.Kind != token_signin || t.Network != "matrix" || t.Device == "" {
		return refuse(false)
	}
	scoped := false
	for _, scope := range clients_api_scopes {
		scoped = scoped || signin_scopes_check(t, scope)
	}
	if !scoped {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "The token does not allow the client interface")
		return nil
	}
	user := user_by_uid(t.User)
	if user == nil {
		return refuse(false)
	}
	h := matrix_hosted_of(user, account_hosted_get(user, t.Account))
	if h == nil {
		return refuse(false)
	}
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?) on conflict(hash) do update set used=excluded.used", t.Hash, t.User, now())
	db := federation_database()
	db.exec("insert or ignore into clients (user, device, name, created) values (?, ?, ?, ?)", h.address, t.Device, t.Name, now())
	db.exec("update clients set seen=? where user=? and device=?", now(), h.address, t.Device)
	return &clients_session{token: t, user: user, h: h, device: t.Device}
}

// === Routing ===

// clients_route answers one request of a Matrix client.
func clients_route(c *gin.Context) bool {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	c.Header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, Authorization")
	if c.Request.Method == "OPTIONS" {
		c.AbortWithStatus(http.StatusOK)
		return true
	}
	domain := federation_domain(c)
	if !federation_local(domain) {
		clients_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "No Matrix server here")
		return true
	}
	path := c.Request.URL.EscapedPath()
	if path == "/.well-known/matrix/client" {
		c.JSON(http.StatusOK, map[string]any{"m.homeserver": map[string]any{"base_url": clients_base(domain)}})
		return true
	}
	area, parts := clients_parts(path)
	switch {
	// The authenticated media paths are the client interface's, under
	// client/v1/media.
	case area == "media" || (area == "client" && len(parts) > 0 && parts[0] == "media"):
		clients_media(c, parts)
	case area == "client":
		clients_dispatch(c, domain, parts)
	default:
		clients_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognised request")
	}
	return true
}

// clients_base is the address clients reach a domain's server at.
func clients_base(domain spec.ServerName) string {
	return strings.TrimSuffix(network_scheme, "://") + "://" + string(domain)
}

// clients_parts splits a request's path after /_matrix/client/ or
// /_matrix/media/ and its version - v1, v3, r0, or unstable with or without
// its namespace - into its parts, each unescaped as a part, so an escaped
// slash in an id stays in it.
func clients_parts(path string) (string, []string) {
	area := ""
	rest := ""
	for _, prefix := range []string{"client", "media"} {
		if r, ok := strings.CutPrefix(path, "/_matrix/"+prefix+"/"); ok {
			area, rest = prefix, r
		}
	}
	if area == "" {
		return "", nil
	}
	parts := strings.Split(rest, "/")
	if len(parts) > 0 {
		switch {
		case parts[0] == "unstable" && len(parts) > 1 && strings.Contains(parts[1], "."):
			parts = parts[2:]
		case parts[0] == "v1", parts[0] == "v3", parts[0] == "r0", parts[0] == "unstable":
			parts = parts[1:]
		}
	}
	for i, part := range parts {
		if unescaped, err := url.PathUnescape(part); err == nil {
			parts[i] = unescaped
		}
	}
	return area, parts
}

// clients_match matches a request's parts to a pattern of parts, "*" taking
// any one part, and answers those it took.
func clients_match(parts []string, pattern ...string) ([]string, bool) {
	if len(parts) != len(pattern) {
		return nil, false
	}
	var taken []string
	for i, p := range pattern {
		switch {
		case p == "*":
			taken = append(taken, parts[i])
		case p != parts[i]:
			return nil, false
		}
	}
	return taken, true
}

// clients_dispatch answers a client request by its method and parts: first
// those that need no sign-in, then the signed-in device's.
func clients_dispatch(c *gin.Context, domain spec.ServerName, parts []string) {
	method := c.Request.Method
	route := method + " " + strings.Join(parts, "/")
	switch route {
	case "GET versions":
		clients_versions(c)
		return
	case "GET login":
		c.JSON(http.StatusOK, map[string]any{"flows": []any{map[string]any{"type": "m.login.password"}}})
		return
	case "POST login":
		clients_login(c, domain)
		return
	case "POST refresh":
		clients_refresh(c)
		return
	case "GET auth_metadata":
		document := signin_document(c)
		if document["issuer"] == "" {
			clients_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "No sign-in server here")
			return
		}
		document["account_management_uri"] = signin_issuer(c) + "/settings/user/networks"
		c.JSON(http.StatusOK, document)
		return
	case "GET auth_issuer":
		issuer := signin_issuer(c)
		if issuer == "" {
			clients_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "No sign-in server here")
			return
		}
		c.JSON(http.StatusOK, map[string]any{"issuer": issuer})
		return
	case "GET register", "POST register", "GET register/available":
		switch {
		case !clients_registration:
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Accounts are made in Mochi")
		case route == "POST register":
			clients_register(c, domain)
		case route == "GET register/available":
			clients_register_available(c, domain)
		default:
			c.JSON(http.StatusOK, clients_register_flows())
		}
		return
	}
	if taken, ok := clients_match(parts, "profile", "*"); ok && method == "GET" {
		clients_profile_get(c, domain, taken[0], "")
		return
	}
	if taken, ok := clients_match(parts, "profile", "*", "*"); ok && method == "GET" {
		clients_profile_get(c, domain, taken[0], taken[1])
		return
	}
	s := clients_authenticate(c)
	if s == nil {
		return
	}
	if clients_rooms_dispatch(c, s, parts) || sync_dispatch(c, s, parts) || pushers_dispatch(c, s, parts) {
		return
	}
	switch route {
	case "GET account/whoami":
		c.JSON(http.StatusOK, map[string]any{"user_id": s.h.address, "device_id": s.device, "is_guest": false})
		return
	case "POST logout":
		clients_device_delete(s, s.device)
		c.JSON(http.StatusOK, map[string]any{})
		return
	case "POST logout/all":
		rows, _ := db_open("db/users.db").rows("select device from tokens where user=? and account=? and kind=? and network='matrix'", s.user.UID, s.h.id, token_signin)
		for _, row := range rows {
			clients_device_delete(s, row_string(row, "device"))
		}
		c.JSON(http.StatusOK, map[string]any{})
		return
	case "GET capabilities":
		clients_capabilities(c)
		return
	case "GET devices":
		clients_devices(c, s)
		return
	case "POST delete_devices":
		var body struct {
			Devices []string        `json:"devices"`
			Auth    json.RawMessage `json:"auth"`
		}
		if !clients_body(c, &body) || !clients_interactive(c, s, body.Auth) {
			return
		}
		for _, device := range body.Devices {
			if !clients_device_delete(s, device) {
				clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Mochi's own device cannot be removed here")
				return
			}
		}
		c.JSON(http.StatusOK, map[string]any{})
		return
	case "GET account/3pid":
		c.JSON(http.StatusOK, map[string]any{"threepids": []any{}})
		return
	case "POST account/password", "POST account/deactivate":
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "This is done in Mochi")
		return
	case "GET voip/turnServer", "GET thirdparty/protocols":
		c.JSON(http.StatusOK, map[string]any{})
		return
	case "POST user_directory/search":
		clients_directory_search(c, s)
		return
	case "GET keys/changes":
		clients_key_changes(c, s)
		return
	}
	if taken, ok := clients_match(parts, "devices", "*"); ok {
		clients_device(c, s, taken[0])
		return
	}
	if taken, ok := clients_match(parts, "profile", "*", "*"); ok && method == "PUT" {
		clients_profile_set(c, s, taken[0], taken[1])
		return
	}
	if taken, ok := clients_match(parts, "presence", "*", "status"); ok {
		clients_presence(c, s, taken[0])
		return
	}
	if taken, ok := clients_match(parts, "user", "*", "filter"); ok && method == "POST" {
		clients_filter_create(c, s, taken[0])
		return
	}
	if taken, ok := clients_match(parts, "user", "*", "filter", "*"); ok && method == "GET" {
		clients_filter_get(c, s, taken[0], taken[1])
		return
	}
	if taken, ok := clients_match(parts, "user", "*", "account_data", "*"); ok {
		clients_data(c, s, taken[0], "", taken[1])
		return
	}
	if taken, ok := clients_match(parts, "user", "*", "rooms", "*", "account_data", "*"); ok {
		clients_data(c, s, taken[0], taken[1], taken[2])
		return
	}
	if taken, ok := clients_match(parts, "user", "*", "rooms", "*", "tags"); ok && method == "GET" {
		clients_tags(c, s, taken[0], taken[1], "")
		return
	}
	if taken, ok := clients_match(parts, "user", "*", "rooms", "*", "tags", "*"); ok {
		clients_tags(c, s, taken[0], taken[1], taken[2])
		return
	}
	// The key store's own: device and one-time keys, cross-signing, what is
	// sent to devices, and the key backup.
	if len(parts) > 0 && (parts[0] == "keys" || parts[0] == "sendToDevice" || parts[0] == "room_keys") {
		body, ok := clients_raw(c)
		if !ok {
			return
		}
		escaped := make([]string, len(parts))
		for i, part := range parts {
			escaped[i] = url.PathEscape(part)
		}
		path := strings.Join(escaped, "/")
		if c.Request.URL.RawQuery != "" {
			path += "?" + c.Request.URL.RawQuery
		}
		clients_answer(c, keyring_request(c.Request.Context(), s.h.id, s.h.address, s.device, method, path, body))
		return
	}
	clients_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognised request")
}

// clients_versions is what the server speaks: the specification's versions,
// and sliding sync, which Element X needs.
func clients_versions(c *gin.Context) {
	c.JSON(http.StatusOK, map[string]any{
		"versions": []string{"r0.6.1", "v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11", "v1.12"},
		"unstable_features": map[string]any{
			"org.matrix.simplified_msc3575": true,
			"org.matrix.msc3916.stable":     true,
			"org.matrix.e2e_cross_signing":  true,
			"org.matrix.msc2965":            true,
		},
	})
}

// clients_capabilities is what the server lets a client do.
func clients_capabilities(c *gin.Context) {
	available := map[string]string{}
	for version, v := range gomatrixserverlib.RoomVersions() {
		if v.Stable() {
			available[string(version)] = "stable"
		} else {
			available[string(version)] = "unstable"
		}
	}
	c.JSON(http.StatusOK, map[string]any{"capabilities": map[string]any{
		"m.change_password": map[string]any{"enabled": false},
		"m.room_versions":   map[string]any{"default": string(rooms_version_default), "available": available},
		"m.set_displayname": map[string]any{"enabled": true},
		"m.set_avatar_url":  map[string]any{"enabled": true},
		"m.3pid_changes":    map[string]any{"enabled": false},
		"m.get_login_token": map[string]any{"enabled": false},
	}})
}

// === Signing in the old way ===

// clients_login signs a device in with a password minted for it: the
// account's address, or its name on the domain asked, and the password.
// The client may ask for a token that expires, with a refresh token.
func clients_login(c *gin.Context, domain spec.ServerName) {
	var body struct {
		Type       string `json:"type"`
		Identifier struct {
			Type string `json:"type"`
			User string `json:"user"`
		} `json:"identifier"`
		User     string `json:"user"`
		Password string `json:"password"`
		Device   string `json:"device_id"`
		Name     string `json:"initial_device_display_name"`
		Refresh  bool   `json:"refresh_token"`
	}
	if !clients_body(c, &body) {
		return
	}
	if body.Type != "m.login.password" {
		clients_error(c, http.StatusBadRequest, "M_UNKNOWN", "Sign in with the password Mochi made for the device")
		return
	}
	name := body.Identifier.User
	if name == "" {
		name = body.User
	}
	if (body.Identifier.Type != "" && body.Identifier.Type != "m.id.user") || name == "" {
		clients_error(c, http.StatusBadRequest, "M_UNKNOWN", "Sign in with the account's address")
		return
	}
	address := name
	if !strings.HasPrefix(address, "@") {
		address = "@" + name + ":" + string(domain)
	}
	user, row, device := password_check("matrix", address, clients_password(address, body.Password))
	if user == nil {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "The address or the password is wrong")
		return
	}
	h := matrix_hosted_of(user, row)
	if h == nil {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "The address or the password is wrong")
		return
	}
	label := strings.TrimSpace(body.Name)
	if label == "" {
		label = device
	}
	if len(body.Device) > 100 || strings.ContainsAny(body.Device, "/ ") {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Invalid device id")
		return
	}
	if mine := encryption_device_of(user, h.id); body.Device != "" && body.Device == mine {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "That device is Mochi's own")
		return
	}
	m := signin_mint(user, "", label, "matrix", h.id, clients_api_scopes[0], "", body.Device, body.Refresh)
	if m == nil {
		clients_error(c, http.StatusInternalServerError, "M_UNKNOWN", "No token")
		return
	}
	answer := map[string]any{
		"user_id":      h.address,
		"access_token": m.token,
		"device_id":    m.device,
		"home_server":  string(domain),
		"well_known":   map[string]any{"m.homeserver": map[string]any{"base_url": clients_base(domain)}},
	}
	if m.refresh != "" {
		answer["refresh_token"] = m.refresh
		answer["expires_in_ms"] = (m.expires - now()) * 1000
	}
	c.JSON(http.StatusOK, answer)
}

// === Registering, in development only ===
//
// Accounts are made in Mochi. A development server with [development]
// registration on makes them through the client interface too, so a test
// suite that registers its people, as Complement does, can drive the engine:
// each registration is a user, their handle on the server's domain and a
// hosted Matrix account on it, the password given kept as one minted for it.

// clients_registration is whether the client interface takes registrations.
var clients_registration = false

// clients_localpart is the form of a name registered: a Matrix localpart of
// letters, digits, hyphens and underscores. A registration records its handle
// itself, so the names a test suite gives, such as user-1-alice, are taken
// though Mochi's own rules for a handle a person reserves are narrower.
var clients_localpart = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// clients_register_flows is the one stage a registration asks.
func clients_register_flows() map[string]any {
	return map[string]any{"flows": []any{map[string]any{"stages": []string{"m.login.dummy"}}}, "params": map[string]any{}, "session": random_alphanumeric(24)}
}

// clients_register makes an account for a name on the server's own domain,
// once the client has answered the dummy stage, and signs a device in to it
// unless asked not to.
func clients_register(c *gin.Context, domain spec.ServerName) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Device   string `json:"device_id"`
		Name     string `json:"initial_device_display_name"`
		Inhibit  bool   `json:"inhibit_login"`
		Refresh  bool   `json:"refresh_token"`
		Auth     struct {
			Type string `json:"type"`
		} `json:"auth"`
	}
	if !clients_body(c, &body) {
		return
	}
	if c.Query("kind") == "guest" {
		clients_error(c, http.StatusForbidden, "M_GUEST_ACCESS_FORBIDDEN", "Guests are not taken")
		return
	}
	if string(domain) != server_domain() {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Accounts are registered on the server's own domain")
		return
	}
	if body.Auth.Type != "m.login.dummy" {
		c.JSON(http.StatusUnauthorized, clients_register_flows())
		return
	}
	name := strings.ToLower(strings.TrimSpace(body.Username))
	if name == "" {
		name = "user" + strings.ToLower(random_alphanumeric(10))
	}
	if !clients_localpart.MatchString(name) {
		clients_error(c, http.StatusBadRequest, "M_INVALID_USERNAME", "The name is not one Mochi can hold")
		return
	}
	if len(body.Device) > 100 || strings.ContainsAny(body.Device, "/ ") {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Invalid device id")
		return
	}
	// Mochi holds user-1 and user_1 as one name, written the second way,
	// which is the account's address.
	name = handle_normalise(name)
	user, account, err := clients_register_account(string(domain), name, body.Password)
	if err != nil {
		if err == clients_taken {
			clients_error(c, http.StatusBadRequest, "M_USER_IN_USE", "The name is taken")
			return
		}
		clients_error(c, http.StatusBadRequest, "M_UNKNOWN", err.Error())
		return
	}
	address := network_address("matrix", name, string(domain))
	if body.Inhibit {
		c.JSON(http.StatusOK, map[string]any{"user_id": address, "home_server": string(domain)})
		return
	}
	label := strings.TrimSpace(body.Name)
	if label == "" {
		label = name
	}
	m := signin_mint(user, "", label, "matrix", account, clients_api_scopes[0], "", body.Device, body.Refresh)
	if m == nil {
		clients_error(c, http.StatusInternalServerError, "M_UNKNOWN", "No token")
		return
	}
	answer := map[string]any{"user_id": address, "access_token": m.token, "device_id": m.device, "home_server": string(domain)}
	if m.refresh != "" {
		answer["refresh_token"] = m.refresh
		answer["expires_in_ms"] = (m.expires - now()) * 1000
	}
	c.JSON(http.StatusOK, answer)
}

// clients_taken is the error of a name someone holds already.
var clients_taken = fmt.Errorf("taken")

// clients_register_account makes a registration's user, with an identity
// named for them, their handle and a hosted Matrix account on it, and keeps
// the password given for it. Answers the user and the account's id.
func clients_register_account(domain string, name string, password string) (*User, string, error) {
	if handle_get(domain, handle_normalise(name)) != nil {
		return nil, "", clients_taken
	}
	address := email_address(name + "@" + domain)
	if address == "" {
		address = name + "@registration.invalid"
	}
	if user_by_username(address) != nil {
		return nil, "", clients_taken
	}
	user, problem := user_create(address)
	if user == nil {
		return nil, "", fmt.Errorf("cannot make the user: %s", problem)
	}
	if _, err := entity_create(user, "person", name, "public", ""); err != nil {
		return nil, "", fmt.Errorf("cannot make the identity: %v", err)
	}
	// Each name registered is a person's own: a test suite's user-1 and
	// user-2 are two people, not names under one person's user.
	h := &handle{Domain: domain, Name: handle_normalise(name), User: user.UID, Kind: "handle", Created: now()}
	db_open("db/users.db").exec("insert into handles (domain, name, user, parent, kind, created) values (?, ?, ?, '', ?, ?)", h.Domain, h.Name, h.User, h.Kind, h.Created)
	account, err := account_hosted_add(user, "matrix", h)
	if err != nil {
		return nil, "", fmt.Errorf("cannot make the account: %v", err)
	}
	if password != "" {
		password_store(user, "", account, "matrix", "registration", clients_password(network_address("matrix", h.Name, domain), password))
	}
	return user, account, nil
}

// clients_password is a password as Mochi keeps it for the account at an
// address: one a registration in development chose is kept with the prefix
// every minted password carries and the account's id, since a test suite
// gives every person the same one, and found again by them.
func clients_password(address string, password string) string {
	if !clients_registration || password == "" || strings.HasPrefix(password, "mochi-") {
		return password
	}
	_, row := account_by_address("matrix", address)
	if row == nil {
		return password
	}
	return "mochi-" + row_string(row, "id") + "-" + password
}

// clients_register_available says whether a name may be registered.
func clients_register_available(c *gin.Context, domain spec.ServerName) {
	name := strings.ToLower(strings.TrimSpace(c.Query("username")))
	if !clients_localpart.MatchString(name) {
		clients_error(c, http.StatusBadRequest, "M_INVALID_USERNAME", "The name is not one Mochi can hold")
		return
	}
	if handle_get(string(domain), handle_normalise(name)) != nil {
		clients_error(c, http.StatusBadRequest, "M_USER_IN_USE", "The name is taken")
		return
	}
	c.JSON(http.StatusOK, map[string]any{"available": true})
}

// clients_refresh renews a token a device signed in for the old way, by its
// refresh token: the old tokens end, and the device keeps its id.
func clients_refresh(c *gin.Context) {
	var body struct {
		Refresh string `json:"refresh_token"`
	}
	if !clients_body(c, &body) {
		return
	}
	var t Token
	if body.Refresh == "" || !db_open("db/users.db").scan(&t, "select hash, user, name, scopes, created, kind, client, account, network, device from tokens where refresh=? and kind=? and network='matrix' and client=''", token_hash(body.Refresh), token_signin) {
		clients_error(c, http.StatusUnauthorized, "M_UNKNOWN_TOKEN", "Unknown refresh token")
		return
	}
	used := token_useds(t.User)[t.Hash]
	if used == 0 {
		used = t.Created
	}
	user := user_by_uid(t.User)
	if now()-used > signin_refresh_lifetime || user == nil || account_hosted_get(user, t.Account) == nil {
		token_delete(t.Hash)
		clients_error(c, http.StatusUnauthorized, "M_UNKNOWN_TOKEN", "The refresh token has expired")
		return
	}
	token_delete(t.Hash)
	json.Unmarshal([]byte(t.ScopesDB), &t.Scopes)
	m := signin_mint(user, "", t.Name, "matrix", t.Account, strings.Join(t.Scopes, " "), "", t.Device, true)
	if m == nil {
		clients_error(c, http.StatusInternalServerError, "M_UNKNOWN", "No token")
		return
	}
	c.JSON(http.StatusOK, map[string]any{"access_token": m.token, "refresh_token": m.refresh, "expires_in_ms": (m.expires - now()) * 1000})
}

// clients_interactive checks the user-interactive authentication an action
// asks for: a password minted for the account. Without one, the client is
// told what to send; writes the refusal.
func clients_interactive(c *gin.Context, s *clients_session, raw json.RawMessage) bool {
	var auth struct {
		Type       string `json:"type"`
		Password   string `json:"password"`
		Session    string `json:"session"`
		Identifier struct {
			User string `json:"user"`
		} `json:"identifier"`
	}
	flows := map[string]any{"flows": []any{map[string]any{"stages": []string{"m.login.password"}}}, "params": map[string]any{}, "session": random_alphanumeric(24)}
	if len(raw) == 0 || string(raw) == "null" || json.Unmarshal(raw, &auth) != nil || auth.Type == "" {
		c.JSON(http.StatusUnauthorized, flows)
		return false
	}
	if auth.Type != "m.login.password" {
		flows["errcode"], flows["error"] = "M_UNRECOGNIZED", "Use the password Mochi made for a device" // i18n-ok: Matrix error codes and their text are protocol, read by clients
		c.JSON(http.StatusUnauthorized, flows)
		return false
	}
	if user, row, _ := password_check("matrix", s.h.address, clients_password(s.h.address, auth.Password)); user == nil || user.UID != s.user.UID || row_string(row, "id") != s.h.id {
		flows["errcode"], flows["error"] = "M_FORBIDDEN", "The password is wrong" // i18n-ok: Matrix error codes and their text are protocol, read by clients
		c.JSON(http.StatusUnauthorized, flows)
		return false
	}
	return true
}

// === Devices ===

// clients_devices answers the account's devices: each client signed in, and
// Mochi's own.
func clients_devices(c *gin.Context, s *clients_session) {
	rows, _ := federation_database().rows("select device, name, seen from clients where user=? order by created", s.h.address)
	devices := []map[string]any{}
	for _, row := range rows {
		devices = append(devices, clients_device_json(row))
	}
	c.JSON(http.StatusOK, map[string]any{"devices": devices})
}

// clients_device_json is a device as clients see it.
func clients_device_json(row map[string]any) map[string]any {
	device := map[string]any{"device_id": row_string(row, "device"), "display_name": row_string(row, "name")}
	if seen := row_int(row, "seen"); seen > 0 {
		device["last_seen_ts"] = seen * 1000
	}
	return device
}

// clients_device answers one device: reading it, renaming it, or removing it
// with the user's password.
func clients_device(c *gin.Context, s *clients_session, device string) {
	db := federation_database()
	row, _ := db.row("select device, name, seen from clients where user=? and device=?", s.h.address, device)
	if row == nil {
		clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such device")
		return
	}
	switch c.Request.Method {
	case "GET":
		c.JSON(http.StatusOK, clients_device_json(row))
	case "PUT":
		var body struct {
			Name *string `json:"display_name"`
		}
		if !clients_body(c, &body) {
			return
		}
		if body.Name != nil {
			if len(*body.Name) > 100 {
				clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "The name is too long")
				return
			}
			db.exec("update clients set name=? where user=? and device=?", *body.Name, s.h.address, device)
			keys, _ := db.row("select keys from clients where user=? and device=?", s.h.address, device)
			var held json.RawMessage
			if raw := row_string(keys, "keys"); raw != "" {
				held = json.RawMessage(raw)
			}
			keyring_changed(s.h.address, device, held, false)
			clients_wake()
		}
		c.JSON(http.StatusOK, map[string]any{})
	case "DELETE":
		var body struct {
			Auth json.RawMessage `json:"auth"`
		}
		if !clients_body(c, &body) || !clients_interactive(c, s, body.Auth) {
			return
		}
		if !clients_device_delete(s, device) {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Mochi's own device cannot be removed here")
			return
		}
		c.JSON(http.StatusOK, map[string]any{})
	default:
		clients_error(c, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "Unrecognised request")
	}
}

// encryption_device_of is the device core holds for a Matrix account, or "".
func encryption_device_of(user *User, account string) string {
	db := encryption_database(user)
	if db == nil {
		return ""
	}
	row, _ := db.row("select device from devices where account=?", account)
	return row_string(row, "device")
}

// clients_device_delete signs a device of the account out: its tokens end,
// its keys go and its pushers with them. Mochi's own device is refused.
func clients_device_delete(s *clients_session, device string) bool {
	if device == "" {
		return true
	}
	if device == encryption_device_of(s.user, s.h.id) {
		return false
	}
	rows, _ := db_open("db/users.db").rows("select hash from tokens where user=? and account=? and kind=? and device=?", s.user.UID, s.h.id, token_signin, device)
	for _, row := range rows {
		token_delete(row_string(row, "hash"))
	}
	federation_database().exec("delete from pushers where user=? and device=?", s.h.address, device)
	keyring_delete(s.h.address, device)
	clients_wake()
	return true
}

// clients_key_changes answers whose devices changed between two of sync's
// positions, of those the user shares a room with.
func clients_key_changes(c *gin.Context, s *clients_session) {
	from, to := sync_parse(c.Query("from")), sync_parse(c.Query("to"))
	rows, _ := federation_database().rows("select user, viewer from changes where id>? and id<=? order by id", from.changes, to.changes)
	shared := keyring_sharing(s.h.address)
	changed := []string{}
	for _, row := range rows {
		person := row_string(row, "user")
		viewer := row_string(row, "viewer")
		if (viewer == "" || viewer == s.h.address) && (person == s.h.address || shared[person]) && !slices.Contains(changed, person) {
			changed = append(changed, person)
		}
	}
	c.JSON(http.StatusOK, map[string]any{"changed": changed, "left": []string{}})
}

// === Profiles ===

// clients_profile_get answers someone's profile, or one field of it: an
// account here from Mochi, someone elsewhere from their server.
func clients_profile_get(c *gin.Context, domain spec.ServerName, person string, field string) {
	address, err := spec.NewUserID(person, true)
	if err != nil {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Invalid user id")
		return
	}
	profile := map[string]any{}
	if federation_local(address.Domain()) {
		if standin := bridge_standin_of(person); standin != nil {
			profile["displayname"] = row_string(standin, "name")
			if avatar := row_string(standin, "avatar"); avatar != "" {
				profile["avatar_url"] = avatar
			}
		} else {
			user, account := federation_user(person)
			if user == nil {
				clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No one is at that address")
				return
			}
			profile["displayname"] = rooms_display(user, account)
			if avatar := clients_avatar(user, account); avatar != "" {
				profile["avatar_url"] = avatar
			}
		}
	} else {
		found, err := federation_client().LookupProfile(c.Request.Context(), domain, address.Domain(), person, "")
		if err != nil {
			debug("Matrix profile of %s not read: %v", person, err)
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "The profile could not be read")
			return
		}
		if found.DisplayName != "" {
			profile["displayname"] = found.DisplayName
		}
		if found.AvatarURL != "" {
			profile["avatar_url"] = found.AvatarURL
		}
	}
	switch field {
	case "":
		c.JSON(http.StatusOK, profile)
	case "displayname", "avatar_url":
		if profile[field] == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such field")
			return
		}
		c.JSON(http.StatusOK, map[string]any{field: profile[field]})
	default:
		clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such field")
	}
}

// clients_avatar is the picture a hosted account shows on Matrix, an mxc
// address, or "".
func clients_avatar(user *User, account string) string {
	avatar, _ := account_data(account_hosted_get(user, account))["avatar"].(string)
	return avatar
}

// clients_profile_set changes the account's name or picture on Matrix - its
// name in Mochi is the account's label - and tells its rooms.
func clients_profile_set(c *gin.Context, s *clients_session, person string, field string) {
	if person != s.h.address {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Cannot change another user's profile")
		return
	}
	var body map[string]any
	if !clients_body(c, &body) {
		return
	}
	value, _ := body[field].(string)
	switch field {
	case "displayname":
		if strings.TrimSpace(value) == "" || len(value) > 100 {
			clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Invalid name")
			return
		}
		db_user(s.user, "user").account_set(s.h.id, map[string]any{"label": strings.TrimSpace(value)})
	case "avatar_url":
		if value != "" && !strings.HasPrefix(value, "mxc://") {
			clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "The picture must be an mxc address")
			return
		}
		data := account_data(account_hosted_get(s.user, s.h.id))
		data["avatar"] = value
		db_user(s.user, "user").account_set(s.h.id, map[string]any{"data": json_encode(data)})
	default:
		clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such field")
		return
	}
	// Each room the account is in is told the new name and picture.
	for _, room := range signals_rooms(s.h.address) {
		key := s.h.address
		rooms_send(context.Background(), room, s.h.address, spec.MRoomMember, &key, rooms_member_content(s.h.address, spec.Join))
	}
	c.JSON(http.StatusOK, map[string]any{})
}

// === Presence ===

// clients_presence answers someone's presence - an account here shows its
// user's in Mochi, or nothing where the user does not share it - or takes a
// client's word on its own, which Mochi's presence is made of elsewhere.
func clients_presence(c *gin.Context, s *clients_session, person string) {
	if c.Request.Method == "PUT" {
		if person != s.h.address {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Cannot set another user's presence")
			return
		}
		var body map[string]any
		if !clients_body(c, &body) {
			return
		}
		c.JSON(http.StatusOK, map[string]any{})
		return
	}
	answer := map[string]any{"presence": "offline"}
	if user, account := federation_user(person); user != nil {
		row, _ := federation_database().row("select hidden from presence where account=?", account)
		if row_int(row, "hidden") == 0 {
			status, _, connections := presence_user(user.UID)
			switch status {
			case "online":
				answer["presence"], answer["currently_active"] = "online", true
			case "away", "busy":
				answer["presence"] = "unavailable"
			}
			for _, connection := range connections {
				if connection.Message != "" {
					answer["status_msg"] = connection.Message
				}
			}
		}
	}
	c.JSON(http.StatusOK, answer)
}

// === Finding people ===

// clients_directory_search finds people by name or address: those the user
// shares a room with, and the server's own accounts by their handle.
func clients_directory_search(c *gin.Context, s *clients_session) {
	var body struct {
		Term  string `json:"search_term"`
		Limit int    `json:"limit"`
	}
	if !clients_body(c, &body) {
		return
	}
	term := strings.ToLower(strings.TrimSpace(body.Term))
	limit := body.Limit
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	found := map[string]map[string]any{}
	var order []string
	add := func(address string, name string, avatar string) {
		if found[address] != nil || len(order) > limit {
			return
		}
		if term != "" && !strings.Contains(strings.ToLower(address), term) && !strings.Contains(strings.ToLower(name), term) {
			return
		}
		result := map[string]any{"user_id": address}
		if name != "" {
			result["display_name"] = name
		}
		if avatar != "" {
			result["avatar_url"] = avatar
		}
		found[address] = result
		order = append(order, address)
	}
	for _, room := range signals_rooms(s.h.address) {
		state, _ := rooms_current(room)
		names := rooms_names(room, state)
		for member, membership := range rooms_members(state) {
			if membership == spec.Join {
				add(member, names[room+" "+member], "")
			}
		}
	}
	if term != "" {
		rows, _ := networks_database().rows("select user, id, domain, handle from actors where network='matrix' and deleted=0 and handle like ? limit 50", "%"+term+"%")
		for _, row := range rows {
			if user := user_by_uid(row_string(row, "user")); user != nil {
				add("@"+row_string(row, "handle")+":"+row_string(row, "domain"), rooms_display(user, row_string(row, "id")), clients_avatar(user, row_string(row, "id")))
			}
		}
	}
	results := []map[string]any{}
	for _, address := range order {
		if len(results) < limit {
			results = append(results, found[address])
		}
	}
	c.JSON(http.StatusOK, map[string]any{"results": results, "limited": len(order) > limit})
}

// === Account data ===

// clients_data answers the account's data of a type, global or for a room.
func clients_data(c *gin.Context, s *clients_session, person string, room string, kind string) {
	if person != s.h.address {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Cannot touch another user's data")
		return
	}
	switch c.Request.Method {
	case "GET":
		content := clients_setting(s.h.address, room, kind)
		if content == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such data")
			return
		}
		c.Data(http.StatusOK, "application/json", content)
	case "PUT":
		raw, ok := clients_raw(c)
		if !ok {
			return
		}
		var object map[string]any
		if json.Unmarshal(raw, &object) != nil {
			clients_error(c, http.StatusBadRequest, "M_NOT_JSON", "The content is not a JSON object")
			return
		}
		if kind == "m.fully_read" || kind == "m.push_rules" {
			clients_error(c, http.StatusBadRequest, "M_BAD_JSON", "That data is set by its own request")
			return
		}
		clients_setting_set(s.h.address, room, kind, raw)
		c.JSON(http.StatusOK, map[string]any{})
	default:
		clients_error(c, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "Unrecognised request")
	}
}

// clients_setting is a piece of an account's data, or nil.
func clients_setting(user string, room string, kind string) json.RawMessage {
	row, _ := federation_database().row("select content from settings where user=? and room=? and type=?", user, room, kind)
	if row == nil {
		return nil
	}
	return json.RawMessage(row_string(row, "content"))
}

// clients_setting_set keeps a piece of an account's data at a new position,
// so the account's syncs are given it.
func clients_setting_set(user string, room string, kind string, content json.RawMessage) {
	db := federation_database()
	db.exec("delete from settings where user=? and room=? and type=?", user, room, kind)
	db.exec("insert into settings (user, room, type, content) values (?, ?, ?, ?)", user, room, kind, string(content))
	clients_wake()
}

// clients_tags answers a room's tags for the account, or adds or removes
// one: kept as the room's m.tag data.
func clients_tags(c *gin.Context, s *clients_session, person string, room string, tag string) {
	if person != s.h.address {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Cannot touch another user's tags")
		return
	}
	var held struct {
		Tags map[string]any `json:"tags"`
	}
	if content := clients_setting(s.h.address, room, "m.tag"); content != nil {
		json.Unmarshal(content, &held)
	}
	if held.Tags == nil {
		held.Tags = map[string]any{}
	}
	switch c.Request.Method {
	case "GET":
		c.JSON(http.StatusOK, map[string]any{"tags": held.Tags})
		return
	case "PUT":
		var body map[string]any
		if !clients_body(c, &body) {
			return
		}
		if len(tag) > 255 {
			clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "The tag is too long")
			return
		}
		held.Tags[tag] = body
	case "DELETE":
		delete(held.Tags, tag)
	default:
		clients_error(c, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "Unrecognised request")
		return
	}
	clients_setting_set(s.h.address, room, "m.tag", json.RawMessage(json_encode(held)))
	c.JSON(http.StatusOK, map[string]any{})
}

// === Filters ===

// clients_filter_create keeps a filter a client will name in its syncs.
func clients_filter_create(c *gin.Context, s *clients_session, person string) {
	if person != s.h.address {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Cannot make another user's filter")
		return
	}
	raw, ok := clients_raw(c)
	if !ok {
		return
	}
	var filter sync_filter
	if json.Unmarshal(raw, &filter) != nil {
		clients_error(c, http.StatusBadRequest, "M_NOT_JSON", "The filter is not JSON")
		return
	}
	result, err := federation_database().internal.Exec("insert into filters (user, body) values (?, ?)", s.h.address, string(raw))
	if err != nil {
		clients_error(c, http.StatusInternalServerError, "M_UNKNOWN", "The filter could not be kept")
		return
	}
	id, _ := result.LastInsertId()
	c.JSON(http.StatusOK, map[string]any{"filter_id": strconv.FormatInt(id, 10)})
}

// clients_filter_get answers a filter the account kept.
func clients_filter_get(c *gin.Context, s *clients_session, person string, id string) {
	row, _ := federation_database().row("select body from filters where id=? and user=?", id, s.h.address)
	if row == nil || person != s.h.address {
		clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such filter")
		return
	}
	c.Data(http.StatusOK, "application/json", []byte(row_string(row, "body")))
}

// === Media ===

// clients_media answers the media paths: the upload limit, an upload, and a
// file or its thumbnail - the account's own and other servers' fetched as
// it - which the authenticated paths and the old ones answer alike.
func clients_media(c *gin.Context, parts []string) {
	// The client interface's media paths are under "media", the old ones
	// directly under /_matrix/media/.
	if len(parts) > 0 && parts[0] == "media" {
		parts = parts[1:]
	}
	if len(parts) == 1 && parts[0] == "config" && c.Request.Method == "GET" {
		if clients_authenticate(c) == nil {
			return
		}
		c.JSON(http.StatusOK, map[string]any{"m.upload.size": clients_upload_maximum()})
		return
	}
	if len(parts) == 1 && parts[0] == "preview_url" {
		if clients_authenticate(c) == nil {
			return
		}
		c.JSON(http.StatusOK, map[string]any{})
		return
	}
	if len(parts) == 1 && parts[0] == "upload" && c.Request.Method == "POST" {
		s := clients_authenticate(c)
		if s == nil {
			return
		}
		data, err := io.ReadAll(io.LimitReader(c.Request.Body, clients_upload_maximum()+1))
		if err != nil || int64(len(data)) > clients_upload_maximum() {
			clients_error(c, http.StatusRequestEntityTooLarge, "M_TOO_LARGE", "The file is too large")
			return
		}
		address, _ := spec.NewUserID(s.h.address, true)
		kind := c.ContentType()
		stored, err := content_store(s.user, string(address.Domain()), data, kind, c.Query("filename"))
		if err != nil {
			clients_error(c, http.StatusBadRequest, "M_UNKNOWN", "The file could not be kept")
			return
		}
		c.JSON(http.StatusOK, map[string]any{"content_uri": stored["url"]})
		return
	}
	if len(parts) >= 3 && (parts[0] == "download" || parts[0] == "thumbnail") && c.Request.Method == "GET" {
		s := clients_authenticate(c)
		if s == nil {
			return
		}
		server, id := parts[1], parts[2]
		if !content_id_re.MatchString(id) {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such file")
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()
		status, kind, data, err := content_fetch(ctx, s.h, content_address("mxc://"+server+"/"+id))
		if err != nil || status != 200 {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "The file could not be fetched")
			return
		}
		if kind == "" {
			kind = "application/octet-stream"
		}
		name := ""
		if len(parts) >= 4 {
			name = parts[3]
		} else {
			name = content_name(s.h.user, server, id)
		}
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("Content-Security-Policy", "sandbox; default-src 'none'; script-src 'none'; plugin-types application/pdf; style-src 'unsafe-inline'; object-src 'self';")
		disposition := "attachment"
		if strings.HasPrefix(kind, "image/") || strings.HasPrefix(kind, "video/") || strings.HasPrefix(kind, "audio/") {
			disposition = "inline"
		}
		if name != "" {
			c.Header("Content-Disposition", disposition+"; filename*=utf-8''"+url.PathEscape(name))
		} else {
			c.Header("Content-Disposition", disposition)
		}
		c.Data(http.StatusOK, kind, data)
		return
	}
	clients_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognised request")
}
