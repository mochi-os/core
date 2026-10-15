// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Linked accounts
//
// A linked account is the user's existing account on a server someone else
// runs: a Mastodon or Lemmy account, a Bluesky account, an XMPP or Matrix
// account. It is a row of the user's accounts table like a hosted one, of the
// provider type the network's client protocol names, with the credential the
// server issued in its data, which never reaches an app. Core acts on the
// account for the app: mochi.account.link connects one, and the account
// functions post, read and fetch through it, each request under the owner's
// and the user's domain blocks, the limiter per host and account, and the
// private-address check. The owner's protocol setting does not apply, since a
// linked account only makes requests as the user. A chat account (XMPP,
// Matrix) also has a long-lived session, held by the manager at the end.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// linked_kinds are the provider types a linked account may have, in the order
// the providers are listed.
var linked_kinds = []string{"bluesky", "lemmy", "mastodon", "matrix", "xmpp"}

// linked_kind reports whether a provider type is one a linked account has.
func linked_kind(kind string) bool {
	return slices.Contains(linked_kinds, kind)
}

// linked_chat reports whether a kind is a chat network, whose account has a
// session the manager holds.
func linked_chat(kind string) bool {
	return kind == "xmpp" || kind == "matrix"
}

// provider_network reports whether a provider type is a network's: a hosted
// account's (fediverse, bluesky, xmpp, matrix) or a linked one's (those and
// mastodon, lemmy).
func provider_network(ptype string) bool {
	return network_known(ptype) || linked_kind(ptype)
}

// linked_signed_out is the error a protocol answers when the server no longer
// accepts the account's credential: the user links the account again.
var linked_signed_out = errors.New("the server no longer accepts this account's sign-in; link it again")

// linked_body_maximum caps what a server may answer with: a page of a
// timeline runs to a few hundred kilobytes. A variable so tests can lower it.
var linked_body_maximum int64 = 8 << 20

// network_scheme is how another server is reached: HTTPS, as the host policy
// requires. A variable so tests can serve from a plain server of their own.
var network_scheme = "https://"

// linked_timeout bounds one request to the account's server, short of
// network_timeout so a Matrix sync's long poll (matrix_sync_timeout) fits
// inside it with room to answer.
const linked_timeout = 45 * time.Second

// A linked is one linked account loaded with its data.
type linked struct {
	user       *User
	id         string
	kind       string
	label      string
	identifier string
	data       map[string]any
}

// A linked_profile is what a server says about the account linked on it.
type linked_profile struct {
	ID      string // the account's permanent identifier on its network
	Address string // readable: @alice@mastodon.social, alice.bsky.social, @alice:matrix.org, alice@example.org
	Name    string
	Avatar  string
	URL     string // the account's page
	Server  string // the host the account is on
}

// A linked_page is one page of a timeline, of an account's posts or of its
// notifications: the items in the network's own form and the cursor for the
// next page, empty at the end.
type linked_page struct {
	Items  []any
	Cursor string
}

// account_data decodes an account row's data column.
func account_data(row map[string]any) map[string]any {
	if data, ok := row["data"].(map[string]any); ok {
		return data
	}
	data := map[string]any{}
	if text, _ := row["data"].(string); text != "" {
		json.Unmarshal([]byte(text), &data)
	}
	return data
}

// account_hosted_flag reads whether a network row is a hosted account.
func account_hosted_flag(row map[string]any) bool {
	hosted, _ := account_data(row)["hosted"].(bool)
	return hosted
}

// accounts_network lists a user's accounts of network types, each with its data
// decoded: every network type when kinds is empty, else those named.
func accounts_network(user *User, kinds ...string) []map[string]any {
	db := db_user(user, "user")
	rows, _ := db.rows("select id, type, label, identifier, data, created, verified, enabled from accounts order by created")
	var out []map[string]any
	for _, row := range rows {
		ptype, _ := row["type"].(string)
		if !provider_network(ptype) || (len(kinds) > 0 && !slices.Contains(kinds, ptype)) {
			continue
		}
		row["data"] = account_data(row)
		out = append(out, row)
	}
	return out
}

// linked_from reads a linked account from its row, or nil for a hosted one or
// a row of another type.
func linked_from(user *User, row map[string]any) *linked {
	if row == nil {
		return nil
	}
	kind, _ := row["type"].(string)
	if !linked_kind(kind) || account_hosted_flag(row) {
		return nil
	}
	l := &linked{user: user, kind: kind, data: account_data(row)}
	l.id, _ = row["id"].(string)
	l.label, _ = row["label"].(string)
	l.identifier, _ = row["identifier"].(string)
	return l
}

// linked_get loads a user's linked account by id, or nil.
func linked_get(user *User, id string) *linked {
	row, _ := db_user(user, "user").row("select id, type, label, identifier, data from accounts where id=?", id)
	return linked_from(user, row)
}

// get reads a string from the account's data.
func (l *linked) get(key string) string {
	value, _ := l.data[key].(string)
	return value
}

// set changes the account's data in memory; save writes it.
func (l *linked) set(key string, value any) {
	if value == nil || value == "" {
		delete(l.data, key)
		return
	}
	l.data[key] = value
}

// save writes the account's data back to its row.
func (l *linked) save() {
	db_user(l.user, "user").account_set(l.id, map[string]any{"data": json_encode(l.data)})
}

// linked_store keeps a newly linked account: the row of the same kind and
// identifier is updated when the account was linked before, so a second link
// renews the credential and keeps the id apps hold, else a row is made. The
// chat manager is started for a chat account. Returns the row, decoded.
func linked_store(user *User, kind string, profile *linked_profile, data map[string]any) map[string]any {
	db := db_user(user, "user")
	data["id"] = profile.ID
	data["url"] = profile.URL
	data["name"] = profile.Name
	data["avatar"] = profile.Avatar
	data["server"] = profile.Server
	delete(data, "hosted")
	delete(data, "error")
	id := ""
	for _, row := range accounts_network(user, kind) {
		if !account_hosted_flag(row) && account_data(row)["id"] == profile.ID {
			id, _ = row["id"].(string)
		}
	}
	if id == "" {
		id = uid()
		db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values (?, ?, ?, ?, ?, ?, ?)", id, kind, profile.Name, profile.Address, json_encode(data), now(), now())
	} else {
		db.account_set(id, map[string]any{"identifier": profile.Address, "data": json_encode(data), "verified": now(), "enabled": 1})
	}
	row, _ := db.row("select id, type, label, identifier, data, created, verified, enabled, \"default\", device from accounts where id=?", id)
	if l := linked_from(user, row); l != nil && linked_chat(kind) {
		linked_stop(user.UID, id)
		linked_start(l)
	}
	return row
}

// linked_forget ends a linked account: the server is told where the protocol
// has a way to say so, and the chat session is stopped. The row is removed by
// the caller.
func linked_forget(ctx context.Context, l *linked) {
	linked_stop(l.user.UID, l.id)
	switch l.kind {
	case "matrix":
		matrix_logout(ctx, l)
	case "bluesky":
		bluesky_forget(ctx, l)
	}
}

// === Linking ===

// provider_link is how an existing account of a provider is linked now: the
// provider's link as declared, except that a Bluesky account links by OAuth
// only where the system has a configured domain for the client document the
// remote server fetches, and by app password elsewhere.
func provider_link(p *Provider) *ProviderLink {
	if p.Link == nil {
		return nil
	}
	if p.Type == "bluesky" && server_domain() == "" {
		return &ProviderLink{Flow: "form", Fields: []ProviderField{
			{Name: "handle", Label: "accounts.field.handle", Type: "text", Required: true, Placeholder: "alice.bsky.social"},
			{Name: "password", Label: "accounts.field.app", Type: "password", Required: true, Placeholder: "xxxx-xxxx-xxxx-xxxx"},
		}}
	}
	return p.Link
}

// mochi.account.link(type, target=..., scheme=..., challenge=..., ...fields) -> dict: Link an existing account on another network. A form provider (lemmy, xmpp, matrix, and bluesky where the system has no configured domain) takes its fields, signs in to the server with them and returns the account as mochi.account.get does. An OAuth provider (mastodon, and bluesky with a configured domain) takes the server or the handle and returns {url}, a path on this server the browser visits at the top window; the remote server's consent returns the browser to target with linked=<account id>, or link_error=<code>. A native app passes its deep-link scheme and PKCE challenge as mochi.account.grant does, is answered {url, nonce}, and lands the link at the exchange
func api_account_link(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <type: string>, [target=...], [scheme=...], [challenge=...], fields...")
	}
	if err := require_permission(t, fn, "accounts/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	kind, _ := sl.AsString(args[0])
	provider := provider_get(kind)
	if provider == nil || !linked_kind(kind) {
		return sl_error(fn, "unknown provider type %q", kind)
	}
	if !networks_development {
		return sl_error(fn, "linking accounts is not available on this server")
	}
	fields := map[string]string{}
	var target, scheme, challenge string
	for _, kv := range kwargs {
		key := string(kv[0].(sl.String))
		value, _ := sl.AsString(kv[1])
		switch key {
		case "target":
			target = value
		case "scheme":
			scheme = value
		case "challenge":
			challenge = value
		default:
			fields[key] = strings.TrimSpace(value)
		}
	}
	link := provider_link(provider)
	for _, f := range link.Fields {
		if f.Required && fields[f.Name] == "" {
			return sl_error(fn, "required field %q is missing", f.Name)
		}
	}
	if link.Flow == "oauth" {
		return linked_oauth_begin(t, fn, user, kind, fields, target, scheme, challenge)
	}
	ctx, cancel := context.WithTimeout(thread_context(t), linked_timeout)
	defer cancel()
	var profile *linked_profile
	var data map[string]any
	var err error
	switch kind {
	case "lemmy":
		profile, data, err = lemmy_link(ctx, user, fields)
	case "bluesky":
		profile, data, err = bluesky_link_password(ctx, user, fields)
	case "matrix":
		profile, data, err = matrix_link(ctx, user, fields)
	case "xmpp":
		profile, data, err = xmpp_link(ctx, user, fields)
	}
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(account_present(user, linked_store(user, kind, profile, data))), nil
}

// linked_oauth_begin starts the consent of a Mastodon or Bluesky server for a
// link: the ceremony is prepared, the protocol registers or pushes what it
// must and names the consent URL, and the ceremony is stored bound to the
// session as a grant's is.
func linked_oauth_begin(t *sl.Thread, fn *sl.Builtin, user *User, kind string, fields map[string]string, target, scheme, challenge string) (sl.Value, error) {
	action, _ := t.Local("action").(*Action)
	if action == nil || action.web == nil {
		return sl_error(fn, "no request context")
	}
	mode := "account"
	if scheme != "" || challenge != "" {
		if !oauth_valid_mobile_scheme(scheme) {
			return sl_error(fn, "invalid scheme")
		}
		if len(challenge) < 32 || len(challenge) > 128 {
			return sl_error(fn, "invalid challenge")
		}
		mode = "mobile"
	}
	if redirect_local(target) == "" {
		return sl_error(fn, "invalid target")
	}
	provider, ok := oauth_providers()[kind]
	if !ok || !oauth_enabled(kind) {
		return sl_error(fn, "unknown provider")
	}
	ctx, cancel := context.WithTimeout(thread_context(t), linked_timeout)
	defer cancel()
	state, st := oauth_ceremony_prepare(action.web, kind, user.UID, target, mode, scheme, challenge, "")
	var err error
	switch kind {
	case "mastodon":
		err = mastodon_prepare(ctx, user, st, fields["server"])
	case "bluesky":
		err = bluesky_prepare(ctx, user, st, fields["handle"])
	}
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	url, returned, err := oauth_ceremony_begin(action.web, provider, kind, user.UID, state, st)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if mode == "mobile" {
		return sl_encode(map[string]any{"url": url, "nonce": returned}), nil
	}
	return sl_encode(map[string]any{"url": url}), nil
}

// linked_callback completes a Mastodon or Bluesky consent: the protocol
// exchanges the code, reads the account and answers what to store. A web link
// stores it and sends the browser back to the target with the account's id; a
// native app's is stashed under its PKCE challenge and landed at the exchange,
// as a grant's is, since the callback's browser proves neither the app nor the
// user.
func linked_callback(c *gin.Context, kind string, st *oauth_state, user_id string, code string) {
	user := user_by_uid(user_id)
	if user == nil {
		linked_link_failed(c, st, "provider_error")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), linked_timeout)
	defer cancel()
	var profile *linked_profile
	var data map[string]any
	var err error
	switch kind {
	case "mastodon":
		profile, data, err = mastodon_callback(ctx, user, st, code)
	case "bluesky":
		profile, data, err = bluesky_callback(ctx, user, st, code, c.Query("iss"))
	}
	if err != nil {
		warn("Linking a %s account: %v", kind, err)
		linked_link_failed(c, st, "provider_error")
		return
	}
	if st.Mode == "mobile" {
		exchange, err := oauth_mobile_store(st.Challenge, map[string]any{
			"linked":  true,
			"user":    user_id,
			"kind":    kind,
			"profile": profile,
			"data":    data,
		})
		if err != nil {
			oauth_mobile_error_named(c, st, oauth_link_return, "server_error", nil)
			return
		}
		oauth_mobile_redirect_named(c, st, oauth_link_return, exchange, "", nil)
		return
	}
	row := linked_store(user, kind, profile, data)
	c.Redirect(http.StatusFound, linked_target(st.Target, "linked", row_string(row, "id")))
}

// linked_link_failed sends the browser, or the app, back with why the link
// did not happen.
func linked_link_failed(c *gin.Context, st *oauth_state, code string) {
	if st.Mode == "mobile" {
		oauth_mobile_error_named(c, st, oauth_link_return, code, nil)
		return
	}
	c.Redirect(http.StatusFound, linked_target(st.Target, "link_error", code))
}

// linked_target is the app's target with one query parameter added.
func linked_target(target string, key string, value string) string {
	target = redirect_local(target)
	if target == "" {
		target = "/"
	}
	sep := "?"
	if strings.Contains(target, "?") {
		sep = "&"
	}
	return target + sep + key + "=" + url.QueryEscape(value)
}

// linked_exchange lands the link a native app began, once the PKCE verifier
// has proved the app and the bearer token the user. Answers {linked: <id>}.
func linked_exchange(c *gin.Context, data map[string]any) {
	link_user, _ := data["user"].(string)
	kind, _ := data["kind"].(string)
	stored, _ := data["data"].(map[string]any)
	var profile linked_profile
	if raw, err := json.Marshal(data["profile"]); err == nil {
		json.Unmarshal(raw, &profile)
	}
	if link_user == "" || !linked_kind(kind) || stored == nil || profile.ID == "" {
		respond_error(c, http.StatusInternalServerError, "server_error", "errors.server_error", nil)
		return
	}
	token := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
	if token == "" {
		respond_error(c, http.StatusUnauthorized, "not_authenticated", "errors.not_authenticated", nil)
		return
	}
	uid, _, err := jwt_verify(token)
	if err != nil || uid == "" || uid != link_user {
		audit_login_failed(link_user, rate_limit_client_ip(c), "oauth_link_user_mismatch")
		respond_error(c, http.StatusForbidden, "link_user_mismatch", "errors.link_user_mismatch", nil)
		return
	}
	user := user_by_uid(link_user)
	if user == nil {
		respond_error(c, http.StatusInternalServerError, "server_error", "errors.server_error", nil)
		return
	}
	row := linked_store(user, kind, &profile, stored)
	c.JSON(http.StatusOK, map[string]any{"linked": row_string(row, "id")})
}

// === Signing in to Mochi with an account on another network ===
//
// A Mastodon or Bluesky account signs its holder in to Mochi: the login page
// begins the same consent a link does, with no user yet, and the callback
// completes it in the protocol. An identity seen before signs its user in and
// refreshes the linked account's credential. A new one, where sign-up is open,
// is kept for ten minutes as a pending identity while the browser goes to the
// login page to give an email address, which the code sent to it proves; the
// login that follows - by that code or any other factor - creates or names the
// user, and the identity joins them as a linked account and as a way to sign
// in. The provider's email is never trusted for this (decision 6): the user
// proves the address themselves.

const network_login_cookie = "mochi_network"
const network_login_lifetime = 600

// network_login_offered reports whether a network's accounts may sign users
// in here: while the server offers networks, and for Bluesky only with a
// configured domain, since its consent is by OAuth as a link's is.
func network_login_offered(name string) bool {
	if !oauth_enabled(name) {
		return false
	}
	switch name {
	case "mastodon":
		return true
	case "bluesky":
		return bluesky_client_base() != ""
	}
	return false
}

// network_login_begin starts a sign-in: the ceremony is prepared as a web
// login's is, bound to the browser, the protocol registers or pushes what it
// must with no user yet, and the browser is sent to the server's consent.
func network_login_begin(c *gin.Context, provider *oauth_provider, target string, server string, handle string) {
	ctx, cancel := context.WithTimeout(context.Background(), linked_timeout)
	defer cancel()
	state, st := oauth_ceremony_prepare(c, provider.name, "", target, "", "", "", "")
	var err error
	switch provider.network {
	case "mastodon":
		if strings.TrimSpace(server) == "" {
			respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
			return
		}
		err = mastodon_prepare(ctx, nil, st, server)
	case "bluesky":
		if strings.TrimSpace(handle) == "" {
			respond_error(c, http.StatusBadRequest, "invalid_request", "errors.invalid_request", nil)
			return
		}
		err = bluesky_prepare(ctx, nil, st, handle)
	}
	if err == nil {
		_, _, err = oauth_ceremony_begin(c, provider, provider.name, "", state, st)
	}
	if err != nil {
		warn("Signing in with a %s account: %v", provider.name, err)
		respond_error(c, http.StatusServiceUnavailable, "provider_unavailable", "errors.provider_unavailable", nil)
		return
	}
	c.JSON(http.StatusOK, gin.H{"url": st.Consent})
}

// linked_subject is the identity a sign-in is keyed by: the account's
// permanent identifier on its network, its actor URI on the fediverse and its
// DID on Bluesky, which outlive a change of handle.
func linked_subject(kind string, profile *linked_profile) string {
	return profile.ID
}

// linked_login completes a sign-in once the account's server has consented.
func linked_login(c *gin.Context, kind string, st *oauth_state, code string) {
	ctx, cancel := context.WithTimeout(context.Background(), linked_timeout)
	defer cancel()
	var profile *linked_profile
	var data map[string]any
	var err error
	switch kind {
	case "mastodon":
		profile, data, err = mastodon_callback(ctx, nil, st, code)
	case "bluesky":
		profile, data, err = bluesky_callback(ctx, nil, st, code, c.Query("iss"))
	}
	if err != nil || profile == nil || profile.ID == "" {
		warn("Signing in with a %s account: %v", kind, err)
		oauth_error_redirect(c, "provider_error", nil)
		return
	}
	subject := linked_subject(kind, profile)
	users := db_open("db/users.db")
	if row, _ := users.row("select user from oauth where provider=? and subject=?", kind, subject); row != nil {
		uid := row_string(row, "user")
		user := user_by_uid(uid)
		if user == nil {
			if user_suspended(uid) {
				audit_login_failed(profile.Address, rate_limit_client_ip(c), "suspended")
				oauth_error_redirect(c, "suspended", nil)
				return
			}
			audit_login_failed(profile.Address, rate_limit_client_ip(c), "oauth_user_missing")
			oauth_error_redirect(c, "provider_error", nil)
			return
		}
		if user_method_disabled(user, "oauth") {
			audit_login_failed(user.Username, rate_limit_client_ip(c), "oauth_disabled")
			oauth_error_redirect(c, "oauth_disallowed", nil)
			return
		}
		linked_store(user, kind, profile, data)
		users.exec("update oauth set name=? where provider=? and subject=?", profile.Name, kind, subject)
		oauth_login_complete(c, user, st.Target)
		return
	}
	if !setting_signup_enabled() {
		audit_login_failed(profile.Address, rate_limit_client_ip(c), "oauth_signup_disabled")
		oauth_error_redirect(c, "signup_disabled", nil)
		return
	}
	pending := map[string]any{"kind": kind, "subject": subject, "profile": profile, "data": data, "target": st.Target}
	id := random_alphanumeric(32)
	db_open("db/sessions.db").exec("insert into ceremonies (id, type, user, challenge, data, expires) values (?, 'network_login', '', ?, ?, ?)",
		id, []byte(id), json_encode(pending), now()+network_login_lifetime)
	network_login_set(c, id)
	c.Redirect(http.StatusFound, app_login_route("")+"?network="+kind)
}

// network_login_set keeps the pending identity's id with the browser for the
// login that follows. Lax, as the binding cookie is: the login page is
// reached by a redirect from the account's server.
func network_login_set(c *gin.Context, id string) {
	secure := web_https && !web_is_localhost(c)
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(network_login_cookie, id, network_login_lifetime, "/", "", secure, true)
}

// network_login_unset clears the pending identity's cookie.
func network_login_unset(c *gin.Context) {
	secure := web_https && !web_is_localhost(c)
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(network_login_cookie, "", -1, "/", "", secure, true)
}

// network_login_apply joins a pending identity to the user a login has just
// proved (auth_establish_session): the identity becomes a way to sign in and
// the account a linked account, and the ceremony is spent. Without the cookie
// nothing happens.
func network_login_apply(c *gin.Context, user *User) {
	id := web_cookie_get(c, network_login_cookie, "")
	if id == "" || user == nil {
		return
	}
	network_login_unset(c)
	sessions := db_open("db/sessions.db")
	row, _ := sessions.row("select data from ceremonies where id=? and type='network_login' and expires>?", id, now())
	if row == nil {
		return
	}
	sessions.exec("delete from ceremonies where id=?", id)
	var pending struct {
		Kind    string         `json:"kind"`
		Subject string         `json:"subject"`
		Profile linked_profile `json:"profile"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(row_string(row, "data")), &pending); err != nil || !linked_kind(pending.Kind) || pending.Subject == "" || pending.Profile.ID == "" || pending.Data == nil {
		return
	}
	users := db_open("db/users.db")
	if taken, _ := users.exists("select 1 from oauth where provider=? and subject=?", pending.Kind, pending.Subject); taken {
		return
	}
	users.exec("insert into oauth (user, provider, subject, email, verified, name, created) values (?, ?, ?, ?, 1, ?, ?)",
		user.UID, pending.Kind, pending.Subject, user.Username, pending.Profile.Name, now())
	linked_store(user, pending.Kind, &pending.Profile, pending.Data)
}

// === Requests ===

// request sends one request as the account: the method, the full address, the
// headers, and a body; the account's credential is added by its protocol, and
// every request goes through the host policy for the user with the limiter
// keyed by the host and the account, since that is how the server limits the
// account. A credential the server says has expired is renewed once and the
// request sent again; a nonce a server demands is taken up the same way. It
// answers the status, the body up to linked_body_maximum and the headers.
func (l *linked) request(ctx context.Context, method string, address string, headers map[string]string, body []byte) (int, []byte, http.Header, error) {
	renewed, nonced := false, false
	for {
		request, err := http.NewRequestWithContext(ctx, method, address, bytes.NewReader(body))
		if err != nil {
			return 0, nil, nil, err
		}
		for name, value := range headers {
			request.Header.Set(name, value)
		}
		if request.Header.Get("Accept") == "" {
			request.Header.Set("Accept", "application/json")
		}
		if err := l.authorize(request, body); err != nil {
			return 0, nil, nil, err
		}
		response, err := network_send(ctx, l.user, "", strings.ToLower(request.URL.Hostname())+"|"+l.id, request, 0)
		if err != nil {
			return 0, nil, nil, err
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, linked_body_maximum+1))
		response.Body.Close()
		if err != nil {
			return 0, nil, nil, err
		}
		if int64(len(data)) > linked_body_maximum {
			return 0, nil, nil, fmt.Errorf("the server's answer is over %d bytes", linked_body_maximum)
		}
		if l.kind == "bluesky" {
			if nonce := response.Header.Get("DPoP-Nonce"); nonce != "" {
				bluesky_nonce_set(request.URL.Host, nonce)
				if !nonced && bluesky_wants_nonce(response, data) {
					nonced = true
					continue
				}
			}
		}
		// A refused sign-in is not an expired credential: only a request made
		// with a token has one to renew.
		if !renewed && l.get("token") != "" && l.expired(response.StatusCode, data) {
			renewed = true
			if err := l.renew(ctx); err == nil {
				continue
			} else if errors.Is(err, linked_signed_out) {
				l.set("error", err.Error())
				l.save()
				return response.StatusCode, data, response.Header, err
			}
		}
		return response.StatusCode, data, response.Header, nil
	}
}

// json sends a request whose body, when not nil, is JSON, and decodes a 2xx
// answer into into when given. It answers the status and the raw body.
func (l *linked) json(ctx context.Context, method string, address string, body any, into any) (int, []byte, error) {
	var encoded []byte
	headers := map[string]string{}
	if body != nil {
		encoded = []byte(json_encode(body))
		headers["Content-Type"] = "application/json"
	}
	status, data, _, err := l.request(ctx, method, address, headers, encoded)
	if err != nil {
		return status, data, err
	}
	if into != nil && status >= 200 && status < 300 && len(data) > 0 {
		if err := json.Unmarshal(data, into); err != nil {
			return status, data, fmt.Errorf("the server's answer is not JSON: %v", err)
		}
	}
	return status, data, nil
}

// form sends a form-encoded POST and decodes a 2xx answer.
func (l *linked) form(ctx context.Context, address string, values url.Values, into any) (int, []byte, error) {
	status, data, _, err := l.request(ctx, "POST", address, map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, []byte(values.Encode()))
	if err != nil {
		return status, data, err
	}
	if into != nil && status >= 200 && status < 300 && len(data) > 0 {
		if err := json.Unmarshal(data, into); err != nil {
			return status, data, fmt.Errorf("the server's answer is not JSON: %v", err)
		}
	}
	return status, data, nil
}

// multipart sends a file as a multipart form, under the field name given with
// the other fields beside it, and decodes a 2xx answer.
func (l *linked) multipart(ctx context.Context, address string, field string, name string, kind string, content []byte, fields map[string]string, into any) (int, []byte, error) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	for key, value := range fields {
		if value != "" {
			writer.WriteField(key, value)
		}
	}
	header := make(map[string][]string)
	header["Content-Disposition"] = []string{fmt.Sprintf(`form-data; name="%s"; filename="%s"`, field, strings.ReplaceAll(name, `"`, ""))}
	header["Content-Type"] = []string{kind}
	part, err := writer.CreatePart(header)
	if err != nil {
		return 0, nil, err
	}
	part.Write(content)
	writer.Close()
	status, data, _, err := l.request(ctx, "POST", address, map[string]string{"Content-Type": writer.FormDataContentType()}, buffer.Bytes())
	if err != nil {
		return status, data, err
	}
	if into != nil && status >= 200 && status < 300 && len(data) > 0 {
		if err := json.Unmarshal(data, into); err != nil {
			return status, data, fmt.Errorf("the server's answer is not JSON: %v", err)
		}
	}
	return status, data, nil
}

// authorize adds the account's credential to a request.
func (l *linked) authorize(request *http.Request, body []byte) error {
	switch l.kind {
	case "mastodon", "lemmy", "matrix":
		// No token yet, as at the sign-in: the request goes plain.
		token := l.get("token")
		if token == "" {
			return nil
		}
		request.Header.Set("Authorization", "Bearer "+token)
		if l.kind == "lemmy" {
			// The image store reads the sign-in from the cookie.
			request.AddCookie(&http.Cookie{Name: "jwt", Value: token})
		}
	case "bluesky":
		return bluesky_authorize(l, request, body)
	}
	return nil
}

// expired reports whether an answer says the credential has run out, so that
// renew should be tried.
func (l *linked) expired(status int, body []byte) bool {
	switch l.kind {
	case "matrix":
		return status == http.StatusUnauthorized && matrix_error_code(body) == "M_UNKNOWN_TOKEN"
	case "bluesky":
		return bluesky_expired(status, body)
	}
	return false
}

// renew gets a fresh credential where the protocol has a way: a Matrix
// refresh token, a Bluesky refresh, or the app password again. It answers
// linked_signed_out when nothing works, which marks the account for the user
// to link again.
func (l *linked) renew(ctx context.Context) error {
	switch l.kind {
	case "matrix":
		return matrix_renew(ctx, l)
	case "bluesky":
		return bluesky_renew(ctx, l)
	}
	return linked_signed_out
}

// failure turns a non-2xx answer into an error naming the status and what the
// server said, within reason.
func linked_failure(status int, body []byte) error {
	message := strings.TrimSpace(string(body))
	if len(message) > 200 {
		message = message[:200]
	}
	var answer struct {
		Error       string `json:"error"`
		Description string `json:"error_description"`
		Message     string `json:"message"`
	}
	if json.Unmarshal(body, &answer) == nil {
		switch {
		case answer.Description != "":
			message = answer.Description
		case answer.Message != "":
			message = answer.Message
		case answer.Error != "":
			message = answer.Error
		}
	}
	return fmt.Errorf("the server answered %d: %s", status, message)
}

// json_decode decodes a server's JSON answer, naming the fault when it is not
// JSON.
func json_decode(data []byte, into any) error {
	if err := json.Unmarshal(data, into); err != nil {
		return fmt.Errorf("the server's answer is not JSON: %v", err)
	}
	return nil
}

// read_limited reads a body up to a cap, refusing one over it.
func read_limited(reader io.Reader, maximum int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maximum {
		return nil, fmt.Errorf("the answer is over %d bytes", maximum)
	}
	return data, nil
}

// linked_decode decodes a server's answer for an app: JSON as it is, anything
// else as text.
func linked_decode(body []byte) any {
	var value any
	if len(body) > 0 && json.Unmarshal(body, &value) == nil {
		return value
	}
	return string(body)
}

// === Idempotency ===
//
// A post is made once however many times an app asks with the same key: the
// network's answer is kept for a day against the key, and a repeat is answered
// from it. Mastodon keeps the same promise itself through its Idempotency-Key
// header, which the key is also sent as.

// remembered answers what a key's post was answered with, if it was made.
func (l *linked) remembered(key string) map[string]any {
	return request_remembered(l.user, l.id, key)
}

// remember keeps a key's answer, and drops the day-old ones.
func (l *linked) remember(key string, result map[string]any) {
	request_remember(l.user, l.id, key, result)
}

// === The functions ===

// linked_create publishes an object through a linked social account, once per
// key, and answers the remote id, the page and the network's own answer.
func linked_create(ctx context.Context, l *linked, object map[string]any, key string) (map[string]any, error) {
	if result := l.remembered(key); result != nil {
		return result, nil
	}
	var result map[string]any
	var err error
	switch l.kind {
	case "mastodon":
		result, err = mastodon_create(ctx, l, object, key)
	case "lemmy":
		result, err = lemmy_create(ctx, l, object)
	case "bluesky":
		result, err = bluesky_create(ctx, l, object)
	default:
		return nil, fmt.Errorf("a %s account cannot publish; its functions come with chat", l.kind)
	}
	if err != nil {
		return nil, err
	}
	l.remember(key, result)
	return result, nil
}

// linked_update publishes a new version of an object the account published.
func linked_update(ctx context.Context, l *linked, id string, object map[string]any) (map[string]any, error) {
	switch l.kind {
	case "mastodon":
		return mastodon_update(ctx, l, id, object)
	case "lemmy":
		return lemmy_update(ctx, l, id, object)
	case "bluesky":
		return bluesky_update(ctx, l, id, object)
	}
	return nil, fmt.Errorf("a %s account cannot publish; its functions come with chat", l.kind)
}

// linked_delete deletes an object the account published.
func linked_delete(ctx context.Context, l *linked, id string) error {
	switch l.kind {
	case "mastodon":
		return mastodon_delete(ctx, l, id)
	case "lemmy":
		return lemmy_delete(ctx, l, id)
	case "bluesky":
		return bluesky_delete(ctx, l, id)
	}
	return fmt.Errorf("a %s account cannot publish; its functions come with chat", l.kind)
}

// linked_timeline reads a page of the account's home timeline.
func linked_timeline(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	switch l.kind {
	case "mastodon":
		return mastodon_timeline(ctx, l, cursor, limit)
	case "lemmy":
		return lemmy_timeline(ctx, l, cursor, limit)
	case "bluesky":
		return bluesky_timeline(ctx, l, cursor, limit)
	}
	return nil, fmt.Errorf("a %s account has no timeline", l.kind)
}

// linked_posts reads a page of the account's own posts.
func linked_posts(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	switch l.kind {
	case "mastodon":
		return mastodon_posts(ctx, l, cursor, limit)
	case "lemmy":
		return lemmy_posts(ctx, l, cursor, limit)
	case "bluesky":
		return bluesky_posts(ctx, l, cursor, limit)
	}
	return nil, fmt.Errorf("a %s account has no posts", l.kind)
}

// linked_notifications reads a page of the account's notifications.
func linked_notifications(ctx context.Context, l *linked, cursor string, limit int) (*linked_page, error) {
	switch l.kind {
	case "mastodon":
		return mastodon_notifications(ctx, l, cursor, limit)
	case "lemmy":
		return lemmy_notifications(ctx, l, cursor, limit)
	case "bluesky":
		return bluesky_notifications(ctx, l, cursor, limit)
	}
	return nil, fmt.Errorf("a %s account has no notifications here", l.kind)
}

// linked_upload uploads a file for a post and answers the reference the post
// names it by.
func linked_upload(ctx context.Context, l *linked, content []byte, kind string, name string, description string) (map[string]any, error) {
	switch l.kind {
	case "mastodon":
		return mastodon_upload(ctx, l, content, kind, name, description)
	case "lemmy":
		return lemmy_upload(ctx, l, content, kind, name)
	case "bluesky":
		return bluesky_upload(ctx, l, content, kind)
	}
	return nil, fmt.Errorf("a %s account cannot upload here", l.kind)
}

// linked_request sends a request of the app's own to the account's server, on
// the protocol's interface, and answers the status and the decoded body.
func linked_request(ctx context.Context, l *linked, method string, path string, query map[string]string, body any) (map[string]any, error) {
	var status int
	var data []byte
	var err error
	link := ""
	switch l.kind {
	case "mastodon":
		// Mastodon pages a list by its Link header alone, so the header comes
		// back with the answer.
		var encoded []byte
		headers := map[string]string{}
		if body != nil {
			encoded = []byte(json_encode(body))
			headers["Content-Type"] = "application/json"
		}
		var header http.Header
		status, data, header, err = l.request(ctx, method, mastodon_api(l, path, query), headers, encoded)
		if err == nil {
			link = header.Get("Link")
		}
	case "lemmy":
		status, data, err = lemmy_request(ctx, l, method, path, query, body)
	case "bluesky":
		status, data, err = bluesky_request(ctx, l, method, path, query, body)
	case "matrix":
		status, data, err = matrix_request(ctx, l, method, path, query, body)
	default:
		return nil, fmt.Errorf("a %s account takes no requests", l.kind)
	}
	if err != nil {
		return nil, err
	}
	result := map[string]any{"status": status, "data": linked_decode(data)}
	if link != "" {
		result["link"] = link
	}
	return result, nil
}

// linked_verify checks that the server still accepts the account.
func linked_verify(ctx context.Context, l *linked) error {
	switch l.kind {
	case "mastodon":
		_, err := mastodon_profile(ctx, l)
		return err
	case "lemmy":
		_, err := lemmy_profile(ctx, l)
		return err
	case "bluesky":
		return bluesky_verify(ctx, l)
	case "matrix":
		return matrix_verify(ctx, l)
	case "xmpp":
		return xmpp_verify(ctx, l)
	}
	return fmt.Errorf("unknown kind")
}

// linked_media fetches a file as the account: with its credential where the
// network needs one for media, as Matrix does, and plainly elsewhere.
func linked_media(ctx context.Context, l *linked, address string) (int, string, []byte, error) {
	headers := map[string]string{"Accept": "*/*"}
	var status int
	var data []byte
	var header http.Header
	var err error
	if l.kind == "matrix" {
		status, data, header, err = l.request(ctx, "GET", address, headers, nil)
	} else {
		request, e := http.NewRequestWithContext(ctx, "GET", address, nil)
		if e != nil {
			return 0, "", nil, e
		}
		request.Header.Set("Accept", "*/*")
		response, e := network_send(ctx, l.user, "", strings.ToLower(request.URL.Hostname())+"|"+l.id, request, 5)
		if e != nil {
			return 0, "", nil, e
		}
		defer response.Body.Close()
		data, err = io.ReadAll(io.LimitReader(response.Body, int64(media_maximum)+1))
		status, header = response.StatusCode, response.Header
	}
	if err != nil {
		return 0, "", nil, err
	}
	if len(data) > media_maximum {
		return 0, "", nil, fmt.Errorf("over %d bytes", media_maximum)
	}
	return status, header.Get("Content-Type"), data, nil
}

// linked_page_arguments reads the cursor and limit of a page function.
func linked_page_arguments(fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (string, string, int, string, error) {
	var account, cursor, signer string
	var limit int
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "cursor?", &cursor, "limit?", &limit, "signer?", &signer); err != nil {
		return "", "", 0, "", err
	}
	if limit <= 0 {
		limit = 40
	}
	return account, cursor, limit, signer, nil
}

// linked_page_encode writes a page for Starlark.
func linked_page_encode(page *linked_page) sl.Value {
	items := page.Items
	if items == nil {
		items = []any{}
	}
	return sl_encode(map[string]any{"items": items, "cursor": page.Cursor})
}

// hosted_bluesky reads a hosted Bluesky account from an acting row, or nil:
// its timeline, notifications and the index's methods go through Bluesky's
// index as the account (appview.go).
func hosted_bluesky(row map[string]any) map[string]any {
	if row != nil && account_hosted_flag(row) && row["type"] == "bluesky" && atproto_did(row) != "" {
		return row
	}
	return nil
}

// mochi.account.timeline(account, cursor="", limit=40) -> dict: A page of a social account's home timeline, {items, cursor}: the items in the network's own form (Mastodon statuses, Bluesky feed items, Lemmy post views), newest first, and the cursor for the next page, empty at the end; a hosted Bluesky account's from Bluesky's index. Behind the account permission
func api_account_timeline(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	account, cursor, limit, _, err := linked_page_arguments(fn, args, kwargs)
	if err != nil {
		return nil, err
	}
	user, row, _, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if hosted := hosted_bluesky(row); hosted != nil {
		page, err := appview_page(thread_context(t), user, hosted, "app.bsky.feed.getTimeline", map[string]string{}, "feed", cursor, limit)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
		return linked_page_encode(page), nil
	}
	l := linked_from(user, row)
	if l == nil {
		return sl_error(fn, "a hosted account has no such function yet")
	}
	page, err := linked_timeline(thread_context(t), l, cursor, limit)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return linked_page_encode(page), nil
}

// mochi.account.posts(account, cursor="", limit=40, signer="") -> dict: A page of a linked social account's own posts, {items, cursor}, in the network's own form, newest first. Behind the account permission. A Bluesky identifier in place of the account reads that account's public posts from its own server, which needs no sign-in, in the same form; a fediverse id or handle reads the person's outbox, the posts as their server gives them and each boost as {type: Announce, object}, the cursor the next page's address, with signer naming a hosted account of the caller's to sign the reading as, for a server that gives its documents to signed requests only; None when the server does not answer
func api_account_posts(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	account, cursor, limit, signer, err := linked_page_arguments(fn, args, kwargs)
	if err != nil {
		return nil, err
	}
	if author_network(account) == "fediverse" || resolve_acct_re.MatchString(account) && !strings.HasPrefix(account, "xmpp:") {
		user := principal_caller(t)
		if user == nil {
			return sl_error(fn, "no user")
		}
		if err := resolve_allowed(user, account); err != nil {
			return sl_error(fn, "%v", err)
		}
		ctx := thread_context(t)
		if signer != "" {
			signing, _, _, err := account_acting(t, fn, signer)
			if err != nil {
				return sl_error(fn, "%v", err)
			}
			ctx = activitypub_signing(ctx, signing, signer)
		}
		page, err := fediverse_public_posts(ctx, account, cursor)
		if err != nil {
			debug("Outbox of %s not read: %v", account, err)
			return sl.None, nil
		}
		return linked_page_encode(page), nil
	}
	if author_network(account) == "bluesky" {
		user := principal_caller(t)
		if user == nil {
			return sl_error(fn, "no user")
		}
		if err := resolve_allowed(user, account); err != nil {
			return sl_error(fn, "%v", err)
		}
		// A server that does not answer is None, as a lookup of the person is:
		// a scheduled poll of a public account must not die with it.
		page, err := bluesky_public_posts(thread_context(t), account, cursor, limit)
		if err != nil {
			return sl.None, nil
		}
		return linked_page_encode(page), nil
	}
	user, row, _, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	// A hosted Bluesky account's posts are its own repository's.
	if hosted := hosted_bluesky(row); hosted != nil {
		return linked_page_encode(atproto_posts(user, hosted, cursor, limit)), nil
	}
	l := linked_from(user, row)
	if l == nil {
		return sl_error(fn, "a hosted account has no such function yet")
	}
	page, err := linked_posts(thread_context(t), l, cursor, limit)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return linked_page_encode(page), nil
}

// mochi.account.notifications(account, cursor="", limit=40) -> dict: A page of a social account's notifications, {items, cursor}, in the network's own form, newest first; on Lemmy each item carries kind, "reply" or "mention"; a hosted Bluesky account's from Bluesky's index. Behind the account permission
func api_account_notifications(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	account, cursor, limit, _, err := linked_page_arguments(fn, args, kwargs)
	if err != nil {
		return nil, err
	}
	user, row, _, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if hosted := hosted_bluesky(row); hosted != nil {
		page, err := appview_page(thread_context(t), user, hosted, "app.bsky.notification.listNotifications", map[string]string{}, "notifications", cursor, limit)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
		return linked_page_encode(page), nil
	}
	l := linked_from(user, row)
	if l == nil {
		return sl_error(fn, "a hosted account has no such function yet")
	}
	page, err := linked_notifications(thread_context(t), l, cursor, limit)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return linked_page_encode(page), nil
}

// mochi.account.upload(account, data, type, name="", description="") -> dict: Upload a file through a linked social account for a post to carry, and return the reference the post names it by: {id, url, data} with the network's own answer in data - a Mastodon media attachment, a Bluesky blob, a Lemmy image. description is the alternative text where the network takes one at upload. Behind the account permission
func api_account_upload(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, kind, name, description string
	var content sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "data", &content, "type", &kind, "name?", &name, "description?", &description); err != nil {
		return nil, err
	}
	var data []byte
	switch value := content.(type) {
	case sl.Bytes:
		data = []byte(value)
	case sl.String:
		data = []byte(value)
	default:
		return sl_error(fn, "data must be bytes")
	}
	if len(data) == 0 || len(data) > media_maximum {
		return sl_error(fn, "data must be between 1 and %d bytes", media_maximum)
	}
	user, row, _, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	// A hosted Bluesky account's blobs live in its own repository.
	if account_hosted_flag(row) && row["type"] == "bluesky" {
		result, err := atproto_upload(user, row, data, kind)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl_encode(result), nil
	}
	l := linked_from(user, row)
	if l == nil {
		return sl_error(fn, "a hosted account has no such function yet")
	}
	if name == "" {
		name = "file"
	}
	result, err := linked_upload(thread_context(t), l, data, kind, name, description)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(result), nil
}

// mochi.account.request(account, method, path, body=None, query=None) -> dict: A request of the app's own through an account, on its server's interface: a path under /api/ on Mastodon ("v1/bookmarks"), under /api/v3/ on Lemmy ("community/list"), under /_matrix/ on Matrix ("client/v3/joined_rooms"), or an XRPC method name on Bluesky ("app.bsky.graph.getFollows"); for a hosted Bluesky account the index's methods (app.bsky.*, chat.bsky.*) are forwarded to Bluesky's index as the account. query is a dict of query parameters, body a dict sent as JSON. Returns {status, data} with the answer decoded. Behind the account permission
func api_account_request(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var account, method, path string
	var body, query sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "account", &account, "method", &method, "path", &path, "body?", &body, "query?", &query); err != nil {
		return nil, err
	}
	method = strings.ToUpper(method)
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE":
	default:
		return sl_error(fn, "invalid method")
	}
	path = strings.TrimLeft(path, "/")
	if path == "" || strings.Contains(path, "..") || strings.Contains(path, "?") {
		return sl_error(fn, "invalid path")
	}
	user, row, _, err := account_acting(t, fn, account)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	var sent any
	if body != nil && body != sl.None {
		sent = sl_decode(body)
	}
	parameters := map[string]string{}
	if query != nil && query != sl.None {
		parameters = sl_decode_strings(query)
	}
	if hosted := hosted_bluesky(row); hosted != nil {
		result, err := appview_forward(thread_context(t), user, hosted, method, path, parameters, sent)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
		return sl_encode(result), nil
	}
	l := linked_from(user, row)
	if l == nil {
		return sl_error(fn, "a hosted account has no such function yet")
	}
	result, err := linked_request(thread_context(t), l, method, path, parameters, sent)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	return sl_encode(result), nil
}

// === The chat manager ===
//
// A linked XMPP account has a connection to its server for as long as it is
// linked and enabled, and a linked Matrix account a sync loop; what arrives is
// handed to linked_inbound. The manager starts one session per account at
// startup and keeps it, reconnecting on failure at a growing interval, until
// the account is unlinked or disabled, or the server stops accepting its
// credential, which waits for the user to link it again.

// A linked_session is one account's running session.
type linked_session struct {
	cancel    context.CancelFunc
	connected atomic.Bool
	reason    atomic.Value // string: why it is not connected
}

var linked_sessions = map[string]*linked_session{}
var linked_lock sync.Mutex

// linked_inbound receives what a chat account's session hears: a message, a
// presence, a Matrix sync's rooms. The chat app's functions take it from here
// when chat is built on the networks; until then it is dropped. A variable so
// tests can watch it.
var linked_inbound = func(user *User, account string, kind string, event map[string]any) {
	debug("Linked %s account %s: %s dropped, no app takes it yet", kind, account, event["kind"])
}

// linked_session_key names a session.
func linked_session_key(uid string, id string) string {
	return uid + "/" + id
}

// linked_manager starts a session for every linked chat account at startup,
// and once an hour reads again the limits of each linked Mastodon account's
// server that are a day old (decision 24). With the development switch off
// nothing runs.
func linked_manager() {
	if !networks_development {
		return
	}
	linked_resume()
	for range time.Tick(time.Hour) {
		linked_each(func(user *User, l *linked) {
			if l.kind == "mastodon" {
				ctx, cancel := context.WithTimeout(context.Background(), linked_timeout)
				mastodon_refresh_limits(ctx, l)
				cancel()
			}
		})
	}
}

// linked_resume starts a session for every enabled linked chat account of
// every active user.
func linked_resume() {
	linked_each(func(user *User, l *linked) {
		if linked_chat(l.kind) {
			linked_start(l)
		}
	})
}

// linked_each calls fn for every enabled linked account of every active user.
func linked_each(fn func(user *User, l *linked)) {
	rows, _ := db_open("db/users.db").rows("select uid from users where status='active'")
	for _, row := range rows {
		user := user_by_uid(row_string(row, "uid"))
		if user == nil {
			continue
		}
		for _, account := range accounts_network(user) {
			if enabled, _ := account["enabled"].(int64); enabled == 0 {
				continue
			}
			if l := linked_from(user, account); l != nil {
				fn(user, l)
			}
		}
	}
}

// linked_start begins an account's session, unless one is running.
func linked_start(l *linked) {
	if !networks_development || !linked_chat(l.kind) {
		return
	}
	key := linked_session_key(l.user.UID, l.id)
	linked_lock.Lock()
	defer linked_lock.Unlock()
	if _, running := linked_sessions[key]; running {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &linked_session{cancel: cancel}
	linked_sessions[key] = s
	go linked_run(ctx, l, s)
}

// linked_stop ends an account's session.
func linked_stop(uid string, id string) {
	key := linked_session_key(uid, id)
	linked_lock.Lock()
	s := linked_sessions[key]
	delete(linked_sessions, key)
	linked_lock.Unlock()
	if s != nil {
		s.cancel()
	}
}

// linked_refresh starts or stops an account's session after its row changed:
// an enabled chat account has one, any other has none.
func linked_refresh(user *User, id string) {
	row, _ := db_user(user, "user").row("select id, type, label, identifier, data, enabled from accounts where id=?", id)
	l := linked_from(user, row)
	enabled, _ := row["enabled"].(int64)
	if l == nil || !linked_chat(l.kind) || enabled == 0 {
		linked_stop(user.UID, id)
		return
	}
	linked_start(l)
}

// linked_connected reports whether an account's session is up, and why not.
func linked_connected(uid string, id string) (bool, string) {
	linked_lock.Lock()
	s := linked_sessions[linked_session_key(uid, id)]
	linked_lock.Unlock()
	if s == nil {
		return false, ""
	}
	reason, _ := s.reason.Load().(string)
	return s.connected.Load(), reason
}

// linked_run keeps one account's session up: it runs the protocol's session,
// which returns when the connection ends, and runs it again after a wait that
// doubles from a minute to an hour while the failures continue, reset by a
// session that lasted. A refused credential ends the loop.
func linked_run(ctx context.Context, l *linked, s *linked_session) {
	wait := time.Minute
	for {
		fresh := linked_get(l.user, l.id)
		if fresh == nil {
			linked_stop(l.user.UID, l.id)
			return
		}
		started := time.Now()
		var err error
		switch fresh.kind {
		case "xmpp":
			err = xmpp_run(ctx, fresh, s)
		case "matrix":
			err = matrix_run(ctx, fresh, s)
		}
		s.connected.Store(false)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.reason.Store(err.Error())
			debug("Linked %s account %s: %v", fresh.kind, fresh.id, err)
		}
		if errors.Is(err, linked_signed_out) {
			fresh.set("error", err.Error())
			fresh.save()
			linked_stop(l.user.UID, l.id)
			return
		}
		if time.Since(started) > 5*time.Minute {
			wait = time.Minute
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait + time.Duration(rand.Int63n(int64(wait/4)))):
		}
		if wait < time.Hour {
			wait *= 2
		}
	}
}
