// Mochi server: API tokens
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Token represents an API token. An empty Action means the token is valid
// across the whole app; a bound token names one action pattern (":wiki/-/rss")
// and one entity, with an empty Entity for a class-level route.
type Token struct {
	Hash     string   `db:"hash"`
	User     string   `db:"user"`
	App      string   `db:"app"`
	Name     string   `db:"name"`
	Scopes   []string `db:"-"`
	ScopesDB string   `db:"scopes"`
	Action   string   `db:"action"`
	Entity   string   `db:"entity"`
	Created  int64    `db:"created"`
	Used     int64    `db:"used"`
	Expires  int64    `db:"expires"`
}

var api_token = sls.FromStringDict(sl.String("mochi.token"), sl.StringDict{
	"create":   sl.NewBuiltin("mochi.token.create", api_token_create),
	"delete":   sl.NewBuiltin("mochi.token.delete", api_token_delete),
	"list":     sl.NewBuiltin("mochi.token.list", api_token_list),
	"scope":    sl.NewBuiltin("mochi.token.scope", api_token_scope),
	"user":     sl.NewBuiltin("mochi.token.user", api_token_user),
	"validate": sl.NewBuiltin("mochi.token.validate", api_token_validate),
})

// Generate a new token with the format mochi-xxxxxxxxxxxxxxxxxxxx
func token_generate() string {
	bytes := make([]byte, 20)
	_, err := rand.Read(bytes)
	if err != nil {
		return ""
	}
	return "mochi-" + hex.EncodeToString(bytes)
}

// Return the SHA256 hash of a token for storage
func token_hash(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

// Create a new token for a user and return the plaintext token. An empty
// action leaves the token valid across the whole app; naming an action binds
// it to that route and entity alone.
func token_create(user string, app string, name string, scopes []string, expires int64, action string, entity string) string {
	token := token_generate()
	if token == "" {
		return ""
	}

	hash := token_hash(token)
	scopes_json, _ := json.Marshal(scopes)

	created := now()
	db_open("db/users.db").exec("insert into tokens (hash, user, app, name, scopes, action, entity, created, expires) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		hash, user, app, name, string(scopes_json), action, entity, created, expires)
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, 0)", hash, user)

	return token
}

// Delete a token by its hash
func token_delete(hash string) bool {
	db_open("db/users.db").exec("delete from tokens where hash = ?", hash)
	db_open("db/sessions.db").exec("delete from accesses where hash = ?", hash)
	return true
}

// Return all tokens for a user and app (without the actual token values)
func token_list(user string, app string) []map[string]any {
	db := db_open("db/users.db")
	rows, _ := db.rows("select hash, name, scopes, action, entity, created, expires from tokens where user = ? and app = ?", user, app)

	useds := token_useds(user)

	var results []map[string]any
	for _, row := range rows {
		scopes_json := row["scopes"].(string)
		var scopes []string
		json.Unmarshal([]byte(scopes_json), &scopes)

		hash, _ := row["hash"].(string)
		results = append(results, map[string]any{
			"hash":    row["hash"],
			"name":    row["name"],
			"scopes":  scopes,
			"action":  row["action"],
			"entity":  row["entity"],
			"created": row["created"],
			"used":    useds[hash],
			"expires": row["expires"],
		})
	}
	return results
}

// token_useds returns the last-used timestamp by hash for every token belonging
// to a user. Unknown hashes map to 0.
func token_useds(user string) map[string]int64 {
	out := map[string]int64{}
	rows, err := db_open("db/sessions.db").rows("select hash, used from accesses where user=?", user)
	if err != nil {
		return out
	}
	for _, r := range rows {
		hash, _ := r["hash"].(string)
		used, _ := r["used"].(int64)
		out[hash] = used
	}
	return out
}

// token_lookup returns a token's info, or nil if invalid, WITHOUT recording
// use. Introspection (mochi.token.*) uses this so that merely inspecting a
// token does not bump its used timestamp.
func token_lookup(token string) *Token {
	if token == "" || len(token) < 7 || token[:6] != "mochi-" {
		return nil
	}

	hash := token_hash(token)
	db := db_open("db/users.db")

	var t Token
	if !db.scan(&t, "select hash, user, app, name, scopes, action, entity, created, expires from tokens where hash = ?", hash) {
		return nil
	}

	// Check expiration (0 means no expiration)
	if t.Expires > 0 && now() > t.Expires {
		return nil
	}

	// Parse scopes
	json.Unmarshal([]byte(t.ScopesDB), &t.Scopes)

	return &t
}

// token_validate validates a token and records its use (bumps the used
// timestamp). Used by the request-auth paths where the token is exercised.
func token_validate(token string) *Token {
	t := token_lookup(token)
	if t == nil {
		return nil
	}

	// Update used timestamp in sessions.db (cold reference store stays cold).
	// Self-healing: if the accesses row was lost (sessions.db wiped), upsert
	// recreates it so the token keeps tracking.
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, ?) on conflict(hash) do update set used=excluded.used",
		t.Hash, t.User, now())

	return t
}

func token_allows(t *Token, action string, entity string) bool {
	if t == nil {
		return false
	}
	if t.Action == "" {
		return true
	}
	return t.Action == action && t.Entity == entity
}

// token_maximum_lifetime caps an unbound token. Zero means "never expires",
// which is right for a bound token - an RSS feed URL lives in a reader for
// years - but an unbound token authenticates as the user across the whole app
// and survives logout, so it gets an outside limit.
const token_maximum_lifetime = 365 * 86400

// token_expiry_capped clamps an unbound token's expiry to token_maximum_lifetime.
// A bound token is confined to one action on one entity and is returned unchanged.
func token_expiry_capped(expires int64, action string) int64 {
	if action != "" {
		return expires
	}
	limit := now() + token_maximum_lifetime
	if expires <= 0 || expires > limit {
		return limit
	}
	return expires
}

// token_unbound reports whether a token may be used on a core route, which
// has no action pattern to compare a binding against. A bound token is minted
// to be handed out - an RSS feed URL, a git credential - so it must not also
// prove identity.
func token_unbound(t *Token) bool {
	return t != nil && t.Action == ""
}

// Check if a token has a specific scope
func token_has_scope(t *Token, scope string) bool {
	if t == nil {
		return false
	}
	// Empty scopes means all scopes allowed
	if len(t.Scopes) == 0 {
		return true
	}
	for _, s := range t.Scopes {
		if s == scope || s == "*" {
			return true
		}
	}
	return false
}

// mochi.token.create(name, scopes?, expires?, action?, entity?) -> string:
// Create a new token, returns plaintext token. Naming an action binds the token
// to that action and entity alone; omitting it leaves it valid across the app.
func api_token_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "tokens/create"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "not authenticated")
	}

	current_app := principal_app(t)
	if current_app == nil {
		return sl_error(fn, "no app")
	}

	if len(args) < 1 {
		return sl_error(fn, "syntax: <name: string>, [scopes: list], [expires: int], [action: string], [entity: string]")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "name must be a string")
	}

	var scopes []string
	if len(args) > 1 && args[1] != sl.None {
		list, ok := args[1].(*sl.List)
		if !ok {
			return sl_error(fn, "scopes must be a list")
		}
		for i := 0; i < list.Len(); i++ {
			s, _ := sl.AsString(list.Index(i))
			scopes = append(scopes, s)
		}
	}

	var expires int64 = 0
	if len(args) > 2 && args[2] != sl.None {
		exp, ok := args[2].(sl.Int)
		if !ok {
			return sl_error(fn, "expires must be an integer")
		}
		expires, _ = exp.Int64()
	}

	var action string
	if len(args) > 3 && args[3] != sl.None {
		action, ok = sl.AsString(args[3])
		if !ok {
			return sl_error(fn, "action must be a string")
		}
	}

	var entity string
	if len(args) > 4 && args[4] != sl.None {
		entity, ok = sl.AsString(args[4])
		if !ok {
			return sl_error(fn, "entity must be a string")
		}
	}

	// An entity without an action would bind nothing: the action is what the
	// check keys on, so accepting this silently would produce an app-wide
	// token the caller believes is confined to one entity.
	if action == "" && entity != "" {
		return sl_error(fn, "entity requires an action")
	}

	expires = token_expiry_capped(expires, action)

	token := token_create(user.UID, current_app.id, name, scopes, expires, action, entity)
	if token == "" {
		return sl_error(fn, "failed to create token")
	}

	return sl.String(token), nil
}

// mochi.token.delete(token) -> bool: Delete a token by its hash or the token
// itself. An app that only kept the token string (not the hash) can still
// revoke it; a raw token carries the "mochi-" prefix, a hash never does.
func api_token_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "not authenticated")
	}

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <token or hash: string>")
	}

	hash, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "token must be a string")
	}
	// Accept the token itself, not just its hash.
	if len(hash) >= 6 && hash[:6] == "mochi-" {
		hash = token_hash(hash)
	}

	// Verify the token belongs to this user and app
	db := db_open("db/users.db")
	row, _ := db.row("select user, app from tokens where hash = ?", hash)
	if row == nil {
		return sl.False, nil
	}
	if row["user"].(string) != user.UID {
		return sl_error(fn, "token does not belong to user")
	}
	if row["app"].(string) != app.id {
		return sl_error(fn, "token does not belong to app")
	}

	token_delete(hash)
	return sl.True, nil
}

// mochi.token.list() -> list: List all tokens for the current user and app
func api_token_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "not authenticated")
	}

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	tokens := token_list(user.UID, app.id)
	return sl_encode(tokens), nil
}

// mochi.token.scope(token, scope) -> bool: Check if a token has a specific scope
func api_token_scope(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <token: string>, <scope: string>")
	}

	token_str, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "token must be a string")
	}

	scope, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "scope must be a string")
	}

	// Scope to the calling app's own tokens: an app must not be able to
	// introspect tokens minted for a different app. token_lookup (not
	// token_validate) so inspecting a token does not bump its used timestamp.
	app := principal_app(t)
	token := token_lookup(token_str)
	if token == nil || app == nil || token.App != app.id {
		return sl.False, nil
	}

	if token_has_scope(token, scope) {
		return sl.True, nil
	}
	return sl.False, nil
}

// mochi.token.user(token) -> string | None: Get the user UID for a valid token
func api_token_user(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <token: string>")
	}

	token_str, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "token must be a string")
	}

	app := principal_app(t)
	token := token_lookup(token_str)
	if token == nil || app == nil || token.App != app.id {
		return sl.None, nil
	}

	return sl.String(token.User), nil
}

// mochi.token.validate(token) -> dict | None: Validate a token and return its info
func api_token_validate(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <token: string>")
	}

	token_str, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "token must be a string")
	}

	app := principal_app(t)
	token := token_lookup(token_str)
	if token == nil || app == nil || token.App != app.id {
		return sl.None, nil
	}

	return sl_encode(map[string]any{
		"user":    token.User,
		"app":     token.App,
		"name":    token.Name,
		"scopes":  token.Scopes,
		"action":  token.Action,
		"entity":  token.Entity,
		"created": token.Created,
		"used":    token.Used,
		"expires": token.Expires,
	}), nil
}
