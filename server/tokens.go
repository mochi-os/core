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

// Token represents an API token
type Token struct {
	Hash     string   `db:"hash"`
	User     string   `db:"user"`
	App      string   `db:"app"`
	Name     string   `db:"name"`
	Scopes   []string `db:"-"`
	ScopesDB string   `db:"scopes"`
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

// Create a new token for a user and return the plaintext token
func token_create(user string, app string, name string, scopes []string, expires int64) string {
	token := token_generate()
	if token == "" {
		return ""
	}

	hash := token_hash(token)
	scopes_json, _ := json.Marshal(scopes)

	created := now()
	db_open("db/users.db").exec("insert into tokens (hash, user, app, name, scopes, created, expires) values (?, ?, ?, ?, ?, ?, ?)",
		hash, user, app, name, string(scopes_json), created, expires)
	db_open("db/sessions.db").exec("insert into accesses (hash, user, used) values (?, ?, 0)", hash, user)

	return token
}

// Delete a token by its hash
func token_delete(hash string) bool {
	// Find the user before deleting so we can sign the cross-host emit.
	var owner string
	if row, _ := db_open("db/users.db").row("select user from tokens where hash=?", hash); row != nil {
		owner, _ = row["user"].(string)
	}
	db_open("db/users.db").exec("delete from tokens where hash = ?", hash)
	db_open("db/sessions.db").exec("delete from accesses where hash = ?", hash)
	if owner != "" {
	}
	return true
}

// Return all tokens for a user and app (without the actual token values)
func token_list(user string, app string) []map[string]any {
	db := db_open("db/users.db")
	rows, _ := db.rows("select hash, name, scopes, created, expires from tokens where user = ? and app = ?", user, app)

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
	if !db.scan(&t, "select hash, user, app, name, scopes, created, expires from tokens where hash = ?", hash) {
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

// mochi.token.create(name, scopes?, expires?) -> string: Create a new token, returns plaintext token
func api_token_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "not authenticated")
	}

	current_app := t.Local("app").(*App)
	if current_app == nil {
		return sl_error(fn, "no app")
	}

	if len(args) < 1 {
		return sl_error(fn, "syntax: <name: string>, [scopes: list], [expires: int]")
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

	token := token_create(user.UID, current_app.id, name, scopes, expires)
	if token == "" {
		return sl_error(fn, "failed to create token")
	}

	return sl.String(token), nil
}

// mochi.token.delete(hash) -> bool: Delete a token by its hash
func api_token_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "not authenticated")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <hash: string>")
	}

	hash, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "hash must be a string")
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
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "not authenticated")
	}

	app := t.Local("app").(*App)
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
	app, _ := t.Local("app").(*App)
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

	app, _ := t.Local("app").(*App)
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

	app, _ := t.Local("app").(*App)
	token := token_lookup(token_str)
	if token == nil || app == nil || token.App != app.id {
		return sl.None, nil
	}

	return sl_encode(map[string]any{
		"user":    token.User,
		"app":     token.App,
		"name":    token.Name,
		"scopes":  token.Scopes,
		"created": token.Created,
		"used":    token.Used,
		"expires": token.Expires,
	}), nil
}
