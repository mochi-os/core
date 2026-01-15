// Mochi server: API tokens
// Copyright Alistair Cunningham 2025

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
	User     int      `db:"user"`
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
func token_create(user int, app string, name string, scopes []string, expires int64) string {
	token := token_generate()
	if token == "" {
		return ""
	}

	hash := token_hash(token)
	scopes_json, _ := json.Marshal(scopes)

	db := db_open("db/users.db")
	db.exec("insert into tokens (hash, user, app, name, scopes, created, used, expires) values (?, ?, ?, ?, ?, ?, 0, ?)",
		hash, user, app, name, string(scopes_json), now(), expires)

	return token
}

// Delete a token by its hash
func token_delete(hash string) bool {
	db := db_open("db/users.db")
	db.exec("delete from tokens where hash = ?", hash)
	return true
}

// Return all tokens for a user and app (without the actual token values)
func token_list(user int, app string) []map[string]any {
	db := db_open("db/users.db")
	rows, _ := db.rows("select hash, name, scopes, created, used, expires from tokens where user = ? and app = ?", user, app)

	var results []map[string]any
	for _, row := range rows {
		scopes_json := row["scopes"].(string)
		var scopes []string
		json.Unmarshal([]byte(scopes_json), &scopes)

		results = append(results, map[string]any{
			"hash":    row["hash"],
			"name":    row["name"],
			"scopes":  scopes,
			"created": row["created"],
			"used":    row["used"],
			"expires": row["expires"],
		})
	}
	return results
}

// Validate a token and return its info, or nil if invalid
func token_validate(token string) *Token {
	if token == "" || len(token) < 7 || token[:6] != "mochi-" {
		return nil
	}

	hash := token_hash(token)
	db := db_open("db/users.db")

	var t Token
	if !db.scan(&t, "select hash, user, app, name, scopes, created, used, expires from tokens where hash = ?", hash) {
		return nil
	}

	// Check expiration (0 means no expiration)
	if t.Expires > 0 && now() > t.Expires {
		return nil
	}

	// Parse scopes
	json.Unmarshal([]byte(t.ScopesDB), &t.Scopes)

	// Update used timestamp
	db.exec("update tokens set used = ? where hash = ?", now(), hash)

	return &t
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

	token := token_create(user.ID, current_app.id, name, scopes, expires)
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
	if int(row["user"].(int64)) != user.ID {
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

	tokens := token_list(user.ID, app.id)
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

	token := token_validate(token_str)
	if token == nil {
		return sl.False, nil
	}

	if token_has_scope(token, scope) {
		return sl.True, nil
	}
	return sl.False, nil
}

// mochi.token.user(token) -> int | None: Get the user ID for a valid token
func api_token_user(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <token: string>")
	}

	token_str, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "token must be a string")
	}

	token := token_validate(token_str)
	if token == nil {
		return sl.None, nil
	}

	return sl.MakeInt(token.User), nil
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

	token := token_validate(token_str)
	if token == nil {
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
