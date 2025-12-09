// Mochi server: Users
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

type Code struct {
	Code     string
	Username string
	Expires  int
}

type Session struct {
	User     int
	Code     string
	Name     string
	Secret   string
	Expires  int
	Created  int
	Accessed int
	Address  string
	Agent    string
}

type User struct {
	ID          int
	Username    string
	Role        string
	Methods     string
	Status      string
	Preferences map[string]string
	Identity    *Entity
	db          *DB // Used by actions
}

var api_user = sls.FromStringDict(sl.String("mochi.user"), sl.StringDict{
	"activate": sl.NewBuiltin("mochi.user.activate", api_user_activate),
	"count":    sl.NewBuiltin("mochi.user.count", api_user_count),
	"create":   sl.NewBuiltin("mochi.user.create", api_user_create),
	"delete":   sl.NewBuiltin("mochi.user.delete", api_user_delete),
	"get": sls.FromStringDict(sl.String("mochi.user.get"), sl.StringDict{
		"fingerprint": sl.NewBuiltin("mochi.user.get.fingerprint", api_user_get_fingerprint),
		"id":          sl.NewBuiltin("mochi.user.get.id", api_user_get_id),
		"identity":    sl.NewBuiltin("mochi.user.get.identity", api_user_get_identity),
		"username":    sl.NewBuiltin("mochi.user.get.username", api_user_get_username),
	}),
	"last_login": sl.NewBuiltin("mochi.user.last_login", api_user_last_login),
	"list":       sl.NewBuiltin("mochi.user.list", api_user_list),
	"methods":    api_user_methods,
	"passkey":    api_user_passkey,
	"recovery":   api_user_recovery,
	"search":     sl.NewBuiltin("mochi.user.search", api_user_search),
	"session": sls.FromStringDict(sl.String("mochi.user.session"), sl.StringDict{
		"list":   sl.NewBuiltin("mochi.user.session.list", api_user_session_list),
		"revoke": sl.NewBuiltin("mochi.user.session.revoke", api_user_session_revoke),
	}),
	"suspend": sl.NewBuiltin("mochi.user.suspend", api_user_suspend),
	"totp":    api_user_totp,
	"update":  sl.NewBuiltin("mochi.user.update", api_user_update),
})

// code_send sends a login code to the given email address. Returns empty string
// on success, or an error reason: "invalid_email" or "signup_disabled".
func code_send(email string) string {
	if !email_valid(email) {
		return "invalid_email"
	}

	// Check if user exists; if not, check signup_enabled
	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where username=?", email)
	if !exists && !setting_signup_enabled() {
		return "signup_disabled"
	}

	// Generate 10 character unambiguous mixed-case code
	code := random_unambiguous(10)
	sessions := db_open("db/sessions.db")
	sessions.exec("replace into codes ( code, username, expires ) values ( ?, ?, ? )", code, email, now()+3600)
	email_send(email, "Mochi login code", "Please copy and paste the code below into your web browser. This code is valid for one hour.\n\n"+code)
	return ""
}

func login_create(user int, address string, agent string) string {
	code := random_alphanumeric(20)
	// Create a per-login secret for signing JWTs for this login/device
	secret := random_alphanumeric(32)
	db := db_open("db/sessions.db")
	db.exec("replace into sessions (user, code, secret, expires, created, accessed, address, agent) values (?, ?, ?, ?, ?, ?, ?, ?)", user, code, secret, now()+365*86400, now(), now(), address, agent)
	return code
}

func login_delete(code string) {
	db := db_open("db/sessions.db")
	db.exec("delete from sessions where code=?", code)
}

func user_by_id(id int) *User {
	db := db_open("db/users.db")
	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where id=?", id) {
		return nil
	}

	u.Preferences = user_preferences_load(&u)
	u.Identity = u.identity()
	if u.Identity == nil {
		return nil
	}

	return &u
}

// user_by_username looks up a user by their username (email).
// Returns nil if the user doesn't exist. Note: does not require identity.
func user_by_username(username string) *User {
	db := db_open("db/users.db")
	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where username=?", username) {
		return nil
	}
	return &u
}

func user_by_identity(id string) *User {
	db := db_open("db/users.db")
	var i Entity
	if !db.scan(&i, "select * from entities where id=?", id) {
		return nil
	}

	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where id=?", i.User) {
		return nil
	}

	u.Preferences = user_preferences_load(&u)
	u.Identity = &i
	return &u
}

func user_by_login(login string) *User {
	if login == "" {
		return nil
	}

	var s Session
	sessions := db_open("db/sessions.db")
	if !sessions.scan(&s, "select * from sessions where code=? and expires>=?", login, now()) {
		return nil
	}

	// Update last accessed time
	sessions.exec("update sessions set accessed=? where code=?", now(), login)

	users := db_open("db/users.db")
	var u User
	if !users.scan(&u, "select id, username, role, methods, status from users where id=?", s.User) {
		return nil
	}

	// Block suspended users
	if u.Status == "suspended" {
		return nil
	}

	u.Preferences = user_preferences_load(&u)
	u.Identity = u.identity()
	return &u
}

// user_from_code exchanges a login code for a user. Returns the user and an
// error reason. Error reason is empty on success, "invalid" for bad/expired
// code, "suspended" for suspended users, or "signup_disabled" if the code was
// valid but signups are disabled.
func user_from_code(code string) (*User, string) {
	var c Code
	sessions := db_open("db/sessions.db")
	if !sessions.scan(&c, "delete from codes where code=? and expires>=? returning *", code, now()) {
		return nil, "invalid"
	}

	db := db_open("db/users.db")
	var u User
	if db.scan(&u, "select id, username, role, methods, status from users where username=?", c.Username) {
		if u.Status == "suspended" {
			return nil, "suspended"
		}
		u.Preferences = user_preferences_load(&u)
		u.Identity = u.identity()
		return &u, ""
	}

	// New user - check if signups are enabled
	if !setting_signup_enabled() {
		return nil, "signup_disabled"
	}

	role := "user"
	has_users, _ := db.exists("select id from users limit 1")
	if !has_users {
		role = "administrator"
	}

	db.exec("replace into users (username, role) values (?, ?)", c.Username, role)

	// Remove once we have hooks
	admin := ini_string("email", "admin", "")
	if admin != "" {
		email_send(admin, "Mochi new user", "New user: "+c.Username)
	}

	if db.scan(&u, "select id, username, role, methods, status from users where username=?", c.Username) {
		u.Preferences = user_preferences_load(&u)
		return &u, ""
	}

	warn("Unable to create user")
	return nil, "invalid"
}

func user_owning_entity(id string) *User {
	if id == "" {
		return nil
	}

	db := db_open("db/users.db")
	var i Entity
	if !db.scan(&i, "select * from entities where id=?", id) {
		return nil
	}

	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where id=?", i.User) {
		return nil
	}

	u.Preferences = user_preferences_load(&u)
	u.Identity = u.identity()
	if u.Identity == nil {
		return nil
	}

	return &u
}

// user_owns_entity checks if a user owns the specified entity
func user_owns_entity(u *User, entity_id string) bool {
	if u == nil || entity_id == "" {
		return false
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from entities where id=? and user=?", entity_id, u.ID)
	return exists
}

func (u *User) administrator() bool {
	if u.Role == "administrator" {
		return true
	}
	return false
}

// mochi.user.get.id(id) -> dict | None: Get a user by ID (admin only)
func api_user_get_id(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	db := db_open("db/users.db")
	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where id=?", id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.get.username(username) -> dict | None: Get a user by username (admin only)
func api_user_get_username(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <username: string>")
	}

	username, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid username")
	}

	db := db_open("db/users.db")
	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where username=?", username) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.get.identity(identity) -> dict | None: Get a user by identity entity ID (admin only)
func api_user_get_identity(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <identity: string>")
	}

	identity, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid identity")
	}

	db := db_open("db/users.db")
	row, err := db.row("select user from entities where id=? and class='person'", identity)
	if err != nil || row == nil {
		return sl.None, nil
	}

	user_id := row["user"]
	if user_id == nil {
		return sl.None, nil
	}

	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where id=?", user_id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.get.fingerprint(fingerprint) -> dict | None: Get a user by fingerprint (admin only)
func api_user_get_fingerprint(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <fingerprint: string>")
	}

	fingerprint, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid fingerprint")
	}

	// Remove hyphens if present
	fingerprint = strings.ReplaceAll(fingerprint, "-", "")

	db := db_open("db/users.db")
	row, err := db.row("select user from entities where fingerprint=? and class='person'", fingerprint)
	if err != nil || row == nil {
		return sl.None, nil
	}

	user_id := row["user"]
	if user_id == nil {
		return sl.None, nil
	}

	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where id=?", user_id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.list(limit, offset) -> list: List all users (admin only)
func api_user_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	limit := 1000000
	offset := 0
	if len(args) > 0 {
		l, err := sl.AsInt32(args[0])
		if err != nil || l < 1 || l > 1000000 {
			return sl_error(fn, "invalid limit")
		}
		limit = int(l)
	}
	if len(args) > 1 {
		o, err := sl.AsInt32(args[1])
		if err != nil || o < 0 {
			return sl_error(fn, "invalid offset")
		}
		offset = int(o)
	}

	db := db_open("db/users.db")
	rows, err := db.rows("select id, username, role, methods, status from users order by id limit ? offset ?", limit, offset)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	return sl_encode(rows), nil
}

// mochi.user.count() -> int: Count all users (admin only)
func api_user_count(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	db := db_open("db/users.db")
	row, err := db.row("select count(*) as count from users")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	return sl_encode(row["count"]), nil
}

// mochi.user.search(query, limit) -> list: Search users by username prefix (admin only)
func api_user_search(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <query: string>, [limit: int]")
	}

	query, ok := sl.AsString(args[0])
	if !ok || query == "" {
		return sl_error(fn, "invalid query")
	}

	limit := 10
	if len(args) > 1 {
		l, err := sl.AsInt32(args[1])
		if err != nil || l < 1 || l > 100 {
			return sl_error(fn, "invalid limit (1-100)")
		}
		limit = int(l)
	}

	db := db_open("db/users.db")
	rows, err := db.rows("select id, username, role, methods, status from users where username like ? order by username limit ?", "%"+query+"%", limit)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	return sl_encode(rows), nil
}

// mochi.user.create(username, role) -> dict: Create a new user (admin only)
func api_user_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <username: string>, [role: string]")
	}

	username, ok := sl.AsString(args[0])
	if !ok || !email_valid(username) {
		return sl_error(fn, "invalid username")
	}

	role := "user"
	if len(args) > 1 {
		role, ok = sl.AsString(args[1])
		if !ok || (role != "user" && role != "administrator") {
			return sl_error(fn, "invalid role")
		}
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where username=?", username)
	if exists {
		return sl_error(fn, "user already exists")
	}

	db.exec("insert into users (username, role) values (?, ?)", username, role)

	var u User
	if !db.scan(&u, "select id, username, role, methods, status from users where username=?", username) {
		return sl_error(fn, "failed to create user")
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.update(id, username, role) -> bool: Update a user (admin only)
func api_user_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) < 1 || len(args) > 3 {
		return sl_error(fn, "syntax: <id: int>, [username: string], [role: string]")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where id=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	if len(args) > 1 && args[1] != sl.None {
		username, ok := sl.AsString(args[1])
		if !ok || !email_valid(username) {
			return sl_error(fn, "invalid username")
		}
		db.exec("update users set username=? where id=?", username, id)
	}

	if len(args) > 2 && args[2] != sl.None {
		role, ok := sl.AsString(args[2])
		if !ok || (role != "user" && role != "administrator") {
			return sl_error(fn, "invalid role")
		}
		db.exec("update users set role=? where id=?", role, id)
	}

	return sl.True, nil
}

// mochi.user.delete(id) -> bool: Delete a user (admin only)
func api_user_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	if int(id) == user.ID {
		return sl_error(fn, "cannot delete self")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where id=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	db.exec("delete from users where id=?", id)
	return sl.True, nil
}

// mochi.user.suspend(id) -> bool: Suspend a user (admin only)
func api_user_suspend(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	if int(id) == user.ID {
		return sl_error(fn, "cannot suspend self")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where id=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	db.exec("update users set status='suspended' where id=?", id)
	return sl.True, nil
}

// mochi.user.activate(id) -> bool: Activate a suspended user (admin only)
func api_user_activate(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where id=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	db.exec("update users set status='active' where id=?", id)
	return sl.True, nil
}

// mochi.user.last_login(id) -> int | None: Get last login timestamp for a user (admin only)
func api_user_last_login(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	db := db_open("db/sessions.db")
	row, err := db.row("select max(accessed) as last from sessions where user=?", id)
	if err != nil || row == nil || row["last"] == nil {
		return sl.None, nil
	}

	last, ok := row["last"].(int64)
	if !ok {
		return sl.None, nil
	}
	return sl.MakeInt64(last), nil
}

// mochi.user.session.list(user?) -> tuple: List active sessions for current user or specified user (admin)
func api_user_session_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	target := user.ID
	if len(args) == 1 {
		id, err := sl.AsInt32(args[0])
		if err != nil {
			return sl_error(fn, "invalid user id")
		}
		target = int(id)
		if target != user.ID && !user.administrator() {
			return sl_error(fn, "access denied")
		}
	}

	db := db_open("db/sessions.db")
	rows, err := db.rows("select code, expires, created, accessed, address, agent from sessions where user=? and expires>=? order by accessed desc", target, now())
	if err != nil {
		return sl_error(fn, "database error")
	}

	return sl_encode(rows), nil
}

// mochi.user.session.revoke(user_id, code?) -> int: Revoke session(s) for a user
// If code is provided, revokes that specific session. If omitted, revokes ALL sessions.
// Returns number of sessions revoked.
func api_user_session_revoke(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <user_id: int>, [code: string]")
	}

	target, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid user id")
	}

	// Only admins can revoke other users' sessions
	if int(target) != user.ID && !user.administrator() {
		return sl_error(fn, "access denied")
	}

	db := db_open("db/sessions.db")
	var count int

	if len(args) == 2 {
		// Revoke specific session
		code, ok := sl.AsString(args[1])
		if !ok {
			return sl_error(fn, "invalid code")
		}
		// Verify session belongs to target user
		exists, _ := db.exists("select 1 from sessions where user=? and code=?", target, code)
		if !exists {
			return sl_error(fn, "session not found")
		}
		db.exec("delete from sessions where user=? and code=?", target, code)
		count = 1
	} else {
		// Revoke all sessions for user
		row, _ := db.row("select count(*) as c from sessions where user=?", target)
		if row != nil && row["c"] != nil {
			count = int(row["c"].(int64))
		}
		db.exec("delete from sessions where user=?", target)
	}

	return sl.MakeInt(count), nil
}

func (u *User) identity() *Entity {
	db := db_open("db/users.db")
	var i Entity
	if db.scan(&i, "select * from entities where user=? and class='person' order by id limit 1", u.ID) {
		return &i
	}
	return nil
}

// Starlark methods
func (u *User) AttrNames() []string {
	return []string{"id", "identity", "methods", "preference", "role", "status", "username"}
}

func (u *User) Attr(name string) (sl.Value, error) {
	switch name {
	case "id":
		return sl.MakeInt(u.ID), nil
	case "identity":
		return u.Identity, nil
	case "methods":
		return sl.String(u.Methods), nil
	case "preference":
		return &UserPreference{user: u}, nil
	case "role":
		return sl.String(u.Role), nil
	case "status":
		return sl.String(u.Status), nil
	case "username":
		return sl.String(u.Username), nil
	default:
		return nil, nil
	}
}

func (u *User) Freeze() {}

func (u *User) Hash() (uint32, error) {
	return sl.String(fmt.Sprintf("%d", u.ID)).Hash()
}

func (u *User) String() string {
	return fmt.Sprintf("User %d", u.ID)
}

func (u *User) Truth() sl.Bool {
	return sl.True
}

func (u *User) Type() string {
	return "User"
}

// UserPreference provides Starlark access to user preferences via a.user.preference.*
type UserPreference struct {
	user *User
}

// Starlark interface methods for UserPreference
func (p *UserPreference) AttrNames() []string {
	return []string{"all", "delete", "get", "set"}
}

func (p *UserPreference) Attr(name string) (sl.Value, error) {
	switch name {
	case "all":
		return sl.NewBuiltin("a.user.preference.all", p.all), nil
	case "delete":
		return sl.NewBuiltin("a.user.preference.delete", p.delete), nil
	case "get":
		return sl.NewBuiltin("a.user.preference.get", p.get), nil
	case "set":
		return sl.NewBuiltin("a.user.preference.set", p.set), nil
	default:
		return nil, nil
	}
}

func (p *UserPreference) Freeze()               {}
func (p *UserPreference) Hash() (uint32, error) { return 0, nil }
func (p *UserPreference) String() string        { return "UserPreference" }
func (p *UserPreference) Truth() sl.Bool        { return sl.True }
func (p *UserPreference) Type() string          { return "UserPreference" }

// a.user.preference.get(name) -> string | None: Get a user preference
func (p *UserPreference) get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <name: string>")
	}
	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid name")
	}
	if v, ok := p.user.Preferences[name]; ok {
		return sl.String(v), nil
	}
	return sl.None, nil
}

// a.user.preference.set(name, value) -> string: Set a user preference
func (p *UserPreference) set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <name: string>, <value: string>")
	}
	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid name")
	}
	value, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid value")
	}
	user_preference_set(p.user, name, value)
	return sl.String(value), nil
}

// a.user.preference.delete(name) -> bool: Delete a user preference
func (p *UserPreference) delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <name: string>")
	}
	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid name")
	}
	deleted := user_preference_delete(p.user, name)
	return sl.Bool(deleted), nil
}

// a.user.preference.all() -> dict: Get all user preferences
func (p *UserPreference) all(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl_encode(p.user.Preferences), nil
}
