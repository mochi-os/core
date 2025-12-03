// Mochi server: Users
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Invite stores an invitation code
type Invite struct {
	Code    string
	Uses    int
	Expires int
}

type Code struct {
	Code     string
	Username string
	Expires  int
}

type Login struct {
	User    int
	Code    string
	Name    string
	Secret  string
	Expires int
}

type User struct {
	ID          int
	Username    string
	Role        string
	Preferences map[string]string
	Identity    *Entity
	db          *DB // Used by actions
}

var api_user = sls.FromStringDict(sl.String("mochi.user"), sl.StringDict{
	"count":  sl.NewBuiltin("mochi.user.count", api_user_count),
	"create": sl.NewBuiltin("mochi.user.create", api_user_create),
	"delete": sl.NewBuiltin("mochi.user.delete", api_user_delete),
	"get": sls.FromStringDict(sl.String("mochi.user.get"), sl.StringDict{
		"fingerprint": sl.NewBuiltin("mochi.user.get.fingerprint", api_user_get_fingerprint),
		"id":          sl.NewBuiltin("mochi.user.get.id", api_user_get_id),
		"identity":    sl.NewBuiltin("mochi.user.get.identity", api_user_get_identity),
		"username":    sl.NewBuiltin("mochi.user.get.username", api_user_get_username),
	}),
	"invite": sls.FromStringDict(sl.String("mochi.user.invite"), sl.StringDict{
		"create":   sl.NewBuiltin("mochi.user.invite.create", api_user_invite_create),
		"delete":   sl.NewBuiltin("mochi.user.invite.delete", api_user_invite_delete),
		"list":     sl.NewBuiltin("mochi.user.invite.list", api_user_invite_list),
		"validate": sl.NewBuiltin("mochi.user.invite.validate", api_user_invite_validate),
	}),
	"list":   sl.NewBuiltin("mochi.user.list", api_user_list),
	"update": sl.NewBuiltin("mochi.user.update", api_user_update),
})

func code_send(email string) bool {
	if !email_valid(email) {
		return false
	}
	code := random_alphanumeric(12)
	debug("code", code)
	db := db_open("db/users.db")
	db.exec("replace into codes ( code, username, expires ) values ( ?, ?, ? )", code, email, now()+3600)
	email_send(email, "Mochi login code", "Please copy and paste the code below into your web browser. This code is valid for one hour.\n\n"+code)
	return true
}

func login_create(user int) string {
	code := random_alphanumeric(20)
	// Create a per-login secret for signing JWTs for this login/device
	secret := random_alphanumeric(32)
	db := db_open("db/users.db")
	db.exec("replace into logins ( user, code, secret, expires ) values ( ?, ?, ?, ? )", user, code, secret, now()+365*86400)
	return code
}

func login_delete(code string) {
	db := db_open("db/users.db")
	db.exec("delete from logins where code=?", code)
}

func user_by_id(id int) *User {
	db := db_open("db/users.db")
	var u User
	if !db.scan(&u, "select id, username, role from users where id=?", id) {
		return nil
	}

	u.Preferences = user_preferences_load(&u)
	u.Identity = u.identity()
	if u.Identity == nil {
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
	if !db.scan(&u, "select id, username, role from users where id=?", i.User) {
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

	var l Login
	db := db_open("db/users.db")
	if !db.scan(&l, "select * from logins where code=? and expires>=?", login, now()) {
		return nil
	}

	var u User
	if !db.scan(&u, "select id, username, role from users where id=?", l.User) {
		return nil
	}

	u.Preferences = user_preferences_load(&u)
	u.Identity = u.identity()
	return &u
}

func user_from_code(code string) *User {
	var c Code
	db := db_open("db/users.db")
	if !db.scan(&c, "delete from codes where code=? and expires>=? returning *", code, now()) {
		return nil
	}

	var u User
	if db.scan(&u, "select id, username, role from users where username=?", c.Username) {
		u.Preferences = user_preferences_load(&u)
		u.Identity = u.identity()
		return &u
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

	if db.scan(&u, "select id, username, role from users where username=?", c.Username) {
		u.Preferences = user_preferences_load(&u)
		return &u
	}

	warn("Unable to create user")
	return nil
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
	if !db.scan(&u, "select id, username, role from users where id=?", i.User) {
		return nil
	}

	u.Preferences = user_preferences_load(&u)
	u.Identity = u.identity()
	if u.Identity == nil {
		return nil
	}

	return &u
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
	if !db.scan(&u, "select id, username, role from users where id=?", id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role}), nil
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
	if !db.scan(&u, "select id, username, role from users where username=?", username) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role}), nil
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
	if !db.scan(&u, "select id, username, role from users where id=?", user_id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role}), nil
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
	if !db.scan(&u, "select id, username, role from users where id=?", user_id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role}), nil
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
	rows, err := db.rows("select id, username, role from users order by id limit ? offset ?", limit, offset)
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
	if !db.scan(&u, "select id, username, role from users where username=?", username) {
		return sl_error(fn, "failed to create user")
	}

	return sl_encode(map[string]any{"id": u.ID, "username": u.Username, "role": u.Role}), nil
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

// mochi.user.invite.create(uses, expires) -> dict: Create an invitation code (admin only)
func api_user_invite_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	uses := 1
	expires_days := 7

	if len(args) > 0 {
		u, err := sl.AsInt32(args[0])
		if err != nil || u < 1 {
			return sl_error(fn, "invalid uses")
		}
		uses = int(u)
	}
	if len(args) > 1 {
		e, err := sl.AsInt32(args[1])
		if err != nil || e < 1 {
			return sl_error(fn, "invalid expires")
		}
		expires_days = int(e)
	}

	code := random_alphanumeric(16)
	expires := now() + int64(expires_days*86400)

	db := db_open("db/users.db")
	db.exec("insert into invites (code, uses, expires) values (?, ?, ?)", code, uses, expires)

	return sl_encode(map[string]any{"code": code, "uses": uses, "expires": expires}), nil
}

// mochi.user.invite.list() -> list: List all invitation codes (admin only)
func api_user_invite_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	db := db_open("db/users.db")
	rows, err := db.rows("select code, uses, expires from invites where expires > ? order by expires", now())
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	return sl_encode(rows), nil
}

// mochi.user.invite.delete(code) -> bool: Delete an invitation code (admin only)
func api_user_invite_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <code: string>")
	}

	code, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid code")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from invites where code=?", code)
	if !exists {
		return sl_error(fn, "invite not found")
	}

	db.exec("delete from invites where code=?", code)
	return sl.True, nil
}

// mochi.user.invite.validate(code) -> bool: Check if an invitation code is valid
func api_user_invite_validate(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <code: string>")
	}

	code, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid code")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from invites where code=? and uses > 0 and expires > ?", code, now())
	return sl.Bool(exists), nil
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
	return []string{"identity", "preference", "role", "username"}
}

func (u *User) Attr(name string) (sl.Value, error) {
	switch name {
	case "identity":
		return u.Identity, nil
	case "preference":
		return &UserPreference{user: u}, nil
	case "role":
		return sl.String(u.Role), nil
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
