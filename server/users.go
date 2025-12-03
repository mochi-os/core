// Mochi server: Users
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
)

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
