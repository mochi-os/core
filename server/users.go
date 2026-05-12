// Mochi server: Users
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

type Code struct {
	Code     string
	Username string
	Expires  int
}

type Session struct {
	User     string
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
	UID         string `db:"uid"`
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
	"get": &userGetModule{},
	"identity": sls.FromStringDict(sl.String("mochi.user.identity"), sl.StringDict{
		"update": sl.NewBuiltin("mochi.user.identity.update", api_user_identity_update),
	}),
	"list":     sl.NewBuiltin("mochi.user.list", api_user_list),
	"methods":  api_user_methods,
	"oauth":    api_user_oauth,
	"passkey":  api_user_passkey,
	"recovery": api_user_recovery,
	"search":   sl.NewBuiltin("mochi.user.search", api_user_search),
	"session": sls.FromStringDict(sl.String("mochi.user.session"), sl.StringDict{
		"list":   sl.NewBuiltin("mochi.user.session.list", api_user_session_list),
		"revoke": sl.NewBuiltin("mochi.user.session.revoke", api_user_session_revoke),
	}),
	"suspend": sl.NewBuiltin("mochi.user.suspend", api_user_suspend),
	"totp":    api_user_totp,
	"update":  sl.NewBuiltin("mochi.user.update", api_user_update),
})

// userGetModule is a callable module exposing the alternate-key user lookups.
// Usage: mochi.user.get(id) for the primary-key lookup, or
// mochi.user.get.{username, identity, fingerprint}(value) for alternate keys.
type userGetModule struct{}

func (m *userGetModule) String() string        { return "mochi.user.get" }
func (m *userGetModule) Type() string          { return "module" }
func (m *userGetModule) Freeze()               {}
func (m *userGetModule) Truth() sl.Bool        { return sl.True }
func (m *userGetModule) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }

func (m *userGetModule) AttrNames() []string {
	return []string{"fingerprint", "identity", "username"}
}

func (m *userGetModule) Attr(name string) (sl.Value, error) {
	switch name {
	case "fingerprint":
		return sl.NewBuiltin("mochi.user.get.fingerprint", api_user_get_fingerprint), nil
	case "identity":
		return sl.NewBuiltin("mochi.user.get.identity", api_user_get_identity), nil
	case "username":
		return sl.NewBuiltin("mochi.user.get.username", api_user_get_username), nil
	}
	return nil, nil
}

func (m *userGetModule) Name() string { return "mochi.user.get" }

func (m *userGetModule) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_user_get_id(thread, nil, args, kwargs)
}

// code_send sends a login code to the given email address. Returns empty string
// on success, or an error reason: "invalid_email" or "signup_disabled". The
// gin.Context is used to resolve the email language: the recipient's stored
// preference if they're an existing user, otherwise Accept-Language from the
// request that triggered the send.
func code_send(email string, c *gin.Context) string {
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
	u := user_by_username(email)
	email_login_code(u, email, code, request_language(c, u))
	return ""
}

func login_create(user string, address string, agent string) string {
	code := random_alphanumeric(20)
	// Create a per-login secret for signing JWTs for this login/device
	secret := random_alphanumeric(32)
	expires := now() + 365*86400
	created := now()
	db := db_open("db/sessions.db")
	db.exec("replace into sessions (user, code, secret, expires, created, accessed, address, agent) values (?, ?, ?, ?, ?, ?, ?, ?)", user, code, secret, expires, created, created, address, agent)

	// Replicate the new session to every peer in the user's host set so a
	// cookie issued here is honoured by every replica.
	if user != "" {
		replication_emit_session_insert(user, code, secret, expires, created, created, address, agent)
	}

	return code
}

func login_delete(code string) {
	db := db_open("db/sessions.db")

	// Look up the owning user_uid before deletion so we can fan out a
	// replicated revoke. After the delete the join is impossible.
	var userUID string
	if row, _ := db.row("select user from sessions where code=?", code); row != nil {
		userUID, _ = row["user"].(string)
	}

	db.exec("delete from sessions where code=?", code)

	if userUID != "" {
		replication_emit_session_delete(userUID, code)
	}
}

// sessions_manager periodically cleans up expired sessions and related data
func sessions_manager() {
	for range time.Tick(time.Hour) {
		sessions_cleanup()
	}
}

// sessions_cleanup deletes expired sessions, codes, ceremonies, and partial auth sessions
func sessions_cleanup() {
	db := db_open("db/sessions.db")
	t := now()
	db.exec("delete from sessions where expires < ?", t)
	db.exec("delete from codes where expires < ?", t)
	db.exec("delete from ceremonies where expires < ?", t)
	db.exec("delete from partial where expires < ?", t)
}

func user_by_uid(uid string) *User {
	if uid == "" {
		return nil
	}
	db := db_open("db/users.db")
	var u User
	if !db.scan(&u, "select uid, username, role, methods, status from users where uid=?", uid) {
		return nil
	}

	if u.Status == "suspended" {
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
	if !db.scan(&u, "select uid, username, role, methods, status from users where username=?", username) {
		return nil
	}
	u.Preferences = user_preferences_load(&u)
	return &u
}

func user_by_identity(id string) *User {
	db := db_open("db/users.db")
	var i Entity
	if !db.scan(&i, "select * from entities where id=?", id) {
		return nil
	}

	var u User
	if !db.scan(&u, "select uid, username, role, methods, status from users where uid=?", i.User) {
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
	if !users.scan(&u, "select uid, username, role, methods, status from users where uid=?", s.User) {
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
	if db.scan(&u, "select uid, username, role, methods, status from users where username=?", c.Username) {
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

	return user_create(c.Username)
}

// user_create inserts a new user with the given username (email), applies the
// first-user-becomes-administrator rule, and sends the admin notification. Used
// by both email-code signup and OAuth signup paths. Returns the created user
// and an error reason ("" on success, "invalid" if the row could not be read
// back).
func user_create(username string) (*User, string) {
	db := db_open("db/users.db")

	role := "user"
	has_users, _ := db.exists("select uid from users limit 1")
	if !has_users {
		role = "administrator"
	}

	db.exec("insert into users (uid, username, role) values (?, ?, ?)", uid(), username, role)

	var u User
	if db.scan(&u, "select uid, username, role, methods, status from users where username=?", username) {
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
	if !db.scan(&u, "select uid, username, role, methods, status from users where uid=?", i.User) {
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
	return u.Role == "administrator"
}

// class_app returns the user's preferred app for a class, or empty string if not set
func (u *User) class_app(class string) string {
	db := db_user(u, "user")
	row, _ := db.row("select app from classes where class = ?", class)
	if row == nil {
		return ""
	}
	return row["app"].(string)
}

// set_class_app sets the user's preferred app for a class
func (u *User) set_class_app(class, app string) {
	db := db_user(u, "user")
	if app == "" {
		db.exec("delete from classes where class = ?", class)
	} else {
		db.exec("replace into classes (class, app) values (?, ?)", class, app)
	}
	audit_user_routing_changed(u.Username, "class", class, app)
}

// service_app returns the user's preferred app for a service, or empty string if not set
func (u *User) service_app(service string) string {
	db := db_user(u, "user")
	row, _ := db.row("select app from services where service = ?", service)
	if row == nil {
		return ""
	}
	return row["app"].(string)
}

// set_service_app sets the user's preferred app for a service
func (u *User) set_service_app(service, app string) {
	db := db_user(u, "user")
	if app == "" {
		db.exec("delete from services where service = ?", service)
	} else {
		db.exec("replace into services (service, app) values (?, ?)", service, app)
	}
	audit_user_routing_changed(u.Username, "service", service, app)
}

// path_app returns the user's preferred app for a path, or empty string if not set
func (u *User) path_app(path string) string {
	db := db_user(u, "user")
	row, _ := db.row("select app from paths where path = ?", path)
	if row == nil {
		return ""
	}
	return row["app"].(string)
}

// set_path_app sets the user's preferred app for a path
func (u *User) set_path_app(path, app string) {
	db := db_user(u, "user")
	if app == "" {
		db.exec("delete from paths where path = ?", path)
	} else {
		db.exec("replace into paths (path, app) values (?, ?)", path, app)
	}
	audit_user_routing_changed(u.Username, "path", path, app)
}

// app_version returns the user's preferred version and track for an app
func (u *User) app_version(app string) (version, track string) {
	db := db_user(u, "user")
	row, _ := db.row("select version, track from versions where app = ?", app)
	if row == nil {
		return "", ""
	}
	return row["version"].(string), row["track"].(string)
}

// set_app_version sets the user's preferred version or track for an app
func (u *User) set_app_version(app, version, track string) {
	db := db_user(u, "user")
	if version == "" && track == "" {
		db.exec("delete from versions where app = ?", app)
	} else {
		db.exec("replace into versions (app, version, track) values (?, ?, ?)", app, version, track)
	}
	audit_user_version_changed(u.Username, app, version, track)
}

// mochi.user.get(id) -> dict | None: Get a user by ID (admin only). The bare
// callable form is the primary-key lookup; sub-attributes (.username, .identity,
// .fingerprint) provide alternate-key lookups.
func api_user_get_id(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Check users/read permission
	if err := require_permission(t, fn, "users/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <uid: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid uid")
	}

	db := db_open("db/users.db")
	var u User
	if !db.scan(&u, "select uid, username, role, methods, status from users where uid=?", id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.UID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.get.username(username) -> dict | None: Get a user by username (admin only)
func api_user_get_username(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Check users/read permission
	if err := require_permission(t, fn, "users/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
	if !db.scan(&u, "select uid, username, role, methods, status from users where username=?", username) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.UID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.get.identity(identity) -> dict | None: Get a user by identity entity ID (admin only)
func api_user_get_identity(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Check users/read permission
	if err := require_permission(t, fn, "users/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
	if !db.scan(&u, "select uid, username, role, methods, status from users where uid=?", user_id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.UID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.get.fingerprint(fingerprint) -> dict | None: Get a user by fingerprint (admin only)
func api_user_get_fingerprint(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Check users/read permission
	if err := require_permission(t, fn, "users/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
	if !db.scan(&u, "select uid, username, role, methods, status from users where uid=?", user_id) {
		return sl.None, nil
	}

	return sl_encode(map[string]any{"id": u.UID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
}

// mochi.user.list(limit, offset, sort, order) -> list: List all users (admin only).
// sort is one of "username" (default, case-insensitive), "id", "status", "last".
// order is "asc" (default) or "desc". Username is the secondary sort for stability.
func api_user_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Check users/read permission
	if err := require_permission(t, fn, "users/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	limit := 1000000
	offset := 0
	sort := "username"
	order := "asc"
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
	if len(args) > 2 {
		s, ok := sl.AsString(args[2])
		if !ok || (s != "id" && s != "username" && s != "status" && s != "last") {
			return sl_error(fn, "invalid sort")
		}
		sort = s
	}
	if len(args) > 3 {
		o, ok := sl.AsString(args[3])
		if !ok || (o != "asc" && o != "desc") {
			return sl_error(fn, "invalid order")
		}
		order = o
	}

	// Fetch all users, attach last-login from sessions.db, sort and paginate
	// in memory. ATTACH is blocked at the driver level so we can't join across
	// DBs, and admin user counts are small enough that in-memory is fine.
	db := db_open("db/users.db")
	rows, err := db.rows("select uid, username, role, methods, status from users")
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	users_attach_last(rows)
	users_sort(rows, sort, order)

	if offset >= len(rows) {
		return sl_encode([]map[string]any{}), nil
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	return sl_encode(rows[offset:end]), nil
}

// users_sort orders user rows by the named column. Username comparison is
// case-insensitive; username is also the secondary key so ordering is stable
// when the primary key has duplicates. Sort by "status" matches what the
// admin UI shows in that column (role badge + suspension): active admins,
// active users, suspended admins, suspended users.
func users_sort(rows []map[string]any, sort string, order string) {
	asc := order != "desc"
	username := func(r map[string]any) string {
		s, _ := r["username"].(string)
		return strings.ToLower(s)
	}
	// status_rank composes suspension + role into a single ordering key:
	// 0=active admin, 1=active user, 2=suspended admin, 3=suspended user.
	status_rank := func(r map[string]any) int {
		rank := 0
		if r["status"] == "suspended" {
			rank += 2
		}
		if r["role"] != "administrator" {
			rank += 1
		}
		return rank
	}
	cmp := func(a, b map[string]any) int {
		var c int
		switch sort {
		case "id":
			ai, _ := a["uid"].(string)
			bi, _ := b["uid"].(string)
			c = strings.Compare(ai, bi)
		case "status":
			c = status_rank(a) - status_rank(b)
		case "last":
			al, _ := a["last"].(int64)
			bl, _ := b["last"].(int64)
			if al < bl {
				c = -1
			} else if al > bl {
				c = 1
			}
		}
		if c == 0 {
			c = strings.Compare(username(a), username(b))
		}
		if !asc {
			c = -c
		}
		return c
	}
	slices.SortFunc(rows, cmp)
}

// users_attach_last sets a "last" key on each row to the user's most recent
// login timestamp (0 if they have never logged in). Source is sessions.db's
// logins table — kept separate from users.db so logins don't write to the
// cold reference store.
func users_attach_last(rows []map[string]any) {
	if len(rows) == 0 {
		return
	}

	last := map[string]int64{}
	logins, err := db_open("db/sessions.db").rows("select user, last from logins")
	if err == nil {
		for _, r := range logins {
			id, _ := r["user"].(string)
			t, _ := r["last"].(int64)
			last[id] = t
		}
	}

	for _, r := range rows {
		id, _ := r["uid"].(string)
		r["last"] = last[id]
	}
}

// mochi.user.count() -> int: Count all users (admin only)
func api_user_count(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Check users/read permission
	if err := require_permission(t, fn, "users/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
	// Check users/read permission
	if err := require_permission(t, fn, "users/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
	rows, err := db.rows("select uid, username, role, methods, status from users where username like ? order by username collate nocase limit ?", "%"+query+"%", limit)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}

	users_attach_last(rows)
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

	db.exec("insert into users (uid, username, role) values (?, ?, ?)", uid(), username, role)

	var u User
	if !db.scan(&u, "select uid, username, role, methods, status from users where username=?", username) {
		return sl_error(fn, "failed to create user")
	}

	audit_user_created(user.Username, username, role)
	return sl_encode(map[string]any{"id": u.UID, "username": u.Username, "role": u.Role, "methods": u.Methods, "status": u.Status}), nil
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
		return sl_error(fn, "syntax: <uid: string>, [username: string], [role: string]")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid uid")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where uid=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	if len(args) > 1 && args[1] != sl.None {
		username, ok := sl.AsString(args[1])
		if !ok || !email_valid(username) {
			return sl_error(fn, "invalid username")
		}
		// Get old username for audit
		row, _ := db.row("select username from users where uid=?", id)
		old := ""
		if row != nil {
			old = row["username"].(string)
		}
		db.exec("update users set username=? where uid=?", username, id)
		if old != username {
			audit_email_changed(user.Username, id, old, username)
		}
	}

	if len(args) > 2 && args[2] != sl.None {
		role, ok := sl.AsString(args[2])
		if !ok || (role != "user" && role != "administrator") {
			return sl_error(fn, "invalid role")
		}
		// Get old role for audit
		row, _ := db.row("select role from users where uid=?", id)
		old := ""
		if row != nil {
			old = row["role"].(string)
		}
		db.exec("update users set role=? where uid=?", role, id)
		if old != role && role == "administrator" {
			audit_admin_escalation(user.Username, id, "promote")
		} else if old != role && old == "administrator" {
			audit_admin_escalation(user.Username, id, "demote")
		}
	}

	return sl.True, nil
}

// mochi.user.identity.update(name=..., privacy=...) -> bool: Update the current user's identity entity
func api_user_identity_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/identity/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if user.Identity == nil {
		return sl_error(fn, "no identity")
	}
	if len(args) != 0 {
		return sl_error(fn, "syntax: [name=string], [privacy=string]")
	}

	for _, kv := range kwargs {
		key := string(kv[0].(sl.String))
		switch key {
		case "name":
			name, ok := sl.AsString(kv[1])
			if !ok {
				return sl_error(fn, "invalid name")
			}
			if err := entity_name_set(user.Identity, name); err != nil {
				return sl_error(fn, err.Error())
			}
		case "privacy":
			privacy, ok := sl.AsString(kv[1])
			if !ok {
				return sl_error(fn, "invalid privacy")
			}
			if err := entity_privacy_set(user.Identity, privacy); err != nil {
				return sl_error(fn, err.Error())
			}
		default:
			return sl_error(fn, "unknown parameter %q", key)
		}
	}

	return sl.True, nil
}

// mochi.user.delete(id) -> bool: Delete a user and all associated data (admin only)
func api_user_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <uid: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid uid")
	}

	if id == user.UID {
		return sl_error(fn, "cannot delete self")
	}

	target, err := user_delete(id)
	if err != nil {
		return sl_error(fn, err.Error())
	}

	audit_user_deleted(user.Username, target)
	return sl.True, nil
}

// user_delete removes a user and all associated rows (entities, sessions,
// passkeys, totp, recovery, oauth) plus the on-disk data directory. Returns
// the deleted username for audit purposes.
func user_delete(id string) (string, error) {
	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where uid=?", id)
	if !exists {
		return "", fmt.Errorf("user not found")
	}

	var entities []Entity
	db.scans(&entities, "select * from entities where user=?", id)
	for _, e := range entities {
		e.delete()
	}

	sdb := db_open("db/sessions.db")
	sdb.exec("delete from sessions where user=?", id)
	sdb.exec("delete from ceremonies where user=?", id)
	sdb.exec("delete from partial where user=?", id)
	sdb.exec("delete from logins where user=?", id)
	sdb.exec("delete from accesses where user=?", id)
	sdb.exec("delete from passkeys where user=?", id)
	sdb.exec("delete from verifications where user=?", id)

	db.exec("delete from credentials where user=?", id)
	db.exec("delete from totp where user=?", id)
	db.exec("delete from recovery where user=?", id)
	db.exec("delete from oauth where user=?", id)

	var target User
	db.scan(&target, "select username from users where uid=?", id)

	db.exec("delete from users where uid=?", id)
	db_purge_prefix(fmt.Sprintf("users/%s", id))
	os.RemoveAll(fmt.Sprintf("%s/users/%s", data_dir, id))

	return target.Username, nil
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
		return sl_error(fn, "syntax: <uid: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid uid")
	}

	if id == user.UID {
		return sl_error(fn, "cannot suspend self")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where uid=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	db.exec("update users set status='suspended' where uid=?", id)
	return sl.True, nil
}

// mochi.user.activate(uid) -> bool: Activate a suspended user (admin only)
func api_user_activate(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	if len(args) != 1 {
		return sl_error(fn, "syntax: <uid: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid uid")
	}

	db := db_open("db/users.db")
	exists, _ := db.exists("select 1 from users where uid=?", id)
	if !exists {
		return sl_error(fn, "user not found")
	}

	db.exec("update users set status='active' where uid=?", id)
	return sl.True, nil
}

// mochi.user.session.list(user?) -> list: List active sessions for current user or specified user (admin)
func api_user_session_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/sessions/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	target := user.UID
	if len(args) == 1 {
		id, ok := sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid user uid")
		}
		target = id
		if target != user.UID && !user.administrator() {
			return sl_error(fn, "access denied")
		}
	}

	db := db_open("db/sessions.db")
	rows, err := db.rows("select code, expires, created, accessed, address, agent from sessions where user=? and expires>=? order by accessed desc", target, now())
	if err != nil {
		return sl_error(fn, "database error")
	}

	// Replace raw session codes with hashed identifiers
	for _, row := range rows {
		if code, ok := row["code"].(string); ok {
			hash := sha256.Sum256([]byte(code))
			row["id"] = hex.EncodeToString(hash[:8])
			delete(row, "code")
		}
	}

	return sl_encode(rows), nil
}

// mochi.user.session.revoke(user_id, code?) -> int: Revoke session(s) for a user
// If code is provided, revokes that specific session. If omitted, revokes ALL sessions.
// Returns number of sessions revoked.
func api_user_session_revoke(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "user/sessions/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <user_uid: string>, [code: string]")
	}

	target, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid user uid")
	}

	// Only admins can revoke other users' sessions
	if target != user.UID && !user.administrator() {
		return sl_error(fn, "access denied")
	}

	db := db_open("db/sessions.db")
	var count int

	if len(args) == 2 {
		// Revoke specific session by hashed identifier
		id, ok := sl.AsString(args[1])
		if !ok {
			return sl_error(fn, "invalid session id")
		}
		// Find the session by matching hash
		codes, err := db.rows("select code from sessions where user=?", target)
		if err != nil {
			return sl_error(fn, "database error")
		}
		var found string
		for _, row := range codes {
			if code, ok := row["code"].(string); ok {
				hash := sha256.Sum256([]byte(code))
				if hex.EncodeToString(hash[:8]) == id {
					found = code
					break
				}
			}
		}
		if found == "" {
			return sl_error(fn, "session not found")
		}
		db.exec("delete from sessions where user=? and code=?", target, found)
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
	if db.scan(&i, "select * from entities where user=? and class='person' order by id limit 1", u.UID) {
		return &i
	}
	return nil
}

// Starlark methods
func (u *User) AttrNames() []string {
	return []string{"app", "id", "identity", "methods", "preference", "role", "status", "uid", "username"}
}

func (u *User) Attr(name string) (sl.Value, error) {
	switch name {
	case "app":
		return &UserApp{user: u}, nil
	case "id":
		return sl.String(u.UID), nil
	case "uid":
		return sl.String(u.UID), nil
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
	return sl.String(u.UID).Hash()
}

func (u *User) String() string {
	return fmt.Sprintf("User %s", u.UID)
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

// UserApp provides access to user app bindings
type UserApp struct {
	user *User
}

func (p *UserApp) AttrNames() []string {
	return []string{"class", "path", "service", "version"}
}

func (p *UserApp) Attr(name string) (sl.Value, error) {
	switch name {
	case "class":
		return &UserAppClass{user: p.user}, nil
	case "path":
		return &UserAppPath{user: p.user}, nil
	case "service":
		return &UserAppService{user: p.user}, nil
	case "version":
		return &UserAppVersion{user: p.user}, nil
	default:
		return nil, nil
	}
}

func (p *UserApp) Freeze()               {}
func (p *UserApp) Hash() (uint32, error) { return 0, nil }
func (p *UserApp) String() string        { return "UserApp" }
func (p *UserApp) Truth() sl.Bool        { return sl.True }
func (p *UserApp) Type() string          { return "UserApp" }

// UserAppClass provides user class bindings
type UserAppClass struct {
	user *User
}

func (p *UserAppClass) AttrNames() []string {
	return []string{"get", "set", "delete", "list"}
}

func (p *UserAppClass) Attr(name string) (sl.Value, error) {
	switch name {
	case "get":
		return sl.NewBuiltin("a.user.app.class.get", p.get), nil
	case "set":
		return sl.NewBuiltin("a.user.app.class.set", p.set), nil
	case "delete":
		return sl.NewBuiltin("a.user.app.class.delete", p.delete), nil
	case "list":
		return sl.NewBuiltin("a.user.app.class.list", p.list), nil
	default:
		return nil, nil
	}
}

func (p *UserAppClass) Freeze()               {}
func (p *UserAppClass) Hash() (uint32, error) { return 0, nil }
func (p *UserAppClass) String() string        { return "UserAppClass" }
func (p *UserAppClass) Truth() sl.Bool        { return sl.True }
func (p *UserAppClass) Type() string          { return "UserAppClass" }

// a.user.app.class.get(class) -> string | None: Get user class binding
func (p *UserAppClass) get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var class string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "class", &class); err != nil {
		return sl_error(fn, "%v", err)
	}
	app := p.user.class_app(class)
	if app == "" {
		return sl.None, nil
	}
	return sl.String(app), nil
}

// a.user.app.class.set(class, app) -> None: Set user class binding
func (p *UserAppClass) set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var class, app string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "class", &class, "app", &app); err != nil {
		return sl_error(fn, "%v", err)
	}
	p.user.set_class_app(class, app)
	return sl.None, nil
}

// a.user.app.class.delete(class) -> None: Delete user class binding
func (p *UserAppClass) delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var class string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "class", &class); err != nil {
		return sl_error(fn, "%v", err)
	}
	p.user.set_class_app(class, "")
	return sl.None, nil
}

// a.user.app.class.list() -> dict: List all user class bindings
func (p *UserAppClass) list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	db := db_user(p.user, "user")
	rows, _ := db.rows("select class, app from classes")
	result := sl.NewDict(len(rows))
	for _, row := range rows {
		result.SetKey(sl.String(row["class"].(string)), sl.String(row["app"].(string)))
	}
	return result, nil
}

// UserAppService provides user service bindings
type UserAppService struct {
	user *User
}

func (p *UserAppService) AttrNames() []string {
	return []string{"get", "set", "delete", "list"}
}

func (p *UserAppService) Attr(name string) (sl.Value, error) {
	switch name {
	case "get":
		return sl.NewBuiltin("a.user.app.service.get", p.get), nil
	case "set":
		return sl.NewBuiltin("a.user.app.service.set", p.set), nil
	case "delete":
		return sl.NewBuiltin("a.user.app.service.delete", p.delete), nil
	case "list":
		return sl.NewBuiltin("a.user.app.service.list", p.list), nil
	default:
		return nil, nil
	}
}

func (p *UserAppService) Freeze()               {}
func (p *UserAppService) Hash() (uint32, error) { return 0, nil }
func (p *UserAppService) String() string        { return "UserAppService" }
func (p *UserAppService) Truth() sl.Bool        { return sl.True }
func (p *UserAppService) Type() string          { return "UserAppService" }

// a.user.app.service.get(service) -> string | None: Get user service binding
func (p *UserAppService) get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var service string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "service", &service); err != nil {
		return sl_error(fn, "%v", err)
	}
	app := p.user.service_app(service)
	if app == "" {
		return sl.None, nil
	}
	return sl.String(app), nil
}

// a.user.app.service.set(service, app) -> None: Set user service binding
func (p *UserAppService) set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var service, app string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "service", &service, "app", &app); err != nil {
		return sl_error(fn, "%v", err)
	}
	p.user.set_service_app(service, app)
	return sl.None, nil
}

// a.user.app.service.delete(service) -> None: Delete user service binding
func (p *UserAppService) delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var service string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "service", &service); err != nil {
		return sl_error(fn, "%v", err)
	}
	p.user.set_service_app(service, "")
	return sl.None, nil
}

// a.user.app.service.list() -> dict: List all user service bindings
func (p *UserAppService) list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	db := db_user(p.user, "user")
	rows, _ := db.rows("select service, app from services")
	result := sl.NewDict(len(rows))
	for _, row := range rows {
		result.SetKey(sl.String(row["service"].(string)), sl.String(row["app"].(string)))
	}
	return result, nil
}

// UserAppPath provides user path bindings
type UserAppPath struct {
	user *User
}

func (p *UserAppPath) AttrNames() []string {
	return []string{"get", "set", "delete", "list"}
}

func (p *UserAppPath) Attr(name string) (sl.Value, error) {
	switch name {
	case "get":
		return sl.NewBuiltin("a.user.app.path.get", p.get), nil
	case "set":
		return sl.NewBuiltin("a.user.app.path.set", p.set), nil
	case "delete":
		return sl.NewBuiltin("a.user.app.path.delete", p.delete), nil
	case "list":
		return sl.NewBuiltin("a.user.app.path.list", p.list), nil
	default:
		return nil, nil
	}
}

func (p *UserAppPath) Freeze()               {}
func (p *UserAppPath) Hash() (uint32, error) { return 0, nil }
func (p *UserAppPath) String() string        { return "UserAppPath" }
func (p *UserAppPath) Truth() sl.Bool        { return sl.True }
func (p *UserAppPath) Type() string          { return "UserAppPath" }

// a.user.app.path.get(path) -> string | None: Get user path binding
func (p *UserAppPath) get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path); err != nil {
		return sl_error(fn, "%v", err)
	}
	app := p.user.path_app(path)
	if app == "" {
		return sl.None, nil
	}
	return sl.String(app), nil
}

// a.user.app.path.set(path, app) -> None: Set user path binding
func (p *UserAppPath) set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path, app string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path, "app", &app); err != nil {
		return sl_error(fn, "%v", err)
	}
	p.user.set_path_app(path, app)
	return sl.None, nil
}

// a.user.app.path.delete(path) -> None: Delete user path binding
func (p *UserAppPath) delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path); err != nil {
		return sl_error(fn, "%v", err)
	}
	p.user.set_path_app(path, "")
	return sl.None, nil
}

// a.user.app.path.list() -> dict: List all user path bindings
func (p *UserAppPath) list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	db := db_user(p.user, "user")
	rows, _ := db.rows("select path, app from paths")
	result := sl.NewDict(len(rows))
	for _, row := range rows {
		result.SetKey(sl.String(row["path"].(string)), sl.String(row["app"].(string)))
	}
	return result, nil
}

// UserAppVersion provides user version bindings
type UserAppVersion struct {
	user *User
}

func (p *UserAppVersion) AttrNames() []string {
	return []string{"get", "set", "delete"}
}

func (p *UserAppVersion) Attr(name string) (sl.Value, error) {
	switch name {
	case "get":
		return sl.NewBuiltin("a.user.app.version.get", p.get), nil
	case "set":
		return sl.NewBuiltin("a.user.app.version.set", p.set), nil
	case "delete":
		return sl.NewBuiltin("a.user.app.version.delete", p.delete), nil
	default:
		return nil, nil
	}
}

func (p *UserAppVersion) Freeze()               {}
func (p *UserAppVersion) Hash() (uint32, error) { return 0, nil }
func (p *UserAppVersion) String() string        { return "UserAppVersion" }
func (p *UserAppVersion) Truth() sl.Bool        { return sl.True }
func (p *UserAppVersion) Type() string          { return "UserAppVersion" }

// a.user.app.version.get(app) -> dict | None: Get user version binding
func (p *UserAppVersion) get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var app string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "app", &app); err != nil {
		return sl_error(fn, "%v", err)
	}
	version, track := p.user.app_version(app)
	if version == "" && track == "" {
		return sl.None, nil
	}
	result := sl.NewDict(2)
	result.SetKey(sl.String("version"), sl.String(version))
	result.SetKey(sl.String("track"), sl.String(track))
	return result, nil
}

// a.user.app.version.set(app, version?, track?) -> None: Set user version binding
func (p *UserAppVersion) set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var app, version, track string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "app", &app, "version?", &version, "track?", &track); err != nil {
		return sl_error(fn, "%v", err)
	}
	p.user.set_app_version(app, version, track)
	return sl.None, nil
}

// a.user.app.version.delete(app) -> None: Delete user version binding
func (p *UserAppVersion) delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var app string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "app", &app); err != nil {
		return sl_error(fn, "%v", err)
	}
	p.user.set_app_version(app, "", "")
	return sl.None, nil
}
