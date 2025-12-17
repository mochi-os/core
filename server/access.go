// Mochi server: Access control
// Copyright Alistair Cunningham 2025

/* Resolution order, first match wins:
	1. User's ID
	2. User's groups, @groupname, recursively
	3. User's role, #administrator or #user
	4. Authenticated (+)
	5. Anonymous (*), including not logged in
Deny has priority over allow */

package main

import (
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"strings"
)

type Access struct {
	ID        int
	Subject   string
	Resource  string
	Operation string
	Grant     int
	Granter   string
	Created   int64
}

var api_access = sls.FromStringDict(sl.String("mochi.access"), sl.StringDict{
	"allow": sl.NewBuiltin("mochi.access.allow", api_access_allow),
	"clear": sls.FromStringDict(sl.String("mochi.access.clear"), sl.StringDict{
		"resource": sl.NewBuiltin("mochi.access.clear.resource", api_access_clear_resource),
		"subject":  sl.NewBuiltin("mochi.access.clear.subject", api_access_clear_subject),
	}),
	"check": sl.NewBuiltin("mochi.access.check", api_access_check),
	"deny":  sl.NewBuiltin("mochi.access.deny", api_access_deny),
	"list": sls.FromStringDict(sl.String("mochi.access.list"), sl.StringDict{
		"resource": sl.NewBuiltin("mochi.access.list.resource", api_access_list_resource),
		"subject":  sl.NewBuiltin("mochi.access.list.subject", api_access_list_subject),
	}),
	"revoke": sl.NewBuiltin("mochi.access.revoke", api_access_revoke),
})

// Create access control table
func (db *DB) access_setup() {
	db.exec("create table if not exists _access ( id integer primary key autoincrement, subject text not null, resource text not null, operation text not null, grant integer not null, granter text not null, created integer not null, unique( subject, resource, operation ) )")
	db.exec("create index if not exists _access_resource on _access( resource, operation )")
	db.exec("create index if not exists _access_subject on _access( subject )")
	db.groups_setup()
}

// Check if a user has access to perform an operation on a resource
func (db *DB) access_check(user string, role string, resource string, operation string) bool {
	// Get resource hierarachy
	var resources []string
	parts := strings.Split(resource, "/")
	for i := len(parts); i > 0; i-- {
		resources = append(resources, strings.Join(parts[:i], "/"))
	}

	operations := []string{operation, "*"}

	// Build subject list in priority order
	var subjects []string
	if user != "" {
		subjects = append(subjects, user)

		for _, g := range db.group_memberships(user) {
			subjects = append(subjects, "@"+g)
		}

		if role == "administrator" {
			subjects = append(subjects, "#administrator")
		}
		if role != "" {
			subjects = append(subjects, "#user")
		}

		subjects = append(subjects, "+")
	}

	subjects = append(subjects, "*")

	// Check from most specific resource to least
	for _, res := range resources {
		for _, act := range operations {
			for _, subj := range subjects {
				var a Access
				if db.scan(&a, "select grant from _access where subject=? and resource=? and operation=?", subj, res, act) {
					return a.Grant == 1
				}
			}
		}
	}

	return false
}

// Grant or deny access
func (db *DB) access_set(subject string, resource string, operation string, grant bool, granter string) {
	db.access_setup() // Ensure table exists
	g := 0
	if grant {
		g = 1
	}

	db.exec("replace into _access ( subject, resource, operation, grant, granter, created ) values ( ?, ?, ?, ?, ?, ? )", subject, resource, operation, g, granter, now())
}

// Clear all access rules for a resource
func (db *DB) access_clear_resource(resource string) {
	db.access_setup() // Ensure table exists
	db.exec("delete from _access where resource=? or resource like ?", resource, resource+"/%")
}

// Clear all access rules for a subject
func (db *DB) access_clear_subject(subject string) {
	db.access_setup() // Ensure table exists
	db.exec("delete from _access where subject=?", subject)
}

// List access rules for a resource
func (db *DB) access_list_resource(resource string) ([]map[string]any, error) {
	return db.rows("select * from _access where resource=? order by subject", resource)
}

// List access rules for a subject
func (db *DB) access_list_subject(subject string) ([]map[string]any, error) {
	return db.rows("select * from _access where subject=? order by resource, operation", subject)
}

// Revoke access
func (db *DB) access_revoke(subject string, resource string, operation string) {
	db.access_setup() // Ensure table exists
	db.exec("delete from _access where subject=? and resource=? and operation=?", subject, resource, operation)
}

// Check access for an operation based on the "access" field in app.json
func (db *DB) access_check_operation(u *User, aa *AppAction) bool {
	if aa.Access.Resource == "" {
		return true
	}

	// Substitute parameters in resource
	resource := aa.Access.Resource
	for k, v := range aa.parameters {
		resource = strings.ReplaceAll(resource, ":"+k, v)
	}

	operation := aa.Access.Operation
	if operation == "" {
		operation = "*"
	}

	// Get user identity and role
	user := ""
	role := ""
	if u != nil {
		if u.Identity != nil {
			user = u.Identity.ID
		}
		role = u.Role
	}

	return db.access_check(user, role, resource, operation)
}

// mochi.access.check(user, resource, operation) -> bool: Check if a user has access to a resource
func api_access_check(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <user: string or None>, <resource: string>, <operation: string>")
	}

	user := ""
	if args[0] != sl.None {
		var ok bool
		user, ok = sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid user")
		}
	}

	resource, ok := sl.AsString(args[1])
	if !ok || resource == "" {
		return sl_error(fn, "invalid resource")
	}

	operation, ok := sl.AsString(args[2])
	if !ok || operation == "" {
		return sl_error(fn, "invalid operation")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	role := ""
	if user != "" {
		if u := user_by_identity(user); u != nil {
			role = u.Role
		}
	}

	db := db_app(owner, app.active)
	if db.access_check(user, role, resource, operation) {
		return sl.True, nil
	}
	return sl.False, nil
}

// mochi.access.allow(subject, resource, operation, granter) -> None: Grant access
func api_access_allow(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_access_set(t, fn, args, true)
}

// mochi.access.deny(subject, resource, operation, granter) -> None: Deny access
func api_access_deny(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_access_set(t, fn, args, false)
}

// mochi.access.allow/deny helper: Set access rule
func api_access_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, grant bool) (sl.Value, error) {
	if len(args) != 4 {
		return sl_error(fn, "syntax: <subject: string>, <resource: string>, <operation: string>, <granter: string>")
	}

	subject, ok := sl.AsString(args[0])
	if !ok || subject == "" {
		return sl_error(fn, "invalid subject")
	}

	resource, ok := sl.AsString(args[1])
	if !ok || resource == "" {
		return sl_error(fn, "invalid resource")
	}

	operation, ok := sl.AsString(args[2])
	if !ok || operation == "" {
		return sl_error(fn, "invalid operation")
	}

	granter, ok := sl.AsString(args[3])
	if !ok {
		return sl_error(fn, "invalid granter")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	db.access_set(subject, resource, operation, grant, granter)
	return sl.None, nil
}

// mochi.access.revoke(subject, resource, operation) -> None: Remove an access rule
func api_access_revoke(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <subject: string>, <resource: string>, <operation: string>")
	}

	subject, ok := sl.AsString(args[0])
	if !ok || subject == "" {
		return sl_error(fn, "invalid subject")
	}

	resource, ok := sl.AsString(args[1])
	if !ok || resource == "" {
		return sl_error(fn, "invalid resource")
	}

	operation, ok := sl.AsString(args[2])
	if !ok || operation == "" {
		return sl_error(fn, "invalid operation")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	db.access_revoke(subject, resource, operation)
	return sl.None, nil
}

// mochi.access.clear.resource(resource) -> None: Clear all access rules for a resource
func api_access_clear_resource(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <resource: string>")
	}

	resource, ok := sl.AsString(args[0])
	if !ok || resource == "" {
		return sl_error(fn, "invalid resource")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	db.access_clear_resource(resource)
	return sl.None, nil
}

// mochi.access.clear.subject(subject) -> None: Clear all access rules for a subject
func api_access_clear_subject(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <subject: string>")
	}

	subject, ok := sl.AsString(args[0])
	if !ok || subject == "" {
		return sl_error(fn, "invalid subject")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	db.access_clear_subject(subject)
	return sl.None, nil
}

// mochi.access.list.resource(resource) -> list: List access rules for a resource
func api_access_list_resource(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <resource: string>")
	}

	resource, ok := sl.AsString(args[0])
	if !ok || resource == "" {
		return sl_error(fn, "invalid resource")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	rows, err := db.access_list_resource(resource)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	return sl_encode(rows), nil
}

// mochi.access.list.subject(subject) -> list: List access rules for a subject
func api_access_list_subject(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <subject: string>")
	}

	subject, ok := sl.AsString(args[0])
	if !ok || subject == "" {
		return sl_error(fn, "invalid subject")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app(owner, app.active)
	rows, err := db.access_list_subject(subject)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	return sl_encode(rows), nil
}
