// Mochi server: Access control
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

/* Resolution order, first match wins:
	1. User's ID
	2. User's groups, @groupname, recursively
	3. User's role, #administrator or #user
	4. Authenticated (+)
	5. Anonymous (*), including not logged in
Deny has priority over allow */

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"strings"
)

type Access struct {
	Grant int
}

// An app may ask about at most this many operations in one
// mochi.access.check.any call. The access model has four levels plus the
// wildcard, so this is generous; it exists so a malformed caller cannot turn
// one call into an unbounded query loop.
const access_operations_maximum = 32

var api_access = sls.FromStringDict(sl.String("mochi.access"), sl.StringDict{
	"allow": sl.NewBuiltin("mochi.access.allow", api_access_allow),
	"clear": sls.FromStringDict(sl.String("mochi.access.clear"), sl.StringDict{
		"resource": sl.NewBuiltin("mochi.access.clear.resource", api_access_clear_resource),
		"subject":  sl.NewBuiltin("mochi.access.clear.subject", api_access_clear_subject),
	}),
	"check": &access_check_module{},
	"deny":  sl.NewBuiltin("mochi.access.deny", api_access_deny),
	"list": sls.FromStringDict(sl.String("mochi.access.list"), sl.StringDict{
		"resource": sl.NewBuiltin("mochi.access.list.resource", api_access_list_resource),
		"subject":  sl.NewBuiltin("mochi.access.list.subject", api_access_list_subject),
	}),
	"revoke": sl.NewBuiltin("mochi.access.revoke", api_access_revoke),
})

// access_check_module is a callable module that also has an .any method
// Usage: mochi.access.check(user, resource, operation) or
// mochi.access.check.any(user, resource, operations)
type access_check_module struct{}

func (m *access_check_module) String() string        { return "mochi.access.check" }
func (m *access_check_module) Type() string          { return "module" }
func (m *access_check_module) Freeze()               {}
func (m *access_check_module) Truth() sl.Bool        { return sl.True }
func (m *access_check_module) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }
func (m *access_check_module) AttrNames() []string   { return []string{"any"} }
func (m *access_check_module) Name() string          { return "mochi.access.check" }

func (m *access_check_module) Attr(name string) (sl.Value, error) {
	if name == "any" {
		return sl.NewBuiltin("mochi.access.check.any", api_access_check_any), nil
	}
	return nil, nil
}

// The builtin is passed through rather than nil so sl_error still prefixes its
// message with "mochi.access.check()", exactly as it did when check was a plain
// builtin.
var access_check_builtin = sl.NewBuiltin("mochi.access.check", api_access_check)

func (m *access_check_module) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_access_check(thread, access_check_builtin, args, kwargs)
}

// Create access control table in the system database (app.db).
func (db *DB) access_setup() {
	db.exec("create table if not exists access ( subject text not null, resource text not null, operation text not null, grant integer not null, granter text not null, created integer not null, primary key ( subject, resource, operation ) )")
	db.exec("create index if not exists access_resource on access( resource, operation )")
	db.exec("create index if not exists access_subject on access( subject )")
}

// access_upsert applies one allow / deny write.
func (db *DB) access_upsert(subject string, resource string, operation string, grant int, granter string) {
	db.exec("insert into access ( subject, resource, operation, grant, granter, created ) values ( ?, ?, ?, ?, ?, ? ) on conflict ( subject, resource, operation ) do update set grant=excluded.grant, granter=excluded.granter, created=excluded.created", subject, resource, operation, grant, granter, now())
}

// Check if a user has access to perform an operation on a resource
// owner is the user whose user.db contains the groups
func (db *DB) access_check(owner *User, user string, role string, resource string, operation string) bool {
	return db.access_check_any(owner, user, role, resource, []string{operation})
}

// access_check_any answers whether the user holds ANY of the operations, taken
// in the given order. Each operation is decided exactly as a single
// access_check would decide it, so a caller passing one operation per call and
// a caller passing them all here get the same answer; only the setup is
// shared. That setup is the expensive part - the subject list costs a
// group_memberships walk of up to group_depth_maximum levels, and the old
// per-operation calling pattern paid for it once per level.
func (db *DB) access_check_any(owner *User, user string, role string, resource string, operations []string) bool {
	db.access_setup() // Ensure table exists

	// Get resource hierarchy
	var resources []string
	parts := strings.Split(resource, "/")
	for i := len(parts); i > 0; i-- {
		resources = append(resources, strings.Join(parts[:i], "/"))
	}

	// Build subject list in priority order
	var subjects []string
	if user != "" {
		subjects = append(subjects, user)

		// Look up group memberships from the owner's user.db
		if owner != nil {
			user_db := db_user(owner, "user")
			for _, g := range user_db.group_memberships(user) {
				subjects = append(subjects, "@"+g)
			}
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

	for _, operation := range operations {
		if db.access_decide(resources, subjects, user, resource, operation) {
			return true
		}
	}

	return false
}

// access_decide settles one operation: the first matching row wins, whether it
// grants or denies. Resource order is most specific to least.
func (db *DB) access_decide(resources []string, subjects []string, user string, resource string, operation string) bool {
	for _, res := range resources {
		for _, act := range []string{operation, "*"} {
			for _, subj := range subjects {
				var a Access
				if db.scan(&a, "select grant from access where subject=? and resource=? and operation=?", subj, res, act) {
					if a.Grant != 1 {
						audit_access_denied(user, resource, operation)
					}
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

	db.access_upsert(subject, resource, operation, g, granter)
	audit_permission_changed(granter, subject, resource, operation, grant)
}

// Clear all access rules for a resource (and its sub-resources).
func (db *DB) access_clear_resource(resource string) {
	db.access_setup() // Ensure table exists
	db.exec("delete from access where resource=? or resource like ?", resource, resource+"/%")
}

// Clear all access rules for a subject.
func (db *DB) access_clear_subject(subject string) {
	db.access_setup() // Ensure table exists
	db.exec("delete from access where subject=?", subject)
}

// List access rules for a resource. access_revoke deletes the row, so every row
// present is an active rule; there is nothing to filter out.
func (db *DB) access_list_resource(resource string) ([]map[string]any, error) {
	return db.rows("select subject, resource, operation, grant, granter, created from access where resource=? order by subject", resource)
}

// List access rules for a subject. See access_list_resource.
func (db *DB) access_list_subject(subject string) ([]map[string]any, error) {
	return db.rows("select subject, resource, operation, grant, granter, created from access where subject=? order by resource, operation", subject)
}

// Revoke access.
func (db *DB) access_revoke(subject string, resource string, operation string) {
	db.access_setup() // Ensure table exists
	db.exec("delete from access where subject=? and resource=? and operation=?", subject, resource, operation)
}

// mochi.access.check(user, resource, operation) -> bool: Check if a user has access to a resource
func api_access_check(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission_acting(t, fn, "access/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

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
		// Reject special subject markers - these are not valid user IDs
		if user == "*" || user == "+" || strings.HasPrefix(user, "#") || strings.HasPrefix(user, "@") {
			return sl_error(fn, "invalid user: special markers (*, +, #, @) are not valid user IDs")
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

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	role := ""
	if user != "" {
		if u := user_by_identity(user); u != nil {
			role = u.Role
		}
	}

	db := db_app_system(owner, app)
	defer db.close()
	if db.access_check(owner, user, role, resource, operation) {
		return sl.True, nil
	}
	return sl.False, nil
}

// mochi.access.check.any(user, resource, operations) -> bool: True if the user
// holds ANY of the operations. Equivalent to calling mochi.access.check() once
// per operation in the same order and stopping at the first True, but the
// subject list - which costs a group-membership walk - is built once instead of
// once per operation.
func api_access_check_any(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission_acting(t, fn, "access/read"); err != nil {
		return sl_error(fn, "%v", err)
	}

	if len(args) != 3 {
		return sl_error(fn, "syntax: <user: string or None>, <resource: string>, <operations: list of string>")
	}

	user := ""
	if args[0] != sl.None {
		var ok bool
		user, ok = sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid user")
		}
		// Reject special subject markers - these are not valid user IDs
		if user == "*" || user == "+" || strings.HasPrefix(user, "#") || strings.HasPrefix(user, "@") {
			return sl_error(fn, "invalid user: special markers (*, +, #, @) are not valid user IDs")
		}
	}

	resource, ok := sl.AsString(args[1])
	if !ok || resource == "" {
		return sl_error(fn, "invalid resource")
	}

	// A tuple is accepted as well as a list: core encodes a JSON array as a
	// Starlark tuple, so a caller forwarding one straight through has one.
	sequence, ok := args[2].(sl.Sequence)
	if !ok {
		return sl_error(fn, "invalid operations: expected a list of strings")
	}
	if sequence.Len() == 0 {
		return sl_error(fn, "invalid operations: empty")
	}
	if sequence.Len() > access_operations_maximum {
		return sl_error(fn, "invalid operations: at most %d", access_operations_maximum)
	}
	var operations []string
	iterator := sl.Iterate(sequence)
	defer iterator.Done()
	var element sl.Value
	for iterator.Next(&element) {
		operation, ok := sl.AsString(element)
		if !ok || operation == "" {
			return sl_error(fn, "invalid operation")
		}
		operations = append(operations, operation)
	}

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	role := ""
	if user != "" {
		if u := user_by_identity(user); u != nil {
			role = u.Role
		}
	}

	db := db_app_system(owner, app)
	defer db.close()
	if db.access_check_any(owner, user, role, resource, operations) {
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

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	defer db.close()
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

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	defer db.close()
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

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	defer db.close()
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

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	defer db.close()
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

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	defer db.close()
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

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	db := db_app_system(owner, app)
	defer db.close()
	rows, err := db.access_list_subject(subject)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	return sl_encode(rows), nil
}
