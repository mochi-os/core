// Mochi server: Access control
// Copyright © 2026 Mochi OÜ
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
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"strings"
)

type Access struct {
	Grant int
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

// Create access control table in the system database (app.db). The table is a
// versioned LWW-Register: each (subject, resource, operation) carries a
// per-key Lamport `version` and an originating-host `writer`, so concurrent
// grants/denies/revokes from different hosts converge identically — a
// deterministic max over (version, fail-closed-on-tie, writer). Revoke is a
// `removed=1` tombstone, never a DELETE, so a stale grant can't resurrect a
// revoked rule.
func (db *DB) access_setup() {
	db.exec("create table if not exists access ( subject text not null, resource text not null, operation text not null, grant integer not null, removed integer not null default 0, granter text not null, writer text not null default '', version integer not null default 1, created integer not null, primary key ( subject, resource, operation ) )")
	db.access_migrate()
	db.exec("create index if not exists access_resource on access( resource, operation )")
	db.exec("create index if not exists access_subject on access( subject )")
}

// access_migrate rebuilds a legacy access table (autoincrement `id` PK, no
// `version` column) into the versioned register. Each host runs it locally over
// its already-converged rows, seeding version=1 / writer='' / removed=0
// deterministically, so the rebuilt tables stay identical across hosts. No-op
// once migrated.
func (db *DB) access_migrate() {
	if db.has_column("access", "version") {
		return
	}
	db.exec("create table access_new ( subject text not null, resource text not null, operation text not null, grant integer not null, removed integer not null default 0, granter text not null, writer text not null default '', version integer not null default 1, created integer not null, primary key ( subject, resource, operation ) )")
	db.exec("insert into access_new ( subject, resource, operation, grant, granter, created ) select subject, resource, operation, grant, granter, created from access")
	db.exec("drop table access")
	db.exec("alter table access_new rename to access")
}

// has_column reports whether table has the named column.
func (db *DB) has_column(table string, column string) bool {
	rows, err := db.internal.Query("select name from pragma_table_info('" + table + "')")
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil && name == column {
			return true
		}
	}
	return false
}

// access_upsert applies one versioned-register write (allow / deny / revoke) and
// resolves conflicts deterministically so every host converges: a higher
// per-key `version` wins; on a version tie a non-allow (deny or removed) beats an
// allow (fail-closed); any remaining tie is settled by the higher `writer`. The
// version is computed here (seen + 1) and carried as a literal — never recomputed
// on apply, which would diverge per host.
func (db *DB) access_upsert(subject string, resource string, operation string, grant int, removed int, granter string) {
	var seen struct{ Version int64 }
	db.scan(&seen, "select coalesce( max( version ), 0 ) as version from access where subject=? and resource=? and operation=?", subject, resource, operation)
	db.exec_replicated(access_upsert_sql, subject, resource, operation, grant, removed, granter, net_id, seen.Version+1, now())
}

const access_upsert_sql = `insert into access ( subject, resource, operation, grant, removed, granter, writer, version, created )
values ( ?, ?, ?, ?, ?, ?, ?, ?, ? )
on conflict ( subject, resource, operation ) do update set
	grant=excluded.grant, removed=excluded.removed, granter=excluded.granter,
	writer=excluded.writer, version=excluded.version, created=excluded.created
where excluded.version > access.version
	or ( excluded.version = access.version
		and ( case when excluded.removed=0 and excluded.grant=1 then 0 else 1 end )
		> ( case when access.removed=0 and access.grant=1 then 0 else 1 end ) )
	or ( excluded.version = access.version
		and ( case when excluded.removed=0 and excluded.grant=1 then 0 else 1 end )
		= ( case when access.removed=0 and access.grant=1 then 0 else 1 end )
		and excluded.writer > access.writer )`

// Check if a user has access to perform an operation on a resource
// owner is the user whose user.db contains the groups
func (db *DB) access_check(owner *User, user string, role string, resource string, operation string) bool {
	db.access_setup() // Ensure table exists

	// Get resource hierarchy
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

	// Check from most specific resource to least
	for _, res := range resources {
		for _, act := range operations {
			for _, subj := range subjects {
				var a Access
				if db.scan(&a, "select grant from access where subject=? and resource=? and operation=? and removed=0", subj, res, act) {
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

	db.access_upsert(subject, resource, operation, g, 0, granter)
	audit_permission_changed(granter, subject, resource, operation, grant)
}

// Clear all access rules for a resource (and its sub-resources). Each rule is
// tombstoned (a versioned removed=1 write), not deleted, so the clear converges
// under multi-master and a stale concurrent grant can't survive it.
func (db *DB) access_clear_resource(resource string) {
	db.access_setup() // Ensure table exists
	rows, _ := db.rows("select subject, resource, operation from access where ( resource=? or resource like ? ) and removed=0", resource, resource+"/%")
	for _, r := range rows {
		db.access_upsert(row_string(r, "subject"), row_string(r, "resource"), row_string(r, "operation"), 0, 1, "")
	}
}

// Clear all access rules for a subject (tombstoned per-rule; see access_clear_resource).
func (db *DB) access_clear_subject(subject string) {
	db.access_setup() // Ensure table exists
	rows, _ := db.rows("select subject, resource, operation from access where subject=? and removed=0", subject)
	for _, r := range rows {
		db.access_upsert(row_string(r, "subject"), row_string(r, "resource"), row_string(r, "operation"), 0, 1, "")
	}
}

// List access rules for a resource (active rules only; tombstones hidden).
func (db *DB) access_list_resource(resource string) ([]map[string]any, error) {
	return db.rows("select subject, resource, operation, grant, granter, created from access where resource=? and removed=0 order by subject", resource)
}

// List access rules for a subject (active rules only; tombstones hidden).
func (db *DB) access_list_subject(subject string) ([]map[string]any, error) {
	return db.rows("select subject, resource, operation, grant, granter, created from access where subject=? and removed=0 order by resource, operation", subject)
}

// Revoke access. Writes a versioned removed=1 tombstone (never a DELETE) so a
// later grant supersedes it and a stale earlier grant can't resurrect it.
func (db *DB) access_revoke(subject string, resource string, operation string) {
	db.access_setup() // Ensure table exists
	db.access_upsert(subject, resource, operation, 0, 1, "")
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

	db := db_app_system(owner, app)
	if db.access_check(owner, user, role, resource, operation) {
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

	db := db_app_system(owner, app)
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

	db := db_app_system(owner, app)
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

	db := db_app_system(owner, app)
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

	db := db_app_system(owner, app)
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

	db := db_app_system(owner, app)
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

	db := db_app_system(owner, app)
	rows, err := db.access_list_subject(subject)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	return sl_encode(rows), nil
}
