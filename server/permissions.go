// Mochi server: App permissions
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Fine-grained permissions to control which Starlark APIs third-party apps can access.
// Built-in apps (internal != nil) bypass all permission checks.
// Third-party apps must declare permissions and have them granted by users.

package main

import (
	"fmt"
	"net/url"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Permission defines a permission with its restriction level and admin requirement
type Permission struct {
	Name       string
	Restricted bool
	AdminOnly  bool
}

// PermissionError is returned when an app lacks a required permission
type PermissionError struct {
	Permission string
	Restricted bool
}

// Error implements the error interface
func (e *PermissionError) Error() string {
	level := "standard"
	if e.Restricted {
		level = "restricted"
	}
	return fmt.Sprintf("permission required: %s (%s)", e.Permission, level)
}

// permissions defines all available permissions except dynamic url permissions.
// Names are <resource>/read and <resource>/write; sign, send, notify, install,
// close, export, create and update are deliberate exceptions naming
// capabilities that do not read or write a resource. Do not fold them into
// write.
var permissions = []Permission{
	// Standard permissions
	{"accounts/read", false, false},
	{"accounts/write", false, false},
	{"accounts/ai", false, false},
	// Standard, not restricted: a restricted permission has no Allow button in the
	// request dialog, and asking to invite a friend is an ordinary request.
	{"friends/read", false, false},
	// entity.owned enumerates every object the user owns across every app, so it
	// is gated - but standard, since picking one's own object is routine.
	{"entity/read", false, false},
	// mochi.access.check resolves the subject's role out of users.db, which
	// accounts/read otherwise gates. The other access APIs touch only the app's
	// own table and stay ungated.
	{"access/read", false, false},
	{"groups/write", false, false},
	{"groups/read", false, false},
	{"camera", false, false},
	{"microphone", false, false},
	{"interests/read", false, false},
	{"interests/write", false, false},
	{"user/authentication/read", false, false},
	{"user/identity/write", false, false},
	{"user/sessions/read", false, false},
	{"user/sessions/write", false, false},

	// Restricted permissions
	{"accounts/notify", true, false},
	// Installing writes executable code to disk under an app's entity id: this is
	// permission to run code as any app. Not admin-only - apps_install_user gates
	// the USER; this gates the calling app.
	{"apps/install", true, false},
	// Signing arbitrary bytes with an entity's private key - checkable by anyone
	// forever, and core reuses those keys for export manifests and pubsub frames.
	{"entity/sign", true, false},
	// The app registry: which app answers a URL prefix, a class or a service
	// name, and which version is active. apps/write can point the login prefix
	// at another app, and core exempts whatever serves that prefix from its own
	// authentication gates.
	{"apps/read", true, true},
	{"apps/write", true, true},
	// The operator's own pages - terms, privacy - served to every visitor.
	{"documents/read", true, true},
	{"documents/write", true, true},
	// Domain routing decides which account a hostname serves: read returns the DNS
	// verification token, write repoints the hostname. Not administrator-only -
	// domain_can_manage_route gates the user; these gate the calling app.
	{"domains/read", true, false},
	{"domains/write", true, false},
	{"notifications/write", true, false},
	{"notifications/read", true, false},
	{"notifications/send", true, false},
	// Reading which permissions an app holds is a display concern; granting
	// and revoking them is not. They were one permission, so an app that only
	// wanted to show the user their grants had to be handed the power to
	// change them - and being restricted, it could not ask for the lesser one.
	{"permissions/read", true, false},
	{"permissions/write", true, false},
	// Repository content is the user's private source code, and the write side
	// can merge branches, so both sit with the permissions a user has to enable
	// deliberately rather than ones any app may ask for in passing.
	{"repositories/read", true, false},
	{"repositories/write", true, false},
	{"server/read", true, true},
	{"server/update", true, true},
	{"settings/write", true, true},
	{"tokens/create", true, false},
	// The shell hosts WebAuthn ceremonies for sandboxed apps, and a ceremony runs
	// on the real Mochi origin - so the assertion is valid for Mochi whoever
	// asked, and the browser prompt names Mochi, not the caller. Restricted:
	// granted from settings, never from a dialog the app raises.
	{"user/authentication/sign", true, false},
	// Rewriting how the account authenticates: recovery.generate invalidates the
	// codes the user holds, and totp.setup drops their authenticator out of the
	// usable factors until re-verified. Restricted; Login and Settings hold it by
	// default.
	{"user/authentication/write", true, false},
	// mochi.user.close schedules deletion and revokes every session - recoverable
	// only if the user cancels in time. Restricted, matching user/export.
	{"user/close", true, false},
	{"user/export", true, false},
	// Confirming the account's own address. Sends a code to the user's stored
	// username and consumes it - so it puts mail in someone's inbox, and
	// code_send creates the account when the address is unknown and signup is
	// open. It was gated on user/export, which describes something else.
	{"user/verification/write", true, false},
	{"users/read", true, true},
	{"users/write", true, true},
	{"webpush/send", true, false},
}

var api_permission = sls.FromStringDict(sl.String("mochi.permission"), sl.StringDict{
	"catalog": sl.NewBuiltin("mochi.permission.catalog", api_permission_catalog),
	"check":   sl.NewBuiltin("mochi.permission.check", api_permission_check),
	"grant":   sl.NewBuiltin("mochi.permission.grant", api_permission_grant),
	"level":   sl.NewBuiltin("mochi.permission.level", api_permission_level),
	"list":    sl.NewBuiltin("mochi.permission.list", api_permission_list),
	"name":    sl.NewBuiltin("mochi.permission.name", api_permission_name),
	"revoke":  sl.NewBuiltin("mochi.permission.revoke", api_permission_revoke),
})

// permission_restricted returns whether a permission is restricted.
// Dynamic permissions: url:* is restricted, url:<domain> is standard.
func permission_restricted(name string) bool {
	// Handle dynamic url permission
	if strings.HasPrefix(name, "url:") {
		object := name[4:]
		return object == "*"
	}

	// Look up static permission
	for _, p := range permissions {
		if p.Name == name {
			return p.Restricted
		}
	}

	// Unknown permission defaults to restricted for safety
	return true
}

// permission_administrator returns whether a permission requires admin role
func permission_administrator(name string) bool {
	// Dynamic url permissions don't require admin
	if strings.HasPrefix(name, "url:") {
		return false
	}

	for _, p := range permissions {
		if p.Name == name {
			return p.AdminOnly
		}
	}

	return false
}

// permission_name resolves the human-readable, translated name for a permission
// code in the given language. Dynamic url:/service: permissions are templated;
// everything else resolves a "permissions.<code>" core label, with "/" in the
// code mapped to "." to match the dotted core-label key convention.
func permission_name(language, permission string) string {
	if permission == "url:*" {
		return resolve_core_label(language, "permissions.url.all", nil)
	}
	if strings.HasPrefix(permission, "url:") {
		return resolve_core_label(language, "permissions.url", map[string]any{"domain": permission[4:]})
	}
	if strings.HasPrefix(permission, "service/") {
		return resolve_core_label(language, "permissions.service", map[string]any{"service": permission[8:]})
	}
	return resolve_core_label(language, "permissions."+strings.ReplaceAll(permission, "/", "."), nil)
}

// thread_language resolves the request's language from a Starlark thread, using
// the same priority as api_app_label: signed-in user preference, then the
// thread-local language stashed by the request handler, then English.
func thread_language(t *sl.Thread) string {
	if user := principal_caller(t); user != nil {
		return user_language(user)
	}
	if l, ok := t.Local("language").(string); ok && l != "" {
		return l
	}
	return "en"
}

// permission_split splits a permission into name and object parts
// "url:github.com" -> "url", "github.com"
// "group/modify" -> "group/modify", ""
func permission_split(permission string) (name, object string) {
	if strings.HasPrefix(permission, "url:") {
		return "url", permission[4:]
	}
	return permission, ""
}

// permission_join joins a name and object back into a permission string
func permission_join(name, object string) string {
	if name == "url" {
		return name + ":" + object
	}
	if object != "" {
		return name + "/" + object
	}
	return name
}

// domain_extract extracts the domain from a URL
func domain_extract(rawurl string) (string, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return "", err
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("no host in URL")
	}
	return strings.ToLower(host), nil
}

// domain_matches checks if a permission domain matches a request domain
// "github.com" matches "github.com" and "api.github.com"
// Subdomain matching: permission for "github.com" allows "api.github.com"
func domain_matches(permission_domain, request_domain string) bool {
	permission_domain = strings.ToLower(permission_domain)
	request_domain = strings.ToLower(request_domain)

	// Exact match
	if permission_domain == request_domain {
		return true
	}

	// Wildcard matches everything
	if permission_domain == "*" {
		return true
	}

	// Subdomain match: "url:github.com" allows "api.github.com"
	// Must end with ".github.com" (note the dot prefix)
	if strings.HasSuffix(request_domain, "."+permission_domain) {
		return true
	}

	return false
}

// app_is_internal returns true for Go-based internal apps that bypass permission checks
func app_is_internal(app *App) bool {
	return app != nil && app.internal != nil
}

// permission_granted checks if a permission is granted for an app to a user.
// For default apps, permissions are lazily granted on first check.
func permission_granted(u *User, app_id string, permission string) bool {
	if u == nil {
		return false
	}

	name, object := permission_split(permission)

	db := db_user(u, "user")
	db.permissions_setup()

	// For url permissions, check if any granted domain matches
	if name == "url" {
		// First check for exact match or wildcard
		granted, _ := db.exists("select 1 from permissions where app=? and permission='url' and (object=? or object='*') and granted=1", app_id, object)
		if granted {
			return true
		}

		// Check all granted domains for subdomain match
		rows, err := db.rows("select object from permissions where app=? and permission='url' and granted=1", app_id)
		if err == nil {
			for _, row := range rows {
				if obj, ok := row["object"].(string); ok {
					if domain_matches(obj, object) {
						return true
					}
				}
			}
		}
		return false
	}

	// For other permissions, check exact match
	granted, _ := db.exists("select 1 from permissions where app=? and permission=? and object=? and granted=1", app_id, name, object)
	if granted {
		return true
	}

	// Default grants are seeded lazily only while the user is still bootstrapping
	// from another host, the window app_user_setup skips. A revoked permission has
	// an explicit granted=0 row, so it is never re-granted here.
	if user_pending(u) {
		if seeded, _ := db.exists("select 1 from permissions where app=? and permission=? and object=?", app_id, name, object); !seeded {
			for _, p := range apps_default_get(app_id) {
				if p.Permission == name && p.Object == object {
					db.exec("insert or ignore into permissions (app, permission, object, granted) values (?, ?, ?, 1)", app_id, name, object) // exec-ok: transient bootstrap-window seed of a re-derivable default grant; app_user_setup writes the authoritative row once the user is active
					return true
				}
			}
		}
	}

	return false
}

// permission_grant grants a permission to an app for a user
func permission_grant(u *User, app_id string, permission string) {
	if u == nil {
		return
	}

	name, object := permission_split(permission)

	db := db_user(u, "user")
	db.permissions_setup()
	db.permissions_upsert(app_id, name, object, 1)
}

// permission_revoke revokes a permission from an app for a user
func permission_revoke(u *User, app_id string, permission string) {
	if u == nil {
		return
	}

	name, object := permission_split(permission)

	db := db_user(u, "user")
	db.permissions_setup()
	// granted=0 rather than a row delete, and the difference matters: the row's
	// presence is the whole reason the revoke sticks. permissions_default is
	// insert-or-ignore, so a later setup pass finds this key already present and
	// skips it instead of re-granting the app's default.
	db.permissions_upsert(app_id, name, object, 0)
}

// permissions_list returns all permissions for an app for a user
func permissions_list(u *User, app_id, language string) []map[string]any {
	if u == nil {
		return nil
	}

	db := db_user(u, "user")
	db.permissions_setup()

	rows, err := db.rows("select permission, object, granted from permissions where app=?", app_id)
	if err != nil {
		return nil
	}

	var result []map[string]any
	for _, row := range rows {
		perm := row["permission"].(string)
		obj := row["object"].(string)
		granted := row["granted"].(int64) == 1
		full := permission_join(perm, obj)

		result = append(result, map[string]any{
			"permission": full,
			"name":       permission_name(language, full),
			"granted":    granted,
			"restricted": permission_restricted(full),
			"admin":      permission_administrator(full),
		})
	}
	return result
}

// permissions_setup creates the permissions table: one row per (app,
// permission, object). A revoke writes granted=0 rather than deleting, which is
// what makes permissions_default's insert-or-ignore safe.
func (db *DB) permissions_setup() {
	db.exec("create table if not exists permissions ( app text not null, permission text not null, object text not null default '', granted integer not null default 0, created integer not null default 0, primary key ( app, permission, object ) )")
}

// permissions_upsert applies one explicit user decision (grant granted=1 /
// revoke granted=0).
func (db *DB) permissions_upsert(app string, permission string, object string, granted int) {
	db.exec("insert into permissions ( app, permission, object, granted, created ) values ( ?, ?, ?, ?, ? ) on conflict ( app, permission, object ) do update set granted=excluded.granted, created=excluded.created", app, permission, object, granted, now())
}

// permissions_default seeds an app's default-permission grant. Insert-or-ignore:
// it never overrides an explicit user grant or revoke, so re-running setup after
// the user revoked a default leaves the revoke in place — the user's decision
// sticks.
func (db *DB) permissions_default(app string, permission string, object string) {
	db.exec("insert or ignore into permissions ( app, permission, object, granted, created ) values ( ?, ?, ?, 1, ? )", app, permission, object, now())
}

// apps_setup creates the apps table in user.db for tracking per-user app state
func (db *DB) apps_setup() {
	db.exec("create table if not exists apps (app text primary key, setup integer not null default 0)")
}

// app_user_setup grants default permissions when a user first accesses an app.
// Tracks the number of default permissions so new ones are applied after server updates.
func app_user_setup(u *User, app_id string) {
	if u == nil || app_id == "" {
		return
	}

	// Skip while the user's per-user DBs are being bootstrapped: user.db is
	// rename(2)-swapped underneath us, and opening it mid-swap reads a malformed
	// image. The `setup != expected` check makes the next call a real setup pass.
	if user_pending(u) {
		return
	}

	db := db_user(u, "user")
	db.apps_setup()

	defaults := apps_default_get(app_id)
	expected := len(defaults) + 1

	// Check if already set up with the current set of default permissions
	setup := db.integer("select setup from apps where app=?", app_id)
	if setup == expected {
		return
	}

	// Grant default permissions. permissions_default is insert-or-ignore, so this
	// never resurrects a permission the user revoked: the revoke left a granted=0
	// row behind, and the seed skips any key that already has one.
	db.permissions_setup()
	for _, p := range defaults {
		db.permissions_default(app_id, p.Permission, p.Object)
	}

	// Record permission count so we detect when defaults change. The counter is an
	// optimisation only - it says the current default set has already been seeded,
	// so the loop above is skipped on later calls. The permissions rows themselves
	// are the authoritative state.
	db.exec("replace into apps (app, setup) values (?, ?)", app_id, expected)
}

// apps_default_get returns the default permissions for an app.
// Matches by entity ID first, then by name (case-insensitive) for development apps.
func apps_default_get(app_id string) []struct{ Permission, Object string } {
	// First try exact ID match (for published apps)
	for _, app := range apps_default {
		if app.ID == app_id {
			return app.Permissions
		}
	}
	// Then try name match (for development apps like "notifications" -> "Notifications")
	app_id_lower := strings.ToLower(app_id)
	for _, app := range apps_default {
		if strings.ToLower(app.Name) == app_id_lower {
			return app.Permissions
		}
	}
	return nil
}

// require_permission checks if an app has a permission, returning an error if not.
// Internal apps always pass. Returns nil if permission is granted.
func require_permission(t *sl.Thread, fn *sl.Builtin, permission string) error {
	app, _ := t.Local("app").(*App)
	if app == nil {
		return fmt.Errorf("no app context")
	}

	// Internal Go apps bypass permission checks
	if app_is_internal(app) {
		return nil
	}

	user := principal_caller(t)
	if user == nil {
		return fmt.Errorf("no user context")
	}

	// Check if permission requires admin
	if permission_administrator(permission) && !user.administrator() {
		return fmt.Errorf("permission %q requires administrator role", permission)
	}

	// Check if permission is granted
	if permission_granted(user, app.id, permission) {
		return nil
	}

	return &PermissionError{
		Permission: permission,
		Restricted: permission_restricted(permission),
	}
}

// require_permission_acting checks against the user the call acts for, not the
// caller. Use it on APIs that legitimately answer an anonymous caller - a
// public action runs as the owner, where plain require_permission refuses on
// "no user context". Use require_permission everywhere else.
func require_permission_acting(t *sl.Thread, fn *sl.Builtin, permission string) error {
	app, _ := t.Local("app").(*App)
	if app == nil {
		return fmt.Errorf("no app context")
	}
	if app_is_internal(app) {
		return nil
	}

	user, err := principal_storage(t)
	if err != nil || user == nil {
		return fmt.Errorf("no user context")
	}
	if permission_administrator(permission) && !user.administrator() {
		return fmt.Errorf("permission %q requires administrator role", permission)
	}
	if permission_granted(user, app.id, permission) {
		return nil
	}
	return &PermissionError{Permission: permission, Restricted: permission_restricted(permission)}
}

// require_permission_url checks url permission for a specific URL
func require_permission_url(t *sl.Thread, fn *sl.Builtin, rawurl string) error {
	domain, err := domain_extract(rawurl)
	if err != nil {
		return fmt.Errorf("invalid URL: %v", err)
	}
	return require_permission(t, fn, "url:"+domain)
}

// mochi.permission.check(permission) -> bool: Check if current app has a permission
func api_permission_check(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <permission: string>")
	}

	permission, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid permission")
	}

	app, _ := t.Local("app").(*App)
	if app == nil {
		return sl.False, nil
	}

	// Internal apps always have all permissions
	if app_is_internal(app) {
		return sl.True, nil
	}

	user := principal_caller(t)
	if user == nil {
		return sl.False, nil
	}

	if permission_granted(user, app.id, permission) {
		return sl.True, nil
	}
	return sl.False, nil
}

// mochi.permission.grant(app, permission) -> None: Grant a permission (requires permission/manage)
func api_permission_grant(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <app: string>, <permission: string>")
	}

	app_id, ok := sl.AsString(args[0])
	if !ok || app_id == "" {
		return sl_error(fn, "invalid app")
	}

	permission, ok := sl.AsString(args[1])
	if !ok || permission == "" {
		return sl_error(fn, "invalid permission")
	}

	// Check that calling app has permissions/write
	if err := require_permission(t, fn, "permissions/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	permission_grant(user, app_id, permission)
	return sl.None, nil
}

// mochi.permission.revoke(app, permission) -> None: Revoke a permission (requires permission/manage)
func api_permission_revoke(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <app: string>, <permission: string>")
	}

	app_id, ok := sl.AsString(args[0])
	if !ok || app_id == "" {
		return sl_error(fn, "invalid app")
	}

	permission, ok := sl.AsString(args[1])
	if !ok || permission == "" {
		return sl_error(fn, "invalid permission")
	}

	// Check that calling app has permissions/write
	if err := require_permission(t, fn, "permissions/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

	// Prevent an app from revoking its own permission/manage (prevents lockout)
	calling_app, _ := t.Local("app").(*App)
	if permission == "permissions/write" && calling_app != nil && calling_app.id == app_id {
		return sl_error(fn, "cannot revoke permission/manage from self")
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	permission_revoke(user, app_id, permission)
	return sl.None, nil
}

// mochi.permission.list(app) -> list: List permissions for an app.
// Apps can list their own permissions freely, but require permission/manage to list other apps.
func api_permission_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <app: string>")
	}

	app_id, ok := sl.AsString(args[0])
	if !ok || app_id == "" {
		return sl_error(fn, "invalid app")
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Check if requesting permissions for a different app
	calling_app, _ := t.Local("app").(*App)
	if calling_app == nil || calling_app.id != app_id {
		// Require permission/manage to list other apps' permissions
		if err := require_permission(t, fn, "permissions/read"); err != nil {
			return nil, err
		}
	}

	perms := permissions_list(user, app_id, thread_language(t))
	return sl_encode(perms), nil
}

// mochi.permission.catalog() -> list: List all defined permissions, each with
// its code, translated name, and security flags. Dynamic url:/service:
// permissions are not listed (they are named on demand via mochi.permission.name).
func api_permission_catalog(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 0 {
		return sl_error(fn, "syntax: no arguments")
	}

	language := thread_language(t)
	result := make([]map[string]any, 0, len(permissions))
	for _, p := range permissions {
		result = append(result, map[string]any{
			"permission": p.Name,
			"name":       permission_name(language, p.Name),
			"restricted": p.Restricted,
			"admin":      p.AdminOnly,
		})
	}
	return sl_encode(result), nil
}

// mochi.permission.name(permission) -> string: The human-readable, translated
// name for a permission code, resolved in the request's language. Handles
// dynamic url:/service: codes as well as the static catalog.
func api_permission_name(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <permission: string>")
	}

	permission, ok := sl.AsString(args[0])
	if !ok || permission == "" {
		return sl_error(fn, "invalid permission")
	}

	return sl.String(permission_name(thread_language(t), permission)), nil
}

// mochi.permission.level(permission) -> string: Returns the permission's security
// level — one of "standard" (freely grantable by any user), "restricted" (requires
// the user to enable it from app settings), or "administrator" (requires the admin
// role to grant). Admin-only is strictly stronger than restricted, so a permission
// with both flags returns "administrator".
func api_permission_level(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <permission: string>")
	}

	permission, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid permission")
	}

	if permission_administrator(permission) {
		return sl.String("administrator"), nil
	}
	if permission_restricted(permission) {
		return sl.String("restricted"), nil
	}
	return sl.String("standard"), nil
}
