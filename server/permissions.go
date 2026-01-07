// Mochi server: App permissions
// Copyright Alistair Cunningham 2025
//
// Fine-grained permissions to control which Starlark APIs third-party apps can access.
// Built-in apps (internal != nil) bypass all permission checks.
// Third-party apps must declare permissions and have them granted by users.

package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"

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

// permissions defines all available permissions except dynamic ones (url, service)
var permissions = []Permission{
	// Standard permissions
	{"group/manage", false, false},

	// Restricted permissions
	{"user/read", true, true},
	{"setting/write", true, true},
	{"permission/manage", true, false},
	{"webpush/send", true, false},
}

var api_permission = sls.FromStringDict(sl.String("mochi.permission"), sl.StringDict{
	"administrator": sl.NewBuiltin("mochi.permission.administrator", api_permission_administrator),
	"apps":          sl.NewBuiltin("mochi.permission.apps", api_permission_apps),
	"check":         sl.NewBuiltin("mochi.permission.check", api_permission_check),
	"grant":         sl.NewBuiltin("mochi.permission.grant", api_permission_grant),
	"list":          sl.NewBuiltin("mochi.permission.list", api_permission_list),
	"restricted":    sl.NewBuiltin("mochi.permission.restricted", api_permission_restricted),
	"revoke":        sl.NewBuiltin("mochi.permission.revoke", api_permission_revoke),
})

// permission_restricted returns whether a permission is restricted.
// Dynamic permissions: url:* is restricted, url:<domain> is standard, service:<name> is standard.
func permission_restricted(name string) bool {
	// Handle dynamic url permission
	if strings.HasPrefix(name, "url:") {
		object := name[4:]
		return object == "*"
	}

	// Handle dynamic service permission
	if strings.HasPrefix(name, "service:") {
		return false
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
	// Dynamic permissions don't require admin
	if strings.HasPrefix(name, "url:") || strings.HasPrefix(name, "service:") {
		return false
	}

	for _, p := range permissions {
		if p.Name == name {
			return p.AdminOnly
		}
	}

	return false
}

// permission_split splits a permission into name and object parts
// "url:github.com" -> "url", "github.com"
// "group/modify" -> "group/modify", ""
func permission_split(permission string) (name, object string) {
	if strings.HasPrefix(permission, "url:") {
		return "url", permission[4:]
	}
	if strings.HasPrefix(permission, "service:") {
		return "service", permission[8:]
	}
	return permission, ""
}

// permission_join joins a name and object back into a permission string
func permission_join(name, object string) string {
	if name == "url" || name == "service" {
		return name + ":" + object
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

// app_label returns the human-readable label for an app
func app_label(app *App, user *User) string {
	if app == nil {
		return ""
	}
	av := app.active(user)
	if av != nil && av.Label != "" {
		return av.Label
	}
	// Fallback: convert "my-app" to "My App"
	name := app.id
	name = strings.ReplaceAll(name, "-", " ")
	name = strings.ReplaceAll(name, "_", " ")
	return strings.Title(name)
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
	return granted
}

// permission_grant grants a permission to an app for a user
func permission_grant(u *User, app_id string, permission string) {
	if u == nil {
		return
	}

	name, object := permission_split(permission)

	db := db_user(u, "user")
	db.permissions_setup()
	db.exec("replace into permissions (app, permission, object, granted) values (?, ?, ?, 1)", app_id, name, object)
}

// permission_revoke revokes a permission from an app for a user
func permission_revoke(u *User, app_id string, permission string) {
	if u == nil {
		return
	}

	name, object := permission_split(permission)

	db := db_user(u, "user")
	db.permissions_setup()
	db.exec("delete from permissions where app=? and permission=? and object=?", app_id, name, object)
}

// permissions_apps returns all app IDs that have permissions for a user
func permissions_apps(u *User) []string {
	if u == nil {
		return nil
	}

	db := db_user(u, "user")
	db.permissions_setup()
	rows, err := db.rows("select distinct app from permissions where granted=1")
	if err != nil {
		return nil
	}

	var result []string
	for _, row := range rows {
		if app, ok := row["app"].(string); ok {
			result = append(result, app)
		}
	}
	return result
}

// permissions_list returns all permissions for an app for a user
func permissions_list(u *User, app_id string) []map[string]any {
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

		result = append(result, map[string]any{
			"permission": permission_join(perm, obj),
			"granted":    granted,
			"restricted": permission_restricted(permission_join(perm, obj)),
		})
	}
	return result
}

// permissions_setup creates the permissions table
func (db *DB) permissions_setup() {
	db.exec("create table if not exists permissions (app text not null, permission text not null, object text not null default '', granted integer not null default 0, primary key (app, permission, object))")
}

// apps_setup creates the apps table in user.db for tracking per-user app state
func (db *DB) apps_setup() {
	db.exec("create table if not exists apps (app text primary key, setup integer not null default 0)")
}

// app_user_setup runs first-time setup when a user accesses an app.
// This grants default permissions for known apps and records the setup timestamp.
func app_user_setup(u *User, app_id string) {
	if u == nil || app_id == "" {
		return
	}

	db := db_user(u, "user")
	db.apps_setup()

	// Check if already set up
	setup := db.integer("select setup from apps where app=?", app_id)
	if setup > 0 {
		return
	}

	// Run setup: grant default permissions
	db.permissions_setup()
	defaults := apps_default_get(app_id)
	for _, p := range defaults {
		db.exec("replace into permissions (app, permission, object, granted) values (?, ?, ?, 1)", app_id, p.Permission, p.Object)
	}

	// Record setup timestamp
	db.exec("replace into apps (app, setup) values (?, ?)", app_id, time.Now().Unix())
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

	user, _ := t.Local("user").(*User)
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

// require_permission_url checks url permission for a specific URL
func require_permission_url(t *sl.Thread, fn *sl.Builtin, rawurl string) error {
	domain, err := domain_extract(rawurl)
	if err != nil {
		return fmt.Errorf("invalid URL: %v", err)
	}
	return require_permission(t, fn, "url:"+domain)
}

// require_permission_service checks service permission for a specific service.
// Services are permissionless - the receiving service decides whether to accept calls.
func require_permission_service(t *sl.Thread, fn *sl.Builtin, service string) error {
	return nil
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

	user, _ := t.Local("user").(*User)
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

	// Check that calling app has permissions/manage
	if err := require_permission(t, fn, "permission/manage"); err != nil {
		return sl_error(fn, "%v", err)
	}

	user, _ := t.Local("user").(*User)
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

	// Check that calling app has permissions/manage
	if err := require_permission(t, fn, "permission/manage"); err != nil {
		return sl_error(fn, "%v", err)
	}

	// Prevent an app from revoking its own permission/manage (prevents lockout)
	calling_app, _ := t.Local("app").(*App)
	if permission == "permission/manage" && calling_app != nil && calling_app.id == app_id {
		return sl_error(fn, "cannot revoke permission/manage from self")
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	permission_revoke(user, app_id, permission)
	return sl.None, nil
}

// mochi.permission.apps() -> list: List app IDs that have permissions
func api_permission_apps(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	apps := permissions_apps(user)
	return sl_encode(apps), nil
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

	user, _ := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Check if requesting permissions for a different app
	calling_app, _ := t.Local("app").(*App)
	if calling_app == nil || calling_app.id != app_id {
		// Require permission/manage to list other apps' permissions
		if err := require_permission(t, fn, "permission/manage"); err != nil {
			return nil, err
		}
	}

	perms := permissions_list(user, app_id)
	return sl_encode(perms), nil
}

// mochi.permission.restricted(permission) -> bool: Check if permission is restricted
func api_permission_restricted(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <permission: string>")
	}

	permission, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid permission")
	}

	if permission_restricted(permission) {
		return sl.True, nil
	}
	return sl.False, nil
}

// mochi.permission.administrator(permission) -> bool: Check if permission requires admin
func api_permission_administrator(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <permission: string>")
	}

	permission, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid permission")
	}

	if permission_administrator(permission) {
		return sl.True, nil
	}
	return sl.False, nil
}
