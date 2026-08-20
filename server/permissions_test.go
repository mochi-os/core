// Mochi server: Permissions system tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// Test helpers

// create_permission_test_user creates a temporary user for testing
func create_permission_test_user(t *testing.T, id string) *User {
	t.Helper()

	// Ensure user directory exists
	user_dir := filepath.Join(data_dir, "users", id)
	os.MkdirAll(user_dir, 0755)

	return &User{
		UID:      id,
		Username: "test@example.com",
		Role:     "user",
	}
}

// create_test_admin creates a temporary admin user for testing
func create_test_admin(t *testing.T, id string) *User {
	t.Helper()
	u := create_permission_test_user(t, id)
	u.Role = "administrator"
	return u
}

// create_test_thread creates a Starlark thread with user/app context
func create_test_thread(user *User, app *App) *sl.Thread {
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", app)
	thread.SetLocal("owner", user)
	return thread
}

// create_external_app creates a mock external (third-party) app
func create_external_app(id string) *App {
	return &App{
		id:       id,
		internal: nil, // External apps have nil internal
	}
}

// create_internal_app creates a mock internal (Go-based) app
func create_internal_app(id string) *App {
	return &App{
		id:       id,
		internal: &AppVersion{}, // Internal apps have non-nil internal
	}
}

// cleanup_test_user removes test user data
//
//lint:ignore U1000 test scaffolding
func cleanup_test_user(t *testing.T, id string) {
	t.Helper()
	user_dir := filepath.Join(data_dir, "users", id)
	os.RemoveAll(user_dir)
}

// =============================================================================
// Permission Restricted Tests
// =============================================================================

func TestPermissionRestrictedStandard(t *testing.T) {
	standard_perms := []string{
		"groups/write",
		"url:example.com",
	}

	for _, perm := range standard_perms {
		if permission_restricted(perm) {
			t.Errorf("permission_restricted(%q) = true, want false (standard)", perm)
		}
	}
}

func TestPermissionRestrictedRestricted(t *testing.T) {
	restricted_perms := []string{
		"users/read",
		"settings/write",
		"permissions/write",
		"webpush/send",
		// Mints MFA-bypassing recovery codes and replaces the stored
		// authenticator, so it must be granted from settings rather than by a
		// consent dialog the app raises at a moment of its choosing.
		"user/authentication/write",
		"url:*", // Wildcard URL is restricted
	}

	for _, perm := range restricted_perms {
		if !permission_restricted(perm) {
			t.Errorf("permission_restricted(%q) = false, want true (restricted)", perm)
		}
	}
}

// TestAuthenticationWriteHoldersKeepTheirDefaultGrant guards the risk the
// reclassification actually carries. Restricted permissions cannot be granted
// through the in-app consent dialog, so the two apps that legitimately rewrite
// authentication have to arrive holding it: their apps_default entry is what
// carries them across, and losing it would leave TOTP and recovery-code
// management dead with no way for the user to re-grant from the dialog.
func TestAuthenticationWriteHoldersKeepTheirDefaultGrant(t *testing.T) {
	if !permission_restricted("user/authentication/write") {
		t.Fatal("user/authentication/write is not restricted; this test guards the consequences of it being so")
	}

	for _, name := range []string{"Login", "Settings"} {
		found := false
		for _, app := range apps_default {
			if app.Name != name {
				continue
			}
			for _, grant := range app.Permissions {
				if grant.Permission == "user/authentication/write" {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("default app %q has no user/authentication/write grant; with the permission restricted it can no longer obtain one from a consent dialog", name)
		}
	}
}

func TestPermissionRestrictedUnknown(t *testing.T) {
	// Unknown permissions should default to restricted for safety
	if !permission_restricted("unknown/permission") {
		t.Error("permission_restricted(unknown) = false, want true (restricted)")
	}
}

// =============================================================================
// Permission Administrator Tests
// =============================================================================

func TestPermissionAdministrator(t *testing.T) {
	admin_only_perms := []string{
		"users/read",
		"settings/write",
	}

	for _, perm := range admin_only_perms {
		if !permission_administrator(perm) {
			t.Errorf("permission_administrator(%q) = false, want true", perm)
		}
	}

	non_admin_perms := []string{
		"groups/write",
		"url:example.com",
	}

	for _, perm := range non_admin_perms {
		if permission_administrator(perm) {
			t.Errorf("permission_administrator(%q) = true, want false", perm)
		}
	}
}

// =============================================================================
// Permission Split/Join Tests
// =============================================================================

func TestPermissionSplit(t *testing.T) {
	tests := []struct {
		permission  string
		want_name   string
		want_object string
	}{
		{"url:github.com", "url", "github.com"},
		{"url:*", "url", "*"},
		{"groups/write", "groups/write", ""},
		{"users/read", "users/read", ""},
	}

	for _, tt := range tests {
		name, object := permission_split(tt.permission)
		if name != tt.want_name || object != tt.want_object {
			t.Errorf("permission_split(%q) = (%q, %q), want (%q, %q)",
				tt.permission, name, object, tt.want_name, tt.want_object)
		}
	}
}

func TestPermissionJoin(t *testing.T) {
	tests := []struct {
		name   string
		object string
		want   string
	}{
		{"url", "github.com", "url:github.com"},
		{"url", "*", "url:*"},
		{"groups/write", "", "groups/write"},
	}

	for _, tt := range tests {
		got := permission_join(tt.name, tt.object)
		if got != tt.want {
			t.Errorf("permission_join(%q, %q) = %q, want %q",
				tt.name, tt.object, got, tt.want)
		}
	}
}

// =============================================================================
// Domain Extraction Tests
// =============================================================================

func TestDomainExtract(t *testing.T) {
	tests := []struct {
		url        string
		want       string
		want_error bool
	}{
		{"https://github.com/foo/bar", "github.com", false},
		{"https://api.github.com/v1/users", "api.github.com", false},
		{"http://localhost:8080/test", "localhost", false},
		{"https://Example.COM/path", "example.com", false},
		{"ftp://files.example.org", "files.example.org", false},
		{"not-a-url", "", true},
		{"://invalid", "", true},
	}

	for _, tt := range tests {
		got, err := domain_extract(tt.url)
		if (err != nil) != tt.want_error {
			t.Errorf("domain_extract(%q) error = %v, want_err = %v", tt.url, err, tt.want_error)
			continue
		}
		if got != tt.want {
			t.Errorf("domain_extract(%q) = %q, want %q", tt.url, got, tt.want)
		}
	}
}

// =============================================================================
// Domain Matching Tests
// =============================================================================

func TestDomainMatchesExact(t *testing.T) {
	tests := []struct {
		perm_domain    string
		request_domain string
		want           bool
	}{
		// Exact matches
		{"github.com", "github.com", true},
		{"example.org", "example.org", true},

		// Case insensitive
		{"GitHub.COM", "github.com", true},
		{"github.com", "GITHUB.COM", true},

		// Non-matches
		{"github.com", "gitlab.com", false},
		{"api.github.com", "github.com", false},
	}

	for _, tt := range tests {
		got := domain_matches(tt.perm_domain, tt.request_domain)
		if got != tt.want {
			t.Errorf("domain_matches(%q, %q) = %v, want %v",
				tt.perm_domain, tt.request_domain, got, tt.want)
		}
	}
}

func TestDomainMatchesSubdomain(t *testing.T) {
	tests := []struct {
		perm_domain    string
		request_domain string
		want           bool
	}{
		// Subdomain matches
		{"github.com", "api.github.com", true},
		{"github.com", "raw.githubusercontent.com", false}, // Different domain
		{"example.org", "sub.example.org", true},
		{"example.org", "deep.sub.example.org", true},

		// Parent domain should not match child permission
		{"api.github.com", "github.com", false},
	}

	for _, tt := range tests {
		got := domain_matches(tt.perm_domain, tt.request_domain)
		if got != tt.want {
			t.Errorf("domain_matches(%q, %q) = %v, want %v",
				tt.perm_domain, tt.request_domain, got, tt.want)
		}
	}
}

func TestDomainMatchesWildcard(t *testing.T) {
	// Wildcard should match everything
	domains := []string{"github.com", "api.github.com", "example.org", "localhost"}

	for _, domain := range domains {
		if !domain_matches("*", domain) {
			t.Errorf("domain_matches(\"*\", %q) = false, want true", domain)
		}
	}
}

// =============================================================================
// Internal App Bypass Tests
// =============================================================================

func TestAppIsInternal(t *testing.T) {
	external := create_external_app("test-external")
	internal := create_internal_app("test-internal")

	if app_is_internal(external) {
		t.Error("app_is_internal(external) = true, want false")
	}

	if !app_is_internal(internal) {
		t.Error("app_is_internal(internal) = false, want true")
	}

	if app_is_internal(nil) {
		t.Error("app_is_internal(nil) = true, want false")
	}
}

func TestInternalAppBypassesPermissions(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	internal_app := create_internal_app("test-internal")
	thread := create_test_thread(user, internal_app)
	fn := sl.NewBuiltin("test", nil)

	// Internal app should always pass permission checks
	permissions := []string{
		"groups/write",
		"url:example.com",
	}

	for _, perm := range permissions {
		err := require_permission(thread, fn, perm)
		if err != nil {
			t.Errorf("require_permission(%q) for internal app returned error: %v", perm, err)
		}
	}
}

// =============================================================================
// Permission Grant/Revoke Tests
// =============================================================================

func TestPermissionGrantAndCheck(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Initially permission should not be granted
	if permission_granted(user, app_id, "groups/write") {
		t.Error("permission_granted before grant = true, want false")
	}

	// Grant the permission
	permission_grant(user, app_id, "groups/write")

	// Now it should be granted
	if !permission_granted(user, app_id, "groups/write") {
		t.Error("permission_granted after grant = false, want true")
	}
}

func TestPermissionRevoke(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Grant then revoke
	permission_grant(user, app_id, "webpush/send")

	if !permission_granted(user, app_id, "webpush/send") {
		t.Error("permission_granted after grant = false, want true")
	}

	permission_revoke(user, app_id, "webpush/send")

	if permission_granted(user, app_id, "webpush/send") {
		t.Error("permission_granted after revoke = true, want false")
	}
}

// TestPermissionRevokeSurvivesResetup locks the soft-delete behaviour of
// permission_revoke. app_user_setup re-applies an app's default
// permissions with `insert or ignore` whenever the default set changes
// (a server update). When revoke removed the row, a revoked default was
// silently re-granted on the next re-setup; revoke now writes a
// granted=0 row, which insert-or-ignore leaves untouched.
func TestPermissionRevokeSurvivesResetup(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	apps_app_id := "12kqLEaEE9L3mh6modywUmo8TC3JGi3ypPZR2N2KqAMhB3VBFdL"

	// First app access grants the app's default permissions.
	app_user_setup(user, apps_app_id)
	if !permission_granted(user, apps_app_id, "permissions/write") {
		t.Fatal("default permission not granted by app_user_setup")
	}

	// The user revokes one of those defaults.
	permission_revoke(user, apps_app_id, "permissions/write")
	if permission_granted(user, apps_app_id, "permissions/write") {
		t.Fatal("permission still granted after revoke")
	}

	// Simulate a server update changing the default set: clear the
	// recorded setup count so app_user_setup re-runs its grant loop.
	db := db_user(user, "user")
	db.exec("update apps set setup=0 where app=?", apps_app_id)
	app_user_setup(user, apps_app_id)

	// The revoke must survive the re-setup — a removed row would have
	// been re-granted here.
	if permission_granted(user, apps_app_id, "permissions/write") {
		t.Error("revoked default permission was re-granted by app_user_setup re-run")
	}
}

// TestFriendsAndGroupsReadDefaults guards the grants that keep friend-gated
// features working once friends/read and groups/read are enforced. Chat, Chess,
// Go and Words check friendship inside P2P event handlers, where a missing grant
// aborts the handler and drops the event with nothing shown to anyone - so a
// dropped entry here surfaces as messages and moves silently vanishing, not as a
// permission prompt. groups/write does not imply groups/read (permission_granted
// matches exactly), which is why People carries both.
func TestFriendsAndGroupsReadDefaults(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	expected := map[string]string{
		"1PfwgL5rwmRW9HNqX1UNfjubHue7JsbZG8ft3C1fUzxfZT1e92":  "friends/read", // Chat
		"12bMvfv6pVEAVLzBjJuS55oPaZDL3qzoUAtBWB8iK2arTk8GQkr": "friends/read", // Chess
		"12NgqPUqEPpSvh3aNCbn1r5wxHRRzTb8mjb3p4LdYFWoXM6qvJG": "friends/read", // Go
		"12s6o3pyRNvDY6UbpjgidgibnYBKoLhak5mUUM9ZGLDnv6tmETy": "friends/read", // Words
		"1WhnggfLs2d1iXHJ5zVhYFhiSdZibh6UzaoYMH91ZoAXGzj8Cv":  "groups/read",  // CRM
		"12254aHfG39LqrizhydT6iYRCTAZqph1EtAkVTR7DcgXZKWqRrj": "groups/read",  // Feeds
		"12PGVUZUrLqgfqp1ovH8ejfKpAQq6uXbrcCqtoxWHjcuxWDxZbt": "groups/read",  // Forums
		"12cTM7noFHaHkdv3JyWw3Dq9eP8iBaQFveu6JTrvVuuEEH8F8Bg": "groups/read",  // Projects
		"1SWnPXg9xpT2Cxemw2aw8CLZCP5yDatQ6ebF9dHoMTXQNFKLuw":  "groups/read",  // Repositories
		"12QcwPkeTpYmxjaYXtA56ff5jMzJYjMZCmV5RpQR1GosFPRXDtf": "groups/read",  // Wikis
		"1gGcjxdhV2VjuEMLs7UZiQwMaY2jvx1ARbu8g9uqM5QeS2vFJV":  "groups/read",  // People
	}

	user := create_permission_test_user(t, "u1")
	for app, perm := range expected {
		app_user_setup(user, app)
		if !permission_granted(user, app, perm) {
			t.Errorf("default permission %q not granted to app %q by app_user_setup", perm, app)
		}
	}

	// Both are standard, so the request dialog can actually grant them. A
	// restricted permission renders with no Allow button at all, which would
	// dead-end any app asking for either.
	for _, perm := range []string{"friends/read", "groups/read"} {
		if permission_restricted(perm) {
			t.Errorf("%q is restricted; it must be standard so apps can request it", perm)
		}
		if permission_administrator(perm) {
			t.Errorf("%q requires administrator; it must be grantable by an ordinary user", perm)
		}
	}
}

func TestPermissionsList(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Grant multiple permissions
	permission_grant(user, app_id, "groups/write")
	permission_grant(user, app_id, "url:github.com")

	perms := permissions_list(user, app_id, "en")

	if len(perms) < 2 {
		t.Errorf("permissions_list returned %d permissions, want at least 2", len(perms))
	}

	// Check that permissions are in the list
	found := map[string]bool{}
	for _, p := range perms {
		perm := p["permission"].(string)
		found[perm] = true
	}

	expected := []string{"groups/write", "url:github.com"}
	for _, exp := range expected {
		if !found[exp] {
			t.Errorf("permissions_list missing %q", exp)
		}
	}
}

// =============================================================================
// URL Permission Tests
// =============================================================================

func TestURLPermissionExactMatch(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Grant permission for github.com
	permission_grant(user, app_id, "url:github.com")

	// Check exact match
	if !permission_granted(user, app_id, "url:github.com") {
		t.Error("url:github.com not granted after grant")
	}

	// Check non-match
	if permission_granted(user, app_id, "url:gitlab.com") {
		t.Error("url:gitlab.com granted without grant")
	}
}

func TestURLPermissionSubdomainMatch(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Grant permission for github.com
	permission_grant(user, app_id, "url:github.com")

	// Check subdomain match
	if !permission_granted(user, app_id, "url:api.github.com") {
		t.Error("url:api.github.com not granted (subdomain of github.com)")
	}

	if !permission_granted(user, app_id, "url:raw.githubusercontent.com") {
		// This should NOT match - different domain
	}
}

func TestURLPermissionWildcard(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Grant wildcard permission
	permission_grant(user, app_id, "url:*")

	// Should match any domain
	domains := []string{"github.com", "api.github.com", "example.org", "localhost"}
	for _, domain := range domains {
		if !permission_granted(user, app_id, "url:"+domain) {
			t.Errorf("url:* should grant access to %s", domain)
		}
	}
}

// =============================================================================
// require_permission Tests
// =============================================================================

func TestRequirePermissionNoApp(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	// No app set
	fn := sl.NewBuiltin("test", nil)

	err := require_permission(thread, fn, "groups/write")
	if err == nil {
		t.Error("require_permission with no app should return error")
	}
}

func TestRequirePermissionNoUser(t *testing.T) {
	app := create_external_app("test-app")
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	// No user set
	fn := sl.NewBuiltin("test", nil)

	err := require_permission(thread, fn, "groups/write")
	if err == nil {
		t.Error("require_permission with no user should return error")
	}
}

func TestRequirePermissionNotGranted(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// Don't grant any permission
	err := require_permission(thread, fn, "groups/write")
	if err == nil {
		t.Error("require_permission without grant should return error")
	}
}

func TestRequirePermissionGranted(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// Grant the permission
	permission_grant(user, app.id, "groups/write")

	err := require_permission(thread, fn, "groups/write")
	if err != nil {
		t.Errorf("require_permission with grant returned error: %v", err)
	}
}

func TestRequirePermissionAdminOnly(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Test with non-admin user
	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// Grant the permission
	permission_grant(user, app.id, "users/read")

	// Should fail because user is not admin
	err := require_permission(thread, fn, "users/read")
	if err == nil {
		t.Error("require_permission(user/read) for non-admin should return error")
	}

	// Test with admin user
	admin := create_test_admin(t, "u2")
	admin_app := create_external_app("admin-test-app")
	admin_thread := create_test_thread(admin, admin_app)

	// Grant the permission
	permission_grant(admin, admin_app.id, "users/read")

	err = require_permission(admin_thread, fn, "users/read")
	if err != nil {
		t.Errorf("require_permission(user/read) for admin returned error: %v", err)
	}
}

// =============================================================================
// require_permission_url Tests
// =============================================================================

func TestRequirePermissionURL(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// Grant permission for github.com
	permission_grant(user, app.id, "url:github.com")

	// Should work for github.com URLs
	err := require_permission_url(thread, fn, "https://github.com/user/repo")
	if err != nil {
		t.Errorf("require_permission_url for github.com returned error: %v", err)
	}

	// Should work for api.github.com (subdomain)
	err = require_permission_url(thread, fn, "https://api.github.com/users")
	if err != nil {
		t.Errorf("require_permission_url for api.github.com returned error: %v", err)
	}

	// Should fail for other domains
	err = require_permission_url(thread, fn, "https://gitlab.com/user/repo")
	if err == nil {
		t.Error("require_permission_url for gitlab.com should return error")
	}
}

// TestURLPreviewRequiresPermission: fetching a preview is an outbound request
// like any other and needs the same url: grant mochi.url.request does. Only the
// rate limiter stood between an ungranted app and any host it named.
func TestURLPreviewRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("preview-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.url.preview", api_url_preview)

	// .invalid never resolves, so the fetch fails immediately either way and
	// the permission check is the only thing that differs.
	target := sl.Tuple{sl.String("https://host.invalid/page")}

	if _, err := api_url_preview(thread, fn, target, nil); err == nil {
		t.Error("api_url_preview accepted a host with no url: grant")
	}

	// Granted: the check passes. A failed fetch reports itself by returning ""
	// rather than an error, so the absence of an error is the signal here.
	permission_grant(user, app.id, "url:host.invalid")
	if _, err := api_url_preview(thread, fn, target, nil); err != nil {
		t.Errorf("api_url_preview refused a granted host: %v", err)
	}
}

func TestRequirePermissionURLInvalid(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// Invalid URL should return error
	err := require_permission_url(thread, fn, "not-a-valid-url")
	if err == nil {
		t.Error("require_permission_url with invalid URL should return error")
	}
}

// =============================================================================
// Default Permissions Tests
// =============================================================================

func TestDefaultPermissionsLazyGrant(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")

	// Use a known default app ID (Notifications)
	notifications_app_id := "12ZwHwqDLsdN5FMLcHhWBrDwwYojNZ67dWcZiaynNFcjuHPnx2P"

	// Before any check, manually check the database
	db := db_user(user, "user")
	db.apps_setup()
	setup := db.integer("select setup from apps where app=?", notifications_app_id)
	if setup > 0 {
		t.Skip("App already set up, skipping lazy grant test")
	}

	// Run app_user_setup (simulates first access to app)
	app_user_setup(user, notifications_app_id)

	// Check if webpush/send is granted
	granted := permission_granted(user, notifications_app_id, "webpush/send")
	if !granted {
		t.Error("Default permission webpush/send not granted for notifications app")
	}

	// Verify setup timestamp was set
	setup = db.integer("select setup from apps where app=?", notifications_app_id)
	if setup == 0 {
		t.Error("Setup timestamp not set after app_user_setup")
	}
}

func TestDefaultPermissionsSettingsApp(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_test_admin(t, "u1") // Settings needs admin for some permissions

	// Settings app ID
	settings_app_id := "1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky"

	// Run app_user_setup to grant default permissions
	app_user_setup(user, settings_app_id)

	// Check if setting/write is granted (Settings app's default permission)
	granted := permission_granted(user, settings_app_id, "settings/write")
	if !granted {
		t.Error("Default permission setting/write not granted for settings app")
	}
}

func TestDefaultPermissionsMenuApp(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")

	// Menu app ID
	menu_app_id := "121eB4VBoaHhBQuBpwoNN7BVtACiEBHzvRLx1FtoHkKgyLBZQdN"

	// Run app_user_setup to grant default permissions
	app_user_setup(user, menu_app_id)

	// Check if notifications/send is granted (existing default)
	if !permission_granted(user, menu_app_id, "notifications/send") {
		t.Error("Default permission notifications/send not granted for menu app")
	}

	// Check if permission/manage is granted (new default for shell permission dialog)
	if !permission_granted(user, menu_app_id, "permissions/write") {
		t.Error("Default permission permission/manage not granted for menu app")
	}
}

func TestMenuAppCanGrantPermissionViaAPI(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")

	// Menu app with permission/manage
	menu_app_id := "121eB4VBoaHhBQuBpwoNN7BVtACiEBHzvRLx1FtoHkKgyLBZQdN"
	menu_app := create_external_app(menu_app_id)
	thread := create_test_thread(user, menu_app)
	fn := sl.NewBuiltin("mochi.permission.grant", nil)

	// Grant permission/manage to menu app (simulates lazy grant from app_user_setup)
	permission_grant(user, menu_app_id, "permissions/write")

	target_app_id := "feeds-app-id-12345"

	// Menu app should be able to grant standard permissions to other apps
	_, err := api_permission_grant(thread, fn, sl.Tuple{sl.String(target_app_id), sl.String("accounts/read")}, nil)
	if err != nil {
		t.Fatalf("Menu app with permission/manage could not grant permission: %v", err)
	}

	// Verify permission was actually granted
	if !permission_granted(user, target_app_id, "accounts/read") {
		t.Error("Permission account/read not granted to target app")
	}
}

func TestMenuAppCannotGrantRestrictedPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")

	menu_app_id := "121eB4VBoaHhBQuBpwoNN7BVtACiEBHzvRLx1FtoHkKgyLBZQdN"
	menu_app := create_external_app(menu_app_id)
	thread := create_test_thread(user, menu_app)
	fn := sl.NewBuiltin("mochi.permission.level", nil)

	// Verify that restricted/admin permissions are correctly identified
	// — the Starlark menu.star checks `mochi.permission.level(perm)`
	// before calling grant. Both "restricted" and "administrator"
	// levels should block grants from the menu app.
	restricted_perms := []string{"users/read", "settings/write", "url:*"}
	for _, perm := range restricted_perms {
		result, err := api_permission_level(thread, fn, sl.Tuple{sl.String(perm)}, nil)
		if err != nil {
			t.Fatalf("api_permission_level(%q) error: %v", perm, err)
		}
		level := string(result.(sl.String))
		if level == "standard" {
			t.Errorf("api_permission_level(%q) = %q, want restricted or administrator", perm, level)
		}
	}

	// Standard permissions should report "standard"
	standard_perms := []string{"accounts/read", "groups/write", "url:example.com"}
	for _, perm := range standard_perms {
		result, err := api_permission_level(thread, fn, sl.Tuple{sl.String(perm)}, nil)
		if err != nil {
			t.Fatalf("api_permission_level(%q) error: %v", perm, err)
		}
		level := string(result.(sl.String))
		if level != "standard" {
			t.Errorf("api_permission_level(%q) = %q, want standard", perm, level)
		}
	}
}

func TestDefaultPermissionsNonDefaultApp(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "non-default-app-12345"

	// Non-default apps should not get any automatic permissions
	granted := permission_granted(user, app_id, "groups/write")

	if granted {
		t.Error("Non-default app should not have permissions granted automatically")
	}
}

// =============================================================================
// Starlark API Tests
// =============================================================================

func TestAPIPermissionCheck(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)

	// Test mochi.permission.check for non-granted permission
	result, err := api_permission_check(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String("groups/write")}, nil)
	if err != nil {
		t.Fatalf("api_permission_check returned error: %v", err)
	}
	if result != sl.False {
		t.Error("api_permission_check for non-granted permission should return False")
	}

	// Grant the permission
	permission_grant(user, app.id, "groups/write")

	// Test mochi.permission.check for granted permission
	result, err = api_permission_check(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String("groups/write")}, nil)
	if err != nil {
		t.Fatalf("api_permission_check returned error: %v", err)
	}
	if result != sl.True {
		t.Error("api_permission_check for granted permission should return True")
	}
}

func TestAPIPermissionCheckInternalApp(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_internal_app("internal-app")
	thread := create_test_thread(user, app)

	// Internal apps should always return True
	result, err := api_permission_check(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String("any/permission")}, nil)
	if err != nil {
		t.Fatalf("api_permission_check returned error: %v", err)
	}
	if result != sl.True {
		t.Error("api_permission_check for internal app should always return True")
	}
}

// TestAPIPermissionLevel exercises the unified mochi.permission.level
// API that replaced mochi.permission.restricted and
// mochi.permission.administrator (commit 8fb4466). Returns one of
// "standard" / "restricted" / "administrator", with administrator
// strictly stronger than restricted (admin-only permissions report
// "administrator" even though they're also restricted internally).
func TestAPIPermissionLevel(t *testing.T) {
	thread := &sl.Thread{Name: "test"}

	tests := []struct {
		permission string
		want_level string
	}{
		// standard: any user can grant
		{"groups/write", "standard"},
		{"url:example.com", "standard"},
		// restricted: requires user to enable from app settings
		{"webpush/send", "restricted"},
		{"url:*", "restricted"},
		// administrator: admin role required to grant
		{"users/read", "administrator"},
		{"settings/write", "administrator"},
	}

	for _, tt := range tests {
		result, err := api_permission_level(thread, sl.NewBuiltin("test", nil),
			sl.Tuple{sl.String(tt.permission)}, nil)
		if err != nil {
			t.Errorf("api_permission_level(%q) returned error: %v", tt.permission, err)
			continue
		}
		level, ok := result.(sl.String)
		if !ok {
			t.Errorf("api_permission_level(%q) returned %T, want sl.String", tt.permission, result)
			continue
		}
		if string(level) != tt.want_level {
			t.Errorf("api_permission_level(%q) = %q, want %q", tt.permission, level, tt.want_level)
		}
	}
}

func TestAPIPermissionGrantRevoke(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Need to use an app with permission/manage permission (settings app)
	user := create_permission_test_user(t, "u1")
	settings_app := create_external_app("1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky")
	thread := create_test_thread(user, settings_app)

	target_app_id := "target-app-123"

	// Grant permission/manage to settings app first (it's a default)
	permission_grant(user, settings_app.id, "permissions/write")

	// Test granting a permission
	_, err := api_permission_grant(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String(target_app_id), sl.String("groups/write")}, nil)
	if err != nil {
		t.Fatalf("api_permission_grant returned error: %v", err)
	}

	// Verify it was granted
	if !permission_granted(user, target_app_id, "groups/write") {
		t.Error("Permission not granted after api_permission_grant")
	}

	// Test revoking the permission
	_, err = api_permission_revoke(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String(target_app_id), sl.String("groups/write")}, nil)
	if err != nil {
		t.Fatalf("api_permission_revoke returned error: %v", err)
	}

	// Verify it was revoked
	if permission_granted(user, target_app_id, "groups/write") {
		t.Error("Permission still granted after api_permission_revoke")
	}
}

func TestAPIPermissionGrantWithoutManagePermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)

	// Try to grant without permission/manage permission
	_, err := api_permission_grant(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String("target-app"), sl.String("groups/write")}, nil)
	if err == nil {
		t.Error("api_permission_grant without permission/manage should return error")
	}
}

func TestAPIPermissionList(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)

	target_app_id := "target-app-123"

	// Listing another app's grants is a read, so it needs permissions/read -
	// permissions/write is what grant and revoke require.
	permission_grant(user, app.id, "permissions/read")

	// Grant some permissions to target app
	permission_grant(user, target_app_id, "groups/write")
	permission_grant(user, target_app_id, "url:github.com")

	// List permissions
	result, err := api_permission_list(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String(target_app_id)}, nil)
	if err != nil {
		t.Fatalf("api_permission_list returned error: %v", err)
	}

	// Result should be a tuple (sl_encode returns tuple for slices)
	tuple, ok := result.(sl.Tuple)
	if !ok {
		t.Fatalf("api_permission_list returned %T, want starlark.Tuple", result)
	}

	if len(tuple) < 2 {
		t.Errorf("api_permission_list returned %d items, want at least 2", len(tuple))
	}
}

// =============================================================================
// Multi-User Isolation Tests
// =============================================================================

func TestPermissionsUserIsolation(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user1 := create_permission_test_user(t, "u1")
	user2 := create_permission_test_user(t, "u2")
	app_id := "test-app-123"

	// Grant permission to user1
	permission_grant(user1, app_id, "groups/write")

	// User1 should have the permission
	if !permission_granted(user1, app_id, "groups/write") {
		t.Error("User1 should have group/manage permission")
	}

	// User2 should NOT have the permission
	if permission_granted(user2, app_id, "groups/write") {
		t.Error("User2 should NOT have group/manage permission")
	}
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestPermissionNilUser(t *testing.T) {
	// permission_granted with nil user should return false
	if permission_granted(nil, "test-app", "groups/write") {
		t.Error("permission_granted(nil, ...) should return false")
	}

	// permission_grant with nil user should not panic
	permission_grant(nil, "test-app", "groups/write") // Should not panic

	// permission_revoke with nil user should not panic
	permission_revoke(nil, "test-app", "groups/write") // Should not panic

	// permissions_list with nil user should return nil
	perms := permissions_list(nil, "test-app", "en")
	if perms != nil {
		t.Error("permissions_list(nil, ...) should return nil")
	}
}

func TestPermissionEmptyValues(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")

	// Empty app ID should still work (no crash)
	permission_grant(user, "", "groups/write")
	permission_granted(user, "", "groups/write")
	permission_revoke(user, "", "groups/write")

	// Empty permission should still work (no crash)
	permission_grant(user, "test-app", "")
	permission_granted(user, "test-app", "")
	permission_revoke(user, "test-app", "")
}

// =============================================================================
// Permission Idempotency Tests
// =============================================================================

func TestPermissionGrantIdempotent(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Grant the same permission twice - should not error or duplicate
	permission_grant(user, app_id, "groups/write")
	permission_grant(user, app_id, "groups/write")

	// Should still be granted
	if !permission_granted(user, app_id, "groups/write") {
		t.Error("Permission should still be granted after double grant")
	}

	// Check that only one entry exists in the list
	perms := permissions_list(user, app_id, "en")
	count := 0
	for _, p := range perms {
		if p["permission"] == "groups/write" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 group/manage permission, got %d", count)
	}
}

func TestPermissionRevokeNonExistent(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Revoking a permission that was never granted should not panic
	permission_revoke(user, app_id, "never/granted")

	// Should still be not granted
	if permission_granted(user, app_id, "never/granted") {
		t.Error("Permission should not be granted after revoking non-existent")
	}
}

// =============================================================================
// Internal App Bypass for URL and Service Permissions
// =============================================================================

func TestInternalAppBypassURLPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	internal_app := create_internal_app("test-internal")
	thread := create_test_thread(user, internal_app)
	fn := sl.NewBuiltin("test", nil)

	// Internal app should bypass URL permission checks
	urls := []string{
		"https://github.com/test",
		"https://api.example.com/data",
		"https://any-domain.org/path",
	}

	for _, url := range urls {
		err := require_permission_url(thread, fn, url)
		if err != nil {
			t.Errorf("require_permission_url(%q) for internal app returned error: %v", url, err)
		}
	}
}

// =============================================================================
// All Defined Permissions Tests
// =============================================================================

func TestAllDefinedPermissionRestriction(t *testing.T) {
	// Verify all permissions in the permissions slice have correct restriction level
	standard_perms := map[string]bool{
		"groups/write": true,
	}

	restricted_perms := map[string]bool{
		"users/read":        true,
		"settings/write":    true,
		"permissions/write": true,
		"webpush/send":      true,
	}

	for perm := range standard_perms {
		if permission_restricted(perm) {
			t.Errorf("permission_restricted(%q) = true, want false (standard)", perm)
		}
	}

	for perm := range restricted_perms {
		if !permission_restricted(perm) {
			t.Errorf("permission_restricted(%q) = false, want true (restricted)", perm)
		}
	}
}

func TestAllDefinedPermissionAdminFlags(t *testing.T) {
	// Only user/read and setting/write should be admin-only
	admin_only_perms := map[string]bool{
		"users/read":     true,
		"settings/write": true,
	}

	all_perms := []string{
		"groups/write", "users/read", "settings/write",
		"permissions/write", "webpush/send",
	}

	for _, perm := range all_perms {
		is_admin := permission_administrator(perm)
		want_admin := admin_only_perms[perm]
		if is_admin != want_admin {
			t.Errorf("permission_administrator(%q) = %v, want %v", perm, is_admin, want_admin)
		}
	}
}

// =============================================================================
// API Error Cases Tests
// =============================================================================

func TestAPIPermissionCheckWrongArgs(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	fn := sl.NewBuiltin("test", nil)

	// No arguments
	_, err := api_permission_check(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_permission_check with no args should return error")
	}

	// Too many arguments
	_, err = api_permission_check(thread, fn, sl.Tuple{sl.String("a"), sl.String("b")}, nil)
	if err == nil {
		t.Error("api_permission_check with too many args should return error")
	}

	// Wrong type
	_, err = api_permission_check(thread, fn, sl.Tuple{sl.MakeInt(123)}, nil)
	if err == nil {
		t.Error("api_permission_check with wrong type should return error")
	}
}

func TestAPIPermissionGrantWrongArgs(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	settings_app := create_external_app("1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky")
	thread := create_test_thread(user, settings_app)
	fn := sl.NewBuiltin("test", nil)

	permission_grant(user, settings_app.id, "permissions/write")

	// No arguments
	_, err := api_permission_grant(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_permission_grant with no args should return error")
	}

	// Only one argument
	_, err = api_permission_grant(thread, fn, sl.Tuple{sl.String("app")}, nil)
	if err == nil {
		t.Error("api_permission_grant with one arg should return error")
	}
}

func TestAPIPermissionRevokeWrongArgs(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	settings_app := create_external_app("1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky")
	thread := create_test_thread(user, settings_app)
	fn := sl.NewBuiltin("test", nil)

	permission_grant(user, settings_app.id, "permissions/write")

	// No arguments
	_, err := api_permission_revoke(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_permission_revoke with no args should return error")
	}

	// Only one argument
	_, err = api_permission_revoke(thread, fn, sl.Tuple{sl.String("app")}, nil)
	if err == nil {
		t.Error("api_permission_revoke with one arg should return error")
	}
}

func TestAPIPermissionListWrongArgs(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// No arguments
	_, err := api_permission_list(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_permission_list with no args should return error")
	}

	// Wrong type
	_, err = api_permission_list(thread, fn, sl.Tuple{sl.MakeInt(123)}, nil)
	if err == nil {
		t.Error("api_permission_list with wrong type should return error")
	}

	// Empty string
	_, err = api_permission_list(thread, fn, sl.Tuple{sl.String("")}, nil)
	if err == nil {
		t.Error("api_permission_list with empty string should return error")
	}
}

func TestAPIPermissionLevelWrongArgs(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	fn := sl.NewBuiltin("test", nil)

	// No arguments
	_, err := api_permission_level(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_permission_level with no args should return error")
	}
}

// =============================================================================
// Settings App Revocation Protection Test
// =============================================================================

func TestSettingsAppPermissionsManageProtection(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	settings_app_id := "1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky"
	settings_app := create_external_app(settings_app_id)
	thread := create_test_thread(user, settings_app)
	fn := sl.NewBuiltin("test", nil)

	// Grant permission/manage to settings app
	permission_grant(user, settings_app_id, "permissions/write")

	// Try to revoke permission/manage from settings app via API
	// This should fail to prevent lockout
	_, err := api_permission_revoke(thread, fn,
		sl.Tuple{sl.String(settings_app_id), sl.String("permissions/write")}, nil)
	if err == nil {
		t.Error("Should not be able to revoke permission/manage from settings app")
	}

	// Verify it's still granted
	if !permission_granted(user, settings_app_id, "permissions/write") {
		t.Error("permission/manage should still be granted to settings app after failed revoke")
	}
}

// =============================================================================
// Permission String Format Tests
// =============================================================================

func TestPermissionSpecialCharacters(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app_id := "test-app-123"

	// Test various special characters in permission strings
	special_perms := []string{
		"url:example.com/path?query=1",
		"url:example.com:8080",
	}

	for _, perm := range special_perms {
		permission_grant(user, app_id, perm)
		if !permission_granted(user, app_id, perm) {
			t.Errorf("Permission %q should be granted after grant", perm)
		}
		permission_revoke(user, app_id, perm)
		if permission_granted(user, app_id, perm) {
			t.Errorf("Permission %q should not be granted after revoke", perm)
		}
	}
}

// =============================================================================
// Multiple Apps Per User Tests
// =============================================================================

func TestMultipleAppsPerUser(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")

	app1 := "app-one"
	app2 := "app-two"

	// Grant different permissions to different apps
	permission_grant(user, app1, "groups/write")
	permission_grant(user, app2, "webpush/send")

	// Verify app1 has its permission but not app2's
	if !permission_granted(user, app1, "groups/write") {
		t.Error("App1 should have group/manage")
	}
	if permission_granted(user, app1, "webpush/send") {
		t.Error("App1 should NOT have webpush/send")
	}

	// Verify app2 has its permission but not app1's
	if !permission_granted(user, app2, "webpush/send") {
		t.Error("App2 should have webpush/send")
	}
	if permission_granted(user, app2, "groups/write") {
		t.Error("App2 should NOT have group/manage")
	}
}

// =============================================================================
// API Integration Tests - Verify API functions enforce permissions
// =============================================================================

// Helper to verify an API function requires a specific permission
//
//lint:ignore U1000 test scaffolding: an assertion helper for permission-gated APIs
func assert_api_requires_permission(t *testing.T, name string, permission string, api_call func(*sl.Thread, *sl.Builtin) (sl.Value, error)) {
	t.Helper()
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-external-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin(name, nil)

	// Call without permission - should fail
	_, err := api_call(thread, fn)
	if err == nil {
		t.Errorf("%s should require %s permission", name, permission)
	}

	// Grant permission and retry - should succeed (or fail for other reasons)
	permission_grant(user, app.id, permission)
	_, err = api_call(thread, fn)
	// We just check it doesn't fail with permission error
	// It may fail for other reasons (missing args, etc) which is fine
	if err != nil && contains_permission_error(err) {
		t.Errorf("%s should not fail with permission error after grant: %v", name, err)
	}
}

func contains_permission_error(err error) bool {
	if err == nil {
		return false
	}
	error_string := err.Error()
	return contains(error_string, "permission") && (contains(error_string, "denied") || contains(error_string, "required") || contains(error_string, "not granted"))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[:len(substr)] == substr || contains(s[1:], substr)))
}

// --- Group API Permission Tests ---

func TestAPIGroupCreateRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.group.create", nil)

	// Without permission
	_, err := api_group_create(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err == nil {
		t.Error("api_group_create should require group/manage permission")
	}

	// With permission
	permission_grant(user, app.id, "groups/write")
	_, err = api_group_create(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err != nil && contains_permission_error(err) {
		t.Errorf("api_group_create should succeed with permission: %v", err)
	}
}

func TestAPIGroupDeleteRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.group.delete", nil)

	// Without permission
	_, err := api_group_delete(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err == nil {
		t.Error("api_group_delete should require group/manage permission")
	}

	// With permission
	permission_grant(user, app.id, "groups/write")
	_, err = api_group_delete(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err != nil && contains_permission_error(err) {
		t.Errorf("api_group_delete should succeed with permission: %v", err)
	}
}

func TestAPIGroupAddRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.group.add", nil)

	// Without permission
	_, err := api_group_add(thread, fn, sl.Tuple{sl.String("test-group"), sl.String("member")}, nil)
	if err == nil {
		t.Error("api_group_add should require group/manage permission")
	}

	// With permission
	permission_grant(user, app.id, "groups/write")
	_, err = api_group_add(thread, fn, sl.Tuple{sl.String("test-group"), sl.String("member")}, nil)
	if err != nil && contains_permission_error(err) {
		t.Errorf("api_group_add should succeed with permission: %v", err)
	}
}

func TestAPIGroupRemoveRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.group.remove", nil)

	// Without permission
	_, err := api_group_remove(thread, fn, sl.Tuple{sl.String("test-group"), sl.String("member")}, nil)
	if err == nil {
		t.Error("api_group_remove should require group/manage permission")
	}

	// With permission
	permission_grant(user, app.id, "groups/write")
	_, err = api_group_remove(thread, fn, sl.Tuple{sl.String("test-group"), sl.String("member")}, nil)
	if err != nil && contains_permission_error(err) {
		t.Errorf("api_group_remove should succeed with permission: %v", err)
	}
}

// --- User API Permission Tests (admin-only) ---

func TestAPIUserGetIDRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Use admin user since user/read requires admin
	user := create_test_admin(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.user.get.id", nil)

	// Without permission
	_, err := api_user_get_id(thread, fn, sl.Tuple{sl.MakeInt(999)}, nil)
	if err == nil {
		t.Error("api_user_get_id should require user/read permission")
	}
	if !contains_permission_error(err) {
		t.Errorf("api_user_get_id error should mention permission: %v", err)
	}
}

func TestAPIUserListRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Use admin user since user/read requires admin
	user := create_test_admin(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.user.list", nil)

	// Without permission
	_, err := api_user_list(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_user_list should require user/read permission")
	}
	if !contains_permission_error(err) {
		t.Errorf("api_user_list error should mention permission: %v", err)
	}
}

func TestAPIUserCountRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Use admin user since user/read requires admin
	user := create_test_admin(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.user.count", nil)

	// Without permission
	_, err := api_user_count(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_user_count should require user/read permission")
	}
	if !contains_permission_error(err) {
		t.Errorf("api_user_count error should mention permission: %v", err)
	}
}

func TestAPIUserSearchRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Use admin user since user/read requires admin
	user := create_test_admin(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.user.search", nil)

	// Without permission
	_, err := api_user_search(thread, fn, sl.Tuple{sl.String("test")}, nil)
	if err == nil {
		t.Error("api_user_search should require user/read permission")
	}
	if !contains_permission_error(err) {
		t.Errorf("api_user_search error should mention permission: %v", err)
	}
}

// --- User API Non-Admin Test ---

func TestAPIUserReadDeniedForNonAdmin(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Use non-admin user
	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.user.list", nil)

	// Grant permission but still should fail since user is not admin
	permission_grant(user, app.id, "users/read")
	_, err := api_user_list(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_user_list should deny non-admin even with user/read permission")
	}
}

// --- Setting API Permission Tests (admin-only) ---

func TestAPISettingSetRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Use admin user since setting/write requires admin
	user := create_test_admin(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.setting.set", nil)

	// Without permission
	_, err := api_setting_set(thread, fn, sl.Tuple{sl.String("signup_enabled"), sl.String("true")}, nil)
	if err == nil {
		t.Error("api_setting_set should require setting/write permission")
	}
	if !contains_permission_error(err) {
		t.Errorf("api_setting_set error should mention permission: %v", err)
	}
}

func TestAPISettingSetDeniedForNonAdmin(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	// Use non-admin user
	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.setting.set", nil)

	// Grant permission but still should fail since user is not admin
	permission_grant(user, app.id, "settings/write")
	_, err := api_setting_set(thread, fn, sl.Tuple{sl.String("signup_enabled"), sl.String("true")}, nil)
	if err == nil {
		t.Error("api_setting_set should deny non-admin even with setting/write permission")
	}
}

// --- WebPush API Permission Tests ---

func TestAPIWebPushSendRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.webpush.send", nil)

	// Without permission
	_, err := api_webpush_send(thread, fn, sl.Tuple{
		sl.String("https://fcm.googleapis.com/test"),
		sl.String("auth"),
		sl.String("p256dh"),
		sl.String("payload"),
	}, nil)
	if err == nil {
		t.Error("api_webpush_send should require webpush/send permission")
	}
	if !contains_permission_error(err) {
		t.Errorf("api_webpush_send error should mention permission: %v", err)
	}
}

// --- URL Request Permission Tests ---
//
// api_url_request returns a {status, headers, body} response value
// even on permission failure (status 403, no Go-level error). Tests
// inspect the response status, not err.

// expect_url_status asserts that the response from api_url_request has the
// given status. Returns true if the status matched.
func expect_url_status(t *testing.T, result sl.Value, want int64) bool {
	t.Helper()
	d, ok := result.(*sl.Dict)
	if !ok {
		t.Errorf("api_url_request returned %T, want *sl.Dict", result)
		return false
	}
	v, found, _ := d.Get(sl.String("status"))
	if !found {
		t.Error("api_url_request response missing 'status' key")
		return false
	}
	got, ok := v.(sl.Int)
	if !ok {
		t.Errorf("api_url_request response 'status' is %T, want sl.Int", v)
		return false
	}
	got_int, _ := got.Int64()
	if got_int != want {
		t.Errorf("api_url_request response status = %d, want %d", got_int, want)
		return false
	}
	return true
}

func TestAPIURLRequestRequiresPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.url.request", nil)

	// Without permission — response status should be 403.
	result, err := api_url_request(thread, fn, sl.Tuple{sl.String("https://example.com/api")}, nil)
	if err != nil {
		t.Fatalf("api_url_request returned unexpected error: %v", err)
	}
	expect_url_status(t, result, 403)

	// With permission for example.com - permission check passes (may still
	// fail for network reasons, but no longer 403).
	permission_grant(user, app.id, "url:example.com")

	// Should still 403 for different domain without permission.
	result, err = api_url_request(thread, fn, sl.Tuple{sl.String("https://other-domain.com/api")}, nil)
	if err != nil {
		t.Fatalf("api_url_request returned unexpected error: %v", err)
	}
	expect_url_status(t, result, 403)
}

func TestAPIURLRequestSubdomainPermission(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.url.request", nil)

	// Without permission for parent domain - subdomain should 403.
	result, err := api_url_request(thread, fn, sl.Tuple{sl.String("https://api.github.com/users")}, nil)
	if err != nil {
		t.Fatalf("api_url_request returned unexpected error: %v", err)
	}
	expect_url_status(t, result, 403)

	// Grant permission for github.com - subdomain should now pass permission check
	permission_grant(user, app.id, "url:github.com")
	// Note: We don't re-test because it would attempt actual network request
}

// --- Service Call Permission Tests ---

func TestAPIServiceCallPermissionless(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_external_app("test-app")
	thread := create_test_thread(user, app)
	fn := sl.NewBuiltin("mochi.service.call", nil)

	// Services are permissionless - calls may fail for other reasons (service not found, etc.)
	// but should not fail for permission reasons
	_, err := api_service_call(thread, fn, sl.Tuple{sl.String("friends"), sl.String("list")}, nil)
	if err != nil && contains_permission_error(err) {
		t.Errorf("api_service_call should not require permission: %v", err)
	}

	_, err = api_service_call(thread, fn, sl.Tuple{sl.String("notifications"), sl.String("list")}, nil)
	if err != nil && contains_permission_error(err) {
		t.Errorf("api_service_call should not require permission: %v", err)
	}
}

// --- Internal App Bypass Integration Tests ---

func TestInternalAppBypassesAllAPIPermissions(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	user := create_permission_test_user(t, "u1")
	app := create_internal_app("internal-app")
	thread := create_test_thread(user, app)

	// Test group API - should not require permission for internal app
	// Internal apps bypass permission check, so error (if any) should not be permission-related
	fn := sl.NewBuiltin("mochi.group.create", nil)
	_, err := api_group_create(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err != nil && contains_permission_error(err) {
		t.Errorf("Internal app should bypass group/manage permission: %v", err)
	}

	// Note: URL request and service call tests are skipped here because they
	// require full server infrastructure. The permission bypass for internal apps
	// is already tested in TestInternalAppBypassURLPermission and
	// TestInternalAppBypassServicePermission above.
}

// =============================================================================
// Test Setup/Teardown Helpers
// =============================================================================

func setup_test_data_dir(t *testing.T) {
	t.Helper()

	// Use a temporary directory for tests
	tmp_dir, err := os.MkdirTemp("", "mochi-permissions-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Set global data_dir
	data_dir = tmp_dir
}

func cleanup_test_data_dir(t *testing.T) {
	t.Helper()
	if data_dir != "" && data_dir != "/" {
		os.RemoveAll(data_dir)
	}
}

// TestPermissionCatalogAndName verifies that the permission catalog includes
// every enforced permission and that permission_name resolves translated,
// templated names (static, url, service, wildcard) via the core labels.
func TestPermissionCatalogAndName(t *testing.T) {
	load_core_labels()

	// The catalog must list the permissions that are actually enforced,
	// including the two that were previously missing from the slice.
	have := map[string]bool{}
	for _, p := range permissions {
		have[p.Name] = true
	}
	for _, want := range []string{"notifications/send", "user/export", "accounts/read", "permissions/write"} {
		if !have[want] {
			t.Errorf("permissions catalog missing %q", want)
		}
	}

	cases := []struct{ lang, code, want string }{
		{"en", "accounts/read", "Read connected accounts"},
		{"en", "settings/write", "Change system settings"},
		{"fr", "accounts/read", "Lire les comptes connectés"},
		{"en", "url:github.com", "Access github.com"},
		{"en", "url:*", "Access any website"},
		{"en", "service/chat", "Handle chat service"},
	}
	for _, c := range cases {
		got := permission_name(c.lang, c.code)
		if got != c.want {
			t.Errorf("permission_name(%q, %q) = %q, want %q", c.lang, c.code, got, c.want)
		}
	}
}

// =============================================================================
// Repositories Service Permission Tests
// =============================================================================

// The repositories service exposes private source code and, through merge, can
// rewrite branches. Both sides are registered so api_service_call has something
// to enforce, and both are restricted so an app cannot pick them up casually.
func TestPermissionRepositoriesRegistered(t *testing.T) {
	want := map[string]bool{"repositories/read": false, "repositories/write": false}
	for _, p := range permissions {
		if _, ok := want[p.Name]; !ok {
			continue
		}
		want[p.Name] = true
		if !p.Restricted {
			t.Errorf("%s is not restricted; repository content should require deliberate enabling", p.Name)
		}
		if p.AdminOnly {
			t.Errorf("%s is administrator-only; a normal user owns their own repositories", p.Name)
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("%s is not in the permission catalogue, so no name or level resolves for it", name)
		}
	}
}

// Projects calls the repositories service (list, branches, merge check, diff,
// merge) from its merge-request UI, so it must hold both sides by default or
// that integration breaks the moment the app starts requiring them.
func TestPermissionProjectsHoldsRepositoriesDefaults(t *testing.T) {
	defaults := apps_default_get("Projects")
	held := map[string]bool{}
	for _, p := range defaults {
		held[p.Permission] = true
	}
	for _, name := range []string{"repositories/read", "repositories/write"} {
		if !held[name] {
			t.Errorf("Projects default permissions lack %s: %v", name, defaults)
		}
	}
}

// A permission granted by default that no longer exists in the catalogue is
// dead weight, and a typo in either list is invisible until an integration
// fails at runtime. url is excluded: it is a dynamic permission whose object
// carries the domain.
func TestPermissionDefaultsAreRegistered(t *testing.T) {
	registered := map[string]bool{"url": true}
	for _, p := range permissions {
		registered[p.Name] = true
	}
	for _, app := range apps_default {
		for _, p := range app.Permissions {
			if !registered[p.Permission] {
				t.Errorf("app %q is granted %q by default, which is not a registered permission", app.Name, p.Permission)
			}
		}
	}
}

// Every registered permission needs a name a user can read, in English at
// minimum: an unresolved label surfaces the raw key in the permissions UI.
func TestPermissionCatalogueHasNames(t *testing.T) {
	for _, p := range permissions {
		name := permission_name("en", p.Name)
		if name == "" || strings.Contains(name, "permissions.") {
			t.Errorf("permission %q has no English name (got %q)", p.Name, name)
		}
	}
}

// TestUserCloseIsRestrictedAndSettingsHoldsIt guards the gate on
// mochi.user.close, which schedules the account for deletion and revokes every
// session. Before it, the API checked only that a user was present and was not
// an administrator - both facts about the USER, neither about the calling app -
// so any installed app could sign its user out everywhere and start the
// deletion clock. The step-up in apps/settings is that app gating itself, and
// lives in Starlark another app can simply not write.
//
// Restricted rather than standard, matching user/export, which this mirrors:
// a restricted permission cannot be obtained from the in-app consent dialog, so
// closure is granted deliberately or not at all. That makes the apps_default
// entry load-bearing - without it the settings app's own close flow would have
// no way to obtain the permission - which is what the second half checks.
func TestUserCloseIsRestrictedAndSettingsHoldsIt(t *testing.T) {
	if !permission_restricted("user/close") {
		t.Error("user/close is not restricted; an app could obtain it from a consent dialog raised at a moment of its choosing")
	}
	if permission_administrator("user/close") {
		t.Error("user/close requires administrator; closing one's own account is not an administrative act, and administrators are refused it outright")
	}

	found := false
	for _, app := range apps_default {
		if app.Name != "Settings" {
			continue
		}
		for _, grant := range app.Permissions {
			if grant.Permission == "user/close" {
				found = true
			}
		}
	}
	if !found {
		t.Error("the Settings app has no user/close default grant; with the permission restricted its account-closure flow can never obtain one")
	}
}

// TestUserCloseGrantedToNobodyElse: closure is the one destructive account
// operation a user performs on themselves, and exactly one app in the default
// set has a reason to offer it. A second holder would be a second surface.
func TestUserCloseGrantedToNobodyElse(t *testing.T) {
	for _, app := range apps_default {
		if app.Name == "Settings" {
			continue
		}
		for _, grant := range app.Permissions {
			if grant.Permission == "user/close" {
				t.Errorf("default app %q holds user/close; only Settings offers account closure", app.Name)
			}
		}
	}
}

// TestUserCloseGateRefusesUngrantedApp exercises the gate itself rather than
// the registry: the two tests above would both pass while api_user_close
// ignored the permission entirely, which is how it shipped. This one calls the
// builtin the way an app does.
func TestUserCloseGateRefusesUngrantedApp(t *testing.T) {
	defer create_test_users_db(t)()

	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create table permissions (user text not null, app text not null, permission text not null, object text not null default '', granted integer not null default 0, primary key (user, app, permission, object))")
	// create_test_users_db's users table predates closure and has no purge
	// column; user_close writes one, so the "untouched" assertion needs it.
	users.exec("alter table users add column purge integer not null default 0")
	users.exec("insert into users (uid, username, methods) values ('u-close', 'close@example.com', 'email')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-close', '', 'fp-close', 'u-close', 'person', 'Closer')")

	user := user_by_uid("u-close")
	if user == nil {
		t.Fatal("test user did not load")
	}

	call := func(app_id string) error {
		thread := &sl.Thread{Name: "test"}
		thread.SetLocal("user", user)
		thread.SetLocal("app", &App{id: app_id})
		_, err := api_user_close(thread, sl.NewBuiltin("mochi.user.close", api_user_close), nil, nil)
		return err
	}

	// An app with no grant is refused, and refused as a PERMISSION failure -
	// so the shell can tell the user what to grant rather than reporting a
	// generic error.
	err := call("app-without-the-grant")
	if err == nil {
		t.Fatal("an app with no user/close grant closed the account")
	}
	var permission_error *PermissionError
	if !errors.As(err, &permission_error) {
		t.Errorf("error = %v (%T), want a *PermissionError", err, err)
	} else if permission_error.Permission != "user/close" {
		t.Errorf("PermissionError names %q, want user/close", permission_error.Permission)
	}

	// The account is untouched: still active, no purge scheduled.
	row, _ := users.row("select status, purge from users where uid='u-close'")
	if status, _ := row["status"].(string); status != "active" {
		t.Errorf("status = %q after a refused close, want active", status)
	}
	if purge, _ := row["purge"].(int64); purge != 0 {
		t.Errorf("purge = %d after a refused close, want 0", purge)
	}

	// With the grant the gate lets the call through. It fails later, on the
	// email the closure sends, which is not configured here - reaching that is
	// proof the permission check is behind us.
	permission_grant(user, "app-with-the-grant", "user/close")
	if err := call("app-with-the-grant"); err != nil && errors.As(err, &permission_error) {
		t.Errorf("a granted app was still refused by the permission gate: %v", err)
	}
}

// TestUsersWriteGatesEveryWriteAPI: the five APIs that create, update, delete,
// suspend and activate accounts checked only user.administrator(), a fact about
// the user rather than the calling app, while their read siblings had required
// users/read all along. An app an administrator happened to be using could mint
// a second administrator.
func TestUsersWriteGatesEveryWriteAPI(t *testing.T) {
	source, err := os.ReadFile("users.go")
	if err != nil {
		t.Fatalf("read users.go: %v", err)
	}
	text := string(source)

	for _, name := range []string{
		"api_user_create",
		"api_user_update",
		"api_user_delete",
		"api_user_suspend",
		"api_user_activate",
	} {
		t.Run(name, func(t *testing.T) {
			start := strings.Index(text, "func "+name+"(")
			if start < 0 {
				t.Fatalf("%s not found; rename it here too", name)
			}
			end := strings.Index(text[start+1:], "\nfunc ")
			if end < 0 {
				t.Fatalf("could not find the end of %s", name)
			}
			body := text[start : start+1+end]

			if !strings.Contains(body, `require_permission(t, fn, "users/write")`) {
				t.Errorf("does not require users/write; an administrator's role would be spendable by any app they happen to be using")
			}
			if !strings.Contains(body, "administrator()") {
				t.Errorf("no longer checks administrator(); the permission is additional to the role, never a replacement for it")
			}
		})
	}
}

func TestUsersWriteIsRestrictedAndSettingsHoldsIt(t *testing.T) {
	if !permission_restricted("users/write") {
		t.Error("users/write is not restricted; users/read is, and rewriting the user list is not the lesser act")
	}
	if !permission_administrator("users/write") {
		t.Error("users/write is not administrator-only")
	}

	found := false
	for _, app := range apps_default {
		if app.Name != "Settings" {
			continue
		}
		for _, grant := range app.Permissions {
			if grant.Permission == "users/write" {
				found = true
			}
		}
	}
	if !found {
		t.Error("the Settings app has no users/write default grant; with the permission restricted its user administration can never obtain one")
	}
}

// TestUsersWriteGateRefusesUngrantedApp exercises the gate rather than the
// source text, on the API whose refusal matters most.
func TestUsersWriteGateRefusesUngrantedApp(t *testing.T) {
	defer create_test_users_db(t)()

	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create table permissions (app text not null, permission text not null, object text not null default '', granted integer not null default 0, created integer not null default 0, primary key (app, permission, object))")
	users.exec("insert into users (uid, username, role, methods) values ('u-admin', 'admin@example.com', 'administrator', 'email')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-admin', '', 'fp-admin', 'u-admin', 'person', 'Admin')")

	admin := user_by_uid("u-admin")
	if admin == nil || !admin.administrator() {
		t.Fatal("test administrator did not load")
	}

	call := func(app_id string) error {
		thread := &sl.Thread{Name: "test"}
		thread.SetLocal("user", admin)
		thread.SetLocal("app", &App{id: app_id})
		_, err := api_user_create(thread, sl.NewBuiltin("mochi.user.create", api_user_create),
			sl.Tuple{sl.String("victim@example.com"), sl.String("administrator")}, nil)
		return err
	}

	err := call("app-without-the-grant")
	if err == nil {
		t.Fatal("an app with no users/write grant created a user")
	}
	var permission_error *PermissionError
	if !errors.As(err, &permission_error) {
		t.Errorf("error = %v (%T), want a *PermissionError", err, err)
	} else if permission_error.Permission != "users/write" {
		t.Errorf("PermissionError names %q, want users/write", permission_error.Permission)
	}
	if exists, _ := users.exists("select 1 from users where username='victim@example.com'"); exists {
		t.Error("the refused call created the account anyway")
	}
}

// TestTokensCreateIsRestrictedAndMintersHoldIt: mochi.token.create mints a
// bearer credential that authenticates as the user, survives logout (tokens
// live in users.db, sessions_revoke_all clears only sessions.db) and has no
// user-facing management surface. It checked only that a user and an app were
// present on the thread - both "is there a context", neither an authorisation.
func TestTokensCreateIsRestrictedAndMintersHoldIt(t *testing.T) {
	if !permission_restricted("tokens/create") {
		t.Error("tokens/create is not restricted; a durable credential must not be grantable from a consent dialog")
	}
	if permission_administrator("tokens/create") {
		t.Error("tokens/create requires administrator; ordinary users mint RSS and git tokens")
	}

	// Every app that calls mochi.token.create must arrive holding the grant:
	// restricted means it cannot obtain one from the dialog later.
	for _, name := range []string{"Feeds", "Forums", "Notifications", "Repositories", "Wikis"} {
		found := false
		for _, app := range apps_default {
			if app.Name != name {
				continue
			}
			for _, grant := range app.Permissions {
				if grant.Permission == "tokens/create" {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("default app %q mints tokens but has no tokens/create grant", name)
		}
	}
}

// TestTokenExpiryCapped: an unbound token authenticates across the whole app
// and reaches /_/identity, so it gets an outside limit. A bound token is
// confined to one action and lives in a URL a reader stores for years, so the
// cap must not touch it - all nine call sites in the app tree pass expires=0
// with an action, and every one of them would break.
func TestTokenExpiryCapped(t *testing.T) {
	horizon := now() + token_maximum_lifetime

	for _, test := range []struct {
		name    string
		expires int64
		action  string
		want    func(int64) bool
	}{
		{"bound, never expires", 0, ":feed/-/rss", func(got int64) bool { return got == 0 }},
		{"bound, far future", horizon * 10, "-/rss", func(got int64) bool { return got == horizon*10 }},
		{"unbound, never expires", 0, "", func(got int64) bool { return got > now() && got <= horizon }},
		{"unbound, beyond the cap", horizon * 10, "", func(got int64) bool { return got <= horizon }},
		{"unbound, negative", -1, "", func(got int64) bool { return got > now() && got <= horizon }},
		{"unbound, inside the cap", now() + 60, "", func(got int64) bool { return got == now()+60 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := token_expiry_capped(test.expires, test.action); !test.want(got) {
				t.Errorf("token_expiry_capped(%d, %q) = %d, outside the expected range (now %d, cap %d)",
					test.expires, test.action, got, now(), horizon)
			}
		})
	}
}

// TestTokensCreateGateRefusesUngrantedApp exercises the gate rather than the
// registry, and checks no row is written on refusal.
func TestTokensCreateGateRefusesUngrantedApp(t *testing.T) {
	defer create_test_users_db(t)()

	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create table tokens (hash text primary key not null, user text not null, app text not null, name text not null default '', scopes text not null default '', action text not null default '', entity text not null default '', created integer not null, expires integer not null default 0)")
	users.exec("create table permissions (app text not null, permission text not null, object text not null default '', granted integer not null default 0, created integer not null default 0, primary key (app, permission, object))")
	users.exec("insert into users (uid, username, methods) values ('u-mint', 'mint@example.com', 'email')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-mint', '', 'fp-mint', 'u-mint', 'person', 'Minter')")
	db_open("db/sessions.db").exec("create table accesses (hash text primary key not null, user text not null, used integer not null default 0)")

	user := user_by_uid("u-mint")
	if user == nil {
		t.Fatal("test user did not load")
	}

	call := func(app_id string) (sl.Value, error) {
		thread := &sl.Thread{Name: "test"}
		thread.SetLocal("user", user)
		thread.SetLocal("app", &App{id: app_id})
		return api_token_create(thread, sl.NewBuiltin("mochi.token.create", api_token_create),
			sl.Tuple{sl.String("probe")}, nil)
	}

	_, err := call("app-without-the-grant")
	if err == nil {
		t.Fatal("an app with no tokens/create grant minted a token")
	}
	var permission_error *PermissionError
	if !errors.As(err, &permission_error) {
		t.Errorf("error = %v (%T), want a *PermissionError", err, err)
	} else if permission_error.Permission != "tokens/create" {
		t.Errorf("PermissionError names %q, want tokens/create", permission_error.Permission)
	}
	if rows, _ := users.rows("select 1 from tokens"); len(rows) != 0 {
		t.Errorf("the refused call wrote %d token rows, want 0", len(rows))
	}

	// Granted, the mint succeeds and the unbound token is capped rather than
	// stored as never-expiring.
	permission_grant(user, "app-with-the-grant", "tokens/create")
	value, err := call("app-with-the-grant")
	if err != nil {
		t.Fatalf("a granted app was refused: %v", err)
	}
	token, _ := sl.AsString(value)
	if !strings.HasPrefix(token, "mochi-") {
		t.Errorf("returned %q, want a mochi- token", token)
	}
	row, _ := users.row("select expires from tokens where name='probe'")
	if row == nil {
		t.Fatal("no token row written")
	}
	expires, _ := row["expires"].(int64)
	if expires <= now() || expires > now()+token_maximum_lifetime {
		t.Errorf("stored expires = %d; want capped to at most %d", expires, now()+token_maximum_lifetime)
	}
}
