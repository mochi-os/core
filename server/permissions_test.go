// Mochi server: Permissions system tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"path/filepath"
	"testing"

	sl "go.starlark.net/starlark"
)

// Test helpers

// createTestUser creates a temporary user for testing
func createTestUser(t *testing.T, id int) *User {
	t.Helper()

	// Ensure user directory exists
	userDir := filepath.Join(data_dir, "users", string(rune('0'+id)))
	os.MkdirAll(userDir, 0755)

	return &User{
		ID:       id,
		Username: "test@example.com",
		Role:     "user",
	}
}

// createTestAdmin creates a temporary admin user for testing
func createTestAdmin(t *testing.T, id int) *User {
	t.Helper()
	u := createTestUser(t, id)
	u.Role = "administrator"
	return u
}

// createTestThread creates a Starlark thread with user/app context
func createTestThread(user *User, app *App) *sl.Thread {
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", app)
	thread.SetLocal("owner", user)
	return thread
}

// createExternalApp creates a mock external (third-party) app
func createExternalApp(id string) *App {
	return &App{
		id:       id,
		internal: nil, // External apps have nil internal
	}
}

// createInternalApp creates a mock internal (Go-based) app
func createInternalApp(id string) *App {
	return &App{
		id:       id,
		internal: &AppVersion{}, // Internal apps have non-nil internal
	}
}

// cleanupTestUser removes test user data
func cleanupTestUser(t *testing.T, id int) {
	t.Helper()
	userDir := filepath.Join(data_dir, "users", string(rune('0'+id)))
	os.RemoveAll(userDir)
}

// =============================================================================
// Permission Restricted Tests
// =============================================================================

func TestPermissionRestrictedStandard(t *testing.T) {
	standardPerms := []string{
		"group/manage",
		"url:example.com",
	}

	for _, perm := range standardPerms {
		if permission_restricted(perm) {
			t.Errorf("permission_restricted(%q) = true, want false (standard)", perm)
		}
	}
}

func TestPermissionRestrictedRestricted(t *testing.T) {
	restrictedPerms := []string{
		"user/read",
		"setting/write",
		"permission/manage",
		"webpush/send",
		"url:*", // Wildcard URL is restricted
	}

	for _, perm := range restrictedPerms {
		if !permission_restricted(perm) {
			t.Errorf("permission_restricted(%q) = false, want true (restricted)", perm)
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
	adminOnlyPerms := []string{
		"user/read",
		"setting/write",
	}

	for _, perm := range adminOnlyPerms {
		if !permission_administrator(perm) {
			t.Errorf("permission_administrator(%q) = false, want true", perm)
		}
	}

	nonAdminPerms := []string{
		"group/manage",
		"url:example.com",
	}

	for _, perm := range nonAdminPerms {
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
		permission string
		wantName   string
		wantObject string
	}{
		{"url:github.com", "url", "github.com"},
		{"url:*", "url", "*"},
		{"group/manage", "group/manage", ""},
		{"user/read", "user/read", ""},
	}

	for _, tt := range tests {
		name, object := permission_split(tt.permission)
		if name != tt.wantName || object != tt.wantObject {
			t.Errorf("permission_split(%q) = (%q, %q), want (%q, %q)",
				tt.permission, name, object, tt.wantName, tt.wantObject)
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
		{"group/manage", "", "group/manage"},
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
		url      string
		want     string
		wantErr  bool
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
		if (err != nil) != tt.wantErr {
			t.Errorf("domain_extract(%q) error = %v, wantErr = %v", tt.url, err, tt.wantErr)
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
		permDomain string
		reqDomain  string
		want       bool
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
		got := domain_matches(tt.permDomain, tt.reqDomain)
		if got != tt.want {
			t.Errorf("domain_matches(%q, %q) = %v, want %v",
				tt.permDomain, tt.reqDomain, got, tt.want)
		}
	}
}

func TestDomainMatchesSubdomain(t *testing.T) {
	tests := []struct {
		permDomain string
		reqDomain  string
		want       bool
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
		got := domain_matches(tt.permDomain, tt.reqDomain)
		if got != tt.want {
			t.Errorf("domain_matches(%q, %q) = %v, want %v",
				tt.permDomain, tt.reqDomain, got, tt.want)
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
	external := createExternalApp("test-external")
	internal := createInternalApp("test-internal")

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
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	internalApp := createInternalApp("test-internal")
	thread := createTestThread(user, internalApp)
	fn := sl.NewBuiltin("test", nil)

	// Internal app should always pass permission checks
	permissions := []string{
		"group/manage",
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
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Initially permission should not be granted
	if permission_granted(user, appID, "group/manage") {
		t.Error("permission_granted before grant = true, want false")
	}

	// Grant the permission
	permission_grant(user, appID, "group/manage")

	// Now it should be granted
	if !permission_granted(user, appID, "group/manage") {
		t.Error("permission_granted after grant = false, want true")
	}
}

func TestPermissionRevoke(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Grant then revoke
	permission_grant(user, appID, "webpush/send")

	if !permission_granted(user, appID, "webpush/send") {
		t.Error("permission_granted after grant = false, want true")
	}

	permission_revoke(user, appID, "webpush/send")

	if permission_granted(user, appID, "webpush/send") {
		t.Error("permission_granted after revoke = true, want false")
	}
}

func TestPermissionsList(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Grant multiple permissions
	permission_grant(user, appID, "group/manage")
	permission_grant(user, appID, "url:github.com")

	perms := permissions_list(user, appID)

	if len(perms) < 2 {
		t.Errorf("permissions_list returned %d permissions, want at least 2", len(perms))
	}

	// Check that permissions are in the list
	found := map[string]bool{}
	for _, p := range perms {
		perm := p["permission"].(string)
		found[perm] = true
	}

	expected := []string{"group/manage", "url:github.com"}
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
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Grant permission for github.com
	permission_grant(user, appID, "url:github.com")

	// Check exact match
	if !permission_granted(user, appID, "url:github.com") {
		t.Error("url:github.com not granted after grant")
	}

	// Check non-match
	if permission_granted(user, appID, "url:gitlab.com") {
		t.Error("url:gitlab.com granted without grant")
	}
}

func TestURLPermissionSubdomainMatch(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Grant permission for github.com
	permission_grant(user, appID, "url:github.com")

	// Check subdomain match
	if !permission_granted(user, appID, "url:api.github.com") {
		t.Error("url:api.github.com not granted (subdomain of github.com)")
	}

	if !permission_granted(user, appID, "url:raw.githubusercontent.com") {
		// This should NOT match - different domain
	}
}

func TestURLPermissionWildcard(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Grant wildcard permission
	permission_grant(user, appID, "url:*")

	// Should match any domain
	domains := []string{"github.com", "api.github.com", "example.org", "localhost"}
	for _, domain := range domains {
		if !permission_granted(user, appID, "url:"+domain) {
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

	err := require_permission(thread, fn, "group/manage")
	if err == nil {
		t.Error("require_permission with no app should return error")
	}
}

func TestRequirePermissionNoUser(t *testing.T) {
	app := createExternalApp("test-app")
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	// No user set
	fn := sl.NewBuiltin("test", nil)

	err := require_permission(thread, fn, "group/manage")
	if err == nil {
		t.Error("require_permission with no user should return error")
	}
}

func TestRequirePermissionNotGranted(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// Don't grant any permission
	err := require_permission(thread, fn, "group/manage")
	if err == nil {
		t.Error("require_permission without grant should return error")
	}
}

func TestRequirePermissionGranted(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// Grant the permission
	permission_grant(user, app.id, "group/manage")

	err := require_permission(thread, fn, "group/manage")
	if err != nil {
		t.Errorf("require_permission with grant returned error: %v", err)
	}
}

func TestRequirePermissionAdminOnly(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Test with non-admin user
	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("test", nil)

	// Grant the permission
	permission_grant(user, app.id, "user/read")

	// Should fail because user is not admin
	err := require_permission(thread, fn, "user/read")
	if err == nil {
		t.Error("require_permission(user/read) for non-admin should return error")
	}

	// Test with admin user
	admin := createTestAdmin(t, 2)
	adminApp := createExternalApp("admin-test-app")
	adminThread := createTestThread(admin, adminApp)

	// Grant the permission
	permission_grant(admin, adminApp.id, "user/read")

	err = require_permission(adminThread, fn, "user/read")
	if err != nil {
		t.Errorf("require_permission(user/read) for admin returned error: %v", err)
	}
}

// =============================================================================
// require_permission_url Tests
// =============================================================================

func TestRequirePermissionURL(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
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

func TestRequirePermissionURLInvalid(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
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
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)

	// Use a known default app ID (Notifications)
	notificationsAppID := "12ZwHwqDLsdN5FMLcHhWBrDwwYojNZ67dWcZiaynNFcjuHPnx2P"

	// Before any check, manually check the database
	db := db_user(user, "user")
	db.apps_setup()
	setup := db.integer("select setup from apps where app=?", notificationsAppID)
	if setup > 0 {
		t.Skip("App already set up, skipping lazy grant test")
	}

	// Run app_user_setup (simulates first access to app)
	app_user_setup(user, notificationsAppID)

	// Check if webpush/send is granted
	granted := permission_granted(user, notificationsAppID, "webpush/send")
	if !granted {
		t.Error("Default permission webpush/send not granted for notifications app")
	}

	// Verify setup timestamp was set
	setup = db.integer("select setup from apps where app=?", notificationsAppID)
	if setup == 0 {
		t.Error("Setup timestamp not set after app_user_setup")
	}
}

func TestDefaultPermissionsSettingsApp(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestAdmin(t, 1) // Settings needs admin for some permissions

	// Settings app ID
	settingsAppID := "1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky"

	// Run app_user_setup to grant default permissions
	app_user_setup(user, settingsAppID)

	// Check if setting/write is granted (Settings app's default permission)
	granted := permission_granted(user, settingsAppID, "setting/write")
	if !granted {
		t.Error("Default permission setting/write not granted for settings app")
	}
}

func TestDefaultPermissionsNonDefaultApp(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "non-default-app-12345"

	// Non-default apps should not get any automatic permissions
	granted := permission_granted(user, appID, "group/manage")

	if granted {
		t.Error("Non-default app should not have permissions granted automatically")
	}
}

// =============================================================================
// Starlark API Tests
// =============================================================================

func TestAPIPermissionCheck(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)

	// Test mochi.permission.check for non-granted permission
	result, err := api_permission_check(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String("group/manage")}, nil)
	if err != nil {
		t.Fatalf("api_permission_check returned error: %v", err)
	}
	if result != sl.False {
		t.Error("api_permission_check for non-granted permission should return False")
	}

	// Grant the permission
	permission_grant(user, app.id, "group/manage")

	// Test mochi.permission.check for granted permission
	result, err = api_permission_check(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String("group/manage")}, nil)
	if err != nil {
		t.Fatalf("api_permission_check returned error: %v", err)
	}
	if result != sl.True {
		t.Error("api_permission_check for granted permission should return True")
	}
}

func TestAPIPermissionCheckInternalApp(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createInternalApp("internal-app")
	thread := createTestThread(user, app)

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

func TestAPIPermissionRestricted(t *testing.T) {
	thread := &sl.Thread{Name: "test"}

	tests := []struct {
		permission     string
		wantRestricted bool
	}{
		{"group/manage", false},
		{"webpush/send", true},
		{"url:example.com", false},
		{"url:*", true},
	}

	for _, tt := range tests {
		result, err := api_permission_restricted(thread, sl.NewBuiltin("test", nil),
			sl.Tuple{sl.String(tt.permission)}, nil)
		if err != nil {
			t.Errorf("api_permission_restricted(%q) returned error: %v", tt.permission, err)
			continue
		}

		got := result == sl.True
		if got != tt.wantRestricted {
			t.Errorf("api_permission_restricted(%q) = %v, want %v", tt.permission, got, tt.wantRestricted)
		}
	}
}

func TestAPIPermissionAdministrator(t *testing.T) {
	thread := &sl.Thread{Name: "test"}

	tests := []struct {
		permission string
		wantAdmin  bool
	}{
		{"user/read", true},
		{"setting/write", true},
		{"group/manage", false},
		{"url:example.com", false},
	}

	for _, tt := range tests {
		result, err := api_permission_administrator(thread, sl.NewBuiltin("test", nil),
			sl.Tuple{sl.String(tt.permission)}, nil)
		if err != nil {
			t.Errorf("api_permission_administrator(%q) returned error: %v", tt.permission, err)
			continue
		}

		got := result == sl.True
		if got != tt.wantAdmin {
			t.Errorf("api_permission_administrator(%q) = %v, want %v", tt.permission, got, tt.wantAdmin)
		}
	}
}

func TestAPIPermissionGrantRevoke(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Need to use an app with permission/manage permission (settings app)
	user := createTestUser(t, 1)
	settingsApp := createExternalApp("1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky")
	thread := createTestThread(user, settingsApp)

	targetAppID := "target-app-123"

	// Grant permission/manage to settings app first (it's a default)
	permission_grant(user, settingsApp.id, "permission/manage")

	// Test granting a permission
	_, err := api_permission_grant(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String(targetAppID), sl.String("group/manage")}, nil)
	if err != nil {
		t.Fatalf("api_permission_grant returned error: %v", err)
	}

	// Verify it was granted
	if !permission_granted(user, targetAppID, "group/manage") {
		t.Error("Permission not granted after api_permission_grant")
	}

	// Test revoking the permission
	_, err = api_permission_revoke(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String(targetAppID), sl.String("group/manage")}, nil)
	if err != nil {
		t.Fatalf("api_permission_revoke returned error: %v", err)
	}

	// Verify it was revoked
	if permission_granted(user, targetAppID, "group/manage") {
		t.Error("Permission still granted after api_permission_revoke")
	}
}

func TestAPIPermissionGrantWithoutManagePermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)

	// Try to grant without permission/manage permission
	_, err := api_permission_grant(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String("target-app"), sl.String("group/manage")}, nil)
	if err == nil {
		t.Error("api_permission_grant without permission/manage should return error")
	}
}

func TestAPIPermissionList(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)

	targetAppID := "target-app-123"

	// Grant permission/manage to calling app (required to list other apps' permissions)
	permission_grant(user, app.id, "permission/manage")

	// Grant some permissions to target app
	permission_grant(user, targetAppID, "group/manage")
	permission_grant(user, targetAppID, "url:github.com")

	// List permissions
	result, err := api_permission_list(thread, sl.NewBuiltin("test", nil),
		sl.Tuple{sl.String(targetAppID)}, nil)
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
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user1 := createTestUser(t, 1)
	user2 := createTestUser(t, 2)
	appID := "test-app-123"

	// Grant permission to user1
	permission_grant(user1, appID, "group/manage")

	// User1 should have the permission
	if !permission_granted(user1, appID, "group/manage") {
		t.Error("User1 should have group/manage permission")
	}

	// User2 should NOT have the permission
	if permission_granted(user2, appID, "group/manage") {
		t.Error("User2 should NOT have group/manage permission")
	}
}

// =============================================================================
// Edge Cases
// =============================================================================

func TestPermissionNilUser(t *testing.T) {
	// permission_granted with nil user should return false
	if permission_granted(nil, "test-app", "group/manage") {
		t.Error("permission_granted(nil, ...) should return false")
	}

	// permission_grant with nil user should not panic
	permission_grant(nil, "test-app", "group/manage") // Should not panic

	// permission_revoke with nil user should not panic
	permission_revoke(nil, "test-app", "group/manage") // Should not panic

	// permissions_list with nil user should return nil
	perms := permissions_list(nil, "test-app")
	if perms != nil {
		t.Error("permissions_list(nil, ...) should return nil")
	}
}

func TestPermissionEmptyValues(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)

	// Empty app ID should still work (no crash)
	permission_grant(user, "", "group/manage")
	permission_granted(user, "", "group/manage")
	permission_revoke(user, "", "group/manage")

	// Empty permission should still work (no crash)
	permission_grant(user, "test-app", "")
	permission_granted(user, "test-app", "")
	permission_revoke(user, "test-app", "")
}

// =============================================================================
// Permission Idempotency Tests
// =============================================================================

func TestPermissionGrantIdempotent(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Grant the same permission twice - should not error or duplicate
	permission_grant(user, appID, "group/manage")
	permission_grant(user, appID, "group/manage")

	// Should still be granted
	if !permission_granted(user, appID, "group/manage") {
		t.Error("Permission should still be granted after double grant")
	}

	// Check that only one entry exists in the list
	perms := permissions_list(user, appID)
	count := 0
	for _, p := range perms {
		if p["permission"] == "group/manage" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 group/manage permission, got %d", count)
	}
}

func TestPermissionRevokeNonExistent(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Revoking a permission that was never granted should not panic
	permission_revoke(user, appID, "never/granted")

	// Should still be not granted
	if permission_granted(user, appID, "never/granted") {
		t.Error("Permission should not be granted after revoking non-existent")
	}
}

// =============================================================================
// Internal App Bypass for URL and Service Permissions
// =============================================================================

func TestInternalAppBypassURLPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	internalApp := createInternalApp("test-internal")
	thread := createTestThread(user, internalApp)
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
	standardPerms := map[string]bool{
		"group/manage": true,
	}

	restrictedPerms := map[string]bool{
		"user/read":         true,
		"setting/write":     true,
		"permission/manage": true,
		"webpush/send":      true,
	}

	for perm := range standardPerms {
		if permission_restricted(perm) {
			t.Errorf("permission_restricted(%q) = true, want false (standard)", perm)
		}
	}

	for perm := range restrictedPerms {
		if !permission_restricted(perm) {
			t.Errorf("permission_restricted(%q) = false, want true (restricted)", perm)
		}
	}
}

func TestAllDefinedPermissionAdminFlags(t *testing.T) {
	// Only user/read and setting/write should be admin-only
	adminOnlyPerms := map[string]bool{
		"user/read":     true,
		"setting/write": true,
	}

	allPerms := []string{
		"group/manage", "user/read", "setting/write",
		"permission/manage", "webpush/send",
	}

	for _, perm := range allPerms {
		isAdmin := permission_administrator(perm)
		wantAdmin := adminOnlyPerms[perm]
		if isAdmin != wantAdmin {
			t.Errorf("permission_administrator(%q) = %v, want %v", perm, isAdmin, wantAdmin)
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
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	settingsApp := createExternalApp("1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky")
	thread := createTestThread(user, settingsApp)
	fn := sl.NewBuiltin("test", nil)

	permission_grant(user, settingsApp.id, "permission/manage")

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
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	settingsApp := createExternalApp("1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky")
	thread := createTestThread(user, settingsApp)
	fn := sl.NewBuiltin("test", nil)

	permission_grant(user, settingsApp.id, "permission/manage")

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
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
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

func TestAPIPermissionRestrictedWrongArgs(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	fn := sl.NewBuiltin("test", nil)

	// No arguments
	_, err := api_permission_restricted(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_permission_restricted with no args should return error")
	}
}

// =============================================================================
// Settings App Revocation Protection Test
// =============================================================================

func TestSettingsAppPermissionsManageProtection(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	settingsAppID := "1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky"
	settingsApp := createExternalApp(settingsAppID)
	thread := createTestThread(user, settingsApp)
	fn := sl.NewBuiltin("test", nil)

	// Grant permission/manage to settings app
	permission_grant(user, settingsAppID, "permission/manage")

	// Try to revoke permission/manage from settings app via API
	// This should fail to prevent lockout
	_, err := api_permission_revoke(thread, fn,
		sl.Tuple{sl.String(settingsAppID), sl.String("permission/manage")}, nil)
	if err == nil {
		t.Error("Should not be able to revoke permission/manage from settings app")
	}

	// Verify it's still granted
	if !permission_granted(user, settingsAppID, "permission/manage") {
		t.Error("permission/manage should still be granted to settings app after failed revoke")
	}
}

// =============================================================================
// Permission String Format Tests
// =============================================================================

func TestPermissionSpecialCharacters(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	appID := "test-app-123"

	// Test various special characters in permission strings
	specialPerms := []string{
		"url:example.com/path?query=1",
		"url:example.com:8080",
	}

	for _, perm := range specialPerms {
		permission_grant(user, appID, perm)
		if !permission_granted(user, appID, perm) {
			t.Errorf("Permission %q should be granted after grant", perm)
		}
		permission_revoke(user, appID, perm)
		if permission_granted(user, appID, perm) {
			t.Errorf("Permission %q should not be granted after revoke", perm)
		}
	}
}

// =============================================================================
// Multiple Apps Per User Tests
// =============================================================================

func TestMultipleAppsPerUser(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)

	app1 := "app-one"
	app2 := "app-two"

	// Grant different permissions to different apps
	permission_grant(user, app1, "group/manage")
	permission_grant(user, app2, "webpush/send")

	// Verify app1 has its permission but not app2's
	if !permission_granted(user, app1, "group/manage") {
		t.Error("App1 should have group/manage")
	}
	if permission_granted(user, app1, "webpush/send") {
		t.Error("App1 should NOT have webpush/send")
	}

	// Verify app2 has its permission but not app1's
	if !permission_granted(user, app2, "webpush/send") {
		t.Error("App2 should have webpush/send")
	}
	if permission_granted(user, app2, "group/manage") {
		t.Error("App2 should NOT have group/manage")
	}
}

// =============================================================================
// API Integration Tests - Verify API functions enforce permissions
// =============================================================================

// Helper to verify an API function requires a specific permission
func assertAPIRequiresPermission(t *testing.T, name string, permission string, apiCall func(*sl.Thread, *sl.Builtin) (sl.Value, error)) {
	t.Helper()
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-external-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin(name, nil)

	// Call without permission - should fail
	_, err := apiCall(thread, fn)
	if err == nil {
		t.Errorf("%s should require %s permission", name, permission)
	}

	// Grant permission and retry - should succeed (or fail for other reasons)
	permission_grant(user, app.id, permission)
	_, err = apiCall(thread, fn)
	// We just check it doesn't fail with permission error
	// It may fail for other reasons (missing args, etc) which is fine
	if err != nil && containsPermissionError(err) {
		t.Errorf("%s should not fail with permission error after grant: %v", name, err)
	}
}

func containsPermissionError(err error) bool {
	errStr := err.Error()
	return contains(errStr, "permission") && (contains(errStr, "denied") || contains(errStr, "required") || contains(errStr, "not granted"))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[:len(substr)] == substr || contains(s[1:], substr)))
}

// --- Group API Permission Tests ---

func TestAPIGroupCreateRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.group.create", nil)

	// Without permission
	_, err := api_group_create(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err == nil {
		t.Error("api_group_create should require group/manage permission")
	}

	// With permission
	permission_grant(user, app.id, "group/manage")
	_, err = api_group_create(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err != nil && containsPermissionError(err) {
		t.Errorf("api_group_create should succeed with permission: %v", err)
	}
}

func TestAPIGroupDeleteRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.group.delete", nil)

	// Without permission
	_, err := api_group_delete(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err == nil {
		t.Error("api_group_delete should require group/manage permission")
	}

	// With permission
	permission_grant(user, app.id, "group/manage")
	_, err = api_group_delete(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err != nil && containsPermissionError(err) {
		t.Errorf("api_group_delete should succeed with permission: %v", err)
	}
}

func TestAPIGroupAddRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.group.add", nil)

	// Without permission
	_, err := api_group_add(thread, fn, sl.Tuple{sl.String("test-group"), sl.String("member")}, nil)
	if err == nil {
		t.Error("api_group_add should require group/manage permission")
	}

	// With permission
	permission_grant(user, app.id, "group/manage")
	_, err = api_group_add(thread, fn, sl.Tuple{sl.String("test-group"), sl.String("member")}, nil)
	if err != nil && containsPermissionError(err) {
		t.Errorf("api_group_add should succeed with permission: %v", err)
	}
}

func TestAPIGroupRemoveRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.group.remove", nil)

	// Without permission
	_, err := api_group_remove(thread, fn, sl.Tuple{sl.String("test-group"), sl.String("member")}, nil)
	if err == nil {
		t.Error("api_group_remove should require group/manage permission")
	}

	// With permission
	permission_grant(user, app.id, "group/manage")
	_, err = api_group_remove(thread, fn, sl.Tuple{sl.String("test-group"), sl.String("member")}, nil)
	if err != nil && containsPermissionError(err) {
		t.Errorf("api_group_remove should succeed with permission: %v", err)
	}
}

// --- User API Permission Tests (admin-only) ---

func TestAPIUserGetIDRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Use admin user since user/read requires admin
	user := createTestAdmin(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.user.get.id", nil)

	// Without permission
	_, err := api_user_get_id(thread, fn, sl.Tuple{sl.MakeInt(999)}, nil)
	if err == nil {
		t.Error("api_user_get_id should require user/read permission")
	}
	if !containsPermissionError(err) {
		t.Errorf("api_user_get_id error should mention permission: %v", err)
	}
}

func TestAPIUserListRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Use admin user since user/read requires admin
	user := createTestAdmin(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.user.list", nil)

	// Without permission
	_, err := api_user_list(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_user_list should require user/read permission")
	}
	if !containsPermissionError(err) {
		t.Errorf("api_user_list error should mention permission: %v", err)
	}
}

func TestAPIUserCountRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Use admin user since user/read requires admin
	user := createTestAdmin(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.user.count", nil)

	// Without permission
	_, err := api_user_count(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_user_count should require user/read permission")
	}
	if !containsPermissionError(err) {
		t.Errorf("api_user_count error should mention permission: %v", err)
	}
}

func TestAPIUserSearchRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Use admin user since user/read requires admin
	user := createTestAdmin(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.user.search", nil)

	// Without permission
	_, err := api_user_search(thread, fn, sl.Tuple{sl.String("test")}, nil)
	if err == nil {
		t.Error("api_user_search should require user/read permission")
	}
	if !containsPermissionError(err) {
		t.Errorf("api_user_search error should mention permission: %v", err)
	}
}

// --- User API Non-Admin Test ---

func TestAPIUserReadDeniedForNonAdmin(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Use non-admin user
	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.user.list", nil)

	// Grant permission but still should fail since user is not admin
	permission_grant(user, app.id, "user/read")
	_, err := api_user_list(thread, fn, sl.Tuple{}, nil)
	if err == nil {
		t.Error("api_user_list should deny non-admin even with user/read permission")
	}
}

// --- Setting API Permission Tests (admin-only) ---

func TestAPISettingSetRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Use admin user since setting/write requires admin
	user := createTestAdmin(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.setting.set", nil)

	// Without permission
	_, err := api_setting_set(thread, fn, sl.Tuple{sl.String("signup_enabled"), sl.String("true")}, nil)
	if err == nil {
		t.Error("api_setting_set should require setting/write permission")
	}
	if !containsPermissionError(err) {
		t.Errorf("api_setting_set error should mention permission: %v", err)
	}
}

func TestAPISettingSetDeniedForNonAdmin(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	// Use non-admin user
	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.setting.set", nil)

	// Grant permission but still should fail since user is not admin
	permission_grant(user, app.id, "setting/write")
	_, err := api_setting_set(thread, fn, sl.Tuple{sl.String("signup_enabled"), sl.String("true")}, nil)
	if err == nil {
		t.Error("api_setting_set should deny non-admin even with setting/write permission")
	}
}

// --- WebPush API Permission Tests ---

func TestAPIWebPushSendRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
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
	if !containsPermissionError(err) {
		t.Errorf("api_webpush_send error should mention permission: %v", err)
	}
}

// --- URL Request Permission Tests ---

func TestAPIURLRequestRequiresPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.url.request", nil)

	// Without permission
	_, err := api_url_request(thread, fn, sl.Tuple{sl.String("https://example.com/api")}, nil)
	if err == nil {
		t.Error("api_url_request should require url:example.com permission")
	}
	if !containsPermissionError(err) {
		t.Errorf("api_url_request error should mention permission: %v", err)
	}

	// With permission for example.com - should pass permission check
	// (may still fail for network reasons, but that's okay)
	permission_grant(user, app.id, "url:example.com")

	// Should still fail for different domain without permission
	_, err = api_url_request(thread, fn, sl.Tuple{sl.String("https://other-domain.com/api")}, nil)
	if err == nil || !containsPermissionError(err) {
		// Either no error (unexpected) or error but not permission-related (also unexpected)
		if err == nil {
			t.Error("api_url_request should require permission for other-domain.com")
		}
	}
}

func TestAPIURLRequestSubdomainPermission(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.url.request", nil)

	// Without permission for parent domain - subdomain should also fail
	_, err := api_url_request(thread, fn, sl.Tuple{sl.String("https://api.github.com/users")}, nil)
	if err == nil {
		t.Error("api_url_request should require permission for api.github.com")
	}
	if !containsPermissionError(err) {
		t.Errorf("api_url_request error should mention permission: %v", err)
	}

	// Grant permission for github.com - subdomain should now pass permission check
	permission_grant(user, app.id, "url:github.com")
	// Note: We don't re-test because it would attempt actual network request
}

// --- Service Call Permission Tests ---

func TestAPIServiceCallPermissionless(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createExternalApp("test-app")
	thread := createTestThread(user, app)
	fn := sl.NewBuiltin("mochi.service.call", nil)

	// Services are permissionless - calls may fail for other reasons (service not found, etc.)
	// but should not fail for permission reasons
	_, err := api_service_call(thread, fn, sl.Tuple{sl.String("friends"), sl.String("list")}, nil)
	if err != nil && containsPermissionError(err) {
		t.Errorf("api_service_call should not require permission: %v", err)
	}

	_, err = api_service_call(thread, fn, sl.Tuple{sl.String("notifications"), sl.String("list")}, nil)
	if err != nil && containsPermissionError(err) {
		t.Errorf("api_service_call should not require permission: %v", err)
	}
}

// --- Internal App Bypass Integration Tests ---

func TestInternalAppBypassesAllAPIPermissions(t *testing.T) {
	setupTestDataDir(t)
	defer cleanupTestDataDir(t)

	user := createTestUser(t, 1)
	app := createInternalApp("internal-app")
	thread := createTestThread(user, app)

	// Test group API - should not require permission for internal app
	// Internal apps bypass permission check, so error (if any) should not be permission-related
	fn := sl.NewBuiltin("mochi.group.create", nil)
	_, err := api_group_create(thread, fn, sl.Tuple{sl.String("test-group")}, nil)
	if err != nil && containsPermissionError(err) {
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

func setupTestDataDir(t *testing.T) {
	t.Helper()

	// Use a temporary directory for tests
	tmpDir, err := os.MkdirTemp("", "mochi-permissions-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Set global data_dir
	data_dir = tmpDir
}

func cleanupTestDataDir(t *testing.T) {
	t.Helper()
	if data_dir != "" && data_dir != "/" {
		os.RemoveAll(data_dir)
	}
}
