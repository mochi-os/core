// Mochi server: Settings and preferences unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"testing"
)

// Helper to create a test user with preferences
func create_test_user(t *testing.T) (*User, func()) {
	tmp_dir, err := os.MkdirTemp("", "mochi_settings_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	orig_data_dir := data_dir
	data_dir = tmp_dir

	// Create a test user
	user := &User{
		ID:          1,
		Username:    "testuser",
		Role:        "user",
		Preferences: map[string]string{},
	}

	cleanup := func() {
		data_dir = orig_data_dir
		os.RemoveAll(tmp_dir)
	}

	return user, cleanup
}

// Test db_user creates database in user directory
func TestDbUser(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	db := db_user(user, "settings")
	if db == nil {
		t.Fatal("db_user should return a database")
	}

	// Verify preferences table was created
	exists, err := db.exists("SELECT name FROM sqlite_master WHERE type='table' AND name='preferences'")
	if err != nil {
		t.Fatalf("exists query failed: %v", err)
	}
	if !exists {
		t.Error("preferences table should exist in settings.db")
	}
}

// Test db_user with non-settings database
func TestDbUserOtherDb(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	db := db_user(user, "other")
	if db == nil {
		t.Fatal("db_user should return a database")
	}

	// Verify preferences table was NOT created for non-settings db
	exists, _ := db.exists("SELECT name FROM sqlite_master WHERE type='table' AND name='preferences'")
	if exists {
		t.Error("preferences table should not exist in non-settings database")
	}
}

// Test user_preferences_load with empty database
func TestUserPreferencesLoadEmpty(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	prefs := user_preferences_load(user)
	if prefs == nil {
		t.Fatal("user_preferences_load should return a map")
	}
	if len(prefs) != 0 {
		t.Errorf("prefs should be empty, got %d items", len(prefs))
	}
}

// Test user_preferences_load with data
func TestUserPreferencesLoadWithData(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	// Insert some preferences directly
	db := db_user(user, "settings")
	db.exec("INSERT INTO preferences (name, value) VALUES ('theme', 'dark')")
	db.exec("INSERT INTO preferences (name, value) VALUES ('language', 'de')")
	db.exec("INSERT INTO preferences (name, value) VALUES ('timezone', 'Europe/Berlin')")

	prefs := user_preferences_load(user)
	if len(prefs) != 3 {
		t.Errorf("prefs should have 3 items, got %d", len(prefs))
	}
	if prefs["theme"] != "dark" {
		t.Errorf("prefs['theme'] = %q, want 'dark'", prefs["theme"])
	}
	if prefs["language"] != "de" {
		t.Errorf("prefs['language'] = %q, want 'de'", prefs["language"])
	}
	if prefs["timezone"] != "Europe/Berlin" {
		t.Errorf("prefs['timezone'] = %q, want 'Europe/Berlin'", prefs["timezone"])
	}
}

// Test user_preference_get with existing preference
func TestUserPreferenceGetExisting(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	user.Preferences = map[string]string{
		"theme":    "dark",
		"language": "fr",
	}

	result := user_preference_get(user, "theme", "light")
	if result != "dark" {
		t.Errorf("user_preference_get('theme') = %q, want 'dark'", result)
	}

	result = user_preference_get(user, "language", "en")
	if result != "fr" {
		t.Errorf("user_preference_get('language') = %q, want 'fr'", result)
	}
}

// Test user_preference_get with default
func TestUserPreferenceGetDefault(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	user.Preferences = map[string]string{}

	result := user_preference_get(user, "theme", "light")
	if result != "light" {
		t.Errorf("user_preference_get('theme') = %q, want 'light' (default)", result)
	}

	result = user_preference_get(user, "timezone", "UTC")
	if result != "UTC" {
		t.Errorf("user_preference_get('timezone') = %q, want 'UTC' (default)", result)
	}
}

// Test user_preference_set creates new preference
func TestUserPreferenceSetNew(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	user.Preferences = map[string]string{}

	user_preference_set(user, "theme", "dark")

	// Check in-memory map was updated
	if user.Preferences["theme"] != "dark" {
		t.Errorf("user.Preferences['theme'] = %q, want 'dark'", user.Preferences["theme"])
	}

	// Check database was updated
	db := db_user(user, "settings")
	row, _ := db.row("SELECT value FROM preferences WHERE name = ?", "theme")
	if row == nil || row["value"] != "dark" {
		t.Error("preference should be persisted to database")
	}
}

// Test user_preference_set updates existing preference
func TestUserPreferenceSetUpdate(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	user.Preferences = map[string]string{"theme": "light"}

	// Insert initial value
	db := db_user(user, "settings")
	db.exec("INSERT INTO preferences (name, value) VALUES ('theme', 'light')")

	// Update it
	user_preference_set(user, "theme", "dark")

	// Check in-memory map was updated
	if user.Preferences["theme"] != "dark" {
		t.Errorf("user.Preferences['theme'] = %q, want 'dark'", user.Preferences["theme"])
	}

	// Check database was updated
	row, _ := db.row("SELECT value FROM preferences WHERE name = ?", "theme")
	if row == nil || row["value"] != "dark" {
		t.Error("preference should be updated in database")
	}

	// Ensure only one row exists
	rows, _ := db.rows("SELECT * FROM preferences WHERE name = ?", "theme")
	if len(rows) != 1 {
		t.Errorf("should have exactly 1 row, got %d", len(rows))
	}
}

// Test user_preference_set with multiple preferences
func TestUserPreferenceSetMultiple(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	user.Preferences = map[string]string{}

	user_preference_set(user, "theme", "dark")
	user_preference_set(user, "language", "de")
	user_preference_set(user, "timezone", "Europe/Berlin")

	if len(user.Preferences) != 3 {
		t.Errorf("should have 3 preferences, got %d", len(user.Preferences))
	}

	// Verify all persisted
	db := db_user(user, "settings")
	rows, _ := db.rows("SELECT * FROM preferences ORDER BY name")
	if len(rows) != 3 {
		t.Errorf("should have 3 rows in database, got %d", len(rows))
	}
}

// Test preferences survive reload
func TestUserPreferencesPersistence(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	user.Preferences = map[string]string{}

	// Set some preferences
	user_preference_set(user, "theme", "dark")
	user_preference_set(user, "language", "ja")

	// Simulate reload by creating new user with same ID
	user2 := &User{
		ID:       user.ID,
		Username: user.Username,
		Role:     user.Role,
	}
	user2.Preferences = user_preferences_load(user2)

	if user2.Preferences["theme"] != "dark" {
		t.Errorf("reloaded theme = %q, want 'dark'", user2.Preferences["theme"])
	}
	if user2.Preferences["language"] != "ja" {
		t.Errorf("reloaded language = %q, want 'ja'", user2.Preferences["language"])
	}
}

// Test user_preference_delete
func TestUserPreferenceDelete(t *testing.T) {
	user, cleanup := create_test_user(t)
	defer cleanup()

	user.Preferences = map[string]string{}

	// Set a preference
	user_preference_set(user, "to_delete", "value")
	if user.Preferences["to_delete"] != "value" {
		t.Error("preference should be set")
	}

	// Delete it
	deleted := user_preference_delete(user, "to_delete")
	if !deleted {
		t.Error("delete should return true for existing preference")
	}

	// Check in-memory map
	if _, ok := user.Preferences["to_delete"]; ok {
		t.Error("preference should be removed from map")
	}

	// Check database
	db := db_user(user, "settings")
	exists, _ := db.exists("SELECT 1 FROM preferences WHERE name = ?", "to_delete")
	if exists {
		t.Error("preference should be removed from database")
	}

	// Delete again should return false
	deleted = user_preference_delete(user, "to_delete")
	if deleted {
		t.Error("delete should return false for non-existent preference")
	}
}

// Test global setting_get function (existing functionality)
func TestSettingGet(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_settings_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	// Create settings table
	db := db_open("db/settings.db")
	db.exec("CREATE TABLE settings (name TEXT PRIMARY KEY, value TEXT NOT NULL)")
	db.exec("INSERT INTO settings (name, value) VALUES ('test_key', 'test_value')")

	result := setting_get("test_key", "default")
	if result != "test_value" {
		t.Errorf("setting_get('test_key') = %q, want 'test_value'", result)
	}

	result = setting_get("nonexistent", "default")
	if result != "default" {
		t.Errorf("setting_get('nonexistent') = %q, want 'default'", result)
	}
}

// Test global setting_set function (existing functionality)
func TestSettingSet(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_settings_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	// Create settings table
	db := db_open("db/settings.db")
	db.exec("CREATE TABLE settings (name TEXT PRIMARY KEY, value TEXT NOT NULL)")

	setting_set("new_key", "new_value")

	result := setting_get("new_key", "")
	if result != "new_value" {
		t.Errorf("setting_get after set = %q, want 'new_value'", result)
	}

	// Update existing
	setting_set("new_key", "updated")
	result = setting_get("new_key", "")
	if result != "updated" {
		t.Errorf("setting_get after update = %q, want 'updated'", result)
	}
}

// Test system_settings map has required fields
func TestSystemSettingsDefinitions(t *testing.T) {
	required_settings := []string{
		"server_started",
		"server_version",
		"signup_enabled",
		"signup_invite_required",
		"site_maintenance_message",
	}

	for _, name := range required_settings {
		def, exists := system_settings[name]
		if !exists {
			t.Errorf("system_settings missing %q", name)
			continue
		}
		if def.Name != name {
			t.Errorf("system_settings[%q].Name = %q, want %q", name, def.Name, name)
		}
		if def.Pattern == "" {
			t.Errorf("system_settings[%q].Pattern is empty", name)
		}
		if def.Description == "" {
			t.Errorf("system_settings[%q].Description is empty", name)
		}
	}
}

// Test read-only settings are marked correctly
func TestSystemSettingsReadOnly(t *testing.T) {
	read_only := []string{"server_version", "server_started"}
	editable := []string{"signup_enabled"}

	for _, name := range read_only {
		def := system_settings[name]
		if !def.ReadOnly {
			t.Errorf("system_settings[%q].ReadOnly should be true", name)
		}
	}

	for _, name := range editable {
		def := system_settings[name]
		if def.ReadOnly {
			t.Errorf("system_settings[%q].ReadOnly should be false", name)
		}
	}
}

// Test user-readable settings are marked correctly
func TestSystemSettingsUserReadable(t *testing.T) {
	user_readable := []string{"server_version", "server_started", "site_maintenance_message"}
	admin_only := []string{"signup_enabled", "signup_invite_required"}

	for _, name := range user_readable {
		def := system_settings[name]
		if !def.UserReadable {
			t.Errorf("system_settings[%q].UserReadable should be true", name)
		}
	}

	for _, name := range admin_only {
		def := system_settings[name]
		if def.UserReadable {
			t.Errorf("system_settings[%q].UserReadable should be false", name)
		}
	}
}

// Test setting_signup_enabled helper
func TestSettingSignupEnabled(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_settings_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/settings.db")
	db.exec("CREATE TABLE settings (name TEXT PRIMARY KEY, value TEXT NOT NULL)")

	// Default should be true
	if !setting_signup_enabled() {
		t.Error("setting_signup_enabled() default should be true")
	}

	// Set to false
	setting_set("signup_enabled", "false")
	if setting_signup_enabled() {
		t.Error("setting_signup_enabled() should be false after setting to 'false'")
	}

	// Set to true
	setting_set("signup_enabled", "true")
	if !setting_signup_enabled() {
		t.Error("setting_signup_enabled() should be true after setting to 'true'")
	}
}

// Test setting_site_maintenance_message helper
func TestSettingSiteMaintenanceMessage(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_settings_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/settings.db")
	db.exec("CREATE TABLE settings (name TEXT PRIMARY KEY, value TEXT NOT NULL)")

	// Default should be empty (not in maintenance)
	if setting_site_maintenance_message() != "" {
		t.Errorf("setting_site_maintenance_message() default = %q, want empty", setting_site_maintenance_message())
	}

	// Set maintenance message
	setting_set("site_maintenance_message", "Site is down for maintenance")
	if setting_site_maintenance_message() != "Site is down for maintenance" {
		t.Errorf("setting_site_maintenance_message() = %q, want 'Site is down for maintenance'", setting_site_maintenance_message())
	}

	// Clear maintenance (set to empty)
	setting_set("site_maintenance_message", "")
	if setting_site_maintenance_message() != "" {
		t.Errorf("setting_site_maintenance_message() after clear = %q, want empty", setting_site_maintenance_message())
	}
}

// Test validation patterns are valid for each setting
func TestSystemSettingsValidation(t *testing.T) {
	tests := []struct {
		name      string
		valid     []string
		invalid   []string
	}{
		{
			name:    "signup_enabled",
			valid:   []string{"true", "false"},
			invalid: []string{"yes", "no", "1", "0", "TRUE"},
		},
	}

	for _, tc := range tests {
		def := system_settings[tc.name]
		for _, v := range tc.valid {
			if !valid(v, def.Pattern) {
				t.Errorf("system_settings[%q] pattern should accept %q", tc.name, v)
			}
		}
		for _, v := range tc.invalid {
			if valid(v, def.Pattern) {
				t.Errorf("system_settings[%q] pattern should reject %q", tc.name, v)
			}
		}
	}
}
