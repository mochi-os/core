// Mochi server: Settings
// Copyright Alistair Cunningham 2024-2025

package main

import (
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Preference stores a user preference key-value pair
type Preference struct {
	Name  string
	Value string
}

// Setting stores a global setting key-value pair
type Setting struct {
	Name  string
	Value string
}

// SystemSetting defines a system setting with validation and access control
type SystemSetting struct {
	Name         string // Setting name
	Pattern      string // Validation pattern for valid()
	Default      string // Default value
	Description  string // Human-readable description
	UserReadable bool   // Whether non-admin users can read this setting
	ReadOnly     bool   // Whether this setting can be modified by anyone
	Public       bool   // Whether this setting can be read without authentication
}

// system_settings defines all available system settings
var system_settings = map[string]SystemSetting{
	"apps_install_user": {
		Name:         "apps_install_user",
		Pattern:      "^(true|false)$",
		Default:      "true",
		Description:  "Whether non-administrators may install and upgrade apps",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_methods_allowed": {
		Name:         "auth_methods_allowed",
		Pattern:      "^[a-z,]+$",
		Default:      "email,passkey,totp,recovery",
		Description:  "Comma-separated list of allowed authentication methods",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_methods_required": {
		Name:         "auth_methods_required",
		Pattern:      "^[a-z,]*$",
		Default:      "",
		Description:  "Methods every user must include (comma-separated, empty = none)",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_passkey_enabled": {
		Name:         "auth_passkey_enabled",
		Pattern:      "^(true|false)$",
		Default:      "true",
		Description:  "Whether passkey authentication is enabled",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_email_enabled": {
		Name:         "auth_email_enabled",
		Pattern:      "^(true|false)$",
		Default:      "true",
		Description:  "Whether email-code authentication is enabled",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_recovery_enabled": {
		Name:         "auth_recovery_enabled",
		Pattern:      "^(true|false)$",
		Default:      "true",
		Description:  "Whether recovery code authentication is enabled",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"domains_verification": {
		Name:         "domains_verification",
		Pattern:      "^(true|false)$",
		Default:      "false",
		Description:  "Whether domains require verification before use",
		UserReadable: false,
		ReadOnly:     false,
	},
	"email_from": {
		Name:         "email_from",
		Pattern:      "email",
		Default:      "mochi-server@localhost",
		Description:  "Email address used as the sender for system emails",
		UserReadable: false,
		ReadOnly:     false,
	},
	"server_started": {
		Name:         "server_started",
		Pattern:      "natural",
		Default:      "",
		Description:  "Server start timestamp",
		UserReadable: true,
		ReadOnly:     true,
	},
	"server_version": {
		Name:         "server_version",
		Pattern:      "line",
		Default:      build_version,
		Description:  "Server version",
		UserReadable: true,
		ReadOnly:     true,
	},
	"signup_enabled": {
		Name:         "signup_enabled",
		Pattern:      "^(true|false)$",
		Default:      "true",
		Description:  "Whether new user signup is enabled",
		UserReadable: false,
		ReadOnly:     false,
		Public:       true,
	},
}

var api_setting = sls.FromStringDict(sl.String("mochi.setting"), sl.StringDict{
	"get":  sl.NewBuiltin("mochi.setting.get", api_setting_get),
	"list": sl.NewBuiltin("mochi.setting.list", api_setting_list),
	"set":  sl.NewBuiltin("mochi.setting.set", api_setting_set),
})

// mochi.setting.get(name) -> string: Get a system setting value
func api_setting_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <name: string>")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid setting name")
	}

	def, exists := system_settings[name]
	if !exists {
		return sl_error(fn, "unknown setting %q", name)
	}

	// Public settings can be read without authentication
	if def.Public {
		value := setting_get(name, def.Default)
		return sl.String(value), nil
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Non-admins can only read user-readable settings
	if !user.administrator() && !def.UserReadable {
		return sl_error(fn, "access denied")
	}

	value := setting_get(name, def.Default)
	return sl.String(value), nil
}

// mochi.setting.set(name, value) -> bool: Set a system setting value (admin only)
func api_setting_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <name: string>, <value: string>")
	}

	name, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid setting name")
	}

	value, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid setting value")
	}

	def, exists := system_settings[name]
	if !exists {
		return sl_error(fn, "unknown setting %q", name)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Only administrators can modify settings
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	// Read-only settings cannot be modified
	if def.ReadOnly {
		return sl_error(fn, "setting %q is read-only", name)
	}

	// Validate the value
	if !valid(value, def.Pattern) {
		return sl_error(fn, "invalid value for setting %q", name)
	}

	setting_set(name, value)
	return sl.True, nil
}

// mochi.setting.list() -> list: List all system settings (admin only)
func api_setting_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Only administrators can list all settings
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	var settings []map[string]any
	for _, def := range system_settings {
		value := setting_get(def.Name, def.Default)
		settings = append(settings, map[string]any{
			"name":          def.Name,
			"value":         value,
			"default":       def.Default,
			"description":   def.Description,
			"pattern":       def.Pattern,
			"user_readable": def.UserReadable,
			"read_only":     def.ReadOnly,
			"public":        def.Public,
		})
	}

	return sl_encode(settings), nil
}

// setting_signup_enabled returns whether new user signup is enabled
func setting_signup_enabled() bool {
	return setting_get("signup_enabled", "true") == "true"
}

// user_preferences_load loads all preferences for a user
func user_preferences_load(u *User) map[string]string {
	prefs := map[string]string{}
	db := db_user(u, "settings")
	rows, err := db.rows("select * from preferences")
	if err != nil {
		return prefs
	}
	for _, row := range rows {
		if name, ok := row["name"].(string); ok {
			if value, ok := row["value"].(string); ok {
				prefs[name] = value
			}
		}
	}
	return prefs
}

// user_preference_get returns a user preference or default
func user_preference_get(u *User, name, def string) string {
	if v, ok := u.Preferences[name]; ok {
		return v
	}
	return def
}

// user_preference_set sets a user preference
func user_preference_set(u *User, name, value string) {
	db := db_user(u, "settings")
	db.exec("replace into preferences (name, value) values (?, ?)", name, value)
	u.Preferences[name] = value
}

// user_preference_delete deletes a user preference, returns true if it existed
func user_preference_delete(u *User, name string) bool {
	if _, ok := u.Preferences[name]; !ok {
		return false
	}
	db := db_user(u, "settings")
	db.exec("delete from preferences where name = ?", name)
	delete(u.Preferences, name)
	return true
}

func setting_get(name string, def string) string {
	var s Setting
	db := db_open("db/settings.db")
	if db.scan(&s, "select * from settings where name=?", name) {
		return s.Value
	}
	return def
}

func setting_set(name string, value string) {
	db := db_open("db/settings.db")
	db.exec("replace into settings ( name, value ) values ( ?, ? )", name, value)
}
