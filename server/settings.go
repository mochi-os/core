// Mochi server: Settings
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Preference stores a user preference key-value pair
type Preference struct {
	Name  string
	Value string
}

// Setting stores a global setting key-value pair.
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
	Secret       bool   // Whether the value is a credential: never returned to a client (read paths report only whether it is set) and masked in the audit log
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
	"auth_email": {
		Name:         "auth_email",
		Pattern:      "^(required|allowed|disabled)$",
		Default:      "allowed",
		Description:  "Email-code login: required (every user must have it), allowed (opt-in), or disabled",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_passkey": {
		Name:         "auth_passkey",
		Pattern:      "^(allowed|disabled)$",
		Default:      "allowed",
		Description:  "Passkey login: allowed (opt-in) or disabled. Cannot be required server-wide — not every user can register one.",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_totp": {
		Name:         "auth_totp",
		Pattern:      "^(allowed|disabled)$",
		Default:      "allowed",
		Description:  "Authenticator app (TOTP) login: allowed (opt-in) or disabled. Cannot be required server-wide — not every user has one set up.",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_recovery": {
		Name:         "auth_recovery",
		Pattern:      "^(allowed|disabled)$",
		Default:      "allowed",
		Description:  "Recovery code login: allowed (fallback) or disabled. Cannot be required — it's a break-glass method.",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"auth_oauth": {
		Name:         "auth_oauth",
		Pattern:      "^(allowed|disabled)$",
		Default:      "allowed",
		Description:  "Third-party (OAuth) login: allowed (opt-in) or disabled. Cannot be required server-wide — not every user links a provider.",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"default_theme": {
		Name:         "default_theme",
		Pattern:      "line",
		Default:      "12sE7AoAuAdWVsMxDPVY3PDM6YXhbwYfytGeDRD1TD49pKAuhno:blue",
		Description:  "Default theme for new users (entity:theme)",
		UserReadable: true,
		ReadOnly:     false,
	},
	"domains_verification": {
		Name:         "domains_verification",
		Pattern:      "^(true|false)$",
		Default:      "true",
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
	"hostname_publish": {
		Name:         "hostname_publish",
		Pattern:      "^(true|false)$",
		Default:      "true",
		Description:  "Whether to announce this server's hostname to other servers",
		UserReadable: false,
		ReadOnly:     false,
	},
	"hostname": {
		Name:         "hostname",
		Pattern:      "^[a-z0-9.-]{1,253}$",
		Default:      "",
		Description:  "Hostname announced to other servers (defaults to the operating system hostname)",
		UserReadable: false,
		ReadOnly:     false,
	},
	"relay": {
		Name:         "relay",
		Pattern:      "^(true|false)$",
		Default:      "true",
		Description:  "Act as a relay for other servers when this server is publicly reachable",
		UserReadable: false,
		ReadOnly:     false,
	},
	"login_app": {
		Name:         "login_app",
		Pattern:      "apppath",
		Default:      "login",
		Description:  "URL path of the app that provides the login experience (landing page and account interstitials)",
		UserReadable: false,
		ReadOnly:     false,
	},
	"operator_name": {
		Name:         "operator_name",
		Pattern:      "line",
		Default:      "",
		Description:  "Operator name shown on the server (data controller for legal purposes)",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"operator_email": {
		Name:         "operator_email",
		Pattern:      "email",
		Default:      "",
		Description:  "Contact email for privacy and legal inquiries",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
	},
	"operator_jurisdiction": {
		Name:         "operator_jurisdiction",
		Pattern:      "line",
		Default:      "",
		Description:  "Jurisdiction whose law applies to this server (e.g. 'England and Wales', 'California, USA')",
		UserReadable: true,
		ReadOnly:     false,
		Public:       true,
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
	"oauth_public_url": {
		Name:         "oauth_public_url",
		Pattern:      "url",
		Default:      "",
		Description:  "Optional public base URL for OAuth redirects (e.g. https://mochi.example.com). Leave empty to derive from request host.",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_google_client_id": {
		Name:         "oauth_google_client_id",
		Pattern:      "line",
		Default:      "",
		Description:  "Google OAuth client ID (set both id and secret to enable Sign in with Google)",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_google_client_secret": {
		Secret:       true,
		Name:         "oauth_google_client_secret",
		Pattern:      "line",
		Default:      "",
		Description:  "Google OAuth client secret",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_github_client_id": {
		Name:         "oauth_github_client_id",
		Pattern:      "line",
		Default:      "",
		Description:  "GitHub OAuth client ID (set both id and secret to enable Sign in with GitHub)",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_github_client_secret": {
		Secret:       true,
		Name:         "oauth_github_client_secret",
		Pattern:      "line",
		Default:      "",
		Description:  "GitHub OAuth client secret",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_microsoft_client_id": {
		Name:         "oauth_microsoft_client_id",
		Pattern:      "line",
		Default:      "",
		Description:  "Microsoft OAuth client ID (set both id and secret to enable Sign in with Microsoft)",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_microsoft_client_secret": {
		Secret:       true,
		Name:         "oauth_microsoft_client_secret",
		Pattern:      "line",
		Default:      "",
		Description:  "Microsoft OAuth client secret",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_microsoft_tenant": {
		Name:         "oauth_microsoft_tenant",
		Pattern:      "^[A-Za-z0-9-]{1,64}$",
		Default:      "common",
		Description:  "Microsoft tenant: 'common' (any Microsoft account), 'organizations', 'consumers', or a specific tenant GUID",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_facebook_client_id": {
		Name:         "oauth_facebook_client_id",
		Pattern:      "line",
		Default:      "",
		Description:  "Facebook OAuth App ID (set both id and secret to enable Sign in with Facebook; requires Meta App Review for the email permission)",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_facebook_client_secret": {
		Secret:       true,
		Name:         "oauth_facebook_client_secret",
		Pattern:      "line",
		Default:      "",
		Description:  "Facebook OAuth App Secret",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_x_client_id": {
		Name:         "oauth_x_client_id",
		Pattern:      "line",
		Default:      "",
		Description:  "X OAuth client ID (set both id and secret to enable Sign in with X; the X app must have the users.email scope enabled)",
		UserReadable: false,
		ReadOnly:     false,
	},
	"oauth_x_client_secret": {
		Secret:       true,
		Name:         "oauth_x_client_secret",
		Pattern:      "line",
		Default:      "",
		Description:  "X OAuth client secret",
		UserReadable: false,
		ReadOnly:     false,
	},
	"fcm.firebase_config": {
		Name:         "fcm.firebase_config",
		Pattern:      "text",
		Default:      "",
		Description:  "google-services.json downloaded from Firebase Console (Project settings → General → Your apps). Public-by-design — exposed to the Android client by /notifications/-/push/setup. Setting this enables FCM as the primary push transport; leave empty to fall back to UnifiedPush.",
		UserReadable: true,
		Public:       true,
		ReadOnly:     false,
	},
	"fcm.service_account": {
		Secret:       true,
		Name:         "fcm.service_account",
		Pattern:      "text",
		Default:      "",
		Description:  "Firebase private key JSON downloaded from Firebase Console (Project settings → Service accounts → Generate new private key). Used by the Mochi server to send pushes via FCM HTTP v1. Secret — anyone with it can send pushes to any token registered against the same Firebase project.",
		UserReadable: false,
		ReadOnly:     false,
	},
	"help_users_forum": {
		Name:         "help_users_forum",
		Pattern:      "entity",
		Default:      "",
		Description:  "Forum entity ID for Help app user submissions (Introduce yourself / Ask a question). Leave empty to use the default canonical Mochi forum.",
		UserReadable: true,
		ReadOnly:     false,
	},
	"help_dev_project": {
		Name:         "help_dev_project",
		Pattern:      "entity",
		Default:      "",
		Description:  "Project entity ID for Help app developer submissions (Report a bug / Suggest a feature). Leave empty to use the default canonical Mochi project.",
		UserReadable: true,
		ReadOnly:     false,
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

	// Credentials are write-only; never hand the stored value to a caller.
	if def.Secret {
		return sl.String(""), nil
	}

	value := setting_get(name, def.Default)
	return sl.String(value), nil
}

// mochi.setting.set(name, value) -> bool: Set a system setting value (admin only)
func api_setting_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Check settings/write permission
	if err := require_permission(t, fn, "settings/write"); err != nil {
		return sl_error(fn, "%v", err)
	}

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

	// Validate the value. The default is always accepted so clearable
	// settings (e.g. empty-default OAuth credentials) can be reset.
	if value != def.Default && !valid(value, def.Pattern) {
		return sl_error(fn, "invalid value for setting %q", name)
	}

	setting_set(name, value)
	// Announce hostname changes immediately rather than waiting for the
	// hourly publish.
	if name == "hostname" || name == "hostname_publish" {
		peers_publish_request()
	}
	// Start or stop the relay service to match the new opt-out state.
	if name == "relay" {
		relay_service_update()
	}
	// Never write a credential's value into the audit log.
	audit_value := value
	if def.Secret {
		audit_value = "***"
	}
	audit_settings_changed(user.Username, name, audit_value)
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
		// Credentials are write-only: report only whether a value is set, never
		// the value itself, so it can't be read back through the admin UI.
		set := value != ""
		if def.Secret {
			value = ""
		}
		settings = append(settings, map[string]any{
			"name":          def.Name,
			"value":         value,
			"set":           set,
			"secret":        def.Secret,
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

// reg_preferences is the user.db preferences register: a (name → value) versioned
// LWW-Register so a preference (language, theme, …) the user changes on one host
// converges on every host of the account.
var reg_preferences = upsert_def{"preferences", []string{"name"}, []string{"value"}}

// user_preferences_load loads all preferences for a user
func user_preferences_load(u *User) map[string]string {
	prefs := map[string]string{}
	db := db_user(u, "user")
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
	for _, obsolete := range []string{"style_preset", "border_radius"} {
		if _, exists := prefs[obsolete]; exists {
			// Local hard-delete, not a replicated tombstone: these are deprecated
			// keys no longer written by any code (so nothing can resurrect them, and
			// each host cleans its own copy on load), and user_preferences_load is
			// reachable from the replication_emit_to package-var initializer, so a
			// replicated write here would form an initialisation cycle.
			db.exec("delete from preferences where name = ?", obsolete)
			delete(prefs, obsolete)
		}
	}
	return prefs
}

// user_preference_get returns a user preference or default
func user_preference_get(u *User, name, def string) string {
	if u == nil {
		return def
	}
	if v, ok := u.Preferences[name]; ok {
		return v
	}
	return def
}

// user_preference_set sets a user preference.
//
// Uses exec_replicated, not exec: preferences (language, theme,
// timezone, …) are account-global, not host-local — the user expects a
// preference changed on one host of their account to take effect on
// every host. The user.db handle is tagged db_kind_user_core, so the
// replicated write fans the statement out to the account's host set,
// the same path mochi.access.* / mochi.group.* writes already use.
// (Caught 2026-05-21: language changed on mochi1 didn't reach mochi2.)
func user_preference_set(u *User, name, value string) {
	db := db_user(u, "user")
	db.row_write(reg_preferences, map[string]any{"name": name, "value": value})
	if u.Preferences == nil {
		u.Preferences = map[string]string{}
	}
	u.Preferences[name] = value
}

// user_preference_delete deletes a user preference, returns true if it
// existed. Replicated for the same reason as user_preference_set —
// clearing a preference must converge across the account's hosts.
func user_preference_delete(u *User, name string) bool {
	if _, ok := u.Preferences[name]; !ok {
		return false
	}
	db := db_user(u, "user")
	db.row_remove(reg_preferences, map[string]any{"name": name})
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

// setting_local reports whether a setting is host-LOCAL — per-host state that
// must never replicate to (or be accepted from) a pair member. `schema` is the
// critical one: it is each host's own DB migration progress, so replicating it
// makes the host that deploys SECOND adopt the first's bumped version and SKIP
// its own migrations (#75 — the production primary silently skipped two
// migrations). `server_started` is likewise per-host. Gates both the emit
// (setting_set) and the apply (replication_system_set_apply_settings).
func setting_local(name string) bool {
	switch name {
	case "schema", "server_started":
		return true
	}
	// replica.join.* is THIS host's in-flight pair-join state (which source it is
	// joining, the poll state/reason). It is meaningless on another member and no
	// request-serving path reads it, so it must not replicate — otherwise an
	// already-paired host initiating a join fans its join-state to existing
	// members (who then show a phantom in-flight join), and setting_delete never
	// emitting leaves those phantoms forever (#150).
	if strings.HasPrefix(name, "replica.join.") {
		return true
	}
	return false
}

func setting_set(name string, value string) {
	db := db_open("db/settings.db")
	db.exec("replace into settings ( name, value ) values ( ?, ? )", name, value)
	if !setting_local(name) {
	}
}

// setting_delete removes a setting row entirely. Distinguished from
// setting_set(name, "") which leaves an explicit empty row. Used by
// callers that want subsequent setting_get to return the default
// rather than an empty string.
func setting_delete(name string) {
	db := db_open("db/settings.db")
	db.exec("delete from settings where name=?", name)
}
