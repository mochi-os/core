// Mochi server: Apps
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tailscale/hujson"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

type App struct {
	id          string
	fingerprint string
	versions    map[string]*AppVersion
	latest      *AppVersion // Highest installed version (external apps)
	internal    *AppVersion // Single version for internal Go apps
}

type AppAction struct {
	Function  string `json:"function"`
	File      string `json:"file"`
	Files     string `json:"files"`
	Feature   string `json:"feature"`
	Cache     string `json:"cache"`
	Public    bool   `json:"public"`
	OpenGraph string `json:"opengraph"` // Starlark function to generate Open Graph meta tags

	name              string            `json:"-"`
	internal_function func(*Action)     `json:"-"`
	segments          int               `json:"-"`
	literals          int               `json:"-"`
	parameters        map[string]string `json:"-"`
	filepath          string            `json:"-"` // For files routes: the file path suffix after the matched pattern
}

type AppEvent struct {
	Function          string       `json:"function"`
	Anonymous         bool         `json:"anonymous"`
	Apps              []any        `json:"apps,omitempty"`
	Services          []string     `json:"services,omitempty"`
	internal_function func(*Event) `json:"-"`
}

// AppError is one entry in an app's `errors` block: the handler the
// framework calls when a core-originated failure event (the fixed
// catalogue in error_event.go) matches. Lean by design — unlike AppEvent
// it carries no access-control fields, because the sender is always core.
type AppError struct {
	Function string `json:"function"`
}

type AppFunction struct {
	Function   string `json:"function"`
	Permission string `json:"permission,omitempty"`
}

// AppTheme is one theme entry in an app's `themes` array. The bundled
// themes live in apps/themes/app.json but any installed app may ship its
// own. IconMask, IconBackground, and Icons drive future icon-pack
// support (apps.go:2110): a theme can ship a CSS mask + tile background
// applied to every home-screen icon, and an Icons map that points to
// alternative icon files (served from the theme app's /icons/ folder)
// keyed by the target app's path. No installed theme exercises these
// fields yet; the resolution code path is in place so a "Brutalist" or
// "Material" theme can plug in without a server change.
type AppTheme struct {
	ID             string            `json:"id"`
	Label          string            `json:"label"`
	Hue            float64           `json:"hue"`
	Chroma         float64           `json:"chroma"`
	HueBG          float64           `json:"hue_bg"`
	BorderRadius   string            `json:"border_radius"`
	Spacing        string            `json:"spacing"`
	FontSans       string            `json:"font_sans"`
	FontMono       string            `json:"font_mono"`
	IconMask       string            `json:"icon_mask"`
	IconBackground string            `json:"icon_background"`
	Background     string            `json:"background"`
	BackgroundDark string            `json:"background_dark"`
	Overrides      map[string]string `json:"overrides"`
	Icons          map[string]string `json:"icons"`
}

type AppVersion struct {
	Version  string   `json:"version"`
	Label    string   `json:"label"`
	Classes  []string `json:"classes"`
	Paths    []string `json:"paths"`
	Services []string `json:"services"`
	Require  struct {
		Role    string `json:"role"`
		Version struct {
			Minimum string `json:"minimum"`
			Maximum string `json:"maximum"`
		} `json:"version"`
	} `json:"require"`
	Architecture struct {
		Engine  string `json:"engine"`
		Version int    `json:"version"`
	} `json:"architecture"`
	Execute  []string `json:"execute"`
	Database struct {
		Schema int    `json:"schema"`
		File   string `json:"file"`
		Create struct {
			Function string `json:"function"`
		} `json:"create"`
		Upgrade struct {
			Function string `json:"function"`
		} `json:"upgrade"`
		Downgrade struct {
			Function string `json:"function"`
		} `json:"downgrade"`
		// Replicate controls which writes to this app's per-user DB
		// fan out to the user's other hosts. Default is opt-out: every
		// INSERT/UPDATE/DELETE replays on every replica. Apps list
		// whole caches / local-only TABLES in
		// `database.replicate.exclude.tables` to keep them off the wire,
		// and host-LOCAL COLUMNS (computed scores, per-host timestamps)
		// inside otherwise-replicated tables in
		// `database.replicate.exclude.columns` (table -> column names).
		// The content-convergence audit derives its exclude-set from both
		// (see claude/plans/audit-host-local-columns.md), so a column
		// whose value legitimately differs per host isn't read as
		// divergence. See claude/plans/replication.md.
		Replicate struct {
			Exclude struct {
				Tables  []string            `json:"tables"`
				Columns map[string][]string `json:"columns"`
			} `json:"exclude"`
		} `json:"replicate"`
		create_function func(*DB) `json:"-"`
	} `json:"database"`
	Icon         string                 `json:"icon"`
	IconSymbolic string                 `json:"icon_symbolic"`
	Icons        []Icon                 `json:"icons"`
	Actions      map[string]AppAction   `json:"actions"`
	Events       map[string]AppEvent    `json:"events"`
	Errors       map[string]AppError    `json:"errors"`
	Functions    map[string]AppFunction `json:"functions"`
	// Commit.Function is the name of a Starlark function the framework
	// invokes after any committed write to this app's per-user DB —
	// both local commits and replication replays. Apps move WebSocket
	// emission and other "after the row lands" work here so remote-host
	// writes still reach local subscribers. See pattern 1.6 in
	// claude/plans/replication.md. Handlers MUST be idempotent.
	Commit struct {
		Function string `json:"function"`
	} `json:"commit,omitempty"`
	Themes []AppTheme `json:"themes"`
	// ThemeIcons lets an app declare per-theme icon variants of itself,
	// keyed by namespaced theme id ("<app_id>:<theme_id>"). Counterpart
	// to AppTheme.Icons — see apps.go:2110 for the resolution priority.
	ThemeIcons map[string]string `json:"theme_icons"`
	Publisher  struct {
		Peer string `json:"peer,omitempty"`
	} `json:"publisher,omitempty"`

	app              *App                         `json:"-"`
	base             string                       `json:"-"`
	labels           map[string]map[string]string `json:"-"`
	starlark_once    sync.Once                    `json:"-"`
	starlark_globals sl.StringDict                `json:"-"`
	app_json_mtime   time.Time                    `json:"-"`
}

type Icon struct {
	Action string `json:"action"`
	Label  string `json:"label"`
	File   string `json:"file"`
}

// Get the primary icon path for the app
func (av *AppVersion) icon() string {
	if av.Icon != "" {
		return av.Icon
	}
	for _, i := range av.Icons {
		if i.Action == "" {
			return i.File
		}
	}
	return ""
}

// Get the primary path for URL generation
func (a *App) url_path(user *User) string {
	if av := a.active(user); av != nil && len(av.Paths) > 0 {
		return av.Paths[0]
	}
	return a.id
}

// default_version returns the system default version and track for this app
func (a *App) default_version() (version, track string) {
	db := db_apps()
	row, _ := db.row("select version, track from versions where app = ?", a.id)
	if row == nil {
		return "", ""
	}
	return row["version"].(string), row["track"].(string)
}

// set_default_version sets the system default version or track for this app
// admin should be passed when called from admin API, empty for system operations
func (a *App) set_default_version(version, track, admin string) {
	db := db_apps()
	if version == "" && track == "" {
		db.exec("delete from versions where app = ?", a.id)
	} else {
		db.exec("replace into versions (app, version, track) values (?, ?, ?)", a.id, version, track)
	}
	if admin != "" {
		audit_default_version_changed(admin, a.id, version, track)
	}
	resolution_invalidate() // system default version changed
}

// track returns the version for a named track, or empty string if not set
func (a *App) track(name string) string {
	db := db_apps()
	row, _ := db.row("select version from tracks where app = ? and track = ?", a.id, name)
	if row == nil {
		return ""
	}
	return row["version"].(string)
}

// set_track sets the version for a named track
// admin should be passed when called from admin API, empty for system operations
func (a *App) set_track(name, version, admin string) {
	db := db_apps()
	if version == "" {
		db.exec("delete from tracks where app = ? and track = ?", a.id, name)
	} else {
		db.exec("replace into tracks (app, track, version) values (?, ?, ?)", a.id, name, version)
	}
	if admin != "" {
		audit_default_track_changed(admin, a.id, name, version)
	}
	resolution_invalidate() // system track changed
}

// tracks returns all tracks for this app as a map of track name to version
func (a *App) tracks() map[string]string {
	db := db_apps()
	rows, _ := db.rows("select track, version from tracks where app = ?", a.id)
	result := make(map[string]string)
	for _, row := range rows {
		result[row["track"].(string)] = row["version"].(string)
	}
	return result
}

// active resolves which version a user should see for this app.
// Resolution order:
// 1. User's preference (if user is not nil)
// 2. System default (from apps.db)
// 3. Highest installed version (fallback)
// If a track is specified, it is resolved to a version.
// Note: For anonymous entity access, pass the entity owner as the user.
func (a *App) active(user *User) *AppVersion {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	return a.active_locked(user)
}

// active_locked is the internal version of active.
// Must be called with apps_lock held.
func (a *App) active_locked(user *User) *AppVersion {
	// Internal Go apps have a single version — free to resolve, never
	// cached.
	if a.internal != nil {
		return a.internal
	}

	// The resolution below runs uncached SQL (per-user preference, system
	// default, track lookup); on the per-event routing path that cost
	// dominates. Serve from the version cache when fresh.
	key := resolution_key{resolution_user_key(user), a.id}
	if av, ok := resolution_version_get(key); ok {
		return av
	}
	av := a.resolve_active_locked(user)
	resolution_version_put(key, av)
	return av
}

// resolve_active_locked resolves the active version from its inputs,
// without consulting the cache. Must be called with apps_lock held.
func (a *App) resolve_active_locked(user *User) *AppVersion {
	// 1. Check user's preference
	if user != nil {
		version, track := user.app_version(a.id)
		if av := a.resolve_version(version, track); av != nil {
			return av
		}
	}

	// 2. Check system default
	version, track := a.default_version()
	if av := a.resolve_version(version, track); av != nil {
		return av
	}

	// 3. Fallback to highest installed version
	return a.latest
}

// resolve_version resolves a version or track to an AppVersion.
// Must be called with apps_lock held.
func (a *App) resolve_version(version, track string) *AppVersion {
	// If a track is specified, try to resolve it to a version
	if track != "" {
		if tv := a.track(track); tv != "" {
			version = tv
		}
		// If track lookup fails, fall through to use the version parameter
	}

	if version == "" {
		return nil
	}

	// Look up the version in the versions map
	if av, found := a.versions[version]; found {
		return av
	}

	return nil
}

// Check if user meets the app's requirements
func (av *AppVersion) user_allowed(user *User) bool {
	if av.Require.Role == "" {
		return true
	}
	if user == nil {
		return false
	}
	return user.Role == av.Require.Role
}

const (
	app_version_minimum = 2
	app_version_maximum = 4
)

// version_greater returns true if version a is greater than version b
// Versions are compared numerically by splitting on "." (e.g., "1.11" > "1.9")
func version_greater(a, b string) bool {
	parts_a := strings.Split(a, ".")
	parts_b := strings.Split(b, ".")
	for i := 0; i < len(parts_a) || i < len(parts_b); i++ {
		var num_a, num_b int
		if i < len(parts_a) {
			num_a, _ = strconv.Atoi(parts_a[i])
		}
		if i < len(parts_b) {
			num_b, _ = strconv.Atoi(parts_b[i])
		}
		if num_a > num_b {
			return true
		}
		if num_a < num_b {
			return false
		}
	}
	return false
}

// DefaultApp defines a default app and its permissions
type DefaultApp struct {
	ID          string
	Name        string
	Permissions []struct{ Permission, Object string }
}

var (
	// Default apps to install. Login, Menu, and Home must be first three (bootstrap depends on it), then alphabetical.
	apps_default = []DefaultApp{
		{"1FLjnMyW4ozYZhNMqkXTWYgjcoHA7Wif3B3UeAe45chxWnuP1F", "Login", []struct{ Permission, Object string }{
			{"user/authentication/read", ""},
			{"user/authentication/write", ""},
		}},
		{"121eB4VBoaHhBQuBpwoNN7BVtACiEBHzvRLx1FtoHkKgyLBZQdN", "Menu", []struct{ Permission, Object string }{
			{"notifications/send", ""},
			{"permissions/manage", ""},
		}},
		{"12YGtmNxgihPn2cmNSpKfpViFWtWH25xYT7o6xKnTXCA2deNvjH", "Home", nil},
		{"12kqLEaEE9L3mh6modywUmo8TC3JGi3ypPZR2N2KqAMhB3VBFdL", "Apps", []struct{ Permission, Object string }{
			{"permissions/manage", ""},
		}},
		{"1PfwgL5rwmRW9HNqX1UNfjubHue7JsbZG8ft3C1fUzxfZT1e92", "Chat", nil},
		{"12bMvfv6pVEAVLzBjJuS55oPaZDL3qzoUAtBWB8iK2arTk8GQkr", "Chess", nil},
		{"1sfEACmTnQhBVgquGhaCs8Jw4SXKF9XY2apnUwJ63duq2QSxh5", "Comptroller", []struct{ Permission, Object string }{
			{"url", "api.stripe.com"},
			{"url", "connect.stripe.com"},
			{"accounts/ai", ""},
		}},
		{"1WhnggfLs2d1iXHJ5zVhYFhiSdZibh6UzaoYMH91ZoAXGzj8Cv", "CRM", nil},
		{"test", "Test", []struct{ Permission, Object string }{
			{"groups/manage", ""},
			{"notifications/send", ""},
			{"settings/write", ""},
			{"users/read", ""},
		}},
		{"12254aHfG39LqrizhydT6iYRCTAZqph1EtAkVTR7DcgXZKWqRrj", "Feeds", []struct{ Permission, Object string }{
			{"accounts/ai", ""},
			{"accounts/read", ""},
			{"interests/read", ""},
			{"interests/write", ""},
		}},
		{"12PGVUZUrLqgfqp1ovH8ejfKpAQq6uXbrcCqtoxWHjcuxWDxZbt", "Forums", []struct{ Permission, Object string }{
			{"accounts/ai", ""},
			{"accounts/read", ""},
			{"interests/read", ""},
			{"interests/write", ""},
		}},
		{"12NgqPUqEPpSvh3aNCbn1r5wxHRRzTb8mjb3p4LdYFWoXM6qvJG", "Go", nil},
		{"17Qx3vcsBJ6RcMhshTKfVSBPigPZUAaA52KkpCi4ZYFaekSgrY", "Help", nil},
		{"12Erusc4s59DJjqmDZXwPQ15ny4RKrRKFJg2DfAmi2unDGaghgq", "Market", nil},
		{"12ZwHwqDLsdN5FMLcHhWBrDwwYojNZ67dWcZiaynNFcjuHPnx2P", "Notifications", []struct{ Permission, Object string }{
			{"webpush/send", ""},
			{"accounts/read", ""},
			{"accounts/manage", ""},
			{"accounts/notify", ""},
		}},
		{"1gGcjxdhV2VjuEMLs7UZiQwMaY2jvx1ARbu8g9uqM5QeS2vFJV", "People", []struct{ Permission, Object string }{
			{"groups/manage", ""},
			{"user/identity/write", ""},
			{"users/read", ""},
		}},
		{"12cTM7noFHaHkdv3JyWw3Dq9eP8iBaQFveu6JTrvVuuEEH8F8Bg", "Projects", nil},
		{"12nG95Lzt5SbKcmAqweB3vEWcz6oXUd7i9vf3nCXfBxuyqG9wJ3", "Publisher", nil},
		{"1SWnPXg9xpT2Cxemw2aw8CLZCP5yDatQ6ebF9dHoMTXQNFKLuw", "Repositories", nil},
		{"1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky", "Settings", []struct{ Permission, Object string }{
			{"settings/write", ""},
			{"users/read", ""},
			{"accounts/read", ""},
			{"accounts/manage", ""},
			{"interests/read", ""},
			{"interests/write", ""},
			{"notifications/send", ""},
			{"user/authentication/read", ""},
			{"user/authentication/write", ""},
			{"user/export", ""},
			{"user/identity/write", ""},
			{"user/sessions/read", ""},
			{"user/sessions/write", ""},
		}},
		{"12sE7AoAuAdWVsMxDPVY3PDM6YXhbwYfytGeDRD1TD49pKAuhno", "Themes", nil},
		{"12QcwPkeTpYmxjaYXtA56ff5jMzJYjMZCmV5RpQR1GosFPRXDtf", "Wikis", nil},
		{"12s6o3pyRNvDY6UbpjgidgibnYBKoLhak5mUUM9ZGLDnv6tmETy", "Words", nil},
	}
	apps_bootstrap_ready = false // True once Login and Home are installed
	apps                 = map[string]*App{}
	apps_lock            = &sync.Mutex{}

	// internal_services maps a core service name (replication, directory,
	// peers) to its built-in handler. Populated once at startup when an
	// internal app calls service(); never written afterwards. Lets core
	// P2P traffic resolve directly (see app_for_service) instead of
	// scanning every installed app on each event.
	internal_services = map[string]*App{}

	api_app_package = sls.FromStringDict(sl.String("mochi.app.package"), sl.StringDict{
		"get":     sl.NewBuiltin("mochi.app.package.get", api_app_package_get),
		"install": sl.NewBuiltin("mochi.app.package.install", api_app_package_install),
	})

	api_app_class = sls.FromStringDict(sl.String("mochi.app.class"), sl.StringDict{
		"get":    sl.NewBuiltin("mochi.app.class.get", api_app_class_get),
		"set":    sl.NewBuiltin("mochi.app.class.set", api_app_class_set),
		"delete": sl.NewBuiltin("mochi.app.class.delete", api_app_class_delete),
		"list":   sl.NewBuiltin("mochi.app.class.list", api_app_class_list),
	})

	api_app_service = sls.FromStringDict(sl.String("mochi.app.service"), sl.StringDict{
		"get":    sl.NewBuiltin("mochi.app.service.get", api_app_service_get),
		"set":    sl.NewBuiltin("mochi.app.service.set", api_app_service_set),
		"delete": sl.NewBuiltin("mochi.app.service.delete", api_app_service_delete),
		"list":   sl.NewBuiltin("mochi.app.service.list", api_app_service_list),
	})

	api_app_path = sls.FromStringDict(sl.String("mochi.app.path"), sl.StringDict{
		"get":    sl.NewBuiltin("mochi.app.path.get", api_app_path_get),
		"set":    sl.NewBuiltin("mochi.app.path.set", api_app_path_set),
		"delete": sl.NewBuiltin("mochi.app.path.delete", api_app_path_delete),
		"list":   sl.NewBuiltin("mochi.app.path.list", api_app_path_list),
	})

	api_app_version = sls.FromStringDict(sl.String("mochi.app.version"), sl.StringDict{
		"download": sl.NewBuiltin("mochi.app.version.download", api_app_version_download),
		"get":      sl.NewBuiltin("mochi.app.version.get", api_app_version_get),
		"list":     sl.NewBuiltin("mochi.app.version.list", api_app_version_list),
		"set":      sl.NewBuiltin("mochi.app.version.set", api_app_version_set),
	})

	api_app_track = sls.FromStringDict(sl.String("mochi.app.track"), sl.StringDict{
		"list": sl.NewBuiltin("mochi.app.track.list", api_app_track_list),
		"set":  sl.NewBuiltin("mochi.app.track.set", api_app_track_set),
	})

	api_app_asset = sls.FromStringDict(sl.String("mochi.app.asset"), sl.StringDict{
		"exists": sl.NewBuiltin("mochi.app.asset.exists", api_app_asset_exists),
		"list":   sl.NewBuiltin("mochi.app.asset.list", api_app_asset_list),
		"read":   sl.NewBuiltin("mochi.app.asset.read", api_app_asset_read),
	})

	api_app = sls.FromStringDict(sl.String("mochi.app"), sl.StringDict{
		"asset":   api_app_asset,
		"class":   api_app_class,
		"cleanup": sl.NewBuiltin("mochi.app.cleanup", api_app_cleanup),
		"get":     sl.NewBuiltin("mochi.app.get", api_app_get),
		"icons":   sl.NewBuiltin("mochi.app.icons", api_app_icons),
		"label":   sl.NewBuiltin("mochi.app.label", api_app_label),
		"list":    sl.NewBuiltin("mochi.app.list", api_app_list),
		"package": api_app_package,
		"path":    api_app_path,
		"presets": sl.NewBuiltin("mochi.app.presets", api_app_presets),
		"service": api_app_service,
		"themes":  sl.NewBuiltin("mochi.app.themes", api_app_themes),
		"track":   api_app_track,
		"version": api_app_version,
	})
)

// Get existing app, loading it into memory as new internal app if necessary
func app(id string) *App {
	apps_lock.Lock()
	a, found := apps[id]
	apps_lock.Unlock()

	if !found {
		a = &App{id: id, fingerprint: fingerprint(id), versions: make(map[string]*AppVersion)}
		a.internal = &AppVersion{}
		a.internal.app = a
		a.internal.Actions = make(map[string]AppAction)
		a.internal.Events = make(map[string]AppEvent)

		apps_lock.Lock()
		apps[id] = a
		apps_lock.Unlock()
	}

	return a
}

// Get existing external app, or create a new one for loading Starlark apps
func app_external(id string) *App {
	apps_lock.Lock()
	a, found := apps[id]
	apps_lock.Unlock()

	if !found {
		a = &App{id: id, fingerprint: fingerprint(id), versions: make(map[string]*AppVersion)}

		apps_lock.Lock()
		apps[id] = a
		apps_lock.Unlock()
	}

	return a
}

// Get an app by id, fingerprint, or path
func app_by_any(user *User, s string) *App {
	if s == "" {
		return nil
	}

	// Check for id
	apps_lock.Lock()
	a, ok := apps[s]
	apps_lock.Unlock()
	if ok {
		return a
	}

	fp := fingerprint_no_hyphens(s)
	apps_lock.Lock()
	defer apps_lock.Unlock()
	for _, a := range apps {
		av := a.active_locked(user)
		if av == nil {
			continue
		}

		// Check for fingerprint, with or without hyphens
		if fingerprint_no_hyphens(a.fingerprint) == fp {
			return a
		}

		// Check for path
		for _, p := range av.Paths {
			if p == s {
				return a
			}
		}
	}

	return nil
}

// Get the app that handles root path
func app_by_root(user *User) *App {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	for _, a := range apps {
		av := a.active_locked(user)
		if av == nil {
			continue
		}
		for _, p := range av.Paths {
			if p == "" {
				return a
			}
		}
	}
	return nil
}

// Check whether app is the correct version, and if not download and install new version
// Downloads all track versions for instant track switching
func app_check_install(id string) bool {
	if !valid(id, "entity") {
		debug("App %q ignoring install status", id)
		return true
	}
	// debug("App %q checking install status", id)

	// Get all track versions from publisher
	tracks, _, default_version, ok := app_check_version(id)
	if !ok {
		return false
	}

	// Download each track's version if not already installed
	// This enables instant track switching without additional downloads
	downloaded := false
	for track, version := range tracks {
		if app_has_version(id, version) {
			debug("App %q track %q version %q already installed", id, track, version)
			continue
		}
		debug("App %q downloading track %q version %q", id, track, version)
		if app_download_version(id, version) {
			downloaded = true
		}
	}

	// If no new versions were downloaded and we already have the default version, we're done
	if !downloaded && app_has_version(id, default_version) {
		debug("App %q all versions up to date", id)
		return true
	}

	// Ensure we have at least the default version
	if !app_has_version(id, default_version) {
		debug("App %q downloading default version %q as fallback", id, default_version)
		if !app_download_version(id, default_version) {
			return false
		}
	}

	return true
}

// Check all track versions of an app on the remote server
// Returns: tracks map (track->version), default track name, default version, success
func app_check_version(id string) (map[string]string, string, string, bool) {
	s, err := stream("", id, "publisher", "version", "", nil)
	if err != nil {
		// debug("App %q using fallback to default publisher", id)
		s, err = stream_to_peer(peer_default_publisher, "", id, "publisher", "version", "", nil)
	}
	if err != nil {
		// debug("App %q version check failed: %v", id, err)
		return nil, "", "", false
	}
	defer s.close()

	// Empty track lets publisher use its default_track
	s.write_content()

	resp, err := s.read_content()
	if err != nil {
		debug("%v", err)
		return nil, "", "", false
	}
	status, _ := resp["status"].(string)
	if status != "200" {
		return nil, "", "", false
	}

	v, err := s.read_content()
	if err != nil {
		debug("%v", err)
		return nil, "", "", false
	}

	// Parse default track and version (always present for backward compat)
	default_track, _ := v["default_track"].(string)
	default_version, _ := v["version"].(string)
	if !valid(default_version, "version") {
		return nil, "", "", false
	}

	// Build tracks map from tracks array
	tracks := make(map[string]string)
	if tracks_array, ok := v["tracks"].([]interface{}); ok {
		for _, t := range tracks_array {
			if tm, ok := t.(map[string]interface{}); ok {
				track, _ := tm["track"].(string)
				version, _ := tm["version"].(string)
				if valid(track, "constant") && valid(version, "version") {
					tracks[track] = version
				}
			}
		}
	}

	return tracks, default_track, default_version, true
}

// Check if a specific version of an app is already installed
func app_has_version(id, version string) bool {
	apps_lock.Lock()
	a := apps[id]
	apps_lock.Unlock()
	if a == nil {
		return false
	}
	apps_lock.Lock()
	_, exists := a.versions[version]
	apps_lock.Unlock()
	return exists
}

// Download and install a specific version of an app (without activating it)
func app_download_version(id, version string) bool {
	debug("App %q downloading version %q", id, version)

	s, err := stream("", id, "publisher", "get", "", nil)
	if err != nil {
		s, err = stream_to_peer(peer_default_publisher, "", id, "publisher", "get", "", nil)
	}
	if err != nil {
		debug("App %q download stream failed: %v", id, err)
		return false
	}
	defer s.close()

	err = s.write_content("version", version)
	if err != nil {
		return false
	}

	response, err := s.read_content()
	if err != nil {
		return false
	}
	status, _ := response["status"].(string)
	if status != "200" {
		debug("App %q download failed with status %q", id, status)
		return false
	}

	zip := fmt.Sprintf("%s/tmp/app_%s_%s.zip", cache_dir, id, version)
	if !file_write_from_reader(zip, s.raw_reader()) {
		_ = os.Remove(zip)
		return false
	}

	av, err := app_install(id, version, zip, false)
	if err != nil {
		_ = os.Remove(zip)
		return false
	}

	app_resolve_paths(av, id)

	a := app_external(id)
	a.load_version(av)
	debug("App %q version %q installed", id, version)
	return true
}

// app_for_service finds the best app for a service with user preferences.
// Resolution order:
// 1. User's binding (if user is not nil)
// 2. System binding (in apps.db)
// 3. Fallback: First app that declares this service (dev apps first, then by install time)
func app_for_service(user *User, service string) *App {
	// 0. Core internal services (replication, directory, peers) have a
	// single built-in handler registered at startup. Resolve them
	// directly: this skips the user/system binding lookups and the
	// O(apps) fallback scan that would otherwise run on every core P2P
	// event, and guarantees a user-installed app cannot shadow a core
	// service by declaring its name.
	if a := internal_service_app(service); a != nil {
		return a
	}

	// The remaining steps hit the DB (per-user and system bindings) and
	// may scan every installed app (fallback); on the per-event routing
	// path that cost dominates. Serve from the service cache when fresh.
	key := resolution_key{resolution_user_key(user), service}
	if a, ok := resolution_services.get(key); ok {
		return a
	}
	a := app_for_service_resolve(user, service)
	resolution_services.put(key, a)
	return a
}

// app_for_service_resolve resolves the handler app from its inputs,
// without consulting the cache. See app_for_service for the order.
func app_for_service_resolve(user *User, service string) *App {
	// 1. Check user's binding
	if user != nil {
		if app_id := user.service_app(service); app_id != "" {
			if a := app_by_id(app_id); a != nil {
				return a
			}
		}
	}

	// 2. Check system binding
	if app_id := apps_service_get(service); app_id != "" {
		if a := app_by_id(app_id); a != nil {
			return a
		}
	}

	// 3. Fallback: First app that declares this service
	if a := app_for_service_fallback(user, service); a != nil {
		return a
	}

	// 4. Handle app entity ID as service (e.g. attachment sync from published apps)
	a := app_by_id(service)
	if a == nil {
		return nil
	}
	// If a published app was found, check if a dev app provides the same service
	if is_entity_id(a.id) {
		av := a.active(user)
		if av != nil {
			for _, s := range av.Services {
				if dev := app_for_service_fallback(user, s); dev != nil && !is_entity_id(dev.id) {
					debug("app_for_service: published app %q -> dev app %q for service %q", a.id, dev.id, s)
					return dev
				}
			}
		}
	}
	return a
}

// app_services returns the services this app is the active handler for
func app_services(a *App, user *User) []string {
	av := a.active(user)
	if av == nil {
		return nil
	}
	var result []string
	for _, s := range av.Services {
		if resolved := app_for_service(user, s); resolved != nil && resolved.id == a.id {
			result = append(result, s)
		}
	}
	return result
}

// internal_service_app returns the built-in app handling a core internal
// service (replication, directory, peers), or nil. The map is written
// only at startup (service()), so the locked read is a tight, cheap
// critical section that never nests with the locks app_for_service's
// other steps take.
func internal_service_app(service string) *App {
	apps_lock.Lock()
	a := internal_services[service]
	apps_lock.Unlock()
	return a
}

// app_for_service_fallback finds the first app that declares a service.
// Dev apps (non-entity IDs) take precedence, then ordered by install time.
func app_for_service_fallback(user *User, service string) *App {
	apps_lock.Lock()
	defer apps_lock.Unlock()

	var candidates []*App
	for _, a := range apps {
		av := a.active_locked(user)
		if av == nil {
			continue
		}
		for _, s := range av.Services {
			if s == service {
				candidates = append(candidates, a)
				break
			}
		}
	}

	return app_select_best(candidates)
}

// app_login_path is the URL path the login app is served at. The
// login_app system setting names it (default "login"); an administrator
// can point it at another app's path to replace the login experience —
// landing page and interstitials — with their own app. Resolving by
// path, not by app id, keeps it mode-independent (the path is the same
// on a development and a published install), which is what stopped the
// published-install redirect loop in ticket #414.
func app_login_path() string {
	return setting_get("login_app", "login")
}

// app_login resolves the app bound to the login role: the app serving
// app_login_path. This is the app the auth gates exempt (it owns the
// interstitial pages) and the landing served to unauthenticated users.
func app_login() *App {
	return app_for_path(nil, app_login_path())
}

// app_is_login reports whether a is the bound login app. The gates that
// exempt the login app (pending-replication, account-closing,
// identity-required) redirect INTO its interstitial pages, so the login
// app must never be gated against itself — matching by the resolved app
// rather than an id literal is what keeps that from looping on a
// published install (ticket #414).
func app_is_login(a *App) bool {
	if a == nil {
		return false
	}
	l := app_login()
	return l != nil && l.id == a.id
}

// app_login_route is the absolute URL of one of the login app's
// interstitial pages (replicating, restore, closing, identity) — the
// targets the auth gates redirect a browser to. The page names are
// fixed conventions; only the login app's path varies.
func app_login_route(name string) string {
	return "/" + app_login_path() + "/" + name
}

// app_login_owns reports whether a trimmed request path belongs to the
// login app (its path or a sub-path) — used to exempt it from the
// shell-level closing gate.
func app_login_owns(raw string) bool {
	p := app_login_path()
	return raw == p || strings.HasPrefix(raw, p+"/")
}

// app_for_path finds the best app for a URL path with user preferences.
// Resolution order:
// 1. User's binding (if user is not nil)
// 2. System binding (in apps.db)
// 3. Fallback: First app that declares this path (dev apps first, then by install time)
func app_for_path(user *User, path string) *App {
	// The steps below hit the DB (per-user and system bindings) and may
	// scan every installed app (fallback); on the per-request routing path
	// that cost dominates. Serve from the path cache when fresh.
	key := resolution_key{resolution_user_key(user), path}
	if a, ok := resolution_paths.get(key); ok {
		return a
	}
	a := app_for_path_resolve(user, path)
	resolution_paths.put(key, a)
	return a
}

// app_for_path_resolve resolves the handler app from its inputs, without
// consulting the cache. See app_for_path for the order.
func app_for_path_resolve(user *User, path string) *App {
	// 1. Check user's binding
	if user != nil {
		if app_id := user.path_app(path); app_id != "" {
			if a := app_by_id(app_id); a != nil {
				return a
			}
		}
	}

	// 2. Check system binding
	if app_id := apps_path_get(path); app_id != "" {
		if a := app_by_id(app_id); a != nil {
			return a
		}
	}

	// 3. Fallback: First app that declares this path
	return app_for_path_fallback(user, path)
}

// app_for_path_fallback finds the first app that declares a path.
// Dev apps (non-entity IDs) take precedence, then ordered by install time.
func app_for_path_fallback(user *User, path string) *App {
	apps_lock.Lock()
	defer apps_lock.Unlock()

	var candidates []*App
	for _, a := range apps {
		av := a.active_locked(user)
		if av == nil {
			continue
		}
		for _, p := range av.Paths {
			if p == path {
				candidates = append(candidates, a)
				break
			}
		}
	}

	return app_select_best(candidates)
}

// app_declares_class returns true if the app's active version declares the given class.
func app_declares_class(app *App, user *User, class string) bool {
	av := app.active(user)
	if av == nil {
		return false
	}
	for _, c := range av.Classes {
		if c == class {
			return true
		}
	}
	return false
}

// class_app_for finds the best app for a class with user preferences.
// Resolution order:
// 1. User's binding (if user is not nil)
// 2. System binding (in apps.db)
// 3. Fallback: First app that declares this class (dev apps first, then by install time)
func class_app_for(user *User, class string) *App {
	// The steps below hit the DB (per-user and system bindings) and may
	// scan every installed app (fallback). Serve from the class cache when
	// fresh.
	key := resolution_key{resolution_user_key(user), class}
	if a, ok := resolution_classes.get(key); ok {
		return a
	}
	a := class_app_resolve(user, class)
	resolution_classes.put(key, a)
	return a
}

// class_app_resolve resolves the handler app from its inputs, without
// consulting the cache. See class_app_for for the order.
func class_app_resolve(user *User, class string) *App {
	// 1. Check user's binding
	if user != nil {
		if app_id := user.class_app(class); app_id != "" {
			if a := app_by_id(app_id); a != nil {
				return a
			}
		}
	}

	// 2. Check system binding
	if app_id := apps_class_get(class); app_id != "" {
		if a := app_by_id(app_id); a != nil {
			return a
		}
	}

	// 3. Fallback: First app that declares this class
	return class_app_fallback(user, class)
}

// class_app_fallback finds the first app that declares a class.
// Dev apps (non-entity IDs) take precedence, then ordered by install time.
func class_app_fallback(user *User, class string) *App {
	apps_lock.Lock()
	defer apps_lock.Unlock()

	var candidates []*App
	for _, a := range apps {
		av := a.active_locked(user)
		if av == nil {
			continue
		}
		for _, c := range av.Classes {
			if c == class {
				candidates = append(candidates, a)
				break
			}
		}
	}

	return app_select_best(candidates)
}

// app_select_best selects the best app from candidates.
// Dev apps (non-entity IDs) take precedence, then ordered by install time.
func app_select_best(candidates []*App) *App {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	// Separate dev apps (non-entity IDs) from published apps (entity IDs)
	var dev, published []*App
	for _, a := range candidates {
		if is_entity_id(a.id) {
			published = append(published, a)
		} else {
			dev = append(dev, a)
		}
	}

	// Dev apps take precedence
	if len(dev) > 0 {
		candidates = dev
	} else {
		candidates = published
	}

	// If multiple, pick the one with earliest install time
	var best *App
	var best_time int64 = 0
	for _, a := range candidates {
		installed := apps_installed(a.id)
		if best == nil || (installed > 0 && installed < best_time) || (best_time == 0 && installed > 0) {
			best = a
			best_time = installed
		}
	}

	// If no install time recorded, just return the first
	if best == nil && len(candidates) > 0 {
		best = candidates[0]
	}

	// Log warning if there are conflicts
	if len(candidates) > 1 {
		var ids []string
		for _, a := range candidates {
			ids = append(ids, a.id)
		}
		debug("App conflict: multiple apps claim same resource, using %q (others: %v)", best.id, ids)
	}

	return best
}

// is_entity_id returns true if the ID looks like an entity ID (50-51 chars)
func is_entity_id(id string) bool {
	return len(id) >= 50 && len(id) <= 51
}

// Global binding functions for apps.db

// apps_class_get returns the app ID bound to a class, or empty string if not set
func apps_class_get(class string) string {
	db := db_apps()
	row, _ := db.row("select app from classes where class = ?", class)
	if row == nil {
		return ""
	}
	return row["app"].(string)
}

// apps_class_set binds a class to an app ID. Emits a system-set op so
// pair members converge on the bindings.
func apps_class_set(class, app string) {
	db := db_apps()
	db.exec("replace into classes (class, app) values (?, ?)", class, app)
	resolution_invalidate() // system class binding changed
}

// apps_class_delete removes a class binding. Emits a system-set with
// empty value (the receiver treats empty as "removed").
func apps_class_delete(class string) {
	db := db_apps()
	db.exec("delete from classes where class = ?", class)
	resolution_invalidate() // system class binding removed
}

// apps_service_get returns the app ID bound to a service, or empty string if not set
func apps_service_get(service string) string {
	db := db_apps()
	row, _ := db.row("select app from services where service = ?", service)
	if row == nil {
		return ""
	}
	return row["app"].(string)
}

// apps_service_set binds a service to an app ID. Emits system-set.
func apps_service_set(service, app string) {
	db := db_apps()
	db.exec("replace into services (service, app) values (?, ?)", service, app)
	resolution_invalidate() // system service binding changed
}

// apps_service_delete removes a service binding (empty-value op).
func apps_service_delete(service string) {
	db := db_apps()
	db.exec("delete from services where service = ?", service)
	resolution_invalidate() // system service binding removed
}

// apps_path_get returns the app ID bound to a path, or empty string if not set
func apps_path_get(path string) string {
	db := db_apps()
	row, _ := db.row("select app from paths where path = ?", path)
	if row == nil {
		return ""
	}
	return row["app"].(string)
}

// apps_path_set binds a path to an app ID. Emits system-set.
func apps_path_set(path, app string) {
	db := db_apps()
	db.exec("replace into paths (path, app) values (?, ?)", path, app)
	resolution_invalidate() // system path binding changed
}

// apps_path_delete removes a path binding (empty-value op).
func apps_path_delete(path string) {
	db := db_apps()
	db.exec("delete from paths where path = ?", path)
	resolution_invalidate() // system path binding removed
}

// apps_record stamps an app's install timestamp and emits a system-set
// op so pair members can pull the same code from the publisher. Always
// writes (REPLACE INTO) and always emits — re-installs after an
// upgrade need to re-broadcast the timestamp so the receiver-side
// apply handler fires app_check_install for the new version.
func apps_record(app string) {
	db := db_apps()
	ts := now()
	db.exec("replace into apps (app, installed) values (?, ?)", app, ts)
}

// apps_installed returns the installation timestamp for an app, or 0 if not recorded
func apps_installed(app string) int64 {
	db := db_apps()
	row, _ := db.row("select installed from apps where app = ?", app)
	if row == nil {
		return 0
	}
	return row["installed"].(int64)
}

// Install an app from a zip file, but do not load it
func app_install(id string, version string, file string, check_only bool, peer ...string) (*AppVersion, error) {
	if version == "" {
		debug("App %q installing from %q", id, file)
	} else {
		debug("App %q installing version %q from %q", id, version, file)
	}
	if err := os.MkdirAll(filepath.Join(data_dir, "tmp"), 0755); err != nil {
		return nil, fmt.Errorf("unable to create tmp dir: %w", err)
	}
	tmp := filepath.Join(data_dir, "tmp", fmt.Sprintf("app_install_%s_%s", id, random_alphanumeric(8)))
	err := unzip(file, tmp)
	if err != nil {
		info("App unzip failed: %v", err)
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	av, err := app_read(id, tmp)
	if err != nil {
		info("App read failed: %v", err)
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	if version != "" && version != av.Version {
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("Specified version does not match file version")
	}

	if check_only {
		debug("App %q not installing", id)
		_ = os.RemoveAll(tmp)
		return av, nil
	}

	// Store publisher peer ID if provided
	if len(peer) > 0 && peer[0] != "" {
		av.Publisher.Peer = peer[0]
		app_write_publisher(tmp, peer[0])
	}

	// Check if this is a new install or upgrade before modifying filesystem
	installed := apps_installed(id) > 0
	a := app_by_id(id)
	previous := ""
	if a != nil && a.latest != nil {
		previous = a.latest.Version
	}

	av.base = fmt.Sprintf("%s/apps/%s/%s", data_dir, id, av.Version)
	if file_exists(av.base) {
		debug("App %q removing old copy of version %q in %q", id, av.Version, av.base)
		if err := os.RemoveAll(av.base); err != nil {
			_ = os.RemoveAll(tmp)
			return nil, fmt.Errorf("removing existing install at %s: %w", av.base, err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(av.base), 0755); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("creating parent dir for install at %s: %w", av.base, err)
	}
	if err := os.Rename(tmp, av.base); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("moving install from %s to %s: %w", tmp, av.base, err)
	}

	// Audit log installation or upgrade
	if !installed {
		audit_app_installed(id, av.Version)
	} else if previous != "" && version_greater(av.Version, previous) {
		audit_app_upgraded(id, previous, av.Version)
	}

	debug("App %q version %q installed", id, av.Version)
	return av, nil
}

// Write publisher info to app.json, preserving existing content
func app_write_publisher(base string, peer string) {
	path := base + "/app.json"
	data, err := os.ReadFile(path)
	if err != nil {
		info("Failed to read app.json: %v", err)
		return
	}

	// Parse existing JSON
	standardized, err := hujson.Standardize(data)
	if err != nil {
		info("Failed to standardize app.json: %v", err)
		return
	}

	var manifest map[string]any
	err = json.Unmarshal(standardized, &manifest)
	if err != nil {
		info("Failed to parse app.json: %v", err)
		return
	}

	// Add or update publisher field
	manifest["publisher"] = map[string]string{"peer": peer}

	// Write back
	output, err := json.MarshalIndent(manifest, "", "\t")
	if err != nil {
		info("Failed to marshal app.json: %v", err)
		return
	}

	if err := file_write(path, output); err != nil {
		info("Failed to write app.json: %v", err)
		return
	}
	debug("Wrote publisher peer %q to %s", peer, path)
}

// apps_manager_wake lets a replicated publisher-catalog write trigger an early
// apps_manager pass instead of waiting out the 24-hour poll — this is what
// kills the version-skew window on replica-pair hosts. Buffered at 1 so a
// deploy's burst of publisher ops (a new version writes several rows)
// coalesces into a single queued pass.
var apps_manager_wake = make(chan struct{}, 1)

// apps_manager_signal wakes apps_manager without blocking. A full buffer means
// a pass is already queued, so the extra signal is harmlessly dropped.
func apps_manager_signal() {
	select {
	case apps_manager_wake <- struct{}{}:
	default:
	}
}

var (
	apps_publisher_lock     sync.Mutex
	apps_publisher_id_value string
)

// apps_publisher_id returns the memoised local publisher app id — the app
// serving the "publisher" service. Resolved by apps_manager on each pass (after
// the install checks) so the replication apply path only reads a cached string
// rather than scanning the app registry per op. Empty until the first pass.
func apps_publisher_id() string {
	apps_publisher_lock.Lock()
	defer apps_publisher_lock.Unlock()
	return apps_publisher_id_value
}

func apps_publisher_id_set(id string) {
	apps_publisher_lock.Lock()
	apps_publisher_id_value = id
	apps_publisher_lock.Unlock()
}


// Manage which apps and their versions are installed
func apps_manager() {
	time.Sleep(time.Second)

	// If we already have apps installed, skip the setup wait
	apps_root := filepath.Join(data_dir, "apps")
	if existing, err := file_list(apps_root); err == nil && len(existing) >= 2 {
		apps_bootstrap_ready = true
		debug("Apps already installed, skipping setup wait")
	}

	for {
		todo := map[string]bool{}

		// Install default apps in priority order (Login and Home first)
		for i, app := range apps_default {
			todo[app.ID] = true
			app_check_install(app.ID)

			// Mark bootstrap ready after Login and Home (first two) are installed
			if i == 1 && !apps_bootstrap_ready {
				apps_bootstrap_ready = true
				debug("Essential apps installed")
			}
		}

		// Check any other installed apps
		ids, err := file_list(apps_root)
		if err != nil {
			warn("Unable to list installed apps: %v", err)
		}
		for _, id := range ids {
			if !todo[id] {
				todo[id] = true
				app_check_install(id)
			}
		}

		// Pin each default app's declared services to it so name-based service
		// resolution can't fall through to an imposter app claiming the same
		// service name (see apps_pin_default_services).
		apps_pin_default_services(apps_default)

		// Re-apply each default app's permission grants to already-set-up
		// users, so a change to an app's apps_default set reaches users
		// provisioned before the change — and a service app driven only by
		// inbound P2P events (the Comptroller) gets its defaults seeded at all
		// (app_user_setup otherwise fires only from a same-host service call).
		apps_seed_default_permissions()

		// Cache the local publisher app id so the replication apply path can
		// recognise a replicated publisher-catalog write cheaply (a string read,
		// no registry scan per op).
		if a := app_for_service(nil, "publisher"); a != nil {
			apps_publisher_id_set(a.id)
		}

		// Wait out the poll, but wake early when a replicated publisher write
		// signals that a peer published a new version (replica-pair hosts).
		select {
		case <-time.After(24 * time.Hour):
		case <-apps_manager_wake:
			debug("apps_manager woken early by a replicated publisher write")
		}
	}
}

// apps_pin_default_services binds each default app's declared services to that
// app via a system service binding, so name-based service resolution for core
// services (repositories, notifications, ...) cannot fall through to the
// fallback and be captured by an imposter app that declares the same service
// name. Deliberately conservative:
//   - it never overwrites an existing system binding, so an administrator
//     override (set via mochi.app.service.set) survives;
//   - it skips any service a dev app provides, so local-development precedence
//     (dev apps win) is preserved.
//
// A user's own binding (a.user.app.service.set) always takes precedence over the
// system binding, so per-user overrides are unaffected either way. Idempotent:
// safe to run on every apps_manager pass.
func apps_pin_default_services(defaults []DefaultApp) {
	default_ids := map[string]bool{}
	for _, d := range defaults {
		default_ids[d.ID] = true
	}

	type binding struct{ service, app string }
	var candidates []binding
	dev := map[string]bool{}

	apps_lock.Lock()
	for _, a := range apps {
		av := a.active_locked(nil)
		if av == nil {
			continue
		}
		if !is_entity_id(a.id) {
			// Dev app: record its services so we never override it.
			for _, s := range av.Services {
				dev[s] = true
			}
			continue
		}
		if default_ids[a.id] {
			for _, s := range av.Services {
				candidates = append(candidates, binding{s, a.id})
			}
		}
	}
	apps_lock.Unlock()

	for _, c := range candidates {
		if dev[c.service] {
			continue // a dev app provides it; preserve dev precedence
		}
		if apps_service_get(c.service) != "" {
			continue // existing binding (admin override or earlier pin) - leave it
		}
		apps_service_set(c.service, c.app)
		debug("Pinned default service %q to app %q", c.service, c.app)
	}
}

// apps_seed_default_permissions re-applies every default app's apps_default
// permission grants to each already-set-up user. The normal grant path,
// app_user_setup(), runs only from a same-host service call (api_service_call /
// service_call_as_server), so two gaps exist: a service app driven solely by
// inbound P2P events (the Comptroller) never has its defaults seeded for its
// owner, and a CHANGE to an existing app's apps_default set never reaches users
// provisioned before the change. This sweep closes both. It is safe to run on
// every apps_manager pass: app_user_setup is idempotent and count-change-aware
// (it early-returns when the stored grant count already matches, and uses
// insert-or-ignore so a user-revoked grant is never re-granted), so it is a
// cheap no-op once every user is current. user_pending users are skipped inside
// app_user_setup — they are seeded when their bootstrap completes.
func apps_seed_default_permissions() {
	db := db_open("db/users.db")
	if db == nil {
		return
	}
	rows, _ := db.rows("select uid from users where status = 'active'")
	for _, row := range rows {
		uid, _ := row["uid"].(string)
		if uid == "" {
			continue
		}
		u := user_by_uid(uid)
		if u == nil {
			continue
		}
		for _, app := range apps_default {
			app_user_setup(u, app.ID)
		}
	}
}

// Read in an external app version from a directory
func app_read(id string, base string) (*AppVersion, error) {
	debug("App loading %q", base)

	// Load app manifest from app.json
	path := base + "/app.json"
	st, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("App %q in %q has no app.json file; ignoring", id, base)
	}
	if err != nil {
		return nil, fmt.Errorf("App unable to stat '%s/app.json': %v", base, err)
	}

	var av AppVersion
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("App unable to read '%s/app.json': %v", base, err)
	}
	data, err := hujson.Standardize(raw)
	if err != nil {
		return nil, fmt.Errorf("App bad app.json '%s/app.json': %v", base, err)
	}
	err = json.Unmarshal(data, &av)
	if err != nil {
		return nil, fmt.Errorf("App bad app.json '%s/app.json': %v", base, err)
	}

	av.base = base
	av.app_json_mtime = st.ModTime()
	error_catalogue_validate(&av, id)

	// Validate manifest
	if !valid(av.Version, "version") {
		return nil, fmt.Errorf("App bad version %q", av.Version)
	}

	// Check server version requirements
	if av.Require.Version.Minimum != "" && version_compare(build_version, av.Require.Version.Minimum) < 0 {
		return nil, fmt.Errorf("App requires server version >= %s (current: %s)", av.Require.Version.Minimum, build_version)
	}
	if av.Require.Version.Maximum != "" && version_compare(build_version, av.Require.Version.Maximum) > 0 {
		return nil, fmt.Errorf("App requires server version <= %s (current: %s)", av.Require.Version.Maximum, build_version)
	}

	if !valid(av.Label, "constant") {
		return nil, fmt.Errorf("App bad label %q", av.Label)
	}

	for _, class := range av.Classes {
		if !valid(class, "constant") {
			return nil, fmt.Errorf("App bad class %q", class)
		}
	}

	for _, path := range av.Paths {
		if !valid(path, "apppath") {
			return nil, fmt.Errorf("App bad path %q", path)
		}
	}

	for _, service := range av.Services {
		if !valid(service, "constant") {
			return nil, fmt.Errorf("App bad service %q", service)
		}
	}

	if av.Architecture.Engine != "starlark" {
		return nil, fmt.Errorf("App bad engine %q version %d", av.Architecture.Engine, av.Architecture.Version)
	}
	if av.Architecture.Version < app_version_minimum {
		return nil, fmt.Errorf("App is too old. Version %d is less than minimum version %d", av.Architecture.Version, app_version_minimum)
	}
	if av.Architecture.Version > app_version_maximum {
		return nil, fmt.Errorf("App is too new. Version %d is greater than maximum version %d", av.Architecture.Version, app_version_maximum)
	}

	for _, file := range av.Execute {
		if !valid(file, "filepath") {
			return nil, fmt.Errorf("App bad executable file %q", file)
		}
	}

	if av.Database.File != "" && !valid(av.Database.File, "filename") {
		return nil, fmt.Errorf("App bad database file %q", av.Database.File)
	}

	if av.Database.Create.Function != "" && !valid(av.Database.Create.Function, "function") {
		return nil, fmt.Errorf("App bad database create function %q", av.Database.Create.Function)
	}

	if av.Database.Upgrade.Function != "" && !valid(av.Database.Upgrade.Function, "function") {
		return nil, fmt.Errorf("App bad database upgrade function %q", av.Database.Upgrade.Function)
	}

	if av.Database.Downgrade.Function != "" && !valid(av.Database.Downgrade.Function, "function") {
		return nil, fmt.Errorf("App bad database downgrade function %q", av.Database.Downgrade.Function)
	}

	if av.Icon != "" && !valid(av.Icon, "filepath") {
		return nil, fmt.Errorf("App bad icon path %q", av.Icon)
	}

	for _, i := range av.Icons {
		if i.Action != "" && !valid(i.Action, "constant") {
			return nil, fmt.Errorf("App bad icon action %q", i.Action)
		}

		if !valid(i.Label, "constant") {
			return nil, fmt.Errorf("App bad icon label %q", i.Label)
		}

		if !valid(i.File, "filepath") {
			return nil, fmt.Errorf("App bad icon file %q", i.File)
		}
	}

	for action, a := range av.Actions {
		if action != "" && !valid(action, "action") {
			return nil, fmt.Errorf("App bad action %q", action)
		}

		if a.Function != "" && !valid(a.Function, "function") {
			return nil, fmt.Errorf("App bad action function %q", a.Function)
		}

		if a.File != "" && !valid(a.File, "filepath") {
			return nil, fmt.Errorf("App bad file path %q", a.File)
		}

		if a.Files != "" && !valid(a.Files, "filepath") {
			return nil, fmt.Errorf("App bad files path %q", a.Files)
		}

		if a.OpenGraph != "" && !valid(a.OpenGraph, "function") {
			return nil, fmt.Errorf("App bad opengraph function %q", a.OpenGraph)
		}

		if a.Cache != "" {
			switch a.Cache {
			case "immutable", "static", "revalidate", "none":
				// valid
			default:
				return nil, fmt.Errorf("App bad cache policy %q for action %q", a.Cache, action)
			}
		}

	}

	for event, e := range av.Events {
		if !valid(event, "constant") {
			return nil, fmt.Errorf("App bad event %q", event)
		}

		if !valid(e.Function, "function") {
			return nil, fmt.Errorf("App bad event function %q", e.Function)
		}
	}

	for function, f := range av.Functions {
		if function != "" && !valid(function, "constant") {
			return nil, fmt.Errorf("App bad function %q", function)
		}

		if !valid(f.Function, "function") {
			return nil, fmt.Errorf("App bad function function %q", f.Function)
		}
	}

	return &av, nil
}

// Load all installed apps
func apps_start() {
	// Load development apps first (unversioned, constant IDs)
	if dev_apps_dir != "" {
		apps_load_dev()
	}

	// Load published apps (versioned, entity IDs)
	apps_load_published()
}

// Check if an app is already loaded
func app_exists(id string) bool {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	_, exists := apps[id]
	return exists
}

// Get an app by ID, returns nil if not found
func app_by_id(id string) *App {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	return apps[id]
}

// Check if a path is already used by another app (excluding the given app ID)
// Checks internal apps and all versions of external apps
func app_path_taken(path string, exclude string) bool {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	for _, a := range apps {
		if a.id == exclude {
			continue
		}
		// Check internal app paths
		if a.internal != nil {
			for _, p := range a.internal.Paths {
				if p == path {
					return true
				}
			}
		}
		// Check all external app version paths
		for _, av := range a.versions {
			for _, p := range av.Paths {
				if p == path {
					return true
				}
			}
		}
	}
	return false
}

// Resolve path conflicts for a published app. If any path conflicts with an
// already-loaded app, replaces paths with the app's fingerprint.
func app_resolve_paths(av *AppVersion, id string) {
	for _, path := range av.Paths {
		if app_path_taken(path, id) {
			fp := fingerprint(id)
			// debug("Published app %s path %q conflicts, using fingerprint %s", id, path, fp)
			av.Paths = []string{fp}
			return
		}
	}
}

// Load development apps from dev_apps_dir (unversioned)
func apps_load_dev() {
	ids, err := file_list(dev_apps_dir)
	if err != nil {
		debug("Dev apps directory unavailable: %v", err)
		return
	}
	for _, id := range ids {
		if strings.HasPrefix(id, ".") {
			continue
		}

		// Skip non-directories and directories without app.json
		base := dev_apps_dir + "/" + id
		if !file_is_directory(base) || !file_exists(base+"/app.json") {
			continue
		}

		// Dev apps must have constant IDs
		if !valid(id, "constant") {
			debug("Dev app skipping invalid ID %q (must be constant)", id)
			continue
		}

		// Read app.json directly (no version subdirectory)
		av, err := app_read(id, base)
		if err != nil {
			info("Dev app load error for %q: %v", id, err)
			continue
		}

		a := app_external(id)
		a.load_version(av)
		debug("Dev app loaded: %s", id)
	}
}

// Load published apps from data_dir/apps (versioned)
func apps_load_published() {
	apps_dir := filepath.Join(data_dir, "apps")
	ids, err := file_list(apps_dir)
	if err != nil {
		// Missing dir is the normal first-boot state — no apps installed yet.
		// Only surface the error if it's something else (permissions, IO).
		if !os.IsNotExist(err) {
			debug("Published apps directory unavailable: %v", err)
		}
		return
	}
	for _, id := range ids {
		if strings.HasPrefix(id, ".") {
			continue
		}

		// Skip non-directories
		if !file_is_directory(filepath.Join(apps_dir, id)) {
			continue
		}

		// Published apps must have entity IDs
		if !valid(id, "entity") {
			debug("Published app skipping invalid ID %q (must be entity)", id)
			continue
		}

		// Skip if already loaded as dev app
		if app_exists(id) {
			continue
		}

		app_dir := filepath.Join(apps_dir, id)
		versions, err := file_list(app_dir)
		if err != nil {
			debug("App %q: unable to list versions: %v", id, err)
			continue
		}
		if len(versions) == 0 {
			continue
		}
		a := app_external(id)

		for _, version := range versions {
			if strings.HasPrefix(version, ".") {
				continue
			}

			if !valid(version, "version") {
				debug("App skipping invalid version %q for app %q", version, id)
				continue
			}

			// Skip non-directories (stray files like a misplaced app.db
			// would otherwise reach app_read and fail noisily).
			if !file_is_directory(filepath.Join(app_dir, version)) {
				continue
			}

			av, err := app_read(id, filepath.Join(app_dir, version))
			if err != nil {
				info("App load error: %v", err)
				continue
			}

			// Check for path conflicts with already-loaded apps (e.g., dev apps)
			// TODO: Remove this workaround in v0.3 when multiple versions of the same app
			// can run simultaneously and users choose which version to use.
			app_resolve_paths(av, id)

			a.load_version(av)
		}
	}
}

// Register an event handler for an internal app
func (a *App) event(event string, f func(*Event)) {
	a.internal.Events[event] = AppEvent{internal_function: f}
}

// Register an anonymous event handler for an internal app
func (a *App) event_anonymous(event string, f func(*Event)) {
	a.internal.Events[event] = AppEvent{internal_function: f, Anonymous: true}
}

// Resolve an app label using the BCP 47 fallback chain and ICU MessageFormat.
// Optional args map contains named placeholders (e.g. {"count": 3, "name": "Alice"}).
// Existing call sites that pass no args resolve the raw label format unchanged.
func (a *App) label(u *User, av *AppVersion, key string, args ...map[string]any) string {
	language := "en"
	if u != nil {
		language = user_language(u)
	}

	var kwargs map[string]any
	if len(args) > 0 {
		kwargs = args[0]
	}
	return resolve_label(av, language, key, kwargs)
}

// Loads a new version into the versions map, updating a.latest if this is the highest version
func (a *App) load_version(av *AppVersion) {
	if a == nil || av == nil {
		return
	}

	// Record app installation timestamp (only recorded once, not updated on upgrades)
	apps_record(a.id)

	for i, file := range av.Execute {
		av.Execute[i] = av.base + "/" + file
	}

	av.labels = make(map[string]map[string]string)
	label_files, err := file_list(av.base + "/labels")
	if err != nil {
		debug("App %q has no labels dir", av.base)
	}
	for _, file := range label_files {
		language := strings.TrimSuffix(file, ".conf")
		if !valid(language, "locale") {
			continue
		}
		av.labels[language] = make(map[string]string)

		path := fmt.Sprintf("%s/labels/%s", av.base, file)
		f, err := os.Open(path)
		if err != nil {
			info("App unable to read labels file %q: %v", path, err)
			continue
		}
		defer f.Close()

		s := bufio.NewScanner(f)
		for s.Scan() {
			parts := strings.SplitN(s.Text(), "=", 2)
			if len(parts) == 2 {
				av.labels[language][strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	apps_lock.Lock()
	av.app = a
	a.versions[av.Version] = av

	latest := ""
	for v := range a.versions {
		if version_greater(v, latest) {
			latest = v
		}
	}
	if latest == av.Version {
		a.latest = av
	}
	apps_lock.Unlock()
	resolution_invalidate() // installed version set changed

	debug("App %q, %q version %q loaded", av.labels["en"][av.Label], a.id, av.Version)
}

// Register a service for an internal app
func (a *App) service(service string) {
	a.internal.Services = append(a.internal.Services, service)
	apps_lock.Lock()
	internal_services[service] = a
	apps_lock.Unlock()
}

// Find the action best matching the specified name
func (av *AppVersion) find_action(name string) *AppAction {
	var candidates []AppAction

	for action, aa := range av.Actions {
		aa.name = action
		segments := strings.Split(action, "/")
		aa.segments = len(segments)
		aa.literals = 0
		for _, s := range segments {
			if !strings.HasPrefix(s, ":") && !strings.HasPrefix(s, "*") {
				aa.literals++
			}
		}
		aa.parameters = map[string]string{}
		candidates = append(candidates, aa)
	}

	// Sort candidates: files/feature routes first, then more segments first, then more literals first
	sort.Slice(candidates, func(i, j int) bool {
		i_special := candidates[i].Files != "" || candidates[i].Feature != ""
		j_special := candidates[j].Files != "" || candidates[j].Feature != ""
		if i_special != j_special {
			return i_special
		} else if candidates[i].segments != candidates[j].segments {
			return candidates[i].segments > candidates[j].segments
		} else {
			return candidates[i].literals > candidates[j].literals
		}
	})

	for _, aa := range candidates {
		// Try exact match first
		if aa.name == name {
			//debug("App found direct action %q with function %q", name, aa.Function)
			return &aa
		}

		// If type files or feature, check for matching parent (try progressively shorter prefixes)
		// Supports parameterized patterns like :wiki/-/assets
		if aa.Files != "" || aa.Feature != "" {
			key_segments := strings.Split(aa.name, "/")
			match := name
			for {
				// Calculate the file path suffix (what comes after the matched pattern)
				suffix := ""
				if len(match) < len(name) {
					suffix = name[len(match)+1:] // +1 to skip the /
				}

				// Try exact match first
				if aa.name == match {
					aa.filepath = suffix
					return &aa
				}
				// Try parameterized match
				value_segments := strings.Split(match, "/")
				if len(key_segments) == len(value_segments) {
					ok := true
					for i := 0; i < len(key_segments); i++ {
						ks := key_segments[i]
						vs := value_segments[i]
						if strings.HasPrefix(ks, ":") {
							pname := ks[1:]
							aa.parameters[pname] = vs
						} else if ks != vs {
							ok = false
							break
						}
					}
					if ok {
						aa.filepath = suffix
						return &aa
					}
				}
				// Try shorter prefix
				idx := strings.LastIndex(match, "/")
				if idx < 0 {
					break
				}
				match = match[:idx]
			}
		}

		// Try dynamic match
		key_segments := strings.Split(aa.name, "/")
		value_segments := strings.Split(name, "/")

		// Find greedy parameter position (starts with *), if any
		greedy_pos := -1
		for i, ks := range key_segments {
			if strings.HasPrefix(ks, "*") {
				greedy_pos = i
				break
			}
		}

		// Calculate suffix length (segments after greedy param)
		suffix_len := 0
		if greedy_pos >= 0 {
			suffix_len = len(key_segments) - greedy_pos - 1
		}

		// Check segment count compatibility
		if greedy_pos >= 0 {
			// Greedy: value must have at least (prefix + 1 + suffix) segments
			if len(value_segments) < greedy_pos+1+suffix_len {
				continue
			}
		} else if len(key_segments) != len(value_segments) {
			continue
		}

		ok := true

		// Match prefix segments (before greedy param)
		prefix_end := len(key_segments)
		if greedy_pos >= 0 {
			prefix_end = greedy_pos
		}
		for i := 0; i < prefix_end; i++ {
			ks := key_segments[i]
			vs := value_segments[i]
			if strings.HasPrefix(ks, ":") {
				aa.parameters[ks[1:]] = vs
			} else if ks != vs {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}

		// Match suffix segments (after greedy param) from the end
		for i := 0; i < suffix_len; i++ {
			ks := key_segments[len(key_segments)-1-i]
			vs := value_segments[len(value_segments)-1-i]
			if strings.HasPrefix(ks, ":") {
				aa.parameters[ks[1:]] = vs
			} else if ks != vs {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}

		// Capture greedy parameter (everything between prefix and suffix)
		if greedy_pos >= 0 {
			greedy_end := len(value_segments) - suffix_len
			pname := key_segments[greedy_pos][1:] // Remove '*'
			aa.parameters[pname] = strings.Join(value_segments[greedy_pos:greedy_end], "/")
		}

		return &aa
	}

	// Fall back to empty action name as catch-all
	for _, aa := range candidates {
		if aa.name == "" {
			//debug("App found fallback action %q with function %q via catch-all", name, aa.Function)
			return &aa
		}
	}

	info("App %q version %q has no action matching %q", av.app.id, av.Version, name)
	return nil
}

// Get a Starlark interpreter for an app version
func (av *AppVersion) starlark() *Starlark {
	if dev_reload {
		return starlark(av.Execute)
	}
	av.starlark_once.Do(func() {
		av.starlark_globals = starlark(av.Execute).globals
	})
	return &Starlark{
		thread:  &sl.Thread{Name: "main"},
		globals: av.starlark_globals,
	}
}

// Call a Starlark database function (create, upgrade, downgrade)
func (av *AppVersion) starlark_db(u *User, function string, args sl.Tuple) error {
	s := av.starlark()
	s.set("app", av.app)
	s.set("user", u)
	s.set("owner", u)
	_, err := s.call(function, args)
	return err
}

// Reload app.json and labels from disk (for development).
// Gated on app.json mtime so normal dev reload callers skip unchanged manifests.
func (av *AppVersion) reload() {
	if av.base == "" {
		return
	}
	path := av.base + "/app.json"
	st, err := os.Stat(path)
	if err != nil {
		info("App reload failed to stat %q: %v", path, err)
		return
	}
	mtime := st.ModTime()
	apps_lock.Lock()
	cached := av.app_json_mtime
	apps_lock.Unlock()
	if !cached.IsZero() && !mtime.After(cached) {
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		info("App reload failed to read %q: %v", path, err)
		return
	}
	data, err := hujson.Standardize(raw)
	if err != nil {
		info("App reload failed to standardize %q: %v", path, err)
		return
	}

	var fresh AppVersion
	if err := json.Unmarshal(data, &fresh); err != nil {
		info("App reload failed to parse %q: %v", path, err)
		return
	}

	// Reload labels
	labels := make(map[string]map[string]string)
	label_files, err := file_list(av.base + "/labels")
	if err != nil {
		debug("App %q has no labels dir", av.base)
	}
	for _, file := range label_files {
		language := strings.TrimSuffix(file, ".conf")
		if !valid(language, "locale") {
			continue
		}
		labels[language] = make(map[string]string)

		lpath := fmt.Sprintf("%s/labels/%s", av.base, file)
		f, err := os.Open(lpath)
		if err != nil {
			continue
		}

		s := bufio.NewScanner(f)
		for s.Scan() {
			parts := strings.SplitN(s.Text(), "=", 2)
			if len(parts) == 2 {
				labels[language][strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
		f.Close()
	}

	// Convert relative execute paths to absolute
	for i, file := range fresh.Execute {
		fresh.Execute[i] = av.base + "/" + file
	}

	// Update fields that are safe to reload.
	// Paths is excluded: it controls URL routing, and by the time reload()
	// runs the request has already been routed to this app by its current path.
	apps_lock.Lock()
	av.Label = fresh.Label
	av.Icons = fresh.Icons
	av.Actions = fresh.Actions
	av.Events = fresh.Events
	av.Functions = fresh.Functions
	av.Database = fresh.Database
	av.Services = fresh.Services
	av.Classes = fresh.Classes
	av.Architecture = fresh.Architecture
	av.Execute = fresh.Execute
	av.Themes = fresh.Themes
	av.ThemeIcons = fresh.ThemeIcons
	av.IconSymbolic = fresh.IconSymbolic
	av.labels = labels
	av.app_json_mtime = mtime
	apps_lock.Unlock()
	resolution_invalidate() // declared services / version metadata changed
}

// mochi.app.get(id) -> dict or None: Get details of an app
func api_app_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid ID %q", id)
	}

	apps_lock.Lock()
	a, found := apps[id]
	apps_lock.Unlock()

	if found {
		user := t.Local("user").(*User)
		av := a.active(user)
		latest := av.Version
		if a.latest != nil {
			latest = a.latest.Version
		}
		result := map[string]any{
			"id":     a.id,
			"name":   a.label(user, av, av.Label),
			"latest": latest,
			"icon":   av.icon(),
		}
		if av.Publisher.Peer != "" {
			result["publisher"] = map[string]string{"peer": av.Publisher.Peer}
		}
		return sl_encode(result), nil
	}

	return sl.None, nil
}

// mochi.app.label(key, **kwargs) -> string: Resolve a label key from the calling
// app's labels/<lang>.conf using the BCP 47 fallback chain (variant -> parent -> en).
// kwargs are passed to ICU MessageFormat substitution (e.g. count=3, name="Alice").
// Returns the literal key if nothing resolves (developer bug).
func api_app_label(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <key: string>, **kwargs")
	}
	key, ok := sl.AsString(args[0])
	if !ok || key == "" {
		return sl_error(fn, "invalid key")
	}

	a, ok := t.Local("app").(*App)
	if !ok || a == nil {
		return sl.String(""), nil
	}

	user, _ := t.Local("user").(*User)
	av := a.active(user)
	if av == nil || av.labels == nil {
		return sl.String(""), nil
	}

	// Language priority: user preference (logged in, including last_language
	// fallback for "auto") > thread-local from request handler (anonymous
	// Accept-Language) > "en". The handler in web.go calls
	// request_language(c, user) and stashes the result via s.set("language",
	// ...) so anonymous public-action calls still get a translated label set.
	// For composed-then-deferred sends (email/push notifications), the user
	// is set on the thread but no request context is available; user_language
	// reads the persisted last_language for those callers.
	language := "en"
	if user != nil {
		language = user_language(user)
	} else if l, ok := t.Local("language").(string); ok && l != "" {
		language = l
	}

	margs, err := starlark_kwargs_to_map(kwargs)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return sl.String(resolve_label(av, language, key, margs)), nil
}

// starlark_kwargs_to_map converts a Starlark kwargs tuple into a Go map[string]any
// suitable for ICU MessageFormat substitution. Numeric values become int64 or
// float64; strings become string; everything else is rendered via String().
func starlark_kwargs_to_map(kwargs []sl.Tuple) (map[string]any, error) {
	if len(kwargs) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(kwargs))
	for _, kv := range kwargs {
		k, ok := sl.AsString(kv[0])
		if !ok {
			continue
		}
		switch v := kv[1].(type) {
		case sl.String:
			out[k] = string(v)
		case sl.Int:
			if i, ok := v.Int64(); ok {
				out[k] = i
			} else {
				out[k] = v.String()
			}
		case sl.Float:
			out[k] = float64(v)
		case sl.Bool:
			out[k] = bool(v)
		default:
			out[k] = v.String()
		}
	}
	return out, nil
}

// mochi.app.icons() -> dict: Get available icons for home screen
// Returns {"icons": [...], "icon_mask": "...", "icon_background": "..."}
func api_app_icons(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	var icons []map[string]string

	// Resolve the user's active theme for icon overrides
	theme_pref := user_preference_get(user, "theme", "")
	var active_theme *AppTheme
	var active_theme_id string
	var active_theme_app *App
	if theme_pref != "" {
		if parts := strings.SplitN(theme_pref, ":", 2); len(parts) == 2 {
			active_theme = app_theme_get(user, parts[0], parts[1])
			active_theme_id = theme_pref
			if active_theme != nil {
				apps_lock.Lock()
				active_theme_app = apps[parts[0]]
				apps_lock.Unlock()
			}
		}
	}

	apps_lock.Lock()
	for _, a := range apps {
		av := a.active_locked(user)
		if av == nil || !av.user_allowed(user) {
			continue
		}
		for _, i := range av.Icons {
			path := a.fingerprint
			if len(av.Paths) > 0 {
				path = av.Paths[0]
			}

			icon_file := i.File
			icon_path := path
			if i.Action != "" {
				icon_path = path + "/" + i.Action
			}

			// Icon resolution priority:
			// 1. Theme's icon for this app
			// 2. App's icon for this theme
			// 3. App's default icon
			if active_theme != nil && active_theme.Icons != nil {
				if override, ok := active_theme.Icons[path]; ok && active_theme_app != nil {
					theme_av := active_theme_app.active_locked(user)
					if theme_av != nil && len(theme_av.Paths) > 0 {
						icon_path = theme_av.Paths[0] + "/icons"
						icon_file = override
					}
				}
			}
			if icon_file == i.File && active_theme_id != "" && av.ThemeIcons != nil {
				if override, ok := av.ThemeIcons[active_theme_id]; ok {
					icon_path = path + "/images"
					icon_file = filepath.Base(override)
				}
			}

			icons = append(icons, map[string]string{"id": a.id, "path": icon_path, "name": a.label(user, av, i.Label), "file": icon_file, "link": path})
		}
	}
	apps_lock.Unlock()

	sort.Slice(icons, func(i, j int) bool {
		return strings.ToLower(icons[i]["name"]) < strings.ToLower(icons[j]["name"])
	})

	result := map[string]any{"icons": icons}
	if active_theme != nil {
		if active_theme.IconMask != "" {
			result["icon_mask"] = active_theme.IconMask
		}
		if active_theme.IconBackground != "" {
			result["icon_background"] = active_theme.IconBackground
		}
	}
	return sl_encode(result), nil
}

// mochi.app.package.get(file) -> dict: Read app info from a .zip file without installing
func api_app_package_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	a, ok := t.Local("app").(*App)
	if !ok || a == nil {
		return sl_error(fn, "no app")
	}

	// Unzip to temp directory
	if err := os.MkdirAll(filepath.Join(data_dir, "tmp"), 0755); err != nil {
		return sl_error(fn, "unable to create tmp dir: %v", err)
	}
	tmp := filepath.Join(data_dir, "tmp", fmt.Sprintf("app_info_%s", random_alphanumeric(8)))
	err := unzip(api_file_path(user, a, file), tmp)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return sl_error(fn, "failed to unzip: %v", err)
	}
	defer os.RemoveAll(tmp)

	// Read app.json
	if !file_exists(filepath.Join(tmp, "app.json")) {
		return sl_error(fn, "no app.json in archive")
	}

	var av AppVersion
	raw, err := os.ReadFile(filepath.Join(tmp, "app.json"))
	if err != nil {
		return sl_error(fn, "unable to read app.json: %v", err)
	}
	data, err := hujson.Standardize(raw)
	if err != nil {
		return sl_error(fn, "bad app.json: %v", err)
	}
	err = json.Unmarshal(data, &av)
	if err != nil {
		return sl_error(fn, "bad app.json: %v", err)
	}

	// Read label from labels/en.conf
	name := av.Label
	labels_path := tmp + "/labels/en.conf"
	if file_exists(labels_path) {
		f, err := os.Open(labels_path)
		if err == nil {
			s := bufio.NewScanner(f)
			for s.Scan() {
				parts := strings.SplitN(s.Text(), "=", 2)
				if len(parts) == 2 && strings.TrimSpace(parts[0]) == av.Label {
					name = strings.TrimSpace(parts[1])
					break
				}
			}
			f.Close()
		}
	}

	return sl_encode(map[string]any{
		"version": av.Version,
		"label":   av.Label,
		"name":    name,
		"paths":   av.Paths,
	}), nil
}

// mochi.app.package.install(id, file, check_only?, peer?) -> string: Install an app from a .zip file, returns version
// Requires administrator role, or apps_install_user setting to be "true"
func api_app_package_install(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 4 {
		return sl_error(fn, "syntax: <app id: string>, <file: string>, [check only: boolean], [peer: string]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (id != "" && !valid(id, "entity")) {
		return sl_error(fn, "invalid ID %q", id)
	}
	if id == "" {
		id, _, _ = entity_id()
		if id == "" {
			return sl_error(fn, "unable to allocate id")
		}
	}

	file, ok := sl.AsString(args[1])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	check_only := false
	if len(args) > 2 {
		check_only = bool(args[2].Truth())
	}

	peer := ""
	if len(args) > 3 {
		peer, _ = sl.AsString(args[3])
	}
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() && setting_get("apps_install_user", "") != "true" {
		return sl_error(fn, "not administrator")
	}

	a, ok := t.Local("app").(*App)
	if !ok || a == nil {
		return sl_error(fn, "no app")
	}

	av, err := app_install(id, "", api_file_path(user, a, file), check_only, peer)
	if err != nil {
		return sl_error(fn, fmt.Sprintf("App install failed: '%v'", err))
	}

	if !check_only {
		na := app_external(id)
		na.load_version(av)
	}

	return sl_encode(av.Version), nil
}

// mochi.app.list() -> list: Get list of installed apps
func api_app_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	var results []map[string]any

	apps_lock.Lock()
	for id, a := range apps {
		if !valid(id, "entity") && !valid(id, "constant") {
			continue
		}
		if a == nil || (a.latest == nil && a.internal == nil) {
			continue
		}
		av := a.active_locked(user)
		if av == nil || !av.user_allowed(user) {
			continue
		}
		// Skip internal service apps without a Label
		if av.Label == "" {
			continue
		}
		latest := av.Version
		if a.latest != nil {
			latest = a.latest.Version
		}
		result := map[string]any{
			"id":       a.id,
			"name":     a.label(user, av, av.Label),
			"active":   av.Version,
			"latest":   latest,
			"engine":   av.Architecture.Engine,
			"icon":     av.icon(),
			"classes":  av.Classes,
			"services": av.Services,
			"paths":    av.Paths,
		}
		if len(av.Themes) > 0 {
			themes := make([]map[string]any, len(av.Themes))
			for i, t := range av.Themes {
				themes[i] = map[string]any{
					"id":     t.ID,
					"label":  a.label(user, av, t.Label),
					"hue":    t.Hue,
					"chroma": t.Chroma,
					"hue_bg": t.HueBG,
				}
			}
			result["themes"] = themes
		}
		results = append(results, result)
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"].(string)) < strings.ToLower(results[j]["name"].(string))
	})

	return sl_encode(results), nil
}

// app_theme_get returns a theme definition from a specific app, or nil if not found
func app_theme_get(user *User, app_id, theme_id string) *AppTheme {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	a := apps[app_id]
	if a == nil {
		return nil
	}
	av := a.active_locked(user)
	if av == nil {
		return nil
	}
	for i := range av.Themes {
		if av.Themes[i].ID == theme_id {
			return &av.Themes[i]
		}
	}
	return nil
}

// mochi.app.themes() -> list: Get flat list of all themes from all installed apps
func api_app_themes(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	var results []map[string]any

	// In dev, pick up theme edits in any app's app.json without requiring
	// the user to first hit that app's web route. reload() is mtime-gated,
	// so on an unchanged manifest this is one stat per app and nothing else.
	if dev_reload {
		apps_lock.Lock()
		to_reload := make([]*AppVersion, 0, len(apps))
		for _, a := range apps {
			if a.latest != nil {
				to_reload = append(to_reload, a.latest)
			}
		}
		apps_lock.Unlock()
		for _, av := range to_reload {
			av.reload()
		}
	}

	apps_lock.Lock()

	// Gather all (app, version, theme) tuples, then drop any whose visual
	// definition is byte-for-byte identical to one we've already seen. Dev
	// apps win the dedup so a developer iterating on a manifest doesn't see
	// their themes shadowed by a published copy of the same app. Diverging
	// any field — hue, spacing, override, background — produces a distinct
	// signature and both themes are shown.
	type entry struct {
		app   *App
		av    *AppVersion
		theme AppTheme
	}
	entries := make([]entry, 0)
	for _, a := range apps {
		av := a.active_locked(user)
		if av == nil || !av.user_allowed(user) {
			continue
		}
		for _, theme := range av.Themes {
			entries = append(entries, entry{app: a, av: av, theme: theme})
		}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return !is_entity_id(entries[i].app.id) && is_entity_id(entries[j].app.id)
	})

	seen := make(map[string]bool, len(entries))
	for _, e := range entries {
		sig, _ := json.Marshal(e.theme)
		if seen[string(sig)] {
			continue
		}
		seen[string(sig)] = true

		a, av, theme := e.app, e.av, e.theme
		label := a.label(user, av, theme.Label)
		if label == "" {
			label = theme.Label
		}
		result := map[string]any{
			"id":          a.id + ":" + theme.ID,
			"app":         a.id,
			"label":       label,
			"hue":         theme.Hue,
			"chroma":      theme.Chroma,
			"hue_bg":      theme.HueBG,
			"development": !is_entity_id(a.id),
		}
		if theme.BorderRadius != "" {
			result["border_radius"] = theme.BorderRadius
		}
		if theme.Spacing != "" {
			result["spacing"] = theme.Spacing
		}
		if theme.FontSans != "" {
			result["font_sans"] = theme.FontSans
		}
		if theme.FontMono != "" {
			result["font_mono"] = theme.FontMono
		}
		if theme.IconMask != "" {
			result["icon_mask"] = theme.IconMask
		}
		if theme.IconBackground != "" {
			result["icon_background"] = theme.IconBackground
		}
		if theme.Background != "" {
			result["background"] = theme.Background
			if len(av.Paths) > 0 && !strings.ContainsAny(theme.Background, `<>"`) {
				result["background_url"] = "/" + av.Paths[0] + "/backgrounds/" + theme.Background
			}
		}
		if theme.BackgroundDark != "" {
			result["background_dark"] = theme.BackgroundDark
		}
		if len(theme.Overrides) > 0 {
			result["overrides"] = theme.Overrides
		}
		results = append(results, result)
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return results[i]["label"].(string) < results[j]["label"].(string)
	})

	return sl_encode(results), nil
}

// mochi.app.presets() -> dict: Get the per-density CSS-var bundles
// referenced by a theme's spacing or the user's density override. The
// result has one entry per density ("compact", "comfortable", "spacious")
// mapping every CSS custom property the preset emits to its value. Lets
// the client apply density changes without duplicating the table.
func api_app_presets(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	result := map[string]any{}
	for _, density := range []string{"compact", "comfortable", "spacious"} {
		vars := style_preset_vars(density)
		bundle := make(map[string]any, len(vars))
		for k, v := range vars {
			bundle[k] = v
		}
		result[density] = bundle
	}
	return sl_encode(result), nil
}

// mochi.app.class.get(class) -> string | None: Get the app bound to a class (admin only)
func api_app_class_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 1 {
		return sl_error(fn, "syntax: <class: string>")
	}
	class, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid class")
	}
	app_id := apps_class_get(class)
	if app_id == "" {
		return sl.None, nil
	}
	return sl.String(app_id), nil
}

// mochi.app.class.set(class, app_id) -> bool: Bind a class to an app (admin only)
func api_app_class_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 2 {
		return sl_error(fn, "syntax: <class: string>, <app_id: string>")
	}
	class, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid class")
	}
	app_id, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}
	apps_class_set(class, app_id)
	audit_default_routing_changed(user.Username, "class", class, app_id)
	return sl.True, nil
}

// mochi.app.class.delete(class) -> bool: Remove a class binding (admin only)
func api_app_class_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 1 {
		return sl_error(fn, "syntax: <class: string>")
	}
	class, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid class")
	}
	apps_class_delete(class)
	return sl.True, nil
}

// mochi.app.class.list() -> dict: List all class bindings
func api_app_class_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	db := db_apps()
	rows, _ := db.rows("select class, app from classes")
	result := make(map[string]string)
	for _, row := range rows {
		result[row["class"].(string)] = row["app"].(string)
	}
	return sl_encode(result), nil
}

// mochi.app.service.get(service) -> string | None: Get the app bound to a service (admin only)
func api_app_service_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 1 {
		return sl_error(fn, "syntax: <service: string>")
	}
	service, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid service")
	}
	app_id := apps_service_get(service)
	if app_id == "" {
		return sl.None, nil
	}
	return sl.String(app_id), nil
}

// mochi.app.service.set(service, app_id) -> bool: Bind a service to an app (admin only)
func api_app_service_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 2 {
		return sl_error(fn, "syntax: <service: string>, <app_id: string>")
	}
	service, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid service")
	}
	app_id, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}
	apps_service_set(service, app_id)
	audit_default_routing_changed(user.Username, "service", service, app_id)
	return sl.True, nil
}

// mochi.app.service.delete(service) -> bool: Remove a service binding (admin only)
func api_app_service_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 1 {
		return sl_error(fn, "syntax: <service: string>")
	}
	service, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid service")
	}
	apps_service_delete(service)
	return sl.True, nil
}

// mochi.app.service.list() -> dict: List all service bindings
func api_app_service_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	db := db_apps()
	rows, _ := db.rows("select service, app from services")
	result := make(map[string]string)
	for _, row := range rows {
		result[row["service"].(string)] = row["app"].(string)
	}
	return sl_encode(result), nil
}

// mochi.app.path.get(path) -> string | None: Get the app bound to a path (admin only)
func api_app_path_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 1 {
		return sl_error(fn, "syntax: <path: string>")
	}
	path, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid path")
	}
	app_id := apps_path_get(path)
	if app_id == "" {
		return sl.None, nil
	}
	return sl.String(app_id), nil
}

// mochi.app.path.set(path, app_id) -> bool: Bind a path to an app (admin only)
func api_app_path_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 2 {
		return sl_error(fn, "syntax: <path: string>, <app_id: string>")
	}
	path, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid path")
	}
	app_id, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}
	apps_path_set(path, app_id)
	audit_default_routing_changed(user.Username, "path", path, app_id)
	return sl.True, nil
}

// mochi.app.path.delete(path) -> bool: Remove a path binding (admin only)
func api_app_path_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 1 {
		return sl_error(fn, "syntax: <path: string>")
	}
	path, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid path")
	}
	apps_path_delete(path)
	return sl.True, nil
}

// mochi.app.path.list() -> dict: List all path bindings
func api_app_path_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	db := db_apps()
	rows, _ := db.rows("select path, app from paths")
	result := make(map[string]string)
	for _, row := range rows {
		result[row["path"].(string)] = row["app"].(string)
	}
	return sl_encode(result), nil
}

// mochi.app.version.get(app_id) -> dict | None: Get the default version/track for an app
func api_app_version_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <app_id: string>")
	}
	app_id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}
	a := app_by_id(app_id)
	if a == nil {
		return sl.None, nil
	}
	version, track := a.default_version()
	if version == "" && track == "" {
		return sl.None, nil
	}
	return sl_encode(map[string]string{"version": version, "track": track}), nil
}

// mochi.app.version.set(app_id, version, track) -> bool: Set the default version/track for an app (admin only)
func api_app_version_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) < 1 || len(args) > 3 {
		return sl_error(fn, "syntax: <app_id: string>, [version: string], [track: string]")
	}
	app_id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}
	a := app_by_id(app_id)
	if a == nil {
		return sl_error(fn, "app not found")
	}
	version := ""
	track := ""
	if len(args) > 1 && args[1] != sl.None {
		version, _ = sl.AsString(args[1])
	}
	if len(args) > 2 && args[2] != sl.None {
		track, _ = sl.AsString(args[2])
	}
	a.set_default_version(version, track, user.Username)
	return sl.True, nil
}

// mochi.app.version.download(app_id, version) -> bool: Download a specific version from publisher
func api_app_version_download(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() && setting_get("apps_install_user", "") != "true" {
		return sl_error(fn, "not allowed to install apps")
	}
	if len(args) != 2 {
		return sl_error(fn, "syntax: <app_id: string>, <version: string>")
	}
	app_id, ok := sl.AsString(args[0])
	if !ok || !valid(app_id, "entity") {
		return sl_error(fn, "invalid app_id")
	}
	version, ok := sl.AsString(args[1])
	if !ok || !valid(version, "version") {
		return sl_error(fn, "invalid version")
	}

	// Check if already installed
	if app_has_version(app_id, version) {
		return sl.True, nil
	}

	// Download from publisher
	if !app_download_version(app_id, version) {
		return sl.False, nil
	}
	return sl.True, nil
}

// mochi.app.track.set(app_id, track, version) -> bool: Set the version for a track (admin only)
func api_app_track_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 3 {
		return sl_error(fn, "syntax: <app_id: string>, <track: string>, <version: string>")
	}
	app_id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}
	track, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid track")
	}
	version, ok := sl.AsString(args[2])
	if !ok {
		return sl_error(fn, "invalid version")
	}
	a := app_by_id(app_id)
	if a == nil {
		return sl_error(fn, "app not found")
	}
	a.set_track(track, version, user.Username)
	return sl.True, nil
}

// mochi.app.track.list(app_id) -> dict: List all tracks for an app
func api_app_track_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <app_id: string>")
	}
	app_id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}
	a := app_by_id(app_id)
	if a == nil {
		return sl_encode(map[string]string{}), nil
	}
	return sl_encode(a.tracks()), nil
}

// mochi.app.version.list(app_id) -> list: List all installed versions of an app
func api_app_version_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <app_id: string>")
	}
	app_id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}

	// For published apps (entity IDs), scan the disk for installed versions
	if valid(app_id, "entity") {
		dir := fmt.Sprintf("%s/apps/%s", data_dir, app_id)
		entries, err := file_list(dir)
		if err != nil {
			return sl_encode([]string{}), nil
		}
		var versions []string
		for _, v := range entries {
			if valid(v, "version") {
				versions = append(versions, v)
			}
		}
		sort.Strings(versions)
		return sl_encode(versions), nil
	}

	// For dev apps, use the in-memory versions map
	a := app_by_id(app_id)
	if a == nil {
		return sl_encode([]string{}), nil
	}
	apps_lock.Lock()
	var versions []string
	for v := range a.versions {
		versions = append(versions, v)
	}
	apps_lock.Unlock()
	sort.Strings(versions)
	return sl_encode(versions), nil
}

// mochi.app.cleanup() -> int: Remove unused app versions (admin only)
func api_app_cleanup(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	u, _ := t.Local("user").(*User)
	if u == nil || !u.administrator() {
		return sl_error(fn, "not administrator")
	}
	removed := apps_cleanup_unused_versions()
	return sl.MakeInt(removed), nil
}

// apps_cleanup_unused_versions removes app versions not referenced by system defaults,
// user bindings, tracks, or being the highest version (fallback).
// Returns the number of versions removed.
func apps_cleanup_unused_versions() int {
	removed := 0

	apps_lock.Lock()
	app_ids := make([]string, 0, len(apps))
	for id := range apps {
		app_ids = append(app_ids, id)
	}
	apps_lock.Unlock()

	for _, app_id := range app_ids {
		a := app_by_id(app_id)
		if a == nil {
			continue
		}

		// Collect versions in use for this app
		in_use := make(map[string]bool)

		// Highest version is always kept as fallback
		apps_lock.Lock()
		var highest string
		for v := range a.versions {
			if highest == "" || v > highest {
				highest = v
			}
		}
		apps_lock.Unlock()
		if highest != "" {
			in_use[highest] = true
		}

		// Check system defaults
		version, track := a.default_version()
		if version != "" {
			in_use[version] = true
		}
		if track != "" {
			if v := a.track(track); v != "" {
				in_use[v] = true
			}
		}

		// Check all tracks for this app
		for _, v := range a.tracks() {
			in_use[v] = true
		}

		// Check all users' version bindings
		db := db_open("db/users.db")
		rows, _ := db.rows("select uid from users where status = 'active'")
		for _, row := range rows {
			user_id, _ := row["uid"].(string)
			u := user_by_uid(user_id)
			if u == nil {
				continue
			}
			uv, ut := u.app_version(app_id)
			if uv != "" {
				in_use[uv] = true
			}
			if ut != "" {
				if v := a.track(ut); v != "" {
					in_use[v] = true
				}
			}
		}

		// Remove versions not in use
		apps_lock.Lock()
		var to_delete []string
		for v := range a.versions {
			if !in_use[v] {
				delete(a.versions, v)
				to_delete = append(to_delete, v)
				removed++
			}
		}
		apps_lock.Unlock()

		// Delete version directories from disk (only for published apps)
		if valid(app_id, "entity") {
			for _, v := range to_delete {
				dir := fmt.Sprintf("%s/apps/%s/%s", data_dir, app_id, v)
				if err := os.RemoveAll(dir); err != nil {
					warn("Failed to delete app version directory %s: %v", dir, err)
				} else {
					info("Deleted unused app version %s %s", app_id, v)
				}
			}
		}
	}

	return removed
}

// Compare two semantic version strings (e.g., "0.2", "0.3.1")
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// Comparison uses the precision of the shorter version:
// "0.2" means "any 0.2.x", so version_compare("0.2.37", "0.2") returns 0.
// "0.2.0" means exactly 0.2.0, so version_compare("0.2.37", "0.2.0") returns 1.
func version_compare(a, b string) int {
	parts_a := strings.Split(a, ".")
	parts_b := strings.Split(b, ".")

	// Compare only up to the segment count of the shorter version
	min := len(parts_a)
	if len(parts_b) < min {
		min = len(parts_b)
	}

	for i := 0; i < min; i++ {
		num_a, _ := strconv.Atoi(parts_a[i])
		num_b, _ := strconv.Atoi(parts_b[i])
		if num_a > num_b {
			return 1
		}
		if num_a < num_b {
			return -1
		}
	}
	return 0
}

// mochi.app.asset.exists(path) -> bool: Check if a file exists in the app's directory
func api_app_asset_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <path: string>")
	}

	path, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "path must be a string")
	}
	if !valid(path, "filepath") {
		return sl.False, nil
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	user, _ := t.Local("user").(*User)
	full := app_local_path(app, user, path)
	if full == "" {
		return sl.False, nil
	}

	// Reject symlinks
	if file_is_symlink(full) {
		return sl.False, nil
	}

	if file_exists(full) {
		return sl.True, nil
	}
	return sl.False, nil
}

// mochi.app.asset.list(path) -> list: List files in an app directory
func api_app_asset_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <path: string>")
	}

	path, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "path must be a string")
	}
	if !valid(path, "filepath") {
		return sl.NewList(nil), nil
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	user, _ := t.Local("user").(*User)
	full := app_local_path(app, user, path)
	if full == "" {
		return sl_encode([]string{}), nil
	}

	// Reject symlinks
	if file_is_symlink(full) {
		return sl_encode([]string{}), nil
	}

	if !file_exists(full) || !file_is_directory(full) {
		return sl_encode([]string{}), nil
	}

	entries, err := file_list(full)
	if err != nil {
		return sl_error(fn, "unable to list directory: %v", err)
	}
	return sl_encode(entries), nil
}

// mochi.app.asset.read(path) -> bytes: Read a file from the app's directory
func api_app_asset_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <path: string>")
	}

	path, ok := sl.AsString(args[0])
	if !ok || !valid(path, "filepath") {
		return sl_error(fn, "invalid path")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	user, _ := t.Local("user").(*User)
	full := app_local_path(app, user, path)
	if full == "" {
		return sl_error(fn, "no active app version")
	}

	// Reject symlinks
	if file_is_symlink(full) {
		return sl_error(fn, "file not found")
	}

	if !file_exists(full) {
		return sl_error(fn, "file not found")
	}

	data, err := os.ReadFile(full)
	if err != nil {
		return sl_error(fn, "unable to read file: %v", err)
	}
	return sl.Bytes(data), nil
}
