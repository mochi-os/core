// Mochi server: Apps
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
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
	Public    bool   `json:"public"`
	OpenGraph string `json:"opengraph"` // Starlark function to generate Open Graph meta tags
	Access    struct {
		Resource  string `json:"resource"`
		Operation string `json:"operation"`
	} `json:"access"`

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
	internal_function func(*Event) `json:"-"`
}

type AppFunction struct {
	Function string `json:"function"`
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
		create_function func(*DB) `json:"-"`
	} `json:"database"`
	Icon      string                 `json:"icon"`
	Icons     []Icon                 `json:"icons"`
	Actions   map[string]AppAction   `json:"actions"`
	Events    map[string]AppEvent    `json:"events"`
	Functions map[string]AppFunction `json:"functions"`
	Publisher struct {
		Peer string `json:"peer,omitempty"`
	} `json:"publisher,omitempty"`

	app              *App                                                `json:"-"`
	base             string                                              `json:"-"`
	labels           map[string]map[string]string                        `json:"-"`
	starlark_runtime *Starlark                                           `json:"-"`
	broadcasts       map[string]func(*User, string, string, string, any) `json:"-"`
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
	// Internal Go apps have a single version
	if a.internal != nil {
		return a.internal
	}

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
	app_version_maximum = 3
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
	// Default apps to install, in priority order (Login and Home first for usability)
	apps_default = []DefaultApp{
		{"1FLjnMyW4ozYZhNMqkXTWYgjcoHA7Wif3B3UeAe45chxWnuP1F", "Login", nil},
		{"12YGtmNxgihPn2cmNSpKfpViFWtWH25xYT7o6xKnTXCA2deNvjH", "Home", nil},
		{"12kqLEaEE9L3mh6modywUmo8TC3JGi3ypPZR2N2KqAMhB3VBFdL", "Apps", []struct{ Permission, Object string }{
			{"permission/manage", ""},
		}},
		{"1PfwgL5rwmRW9HNqX1UNfjubHue7JsbZG8ft3C1fUzxfZT1e92", "Chat", []struct{ Permission, Object string }{
			{"service", "friends"},
			{"service", "notifications"},
		}},
		{"12254aHfG39LqrizhydT6iYRCTAZqph1EtAkVTR7DcgXZKWqRrj", "Feeds", []struct{ Permission, Object string }{
			{"service", "friends"},
			{"service", "notifications"},
		}},
		{"12PGVUZUrLqgfqp1ovH8ejfKpAQq6uXbrcCqtoxWHjcuxWDxZbt", "Forums", []struct{ Permission, Object string }{
			{"service", "friends"},
		}},
		{"12ZwHwqDLsdN5FMLcHhWBrDwwYojNZ67dWcZiaynNFcjuHPnx2P", "Notifications", []struct{ Permission, Object string }{
			{"webpush/send", ""},
			{"account/read", ""},
			{"account/manage", ""},
			{"account/notify", ""},
		}},
		{"1gGcjxdhV2VjuEMLs7UZiQwMaY2jvx1ARbu8g9uqM5QeS2vFJV", "People", []struct{ Permission, Object string }{
			{"group/manage", ""},
			{"user/read", ""},
			{"service", "notifications"},
		}},
		{"1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky", "Settings", []struct{ Permission, Object string }{
			{"setting/write", ""},
			{"user/read", ""},
			{"account/read", ""},
			{"account/manage", ""},
		}},
		{"12QcwPkeTpYmxjaYXtA56ff5jMzJYjMZCmV5RpQR1GosFPRXDtf", "Wikis", []struct{ Permission, Object string }{
			{"service", "friends"},
		}},
		{"test", "Test", []struct{ Permission, Object string }{
			{"account/read", ""},
			{"account/manage", ""},
		}},
	}
	apps_bootstrap_ready = false // True once Login and Home are installed
	apps                 = map[string]*App{}
	apps_lock            = &sync.Mutex{}

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
		"set":      sl.NewBuiltin("mochi.app.version.set", api_app_version_set),
	})

	api_app_track = sls.FromStringDict(sl.String("mochi.app.track"), sl.StringDict{
		"get":  sl.NewBuiltin("mochi.app.track.get", api_app_track_get),
		"set":  sl.NewBuiltin("mochi.app.track.set", api_app_track_set),
		"list": sl.NewBuiltin("mochi.app.track.list", api_app_track_list),
	})

	api_app_file = sls.FromStringDict(sl.String("mochi.app.file"), sl.StringDict{
		"exists": sl.NewBuiltin("mochi.app.file.exists", api_app_file_exists),
		"list":   sl.NewBuiltin("mochi.app.file.list", api_app_file_list),
		"read":   sl.NewBuiltin("mochi.app.file.read", api_app_file_read),
	})

	api_app = sls.FromStringDict(sl.String("mochi.app"), sl.StringDict{
		"class":    api_app_class,
		"cleanup":  sl.NewBuiltin("mochi.app.cleanup", api_app_cleanup),
		"file":     api_app_file,
		"get":      sl.NewBuiltin("mochi.app.get", api_app_get),
		"icons":    sl.NewBuiltin("mochi.app.icons", api_app_icons),
		"list":     sl.NewBuiltin("mochi.app.list", api_app_list),
		"package":  api_app_package,
		"path":     api_app_path,
		"service":  api_app_service,
		"track":    api_app_track,
		"tracks":   sl.NewBuiltin("mochi.app.tracks", api_app_tracks),
		"version":  api_app_version,
		"versions": sl.NewBuiltin("mochi.app.versions", api_app_versions),
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
	debug("App %q checking install status", id)

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
	s, err := stream("", id, "publisher", "version")
	if err != nil {
		debug("App %q using fallback to default publisher", id)
		s, err = stream_to_peer(peer_default_publisher, "", id, "publisher", "version")
	}
	if err != nil {
		debug("App %q version check failed: %v", id, err)
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

	// Build tracks map - try new format first, fall back to old format
	// TODO(0.3-cleanup): Remove old format fallback when all servers are 0.3
	tracks := make(map[string]string)
	if tracksArray, ok := v["tracks"].([]interface{}); ok {
		// New format (0.3+) - parse all tracks
		for _, t := range tracksArray {
			if tm, ok := t.(map[string]interface{}); ok {
				track, _ := tm["track"].(string)
				version, _ := tm["version"].(string)
				if valid(track, "constant") && valid(version, "version") {
					tracks[track] = version
				}
			}
		}
	} else {
		// Old format (0.2) - single track only
		track, _ := v["track"].(string)
		if !valid(track, "constant") {
			track = "Production"
		}
		tracks[track] = default_version
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

	s, err := stream("", id, "publisher", "get")
	if err != nil {
		s, err = stream_to_peer(peer_default_publisher, "", id, "publisher", "get")
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
	if !file_write_from_reader(zip, s.reader) {
		file_delete(zip)
		return false
	}

	av, err := app_install(id, version, zip, false)
	if err != nil {
		file_delete(zip)
		return false
	}

	app_resolve_paths(av, id)

	a := app(id)
	a.load_version(av)
	debug("App %q version %q installed", id, version)
	return true
}

// Find the best app for a service (prefers dev apps over published apps)
func app_for_service(service string) *App {
	// Use the fallback logic which properly prefers dev apps
	if a := app_for_service_fallback(nil, service); a != nil {
		return a
	}

	apps_lock.Lock()
	defer apps_lock.Unlock()

	// Handle "app/<id>" service names used by attachment federation
	if strings.HasPrefix(service, "app/") {
		app_id := service[4:]
		if a, found := apps[app_id]; found {
			return a
		}
	}

	// Handle app entity ID as service (for mochi.remote.stream calls)
	if a, found := apps[service]; found {
		return a
	}

	return nil
}

// app_for_service_for finds the best app for a service with user preferences.
// Resolution order:
// 1. User's binding (if user is not nil)
// 2. System binding (in apps.db)
// 3. Fallback: First app that declares this service (dev apps first, then by install time)
func app_for_service_for(user *User, service string) *App {
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
	return app_for_service_fallback(user, service)
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

// app_for_path_for finds the best app for a URL path with user preferences.
// Resolution order:
// 1. User's binding (if user is not nil)
// 2. System binding (in apps.db)
// 3. Fallback: First app that declares this path (dev apps first, then by install time)
func app_for_path_for(user *User, path string) *App {
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

// class_app_for finds the best app for a class with user preferences.
// Resolution order:
// 1. User's binding (if user is not nil)
// 2. System binding (in apps.db)
// 3. Fallback: First app that declares this class (dev apps first, then by install time)
func class_app_for(user *User, class string) *App {
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

// apps_class_set binds a class to an app ID
func apps_class_set(class, app string) {
	db := db_apps()
	db.exec("replace into classes (class, app) values (?, ?)", class, app)
}

// apps_class_delete removes a class binding
func apps_class_delete(class string) {
	db := db_apps()
	db.exec("delete from classes where class = ?", class)
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

// apps_service_set binds a service to an app ID
func apps_service_set(service, app string) {
	db := db_apps()
	db.exec("replace into services (service, app) values (?, ?)", service, app)
}

// apps_service_delete removes a service binding
func apps_service_delete(service string) {
	db := db_apps()
	db.exec("delete from services where service = ?", service)
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

// apps_path_set binds a path to an app ID
func apps_path_set(path, app string) {
	db := db_apps()
	db.exec("replace into paths (path, app) values (?, ?)", path, app)
}

// apps_path_delete removes a path binding
func apps_path_delete(path string) {
	db := db_apps()
	db.exec("delete from paths where path = ?", path)
}

// apps_record records an app installation timestamp (only if not already recorded)
func apps_record(app string) {
	db := db_apps()
	db.exec("insert or ignore into apps (app, installed) values (?, ?)", app, now())
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
	file_mkdir(data_dir + "/tmp")
	tmp := fmt.Sprintf("%s/tmp/app_install_%s_%s", data_dir, id, random_alphanumeric(8))
	err := unzip(file, tmp)
	if err != nil {
		info("App unzip failed: %v", err)
		file_delete_all(tmp)
		return nil, err
	}

	av, err := app_read(id, tmp)
	if err != nil {
		info("App read failed: %v", err)
		file_delete_all(tmp)
		return nil, err
	}

	if version != "" && version != av.Version {
		file_delete_all(tmp)
		return nil, fmt.Errorf("Specified version does not match file version")
	}

	if check_only {
		debug("App %q not installing", id)
		file_delete_all(tmp)
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
		file_delete_all(av.base)
	}
	file_move(tmp, av.base)

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
	data := file_read(path)
	if data == nil {
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

	file_write(path, output)
	debug("Wrote publisher peer %q to %s", peer, path)
}

// Manage which apps and their versions are installed
func apps_manager() {
	time.Sleep(time.Second)

	// If we already have apps installed, skip the setup wait
	if len(file_list(data_dir+"/apps")) >= 2 {
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
		for _, id := range file_list(data_dir + "/apps") {
			if !todo[id] {
				todo[id] = true
				app_check_install(id)
			}
		}

		time.Sleep(24 * time.Hour)
	}
}

// Read in an external app version from a directory
func app_read(id string, base string) (*AppVersion, error) {
	debug("App loading %q", base)

	// Load app manifest from app.json
	if !file_exists(base + "/app.json") {
		return nil, fmt.Errorf("App %q in %q has no app.json file; ignoring", id, base)
	}

	var av AppVersion
	data, err := hujson.Standardize(file_read(base + "/app.json"))
	if err != nil {
		return nil, fmt.Errorf("App bad app.json '%s/app.json': %v", base, err)
	}
	err = json.Unmarshal(data, &av)
	if err != nil {
		return nil, fmt.Errorf("App bad app.json '%s/app.json': %v", base, err)
	}

	av.base = base

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

		if a.Access.Resource != "" {
			if !valid(a.Access.Resource, "parampath") {
				return nil, fmt.Errorf("action %q has invalid access resource", action)
			}
			if a.Access.Operation == "" {
				return nil, fmt.Errorf("action %q has access resource but no access operation", action)
			}
			if !valid(a.Access.Operation, "constant") {
				return nil, fmt.Errorf("action %q has invalid access operation", action)
			}
		}
		if a.Access.Operation != "" && a.Access.Resource == "" {
			return nil, fmt.Errorf("action %q has access operation but no access resource", action)
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
			debug("Published app %s path %q conflicts, using fingerprint %s", id, path, fp)
			av.Paths = []string{fp}
			return
		}
	}
}

// Load development apps from dev_apps_dir (unversioned)
func apps_load_dev() {
	for _, id := range file_list(dev_apps_dir) {
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
	for _, id := range file_list(data_dir + "/apps") {
		if strings.HasPrefix(id, ".") {
			continue
		}

		// Skip non-directories
		if !file_is_directory(data_dir + "/apps/" + id) {
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

		versions := file_list(data_dir + "/apps/" + id)
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

			av, err := app_read(id, fmt.Sprintf("%s/apps/%s/%s", data_dir, id, version))
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

// Register an action for an internal app
func (a *App) action(action string, f func(*Action)) {
	a.internal.Actions[action] = AppAction{name: action, internal_function: f}
}

// Register a broadcast for an internal app
func (a *App) broadcast(sender string, action string, f func(*User, string, string, string, any)) {
	a.internal.broadcasts[sender+"/"+action] = f
}

// Register the user database file for an internal app
func (a *App) db(file string, create func(*DB)) {
	a.internal.Database.File = file
	a.internal.Database.create_function = create
}

// Register an event handler for an internal app
func (a *App) event(event string, f func(*Event)) {
	a.internal.Events[event] = AppEvent{internal_function: f}
}

// Register an anonymous event handler for an internal app
func (a *App) event_anonymous(event string, f func(*Event)) {
	a.internal.Events[event] = AppEvent{internal_function: f, Anonymous: true}
}

// Register an icon for an internal app
func (a *App) icon(action string, label string, file string) {
	a.internal.Icons = append(a.internal.Icons, Icon{Action: action, Label: label, File: file})
}

// Resolve an app label
func (a *App) label(u *User, av *AppVersion, key string, values ...any) string {
	language := "en"
	if u != nil {
		language = user_preference_get(u, "language", "en")
	}

	labels := av.labels
	if labels == nil {
		labels = map[string]map[string]string{}
	}

	format := ""
	if labels[language] != nil {
		format = labels[language][key]
	}
	if format == "" && labels["en"] != nil {
		format = labels["en"][key]
	}
	if format == "" {
		info("App label %q in language %q not set", key, language)
		return key
	}

	return fmt.Sprintf(format, values...)
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
	for _, file := range file_list(av.base + "/labels") {
		language := strings.TrimSuffix(file, ".conf")
		if !valid(language, "constant") {
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

	debug("App %q, %q version %q loaded", av.labels["en"][av.Label], a.id, av.Version)
}

// Register a path for an internal app
func (a *App) path(path string) {
	a.internal.Paths = append(a.internal.Paths, path)
}

// Register a service for an internal app
func (a *App) service(service string) {
	a.internal.Services = append(a.internal.Services, service)
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
			if !strings.HasPrefix(s, ":") {
				aa.literals++
			}
		}
		aa.parameters = map[string]string{}
		candidates = append(candidates, aa)
	}

	// Sort candidates: type files first, then more segments first, then more literals first
	sort.Slice(candidates, func(i, j int) bool {
		if (candidates[i].Files != "") != (candidates[j].Files != "") {
			return candidates[i].Files != ""
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
					//debug("App found files action %q via pattern %q, filepath %q", name, aa.name, suffix)
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
						//debug("App found files action %q via pattern %q, filepath %q", name, aa.name, suffix)
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

		//debug("App matched %q to pattern %q, params=%v", name, aa.name, aa.parameters)
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
	if av.starlark_runtime == nil {
		av.starlark_runtime = starlark(av.Execute)
	}
	return av.starlark_runtime
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

// Reload app.json and labels from disk (for development)
func (av *AppVersion) reload() {
	if av.base == "" {
		return
	}
	path := av.base + "/app.json"
	data, err := hujson.Standardize(file_read(path))
	if err != nil {
		info("App reload failed to read %q: %v", path, err)
		return
	}

	var fresh AppVersion
	if err := json.Unmarshal(data, &fresh); err != nil {
		info("App reload failed to parse %q: %v", path, err)
		return
	}

	// Reload labels
	labels := make(map[string]map[string]string)
	for _, file := range file_list(av.base + "/labels") {
		language := strings.TrimSuffix(file, ".conf")
		if !valid(language, "constant") {
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

	// Update fields that are safe to reload
	apps_lock.Lock()
	av.Label = fresh.Label
	av.Icons = fresh.Icons
	av.Actions = fresh.Actions
	av.Events = fresh.Events
	av.Functions = fresh.Functions
	av.labels = labels
	apps_lock.Unlock()
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

// mochi.app.icons() -> list: Get available icons for home screen
func api_app_icons(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	var results []map[string]string

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
			if i.Action != "" {
				path = path + "/" + i.Action
			}
			results = append(results, map[string]string{"id": a.id, "path": path, "name": a.label(user, av, i.Label), "file": i.File})
		}
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return sl_encode(results), nil
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
	file_mkdir(data_dir + "/tmp")
	tmp := fmt.Sprintf("%s/tmp/app_info_%s", data_dir, random_alphanumeric(8))
	err := unzip(api_file_path(user, a, file), tmp)
	if err != nil {
		file_delete_all(tmp)
		return sl_error(fn, "failed to unzip: %v", err)
	}
	defer file_delete_all(tmp)

	// Read app.json
	if !file_exists(tmp + "/app.json") {
		return sl_error(fn, "no app.json in archive")
	}

	var av AppVersion
	data, err := hujson.Standardize(file_read(tmp + "/app.json"))
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
	debug("api_app_install() check only '%v' peer '%v'", check_only, peer)

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
		na := app(id)
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
		results = append(results, map[string]any{
			"id":       a.id,
			"name":     a.label(user, av, av.Label),
			"active":   av.Version,
			"latest":   latest,
			"engine":   av.Architecture.Engine,
			"icon":     av.icon(),
			"classes":  av.Classes,
			"services": av.Services,
			"paths":    av.Paths,
		})
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"].(string)) < strings.ToLower(results[j]["name"].(string))
	})

	return sl_encode(results), nil
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
		return sl_error(fn, "not authorized to install apps")
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

// mochi.app.track.get(app_id, track) -> string | None: Get the version for a track (admin only)
func api_app_track_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
	if len(args) != 2 {
		return sl_error(fn, "syntax: <app_id: string>, <track: string>")
	}
	app_id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid app_id")
	}
	track, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid track")
	}
	a := app_by_id(app_id)
	if a == nil {
		return sl.None, nil
	}
	version := a.track(track)
	if version == "" {
		return sl.None, nil
	}
	return sl.String(version), nil
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

// mochi.app.track.list(app_id) -> dict: List all tracks for an app (admin only)
func api_app_track_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	if user == nil || !user.administrator() {
		return sl_error(fn, "not administrator")
	}
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

// mochi.app.tracks(app_id) -> dict: List all tracks for an app
func api_app_tracks(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

// mochi.app.versions(app_id) -> list: List all installed versions of an app
func api_app_versions(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
		var versions []string
		for _, v := range file_list(dir) {
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
		rows, _ := db.rows("select id from users where status = 'active'")
		for _, row := range rows {
			user_id := int(row["id"].(int64))
			u := user_by_id(user_id)
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

// mochi.app.file.exists(path) -> bool: Check if a file exists in the app's directory
func api_app_file_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

// mochi.app.file.list(path) -> list: List files in an app directory
func api_app_file_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

	return sl_encode(file_list(full)), nil
}

// mochi.app.file.read(path) -> bytes: Read a file from the app's directory
func api_app_file_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

	return sl.Bytes(file_read(full)), nil
}
