// Mochi server: Apps
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
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
	active      *AppVersion
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
		Role string `json:"role"`
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
	Icons     []Icon                 `json:"icons"`
	Actions   map[string]AppAction   `json:"actions"`
	Events    map[string]AppEvent    `json:"events"`
	Functions map[string]AppFunction `json:"functions"`

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

// Get the primary path for URL generation
func (a *App) url_path() string {
	if a.active != nil && len(a.active.Paths) > 0 {
		return a.active.Paths[0]
	}
	return a.id
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
	app_version_maximum = 2
)

var (
	// Default apps to install, in priority order (Login and Home first for usability)
	apps_install_by_default = []string{
		"1FLjnMyW4ozYZhNMqkXTWYgjcoHA7Wif3B3UeAe45chxWnuP1F",  // Login
		"12YGtmNxgihPn2cmNSpKfpViFWtWH25xYT7o6xKnTXCA2deNvjH", // Home
		"12kqLEaEE9L3mh6modywUmo8TC3JGi3ypPZR2N2KqAMhB3VBFdL", // App Manager
		"1PfwgL5rwmRW9HNqX1UNfjubHue7JsbZG8ft3C1fUzxfZT1e92",  // Chat
		"12254aHfG39LqrizhydT6iYRCTAZqph1EtAkVTR7DcgXZKWqRrj", // Feeds
		"12PGVUZUrLqgfqp1ovH8ejfKpAQq6uXbrcCqtoxWHjcuxWDxZbt", // Forums
		"12ZwHwqDLsdN5FMLcHhWBrDwwYojNZ67dWcZiaynNFcjuHPnx2P", // Notifications
		"1gGcjxdhV2VjuEMLs7UZiQwMaY2jvx1ARbu8g9uqM5QeS2vFJV",  // People
		"1FEuUQ9D5usB16Rb5d2QruSbVr6AYqaLkcu3DLhpqCA49VF8Ky",  // Settings
		"12QcwPkeTpYmxjaYXtA56ff5jMzJYjMZCmV5RpQR1GosFPRXDtf", // Wikis
	}
	apps_bootstrap_ready = false // True once Login and Home are installed
	apps                 = map[string]*App{}
	apps_lock            = &sync.Mutex{}

	api_app_file = sls.FromStringDict(sl.String("mochi.app.file"), sl.StringDict{
		"get":     sl.NewBuiltin("mochi.app.file.get", api_app_file_get),
		"install": sl.NewBuiltin("mochi.app.file.install", api_app_file_install),
	})

	api_app = sls.FromStringDict(sl.String("mochi.app"), sl.StringDict{
		"file":  api_app_file,
		"get":   sl.NewBuiltin("mochi.app.get", api_app_get),
		"icons": sl.NewBuiltin("mochi.app.icons", api_app_icons),
		"list":  sl.NewBuiltin("mochi.app.list", api_app_list),
	})
)

// Get existing app, loading it into memory as new app if necessary
func app(id string) *App {
	apps_lock.Lock()
	a, found := apps[id]
	apps_lock.Unlock()

	if !found {
		a = &App{id: id, fingerprint: fingerprint(id), versions: make(map[string]*AppVersion)}
		a.active = &AppVersion{}
		a.active.app = a
		a.active.Actions = make(map[string]AppAction)
		a.active.Events = make(map[string]AppEvent)

		apps_lock.Lock()
		apps[id] = a
		apps_lock.Unlock()
	}

	return a
}

// Get an app by id, fingerprint, or path
func app_by_any(s string) *App {
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
		av := a.active
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
func app_by_root() *App {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	for _, a := range apps {
		if a.active == nil {
			continue
		}
		for _, p := range a.active.Paths {
			if p == "" {
				return a
			}
		}
	}
	return nil
}

// Check whether app is the correct version, and if not download and install new version
func app_check_install(id string) bool {
	if !valid(id, "entity") {
		debug("App %q ignoring install status", id)
		return true
	}
	debug("App %q checking install status", id)

	// Check if app is already installed
	apps_lock.Lock()
	a := apps[id]
	apps_lock.Unlock()
	installed := a != nil && a.active != nil

	// Check version (always try fallback if entity location is unknown)
	version, ok := app_check_version(id)
	if !ok {
		return false
	}

	if installed {
		if a.active.Version == version {
			debug("App %q keeping at version %q", id, a.active.Version)
			return true
		} else {
			debug("App %q upgrading from %q to %q", id, a.active.Version, version)
		}
	}

	// Download and install new version (always try fallback if entity location is unknown)
	s, err := stream("", id, "publisher", "get")
	if err != nil {
		s, err = stream_to_peer(peer_default_publisher, "", id, "publisher", "get")
	}
	if err != nil {
		debug("%v", err)
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
	respStatus, _ := response["status"].(string)
	if respStatus != "200" {
		return false
	}

	zip := fmt.Sprintf("%s/tmp/app_%s_%s.zip", cache_dir, id, version)
	if !file_write_from_reader(zip, s.reader) {
		file_delete(zip)
		return false
	}

	new, err := app_install(id, version, zip, false)
	if err != nil {
		file_delete(zip)
		return false
	}

	na := app(id)
	na.load_version(new)
	return true
}

// Check the version of an app on the remote server
func app_check_version(id string) (string, bool) {
	s, err := stream("", id, "publisher", "version")
	if err != nil {
		debug("App %q using fallback to default publisher", id)
		s, err = stream_to_peer(peer_default_publisher, "", id, "publisher", "version")
	}
	if err != nil {
		debug("App %q version check failed: %v", id, err)
		return "", false
	}
	defer s.close()

	s.write_content("track", "production")

	statusResp, err := s.read_content()
	if err != nil {
		debug("%v", err)
		return "", false
	}
	statusCode, _ := statusResp["status"].(string)
	if statusCode != "200" {
		return "", false
	}

	v, err := s.read_content()
	if err != nil {
		debug("%v", err)
		return "", false
	}
	version, _ := v["version"].(string)
	if !valid(version, "version") {
		return "", false
	}

	return version, true
}

// Find the best app for a service
func app_for_service(service string) *App {
	apps_lock.Lock()
	defer apps_lock.Unlock()

	for _, a := range apps {
		for _, candidate := range a.active.Services {
			if candidate == service {
				return a
			}
		}
	}

	// Handle "app/<id>" service names used by attachment federation
	if strings.HasPrefix(service, "app/") {
		app_id := service[4:]
		if a, found := apps[app_id]; found {
			return a
		}
	}

	return nil
}

// Install an app from a zip file, but do not load it
func app_install(id string, version string, file string, check_only bool) (*AppVersion, error) {
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

	av.base = fmt.Sprintf("%s/apps/%s/%s", data_dir, id, av.Version)
	if file_exists(av.base) {
		debug("App %q removing old copy of version %q in %q", id, av.Version, av.base)
		file_delete_all(av.base)
	}
	file_move(tmp, av.base)

	debug("App %q version %q installed", id, av.Version)
	return av, nil
}

// Manage which apps and their versions are installed
func apps_manager() {
	time.Sleep(time.Second)
	for {
		todo := map[string]bool{}

		// Install default apps in priority order (Login and Home first)
		for i, id := range apps_install_by_default {
			todo[id] = true
			app_check_install(id)

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

	if !valid(av.Label, "constant") {
		return nil, fmt.Errorf("App bad label %q", av.Label)
	}

	for _, class := range av.Classes {
		if !valid(class, "constant") {
			return nil, fmt.Errorf("App bad class %q", class)
		}
	}

	for _, path := range av.Paths {
		if !valid(path, "path") {
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

// Check if a path is already used by another app (excluding the given app ID)
func app_path_taken(path string, exclude string) bool {
	apps_lock.Lock()
	defer apps_lock.Unlock()
	for _, a := range apps {
		if a.id == exclude {
			continue
		}
		if a.active == nil {
			continue
		}
		for _, p := range a.active.Paths {
			if p == path {
				return true
			}
		}
	}
	return false
}

// Load development apps from dev_apps_dir (unversioned)
func apps_load_dev() {
	for _, id := range file_list(dev_apps_dir) {
		if strings.HasPrefix(id, ".") {
			continue
		}

		// Dev apps must have constant IDs
		if !valid(id, "constant") {
			debug("Dev app skipping invalid ID %q (must be constant)", id)
			continue
		}

		// Read app.json directly (no version subdirectory)
		base := dev_apps_dir + "/" + id
		av, err := app_read(id, base)
		if err != nil {
			info("Dev app load error for %q: %v", id, err)
			continue
		}

		a := app(id)
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
		a := app(id)

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
			// If any path conflicts, use the fingerprint as the mount point
			// TODO: Remove this workaround in v0.3 when multiple versions of the same app
			// can run simultaneously and users choose which version to use.
			for _, path := range av.Paths {
				if app_path_taken(path, id) {
					fp := fingerprint(id)
					debug("Published app %s path %q conflicts, using fingerprint %s", id, path, fp)
					av.Paths = []string{fp}
					break
				}
			}

			a.load_version(av)
		}
	}
}

// Register an action for an internal app
func (a *App) action(action string, f func(*Action)) {
	a.active.Actions[action] = AppAction{name: action, internal_function: f}
}

// Register a broadcast for an internal app
func (a *App) broadcast(sender string, action string, f func(*User, string, string, string, any)) {
	a.active.broadcasts[sender+"/"+action] = f
}

// Register the user database file for an internal app
func (a *App) db(file string, create func(*DB)) {
	a.active.Database.File = file
	a.active.Database.create_function = create
}

// Register an event handler for an internal app
func (a *App) event(event string, f func(*Event)) {
	a.active.Events[event] = AppEvent{internal_function: f}
}

// Register an anonymous event handler for an internal app
func (a *App) event_anonymous(event string, f func(*Event)) {
	a.active.Events[event] = AppEvent{internal_function: f, Anonymous: true}
}

// Register an icon for an internal app
func (a *App) icon(action string, label string, file string) {
	a.active.Icons = append(a.active.Icons, Icon{Action: action, Label: label, File: file})
}

// Resolve an app label
func (a *App) label(u *User, key string, values ...any) string {
	language := "en"
	if u != nil {
		language = user_preference_get(u, "language", "en")
	}

	labels := a.active.labels
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

// Loads details of a new version, and if it's the latest activate it
func (a *App) load_version(av *AppVersion) {
	if a == nil || av == nil {
		return
	}

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
		if v > latest {
			latest = v
		}
	}
	if latest == av.Version {
		a.active = av
	}
	apps_lock.Unlock()

	if latest == av.Version {
		debug("App %q, %q version %q activated", av.labels["en"][av.Label], a.id, av.Version)
	} else {
		debug("App %q, %q version %q loaded, but not activated", av.labels["en"][av.Label], a.id, av.Version)
	}
}

// Register a path for an internal app
func (a *App) path(path string) {
	a.active.Paths = append(a.active.Paths, path)
}

// Register a service for an internal app
func (a *App) service(service string) {
	a.active.Services = append(a.active.Services, service)
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
		if len(key_segments) != len(value_segments) {
			continue
		}

		ok := true
		for i := 0; i < len(key_segments); i++ {
			ks := key_segments[i]
			vs := value_segments[i]
			if strings.HasPrefix(ks, ":") {
				name := ks[1:]
				aa.parameters[name] = vs
			} else if ks != vs {
				ok = false
				break
			}
		}

		if ok {
			//debug("App found action %q with function %q via pattern %q", name, aa.Function, aa.name)
			return &aa
		}
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
		return sl_encode(map[string]string{"id": a.id, "name": a.label(user, a.active.Label), "latest": a.active.Version}), nil
	}

	return sl.None, nil
}

// mochi.app.icons() -> list: Get available icons for home screen
func api_app_icons(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	var results []map[string]string

	apps_lock.Lock()
	for _, a := range apps {
		if !a.active.user_allowed(user) {
			continue
		}
		for _, i := range a.active.Icons {
			path := a.fingerprint
			if len(a.active.Paths) > 0 {
				path = a.active.Paths[0]
			}
			if i.Action != "" {
				path = path + "/" + i.Action
			}
			results = append(results, map[string]string{"id": a.id, "path": path, "name": a.label(user, i.Label), "file": i.File})
		}
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return sl_encode(results), nil
}

// mochi.app.file.get(file) -> dict: Read app info from a .zip file without installing
func api_app_file_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
	labelsPath := tmp + "/labels/en.conf"
	if file_exists(labelsPath) {
		f, err := os.Open(labelsPath)
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

// mochi.app.file.install(id, file, check_only?) -> string: Install an app from a .zip file, returns version
func api_app_file_install(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return sl_error(fn, "syntax: <app id: string>, <file: string>, [ check only: boolean]")
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
	debug("api_app_install() check only '%v'", check_only)

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	a, ok := t.Local("app").(*App)
	if !ok || a == nil {
		return sl_error(fn, "no app")
	}

	av, err := app_install(id, "", api_file_path(user, a, file), check_only)
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
	var results []map[string]string

	apps_lock.Lock()
	for id, a := range apps {
		if !valid(id, "entity") && !valid(id, "constant") {
			continue
		}
		if a == nil || a.active == nil {
			continue
		}
		if !a.active.user_allowed(user) {
			continue
		}
		results = append(results, map[string]string{"id": a.id, "name": a.label(user, a.active.Label), "latest": a.active.Version, "engine": a.active.Architecture.Engine})
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return sl_encode(results), nil
}
