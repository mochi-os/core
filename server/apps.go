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

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

type App struct {
	id          string                 `json:"id"`
	fingerprint string                 `json:"-"`
	versions    map[string]*AppVersion `json:"-"`
	active      *AppVersion            `json:"-"`
}

type AppAction struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Files    string `json:"files"`
	Public   bool   `json:"public"`
	Access   struct {
		Resource  string `json:"resource"`
		Operation string `json:"operation"`
	} `json:"access"`

	name              string            `json:"-"`
	internal_function func(*Action)     `json:"-"`
	segments          int               `json:"-"`
	literals          int               `json:"-"`
	parameters        map[string]string `json:"-"`
}

type AppEvent struct {
	Function          string       `json:"function"`
	internal_function func(*Event) `json:"-"`
}

type AppFunction struct {
	Function string `json:"function"`
}

type AppVersion struct {
	Version      string   `json:"version"`
	Label        string   `json:"label"`
	Classes      []string `json:"classes"`
	Paths        []string `json:"paths"`
	Services     []string `json:"services"`
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
		Helpers         []string  `json:"helpers"`
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

const (
	app_version_minimum = 2
	app_version_maximum = 2
)

var (
	apps_install_by_default = []string{
		"12qMc1J5PZJDmgbdxtjB1b8xWeA6zhFJUbz5wWUEJSK3gyeFUPb", // Home
		"123jjo8q9kx8HZHmxbQ6DMfWPsMSByongGbG3wTrywcm2aA5b8x", // Notifications
		"12Wa5korrLAaomwnwj1bW4httRgo6AXHNK1wgSZ19ewn8eGWa1C", // Friends
		"1KKFKiz49rLVfaGuChexEDdphu4dA9tsMroNMfUfC7oYuruHRZ",  // Chat
	}
	apps        = map[string]*App{}
	apps_lock   = &sync.Mutex{}
	app_helpers = make(map[string]func(*DB))

	api_app = sls.FromStringDict(sl.String("mochi.app"), sl.StringDict{
		"get":     sl.NewBuiltin("mochi.app.get", api_app_get),
		"icons":   sl.NewBuiltin("mochi.app.icons", api_app_icons),
		"install": sl.NewBuiltin("mochi.app.install", api_app_install),
		"list":    sl.NewBuiltin("mochi.app.list", api_app_list),
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

// Check whether app is the correct version, and if not download and install new version
func app_check_install(id string) bool {
	debug("App %q checking install status", id)

	s, err := stream("", id, "app", "version")
	if err != nil {
		debug("%v", err)
		return false
	}
	s.write_content("track", "production")

	statusResp, err := s.read_content()
	if err != nil {
		debug("%v", err)
		return false
	}
	statusCode, _ := statusResp["status"].(string)
	if statusCode != "200" {
		return false
	}

	v, err := s.read_content()
	if err != nil {
		debug("%v", err)
		return false
	}
	version, _ := v["version"].(string)
	if !valid(version, "version") {
		return false
	}

	apps_lock.Lock()
	a := apps[id]
	apps_lock.Unlock()
	if a != nil && a.active != nil && a.active.Version == version {
		debug("App %q keeping at version %q", id, a.active.Version)
		return true
	}

	oldVersion := ""
	if a != nil && a.active != nil {
		oldVersion = a.active.Version
	}
	debug("App %q upgrading from %q to %q", id, oldVersion, version)

	s, err = stream("", id, "app", "get")
	if err != nil {
		debug("%v", err)
		return false
	}

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

	return nil
}

// Register a helper for all apps
func app_helper(name string, setup func(*DB)) {
	app_helpers[name] = setup
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
	debug("App unzipping into tmp directory %q", tmp)

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
	debug("Moving unzipped app from %q to %q", tmp, av.base)
	file_move(tmp, av.base)

	debug("App %q version %q installed", id, av.Version)
	return av, nil
}

// Manage which apps and their versions are installed
func apps_manager() {
	time.Sleep(time.Second)
	for {
		todo := map[string]bool{}

		for _, id := range apps_install_by_default {
			todo[id] = true
		}

		for _, id := range file_list(data_dir + "/apps") {
			if valid(id, "entity") {
				todo[id] = true
			}
		}

		failed := false
		for id := range todo {
			if !app_check_install(id) {
				failed = true
			}
		}

		if failed {
			time.Sleep(time.Minute)
		} else {
			time.Sleep(24 * time.Hour)
		}
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
	err := json.Unmarshal(file_read(base+"/app.json"), &av)
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

	access_helper_exists := false
	for _, helper := range av.Database.Helpers {
		_, ok := app_helpers[helper]
		if !ok {
			return nil, fmt.Errorf("unknown helper %q", helper)
		}
		if helper == "access" {
			access_helper_exists = true
		}
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

		if a.Access.Resource != "" {
			if !valid(a.Access.Resource, "parampath") {
				return nil, fmt.Errorf("action %q has invalid access resource", action)
			}
			if !access_helper_exists {
				return nil, fmt.Errorf("action %q uses access but access helper not enabled", action)
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
	for _, id := range file_list(data_dir + "/apps") {
		versions := file_list(data_dir + "/apps/" + id)
		if len(versions) == 0 {
			continue
		}
		a := app(id)

		for _, version := range versions {
			av, err := app_read(id, fmt.Sprintf("%s/apps/%s/%s", data_dir, id, version))
			if err != nil {
				info("App load error: %v", err)
				continue
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

// Register an icon for an internal app
func (a *App) icon(action string, label string, file string) {
	a.active.Icons = append(a.active.Icons, Icon{Action: action, Label: label, File: file})
}

// Resolve an app label
func (a *App) label(u *User, key string, values ...any) string {
	language := "en"
	if u != nil {
		language = u.Language
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
			debug("App found direct action %q with function %q", name, aa.Function)
			return &aa
		}

		// If type files, check for matching parent
		if aa.Files != "" {
			match := name
			for {
				parts := strings.SplitN(match, "/", 2)
				match = parts[0]
				if aa.name == match {
					debug("App found files action %q via pattern %q", name, aa.name)
					return &aa
				}
				if len(parts) < 2 {
					break
				}
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
			debug("App found action %q with function %q via pattern %q", name, aa.Function, aa.name)
			return &aa
		}
	}

	info("App %q version %q has no action matching %q", av.app.id, av.Version, name)
	return nil
}

// Get a new Starlark interpreter for an app version
func (av *AppVersion) starlark() *Starlark {
	if av.starlark_runtime == nil {
		av.starlark_runtime = starlark(av.Execute)
	}
	return av.starlark_runtime
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
		for _, i := range a.active.Icons {
			path := a.fingerprint
			if len(a.active.Paths) > 0 {
				path = a.active.Paths[0]
			}
			if i.Action != "" {
				path = path + "/" + i.Action
			}
			results = append(results, map[string]string{"path": path, "name": a.label(user, i.Label), "file": i.File})
		}
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return sl_encode(results), nil
}

// mochi.app.install(id, file, check_only?) -> string: Install an app from a .zip file, returns version
func api_app_install(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
	var ids []string
	apps_lock.Lock()
	for id := range apps {
		if valid(id, "entity") {
			ids = append(ids, id)
		}
	}
	apps_lock.Unlock()

	user := t.Local("user").(*User)
	results := make([]map[string]string, len(ids))
	apps_lock.Lock()
	for i, id := range ids {
		a := apps[id]
		if a == nil {
			return sl_error(fn, "App %q is nil", id)
		}
		if a.active == nil {
			return sl_error(fn, "App %q has no active version", id)
		}
		results[i] = map[string]string{"id": a.id, "name": a.label(user, a.active.Label), "latest": a.active.Version}
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return sl_encode(results), nil
}
