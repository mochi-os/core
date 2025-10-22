// Mochi server: Apps
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type App struct {
	id       string                 `json:"id"`
	versions map[string]*AppVersion `json:"-"`
	active   *AppVersion            `json:"-"`
	internal struct {
		actions          map[string]func(*Action) `json:"-"`
		events           map[string]func(*Event)  `json:"-"`
		events_broadcast map[string]func(*Event)  `json:"-"`
	}
}

type AppVersion struct {
	Version string `json:"version"`
	Label   string `json:"label"`
	Engine  struct {
		Architecture string `json:"architecture"`
		Version      int    `json:"version"`
	} `json:"engine"`
	Files    []string `json:"files"`
	Requires struct {
		Role string `json:"role"`
	} `json:"requires"`
	Database struct {
		File           string    `json:"file"`
		Create         string    `json:"create"`
		CreateFunction func(*DB) `json:"-"`
	} `json:"database"`
	Icons []Icon `json:"icons"`
	Paths map[string]struct {
		Actions map[string]struct {
			Function string `json:"function"`
			Public   bool   `json:"public"`
		} `json:"actions"`
	} `json:"paths"`
	Services map[string]struct {
		Events map[string]struct {
			Function  string `json:"function"`
			Broadcast bool   `json:"broadcast"`
		} `json:"events"`
		Functions map[string]struct {
			Function string `json:"function"`
		} `json:"functions"`
	} `json:"services"`

	app              *App                         `json:"-"`
	base             string                       `json:"-"`
	entity_field     string                       `json:"-"`
	labels           map[string]map[string]string `json:"-"`
	starlark_runtime *Starlark                    `json:"-"`
}

type Icon struct {
	Path  string `json:"path"`
	Label string `json:"label"`
	Icon  string `json:"icon"`
	app   *App
}

type Path struct {
	path     string
	app      *App
	function string
	public   bool
	internal func(*Action)
}

const (
	app_version_minimum = 1
	app_version_maximum = 2
)

var (
	apps_install_by_default = []string{
		"12qMc1J5PZJDmgbdxtjB1b8xWeA6zhFJUbz5wWUEJSK3gyeFUPb", // Home
		"123jjo8q9kx8HZHmxbQ6DMfWPsMSByongGbG3wTrywcm2aA5b8x", // Notifications
		"12Wa5korrLAaomwnwj1bW4httRgo6AXHNK1wgSZ19ewn8eGWa1C", // Friends
		"1KKFKiz49rLVfaGuChexEDdphu4dA9tsMroNMfUfC7oYuruHRZ",  // Chat
	}
	apps      = map[string]*App{}
	apps_lock = &sync.Mutex{}
	icons     []Icon
	paths     = map[string]*Path{}
	services  = map[string]*App{}
)

// Create data structure for new internal app
func app(name string) *App {
	a := &App{id: name}
	a.active = &AppVersion{}
	a.internal.actions = make(map[string]func(*Action))
	a.internal.events = make(map[string]func(*Event))
	a.internal.events_broadcast = make(map[string]func(*Event))
	apps[name] = a
	return a
}

// Check whether app is the correct version, and if not download and install new version
func app_check_install(id string) bool {
	debug("App '%s' checking install status", id)

	s := stream("", id, "app", "version")
	if s == nil {
		return false
	}
	s.write_content("track", "production")

	status, err := s.read_content()
	if err != nil || status["status"] != "200" {
		return false
	}

	v, err := s.read_content()
	if err != nil {
		return false
	}
	version := v["version"]
	if !valid(version, "version") {
		return false
	}

	a := apps[id]
	if a != nil && a.active != nil && a.active.Version == version {
		debug("App '%s' keeping at version '%s'", id, a.active.Version)
		return true
	}

	oldVersion := ""
	if a != nil && a.active != nil {
		oldVersion = a.active.Version
	}
	debug("App '%s' upgrading from '%s' to '%s'", id, oldVersion, version)

	s = stream("", id, "app", "get")
	err = s.write_content("version", version)
	if err != nil {
		return false
	}

	response, err := s.read_content()
	if err != nil || response["status"] != "200" {
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

	apps_lock.Lock()
	if apps[id] == nil {
		apps[id] = &App{id: id}
	}
	a = apps[id]
	apps_lock.Unlock()

	a.load_version(new)
	debug("App '%s' version '%s' loaded", id, version)

	return true
}

// Install an app from a zip file, but do not load it
func app_install(id string, version string, file string, check_only bool) (*AppVersion, error) {
	if version == "" {
		debug("App '%s' installing from '%s'", id, file)
	} else {
		debug("App '%s' installing version '%s' from '%s'", id, version, file)
	}
	file_mkdir(data_dir + "/tmp")
	tmp := fmt.Sprintf("%s/tmp/app_install_%s_%s", data_dir, id, random_alphanumeric(8))
	debug("App unzipping into tmp directory '%s'", tmp)

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
		debug("App '%s' not installing", id)
		file_delete_all(tmp)
		return av, nil
	}

	av.base = fmt.Sprintf("%s/apps/%s/%s", data_dir, id, av.Version)
	if file_exists(av.base) {
		debug("App '%s' removing old copy of version '%s' in '%s'", id, av.Version, av.base)
		file_delete_all(av.base)
	}
	debug("Moving unzipped app from '%s' to '%s'", tmp, av.base)
	file_move(tmp, av.base)

	debug("App '%s' version '%s' installed", id, av.Version)
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
	debug("App '%s' loading from '%s'", id, base)

	// Load app manifest from app.json
	if !file_exists(base + "/app.json") {
		return nil, fmt.Errorf("App '%s' in '%s' has no app.json file; ignoring", id, base)
	}

	var av AppVersion
	if !json_decode(&av, string(file_read(base+"/app.json"))) {
		return nil, fmt.Errorf("App bad app.json '%s/app.json'; ignoring app", base)
	}

	av.base = base

	// Validate manifest
	if !valid(av.Version, "version") {
		return nil, fmt.Errorf("App bad version '%s'", av.Version)
	}

	if !valid(av.Label, "constant") {
		return nil, fmt.Errorf("App bad label '%s'", av.Label)
	}

	if av.Engine.Architecture != "starlark" {
		return nil, fmt.Errorf("App bad engine '%s' version %d", av.Engine.Architecture, av.Engine.Version)
	}
	if av.Engine.Version < app_version_minimum {
		return nil, fmt.Errorf("App is too old. Version %d is less than minimum version %d", av.Engine.Version, app_version_minimum)
	}
	if av.Engine.Version > app_version_maximum {
		return nil, fmt.Errorf("App is too new. Version %d is greater than maximum version %d", av.Engine.Version, app_version_maximum)
	}

	for _, file := range av.Files {
		if !valid(file, "filepath") {
			return nil, fmt.Errorf("App bad executable file '%s'", file)
		}
	}

	if av.Database.File != "" && !valid(av.Database.File, "filename") {
		return nil, fmt.Errorf("App bad database file '%s'", av.Database.File)
	}

	if av.Database.Create != "" && !valid(av.Database.Create, "function") {
		return nil, fmt.Errorf("App bad database create function '%s'", av.Database.Create)
	}

	for _, i := range av.Icons {
		if i.Path != "" && !valid(i.Path, "constant") {
			return nil, fmt.Errorf("App bad icon path '%s'", i.Path)
		}

		if !valid(i.Label, "constant") {
			return nil, fmt.Errorf("App bad icon label '%s'", i.Label)
		}

		if !valid(i.Icon, "filepath") {
			return nil, fmt.Errorf("App bad icon '%s'", i.Icon)
		}
	}

	for path, p := range av.Paths {
		if !valid(path, "path") {
			return nil, fmt.Errorf("App bad path '%s'", path)
		}

		for action, a := range p.Actions {
			if action != "" && !valid(action, "action") {
				return nil, fmt.Errorf("App bad action '%s'", action)
			}

			if !valid(a.Function, "function") {
				return nil, fmt.Errorf("App bad action function '%s'", a.Function)
			}
		}
	}

	for service, s := range av.Services {
		if !valid(service, "constant") {
			return nil, fmt.Errorf("App bad service '%s'", service)
		}

		for event, e := range s.Events {
			if !valid(event, "constant") {
				return nil, fmt.Errorf("App bad event '%s'", event)
			}

			if !valid(e.Function, "function") {
				return nil, fmt.Errorf("App bad event function '%s'", e.Function)
			}
		}

		for function, f := range s.Functions {
			if function != "" && !valid(function, "constant") {
				return nil, fmt.Errorf("App bad function '%s'", function)
			}

			if !valid(f.Function, "function") {
				return nil, fmt.Errorf("App bad function function '%s'", f.Function)
			}
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
		if apps[id] == nil {
			apps[id] = &App{id: id}
		}
		a := apps[id]

		for _, version := range versions {
			debug("App '%s' version '%s' found", id, version)
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
	a.internal.actions[action] = f
	actions[action] = f
}

// Register a broadcast for an internal app
func (a *App) broadcast(sender string, action string, f func(*User, string, string, string, any)) {
	s, sender_found := broadcasts_by_sender[sender]
	if sender_found {
		_, action_found := s[action]
		if action_found {
			s[action] = append(s[action], f)
		} else {
			s[action] = broadcast_action_functions{f}
		}
	} else {
		broadcasts_by_sender[sender] = broadcast_actions{action: broadcast_action_functions{f}}
	}
}

// Register the user database file for an internal app
func (a *App) db(file string, create func(*DB)) {
	a.active.Database.File = file
	a.active.Database.CreateFunction = create
}

// Register the entity field for an internal app
func (a *App) entity(field string) {
	a.active.entity_field = field
}

// Register an event handler for an internal app
func (a *App) event(event string, f func(*Event)) {
	a.internal.events[event] = f
}

// Register a broadcast event handler for an internal app
// This will probably be removed at some point
func (a *App) event_broadcast(event string, f func(*Event)) {
	a.internal.events_broadcast[event] = f
}

// Register an icon for an internal app
func (a *App) icon(path string, label string, icon string) {
	icons = append(icons, Icon{Path: path, Label: label, Icon: icon, app: a})
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
		info("App label '%s' in language '%s' not set", key, language)
		return key
	}

	return fmt.Sprintf(format, values...)
}

// Loads details of a new version, and if it's the latest activate it
func (a *App) load_version(av *AppVersion) {
	if a == nil || av == nil {
		return
	}
	debug("App '%s' loading version '%s'", a.id, av.Version)

	apps_lock.Lock()
	defer apps_lock.Unlock()

	av.app = a
	if a.versions == nil {
		a.versions = make(map[string]*AppVersion)
	}
	a.versions[av.Version] = av

	for i, file := range av.Files {
		av.Files[i] = av.base + "/" + file
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
			info("App unable to read labels file '%s': %v", path, err)
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

	latest := ""
	for v := range a.versions {
		if v > latest {
			latest = v
		}
	}

	if latest == av.Version {
		a.active = av

		// Remove old active version from globals
		for p := range paths {
			if paths[p].app != nil && paths[p].app.id == a.id {
				delete(paths, p)
				//TODO Remove Gin path on unload old app version
			}
		}

		for service := range services {
			if services[service] != nil && services[service].id == a.id {
				delete(services, service)
			}
		}

		new_icons := []Icon{}
		for _, ic := range icons {
			if ic.app == nil || ic.app.id != a.id {
				new_icons = append(new_icons, ic)
			}
		}
		icons = new_icons

		// Add new version to globals
		for path, p := range av.Paths {
			for action, ac := range p.Actions {
				full := path
				if action != "" {
					full = path + "/" + action
				}
				paths[full] = &Path{path: full, app: a, function: ac.Function, public: ac.Public, internal: nil}
			}
		}

		for service := range av.Services {
			services[service] = a
		}

		for _, i := range av.Icons {
			i.app = a
			icons = append(icons, i)
		}

		debug("App '%s' version '%s' loaded and activated", a.id, av.Version)

	} else {
		debug("App '%s' version '%s' loaded, but not activated", a.id, av.Version)
	}

	debug("App loaded")
}

// Register a path for actions for an internal app
func (a *App) path(path string, f func(*Action)) {
	paths[path] = &Path{path: path, app: a, internal: f}
}

// Register a service for an internal app
func (a *App) service(service string) {
	services[service] = a
}

// Get a new Starlark interpreter for an app version
func (av *AppVersion) starlark() *Starlark {
	//TODO Re-enable caching loading Starlark files
	//if a.starlark_runtime == nil {
	av.starlark_runtime = starlark(av.Files)
	//}
	return av.starlark_runtime
}
