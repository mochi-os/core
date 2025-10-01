// Mochi server: Apps
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	"time"
)

type App struct {
	// Read from app.json
	Name    string `json:"name"`
	Version string `json:"version"`
	Engine  struct {
		Architecture string `json:"architecture"`
		Version      string `json:"version"`
	} `json:"engine"`
	Files    []string `json:"files"`
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

	// For Go code use
	id               string    `json:"-"`
	base             string    `json:"-"`
	entity_field     string    `json:"-"`
	starlark_runtime *Starlark `json:"-"`

	// For internal apps only, possibly to be removed in a future version
	internal struct {
		actions          map[string]func(*Action) `json:"-"`
		events           map[string]func(*Event)  `json:"-"`
		events_broadcast map[string]func(*Event)  `json:"-"`
	}
}

type Icon struct {
	Path  string `json:"path"`
	Label string `json:"label"`
	// Remove Name field once we have multi-language label system in place
	Name string `json:"name"`
	Icon string `json:"icon"`
}

type Path struct {
	path     string
	app      *App
	function string
	public   bool
	internal func(*Action)
}

var (
	apps_install_by_default = []string{
		"12qMc1J5PZJDmgbdxtjB1b8xWeA6zhFJUbz5wWUEJSK3gyeFUPb", // Home
		"123jjo8q9kx8HZHmxbQ6DMfWPsMSByongGbG3wTrywcm2aA5b8x", // Notifications
		"12Wa5korrLAaomwnwj1bW4httRgo6AXHNK1wgSZ19ewn8eGWa1C", // Friends
		"1KKFKiz49rLVfaGuChexEDdphu4dA9tsMroNMfUfC7oYuruHRZ",  // Chat
	}
	apps     = map[string]*App{}
	icons    = map[string]Icon{}
	paths    = map[string]*Path{}
	services = map[string]*App{}
)

// Create data structure for new internal app
func app(name string) *App {
	a := App{id: name, Name: name, entity_field: "entity"}
	a.Engine.Architecture = "internal"
	a.internal.actions = make(map[string]func(*Action))
	a.internal.events = make(map[string]func(*Event))
	a.internal.events_broadcast = make(map[string]func(*Event))
	apps[name] = &a
	return &a
}

func app_check_install(id string) bool {
	//TODO Check and install/update app
	s := stream("", id, "app", "versions")
	s.write_content()
	//response := s.read_content()

	return true
}

// Load details of an installed app from the filesystems
func app_load(id string, version string) error {
	debug("App '%s' version '%s' loading", id, version)

	// Load app manifest from app.json
	base := fmt.Sprintf("%s/apps/%s/%s", data_dir, id, version)
	if !file_exists(base + "/app.json") {
		return error_message("App '%s' version '%s' has no app.json file; ignoring", id, version)
	}

	var a App
	if !json_decode(&a, string(file_read(base+"/app.json"))) {
		return error_message("App bad app.json '%s/app.json'; ignoring app", base)
	}

	a.id = id
	a.base = base

	// Vaildate manifest
	if !valid(a.Name, "name") {
		return error_message("App bad name '%s'", a.Name)
	}

	if !valid(a.Version, "version") {
		return error_message("App bad version '%s'", a.Version)
	}

	if a.Engine.Architecture != "starlark" || a.Engine.Version != "1" {
		return error_message("App bad engine '%s' version '%s'", a.Engine.Architecture, a.Engine.Version)
	}

	for _, file := range a.Files {
		if !valid(file, "filepath") {
			return error_message("App bad executable file '%s'", file)
		}
	}

	if a.Database.File != "" && !valid(a.Database.File, "filename") {
		return error_message("App bad database file '%s'", a.Database.File)
	}

	if a.Database.Create != "" && !valid(a.Database.Create, "function") {
		return error_message("App bad database create function '%s'", a.Database.Create)
	}

	for _, i := range a.Icons {
		if i.Path != "" && !valid(i.Path, "constant") {
			return error_message("App bad icon path '%s'", i.Path)
		}

		if i.Label != "" && !valid(i.Label, "constant") {
			return error_message("App bad icon label '%s'", i.Label)
		}

		if i.Name != "" && !valid(i.Name, "name") {
			return error_message("App bad icon name '%s'", i.Name)
		}

		if !valid(i.Icon, "filepath") {
			return error_message("App bad icon '%s'", i.Icon)
		}
	}

	for path, p := range a.Paths {
		if !valid(path, "path") {
			return error_message("App bad path '%s'", path)
		}

		for action, a := range p.Actions {
			if action != "" && !valid(action, "action") {
				return error_message("App bad action '%s'", action)
			}

			if !valid(a.Function, "function") {
				return error_message("App bad action function '%s'", a.Function)
			}
		}
	}

	for service, s := range a.Services {
		if !valid(service, "constant") {
			return error_message("App bad service '%s'", service)
		}

		for event, e := range s.Events {
			if !valid(event, "constant") {
				return error_message("App bad event '%s'", event)
			}

			if !valid(e.Function, "function") {
				return error_message("App bad event function '%s'", e.Function)
			}
		}

		for function, f := range s.Functions {
			if function != "" && !valid(function, "constant") {
				return error_message("App bad function '%s'", function)
			}

			if !valid(f.Function, "function") {
				return error_message("App bad function function '%s'", f.Function)
			}
		}
	}

	// Add app to system
	apps[a.Name] = &a

	for i, file := range a.Files {
		a.Files[i] = a.base + "/" + file
	}

	for _, i := range a.Icons {
		icons[i.Name] = i
	}

	for path, p := range a.Paths {
		for action, ac := range p.Actions {
			full := path
			if action != "" {
				full = path + "/" + action
			}
			paths[full] = &Path{path: full, app: &a, function: ac.Function, public: ac.Public, internal: nil}
		}
	}

	for service, _ := range a.Services {
		services[service] = &a
	}

	debug("App loaded: %+v", a)
	return nil
}

// Manage which apps and their versions are installed
func apps_manager() {
	time.Sleep(time.Second)
	for {
		todo := map[string]bool{}

		for _, id := range apps_install_by_default {
			todo[id] = true
		}

		for _, id := range files_dir(data_dir + "/apps") {
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

// Check which apps are installed, and load them
func apps_start() {
	for _, id := range files_dir(data_dir + "/apps") {
		for _, version := range files_dir(data_dir + "/apps/" + id) {
			debug("App '%s' version '%s' found", id, version)
			err := app_load(id, version)
			if err != nil {
				info("App load error: %v", err)
			}
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
	a.Database.File = file
	a.Database.CreateFunction = create
}

// Register the entity field for an internal app
func (a *App) entity(field string) {
	a.entity_field = field
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
func (a *App) icon(path string, label string, name string, icon string) {
	icons[path] = Icon{Path: path, Label: label, Name: name, Icon: icon}
}

// Register a path for actions for an internal app
func (a *App) path(path string, f func(*Action)) {
	paths[path] = &Path{path: path, app: a, internal: f}
}

// Register a service for an internal app
func (a *App) service(service string) {
	services[service] = a
}
