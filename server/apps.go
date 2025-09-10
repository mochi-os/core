// Mochi server: Apps
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
)

type App struct {
	// Read from app.json
	Name     string `json:"name"`
	Version  string
	Engine   string `json:"engine"`
	Protocol int    `json:"protocol"`
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
	} `json:"services"`

	// For Go code use
	id           string    `json:"-"`
	base         string    `json:"-"`
	entity_field string    `json:"-"`
	starlark     *Starlark `json:"-"`

	// For internal apps only, possibly to be removed in a future version
	internal struct {
		actions          map[string]func(*Action) `json:"-"`
		events           map[string]func(*Event)  `json:"-"`
		events_broadcast map[string]func(*Event)  `json:"-"`
	}
}

type Path struct {
	path     string
	app      *App
	engine   string
	function string
	public   bool
	internal func(*Action)
}

var (
	apps     = map[string]*App{}
	paths    = map[string]*Path{}
	services = map[string]*App{}
)

func app(name string) *App {
	a := App{id: name, Name: name, Engine: "internal", entity_field: "entity"}
	a.internal.actions = make(map[string]func(*Action))
	a.internal.events = make(map[string]func(*Event))
	a.internal.events_broadcast = make(map[string]func(*Event))
	apps[name] = &a
	return &a
}

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

	// Vaildate manifest
	if !valid(a.Name, "name") {
		return error_message("App bad name '%s'", a.Name)
	}

	if !valid(a.Version, "version") {
		return error_message("App bad version '%s'", a.Version)
	}

	if a.Engine != "starlark" {
		return error_message("App bad engine '%s'", a.Engine)
	}

	if a.Protocol != 1 {
		return error_message("App bad protocol version %d", a.Protocol)
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
	}

	// Set app properties
	a.id = id
	a.base = base
	a.starlark = nil

	for _, i := range a.Icons {
		icons[i.Name] = i
	}

	for path, p := range a.Paths {
		for action, ac := range p.Actions {
			full := path
			if action != "" {
				full = path + "/" + action
			}
			paths[full] = &Path{path: full, app: &a, engine: a.Engine, function: ac.Function, public: ac.Public, internal: nil}
		}
	}

	// Add to list of apps
	apps[a.Name] = &a
	debug("App loaded: %+v", a)
	return nil
}

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

func (a *App) action(action string, f func(*Action)) {
	a.internal.actions[action] = f
	actions[action] = f
}

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

func (a *App) db(file string, create func(*DB)) {
	a.Database.File = file
	a.Database.CreateFunction = create
}

func (a *App) entity(field string) {
	a.entity_field = field
}

func (a *App) event(event string, f func(*Event)) {
	a.internal.events[event] = f
}

func (a *App) event_broadcast(event string, f func(*Event)) {
	a.internal.events_broadcast[event] = f
}

func (a *App) icon(path string, label string, name string, icon string) {
	icons[path] = Icon{Path: path, Label: label, Name: name, Icon: icon}
}

func (a *App) path(path string, f func(*Action)) {
	paths[path] = &Path{path: path, app: a, engine: "internal", internal: f}
}

func (a *App) service(service string) {
	services[service] = a
}
