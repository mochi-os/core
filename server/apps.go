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
	Icons []struct {
		Action string `json:"action"`
		Label  string `json:"label"`
		Value  string `json:"value"`
		Icon   string `json:"icon"`
	} `json:"icons"`
	Paths map[string]struct {
		Actions map[string]string `json:"actions"`
	} `json:"paths"`
	Services map[string]struct {
		Events          map[string]string `json:"events"`
		EventsBroadcast map[string]string `json:"events_broadcast"`
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

func app_load(id string, version string) {
	debug("App '%s' loading version '%s'", id, version)

	base := fmt.Sprintf("%s/apps/%s/%s", data_dir, id, version)
	if !file_exists(base + "/app.json") {
		debug("App '%s' version '%s' has no app.json file; ignoring", id, version)
		return
	}
	var a App
	if !json_decode(&a, string(file_read(base+"/app.json"))) {
		warn("App bad app.json '%s/app.json'; ignoring app", base)
		return
	}

	//TODO Validate app.json fields

	a.id = id
	a.base = base
	a.starlark = nil

	for path, p := range a.Paths {
		for action, function := range p.Actions {
			full := path
			if action != "" {
				full = path + "/" + action
			}
			debug("App adding path '%s' to function '%s'", full, function)
			paths[full] = &Path{path: full, app: &a, engine: a.Engine, function: function, internal: nil}
		}
	}

	apps[a.Name] = &a
	debug("App loaded: %+v", a)
}

func apps_start() {
	for _, id := range files_dir(data_dir + "/apps") {
		for _, version := range files_dir(data_dir + "/apps/" + id) {
			debug("App '%s' version '%s' found", id, version)
			app_load(id, version)
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

func (a *App) home(path string, labels map[string]string) {
	home_paths[path] = HomePath{Path: path, Labels: labels}
}

func (a *App) path(path string, f func(*Action)) {
	paths[path] = &Path{path: path, app: a, engine: "internal", internal: f}
}

func (a *App) service(service string) {
	services[service] = a
}
