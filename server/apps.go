// Mochi server: Apps
// Copyright Alistair Cunningham 2024-2025

package main

type App struct {
	id               string
	name             string
	actions          map[string]func(*Action)
	db_file          string
	db_create        func(*DB)
	events           map[string]func(*Event)
	events_broadcast map[string]func(*Event)
	entity_field     string
}

type Path struct {
	path   string
	app    *App
	action func(*Action)
}

var (
	apps     = map[string]*App{}
	paths    = map[string]*Path{}
	services = map[string]*App{}
)

/* Not used for now
func apps_start() {
	for _, id := range files_dir(data_dir + "/apps") {
		for _, version := range files_dir(data_dir + "/apps/" + id) {
			log_debug("Found installed app ID '%s' version '%s'", id, version)
			base := data_dir + "/apps/" + id + "/" + version

			if !file_exists(base + "/manifest.json") {
				log_debug("App ID '%s' version '%s' has no manifest.json file; ignoring app", id, version)
				continue
			}
			var a App
			if !json_decode(&a, file_read(base+"/manifest.json")) {
				log_warn("Bad manifest.json file '%s/manifest.json'; ignoring app", base)
				continue
			}
			a.id = id
			apps[a.Name] = &a

			if a.Path != "" {
				e, found := app_paths[a.Path]
				if found {
					log_warn("Path conflict for '%s' between apps '%s' and '%s'", a.Path, e.Name, a.Name)
				} else {
					app_paths[a.Path] = &a
				}
			}
		}
	}
} */

func app(name string) *App {
	a := &App{id: name, name: name, db_file: "", db_create: nil, entity_field: "entity"}
	a.actions = make(map[string]func(*Action))
	a.events = make(map[string]func(*Event))
	a.events_broadcast = make(map[string]func(*Event))
	apps[name] = a
	return a
}

func (a *App) entity(field string) {
	a.entity_field = field
}

func (a *App) event(event string, f func(*Event)) {
	a.events[event] = f
}

func (a *App) event_broadcast(event string, f func(*Event)) {
	a.events_broadcast[event] = f
}

func (a *App) path(path string, f func(*Action)) {
	paths[path] = &Path{path: path, app: a, action: f}
}

func (a *App) service(service string) {
	services[service] = a
}
