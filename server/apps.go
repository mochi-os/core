// Comms server: Apps
// Copyright Alistair Cunningham 2024

package main

// TODO Lower case struct fields
type App struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Version  string `json:"version"`
	Track    string `json:"track"`
	Type     string `json:"type"`
	Internal struct {
		Actions         map[string]func(*Action)
		DBFile          string
		DBCreate        func(*DB)
		Events          map[string]func(*Event)
		EventsBroadcast map[string]func(*Event)
		Paths           map[string]func(*Action)
		Services        map[string]func(int, string, string, string, ...any) any
	}
	WASM struct {
		File     string            `json:"file"`
		Actions  map[string]string `json:"actions"`
		Events   map[string]string `json:"events"`
		Paths    map[string]string `json:"paths"`
		Services map[string]string `json:"services"`
	} `json:"wasm"`
}

var apps = map[string]*App{}
var classes = map[string]*App{}

// TODO Move to web.go?
// TODO Replace paths_authenticated?
var paths = map[string]func(*Action){}
var paths_apps = map[string]*App{}
var paths_authenticated = map[string]bool{}

/* Not used for now
func apps_start() {
	for _, id := range files_dir("apps") {
		for _, version := range files_dir("apps/" + id) {
			log_debug("Found installed app ID '%s' version '%s'", id, version)
			base := "apps/" + id + "/" + version

			if !file_exists(base + "/manifest.json") {
				log_debug("App ID '%s' version '%s' has no manifest.json file; ignoring app", id, version)
				continue
			}
			var a App
			if !json_decode(&a, file_read(base+"/manifest.json")) {
				log_warn("Bad manifest.json file '%s/manifest.json'; ignoring app", base)
				continue
			}
			a.ID = id
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
	a := &App{ID: name, Name: name, Type: "internal"}
	a.Internal.Actions = make(map[string]func(*Action))
	a.Internal.DBFile = ""
	a.Internal.DBCreate = nil
	a.Internal.Events = make(map[string]func(*Event))
	a.Internal.EventsBroadcast = make(map[string]func(*Event))
	a.Internal.Paths = make(map[string]func(*Action))
	a.Internal.Services = make(map[string]func(int, string, string, string, ...any) any)
	apps[name] = a
	return a
}

func (a *App) class(class string) {
	classes[class] = a
}

func (a *App) path(path string, f func(*Action), authenticated bool) {
	a.Internal.Paths[path] = f
	paths[path] = f
	paths_apps[path] = a
	paths_authenticated[path] = authenticated
}
