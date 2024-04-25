// Comms server: Apps
// Copyright Alistair Cunningham 2024

package main

type App struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Version  string `json:"version"`
	Track    string `json:"track"`
	Type     string `json:"type"`
	Internal struct {
		Actions  map[string]func(*User, *Action)
		Events   map[string]func(*User, *Event)
		Services map[string]func(*User, string, ...any) any
	}
	WASM struct {
		File     string            `json:"file"`
		Actions  map[string]string `json:"actions"`
		Events   map[string]string `json:"events"`
		Services map[string]string `json:"services"`
	} `json:"wasm"`
}

var apps = map[string]*App{}

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
			if !json_decode(file_read(base+"/manifest.json"), &a) {
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

func register_app(name string) *App {
	a := App{ID: name, Name: name, Type: "internal"}
	a.Internal.Actions = make(map[string]func(*User, *Action))
	a.Internal.Events = make(map[string]func(*User, *Event))
	a.Internal.Services = make(map[string]func(*User, string, ...any) any)
	apps[name] = &a
	return &a
}
