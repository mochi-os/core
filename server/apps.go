// Comms server: Apps
// Copyright Alistair Cunningham 2024

package main

import (
	"bytes"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"html/template"
)

type App struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Version  string            `json:"version"`
	Track    string            `json:"track"`
	Labels   map[string]string `json:"labels"`
	Path     string            `json:"path"`
	Type     string            `json:"type"`
	Internal struct {
		Actions  map[string]func(*User, string, string, app_parameters) string
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

type AppPubSub struct {
	Name    string
	Topic   string
	Publish func(*pubsub.Topic)
}

type app_parameters map[string][]string

var apps_by_name = map[string]*App{}
var apps_by_path = map[string]*App{}
var app_pubsubs = map[string]*AppPubSub{}

func init() {
	app_register("apps", map[string]string{"en": "Apps"})
	app_register_action("apps", "", apps_action_list)
	app_register_action("apps", "create", apps_action_create)
	app_register_action("apps", "delete", apps_action_delete)
	app_register_action("apps", "list", apps_action_list)
	app_register_action("apps", "new", apps_action_new)
	app_register_path("apps", "apps")
}

// Create new app
func apps_action_create(u *User, action string, format string, p app_parameters) string {
	return app_template("apps/" + format + "/created")
}

// Delete app
func apps_action_delete(u *User, action string, format string, p app_parameters) string {
	return app_template("apps/" + format + "/deleted")
}

// Show list of apps
func apps_action_list(u *User, action string, format string, p app_parameters) string {
	return app_template("apps/"+format+"/list", objects_by_category(u, "apps", "app", "name"))
}

// New app selector
func apps_action_new(u *User, action string, format string, p app_parameters) string {
	return app_template("apps/" + format + "/new")
}

func app_display(u *User, app string, action string, format string, parameters app_parameters) (string, error) {
	//log_debug("Displaying app: user='%d', app='%s', action='%s', format='%s', parameters='%#v'", u.ID, app, action, format, parameters)
	a, found := apps_by_name[app]
	if !found {
		return "", error_message("App not installed")
	}

	switch a.Type {
	case "internal":
		for _, try := range []string{action, ""} {
			f, found := a.Internal.Actions[try]
			if found {
				return f(u, action, format, parameters), nil
			}
		}

	case "wasm":
		for _, try := range []string{action, ""} {
			function, found := a.WASM.Actions[try]
			if found {
				return wasm_run(u, a, function, 0, map[string]any{"action": action, "format": format, "parameters": parameters})
			}
		}
	}

	return "", error_message("App '%s' has no '%s' action or default action", a.Name, action)
}

func app_error(e error) string {
	return e.Error()
}

func app_parameter(p map[string][]string, key string, def string) string {
	values, found := p[key]
	if !found {
		return def
	}
	return values[0]
}

// Register internal functions
func app_register(name string, labels map[string]string) {
	//log_debug("App register internal: name='%s', label='%s'", name, labels["en"])
	a := App{ID: name, Name: name, Labels: labels, Path: "", Type: "internal"}
	a.Internal.Actions = make(map[string]func(*User, string, string, app_parameters) string)
	a.Internal.Events = make(map[string]func(*User, *Event))
	a.Internal.Services = make(map[string]func(*User, string, ...any) any)
	apps_by_name[name] = &a
}

// Register action for internal app
func app_register_action(name string, action string, f func(*User, string, string, app_parameters) string) {
	//log_debug("App register action: name='%s', action='%s'", name, action)
	a, found := apps_by_name[name]
	if !found || a.Type != "internal" {
		log_warn("app_register_action() called for non-installed or non-internal app '%s'", name)
		return
	}
	a.Internal.Actions[action] = f
}

// Register broadcast receiver for internal app
func app_register_broadcast(name string, sender string, action string, f func(*User, string, string, string, string)) {
	log_debug("App register broadcast: name='%s', sender='%s', action='%s'", name, sender, action)
	s, found := broadcasts_by_sender[sender]
	if found {
		s[action] = f
	} else {
		s = make(broadcast_map)
		s[action] = f
		broadcasts_by_sender[sender] = s
	}
}

// Register event for internal app
func app_register_event(name string, action string, f func(*User, *Event)) {
	//log_debug("App register event: name='%s', action='%s'", name, action)
	a, found := apps_by_name[name]
	if !found || a.Type != "internal" {
		log_warn("app_register_event() called for non-installed or non-internal app '%s'", name)
		return
	}
	a.Internal.Events[action] = f
}

// Register path for any app
func app_register_path(name string, path string) {
	//log_debug("App register path: name='%s', path='%s'", name, path)
	if !valid(path, "id") {
		log_warn("Invalid path '%s'", path)
		return
	}

	a, found := apps_by_name[name]
	if !found {
		log_warn("app_register_path() called for non-installed app '%s'", name)
		return
	}

	e, found := apps_by_path[path]
	if found {
		log_warn("Path conflict for '%s' between apps '%s' and '%s'", path, e.Name, name)
	} else {
		a.Path = path
		apps_by_path[path] = a
	}
}

// Register pubsub for any app
func app_register_pubsub(name string, topic string, publish func(*pubsub.Topic)) {
	//log_debug("App register pubsub: name='%s', topic='%s'", name, topic)
	_, found := apps_by_name[name]
	if !found {
		log_warn("app_register_pubsub() called for non-installed app '%s'", name)
		return
	}
	app_pubsubs[name] = &AppPubSub{Name: name, Topic: topic, Publish: publish}
}

// Register service for internal app
func app_register_service(name string, service string, f func(*User, string, ...any) any) {
	//log_debug("App register service: name='%s', service='%s'", name, service)
	a, found := apps_by_name[name]
	if !found || a.Type != "internal" {
		log_warn("app_register_service() called for non-installed or non-internal app '%s'", name)
		return
	}
	a.Internal.Services[service] = f
}

func apps_start() {
	// Not used for now
	return

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
			apps_by_name[a.Name] = &a

			if a.Path != "" {
				e, found := apps_by_path[a.Path]
				if found {
					log_warn("Path conflict for '%s' between apps '%s' and '%s'", a.Path, e.Name, a.Name)
				} else {
					apps_by_path[a.Path] = &a
				}
			}
		}
	}
}

func app_template(file string, values ...any) string {
	t, err := template.ParseFS(templates, "templates/en/"+file+".tmpl")
	if err != nil {
		log_warn(err.Error())
		return "App template error: " + err.Error()
	}
	var out bytes.Buffer
	if len(values) > 0 {
		err = t.Execute(&out, values[0])
	} else {
		err = t.Execute(&out, nil)
	}
	if err != nil {
		log_warn(err.Error())
		return "App template error: " + err.Error()
	}
	return out.String()
}
