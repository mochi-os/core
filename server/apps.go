// Comms server: Apps
// Copyright Alistair Cunningham 2024

package main

import (
	"fmt"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"net/http"
)

type App struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Version  string `json:"version"`
	Track    string `json:"track"`
	Type     string `json:"type"`
	Internal struct {
		Actions  map[string]func(*User, http.ResponseWriter, *http.Request)
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

var apps = map[string]*App{}
var actions = map[string]func(*User, http.ResponseWriter, *http.Request){}
var actions_authenticated = map[string]bool{}
var pubsubs = map[string]*AppPubSub{}

func app_error(w http.ResponseWriter, code int, message string, values ...any) {
	w.WriteHeader(code)
	fmt.Fprintf(w, message, values...)
}

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

func app_write(w http.ResponseWriter, format string, template string, values ...any) {
	switch format {
	case "json":
		fmt.Fprintf(w, json_encode(values[0]))
	default:
		web_template(w, template, values...)
	}
}

func register_app(name string) {
	//log_debug("Register app '%s'", name)
	a := App{ID: name, Name: name, Type: "internal"}
	a.Internal.Actions = make(map[string]func(*User, http.ResponseWriter, *http.Request))
	a.Internal.Events = make(map[string]func(*User, *Event))
	a.Internal.Services = make(map[string]func(*User, string, ...any) any)
	apps[name] = &a
}

func register_action(name string, action string, f func(*User, http.ResponseWriter, *http.Request), authenticated bool) {
	//log_debug("Register action: name='%s', action='%s'", name, action)
	a, found := apps[name]
	if !found || a.Type != "internal" {
		log_warn("register_action() called for non-installed or non-internal app '%s'", name)
		return
	}
	a.Internal.Actions[action] = f
	actions[action] = f
	actions_authenticated[action] = authenticated
}

func register_pubsub(name string, topic string, publish func(*pubsub.Topic)) {
	//log_debug("App register pubsub: name='%s', topic='%s'", name, topic)
	_, found := apps[name]
	if !found {
		log_warn("register_pubsub() called for non-installed app '%s'", name)
		return
	}
	pubsubs[name] = &AppPubSub{Name: name, Topic: topic, Publish: publish}
}
