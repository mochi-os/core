// Comms server: Actions
// Copyright Alistair Cunningham 2024

package main

import (
	"fmt"
	"net/http"
)

type Action struct {
	App       *App
	Databases map[string]*DB
	Owner     *User
	R         *http.Request
	W         http.ResponseWriter
}

var actions = map[string]func(*User, *Action){}
var actions_apps = map[string]*App{}
var actions_authenticated = map[string]bool{}

func (a *Action) cleanup() {
	for _, db := range a.Databases {
		db.close()
	}
}

func (a *Action) db(file string) *DB {
	path := fmt.Sprintf("users/%d/identities/%s/apps/%s/%s", a.Owner.ID, a.Owner.Identity.ID, a.App.Name, file)
	if file_exists(path) {
		a.Databases[path] = db_open(path)
		return a.Databases[path]
	}

	f, found := a.App.AppCreate[file]
	if !found {
		log_error("App '%s' has no database creator for '%s'", a.App.Name, file)
	}
	db := db_open(path)
	f(db)
	return db
}

func (a *Action) error(code int, message string, values ...any) {
	web_error(a.W, code, message, values...)
}

func (a *Action) input(name string) string {
	return a.R.FormValue(name)
}

func (a *Action) json(in any) {
	fmt.Fprintf(a.W, json_encode(in))
}

func (a *Action) redirect(url string) {
	web_redirect(a.W, url)
}

func (a *Action) template(template string, values ...any) {
	web_template(a.W, template, values...)
}

func (a *App) register_action(action string, f func(*User, *Action), authenticated bool) {
	a.Internal.Actions[action] = f
	actions[action] = f
	actions_apps[action] = a
	actions_authenticated[action] = authenticated
}

func (a *Action) write(format string, template string, values ...any) {
	switch format {
	case "json":
		a.json(values[0])
	default:
		a.template(template, values...)
	}
}
