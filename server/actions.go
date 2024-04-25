// Comms server: Actions
// Copyright Alistair Cunningham 2024

package main

import (
	"fmt"
	"net/http"
)

type Action struct {
	r *http.Request
	w http.ResponseWriter
}

var actions = map[string]func(*User, *Action){}
var actions_authenticated = map[string]bool{}

func (a *Action) error(code int, message string, values ...any) {
	web_error(a.w, code, message, values...)
}

func (a *Action) input(name string) string {
	return a.r.FormValue(name)
}

func (a *Action) redirect(url string) {
	web_redirect(a.w, url)
}

func (a *Action) write_format(format string, template string, values ...any) {
	switch format {
	case "json":
		a.write_json(values[0])
	default:
		a.write_template(template, values...)
	}
}

func (a *Action) write_json(in any) {
	fmt.Fprintf(a.w, json_encode(in))
}

func (a *Action) write_template(template string, values ...any) {
	web_template(a.w, template, values...)
}

func (a *App) register_action(action string, f func(*User, *Action), authenticated bool) {
	a.Internal.Actions[action] = f
	actions[action] = f
	actions_authenticated[action] = authenticated
}
