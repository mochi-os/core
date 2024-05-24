// Comms server: Actions
// Copyright Alistair Cunningham 2024

package main

import (
	"fmt"
	"net/http"
)

type Action struct {
	object *Identity
	user   *User
	db     *DB
	r      *http.Request
	w      http.ResponseWriter
}

var actions = map[string]func(*Action){}

func (a *Action) error(code int, message string, values ...any) {
	web_error(a.w, code, message, values...)
}

func (a *Action) input(name string) string {
	return a.r.FormValue(name)
}

func (a *Action) json(in any) {
	fmt.Fprintf(a.w, json_encode(in))
}

func (a *Action) redirect(url string) {
	web_redirect(a.w, url)
}

func (a *Action) template(template string, values ...any) {
	web_template(a.w, template, values...)
}

func (a *Action) write(format string, template string, values ...any) {
	switch format {
	case "json":
		a.json(values[0])
	default:
		a.template(template, values...)
	}
}

func (a *App) action(action string, f func(*Action)) {
	a.Internal.Actions[action] = f
	actions[action] = f
}
