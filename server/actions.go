// Comms server: Actions
// Copyright Alistair Cunningham 2024

package main

import (
	"github.com/gin-gonic/gin"
)

type Action struct {
	object *Identity
	user   *User
	db     *DB
	web    *gin.Context
}

var actions = map[string]func(*Action){}

func (a *Action) error(code int, message string, values ...any) {
	web_error(a.web, code, message, values...)
}

func (a *Action) input(name string) string {
	return a.web.Query(name)
}

func (a *Action) json(in any) {
	a.web.JSON(200, in)
}

func (a *Action) redirect(url string) {
	a.web.Redirect(301, url)
}

func (a *Action) template(template string, values ...any) {
	web_template(a.web, 200, template, values...)
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
