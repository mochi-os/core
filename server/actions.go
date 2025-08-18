// Mochi server: Actions
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"github.com/gin-gonic/gin"
)

type Action struct {
	user  *User
	owner *User
	db    *DB
	web   *gin.Context
	path  *Path
}

var (
	actions = map[string]func(*Action){}
)

func (a *Action) error(code int, message string, values ...any) {
	debug(message, values...)
	web_error(a.web, code, message, values...)
}

func (a *Action) input(name string) string {
	value := a.web.Param(name)
	if value != "" {
		return value
	}
	value = a.web.Query(name)
	if value != "" {
		return value
	}
	return a.web.PostForm(name)
}

func (a *Action) json(in any) {
	a.web.JSON(200, in)
}

// TODO Replace public mode with something better
func (a *Action) public_mode() *Action {
	debug("Switching action to public mode")
	a.user = nil

	if a.db != nil {
		a.db.close()
		if a.owner != nil {
			a.db = db_user(a.owner, a.path.app.db_file, a.path.app.db_create)
			defer a.db.close()
		}
	}

	return a
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
