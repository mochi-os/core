// Mochi server: Actions
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"github.com/gin-gonic/gin"
)

type Action struct {
	user  *User
	owner *User
	app   *App
	web   *gin.Context
	path  *Path
}

var (
	actions = map[string]func(*Action){}
)

func (a *Action) dump(values ...any) {
	debug("Web dump: %+v", values...)
	web_template(a.web, 200, "dev/dump", values...)
}

func (a *Action) error(code int, message string, values ...any) {
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

func (a *Action) redirect(url string) {
	a.web.Redirect(301, url)
}

func (a *Action) template(template string, format string, values ...any) {
	switch format {
	case "json":
		a.json(values[0])
	default:
		web_template(a.web, 200, template, values...)
	}
}
