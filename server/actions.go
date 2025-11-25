// Mochi server: Actions
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	"html/template"
	"sync"
)

type Action struct {
	id     int64
	user   *User
	owner  *User
	app    *App
	web    *gin.Context
	inputs map[string]string
}

var (
	actions_lock       = &sync.Mutex{}
	action_next  int64 = 1
)

func action_id() int64 {
	actions_lock.Lock()
	id := action_next
	action_next = action_next + 1
	actions_lock.Unlock()
	return id
}

func (a *Action) dump(values ...any) {
	debug("Web dump: %+v", values...)
	web_template(a.web, 200, "dev/dump", values...)
}

func (a *Action) error(code int, message string, values ...any) {
	web_error(a.web, code, message, values...)
}

func (a *Action) input(name string) string {
	input, found := a.inputs[name]
	if found {
		return input
	}

	value := a.web.Query(name)
	if value != "" {
		return value
	}

	value = a.web.PostForm(name)
	if value != "" {
		return value
	}

	ff, err := a.web.FormFile(name)
	if err == nil {
		return ff.Filename
	}

	return ""
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

// Starlark methods
func (a *Action) AttrNames() []string {
	return []string{"dump", "error", "input", "json", "logout", "redirect", "template", "upload", "user"}
}

func (a *Action) Attr(name string) (sl.Value, error) {
	switch name {
	case "dump":
		return sl.NewBuiltin("dump", a.sl_dump), nil
	case "error":
		return sl.NewBuiltin("error", a.sl_error), nil
	case "input":
		return sl.NewBuiltin("input", a.sl_input), nil
	case "json":
		return sl.NewBuiltin("json", a.sl_json), nil
	case "logout":
		return sl.NewBuiltin("logout", a.sl_logout), nil
	case "redirect":
		return sl.NewBuiltin("redirect", a.sl_redirect), nil
	case "template":
		return sl.NewBuiltin("template", a.sl_template), nil
	case "upload":
		return sl.NewBuiltin("upload", a.sl_upload), nil
	case "user":
		return a.user, nil
	default:
		return nil, nil
	}
}

func (a *Action) Freeze() {}

func (a *Action) Hash() (uint32, error) {
	return sl.String(a.id).Hash()
}

func (a *Action) String() string {
	return fmt.Sprintf("Action %d", a.id)
}

func (a *Action) Truth() sl.Bool {
	return sl.True
}

func (a *Action) Type() string {
	return "Action"
}

// Dump the variables passed for debugging
func (a *Action) sl_dump(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) > 0 {
		var vars []any
		for _, v := range args {
			vars = append(vars, sl_decode(v))
		}
		debug("%s() %+v", fn.Name(), vars)
		a.dump(vars)

	} else {
		a.dump(map[string]any{"form": a.web.Request.PostForm, "query": a.web.Request.URL.Query(), "url": a.web.Params})
	}

	return sl.None, nil
}

// Print an error
func (a *Action) sl_error(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 {
		a.dump(args)
		return sl.None, nil
	}

	var code int
	var format string
	err := sl.UnpackArgs(fn.Name(), args, kwargs, "code", &code, "format", &format)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	var vars []any
	for _, v := range args[1:] {
		vars = append(vars, sl_decode(v))
	}
	debug("%s() %d %s %+v", fn.Name(), code, format, vars)
	a.error(code, format, vars...)

	return sl.None, nil
}

// Get input parameter
func (a *Action) sl_input(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var field string
	err := sl.UnpackArgs(fn.Name(), args, kwargs, "field", &field)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return sl.String(a.input(field)), nil
}

// Print JSON
func (a *Action) sl_json(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var v any
	err := sl.UnpackArgs(fn.Name(), args, kwargs, "data", &v)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	a.web.JSON(200, v)
	return sl.None, nil
}

// Log the current user out
func (a *Action) sl_logout(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	login := web_cookie_get(a.web, "login", "")
	if login != "" {
		login_delete(login)
	}
	web_cookie_unset(a.web, "login")

	return sl.None, nil
}

// Redirect the action
func (a *Action) sl_redirect(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path string
	err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if !valid(path, "path") {
		return sl_error(fn, "invalid path %q", path)
	}

	a.web.Redirect(301, path)
	return sl.None, nil
}

// Print template
func (a *Action) sl_template(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <template path: string>, [data: dictionary]")
	}

	path, ok := sl.AsString(args[0])
	if !ok || (path != "" && !valid(path, "path")) {
		return sl_error(fn, "invalid template file %q", path)
	}

	// This should be done using ParseFS() followed by ParseFiles(), but I can't get this to work.
	file := fmt.Sprintf("%s/templates/en/%s.tmpl", a.app.active.base, path)
	if !file_exists(file) {
		return sl_error(fn, "template %q not found", path)
	}
	data := file_read(file)
	include := must(templates.ReadFile("templates/en/include.tmpl"))

	tmpl, err := template.New("").Parse(string(data) + "\n" + string(include))
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	if len(args) > 1 {
		err = tmpl.Execute(a.web.Writer, sl_decode(args[1]))
	} else {
		err = tmpl.Execute(a.web.Writer, Map{})
	}

	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return sl.None, nil
}

// Write the contents of an uploaded file
func (a *Action) sl_upload(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <field: string>, <file: string>")
	}

	field, ok := sl.AsString(args[0])
	if !ok || !valid(field, "constant") {
		return sl_error(fn, "invalid field %q", field)
	}

	file, ok := sl.AsString(args[1])
	if !ok || !valid(field, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	ff, err := a.web.FormFile(field)
	if err != nil {
		return sl_error(fn, "unable to get file field %q: %v", field, err)
	}

	err = a.web.SaveUploadedFile(ff, api_file(a.user, app, file))
	if err != nil {
		return sl_error(fn, "unable to write file for field %q: %v", field, err)
	}

	return sl.None, nil
}
