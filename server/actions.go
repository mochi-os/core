// Mochi server: Actions
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

type Action struct {
	id     int64
	user   *User
	owner  *User
	domain *DomainInfo
	app    *App
	web    *gin.Context
	inputs map[string]string
}

// ActionCookie provides cookie manipulation methods for actions
type ActionCookie struct {
	action *Action
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

// Dump values as formatted JSON in a simple HTML page
func (a *Action) dump(values ...any) {
	debug("Web dump: %+v", values...)

	a.web.Writer.WriteString("<html><head><title>Dump</title></head><body><pre>")

	for i, v := range values {
		if i > 0 {
			a.web.Writer.WriteString("\n\n")
		}
		data, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			a.web.Writer.WriteString(fmt.Sprintf("Error encoding value %d: %v", i, err))
		} else {
			a.web.Writer.Write(data)
		}
	}

	a.web.Writer.WriteString("</pre></body></html>")
}

// Display an error as a simple HTML page
func (a *Action) error(code int, message string, values ...any) {
	msg := fmt.Sprintf(message, values...)

	// Return JSON for API requests, HTML for browser requests
	if strings.Contains(a.web.GetHeader("Accept"), "application/json") ||
		strings.HasPrefix(a.web.GetHeader("Content-Type"), "application/json") {
		a.web.JSON(code, gin.H{"error": msg})
		return
	}

	a.web.Status(code)
	a.web.Writer.WriteString("<html><head><title>Error</title></head><body>")
	a.web.Writer.WriteString(fmt.Sprintf("<h1>Error %d</h1>", code))
	a.web.Writer.WriteString("<pre>")
	a.web.Writer.WriteString(msg)
	a.web.Writer.WriteString("</pre></body></html>")
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

func (a *Action) redirect(code int, location string) {
	a.web.Redirect(code, location)
}

// Starlark methods
func (a *Action) AttrNames() []string {
	return []string{"access_require", "cookie", "domain", "dump", "error", "header", "input", "json", "logout", "print", "redirect", "template", "upload", "user", "write_from_file"}
}

func (a *Action) Attr(name string) (sl.Value, error) {
	switch name {
	case "access_require":
		return sl.NewBuiltin("access_require", a.sl_access_require), nil
	case "cookie":
		return &ActionCookie{action: a}, nil
	case "domain":
		return a.domain, nil
	case "dump":
		return sl.NewBuiltin("dump", a.sl_dump), nil
	case "error":
		return sl.NewBuiltin("error", a.sl_error), nil
	case "header":
		return sl.NewBuiltin("header", a.sl_header), nil
	case "input":
		return sl.NewBuiltin("input", a.sl_input), nil
	case "json":
		return sl.NewBuiltin("json", a.sl_json), nil
	case "logout":
		return sl.NewBuiltin("logout", a.sl_logout), nil
	case "print":
		return sl.NewBuiltin("print", a.sl_print), nil
	case "redirect":
		return sl.NewBuiltin("redirect", a.sl_redirect), nil
	case "template":
		return sl.NewBuiltin("template", a.sl_template), nil
	case "upload":
		return sl.NewBuiltin("upload", a.sl_upload), nil
	case "user":
		return a.user, nil
	case "write_from_file":
		return sl.NewBuiltin("write_from_file", a.sl_write_from_file), nil
	default:
		return nil, nil
	}
}

func (a *Action) Freeze() {}

func (a *Action) Hash() (uint32, error) {
	return sl.String(fmt.Sprintf("%d", a.id)).Hash()
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

// a.access_require(resource, operation) -> None: Require access or raise error
func (a *Action) sl_access_require(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <resource: string>, <operation: string>")
	}

	resource, ok := sl.AsString(args[0])
	if !ok || resource == "" {
		return sl_error(fn, "invalid resource")
	}

	operation, ok := sl.AsString(args[1])
	if !ok || operation == "" {
		return sl_error(fn, "invalid operation")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		return sl_error(fn, "no owner")
	}

	user := ""
	role := ""
	if a.user != nil {
		if a.user.Identity != nil {
			user = a.user.Identity.ID
		}
		role = a.user.Role
	}

	db := db_app(owner, app.active)
	if db == nil {
		return sl_error(fn, "app has no database configured")
	}
	if !db.access_check(user, role, resource, operation) {
		return sl_error(fn, "access denied")
	}

	return sl.None, nil
}

// a.dump(values...) -> None: Dump variables as formatted JSON for debugging
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

// a.error(code?, messages...) -> None: Display an error page
func (a *Action) sl_error(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		a.error(500, "No error message provided")
		return sl.None, nil
	}

	code := 500
	if len(args) >= 2 {
		if c, err := sl.AsInt32(args[0]); err == nil {
			code = int(c)
			args = args[1:]
		}
	}

	var parts []string
	for _, arg := range args {
		parts = append(parts, fmt.Sprintf("%v", sl_decode(arg)))
	}
	message := fmt.Sprintf("%s", parts)
	if len(parts) == 1 {
		message = parts[0]
	}

	debug("sl_error() %d %s", code, message)
	a.error(code, "%s", message)

	return sl.None, nil
}

// a.input(field, default?) -> string: Get form/query input parameter
func (a *Action) sl_input(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var field string
	var def string
	err := sl.UnpackArgs(fn.Name(), args, kwargs, "field", &field, "default?", &def)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	value := a.input(field)
	if value == "" {
		value = def
	}
	return sl.String(value), nil
}

// a.json(data) -> None: Send JSON response
func (a *Action) sl_json(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <data>")
	}

	a.web.JSON(200, sl_decode(args[0]))
	return sl.None, nil
}

// a.logout() -> None: Log the current user out
func (a *Action) sl_logout(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	session := web_cookie_get(a.web, "session", "")
	if session != "" {
		login_delete(session)
	}
	web_cookie_unset(a.web, "session")

	return sl.None, nil
}

// a.print(strings...) -> None: Print raw content to browser
func (a *Action) sl_print(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	for _, arg := range args {
		s, ok := sl.AsString(arg)
		if ok {
			a.web.Writer.WriteString(s)
		}
	}
	return sl.None, nil
}

// a.redirect(path) -> None: Redirect to another path
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

// a.template(path, data?) -> None: Render and output a template
func (a *Action) sl_template(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <template path: string>, [data: dictionary]")
	}

	path, ok := sl.AsString(args[0])
	if !ok || (path != "" && !valid(path, "path")) {
		return sl_error(fn, "invalid template file %q", path)
	}

	file := fmt.Sprintf("%s/templates/en/%s.tmpl", a.app.active.base, path)
	if !file_exists(file) {
		return sl_error(fn, "template %q not found", path)
	}

	tmpl, err := template.New("").ParseFiles(file)
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

// a.upload(field, file) -> None: Save an uploaded file
func (a *Action) sl_upload(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <field: string>, <file: string>")
	}

	field, ok := sl.AsString(args[0])
	if !ok || !valid(field, "constant") {
		return sl_error(fn, "invalid field %q", field)
	}

	file, ok := sl.AsString(args[1])
	if !ok || !valid(file, "filepath") {
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

	// Check storage limit (10GB per user across all apps)
	current := dir_size(user_storage_dir(a.user))
	if current+ff.Size > file_max_storage {
		return sl_error(fn, "storage limit exceeded")
	}

	err = a.web.SaveUploadedFile(ff, api_file_path(a.user, app, file))
	if err != nil {
		return sl_error(fn, "unable to write file for field %q: %v", field, err)
	}

	return sl.None, nil
}

// a.header(name, value) -> None: Set response header
func (a *Action) sl_header(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, value string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "value", &value); err != nil {
		return nil, err
	}
	a.web.Header(name, value)
	return sl.None, nil
}

// a.write_from_file(path) -> None: Serve file from app's data directory
func (a *Action) sl_write_from_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path); err != nil {
		return nil, err
	}

	if !valid(path, "filepath") {
		a.error(400, "Invalid path")
		return sl.None, nil
	}

	owner := t.Local("owner").(*User)
	if owner == nil {
		a.error(500, "No owner")
		return sl.None, nil
	}

	app := t.Local("app").(*App)
	file := api_file_path(owner, app, path)

	a.web.File(file)
	return sl.None, nil
}

// ActionCookie Starlark interface
func (c *ActionCookie) AttrNames() []string {
	return []string{"get", "set", "unset"}
}

func (c *ActionCookie) Attr(name string) (sl.Value, error) {
	switch name {
	case "get":
		return sl.NewBuiltin("get", c.sl_get), nil
	case "set":
		return sl.NewBuiltin("set", c.sl_set), nil
	case "unset":
		return sl.NewBuiltin("unset", c.sl_unset), nil
	default:
		return nil, nil
	}
}

func (c *ActionCookie) Freeze()               {}
func (c *ActionCookie) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: ActionCookie") }
func (c *ActionCookie) String() string        { return "ActionCookie" }
func (c *ActionCookie) Truth() sl.Bool        { return sl.True }
func (c *ActionCookie) Type() string          { return "ActionCookie" }

// a.cookie.get(name, default?) -> string: Get cookie value
func (c *ActionCookie) sl_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, def string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "default?", &def); err != nil {
		return nil, err
	}
	return sl.String(web_cookie_get(c.action.web, name, def)), nil
}

// a.cookie.set(name, value) -> None: Set a cookie
func (c *ActionCookie) sl_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, value string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "value", &value); err != nil {
		return nil, err
	}
	web_cookie_set(c.action.web, name, value)
	return sl.None, nil
}

// a.cookie.unset(name) -> None: Remove a cookie
func (c *ActionCookie) sl_unset(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name); err != nil {
		return nil, err
	}
	web_cookie_unset(c.action.web, name)
	return sl.None, nil
}
