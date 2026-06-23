// Mochi server: Actions
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// is_client_disconnect reports whether err looks like the client closed the
// connection mid-write (EPIPE / connection reset / cancelled context). These
// are normal in HTTP serving — the browser navigated away, scrolled past, or
// cancelled the request — and should not be surfaced as server errors.
func is_client_disconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "broken pipe") || strings.Contains(s, "connection reset")
}

type Action struct {
	id     int64
	user   *User
	owner  *User
	domain *DomainInfo
	app    *App
	active *AppVersion
	token  *Token
	web    *gin.Context
	inputs map[string]string
	body   string
}

// ActionInput provides input methods (callable as a.input(), with a.input.has())
type ActionInput struct {
	action *Action
}

func (ai *ActionInput) String() string        { return "action.input" }
func (ai *ActionInput) Type() string          { return "action.input" }
func (ai *ActionInput) Freeze()               {}
func (ai *ActionInput) Truth() sl.Bool        { return true }
func (ai *ActionInput) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable: action.input") }
func (ai *ActionInput) Name() string          { return "input" }

// Callable: a.input(field, default?) -> string or None
func (ai *ActionInput) CallInternal(t *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var field string
	var def sl.Value
	err := sl.UnpackArgs("input", args, kwargs, "field", &field, "default?", &def)
	if err != nil {
		return nil, err
	}

	// Check inputs map first (handles empty strings from JSON body)
	if value, found := ai.action.inputs[field]; found {
		return sl.String(value), nil
	}

	// Check query/form/file fallbacks
	value := ai.action.input(field)
	if value != "" {
		return sl.String(value), nil
	}

	// Field is missing
	if def != nil {
		return def, nil
	}
	if ai.action.active.Architecture.Version >= 4 {
		return sl.None, nil
	}
	return sl.String(""), nil
}

func (ai *ActionInput) AttrNames() []string {
	if ai.action.active.Architecture.Version >= 4 {
		return nil
	}
	return []string{"exists"}
}

func (ai *ActionInput) Attr(name string) (sl.Value, error) {
	switch name {
	case "exists":
		if ai.action.active.Architecture.Version >= 4 {
			return nil, fmt.Errorf("a.input.exists() is not available in API version 4+; use 'a.input(field) != None' instead")
		}
		return sl.NewBuiltin("input.exists", ai.sl_exists), nil
	}
	return nil, nil
}

// a.input.exists(field) -> bool: Check if a form/query input field was explicitly provided
func (ai *ActionInput) sl_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var field string
	err := sl.UnpackArgs(fn.Name(), args, kwargs, "field", &field)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	_, found := ai.action.inputs[field]
	return sl.Bool(found), nil
}

// ActionCookie provides cookie manipulation methods for actions
type ActionCookie struct {
	action *Action
}

// ActionAccess provides access-control methods for actions
type ActionAccess struct {
	action *Action
}

// ActionError is callable as a.error(status, message) and exposes a.error.label(status, key, ...).
type ActionError struct {
	action *Action
}

func (ae *ActionError) String() string        { return "action.error" }
func (ae *ActionError) Type() string          { return "action.error" }
func (ae *ActionError) Freeze()               {}
func (ae *ActionError) Truth() sl.Bool        { return true }
func (ae *ActionError) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable: action.error") }
func (ae *ActionError) Name() string          { return "error" }

// Callable: a.error(status, message, log=True) -> None
func (ae *ActionError) CallInternal(t *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return ae.action.sl_error(t, nil, args, kwargs)
}

func (ae *ActionError) AttrNames() []string {
	return []string{"label"}
}

func (ae *ActionError) Attr(name string) (sl.Value, error) {
	switch name {
	case "label":
		return sl.NewBuiltin("error.label", ae.action.sl_error_label), nil
	}
	return nil, nil
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
			a.web.Writer.WriteString(template.HTMLEscapeString(fmt.Sprintf("Error encoding value %d: %v", i, err)))
		} else {
			a.web.Writer.WriteString(template.HTMLEscapeString(string(data)))
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
	a.web.Writer.WriteString(template.HTMLEscapeString(msg))
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

// Starlark methods
func (a *Action) AttrNames() []string {
	return []string{"access", "body", "cookie", "domain", "dump", "error", "file", "header", "input", "inputs", "json", "logout", "print", "redirect", "template", "token", "upload", "user", "write"}
}

func (a *Action) Attr(name string) (sl.Value, error) {
	switch name {
	case "access":
		return &ActionAccess{action: a}, nil
	case "body":
		return sl.String(a.body), nil
	case "cookie":
		return &ActionCookie{action: a}, nil
	case "domain":
		return a.domain, nil
	case "dump":
		return sl.NewBuiltin("dump", a.sl_dump), nil
	case "error":
		return &ActionError{action: a}, nil
	case "file":
		return sl.NewBuiltin("file", a.sl_file), nil
	case "header":
		return sl.NewBuiltin("header", a.sl_header), nil
	case "input":
		return &ActionInput{action: a}, nil
	case "inputs":
		return sl.NewBuiltin("inputs", a.sl_inputs), nil
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
	case "token":
		if a.token == nil {
			return sl.None, nil
		}
		return sl_encode(map[string]any{
			"name":    a.token.Name,
			"created": a.token.Created,
			"used":    a.token.Used,
			"expires": a.token.Expires,
		}), nil
	case "upload":
		return sl.NewBuiltin("upload", a.sl_upload), nil
	case "user":
		if a.user == nil {
			return sl.None, nil
		}
		return a.user, nil
	case "write":
		return &ActionWrite{action: a}, nil
	default:
		return nil, nil
	}
}

// ActionWrite is the a.write namespace exposing source-typed response writers.
// Usage: a.write.file(path), a.write.asset(path), a.write.stream(stream).
type ActionWrite struct {
	action *Action
}

func (w *ActionWrite) String() string        { return "Action.write" }
func (w *ActionWrite) Type() string          { return "module" }
func (w *ActionWrite) Freeze()               {}
func (w *ActionWrite) Truth() sl.Bool        { return sl.True }
func (w *ActionWrite) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }
func (w *ActionWrite) AttrNames() []string   { return []string{"asset", "file", "stream"} }
func (w *ActionWrite) Attr(name string) (sl.Value, error) {
	switch name {
	case "asset":
		return sl.NewBuiltin("write.asset", w.action.sl_write_asset), nil
	case "file":
		return sl.NewBuiltin("write.file", w.action.sl_write_file), nil
	case "stream":
		return sl.NewBuiltin("write.stream", w.action.sl_write_stream), nil
	}
	return nil, nil
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

// ActionAccess Starlark interface
func (aa *ActionAccess) AttrNames() []string {
	return []string{"require"}
}

func (aa *ActionAccess) Attr(name string) (sl.Value, error) {
	switch name {
	case "require":
		return sl.NewBuiltin("require", aa.sl_require), nil
	default:
		return nil, nil
	}
}

func (aa *ActionAccess) Freeze()               {}
func (aa *ActionAccess) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: ActionAccess") }
func (aa *ActionAccess) String() string        { return "ActionAccess" }
func (aa *ActionAccess) Truth() sl.Bool        { return sl.True }
func (aa *ActionAccess) Type() string          { return "ActionAccess" }

// a.access.require(resource, operation) -> None: Require access or raise error
func (aa *ActionAccess) sl_require(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := aa.action
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

	db := db_app(owner, app)
	if db == nil {
		return sl_error(fn, "app has no database configured")
	}
	if !db.access_check(owner, user, role, resource, operation) {
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

// a.error(code?, messages..., log=True) -> None: Display an error page.
// Pass log=False for expected 4xx outcomes (e.g. proxying a 404 from another
// service) where logging every occurrence would just be noise.
func (a *Action) sl_error(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		a.error(500, "No error message provided")
		return sl.None, nil
	}

	log_it := true
	for _, kv := range kwargs {
		key, ok := sl.AsString(kv[0])
		if !ok || key != "log" {
			continue
		}
		if b, ok := kv[1].(sl.Bool); ok {
			log_it = bool(b)
		}
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

	if log_it {
		debug("sl_error() %d %s", code, message)
	}
	a.error(code, "%s", message)

	return sl.None, nil
}

// a.error.label(status, key, **kwargs) -> None: Resolve a label key from the
// calling app's labels/<lang>.conf and return it as the HTTP error message.
// kwargs become ICU MessageFormat substitutions. Language is the caller's
// preference (logged in) or Accept-Language (anonymous), via the same
// request_language() machinery the resolver uses. Pass log=False to suppress
// the diagnostic log line, as with a.error().
func (a *Action) sl_error_label(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 {
		return sl_error(fn, "syntax: <status: int>, <key: string>, **kwargs")
	}
	code, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "status must be an integer")
	}
	key, ok := sl.AsString(args[1])
	if !ok || key == "" {
		return sl_error(fn, "key must be a non-empty string")
	}

	// Filter `log` out of the substitution kwargs (matches sl_error's API).
	log_it := true
	var sub_kwargs []sl.Tuple
	for _, kv := range kwargs {
		k, _ := sl.AsString(kv[0])
		if k == "log" {
			if b, ok := kv[1].(sl.Bool); ok {
				log_it = bool(b)
			}
			continue
		}
		sub_kwargs = append(sub_kwargs, kv)
	}
	margs, err := starlark_kwargs_to_map(sub_kwargs)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	app_local, _ := t.Local("app").(*App)
	user, _ := t.Local("user").(*User)
	language := request_language(a.web, user)
	var av *AppVersion
	if app_local != nil {
		av = app_local.active(user)
	}

	msg := key
	if av != nil {
		msg = resolve_label(av, language, key, margs)
	}
	if log_it {
		debug("sl_error_label() %d %s -> %s", int(code), key, msg)
	}
	a.error(int(code), "%s", msg)
	return sl.None, nil
}

// a.inputs(field) -> list: Get all values for a form/query input field
func (a *Action) sl_inputs(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var field string
	err := sl.UnpackArgs(fn.Name(), args, kwargs, "field", &field)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	var values []string

	// Check query string first
	values = a.web.QueryArray(field)

	// If no query values, check form values
	if len(values) == 0 {
		values = a.web.PostFormArray(field)
	}

	// Convert to Starlark list
	items := make([]sl.Value, len(values))
	for i, v := range values {
		items[i] = sl.String(v)
	}
	return sl.NewList(items), nil
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

	// Audit log logout
	if a.user != nil {
		audit_logout(a.user.Username, rate_limit_client_ip(a.web))
	}

	return sl.None, nil
}

// a.print(strings...) -> None: Print raw content to browser
func (a *Action) sl_print(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Set status 200 on first write (matches a.error() pattern)
	if !a.web.Writer.Written() {
		a.web.Status(200)
	}
	for _, arg := range args {
		s, ok := sl.AsString(arg)
		if ok {
			a.web.Writer.WriteString(s)
		}
	}
	return sl.None, nil
}

// a.redirect(path, code=302) -> None: Redirect to another path
func (a *Action) sl_redirect(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path string
	code := 302
	err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path, "code?", &code)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	a.web.Redirect(code, path)
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

	av := a.app.active(a.user)
	file := fmt.Sprintf("%s/templates/en/%s.tmpl", av.base, path)
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

	if err != nil && !is_client_disconnect(err) {
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

	// Check storage limit (10GB per user across all apps; admins exempt)
	remaining, err := user_storage_remaining(a.user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if ff.Size > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	err = a.web.SaveUploadedFile(ff, api_file_path(a.user, app, file))
	if err != nil {
		return sl_error(fn, "unable to write file for field %q: %v", field, err)
	}

	// Replicate the upload to the user's host set. SaveUploadedFile
	// streams the body to disk without materialising it in memory, so
	// this path handles arbitrarily large files; the file/push pusher
	// streams the on-disk copy to each peer the same way.
	if a.user != nil && a.user.UID != "" {
		replication_emit_file_push(a.user.UID, app.id, file)
	}

	return sl.None, nil
}

// a.file(field) -> dict or None: Read uploaded file data
// Returns dict with: name, content_type, size, data (bytes)
func (a *Action) sl_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <field: string>")
	}

	field, ok := sl.AsString(args[0])
	if !ok || !valid(field, "constant") {
		return sl_error(fn, "invalid field %q", field)
	}

	form, err := a.web.MultipartForm()
	if err != nil {
		return sl.None, nil
	}

	files := form.File[field]
	if len(files) == 0 {
		return sl.None, nil
	}

	ff := files[0] // Get first file

	// Open and read file contents
	f, err := ff.Open()
	if err != nil {
		return sl_error(fn, "unable to open file: %v", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return sl_error(fn, "unable to read file: %v", err)
	}

	// Return a dict (not a struct) so it can be accessed with file["name"] syntax
	d := sl.NewDict(4)
	d.SetKey(sl.String("name"), sl.String(ff.Filename))
	d.SetKey(sl.String("content_type"), sl.String(ff.Header.Get("Content-Type")))
	d.SetKey(sl.String("size"), sl.MakeInt64(ff.Size))
	d.SetKey(sl.String("data"), sl.Bytes(data))
	return d, nil
}

// a.header(name, value?) -> string|None: Get request header (1 arg) or set response header (2 args)
func (a *Action) sl_header(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, value string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "value?", &value); err != nil {
		return nil, err
	}

	// One argument: read request header
	if len(args) == 1 && value == "" {
		return sl.String(a.web.GetHeader(name)), nil
	}

	// Two arguments: set response header
	a.web.Header(name, value)
	return sl.None, nil
}

// a.write.file(path) -> None: Serve file from app's data directory
func (a *Action) sl_write_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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

	t.SetLocal("file_serving", true)
	a.web.File(file)
	return sl.None, nil
}

// a.write.asset(path) -> None: Serve a bundled asset from the installed app directory
func (a *Action) sl_write_asset(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path); err != nil {
		return nil, err
	}

	if !valid(path, "filepath") {
		a.error(400, "Invalid path")
		return sl.None, nil
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		a.error(500, "No app")
		return sl.None, nil
	}

	user, _ := t.Local("user").(*User)
	file := app_local_path(app, user, path)
	if file == "" {
		a.error(500, "No active app version")
		return sl.None, nil
	}

	// Reject symlinks
	if file_is_symlink(file) {
		a.error(404, "File not found")
		return sl.None, nil
	}

	if !file_exists(file) {
		a.error(404, "File not found")
		return sl.None, nil
	}

	// Auto-set Content-Type if not already set
	if a.web.Writer.Header().Get("Content-Type") == "" {
		a.web.Header("Content-Type", file_name_type(path))
	}

	t.SetLocal("file_serving", true)
	a.web.File(file)
	return sl.None, nil
}

// a.write.stream(stream) -> int: Pipe Net stream content directly to HTTP response, returns bytes written
func (a *Action) sl_write_stream(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: write_from_stream(stream)")
	}

	stream, ok := args[0].(*Stream)
	if !ok {
		return sl_error(fn, "argument must be a Stream")
	}

	// Mark as file serving so the timeout handler waits for I/O to complete
	t.SetLocal("file_serving", true)

	// Get the raw reader (includes any buffered bytes from CBOR decoder)
	reader := stream.raw_reader()

	// Set Content-Type to octet-stream if not already set (avoids JSON interpretation)
	if a.web.Writer.Header().Get("Content-Type") == "" {
		a.web.Header("Content-Type", "application/octet-stream")
	}

	// Set status 200 on first write (matches a.print() pattern)
	if !a.web.Writer.Written() {
		a.web.Status(200)
	}

	// Copy stream data directly to HTTP response
	n, err := io.Copy(a.web.Writer, reader)
	if err != nil && !is_client_disconnect(err) {
		return sl_error(fn, "stream copy error: %v", err)
	}

	return sl.MakeInt64(n), nil
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

// a.cookie.get(name, default?) -> string or None: Get cookie value
func (c *ActionCookie) sl_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	var def sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "default?", &def); err != nil {
		return nil, err
	}

	value, err := c.action.web.Cookie(name)
	if err == nil {
		return sl.String(value), nil
	}

	// Cookie not found
	if def != nil {
		return def, nil
	}
	if c.action.active.Architecture.Version >= 4 {
		return sl.None, nil
	}
	return sl.String(""), nil
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
