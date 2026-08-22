// Mochi server: Actions
// Copyright © 2026 Mochisoft OÜ
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
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
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
	// entity is the entity this action was routed to, nil for a class-level
	// action. routing names how it was reached - see a.routing below.
	entity  *Entity
	routing string
	// definition is the action as its app.json declares it, kept so serving can
	// consult what the app said it was doing rather than infer it from how the
	// request happened to arrive.
	definition *AppAction
}

// action_entity is the routed entity as a.entity sees it. A dict, not an
// object: `class` is a Starlark reserved word, so a.entity.class would not
// parse.
func action_entity(e *Entity) sl.Value {
	return sl_encode(map[string]any{
		"id":    e.ID,
		"class": e.Class,
		"name":  e.Name,
	})
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

	// Explicit status and type: actions run under gin's NoRoute handler, which
	// pre-sets 404, and responses carry X-Content-Type-Options: nosniff, so
	// neither may be left implicit.
	a.web.Status(200)
	a.web.Header("Content-Type", "text/html; charset=utf-8")
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

// error_label writes an error from core's own label catalogue in the request's
// language; a.error.label reads the app's, which cannot hold core's own
// strings. Passed as an argument, not a format string, so a percent verb cannot
// corrupt it.
func (a *Action) error_label(code int, key string) {
	a.error(code, "%s", resolve_core_label(request_language(a.web, a.user), key, nil))
}

// Display an error as a simple HTML page
func (a *Action) error(code int, format string, values ...any) {
	message := fmt.Sprintf(format, values...)

	// Return JSON for API requests, HTML for browser requests
	if strings.Contains(a.web.GetHeader("Accept"), "application/json") ||
		strings.HasPrefix(a.web.GetHeader("Content-Type"), "application/json") {
		a.web.JSON(code, gin.H{"error": message})
		return
	}

	a.web.Status(code)
	a.web.Header("Content-Type", "text/html; charset=utf-8")
	a.web.Writer.WriteString("<html><head><title>Error</title></head><body>")
	a.web.Writer.WriteString(fmt.Sprintf("<h1>Error %d</h1>", code))
	a.web.Writer.WriteString("<pre>")
	a.web.Writer.WriteString(template.HTMLEscapeString(message))
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
	return []string{"access", "body", "domain", "dump", "entity", "error", "file", "files", "header", "input", "inputs", "json", "logout", "owner", "print", "redirect", "routing", "template", "token", "upload", "user", "write"}
}

func (a *Action) Attr(name string) (sl.Value, error) {
	switch name {
	case "access":
		return &ActionAccess{action: a}, nil
	case "body":
		return sl.String(a.body), nil
	case "entity":
		// None on a class-level action, so an app can tell "no entity" from
		// "an entity I know nothing about".
		if a.entity == nil {
			return sl.None, nil
		}
		return action_entity(a.entity), nil
	case "owner":
		// Does the AUTHENTICATED caller own the routed entity. a.user, not the
		// effective user: web_action substitutes the owner for an anonymous request
		// to a public action. Always a bool, so a class-level `if a.owner` fails
		// closed.
		if a.user == nil || a.entity == nil {
			return sl.Bool(false), nil
		}
		return sl.Bool(a.entity.User == a.user.UID), nil
	case "routing":
		return sl.String(a.routing), nil
	case "domain":
		return a.domain, nil
	case "dump":
		return sl.NewBuiltin("dump", a.sl_dump), nil
	case "error":
		return &ActionError{action: a}, nil
	case "file":
		return sl.NewBuiltin("file", a.sl_file), nil
	case "files":
		return sl.NewBuiltin("files", a.sl_files), nil
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
func (w *ActionWrite) AttrNames() []string {
	return []string{"asset", "cache", "file", "stream"}
}
func (w *ActionWrite) Attr(name string) (sl.Value, error) {
	switch name {
	case "asset":
		return sl.NewBuiltin("write.asset", w.action.sl_write_asset), nil
	case "cache":
		return sl.NewBuiltin("write.cache", w.action.sl_write_cache), nil
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

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	owner := principal_owner(t)
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
// kwargs become ICU MessageFormat substitutions; the language is the caller's
// preference or Accept-Language. Pass log=False to suppress the log line.
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

	app_local := principal_app(t)
	user := principal_caller(t)
	language := request_language(a.web, user)
	var av *AppVersion
	if app_local != nil {
		av = app_local.active(user)
	}

	message := key
	if av != nil {
		message = resolve_label(av, language, key, margs)
	}
	if log_it {
		debug("sl_error_label() %d %s -> %s", int(code), key, message)
	}
	a.error(int(code), "%s", message)
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

// a.upload(field, file, index=0) -> int: Save an uploaded file to the app's
// file storage, returning its size. index selects which file to save when a
// field carries several (a multi-file attachment upload); it defaults to the
// first, so single-file callers are unchanged.
func (a *Action) sl_upload(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var field, file string
	var index int
	var maximum int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "field", &field, "file", &file, "index?", &index, "maximum?", &maximum); err != nil {
		return sl_error(fn, "syntax: upload(field, file, index=0, maximum=0)")
	}
	if !valid(field, "constant") {
		return sl_error(fn, "invalid field %q", field)
	}
	if !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app")
	}

	ff := a.upload_header(field, index)
	if ff == nil {
		return sl_error(fn, "no file %d for field %q", index, field)
	}

	// Per-call ceiling: an attachment must stay within what a transfer can carry,
	// or subscribers receive a prefix. Zero leaves the quota as the only bound.
	if maximum > 0 && ff.Size > maximum {
		return sl_error(fn, "file too large: %d bytes exceeds %d", ff.Size, maximum)
	}

	// The thread's user, not a.user: a.user is nil on an anonymous request to a
	// public action, and every mochi.file.* call reads the thread's user, so the
	// upload lands where those calls read it back.
	user, _ := principal_storage(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Check storage limit (10GB per user across all apps; admins exempt)
	remaining, err := user_storage_remaining(user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if ff.Size > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	// Through os.Root, like every other write to app storage: the path string is
	// validated but a symlink is not in the string, and MkdirAll+Create both
	// follow one. Create the base first - OpenRoot will not.
	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to open file storage: %v", err)
	}
	defer root.Close()

	source, err := ff.Open()
	if err != nil {
		return sl_error(fn, "unable to read upload for field %q: %v", field, err)
	}
	defer source.Close()

	written, err := root_write_file(root, file, source)
	if err != nil {
		return sl_error(fn, "unable to write file for field %q: %v", field, err)
	}

	return sl.MakeInt64(written), nil
}

// upload_header returns the index-th uploaded file header for a field, or nil.
// FormFile reads only the first; the attachment library uploads each file of a
// multi-file field by index.
func (a *Action) upload_header(field string, index int) *multipart.FileHeader {
	form, err := a.web.MultipartForm()
	if err != nil {
		return nil
	}
	files := form.File[field]
	if index < 0 || index >= len(files) {
		return nil
	}
	return files[index]
}

// a.files(field) -> list: Metadata for every uploaded file in a field, each a
// dict of name, content_type, size - without the bytes. Pair with
// a.upload(field, path, index=i) to stream each to storage. a.file(field)
// remains the single-file, data-included form.
func (a *Action) sl_files(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var field string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "field", &field); err != nil {
		return sl_error(fn, "syntax: files(field)")
	}
	form, err := a.web.MultipartForm()
	if err != nil {
		return sl_encode([]map[string]any{}), nil
	}
	results := []map[string]any{}
	for _, ff := range form.File[field] {
		results = append(results, map[string]any{
			"name":         ff.Filename,
			"content_type": ff.Header.Get("Content-Type"),
			"size":         ff.Size,
		})
	}
	return sl_encode(results), nil
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

// header_readable lists the request headers an app may read. An allowlist, not a
// denylist: Cookie carries the session, which web_auth treats as a bearer
// credential, and Authorization carries whatever the client sent - but naming
// only those two would expose by default every sensitive header added later.
// Keys are canonical, because Header.Get canonicalises and "cookie" therefore
// reaches the same value as "Cookie".
var header_readable = map[string]bool{
	"Accept":           true,
	"Accept-Language":  true,
	"Content-Type":     true,
	"Referer":          true,
	"Sec-Fetch-Site":   true,
	"Stripe-Signature": true,
	"User-Agent":       true,
}

// header_writable lists the response headers an app may set. Header.Set
// replaces and an empty value deletes, so without this an app could write
// Set-Cookie - the session-fixation primitive a.cookie.set was removed for -
// or delete the headers web_security_headers sets, that middleware running
// before the handler and writing to the same map. Same allowlist reasoning
// and same canonical keys as header_readable. Location is absent because
// a.redirect owns it.
var header_writable = map[string]bool{
	"Cache-Control":       true,
	"Content-Disposition": true,
	"Content-Type":        true,
	"Etag":                true,
	"Last-Modified":       true,
}

// a.header(name, value?) -> string|None: Get request header (1 arg) or set response header (2 args)
func (a *Action) sl_header(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, value string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "value?", &value); err != nil {
		return nil, err
	}

	// One argument: read request header
	if len(args) == 1 && value == "" {
		if !header_readable[textproto.CanonicalMIMEHeaderKey(name)] {
			return sl_error(fn, "header %q is not readable by an app", name)
		}
		return sl.String(a.web.GetHeader(name)), nil
	}

	// Two arguments: set response header. An empty value still deletes, which
	// is what gin's Header does - the allowlist bounds which header that can
	// reach, not what an app may do to its own.
	if !header_writable[textproto.CanonicalMIMEHeaderKey(name)] {
		return sl_error(fn, "header %q is not writable by an app", name)
	}
	a.web.Header(name, value)
	return sl.None, nil
}

// a.write.file(path) -> None: Serve file from app's data directory
//
// Opened through os.Root, so a symlink cannot serve outside the app's
// directory; links that stay inside still resolve. A directory is answered from
// its index.html or not at all - http.ServeFile would generate a browsable
// index.
func (a *Action) sl_write_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path); err != nil {
		return nil, err
	}

	if !valid(path, "filepath") {
		a.error_label(400, "errors.invalid_file")
		return sl.None, nil
	}

	owner := principal_owner(t)

	// A hosted domain publishes one account's files to every visitor alike, so the
	// route's owner picks the directory, not the requester. Only the bytes move:
	// the action's owner must not, and an unresolvable route owner fails closed.
	if a.domain != nil && a.domain.route != nil {
		if a.domain.route.owner == nil {
			warn("Domain route serving %q has no resolvable owner; refusing to serve a file", a.web.Request.Host)
			a.error_label(500, "errors.server_error")
			return sl.None, nil
		}
		owner = a.domain.route.owner
	}

	if owner == nil {
		a.error_label(500, "errors.server_error")
		return sl.None, nil
	}

	app := principal_app(t)
	if app == nil {
		a.error_label(500, "errors.server_error")
		return sl.None, nil
	}

	root, err := os.OpenRoot(api_file_base(owner, app))
	if err != nil {
		a.error_label(404, "errors.file_not_found")
		return sl.None, nil
	}
	defer root.Close()

	file, err := root.Open(path)
	if err != nil {
		a.error_label(404, "errors.file_not_found")
		return sl.None, nil
	}
	information, err := file.Stat()
	if err != nil {
		file.Close()
		a.error_label(404, "errors.file_not_found")
		return sl.None, nil
	}

	if information.IsDir() {
		file.Close()

		// Relative links in the index resolve against the request path, so it
		// has to end in a slash before the page is served. This is the redirect
		// http.ServeFile issued for the same case.
		if !strings.HasSuffix(a.web.Request.URL.Path, "/") {
			target := a.web.Request.URL.Path[strings.LastIndex(a.web.Request.URL.Path, "/")+1:] + "/"
			if a.web.Request.URL.RawQuery != "" {
				target += "?" + a.web.Request.URL.RawQuery
			}
			a.web.Redirect(http.StatusMovedPermanently, target)
			return sl.None, nil
		}

		path = strings.TrimSuffix(path, "/") + "/index.html"
		file, err = root.Open(path)
		if err != nil {
			a.error_label(404, "errors.file_not_found")
			return sl.None, nil
		}
		information, err = file.Stat()
		if err != nil || information.IsDir() {
			file.Close()
			a.error_label(404, "errors.file_not_found")
			return sl.None, nil
		}
	}
	defer file.Close()

	// A files directory holds content someone else supplied, so anything outside
	// the inline allowlist downloads and SVG is sanitized. Exempt only when the
	// action declares "site": true - a domain route is not itself a licence to
	// render.
	if a.definition == nil || !a.definition.Site {
		content_type := file_name_type(path)
		if content_type == "image/svg+xml" {
			starlark_serving_set(t, a.web.Writer)
			web_serve_svg(a.web, file)
			return sl.None, nil
		}
		if !content_type_inline(content_type) && a.web.Writer.Header().Get("Content-Disposition") == "" {
			a.web.Header("Content-Disposition", "attachment")
		}
	}

	// The bytes depend on whose directory is read, so the response must never
	// reach a shared cache. must-revalidate plus an ETag keeps a repeat fetch
	// cheap and stops a replaced or deleted file being served from cache.
	if a.web.Writer.Header().Get("Cache-Control") == "" {
		a.web.Header("Cache-Control", "private, must-revalidate")
	}
	if a.web.Writer.Header().Get("Etag") == "" {
		a.web.Header("ETag", fmt.Sprintf(`"%x-%x"`, information.ModTime().UnixNano(), information.Size()))
	}

	starlark_serving_set(t, a.web.Writer)
	http.ServeContent(a.web.Writer, a.web.Request, path, information.ModTime(), file)
	return sl.None, nil
}

// a.write.cache(name, content_type="") -> bool: Serve a cache entry to the HTTP
// response, returning False on a cache miss so the caller can fill and retry.
// The calling action MUST authorise the request first, as with a.write.file.
// Content type and disposition are set safely by core.
func (a *Action) sl_write_cache(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, content_type string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "content_type?", &content_type); err != nil {
		return nil, err
	}

	path, err := cache_serve_file(t, name)
	if err != nil {
		return sl.False, nil
	}

	if content_type == "" {
		content_type = file_name_type(name)
	}
	base := content_type_base(content_type)

	file, err := os.Open(path)
	if err != nil {
		return sl.False, nil
	}
	defer file.Close()
	information, err := file.Stat()
	if err != nil || information.IsDir() {
		return sl.False, nil
	}

	if base == "image/svg+xml" {
		starlark_serving_set(t, a.web.Writer)
		web_serve_svg(a.web, file)
		return sl.True, nil
	}

	if a.web.Writer.Header().Get("Content-Type") == "" {
		a.web.Header("Content-Type", content_type)
	}
	if !content_type_inline(base) && a.web.Writer.Header().Get("Content-Disposition") == "" {
		a.web.Header("Content-Disposition", "attachment")
	}
	if a.web.Writer.Header().Get("Cache-Control") == "" {
		a.web.Header("Cache-Control", "private, must-revalidate")
	}
	if a.web.Writer.Header().Get("Etag") == "" {
		a.web.Header("ETag", fmt.Sprintf(`"%x-%x"`, information.ModTime().UnixNano(), information.Size()))
	}

	starlark_serving_set(t, a.web.Writer)
	http.ServeContent(a.web.Writer, a.web.Request, path, information.ModTime(), file)
	return sl.True, nil
}

// a.write.asset(path) -> None: Serve a bundled asset from the installed app directory
func (a *Action) sl_write_asset(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var path string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "path", &path); err != nil {
		return nil, err
	}

	if !valid(path, "filepath") {
		a.error_label(400, "errors.invalid_file")
		return sl.None, nil
	}

	app := principal_app(t)
	if app == nil {
		a.error_label(500, "errors.server_error")
		return sl.None, nil
	}

	user := principal_caller(t)
	file := app_asset_path(app, user, path)
	if file == "" {
		a.error_label(404, "errors.file_not_found")
		return sl.None, nil
	}

	// An app's bundled SVG is attacker-controlled under the untrusted-app model,
	// so it is sanitized here as the static-file routes do. Overrides any
	// Content-Type the app set.
	if strings.HasSuffix(strings.ToLower(path), ".svg") {
		starlark_serving_set(t, a.web.Writer)
		web_serve_svg_path(a.web, file)
		return sl.None, nil
	}

	// Auto-set Content-Type if not already set
	if a.web.Writer.Header().Get("Content-Type") == "" {
		a.web.Header("Content-Type", file_name_type(path))
	}

	starlark_serving_set(t, a.web.Writer)
	a.web.File(file)
	return sl.None, nil
}

// stream_maximum_default backstops a.write.stream when the app names no limit.
// It matches object_maximum because a repository archive and a market asset
// download legitimately run that large. Apps relaying images should pass less.
const stream_maximum_default = object_maximum

// stream_limit_reader relays at most remaining bytes and records whether the far
// end tried to send more, so the caller can tell a complete body from a curtailed
// one. Returning io.EOF at the limit stops io.Copy without inventing an error for
// the honest case where the body simply ended.
type stream_limit_reader struct {
	reader    io.Reader
	remaining int64
	exceeded  bool
	// client and app key the byte budgets this relay is charged against. Empty
	// client disables metering, for relays with no HTTP caller to attribute.
	client string
	app    string
	// spent records that a budget ran out mid-relay, which is reported
	// differently from exceeding the caller's own per-call cap: one is this
	// client taking more than their share, the other is the far end sending more
	// than the app allowed.
	spent bool
}

func (r *stream_limit_reader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		// One speculative byte distinguishes "ended exactly at the limit" from
		// "had more to give", which is the difference between a complete asset
		// and a truncated one.
		var probe [1]byte
		if n, _ := r.reader.Read(probe[:]); n > 0 {
			r.exceeded = true
		}
		return 0, io.EOF
	}
	// Checked per read, not once up front: a single relay can be a gigabyte, so a
	// budget that was open when it started can be gone long before it finishes.
	// Stopping here is what makes the budget a bound on traffic rather than a
	// bound on how many relays may BEGIN.
	if r.client != "" && stream_bytes_refusal(r.client, r.app) != nil {
		r.spent = true
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	if r.client != "" && n > 0 {
		stream_bytes_charge(r.client, r.app, n)
	}
	return n, err
}

// a.write.stream(stream, maximum=bytes, cache=name) -> int: Pipe Net stream
// content directly to HTTP response, returns bytes written
//
// cache names an entry to fill as a side effect: only a complete relay is
// renamed into place, so a partial body never becomes a cache hit.
func (a *Action) sl_write_stream(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var stream_value sl.Value
	maximum := int64(stream_maximum_default)
	cache := ""
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "stream", &stream_value, "maximum?", &maximum, "cache?", &cache); err != nil {
		return sl_error(fn, "syntax: write.stream(stream, maximum=bytes, cache=name)")
	}
	if maximum <= 0 || maximum > stream_maximum_default {
		return sl_error(fn, "maximum must be between 1 and %d", int64(stream_maximum_default))
	}

	stream, ok := stream_value.(*Stream)
	if !ok {
		return sl_error(fn, "argument must be a Stream")
	}

	// Mark as file serving so the timeout handler waits for I/O to complete
	starlark_serving_set(t, a.web.Writer)

	// raw_reader is UNWRAPPED - the CBOR limit caps decoded messages, not the byte
	// body - and every caller relays bytes a remote peer chose, so bound it first.
	// Refuse an exhausted budget before writing: the status cannot be retracted.
	app_id := ""
	if a.app != nil {
		app_id = a.app.id
	}
	client := app_id + "/" + rate_limit_client_ip(a.web)
	if refusal := stream_bytes_refusal(client, app_id); refusal != nil {
		return sl_error(fn, refusal)
	}

	limited := &stream_limit_reader{
		reader:    stream.raw_reader(),
		remaining: maximum,
		client:    client,
		app:       app_id,
	}
	var reader io.Reader = limited

	// Cache fill is best effort: a tee that cannot start still serves.
	filled := false
	if cache != "" {
		if tee, err := cache_tee_start(t, cache); err == nil {
			defer func() { tee.finish(filled) }()
			reader = io.TeeReader(reader, tee.file)
		}
	}

	// Set Content-Type to octet-stream if not already set (avoids JSON interpretation)
	if a.web.Writer.Header().Get("Content-Type") == "" {
		a.web.Header("Content-Type", "application/octet-stream")
	}

	// The content type is the remote peer's claim, not this server's: sanitize
	// SVG, inline other known media, and force everything else to download, as
	// web_serve_attachment does for stored files.
	content_type := content_type_base(a.web.Writer.Header().Get("Content-Type"))
	if content_type == "image/svg+xml" {
		written, err := a.write_stream_svg(fn, reader)
		if err != nil {
			return written, err
		}
		// Same overrun check as the plain path below: an SVG larger than the
		// caller allowed must not be reported as a complete document either.
		if limited.exceeded {
			n, _ := sl.AsInt32(written)
			return a.write_stream_curtailed(fn, maximum, int64(n))
		}
		filled = true
		return written, nil
	}
	if !content_type_inline(content_type) && a.web.Writer.Header().Get("Content-Disposition") == "" {
		a.web.Header("Content-Disposition", "attachment")
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
	if limited.spent {
		return a.write_stream_spent(fn, n)
	}
	if limited.exceeded {
		return a.write_stream_curtailed(fn, maximum, n)
	}

	// A clean copy error already returned above; reaching here means the body
	// arrived complete, so the tee may be committed to the cache.
	filled = true
	return sl.MakeInt64(n), nil
}

// write_stream_spent reports a relay stopped because the caller's byte budget ran
// out mid-transfer, as distinct from the far end exceeding the app's per-call cap.
// Erroring rather than returning the count keeps a partial body from being taken
// for a complete one, exactly as the cap does.
func (a *Action) write_stream_spent(fn *sl.Builtin, written int64) (sl.Value, error) {
	app := ""
	if a.app != nil {
		app = a.app.id
	}
	if rate_limit_refusal_log.allow(app) {
		warn("actions: %s stopped a relay at %d bytes: the caller's byte budget is spent", app, written)
	}
	return sl_error(fn, &RateLimitError{
		Retry:  rate_limit_stream_client.retry(app + "/" + rate_limit_client_ip(a.web)),
		detail: strconv.Itoa(rate_limit_stream_client.limit) + " kilobytes relayed per minute per client",
	})
}

// write_stream_curtailed reports a peer that sent more than the caller allowed.
// It errors rather than returning the byte count so the caller cannot cache or
// announce a truncated asset as complete.
func (a *Action) write_stream_curtailed(fn *sl.Builtin, maximum, written int64) (sl.Value, error) {
	app := ""
	if a.app != nil {
		app = a.app.id
	}
	warn("actions: %s curtailed a stream at %d of at most %d bytes: the far end had more to send",
		app, written, maximum)
	return sl_error(fn, "stream exceeded %d bytes", maximum)
}

// stream_svg_maximum caps how much of a streamed SVG is buffered for
// sanitizing. An SVG larger than this is served as a download instead, which
// is equally safe and avoids holding an unbounded response in memory.
const stream_svg_maximum = 2 * 1024 * 1024

// write_stream_svg buffers a streamed SVG, sanitizes it, and serves it under
// svg_content_policy. Sanitizing needs the whole document, so unlike the plain
// copy path this cannot stream straight through.
func (a *Action) write_stream_svg(fn *sl.Builtin, reader io.Reader) (sl.Value, error) {
	buffer, err := io.ReadAll(io.LimitReader(reader, stream_svg_maximum+1))
	if err != nil && !is_client_disconnect(err) {
		return sl_error(fn, "stream read error: %v", err)
	}

	if len(buffer) > stream_svg_maximum {
		// Too large to sanitize in memory. A download cannot execute, so serve
		// it as one rather than refusing or truncating.
		a.web.Header("Content-Disposition", "attachment")
		if !a.web.Writer.Written() {
			a.web.Status(200)
		}
		written, err := a.web.Writer.Write(buffer)
		if err != nil && !is_client_disconnect(err) {
			return sl_error(fn, "stream copy error: %v", err)
		}
		n, err := io.Copy(a.web.Writer, reader)
		if err != nil && !is_client_disconnect(err) {
			return sl_error(fn, "stream copy error: %v", err)
		}
		// reader is the caller's limited reader, so this copy is bounded too; the
		// overrun is reported by sl_write_stream once the copy returns.
		return sl.MakeInt64(int64(written) + n), nil
	}

	a.web.Header("Content-Security-Policy", svg_content_policy)
	if !a.web.Writer.Written() {
		a.web.Status(200)
	}
	n, err := a.web.Writer.Write(svg_sanitize(buffer))
	if err != nil && !is_client_disconnect(err) {
		return sl_error(fn, "stream copy error: %v", err)
	}

	return sl.MakeInt64(int64(n)), nil
}
