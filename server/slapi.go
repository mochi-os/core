// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"html/template"
	"maps"
	"reflect"
	"slices"
)

var (
	slapi sl.StringDict
)

func init() {
	slapi = sl.StringDict{
		"mochi": sls.FromStringDict(sl.String("mochi"), sl.StringDict{
			"action": sls.FromStringDict(sl.String("action"), sl.StringDict{
				"dump":     sl.NewBuiltin("dump", slapi_action_dump),
				"error":    sl.NewBuiltin("error", slapi_action_error),
				"redirect": sl.NewBuiltin("redirect", slapi_action_redirect),
				"websocket": sls.FromStringDict(sl.String("websocket"), sl.StringDict{
					"write": sl.NewBuiltin("write", slapi_action_websocket_write),
				}),
				"write": sl.NewBuiltin("write", slapi_action_write),
			}),
			"attachments": sls.FromStringDict(sl.String("attachments"), sl.StringDict{
				"get":  sl.NewBuiltin("get", slapi_attachments_get),
				"put":  sl.NewBuiltin("put", slapi_attachments_put),
				"save": sl.NewBuiltin("save", slapi_attachments_save),
			}),
			"apps": sls.FromStringDict(sl.String("apps"), sl.StringDict{
				"icons": sl.NewBuiltin("icons", slapi_apps_icons),
			}),
			"db": sls.FromStringDict(sl.String("db"), sl.StringDict{
				"exists": sl.NewBuiltin("query", slapi_db_exists),
				"query":  sl.NewBuiltin("query", slapi_db_query),
			}),
			"directory": sls.FromStringDict(sl.String("directory"), sl.StringDict{
				"search": sl.NewBuiltin("search", slapi_directory_search),
			}),
			"event": sls.FromStringDict(sl.String("event"), sl.StringDict{
				"segment": sl.NewBuiltin("search", slapi_event_segment),
			}),
			"log": sls.FromStringDict(sl.String("log"), sl.StringDict{
				"debug": sl.NewBuiltin("debug", slapi_log),
				"info":  sl.NewBuiltin("info", slapi_log),
				"warn":  sl.NewBuiltin("warn", slapi_log),
			}),
			"message": sls.FromStringDict(sl.String("message"), sl.StringDict{
				"send": sl.NewBuiltin("search", slapi_message_send),
			}),
			"service": sls.FromStringDict(sl.String("service"), sl.StringDict{
				"call": sl.NewBuiltin("call", slapi_service_call),
			}),
			"text": sls.FromStringDict(sl.String("text"), sl.StringDict{
				"markdown": sls.FromStringDict(sl.String("markdown"), sl.StringDict{
					"render": sl.NewBuiltin("render", slapi_text_markdown_render),
				}),
				"uid":   sl.NewBuiltin("uid", slapi_text_uid),
				"valid": sl.NewBuiltin("valid", slapi_text_valid),
			}),
			"time": sls.FromStringDict(sl.String("time"), sl.StringDict{
				"local": sl.NewBuiltin("local", slapi_time_local),
				"now":   sl.NewBuiltin("local", slapi_time_now),
			}),
			"user": sls.FromStringDict(sl.String("user"), sl.StringDict{
				"get":    sl.NewBuiltin("get", slapi_user_get),
				"logout": sl.NewBuiltin("logout", slapi_user_logout),
			}),
		}),
	}
}

// Dump the variables passed for debugging
func slapi_action_dump(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var vars []any
	for _, v := range args {
		vars = append(vars, starlark_decode(v))
	}
	debug("%s() %+v", f.Name(), vars)

	a := t.Local("action").(*Action)
	if a != nil {
		a.dump(vars)
	}

	return sl.None, nil
}

// Print an error
func slapi_action_error(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return slapi_error(f, "syntax: <code: integer> <message: string>")
	}

	code, err := sl.AsInt32(args[0])
	if err != nil {
		return slapi_error(f, "invalid error code")
	}

	message, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(f, "invalid error message")
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(f, "called from non-action")
	}

	a.error(code, message)
	return sl.None, nil
}

// Redirect the action
func slapi_action_redirect(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return slapi_error(f, "syntax: <path: string>")
	}

	path, ok := sl.AsString(args[0])
	if !ok || !valid(path, "path") {
		return slapi_error(f, "invalid path '%s'", path)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(f, "called from non-action")
	}

	a.redirect(path)
	return sl.None, nil
}

// Write data back to the caller of the action
func slapi_action_write(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return slapi_error(f, "syntax: <template path: string> <format: string> [data: dictionary]")
	}

	path, ok := sl.AsString(args[0])
	if !ok || (path != "" && !valid(path, "path")) {
		return slapi_error(f, "invalid template file '%s'", path)
	}

	format, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(f, "invalid format '%s'", format)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(f, "called from non-action")
	}

	switch format {
	case "json":
		if len(args) < 3 {
			return slapi_error(f, "JSON called without data")
		}
		debug("Writing JSON for: %#v", starlark_decode(args[2]))
		a.json(starlark_decode(args[2]))

	default:
		// This should be done using ParseFS() followed by ParseFiles(), but I can't get this to work.
		file := fmt.Sprintf("%s/templates/en/%s.tmpl", a.app.base, path)
		if !file_exists(file) {
			return slapi_error(f, "template '%s' not found", path)
		}
		data := file_read(file)
		include := must(templates.ReadFile("templates/en/include.tmpl"))

		tmpl, err := template.New("").Parse(string(data) + "\n" + string(include))
		if err != nil {
			return slapi_error(f, "%v", err)
		}

		if len(args) > 2 {
			err = tmpl.Execute(a.web.Writer, starlark_decode(args[2]))
		} else {
			err = tmpl.Execute(a.web.Writer, Map{})
		}
		if err != nil {
			return slapi_error(f, "%v", err)
		}
	}

	return sl.None, nil
}

// Write data back to the caller of the action via websocket
func slapi_action_websocket_write(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return slapi_error(f, "syntax: <app: string> <content: any>")
	}

	app, ok := sl.AsString(args[0])
	if !ok || !valid(app, "constant") {
		return slapi_error(f, "invalid app '%s'", app)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(f, "no user")
	}

	websockets_send(user, app, starlark_decode(args[1]))
	return sl.None, nil
}

// Get available icons for home
func slapi_apps_icons(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	results := make([]map[string]string, len(icons))
	for j, i := range slices.Sorted(maps.Keys(icons)) {
		results[j] = map[string]string{"path": icons[i].Path, "label": icons[i].Label, "name": icons[i].Name, "icon": icons[i].Icon}
	}
	return starlark_encode(results), nil
}

// Get attachments for an object
func slapi_attachments_get(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(f, "syntax: <object: string>")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return slapi_error(f, "invalid object '%s'", object)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(f, "no user")
	}

	attachments := attachments(user, object)
	return starlark_encode(structs_to_maps(*attachments)), nil
}

// Upload attachments for an object
func slapi_attachments_put(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 4 {
		return slapi_error(f, "syntax: <field: string> <object: string> <entity: string> <save locally: boolean>")
	}

	field, ok := sl.AsString(args[0])
	if !ok || !valid(field, "constant") {
		return slapi_error(f, "field '%s'", field)
	}

	object, ok := sl.AsString(args[1])
	if !ok || !valid(object, "path") {
		return slapi_error(f, "invalid object '%s'", object)
	}

	entity, ok := sl.AsString(args[2])
	if !ok || !valid(entity, "entity") {
		return slapi_error(f, "invalid entity '%s'", entity)
	}

	local := bool(args[3].Truth())

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(f, "called from non-action")
	}

	attachments := a.upload_attachments(field, entity, object, local)
	return starlark_encode(structs_to_maps(*attachments)), nil
}

// Save attachments
func slapi_attachments_save(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return slapi_error(f, "syntax: <attachments: array of dictionaries> <object: string> <entity: string>")
	}

	attachments := starlark_decode_multi_strings(args[0])
	debug("slapi_attachments_save() got: %+v", attachments)

	object, ok := sl.AsString(args[1])
	if !ok || !valid(object, "path") {
		return slapi_error(f, "invalid object '%s'", object)
	}

	entity, ok := sl.AsString(args[2])
	if !ok || !valid(entity, "entity") {
		return slapi_error(f, "invalid entity '%s'", entity)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(f, "no user")
	}

	attachments_save_maps(attachments, user, entity, object)
	return sl.None, nil
}

// Check if database row exists
func slapi_db_exists(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return slapi_error(f, "syntax: <SQL statement: string> [parameters: strings, variadic]")
	}

	query, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(f, "invalid SQL statement '%s'", query)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(f, "no user")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return slapi_error(f, "unknown app")
	}

	db := db_app(user, app)
	if db.exists(query, starlark_decode(args[1:]).([]any)...) {
		return sl.True, nil
	}
	return sl.False, nil
}

// General database query
func slapi_db_query(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return slapi_error(f, "syntax: <SQL statement: string> [parameters: strings, variadic]")
	}

	query, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(f, "invalid SQL statement '%s'", query)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(f, "no user")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return slapi_error(f, "unknown app")
	}

	db := db_app(user, app)
	return starlark_encode(db.maps(query, starlark_decode(args[1:]).([]any)...)), nil
}

// Directory search
func slapi_directory_search(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return slapi_error(f, "syntax: <class: string> <search: string> <include self: boolean>")
	}

	class, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(f, "invalid class '%s'", class)
	}

	search, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(f, "invalid search '%s'", search)
	}

	include_self := bool(args[2].Truth())
	u := t.Local("user").(*User)

	db := db_open("db/directory.db")
	ds := db.maps("select * from directory where class=? and name like ? order by name, created", class, "%"+search+"%")

	for _, d := range *ds {
		d["fingerprint_hyphens"] = fingerprint_hyphens(d["fingerprint"].(string))
	}

	if u == nil || include_self || class != "person" {
		return starlark_encode(ds), nil
	}

	dbu := db_open("db/users.db")
	var es []Entity
	dbu.scans(&es, "select id from entities where user=?", u.ID)
	me := map[string]bool{}
	for _, e := range es {
		me[e.ID] = true
	}

	var o []map[string]any
	for _, d := range *ds {
		_, found := me[d["id"].(string)]
		if !found {
			o = append(o, d)
		}
	}
	return starlark_encode(&o), nil
}

// Helper function to return an error
func slapi_error(f *sl.Builtin, format string, values ...any) (sl.Value, error) {
	if f == nil {
		return sl.None, error_message(format, values...)
	} else {
		return sl.None, error_message(fmt.Sprintf("%s() %s", f.Name(), format), values...)
	}
}

// Decode the next segment of an event
func slapi_event_segment(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	debug("mochi.event.segment() decoding segment")
	e := t.Local("event").(*Event)
	if e == nil {
		return slapi_error(f, "called from non-event")
	}

	var v any
	err := e.decoder.Decode(&v)
	if err != nil {
		return nil, err
	}
	debug("mochi.event.segment() returning: %#v", v)
	return starlark_encode(v), nil
}

// Log message from app
func slapi_log(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return slapi_error(f, "syntax: <format: string> [values: strings, variadic]")
	}

	format, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(f, "invalid format")
	}

	a, ok := t.Local("app").(*App)
	if a == nil {
		format = fmt.Sprintf("%s(): %s", t.Local("function"), format)
	} else if ok {
		format = fmt.Sprintf("App %s:%s() %s", a.Name, t.Local("function"), format)
	}

	values := make([]any, len(args)-1)
	for i, a := range args[1:] {
		values[i] = starlark_decode(a)
	}

	switch f.Name() {
	case "debug":
		debug(format, values...)

	case "info":
		info(format, values...)

	case "warn":
		warn(format, values...)
	}

	return sl.None, nil
}

// Send a message
func slapi_message_send(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return slapi_error(f, "syntax: <headers: dictionary> [content: dictionary] [data: bytes]")
	}

	headers := starlark_decode_strings(args[0])
	if headers == nil {
		return slapi_error(f, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(f, "no user")
	}

	db := db_open("db/users.db")
	if !db.exists("select id from entities where id=? and user=?", headers["from"], user.ID) {
		return slapi_error(f, "invalid from header")
	}

	if !valid(headers["to"], "entity") {
		return slapi_error(f, "invalid to header")
	}

	if !valid(headers["service"], "constant") {
		return slapi_error(f, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return slapi_error(f, "invalid event header")
	}

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])
	if len(args) > 1 {
		debug("mochi.message.send() content: %+v", starlark_decode_strings(args[1]))
		m.content = starlark_decode_strings(args[1])
	}

	if len(args) > 2 {
		debug("mochi.message.send() adding segment: %#v", starlark_decode(args[2]))
		m.add(starlark_decode(args[2]))
	}

	m.send()
	return sl.None, nil
}

// Call a function in another app
func slapi_service_call(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 {
		return slapi_error(f, "syntax: <service: string> <function: string> [parameters: any variadic]")
	}

	service, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(f, "invalid service")
	}

	function, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(f, "invalid function")
	}

	// Check for deep recursion
	depth := 1
	depth_var := t.Local("depth")
	if depth_var != nil {
		depth = depth_var.(int)
	}
	if depth > 1000 {
		return slapi_error(f, "reached maximum recursion depth")
	}

	// Look for matching app function, using default if necessary
	a, _ := services[service]
	if a == nil {
		return slapi_error(f, "unknown service '%s'", service)
	}
	fn, found := a.Services[service].Functions[function]
	if !found {
		fn, found = a.Services[service].Functions[""]
	}
	if !found {
		return slapi_error(f, "unknown function '%s' for service '%s'", function, service)
	}

	// Call function
	s := a.starlark()
	s.set("app", a)
	s.set("user", t.Local("user").(*User))
	s.set("owner", t.Local("owner").(*User))
	s.set("depth", depth+1)

	debug("mochi.service.call() calling app '%s' service '%s' function '%s' args: %+v", a.Name, service, function, args[2:])
	var result sl.Value
	var err error
	if len(args) > 2 {
		result, err = s.call(fn.Function, args[2:])
	} else {
		result, err = s.call(fn.Function, nil)
	}
	debug("mochi.service.call() got result (type %s): %+v", reflect.TypeOf(result), result)

	return result, err
}

// Render markdown
func slapi_text_markdown_render(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	//TODO slapi_text_markdown_render()
	return sl.None, nil
}

// Get a UID
func slapi_text_uid(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return starlark_encode(uid()), nil
}

// Check if a string is valid
func slapi_text_valid(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return slapi_error(f, "syntax: <string to check: string> <pattern to match: string>")
	}

	s, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(f, "invalid string to check '%s'", s)
	}

	match, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(f, "invalid match pattern '%s'", match)
	}

	return starlark_encode(valid(s, match)), nil
}

// Return the local time in the user's time zone
func slapi_time_local(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	//TODO slapi_time_local()
	return sl.None, nil
}

// Return the current Unix time
func slapi_time_now(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return starlark_encode(now()), nil
}

// Get details of the current user
func slapi_user_get(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(f, "no user")
	}

	return starlark_encode(map[string]any{"id": a.user.ID, "username": a.user.Username}), nil
}

// Log the user out
func slapi_user_logout(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(f, "called from non-action")
	}

	login := web_cookie_get(a.web, "login", "")
	if login != "" {
		login_delete(login)
	}
	web_cookie_unset(a.web, "login")

	return sl.None, nil
}
