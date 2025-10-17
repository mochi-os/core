// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"html/template"
	"sort"
	"strconv"
	"strings"
)

var (
	slapi sl.StringDict
)

func init() {
	slapi = sl.StringDict{
		"mochi": sls.FromStringDict(sl.String("mochi"), sl.StringDict{
			//TODO Remove all action functions once all apps are on version 2
			"action": sls.FromStringDict(sl.String("action"), sl.StringDict{
				"dump":  sl.NewBuiltin("mochi.action.dump", slapi_action_dump),
				"error": sl.NewBuiltin("mochi.action.error", slapi_action_error),
				"file": sls.FromStringDict(sl.String("file"), sl.StringDict{
					"name":   sl.NewBuiltin("mochi.action.file.name", slapi_action_file_name),
					"upload": sl.NewBuiltin("mochi.action.file.upload", slapi_action_file_upload),
				}),
				"json":     sl.NewBuiltin("mochi.action.json", slapi_action_json),
				"redirect": sl.NewBuiltin("mochi.action.redirect", slapi_action_redirect),
				"write":    sl.NewBuiltin("mochi.action.write", slapi_action_write),
			}),
			"attachment": sls.FromStringDict(sl.String("attachment"), sl.StringDict{
				"get":  sl.NewBuiltin("mochi.attachment.get", slapi_attachment_get),
				"put":  sl.NewBuiltin("mochi.attachment.put", slapi_attachment_put),
				"save": sl.NewBuiltin("mochi.attachment.save", slapi_attachment_save),
			}),
			"app": sls.FromStringDict(sl.String("app"), sl.StringDict{
				"get":     sl.NewBuiltin("mochi.app.get", slapi_app_get),
				"icons":   sl.NewBuiltin("mochi.app.icons", slapi_app_icons),
				"install": sl.NewBuiltin("mochi.app.install", slapi_app_install),
				"list":    sl.NewBuiltin("mochi.app.list", slapi_app_list),
			}),
			"db": sls.FromStringDict(sl.String("db"), sl.StringDict{
				"exists": sl.NewBuiltin("mochi.db.exists", slapi_db_query),
				"query":  sl.NewBuiltin("mochi.db.query", slapi_db_query),
				"row":    sl.NewBuiltin("mochi.db.row", slapi_db_query),
			}),
			"directory": sls.FromStringDict(sl.String("directory"), sl.StringDict{
				"search": sl.NewBuiltin("mochi.directory.search", slapi_directory_search),
			}),
			"entity": sls.FromStringDict(sl.String("directory"), sl.StringDict{
				"create":      sl.NewBuiltin("mochi.entity.create", slapi_entity_create),
				"fingerprint": sl.NewBuiltin("mochi.entity.fingerprint", slapi_entity_fingerprint),
				"get":         sl.NewBuiltin("mochi.entity.get", slapi_entity_get),
			}),
			"file": sls.FromStringDict(sl.String("file"), sl.StringDict{
				"delete": sl.NewBuiltin("mochi.file.delete", slapi_file_delete),
				"exists": sl.NewBuiltin("mochi.file.exists", slapi_file_exists),
				"list":   sl.NewBuiltin("mochi.file.list", slapi_file_list),
				"read":   sl.NewBuiltin("mochi.file.read", slapi_file_read),
				"write":  sl.NewBuiltin("mochi.file.write", slapi_file_write),
			}),
			"log": sls.FromStringDict(sl.String("log"), sl.StringDict{
				"debug": sl.NewBuiltin("mochi.log.debug", slapi_log),
				"info":  sl.NewBuiltin("mochi.log.info", slapi_log),
				"warn":  sl.NewBuiltin("mochi.log.warn", slapi_log),
			}),
			"markdown": sls.FromStringDict(sl.String("markdown"), sl.StringDict{
				"render": sl.NewBuiltin("mochi.markdown.render", slapi_markdown_render),
			}),
			"message": sls.FromStringDict(sl.String("message"), sl.StringDict{
				"send": sl.NewBuiltin("mochi.message.send", slapi_message_send),
			}),
			"random": sls.FromStringDict(sl.String("random"), sl.StringDict{
				"alphanumeric": sl.NewBuiltin("mochi.random.alphanumeric", slapi_random_alphanumeric),
			}),
			"service": sls.FromStringDict(sl.String("service"), sl.StringDict{
				"call": sl.NewBuiltin("mochi.service.call", slapi_service_call),
			}),
			"stream": sl.NewBuiltin("mochi.stream", slapi_stream),
			"time": sls.FromStringDict(sl.String("time"), sl.StringDict{
				"local": sl.NewBuiltin("mochi.time.local", slapi_time_local),
				"now":   sl.NewBuiltin("mochi.time.now", slapi_time_now),
			}),
			"uid": sl.NewBuiltin("mochi.uid", slapi_uid),
			"user": sls.FromStringDict(sl.String("user"), sl.StringDict{
				"get":    sl.NewBuiltin("mochi.user.get", slapi_user_get),
				"logout": sl.NewBuiltin("mochi.user.logout", slapi_user_logout),
			}),
			"valid": sl.NewBuiltin("mochi.valid", slapi_valid),
			"websocket": sls.FromStringDict(sl.String("websocket"), sl.StringDict{
				"write": sl.NewBuiltin("mochi.websocket.write", slapi_websocket_write),
			}),
		}),
	}
}

// Dump the variables passed for debugging
func slapi_action_dump(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var vars []any
	for _, v := range args {
		vars = append(vars, starlark_decode(v))
	}
	debug("%s() %+v", fn.Name(), vars)

	a := t.Local("action").(*Action)
	if a != nil {
		a.dump(vars)
	}

	return sl.None, nil
}

// Print an error
func slapi_action_error(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return slapi_error(fn, "syntax: <code: integer> <message: string>")
	}

	code, err := sl.AsInt32(args[0])
	if err != nil {
		return slapi_error(fn, "invalid error code")
	}

	message, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(fn, "invalid error message")
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "called from non-action")
	}

	a.error(code, message)
	return sl.None, nil
}

// Get the name of an uploaded file
func slapi_action_file_name(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <field: string>")
	}

	field, ok := sl.AsString(args[0])
	if !ok || !valid(field, "constant") {
		return slapi_error(fn, "invalid field '%s'", field)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "called from non-action")
	}

	ff, err := a.web.FormFile(field)
	if err != nil {
		return slapi_error(fn, "unable to get name of file field '%s': %v", field, err)
	}

	return starlark_encode(ff.Filename), nil
}

// Write the contents of an uploaded file
func slapi_action_file_upload(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return slapi_error(fn, "syntax: <field: string> <file: string>")
	}

	field, ok := sl.AsString(args[0])
	if !ok || !valid(field, "constant") {
		return slapi_error(fn, "invalid field '%s'", field)
	}

	file, ok := sl.AsString(args[1])
	if !ok || !valid(field, "filepath") {
		return slapi_error(fn, "invalid file '%s'", file)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "called from non-action")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return slapi_error(fn, "no app")
	}

	ff, err := a.web.FormFile(field)
	if err != nil {
		return slapi_error(fn, "unable to get file field '%s': %v", field, err)
	}

	err = a.web.SaveUploadedFile(ff, slapi_file(user, app, file))
	if err != nil {
		return slapi_error(fn, "unable to write file for field '%s': %v", field, err)
	}

	return sl.None, nil
}

// Write JSON back to the caller of the function
func slapi_action_json(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <data: dictionary>")
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "called from non-action")
	}

	a.json(starlark_decode(args[0]))

	return sl.None, nil
}

// Redirect the action
func slapi_action_redirect(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return slapi_error(fn, "syntax: <path: string>")
	}

	path, ok := sl.AsString(args[0])
	if !ok || !valid(path, "path") {
		return slapi_error(fn, "invalid path '%s'", path)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "called from non-action")
	}

	a.redirect(path)
	return sl.None, nil
}

// Write data back to the caller of the action
// This can be removed when all apps have been transitioned to React
func slapi_action_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return slapi_error(fn, "syntax: <template path: string> <format: string> [data: dictionary]")
	}

	path, ok := sl.AsString(args[0])
	if !ok || (path != "" && !valid(path, "path")) {
		return slapi_error(fn, "invalid template file '%s'", path)
	}

	format, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(fn, "invalid format '%s'", format)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "called from non-action")
	}

	switch format {
	case "json":
		if len(args) < 3 {
			return slapi_error(fn, "JSON called without data")
		}
		a.json(starlark_decode(args[2]))

	default:
		// This should be done using ParseFS() followed by ParseFiles(), but I can't get this to work.
		file := fmt.Sprintf("%s/templates/en/%s.tmpl", a.app.base, path)
		if !file_exists(file) {
			return slapi_error(fn, "template '%s' not found", path)
		}
		data := file_read(file)
		include := must(templates.ReadFile("templates/en/include.tmpl"))

		tmpl, err := template.New("").Parse(string(data) + "\n" + string(include))
		if err != nil {
			return slapi_error(fn, "%v", err)
		}

		if len(args) > 2 {
			err = tmpl.Execute(a.web.Writer, starlark_decode(args[2]))
		} else {
			err = tmpl.Execute(a.web.Writer, Map{})
		}
		if err != nil {
			return slapi_error(fn, "%v", err)
		}
	}

	return sl.None, nil
}

// Get details of an app
func slapi_app_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(fn, "invalid ID '%s'", id)
	}

	apps_lock.Lock()
	a, found := apps[id]
	apps_lock.Unlock()

	if found {
		user := t.Local("user").(*User)
		return starlark_encode(map[string]string{"id": a.id, "name": a.label(user, a.Label), "version": a.Version}), nil
	}

	return sl.None, nil
}

// Get available icons for home
func slapi_app_icons(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	var results []map[string]string

	for _, i := range icons {
		if i.app.Requires.Role == "administrator" && user.Role != "administrator" {
			continue
		}
		results = append(results, map[string]string{"path": i.Path, "name": i.app.label(user, i.Label), "icon": i.Icon})
	}

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return starlark_encode(results), nil
}

// Install an app from a .zip file
func slapi_app_install(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return slapi_error(fn, "syntax: <app id: string> <file: string> [check only: boolean]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (id != "" && !valid(id, "entity")) {
		return slapi_error(fn, "invalid ID '%s'", id)
	}
	if id == "" {
		id, _, _ = entity_id()
		if id == "" {
			return slapi_error(fn, "unable to allocate id")
		}
	}

	file, ok := sl.AsString(args[1])
	if !ok || !valid(file, "filepath") {
		return slapi_error(fn, "invalid file '%s'", file)
	}

	check_only := false
	if len(args) > 2 {
		check_only = bool(args[2].Truth())
	}
	debug("slapi_app_install() check only '%v'", check_only)

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}
	if !user.administrator() {
		return slapi_error(fn, "not administrator")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return slapi_error(fn, "no app")
	}

	a, err := app_install(id, "", slapi_file(user, app, file), check_only)
	if err != nil {
		return slapi_error(fn, fmt.Sprintf("App install failed: '%v'", err))
	}
	if !check_only {
		a.load()
	}

	return starlark_encode(a.Version), nil
}

// Get list of installed apps
func slapi_app_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var ids []string
	apps_lock.Lock()
	for id, _ := range apps {
		if valid(id, "entity") {
			ids = append(ids, id)
		}
	}
	apps_lock.Unlock()

	user := t.Local("user").(*User)
	results := make([]map[string]string, len(ids))
	apps_lock.Lock()
	for i, id := range ids {
		a := apps[id]
		results[i] = map[string]string{"id": a.id, "name": a.label(user, a.Label), "version": a.Version}
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return starlark_encode(results), nil
}

// Get attachments for an object
func slapi_attachment_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <object: string>")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return slapi_error(fn, "invalid object '%s'", object)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	attachments := attachments(user, object)
	return starlark_encode(structs_to_maps(*attachments)), nil
}

// Upload attachments for an object
func slapi_attachment_put(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 4 {
		return slapi_error(fn, "syntax: <field: string> <object: string> <entity: string> <save locally: boolean>")
	}

	field, ok := sl.AsString(args[0])
	if !ok || !valid(field, "constant") {
		return slapi_error(fn, "field '%s'", field)
	}

	object, ok := sl.AsString(args[1])
	if !ok || !valid(object, "path") {
		return slapi_error(fn, "invalid object '%s'", object)
	}

	entity, ok := sl.AsString(args[2])
	if !ok || !valid(entity, "entity") {
		return slapi_error(fn, "invalid entity '%s'", entity)
	}

	local := bool(args[3].Truth())

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "called from non-action")
	}

	attachments := a.upload_attachments(field, entity, object, local)
	return starlark_encode(structs_to_maps(*attachments)), nil
}

// Save attachments
func slapi_attachment_save(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return slapi_error(fn, "syntax: <attachments: array of dictionaries> <object: string> <entity: string>")
	}

	attachments := starlark_decode_multi_strings(args[0])

	object, ok := sl.AsString(args[1])
	if !ok || !valid(object, "path") {
		return slapi_error(fn, "invalid object '%s'", object)
	}

	entity, ok := sl.AsString(args[2])
	if !ok || !valid(entity, "entity") {
		return slapi_error(fn, "invalid entity '%s'", entity)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	attachments_save_maps(attachments, user, entity, object)
	return sl.None, nil
}

// General database query
func slapi_db_query(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return slapi_error(fn, "syntax: <SQL statement: string> [parameters: strings, variadic]")
	}

	query, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(fn, "invalid SQL statement '%s'", query)
	}

	as := starlark_decode(args[1:]).([]any)
	//debug("%s '%s' '%+v'", fn.Name(), query, as)

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return slapi_error(fn, "unknown app")
	}

	db := db_app(user, app)

	switch fn.Name() {
	case "mochi.db.exists":
		if db.exists(query, as...) {
			return sl.True, nil
		}
		return sl.False, nil

	case "mochi.db.row":
		return starlark_encode(db.row(query, as...)), nil

	case "mochi.db.query":
		return starlark_encode(db.rows(query, as...)), nil
	}

	return slapi_error(fn, "invalid database query '%s'", fn.Name())
}

// Directory search
func slapi_directory_search(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return slapi_error(fn, "syntax: <class: string> <search: string> <include self: boolean>")
	}

	class, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(fn, "invalid class '%s'", class)
	}

	search, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(fn, "invalid search '%s'", search)
	}

	include_self := bool(args[2].Truth())
	u := t.Local("user").(*User)

	db := db_open("db/directory.db")
	ds := db.rows("select * from directory where class=? and name like ? order by name, created", class, "%"+search+"%")

	for _, d := range ds {
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
	for _, d := range ds {
		_, found := me[d["id"].(string)]
		if !found {
			o = append(o, d)
		}
	}
	return starlark_encode(&o), nil
}

// Helper function to return an error
func slapi_error(fn *sl.Builtin, format string, values ...any) (sl.Value, error) {
	if fn == nil {
		return sl.None, error_message(format, values...)
	} else {
		return sl.None, error_message(fmt.Sprintf("%s() %s", fn.Name(), format), values...)
	}
}

// Create a new entity
func slapi_entity_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return slapi_error(fn, "syntax: <class: string> <name: string> <privacy: string> [data: string]")
	}

	class, ok := sl.AsString(args[0])
	if !ok || !valid(class, "constant") {
		return slapi_error(fn, "invalid class '%s'", class)
	}

	name, ok := sl.AsString(args[1])
	if !ok || !valid(name, "name") {
		return slapi_error(fn, "invalid name '%s'", name)
	}

	privacy, ok := sl.AsString(args[2])
	if !ok || !valid(privacy, "^(private|public)$") {
		return slapi_error(fn, "invalid privacy '%s'", privacy)
	}

	data := ""
	if len(args) > 3 {
		data, ok = sl.AsString(args[3])
		if !ok || !valid(data, "text") {
			return slapi_error(fn, "invalid data '%s'", data)
		}
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	e, err := entity_create(user, class, name, privacy, data)
	if err != nil {
		return slapi_error(fn, "unable to create entity: ", err)
	}

	return starlark_encode(e.ID), nil
}

// Get the fingerprint of an entity
func slapi_entity_fingerprint(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return slapi_error(fn, "syntax: <id: string> [include hyphens: boolean]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return slapi_error(fn, "invalid id '%s'", id)
	}

	if len(args) > 1 && bool(args[1].Truth()) {
		return starlark_encode(fingerprint_hyphens(fingerprint(id))), nil
	} else {
		return starlark_encode(fingerprint(id)), nil
	}
}

// Get an entity
func slapi_entity_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return slapi_error(fn, "invalid id '%s'", id)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	db := db_open("db/users.db")
	var e Entity
	if db.scan(&e, "select id, fingerprint, parent, class, name, privacy, data, updated from entities where id=? and user=?", id, user.ID) {
		return starlark_encode(e), nil
	}

	return sl.None, nil
}

// Helper function to get the path of a file
func slapi_file(u *User, a *App, file string) string {
	return fmt.Sprintf("%s/users/%d/%s/files/%s", data_dir, u.ID, a.id, file)
}

// Delete a file
func slapi_file_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return slapi_error(fn, "invalid file '%s'", file)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return slapi_error(fn, "no app")
	}

	file_delete(slapi_file(user, app, file))
	return sl.None, nil
}

// Return whether a file exists
func slapi_file_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return slapi_error(fn, "invalid file '%s'", file)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return slapi_error(fn, "no app")
	}

	if file_exists(slapi_file(user, app, file)) {
		return sl.True, nil
	} else {
		return sl.False, nil
	}
}

// List files
func slapi_file_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <directory: string>")
	}

	dir, ok := sl.AsString(args[0])
	if !ok || !valid(dir, "filepath") {
		return slapi_error(fn, "invalid directory '%s'", dir)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return slapi_error(fn, "no app")
	}

	path := slapi_file(user, app, dir)
	if !file_exists(path) {
		return slapi_error(fn, "does not exist")
	}
	if !file_is_directory(path) {
		return slapi_error(fn, "not a directory")
	}

	return starlark_encode(file_list(path)), nil
}

// Read a file into memory
func slapi_file_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return slapi_error(fn, "invalid file '%s'", file)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return slapi_error(fn, "no app")
	}

	return starlark_encode(file_read(slapi_file(user, app, file))), nil
}

// Write a file from memory
func slapi_file_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return slapi_error(fn, "syntax: <file: string> <data: array of bytes>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return slapi_error(fn, "invalid file '%s'", file)
	}

	data, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(fn, "invalid file data")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return slapi_error(fn, "no app")
	}

	file_write(slapi_file(user, app, file), []byte(data))

	return sl.None, nil
}

// Log message from app
func slapi_log(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return slapi_error(fn, "syntax: <format: string> [values: strings, variadic]")
	}

	format, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(fn, "invalid format")
	}

	a, ok := t.Local("app").(*App)
	if !ok || a == nil {
		format = fmt.Sprintf("%s(): %s", t.Local("function"), format)
	} else {
		format = fmt.Sprintf("App %s:%s() %s", a.id, t.Local("function"), format)
	}

	values := make([]any, len(args)-1)
	for i, a := range args[1:] {
		values[i] = starlark_decode(a)
	}

	switch fn.Name() {
	case "mochi.log.debug":
		debug(format, values...)

	case "mochi.log.info":
		info(format, values...)

	case "mochi.log.warn":
		warn(format, values...)
	}

	return sl.None, nil
}

// Render markdown
func slapi_markdown_render(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <markdown: string>")
	}

	in, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(fn, "invalid markdown")
	}

	return starlark_encode(markdown([]byte(in))), nil
}

// Send a message
func slapi_message_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return slapi_error(fn, "syntax: <headers: dictionary> [content: dictionary] [data: bytes]")
	}

	headers := starlark_decode_strings(args[0])
	if headers == nil {
		return slapi_error(fn, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	db := db_open("db/users.db")
	if !db.exists("select id from entities where id=? and user=?", headers["from"], user.ID) {
		return slapi_error(fn, "invalid from header")
	}

	if !valid(headers["to"], "entity") {
		return slapi_error(fn, "invalid to header")
	}

	if !valid(headers["service"], "constant") {
		return slapi_error(fn, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return slapi_error(fn, "invalid event header")
	}

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])
	if len(args) > 1 {
		m.content = starlark_decode_strings(args[1])
	}

	if len(args) > 2 {
		m.add(starlark_decode(args[2]))
	}

	m.send()
	return sl.None, nil
}

// Return a random alphanumeric string
func slapi_random_alphanumeric(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <length: integer>")
	}

	length, err := sl.AsInt32(args[0])
	if err != nil || length < 1 || length > 1000 {
		return slapi_error(fn, "invalid length")
	}

	return starlark_encode(random_alphanumeric(length)), nil
}

// Call a function in another app
func slapi_service_call(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 {
		return slapi_error(fn, "syntax: <service: string> <function: string> [parameters: any variadic]")
	}

	service, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(fn, "invalid service")
	}

	function, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(fn, "invalid function")
	}

	// Check for deep recursion
	depth := 1
	depth_var := t.Local("depth")
	if depth_var != nil {
		depth = depth_var.(int)
	}
	if depth > 1000 {
		return slapi_error(fn, "reached maximum recursion depth")
	}

	// Look for matching app function, using default if necessary
	a, _ := services[service]
	if a == nil {
		return slapi_error(fn, "unknown service '%s'", service)
	}
	f, found := a.Services[service].Functions[function]
	if !found {
		f, found = a.Services[service].Functions[""]
	}
	if !found {
		return slapi_error(fn, "unknown function '%s' for service '%s'", function, service)
	}

	// Call function
	s := a.starlark()
	s.set("app", a)
	s.set("user", t.Local("user").(*User))
	s.set("owner", t.Local("owner").(*User))
	s.set("depth", depth+1)

	//debug("mochi.service.call() calling app '%s' service '%s' function '%s' args: %+v", a.id, service, function, args[2:])
	var result sl.Value
	var err error
	if len(args) > 2 {
		result, err = s.call(f.Function, args[2:])
	} else {
		result, err = s.call(f.Function, nil)
	}
	//debug("mochi.service.call() result '%+v', type %T", result, result)

	return result, err
}

// Create a stream
func slapi_stream(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return slapi_error(fn, "syntax: <headers: dictionary> <content: dictionary>")
	}

	headers := starlark_decode_strings(args[0])
	if headers == nil {
		return slapi_error(fn, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	db := db_open("db/users.db")
	if !db.exists("select id from entities where id=? and user=?", headers["from"], user.ID) {
		return slapi_error(fn, "invalid from header")
	}

	if !valid(headers["to"], "entity") {
		return slapi_error(fn, "invalid to header")
	}

	if !valid(headers["service"], "constant") {
		return slapi_error(fn, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return slapi_error(fn, "invalid event header")
	}

	s := stream(headers["from"], headers["to"], headers["service"], headers["event"])
	s.write(starlark_decode(args[1]))
	return s, nil
}

// Return the local time in the user's time zone
func slapi_time_local(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return slapi_error(fn, "syntax: <timestamp: int64>")
	}

	var time int64
	var err error
	v := starlark_decode(args[0])

	switch x := v.(type) {
	case int:
		time = int64(x)

	case int64:
		time = x

	case string:
		s, ok := sl.AsString(args[0])
		if !ok {
			return slapi_error(fn, "invalid timestamp '%v'", args[0])
		}
		time, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return slapi_error(fn, "invalid timestamp '%v': %v", args[0], err)
		}

	default:
		return slapi_error(fn, "invalid time type %T", x)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	return starlark_encode(time_local(user, time)), nil
}

// Return the current Unix time
func slapi_time_now(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return starlark_encode(now()), nil
}

// Get a UID
func slapi_uid(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return starlark_encode(uid()), nil
}

// Get details of the current user
func slapi_user_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "no user")
	}

	return starlark_encode(map[string]any{"id": a.user.ID, "username": a.user.Username, "role": a.user.Role}), nil
}

// Log the user out
func slapi_user_logout(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error(fn, "called from non-action")
	}

	login := web_cookie_get(a.web, "login", "")
	if login != "" {
		login_delete(login)
	}
	web_cookie_unset(a.web, "login")

	return sl.None, nil
}

// Check if a string is valid
func slapi_valid(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return slapi_error(fn, "syntax: <string to check: string> <pattern to match: string>")
	}

	s, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error(fn, "invalid string to check '%s'", s)
	}

	match, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error(fn, "invalid match pattern '%s'", match)
	}

	return starlark_encode(valid(s, match)), nil
}

// Write data to all user's websockets listening on a given key
func slapi_websocket_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return slapi_error(fn, "syntax: <key: string> <content: any>")
	}

	key, ok := sl.AsString(args[0])
	if !ok || !valid(key, "constant") {
		return slapi_error(fn, "invalid key '%s'", key)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error(fn, "no user")
	}

	websockets_send(user, key, starlark_decode(args[1]))
	return sl.None, nil
}
