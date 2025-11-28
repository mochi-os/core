// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"io"
	"sort"
	"strconv"
	"strings"
)

var (
	api_globals sl.StringDict
)

func init() {
	api_globals = sl.StringDict{
		"mochi": sls.FromStringDict(sl.String("mochi"), sl.StringDict{
			"access": api_access,
			"app": sls.FromStringDict(sl.String("mochi.app"), sl.StringDict{
				"get":     sl.NewBuiltin("mochi.app.get", api_app_get),
				"icons":   sl.NewBuiltin("mochi.app.icons", api_app_icons),
				"install": sl.NewBuiltin("mochi.app.install", api_app_install),
				"list":    sl.NewBuiltin("mochi.app.list", api_app_list),
			}),
			"attachment": sls.FromStringDict(sl.String("mochi.attachment"), sl.StringDict{
				"get":  sl.NewBuiltin("mochi.attachment.get", api_attachment_get),
				"put":  sl.NewBuiltin("mochi.attachment.put", api_attachment_put),
				"save": sl.NewBuiltin("mochi.attachment.save", api_attachment_save),
			}),
			"db": sls.FromStringDict(sl.String("mochi.db"), sl.StringDict{
				"exists": sl.NewBuiltin("mochi.db.exists", api_db_query),
				"query":  sl.NewBuiltin("mochi.db.query", api_db_query),
				"row":    sl.NewBuiltin("mochi.db.row", api_db_query),
			}),
			"directory": sls.FromStringDict(sl.String("mochi.directory"), sl.StringDict{
				"get":    sl.NewBuiltin("mochi.directory.get", api_directory_get),
				"search": sl.NewBuiltin("mochi.directory.search", api_directory_search),
			}),
			"entity": sls.FromStringDict(sl.String("mochi.entity"), sl.StringDict{
				"create":      sl.NewBuiltin("mochi.entity.create", api_entity_create),
				"fingerprint": sl.NewBuiltin("mochi.entity.fingerprint", api_entity_fingerprint),
				"get":         sl.NewBuiltin("mochi.entity.get", api_entity_get),
			}),
			"file": sls.FromStringDict(sl.String("mochi.file"), sl.StringDict{
				"delete": sl.NewBuiltin("mochi.file.delete", api_file_delete),
				"exists": sl.NewBuiltin("mochi.file.exists", api_file_exists),
				"list":   sl.NewBuiltin("mochi.file.list", api_file_list),
				"read":   sl.NewBuiltin("mochi.file.read", api_file_read),
				"write":  sl.NewBuiltin("mochi.file.write", api_file_write),
			}),
			"group": api_group,
			"log":   api_log,
			"markdown": sls.FromStringDict(sl.String("mochi.markdown"), sl.StringDict{
				"render": sl.NewBuiltin("mochi.markdown.render", api_markdown_render),
			}),
			"message": sls.FromStringDict(sl.String("mochi.message"), sl.StringDict{
				"send": sl.NewBuiltin("mochi.message.send", api_message_send),
			}),
			"random": sls.FromStringDict(sl.String("mochi.random"), sl.StringDict{
				"alphanumeric": sl.NewBuiltin("mochi.random.alphanumeric", api_random_alphanumeric),
			}),
			"service": sls.FromStringDict(sl.String("mochi.service"), sl.StringDict{
				"call": sl.NewBuiltin("mochi.service.call", api_service_call),
			}),
			"stream": sl.NewBuiltin("mochi.stream", api_stream),
			"time": sls.FromStringDict(sl.String("mochi.time"), sl.StringDict{
				"local": sl.NewBuiltin("mochi.time.local", api_time_local),
				"now":   sl.NewBuiltin("mochi.time.now", api_time_now),
			}),
			"uid": sl.NewBuiltin("mochi.uid", api_uid),
			"url": sls.FromStringDict(sl.String("mochi.url"), sl.StringDict{
				"delete": sl.NewBuiltin("mochi.url.delete", api_url_request),
				"get":    sl.NewBuiltin("mochi.url.get", api_url_request),
				"patch":  sl.NewBuiltin("mochi.url.patch", api_url_request),
				"post":   sl.NewBuiltin("mochi.url.post", api_url_request),
				"put":    sl.NewBuiltin("mochi.url.put", api_url_request),
			}),
			"valid":     sl.NewBuiltin("mochi.valid", api_valid),
			"websocket": api_websocket,
		}),
	}
}

// Get details of an app
func api_app_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid ID %q", id)
	}

	apps_lock.Lock()
	a, found := apps[id]
	apps_lock.Unlock()

	if found {
		user := t.Local("user").(*User)
		return sl_encode(map[string]string{"id": a.id, "name": a.label(user, a.active.Label), "latest": a.active.Version}), nil
	}

	return sl.None, nil
}

// Get available icons for home
func api_app_icons(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user := t.Local("user").(*User)
	var results []map[string]string

	apps_lock.Lock()
	for _, a := range apps {
		for _, i := range a.active.Icons {
			path := a.fingerprint
			if len(a.active.Paths) > 0 {
				path = a.active.Paths[0]
			}
			if i.Action != "" {
				path = path + "/" + i.Action
			}
			results = append(results, map[string]string{"path": path, "name": a.label(user, i.Label), "file": i.File})
		}
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return sl_encode(results), nil
}

// Install an app from a .zip file
func api_app_install(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return sl_error(fn, "syntax: <app id: string>, <file: string>, [ check only: boolean]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || (id != "" && !valid(id, "entity")) {
		return sl_error(fn, "invalid ID %q", id)
	}
	if id == "" {
		id, _, _ = entity_id()
		if id == "" {
			return sl_error(fn, "unable to allocate id")
		}
	}

	file, ok := sl.AsString(args[1])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	check_only := false
	if len(args) > 2 {
		check_only = bool(args[2].Truth())
	}
	debug("api_app_install() check only '%v'", check_only)

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !user.administrator() {
		return sl_error(fn, "not administrator")
	}

	a, ok := t.Local("app").(*App)
	if !ok || a == nil {
		return sl_error(fn, "no app")
	}

	av, err := app_install(id, "", api_file(user, a, file), check_only)
	if err != nil {
		return sl_error(fn, fmt.Sprintf("App install failed: '%v'", err))
	}

	if !check_only {
		na := app(id)
		na.load_version(av)
	}

	return sl_encode(av.Version), nil
}

// Get list of installed apps
func api_app_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
		if a == nil {
			return sl_error(fn, "App %q is nil", id)
		}
		if a.active == nil {
			return sl_error(fn, "App %q has no active version", id)
		}
		results[i] = map[string]string{"id": a.id, "name": a.label(user, a.active.Label), "latest": a.active.Version}
	}
	apps_lock.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return strings.ToLower(results[i]["name"]) < strings.ToLower(results[j]["name"])
	})

	return sl_encode(results), nil
}

// Get attachments for an object
func api_attachment_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <object: string>")
	}

	object, ok := sl.AsString(args[0])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object %q", object)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	attachments := attachments(user, object)
	return sl_encode(structs_to_maps(*attachments)), nil
}

// Upload attachments for an object
func api_attachment_put(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 4 {
		return sl_error(fn, "syntax: <field: string>, <object: string>, <entity: string>, <save locally: boolean>")
	}

	field, ok := sl.AsString(args[0])
	if !ok || !valid(field, "constant") {
		return sl_error(fn, "field %q", field)
	}

	object, ok := sl.AsString(args[1])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object %q", object)
	}

	entity, ok := sl.AsString(args[2])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity %q", entity)
	}

	local := bool(args[3].Truth())

	a := t.Local("action").(*Action)
	if a == nil {
		return sl_error(fn, "called from non-action")
	}

	attachments := a.upload_attachments(field, entity, object, local)
	return sl_encode(structs_to_maps(*attachments)), nil
}

// Save attachments
func api_attachment_save(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <attachments: array of dictionaries>, <object: string>, <entity: string>")
	}

	attachments := sl_decode_multi_strings(args[0])

	object, ok := sl.AsString(args[1])
	if !ok || !valid(object, "path") {
		return sl_error(fn, "invalid object %q", object)
	}

	entity, ok := sl.AsString(args[2])
	if !ok || !valid(entity, "entity") {
		return sl_error(fn, "invalid entity %q", entity)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	attachments_save_maps(attachments, user, entity, object)
	return sl.None, nil
}

// General database query
func api_db_query(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return sl_error(fn, "syntax: <SQL statement: string>, [parameters: variadic strings]")
	}

	query, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid SQL statement %q", query)
	}

	as := sl_decode(args[1:]).([]any)
	//debug("%s %q '%+v'", fn.Name(), query, as)

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "unknown app")
	}

	db := db_app(user, app.active)

	switch fn.Name() {
	case "mochi.db.exists":
		if db.exists(query, as...) {
			return sl.True, nil
		}
		return sl.False, nil

	case "mochi.db.row":
		return sl_encode(db.row(query, as...)), nil

	case "mochi.db.query":
		return sl_encode(db.rows(query, as...)), nil
	}

	return sl_error(fn, "invalid database query %q", fn.Name())
}

// Get a directory entry
func api_directory_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid ID %q", id)
	}

	db := db_open("db/directory.db")
	d := db.row("select * from directory where id=?", id)
	d["fingerprint_hyphens"] = fingerprint_hyphens(d["fingerprint"].(string))

	return sl_encode(d), nil
}

// Directory search
func api_directory_search(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <class: string>, <search: string>, <include self: boolean>")
	}

	class, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid class %q", class)
	}

	search, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid search %q", search)
	}

	include_self := bool(args[2].Truth())
	u := t.Local("user").(*User)

	db := db_open("db/directory.db")
	ds := db.rows("select * from directory where class=? and name like ? order by name, created", class, "%"+search+"%")

	for _, d := range ds {
		d["fingerprint_hyphens"] = fingerprint_hyphens(d["fingerprint"].(string))
	}

	if u == nil || include_self || class != "person" {
		return sl_encode(ds), nil
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
	return sl_encode(&o), nil
}

// Create a new entity
func api_entity_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 3 || len(args) > 4 {
		return sl_error(fn, "syntax: <class: string>, <name: string>, <privacy: string>, [data: string]")
	}

	class, ok := sl.AsString(args[0])
	if !ok || !valid(class, "constant") {
		return sl_error(fn, "invalid class %q", class)
	}

	name, ok := sl.AsString(args[1])
	if !ok || !valid(name, "name") {
		return sl_error(fn, "invalid name %q", name)
	}

	privacy, ok := sl.AsString(args[2])
	if !ok || !valid(privacy, "^(private|public)$") {
		return sl_error(fn, "invalid privacy %q", privacy)
	}

	data := ""
	if len(args) > 3 {
		data, ok = sl.AsString(args[3])
		if !ok || !valid(data, "text") {
			return sl_error(fn, "invalid data %q", data)
		}
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	e, err := entity_create(user, class, name, privacy, data)
	if err != nil {
		return sl_error(fn, "unable to create entity: ", err)
	}

	return sl_encode(e.ID), nil
}

// Get the fingerprint of an entity
func api_entity_fingerprint(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <id: string>, [include hyphens: boolean]")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	if len(args) > 1 && bool(args[1].Truth()) {
		return sl_encode(fingerprint_hyphens(fingerprint(id))), nil
	} else {
		return sl_encode(fingerprint(id)), nil
	}
}

// Get an entity
func api_entity_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: string>")
	}

	id, ok := sl.AsString(args[0])
	if !ok || !valid(id, "entity") {
		return sl_error(fn, "invalid id %q", id)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	e := db.rows("select id, fingerprint, parent, class, name, data, published from entities where id=? and user=?", id, user.ID)

	return sl_encode(e), nil
}

// Helper function to get the path of a file
func api_file(u *User, a *App, file string) string {
	return fmt.Sprintf("%s/users/%d/%s/files/%s", data_dir, u.ID, a.id, file)
}

// Delete a file
func api_file_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	file_delete(api_file(user, app, file))
	return sl.None, nil
}

// Return whether a file exists
func api_file_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	if file_exists(api_file(user, app, file)) {
		return sl.True, nil
	} else {
		return sl.False, nil
	}
}

// List files in a subdirectory
func api_file_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <subdirectory: string>")
	}

	dir, ok := sl.AsString(args[0])
	if !ok || !valid(dir, "filepath") {
		return sl_error(fn, "invalid directory %q", dir)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	path := api_file(user, app, dir)
	if !file_exists(path) {
		return sl_error(fn, "does not exist")
	}
	if !file_is_directory(path) {
		return sl_error(fn, "not a directory")
	}

	return sl_encode(file_list(path)), nil
}

// Read a file into memory
func api_file_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	return sl_encode(file_read(api_file(user, app, file))), nil
}

// Write a file from memory
func api_file_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <file: string>, <data: array of bytes>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	data, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid file data")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	file_write(api_file(user, app, file), []byte(data))

	return sl.None, nil
}

// Render markdown
func api_markdown_render(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <markdown: string>")
	}

	in, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid markdown")
	}

	return sl_encode(string(markdown([]byte(in)))), nil
}

// Send a message
func api_message_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return sl_error(fn, "syntax: <headers: dictionary>, [content: dictionary], [data: bytes]")
	}

	headers := sl_decode_strings(args[0])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	if !db.exists("select id from entities where id=? and user=?", headers["from"], user.ID) {
		return sl_error(fn, "invalid from header")
	}

	if !valid(headers["to"], "entity") {
		return sl_error(fn, "invalid to header")
	}

	if !valid(headers["service"], "constant") {
		return sl_error(fn, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return sl_error(fn, "invalid event header")
	}

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])
	if len(args) > 1 {
		m.content = sl_decode_strings(args[1])
	}

	if len(args) > 2 {
		m.add(sl_decode(args[2]))
	}

	m.send()
	return sl.None, nil
}

// Return a random alphanumeric string
func api_random_alphanumeric(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <length: integer>")
	}

	length, err := sl.AsInt32(args[0])
	if err != nil || length < 1 || length > 1000 {
		return sl_error(fn, "invalid length")
	}

	return sl_encode(random_alphanumeric(length)), nil
}

// Call a function in another app
func api_service_call(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 {
		return sl_error(fn, "syntax: <service: string>, <function: string>, [parameters: variadic any]")
	}

	service, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid service")
	}

	function, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid function")
	}

	// Check for deep recursion
	depth := 1
	depth_var := t.Local("depth")
	if depth_var != nil {
		depth = depth_var.(int)
	}
	if depth > 10000 {
		return sl_error(fn, "reached maximum service recursion depth")
	}

	// Look for matching app function, using default if necessary
	a := app_for_service(service)
	if a == nil {
		return sl_error(fn, "unknown service %q", service)
	}
	f, found := a.active.Functions[function]
	if !found {
		f, found = a.active.Functions[""]
	}
	if !found {
		return sl_error(fn, "unknown function %q for service %q", function, service)
	}

	// Call function
	s := a.active.starlark()
	s.set("app", a)
	s.set("user", t.Local("user").(*User))
	s.set("owner", t.Local("owner").(*User))
	s.set("depth", depth+1)

	debug("mochi.service.call() calling app %q service %q function %q args: %+v", a.id, service, function, args[2:])
	var result sl.Value
	var err error

	if len(args) > 2 {
		result, err = s.call(f.Function, args[2:])
	} else {
		result, err = s.call(f.Function, nil)
	}
	debug("mochi.service.call() result '%+v', type %T", result, result)
	if err != nil {
		info("mochi.service.call() error: %v", err)
	}

	return result, err
}

// Create a stream
func api_stream(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <headers: dictionary>, <content: dictionary>")
	}

	headers := sl_decode_strings(args[0])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	if !db.exists("select id from entities where id=? and user=?", headers["from"], user.ID) {
		return sl_error(fn, "invalid from header")
	}

	if !valid(headers["to"], "entity") {
		return sl_error(fn, "invalid to header")
	}

	if !valid(headers["service"], "constant") {
		return sl_error(fn, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return sl_error(fn, "invalid event header")
	}

	s, err := stream(headers["from"], headers["to"], headers["service"], headers["event"])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	s.write(sl_decode(args[1]))
	return s, nil
}

// Return the local time in the user's time zone
func api_time_local(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <timestamp: int64>")
	}

	var time int64
	var err error
	v := sl_decode(args[0])

	switch x := v.(type) {
	case int:
		time = int64(x)

	case int64:
		time = x

	case string:
		s, ok := sl.AsString(args[0])
		if !ok {
			return sl_error(fn, "invalid timestamp '%v'", args[0])
		}
		time, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return sl_error(fn, "invalid timestamp '%v': %v", args[0], err)
		}

	default:
		return sl_error(fn, "invalid time type %T", x)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	return sl_encode(time_local(user, time)), nil
}

// Return the current Unix time
func api_time_now(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl_encode(now()), nil
}

// Get a UID
func api_uid(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl_encode(uid()), nil
}

// Request a URL
func api_url_request(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 4 {
		return sl_error(fn, "syntax: <url: string>, [options: dictionary], [headers: dictionary], [body: string|dictionary]")
	}

	url, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid URL")
	}

	var options map[string]string
	if len(args) > 1 {
		options = sl_decode_strings(args[1])
	}

	var headers map[string]string
	if len(args) > 2 {
		headers = sl_decode_strings(args[2])
	}

	var body any
	if len(args) > 3 {
		body = sl_decode(args[3])
	}

	parts := strings.Split(fn.Name(), ".")
	r, err := url_request(parts[len(parts)-1], url, options, headers, body)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	defer r.Body.Close()

	data, _ := io.ReadAll(r.Body)
	return sl_encode(map[string]any{"status": r.StatusCode, "headers": r.Header, "body": string(data)}), nil
}

// Check if a string is valid
func api_valid(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <string to check: string>, <pattern to match: string>")
	}

	s, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid string to check %q", s)
	}

	match, ok := sl.AsString(args[1])
	if !ok {
		return sl_error(fn, "invalid match pattern %q", match)
	}

	return sl_encode(valid(s, match)), nil
}
