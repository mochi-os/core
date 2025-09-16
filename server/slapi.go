// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	sl_time "go.starlark.net/lib/time"
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
				"write":    sl.NewBuiltin("write", slapi_action_write),
			}),
			"apps": sls.FromStringDict(sl.String("apps"), sl.StringDict{
				"icons": sl.NewBuiltin("icons", slapi_apps_icons),
			}),
			"db": sls.FromStringDict(sl.String("db"), sl.StringDict{
				"query": sl.NewBuiltin("query", slapi_db_query),
			}),
			"directory": sls.FromStringDict(sl.String("directory"), sl.StringDict{
				"search": sl.NewBuiltin("search", slapi_directory_search),
			}),
			"service": sls.FromStringDict(sl.String("service"), sl.StringDict{
				"call": sl.NewBuiltin("call", slapi_service_call),
			}),
			"user": sls.FromStringDict(sl.String("user"), sl.StringDict{
				"get":    sl.NewBuiltin("get", slapi_user_get),
				"logout": sl.NewBuiltin("logout", slapi_user_logout),
			}),
			"valid": sl.NewBuiltin("valid", slapi_valid),
		}),
		"starlark": sls.FromStringDict(sl.String("starlark"), sl.StringDict{
			"time": sl_time.Module,
		}),
	}
}

// Dump the variables passed for debugging
func slapi_action_dump(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error("mochi.action.dump() called from non-action")
	}

	var vars []any
	for _, v := range args {
		vars = append(vars, starlark_decode(v))
	}

	a.dump(vars)
	return sl.None, nil
}

// Print an error
func slapi_action_error(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return slapi_error("mochi.action.error() syntax: <code: integer> <message: string>")
	}

	code, err := sl.AsInt32(args[0])
	if err != nil {
		return slapi_error("mochi.action.error() invalid error code")
	}

	message, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error("mochi.action.error() invalid error message")
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error("mochi.action.error() called from non-action")
	}

	a.error(code, message)
	return sl.None, nil
}

// Redirect the action
func slapi_action_redirect(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 {
		return slapi_error("mochi.action.redirect() syntax: <path: string>")
	}

	path, ok := sl.AsString(args[0])
	if !ok || !valid(path, "path") {
		return slapi_error("mochi.action.redirect() invalid path '%s'", path)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error("mochi.action.redirect() called from non-action")
	}

	a.redirect(path)
	return sl.None, nil
}

// Write data back to the caller of the action
func slapi_action_write(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return slapi_error("mochi.action.write() syntax: <template path: string> <format: string> [data: dictionary]")
	}

	path, ok := sl.AsString(args[0])
	if !ok || !valid(path, "path") {
		return slapi_error("mochi.action.write() invalid template file '%s'", path)
	}

	format, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error("mochi.action.write() invalid format '%s'", format)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error("mochi.action.write() called from non-action")
	}

	switch format {
	case "json":
		if len(args) < 3 {
			return slapi_error("mochi.action.write() JSON called without data")
		}
		a.json(args[2])

	default:
		// This should be done using ParseFS() followed by ParseFiles(), but I can't get this to work.
		file := fmt.Sprintf("%s/templates/en/%s.tmpl", a.app.base, path)
		if !file_exists(file) {
			return slapi_error("mochi.action.write() template '%s' not found", path)
		}
		data := file_read(file)
		include := must(templates.ReadFile("templates/en/include.tmpl"))

		tmpl, err := template.New("").Parse(string(data) + "\n" + string(include))
		if err != nil {
			return slapi_error("mochi.action.write(): %v", err)
		}

		if len(args) > 2 {
			err = tmpl.Execute(a.web.Writer, starlark_decode(args[2]))
		} else {
			err = tmpl.Execute(a.web.Writer, Map{})
		}
		if err != nil {
			return slapi_error("mochi.action.write(): %v", err)
		}
	}

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

// Database query
func slapi_db_query(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return slapi_error("mochi.db.query() syntax: <SQL statement: string> [parameters: list]")
	}

	query, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error("mochi.db.query() invalid SQL statement '%s'", query)
	}

	user := t.Local("user").(*User)
	if user == nil {
		return slapi_error("mochi.db.query() not logged in, so no user database available")
	}

	app := t.Local("app").(*App)
	if app == nil {
		return slapi_error("mochi.db.query() unknown app")
	}

	db := db_app(user, app)
	var result *[]map[string]any
	if len(args) > 1 {
		result = db.maps(query, starlark_decode(args[1]))
	} else {
		result = db.maps(query)
	}
	return starlark_encode(result), nil
}

// Directory search
func slapi_directory_search(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return slapi_error("mochi.directory.search() syntax: <class: string> <search: string> <include self: boolean>")
	}

	class, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error("mochi.directory.search() invalid class '%s'", class)
	}

	search, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error("mochi.directory.search() invalid search '%s'", search)
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
func slapi_error(format string, values ...any) (sl.Value, error) {
	return sl.None, error_message(format, values...)
}

// Call a function in another app
func slapi_service_call(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Get service and function
	if len(args) < 1 || len(args) > 2 {
		return slapi_error("mochi.service.call() syntax: <service: string> <function: string> [parameters: variadic]")
	}

	service, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error("mochi.service.call() invalid service")
	}

	function, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error("mochi.service.call() invalid function")
	}

	// Check for deep recursion
	depth := 1
	depth_var := t.Local("depth")
	if depth_var != nil {
		depth = depth_var.(int)
	}
	if depth > 1000 {
		return slapi_error("mochi.service.call() reached maximum recursion depth")
	}

	// Look for matching app function, using default if necessary
	a, _ := services[service]
	if a == nil {
		return slapi_error("mochi.service.call() unknown service '%s'", service)
	}
	fn, found := a.Services[service].Functions[function]
	if !found {
		fn, found = a.Services[service].Functions[""]
	}
	if !found {
		return slapi_error("mochi.service.call() unknown function '%s' for service '%s'", function, service)
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
		result, err = s.call(fn.Function)
	}
	debug("mochi.service.call() got result (type %s): %+v", reflect.TypeOf(result), result)

	return result, err
}

// Get details of the current user
func slapi_user_get(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error("mochi.user.get() not logged in")
	}

	return starlark_encode(map[string]any{"id": a.user.ID, "username": a.user.Username}), nil
}

// Log the user out
func slapi_user_logout(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return slapi_error("mochi.user.logout() called from non-action")
	}

	login := web_cookie_get(a.web, "login", "")
	if login != "" {
		login_delete(login)
	}
	web_cookie_unset(a.web, "login")

	return sl.None, nil
}

// Check if a string is valid
func slapi_valid(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return slapi_error("mochi.valid() syntax: <string to check: string> <pattern to match: string>")
	}

	s, ok := sl.AsString(args[0])
	if !ok {
		return slapi_error("mochi.valid() invalid string to check '%s'", s)
	}

	match, ok := sl.AsString(args[1])
	if !ok {
		return slapi_error("mochi.valid() invalid match pattern '%s'", match)
	}

	return starlark_encode(valid(s, match)), nil
}
