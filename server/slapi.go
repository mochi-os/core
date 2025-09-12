// Mochi server: Starlark API
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"html/template"
)

var (
	slapi sl.StringDict
)

func init() {
	slapi = sl.StringDict{
		"mochi": sls.FromStringDict(sl.String("mochi"), sl.StringDict{
		   "action": sls.FromStringDict(sl.String("action"), sl.StringDict{
                "dump":  sl.NewBuiltin("dump", slapi_action_dump),
                "error": sl.NewBuiltin("error", slapi_action_error),
                "write": sl.NewBuiltin("write", slapi_action_write),
            }),
			"db": sls.FromStringDict(sl.String("db"), sl.StringDict{
				"query": sl.NewBuiltin("query", slapi_db_query),
			}),
			"service": sls.FromStringDict(sl.String("service"), sl.StringDict{
				"call": sl.NewBuiltin("call", slapi_service_call),
			}),
		}),
	}
}

// Dump the variables passed for debugging
func slapi_action_dump(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("mochi.action.dump() called from non-action")
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
		return sl.None, error_message("mochi.action.error() syntax: <code> <message>")
	}

	code, err := sl.AsInt32(args[0])
	if err != nil {
		return sl.None, error_message("mochi.action.error() invalid error code")
	}

	message, ok := sl.AsString(args[1])
	if !ok {
		return sl.None, error_message("mochi.action.error() invalid error message")
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("mochi.action.error() called from non-action")
	}

	a.error(code, message)
	return sl.None, nil
}

// Write data back to the caller of the action
func slapi_action_write(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 3 {
		return sl.None, error_message("mochi.action.write() syntax: <template path> <format> [data]")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "path") {
		return sl.None, error_message("mochi.action.write() invalid template file '%s'", file)
	}

	format, ok := sl.AsString(args[1])
	if !ok {
		return sl.None, error_message("mochi.action.write() invalid format '%s'", format)
	}

	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("mochi.action.write() called from non-action")
	}

	switch format {
	case "json":
		if len(args) < 3 {
			return sl.None, error_message("mochi.action.write() JSON called without data")
		}
		a.json(args[2])

	default:
		// This should be done using ParseFS() followed by ParseFiles(), but I can't get this to work.
		data := file_read(fmt.Sprintf("%s/templates/en/%s.tmpl", a.app.base, file))
		include := must(templates.ReadFile("templates/en/include.tmpl"))

		tmpl, err := template.New("").Parse(string(data) + "\n" + string(include))
		if err != nil {
			return sl.None, error_message("mochi.action.write(): %v", err)
		}

		if len(args) > 2 {
			err = tmpl.Execute(a.web.Writer, starlark_decode(args[2]))
		} else {
			err = tmpl.Execute(a.web.Writer, Map{})
		}
		if err != nil {
			return sl.None, error_message("mochi.action.write(): %v", err)
		}
	}

	return sl.None, nil
}

// Database query
func slapi_db_query(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl.None, error_message("mochi.db.query() syntax: <SQL statement> [parameters]")
	}

	query, ok := sl.AsString(args[0])
	if !ok {
		return sl.None, error_message("mochi.db.query() invalid SQL statement '%s'", query)
	}

	db_var := t.Local("db.user")
	if db_var == nil {
		return sl.None, error_message("mochi.db.query() no database connected")
	}
	db := db_var.(*DB)

	if len(args) > 1 {
		return starlark_encode(db.maps(query, starlark_decode(args[1]))), nil
	}
	r := starlark_encode(db.maps(query))
	return r, nil
}

// Call a service in another app
func slapi_service_call(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Get service and function
	service, ok := sl.AsString(args[0])
	if !ok {
		return sl.None, error_message("mochi.service.call() invalid service")
	}
	function, ok := sl.AsString(args[1])
	if !ok {
		return sl.None, error_message("mochi.service.call() invalid function")
	}

	// Check for deep recursion
	depth := 1
	depth_var := t.Local("depth")
	if depth_var != nil {
		depth = depth_var.(int)
	}
	if depth > 1000 {
		return sl.None, error_message("mochi.service.call() reached maximum recursion depth")
	}

	// Look for matching app function, using default if necessary
	a, _ := services[service]
	if a == nil {
		return sl.None, error_message("mochi.service.call() unknown service '%s'", service)
	}
	fn, found := a.Services[service].Functions[function]
	if !found {
		fn, found = a.Services[service].Functions[""]
	}
	if !found {
		return sl.None, error_message("mochi.service.call() unknown function '%s' for service '%s'", function, service)
	}

	// Call function
	s := a.starlark()
	s.set("depth", depth+1)

	db := t.Local("db.user")
	if db != nil {
		u := db.(*DB).user
		if u == nil {
			return sl.None, error_message("mochi.service.call() has database but no database user")
		}
		s.set("db.user", db_app(u, a))
	}

	db = t.Local("db.owner")
	if db != nil {
		u := db.(*DB).user
		if u == nil {
			return sl.None, error_message("mochi.service.call() has database but no database owner")
		}
		s.set("db.owner", db_app(u, a))
	}

	debug("mochi.service.call() calling app '%s' service '%s' function '%s' args: %+v", a.Name, service, function, args[2:])
	var result sl.Value
	var err error
	if len(args) > 2 {
		result, err = s.call(fn.Function, args[2:])
	} else {
		result, err = s.call(fn.Function)
	}
	debug("mochi.service.call() got result: %+v", result)

	return result, err
}
