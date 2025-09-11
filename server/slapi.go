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
	slapi = sl.StringDict{
		"mochi": sls.FromStringDict(sl.String("mochi"), sl.StringDict{
			"db": sls.FromStringDict(sl.String("db"), sl.StringDict{
				"query": sl.NewBuiltin("query", slapi_db_query),
			}),
			"web": sls.FromStringDict(sl.String("web"), sl.StringDict{
				"dump":     sl.NewBuiltin("dump", slapi_web_dump),
				"error":    sl.NewBuiltin("error", slapi_web_error),
				"template": sl.NewBuiltin("template", slapi_web_template),
			}),
		}),
	}
)

// Database query
func slapi_db_query(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("mochi.db.query(): Called from non-action")
	}

	if len(args) < 1 || len(args) > 2 {
		return sl.None, error_message("mochi.db.query() syntax: <SQL statement> [parameters]")
	}

	query, ok := sl.AsString(args[0])
	if !ok {
		return sl.None, error_message("mochi.db.query(): Invalid SQL statement '%s'", query)
	}

	if len(args) > 1 {
		return starlark_encode(a.user.db.maps(query, starlark_decode(args[1]))), nil
	}
	r := starlark_encode(a.user.db.maps(query))
	debug("API SQL returning '%+v'", r)
	return r, nil
}

// Dump the variables passed to a web page for debugging
func slapi_web_dump(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("mochi.web.dump() called from non-action")
	}

	var vars []any
	for _, v := range args {
		vars = append(vars, starlark_decode(v))
	}

	a.dump(vars)
	return sl.None, nil
}

// Print an error
func slapi_web_error(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("mochi.web.error() called from non-action")
	}

	if len(args) != 2 {
		return sl.None, error_message("mochi.web.error() syntax: <code> <message>")
	}

	code, err := sl.AsInt32(args[0])
	if err != nil {
		return sl.None, error_message("mochi.web.error() invalid error code")
	}

	message, ok := sl.AsString(args[1])
	if !ok {
		return sl.None, error_message("mochi.web.error() invalid error message")
	}

	a.error(code, message)
	return sl.None, nil
}

// Web template
func slapi_web_template(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("mochi.web.template() called from non-action")
	}

	if len(args) < 1 || len(args) > 2 {
		return sl.None, error_message("mochi.web.template() syntax: <template file> [parameters]")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "path") {
		return sl.None, error_message("mochi.web.template(): Invalid template file '%s'", file)
	}

	// This should be done using ParseFS() followed by ParseFiles(), but I can't get this to work.
	data := file_read(fmt.Sprintf("%s/templates/en/%s.tmpl", a.app.base, file))
	include := must(templates.ReadFile("templates/en/include.tmpl"))

	tmpl, err := template.New("").Parse(string(data) + "\n" + string(include))
	if err != nil {
		return sl.None, error_message("mochi.web.template(): %v", err)
	}

	if len(args) > 1 {
		err = tmpl.Execute(a.web.Writer, starlark_decode(args[1]))
	} else {
		err = tmpl.Execute(a.web.Writer, Map{})
	}
	if err != nil {
		return sl.None, error_message("mochi.web.template(): %v", err)
	}

	return sl.None, nil
}
