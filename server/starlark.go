// Mochi server: Starlark
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	"html/template"
)

type Starlark struct {
	thread  *sl.Thread
	globals sl.StringDict
}

// Create a new Starlark interpreter for a set of files
func starlark(files []string) *Starlark {
	s := Starlark{thread: &sl.Thread{Name: "main"}}

	s.globals = sl.StringDict{
		"template": sl.NewBuiltin("template", starlark_template),
	}

	for _, file := range files {
		debug("Starlark reading file '%s'", file)
		var err error
		s.globals, err = sl.ExecFile(s.thread, file, nil, s.globals)
		if err != nil {
			info("Starlark error reading file %v", err)
			continue
		}
	}

	return &s
}

// Convert a Starlark value to a Go variable
func starlark_decode(value sl.Value) any {
	switch v := value.(type) {
	case sl.NoneType:
		return nil

	case sl.Bool:
		return bool(v)

	case sl.Int:
		i, ok := v.Int64()
		if ok {
			return i
		}
		return v.String()

	case sl.Float:
		return float64(v)

	case sl.String:
		return string(v)

	case *sl.List:
		out := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = starlark_decode(v.Index(i))
		}
		return out

	case sl.Tuple:
		out := make([]interface{}, len(v))
		for i, e := range v {
			out[i] = starlark_decode(e)
		}
		return out

	case *sl.Dict:
		out := make(map[string]any)
		for _, i := range v.Items() {
			k := starlark_decode(i[0])
			val := starlark_decode(i[1])
			ks, ok := k.(string)
			if ok {
				out[ks] = val
			} else {
				out[fmt.Sprint(k)] = val
			}
		}
		return out

	default:
		return v.String()
	}
}

// Convert one or more of anything to a Starlark tuple
func starlark_encode(in ...any) sl.Tuple {
	t := make(sl.Tuple, len(in))
	for i, v := range in {
		var sv sl.Value
		switch x := v.(type) {
		case string:
			sv = sl.String(x)
		case int:
			sv = sl.MakeInt(x)
		case bool:
			sv = sl.Bool(x)
		case map[string]string:
			d := sl.NewDict(len(x))
			for i, value := range x {
				d.SetKey(sl.String(i), sl.String(value))
			}
			sv = d
		default:
			warn("Starlark unknown type %T", v)
			return nil
		}
		t[i] = sv
	}
	return t
}

// Template API function
func starlark_template(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Get action
	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("Starlark template called for non-action")
	}
	debug("Starlark template got '%+v', '%#v'", args, args)

	// Unpack the arguments from Starlark
	if len(args) == 0 {
		return sl.None, error_message("Starlark call to template() without template file")
	}
	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "path") {
		return sl.None, error_message("Template called with invalid file '%s'", file)
	}

	// Build the template
	// This should be done using ParseFS() followed by ParseFiles(), but I can't get this to work
	data := file_read(fmt.Sprintf("%s/templates/en/%s.tmpl", a.app.base, file))
	include := must(templates.ReadFile("templates/en/include.tmpl"))

	tmpl, err := template.New("").Parse(string(data) + "\n" + string(include))
	if err != nil {
		return sl.None, error_message("Template error: %v", err)
	}

	// Execute the template
	if len(args) > 1 {
		err = tmpl.Execute(a.web.Writer, starlark_decode(args[1]))
	} else {
		err = tmpl.Execute(a.web.Writer, Map{})
	}
	if err != nil {
		return sl.None, error_message("Template error: %v", err)
	}

	return sl.None, nil
}

// Call a Starlark function
func (s *Starlark) call(function string, args ...any) error {
	f, found := s.globals[function]
	if !found {
		return error_message("App function '%s' not found", function)
	}

	t := starlark_encode(args...)
	if t == nil {
		return error_message("Unable to encode Starlark arguments")
	}

	debug("Starlark running '%s': %+v", function, t)
	_, err := sl.Call(s.thread, f, t, nil)
	debug("Starlark finished")
	return err
}
