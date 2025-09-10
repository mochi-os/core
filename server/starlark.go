// Mochi server: Starlark
// Copyright Alistair Cunningham 2025

package main

import (
	sl "go.starlark.net/starlark"
)

type Starlark struct {
	thread  *sl.Thread
	globals sl.StringDict
}

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
			info("Starlark unable to read file '%s'", file)
			continue
		}
	}

	return &s
}

func starlark_template(t *sl.Thread, f *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	a := t.Local("action").(*Action)
	if a == nil {
		return sl.None, error_message("Starlark template called for non-action")
	}

	var file string
	var values any
	err := sl.UnpackPositionalArgs("template", args, kwargs, 1, &file, &values)
	if err != nil {
		return sl.None, error_message("Template arg error: %v", err)
	}
	if !valid(file, "path") {
		return sl.None, error_message("Template called with invalid file '%s'", file)
	}

	a.template(file, a.input("format"), values)

	return sl.None, nil
}

func starlark_tuple(in ...any) sl.Tuple {
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

func (s *Starlark) call(function string, args ...any) error {
	f, found := s.globals[function]
	if !found {
		return error_message("App function '%s' not found", function)
	}

	t := starlark_tuple(args...)
	if t == nil {
		return error_message("Unable to marshall arguments to tuple")
	}

	debug("Starlark running '%s': %+v", function, t)
	_, err := sl.Call(s.thread, f, t, nil)
	debug("Starlark finished")
	return err
}
