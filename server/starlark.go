// Mochi server: Starlark
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
)

type Starlark struct {
	thread  *sl.Thread
	globals sl.StringDict
}

// Create a new Starlark interpreter for a set of files
func starlark(files []string) *Starlark {
	s := Starlark{thread: &sl.Thread{Name: "main"}}
	s.globals = slapi

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
	//debug("Decoding '%+v', type '%s'", value, reflect.TypeOf(value))
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
		f, _ := sl.AsFloat(v)
		return f

	case sl.String:
		s, _ := sl.AsString(v)
		return s

	case *sl.List:
		out := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = starlark_decode(v.Index(i))
		}
		return out

	case sl.Tuple:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = starlark_decode(e)
		}
		return out

	case *sl.Dict:
		out := make(map[string]any)
		for _, i := range v.Items() {
			out[starlark_decode_string(i[0])] = starlark_decode(i[1])
		}
		return out

	default:
		warn("Starlark decode unknown type '%T'", v)
		return nil
	}
}

func starlark_decode_string(value sl.Value) string {
	s, ok := sl.AsString(value)
	if ok {
		return s
	}
	return fmt.Sprint(s)
}

func starlark_decode_strings(value any) map[string]string {
	switch v := value.(type) {
	case *sl.Dict:
		out := make(map[string]string)
		for _, i := range v.Items() {
			out[starlark_decode_string(i[0])] = starlark_decode_string(i[1])
		}
		return out

	default:
		warn("Starlark decode strings unknown type '%T'", v)
		return nil
	}
}

// Convert a single Go variable to a Starlark value
func starlark_encode(v any) sl.Value {
	debug("Encoding '%#v'", v)

	switch x := v.(type) {
	case nil:
		return sl.None

	case string:
		return sl.String(x)

	case int:
		return sl.MakeInt(x)

	case int64:
		return sl.MakeInt64(x)

	case bool:
		return sl.Bool(x)

	case map[any]any:
		d := sl.NewDict(len(x))
		for i, v := range x {
			d.SetKey(starlark_encode(i), starlark_encode(v))
		}
		return d

	case map[string]any:
		d := sl.NewDict(len(x))
		for i, v := range x {
			d.SetKey(sl.String(i), starlark_encode(v))
		}
		return d

	case map[string]string:
		d := sl.NewDict(len(x))
		for i, v := range x {
			d.SetKey(sl.String(i), sl.String(v))
		}
		return d

	case []any:
		var t []sl.Value
		for _, r := range x {
			t = append(t, starlark_encode(r))
		}
		return sl.Tuple(t)

	case []map[string]string:
		var t []sl.Value
		for _, r := range x {
			t = append(t, starlark_encode(r))
		}
		return sl.Tuple(t)

	case *[]map[string]any:
		var t []sl.Value
		for _, r := range *x {
			t = append(t, starlark_encode(r))
		}
		return sl.Tuple(t)

	case sl.Tuple:
		return x

	default:
		warn("Starlark encode unknown type '%T'", v)
		return nil
	}
}

// Convert one or more Go variables to a Starlark tuple
func starlark_encode_tuple(in ...any) sl.Tuple {
	//debug("Encoding to tuple '%+v'", in...)
	t := make(sl.Tuple, len(in))
	for i, v := range in {
		t[i] = starlark_encode(v)
	}
	return t
}

// Call a Starlark function
func (s *Starlark) call(function string, args sl.Tuple) (sl.Value, error) {
	f, found := s.globals[function]
	if !found {
		return nil, error_message("Starlark app function '%s' not found", function)
	}

	debug("Starlark running '%s': %+v", function, args)
	result, err := sl.Call(s.thread, f, args, nil)
	if err == nil {
		debug("Starlark finished")
	} else {
		debug("Starlark error: %v", err)
	}
	return result, err
}

// Convert a Starlark value to an int
func (s *Starlark) int(v sl.Value) int {
	var i int
	err := sl.AsInt(v, &i)
	if err != nil {
		info("Starlark failed to convert '%s' to int", v)
		return 0
	}
	return i
}

// Set a Starlark thread variable
func (s *Starlark) set(key string, value any) {
	s.thread.SetLocal(key, value)
}

// Get a new Starlark interpreter for an app
func (a *App) starlark() *Starlark {
	if a.starlark_runtime == nil {
		a.starlark_runtime = starlark(a.Files)
	}
	return a.starlark_runtime
}
