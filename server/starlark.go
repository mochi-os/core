// Mochi server: Starlark
// Copyright Alistair Cunningham 2025

package main

import (
	"fmt"
	"time"

	"go.starlark.net/resolve"
	sl "go.starlark.net/starlark"
)

// Runtime controls (configurable via ini)
var starlark_sem chan struct{}
var starlark_default_timeout time.Duration

// starlark_configure reads runtime settings from the loaded INI and applies them.
// Call this after ini_load(...) so configuration from the file takes effect.
func starlark_configure() {
	// if ini_file is nil, don't change current settings
	if ini_file == nil {
		return
	}

	c := ini_int("starlark", "concurrency", 4)
	if c < 1 {
		c = 4
	}
	starlark_sem = make(chan struct{}, c)

	secs := ini_int("starlark", "timeout", 60)
	if secs < 1 {
		secs = 60
	}
	starlark_default_timeout = time.Duration(secs) * time.Second
}

type Starlark struct {
	thread  *sl.Thread
	globals sl.StringDict
}

// Create a new Starlark interpreter for a set of files
func starlark(files []string) *Starlark {
	resolve.AllowSet = true
	resolve.AllowGlobalReassign = true
	resolve.AllowRecursion = true

	s := Starlark{thread: &sl.Thread{Name: "main"}}
	s.globals = api_globals

	for _, file := range files {
		debug("Starlark reading file %q", file)
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
func sl_decode(value sl.Value) any {
	//debug("Decoding '%+v', type '%T'", value, value)
	switch v := value.(type) {
	case sl.NoneType, nil:
		return nil

	case sl.Bool:
		return bool(v)

	case sl.Int:
		i, _ := v.Int64()
		return i

	case sl.Float:
		f, _ := sl.AsFloat(v)
		return f

	case sl.String:
		s, _ := sl.AsString(v)
		return s

	case *sl.List:
		out := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = sl_decode(v.Index(i))
		}
		return out

	case sl.Tuple:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = sl_decode(e)
		}
		return out

	case *sl.Dict:
		out := make(map[string]any, v.Len())
		for _, i := range v.Items() {
			key, ok := sl.AsString(i[0])
			if !ok {
				continue
			}
			out[key] = sl_decode(i[1])
		}
		return out

	default:
		warn("Starlark decode unknown type '%T'", v)
		return nil
	}
}

// Decode Starlark value to a string
func sl_decode_string(value any) string {
	//debug("Decoding to string '%v', type %T", value, value)
	switch v := value.(type) {
	case []any:
		return ""

	case sl.Int:
		var i int
		err := sl.AsInt(v, &i)
		if err == nil {
			return itoa(i)
		}
		return ""

	case sl.String:
		return v.GoString()

	case sl.Value:
		s, ok := sl.AsString(v)
		if ok {
			return s
		}
		return ""

	default:
		return fmt.Sprint(v)
	}
}

// Decode a single Starlark value to a map of strings to strings
func sl_decode_strings(value any) map[string]string {
	//debug("Decoding to strings '%#v'", value)
	switch v := value.(type) {
	case *sl.Dict:
		out := make(map[string]string, v.Len())
		for _, i := range v.Items() {
			out[sl_decode_string(i[0])] = sl_decode_string(i[1])
		}
		return out

	default:
		warn("Starlark decode strings unknown type '%T'", v)
		return nil
	}
}

// Decode a Starlark array to an array of map of strings to strings
func sl_decode_multi_strings(value any) *[]map[string]string {
	//debug("Decoding to multi strings '%+v'", value)
	switch v := value.(type) {
	case sl.Tuple:
		out := make([]map[string]string, len(v))
		for i, e := range v {
			out[i] = sl_decode_strings(e)
		}
		return &out

	default:
		warn("Starlark decode multi strings unknown type '%T'", v)
		return nil
	}
}

// Convert a single Go variable to a Starlark value
func sl_encode(v any) sl.Value {
	//debug("Encoding '%+v', type %T", v, v)

	switch x := v.(type) {
	case nil:
		return sl.None

	case string:
		return sl.String(x)

	case []string:
		t := make([]sl.Value, len(x))
		for i, r := range x {
			t[i] = sl.String(r)
		}
		return sl.Tuple(t)

	case int:
		return sl.MakeInt(x)

	case int64:
		return sl.MakeInt64(x)

	case uint64:
		return sl.MakeUint64(x)

	case []uint8:
		t := make([]sl.Value, len(x))
		for i, r := range x {
			t[i] = sl.MakeInt(int(r))
		}
		return sl.Tuple(t)

	case bool:
		return sl.Bool(x)

	case map[any]any:
		d := sl.NewDict(len(x))
		for i, v := range x {
			d.SetKey(sl_encode(i), sl_encode(v))
		}
		return d

	case map[string]any:
		d := sl.NewDict(len(x))
		for i, v := range x {
			d.SetKey(sl_encode(i), sl_encode(v))
		}
		return d

	case map[string]string:
		d := sl.NewDict(len(x))
		for i, v := range x {
			d.SetKey(sl_encode(i), sl_encode(v))
		}
		return d

	case []any:
		t := make([]sl.Value, len(x))
		for i, r := range x {
			t[i] = sl_encode(r)
		}
		return sl.Tuple(t)

	case []map[string]string:
		t := make([]sl.Value, len(x))
		for i, r := range x {
			t[i] = sl_encode(r)
		}
		return sl.Tuple(t)

	case []map[string]any:
		t := make([]sl.Value, len(x))
		for i, r := range x {
			t[i] = sl_encode(r)
		}
		return sl.Tuple(t)

	case *[]map[string]any:
		t := make([]sl.Value, len(*x))
		for i, r := range *x {
			t[i] = sl_encode(r)
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
func sl_encode_tuple(in ...any) sl.Tuple {
	//debug("Encoding to tuple '%+v', type %T", in)
	t := make(sl.Tuple, len(in))
	for i, v := range in {
		t[i] = sl_encode(v)
	}
	return t
}

// Helper function to return an error
func sl_error(fn *sl.Builtin, e any, values ...any) (sl.Value, error) {
	format := "Unknown error type"

	switch v := e.(type) {
	case error:
		if v == nil {
			format = "Nil error"
		} else {
			format = v.Error()
		}

	case string:
		format = v
	}

	if fn == nil {
		return sl.None, fmt.Errorf(format, values...)
	} else {
		return sl.None, fmt.Errorf(fmt.Sprintf("%s() %s", fn.Name(), format), values...)
	}
}

// Call a Starlark function
func (s *Starlark) call(function string, args sl.Tuple) (sl.Value, error) {
	f, found := s.globals[function]
	if !found {
		return nil, fmt.Errorf("Starlark app function %q not found", function)
	}

	debug("Starlark running %q: %+v", function, args)
	s.thread.SetLocal("function", function)

	// Acquire semaphore to limit concurrency
	starlark_sem <- struct{}{}
	defer func() { <-starlark_sem }()

	// Run the call in a goroutine so we can interrupt on timeout
	done := make(chan struct{})
	var result sl.Value
	var callErr error

	go func() {
		result, callErr = sl.Call(s.thread, f, args, nil)
		close(done)
	}()

	select {
	case <-done:
		if callErr == nil {
			debug("Starlark finished")
		} else {
			a, ok := s.thread.Local("app").(*App)
			if a == nil {
				debug("%s(): %v", function, callErr)
			} else if ok {
				debug("App %s:%s() %v", a.id, function, callErr)
			}
		}
		return result, callErr
	case <-time.After(starlark_default_timeout):
		s.thread.Cancel("timeout")
		debug("Starlark %s() timed out after %s", function, starlark_default_timeout)
		if callErr == nil {
			callErr = fmt.Errorf("starlark: timeout after %s", starlark_default_timeout)
		}
		return nil, callErr
	}
}

// Convert a Starlark value to an int
func (s *Starlark) int(v sl.Value) int {
	var i int
	err := sl.AsInt(v, &i)
	if err != nil {
		info("Starlark failed to convert %q to int", v)
		return 0
	}
	return i
}

// Set a Starlark thread variable
func (s *Starlark) set(key string, value any) {
	s.thread.SetLocal(key, value)
}
