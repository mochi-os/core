// Mochi server: Starlark
// Copyright Alistair Cunningham 2025

package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"go.starlark.net/resolve"
	sl "go.starlark.net/starlark"
)

const starlark_max_steps = 10000000 // 10 million steps

var (
	starlark_sem             chan struct{}
	starlark_default_timeout time.Duration
)

// starlark_configure reads runtime settings from the loaded INI and applies them.
// Call this after ini_load(...) so configuration from the file takes effect.
func starlark_configure() {
	// if ini_file is nil, don't change current settings
	if ini_file == nil {
		return
	}

	c := ini_int("starlark", "concurrency", 32)
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
	mu      sync.Mutex
}

// Create a new Starlark interpreter for a set of files
func starlark(files []string) *Starlark {
	resolve.AllowSet = true
	resolve.AllowGlobalReassign = true
	resolve.AllowRecursion = true

	s := Starlark{thread: &sl.Thread{Name: "main"}}
	s.globals = make(sl.StringDict)

	// Copy api_globals to s.globals (predeclared globals like mochi)
	for k, v := range api_globals {
		s.globals[k] = v
	}

	for _, file := range files {
		//debug("Starlark reading file %q", file)
		defined, err := sl.ExecFile(s.thread, file, nil, s.globals)
		if err != nil {
			info("Starlark error reading file %v", err)
			continue
		}
		// Merge defined names into globals for subsequent files
		for k, v := range defined {
			s.globals[k] = v
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

	case sl.Bytes:
		return []byte(v)

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

// Extract a string from a Starlark value, returning empty string for None
func sl_string(value sl.Value) string {
	if value == nil || value == sl.None {
		return ""
	}
	if s, ok := value.(sl.String); ok {
		return string(s)
	}
	return ""
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

// Decode a Starlark value to a map[string]any
func sl_decode_map(value sl.Value) map[string]any {
	result := sl_decode(value)
	if m, ok := result.(map[string]any); ok {
		return m
	}
	return nil
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

	case float64:
		return sl.Float(x)

	case []uint8: // []byte
		return sl.Bytes(x)

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
		// Handle structs and pointers by converting through JSON
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Struct {
			data, err := json.Marshal(v)
			if err == nil {
				var m map[string]any
				if json.Unmarshal(data, &m) == nil {
					return sl_encode(m)
				}
			}
		}
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
	var underlying_err error

	switch v := e.(type) {
	case error:
		if v == nil {
			format = "Nil error"
		} else {
			format = v.Error()
			underlying_err = v
		}

	case string:
		format = v
	}

	// Check if any of the values is an error we should preserve
	for _, v := range values {
		if err, ok := v.(error); ok && underlying_err == nil {
			underlying_err = err
		}
	}

	var final_err error
	if fn == nil {
		final_err = fmt.Errorf(format, values...)
	} else {
		final_err = fmt.Errorf(fmt.Sprintf("%s() %s", fn.Name(), format), values...)
	}

	// Wrap with the underlying error to preserve error types for errors.As
	if underlying_err != nil {
		final_err = fmt.Errorf("%w: %v", underlying_err, final_err)
	}

	return sl.None, final_err
}

// Call a Starlark function
func (s *Starlark) call(function string, args sl.Tuple) (sl.Value, error) {
	f, found := s.globals[function]
	if !found {
		return nil, fmt.Errorf("Starlark app function %q not found", function)
	}

	// Acquire mutex to protect thread locals from concurrent access
	s.mu.Lock()
	defer s.mu.Unlock()

	// Acquire semaphore to limit concurrency
	starlark_sem <- struct{}{}
	defer func() { <-starlark_sem }()

	//debug("Starlark running %q: %+v", function, args)
	s.thread.SetLocal("function", function)

	// Set execution step limit
	s.thread.SetMaxExecutionSteps(starlark_max_steps)

	// Run the call in a goroutine so we can interrupt on timeout
	done := make(chan struct{})
	var result sl.Value
	var call_err error

	go func() {
		result, call_err = sl.Call(s.thread, f, args, nil)
		close(done)
	}()

	select {
	case <-done:
		if call_err == nil {
			//debug("Starlark finished")
		} else {
			a, ok := s.thread.Local("app").(*App)
			if a == nil {
				debug("%s(): %v", function, call_err)
			} else if ok {
				debug("App %s:%s() %v", a.id, function, call_err)
			}
		}
		// Clean up any streams opened during script execution
		if streams, ok := s.thread.Local("streams").([]*Stream); ok {
			for _, stream := range streams {
				stream.close()
			}
			s.thread.SetLocal("streams", nil)
		}
		return result, call_err
	case <-time.After(starlark_default_timeout):
		s.thread.Cancel("timeout")
		debug("Starlark %s() timed out after %s", function, starlark_default_timeout)
		if call_err == nil {
			call_err = fmt.Errorf("starlark: timeout after %s", starlark_default_timeout)
		}
		// Clean up any streams opened during script execution
		if streams, ok := s.thread.Local("streams").([]*Stream); ok {
			for _, stream := range streams {
				stream.close()
			}
			s.thread.SetLocal("streams", nil)
		}
		return nil, call_err
	}
}

// Set a Starlark thread variable
func (s *Starlark) set(key string, value any) {
	s.mu.Lock()
	s.thread.SetLocal(key, value)
	s.mu.Unlock()
}
