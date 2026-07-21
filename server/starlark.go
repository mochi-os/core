// Mochi server: Starlark
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"go.starlark.net/resolve"
	sl "go.starlark.net/starlark"
)

const starlark_max_steps = 1000000000 // 1 billion steps

// How long to wait for a cancelled call to actually stop before abandoning it.
const starlark_cancel_grace = 5 * time.Second

// Default bound for a call that is streaming a response to the client.
const starlark_file_default = 900 * time.Second

var (
	starlark_sem             chan struct{}
	starlark_default_timeout time.Duration
	// starlark_file_timeout bounds a call that has handed the response to the
	// client and is streaming bytes. Such a call must outlive the compute
	// timeout — the Starlark work is already done — but it still needs a
	// bound, or a client that reads slowly (or stops) holds a concurrency slot
	// indefinitely. Initialised here as well as in starlark_configure, so a
	// zero value can never cut a download short before the INI is read.
	starlark_file_timeout = starlark_file_default
)

// starlark_result carries a call's outcome back from its goroutine. Passing it
// through a channel rather than writing shared variables means an abandoned
// call cannot race the caller that gave up on it.
type starlark_result struct {
	value sl.Value
	err   error
}

// starlark_configure reads runtime settings from the loaded INI and applies them.
// Call this after ini_load(...) so configuration from the file takes effect.
func starlark_configure() {
	// if no ini has been loaded, don't change current settings
	if !ini_loaded() {
		return
	}

	c := ini_int("starlark", "concurrency", 32)
	if c < 1 {
		c = 4
	}
	starlark_sem = make(chan struct{}, c)

	secs := ini_int("starlark", "timeout", 90)
	if secs < 1 {
		secs = 60
	}
	starlark_default_timeout = time.Duration(secs) * time.Second

	file_secs := ini_int("starlark", "file_timeout", int(starlark_file_default/time.Second))
	if file_secs < secs {
		file_secs = secs
	}
	starlark_file_timeout = time.Duration(file_secs) * time.Second
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
		if x == nil {
			return sl.None
		}
		d := sl.NewDict(len(x))
		for i, v := range x {
			d.SetKey(sl_encode(i), sl_encode(v))
		}
		return d

	case map[string]any:
		if x == nil {
			return sl.None
		}
		d := sl.NewDict(len(x))
		for i, v := range x {
			d.SetKey(sl_encode(i), sl_encode(v))
		}
		return d

	case map[string]string:
		if x == nil {
			return sl.None
		}
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

// Mark this thread as having handed its response to the client: the Starlark
// work is done and only HTTP I/O remains. The flag is an atomic because the
// call's own goroutine sets it while Starlark.call may be reading it after the
// compute timeout has fired.
func starlark_serving_set(t *sl.Thread) {
	if serving, ok := t.Local("file_serving").(*atomic.Bool); ok {
		serving.Store(true)
	}
}

// Report whether this thread has handed its response to the client.
func starlark_serving_get(t *sl.Thread) bool {
	serving, ok := t.Local("file_serving").(*atomic.Bool)
	return ok && serving.Load()
}

// Call a Starlark function
func (s *Starlark) call(function string, args sl.Tuple, kwargs ...[]sl.Tuple) (sl.Value, error) {
	f, found := s.globals[function]
	if !found {
		return nil, fmt.Errorf("Starlark app function %q not found", function)
	}
	var kw []sl.Tuple
	if len(kwargs) > 0 {
		kw = kwargs[0]
	}

	// Acquire semaphore to limit concurrency. It is released by the goroutine
	// below, NOT here: a call abandoned on timeout keeps running and keeps
	// consuming resources, so it must keep occupying its slot until it really
	// finishes. Releasing it on return let abandoned calls push the process
	// past its configured concurrency limit.
	starlark_sem <- struct{}{}

	//debug("Starlark running %q: %+v", function, args)
	s.thread.SetLocal("function", function)

	// file_serving is set mid-call by the a.write.* builtins, and read here
	// after a timeout — while the call may still be running. An atomic
	// installed before the goroutine starts keeps that cross-goroutine flag
	// out of the thread's unsynchronised locals map.
	serving := &atomic.Bool{}
	s.thread.SetLocal("file_serving", serving)

	// Reset cancel state from any previous timeout
	s.thread.Uncancel()

	// Set execution step limit
	s.thread.SetMaxExecutionSteps(starlark_max_steps)

	// Run the call in a goroutine so we can interrupt on timeout. Buffered so
	// a goroutine we have already abandoned can always send and exit.
	done := make(chan starlark_result, 1)

	go func() {
		// Recover panics so a fault in one Starlark call (a malformed
		// SQLite DB, a nil deref in a Go-side API, etc.) becomes an
		// error the caller can report — instead of unwinding an
		// unguarded goroutine and taking the whole server down. gin's
		// Recovery middleware only wraps HTTP handlers; this goroutine
		// is spawned outside it, so without this defer a single bad DB
		// crashed the process (2026-05-21: a user DB mid-bootstrap-swap
		// panicked here and killed mochi2).
		var out starlark_result
		defer func() {
			if r := recover(); r != nil {
				out = starlark_result{err: fmt.Errorf("Starlark call %q panicked: %v", function, r)}
			}
			// Cleanup belongs to the goroutine that owns this thread, not to
			// the caller. Doing it in the caller raced with a timed-out call
			// that was still running: its streams were closed and its
			// transaction rolled back out from under it, on the same thread it
			// was still using. Here it happens exactly once, when the work has
			// genuinely finished.
			if streams, ok := s.thread.Local("streams").([]*Stream); ok {
				for _, stream := range streams {
					stream.close()
				}
				s.thread.SetLocal("streams", nil)
			}
			transaction_close(s.thread)
			<-starlark_sem
			done <- out
		}()
		value, err := sl.Call(s.thread, f, args, kw)
		out = starlark_result{value: value, err: err}
	}()

	select {
	case out := <-done:
		if out.err != nil {
			a, ok := s.thread.Local("app").(*App)
			if a == nil {
				debug("%s(): %v", function, out.err)
			} else if ok {
				debug("App %s:%s() %v", a.id, function, out.err)
			}
		}
		return out.value, out.err
	case <-time.After(starlark_default_timeout):
		// A call that has handed the response to the client has finished its
		// Starlark work and is only streaming bytes, so cancelling it at the
		// compute timeout would truncate a legitimate download. Give it the
		// longer file bound instead — but do bound it, so a client that stops
		// reading cannot hold this concurrency slot forever.
		if serving.Load() {
			select {
			case out := <-done:
				return out.value, out.err
			case <-time.After(starlark_file_timeout):
				s.thread.Cancel("timeout")
				debug("Starlark %s() file serving timed out after %s", function, starlark_file_timeout)
				return nil, fmt.Errorf("starlark: file serving timeout after %s", starlark_file_timeout)
			}
		}
		s.thread.Cancel("timeout")
		// Give the interpreter a moment to observe the cancel, so the caller
		// gets the specific cancellation error rather than a bare timeout. The
		// outcome arrives over the channel: reading it from shared variables
		// raced with a call still running inside a built-in.
		select {
		case out := <-done:
			return out.value, out.err
		case <-time.After(starlark_cancel_grace):
		}
		// The call did not stop — it is stuck in a built-in that does not check
		// for cancellation. Abandon it. The goroutine runs its own cleanup and
		// frees its semaphore slot when it eventually exits; touching the
		// thread, its streams or its transaction from here would race with it.
		debug("Starlark %s() timed out after %s", function, starlark_default_timeout)
		return nil, fmt.Errorf("starlark: timeout after %s", starlark_default_timeout)
	}
}

// Set a Starlark thread variable
func (s *Starlark) set(key string, value any) {
	s.thread.SetLocal(key, value)
}
