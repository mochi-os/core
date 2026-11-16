// Mochi server: Starlark
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.starlark.net/resolve"
	sl "go.starlark.net/starlark"
)

const starlark_steps_maximum = 1000000000 // 1 billion steps

// How long to wait for a cancelled call to actually stop before abandoning it.
var starlark_cancel_grace = 5 * time.Second

// How long a call may wait for a concurrency slot before giving up. Generous,
// so a burst queues rather than failing, but finite so saturation surfaces as
// an error instead of an unbounded wait.
const starlark_queue_timeout = 60 * time.Second

// starlark_concurrency_default is how many Starlark calls may run at once
// before the rest queue. Sized for what the pool actually holds rather than for
// core count: a slot is held for the handler's whole wall-clock, and most
// handlers spend it blocked on SQLite, a P2P stream or an outbound fetch rather
// than on a CPU. Nesting also means a slot is not a request - every commit-hook
// level takes one, up to commit_hook_depth_maximum.
const starlark_concurrency_default = 1000

// Default bound for a call that is streaming a response to the client.
const starlark_file_default = 900 * time.Second

// starlark_holder is one call currently occupying a concurrency slot. started
// is the acquire time, so a report can rank by age: the oldest holder is the
// one that needs explaining.
type starlark_holder struct {
	app      string
	function string
	user     string
	started  time.Time
}

// starlark_holders records what occupies the pool right now, so a call that
// times out waiting can name what it waited for. Saturation is otherwise
// undiagnosable after the fact: the error names the victim and never the
// cause, and the holders have finished long before anyone reads the log.
var starlark_holders = struct {
	lock     sync.Mutex
	calls    map[int64]starlark_holder
	sequence int64
	reported time.Time
}{calls: map[int64]starlark_holder{}}

// starlark_holders_listed bounds how many distinct app:function groups one
// report names. The pool is configurable, so an operator who raised it should
// still get a line they can read.
const starlark_holders_listed = 10

// starlark_holder_add registers a call against the slot it just took and
// answers the key that removes it again.
func starlark_holder_add(app string, function string, user string) int64 {
	starlark_holders.lock.Lock()
	defer starlark_holders.lock.Unlock()
	starlark_holders.sequence++
	key := starlark_holders.sequence
	starlark_holders.calls[key] = starlark_holder{app: app, function: function, user: user, started: time.Now()}
	return key
}

// starlark_holder_remove releases a registration. Paired with the same defer
// that returns the slot, so an abandoned call cannot leave a phantom holder
// behind after its slot is back in the pool.
func starlark_holder_remove(key int64) {
	starlark_holders.lock.Lock()
	defer starlark_holders.lock.Unlock()
	delete(starlark_holders.calls, key)
}

// starlark_holder_app and starlark_holder_user name the call's app and account
// for the report. Both locals are absent while a file is still loading, so
// both answer "" rather than assuming a running action set them.
func starlark_holder_app(t *sl.Thread) string {
	if app, ok := t.Local("app").(*App); ok && app != nil {
		return app.id
	}
	return "unknown"
}

func starlark_holder_user(t *sl.Thread) string {
	if user, ok := t.Local("owner").(*User); ok && user != nil {
		return user.UID
	}
	return ""
}

// starlark_holders_report describes what holds the pool, grouped by
// app:function and ordered by how many slots each group holds. Answers "" when
// a report was already made inside the current timeout window, so one
// saturation episode does not repeat the same line for every victim.
func starlark_holders_report() string {
	starlark_holders.lock.Lock()
	if time.Since(starlark_holders.reported) < starlark_queue_timeout {
		starlark_holders.lock.Unlock()
		return ""
	}
	starlark_holders.reported = time.Now()
	held := make([]starlark_holder, 0, len(starlark_holders.calls))
	for _, holder := range starlark_holders.calls {
		held = append(held, holder)
	}
	starlark_holders.lock.Unlock()

	if len(held) == 0 {
		// Reachable: the pool can drain between the failed acquire and this
		// call. Say so rather than reporting an empty list as if it were the
		// state during the wait.
		return "the pool drained before it could be sampled"
	}

	type group struct {
		name   string
		count  int
		oldest time.Time
		user   string
	}
	groups := map[string]*group{}
	for _, holder := range held {
		name := holder.app + ":" + holder.function
		g, found := groups[name]
		if !found {
			groups[name] = &group{name: name, count: 1, oldest: holder.started, user: holder.user}
			continue
		}
		g.count++
		if holder.started.Before(g.oldest) {
			g.oldest = holder.started
			g.user = holder.user
		}
	}

	ranked := make([]*group, 0, len(groups))
	for _, g := range groups {
		ranked = append(ranked, g)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].count != ranked[j].count {
			return ranked[i].count > ranked[j].count
		}
		return ranked[i].oldest.Before(ranked[j].oldest)
	})

	parts := []string{}
	for i, g := range ranked {
		if i >= starlark_holders_listed {
			parts = append(parts, fmt.Sprintf("and %d more", len(ranked)-starlark_holders_listed))
			break
		}
		entry := fmt.Sprintf("%s x%d (oldest %s", g.name, g.count, time.Since(g.oldest).Round(time.Second))
		if g.user != "" {
			entry += ", user " + g.user
		}
		parts = append(parts, entry+")")
	}
	return fmt.Sprintf("%d slots held: %s", len(held), strings.Join(parts, ", "))
}

var (
	starlark_semaphore       chan struct{}
	starlark_default_timeout time.Duration
	// starlark_file_timeout bounds a call that has handed the response to the
	// client and is only streaming bytes. Initialised here as well as in
	// starlark_configure, so a zero value cannot cut a download short pre-INI.
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

	c := ini_int("starlark", "concurrency", starlark_concurrency_default)
	if c < 1 {
		c = 4
	}
	starlark_semaphore = make(chan struct{}, c)

	secs := ini_int("starlark", "timeout", 300)
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

// ErrStarlarkLoad marks a missing function that is missing because a file in
// the app's execute list failed to load, not because the app never declared it.
// The two need opposite handling on the P2P path - one is transient, the other
// is fixed until the app changes - and the message alone cannot tell them apart.
var ErrStarlarkLoad = errors.New("starlark file failed to load")

type Starlark struct {
	thread  *sl.Thread
	globals sl.StringDict

	// timeout overrides the compute bound for this interpreter's calls. Zero
	// means the configured default. Set it where a slow call is worse than a
	// failed one - a visibility check blocks a page render, so it gives up
	// early and is read as a refusal.
	timeout time.Duration

	// Files whose ExecFile failed. Every definition in them is absent from
	// globals, and starlark_once caches that partial set for the process
	// lifetime outside dev_reload, so a missing function has to be able to name
	// the file that actually broke.
	failed []string
}

// Create a new Starlark interpreter for a set of files
func starlark(files []string) *Starlark {
	resolve.AllowSet = true
	resolve.AllowGlobalReassign = true
	resolve.AllowRecursion = true

	s := Starlark{thread: &sl.Thread{Name: "main"}}
	s.globals = make(sl.StringDict)

	// Copy the API table into s.globals (predeclared globals like mochi)
	for k, v := range api_globals {
		s.globals[k] = v
	}

	for _, file := range files {
		//debug("Starlark reading file %q", file)
		defined, err := sl.ExecFile(s.thread, file, nil, s.globals)
		if err != nil {
			// Name the file explicitly: an error from a mochi.* call at module level is
			// a bare Go error carrying no position. warn, not info - ExecFile loses
			// every definition in the file, and starlark_once caches that partial set
			// for the process lifetime outside dev_reload.
			warn("Starlark error reading %s: %v", file, err)
			s.failed = append(s.failed, filepath.Base(file))
			continue
		}
		// Merge defined names into globals for subsequent files
		for k, v := range defined {
			s.globals[k] = v
		}
	}

	return &s
}

// sl_depth_maximum is how deep a value core converts may nest.
const sl_depth_maximum = 1000

// sl_cut reports whether container, a list or dictionary being converted
// within path, is left out, as null: one already on the path, or one deeper
// than sl_depth_maximum. Starlark lets a list or dictionary hold itself, and
// followed without a bound such a value recursed until the stack ran out, a
// fatal error no recover catches: one app's value ended the whole server. A
// value holding itself twice over branched at every level, so a bound on
// depth alone would have hung instead.
func sl_cut(path []sl.Value, container sl.Value) bool {
	if len(path) >= sl_depth_maximum {
		warn("Starlark value nested deeper than %d, left out", sl_depth_maximum)
		return true
	}
	for _, p := range path {
		if p == container {
			warn("Starlark value that holds itself, the inner one left out")
			return true
		}
	}
	return false
}

// Convert a Starlark value to a Go variable
func sl_decode(value sl.Value) any {
	return sl_decode_within(value, nil)
}

// sl_decode_within is sl_decode inside path, the lists and dictionaries that
// hold value.
func sl_decode_within(value sl.Value, path []sl.Value) any {
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
		if sl_cut(path, v) {
			return nil
		}
		path = append(path, v)
		out := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = sl_decode_within(v.Index(i), path)
		}
		return out

	case sl.Tuple:
		// A tuple cannot hold itself, only through a list or dictionary,
		// which is where the path catches it; it still counts toward depth.
		if len(path) >= sl_depth_maximum {
			warn("Starlark value nested deeper than %d, left out", sl_depth_maximum)
			return nil
		}
		path = append(path, nil)
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = sl_decode_within(e, path)
		}
		return out

	case *sl.Dict:
		if sl_cut(path, v) {
			return nil
		}
		path = append(path, v)
		out := make(map[string]any, v.Len())
		for _, i := range v.Items() {
			key, ok := sl.AsString(i[0])
			if !ok {
				continue
			}
			out[key] = sl_decode_within(i[1], path)
		}
		return out

	default:
		warn("Starlark decode unknown type '%T'", v)
		return nil
	}
}

// sl_json is an action's answer, rendered as JSON straight from its Starlark
// value: the same bytes json.Marshal(sl_decode(value)) gives, without the tree
// of Go maps and slices sl_decode builds and the reflection that marshals it,
// and written as it is made, a piece at a time, rather than whole. A 40,000-row
// answer of 3.7 MB allocated 43-47 MB and took 45-49 ms that way before a byte
// was written.
type sl_json struct {
	value sl.Value
}

// sl_json_piece is how much of an answer is held before it is written.
const sl_json_piece = 32 << 10

// Render writes the answer, or nothing when it holds a value JSON cannot carry
// (a NaN or an infinity), which gin then records against the request as it
// did when json.Marshal refused one.
func (r sl_json) Render(w http.ResponseWriter) error {
	r.WriteContentType(w)
	writer := sl_json_writer{data: make([]byte, 0, sl_json_piece), out: w, root: r.value}
	if err := writer.value(r.value, nil); err != nil {
		return err
	}
	_, err := w.Write(writer.data)
	return err
}

// WriteContentType names JSON unless the action named a type itself.
func (r sl_json) WriteContentType(w http.ResponseWriter) {
	header := w.Header()
	if len(header["Content-Type"]) == 0 {
		header["Content-Type"] = []string{"application/json; charset=utf-8"}
	}
}

// sl_json_writer writes a value as encoding/json writes what sl_decode makes of
// it: a dictionary's string keys only, sorted; a whole number past 64 bits as
// 0; bytes in base64; and anything else null. It holds the JSON made so far
// and not yet written, and the entries of every dictionary being written, the
// innermost last, each sorted in place: one slice for the whole answer, where
// a slice per dictionary allocated more than the answer's own bytes. With out,
// a piece is written once it is full; without, the answer is only appended to
// data.
type sl_json_writer struct {
	data    []byte
	entries []sl_json_entry
	out     io.Writer
	root    sl.Value
	checked bool
}

type sl_json_entry struct {
	key   string
	value sl.Value
}

// spill writes what is held once a piece is full. The first time, before any
// byte goes, the whole answer is checked for a value JSON cannot carry: one
// found later would leave the client a 200 cut short, where an answer that
// fails before its first byte is no answer at all.
func (w *sl_json_writer) spill() error {
	if w.out == nil || len(w.data) < sl_json_piece {
		return nil
	}
	if !w.checked {
		if err := sl_json_check(w.root, nil); err != nil {
			return err
		}
		w.checked = true
	}
	_, err := w.out.Write(w.data)
	w.data = w.data[:0]
	return err
}

// sl_json_check finds what sl_json_writer would refuse, without writing: a
// NaN or an infinity, in any part of value it would write - which leaves out
// what sl_cut cuts, as the writer does, within path.
func sl_json_check(value sl.Value, path []sl.Value) error {
	switch v := value.(type) {
	case sl.Float:
		if f := float64(v); math.IsInf(f, 0) || math.IsNaN(f) {
			return fmt.Errorf("json: unsupported value: %s", strconv.FormatFloat(f, 'g', -1, 64))
		}
	case *sl.List:
		if sl_cut(path, v) {
			return nil
		}
		path = append(path, v)
		for i := 0; i < v.Len(); i++ {
			if err := sl_json_check(v.Index(i), path); err != nil {
				return err
			}
		}
	case sl.Tuple:
		if len(path) >= sl_depth_maximum {
			return nil
		}
		path = append(path, nil)
		for _, e := range v {
			if err := sl_json_check(e, path); err != nil {
				return err
			}
		}
	case *sl.Dict:
		if sl_cut(path, v) {
			return nil
		}
		path = append(path, v)
		for key, value := range v.Entries() {
			if _, ok := key.(sl.String); ok {
				if err := sl_json_check(value, path); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// value writes value, within path, the lists and dictionaries that hold it:
// what sl_cut cuts is written null, as sl_decode leaves it.
func (w *sl_json_writer) value(value sl.Value, path []sl.Value) error {
	switch v := value.(type) {
	case sl.NoneType, nil:
		w.data = append(w.data, "null"...)

	case sl.Bool:
		w.data = strconv.AppendBool(w.data, bool(v))

	case sl.Int:
		i, _ := v.Int64()
		w.data = strconv.AppendInt(w.data, i, 10)

	case sl.Float:
		f := float64(v)
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return fmt.Errorf("json: unsupported value: %s", strconv.FormatFloat(f, 'g', -1, 64))
		}
		// As encoding/json writes a float: like %g, with its own exponent
		// cutoffs and no padding of the exponent.
		format := byte('f')
		if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
			format = 'e'
		}
		w.data = strconv.AppendFloat(w.data, f, format, -1, 64)
		if format == 'e' {
			if n := len(w.data); n >= 4 && w.data[n-4] == 'e' && w.data[n-3] == '-' && w.data[n-2] == '0' {
				w.data[n-2] = w.data[n-1]
				w.data = w.data[:n-1]
			}
		}

	case sl.String:
		w.data = sl_json_string(w.data, string(v))

	case sl.Bytes:
		b := []byte(v)
		if b == nil {
			w.data = append(w.data, "null"...)
			break
		}
		w.data = append(w.data, '"')
		w.data = base64.StdEncoding.AppendEncode(w.data, b)
		w.data = append(w.data, '"')

	case *sl.List:
		if sl_cut(path, v) {
			w.data = append(w.data, "null"...)
			break
		}
		path = append(path, v)
		w.data = append(w.data, '[')
		for i := 0; i < v.Len(); i++ {
			if i > 0 {
				w.data = append(w.data, ',')
			}
			if err := w.value(v.Index(i), path); err != nil {
				return err
			}
			if err := w.spill(); err != nil {
				return err
			}
		}
		w.data = append(w.data, ']')

	case sl.Tuple:
		if len(path) >= sl_depth_maximum {
			warn("Starlark value nested deeper than %d, left out", sl_depth_maximum)
			w.data = append(w.data, "null"...)
			break
		}
		path = append(path, nil)
		w.data = append(w.data, '[')
		for i, e := range v {
			if i > 0 {
				w.data = append(w.data, ',')
			}
			if err := w.value(e, path); err != nil {
				return err
			}
			if err := w.spill(); err != nil {
				return err
			}
		}
		w.data = append(w.data, ']')

	case *sl.Dict:
		if sl_cut(path, v) {
			w.data = append(w.data, "null"...)
			break
		}
		path = append(path, v)
		start := len(w.entries)
		defer func() { w.entries = w.entries[:start] }()
		for key, value := range v.Entries() {
			if text, ok := key.(sl.String); ok {
				w.entries = append(w.entries, sl_json_entry{key: string(text), value: value})
			}
		}
		end := len(w.entries)
		slices.SortFunc(w.entries[start:end], func(a, b sl_json_entry) int {
			return strings.Compare(a.key, b.key)
		})
		w.data = append(w.data, '{')
		for i := start; i < end; i++ {
			// Indexed afresh each time: a dictionary inside this one adds its
			// entries past end, which may move the slice.
			entry := w.entries[i]
			if i > start {
				w.data = append(w.data, ',')
			}
			w.data = sl_json_string(w.data, entry.key)
			w.data = append(w.data, ':')
			if err := w.value(entry.value, path); err != nil {
				return err
			}
			if err := w.spill(); err != nil {
				return err
			}
		}
		w.data = append(w.data, '}')

	default:
		warn("Starlark decode unknown type '%T'", v)
		w.data = append(w.data, "null"...)
	}
	return nil
}

// sl_json_string appends text as a JSON string escaped as encoding/json
// escapes it: HTML's <, > and & too, U+2028 and U+2029, and each byte that is
// not UTF-8 as U+FFFD.
func sl_json_string(data []byte, text string) []byte {
	const hex = "0123456789abcdef"
	data = append(data, '"')
	start := 0
	for i := 0; i < len(text); {
		if b := text[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			data = append(data, text[start:i]...)
			switch b {
			case '\\', '"':
				data = append(data, '\\', b)
			case '\b':
				data = append(data, '\\', 'b')
			case '\f':
				data = append(data, '\\', 'f')
			case '\n':
				data = append(data, '\\', 'n')
			case '\r':
				data = append(data, '\\', 'r')
			case '\t':
				data = append(data, '\\', 't')
			default:
				data = append(data, '\\', 'u', '0', '0', hex[b>>4], hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(text[i:])
		if c == utf8.RuneError && size == 1 {
			data = append(data, text[start:i]...)
			data = append(data, `\ufffd`...)
			i += size
			start = i
			continue
		}
		if c == '\u2028' || c == '\u2029' {
			data = append(data, text[start:i]...)
			data = append(data, '\\', 'u', '2', '0', '2', hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	data = append(data, text[start:]...)
	return append(data, '"')
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
	var underlying_error error

	switch v := e.(type) {
	case error:
		if v == nil {
			format = "Nil error"
		} else {
			format = v.Error()
			underlying_error = v
		}

	case string:
		format = v
	}

	// Check if any of the values is an error we should preserve
	for _, v := range values {
		if err, ok := v.(error); ok && underlying_error == nil {
			underlying_error = err
		}
	}

	var final_error error
	if fn == nil {
		final_error = fmt.Errorf(format, values...)
	} else {
		final_error = fmt.Errorf(fmt.Sprintf("%s() %s", fn.Name(), format), values...)
	}

	// Wrap with the underlying error to preserve error types for errors.As
	if underlying_error != nil {
		final_error = fmt.Errorf("%w: %v", underlying_error, final_error)
	}

	return sl.None, final_error
}

// Mark this thread as having handed its response to the client. The flag is an
// atomic because Starlark.call may read it after the compute timeout fires.
// Also sets a write deadline: the listener has no WriteTimeout, so a client
// that stops reading would otherwise hold this concurrency slot for ever.
func starlark_serving_set(t *sl.Thread, writer http.ResponseWriter) {
	if serving, ok := t.Local("file_serving").(*atomic.Bool); ok {
		serving.Store(true)
	}
	if writer != nil {
		// Fails only on a writer with no deadline support, where there is
		// nothing to bound; the call-level timeout still applies.
		_ = http.NewResponseController(writer).SetWriteDeadline(time.Now().Add(starlark_file_timeout))
	}
}

// starlark_transfer_set marks a call as moving bulk bytes rather than
// computing, so the transfer bound applies instead of the compute one. The copy
// sits inside a built-in that ignores cancellation, so a compute bound would
// abandon the caller while the transfer ran on regardless.
func starlark_transfer_set(t *sl.Thread) {
	starlark_serving_set(t, nil)
}

// Report whether this thread has handed its response to the client.
func starlark_serving_get(t *sl.Thread) bool {
	serving, ok := t.Local("file_serving").(*atomic.Bool)
	return ok && serving.Load()
}

// starlark_context returns the calling thread's context, cancelled at the call
// timeout. Builtins that block inside Go must pass it down: thread.Cancel is
// only observed between interpreter steps, so a statement already inside SQLite
// runs to completion. Falls back to a background context outside Starlark.call.
func starlark_context(t *sl.Thread) context.Context {
	if ctx, ok := t.Local("context").(context.Context); ok && ctx != nil {
		return ctx
	}
	return context.Background()
}

// has reports whether the app defines a Starlark global of this name — the
// same lookup call() does, so a caller can test resolvability without running
// anything.
func (s *Starlark) has(function string) bool {
	_, found := s.globals[function]
	if !found && len(s.failed) > 0 {
		// Not "the app does not handle this": the definition may be in a file
		// that did not load. Say so, since the bool cannot carry the reason.
		info("Starlark function %q absent and %s failed to load", function, strings.Join(s.failed, ", "))
	}
	return found
}

// Call a Starlark function
func (s *Starlark) call(function string, args sl.Tuple, kwargs ...[]sl.Tuple) (sl.Value, error) {
	f, found := s.globals[function]
	if !found {
		if len(s.failed) > 0 {
			// Points the reader at the file that broke rather than at the app's
			// handler declaration, which is usually correct, and marks the error
			// so the P2P classifier retries instead of dropping the event.
			return nil, fmt.Errorf("Starlark app function %q not found: %s failed to load: %w",
				function, strings.Join(s.failed, ", "), ErrStarlarkLoad)
		}
		return nil, fmt.Errorf("Starlark app function %q not found", function)
	}
	var kw []sl.Tuple
	if len(kwargs) > 0 {
		kw = kwargs[0]
	}

	// Acquire a concurrency slot, released by the CALLER, not the goroutine below:
	// Starlark calls nest (mochi.service.call, commit hooks, migrations), and a
	// blocked nested acquire cannot be interrupted by thread.Cancel, so releasing
	// from the goroutine would lose the slot for good.
	select {
	case starlark_semaphore <- struct{}{}:
	case <-time.After(starlark_queue_timeout):
		// Name what the pool was doing. Without this the failure records only
		// which call lost the race, which is never the call worth fixing.
		if report := starlark_holders_report(); report != "" {
			warn("Starlark concurrency exhausted, %q waited %s: %s", function, starlark_queue_timeout, report)
		}
		return nil, fmt.Errorf("starlark: no concurrency slot available after %s", starlark_queue_timeout)
	}
	holder := starlark_holder_add(starlark_holder_app(s.thread), function, starlark_holder_user(s.thread))
	defer func() {
		starlark_holder_remove(holder)
		<-starlark_semaphore
	}()

	//debug("Starlark running %q: %+v", function, args)
	s.thread.SetLocal("function", function)

	// Cancelled when this call returns, including when it returns because it
	// timed out and abandoned its goroutine. That is what interrupts a query
	// still running inside SQLite; without it the abandoned call keeps a
	// connection and a concurrency slot for as long as the statement takes.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.thread.SetLocal("context", ctx)

	// file_serving is set mid-call by the a.write.* builtins, and read here
	// after a timeout — while the call may still be running. An atomic
	// installed before the goroutine starts keeps that cross-goroutine flag
	// out of the thread's unsynchronised locals map.
	serving := &atomic.Bool{}
	s.thread.SetLocal("file_serving", serving)

	// Compute bound for this call. s.timeout only ever shortens it: an app
	// must not be able to buy itself more than the configured ceiling.
	deadline := starlark_default_timeout
	if s.timeout > 0 && (deadline <= 0 || s.timeout < deadline) {
		deadline = s.timeout
	}

	// Reset cancel state from any previous timeout
	s.thread.Uncancel()

	// Set execution step limit
	s.thread.SetMaxExecutionSteps(starlark_steps_maximum)

	// Run the call in a goroutine so we can interrupt on timeout. Buffered so
	// a goroutine we have already abandoned can always send and exit.
	done := make(chan starlark_result, 1)

	go func() {
		// Recover panics so a fault in one Starlark call becomes an error the caller
		// can report. gin's Recovery middleware only wraps HTTP handlers; this
		// goroutine is spawned outside it, so an unguarded panic kills the process.
		var out starlark_result
		defer func() {
			if r := recover(); r != nil {
				out = starlark_result{err: fmt.Errorf("Starlark call %q panicked: %v", function, r)}
			}
			// Cleanup belongs to the goroutine that owns this thread. Doing it in the
			// caller raced a timed-out call still running: its streams were closed and
			// its transaction rolled back out from under it.
			if streams, ok := s.thread.Local("streams").([]*Stream); ok {
				for _, stream := range streams {
					stream.close()
				}
				s.thread.SetLocal("streams", nil)
			}
			transaction_close(s.thread)
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
	case <-time.After(deadline):
		// A call that has handed the response to the client is only streaming bytes,
		// so the compute timeout would truncate a legitimate download. Give it the
		// longer file bound - but bound it, or a stalled reader holds the slot.
		if serving.Load() {
			select {
			case out := <-done:
				return out.value, out.err
			case <-time.After(starlark_file_timeout):
				// Cancelled as the compute bound cancels, the context too, so a
				// relay tied to it stops, and given the same grace to.
				s.thread.Cancel("timeout")
				cancel()
				select {
				case out := <-done:
					return out.value, out.err
				case <-time.After(starlark_cancel_grace):
				}
				select {
				case out := <-done:
					return out.value, out.err
				default:
				}
				debug("Starlark %s() file serving timed out after %s", function, starlark_file_timeout)
				return nil, fmt.Errorf("starlark: file serving timeout after %s: %w", starlark_file_timeout, starlark_abandoned)
			}
		}
		// The context goes with the thread, so a builtin that honours it - a
		// query inside SQLite, a relay tied to it - stops within the grace and
		// the call ends, rather than being abandoned while it winds down.
		s.thread.Cancel("timeout")
		cancel()
		// Give the interpreter a moment to observe the cancel, so the caller
		// gets the specific cancellation error rather than a bare timeout. The
		// outcome arrives over the channel: reading it from shared variables
		// raced with a call still running inside a built-in.
		select {
		case out := <-done:
			return out.value, out.err
		case <-time.After(starlark_cancel_grace):
		}
		// A call that ended as the grace ran out is not abandoned.
		select {
		case out := <-done:
			return out.value, out.err
		default:
		}
		// The call did not stop — it is stuck in a built-in that does not check
		// for cancellation. Abandon it. The goroutine runs its own cleanup and
		// frees its semaphore slot when it eventually exits; touching the
		// thread, its streams or its transaction from here would race with it.
		debug("Starlark %s() timed out after %s", function, deadline)
		return nil, fmt.Errorf("starlark: timeout after %s: %w", deadline, starlark_abandoned)
	}
}

// starlark_abandoned marks the error of a call given up on while it still runs,
// inside a built-in that does not check for cancellation. Whatever it holds
// stays in use after the caller returns: an HTTP handler must then not hand
// its request's context back for reuse (see web_action_abandoned).
var starlark_abandoned = errors.New("the call was abandoned while still running")

// Set a Starlark thread variable
func (s *Starlark) set(key string, value any) {
	s.thread.SetLocal(key, value)
}
