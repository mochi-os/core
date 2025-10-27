// Mochi server: Events
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	rd "runtime/debug"
	"sync"
)

type Event struct {
	id      int64
	from    string
	to      string
	service string
	event   string
	peer    string
	content map[string]string
	user    *User
	db      *DB
	stream  *Stream
}

var (
	events_lock       = &sync.Mutex{}
	event_next  int64 = 1
)

func event_id() int64 {
	events_lock.Lock()
	id := event_next
	event_next = event_next + 1
	events_lock.Unlock()
	return id
}

// Get a field from the content segment of a received event
func (e *Event) get(field string, def string) string {
	result, found := e.content[field]
	if found {
		return result
	}
	return def
}

// Route a received event to the correct app
func (e *Event) route() {
	if e.to != "" {
		e.user = user_owning_entity(e.to)
		if e.user == nil {
			info("Event dropping to unknown user '%s'", e.to)
			return
		}
	}

	// Find which app provides this service
	a := services[e.service]
	if a == nil {
		info("Event dropping to unknown service '%s'", e.service)
		return
	}

	// Lock everything below here to prevent concurrent database creations in db_app()
	user := 0
	if e.user != nil {
		user = e.user.ID
	}
	l := lock(fmt.Sprintf("%d-%s", user, a.id))
	l.Lock()
	defer l.Unlock()

	// Load a database file for the app
	if a.active.Database.File != "" {
		if e.user == nil {
			info("Event dropping broadcast for service requiring user")
			return
		}
		e.db = db_app(e.user, a.active)
		if e.db == nil {
			info("Event app '%s' has no database file", a.id)
			return
		}
		defer e.db.close()
	}

	// Check which engine the app uses, and run it
	switch a.active.Engine.Architecture {
	case "internal":
		// Look for matching app event, using default if necessary
		var f func(*Event)
		var found bool
		if e.to == "" {
			f, found = a.internal.events_broadcast[e.event]
		} else {
			f, found = a.internal.events[e.event]
		}
		if !found {
			if e.to == "" {
				f, found = a.internal.events_broadcast[""]
			} else {
				f, found = a.internal.events[""]
			}
		}
		if !found {
			info("Event dropping to unknown event '%s' in app '%s' for service '%s'", e.event, a.id, e.service)
			return
		}

		defer func() {
			r := recover()
			if r != nil {
				warn("Event handler error: %v\n\n%s", r, string(rd.Stack()))
			}
		}()

		f(e)

	case "starlark":
		// Look for matching app event, using default if necessary
		ev, found := a.active.Services[e.service].Events[e.event]
		if !found {
			ev, found = a.active.Services[e.service].Events[""]
		}
		if !found {
			info("Event dropping to unknown event '%s' in app '%s' for service '%s'", e.event, a.id, e.service)
			return
		}

		if e.to == "" && !ev.Broadcast {
			info("Event dropping broadcast to non-broadcast")
			return
		}

		if e.to != "" && ev.Broadcast {
			info("Event dropping non-broadcast to broadcast")
			return
		}

		s := a.active.starlark()
		s.set("event", e)
		s.set("app", a)
		s.set("user", e.user)
		s.set("owner", e.user)

		headers := map[string]string{
			"from":    e.from,
			"to":      e.to,
			"service": e.service,
			"event":   e.event,
		}

		debug("App event %s:%s(): %v", a.id, ev.Function, e)
		if a.active.Engine.Version == 1 {
			s.call(ev.Function, sl_encode_tuple(headers, e.content))
		} else {
			s.call(ev.Function, sl.Tuple{e})
		}

	default:
		info("Event unknown engine '%s' version '%s'", a.active.Engine.Architecture, a.active.Engine.Version)
		return
	}
}

// Decode the next segment from a received event
func (e *Event) segment(v any) bool {
	err := e.stream.decoder.Decode(v)
	if err != nil {
		info("Event unable to decode segment: %v", err)
		return false
	}
	return true
}

// Starlark methods
func (e *Event) AttrNames() []string {
	return []string{"content", "dump", "header", "read", "read_to_file", "stream", "user", "write", "write_from_file"}
}

func (e *Event) Attr(name string) (sl.Value, error) {
	switch name {
	case "content":
		return sl.NewBuiltin("content", e.sl_content), nil
	case "dump":
		return sl.NewBuiltin("dump", e.sl_dump), nil
	case "header":
		return sl.NewBuiltin("header", e.sl_header), nil
	case "read":
		return sl.NewBuiltin("read", e.stream.sl_read), nil
	case "read_to_file":
		return sl.NewBuiltin("read_to_file", e.stream.sl_read_to_file), nil
	case "stream":
		return e.stream, nil
	case "user":
		return e.user, nil
	case "write":
		return sl.NewBuiltin("write", e.stream.sl_write), nil
	case "write_from_file":
		return sl.NewBuiltin("write_from_file", e.stream.sl_write_from_file), nil
	default:
		return nil, nil
	}
}

func (e *Event) Freeze() {}

func (e *Event) Hash() (uint32, error) {
	return sl.String(e.id).Hash()
}

func (e *Event) String() string {
	return fmt.Sprintf("Event %d", e.id)
}

func (e *Event) Truth() sl.Bool {
	return sl.True
}

func (e *Event) Type() string {
	return "Event"
}

// Get a content field
func (e *Event) sl_content(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <field: string>, [default: string]")
	}

	field, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid field '%s'", field)
	}

	value, found := e.content[field]
	if found {
		return sl_encode(value), nil
	}

	if len(args) > 1 {
		return args[1], nil
	}

	return sl_encode(""), nil
}

// Dump the event contents
func (e *Event) sl_dump(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl_encode(map[string]any{"from": e.from, "to": e.to, "service": e.service, "event": e.event, "content": e.content}), nil
}

// Get a header
func (e *Event) sl_header(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <header: string>")
	}

	header, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid header '%s'", header)
	}

	switch header {
	case "from":
		return sl_encode(e.from), nil
	case "to":
		return sl_encode(e.to), nil
	case "service":
		return sl_encode(e.service), nil
	case "event":
		return sl_encode(e.event), nil
	default:
		return sl_error(fn, "invalid header '%s'", header)
	}
}
