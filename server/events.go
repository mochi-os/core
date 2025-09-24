// Mochi server: Events
// Copyright Alistair Cunningham 2024-2025

package main

import (
	cbor "github.com/fxamacker/cbor/v2"
	"io"
	rd "runtime/debug"
)

type Event struct {
	id      string
	from    string
	to      string
	service string
	event   string
	content map[string]string
	peer    string
	user    *User
	db      *DB
	reader  io.Reader
	decoder *cbor.Decoder
}

// Decode the next segment from a received event
func (e *Event) segment(v any) bool {
	err := e.decoder.Decode(v)
	if err != nil {
		info("Event '%s' unable to decode segment: %v", e.id, err)
		return false
	}
	return true
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
// TODO Lock app for user
func (e *Event) route() {
	if e.to != "" {
		e.user = user_owning_entity(e.to)
		if e.user == nil {
			info("Event dropping '%s' to unknown user '%s'", e.id, e.to)
			return
		}
	}

	// Find which app provides this service
	a := services[e.service]
	if a == nil {
		info("Event dropping '%s' to unknown service '%s'", e.id, e.service)
		return
	}

	// Load a database file for the app
	if a.Database.File != "" {
		if e.user == nil {
			info("Event dropping broadcast '%s' for service requiring user", e.id)
			return
		}
		e.db = db_app(e.user, a)
		if e.db == nil {
			info("Event app '%s' has no database file", a.id)
			return
		}
		defer e.db.close()
	}

	// Check which engine the app uses, and run it
	switch a.Engine.Architecture {
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
			info("Event dropping '%s' to unknown event '%s' in app '%s' for service '%s'", e.id, e.event, a.Name, e.service)
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
		ev, found := a.Services[e.service].Events[e.event]
		if !found {
			ev, found = a.Services[e.service].Events[""]
		}
		if !found {
			info("Event dropping '%s' to unknown event '%s' in app '%s' for service '%s'", e.id, e.event, a.Name, e.service)
			return
		}

		if e.to == "" && !ev.Broadcast {
			info("Event dropping broadcast '%s' to non-broadcast", e.id)
			return
		}

		if e.to != "" && ev.Broadcast {
			info("Event dropping non-broadcast '%s' to broadcast", e.id)
			return
		}

		s := a.starlark()
		s.set("event", e)
		s.set("app", a)
		s.set("user", e.user)
		s.set("owner", e.user)

		headers := map[string]string{
			"id":      e.id,
			"from":    e.from,
			"to":      e.to,
			"service": e.service,
			"event":   e.event,
		}

		s.call(ev.Function, starlark_encode_tuple(headers, e.content))

	default:
		info("Event unknown engine '%s' version '%s'", a.Engine.Architecture, a.Engine.Version)
		return
	}
}
