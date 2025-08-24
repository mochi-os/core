// Mochi server: Events
// Copyright Alistair Cunningham 2024-2025

package main

import (
	cbor "github.com/fxamacker/cbor/v2"
	"io"
)

type Event struct {
	id          string
	from        string
	to          string
	service     string
	action      string
	content     map[string]string
	p2p_peer    string
	p2p_address string
	user        *User
	db          *DB
	reader      io.Reader
	decoder     *cbor.Decoder
}

// Decode the next segment from a received event
func (e *Event) decode(v any) bool {
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
func (e *Event) route() {
	e.user = user_owning_entity(e.to)

	a := services[e.service]
	if a == nil {
		info("Event dropping '%s' to unknown service '%s'", e.id, e.service)
		return
	}

	if a.db_file != "" {
		e.db = db_user(e.user, a.db_file, a.db_create)
		defer e.db.close()
	}

	var f func(*Event)
	var found bool
	// Look for app event matching action
	if e.to == "" {
		f, found = a.events_broadcast[e.action]
	} else {
		f, found = a.events[e.action]
	}
	if !found {
		// Look for app default event
		if e.to == "" {
			f, found = a.events_broadcast[""]
		} else {
			f, found = a.events[""]
		}
	}
	if !found {
		info("Event dropping '%s' to unknown action '%s' in app '%s' for service '%s'", e.id, e.action, a.name, e.service)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			warn("Event handler crashed with error: %v", r)
		}
	}()

	f(e)
}
