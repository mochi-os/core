// Mochi server: Events
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
	cbor "github.com/fxamacker/cbor/v2"
	"io"
	"os"
	"slices"
)

type Event struct {
	ID          string `cbor:"id,omitempty"`
	From        string `cbor:"from,omitempty"`
	To          string `cbor:"to,omitempty"`
	Service     string `cbor:"service,omitempty"`
	Action      string `cbor:"action,omitempty"`
	content     map[string]string
	data        []byte
	file        string
	p2p_peer    string
	p2p_address string
	user        *User
	db          *DB
	reader      io.Reader
	decoder     *cbor.Decoder
}

// Create a new event
func event(from string, to string, service string, action string) *Event {
	return &Event{ID: uid(), From: from, To: to, Service: service, Action: action, content: map[string]string{}}
}

// Receive event from reader
func event_receive_reader(r io.Reader, peer string, address string) {
	d := cbor.NewDecoder(r)

	// Get and verify event headers
	var e Event
	err := d.Decode(&e)
	if err != nil {
		info("Dropping event with bad headers: %v", err)
		return
	}

	debug("Received event from peer '%s', from='%s', to='%s', service='%s', action='%s'", peer, e.From, e.To, e.Service, e.Action)
	e.decoder = d
	e.reader = r
	e.p2p_peer = peer
	e.p2p_address = address

	if !valid(e.ID, "id") {
		info("Dropping event with invalid id '%s'", e.ID)
		return
	}

	if e.From != "" && !valid(e.From, "entity") {
		info("Dropping event '%s' with invalid from '%s'", e.ID, e.From)
		return
	}

	if e.To != "" && !valid(e.To, "entity") {
		info("Dropping event '%s' with invalid to '%s'", e.ID, e.To)
		return
	}

	if e.Service != "" && !valid(e.Service, "constant") {
		info("Dropping event '%s' with invalid service '%s'", e.ID, e.Service)
		return
	}

	if !valid(e.Action, "constant") {
		info("Dropping event '%s' with invalid action '%s'", e.ID, e.Action)
		return
	}

	// Get and verify headers signature
	var signature string
	err = d.Decode(&signature)
	if err != nil {
		info("Dropping event '%s' with invalid signature: %v", e.ID, err)
		return
	}
	if e.From != "" {
		public := base58_decode(e.From, "")
		if len(public) != ed25519.PublicKeySize {
			info("Dropping event '%s' with invalid from length %d!=%d", e.ID, len(public), ed25519.PublicKeySize)
			return
		}
		if !ed25519.Verify(public, []byte(e.ID+e.From+e.To+e.Service+e.Action), base58_decode(signature, "")) {
			info("Dropping event '%s' with incorrect signature", e.ID)
			return
		}
	}
	debug("Received event signature '%s'", signature)

	// Decode the content segment
	err = d.Decode(&e.content)
	if err != nil {
		info("Dropping event with bad content segment: %v", err)
		return
	}
	debug("Received event content segment: %#v", e.content)

	// Route the event to app
	e.route()
}

// Add a CBOR segment to an outgoing event
func (e *Event) add(v any) {
	e.data = append(e.data, cbor_encode(v)...)
}

// Decode the next segment from a received event
func (e *Event) decode(v any) bool {
	err := e.decoder.Decode(v)
	if err != nil {
		info("Event '%s' unable to decode segment: %v", e.ID, err)
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

// Publish an event to a pubsub
func (e *Event) publish(allow_queue bool) {
	debug("Publishing event, from='%s', to='%s', service='%s', action='%s'", e.From, e.To, e.Service, e.Action)
	data := cbor_encode(&e)
	data = append(data, cbor_encode(e.signature())...)
	data = append(data, cbor_encode(e.content)...)

	if peers_sufficient() {
		debug("Sending event via libp2p, length=%d", len(data))
		p2p_broadcast_events.Publish(p2p_context, data)

	} else if allow_queue {
		debug("Unable to send broadcast event, adding to queue")
		db := db_open("db/queue.db")
		db.exec("replace into broadcast ( id, data, created ) values ( ?, ?, ? )", e.ID, data, now())
	}
}

// Route a received event to the correct app
func (e *Event) route() {
	e.user = user_owning_entity(e.To)

	a := services[e.Service]
	if a == nil {
		info("Dropping event '%s' to unknown service '%s'", e.ID, e.Service)
		return
	}

	if a.db_file != "" {
		e.db = db_user(e.user, a.db_file, a.db_create)
		defer e.db.close()
	}

	var f func(*Event)
	var found bool
	// Look for app event matching action
	if e.To == "" {
		f, found = a.events_broadcast[e.Action]
	} else {
		f, found = a.events[e.Action]
	}
	if !found {
		// Look for app default event
		if e.To == "" {
			f, found = a.events_broadcast[""]
		} else {
			f, found = a.events[""]
		}
	}
	if !found {
		info("Dropping event '%s' to unknown action '%s' in app '%s' for service '%s'", e.ID, e.Action, a.name, e.Service)
		return
	}

	f(e)
}

// Send a completed outgoing event
func (e *Event) send() {
	if e.ID == "" {
		e.ID = uid()
	}

	peer := entity_peer(e.To)
	debug("Sending event to peer '%s', from='%s', to='%s', service='%s', action='%s'", peer, e.From, e.To, e.Service, e.Action)
	failed := false

	//TODO Test sending to local entity
	s := peer_stream(peer)
	if s == nil {
		debug("Unable to open stream to peer")
		failed = true
	}

	headers := cbor_encode(e)
	if !failed {
		_, err := s.Write(headers)
		if err != nil {
			debug("Error sending headers segment: %v", err)
			failed = true
		}
	}

	signature := cbor_encode(e.signature())
	if !failed {
		_, err := s.Write(signature)
		if err != nil {
			debug("Error sending signature segment: %v", err)
			failed = true
		}
	}

	content := cbor_encode(e.content)
	if !failed {
		_, err := s.Write(content)
		if err != nil {
			debug("Error sending content segment: %v", err)
			failed = true
		}
	}

	if len(e.data) > 0 && !failed {
		_, err := s.Write(e.data)
		if err != nil {
			debug("Error sending data segment: %v", err)
			failed = true
		}
	}

	if e.file != "" && !failed {
		debug("Sending file segment to peer: %s", e.file)
		f, err := os.Open(e.file)
		if err != nil {
			warn("Unable to read file '%s'", e.file)
			failed = true
		}
		defer f.Close()
		if !failed {
			n, err := io.Copy(s, f)
			if err != nil {
				debug("Error sending file segment: %v", err)
				failed = true
			}
			debug("Finished sending file segment, length %d", n)
		}
	}

	if s != nil {
		s.Close()
	}

	if !failed {
		debug("Finished sending event to peer")
		return
	}

	//TODO Mark peer as disconnected?
	debug("Unable to send event to '%s', adding to queue", e.To)
	data := slices.Concat(headers, signature, content, e.data)
	db := db_open("db/queue.db")
	if peer == "" {
		db.exec("replace into entities ( id, entity, data, file, created ) values ( ?, ?, ?, ?, ? )", e.ID, e.To, data, e.file, now())
	} else {
		db.exec("replace into peers ( id, peer, data, file, created ) values ( ?, ?, ?, ?, ? )", e.ID, peer, data, e.file, now())
	}
}

// Set the content segment of an outgoing event
func (e *Event) set(in ...string) {
	for {
		e.content[in[0]] = in[1]
		in = in[2:]
		if len(in) < 2 {
			return
		}
	}
}

// Get the signature of an event's headers
func (e *Event) signature() string {
	if e.From == "" {
		return ""
	}

	if e.ID == "" {
		panic("Event did not specify ID")
	}

	db := db_open("db/users.db")
	var from Entity
	if !db.scan(&from, "select private from entities where id=?", e.From) {
		warn("Not signing event due unknown sending entity")
		return ""
	}
	private := base58_decode(from.Private, "")
	if string(private) == "" {
		warn("Not signing event due to invalid private key")
		return ""
	}
	return base58_encode(ed25519.Sign(private, []byte(e.ID+e.From+e.To+e.Service+e.Action)))
}
