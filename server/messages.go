// Mochi server: Messages
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/ed25519"
	cbor "github.com/fxamacker/cbor/v2"
	"io"
	"os"
	"slices"
)

type Message struct {
	ID        string `cbor:"id,omitempty"`
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Action    string `cbor:"action,omitempty"`
	Signature string `cbor:"signature,omitempty"`
	content   map[string]string
	data      []byte
	file      string
}

// Create a new message
func message(from string, to string, service string, action string) *Message {
	return &Message{ID: uid(), From: from, To: to, Service: service, Action: action, content: map[string]string{}}
}

// Receive message from reader
func message_receive(r io.Reader, version int, peer string, address string) {
	d := cbor.NewDecoder(r)

	// Get and verify message headers
	var m Message
	err := d.Decode(&m)
	if err != nil {
		info("Dropping message with bad headers: %v", err)
		return
	}

	debug("Message received from peer '%s', id '%s', from '%s', to '%s', service '%s', action '%s'", peer, m.ID, m.From, m.To, m.Service, m.Action)

	if !valid(m.ID, "id") {
		info("Dropping message with invalid id '%s'", m.ID)
		return
	}

	if m.From != "" && !valid(m.From, "entity") {
		info("Dropping message '%s' with invalid from '%s'", m.ID, m.From)
		return
	}

	if m.To != "" && m.To != p2p_id && !valid(m.To, "entity") {
		info("Dropping message '%s' with invalid to '%s'", m.ID, m.To)
		return
	}

	if m.Service != "" && !valid(m.Service, "constant") {
		info("Dropping message '%s' with invalid service '%s'", m.ID, m.Service)
		return
	}

	if !valid(m.Action, "constant") {
		info("Dropping message '%s' with invalid action '%s'", m.ID, m.Action)
		return
	}

	if m.From != "" {
		public := base58_decode(m.From, "")
		if len(public) != ed25519.PublicKeySize {
			info("Dropping message '%s' with invalid from length %d!=%d", m.ID, len(public), ed25519.PublicKeySize)
			return
		}
		if !ed25519.Verify(public, []byte(m.ID+m.From+m.To+m.Service+m.Action), base58_decode(m.Signature, "")) {
			info("Dropping message '%s' with incorrect signature", m.ID)
			return
		}
	}

	// Decode the content segment
	err = d.Decode(&m.content)
	if err != nil {
		info("Dropping message with bad content segment: %v", err)
		return
	}

	// Create event, and route to app
	e := Event{id: m.ID, from: m.From, to: m.To, service: m.Service, action: m.Action, content: m.content, decoder: d, reader: r, p2p_peer: peer, p2p_address: address}
	e.route()
}

// Add a CBOR segment to an outgoing message
func (m *Message) add(v any) *Message {
	m.data = append(m.data, cbor_encode(v)...)
	return m
}

// Publish an message to a pubsub
func (m *Message) publish(allow_queue bool) {
	debug("Message publishing, from='%s', to='%s', service='%s', action='%s'", m.From, m.To, m.Service, m.Action)
	m.Signature = m.signature()
	data := cbor_encode(&m)
	data = append(data, cbor_encode(m.content)...)

	if peers_sufficient() {
		p2p_pubsub_messages_1.Publish(p2p_context, data)

	} else if allow_queue {
		debug("Unable to send broadcast message, adding to queue")
		db := db_open("db/queue.db")
		db.exec("replace into broadcasts ( id, data, created ) values ( ?, ?, ? )", m.ID, data, now())
	}
}

// Send a completed outgoing message
func (m *Message) send() {
	peer := entity_peer(m.To)
	go m.send_work(peer)
}

// Send a completed outgoing message to a specified peer
func (m *Message) send_peer(peer string) {
	go m.send_work(peer)
}

// Do the work of sending
func (m *Message) send_work(peer string) {
	if m.ID == "" {
		m.ID = uid()
	}
	debug("Message sending to peer '%s', id '%s', from '%s', to '%s', service '%s', action '%s'", peer, m.ID, m.From, m.To, m.Service, m.Action)

	failed := false
	m.Signature = m.signature()

	w := peer_writer(peer)
	if w == nil {
		debug("Unable to open peer for writing")
		failed = true
	}

	headers := cbor_encode(m)
	if !failed {
		_, err := w.Write(headers)
		if err != nil {
			debug("Error sending headers segment: %v", err)
			failed = true
		}
	}

	content := cbor_encode(m.content)
	if !failed {
		_, err := w.Write(content)
		if err != nil {
			debug("Error sending content segment: %v", err)
			failed = true
		}
	}

	if len(m.data) > 0 && !failed {
		_, err := w.Write(m.data)
		if err != nil {
			debug("Error sending data segment: %v", err)
			failed = true
		}
	}

	if m.file != "" && !failed {
		f, err := os.Open(m.file)
		if err != nil {
			warn("Unable to read file '%s'", m.file)
			failed = true
		}
		defer f.Close()
		if !failed {
			_, err := io.Copy(w, f)
			if err != nil {
				debug("Error sending file segment: %v", err)
				failed = true
			}
		}
	}

	if w != nil {
		w.Close()
	}

	if failed {
		peer_disconnected(peer)

		debug("Message unable to send to '%s', adding to queue", m.To)
		data := slices.Concat(headers, content, m.data)
		db := db_open("db/queue.db")
		if peer == "" {
			db.exec("replace into entities ( id, entity, data, file, created ) values ( ?, ?, ?, ?, ? )", m.ID, m.To, data, m.file, now())
		} else {
			db.exec("replace into peers ( id, peer, data, file, created ) values ( ?, ?, ?, ?, ? )", m.ID, peer, data, m.file, now())
		}
	}
}

// Set the content segment of an outgoing message
func (m *Message) set(in ...string) *Message {
	for {
		m.content[in[0]] = in[1]
		in = in[2:]
		if len(in) < 2 {
			return m
		}
	}
	return m
}

// Get the signature of an message's headers
func (m *Message) signature() string {
	if m.From == "" {
		return ""
	}

	db := db_open("db/users.db")
	var from Entity
	if !db.scan(&from, "select private from entities where id=?", m.From) {
		warn("Not signing message due unknown sending entity")
		return ""
	}

	private := base58_decode(from.Private, "")
	if string(private) == "" {
		warn("Not signing message due to invalid private key")
		return ""
	}

	return base58_encode(ed25519.Sign(private, []byte(m.ID+m.From+m.To+m.Service+m.Action)))
}
