// Mochi server: Messages
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"slices"
)

type Message struct {
	ID        string `cbor:-"`
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	Signature string `cbor:"signature,omitempty"`
	content   map[string]string
	data      []byte
	file      string
}

// Create a new message
func message(from string, to string, service string, event string) *Message {
	return &Message{ID: uid(), From: from, To: to, Service: service, Event: event, content: map[string]string{}}
}

// Add a CBOR segment to an outgoing message
func (m *Message) add(v any) *Message {
	m.data = append(m.data, cbor_encode(v)...)
	return m
}

// Publish an message to a pubsub
func (m *Message) publish(allow_queue bool) {
	debug("Message publishing: id '%s', from '%s', to '%s', service '%s', event '%s', content '%+v'", m.ID, m.From, m.To, m.Service, m.Event, m.content)
	m.Signature = entity_sign(m.From, m.From+m.To+m.Service+m.Event)
	data := cbor_encode(&m)
	data = append(data, cbor_encode(m.content)...)

	if peers_sufficient() {
		debug("Message sending via P2P pubsub")
		p2p_pubsub_messages_1.Publish(p2p_context, data)

	} else if allow_queue {
		debug("Message not enough peers to publish, adding to queue")
		db := db_open("db/queue.db")
		db.exec("replace into broadcasts ( id, data, created ) values ( ?, ?, ? )", m.ID, data, now())
	}
}

// Send a completed outgoing message
func (m *Message) send() {
	go m.send_work(entity_peer(m.To))
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
	debug("Message sending to peer '%s': id '%s', from '%s', to '%s', service '%s', event '%s', content '%#v', data %d bytes, file '%s'", peer, m.ID, m.From, m.To, m.Service, m.Event, m.content, len(m.data), m.file)

	ok := true
	s := peer_stream(peer)
	if s == nil {
		debug("Unable to open stream to peer")
		ok = false
	}

	m.Signature = entity_sign(m.From, m.From+m.To+m.Service+m.Event)
	headers := cbor_encode(m)
	if ok {
		ok = s.write(headers)
	}

	content := cbor_encode(m.content)
	if ok {
		ok = s.write(content)
	}

	if len(m.data) > 0 && ok {
		ok = s.write(m.data)
	}

	if m.file != "" && ok {
		ok = s.write_file(m.file)
	}

	if s != nil {
		s.close()
	}

	if !ok {
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
		if len(in) < 2 {
			return m
		}
		m.content[in[0]] = in[1]
		in = in[2:]
	}

	return m
}
