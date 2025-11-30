// Mochi server: Messages
// Copyright Alistair Cunningham 2024-2025

package main

type Message struct {
	ID        string `cbor:"-"`
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	Timestamp int64  `cbor:"timestamp,omitempty"`
	Nonce     string `cbor:"nonce,omitempty"`
	Signature string `cbor:"signature,omitempty"`
	content   map[string]string
	data      []byte
	file      string
	target    string // specific peer to send to (optional)
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

// Publish a message to a pubsub (queue-first for reliability)
func (m *Message) publish(allow_queue bool) {
	if m.ID == "" {
		m.ID = uid()
	}
	debug("Message publishing: id %q, from %q, to %q, service %q, event %q, content '%+v'", m.ID, m.From, m.To, m.Service, m.Event, m.content)

	// Encode content for queue storage
	content := cbor_encode(m.content)

	if allow_queue {
		// Queue first for reliability
		queue_add_broadcast(m.ID, m.From, m.To, m.Service, m.Event, content, m.data)
	}

	// Attempt immediate send if we have enough peers
	if peers_sufficient() {
		timestamp := now()
		nonce := uid()
		m.Timestamp = timestamp
		m.Nonce = nonce
		m.Signature = entity_sign(m.From, string(signable_headers("msg", m.From, m.To, m.Service, m.Event, timestamp, nonce)))
		data := cbor_encode(m)
		data = append(data, content...)
		if len(m.data) > 0 {
			data = append(data, m.data...)
		}

		debug("Message sending via P2P pubsub")
		p2p_pubsub_1.Publish(p2p_context, data)

		if allow_queue {
			// Remove from queue on successful immediate send (broadcasts don't need ACK)
			queue_ack(m.ID)
		}
	}
}

// Send a completed outgoing message
func (m *Message) send() {
	m.target = ""
	go m.send_work()
}

// Send a completed outgoing message to a specified peer
func (m *Message) send_peer(peer string) {
	m.target = peer
	go m.send_work()
}

// Do the work of sending (queue-first for reliability)
func (m *Message) send_work() {
	if m.ID == "" {
		m.ID = uid()
	}

	// Determine peer
	peer := m.target
	if peer == "" {
		peer = entity_peer(m.To)
	}

	debug("Message sending to peer %q: id %q, from %q, to %q, service %q, event %q, content '%#v', data %d bytes, file %q", peer, m.ID, m.From, m.To, m.Service, m.Event, m.content, len(m.data), m.file)

	// Queue first for reliability
	content := cbor_encode(m.content)
	queue_add_direct(m.ID, m.target, m.From, m.To, m.Service, m.Event, content, m.data, m.file)

	// Attempt immediate send
	if peer == "" {
		debug("Message unable to determine peer, will retry from queue")
		return
	}

	s := peer_stream(peer)
	if s == nil {
		debug("Unable to open stream to peer, will retry from queue")
		return
	}

	timestamp := now()
	nonce := uid()
	m.Timestamp = timestamp
	m.Nonce = nonce
	m.Signature = entity_sign(m.From, string(signable_headers("msg", m.From, m.To, m.Service, m.Event, timestamp, nonce)))

	headers := cbor_encode(Headers{
		Type: "msg", From: m.From, To: m.To, Service: m.Service, Event: m.Event,
		Timestamp: timestamp, Nonce: m.ID, Signature: m.Signature,
	})

	ok := s.write_raw(headers) == nil
	if ok {
		ok = s.write_raw(content) == nil
	}
	if len(m.data) > 0 && ok {
		ok = s.write_raw(m.data) == nil
	}
	if m.file != "" && ok {
		ok = s.write_file(m.file) == nil
	}

	if s.writer != nil {
		s.writer.Close()
	}

	if !ok {
		peer_disconnected(peer)
		debug("Message send failed, will retry from queue")
		queue_fail(m.ID, "send failed")
	} else {
		debug("Message sent, awaiting ACK")
		// Message stays in queue with 'sent' status until ACK received
	}
}

// Send an ACK or NACK response for a received message
func send_ack(ack_type string, ack_nonce string, from string, to string, peer string) {
	s := peer_stream(peer)
	if s == nil {
		debug("Unable to send %s: no stream to peer %q", ack_type, peer)
		return
	}

	timestamp := now()
	nonce := uid()
	signature := entity_sign(from, string(signable_headers(ack_type, from, to, "", "", timestamp, nonce)))

	headers := cbor_encode(Headers{
		Type: ack_type, From: from, To: to, AckNonce: ack_nonce,
		Timestamp: timestamp, Nonce: nonce, Signature: signature,
	})

	if s.write_raw(headers) == nil {
		debug("Sent %s for nonce %q", ack_type, ack_nonce)
	}

	if s.writer != nil {
		s.writer.Close()
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
