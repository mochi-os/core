// Mochi server: Messages
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"sync"
)

// Deduplication cache for processed messages
var (
	seen_messages      = make(map[string]int64) // id -> timestamp
	seen_messages_lock sync.Mutex
	seen_messages_ttl  = int64(3600) // 1 hour
)

// Check if message was already processed
func message_seen(id string) bool {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	_, exists := seen_messages[id]
	return exists
}

// Mark message as processed
func message_mark_seen(id string) {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	seen_messages[id] = now()
}

// Clean up old entries
func message_seen_cleanup() {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	cutoff := now() - seen_messages_ttl
	for id, ts := range seen_messages {
		if ts < cutoff {
			delete(seen_messages, id)
		}
	}
}

var api_message = sls.FromStringDict(sl.String("mochi.message"), sl.StringDict{
	"send":    &messageSendModule{},
	"publish": sl.NewBuiltin("mochi.message.publish", api_message_publish),
})

// messageSendModule is a callable module that also has a .peer method
// Usage: mochi.message.send(headers, content) or mochi.message.send.peer(peer, headers, content)
type messageSendModule struct{}

func (m *messageSendModule) String() string        { return "mochi.message.send" }
func (m *messageSendModule) Type() string          { return "module" }
func (m *messageSendModule) Freeze()               {}
func (m *messageSendModule) Truth() sl.Bool        { return sl.True }
func (m *messageSendModule) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }
func (m *messageSendModule) AttrNames() []string   { return []string{"peer"} }
func (m *messageSendModule) Name() string          { return "mochi.message.send" }

func (m *messageSendModule) Attr(name string) (sl.Value, error) {
	if name == "peer" {
		return sl.NewBuiltin("mochi.message.send.peer", api_message_send_peer), nil
	}
	return nil, nil
}

func (m *messageSendModule) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return api_message_send(thread, nil, args, kwargs)
}

type Message struct {
	ID        string `cbor:"-"`
	From      string `cbor:"from,omitempty"`
	To        string `cbor:"to,omitempty"`
	Service   string `cbor:"service,omitempty"`
	Event     string `cbor:"event,omitempty"`
	App       string `cbor:"app,omitempty"`
	Signature string `cbor:"signature,omitempty"`
	content   map[string]any
	data      []byte
	file      string
	target    string // specific peer to send to (optional)
	expires   int64  // expiry timestamp (0 = no expiry)
}

// Create a new message
func message(from string, to string, service string, event string) *Message {
	return &Message{ID: uid(), From: from, To: to, Service: service, Event: event, content: map[string]any{}}
}

// Add a CBOR segment to an outgoing message
func (m *Message) add(v any) *Message {
	m.data = append(m.data, cbor_encode(v)...)
	return m
}

// Publish a message to pubsub (broadcasts - no challenge, untrusted)
func (m *Message) publish(allow_queue bool) {
	if m.ID == "" {
		m.ID = uid()
	}
	//debug("Message publishing: id %q, from %q, to %q, service %q, event %q, content '%+v'", m.ID, m.From, m.To, m.Service, m.Event, m.content)

	content := cbor_encode(m.content)

	if allow_queue {
		queue_add_broadcast(m.ID, m.From, m.To, m.Service, m.Event, m.App, content, m.data, m.expires)
	}

	if peers_sufficient() {
		// Broadcasts: sign without challenge (untrusted anyway)
		signature := entity_sign(m.From, string(signable_headers("msg", m.From, m.To, m.Service, m.Event, m.App, m.ID, "", nil)))
		headers := cbor_encode(Headers{
			Type: "msg", From: m.From, To: m.To, Service: m.Service, Event: m.Event,
			App: m.App, ID: m.ID, Signature: signature,
		})
		data := headers
		data = append(data, content...)
		if len(m.data) > 0 {
			data = append(data, m.data...)
		}

		//debug("Message sending via P2P pubsub")
		p2p_pubsub_1.Publish(p2p_context, data)

		if allow_queue {
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

// Do the work of sending (queue-first, read challenge before sending, wait for ACK)
func (m *Message) send_work() {
	if m.ID == "" {
		m.ID = uid()
	}

	peer := m.target
	if peer == "" {
		peer = entity_peer(m.To)
	}

	//debug("Message sending to peer %q: id %q, from %q, to %q, service %q, event %q", peer, m.ID, m.From, m.To, m.Service, m.Event)

	content := cbor_encode(m.content)
	queue_add_direct(m.ID, m.target, m.From, m.To, m.Service, m.Event, m.App, content, m.data, m.file, m.expires)

	if peer == "" {
		debug("Message unable to determine peer, will retry from queue")
		return
	}

	// Mark as sending to prevent other queue processors from picking it up
	queue_sending(m.ID)

	s := peer_stream(peer)
	if s == nil {
		debug("Unable to open stream to peer, will retry from queue")
		queue_fail(m.ID, "unable to open stream")
		return
	}
	defer s.close()

	// Read challenge from receiver
	challenge, err := s.read_challenge()
	if err != nil {
		debug("Unable to read challenge: %v, will retry from queue", err)
		queue_fail(m.ID, "challenge read failed")
		return
	}

	signature := entity_sign(m.From, string(signable_headers("msg", m.From, m.To, m.Service, m.Event, m.App, m.ID, "", challenge)))

	headers := cbor_encode(Headers{
		Type: "msg", From: m.From, To: m.To, Service: m.Service, Event: m.Event,
		App: m.App, ID: m.ID, Signature: signature,
	})

	// Batch headers + content + data into single write
	data := headers
	data = append(data, content...)
	if len(m.data) > 0 {
		data = append(data, m.data...)
	}

	ok := s.write_raw(data) == nil
	if m.file != "" && ok {
		_, err := s.write_file(m.file)
		ok = err == nil
	}

	// Close write direction to signal we're done sending (keeps read open for ACK)
	s.close_write()

	if !ok {
		peer_disconnected(peer)
		debug("Message send failed, will retry from queue")
		queue_fail(m.ID, "send failed")
		return
	}

	// Read ACK from stream
	var h Headers
	if s.read_headers(&h) != nil {
		debug("Message %q failed to read ACK, will retry from queue", m.ID)
		queue_fail(m.ID, "ACK read failed")
		return
	}

	if h.msg_type() == "ack" && h.AckID == m.ID {
		debug("Message %q received ACK", m.ID)
		queue_ack(m.ID)
		return
	}

	if h.msg_type() == "nack" && h.AckID == m.ID {
		debug("Message %q received NACK", m.ID)
		queue_fail(m.ID, "NACK received")
		return
	}

	debug("Message %q received unexpected response type=%q ack=%q", m.ID, h.msg_type(), h.AckID)
	queue_fail(m.ID, "unexpected response")
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
}

// mochi.message.send(headers, content?, data?, expires=seconds) -> None: Send a P2P message
func api_message_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return sl_error(fn, "syntax: <headers: dictionary>, [content: dictionary], [data: bytes]")
	}

	// Rate limit by app ID
	app, _ := t.Local("app").(*App)
	if app != nil && !rate_limit_p2p_send.allow(app.id) {
		return sl_error(fn, "rate limit exceeded (100 messages per second)")
	}

	headers := sl_decode_strings(args[0])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.ID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if !from_valid {
		info("message.send: invalid from header - from=%q user.ID=%d user.Identity=%v", headers["from"], user.ID, user.Identity)
		return sl_error(fn, "invalid from header")
	}

	if !valid(headers["to"], "entity") {
		return sl_error(fn, "invalid to header")
	}

	if !valid(headers["service"], "constant") {
		return sl_error(fn, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return sl_error(fn, "invalid event header")
	}

	// Sender-side service enforcement
	if app != nil && !app_handles_service(app, user, headers["service"]) {
		return sl_error(fn, "app is not the handler for service %q", headers["service"])
	}

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])

	if app != nil {
		m.App = app.id
	}

	if len(args) > 1 {
		if content, ok := sl_decode(args[1]).(map[string]any); ok {
			m.content = content
		}
	}

	if len(args) > 2 {
		m.add(sl_decode(args[2]))
	}

	// Parse expires kwarg (seconds from now)
	for _, kw := range kwargs {
		if string(kw[0].(sl.String)) == "expires" {
			if v, ok := kw[1].(sl.Int); ok {
				m.expires = now() + v.BigInt().Int64()
			}
		}
	}

	m.send()
	return sl.None, nil
}

// mochi.message.send.peer(peer, headers, content?, data?, expires=seconds) -> None: Send a P2P message to a specific peer
func api_message_send_peer(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 2 || len(args) > 4 {
		return sl_error(fn, "syntax: <peer: string>, <headers: dictionary>, [content: dictionary], [data: bytes]")
	}

	peer, ok := sl.AsString(args[0])
	if !ok || peer == "" {
		return sl_error(fn, "peer not specified or invalid")
	}

	// Rate limit by app ID
	app, _ := t.Local("app").(*App)
	if app != nil && !rate_limit_p2p_send.allow(app.id) {
		return sl_error(fn, "rate limit exceeded (100 messages per second)")
	}

	headers := sl_decode_strings(args[1])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.ID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if !from_valid {
		info("message.send.peer: invalid from header - from=%q user.ID=%d user.Identity=%v", headers["from"], user.ID, user.Identity)
		return sl_error(fn, "invalid from header")
	}

	if !valid(headers["to"], "entity") {
		return sl_error(fn, "invalid to header")
	}

	if !valid(headers["service"], "constant") {
		return sl_error(fn, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return sl_error(fn, "invalid event header")
	}

	// Sender-side service enforcement
	if app != nil && !app_handles_service(app, user, headers["service"]) {
		return sl_error(fn, "app is not the handler for service %q", headers["service"])
	}

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])

	if app != nil {
		m.App = app.id
	}

	if len(args) > 2 {
		if content, ok := sl_decode(args[2]).(map[string]any); ok {
			m.content = content
		}
	}

	if len(args) > 3 {
		m.add(sl_decode(args[3]))
	}

	// Parse expires kwarg (seconds from now)
	for _, kw := range kwargs {
		if string(kw[0].(sl.String)) == "expires" {
			if v, ok := kw[1].(sl.Int); ok {
				m.expires = now() + v.BigInt().Int64()
			}
		}
	}

	m.send_peer(peer)
	return sl.None, nil
}

// mochi.message.publish(headers, content?, expires=seconds) -> None: Publish a broadcast message
func api_message_publish(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <headers: dictionary>, [content: dictionary]")
	}

	// Rate limit outbound pubsub messages
	if !rate_limit_pubsub_out.allow("global") {
		return sl.None, nil
	}

	headers := sl_decode_strings(args[0])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Validate from entity belongs to user
	if headers["from"] != "" {
		db := db_open("db/users.db")
		from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.ID)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		if !from_valid {
			return sl_error(fn, "invalid from header")
		}
	}

	if !valid(headers["service"], "constant") {
		return sl_error(fn, "invalid service header")
	}

	if !valid(headers["event"], "constant") {
		return sl_error(fn, "invalid event header")
	}

	app, _ := t.Local("app").(*App)

	// Sender-side service enforcement
	if app != nil && !app_handles_service(app, user, headers["service"]) {
		return sl_error(fn, "app is not the handler for service %q", headers["service"])
	}

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])

	if app != nil {
		m.App = app.id
	}

	if len(args) > 1 {
		if content, ok := sl_decode(args[1]).(map[string]any); ok {
			m.content = content
		}
	}

	// Parse expires kwarg (seconds from now)
	for _, kw := range kwargs {
		if string(kw[0].(sl.String)) == "expires" {
			if v, ok := kw[1].(sl.Int); ok {
				m.expires = now() + v.BigInt().Int64()
			}
		}
	}

	m.publish(true)
	return sl.None, nil
}
