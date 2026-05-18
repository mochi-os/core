// Mochi server: Messages
// Copyright Alistair Cunningham 2024-2026

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
	ID        string   `cbor:"-"`
	From      string   `cbor:"from,omitempty"`
	To        string   `cbor:"to,omitempty"`
	Service   string   `cbor:"service,omitempty"`
	Event     string   `cbor:"event,omitempty"`
	FromApp   string   `cbor:"from-app,omitempty"`
	Services  []string `cbor:"from-services,omitempty"`
	Signature string   `cbor:"signature,omitempty"`
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
		queue_add_broadcast(m.ID, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.expires)
	}

	if peers_sufficient() {
		// Broadcasts: sign without challenge (untrusted anyway)
		signature := entity_sign(m.From, string(signable_headers("msg", m.From, m.To, m.Service, m.Event, m.FromApp, m.ID, "", m.Services, nil)))
		headers := cbor_encode(Headers{
			Type: "msg", From: m.From, To: m.To, Service: m.Service, Event: m.Event,
			FromApp: m.FromApp, Services: m.Services, ID: m.ID, Signature: signature,
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

// Send a completed outgoing message to a specified peer. Persists the
// message to queue.db and signals the queue manager to drain — does
// NOT spawn a goroutine to try the send immediately. The queue manager
// handles every outbound message serially within its single goroutine,
// so multiple send_peer() calls for the same peer can't race each
// other into N concurrent libp2p streams (which trip the receiver's
// per-peer rate limit and snowball into unbounded queue.db growth —
// observed live as instance 1's queue hitting the 1GB SQLite cap and
// panicking after ~1100 unack'd bootstrap-db-chunks accumulated).
//
// Latency: worst case is the queue tick interval (1 second). For
// interactive operations like an Approve click → join-approved emit,
// that's imperceptible. For high-volume operations like bulk bootstrap
// chunk delivery, the queue drains 50 entries per tick — sufficient
// throughput because the drain is serial through one peer's connection
// rather than fan-out from many goroutines.
//
// send_work is retained as the per-message wire send helper used by
// queue_process; no longer called from send_peer directly.
func (m *Message) send_peer(peer string) {
	m.target = peer
	if m.ID == "" {
		m.ID = uid()
	}
	content := cbor_encode(m.content)
	queue_add_direct(m.ID, m.target, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
	queue_wake()
}

// Do the work of sending (queue-first, read challenge before sending, wait for ACK)
//
// Multi-host fan-out: when the caller didn't pin a specific peer (the
// usual case for app-level `mochi.message.send`), look up every live
// peer hosting the recipient entity and queue one row per peer with its
// target set. Each replica receives the event directly from the source,
// rather than relying on the chosen-peer → pair-replication relay to
// fan it out internally. Resilient to one replica being briefly
// unreachable (the others still get the direct hit) and to stale
// directory entries pinning routing at a dead peer.
//
// `send_peer` (target already set) keeps single-row behaviour — it's
// the path for system-to-system / replication messages where the
// sender already picked which peer to talk to.
//
// When entity_peers returns nothing (entity unknown to local directory)
// the original single empty-target row is queued so the queue retry
// can attempt resolution again later via entity_peer.
func (m *Message) send_work() {
	if m.ID == "" {
		m.ID = uid()
	}
	content := cbor_encode(m.content)

	if m.target != "" {
		queue_add_direct(m.ID, m.target, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
		message_attempt_send(m, m.target, content)
		return
	}

	peers := entity_peers(m.To)
	if len(peers) == 0 {
		// Unknown entity — queue one row with empty target so the
		// retry loop can re-resolve later. Same as before.
		queue_add_direct(m.ID, "", m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
		return
	}

	for i, peer := range peers {
		// Each replica gets its own queue row + ID. The original m.ID
		// becomes the first row; additional peers get fresh ids.
		id := m.ID
		if i > 0 {
			id = uid()
		}
		queue_add_direct(id, peer, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
	}
	// Try to send the primary row immediately; additional peer rows ride
	// the queue tick. Avoids fanning N goroutines from a single send.
	message_attempt_send(m, peers[0], content)
}

// message_attempt_send is the inline send-now path extracted from
// send_work for the single-peer case. Splits naturally now that
// send_work loops over peers. Package-level var so unit tests can
// stub it out — the real implementation reaches into the libp2p
// infrastructure which isn't set up under in-process tests.
var message_attempt_send = message_attempt_send_real

func message_attempt_send_real(m *Message, peer string, content []byte) {
	// Mark as sending to prevent other queue processors from picking it up
	queue_sending(m.ID)

	s := peer_stream(peer)
	if s == nil {
		//debug("Unable to open stream to peer, will retry from queue")
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

	signature := entity_sign(m.From, string(signable_headers("msg", m.From, m.To, m.Service, m.Event, m.FromApp, m.ID, "", m.Services, challenge)))

	headers := cbor_encode(Headers{
		Type: "msg", From: m.From, To: m.To, Service: m.Service, Event: m.Event,
		FromApp: m.FromApp, Services: m.Services, ID: m.ID, Signature: signature,
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
		//debug("Message %q received ACK", m.ID)
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
		return sl_error(fn, "rate limit exceeded (1000 messages per second)")
	}

	headers := sl_decode_strings(args[0])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	// Use user context, falling back to owner (for public actions like webhooks)
	user, _ := t.Local("user").(*User)
	if user == nil {
		user, _ = t.Local("owner").(*User)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.UID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if !from_valid {
		if re, ok := t.Local("route_entity").(string); ok && re == headers["from"] {
			from_valid = true
		}
	}
	if !from_valid {
		info("message.send: invalid from header - from=%q user.UID=%d user.Identity=%v", headers["from"], user.UID, user.Identity)
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

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])

	if app != nil {
		m.FromApp = app.id
		m.Services = app_services(app, user)
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
		return sl_error(fn, "rate limit exceeded (1000 messages per second)")
	}

	headers := sl_decode_strings(args[1])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	user, _ := t.Local("user").(*User)
	if user == nil {
		user, _ = t.Local("owner").(*User)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}

	db := db_open("db/users.db")
	from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.UID)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	if !from_valid {
		if re, ok := t.Local("route_entity").(string); ok && re == headers["from"] {
			from_valid = true
		}
	}
	if !from_valid {
		info("message.send.peer: invalid from header - from=%q user.UID=%d user.Identity=%v", headers["from"], user.UID, user.Identity)
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

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])

	if app != nil {
		m.FromApp = app.id
		m.Services = app_services(app, user)
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

	user, _ := t.Local("user").(*User)
	if user == nil {
		user, _ = t.Local("owner").(*User)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}

	// Validate from entity belongs to user
	if headers["from"] != "" {
		db := db_open("db/users.db")
		from_valid, err := db.exists("select id from entities where id=? and user=?", headers["from"], user.UID)
		if err != nil {
			return sl_error(fn, "database error: %v", err)
		}
		if !from_valid {
			if re, ok := t.Local("route_entity").(string); ok && re == headers["from"] {
				from_valid = true
			}
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

	m := message(headers["from"], headers["to"], headers["service"], headers["event"])

	if app != nil {
		m.FromApp = app.id
		m.Services = app_services(app, user)
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
