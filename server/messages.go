// Mochi server: Messages
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"fmt"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"sync"
)

// Deduplication cache for processed messages.
//
// TTL invariant (claude/plans/protocol2.md → Failure recovery →
// Ack loss): seen_messages_ttl MUST be ≥ 2× the longest gap in
// retry_delays. queue.go caps retries at 3600s (1h), so the dedup
// window has to outlive 2× that = 7200s. We pick 8h for the safety
// margin — a late retry under chained ack-loss + sender-restart can
// arrive several retry cycles after the original apply, and a 1h
// TTL was already on the edge. Memory cost is bounded by the cache
// cleanup that runs hourly; at typical traffic this is single-digit
// MB. The relation is enforced by TestDedupWindowExceedsMaxRetryInterval.
var (
	seen_messages      = make(map[string]int64) // id -> timestamp
	seen_messages_lock sync.Mutex
	seen_messages_ttl  = int64(8 * 3600) // 8 hours
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
	"send":    &message_send_module{},
	"publish": sl.NewBuiltin("mochi.message.publish", api_message_publish),
})

// message_send_module is a callable module that also has a .peer method
// Usage: mochi.message.send(headers, content) or mochi.message.send.peer(peer, headers, content)
type message_send_module struct{}

func (m *message_send_module) String() string        { return "mochi.message.send" }
func (m *message_send_module) Type() string          { return "module" }
func (m *message_send_module) Freeze()               {}
func (m *message_send_module) Truth() sl.Bool        { return sl.True }
func (m *message_send_module) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }
func (m *message_send_module) AttrNames() []string   { return []string{"peer"} }
func (m *message_send_module) Name() string          { return "mochi.message.send" }

func (m *message_send_module) Attr(name string) (sl.Value, error) {
	if name == "peer" {
		return sl.NewBuiltin("mochi.message.send.peer", api_message_send_peer), nil
	}
	return nil, nil
}

func (m *message_send_module) CallInternal(thread *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
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
		signature := entity_sign(m.From, string(signable_headers("msg", m.From, m.To, m.Service, m.Event, m.FromApp, m.ID, "", "", m.Services, nil)))
		headers := cbor_encode(Headers{
			Type: "msg", From: m.From, To: m.To, Service: m.Service, Event: m.Event,
			FromApp: m.FromApp, Services: m.Services, ID: m.ID, Signature: signature,
		})
		data := headers
		data = append(data, content...)
		if len(m.data) > 0 {
			data = append(data, m.data...)
		}

		//debug("Message sending via Net pubsub")
		net_pubsub_1.Publish(net_context, data)

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

// send_peer_priority is send_peer with an explicit queue priority
// override. Used by broadcast_resync to ship replay messages in the
// priority_replay lane so they overtake live broadcast traffic in
// wasabi's outbound queue. See task #96.
func (m *Message) send_peer_priority(peer string, priority int) {
	m.target = peer
	if m.ID == "" {
		m.ID = uid()
	}
	content := cbor_encode(m.content)
	queue_add_direct_priority(m.ID, m.target, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires, priority)
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
	// File pushes still go through the legacy slow path (one libp2p
	// stream per file; bytes don't multiplex through /mochi/2/messages
	// — too much HOL blocking). All other messages prefer /mochi/2;
	// peer_send marks the queue row 'sending' and the inflight
	// resolver (sender_read) will queue_ack/queue_fail.
	if m.file == "" && protocol_known_get(peer, protocol_messages) != protocol_state_unsupported {
		f, err := frame_for_message(m, content)
		if err == nil {
			queue_sending(m.ID)
			send_err := peer_send(peer, m.ID, f)
			if send_err == nil {
				return
			}
			queue_unsending(m.ID)
			if !is_v2_unsupported(send_err) {
				queue_fail(m.ID, fmt.Sprintf("peer_send: %v", send_err))
				return
			}
			// fall through to legacy
		}
	}

	// Legacy /mochi/1 slow path. Mark as sending to prevent other
	// queue processors from picking it up.
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

	signature := entity_sign(m.From, string(signable_headers("msg", m.From, m.To, m.Service, m.Event, m.FromApp, m.ID, "", "", m.Services, challenge)))

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
		debug("Message %q received NACK reason=%q", m.ID, h.Reason)
		// Reason-aware: drop on hints that say "retrying won't
		// help" (broadcast gap, malformed payload). Default still
		// goes to queue_fail with retry/backoff.
		if nack_should_drop(h.Reason) {
			queue_drop(m.ID, h.Reason)
		} else {
			queue_fail(m.ID, "NACK received")
		}
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

// mochi.message.send(headers, content?, data?, expires=seconds) -> None: Send a Net message
func api_message_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return sl_error(fn, "syntax: <headers: dictionary>, [content: dictionary], [data: bytes]")
	}

	// Rate limit by app ID
	app, _ := t.Local("app").(*App)
	if app != nil && !rate_limit_net_send.allow(app.id) {
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

// mochi.message.send.peer(peer, headers, content?, data?, expires=seconds) -> None: Send a Net message to a specific peer
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
	if app != nil && !rate_limit_net_send.allow(app.id) {
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
