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

// message_seen_mark atomically reports whether id was already processed
// and, if not, marks it seen — both under one lock. The separate
// message_seen / message_mark_seen pair has a check-then-mark gap that two
// concurrent receivers can both slip through; pubsub's two topic managers
// (mochi/1 + /mochi/2) receive the same dual-published message in parallel
// during the migration, so they need this atomic coalescing. Returns true
// when id was already seen (caller drops the duplicate).
func message_seen_mark(id string) bool {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	if _, exists := seen_messages[id]; exists {
		return true
	}
	seen_messages[id] = now()
	return false
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
	"send": &message_send_module{},
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

	content := cbor_encode(m.content)

	if allow_queue {
		queue_add_broadcast(m.ID, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.expires)
	}

	if peers_sufficient() {
		pubsub_publish(m.From, m.To, m.Service, m.Event, m.ID, content, m.data)

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
	if message_self_loop_dispatch(m, content) {
		return
	}
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
	if message_self_loop_dispatch(m, content) {
		return
	}
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
		if message_self_loop_dispatch(m, content) {
			return
		}
		queue_add_direct(m.ID, m.target, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
		if m.target == net_id {
			// Self-loop dispatch fell back to queue.db (no worker yet,
			// or inbox full). Don't inline-attempt: message_attempt_send
			// → peer_send(net_id) → net_me.NewStream(self) self-dials
			// and fails. self_loop_drain owns target==net_id rows; nudge
			// it so the row drains on the next tick rather than waiting
			// out the heartbeat.
			queue_wake()
			return
		}
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

	// Multi-host fan-out: one peer gets the primary inline send (using
	// m.ID), the rest queue with fresh uids. Self-loop peers (peer ==
	// net_id) divert through the direct-dispatch path that skips
	// queue.db entirely; they fall back to queue_add_direct only when
	// the worker inbox is full.
	//
	// m.ID is held back for the first peer that actually lands a row
	// in queue.db, because message_attempt_send below uses m.ID to
	// drive queue_sending / peer_send / queue_ack. Direct-dispatched
	// rows get fresh uids — they're consumed by the worker and never
	// touch queue.db, so giving them m.ID would orphan the inline
	// send (no row for it to ack against, receiver gets a duplicate).
	primary_peer := ""
	self_queued := false
	for _, peer := range peers {
		if peer == net_id {
			tmp := *m
			tmp.ID = uid()
			tmp.target = peer
			if message_self_loop_dispatch(&tmp, content) {
				continue
			}
			// Dispatch fell back to queue.db. Queue a self-loop row for
			// self_loop_drain, but never make net_id the inline primary
			// — message_attempt_send(net_id) would self-dial and fail.
			queue_add_direct(uid(), peer, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
			self_queued = true
			continue
		}
		id := m.ID
		if primary_peer != "" {
			id = uid()
		}
		queue_add_direct(id, peer, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
		if primary_peer == "" {
			primary_peer = peer
		}
	}
	// Try to send the primary (non-self) row immediately; the rest ride
	// the queue tick. Avoids fanning N goroutines from a single send.
	if primary_peer != "" {
		message_attempt_send(m, primary_peer, content)
	}
	// Nudge self_loop_drain if a self-loop row was queued as fallback.
	if self_queued {
		queue_wake()
	}
}

// message_attempt_send is the inline send-now path extracted from
// send_work for the single-peer case. Splits naturally now that
// send_work loops over peers. Package-level var so unit tests can
// stub it out — the real implementation reaches into the libp2p
// infrastructure which isn't set up under in-process tests.
var message_attempt_send = message_attempt_send_real

func message_attempt_send_real(m *Message, peer string, content []byte) {
	// /mochi/2/messages: peer_send marks the queue row 'sending' and the
	// inflight resolver (sender_read) drives queue_ack / queue_fail. On a
	// stream-open failure the row stays queued for queue_process to retry.
	f, err := frame_for_message(m, content)
	if err != nil {
		queue_fail(m.ID, fmt.Sprintf("frame build: %v", err))
		return
	}
	queue_sending(m.ID)
	if send_err := peer_send(peer, m.ID, f); send_err != nil {
		queue_unsending(m.ID)
		queue_fail(m.ID, fmt.Sprintf("peer_send: %v", send_err))
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
