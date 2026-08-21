// Mochi server: Messages
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"slices"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"sync"
)

// Deduplication cache for processed messages.
//
// seen_messages_ttl MUST be at least 2x the longest gap in retry_delays
// (queue.go caps at 1h); TestDedupWindowExceedsMaxRetryInterval enforces it. In
// memory on purpose and NOT a replay defence: every handler reachable this way
// already refuses a stale message on its own terms.
var (
	seen_messages      = make(map[string]int64) // id -> timestamp
	seen_messages_lock sync.Mutex
	seen_messages_ttl  = int64(8 * 3600) // 8 hours

	// Ceiling on the dedup map: the TTL alone does not bound it, since the stream
	// rate limit is charged per stream OPEN, not per frame. Eviction drops the
	// OLDEST entries, which a retry (1h ladder, 8h TTL) never needs.
	seen_messages_maximum = 1000000
)

// message_seen_evict drops the oldest entries until the map is back under its
// ceiling. Caller holds seen_messages_lock; sheds a slice at a time, so this is
// O(n) rarely rather than per insert.
func message_seen_evict() {
	if len(seen_messages) <= seen_messages_maximum {
		return
	}
	target := len(seen_messages) - seen_messages_maximum + seen_messages_maximum/10

	// One sort to find the age cutoff that sheds `target` entries. Walking
	// the cutoff forward a second at a time instead would rescan the map on
	// every step - O(window x n), billions of operations at this size.
	timestamps := make([]int64, 0, len(seen_messages))
	for _, ts := range seen_messages {
		timestamps = append(timestamps, ts)
	}
	slices.Sort(timestamps)
	cutoff := timestamps[target]
	for id, ts := range seen_messages {
		if ts < cutoff {
			delete(seen_messages, id)
		}
	}
	warn("Messages: dedup map hit its %d-entry ceiling; shed entries older than %d", seen_messages_maximum, cutoff)
}

// Check if message was already processed
func message_seen(id string) bool {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	_, exists := seen_messages[id]
	return exists
}

// message_seen_mark atomically reports whether id was already processed and, if
// not, marks it seen. The pubsub manager and the direct-stream workers share
// this map, so the separate check-then-mark pair is not safe here.
func message_seen_mark(id string) bool {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	if _, exists := seen_messages[id]; exists {
		return true
	}
	seen_messages[id] = now()
	message_seen_evict()
	return false
}

// message_seen_clear forgets id so a later delivery is dispatched rather than
// coalesced away as a duplicate. The mark is set before the handler runs, so
// clear it only on a reason the sender will retry, never on a drop reason.
func message_seen_clear(id string) {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	delete(seen_messages, id)
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

	// A flood is signed over its content only, so a data segment would ride
	// unsigned and a relay could swap it. Nothing broadcasts segments today;
	// refuse loudly rather than drop them silently if that ever changes.
	if len(m.data) > 0 {
		warn("Pubsub refusing to broadcast %q/%q with %d bytes of unsigned segment data", m.Service, m.Event, len(m.data))
		return
	}

	content := cbor_encode(m.content)

	if allow_queue {
		queue_add_broadcast(m.ID, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, nil, m.expires)
	}

	if peers_sufficient() {
		pubsub_publish(m.From, m.Service, m.Event, m.ID, content)

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

// send_peer queues a message for a specified peer and wakes the queue manager
// rather than sending inline: the manager is serial, so concurrent calls cannot
// open N streams to one peer and trip its rate limit. Latency is one tick (1s).
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
// the outbound queue.
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

// send_work queues the message and, with no peer pinned, fans out one row per
// live peer hosting the recipient entity, so one peer's outage does not stop
// the others. With no peers known, one empty-target row queues for later
// resolution.
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
			// Never inline-attempt a self-loop row: peer_send(net_id) self-dials and
			// fails. self_loop_drain owns these; nudge it.
			queue_wake()
			return
		}
		message_attempt_send(m, m.target, content)
		return
	}

	peers := entity_peers_for(m.From, m.To)
	if len(peers) == 0 {
		// Unknown entity — queue one row with empty target so the
		// retry loop can re-resolve later. Same as before.
		queue_add_direct(m.ID, "", m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
		return
	}

	// One peer gets the inline send under m.ID; the rest queue with fresh uids.
	// m.ID must stay with a row in queue.db, since message_attempt_send drives
	// queue_sending / queue_ack through it. Self-loop peers divert to dispatch.
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

// message_attempt_send is the inline send-now path for a single peer. A
// package-level var so tests can stub out the libp2p reach-through.
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
	if send_error := peer_send(peer, m.ID, f); send_error != nil {
		queue_unsending(m.ID)
		queue_fail(m.ID, fmt.Sprintf("peer_send: %v", send_error))
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

// sender_check reports whether user owns the entity named in a from header, for
// message.send, message.send.peer and the stream openers. Ownership is the only
// test: /<app>/<entity>/... routing carries no ownership check of its own.
func sender_check(t *sl.Thread, user *User, from string, context string) (bool, error) {
	db := db_open("db/users.db")
	owned, err := db.exists("select id from entities where id=? and user=?", from, user.UID)
	if err != nil {
		return false, fmt.Errorf("database error: %v", err)
	}
	if owned {
		return true, nil
	}

	identity := ""
	if user.Identity != nil {
		identity = user.Identity.ID
	}
	routed := ""
	if route, ok := t.Local("route_entity").(string); ok && route == from {
		routed = " routed-entity"
	}
	info("%s: invalid%s from header - from=%q user=%q identity=%q", context, routed, from, user.UID, identity)
	return false, nil
}

// mochi.message.send(headers, content?, data?, expires=seconds) -> None: Send a Net message
func api_message_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 3 {
		return sl_error(fn, "syntax: <headers: dictionary>, [content: dictionary], [data: bytes]")
	}

	// Rate limit by app ID
	app, _ := t.Local("app").(*App)
	if app != nil && !rate_limit_net_send.allow(app.id) {
		return sl_error(fn, rate_limit_refuse(rate_limit_net_send, app.id, "messages per second"))
	}

	headers := sl_decode_strings(args[0])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	// Use user context, falling back to owner (for public actions like webhooks)
	user := principal_caller(t)
	if user == nil {
		user = principal_owner(t)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}

	from_valid, err := sender_check(t, user, headers["from"], "message.send")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if !from_valid {
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
		return sl_error(fn, rate_limit_refuse(rate_limit_net_send, app.id, "messages per second"))
	}

	headers := sl_decode_strings(args[1])
	if headers == nil {
		return sl_error(fn, "headers not specified or invalid")
	}

	user := principal_caller(t)
	if user == nil {
		user = principal_owner(t)
	}
	if user == nil {
		return sl_error(fn, "no user")
	}

	from_valid, err := sender_check(t, user, headers["from"], "message.send.peer")
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if !from_valid {
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
