// Mochi server: Messages
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"fmt"
	"slices"
	"time"

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
		// The row is only the retry for when no peers are up: the announcement
		// still goes out below, and its publishers re-announce on their own
		// schedule.
		if err := queue_add_broadcast(m.ID, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, nil, m.expires); err != nil {
			info("Pubsub unable to queue %q/%q %q for retry: %v", m.Service, m.Event, m.ID, err)
		}
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
	go guard("message send", nil, m.send_work)
}

// message_enqueue_patience bounds how long send_work keeps trying to queue a
// row the database refuses, attempts included; each attempt gives up after
// queue_add_timeout, well inside the driver's busy timeout, so a lock held
// past one attempt is tried again rather than ending the row. The first pause
// between attempts is message_enqueue_pause, doubling up to
// message_enqueue_pause_maximum. vars so tests can shorten them.
var (
	message_enqueue_patience      = 5 * time.Minute
	message_enqueue_pause         = 250 * time.Millisecond
	message_enqueue_pause_maximum = 8 * time.Second
)

// enqueue queues one row of m for send_work, which runs after the app's call
// has returned and so has nobody to hand a failure to. A busy or full database
// is waited out with growing pauses for up to message_enqueue_patience; any
// other failure, or one that outlasts it, gives the row up with a warning.
// Reports whether the row landed.
func (m *Message) enqueue(id, target string, content []byte) bool {
	pause := message_enqueue_pause
	deadline := time.Now().Add(message_enqueue_patience)
	for {
		err := queue_add_direct(id, target, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires)
		if err == nil {
			return true
		}
		if !queue_add_transient(err) || time.Now().Add(pause).After(deadline) {
			warn("Messages unable to queue %s/%s %q from %q to %q via peer %q: %v", m.Service, m.Event, id, m.From, m.To, target, err)
			return false
		}
		time.Sleep(pause)
		pause = min(2*pause, message_enqueue_pause_maximum)
	}
}

// send_peer queues a message for a specified peer and wakes the queue manager
// rather than sending inline: the manager is serial, so concurrent calls cannot
// open N streams to one peer and trip its rate limit. Latency is one tick (1s).
// An error is the queue.db write's: nothing was queued.
func (m *Message) send_peer(peer string) error {
	m.target = peer
	if m.ID == "" {
		m.ID = uid()
	}
	content := cbor_encode(m.content)
	if message_self_loop_dispatch(m, content) {
		return nil
	}
	if err := queue_add_direct(m.ID, m.target, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires); err != nil {
		return err
	}
	queue_wake()
	return nil
}

// send_peer_priority is send_peer with an explicit queue priority
// override. Used by broadcast_resync to ship replay messages in the
// priority_replay lane so they overtake live broadcast traffic in
// the outbound queue.
func (m *Message) send_peer_priority(peer string, priority int) error {
	m.target = peer
	if m.ID == "" {
		m.ID = uid()
	}
	content := cbor_encode(m.content)
	if message_self_loop_dispatch(m, content) {
		return nil
	}
	if err := queue_add_direct_priority(m.ID, m.target, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires, priority); err != nil {
		return err
	}
	queue_wake()
	return nil
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
		if !m.enqueue(m.ID, m.target, content) {
			return
		}
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
		m.enqueue(m.ID, "", content)
		return
	}

	// One peer gets the inline send under m.ID; each other peer's copy takes
	// queue_sibling's id, the one a later retarget gives it too, so the two
	// cannot queue a peer the message twice. m.ID must stay with a row in
	// queue.db, since message_attempt_send drives queue_sending / queue_ack
	// through it. Self-loop peers divert to dispatch. The first row queue.db
	// refuses ends the fan-out: the next would wait out the same failure.
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
			if !m.enqueue(uid(), peer, content) {
				break
			}
			self_queued = true
			continue
		}
		id := m.ID
		if primary_peer != "" {
			id = queue_sibling(m.ID, peer)
		}
		if !m.enqueue(id, peer, content) {
			break
		}
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

// messages_fanout_open opens a Sender for a peer a fan-out has queued rows for.
var messages_fanout_open = sender_for

// messages_fanout queues messages sharing one content, encoded once by the
// caller, recipient after recipient: each to its pinned peer, or else to every
// live peer hosting its recipient, then wakes the queue once so each peer's
// Sender drains its rows. It makes no inline send: in turn, one to a busy
// Sender would hold every later recipient for the send timeout.
//
// The first row queue.db refuses ends the fan-out, and it is logged rather
// than retried: every later row would wait out the same failure, with the
// caller holding its broadcast stream's lock. The fan-out is of a broadcast
// whose log holds the event, and a subscriber missing it fetches it by resync
// when it sees the gap.
func messages_fanout(list []*Message, content []byte) {
	if len(list) == 0 {
		return
	}
	cold := map[string]bool{}
	queued := 0
	var failure error
	add := func(m *Message, id, target string) bool {
		if err := queue_add_direct(id, target, m.From, m.To, m.Service, m.Event, m.FromApp, m.Services, content, m.data, m.file, m.expires); err != nil {
			failure = err
			return false
		}
		queued++
		return true
	}
fanout:
	for _, m := range list {
		if m.ID == "" {
			m.ID = uid()
		}
		if m.target != "" {
			if !message_self_loop_dispatch(m, content) {
				if !add(m, m.ID, m.target) {
					break
				}
				if m.target != net_id {
					cold[m.target] = true
				}
			}
			continue
		}
		peers := entity_peers_for(m.From, m.To)
		if len(peers) == 0 {
			// Unknown entity: one row with no target, resolved again later.
			if !add(m, m.ID, "") {
				break
			}
			continue
		}
		// The first peer's row carries m.ID, as send_work's does; the other
		// peers' take queue_sibling's id, as send_work's do, and the self-loop
		// rows fresh ones.
		first := true
		for _, peer := range peers {
			if peer == net_id {
				local := *m
				local.ID = uid()
				local.target = peer
				if !message_self_loop_dispatch(&local, content) && !add(m, uid(), peer) {
					break fanout
				}
				continue
			}
			id := m.ID
			if !first {
				id = queue_sibling(m.ID, peer)
			}
			if !add(m, id, peer) {
				break fanout
			}
			first = false
			cold[peer] = true
		}
	}
	if failure != nil {
		info("Fan-out stopped after queuing %d rows of %d %s/%s messages: %v", queued, len(list), list[0].Service, list[0].Event, failure)
	}
	queue_wake()

	// A peer with no Sender open would wait for the queue manager, which opens a
	// batch of peers at a time and waits for the slowest. Open each here, in
	// parallel as the inline sends did, so its pull_loop takes the rows at once.
	for peer := range cold {
		if senders_has(peer) || senders_opening_has(peer) || peer_is_silent(peer) || peer_is_stalled(peer) {
			continue
		}
		go guard("fan-out sender open", nil, func() {
			if _, err := messages_fanout_open(peer); err != nil {
				debug("Fan-out: no Sender for peer %q: %v", peer, err)
			}
		})
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
		frame_drop(m.ID, peer, err)
		return
	}
	queue_sending(m.ID)
	if send_error := peer_send(peer, m.ID, f); send_error != nil {
		queue_unsending(m.ID)
		// A Sender that is only busy takes the row itself: its pull_loop
		// claims the peer's due rows as the outbox drains, so the row stays
		// due rather than waiting out the retry ladder. A peer that cannot be
		// reached is retried with backoff.
		if errors.Is(send_error, error_sender_full) {
			return
		}
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
	if !ok || !valid(peer, "peer") {
		// Unvalidated, this became a queue row that can never deliver and is
		// retried on the backoff schedule for the life of the row.
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

	if err := m.send_peer(peer); err != nil {
		return sl_error(fn, "unable to queue message: %v", err)
	}
	return sl.None, nil
}
