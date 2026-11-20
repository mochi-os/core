// Mochi server: Messages
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"sync"
)

// Deduplication of inbound messages and pubsub announcements, by message id.
//
// seen_messages_ttl MUST be at least 2x the longest gap in retry_delays
// (queue.go caps at 1h); TestDedupWindowExceedsMaxRetryInterval enforces it. In
// memory on purpose and NOT a replay defence: every handler reachable this way
// already refuses a stale message on its own terms.
//
// A record is kept per id, with what became of the message: a copy arriving
// while the first is still running waits for its outcome rather than being
// acknowledged for a delivery that may yet fail. Each record is accounted to the
// peer that delivered the copy which made it, and a peer at its share sheds its
// own oldest, so one peer filling the map evicts its own records, not everyone
// else's.
var (
	seen_messages      = map[string]*message_record{}
	seen_sources       = map[string]*message_source{}
	seen_messages_lock sync.Mutex
	seen_messages_ttl  = int64(8 * 3600) // 8 hours

	// Ceiling on all records, and on one peer's. The TTL alone bounds neither,
	// since the stream rate limit is charged per stream OPEN, not per frame.
	seen_messages_maximum = 1000000
	seen_source_maximum   = 250000
)

// The states of a message's record, and what message_begin answers a copy with.
const (
	message_new     = iota // not seen, or failed for a reason the sender retries: the caller runs it
	message_running        // a copy is with a handler: the caller's reply waits for its outcome
	message_applied        // its handler succeeded: the caller acknowledges it
	message_refused        // it failed for a reason no retry changes: the caller answers that reason
)

// message_waiting_maximum bounds the copies waiting on one running message. A
// copy past it is answered fail_transient, for its sender to send again later.
const message_waiting_maximum = 16

// message_record is what this host knows of one message id.
type message_record struct {
	when    int64 // when it was marked or last settled, for the TTL
	source  *message_source
	state   int
	reason  string         // the refusal, in message_refused
	waiting []reply_target // copies that arrived while it ran
}

// message_source is one peer's share of the records: its ids, oldest first.
type message_source struct {
	peer  string
	order []string
	start int
}

// size is how many records the source holds.
func (s *message_source) size() int {
	return len(s.order) - s.start
}

// message_record_add records id, accounted to peer. Caller holds
// seen_messages_lock and knows id is not recorded.
func message_record_add(id, peer string, state int) *message_record {
	source := seen_sources[peer]
	if source == nil {
		source = &message_source{peer: peer}
		seen_sources[peer] = source
	}
	if source.size() >= seen_source_maximum {
		message_source_shed(source, 1)
	}
	record := &message_record{when: now(), source: source, state: state}
	seen_messages[id] = record
	source.order = append(source.order, id)
	if len(seen_messages) > seen_messages_maximum {
		message_seen_evict()
	}
	return record
}

// message_source_shed forgets the source's count oldest records. Caller holds
// seen_messages_lock.
func message_source_shed(source *message_source, count int) {
	for ; count > 0 && source.size() > 0; count-- {
		id := source.order[source.start]
		if record := seen_messages[id]; record != nil && record.source == source {
			delete(seen_messages, id)
		}
		source.order[source.start] = ""
		source.start++
	}
	if source.size() == 0 {
		delete(seen_sources, source.peer)
		return
	}
	// Compacted once half is spent, so the slice does not grow without bound.
	if source.start > len(source.order)/2 {
		source.order = append([]string(nil), source.order[source.start:]...)
		source.start = 0
	}
}

// message_seen_evict brings the map back under its ceiling, a tenth below it so
// this runs rarely, by shedding the oldest records of whichever peer holds the
// most. By count, not by age: ids marked within the same second shed as readily
// as any. Caller holds seen_messages_lock.
func message_seen_evict() {
	target := seen_messages_maximum - seen_messages_maximum/10
	shed := 0
	for len(seen_messages) > target {
		var largest *message_source
		for _, source := range seen_sources {
			if largest == nil || source.size() > largest.size() {
				largest = source
			}
		}
		if largest == nil {
			break
		}
		before := len(seen_messages)
		message_source_shed(largest, len(seen_messages)-target)
		shed += before - len(seen_messages)
	}
	warn("Messages: dedup map hit its %d-entry ceiling; shed the oldest %d of the peers holding the most", seen_messages_maximum, shed)
}

// message_begin is the check-and-mark a delivered message frame takes before it
// is handed to a handler. message_new marks it running, for the caller to run
// and settle with message_end; message_running adds reply to the copies waiting
// on the one running, or, with a reason, finds no room for it and leaves the
// caller to answer that; message_applied and message_refused say how the first
// copy ended, reason being the refusal.
func message_begin(id, peer string, reply reply_target) (int, string) {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	record := seen_messages[id]
	if record == nil {
		message_record_add(id, peer, message_running)
		return message_new, ""
	}
	switch record.state {
	case message_new:
		record.state = message_running
		record.when = now()
		return message_new, ""
	case message_running:
		if len(record.waiting) >= message_waiting_maximum {
			return message_running, fail_transient
		}
		record.waiting = append(record.waiting, reply)
		return message_running, ""
	}
	return record.state, record.reason
}

// message_state is message_begin without the mark, for a copy that may be
// answered from its record before it is admitted.
func message_state(id string) (int, string) {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	record := seen_messages[id]
	if record == nil {
		return message_new, ""
	}
	return record.state, record.reason
}

// message_end settles a message message_begin marked running, with the failure
// reason its handler gave, empty for success, and returns the copies that
// waited for it, to be answered the same. A reason the sender retries leaves
// the record due a retry, so the next copy runs.
func message_end(id, reason string) []reply_target {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	record := seen_messages[id]
	if record == nil || record.state != message_running {
		return nil
	}
	waiting := record.waiting
	record.waiting = nil
	record.when = now()
	switch {
	case reason == "":
		record.state = message_applied
	case fail_retryable(reason):
		record.state = message_new
	default:
		record.state = message_refused
		record.reason = reason
	}
	return waiting
}

// message_seen_mark atomically reports whether id was already seen and, if not,
// records it as applied, accounted to peer. For pubsub announcements, which are
// routed once and never retried; the direct-stream workers share the map, so
// the separate check-then-mark pair is not safe here.
func message_seen_mark(id, peer string) bool {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	if seen_messages[id] != nil {
		return true
	}
	message_record_add(id, peer, message_applied)
	return false
}

// message_seen_cleanup forgets records past the TTL, and the place each held in
// its peer's order.
func message_seen_cleanup() {
	seen_messages_lock.Lock()
	defer seen_messages_lock.Unlock()
	cutoff := now() - seen_messages_ttl
	for id, record := range seen_messages {
		if record.when < cutoff {
			delete(seen_messages, id)
		}
	}
	for peer, source := range seen_sources {
		kept := make([]string, 0, source.size())
		for _, id := range source.order[source.start:] {
			if record := seen_messages[id]; record != nil && record.source == source {
				kept = append(kept, id)
			}
		}
		if len(kept) == 0 {
			delete(seen_sources, peer)
			continue
		}
		source.order = kept
		source.start = 0
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
		// A failed publish leaves the row for the queue's retry.
		if pubsub_publish(m.From, m.Service, m.Event, m.ID, content) == nil && allow_queue {
			queue_ack(m.ID)
		}
	}
}

// send queues a completed outgoing message before it returns, for a caller
// that answers a web request, so the message is held once the request ends,
// within bound: what is left of the action's message_send_budget. An error
// means none of its rows was queued, and the caller hands the message to
// send_later. Only the inline send to the primary peer, and the queue's
// wake-up, run in the background.
func (m *Message) send(bound time.Duration) error {
	content := m.prepare()
	primary, wake, err := m.queue(content, bound)
	if err != nil {
		return err
	}
	m.release(message_attempt_send, primary, wake, content)
	return nil
}

// send_later queues a completed outgoing message in the background: for a
// caller with no web request to answer (an event handler, a schedule, a service
// call), and for one whose send found queue.db busy. A failure raised to either
// would leave its work half done - an event's sender redelivers the whole
// handler, repeating its writes and its earlier sends, and an action that sends
// to several recipients has told some and not others - so a busy queue.db is
// waited out for message_send_patience and a message it still refuses is
// reported to the administrator.
func (m *Message) send_later() {
	content := m.prepare()
	attempt := message_attempt_send
	go guard("message send", nil, func() {
		primary, wake, err := m.queue(content, message_send_patience)
		if err != nil {
			warn("Messages unable to queue %s/%s %q from %q to %q: %v", m.Service, m.Event, m.ID, m.From, m.To, err)
			return
		}
		m.release(attempt, primary, wake, content)
	})
}

// prepare gives m its id and answers its encoded content.
func (m *Message) prepare() []byte {
	m.target = ""
	if m.ID == "" {
		m.ID = uid()
	}
	return cbor_encode(m.content)
}

// release starts the inline send to the primary peer and wakes the queue for
// the rest, in the background. A lone row to a peer needs no wake-up, since
// the inline send takes it; several rows, or one for this server, do.
func (m *Message) release(attempt func(*Message, string, []byte), primary string, wake bool, content []byte) {
	if primary == "" && !wake {
		return
	}
	go guard("message send", nil, func() {
		if primary != "" {
			attempt(m, primary, content)
		}
		if wake {
			queue_wake()
		}
	})
}

// message_send_budget bounds how long send waits for queue.db, every attempt
// included, and message_send_patience how long send_later does; each attempt
// gives up after queue_add_timeout, or sooner when the bound runs out first.
// message_send_pause is the first wait between attempts, doubling up to
// message_send_pause_maximum. vars so tests can shorten them.
var (
	message_send_budget        = 10 * time.Second
	message_send_patience      = 5 * time.Minute
	message_send_pause         = 250 * time.Millisecond
	message_send_pause_maximum = 8 * time.Second
)

// queue resolves m's routes and queues its rows within bound, answering the
// peer whose row carries m.ID for the inline send ("" when there is none), and
// whether the queue needs waking for the rest. Each live peer hosting the
// recipient gets a row, so one peer's outage does not stop the others: m.ID for
// the first, and for each other the queue_sibling id a later retarget gives the
// copy too, so the two cannot queue a peer the message twice. With no peer
// known, one row with no target waits to be resolved again. The rows land in one
// transaction, all or none.
//
// A recipient of this server's own is routed here alone (entity_peers_for), and
// is handed to its worker directly, with a row only when the worker's inbox is
// full. This server is one of several routes only by the sender's learned
// directory, for a recipient that is not its own, which a worker would not
// take; its row is queued in the same transaction as the others and
// self_loop_drain settles it, so nothing goes out before every row is sure to
// be queued. A row for this server is never the inline one, since
// message_attempt_send to net_id would dial this server.
func (m *Message) queue(content []byte, bound time.Duration) (string, bool, error) {
	peers := entity_peers_for(m.From, m.To)
	if len(peers) == 1 && peers[0] == net_id {
		local := *m
		local.ID = uid()
		local.target = net_id
		if message_self_loop_dispatch(&local, content) {
			return "", false, nil
		}
		return "", true, m.queue_routes([]queue_route{{id: uid(), target: net_id}}, content, bound)
	}
	var routes []queue_route
	primary := ""
	wake := false
	if len(peers) == 0 {
		routes = append(routes, queue_route{id: m.ID})
	}
	for _, peer := range peers {
		if peer == net_id {
			routes = append(routes, queue_route{id: uid(), target: peer})
			wake = true
			continue
		}
		id := m.ID
		if primary == "" {
			primary = peer
		} else {
			id = queue_sibling(m.ID, peer)
		}
		routes = append(routes, queue_route{id: id, target: peer})
	}
	if len(routes) > 1 {
		wake = true
	}
	if err := m.queue_routes(routes, content, bound); err != nil {
		return "", false, err
	}
	return primary, wake, nil
}

// queue_routes queues m's rows in one transaction, trying again after a busy
// or full queue.db until bound has passed.
func (m *Message) queue_routes(routes []queue_route, content []byte, bound time.Duration) error {
	start := time.Now()
	deadline := start.Add(bound)
	pause := message_send_pause
	for {
		err := queue_add_routes(min(queue_add_timeout, time.Until(deadline)), m, content, routes)
		if err == nil {
			return nil
		}
		if !queue_add_transient(err) || time.Now().Add(pause).After(deadline) {
			return fmt.Errorf("queue database after %v: %w", time.Since(start).Round(time.Millisecond), err)
		}
		time.Sleep(pause)
		pause = min(2*pause, message_send_pause_maximum)
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

// messages_fanout_open opens a Sender for a peer a fan-out has queued rows for.
var messages_fanout_open = sender_start

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
		// The first peer's row carries m.ID, as send's does; the other peers'
		// take queue_sibling's id, as send's do, and the self-loop rows fresh
		// ones.
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
	// /mochi/2/messages: the row is claimed here and the inflight resolver
	// (the Sender's read_loop) drives queue_ack / queue_fail. On a stream-open failure the
	// row stays queued for queue_process to retry.
	f, err := frame_for_message(m, content)
	if err != nil {
		frame_drop(m.ID, peer, err)
		return
	}
	// A Sender's pull_loop may have claimed the row first: it is a new row, due
	// at once. Then it is that Sender's to send.
	if !queue_sending(m.ID) {
		return
	}
	if send_error := peer_send(peer, m.ID, f); send_error != nil {
		queue_unsending(m.ID)
		// A Sender that is only busy takes the row itself: its pull_loop
		// claims the peer's due rows as the outbox drains, so the row stays
		// due rather than waiting out the retry ladder; so does one
		// draining for shutdown, or closed. A peer that cannot be reached is
		// retried with backoff.
		if sender_refused(send_error) {
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

	// A web request's action queues the message before it returns, so it is
	// held once the request ends; anywhere else it is queued in the background
	// (send_later). A queue failure is never raised: an action that sends to
	// several recipients would be left having told some, and a machine caller
	// such as a webhook redelivers, finding its work done and its messages
	// unsent. A message the action could not queue in time goes to send_later.
	// A service call counts as anywhere else even inside an action: it runs
	// another app's function on a thread of its own. The action's sends share
	// one budget of time spent queueing, and once it is spent the rest go to
	// send_later: a busy queue.db would otherwise cost a group's creation the
	// budget once per member, past the request's own deadline.
	if action, _ := t.Local("action").(*Action); action != nil {
		left := message_send_budget - action.sending
		if left <= 0 {
			m.send_later()
			return sl.None, nil
		}
		start := time.Now()
		err := m.send(left)
		action.sending += time.Since(start)
		if err != nil {
			info("Messages queueing %s/%s %q from %q to %q later: %v", m.Service, m.Event, m.ID, m.From, m.To, err)
			m.send_later()
		}
		return sl.None, nil
	}
	m.send_later()
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
