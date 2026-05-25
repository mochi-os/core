// Mochi server: Durable broadcast log for subscriber fan-out
// Copyright Alistair Cunningham 2026

package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// NACK reason hints. Receiver populates Headers.Reason on the
// outbound NACK frame; sender's NACK handler reads it to decide
// between retry (the legacy unconditional behaviour) and drop. New
// reasons can be added freely - the omitempty wire field falls back
// to "" on older peers, which maps to the legacy retry-everything
// path. See claude/sessions/2026-05-25-broadcast-resync-seq-643-
// investigation.md for context.
const (
	nack_reason_broadcast_gap  = "broadcast-gap"
	nack_reason_decode_failed  = "decode-failed"
)

// ErrBroadcastGap is the sentinel the gap detector wraps its returned
// error with so the stream-layer NACK responder can map it to the
// nack_reason_broadcast_gap wire hint without parsing the (info-only)
// error string. Other apply paths that want a non-retry NACK should
// define their own sentinel and extend nack_reason_from_error.
var ErrBroadcastGap = errors.New("broadcast gap")

// nack_reason_from_error maps a route() error to the wire Reason
// hint. Unknown errors return "" which preserves legacy retry
// behaviour at the sender. Called from the stream-receive NACK path
// in streams.go.
func nack_reason_from_error(err error) string {
	if errors.Is(err, ErrBroadcastGap) {
		return nack_reason_broadcast_gap
	}
	return ""
}

// mochi.broadcast.* — sequenced broadcast with a durable log per
// (app, key, peer) so subscribers can replay gaps from the owner.
//
// Sender side:
//
//	mochi.broadcast.send(key, [subscriber, ...], event, data) -> int
//	  allocates seq, writes _log row, fans out to subscribers.
//	mochi.broadcast.replay(key, peer, after, limit) -> [{sequence, event, data}, ...]
//	  reads the log for a (key, peer) stream starting after `after`.
//
// Receiver side:
//
//	mochi.broadcast.next(key) -> int (legacy; sequence allocator)
//	mochi.broadcast.received(sender, key) -> int (highest applied seq)
//	mochi.broadcast.advance(sender, key, sequence)
//
// Core's events.go auto-applies gap detection on inbound events
// carrying `_key` + `_sequence` in content + `peer` header: dedups
// against `_received`, NACKs on gap (with async resync request),
// advances `_received` after a successful handler.
//
// Tables (per app DB, lazy-created):
//
//	_sequence(key, peer, last)               — sender outbound counter per (key, this_host)
//	_log(key, peer, sequence, event, data, created)
//	_acknowledged(key, peer, subscriber, last)
//	_received(sender, key, last)             — receiver-side dedup
var api_broadcast = sls.FromStringDict(sl.String("mochi.broadcast"), sl.StringDict{
	"next":     sl.NewBuiltin("mochi.broadcast.next", api_broadcast_next),
	"received": sl.NewBuiltin("mochi.broadcast.received", api_broadcast_received),
	"advance":  sl.NewBuiltin("mochi.broadcast.advance", api_broadcast_advance),
	"send":     sl.NewBuiltin("mochi.broadcast.send", api_broadcast_send),
	"replay":   sl.NewBuiltin("mochi.broadcast.replay", api_broadcast_replay),
})

const broadcast_log_age = 7 * 86400

func broadcast_sequence_table_create(db *DB) {
	db.exec("create table if not exists _sequence (key text not null, peer text not null, last integer not null default 0, primary key (key, peer))")
}

func broadcast_received_table_create(db *DB) {
	db.exec("create table if not exists _received (sender text not null, key text not null, last integer not null default 0, primary key (sender, key))")
}

// broadcast_log_table_create lazily creates _log for an app DB on
// first emission. Replication carries the table to paired hosts two
// ways:
//   - Bulk bootstrap: a new pair member receives the per-app DB
//     snapshot (db_snapshot.go) which page-copies the entire file
//     including _log + the BootstrapDBChunk.Seed cursor seed, so
//     subsequent live ops chain correctly from where the snapshot
//     ended. The new member can serve resync requests for any of
//     the (key, peer) streams the existing pair members had logged.
//   - Live: each broadcast_log_append uses exec_app_user, which
//     emits a sql/op that replays as the same insert on every paired
//     host. Both pair members converge in steady state.
//
// Apps that adopt mochi.broadcast.send after their per-app DB
// already has data don't get a retroactive _log for pre-upgrade
// events (claude/plans/broadcast.md: "No backfill on migration").
// Subscribers reaching for those older sequences fall back to the
// per-app request_resync helper, which pulls a fresh schema dump
// from the owner instead of a per-op replay.
func broadcast_log_table_create(db *DB) {
	db.exec("create table if not exists _log (key text not null, peer text not null, sequence integer not null, event text not null, data text not null, created integer not null, primary key (key, peer, sequence))")
	db.exec("create index if not exists _log_created on _log(created)")
}

func broadcast_acknowledged_table_create(db *DB) {
	db.exec("create table if not exists _acknowledged (key text not null, peer text not null, subscriber text not null, last integer not null default 0, primary key (key, peer, subscriber))")
}

// broadcast_next_local allocates and returns the next outbound sequence
// number on the given DB for (key, peer). Per-(key, peer) PK gives each
// paired host its own sequence space.
func broadcast_next_local(db *DB, key, peer string) int64 {
	broadcast_sequence_table_create(db)
	db.exec_app_user("insert into _sequence (key, peer, last) values (?, ?, 1) on conflict(key, peer) do update set last = _sequence.last + 1", key, peer)
	return int64(db.integer("select last from _sequence where key=? and peer=?", key, peer))
}

func broadcast_received_get(db *DB, sender, key string) int64 {
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='_received'")
	if !exists {
		return 0
	}
	return int64(db.integer("select last from _received where sender=? and key=?", sender, key))
}

// broadcast_advance_local is the public advance: bumps _received,
// clears the in-flight resync gate, then drains any pending-buffer
// rows that chain onto the new _received.last. Callers (events.go,
// api_broadcast_advance) just want "this seq is done, do all
// follow-ups" - the drain is part of that.
func broadcast_advance_local(db *DB, sender, key string, sequence int64) {
	broadcast_advance_local_simple(db, sender, key, sequence)
	// Any advance is evidence the resync request (if any) is
	// producing replies, so the in-flight gate clears and the next
	// gap-detection can fire its follow-up batch immediately rather
	// than waiting out a fixed time window. db.user can be nil for
	// the api_broadcast_advance Starlark callsite without a user
	// context - skip the clear there; the throttle has its own
	// timeout fallback for the no-user case.
	if db.user != nil && db.user.UID != "" {
		broadcast_resync_clear(db.user.UID, sender, key)
	}
	// Pull in any buffered events that now chain onto _received.last.
	// Common case is "nothing pending" - one indexed SELECT.
	broadcast_pending_drain_chain(db, sender, key)
}

// broadcast_advance_local_simple is the bare advance with no drain
// recursion. broadcast_pending_drain_chain calls this directly after
// dispatching a buffered row, so the drain's own advance doesn't
// re-enter the drain loop. Keep this in sync with the SQL in the
// public advance above.
func broadcast_advance_local_simple(db *DB, sender, key string, sequence int64) {
	broadcast_received_table_create(db)
	db.exec_app_user("insert into _received (sender, key, last) values (?, ?, ?) on conflict(sender, key) do update set last = max(_received.last, excluded.last)", sender, key, sequence)
}

// broadcast_log_append writes one log row in the same transaction as
// the sequence bump. Returns the allocated sequence.
func broadcast_log_append(db *DB, key, peer, event string, data []byte) int64 {
	broadcast_log_table_create(db)
	broadcast_log_age_trim(db, key, peer)
	sequence := broadcast_next_local(db, key, peer)
	db.exec_app_user("insert into _log (key, peer, sequence, event, data, created) values (?, ?, ?, ?, ?, ?)", key, peer, sequence, event, string(data), now())
	return sequence
}

// broadcast_log_age_trim deletes log rows older than the age cap for
// the given (key, peer). Called on send; no-op when nothing's aged out.
func broadcast_log_age_trim(db *DB, key, peer string) {
	db.exec_app_user("delete from _log where key=? and peer=? and created < ?", key, peer, now()-broadcast_log_age)
}

// broadcast_log_ack_trim deletes log rows below the min ack across all
// subscribers for (key, peer). Called from the acknowledge handler
// after _acknowledged is updated.
func broadcast_log_ack_trim(db *DB, key, peer string) {
	row, _ := db.row("select min(last) as m from _acknowledged where key=? and peer=?", key, peer)
	if row == nil {
		return
	}
	last, ok := row["m"].(int64)
	if !ok || last <= 0 {
		return
	}
	db.exec_app_user("delete from _log where key=? and peer=? and sequence < ?", key, peer, last)
}

// mochi.broadcast.next(key) -> int: allocate the next outbound sequence
// number for (key, this_host).
func api_broadcast_next(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "key", &key); err != nil {
		return nil, err
	}
	if key == "" {
		return sl_error(fn, "key must be non-empty")
	}

	user, _ := t.Local("user").(*User)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app(user, app)
	if db == nil {
		return sl_error(fn, "no app database")
	}
	return sl.MakeInt64(broadcast_next_local(db, key, p2p_id)), nil
}

// mochi.broadcast.received(sender, key) -> int: highest applied seq.
func api_broadcast_received(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var sender, key string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "sender", &sender, "key", &key); err != nil {
		return nil, err
	}

	user, _ := t.Local("user").(*User)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl.MakeInt(0), nil
	}

	db := db_app(user, app)
	if db == nil {
		return sl.MakeInt(0), nil
	}
	return sl.MakeInt64(broadcast_received_get(db, sender, key)), nil
}

// mochi.broadcast.advance(sender, key, sequence) -> None: record applied seq.
func api_broadcast_advance(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var sender, key string
	var sequence int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "sender", &sender, "key", &key, "sequence", &sequence); err != nil {
		return nil, err
	}
	if sender == "" || key == "" {
		return sl_error(fn, "sender and key must be non-empty")
	}
	if sequence < 0 {
		return sl_error(fn, "sequence must be non-negative")
	}

	user, _ := t.Local("user").(*User)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app(user, app)
	if db == nil {
		return sl_error(fn, "no app database")
	}
	broadcast_advance_local(db, sender, key, sequence)
	return sl.None, nil
}

// mochi.broadcast.send(from, key, subscribers, service, event, data, exclude=None) -> int
//
// Allocates a sequence for (key, this_host), writes the event to the
// per-app _log table, and fans out one mochi.message.send per
// subscriber. Each outbound message carries _key and _sequence in
// content; the receiver's peer header identifies the originating host.
//
// `from` is the sender entity ID (must be owned by the calling user).
// `key` is the broadcast stream key (typically the same entity ID;
// apps that want multiple streams per scope can use other keys).
// `subscribers` is a list of recipient entity IDs. `exclude` skips a
// single entity (typically the original event author).
func api_broadcast_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var from, key, service, event, exclude string
	var subscribers *sl.List
	var data sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs,
		"from", &from,
		"key", &key,
		"subscribers", &subscribers,
		"service", &service,
		"event", &event,
		"data", &data,
		"exclude?", &exclude,
	); err != nil {
		return nil, err
	}
	if !valid(from, "entity") {
		return sl_error(fn, "invalid from %q", from)
	}
	if key == "" {
		return sl_error(fn, "key must be non-empty")
	}
	if !valid(service, "constant") {
		return sl_error(fn, "invalid service %q", service)
	}
	if !valid(event, "constant") {
		return sl_error(fn, "invalid event %q", event)
	}

	user, _ := t.Local("user").(*User)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	udb := db_open("db/users.db")
	owned, err := udb.exists("select id from entities where id=? and user=?", from, user.UID)
	if err != nil || !owned {
		return sl_error(fn, "from %q not owned by caller", from)
	}

	db := db_app(user, app)
	if db == nil {
		return sl_error(fn, "no app database")
	}

	payload, _ := sl_decode(data).(map[string]any)
	if payload == nil {
		payload = map[string]any{}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return sl_error(fn, "payload not JSON-encodable: %v", err)
	}

	sequence := broadcast_log_append(db, key, p2p_id, event, body)

	// Attach broadcast metadata to outbound content. _peer is implicit
	// via the originating libp2p host (peer header on the receiver
	// side) and doesn't need to ride in content.
	content := map[string]any{}
	for k, v := range payload {
		content[k] = v
	}
	content["_key"] = key
	content["_sequence"] = sequence

	services := app_services(app, user)
	iter := subscribers.Iterate()
	defer iter.Done()
	var item sl.Value
	for iter.Next(&item) {
		sub, _ := sl.AsString(item)
		if sub == "" || sub == exclude {
			continue
		}
		m := message(from, sub, service, event)
		m.FromApp = app.id
		m.Services = services
		m.content = content
		m.send()
	}

	return sl.MakeInt64(sequence), nil
}

// mochi.broadcast.replay(key, peer, after, limit=100) -> [{sequence, event, data}, ...]
//
// Reads log rows from the per-app _log table for the given (key, peer)
// stream starting at sequence > after, capped at limit. Used by the
// broadcast/resync event handler to feed a resync request — apps
// shouldn't normally call this directly.
func api_broadcast_replay(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var key, peer string
	var after int64
	limit := int64(100)
	if err := sl.UnpackArgs(fn.Name(), args, kwargs,
		"key", &key,
		"peer", &peer,
		"after", &after,
		"limit?", &limit,
	); err != nil {
		return nil, err
	}
	if key == "" || peer == "" {
		return sl_error(fn, "key and peer must be non-empty")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	user, _ := t.Local("user").(*User)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app(user, app)
	if db == nil {
		return sl_error(fn, "no app database")
	}

	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='_log'")
	if !exists {
		return sl.NewList(nil), nil
	}

	rows, _ := db.rows("select sequence, event, data from _log where key=? and peer=? and sequence > ? order by sequence limit ?", key, peer, after, limit)
	out := make([]sl.Value, 0, len(rows))
	for _, row := range rows {
		sequence, _ := row["sequence"].(int64)
		event_name, _ := row["event"].(string)
		data_raw, _ := row["data"].(string)
		var data any
		_ = json.Unmarshal([]byte(data_raw), &data)
		out = append(out, sl_encode(map[string]any{
			"sequence": sequence,
			"event":    event_name,
			"data":     data,
		}))
	}
	return sl.NewList(out), nil
}

// broadcast_resync handles an inbound broadcast/resync event. The
// subscriber's content has {key, peer, after}: we read the matching
// rows from _log and re-emit each one to the requester via
// send_peer (direct libp2p delivery, not fanned). Replayed events
// flow through the normal event pipeline at the receiver, where the
// gap wrapper applies them in order.
const broadcast_replay_limit = 100

func (e *Event) broadcast_resync(a *App, av *AppVersion) error {
	key, _ := e.content["key"].(string)
	peer, _ := e.content["peer"].(string)
	after := event_int64(e.content["after"])
	if key == "" || peer == "" {
		return fmt.Errorf("broadcast/resync requires key and peer")
	}

	exists, _ := e.db.exists("select 1 from sqlite_master where type='table' and name='_log'")
	if !exists {
		return nil
	}

	rows, _ := e.db.rows("select sequence, event, data from _log where key=? and peer=? and sequence > ? order by sequence limit ?", key, peer, after, broadcast_replay_limit)
	if len(rows) == 0 {
		return nil
	}

	services := app_services(a, e.user)
	for _, row := range rows {
		sequence, _ := row["sequence"].(int64)
		event_name, _ := row["event"].(string)
		data_raw, _ := row["data"].(string)
		var payload map[string]any
		_ = json.Unmarshal([]byte(data_raw), &payload)
		if payload == nil {
			payload = map[string]any{}
		}
		content := map[string]any{}
		for k, v := range payload {
			content[k] = v
		}
		content["_key"] = key
		content["_sequence"] = sequence

		m := message(e.to, e.from, e.service, event_name)
		m.FromApp = a.id
		m.Services = services
		m.content = content
		m.send_peer(e.peer)
	}
	return nil
}

// broadcast_acknowledge handles an inbound broadcast/acknowledge event.
// The subscriber's content has {key, peer, sequence}: we update
// _acknowledged for (key, peer, subscriber=e.from) and run the
// log-trim step.
func (e *Event) broadcast_acknowledge() error {
	key, _ := e.content["key"].(string)
	peer, _ := e.content["peer"].(string)
	sequence := event_int64(e.content["sequence"])
	if key == "" || peer == "" || sequence <= 0 {
		return fmt.Errorf("broadcast/acknowledge requires key, peer, and sequence")
	}

	broadcast_acknowledged_table_create(e.db)
	e.db.exec_app_user("insert into _acknowledged (key, peer, subscriber, last) values (?, ?, ?, ?) on conflict(key, peer, subscriber) do update set last = max(_acknowledged.last, excluded.last)", key, peer, e.from, sequence)
	broadcast_log_ack_trim(e.db, key, peer)
	return nil
}

// broadcast_resync_throttle gates resync requests per (user, peer, key)
// to at most ONE IN FLIGHT, not one per time window. Previous design
// locked out for 60 seconds after every request regardless of whether
// the request succeeded - a 300-event gap took 3+ minutes minimum
// even on a fast link, because four sequential 100-event resyncs
// each waited out 60s of throttle. New design tracks "request out,
// no advance yet" as a bool; clears it on any _received.last advance
// for the (user, peer, key) tuple (broadcast_advance_local calls
// broadcast_resync_clear). A timeout fallback covers the case where
// the resync reply never arrives - same throttle behaviour as before
// but only when something is actually stuck, not after every success.
//
// Burst dedup (the original throttle's load-bearing property) still
// holds: if 50 inbound events trip the gap detector in 200ms, only
// the first sees broadcast_resync_inflight=false and proceeds; the
// other 49 see the flag and return. Once that resync's replies start
// advancing _received, the flag clears and the next gap-detection
// request fires immediately.
//
// See claude/sessions/2026-05-25-broadcast-resync-seq-643-
// investigation.md and follow-up task #81.
const broadcast_resync_timeout = 30 * time.Second

var (
	broadcast_resync_lock     sync.Mutex
	broadcast_resync_inflight = map[string]int64{} // tag -> request unix time
)

func broadcast_resync_tag(user_uid, peer, key string) string {
	return fmt.Sprintf("%s|%s|%s", user_uid, peer, key)
}

func broadcast_resync_throttle(user_uid, peer, key string) bool {
	broadcast_resync_lock.Lock()
	defer broadcast_resync_lock.Unlock()
	tag := broadcast_resync_tag(user_uid, peer, key)
	now_ts := time.Now().Unix()
	if last, inflight := broadcast_resync_inflight[tag]; inflight {
		// Timeout fallback: if the resync reply never arrived
		// (link flapped, owner offline at the moment), clear the
		// in-flight flag so the next gap-detection can retry. Keeps
		// the subsystem from wedging on a lost reply.
		if now_ts-last < int64(broadcast_resync_timeout/time.Second) {
			return false
		}
	}
	broadcast_resync_inflight[tag] = now_ts
	return true
}

// broadcast_resync_clear marks the in-flight resync for the given
// (user, peer, key) tuple complete - subsequent gap-detections can
// fire the next request without waiting. Called from
// broadcast_advance_local on every _received.last advance; idempotent
// when no flag is set, so safe to call on every advance whether or
// not a resync was in flight.
func broadcast_resync_clear(user_uid, peer, key string) {
	broadcast_resync_lock.Lock()
	defer broadcast_resync_lock.Unlock()
	delete(broadcast_resync_inflight, broadcast_resync_tag(user_uid, peer, key))
}

// broadcast_resync_jitter_maximum bounds the random delay added before
// a resync request leaves the subscriber. Spreads simultaneous gap
// detections - after a server restart, a sleep / wake cycle, or any
// event that causes thousands of subscribers to detect a gap on their
// first inbound event - across the interval, so the owner doesn't get
// every subscriber's resync request landing in the same second. The
// 60-second per-(user, peer, key) throttle above prevents same-stream
// churn; jitter prevents cross-subscriber thundering-herd at the
// owner.
const broadcast_resync_jitter_maximum = 5 * time.Second

// broadcast_resync_jitter returns a uniform random delay in
// [0, broadcast_resync_jitter_maximum). Uses crypto/rand because it's
// the rand source the rest of the package already imports; the jitter
// only needs randomness, not unpredictability.
func broadcast_resync_jitter() time.Duration {
	var buffer [2]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return 0
	}
	return time.Duration(int(buffer[0])<<8|int(buffer[1])) * time.Millisecond % broadcast_resync_jitter_maximum
}

// broadcast_request_resync sends a fire-and-forget broadcast/resync to
// the originating host asking for replay of (key, peer) starting after
// the receiver's current last. Called from the gap-detection wrapper
// in events.go when an out-of-order event arrives.
//
// from: the subscriber entity (the local user's entity that's
//
//	subscribed to the broadcast)
//
// to:   the broadcast owner entity
// peer: the libp2p peer ID of the originating host (matches e.peer on
//
//	the inbound event)
func broadcast_request_resync(user *User, a *App, from, to, key, peer string, last int64) {
	if user == nil || a == nil {
		return
	}
	if !broadcast_resync_throttle(user.UID, peer, key) {
		return
	}
	// Jitter the send to spread simultaneous gap detections across
	// subscribers - see broadcast_resync_jitter_maximum's comment. The
	// caller is already in a goroutine (events.go fires this with a
	// `go` statement), so the sleep doesn't block the apply path.
	time.Sleep(broadcast_resync_jitter())
	services := app_services(a, user)
	service := ""
	if len(services) > 0 {
		service = services[0]
	}
	m := message(from, to, service, "broadcast/resync")
	m.FromApp = a.id
	m.Services = services
	m.content = map[string]any{
		"key":   key,
		"peer":  peer,
		"after": last,
	}
	m.send_peer(peer)
}

// broadcast_send_ack delivers a broadcast/acknowledge event back to
// the originating host of a broadcast we've just applied. Fired by
// the receiver wrapper in events.go after each successful advance;
// the owner's broadcast_acknowledge handler upserts _acknowledged
// for (key, peer, subscriber=us) and runs broadcast_log_ack_trim,
// which drops _log rows below the slowest subscriber's progress.
//
// Self-loops (peer == p2p_id) are skipped: the owner is its own
// subscriber and already knows its state; the 7d age trim handles
// _log cleanup for self-loop streams without needing a network
// round-trip.
//
// Bursts coalesce within broadcast_acknowledge_coalesce_window per
// (user, key, peer) - a chat full of messages or a fast game's move
// sequence sends one outbound ack per window per stream instead of
// one per applied event. Semantically equivalent because each ack
// carries the latest applied sequence (not a delta); a single ack at
// seq=N is the same as N individual acks at seqs 1..N. The owner
// upserts max(existing, new) in either case.
//
// Fire-and-forget: the flushed message goes to the queue and retries;
// an ack that fails to deliver is harmless because the next applied
// event will trigger a fresh ack carrying an equal-or-higher sequence.
//
// from: the local subscriber entity (e.to of the inbound broadcast —
//
//	the local entity that received the event).
//
// to:   the broadcast owner entity (e.from of the inbound — who
//
//	broadcast it).
//
// peer: the originating libp2p peer ID (e.peer of the inbound — the
//
//	host to send the ack back to).
func broadcast_send_ack(user *User, a *App, from, to, key, peer string, sequence int64) {
	if user == nil || a == nil {
		return
	}
	if from == "" || to == "" || key == "" || peer == "" || sequence <= 0 {
		return
	}
	if peer == p2p_id {
		return
	}
	broadcast_acknowledge_enqueue(user.UID, a.id, from, to, key, peer, sequence)
}

// broadcast_acknowledge_coalesce_window bounds how long a pending ack
// is held before flushing. Larger = more batching; smaller = lower
// latency to the owner's log trim. 250ms means bursty subscribers
// emit one ack per quarter-second per stream; an idle stream sees
// no extra latency because the first applied event after idle starts
// the timer fresh.
const broadcast_acknowledge_coalesce_window = 250 * time.Millisecond

// broadcast_acknowledge_pending holds one pending ack between its
// first scheduling and the timer flush. The pending entry's sequence
// is bumped by later inbound applies to the same (user, key, peer)
// tuple within the coalesce window; the timer always sends the latest.
type broadcast_acknowledge_pending struct {
	user     string
	app      string
	from     string
	to       string
	key      string
	peer     string
	sequence int64
}

var (
	broadcast_acknowledge_lock    sync.Mutex
	broadcast_acknowledge_pending_map = map[string]*broadcast_acknowledge_pending{}
)

// broadcast_acknowledge_enqueue accumulates the latest applied seq for
// one (user, key, peer) tuple and starts a flush timer if none exists.
// Subsequent enqueues within the window bump the sequence and ride the
// existing timer.
func broadcast_acknowledge_enqueue(user, app, from, to, key, peer string, sequence int64) {
	tag := user + "|" + key + "|" + peer
	broadcast_acknowledge_lock.Lock()
	pending, exists := broadcast_acknowledge_pending_map[tag]
	if exists {
		if sequence > pending.sequence {
			pending.sequence = sequence
		}
		broadcast_acknowledge_lock.Unlock()
		return
	}
	broadcast_acknowledge_pending_map[tag] = &broadcast_acknowledge_pending{
		user:     user,
		app:      app,
		from:     from,
		to:       to,
		key:      key,
		peer:     peer,
		sequence: sequence,
	}
	broadcast_acknowledge_lock.Unlock()
	time.AfterFunc(broadcast_acknowledge_coalesce_window, func() {
		broadcast_acknowledge_flush(tag)
	})
}

// broadcast_acknowledge_flush sends the coalesced ack for one tag and
// clears the pending entry. Called from the timer goroutine.
func broadcast_acknowledge_flush(tag string) {
	broadcast_acknowledge_lock.Lock()
	pending := broadcast_acknowledge_pending_map[tag]
	if pending == nil {
		broadcast_acknowledge_lock.Unlock()
		return
	}
	delete(broadcast_acknowledge_pending_map, tag)
	broadcast_acknowledge_lock.Unlock()

	user := user_by_uid(pending.user)
	a := app_by_id(pending.app)
	if user == nil || a == nil {
		return
	}
	services := app_services(a, user)
	service := ""
	if len(services) > 0 {
		service = services[0]
	}
	m := message(pending.from, pending.to, service, "broadcast/acknowledge")
	m.FromApp = pending.app
	m.Services = services
	m.content = map[string]any{
		"key":      pending.key,
		"peer":     pending.peer,
		"sequence": pending.sequence,
	}
	m.send_peer(pending.peer)
}
