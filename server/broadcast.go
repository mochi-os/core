// Mochi server: Durable broadcast log for subscriber fan-out
// Copyright Alistair Cunningham 2026

package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

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
	db.exec("insert into _sequence (key, peer, last) values (?, ?, 1) on conflict(key, peer) do update set last = _sequence.last + 1", key, peer)
	return int64(db.integer("select last from _sequence where key=? and peer=?", key, peer))
}

func broadcast_received_get(db *DB, sender, key string) int64 {
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='_received'")
	if !exists {
		return 0
	}
	return int64(db.integer("select last from _received where sender=? and key=?", sender, key))
}

func broadcast_advance_local(db *DB, sender, key string, sequence int64) {
	broadcast_received_table_create(db)
	db.exec("insert into _received (sender, key, last) values (?, ?, ?) on conflict(sender, key) do update set last = max(_received.last, excluded.last)", sender, key, sequence)
}

// broadcast_log_append writes one log row in the same transaction as
// the sequence bump. Returns the allocated sequence.
func broadcast_log_append(db *DB, key, peer, event string, data []byte) int64 {
	broadcast_log_table_create(db)
	broadcast_log_age_trim(db, key, peer)
	sequence := broadcast_next_local(db, key, peer)
	db.exec("insert into _log (key, peer, sequence, event, data, created) values (?, ?, ?, ?, ?, ?)", key, peer, sequence, event, string(data), now())
	return sequence
}

// broadcast_log_age_trim deletes log rows older than the age cap for
// the given (key, peer). Called on send; no-op when nothing's aged out.
func broadcast_log_age_trim(db *DB, key, peer string) {
	db.exec("delete from _log where key=? and peer=? and created < ?", key, peer, now()-broadcast_log_age)
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
	db.exec("delete from _log where key=? and peer=? and sequence < ?", key, peer, last)
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
	e.db.exec("insert into _acknowledged (key, peer, subscriber, last) values (?, ?, ?, ?) on conflict(key, peer, subscriber) do update set last = max(_acknowledged.last, excluded.last)", key, peer, e.from, sequence)
	broadcast_log_ack_trim(e.db, key, peer)
	return nil
}

// broadcast_resync_throttle gates resync requests per (peer, key, user)
// to one every 60 seconds — bursts of out-of-order events should fire a
// single resync, not N. Live across goroutines.
var (
	broadcast_resync_lock sync.Mutex
	broadcast_resync_last = map[string]int64{}
)

func broadcast_resync_throttle(user_uid, peer, key string) bool {
	broadcast_resync_lock.Lock()
	defer broadcast_resync_lock.Unlock()
	now_ts := time.Now().Unix()
	tag := fmt.Sprintf("%s|%s|%s", user_uid, peer, key)
	last := broadcast_resync_last[tag]
	if now_ts-last < 60 {
		return false
	}
	broadcast_resync_last[tag] = now_ts
	return true
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
