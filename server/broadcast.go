// Mochi server: Subscriber sequence-number pattern library helper
// Copyright Alistair Cunningham 2026

package main

import (
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// api_broadcast exposes mochi.broadcast.{next,received,advance}.
//
// Apps that broadcast to many subscribers (vote-delta fan-out, project
// activity feeds, crm log replication) attach a per-(key, sender)
// monotonic sequence number to every message they send and recipients
// track the highest sequence they've processed for each (sender, key).
// On reconnect a recipient who observes a gap can request a replay of
// the missing range from the sender; that request mechanism is
// app-specific so it's not built into this helper, but the
// gap-detection it enables is.
//
// Sender side:
//
//	seq = mochi.broadcast.next(key)
//	content["_seq"] = seq
//	content["_seq_key"] = key
//	content["_seq_peer"] = mochi.server.id()
//	mochi.message.send.peer(peer, headers, content)
//
// Receiver side:
//
//	seq = event.content("_seq", 0)
//	key = event.content("_seq_key", "")
//	sender = event.content("_seq_peer", "")
//	last = mochi.broadcast.received(sender, key)
//	if seq > last + 1:
//	    # gap detected; request replay (app-specific)
//	process(event)
//	mochi.broadcast.advance(sender, key, seq)
var api_broadcast = sls.FromStringDict(sl.String("mochi.broadcast"), sl.StringDict{
	"next":     sl.NewBuiltin("mochi.broadcast.next", api_broadcast_next),
	"received": sl.NewBuiltin("mochi.broadcast.received", api_broadcast_received),
	"advance":  sl.NewBuiltin("mochi.broadcast.advance", api_broadcast_advance),
})

func broadcast_seq_table_create(db *DB) {
	db.exec("create table if not exists _broadcast_seq (key text not null, peer text not null, last integer not null default 0, primary key (key, peer))")
}

func broadcast_received_table_create(db *DB) {
	db.exec("create table if not exists _broadcast_received (sender text not null, key text not null, last integer not null default 0, primary key (sender, key))")
}

// broadcast_next_local allocates and returns the next outbound sequence
// number on the given DB for (key, peer). Used by api_broadcast_next and
// directly by tests.
func broadcast_next_local(db *DB, key, peer string) int {
	broadcast_seq_table_create(db)
	db.exec("insert into _broadcast_seq (key, peer, last) values (?, ?, 1) on conflict(key, peer) do update set last = _broadcast_seq.last + 1", key, peer)
	return db.integer("select last from _broadcast_seq where key=? and peer=?", key, peer)
}

// broadcast_received_get reads the highest seen sequence from (sender,
// key). Returns 0 when nothing has been received or the table doesn't
// yet exist.
func broadcast_received_get(db *DB, sender, key string) int {
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='_broadcast_received'")
	if !exists {
		return 0
	}
	return db.integer("select last from _broadcast_received where sender=? and key=?", sender, key)
}

// broadcast_advance_local records seq as processed from (sender, key).
// Monotonic — a smaller sequence cannot regress an existing larger one.
func broadcast_advance_local(db *DB, sender, key string, seq int64) {
	broadcast_received_table_create(db)
	db.exec("insert into _broadcast_received (sender, key, last) values (?, ?, ?) on conflict(sender, key) do update set last = max(_broadcast_received.last, excluded.last)", sender, key, seq)
}

// mochi.broadcast.next(key) -> int: allocate the next outbound sequence
// number for (key, this_host). Returns the new value. Always > 0.
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
	return sl.MakeInt(broadcast_next_local(db, key, p2p_id)), nil
}

// mochi.broadcast.received(sender, key) -> int: return the highest
// sequence processed from `sender` for `key`. 0 when no message has
// been received from this sender on this key, or when the receiver-side
// table doesn't exist yet.
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
	return sl.MakeInt(broadcast_received_get(db, sender, key)), nil
}

// mochi.broadcast.advance(sender, key, sequence) -> None: record that
// the app has now processed up to `sequence` from `sender` for `key`.
// Monotonic: a lower sequence cannot regress an already-recorded
// higher one, so out-of-order replay tail messages can't undo state.
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
