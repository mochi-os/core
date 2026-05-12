// Mochi server: PN-counter pattern library helper
// Copyright Alistair Cunningham 2026

package main

import (
	"fmt"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// api_counter exposes mochi.counter.{add,get}.
//
// A PN-counter is a CRDT: each host accumulates its own positive and
// negative contributions in its own row of `_counters(name, peer, pos,
// neg)`, and the logical value of a counter is the sum across all peers.
// Adds are commutative — applying the same delta in any order across
// replicas converges to the same value — so the replication transport's
// at-least-once delivery just works without needing tombstones or row
// versions.
var api_counter = sls.FromStringDict(sl.String("mochi.counter"), sl.StringDict{
	"add": sl.NewBuiltin("mochi.counter.add", api_counter_add),
	"get": sl.NewBuiltin("mochi.counter.get", api_counter_get),
})

// counter_table_create creates `_counters` on the given app DB if it
// doesn't exist. Called lazily on first counter use so apps without
// counters pay nothing.
func counter_table_create(db *DB) {
	db.exec("create table if not exists _counters (name text not null, peer text not null, pos integer not null default 0, neg integer not null default 0, primary key (name, peer))")
}

// counter_local_apply applies a single (peer, delta) update to the
// per-app `_counters` table. Shared between the local-side add and the
// replication-replay apply path so the SQL stays in one place.
func counter_local_apply(db *DB, name, peer string, delta int64) {
	counter_table_create(db)
	if delta > 0 {
		db.exec("insert into _counters (name, peer, pos, neg) values (?, ?, ?, 0) on conflict(name, peer) do update set pos = pos + ?", name, peer, delta, delta)
	} else if delta < 0 {
		db.exec("insert into _counters (name, peer, pos, neg) values (?, ?, 0, ?) on conflict(name, peer) do update set neg = neg + ?", name, peer, -delta, -delta)
	}
}

// mochi.counter.add(name, delta) -> None: add `delta` (positive or
// negative) to the named counter on the calling host's slot.
func api_counter_add(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	var delta int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "delta", &delta); err != nil {
		return nil, err
	}
	if !valid(name, "constant") {
		return sl_error(fn, "invalid name %q", name)
	}
	if delta == 0 {
		return sl.None, nil
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
	counter_local_apply(db, name, p2p_id, delta)

	if user.UID != "" {
		payload := cbor_encode(&CounterDelta{Name: name, Peer: p2p_id, Delta: delta})
		replication_emit(user.UID, &ReplicationOp{
			Scope:    repl_scope_app,
			User:     user.UID,
			Database: app.id,
			Table:    "_counters",
			Kind:     "delta",
			Payload:  payload,
		})
	}

	return sl.None, nil
}

// mochi.counter.get(name) -> int: read the current value of the named
// counter (sum of pos - neg across every peer's slot).
func api_counter_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name); err != nil {
		return nil, err
	}
	if !valid(name, "constant") {
		return sl_error(fn, "invalid name %q", name)
	}

	user, _ := t.Local("user").(*User)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app(user, app)
	if db == nil {
		return sl.MakeInt(0), nil
	}

	// If the table doesn't exist yet (no adds ever made), return 0
	// without creating it — get is a read.
	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='_counters'")
	if !exists {
		return sl.MakeInt(0), nil
	}
	n := db.integer("select coalesce(sum(pos) - sum(neg), 0) from _counters where name=?", name)
	return sl.MakeInt(n), nil
}

// replication_counter_apply lands a remote PN-counter delta into the
// receiving host's app DB. Resolves the user_uid + app id to the
// correct app DB; if either isn't local yet, the op is deferred so the
// caller buffers it in `replication.db.pending`.
func replication_counter_apply(userUID, appID string, d *CounterDelta) ApplyResult {
	if d.Peer == "" {
		return ApplyInvalid
	}

	udb := db_open("db/users.db")
	row, _ := udb.row("select id from users where uid=?", userUID)
	if row == nil {
		return ApplyDeferred
	}
	var localID int
	if v, ok := row["id"].(int64); ok {
		localID = int(v)
	}
	if localID == 0 {
		return ApplyDeferred
	}

	u := user_by_id(localID)
	if u == nil {
		return ApplyDeferred
	}
	a := app_by_id(appID)
	if a == nil {
		return ApplyDeferred
	}

	db := db_app(u, a)
	if db == nil {
		return ApplyDeferred
	}
	counter_local_apply(db, d.Name, d.Peer, d.Delta)
	debug("Replication counter apply: user_uid=%q app=%q name=%q peer=%q delta=%d", userUID, appID, d.Name, d.Peer, d.Delta)
	return ApplyApplied
}

// counter_value_string is exposed for diagnostics so a Starlark caller can
// inspect the per-peer breakdown rather than just the summed value. Not
// part of the public surface — internal-only.
func counter_value_string(db *DB, name string) string {
	rows, _ := db.rows("select peer, pos, neg from _counters where name=? order by peer", name)
	out := ""
	for _, r := range rows {
		peer, _ := r["peer"].(string)
		pos, _ := r["pos"].(int64)
		neg, _ := r["neg"].(int64)
		if out != "" {
			out += " "
		}
		out += fmt.Sprintf("%s:+%d/-%d", peer, pos, neg)
	}
	return out
}
