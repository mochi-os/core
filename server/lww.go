// Mochi server: Last-write-wins register pattern library helper
// Copyright Alistair Cunningham 2026

package main

import (
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// api_lww exposes mochi.lww.{set,get}.
//
// An LWW register stores one value per logical (tbl, row, field) slot;
// concurrent writes from different hosts converge on the one with the
// higher (ts, peer-id) pair. The `_lww` table is per-app, lazily created
// on first use. Apps that have a few fields needing LWW use this helper
// directly; apps with many LWW columns can model the field as a derived
// view backed by `_lww` rows.
var api_lww = sls.FromStringDict(sl.String("mochi.lww"), sl.StringDict{
	"set": sl.NewBuiltin("mochi.lww.set", api_lww_set),
	"get": sl.NewBuiltin("mochi.lww.get", api_lww_get),
})

// lww_table_create creates `_lww` on the given app DB if it doesn't
// exist. Schema kept minimal: keys, value, and the (ts, peer) tiebreak.
func lww_table_create(db *DB) {
	db.exec("create table if not exists _lww (tbl text not null, row text not null, field text not null, value text not null, ts integer not null, peer text not null, primary key (tbl, row, field))")
}

// lww_local_apply writes (or refuses to overwrite, when stale) a single
// (tbl, row, field, value, ts, peer) into `_lww`. The ON CONFLICT clause
// uses SQLite's row-value comparison: `(excluded.ts, excluded.peer) >
// (_lww.ts, _lww.peer)` resolves lex-on-peer as a deterministic tiebreak
// when timestamps collide.
func lww_local_apply(db *DB, tbl, row, field, value string, ts int64, peer string) {
	lww_table_create(db)
	db.exec(`insert into _lww (tbl, row, field, value, ts, peer) values (?, ?, ?, ?, ?, ?)
		on conflict(tbl, row, field) do update set value=excluded.value, ts=excluded.ts, peer=excluded.peer
		where (excluded.ts, excluded.peer) > (_lww.ts, _lww.peer)`,
		tbl, row, field, value, ts, peer)
}

// mochi.lww.set(tbl, row, field, value) -> None: write `value` to the
// (tbl, row, field) register with the calling host's timestamp.
func api_lww_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var tbl, row, field, value string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "tbl", &tbl, "row", &row, "field", &field, "value", &value); err != nil {
		return nil, err
	}
	if !valid(tbl, "constant") || !valid(field, "constant") {
		return sl_error(fn, "invalid tbl/field")
	}
	if row == "" {
		return sl_error(fn, "empty row key")
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

	ts := now()
	lww_local_apply(db, tbl, row, field, value, ts, p2p_id)

	if user.UID != "" {
		payload := cbor_encode(&LWWSet{
			Tbl: tbl, Row: row, Field: field, Value: value, TS: ts, Peer: p2p_id,
		})
		replication_emit(user.UID, &ReplicationOp{
			Scope:    repl_scope_app,
			User:     user.UID,
			Database: app.id,
			Table:    "_lww",
			Kind:     "set",
			Payload:  payload,
		})
	}

	return sl.None, nil
}

// mochi.lww.get(tbl, row, field) -> string | None: read the current
// value of the (tbl, row, field) register. Returns None if no value has
// ever been set.
func api_lww_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var tbl, row, field string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "tbl", &tbl, "row", &row, "field", &field); err != nil {
		return nil, err
	}

	user, _ := t.Local("user").(*User)
	app, _ := t.Local("app").(*App)
	if user == nil || app == nil {
		return sl_error(fn, "no user/app context")
	}

	db := db_app(user, app)
	if db == nil {
		return sl.None, nil
	}

	exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='_lww'")
	if !exists {
		return sl.None, nil
	}
	r, _ := db.row("select value from _lww where tbl=? and row=? and field=?", tbl, row, field)
	if r == nil {
		return sl.None, nil
	}
	if v, ok := r["value"].(string); ok {
		return sl.String(v), nil
	}
	return sl.None, nil
}

// replication_lww_apply lands a remote LWW write into the receiving
// host's app DB; the per-row LWW resolution is done by SQL.
func replication_lww_apply(userUID, appID string, s *LWWSet) ApplyResult {
	if s.Tbl == "" || s.Field == "" || s.Peer == "" {
		return ApplyInvalid
	}

	u := user_by_uid(userUID)
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
	lww_local_apply(db, s.Tbl, s.Row, s.Field, s.Value, s.TS, s.Peer)
	debug("Replication lww apply: user_uid=%q app=%q tbl=%q row=%q field=%q peer=%q ts=%d", userUID, appID, s.Tbl, s.Row, s.Field, s.Peer, s.TS)
	return ApplyApplied
}
