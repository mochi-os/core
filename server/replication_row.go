// Mochi server: app-DB row replication helper
// Copyright Alistair Cunningham 2026
//
// Provides mochi.replication.row.{set,delete} for apps that want
// every-row replication without per-table CRDT classification. The
// helper does the local INSERT OR REPLACE / DELETE, then emits an
// AppRow op to every peer in the user's host set; receivers re-apply
// the same write against their copy of the (user, app) DB.
//
// Conflict resolution: simple last-applied-wins via INSERT OR REPLACE.
// Two hosts writing the same row at the same time end up with the
// later op's content on every replica (where "later" is per-receiver
// arrival order, not per-source-timestamp). Apps that need true LWW
// with (ts, peer) tiebreak should use mochi.lww instead.
//
// Not for: append-only logs where every write must survive (use a
// log helper — TODO #1, not yet built). Tables with NOW() / random()
// embedded in the SQL (the op replays the call-site SQL but with the
// arguments captured at emit time, so apps must compute timestamps in
// Starlark and pass them in as columns).

package main

import (
	"fmt"
	"sort"
	"strings"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// AppRow is the wire payload for a per-user-app DB row replication
// op. Mirrors SystemRow's shape but lands in users/<u>/<app>/db/<app>.db
// instead of sysdb files; the per-row apply path is the same
// INSERT OR REPLACE / DELETE pattern.
type AppRow struct {
	Table  string            `cbor:"table"`
	Key    map[string]string `cbor:"key"`
	Cols   map[string]string `cbor:"cols,omitempty"`
	Delete bool              `cbor:"delete,omitempty"`
}

// api_replication_row exposes mochi.replication.row.{set,delete}.
var api_replication_row = sls.FromStringDict(sl.String("mochi.replication.row"), sl.StringDict{
	"set":    sl.NewBuiltin("mochi.replication.row.set", api_replication_row_set),
	"delete": sl.NewBuiltin("mochi.replication.row.delete", api_replication_row_delete),
})

// mochi.replication.row.set(table, key, cols) -> None
//
// Inserts (or replaces) a row in the calling app's DB and emits an
// AppRow op to every peer in the user's host set so each replica
// applies the same write. Idempotent: re-applying the same op yields
// the same row.
//
// table: SQL table name in the app DB.
// key:   dict of primary-key columns (one or more; receiver builds the
//        WHERE clause for the conflict target).
// cols:  dict of remaining columns being written. May be empty when
//        the table has only key columns (rare but legal).
func api_replication_row_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var table string
	var keyDict, colsDict *sl.Dict
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "table", &table, "key", &keyDict, "cols?", &colsDict); err != nil {
		return nil, err
	}
	if !valid(table, "constant") {
		return sl_error(fn, "invalid table name %q", table)
	}
	key, err := dict_to_string_map(keyDict)
	if err != nil {
		return sl_error(fn, "invalid key: %v", err)
	}
	if len(key) == 0 {
		return sl_error(fn, "empty key")
	}
	var cols map[string]string
	if colsDict != nil {
		cols, err = dict_to_string_map(colsDict)
		if err != nil {
			return sl_error(fn, "invalid cols: %v", err)
		}
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
	if err := replication_row_apply_local(db, table, key, cols, false); err != nil {
		return sl_error(fn, "local apply: %v", err)
	}

	if user.UID != "" {
		payload := cbor_encode(&AppRow{
			Table:  table,
			Key:    key,
			Cols:   cols,
			Delete: false,
		})
		replication_emit(user.UID, &ReplicationOp{
			Scope:    repl_scope_app,
			User:     user.UID,
			Database: app.id,
			Table:    table,
			Kind:     "row.set",
			Payload:  payload,
		})
	}

	return sl.None, nil
}

// mochi.replication.row.delete(table, key) -> None
//
// Deletes the matching row(s) and emits an AppRow delete op to peers.
func api_replication_row_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var table string
	var keyDict *sl.Dict
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "table", &table, "key", &keyDict); err != nil {
		return nil, err
	}
	if !valid(table, "constant") {
		return sl_error(fn, "invalid table name %q", table)
	}
	key, err := dict_to_string_map(keyDict)
	if err != nil {
		return sl_error(fn, "invalid key: %v", err)
	}
	if len(key) == 0 {
		return sl_error(fn, "empty key")
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
	if err := replication_row_apply_local(db, table, key, nil, true); err != nil {
		return sl_error(fn, "local apply: %v", err)
	}

	if user.UID != "" {
		payload := cbor_encode(&AppRow{
			Table:  table,
			Key:    key,
			Delete: true,
		})
		replication_emit(user.UID, &ReplicationOp{
			Scope:    repl_scope_app,
			User:     user.UID,
			Database: app.id,
			Table:    table,
			Kind:     "row.delete",
			Payload:  payload,
		})
	}

	return sl.None, nil
}

// replication_row_apply_local performs the SQL write against `db`. For
// set, builds an INSERT OR REPLACE with key+cols columns. For delete,
// builds a DELETE WHERE on the key columns. Validates column names so
// the receiver-side handler can't be used to inject arbitrary SQL.
func replication_row_apply_local(db *DB, table string, key, cols map[string]string, del bool) error {
	if !valid(table, "constant") {
		return fmt.Errorf("invalid table %q", table)
	}
	keys := make([]string, 0, len(key))
	for k := range key {
		if !valid(k, "constant") {
			return fmt.Errorf("invalid key column %q", k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if del {
		where := make([]string, len(keys))
		whereArgs := make([]any, len(keys))
		for i, k := range keys {
			where[i] = fmt.Sprintf("%s = ?", k)
			whereArgs[i] = key[k]
		}
		sqlStr := fmt.Sprintf("delete from %s where %s", table, strings.Join(where, " and "))
		db.exec(sqlStr, whereArgs...)
		return nil
	}

	colKeys := make([]string, 0, len(cols))
	for c := range cols {
		if !valid(c, "constant") {
			return fmt.Errorf("invalid column %q", c)
		}
		colKeys = append(colKeys, c)
	}
	sort.Strings(colKeys)

	allCols := append([]string{}, keys...)
	allCols = append(allCols, colKeys...)
	placeholders := strings.Repeat("?, ", len(allCols))
	placeholders = strings.TrimSuffix(placeholders, ", ")
	allArgs := make([]any, 0, len(allCols))
	for _, k := range keys {
		allArgs = append(allArgs, key[k])
	}
	for _, c := range colKeys {
		allArgs = append(allArgs, cols[c])
	}
	sqlStr := fmt.Sprintf("insert or replace into %s (%s) values (%s)",
		table, strings.Join(allCols, ", "), placeholders)
	db.exec(sqlStr, allArgs...)
	return nil
}

// replication_row_apply lands a remote AppRow op into the receiver's
// copy of the (user, app) DB. Defers if the user or the app isn't yet
// local (waiting on a keys-transfer or app install).
func replication_row_apply(userUID, appID string, r *AppRow) ApplyResult {
	if r.Table == "" || len(r.Key) == 0 {
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
	if err := replication_row_apply_local(db, r.Table, r.Key, r.Cols, r.Delete); err != nil {
		info("Replication row apply failed: user=%q app=%q table=%q: %v",
			userUID, appID, r.Table, err)
		return ApplyInvalid
	}
	debug("Replication row apply: user=%q app=%q table=%q delete=%v",
		userUID, appID, r.Table, r.Delete)
	return ApplyApplied
}

// dict_to_string_map converts a Starlark dict whose values are all
// strings/ints/bools into a Go map[string]string. Ints are decimal-
// formatted; bools become "0"/"1". Returns an error for unsupported
// value types.
func dict_to_string_map(d *sl.Dict) (map[string]string, error) {
	if d == nil {
		return nil, nil
	}
	out := make(map[string]string, d.Len())
	for _, item := range d.Items() {
		k, ok := sl.AsString(item[0])
		if !ok {
			return nil, fmt.Errorf("non-string key")
		}
		switch v := item[1].(type) {
		case sl.String:
			out[k] = string(v)
		case sl.Int:
			out[k] = v.String()
		case sl.Bool:
			if v {
				out[k] = "1"
			} else {
				out[k] = "0"
			}
		case sl.NoneType:
			out[k] = ""
		default:
			return nil, fmt.Errorf("unsupported type for key %q", k)
		}
	}
	return out, nil
}
