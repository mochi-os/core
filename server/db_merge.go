// Mochi server: versioned LWW-Register upsert for replicated app tables.
//
// Apps that keep mutable shared state (membership, subscriptions, roles) in a
// replicated table must not write it with `replace into` / `delete`: those are
// last-ARRIVAL-wins under multi-master and diverge (a concurrent add+remove can
// leave the two hosts disagreeing, or a stale write resurrect a removed row).
// mochi.db.merge / mochi.db.tombstone make such a table a converging register:
// each row carries a per-key Lamport `version` and an originating-host `writer`,
// and the merge keeps the higher version (ties broken deterministically by
// writer), so every host converges regardless of arrival order. A removal is a
// `removed=1` tombstone, never a DELETE, so it is ordered like any other write.
//
// The table must carry `writer text`, `version integer`, `removed integer
// default 0` columns (plus its key + value columns); reads filter `removed=0`.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"regexp"
	"sort"
	"strings"

	sl "go.starlark.net/starlark"
)

var sql_identifier_re = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// sql_identifier_ok guards table/column names that are interpolated into SQL
// (they can't be bound parameters). Apps supply them, so reject anything that
// isn't a plain identifier.
func sql_identifier_ok(s string) bool { return sql_identifier_re.MatchString(s) }

// db_merge_statement builds the versioned conditional upsert: insert the row, or
// on a key conflict update it only when the incoming write wins the total order
// (higher version; tie → higher writer). It's a commutative max, so applying the
// same set of ops in any order on any host converges. Factored out so the merge
// semantics are unit-testable without the Starlark machinery.
func db_merge_statement(table string, keyCols []string, fieldCols []string) string {
	// Quote the caller-supplied identifiers (already sql_identifier_ok-validated,
	// so no embedded quote/NUL is possible) so a column named after a SQL reserved
	// word — e.g. `order` in a register table — is a legal identifier rather than a
	// syntax error. writer/version/removed are our own fixed names, never reserved.
	q := func(s string) string { return `"` + s + `"` }
	qt := q(table)
	insCols := make([]string, 0, len(keyCols)+len(fieldCols)+3)
	for _, c := range keyCols {
		insCols = append(insCols, q(c))
	}
	for _, c := range fieldCols {
		insCols = append(insCols, q(c))
	}
	insCols = append(insCols, "writer", "version", "removed")
	placeholders := make([]string, len(insCols))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	sets := make([]string, 0, len(fieldCols)+3)
	for _, c := range fieldCols {
		sets = append(sets, q(c)+"=excluded."+q(c))
	}
	sets = append(sets, "writer=excluded.writer", "version=excluded.version", "removed=excluded.removed")
	qkeys := make([]string, len(keyCols))
	for i, c := range keyCols {
		qkeys[i] = q(c)
	}
	return "insert into " + qt + " ( " + strings.Join(insCols, ", ") + " ) values ( " + strings.Join(placeholders, ", ") + " )" +
		" on conflict ( " + strings.Join(qkeys, ", ") + " ) do update set " + strings.Join(sets, ", ") +
		" where excluded.version > " + qt + ".version" +
		" or ( excluded.version = " + qt + ".version and excluded.writer > " + qt + ".writer )"
}

// mochi.db.merge(table, keys, row) -> int: Versioned LWW-Register upsert. `keys`
// names the conflict-key columns; `row` is a dict of every column value (keys +
// fields). Converges under multi-master where `replace into` would diverge.
func api_db_merge(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return db_merge_builtin(t, fn, args, 0)
}

// mochi.db.tombstone(table, keys, row) -> int: Versioned removal — writes a
// removed=1 tombstone (the `row` dict need only carry the key columns) instead
// of DELETE, so a stale concurrent write can't resurrect the row and the removal
// converges. Pair with mochi.db.merge.
func api_db_tombstone(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return db_merge_builtin(t, fn, args, 1)
}

func db_merge_builtin(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, removed int) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <table: string>, <keys: list of column names>, <row: dict>")
	}
	table, ok := sl.AsString(args[0])
	if !ok || !sql_identifier_ok(table) {
		return sl_error(fn, "invalid table name")
	}
	keyList, ok := args[1].(*sl.List)
	if !ok || keyList.Len() == 0 {
		return sl_error(fn, "keys must be a non-empty list of column names")
	}
	isKey := map[string]bool{}
	var keyCols []string
	for i := 0; i < keyList.Len(); i++ {
		k, ok := sl.AsString(keyList.Index(i))
		if !ok || !sql_identifier_ok(k) {
			return sl_error(fn, "invalid key column")
		}
		keyCols = append(keyCols, k)
		isKey[k] = true
	}
	rowDict, ok := args[2].(*sl.Dict)
	if !ok {
		return sl_error(fn, "row must be a dict")
	}
	row := map[string]any{}
	for _, it := range rowDict.Items() {
		col, ok := sl.AsString(it[0])
		if !ok || !sql_identifier_ok(col) {
			return sl_error(fn, "invalid column name")
		}
		row[col] = sl_decode(sl.Tuple{it[1]}).([]any)[0]
	}
	keyVals := make([]any, 0, len(keyCols))
	for _, k := range keyCols {
		v, ok := row[k]
		if !ok {
			return sl_error(fn, "row is missing key column %q", k)
		}
		keyVals = append(keyVals, v)
	}
	// Field columns = row minus keys, sorted for a stable statement.
	var fieldCols []string
	for c := range row {
		if !isKey[c] {
			fieldCols = append(fieldCols, c)
		}
	}
	sort.Strings(fieldCols)

	db, err := db_for_thread(t)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	ctx := context.Background()
	conn, err := db.starlark.Connx(ctx)
	if err != nil {
		return sl_error(fn, "database error: %v", err)
	}
	defer conn.Close()

	// Per-key Lamport version: read the highest version seen for this key (its
	// own writes + applied replicated writes) and add one. Computed here and
	// carried as a literal — never recomputed on apply, which would diverge.
	where := make([]string, len(keyCols))
	for i, k := range keyCols {
		where[i] = `"` + k + `"=?`
	}
	var seen int64
	if r, err := conn.QueryContext(ctx, `select coalesce( max( version ), 0 ) from "`+table+`" where `+strings.Join(where, " and "), keyVals...); err == nil {
		if r.Next() {
			r.Scan(&seen)
		}
		r.Close()
	}

	vals := append([]any{}, keyVals...)
	for _, c := range fieldCols {
		vals = append(vals, row[c])
	}
	vals = append(vals, net_id, seen+1, removed)

	u, _ := db_user_for_thread(t)
	app, _ := t.Local("app").(*App)
	var av *AppVersion
	if u != nil && app != nil {
		av = app.active(u)
	}
	suppressed, _ := t.Local("replication_suppressed").(bool)
	affected, recorded, err := db_execute_journal(ctx, conn, db, av, suppressed, db_merge_statement(table, keyCols, fieldCols), vals)
	if err != nil {
		db_starlark_rollback(conn)
		return sl_error(fn, "database error: %v", err)
	}
	if recorded {
		journal_wake(db)
	}
	return sl.MakeInt64(affected), nil
}
