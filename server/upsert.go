// Mochi server: generic keyed-table upserts
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "strings"

// upsert_def describes a keyed user.db/app.db table written through the generic
// helpers below: insert-or-update on the primary key (row_write) and delete by
// key (row_remove).
type upsert_def struct {
	table   string   // table name
	keys    []string // primary-key columns, in PK order
	payload []string // remaining columns (may be empty)
}

// upsert_sql builds the upsert statement for a def. Column names come only from
// the def (code constants, never user input) and are double-quoted so a column
// that is a SQL keyword (e.g. accounts."default") is handled.
func upsert_sql(d upsert_def) string {
	cols := append(append([]string{}, d.keys...), d.payload...)
	all := make([]string, 0, len(cols))
	for _, c := range cols {
		all = append(all, q(c))
	}
	keys := make([]string, len(d.keys))
	for i, k := range d.keys {
		keys[i] = q(k)
	}
	statement := "insert into " + d.table + " ( " + strings.Join(all, ", ") + " ) values ( " +
		strings.Repeat("?, ", len(all)-1) + "? ) on conflict ( " + strings.Join(keys, ", ") + " ) "
	if len(d.payload) == 0 {
		return statement + "do nothing"
	}
	set := make([]string, 0, len(d.payload))
	for _, c := range d.payload {
		set = append(set, q(c)+"=excluded."+q(c))
	}
	return statement + "do update set " + strings.Join(set, ", ")
}

// q double-quotes a SQL identifier so reserved words (e.g. "default") are safe.
func q(identifier string) string { return `"` + identifier + `"` }

// row_write upserts one whole row. vals must hold every key and payload column.
func (db *DB) row_write(d upsert_def, vals map[string]any) {
	cols := append(append([]string{}, d.keys...), d.payload...)
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		args = append(args, vals[c])
	}
	db.exec(upsert_sql(d), args...)
}

// row_remove deletes a row by key.
func (db *DB) row_remove(d upsert_def, keyvals map[string]any) {
	parts := make([]string, len(d.keys))
	args := make([]any, len(d.keys))
	for i, k := range d.keys {
		parts[i] = q(k) + "=?"
		args[i] = keyvals[k]
	}
	db.exec("delete from "+d.table+" where "+strings.Join(parts, " and "), args...)
}
