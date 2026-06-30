// Mochi server: versioned replicated registers
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "strings"

// A replicated register is a keyed user.db/app.db table whose rows are mutable
// state a user can write from any of their hosts. Like the access and permissions
// tables, every row carries a Lamport `version`, an originating-host `writer`, and
// a `deleted` tombstone, so concurrent writes from different hosts converge
// deterministically — a higher version wins; on a tie the higher writer wins — and
// a delete is a tombstone a stale write cannot silently resurrect. Every mutation
// is a whole-row write at version seen+1 (the caller read-modify-writes the row),
// so a partial-column update is just a whole-row write with one field changed.
//
// A register with NO payload columns (keys only) is a convergent set: an insert
// means "member present" (deleted=0) and register_remove means "member absent"
// (deleted=1), with add/remove races settled by the same (version, writer) order.
//
// access/permissions predate this and keep their own bespoke upsert — they add a
// fail-closed deny-beats-allow tie-break this generic register has no concept of.
// Everything else routes through here.
type register_def struct {
	table   string   // table name
	keys    []string // primary-key columns, in PK order
	payload []string // remaining replicated columns (may be empty: a set)
}

// register_columns adds the deleted/writer/revision bookkeeping columns to a legacy
// register table in place. The table's primary key is already its natural key, so
// (unlike access/permissions, which had to drop an autoincrement id) no rebuild is
// needed. The Lamport counter is named `revision`, not `version`, on purpose: the
// versions table already has a domain `version` column, and a name clash would make
// register_columns silently skip the bookkeeping column and compare the domain value
// instead. There is deliberately no write-time column — revision+writer settle every
// conflict — so a table's own domain timestamp (groups.created, interests.updated)
// stays ordinary payload and is never clobbered. Idempotent; safe to call on every
// open.
func (db *DB) register_columns(d register_def) {
	for _, c := range []struct{ name, definition string }{
		{"deleted", "integer not null default 0"},
		{"writer", "text not null default ''"},
		{"revision", "integer not null default 1"},
	} {
		if !db.has_column(d.table, c.name) {
			db.exec("alter table " + d.table + " add column " + c.name + " " + c.definition)
		}
	}
}

// register_sql builds the deterministic versioned-upsert statement for a register.
// Column names come only from the register_def (code constants, never user input)
// and are double-quoted so a column that is a SQL keyword (e.g. accounts."default")
// is handled.
func register_sql(d register_def) string {
	cols := append(append([]string{}, d.keys...), d.payload...)
	all := make([]string, 0, len(cols)+3)
	for _, c := range cols {
		all = append(all, q(c))
	}
	all = append(all, q("deleted"), q("writer"), q("revision"))
	keys := make([]string, len(d.keys))
	for i, k := range d.keys {
		keys[i] = q(k)
	}
	set := make([]string, 0, len(d.payload)+3)
	for _, c := range d.payload {
		set = append(set, q(c)+"=excluded."+q(c))
	}
	set = append(set, q("deleted")+"=excluded."+q("deleted"), q("writer")+"=excluded."+q("writer"), q("revision")+"=excluded."+q("revision"))
	t := d.table
	return "insert into " + t + " ( " + strings.Join(all, ", ") + " ) values ( " +
		strings.Repeat("?, ", len(all)-1) + "? ) " +
		"on conflict ( " + strings.Join(keys, ", ") + " ) do update set " + strings.Join(set, ", ") +
		" where excluded." + q("revision") + " > " + t + "." + q("revision") +
		" or ( excluded." + q("revision") + " = " + t + "." + q("revision") + " and excluded." + q("writer") + " > " + t + "." + q("writer") + " )"
}

// q double-quotes a SQL identifier so reserved words (e.g. "default") are safe.
func q(identifier string) string { return `"` + identifier + `"` }

// register_args orders the bound values to match register_sql: keys, payload, then
// deleted, writer, version.
func register_args(d register_def, vals map[string]any, deleted int, writer string, version int64) []any {
	cols := append(append([]string{}, d.keys...), d.payload...)
	args := make([]any, 0, len(cols)+3)
	for _, c := range cols {
		args = append(args, vals[c])
	}
	return append(args, deleted, writer, version)
}

// register_write applies one whole-row versioned write and replicates it. vals must
// hold every key and payload column. deleted=1 writes a tombstone. The version is
// computed here (seen+1) and carried as a literal so apply is deterministic on
// every host — never recomputed on the replica.
func (db *DB) register_write(d register_def, vals map[string]any, deleted int) {
	where, args := d.predicate(vals)
	var seen struct{ Version int64 }
	db.scan(&seen, "select coalesce( max( revision ), 0 ) as version from "+d.table+" where "+where, args...)
	db.exec_replicated(register_sql(d), register_args(d, vals, deleted, net_id, seen.Version+1)...)
}

// register_remove tombstones a row (deleted=1) at version seen+1. Payload columns
// are irrelevant once deleted (reads filter deleted=0), so any not supplied are
// zeroed; a later higher-version write revives the row with fresh payload.
func (db *DB) register_remove(d register_def, keyvals map[string]any) {
	vals := map[string]any{}
	for k, v := range keyvals {
		vals[k] = v
	}
	for _, p := range d.payload {
		if _, ok := vals[p]; !ok {
			vals[p] = ""
		}
	}
	db.register_write(d, vals, 1)
}

// predicate builds the "key1=? and key2=? …" clause and its args for a row's keys.
func (d register_def) predicate(vals map[string]any) (string, []any) {
	parts := make([]string, len(d.keys))
	args := make([]any, len(d.keys))
	for i, k := range d.keys {
		parts[i] = q(k) + "=?"
		args[i] = vals[k]
	}
	return strings.Join(parts, " and "), args
}
