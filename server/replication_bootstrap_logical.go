// Mochi server: logical (row-copy) bootstrap engine (#15).
//
// Replaces the physical page-copy bootstrap. The source dumps a database as a
// stream of row batches within one consistent snapshot; the destination
// rebuilds a scratch file by executing parameterised INSERTs, verifies it
// (quick_check + per-table count/checksum), and the caller atomically renames
// it over the live file. Building a fresh file by SQL — never page-copying into
// the live handle — makes the corruption class (truncated/torn lands, WAL
// blow-up) structurally impossible. See claude/plans/replication-logical-bootstrap.md.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
)

// Wire messages. A dump emits one BootstrapSchema, then per replicated table a
// run of BootstrapRowBatch followed by one BootstrapTableDone.
type BootstrapSchema struct {
	Tables  []string `cbor:"tables"`  // CREATE TABLE statements (applied first)
	Views   []string `cbor:"views,omitempty"` // CREATE VIEW statements (applied after tables)
	Indexes []string `cbor:"indexes"` // CREATE INDEX statements (applied after rows)
	Version int      `cbor:"version"` // app schema version
	// AUTOINCREMENT high-water marks (sqlite_sequence: table -> seq). Its sql is
	// NULL so it never appears as a dumped table; restored verbatim after load so
	// the destination doesn't reissue ids the source already consumed.
	Sequences map[string]int64 `cbor:"sequences,omitempty"`
}

type BootstrapRowBatch struct {
	Table   string   `cbor:"table"`
	Columns []string `cbor:"columns"`
	Rows    [][]any  `cbor:"rows"`
	LastKey any      `cbor:"last_key,omitempty"` // resume cursor (highest pk in batch)
}

type BootstrapTableDone struct {
	Table    string `cbor:"table"`
	Count    int64  `cbor:"count"`
	Checksum uint64 `cbor:"checksum"` // order-independent XOR of per-row hashes
}

// bootstrap_db_skip_tables are host-local tables never transferred in a logical
// DB bootstrap — the receiver keeps/recreates its own. journal/journal_delivery
// are the per-DB replication change-capture + delivery bookkeeping.
var bootstrap_db_skip_tables = map[string]bool{
	"journal":          true,
	"journal_delivery": true,
}

// bootstrap_row_hash is an order-independent per-row hash: the same row always
// hashes the same, and a table's checksum is the XOR of its rows' hashes, so
// source and destination agree without forcing identical row order.
func bootstrap_row_hash(values []any) uint64 {
	h := fnv.New64a()
	h.Write(cbor_encode(values))
	return h.Sum64()
}

// bootstrap_quote_ident double-quotes a SQLite identifier (table/column),
// escaping embedded quotes. Identifiers arrive over the wire from an untrusted
// sender, so they are never concatenated raw — always quoted, and rejected if
// they contain a NUL.
func bootstrap_quote_ident(name string) (string, error) {
	if name == "" || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("bootstrap: invalid identifier %q", name)
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`, nil
}

// bootstrap_ddl_allowed restricts executed DDL to CREATE TABLE / CREATE INDEX
// (the only statements a schema dump produces), so a malicious BootstrapSchema
// can't ATTACH, install triggers, or run arbitrary SQL on the scratch build.
func bootstrap_ddl_allowed(ddl, kind string) bool {
	u := strings.ToUpper(strings.TrimSpace(ddl))
	switch kind {
	case "table":
		return strings.HasPrefix(u, "CREATE TABLE")
	case "index":
		return strings.HasPrefix(u, "CREATE INDEX") || strings.HasPrefix(u, "CREATE UNIQUE INDEX")
	case "view":
		return strings.HasPrefix(u, "CREATE VIEW")
	}
	return false
}

// bootstrap_logical_dump reads db within one consistent snapshot and emits the
// schema, then row batches + a table-done per replicated table. Tables in skip
// (and their indexes) are omitted — host-local tables (journal, cursors) are
// never transferred. emit is called synchronously in stream order.
func bootstrap_logical_dump(db *DB, skip map[string]bool, batchSize int, version int, emit func(any) error) error {
	if batchSize <= 0 {
		batchSize = 5000
	}
	tx, err := db.internal.Beginx()
	if err != nil {
		return fmt.Errorf("bootstrap-dump: begin: %w", err)
	}
	defer tx.Rollback()

	schema := BootstrapSchema{Version: version}
	// The app DB schema version lives in pragma user_version (db.go reads/writes
	// it). The row-copy doesn't carry it, so read it here and restore it on the
	// receiver — otherwise every rebuilt DB lands at 0 and the server re-runs
	// database_upgrade from scratch, erroring on non-idempotent migrations
	// (observed live on wasabi: "no such column: category").
	var userVersion int
	if err := tx.QueryRow("pragma user_version").Scan(&userVersion); err == nil {
		schema.Version = userVersion
	}
	var tables []string
	rows, err := tx.Query("select type, name, tbl_name, sql from sqlite_master where type in ('table','index','view') and sql is not null and name not like 'sqlite_%' order by case type when 'table' then 0 else 1 end")
	if err != nil {
		return fmt.Errorf("bootstrap-dump: read schema: %w", err)
	}
	for rows.Next() {
		var typ, name, tbl, ddl string
		if err := rows.Scan(&typ, &name, &tbl, &ddl); err != nil {
			rows.Close()
			return fmt.Errorf("bootstrap-dump: scan schema: %w", err)
		}
		if skip[name] || skip[tbl] {
			continue
		}
		if typ == "table" {
			schema.Tables = append(schema.Tables, ddl)
			tables = append(tables, name)
		} else if typ == "view" {
			schema.Views = append(schema.Views, ddl)
		} else {
			schema.Indexes = append(schema.Indexes, ddl)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	// AUTOINCREMENT high-water marks. sqlite_sequence exists only when the DB has
	// an AUTOINCREMENT table; the query errors otherwise, which we treat as "none".
	if seqRows, err := tx.Query("select name, seq from sqlite_sequence"); err == nil {
		for seqRows.Next() {
			var name string
			var seq int64
			if err := seqRows.Scan(&name, &seq); err == nil && !skip[name] {
				if schema.Sequences == nil {
					schema.Sequences = map[string]int64{}
				}
				schema.Sequences[name] = seq
			}
		}
		seqRows.Close()
	}

	if err := emit(&schema); err != nil {
		return err
	}

	for _, table := range tables {
		if err := bootstrap_dump_table(tx, table, batchSize, emit); err != nil {
			return err
		}
	}
	return nil
}

func bootstrap_dump_table(tx interface {
	Query(string, ...any) (*sql.Rows, error)
}, table string, batchSize int, emit func(any) error) error {
	qt, err := bootstrap_quote_ident(table)
	if err != nil {
		return err
	}
	rows, err := tx.Query("select * from " + qt)
	if err != nil {
		return fmt.Errorf("bootstrap-dump: select %s: %w", table, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	var batch [][]any
	var count int64
	var checksum uint64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := emit(&BootstrapRowBatch{Table: table, Columns: cols, Rows: batch})
		batch = nil
		return err
	}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("bootstrap-dump: scan %s: %w", table, err)
		}
		// Scan may reuse the driver's blob buffer; copy so the batch is stable.
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				c := make([]byte, len(b))
				copy(c, b)
				vals[i] = c
			}
		}
		checksum ^= bootstrap_row_hash(vals)
		count++
		batch = append(batch, vals)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	return emit(&BootstrapTableDone{Table: table, Count: count, Checksum: checksum})
}

// bootstrapLoader rebuilds a scratch database from dump messages. apply() is fed
// each message in stream order; finish() builds the deferred indexes, runs
// quick_check, and verifies every table's count + checksum against the source's
// declared BootstrapTableDone. The scratch file is left ready for the caller to
// atomically rename into place; any error means discard it (the live DB is
// untouched).
type bootstrapLoader struct {
	path     string
	d        *sql.DB
	tx        *sql.Tx
	indexes   []string
	sequences map[string]int64
	version   int
	inserts   map[string]*sql.Stmt
	count    map[string]int64
	checksum map[string]uint64
	expected map[string]BootstrapTableDone
	pending  int
}

const bootstrap_load_commit_every = 20000

func bootstrap_logical_loader(scratchPath string) (*bootstrapLoader, error) {
	_ = os.Remove(scratchPath)
	for _, s := range []string{"-wal", "-shm", "-journal"} {
		_ = os.Remove(scratchPath + s)
	}
	// journal+synchronous off: the scratch file is rebuilt on any failure, so
	// mid-load durability is pointless and these make the bulk load fast.
	//
	// foreign_keys off is REQUIRED, not just an optimisation: a bootstrap is a
	// faithful row-copy of the source's current contents, which may legitimately
	// hold FK-inconsistent rows (data predating a constraint, an app that never
	// enabled enforcement, rows the source's own FK-on connection would now
	// reject). The ncruces driver defaults foreign_keys ON, so without this the
	// loader re-validates every insert and a single dangling reference fails the
	// whole DB — silently dropping it from the transfer, where the old physical
	// page-copy reproduced it byte-for-byte. It also frees us from inserting
	// parent rows before children across tables.
	d, err := sql.Open("sqlite3", "file:"+scratchPath+"?_pragma=journal_mode(off)&_pragma=synchronous(off)&_pragma=foreign_keys(off)")
	if err != nil {
		return nil, err
	}
	d.SetMaxOpenConns(1) // single writer; keeps the prepared inserts on one conn
	return &bootstrapLoader{
		path:     scratchPath,
		d:        d,
		inserts:  map[string]*sql.Stmt{},
		count:    map[string]int64{},
		checksum: map[string]uint64{},
		expected: map[string]BootstrapTableDone{},
	}, nil
}

func (l *bootstrapLoader) apply(msg any) error {
	switch m := msg.(type) {
	case *BootstrapSchema:
		return l.applySchema(m)
	case *BootstrapRowBatch:
		return l.applyBatch(m)
	case *BootstrapTableDone:
		l.expected[m.Table] = *m
		return nil
	default:
		return fmt.Errorf("bootstrap-load: unknown message %T", msg)
	}
}

func (l *bootstrapLoader) applySchema(s *BootstrapSchema) error {
	for _, ddl := range s.Tables {
		if !bootstrap_ddl_allowed(ddl, "table") {
			return fmt.Errorf("bootstrap-load: rejected non-CREATE-TABLE schema statement")
		}
		if _, err := l.d.Exec(ddl); err != nil {
			return fmt.Errorf("bootstrap-load: create table: %w", err)
		}
	}
	// Views are created after tables (they reference them) but carry no row data.
	// The dump ships them because a view otherwise never reaches a replica: the
	// bootstrap only copies table/index DDL + rows, and the replicated schema-version
	// stamp means the app migration that created the view never re-runs here.
	for _, ddl := range s.Views {
		if !bootstrap_ddl_allowed(ddl, "view") {
			return fmt.Errorf("bootstrap-load: rejected non-CREATE-VIEW schema statement")
		}
		if _, err := l.d.Exec(ddl); err != nil {
			return fmt.Errorf("bootstrap-load: create view: %w", err)
		}
	}
	l.indexes = append(l.indexes, s.Indexes...)
	l.sequences = s.Sequences
	l.version = s.Version
	tx, err := l.d.Begin()
	if err != nil {
		return err
	}
	l.tx = tx
	return nil
}

func (l *bootstrapLoader) insertStmt(table string, cols []string) (*sql.Stmt, error) {
	if st, ok := l.inserts[table]; ok {
		return st, nil
	}
	qt, err := bootstrap_quote_ident(table)
	if err != nil {
		return nil, err
	}
	qcols := make([]string, len(cols))
	for i, c := range cols {
		if qcols[i], err = bootstrap_quote_ident(c); err != nil {
			return nil, err
		}
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",")
	// Prepare on the transaction (its connection), not the pool — the pool is
	// pinned to one connection held by the open tx, so a pool-level Prepare
	// would deadlock waiting for a second connection.
	st, err := l.tx.Prepare(fmt.Sprintf("insert into %s (%s) values (%s)", qt, strings.Join(qcols, ","), ph))
	if err != nil {
		return nil, fmt.Errorf("bootstrap-load: prepare insert %s: %w", table, err)
	}
	l.inserts[table] = st
	return st, nil
}

func (l *bootstrapLoader) applyBatch(b *BootstrapRowBatch) error {
	if l.tx == nil {
		return fmt.Errorf("bootstrap-load: batch before schema")
	}
	st, err := l.insertStmt(b.Table, b.Columns)
	if err != nil {
		return err
	}
	for _, row := range b.Rows {
		if len(row) != len(b.Columns) {
			return fmt.Errorf("bootstrap-load: %s row has %d values, %d columns", b.Table, len(row), len(b.Columns))
		}
		// CBOR decodes a non-negative integer into uint64, but SQLite values
		// are int64; normalise so the stored value and the checksum match the
		// source (which hashed int64s straight from the scan).
		for i, v := range row {
			if u, ok := v.(uint64); ok {
				row[i] = int64(u)
			}
		}
		if _, err := st.Exec(row...); err != nil {
			return fmt.Errorf("bootstrap-load: insert %s: %w", b.Table, err)
		}
		l.count[b.Table]++
		l.checksum[b.Table] ^= bootstrap_row_hash(row)
		l.pending++
	}
	if l.pending >= bootstrap_load_commit_every {
		return l.rollover()
	}
	return nil
}

// commitCurrent closes the tx-prepared inserts and commits the current batch
// transaction. The inserts re-prepare lazily on the next transaction.
func (l *bootstrapLoader) commitCurrent() error {
	for _, st := range l.inserts {
		st.Close()
	}
	l.inserts = map[string]*sql.Stmt{}
	err := l.tx.Commit()
	l.tx = nil
	return err
}

func (l *bootstrapLoader) rollover() error {
	if err := l.commitCurrent(); err != nil {
		return err
	}
	l.pending = 0
	tx, err := l.d.Begin()
	if err != nil {
		return err
	}
	l.tx = tx
	return nil
}

// finish commits the final batch, builds indexes, runs quick_check, and verifies
// every table's count + checksum. Closes the loader either way.
func (l *bootstrapLoader) finish() error {
	err := l.finishWork()
	l.d.Close()
	return err
}

func (l *bootstrapLoader) finishWork() error {
	if l.tx != nil {
		if err := l.commitCurrent(); err != nil {
			return err
		}
	}
	for _, idx := range l.indexes {
		if !bootstrap_ddl_allowed(idx, "index") {
			return fmt.Errorf("bootstrap-load: rejected non-CREATE-INDEX schema statement")
		}
		if _, err := l.d.Exec(idx); err != nil {
			return fmt.Errorf("bootstrap-load: create index: %w", err)
		}
	}
	// Restore AUTOINCREMENT high-water marks after the data load (the inserts
	// themselves only bump sqlite_sequence to max(id), which understates a
	// source whose top rows were since deleted). sqlite_sequence exists because
	// any table here is AUTOINCREMENT; INSERT OR REPLACE seeds or overrides.
	for name, seq := range l.sequences {
		// sqlite_sequence has no unique constraint on name, so INSERT OR REPLACE
		// would append a duplicate rather than overwrite the auto-inserted row.
		// Clear then insert to leave exactly one row at the source's value.
		if _, err := l.d.Exec("delete from sqlite_sequence where name=?", name); err != nil {
			return fmt.Errorf("bootstrap-load: clear sequence %q: %w", name, err)
		}
		if _, err := l.d.Exec("insert into sqlite_sequence (name, seq) values (?, ?)", name, seq); err != nil {
			return fmt.Errorf("bootstrap-load: restore sequence %q: %w", name, err)
		}
	}
	// Restore the app DB schema version so the receiver doesn't re-run
	// database_upgrade from 0. PRAGMA takes no bind params; version is an int
	// read from the source, so the format is safe.
	if l.version != 0 {
		if _, err := l.d.Exec(fmt.Sprintf("pragma user_version = %d", l.version)); err != nil {
			return fmt.Errorf("bootstrap-load: restore user_version: %w", err)
		}
	}
	var qc string
	if err := l.d.QueryRow("pragma quick_check").Scan(&qc); err != nil {
		return fmt.Errorf("bootstrap-load: quick_check: %w", err)
	}
	if qc != "ok" {
		return fmt.Errorf("bootstrap-load: rebuilt db failed quick_check: %s", qc)
	}
	for table, exp := range l.expected {
		if l.count[table] != exp.Count {
			return fmt.Errorf("bootstrap-load: %s row count %d != declared %d", table, l.count[table], exp.Count)
		}
		if l.checksum[table] != exp.Checksum {
			return fmt.Errorf("bootstrap-load: %s checksum mismatch", table)
		}
	}
	return nil
}
