// Mochi server: logical bootstrap atomic cutover (#15).
//
// Replaces a live database with a freshly-built, verified scratch file by
// renaming it into place and swapping the cached *DB object — never by
// page-copying into the live handle (which is the corruption/WAL-blow-up
// source). Borrowers holding the old handle keep serving the old inode (no
// panic, no error); new db_open callers get the new file. The old pools are
// closed after a grace period, by which time no borrower still holds them.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
)

// bootstrap_swap_grace is how long the old pools stay open after a swap before
// being closed. Comfortably longer than any single DB operation, so a borrower
// that fetched the old handle just before the swap finishes on the old inode
// rather than hitting a closed pool.
var bootstrap_swap_grace = 120 * time.Second

// bootstrap_db_swap atomically replaces the live database at target with the
// verified scratch file, and re-points the open-DB cache at fresh pools on the
// new file. target is an absolute path (the cache key the bootstrap opened it
// under). Holds databases_lock for the rename + reopen so a concurrent db_open
// can't observe a half-swapped state.
func bootstrap_db_swap(target, scratch string) error {
	// Row-level SUBSET guard (#101). The count-based no-shrink guard below still
	// misses the same-count-different-rows case: a target holding rows whose keys the
	// source lacks would pass it and be silently wiped by the swap. Verify every row
	// in the live target — by primary key — exists in the scratch (the source's copy)
	// before swapping; a catch-up that merely updates shared keys still passes. This is the gate that makes an AUTOMATIC reseed safe — it
	// refuses the swap (so the reseed aborts and falls back to the operator) whenever
	// the target uniquely holds data. FAIL-CLOSED: any error refuses, because a false
	// "subset" verdict would auto-wipe data (#42). Run BEFORE databases_lock so the
	// full scan doesn't stall every db_open; only for a populated target (a fresh
	// replica's empty target has nothing to lose, and the #27 guard covers empty).
	if db_file_has_rows(target) {
		if ok, detail := bootstrap_swap_subset_ok(target, scratch); !ok {
			_ = os.Remove(scratch)
			return fmt.Errorf("bootstrap-db-swap: refusing reseed of %q — the live target is NOT a subset of the source (%s); a swap would lose data the target uniquely holds (subset guard #101)", target, detail)
		}
	}

	databases_lock.Lock()
	defer databases_lock.Unlock()

	// Defense-in-depth data-loss guard (#27): never atomically replace a
	// populated live DB with an EMPTY scratch. The wiped-replica recovery guard
	// blocks the known trigger; this blocks the catastrophe at the swap itself,
	// whatever path requested it (reseed, a future bidirectional reconcile). A
	// genuinely fresh replica's live DB is empty, so legitimate first-bootstrap
	// swaps still pass.
	if db_file_has_rows(target) && !db_file_has_rows(scratch) {
		_ = os.Remove(scratch)
		return fmt.Errorf("bootstrap-db-swap: refusing to replace populated %q with an empty scratch (data-loss guard #27)", target)
	}

	// Defense-in-depth NO-SHRINK guard (#42): the empty-scratch guard above
	// misses a scratch that holds a row or two but is missing the BULK of the
	// populated target — exactly how a cursor-misaligned reseed from a
	// near-empty replica overwrote 1.56M rows on the live primary (the reseed's
	// own gate trusted the misaligned cursors and wrongly judged the empty side
	// authoritative). This guard reads ACTUAL replicated DATA rows (journal/
	// journal_delivery excluded, since the scratch omits them), so a misaligned
	// cursor can't fool it. A legitimate catch-up reseed brings MORE rows (the
	// receiver was behind) and passes; only a wrong-direction / near-empty
	// source — which would more than halve a populated target — is refused.
	if tr := db_file_data_rows(target); tr > 0 {
		if sr := db_file_data_rows(scratch); sr >= 0 && sr*2 < tr {
			_ = os.Remove(scratch)
			return fmt.Errorf("bootstrap-db-swap: refusing to shrink populated %q from %d to %d data rows (no-shrink guard #42)", target, tr, sr)
		}
	}

	// Find the existing cache entry for this file (keyed by path for bootstrap
	// targets, but scan by path so a custom cache key is still caught).
	var oldDB *DB
	oldKey := target
	for k, d := range databases {
		if d.path == target {
			oldDB = d
			oldKey = k
			break
		}
	}

	// Replace the file content. The old DB's open fds keep serving the old
	// inode, so in-flight borrowers are unaffected.
	if err := os.Rename(scratch, target); err != nil {
		return fmt.Errorf("bootstrap-swap: rename %q -> %q: %w", scratch, target, err)
	}
	// The old inode's WAL/SHM still sit at target-wal / target-shm on disk; the
	// freshly-renamed content has none. Remove them so the new pool can't try to
	// recover a stale WAL over the new file. The old DB keeps its own open fds.
	_ = os.Remove(target + "-wal")
	_ = os.Remove(target + "-shm")

	// A fresh, verified copy is now in place — lift any corruption quarantine on
	// this path so background ops resume on it.
	db_quarantine_clear(target)

	internal_db, err := sqlitedrv.Open(target, db_setup_conn)
	if err != nil {
		return fmt.Errorf("bootstrap-swap: open internal pool %q: %w", target, err)
	}
	starlark_db, err := sqlitedrv.Open(target, db_setup_conn_starlark)
	if err != nil {
		internal_db.Close()
		return fmt.Errorf("bootstrap-swap: open starlark pool %q: %w", target, err)
	}
	newDB := &DB{
		key:      oldKey,
		path:     target,
		internal: sqlx.NewDb(internal_db, "sqlite3"),
		starlark: sqlx.NewDb(starlark_db, "sqlite3"),
	}
	// The rebuilt file omits the per-app journal table (skipped on the wire —
	// the receiver owns its own change-capture), so re-create it on the new
	// handle now. The cached handle means the next db_app open is reused and
	// won't run journal_setup, so a missing table would otherwise fail the next
	// replicated write with "no such table: journal" (#424).
	newDB.journal_setup()
	databases[oldKey] = newDB

	if oldDB != nil {
		go func() {
			time.Sleep(bootstrap_swap_grace)
			oldDB.internal.Close()
			oldDB.starlark.Close()
		}()
	}
	return nil
}

// bootstrap_swap_subset_ok reports whether every replicated row in the live target DB
// — identified by its PRIMARY KEY — also exists in the scratch (the source's freshly-
// built copy). True ⇒ the reseed target<-source loses no ROW the target uniquely
// holds, so it is safe to swap. This is the gate that lets an unfillable gap
// auto-reseed without the #42 wipe risk.
//
// Identity is the primary key, NOT the full row content, on purpose: a legitimate
// catch-up reseed UPDATES rows (same key, newer value on the source), which must pass
// — a content match would refuse every catch-up. The two concerns a key match leaves
// open are handled at their own layers: a stale local EDIT being overwritten is gated
// by `local_writes_present` at the reseed trigger (don't reseed with un-shipped local
// writes), and value freshness / reverts are the anti-revert gate's domain (#62). What
// THIS guard prevents is the structural #42 loss — a target row whose key the source
// simply does not have. Tables with no primary key fall back to a full-content
// identity (host-local columns excluded), which is conservative.
//
// FAIL-CLOSED: returns false (refuse the swap) on ANY error — unreadable DB/table, a
// table the scratch lacks, or a target key absent from the scratch — because a false
// "subset" verdict would auto-wipe data. Host-local tables are skipped
// (audit_table_replicates). Target and scratch are read the SAME way (db_file_rows) so
// identical keys hash identically regardless of the driver's text/blob typing.
func bootstrap_swap_subset_ok(target, scratch string) (bool, string) {
	rel := filepath.ToSlash(strings.TrimPrefix(target, filepath.Clean(data_dir)+string(filepath.Separator)))
	localTables, localColumns := audit_excludes_for_path(rel)

	tables, err := db_file_rows(target, "select name from sqlite_master where type='table' and name not like 'sqlite_%'")
	if err != nil {
		return false, "list target tables: " + err.Error()
	}
	for _, tr := range tables {
		name, _ := tr["name"].(string)
		if !audit_table_replicates(name, localTables) {
			continue
		}

		// Row identity = the primary-key columns (in key order), or the whole row
		// (host-local columns excluded) when the table has no PK.
		pkrows, err := db_file_rows(target, "select name from pragma_table_info('"+name+"') where pk > 0 order by pk")
		if err != nil {
			return false, "pk of " + name + ": " + err.Error()
		}
		var pk []string
		for _, p := range pkrows {
			if n, _ := p["name"].(string); n != "" {
				pk = append(pk, n)
			}
		}
		exclude := localColumns[name]
		identity := func(r map[string]any) [sha256.Size]byte {
			if len(pk) == 0 {
				return audit_row_hash(r, exclude)
			}
			key := make(map[string]any, len(pk))
			for _, c := range pk {
				key[c] = r[c]
			}
			return audit_row_hash(key, nil)
		}

		srows, err := db_file_rows(scratch, "select * from \""+name+"\"")
		if err != nil {
			return false, "read scratch." + name + ": " + err.Error()
		}
		have := make(map[[sha256.Size]byte]int, len(srows))
		for _, r := range srows {
			have[identity(r)]++
		}

		trows, err := db_file_rows(target, "select * from \""+name+"\"")
		if err != nil {
			return false, "read target." + name + ": " + err.Error()
		}
		for _, r := range trows {
			h := identity(r)
			if have[h] <= 0 {
				return false, "table " + name + " holds a row whose key is absent from the source"
			}
			have[h]--
		}
	}
	return true, ""
}

// db_file_rows opens the DB file at the absolute path read-only and returns the
// query's rows as maps, typed by the database/sql driver. The subset guard reads the
// live target and the scratch through this one path so the same logical row produces
// the same audit_row_hash on both sides. Uses sql.Open, never db_open, so it never
// contends with databases_lock (which the swap takes).
func db_file_rows(path, query string) ([]map[string]any, error) {
	d, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	defer d.Close()
	rows, err := d.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			m[c] = vals[i]
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
