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
	"fmt"
	"os"
	"time"

	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
	"github.com/jmoiron/sqlx"
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
	databases_lock.Lock()
	defer databases_lock.Unlock()

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

	// The rebuilt file omits journal/journal_delivery (skipped on the wire — the
	// receiver owns its own change-capture). Drop the stale journal_ensured cache
	// entry so the next replicated write re-creates them instead of failing with
	// "no such table: journal" (#424).
	journal_ensured.Delete(target)

	internal_db, err := sqlitedrv.Open(target, db_setup_conn)
	if err != nil {
		return fmt.Errorf("bootstrap-swap: open internal pool %q: %w", target, err)
	}
	starlark_db, err := sqlitedrv.Open(target, db_setup_conn_starlark)
	if err != nil {
		internal_db.Close()
		return fmt.Errorf("bootstrap-swap: open starlark pool %q: %w", target, err)
	}
	databases[oldKey] = &DB{
		key:      oldKey,
		path:     target,
		internal: sqlx.NewDb(internal_db, "sqlite3"),
		starlark: sqlx.NewDb(starlark_db, "sqlite3"),
	}

	if oldDB != nil {
		go func() {
			time.Sleep(bootstrap_swap_grace)
			oldDB.internal.Close()
			oldDB.starlark.Close()
		}()
	}
	return nil
}
