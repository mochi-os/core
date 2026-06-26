// Mochi server: logical bootstrap cutover (#15).
//
// After bootstrap_db_swap, db_open sees the new file while a handle held from
// before the swap still serves the old inode — no panic, no "database is
// closed" — which is what lets the cutover happen under live traffic.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"os"
	"testing"
)

func TestBootstrapDbSwap(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	live := db_open("swap-live.db")
	live.exec("create table t (id integer primary key, v text)")
	live.exec("insert into t (id, v) values (1, 'old')")
	target := live.path
	oldPool := live.internal // a borrower's already-fetched handle

	// Build a scratch file with different content.
	scratch := target + ".rebuild"
	sd, err := sql.Open("sqlite3", "file:"+scratch)
	if err != nil {
		t.Fatal(err)
	}
	sd.Exec("create table t (id integer primary key, v text)")
	sd.Exec("insert into t (id, v) values (1, 'new')")
	sd.Exec("insert into t (id, v) values (2, 'new2')")
	sd.Close()

	if err := bootstrap_db_swap(target, scratch); err != nil {
		t.Fatalf("swap: %v", err)
	}

	// db_open now returns the new file.
	fresh := db_open("swap-live.db")
	var v string
	if err := fresh.internal.Get(&v, "select v from t where id=1"); err != nil || v != "new" {
		t.Errorf("after swap db_open v=%q err=%v, want \"new\"", v, err)
	}
	var cnt int
	fresh.internal.Get(&cnt, "select count(*) from t")
	if cnt != 2 {
		t.Errorf("after swap rows=%d, want 2", cnt)
	}

	// The pre-swap handle still serves the old inode — no panic, no error.
	var ov string
	if err := oldPool.Get(&ov, "select v from t where id=1"); err != nil || ov != "old" {
		t.Errorf("old handle v=%q err=%v, want \"old\" (must not error/panic)", ov, err)
	}

	// The scratch file was consumed by the rename.
	if _, err := os.Stat(scratch); !os.IsNotExist(err) {
		t.Errorf("scratch file should have been renamed away")
	}
}

// TestBootstrapDbSwapReseedsJournal (#424): the rebuilt file has no journal
// table (skipped on the wire), so bootstrap_db_swap must re-create it on the new
// handle eagerly — otherwise the next journaled write fails with "no such table:
// journal". The cached handle means a later db_app open is reused and won't run
// journal_setup, so the swap can't defer it.
func TestBootstrapDbSwapReseedsJournal(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	tdb := db_open("swap-journal.db")
	tdb.journal_setup()
	target := tdb.path

	// Rebuilt scratch with NO journal table.
	scratch := target + ".rebuild"
	sd, err := sql.Open("sqlite3", "file:"+scratch)
	if err != nil {
		t.Fatal(err)
	}
	sd.Exec("create table messages (id text primary key)")
	sd.Close()

	if err := bootstrap_db_swap(target, scratch); err != nil {
		t.Fatalf("swap: %v", err)
	}

	// The swap re-created the journal table on the rebuilt file; no later write
	// or lazy ensure is needed.
	db := db_open("swap-journal.db")
	if has, _ := db.exists("select 1 from sqlite_master where type='table' and name='journal'"); !has {
		t.Fatal("bootstrap_db_swap must re-create the journal table on the rebuilt file (#424)")
	}
}
