// Mochi server: #42 SEV1 regression — the auto-reanchor reseeded the live
// primary's feeds.db FROM a near-empty replica (cursor-misaligned), overwriting
// 1.56M rows. The reseed's cursor-based gate was fooled by the very misalignment
// it heals. Two cursor-INDEPENDENT guards now make that impossible:
//   1. the swap-guard refuses to shrink populated data (reads actual rows), and
//   2. the reanchor never reseeds a stream this host originated (tail>0).
// Either alone would have prevented the wipe; together they are defence in depth.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// mkdb_rows writes a sqlite file with dataRows in a `data` table and (optionally)
// journalRows in a `journal` table — the host-local change-capture table the
// no-shrink count must ignore.
func mkdb_rows(t *testing.T, path string, dataRows, journalRows int) {
	t.Helper()
	d, err := sql.Open("sqlite3", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Exec("create table data (id integer primary key, body text)")
	for i := 0; i < dataRows; i++ {
		d.Exec("insert into data (body) values ('x')")
	}
	if journalRows > 0 {
		d.Exec("create table journal (id integer primary key, state text)")
		for i := 0; i < journalRows; i++ {
			d.Exec("insert into journal (state) values ('pending')")
		}
	}
}

func data_count(t *testing.T, path string) int {
	d, _ := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	defer d.Close()
	var n int
	d.QueryRow("select count(*) from data").Scan(&n)
	return n
}

// TestSwapGuardRefusesShrink reproduces the wipe shape and pins the no-shrink
// guard: a populated target must never be replaced by a much smaller scratch —
// including the journal-row disguise that let the old empty-only guard through.
func TestSwapGuardRefusesShrink(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// (1) 100-row target + 1-row scratch -> REFUSE, target untouched. The wipe.
	target := filepath.Join(data_dir, "live.db")
	mkdb_rows(t, target, 100, 0)
	scratch := filepath.Join(data_dir, "scratch_tiny.db")
	mkdb_rows(t, scratch, 1, 0)
	if err := bootstrap_db_swap(target, scratch); err == nil {
		t.Errorf("SEV1 (#42): swap of a 1-row scratch over a 100-row target was NOT refused")
	}
	if data_count(t, target) != 100 {
		t.Errorf("target shrunk despite the no-shrink guard: now %d rows", data_count(t, target))
	}

	// (1b) journal disguise: scratch has 1 data row + 50 journal rows. The
	// empty-guard sees rows and passes; the no-shrink guard ignores journal and
	// still fires. This is the precise mechanism that defeated the old guard.
	t2 := filepath.Join(data_dir, "live2.db")
	mkdb_rows(t, t2, 100, 0)
	s2 := filepath.Join(data_dir, "scratch_journal.db")
	mkdb_rows(t, s2, 1, 50)
	if err := bootstrap_db_swap(t2, s2); err == nil {
		t.Errorf("#42: a scratch padded with journal rows bypassed the no-shrink guard")
	}
	if data_count(t, t2) != 100 {
		t.Errorf("target2 shrunk: now %d rows", data_count(t, t2))
	}

	// (2) legitimate catch-up: behind target (5) + fuller scratch (200) -> SUCCEED.
	t3 := filepath.Join(data_dir, "behind.db")
	mkdb_rows(t, t3, 5, 0)
	s3 := filepath.Join(data_dir, "ahead.db")
	mkdb_rows(t, s3, 200, 0)
	if err := bootstrap_db_swap(t3, s3); err != nil {
		t.Errorf("legit catch-up reseed (5 -> 200) wrongly refused: %v", err)
	}
	if data_count(t, t3) != 200 {
		t.Errorf("legit reseed did not land: %d rows", data_count(t, t3))
	}

	// (3) fresh bootstrap: empty target + populated scratch -> SUCCEED.
	t4 := filepath.Join(data_dir, "fresh.db")
	mkdb_rows(t, t4, 0, 0)
	s4 := filepath.Join(data_dir, "seed.db")
	mkdb_rows(t, s4, 50, 0)
	if err := bootstrap_db_swap(t4, s4); err != nil {
		t.Errorf("fresh-bootstrap swap (empty target) wrongly refused: %v", err)
	}
}

// TestDbFileDataRows pins the row-count helper: journal/journal_delivery
// excluded, -1 on a missing file.
func TestDbFileDataRows(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	p := filepath.Join(data_dir, "counted.db")
	mkdb_rows(t, p, 7, 99) // 7 data rows + 99 journal rows
	if got := db_file_data_rows(p); got != 7 {
		t.Errorf("db_file_data_rows = %d, want 7 (journal must be excluded)", got)
	}
	if got := db_file_data_rows(filepath.Join(data_dir, "nope.db")); got != -1 {
		t.Errorf("db_file_data_rows(missing) = %d, want -1", got)
	}
}

// TestReanchorSkipsOriginatedStream is the originator guard: the reanchor must
// NOT reseed a stream this host emitted ops on (tail>0) — yuzu reseeding its own
// feeds.db was the wipe. A pure-receiver stream (tail==0) still reseeds (covered
// by TestReanchorReseedsCleanCatchup).
func TestReanchorSkipsOriginatedStream(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	reanchor_reset()

	got := make(chan string, 4)
	orig := bootstrap_db_reseed
	bootstrap_db_reseed = func(peer, scope, path string) error { got <- path; return nil }
	defer func() { bootstrap_db_reseed = orig }()

	uid, entity := "uOrig", "12entityO"
	mkapp(t, uid, entity, "feeds.db", 3, 0) // populated, no journal (gate would otherwise allow)
	stream := "app:" + entity

	// This host has EMITTED on the stream (tail>0) — originated, must SKIP.
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists tail (user text not null, scope text not null, db text not null, last integer not null default 0, primary key (user, scope, db))")
	rdb.exec("insert into tail (user, scope, db, last) values (?, ?, ?, 42) on conflict(user,scope,db) do update set last=42",
		uid, repl_scope_app, stream)

	replication_reanchor_misaligned(StalledStream{Peer: "peerX", Scope: repl_scope_app, User: uid, Database: stream})

	// The reseed launches in a goroutine, so observe via the channel: the guard
	// must skip BEFORE any reseed fires. (With the guard off, the reseed lands
	// within milliseconds — proven by toggling it off.)
	select {
	case p := <-got:
		t.Errorf("#42: reanchor reseeded a stream THIS host originated (tail>0) — the wipe path; path=%q", p)
	case <-time.After(500 * time.Millisecond):
		// good: originator guard skipped it, no reseed launched
	}
}
