// Mochi server: the commit log's retry is bounded.
//
// commits_trim deletes only fired rows, so without an attempt budget a failed
// handler is retried - a Starlark build, call and slot each - on every later
// commit.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// commits_attempts_db builds an empty commits table in a temporary data
// directory and returns the handle.
func commits_attempts_db(t *testing.T) *DB {
	t.Helper()
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}
	db := db_open("db/commits.db")
	commits_table_create(db)
	return db
}

// commits_pending reports how many rows the drain would still pick up.
func commits_pending(db *DB) int {
	return db.integer("select count(*) from commits where fired=0")
}

// TestCommitsFailedCountsThenGivesUp is the regression: a row that keeps
// failing leaves the pending set instead of being retried forever.
func TestCommitsFailedCountsThenGivesUp(t *testing.T) {
	db := commits_attempts_db(t)
	seq := commits_append(db, "widgets", "update", "row-1")

	// Every failure below the budget counts the attempt and keeps the row.
	for attempt := 1; attempt < commits_attempts_maximum; attempt++ {
		commits_failed(db, seq, int64(attempt-1), "testapp", "on_db_commit", "widgets", "update", "row-1")
		if got := db.integer("select attempts from commits where seq=?", seq); got != attempt {
			t.Fatalf("after failure %d the row records %d attempts, want %d", attempt, got, attempt)
		}
		if commits_pending(db) != 1 {
			t.Fatalf("after failure %d the row is no longer pending; the budget is %d", attempt, commits_attempts_maximum)
		}
	}

	// The one that spends the budget gives up.
	commits_failed(db, seq, commits_attempts_maximum-1, "testapp", "on_db_commit", "widgets", "update", "row-1")
	if pending := commits_pending(db); pending != 0 {
		t.Errorf("the row is still pending after %d failed attempts; it would be redrained and rerun on every later commit", commits_attempts_maximum)
	}
}

// TestCommitsGivenUpRowStillAgesOut: giving up marks the row fired rather than
// deleting it, so the ordinary trim reclaims it and a day of evidence survives
// in the meantime.
func TestCommitsGivenUpRowStillAgesOut(t *testing.T) {
	db := commits_attempts_db(t)
	seq := commits_append(db, "widgets", "update", "row-1")
	commits_failed(db, seq, commits_attempts_maximum-1, "testapp", "on_db_commit", "widgets", "update", "row-1")

	if db.integer("select count(*) from commits where seq=? and fired=1", seq) != 1 {
		t.Fatal("the abandoned row is not marked fired; the drain would keep picking it up")
	}

	// Still present, so it can be looked at.
	commits_trim(db)
	if db.integer("select count(*) from commits where seq=?", seq) != 1 {
		t.Error("the abandoned row was trimmed immediately; it should survive the debugging window like any other fired row")
	}

	// And gone once it is past the window.
	db.exec("update commits set ts=? where seq=?", now()-commits_log_age-100, seq)
	commits_trim(db)
	if db.integer("select count(*) from commits where seq=?", seq) != 0 {
		t.Error("the abandoned row outlived commits_log_age; it is an ordinary fired row now and must be trimmed like one")
	}
}

// TestCommitsFailedLeavesASucceedingRowAlone: the budget must not touch the
// path that works. A row marked fired by a successful invocation is done, and
// a later stray failure report must not resurrect or recount it.
func TestCommitsFailedLeavesASucceedingRowAlone(t *testing.T) {
	db := commits_attempts_db(t)
	seq := commits_append(db, "widgets", "update", "row-1")

	commits_mark_fired(db, seq)
	if commits_pending(db) != 0 {
		t.Fatal("a successfully handled row is still pending")
	}
	if got := db.integer("select attempts from commits where seq=?", seq); got != 0 {
		t.Errorf("a row that succeeded first time records %d attempts, want 0", got)
	}
}

// TestCommitsFailedIgnoresAnInvalidSeq mirrors commits_mark_fired, which has
// always ignored seq <= 0. Before the LastInsertId change a recovery failure
// returned 0, and a bare update would then have counted an attempt against
// every row in the table.
func TestCommitsFailedIgnoresAnInvalidSeq(t *testing.T) {
	db := commits_attempts_db(t)
	first := commits_append(db, "widgets", "update", "row-1")
	second := commits_append(db, "widgets", "update", "row-2")

	commits_failed(db, 0, 0, "testapp", "on_db_commit", "widgets", "update", "row-1")
	commits_failed(db, -1, 0, "testapp", "on_db_commit", "widgets", "update", "row-1")

	for _, seq := range []int64{first, second} {
		if got := db.integer("select attempts from commits where seq=?", seq); got != 0 {
			t.Errorf("row %d records %d attempts after two calls with an invalid seq, want 0", seq, got)
		}
	}
	if commits_pending(db) != 2 {
		t.Error("a call with an invalid seq changed which rows are pending")
	}
}

// TestCommitsTableMigratesAttempts: an app.db predating the column gets it in
// place, the way received.seen and directory.confirmed are added. Without this
// every drain against an existing table would fail to scan.
func TestCommitsTableMigratesAttempts(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = original }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("creating the db directory: %v", err)
	}
	db := db_open("db/legacy.db")

	// The table exactly as it was before the column existed.
	db.exec("create table commits (seq integer primary key autoincrement, name text not null, kind text not null, row_uid text not null default '', ts integer not null, fired integer not null default 0)")
	db.exec("insert into commits (name, kind, row_uid, ts, fired) values ('widgets','update','row-1',?,0)", now())

	commits_table_create(db)

	if exists, _ := db.exists("select 1 from pragma_table_info('commits') where name='attempts'"); !exists {
		t.Fatal("commits_table_create did not add the attempts column to an existing table")
	}
	if got := db.integer("select attempts from commits where row_uid='row-1'"); got != 0 {
		t.Errorf("the pre-existing row has attempts=%d, want 0", got)
	}
	// The drain's select must work against the migrated table.
	if _, err := db.rows("select seq, name, kind, row_uid, attempts from commits where fired=0 order by seq limit 100"); err != nil {
		t.Errorf("the drain's query fails against a migrated table: %v", err)
	}
}

// TestDrainBoundsItsRetries is the gate. The drain is where the cost is paid -
// up to a hundred rows per fire - so it is the drain that has to account for
// failures rather than silently leaving the row for next time.
func TestDrainBoundsItsRetries(t *testing.T) {
	data, err := os.ReadFile("commit_hook.go")
	if err != nil {
		t.Fatalf("reading commit_hook.go: %v", err)
	}
	source := string(data)
	at := strings.Index(source, "func commit_hook_drain(")
	if at < 0 {
		t.Fatal("commit_hook.go no longer defines commit_hook_drain")
	}
	body := source[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "commits_failed(") {
		t.Error("commit_hook_drain does not record failed invocations, so a row whose handler always fails is retried on every later commit forever")
	}
	if !strings.Contains(body, "attempts") {
		t.Error("commit_hook_drain does not read the attempts column, so it cannot tell a first failure from a fiftieth")
	}
}
