// Mochi server: a commit-log row is identified by the insert, not by a search.
//
// commits_append used to insert and then re-query "the newest unfired row with
// this (name, kind, row_uid)" to recover the seq. Those three columns do not
// identify a row: the same table row committing twice writes two commits rows
// with all three equal. Any insert landing between another caller's insert and
// its re-query hands both callers the same seq, so one row is marked fired
// twice and the other never - it stays pending, and every later fire redrains
// it and reruns the handler for work already done.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// commits_test_db builds an empty commits table in a temporary data directory.
func commits_test_db(t *testing.T) *DB {
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

// TestCommitsAppendGivesEachCallerItsOwnRow is the regression. Every caller
// commits the SAME (table, kind, row_uid), which is what the old re-query
// keyed on, and they are released together so their inserts and recoveries
// actually overlap.
func TestCommitsAppendGivesEachCallerItsOwnRow(t *testing.T) {
	db := commits_test_db(t)

	const callers, rounds = 16, 20
	for round := 0; round < rounds; round++ {
		seqs := make([]int64, callers)
		var release, finished sync.WaitGroup
		release.Add(1)
		for i := 0; i < callers; i++ {
			finished.Add(1)
			go func(i int) {
				defer finished.Done()
				release.Wait()
				seqs[i] = commits_append(db, "widgets", "update", "row-1")
			}(i)
		}
		release.Done()
		finished.Wait()

		seen := map[int64]bool{}
		for _, seq := range seqs {
			if seq <= 0 {
				t.Fatalf("round %d: commits_append returned %d; commits_mark_fired ignores anything <= 0, so that row could never be marked", round, seq)
			}
			if seen[seq] {
				t.Fatalf("round %d: two callers were both handed seq %d; one of their rows can never be marked fired", round, seq)
			}
			seen[seq] = true
		}
	}

	// Every append inserted exactly one row, and every caller got one of them.
	if rows := db.integer("select count(*) from commits"); rows != callers*rounds {
		t.Errorf("table holds %d rows, want %d", rows, callers*rounds)
	}
}

// TestCommitsAppendLeavesNothingPending is the consequence the seq collision
// produced: rows nobody can mark, redrained and rehandled forever.
func TestCommitsAppendLeavesNothingPending(t *testing.T) {
	db := commits_test_db(t)

	const callers = 12
	seqs := make([]int64, callers)
	var release, finished sync.WaitGroup
	release.Add(1)
	for i := 0; i < callers; i++ {
		finished.Add(1)
		go func(i int) {
			defer finished.Done()
			release.Wait()
			seqs[i] = commits_append(db, "widgets", "update", "row-1")
		}(i)
	}
	release.Done()
	finished.Wait()

	// Each caller marks the seq it was given, as commit_hook_fire does when
	// its handler succeeds.
	for _, seq := range seqs {
		commits_mark_fired(db, seq)
	}

	if pending := db.integer("select count(*) from commits where fired=0"); pending != 0 {
		t.Errorf("%d of %d rows are still pending after every caller marked its own seq; those rows are redrained and their handler rerun on every later fire", pending, callers)
	}
}

// TestCommitsAppendReturnsTheRowItInserted: the seq must name this call's row,
// not merely some row. Distinguished by row_uid, which the old re-query keyed
// on and so could never get wrong in a single-caller test - hence the
// concurrent tests above. This one guards the simple case.
func TestCommitsAppendReturnsTheRowItInserted(t *testing.T) {
	db := commits_test_db(t)

	for _, row_uid := range []string{"alpha", "beta", "gamma"} {
		seq := commits_append(db, "widgets", "update", row_uid)
		row, err := db.row("select name, kind, row_uid from commits where seq=?", seq)
		if err != nil || row == nil {
			t.Fatalf("seq %d for %q names no row: %v", seq, row_uid, err)
		}
		if got, _ := row["row_uid"].(string); got != row_uid {
			t.Errorf("seq %d names the row for %q, want %q", seq, got, row_uid)
		}
		if got, _ := row["name"].(string); got != "widgets" {
			t.Errorf("seq %d names table %q, want %q", seq, got, "widgets")
		}
	}
}

// TestCommitsAppendDoesNotRecoverBySearching is the gate. The re-query is the
// defect, not a detail of it: any "find the row I just wrote" search over
// columns that do not identify a row has the same failure.
func TestCommitsAppendDoesNotRecoverBySearching(t *testing.T) {
	data, err := os.ReadFile("commit_hook.go")
	if err != nil {
		t.Fatalf("reading commit_hook.go: %v", err)
	}
	source := string(data)
	at := strings.Index(source, "func commits_append(")
	if at < 0 {
		t.Fatal("commit_hook.go no longer defines commits_append")
	}
	body := source[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if strings.Contains(body, "select seq from commits") {
		t.Error("commits_append recovers its seq by querying for it; (name, kind, row_uid) does not identify a row, so a concurrent insert hands two callers the same seq. Take it from the insert's LastInsertId")
	}
	if !strings.Contains(body, "LastInsertId()") {
		t.Error("commits_append no longer takes its seq from the insert")
	}
}
