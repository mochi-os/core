// Mochi server: corrupt-DB resilience — a corrupt user DB must not crash the
// multi-user process. Covers the corruption matcher, the quarantine
// lifecycle, and exec_bg's skip/no-over-quarantine behaviour.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDbErrorIsCorruption(t *testing.T) {
	for _, m := range []string{
		"database disk image is malformed",
		"file is not a database",
		"database corruption detected",
	} {
		if !db_error_is_corruption(errors.New(m)) {
			t.Errorf("%q should be treated as corruption", m)
		}
	}
	for _, m := range []string{
		"no such table: t",
		"database is locked",
		"UNIQUE constraint failed: t.id",
		"near \"x\": syntax error",
	} {
		if db_error_is_corruption(errors.New(m)) {
			t.Errorf("%q must NOT be treated as corruption", m)
		}
	}
	if db_error_is_corruption(nil) {
		t.Error("nil is not corruption")
	}
}

func TestDbQuarantineLifecycle(t *testing.T) {
	path := "test/quarantine-lifecycle.db"
	defer db_integrity_state.Delete(path)

	if db_quarantined(path) {
		t.Fatal("path should not start quarantined")
	}
	db_quarantine(path, "test", errors.New("database disk image is malformed"))
	if !db_quarantined(path) {
		t.Error("db_quarantine should flag the path corrupt")
	}
	// Re-quarantine is idempotent (state stays corrupt).
	db_quarantine(path, "test", errors.New("database disk image is malformed"))
	if !db_quarantined(path) {
		t.Error("path should stay quarantined after a second flag")
	}
	db_quarantine_clear(path)
	if db_quarantined(path) {
		t.Error("db_quarantine_clear should lift the flag (fresh copy swapped in)")
	}
}

// exec_bg never panics: a non-corruption error logs without quarantining, a
// clean write succeeds, and a quarantined DB is skipped entirely.
func TestExecBgSkipsAndDoesNotOverQuarantine(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	defer db_integrity_state.Delete(db.path)
	db.exec("create table t (id integer primary key)")

	// A non-corruption error (no such table) must NOT quarantine.
	db.exec_bg("bad write", "insert into nonexistent (x) values (1)")
	if db_quarantined(db.path) {
		t.Error("a non-corruption error must not quarantine the DB")
	}

	// A clean write through exec_bg works.
	db.exec_bg("good write", "insert into t (id) values (1)")
	if n := db.integer("select count(*) from t"); n != 1 {
		t.Errorf("exec_bg clean write: count=%d, want 1", n)
	}

	// Once quarantined, exec_bg is a no-op.
	db_integrity_state.Store(db.path, "corrupt")
	db.exec_bg("skipped write", "insert into t (id) values (2)")
	db_integrity_state.Delete(db.path)
	if n := db.integer("select count(*) from t"); n != 1 {
		t.Errorf("exec_bg wrote to a quarantined DB: count=%d, want 1", n)
	}
}

// TestExecBgOnANilDatabase pins the other half of exec_bg's never-panics
// contract. The queue calls it from background goroutines where a handle can be
// nil, and a panic there takes the process down rather than one user's write.
func TestExecBgOnANilDatabase(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("exec_bg panicked on a nil database: %v", r)
		}
	}()
	var db *DB
	db.exec_bg("nil handle", "insert into t (id) values (1)")
}

// TestExecBgReturnsNothing keeps the discarded tri-state from returning.
//
// exec_bg used to answer ExecWrote / ExecRetryable / ExecSkipped so a
// replicated apply could decide whether to retry. Replication went in July
// 2026 and all 27 remaining call sites - every one in queue.go - discarded it,
// which is why removing the return compiled untouched.
//
// A restored return would not fail the build, since Go lets a caller ignore a
// result: it would come back silently and be discarded exactly as before.
// Hence a source-shape gate rather than reliance on the compiler.
func TestExecBgReturnsNothing(t *testing.T) {
	source, err := os.ReadFile("db.go")
	if err != nil {
		t.Fatalf("reading db.go: %v", err)
	}
	if !strings.Contains(string(source), "func (db *DB) exec_bg(context, query string, values ...any) {") {
		t.Error("exec_bg no longer returns nothing; a result no caller reads is residue, and the one that existed outlived its only consumer by six weeks")
	}

	files, _ := filepath.Glob("*.go")
	for _, name := range files {
		if name == "db_quarantine_test.go" {
			continue // this comment is the one legitimate mention
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		for _, dead := range []string{"ExecResult", "ExecWrote", "ExecRetryable", "ExecSkipped"} {
			if strings.Contains(string(body), dead) {
				t.Errorf("%s references %s", name, dead)
			}
		}
	}
}
