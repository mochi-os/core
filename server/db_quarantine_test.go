// Mochi server: corrupt-DB resilience — a corrupt user DB must not crash the
// multi-user process. Covers the corruption matcher, the quarantine
// lifecycle, exec_bg's skip/no-over-quarantine behaviour, and the
// db_recover_background backstop (swallow corruption, re-fire genuine bugs).
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
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

// db_recover_background swallows a corruption panic (keeps the loop + process
// alive) but re-fires any other panic so a genuine bug still crashes.
func TestDbRecoverBackground(t *testing.T) {
	swallowed := func() (ok bool) {
		defer func() {
			if recover() != nil {
				ok = false // re-panicked past the backstop — not swallowed
			}
		}()
		defer db_recover_background("test")
		ok = true
		panic(errors.New("database disk image is malformed"))
	}
	if !swallowed() {
		t.Error("a corruption panic must be swallowed by db_recover_background")
	}

	refired := func() (fired bool) {
		defer func() {
			if recover() != nil {
				fired = true
			}
		}()
		defer db_recover_background("test")
		panic(errors.New("runtime error: index out of range [3]"))
	}
	if !refired() {
		t.Error("a non-corruption panic must re-fire (genuine bugs still crash)")
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
