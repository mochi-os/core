// restore_integrity_guard quick_checks every *.db in a user-supplied restore
// bundle before it is swapped into place. The manifest file-hash is self-attested
// (the user signs their own bundle), so it can't prove a DB is sound; without this
// guard a corrupt or malicious sqlite would be restored and only caught later by the
// runtime quarantine sweep. Threat-model #95, claude/plans/replication-threat-model.md.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreIntegrityGuard(t *testing.T) {
	bundle := t.TempDir()
	write := func(rel string, content []byte) {
		p := filepath.Join(bundle, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// A clean sqlite DB nested in the bundle.
	cleanPath := filepath.Join(bundle, "app", "db", "clean.db")
	if err := os.MkdirAll(filepath.Dir(cleanPath), 0o755); err != nil {
		t.Fatal(err)
	}
	c, err := sql.Open("sqlite3", "file:"+cleanPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec("create table t (id integer primary key); insert into t values (1)"); err != nil {
		t.Fatal(err)
	}
	c.Close()

	// Non-.db files and a .db-wal sibling must be ignored (only *.db is checked).
	write("notes.txt", []byte("not a database, and not checked"))
	write("app/db/clean.db-wal", []byte("garbage wal, not a standalone db"))

	// An all-clean bundle passes.
	if bad, err := restore_integrity_guard(bundle); bad != "" || err != nil {
		t.Fatalf("clean bundle rejected: bad=%q err=%v", bad, err)
	}

	// A corrupt .db is rejected and named.
	write("app2/db/corrupt.db", []byte("this is not a sqlite database — it is garbage bytes"))
	bad, err := restore_integrity_guard(bundle)
	if err != nil {
		t.Fatalf("guard errored: %v", err)
	}
	if bad != "corrupt.db" {
		t.Fatalf("corrupt DB not rejected (bad=%q, want corrupt.db)", bad)
	}
}
