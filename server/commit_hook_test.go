// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCommitsTrim (#44): the fired-row trim deletes fired rows past
// commits_log_age, keeps recent fired rows, and never touches unfired (pending)
// rows regardless of age — so a stuck handler's retries are preserved.
func TestCommitsTrim(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	db := db_open("db/test.db")
	commits_table_create(db)
	old := now() - commits_log_age - 100
	recent := now() - 10
	db.exec("insert into commits (name, kind, row_uid, ts, fired) values ('t','insert','r1',?,1)", old)    // old fired -> trimmed
	db.exec("insert into commits (name, kind, row_uid, ts, fired) values ('t','insert','r2',?,1)", recent) // recent fired -> kept
	db.exec("insert into commits (name, kind, row_uid, ts, fired) values ('t','insert','r3',?,0)", old)    // old UNfired -> kept (pending)

	commits_trim(db)

	if n := db.integer("select count(*) from commits"); n != 2 {
		t.Fatalf("after trim want 2 rows (recent-fired + old-unfired), got %d", n)
	}
	if db.integer("select count(*) from commits where row_uid='r1'") != 0 {
		t.Error("old fired row should be trimmed")
	}
	if db.integer("select count(*) from commits where row_uid='r2'") != 1 {
		t.Error("recent fired row should be kept")
	}
	if db.integer("select count(*) from commits where row_uid='r3'") != 1 {
		t.Error("unfired (pending) row should never be trimmed regardless of age")
	}
}
