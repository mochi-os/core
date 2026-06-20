// Mochi server: bootstrap snapshot-landing safety test.
//
// bootstrap_db_land must install a fetched snapshot by page-copying it INTO
// the live connection (Restore), not by evicting the cached handle and
// renaming the file under it. The old evict+rename closed the pooled handle
// mid-use (borrowers hit "database is closed" -> worker panic) and left a
// gap where a re-open saw the old file with its WAL deleted ("disk image is
// malformed"). This test holds a live handle across a landing and asserts it
// keeps working and sees the new content.
//
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

func TestBootstrapDbLandRestoresIntoLiveHandle(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Target: a live, cached DB handle with "old" content. Holding `db`
	// across the landing simulates a borrower (worker / request handler)
	// that fetched the handle before the snapshot arrives.
	rel := "users/u-land/test/db/test.db"
	target := filepath.Join(data_dir, rel)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	db := db_open(rel)
	db.exec("create table t (id integer primary key, v text)")
	db.exec("insert into t (id, v) values (1, 'old')")
	if got := db.integer("select count(*) from t"); got != 1 {
		t.Fatalf("setup: row count = %d, want 1", got)
	}

	// Build a partial snapshot with different content via the real source-side
	// Backup path, so the test exercises the Backup -> Restore pair end to end.
	srcRel := "tmp-land-src.db"
	srcPath := filepath.Join(data_dir, srcRel)
	src := db_open(srcRel)
	src.exec("create table t (id integer primary key, v text)")
	src.exec("insert into t (id, v) values (1, 'new')")
	src.exec("insert into t (id, v) values (2, 'new2')")
	src.exec("PRAGMA wal_checkpoint(TRUNCATE)") // flush WAL so the RO snapshot read sees everything
	partial := target + ".partial"
	if _, err := snapshot_copy_db(srcPath, partial); err != nil {
		t.Fatalf("make partial snapshot: %v", err)
	}

	if err := bootstrap_db_land(partial, target); err != nil {
		t.Fatalf("bootstrap_db_land: %v", err)
	}

	// The SAME cached handle must still work (not evicted/closed) and, via a
	// possibly-different pool connection, see the landed content — proving the
	// cross-connection WAL visibility the fix relies on.
	if got := db.integer("select count(*) from t"); got != 2 {
		t.Errorf("post-landing row count via live handle = %d, want 2", got)
	}
	if got := db.integer("select count(*) from t where v='old'"); got != 0 {
		t.Errorf("old content must be replaced; rows still 'old' = %d", got)
	}
	if got := db.integer("select count(*) from t where v='new2'"); got != 1 {
		t.Errorf("landed content must be present; rows 'new2' = %d, want 1", got)
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Error("partial file must be removed after landing")
	}
}
