// Mochi server: bootstrap integrity-gate regression (#6).
//
// A freshly-fetched bootstrap snapshot is quick_check'd before it is landed, so
// a corrupt source/transfer is rejected and retried rather than installed and
// re-propagated (the corruption ping-pong that wrecked feeds.db).
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotIntegrityGate(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	cleanRel := "gate-clean.db"
	cleanPath := filepath.Join(data_dir, cleanRel)
	c := db_open(cleanRel)
	c.exec("create table t (id integer primary key, v blob)")
	payload := strings.Repeat("x", 1024)
	for i := 0; i < 300; i++ { // ~300 KB -> dozens of btree pages
		c.exec("insert into t (id, v) values (?, ?)", i, payload)
	}
	c.exec("PRAGMA wal_checkpoint(TRUNCATE)") // flush the WAL into the main file

	if !snapshot_integrity_ok(cleanPath) {
		t.Fatal("clean snapshot must pass the integrity gate")
	}

	// Corrupt a copy: garble a swath of btree pages (pages 2..10) in the main
	// file — invalid page-type bytes are exactly the btreeInitPage corruption
	// the prod incident showed.
	corruptPath := filepath.Join(data_dir, "gate-corrupt.db")
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 4096; i < 40960 && i < len(data); i++ {
		data[i] = 0xFF
	}
	if err := os.WriteFile(corruptPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if snapshot_integrity_ok(corruptPath) {
		t.Fatal("corrupt snapshot must be rejected by the integrity gate")
	}
}
