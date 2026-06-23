// Mochi server: proactive corruption-detection watchdog regression (#6/#7).
//
// The 2026-06-23 feeds.db corruption ran silently for hours. db_integrity_watchdog
// quick_checks open DBs on a rolling schedule and flags+warns the moment one is
// corrupt, so it surfaces as an alert in minutes instead of a silent outage.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

func TestDbIntegrityWatchdog(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	orig := db_integrity_max_per_check
	db_integrity_max_per_check = 1000 // check every due DB in one pass
	defer func() { db_integrity_max_per_check = orig }()

	// Clean DB: recorded as checked with an ok timestamp, not flagged.
	clean := db_open("users/u/clean/db/app.db")
	clean.exec("create table t (id integer primary key, v text)")
	for i := 0; i < 50; i++ {
		clean.exec("insert into t (id, v) values (?, 'x')", i)
	}
	db_integrity_watchdog()
	v, ok := db_integrity_state.Load(clean.path)
	if !ok {
		t.Fatal("clean DB should be recorded as checked")
	}
	if _, isTime := v.(int64); !isTime {
		t.Fatalf("clean DB state = %v, want an ok timestamp", v)
	}

	// Throttle: an immediate second pass must not re-check (recently checked).
	db_integrity_watchdog()
	if v2, _ := db_integrity_state.Load(clean.path); v2 != v {
		t.Error("clean DB re-checked within the throttle period")
	}

	// Corrupt DB: garble btree pages on disk under the open handle; the
	// watchdog's read-only quick_check sees it and flags the DB.
	corrupt := db_open("users/u/corrupt/db/app.db")
	corrupt.exec("create table t (id integer primary key, v blob)")
	payload := strings.Repeat("x", 1024)
	for i := 0; i < 300; i++ {
		corrupt.exec("insert into t (id, v) values (?, ?)", i, payload)
	}
	corrupt.exec("PRAGMA wal_checkpoint(TRUNCATE)") // flush WAL into the main file
	data, err := os.ReadFile(corrupt.path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 4096; i < 40960 && i < len(data); i++ {
		data[i] = 0xFF
	}
	if err := os.WriteFile(corrupt.path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	db_integrity_watchdog()
	if state, _ := db_integrity_state.Load(corrupt.path); state != "corrupt" {
		t.Fatalf("corrupt DB state = %v, want \"corrupt\"", state)
	}
}
