// Mochi server: WAL watchdog regression (#6).
//
// A long-lived reader starving the checkpoint ballooned feeds.db's WAL to
// 2.9 GB with no alert before it corrupted. db_wal_watchdog force-checkpoints
// an oversized WAL and, if a reader keeps the checkpoint from reclaiming it,
// strikes toward a warning.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

func TestDbWalWatchdogReclaimsTransientStrikesStarved(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	orig := db_wal_warn_bytes
	db_wal_warn_bytes = 256 * 1024 // 256 KB so the test needs only a small WAL
	defer func() { db_wal_warn_bytes = orig }()

	db := db_open("users/u-wal/test/db/big.db")
	db.exec("create table t (id integer primary key, v blob)")
	payload := strings.Repeat("x", 16*1024) // 16 KB rows, below the 4 MB autocheckpoint

	strikes := func() int {
		if v, ok := db_wal_strikes.Load(db.path); ok {
			return v.(int)
		}
		return -1
	}

	// Transient: an oversized WAL with no blocking reader is reclaimed by the
	// watchdog's checkpoint, so no strike accrues.
	for i := 0; i < 40; i++ { // ~640 KB > threshold
		db.exec("insert into t (id, v) values (?, ?)", i, payload)
	}
	db_wal_watchdog()
	if strikes() != -1 {
		t.Fatalf("transient WAL must reclaim (no strike); got %d", strikes())
	}

	// Sustained: a reader pins an old WAL frame so the checkpoint can't reclaim;
	// strikes accrue one per pass up to the warn threshold.
	reader, err := db.internal.Beginx()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var c int
	_ = reader.Get(&c, "select count(*) from t") // pin the snapshot here

	for i := 100; i < 160; i++ {
		db.exec("insert into t (id, v) values (?, ?)", i, payload)
	}
	for pass := 1; pass <= db_wal_warn_strikes; pass++ {
		db_wal_watchdog()
		if strikes() != pass {
			t.Fatalf("strike after pass %d = %d, want %d (reader-pinned WAL must not reclaim)", pass, strikes(), pass)
		}
	}

	// Releasing the reader lets the next pass reclaim the WAL and clear strikes.
	reader.Rollback()
	db_wal_watchdog()
	if strikes() != -1 {
		t.Fatalf("after reader released, WAL must reclaim and strike clear; got %d", strikes())
	}
}
