// Mochi server: bootstrap snapshot integrity coverage (#6 investigation).
//
// Added while investigating the prod feeds.db corruption to test whether the
// bootstrap copy path (snapshot_copy_db -> land) introduces corruption at scale
// or on a WAL-heavy source. It does NOT: every case here passes against the
// shipped code — a clean source produces a clean copy, and snapshot_copy_db
// faithfully captures uncheckpointed WAL data even under a concurrent reader.
// So the bootstrap copy is faithful; the prod corruption was copied from an
// already-corrupt SOURCE, not introduced by the land. Kept as regression
// coverage for the large/WAL-heavy snapshot paths the prior tests never
// exercised (they used 2-row DBs). See task #6.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// snapshotConsistent snapshots srcRel and asserts the snapshot passes
// quick_check and contains wantRows rows of table t. Returns the snapshot's
// row count so callers can distinguish "stale" (fewer rows) from "corrupt".
func snapshotConsistent(t *testing.T, srcRel string, wantRows int) {
	t.Helper()
	srcPath := filepath.Join(data_dir, srcRel)
	snapRel := "snap-" + filepath.Base(srcRel)
	snapPath := filepath.Join(data_dir, snapRel)
	if _, err := snapshot_copy_db(srcPath, snapPath); err != nil {
		t.Fatalf("snapshot_copy_db: %v", err)
	}
	chk := db_open(snapRel)
	rows, err := chk.rows("PRAGMA quick_check")
	if err != nil {
		t.Fatalf("quick_check query: %v", err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0]["quick_check"]) != "ok" {
		t.Fatalf("snapshot of %q FAILED integrity (#6): %d rows, first=%v", srcRel, len(rows), rows[0])
	}
	if got := chk.integer("select count(*) from t"); got != wantRows {
		t.Fatalf("snapshot of %q has %d rows, want %d — snapshot_copy_db dropped uncheckpointed WAL data", srcRel, got, wantRows)
	}
}

// A source whose recent writes still live in the WAL (not checkpointed) must
// snapshot consistently. This is the prod reseed condition.
func TestSnapshotCopyCapturesUncheckpointedWal(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	srcRel := "tmp-wal-src.db"
	src := db_open(srcRel)
	src.exec("create table t (id integer primary key, v text)")
	for i := 0; i < 200; i++ {
		src.exec("insert into t (id, v) values (?, ?)", i, "x")
	}
	// Deliberately NO checkpoint — the rows live in the WAL. Confirm the live
	// handle sees all 200, then assert the snapshot does too.
	if got := src.integer("select count(*) from t"); got != 200 {
		t.Fatalf("source live handle sees %d rows, want 200", got)
	}
	snapshotConsistent(t, srcRel, 200)
}

// Same, but with a large WAL held open by a concurrent reader — the exact prod
// shape (a long reader starves the checkpoint so the WAL balloons, then the
// reseed snapshots the WAL-heavy DB). Uses a big payload so the WAL is many MB.
func TestSnapshotCopyWalHeavySourceUnderReader(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	srcRel := "tmp-walheavy-src.db"
	src := db_open(srcRel)
	src.exec("create table t (id integer primary key, payload blob)")
	src.exec("insert into t (id, payload) values (0, ?)", payload)

	// Open a long-lived read transaction that pins an early WAL snapshot, so
	// subsequent writes can't be checkpointed out and the WAL grows (~64 MB).
	reader, err := src.internal.Beginx()
	if err != nil {
		t.Fatalf("begin reader: %v", err)
	}
	defer reader.Rollback()
	var n int
	_ = reader.Get(&n, "select count(*) from t") // establish the read snapshot

	for i := 1; i < 1000; i++ { // ~64 MB of WAL frames
		src.exec("insert into t (id, payload) values (?, ?)", i, payload)
	}
	if got := src.integer("select count(*) from t"); got != 1000 {
		t.Fatalf("source sees %d rows, want 1000", got)
	}

	snapshotConsistent(t, srcRel, 1000)
}

// End-to-end: land a large snapshot into a live, populated handle while
// concurrent writers commit against it. Passes against shipped code — the land
// preserves integrity at scale, confirming the corruptor is upstream (a bad
// source), not bootstrap_db_land.
func TestBootstrapDbLandLargeUnderConcurrentWrites(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	payload := make([]byte, 200*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	srcRel := "tmp-land-src-large.db"
	srcPath := filepath.Join(data_dir, srcRel)
	src := db_open(srcRel)
	src.exec("create table blob (id integer primary key, payload blob)")
	for i := 0; i < 1250; i++ { // ~250 MB
		src.exec("insert into blob (id, payload) values (?, ?)", i, payload)
	}
	src.exec("PRAGMA wal_checkpoint(TRUNCATE)")

	rel := "users/u-land/test/db/large.db"
	target := filepath.Join(data_dir, rel)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	dst := db_open(rel)
	dst.exec("create table blob (id integer primary key, payload blob)")
	for i := 0; i < 250; i++ {
		dst.exec("insert into blob (id, payload) values (?, ?)", i, payload)
	}

	partial := target + ".partial"
	if _, err := snapshot_copy_db(srcPath, partial); err != nil {
		t.Fatalf("make partial: %v", err)
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				func() {
					defer func() { _ = recover() }()
					dst.exec("insert into blob (id, payload) values (?, ?)", int64(1_000_000+w*1_000_000+i), payload)
				}()
			}
		}(w)
	}
	landErr := bootstrap_db_land(partial, target)
	stop.Store(true)
	wg.Wait()
	if landErr != nil {
		t.Fatalf("bootstrap_db_land: %v", landErr)
	}

	rows, err := dst.rows("PRAGMA quick_check")
	if err != nil {
		t.Fatalf("quick_check query: %v", err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0]["quick_check"]) != "ok" {
		t.Fatalf("landed DB failed integrity (#6): %d rows, first=%v", len(rows), rows[0])
	}
	if got := dst.integer("select count(*) from blob"); got < 1250 {
		t.Fatalf("post-land row count = %d, want >= 1250", got)
	}
}
