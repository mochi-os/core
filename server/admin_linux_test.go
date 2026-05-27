// Mochi server: snapshot routine tests.
// Copyright Alistair Cunningham 2026

//go:build linux

package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"
)

// make_test_db creates a sqlite db at path with one table and three rows.
func make_test_db(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`create table t (k text primary key, v text)`); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if _, err := db.Exec(`insert into t values (?, ?)`, row.k, row.v); err != nil {
			t.Fatal(err)
		}
	}
}

// read_test_db returns the row count of table t.
func read_test_db(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`select count(*) from t`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSnapshotInPlaceProducesBackupFiles(t *testing.T) {
	tmp := t.TempDir()
	prev_data_dir := data_dir
	data_dir = tmp
	defer func() { data_dir = prev_data_dir }()

	if err := os.MkdirAll(filepath.Join(tmp, "run"), 0750); err != nil {
		t.Fatal(err)
	}

	// Two live DBs in different sub-paths.
	make_test_db(t, filepath.Join(tmp, "db", "users.db"))
	make_test_db(t, filepath.Join(tmp, "users", "alice", "feeds", "db", "feed.db"))

	out := snapshot_in_place()

	if out.Dbs != 2 {
		t.Errorf("Dbs: got %d, want 2 (errors: %v)", out.Dbs, out.Errors)
	}
	if len(out.Errors) != 0 {
		t.Errorf("unexpected errors: %v", out.Errors)
	}

	// Both .backup files should exist with the same row count.
	for _, backup := range []string{
		filepath.Join(tmp, "db", "users.db.backup"),
		filepath.Join(tmp, "users", "alice", "feeds", "db", "feed.db.backup"),
	} {
		if _, err := os.Stat(backup); err != nil {
			t.Errorf("expected backup at %s: %v", backup, err)
			continue
		}
		if got := read_test_db(t, backup); got != 3 {
			t.Errorf("backup %s: got %d rows, want 3", backup, got)
		}
	}

	// No leftover .backup.tmp files.
	_ = filepath.WalkDir(tmp, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".backup.tmp") {
			t.Errorf("leftover temp file: %s", p)
		}
		return nil
	})
}

func TestSnapshotReapsStaleBackup(t *testing.T) {
	tmp := t.TempDir()
	prev_data_dir := data_dir
	data_dir = tmp
	defer func() { data_dir = prev_data_dir }()

	if err := os.MkdirAll(filepath.Join(tmp, "run"), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmp, "db"), 0750); err != nil {
		t.Fatal(err)
	}

	// Stale .backup with no live sibling.
	stale := filepath.Join(tmp, "db", "deleted_app.db.backup")
	if err := os.WriteFile(stale, []byte("stale"), 0644); err != nil {
		t.Fatal(err)
	}

	// One real DB so the snapshot routine has something to do.
	make_test_db(t, filepath.Join(tmp, "db", "users.db"))

	out := snapshot_in_place()

	if out.Reaped != 1 {
		t.Errorf("Reaped: got %d, want 1", out.Reaped)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale backup should have been removed: %v", err)
	}
}

// TestSnapshotReapsStaleLegacySnap covers the compat path: a `.db.snap`
// from before the 2026-05-27 rename whose live `.db` was deleted should
// still be reaped.
func TestSnapshotReapsStaleLegacySnap(t *testing.T) {
	tmp := t.TempDir()
	prev_data_dir := data_dir
	data_dir = tmp
	defer func() { data_dir = prev_data_dir }()

	if err := os.MkdirAll(filepath.Join(tmp, "run"), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmp, "db"), 0750); err != nil {
		t.Fatal(err)
	}

	stale := filepath.Join(tmp, "db", "deleted_app.db.snap")
	if err := os.WriteFile(stale, []byte("stale"), 0644); err != nil {
		t.Fatal(err)
	}

	make_test_db(t, filepath.Join(tmp, "db", "users.db"))

	out := snapshot_in_place()

	if out.Reaped != 1 {
		t.Errorf("Reaped: got %d, want 1", out.Reaped)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale legacy snap should have been removed: %v", err)
	}
}

// TestSnapshotRemovesLegacySnapAfterWrite covers the case where a live
// `.db` has a `.db.snap` sibling from before the rename. After the new
// snapshot writes `.db.backup`, the legacy sibling must be dropped so the
// tar export does not ship two copies of the same DB.
func TestSnapshotRemovesLegacySnapAfterWrite(t *testing.T) {
	tmp := t.TempDir()
	prev_data_dir := data_dir
	data_dir = tmp
	defer func() { data_dir = prev_data_dir }()

	if err := os.MkdirAll(filepath.Join(tmp, "run"), 0750); err != nil {
		t.Fatal(err)
	}

	make_test_db(t, filepath.Join(tmp, "db", "users.db"))
	legacy := filepath.Join(tmp, "db", "users.db.snap")
	if err := os.WriteFile(legacy, []byte("legacy"), 0644); err != nil {
		t.Fatal(err)
	}

	out := snapshot_in_place()

	if out.Dbs != 1 {
		t.Errorf("Dbs: got %d, want 1 (errors: %v)", out.Dbs, out.Errors)
	}
	if _, err := os.Stat(filepath.Join(tmp, "db", "users.db.backup")); err != nil {
		t.Errorf("expected new .backup sibling: %v", err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Errorf("legacy .snap sibling should have been removed: %v", err)
	}
}

func TestSnapshotSkipsRunAndCache(t *testing.T) {
	tmp := t.TempDir()
	prev_data_dir := data_dir
	data_dir = tmp
	defer func() { data_dir = prev_data_dir }()

	if err := os.MkdirAll(filepath.Join(tmp, "run"), 0750); err != nil {
		t.Fatal(err)
	}

	// A *.db file in run/ shouldn't be snapshotted.
	make_test_db(t, filepath.Join(tmp, "run", "should_be_ignored.db"))
	make_test_db(t, filepath.Join(tmp, "cache", "should_be_ignored.db"))
	make_test_db(t, filepath.Join(tmp, "db", "users.db"))

	out := snapshot_in_place()

	if out.Dbs != 1 {
		t.Errorf("expected only 1 DB to be snapshotted (excluding run/, cache/), got %d", out.Dbs)
	}
	if _, err := os.Stat(filepath.Join(tmp, "run", "should_be_ignored.db.backup")); err == nil {
		t.Errorf("run/ DB should not have been snapshotted")
	}
}
