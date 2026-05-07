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

	_ "github.com/mattn/go-sqlite3"
)

// makeTestDB creates a sqlite db at path with one table and three rows.
func makeTestDB(t *testing.T, path string) {
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

// readTestDB returns the row count of table t.
func readTestDB(t *testing.T, path string) int {
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

func TestSnapshotInPlaceProducesSnapFiles(t *testing.T) {
	tmp := t.TempDir()
	prevDataDir := data_dir
	data_dir = tmp
	defer func() { data_dir = prevDataDir }()

	if err := os.MkdirAll(filepath.Join(tmp, "run"), 0750); err != nil {
		t.Fatal(err)
	}

	// Two live DBs in different sub-paths.
	makeTestDB(t, filepath.Join(tmp, "db", "users.db"))
	makeTestDB(t, filepath.Join(tmp, "users", "alice", "feeds", "db", "feed.db"))

	out := snapshot_in_place()

	if out.Dbs != 2 {
		t.Errorf("Dbs: got %d, want 2 (errors: %v)", out.Dbs, out.Errors)
	}
	if len(out.Errors) != 0 {
		t.Errorf("unexpected errors: %v", out.Errors)
	}

	// Both .snap files should exist with the same row count.
	for _, snap := range []string{
		filepath.Join(tmp, "db", "users.db.snap"),
		filepath.Join(tmp, "users", "alice", "feeds", "db", "feed.db.snap"),
	} {
		if _, err := os.Stat(snap); err != nil {
			t.Errorf("expected snapshot at %s: %v", snap, err)
			continue
		}
		if got := readTestDB(t, snap); got != 3 {
			t.Errorf("snapshot %s: got %d rows, want 3", snap, got)
		}
	}

	// No leftover .snap.tmp files.
	_ = filepath.WalkDir(tmp, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".snap.tmp") {
			t.Errorf("leftover temp file: %s", p)
		}
		return nil
	})
}

func TestSnapshotReapsStaleSnap(t *testing.T) {
	tmp := t.TempDir()
	prevDataDir := data_dir
	data_dir = tmp
	defer func() { data_dir = prevDataDir }()

	if err := os.MkdirAll(filepath.Join(tmp, "run"), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmp, "db"), 0750); err != nil {
		t.Fatal(err)
	}

	// Stale snap with no live sibling.
	stale := filepath.Join(tmp, "db", "deleted_app.db.snap")
	if err := os.WriteFile(stale, []byte("stale"), 0644); err != nil {
		t.Fatal(err)
	}

	// One real DB so the snapshot routine has something to do.
	makeTestDB(t, filepath.Join(tmp, "db", "users.db"))

	out := snapshot_in_place()

	if out.Reaped != 1 {
		t.Errorf("Reaped: got %d, want 1", out.Reaped)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale snap should have been removed: %v", err)
	}
}

func TestSnapshotSkipsRunAndCache(t *testing.T) {
	tmp := t.TempDir()
	prevDataDir := data_dir
	data_dir = tmp
	defer func() { data_dir = prevDataDir }()

	if err := os.MkdirAll(filepath.Join(tmp, "run"), 0750); err != nil {
		t.Fatal(err)
	}

	// A *.db file in run/ shouldn't be snapshotted.
	makeTestDB(t, filepath.Join(tmp, "run", "should_be_ignored.db"))
	makeTestDB(t, filepath.Join(tmp, "cache", "should_be_ignored.db"))
	makeTestDB(t, filepath.Join(tmp, "db", "users.db"))

	out := snapshot_in_place()

	if out.Dbs != 1 {
		t.Errorf("expected only 1 DB to be snapshotted (excluding run/, cache/), got %d", out.Dbs)
	}
	if _, err := os.Stat(filepath.Join(tmp, "run", "should_be_ignored.db.snap")); err == nil {
		t.Errorf("run/ DB should not have been snapshotted")
	}
}
