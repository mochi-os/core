// mochictl: restore must refuse to run against a live server.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build !windows

package main

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// restore_tree builds a data directory holding one snapshot and the live
// database it would be renamed over. Returns the root.
func restore_tree(t *testing.T) string {
	t.Helper()

	// A short path: the tree also carries a unix socket, whose address is
	// capped near 100 bytes, and t.TempDir() under a long test name overruns it.
	root, err := os.MkdirTemp("", "restore")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })

	if err := os.MkdirAll(filepath.Join(root, "db"), 0o755); err != nil {
		t.Fatalf("make db dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "db", "users.db"), []byte("live"), 0o644); err != nil {
		t.Fatalf("write live database: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "db", "users.db.backup"), []byte("snapshot"), 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	return root
}

// restore_listener puts a listening socket at <root>/run/admin.sock, standing
// in for a running server, and returns it so a caller can close it. It answers
// nothing: the guard only dials.
func restore_listener(t *testing.T, root string) net.Listener {
	t.Helper()

	if err := os.MkdirAll(filepath.Join(root, "run"), 0o755); err != nil {
		t.Fatalf("make run dir: %v", err)
	}
	listener, err := net.Listen("unix", filepath.Join(root, "run", "admin.sock"))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	return listener
}

// restore_isolate points the configured socket at a path with nothing on it, so
// only the tree's own socket can make the guard fire.
func restore_isolate(t *testing.T) {
	t.Helper()

	previous := socket
	socket = filepath.Join(t.TempDir(), "absent.sock")
	t.Cleanup(func() { socket = previous })
}

func TestRestoreRefusesWhileTheServerIsLive(t *testing.T) {
	restore_isolate(t)
	root := restore_tree(t)
	restore_listener(t, root)

	err := cmd_restore([]string{root})
	if err == nil {
		t.Fatal("restore succeeded against a live admin socket; it must refuse")
	}
	if !strings.Contains(err.Error(), "running") {
		t.Errorf("error does not explain the refusal: %v", err)
	}

	// The point of the guard: nothing was renamed over the live database.
	live, read_error := os.ReadFile(filepath.Join(root, "db", "users.db"))
	if read_error != nil {
		t.Fatalf("read live database: %v", read_error)
	}
	if string(live) != "live" {
		t.Errorf("live database was overwritten with %q despite the refusal", live)
	}
	if _, stat_error := os.Stat(filepath.Join(root, "db", "users.db.backup")); stat_error != nil {
		t.Errorf("snapshot was consumed despite the refusal: %v", stat_error)
	}
}

func TestRestoreProceedsWhenNothingIsListening(t *testing.T) {
	restore_isolate(t)
	root := restore_tree(t)

	if err := cmd_restore([]string{root}); err != nil {
		t.Fatalf("restore refused with no server running: %v", err)
	}

	live, err := os.ReadFile(filepath.Join(root, "db", "users.db"))
	if err != nil {
		t.Fatalf("read restored database: %v", err)
	}
	if string(live) != "snapshot" {
		t.Errorf("restored database holds %q, want the snapshot", live)
	}
}

// A socket file with no listener is what a crashed server leaves behind. It
// must not be mistaken for a live one, or restore becomes unusable exactly when
// it is needed.
func TestRestoreIgnoresAStaleSocketFile(t *testing.T) {
	restore_isolate(t)
	root := restore_tree(t)
	listener := restore_listener(t, root)

	// Close the listener but leave its socket file behind, as a crash does. Go's
	// unix listener unlinks the file on Close, so put it back.
	stale := filepath.Join(root, "run", "admin.sock")
	listener.Close()
	if err := os.WriteFile(stale, nil, 0o644); err != nil {
		t.Fatalf("recreate the stale socket file: %v", err)
	}

	if err := cmd_restore([]string{root}); err != nil {
		t.Fatalf("restore refused on a stale socket file: %v", err)
	}
}

// TestRestoreRemovesStaleSidecars. A crashed server leaves the live database's
// WAL and shared-memory files behind, and SQLite replays that WAL onto the
// restored snapshot at the next open, corrupting it (reproduced: "database disk
// image is malformed"). Only the snapshot may remain beside the live name.
func TestRestoreRemovesStaleSidecars(t *testing.T) {
	restore_isolate(t)
	root := restore_tree(t)
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if err := os.WriteFile(filepath.Join(root, "db", "users.db"+suffix), []byte("stale"), 0o644); err != nil {
			t.Fatalf("seed %s: %v", suffix, err)
		}
	}

	if err := cmd_restore([]string{root}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(root, "db"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if strings.Join(names, " ") != "users.db" {
		t.Errorf("db/ holds %v after the restore, want only users.db", names)
	}
}
