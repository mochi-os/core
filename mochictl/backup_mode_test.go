// Mochi mochictl: the backup tarball is written private to its owner.
//
// The archive is the entire data directory - entity private keys, session
// secrets and the libp2p host key - and admin_backup preserves each file's
// own 0600 inside the tar. A world-readable container gives all of it away
// in one step, and the documented no-argument form drops the file in
// whatever directory cron happened to pick.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// backup_test_server stands in for the admin socket, answering
// GET /_/admin/backup with the given body. Returns the socket path.
func backup_test_server(t *testing.T, body string) string {
	t.Helper()

	// A short path: a unix socket address is capped near 100 bytes, and
	// t.TempDir() under a long test name overruns it.
	dir, err := os.MkdirTemp("", "mochictl")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "admin.sock")

	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/_/admin/backup", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/gzip")
		fmt.Fprint(w, body)
	})
	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })

	return path
}

// backup_run points cmd_backup at the stub server and runs it. The socket
// global is what client() reads, so set and restore it around the call.
func backup_run(t *testing.T, socket_path string, args ...string) error {
	t.Helper()
	previous := socket
	socket = socket_path
	t.Cleanup(func() { socket = previous })
	return cmd_backup(args)
}

// TestBackupFileIsPrivate is the finding: os.Create yields 0644 under the
// usual umask, so the tarball was readable by every local account.
func TestBackupFileIsPrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission bits")
	}
	socket_path := backup_test_server(t, "tarball")

	path := filepath.Join(t.TempDir(), "backup.tar.gz")
	if err := backup_run(t, socket_path, path); err != nil {
		t.Fatalf("backup: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("backup written %04o, want 0600 - readable by %s",
			mode, backup_readers(mode))
	}

	// The bytes still have to arrive; a mode fix that broke the write
	// would pass a permissions-only assertion.
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(content) != "tarball" {
		t.Errorf("content %q, want the streamed body", content)
	}
}

// TestBackupTightensAnExistingFile is the case the mode argument alone does
// not cover. O_CREATE applies its mode only when it creates the file, so a
// nightly cron writing to a fixed path truncates yesterday's 0644 file and
// inherits its mode forever.
func TestBackupTightensAnExistingFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission bits")
	}
	socket_path := backup_test_server(t, "tarball")

	path := filepath.Join(t.TempDir(), "nightly.tar.gz")
	if err := os.WriteFile(path, []byte("yesterday"), 0o644); err != nil {
		t.Fatalf("seed the previous run's file: %v", err)
	}

	if err := backup_run(t, socket_path, path); err != nil {
		t.Fatalf("backup: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("re-run left %04o, want 0600 - readable by %s",
			mode, backup_readers(mode))
	}
}

// TestBackupAutoNamedFileIsPrivate covers the documented no-argument form,
// which names the file itself and drops it in the working directory - the
// shape a cron entry actually takes, and the one whose destination the
// operator has least control over.
func TestBackupAutoNamedFileIsPrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission bits")
	}
	socket_path := backup_test_server(t, "tarball")

	dir := t.TempDir()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(previous) })

	if err := backup_run(t, socket_path); err != nil {
		t.Fatalf("backup: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "mochi-backup_*.tar.gz"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected one auto-named backup, got %v (err %v)", matches, err)
	}
	info, err := os.Stat(matches[0])
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("auto-named backup written %04o, want 0600 - readable by %s",
			mode, backup_readers(mode))
	}
}

// TestBackupToStdoutCreatesNoFile pins the one form that was never exposed,
// so a fix cannot quietly start writing a file for `mochictl backup -`.
func TestBackupToStdoutCreatesNoFile(t *testing.T) {
	socket_path := backup_test_server(t, "tarball")

	dir := t.TempDir()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { os.Chdir(previous) })

	// Capture stdout so the tarball does not land in the test output.
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	stdout := os.Stdout
	os.Stdout = write
	run_err := backup_run(t, socket_path, "-")
	os.Stdout = stdout
	write.Close()
	streamed, _ := io.ReadAll(read)

	if run_err != nil {
		t.Fatalf("backup -: %v", run_err)
	}
	if string(streamed) != "tarball" {
		t.Errorf("streamed %q, want the body on stdout", streamed)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("`backup -` created %d file(s) in the working directory; it must only stream", len(entries))
	}
}

// backup_readers names who a mode leaks to, so a failure says what went
// wrong rather than only which octal was expected.
func backup_readers(mode os.FileMode) string {
	switch {
	case mode&0o004 != 0 && mode&0o040 != 0:
		return "group and every other local account"
	case mode&0o004 != 0:
		return "every other local account"
	case mode&0o040 != 0:
		return "group"
	}
	return "nobody beyond the owner"
}
