// Mochi server: startup must not fail illegibly, and must not alarm on a state
// that is simply new.
//
// Both Unix sockets live under <data_dir>/run/, so a long directories.data can
// push them past sockaddr_un's 108-byte sun_path. The kernel answers EINVAL,
// Go prints "invalid argument", and nothing in that says "your path is two
// bytes too long" - the operator goes looking at permissions and SELinux.
//
// <data_dir>/apps/ was never created at startup the way <data_dir>/run/ is, so
// a server that has not yet installed an app had no such directory and the
// startup listing warned - which emails the administrator - on every start
// until the first install. The same missing directory was already tolerated
// silently fifteen lines earlier in the same function.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// paths_test_of_length builds a bindable socket path of exactly n bytes under a
// short root, padding with one long directory name (NAME_MAX is 255, well
// clear of the ~108 we care about).
func paths_test_of_length(t *testing.T, n int) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "mp")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })

	const leaf = "/s.sock"
	pad := n - len(root) - len(leaf) - 1 // -1 for the separator before the pad
	if pad < 1 {
		t.Fatalf("temp root %q is already %d bytes; cannot build a %d-byte path", root, len(root), n)
	}
	dir := filepath.Join(root, strings.Repeat("p", pad))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := dir + leaf
	if len(path) != n {
		t.Fatalf("built a %d-byte path, wanted %d", len(path), n)
	}
	return path
}

// TestSocketPathMaximumMatchesTheKernel is the test worth having: it asks the
// operating system where the boundary is rather than trusting a constant
// transcribed from a header. If a platform ever disagrees, this fails here
// instead of at a customer's startup.
func TestSocketPathMaximumMatchesTheKernel(t *testing.T) {
	allowed := paths_test_of_length(t, socket_path_maximum)
	listener, err := net.Listen("unix", allowed)
	if err != nil {
		t.Fatalf("a %d-byte path was rejected by the kernel: %v\nsocket_path_maximum is too high, so the check passes paths that then fail to bind", socket_path_maximum, err)
	}
	listener.Close()

	refused := paths_test_of_length(t, socket_path_maximum+1)
	listener, err = net.Listen("unix", refused)
	if err == nil {
		listener.Close()
		t.Errorf("a %d-byte path bound successfully; socket_path_maximum is too low, so the check refuses paths that would have worked", socket_path_maximum+1)
	}
}

// TestSocketPathCheckExplainsTheLimit. The whole point is the message: a
// refusal that does not say "too long" is no better than EINVAL.
func TestSocketPathCheckExplainsTheLimit(t *testing.T) {
	long := "/" + strings.Repeat("x", socket_path_maximum)
	err := socket_path_check("admin", long)
	if err == nil {
		t.Fatalf("a %d-byte path passed the check", len(long))
	}
	text := err.Error()
	for _, want := range []string{
		"admin",                           // which socket
		long,                              // the offending path
		strconv.Itoa(len(long)),           // how long it is
		strconv.Itoa(socket_path_maximum), // what the limit is
		"directories.data",                // what to change
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the error does not mention %q: %s", want, text)
		}
	}
	if strings.Contains(text, "invalid argument") {
		t.Error("the error still reads as EINVAL")
	}
}

// TestSocketPathCheckAdmitsTheLimit. An off-by-one here refuses a path the
// kernel would have taken, which is a server that will not start.
func TestSocketPathCheckAdmitsTheLimit(t *testing.T) {
	for _, size := range []int{1, socket_path_maximum - 1, socket_path_maximum} {
		path := "/" + strings.Repeat("x", size-1)
		if err := socket_path_check("world", path); err != nil {
			t.Errorf("a %d-byte path was refused: %v", size, err)
		}
	}
	if err := socket_path_check("world", "/"+strings.Repeat("x", socket_path_maximum)); err == nil {
		t.Errorf("a %d-byte path was admitted", socket_path_maximum+1)
	}
}

// TestBothListenersCheckBeforeBinding. The helper is worthless if only one of
// the two callers uses it, and the two failed identically in production.
func TestBothListenersCheckBeforeBinding(t *testing.T) {
	for file, kind := range map[string]string{"admin_unix.go": "admin", "world_unix.go": "world"} {
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		text := string(body)
		check := strings.Index(text, `socket_path_check("`+kind+`"`)
		bind := strings.Index(text, `net.Listen("unix", path)`)
		if check < 0 {
			t.Errorf("%s does not check the socket path length", file)
			continue
		}
		if bind >= 0 && check > bind {
			t.Errorf("%s checks the path after binding, so the kernel's EINVAL still wins", file)
		}
	}
}

// TestAppDirectoryExistsAfterStartup. The listing that warned must find the
// directory on a data directory that has never had an app installed.
func TestAppDirectoryExistsAfterStartup(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = original }()

	if _, err := file_list(apps_dir()); err == nil {
		t.Fatal("the app directory already exists on a fresh data directory; this test cannot show the fix")
	}

	if err := apps_dir_create(); err != nil {
		t.Fatalf("apps_dir_create: %v", err)
	}
	if _, err := file_list(apps_dir()); err != nil {
		t.Errorf("the app listing still fails after startup: %v\nthat warn emails the administrator on every start until the first install", err)
	}
}

// TestAppDirectoryCreateIsIdempotent, and leaves an existing directory's mode
// alone - a server restart must not silently retighten an operator's choice.
func TestAppDirectoryCreateIsIdempotent(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = original }()

	if err := os.MkdirAll(apps_dir(), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Chmod(apps_dir(), 0755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := apps_dir_create(); err != nil {
		t.Fatalf("apps_dir_create on an existing directory: %v", err)
	}
	information, err := os.Stat(apps_dir())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if information.Mode().Perm() != 0755 {
		t.Errorf("mode became %o; an existing app directory must keep the mode it had", information.Mode().Perm())
	}
}

// TestAppDirectoryIsPrivateWhenCreated. A new one is 0700, matching what
// installs leave on disk - app packages are not world-readable.
func TestAppDirectoryIsPrivateWhenCreated(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = original }()

	if err := apps_dir_create(); err != nil {
		t.Fatalf("apps_dir_create: %v", err)
	}
	information, err := os.Stat(apps_dir())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := information.Mode().Perm(); perm&0077 != 0 {
		t.Errorf("a new app directory is %o; it holds installed app packages and must not be group- or world-readable", perm)
	}
}

// TestStartupCreatesTheAppDirectory. The helper only helps if startup calls it,
// beside the run directory it mirrors.
func TestStartupCreatesTheAppDirectory(t *testing.T) {
	body, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "apps_dir_create()") {
		t.Error("startup never creates the app directory, so a fresh install still warns on every start")
	}
	if run := strings.Index(text, "run_dir_create()"); run >= 0 {
		if apps := strings.Index(text, "apps_dir_create()"); apps >= 0 && apps < run {
			t.Error("the app directory is created before the runtime directory; keep the startup order stable")
		}
	}
}
