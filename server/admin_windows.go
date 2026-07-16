// Mochi server: Windows admin transport — named-pipe listener, admin_start,
// and the LockFileEx-based snapshot lock.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Windows has no Unix-socket peer credentials (SO_PEERCRED / LOCAL_PEERCRED),
// so the admin channel is a named pipe whose security descriptor gates access
// at connect time: only LocalSystem (SY) and the Administrators group (BA) may
// open it. That mirrors the Unix design — the OS, not a token, proves the
// caller's identity — so no per-connection credential check is needed and no
// admin_cred is attached (admin_peer_cred returns nil, and the audit row logs
// peer_uid/gid as -1). The same /_/admin/* router (admin_routes.go) is served
// over the pipe. The server runs as the LocalSystem service installed by the
// MSI, so it can create the pipe with this descriptor; the operator runs
// mochictl from an elevated (Administrator) prompt.

//go:build windows

package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Microsoft/go-winio"
	"github.com/gin-gonic/gin"
	"golang.org/x/sys/windows"
)

// admin_pipe_sddl restricts the admin pipe to LocalSystem and the built-in
// Administrators group, with no inheritance (P): an Allow ACE granting
// GENERIC_ALL (GA) to SY and BA.
const admin_pipe_sddl = "D:P(A;;GA;;;SY)(A;;GA;;;BA)"

// admin_socket_path returns the admin pipe name. mochictl resolves the same
// name (see mochictl/admin_socket_windows.go).
func admin_socket_path() string {
	return `\\.\pipe\mochi-admin`
}

// admin_start binds the admin named pipe and serves the admin Gin router in a
// goroutine. Called once during startup.
func admin_start() error {
	pipe := admin_socket_path()
	ln, err := winio.ListenPipe(pipe, &winio.PipeConfig{SecurityDescriptor: admin_pipe_sddl})
	if err != nil {
		return fmt.Errorf("listen on admin pipe %s: %w", pipe, err)
	}

	admin_router = gin.New()
	admin_router.Use(gin.Recovery())
	admin_register_routes(admin_router)

	server := &http.Server{Handler: admin_router}
	go func() {
		info("admin: listening on %s", pipe)
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			warn("admin: server.Serve returned: %v", err)
		}
	}()

	return nil
}

// snapshot_acquire_lock takes an exclusive lock on <run_dir>/snapshot.lock via
// LockFileEx so concurrent snapshot calls don't race. The lock is released
// when the handle closes (and on process exit), so a crashed snapshot won't
// leave a stale lock.
func snapshot_acquire_lock() (*os.File, error) {
	lock_path := filepath.Join(run_dir(), snapshot_lock_name)
	f, err := os.OpenFile(lock_path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", lock_path, err)
	}
	overlapped := new(windows.Overlapped)
	err = windows.LockFileEx(windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("snapshot already in progress (lock %s busy)", lock_path)
	}
	return f, nil
}

// snapshot_release_lock releases the lock and removes the lockfile so the run/
// dir doesn't accumulate leftovers.
func snapshot_release_lock(f *os.File) {
	overlapped := new(windows.Overlapped)
	_ = windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, overlapped)
	_ = f.Close()
	_ = os.Remove(filepath.Join(run_dir(), snapshot_lock_name))
}
