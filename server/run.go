// Mochi server: <data_dir>/run/ holds runtime state (admin socket, future PID/lock files).
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// run_dir returns the absolute path of the runtime-state directory.
// Contents are recreated on each server start; nothing here is shipped in
// any package or image, and nothing here should be backed up.
func run_dir() string {
	return filepath.Join(data_dir, "run")
}

// run_dir_create ensures <data_dir>/run/ exists, before the UDS admin listener
// binds. Mode 0751: the traverse bit is load-bearing - each socket is gated by
// its own 0660 and group, and a mochi-world process is not in the mochi group.
func run_dir_create() error {
	if err := os.MkdirAll(run_dir(), 0751); err != nil {
		return err
	}
	return os.Chmod(run_dir(), 0751) // an existing directory keeps its old mode through MkdirAll
}

// socket_path_maximum is the longest path net.Listen("unix", ...) can bind.
// sockaddr_un.sun_path is char[108] and the kernel wants a terminating NUL, so
// 107 bytes are usable.
const socket_path_maximum = 107

// socket_path_check rejects a path too long to bind. Past the limit bind
// returns EINVAL, which Go renders as "invalid argument" - a message that sends
// the operator looking at permissions instead of at data_dir's length.
func socket_path_check(kind, path string) error {
	if len(path) <= socket_path_maximum {
		return nil
	}
	return fmt.Errorf("%s socket path %s is %d bytes, over the %d the operating system allows: set a shorter directories.data",
		kind, path, len(path), socket_path_maximum)
}
