// Mochi server: <data_dir>/run/ holds runtime state (admin socket, future PID/lock files).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
)

// run_dir returns the absolute path of the runtime-state directory.
// Contents are recreated on each server start; nothing here is shipped in
// any package or image, and nothing here should be backed up.
func run_dir() string {
	return filepath.Join(data_dir, "run")
}

// run_dir_create ensures <data_dir>/run/ exists with mode 0750.
// Called early during startup, before the UDS admin listener tries to bind.
func run_dir_create() error {
	return os.MkdirAll(run_dir(), 0750)
}
