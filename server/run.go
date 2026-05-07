// Mochi server: <data_dir>/run/ holds runtime state (admin socket, future PID/lock files).
// Copyright Alistair Cunningham 2026

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
