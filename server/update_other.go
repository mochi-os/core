// Mochi server: stub of the Windows-only self-install spawn helper.
// Copyright Alistair Cunningham 2026
//
// The self-install code path is guarded by `runtime.GOOS == "windows"`
// before it ever reaches update_install_detach, so this no-op exists
// only to keep update.go cross-compiling on Linux and macOS.

//go:build !windows

package main

import "os/exec"

func update_install_detach(cmd *exec.Cmd) {
	// No-op outside Windows.
	_ = cmd
}
