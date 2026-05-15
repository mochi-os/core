// Mochi server: stub of the Windows-only self-install spawn helper.
// Copyright Alistair Cunningham 2026
//
// The self-install code path is guarded by `runtime.GOOS == "windows"`
// before it ever reaches update_install_spawn, so this no-op exists
// only to keep update.go cross-compiling on Linux and macOS.

//go:build !windows

package main

import "fmt"

func update_install_spawn(msi_path, msi_log string) error {
	return fmt.Errorf("self-install not supported on this platform")
}
