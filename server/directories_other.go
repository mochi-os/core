// Mochi server: directories.ensure stub for non-Linux platforms.
// Copyright Alistair Cunningham 2026

//go:build !linux

package main

import "fmt"

// directories_ensure errors if the operator has set directories.ensure = true
// on a platform that doesn't support the privilege-drop sequence. Linux-only
// by design; see claude/plans/mochictl.md → "Platform support".
func directories_ensure() error {
	if ini_bool("directories", "ensure", false) {
		return fmt.Errorf("directories.ensure = true is only supported on Linux")
	}
	return nil
}
