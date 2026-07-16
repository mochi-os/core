// Mochi server: directories.ensure stub for non-Linux platforms.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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
