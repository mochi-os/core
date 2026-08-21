// Mochi server: stub of the Windows-only self-install spawn helper.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// Callers gate on runtime.GOOS == "windows"; this no-op only keeps update.go
// building on Linux and macOS.

//go:build !windows

package main

import "fmt"

func update_install_spawn(msi_path, msi_log string) error {
	return fmt.Errorf("self-install not supported on this platform")
}
