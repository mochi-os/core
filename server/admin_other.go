// Mochi server: admin stubs for platforms without a UDS admin listener.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// The admin listener exists on Linux and macOS (UDS, admin_unix.go) and Windows
// (named pipe, admin_windows.go). Elsewhere admin_start is a no-op.

//go:build !linux && !darwin && !windows

package main

// admin_start is a no-op on platforms with no admin transport wired up. The
// admin endpoints are intentionally unreachable there.
func admin_start() error {
	return nil
}
