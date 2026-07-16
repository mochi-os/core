// mochictl: supervisor stub for non-Linux platforms.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Service lifecycle (`mochictl start`) shells out to systemd/Docker, which
// only exist on Linux. On Windows and macOS the OS service manager owns the
// mochi-server lifecycle, so this returns a clear pointer there. Every other
// mochictl subcommand talks to the admin transport (UDS / named pipe) and
// works on all platforms.

//go:build !linux

package main

import "fmt"

// supervisor_start is unsupported off Linux: there is no systemd/Docker to
// shell to. Direct the operator at their platform's service manager.
func supervisor_start() error {
	return fmt.Errorf("`mochictl start` is only supported on Linux; start mochi-server via your platform's service manager (Windows: `sc start mochi-server` or Services.msc; macOS: launchctl)")
}
