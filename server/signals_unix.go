// Mochi server: signals on Unix (SIGTERM for graceful shutdown, SIGHUP for reload).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build !windows

package main

import (
	"os"
	"syscall"
)

// extra_signals returns the OS-specific signals the main loop should wait on
// in addition to os.Interrupt. SIGHUP is included so a legacy
// `systemctl reload mochi-server` (which sends kill -HUP) doesn't terminate
// the process via the default signal action — the loop logs and ignores it.
func extra_signals() []os.Signal {
	return []os.Signal{syscall.SIGTERM, syscall.SIGHUP}
}

// is_ignorable_signal reports whether a signal should be logged and ignored
// rather than triggering shutdown. SIGHUP is the only one currently — it
// used to mean "reload config" but reload was dropped (config changes
// require restart). Receiving it now is a no-op.
func is_ignorable_signal(s os.Signal) bool {
	return s == syscall.SIGHUP
}
