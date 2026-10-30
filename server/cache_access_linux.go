//go:build linux

// Mochi server: Cache entry access time (linux)
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"syscall"
	"time"
)

// file_accessed reports when a file was last used, as cache_used marks it.
func file_accessed(information os.FileInfo) time.Time {
	if stat, ok := information.Sys().(*syscall.Stat_t); ok {
		return time.Unix(stat.Atim.Unix())
	}
	return information.ModTime()
}
