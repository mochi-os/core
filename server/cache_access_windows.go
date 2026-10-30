//go:build windows

// Mochi server: Cache entry access time (windows)
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
	if data, ok := information.Sys().(*syscall.Win32FileAttributeData); ok {
		return time.Unix(0, data.LastAccessTime.Nanoseconds())
	}
	return information.ModTime()
}
