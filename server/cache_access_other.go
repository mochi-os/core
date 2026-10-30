//go:build !linux && !darwin && !windows

// Mochi server: Cache entry access time (other platforms)
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"time"
)

// file_accessed reports when a file was last used; where the access time
// cannot be read, eviction falls back to the order entries were written.
func file_accessed(information os.FileInfo) time.Time {
	return information.ModTime()
}
