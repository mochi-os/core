//go:build linux || darwin

// Mochi server: Cache disk capacity (unix)
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "golang.org/x/sys/unix"

// disk_capacity reports the total size in bytes of the filesystem holding
// path, or zero when it cannot be measured.
func disk_capacity(path string) int64 {
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		return 0
	}
	return int64(s.Blocks) * int64(s.Bsize)
}
