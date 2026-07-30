//go:build windows

// Mochi server: Cache disk capacity (windows)
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "golang.org/x/sys/windows"

// disk_capacity reports the total size in bytes of the filesystem holding
// path, or zero when it cannot be measured.
func disk_capacity(path string) int64 {
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0
	}
	var available, total, free uint64
	if err := windows.GetDiskFreeSpaceEx(pointer, &available, &total, &free); err != nil {
		return 0
	}
	return int64(total)
}
