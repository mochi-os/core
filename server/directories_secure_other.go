// Mochi server: data directory ACL hardening is Windows-only; the Linux
// packaging achieves the same with UMask=0077 and chmod go-rwx.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

//go:build !windows

package main

// directories_secure does nothing off Windows.
func directories_secure(_ string) error { return nil }
