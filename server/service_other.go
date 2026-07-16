// Mochi server: Service stubs for non-Windows platforms.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build !windows

package main

// windows_service_run is a no-op outside Windows. Returns false so main()
// falls through to the interactive / Unix daemon path.
func windows_service_run() bool { return false }

// windows_service_redirect_logs is a no-op outside Windows.
func windows_service_redirect_logs() {}
