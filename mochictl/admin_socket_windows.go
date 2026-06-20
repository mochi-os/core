// Default admin socket for Windows: the named pipe the server listens on
// (see core/server/admin_windows.go).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build windows

package main

// admin_socket_default returns the admin named-pipe name. Unlike the Unix
// socket it is not derived from the data dir; the server uses a fixed pipe
// name.
func admin_socket_default() string {
	return `\\.\pipe\mochi-admin`
}
