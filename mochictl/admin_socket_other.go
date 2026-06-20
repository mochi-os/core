// Default admin socket path for Linux/macOS: <data_dir>/run/admin.sock.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build !windows

package main

import (
	"path/filepath"

	"core/common/ini"
	"core/common/paths"
)

// admin_socket_default returns the admin UDS path derived from the data dir in
// the loaded mochi.conf, defaulting to the platform data directory.
func admin_socket_default() string {
	data := ini.String("directories", "data", paths.Data())
	return filepath.Join(data, "run", "admin.sock")
}
