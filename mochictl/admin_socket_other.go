// Default admin socket path for Linux/macOS: <data_dir>/run/admin.sock.
// Copyright Alistair Cunningham 2026

//go:build !windows

package main

import (
	"path/filepath"

	"core/common/ini"
)

// admin_socket_default returns the admin UDS path derived from the data dir in
// the loaded mochi.conf (defaulting to /var/lib/mochi).
func admin_socket_default() string {
	data := ini.String("directories", "data", "/var/lib/mochi")
	return filepath.Join(data, "run", "admin.sock")
}
