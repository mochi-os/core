// Default admin socket path for Linux/macOS: <data_dir>/run/admin.sock.
// Copyright Alistair Cunningham 2026

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
