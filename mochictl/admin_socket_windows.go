// Default admin socket for Windows: the named pipe the server listens on
// (see core/server/admin_windows.go).
// Copyright Alistair Cunningham 2026

//go:build windows

package main

// admin_socket_default returns the admin named-pipe name. Unlike the Unix
// socket it is not derived from the data dir; the server uses a fixed pipe
// name.
func admin_socket_default() string {
	return `\\.\pipe\mochi-admin`
}
