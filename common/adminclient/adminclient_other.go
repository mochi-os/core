// Admin transport dialer for Linux/macOS: a Unix domain socket.
// Copyright Alistair Cunningham 2026

//go:build !windows

package adminclient

import (
	"context"
	"net"
)

// admin_dial dials the admin Unix domain socket at path.
func admin_dial(ctx context.Context, path string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "unix", path)
}
