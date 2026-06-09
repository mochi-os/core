// Admin transport dialer for Windows: a named pipe.
// Copyright Alistair Cunningham 2026

//go:build windows

package adminclient

import (
	"context"
	"net"

	"github.com/Microsoft/go-winio"
)

// admin_dial dials the admin named pipe at path (e.g. \\.\pipe\mochi-admin).
func admin_dial(ctx context.Context, path string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, path)
}
