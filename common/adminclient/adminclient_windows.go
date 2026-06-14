// Admin transport dialer for Windows: a named pipe.
// Copyright Alistair Cunningham 2026

//go:build windows

package adminclient

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

// admin_dial dials the admin named pipe at path (e.g. \\.\pipe\mochi-admin).
func admin_dial(ctx context.Context, path string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, path)
}

// connect_hint maps the common Windows dial failures to one-line errors
// with the action the operator needs. ERROR_FILE_NOT_FOUND means the pipe
// doesn't exist (the service isn't running); ERROR_ACCESS_DENIED means the
// pipe's security descriptor rejected the caller — it admits only
// LocalSystem and Administrators (see core/server/admin_windows.go), so a
// non-elevated prompt is the usual cause. UAC disables the Administrators
// group in non-elevated tokens even for administrator accounts, which is
// why the elevation state is reported. Returns nil for unrecognised
// errors.
func connect_hint(socket string, err error) error {
	switch {
	case errors.Is(err, windows.ERROR_FILE_NOT_FOUND):
		return fmt.Errorf("admin pipe %s not found (is the mochi-server service running? Check Services.msc for 'Mochi Server')", socket)
	case errors.Is(err, windows.ERROR_ACCESS_DENIED):
		if windows.GetCurrentProcessToken().IsElevated() {
			return fmt.Errorf("access denied on %s even though this prompt is elevated; check that the pipe belongs to the mochi-server service (running as LocalSystem) and not another process", socket)
		}
		return fmt.Errorf("access denied on %s: the admin pipe admits only Administrators, run mochictl from an elevated prompt (Run as administrator)", socket)
	}
	return nil
}
