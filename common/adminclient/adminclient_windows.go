// Admin transport dialer for Windows: a named pipe.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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

// connect_hint maps common Windows dial failures to a one-line error naming the
// operator's next action; nil for anything unrecognised. Elevation is reported
// because UAC disables the Administrators group in a non-elevated token.
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
