// Admin transport dialer for Linux/macOS: a Unix domain socket.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build !windows

package adminclient

import (
	"context"
	"fmt"
	"net"
	"strings"
)

// admin_dial dials the admin Unix domain socket at path.
func admin_dial(ctx context.Context, path string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "unix", path)
}

// connect_hint maps the common Unix dial failures — server not running,
// socket missing, wrong user — to one-line errors with the action the
// operator needs. Returns nil for unrecognised errors.
func connect_hint(socket string, err error) error {
	message := err.Error()
	switch {
	case strings.Contains(message, "connection refused"):
		return fmt.Errorf("server is not running (no listener at %s)", socket)
	case strings.Contains(message, "no such file or directory"):
		return fmt.Errorf("admin socket not found at %s (server not started?)", socket)
	case strings.Contains(message, "permission denied"):
		return fmt.Errorf("permission denied on %s (run as the mochi user, or join the mochi group)", socket)
	}
	return nil
}
