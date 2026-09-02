// Admin transport client tests.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build !windows

package adminclient

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLiveTellsAnUnusablePathFromARefusingListener. Live fails closed on any
// dial error it does not recognise, which is right for "permission denied"
// and wrong for a path the kernel cannot use as a socket address at all: the
// restore guard then blames a running server on a tree nothing could bind.
func TestLiveTellsAnUnusablePathFromARefusingListener(t *testing.T) {
	dir, err := os.MkdirTemp("", "adminclient")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	if Live(filepath.Join(dir, "absent.sock")) {
		t.Errorf("a path with nothing at it counts as live")
	}
	long := filepath.Join(dir, strings.Repeat("a", 120)+".sock")
	if Live(long) {
		t.Errorf("a path over the socket address limit counts as live")
	}

	listener, err := net.Listen("unix", filepath.Join(dir, "admin.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if !Live(listener.Addr().String()) {
		t.Errorf("a listening socket does not count as live")
	}
}
