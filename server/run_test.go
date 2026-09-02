// Mochi server: the runtime-state directory and the path to it.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestRunDirectoryRestoresTheParentTraverseBit. The package post-install
// closes the data directory to 0700, and nothing reopened it, so a member of
// the mochi group could never reach the admin socket under run/. Creating
// run/ now restores the parent's group traverse bit, and touches nothing else
// about its mode.
func TestRunDirectoryRestoresTheParentTraverseBit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX modes")
	}
	original := data_dir
	defer func() { data_dir = original }()

	for _, before := range []os.FileMode{0700, 0750, 0755} {
		data_dir = filepath.Join(t.TempDir(), "data")
		if err := os.Mkdir(data_dir, before); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(data_dir, before); err != nil { // Mkdir applies the umask
			t.Fatal(err)
		}
		if err := run_dir_create(); err != nil {
			t.Fatalf("run_dir_create from %04o: %v", before, err)
		}
		information, err := os.Stat(data_dir)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := information.Mode().Perm(), before|0010; got != want {
			t.Errorf("data directory created at %04o is %04o after run_dir_create, want %04o", before, got, want)
		}
		information, err = os.Stat(run_dir())
		if err != nil {
			t.Fatal(err)
		}
		if got := information.Mode().Perm(); got != 0751 {
			t.Errorf("run directory is %04o, want 0751", got)
		}
	}
}
