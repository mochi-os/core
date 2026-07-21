// Mochi server: temporary directory tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestTemporaryConfigure pins that the process spools temporary files under
// cache_dir rather than the system default.
//
// Go writes the part of a multipart upload that exceeds its in-memory
// threshold to os.TempDir(). On a systemd host that is normally a tmpfs, so
// the "spill to disk" that bounds memory does the opposite — a large upload is
// held in RAM, competing with the server itself.
func TestTemporaryConfigure(t *testing.T) {
	original_cache := cache_dir
	original_tmpdir, had_tmpdir := os.LookupEnv("TMPDIR")
	t.Cleanup(func() {
		cache_dir = original_cache
		if had_tmpdir {
			os.Setenv("TMPDIR", original_tmpdir)
		} else {
			os.Unsetenv("TMPDIR")
		}
	})
	cache_dir = t.TempDir()

	temporary_configure()

	expected := filepath.Join(cache_dir, "tmp")
	if got := os.TempDir(); got != expected {
		t.Errorf("os.TempDir() = %q, want %q — uploads would spool to the system default", got, expected)
	}
	info, err := os.Stat(expected)
	if err != nil {
		t.Fatalf("temporary directory was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", expected)
	}

	// The spooled parts of an upload land here, so the directory must not be
	// readable by other users on the host.
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("temporary directory is mode %o, accessible beyond its owner", mode)
	}

	// And it must be tightened even when it already exists: this directory
	// predates the change — app staging created it 0755 — and MkdirAll leaves
	// an existing directory's mode alone, so asserting only on a freshly
	// created one would pass while every real install stayed world-listable.
	if err := os.Chmod(expected, 0o755); err != nil {
		t.Fatal(err)
	}
	temporary_configure()
	again, err := os.Stat(expected)
	if err != nil {
		t.Fatal(err)
	}
	if mode := again.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("existing temporary directory left at mode %o, accessible beyond its owner", mode)
	}

	// What actually matters: a temp file created the way multipart creates one
	// has to land there.
	file, err := os.CreateTemp("", "upload-")
	if err != nil {
		t.Fatalf("create temporary file: %v", err)
	}
	defer os.Remove(file.Name())
	file.Close()
	if directory := filepath.Dir(file.Name()); directory != expected {
		t.Errorf("a temporary file was created in %q, want %q", directory, expected)
	}
}

// TestTemporaryConfigureSurvivesFailure pins that an unusable cache directory
// degrades to the system default rather than stopping the server. Writing
// uploads to the wrong place is a memory problem; refusing to start is an
// outage.
func TestTemporaryConfigureSurvivesFailure(t *testing.T) {
	original_cache := cache_dir
	original_tmpdir, had_tmpdir := os.LookupEnv("TMPDIR")
	t.Cleanup(func() {
		cache_dir = original_cache
		if had_tmpdir {
			os.Setenv("TMPDIR", original_tmpdir)
		} else {
			os.Unsetenv("TMPDIR")
		}
	})

	// A file where the directory should be: MkdirAll cannot succeed.
	blocked := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	cache_dir = blocked
	os.Unsetenv("TMPDIR")

	temporary_configure() // must not panic

	if got := os.TempDir(); got == filepath.Join(cache_dir, "tmp") {
		t.Errorf("TMPDIR was set to %q, which could not be created", got)
	}
}
