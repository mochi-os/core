// Mochi server: chown_recursive runs as root over a tree the unprivileged
// server can write, so it must act on links rather than their targets.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// A dangling symlink is the probe: os.Chown follows the link and fails ENOENT
// (which is not a permission error, so it propagates), while os.Lchown acts on
// the link itself and skipping non-regular entries never touches it at all.
// Planting one therefore separates "follows the target" from "does not" without
// needing root.
func TestChownRecursiveDoesNotFollowSymlinks(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "regular"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "planted")
	if err := os.Symlink(filepath.Join(dir, "absent"), link); err != nil {
		t.Fatal(err)
	}

	// Positive control: the tree really does contain a link the walk reaches,
	// so a pass below is the skip working rather than an empty walk.
	information, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("planted link not readable: %v", err)
	}
	if information.Mode()&os.ModeSymlink == 0 {
		t.Fatal("planted entry is not a symlink")
	}

	if err := chown_recursive(dir, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("chown_recursive followed the symlink: %v", err)
	}

	// The link is left as a link, not replaced or resolved.
	after, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("link gone after chown: %v", err)
	}
	if after.Mode()&os.ModeSymlink == 0 {
		t.Fatal("link is no longer a symlink")
	}
	if _, err := os.Stat(filepath.Join(dir, "absent")); !os.IsNotExist(err) {
		t.Fatal("the walk created the symlink's target")
	}
}

// The skip must not cost the walk its actual job: regular files and directories
// are still visited.
func TestChownRecursiveStillWalksRegularEntries(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, "file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := chown_recursive(dir, os.Getuid(), os.Getgid()); err != nil {
		t.Fatalf("chown_recursive failed on a plain tree: %v", err)
	}
}
