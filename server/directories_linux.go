// Mochi server: optional directories.ensure mkdir/chown + privilege drop.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// With [directories] ensure = true (default off) the server starts as root,
// creates and chowns the cache/data/run directories, then drops to the
// configured uid/gid before serving. Used by the Docker image; deb/rpm let
// systemd start as the mochi user.

//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// directories_ensure runs the mkdir + chown + setuid sequence if the config
// requests it. Called early in startup (after ini_load, before any other
// component touches the filesystem).
func directories_ensure() error {
	if !ini_bool("directories", "ensure", false) {
		return nil
	}

	uid := ini_int("directories", "uid", 1000)
	gid := ini_int("directories", "gid", 1000)

	if os.Geteuid() != 0 {
		return fmt.Errorf("directories.ensure = true requires the server to start as root (current euid=%d)", os.Geteuid())
	}

	for _, dir := range []string{cache_dir, data_dir, run_dir()} {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	if err := chown_recursive(cache_dir, uid, gid); err != nil {
		warn("directories.ensure: chown %s: %v", cache_dir, err)
	}
	if err := chown_recursive(data_dir, uid, gid); err != nil {
		warn("directories.ensure: chown %s: %v", data_dir, err)
	}

	// setgid first, while still root: after setuid the process cannot regain root
	// and the setgid would fail.
	if err := syscall.Setgid(gid); err != nil {
		return fmt.Errorf("setgid(%d): %w", gid, err)
	}
	if err := syscall.Setuid(uid); err != nil {
		return fmt.Errorf("setuid(%d): %w", uid, err)
	}

	info("directories.ensure: dropped privileges to uid=%d gid=%d", uid, gid)
	return nil
}

// chown_recursive walks dir and chowns every entry. Tolerates EPERM on
// individual files (e.g. read-only volume mount) with a warning rather than
// failing the whole startup.
func chown_recursive(dir string, uid, gid int) error {
	return filepath.Walk(dir, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := os.Chown(path, uid, gid); err != nil && !os.IsPermission(err) {
			return err
		}
		return nil
	})
}
