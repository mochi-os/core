// Package paths resolves the platform default locations of mochi.conf and
// the cache/data directories. Shared by mochi-server and mochictl so both
// see the same installed layout on every platform.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package paths

import (
	"os"
	"path/filepath"
	"runtime"
)

// Config returns the platform default mochi.conf path.
func Config() string {
	config, _, _ := defaults()
	return config
}

// Cache returns the platform default cache directory.
func Cache() string {
	_, cache, _ := defaults()
	return cache
}

// Data returns the platform default data directory.
func Data() string {
	_, _, data := defaults()
	return data
}

// defaults resolves the config/cache/data trio for the current platform.
func defaults() (config, cache, data string) {
	config = "/etc/mochi/mochi.conf"
	cache = "/var/cache/mochi"
	data = "/var/lib/mochi"
	switch runtime.GOOS {
	case "darwin":
		// Prefer the .pkg-installed system layout when /etc/mochi/mochi.conf
		// exists. Otherwise fall back to macOS-native per-user paths so
		// running from source without `sudo make install` Just Works.
		if exists("/etc/mochi/mochi.conf") {
			return
		}
		home := os.Getenv("HOME")
		support := filepath.Join(home, "Library", "Application Support", "Mochi")
		config = filepath.Join(support, "mochi.conf")
		cache = filepath.Join(home, "Library", "Caches", "Mochi")
		data = support
	case "windows":
		// %ProgramData%\Mochi is shared across users and accessible to the
		// LocalSystem account that the Windows service runs under. Falls
		// back to %LocalAppData%\mochi if ProgramData isn't set (rare).
		program := os.Getenv("ProgramData")
		if program == "" {
			program = os.Getenv("ALLUSERSPROFILE")
		}
		if program != "" {
			config = filepath.Join(program, "Mochi", "mochi.conf")
			cache = filepath.Join(program, "Mochi", "cache")
			data = filepath.Join(program, "Mochi", "data")
		} else {
			local := os.Getenv("LOCALAPPDATA")
			if local == "" {
				local = filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Local")
			}
			config = filepath.Join(local, "mochi", "mochi.conf")
			cache = filepath.Join(local, "mochi", "cache")
			data = filepath.Join(local, "mochi", "data")
		}
	}
	return
}

// exists reports whether the path exists.
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
