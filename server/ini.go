// Mochi server: Read ini file (thin shim over core/common/ini).
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"core/common/ini"
)

func ini_bool(section string, key string, def bool) bool {
	return ini.Bool(section, key, def)
}

func ini_int(section string, key string, def int) int {
	return ini.Int(section, key, def)
}

func ini_load(file string) error {
	return ini.Load(file)
}

func ini_loaded() bool {
	return ini.Loaded()
}

func ini_string(section string, key string, def string) string {
	return ini.String(section, key, def)
}

func ini_strings_commas(section string, key string) []string {
	return ini.Strings(section, key)
}

func ini_ints_commas(section string, key string) []int {
	return ini.Ints(section, key)
}
