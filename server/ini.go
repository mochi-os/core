// Mochi server: Read ini file (thin shim over core/common/ini).
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"core/common/ini"

	goini "gopkg.in/ini.v1"
)

// ini_parse reads an ini source with inline comments disabled, so a ";" or "#"
// inside a value is text rather than the start of a comment. go-ini's default
// truncates at the first one, silently: errors.populated_server read "This
// server has users" and lost the sentence saying what to do instead, in every
// locale. Full-line ";" and "#" comments are unaffected - the option governs
// only what happens after a value has begun.
//
// Every ini source in this package goes through here. core/common/ini parses
// mochi.conf with the same option, for the same reason.
func ini_parse(source any) (*goini.File, error) {
	return goini.LoadSources(goini.LoadOptions{IgnoreInlineComment: true}, source)
}

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
