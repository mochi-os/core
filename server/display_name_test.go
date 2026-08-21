// Mochi server: names other people see are checked for more than newlines.
//
// Bidirectional overrides and isolates are category Cf, so they pass
// valid_with's Cc-only global filter; a name carrying U+202E renders reversed
// and HTML escaping does not touch it. Angle brackets are excluded as they are
// for entity names. Every such character below is a \u escape on purpose - raw
// they are invisible.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

// bidirectional_names are format characters that reorder or hide rendered
// text. Each is category Cf, so each passed the Cc-only global filter.
var bidirectional_names = map[string]string{
	"right-to-left override U+202E":     "Nice Server\u202egnip",
	"left-to-right override U+202D":     "\u202dAdmin Server",
	"right-to-left embedding U+202B":    "Server\u202bevil",
	"pop directional formatting U+202C": "Server\u202c",
	"right-to-left isolate U+2067":      "\u2067evil\u2069",
	"left-to-right mark U+200E":         "Server\u200e",
	"zero width joiner U+200D":          "Ad\u200dmin",
	"zero width non-joiner U+200C":      "Ad\u200cmin",
	"byte order mark U+FEFF":            "\ufeffServer",
}

// markup_names are refused by "name" already; "line" was not "name".
var markup_names = []string{
	"<script>alert(1)</script>",
	"<img src=x onerror=alert(1)>",
	"Server <b>bold</b>",
	"a > b",
	"a < b",
}

func TestDisplayRefusesBidirectionalControls(t *testing.T) {
	for label, name := range bidirectional_names {
		if valid(name, "display") {
			t.Errorf("%s: valid(%+q, \"display\") = true; it reorders or hides rendered text", label, name)
		}
		// The premise: "line" let it through, so this is a real change.
		if !valid(name, "line") {
			t.Errorf("%s: valid(%+q, \"line\") already refused it; this test's premise is gone", label, name)
		}
	}
}

func TestDisplayRefusesMarkup(t *testing.T) {
	for _, name := range markup_names {
		if valid(name, "display") {
			t.Errorf("valid(%q, \"display\") = true; the value is interpolated by consumers this server does not control", name)
		}
	}
}

// TestDisplayAcceptsRealNames is the half that stops this being a blanket
// refusal. Every script, accents, emoji and punctuation must survive - a name
// in Japanese or Arabic is an ordinary name, not an attack.
func TestDisplayAcceptsRealNames(t *testing.T) {
	for _, name := range []string{
		"Dogfight Alley",
		"Café Olé",
		"東京サーバー",
		"Сервер",
		"مرحبا",
		"Server ✈ 3",
		"O'Brien's Server",
		"Test (EU) [beta] - #2",
		"100% Uptime",
		strings.Repeat("a", 1000),
	} {
		if !valid(name, "display") {
			t.Errorf("valid(%q, \"display\") = false; that is an ordinary name", name)
		}
	}
}

// TestDisplayKeepsTheGlobalControlFilter: Cc was already refused and must stay
// refused - mochictl prints these names to a terminal.
func TestDisplayKeepsTheGlobalControlFilter(t *testing.T) {
	for _, name := range []string{
		"Server[31m",
		"Server",
		"Server\nSecond",
		"Server\rSecond",
		"",
		strings.Repeat("a", 1001),
	} {
		if valid(name, "display") {
			t.Errorf("valid(%+q, \"display\") = true, want false", name)
		}
	}
}

// TestWorldValidateRefusesBidirectionalNames drives world_validate, so the
// check is wired into the path both the local push and gossip take.
func TestWorldValidateRefusesBidirectionalNames(t *testing.T) {
	id := strings.Repeat("a", 32)
	services := `[{"service":"air","players":1}]`

	if _, ok := world_validate(id, "Nice Server\u202egnip", "example.com:4433", "1", services); ok {
		t.Error("world_validate accepted a world name carrying a right-to-left override; the listing is gossiped to other servers and rendered on their join pages")
	}
	if _, ok := world_validate(id, "<script>alert(1)</script>", "example.com:4433", "1", services); ok {
		t.Error("world_validate accepted a world name carrying markup")
	}
	// The per-service name is validated separately and was the second site.
	hostile := "[{\"service\":\"air\",\"players\":1,\"name\":\"Match\u202egnip\"}]"
	if _, ok := world_validate(id, "Nice Server", "example.com:4433", "1", hostile); ok {
		t.Error("world_validate accepted a per-service name carrying a right-to-left override")
	}
	if _, ok := world_validate(id, "Nice Server", "example.com:4433", "1", services); !ok {
		t.Error("world_validate rejected an ordinary listing")
	}
}

// TestNoDisplayNameUsesTheLineValidator is the gate. "line" remains correct
// for a string that is genuinely just one line; it is not correct for a name
// that arrives over the network and is shown to someone.
func TestNoDisplayNameUsesTheLineValidator(t *testing.T) {
	for _, target := range []struct{ file, field string }{
		{"world.go", "name"},
		{"world.go", "s.Name"},
		{"directory.go", "en.Name"},
	} {
		data, err := os.ReadFile(target.file)
		if err != nil {
			t.Fatalf("reading %s: %v", target.file, err)
		}
		if strings.Contains(string(data), `valid(`+target.field+`, "line")`) {
			t.Errorf(`%s validates %s with "line"; that type excludes only CR and LF, so angle brackets and the category-Cf bidirectional controls pass. Use "display"`, target.file, target.field)
		}
	}
}
