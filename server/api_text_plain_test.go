// Mochi server: Plain text from markdown - tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// TestTextPlain: markdown becomes the text a network that takes no markup
// shows - emphasis and links unwrapped, paragraphs kept apart, entities
// decoded.
func TestTextPlain(t *testing.T) {
	fn := sl.NewBuiltin("mochi.text.plain", api_text_plain)
	value, err := api_text_plain(nil, fn, sl.Tuple{sl.String("Hello **world** &amp; [Mochi](https://mochi-os.org)\n\nSecond paragraph\nsame paragraph")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := sl.AsString(value)
	if text != "Hello world & Mochi\n\nSecond paragraph\nsame paragraph" {
		t.Errorf("plain = %q", text)
	}
	if _, err := api_text_plain(nil, fn, sl.Tuple{sl.MakeInt(1)}, nil); err == nil {
		t.Error("a number was accepted")
	}
}
