// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

func TestAttachmentDisposition(t *testing.T) {
	tests := []struct {
		name       string
		want_inline bool
	}{
		{"photo.jpg", true},
		{"photo.jpeg", true},
		{"image.png", true},
		{"image.gif", true},
		{"image.webp", true},
		{"video.mp4", true},
		{"audio.mp3", true},
		{"document.pdf", true},
		{"evil.html", false},
		{"evil.htm", false},
		{"evil.svg", false},
		{"script.js", false},
		{"style.css", false},
		{"data.json", false},
		{"archive.zip", false},
		{"program.exe", false},
		{"noextension", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct := attachment_content_type(tt.name)
			disposition := "attachment"
			if (strings.HasPrefix(ct, "image/") && ct != "image/svg+xml") ||
				strings.HasPrefix(ct, "video/") || strings.HasPrefix(ct, "audio/") ||
				ct == "application/pdf" {
				disposition = "inline"
			}
			is_inline := disposition == "inline"
			if is_inline != tt.want_inline {
				t.Errorf("%s: ct=%q disposition=%q, want_inline=%v", tt.name, ct, disposition, tt.want_inline)
			}
		})
	}
}
