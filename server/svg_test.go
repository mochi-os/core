// Mochi server: SVG sanitization tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
)

// TestContentTypeInline pins the inline/download split shared by
// web_serve_attachment and the a.write.stream guard. SVG must stay out of the
// inline set: it is the one image type that can carry script, so it is served
// only through the sanitize-and-CSP path.
func TestContentTypeInline(t *testing.T) {
	inline := []string{"image/png", "image/jpeg", "image/gif", "image/webp", "video/mp4", "audio/mpeg", "application/pdf"}
	for _, ct := range inline {
		if !content_type_inline(ct) {
			t.Errorf("content_type_inline(%q) = false, want true", ct)
		}
	}

	download := []string{"image/svg+xml", "text/html", "application/xhtml+xml", "text/xml", "application/javascript", "application/octet-stream", ""}
	for _, ct := range download {
		if content_type_inline(ct) {
			t.Errorf("content_type_inline(%q) = true, want false", ct)
		}
	}
}

// TestContentTypeBase checks parameters and casing are stripped before the
// comparison, so an SVG cannot slip into the inline set by announcing itself
// as "IMAGE/SVG+XML" or by trailing a charset.
func TestContentTypeBase(t *testing.T) {
	cases := map[string]string{
		"image/svg+xml":                  "image/svg+xml",
		"IMAGE/SVG+XML":                  "image/svg+xml",
		"image/svg+xml; charset=utf-8":   "image/svg+xml",
		"  image/svg+xml ; charset=utf8": "image/svg+xml",
		"image/png":                      "image/png",
	}
	for in, want := range cases {
		if got := content_type_base(in); got != want {
			t.Errorf("content_type_base(%q) = %q, want %q", in, got, want)
		}
		if want == "image/svg+xml" && content_type_inline(content_type_base(in)) {
			t.Errorf("%q reached the inline set", in)
		}
	}
}

// TestSvgSanitizeStripsScripts covers the best-effort sanitizer layer only. The
// Content-Security-Policy on the SVG response is the real guarantee.
func TestSvgSanitizeStripsScripts(t *testing.T) {
	stripped := []struct {
		name  string
		input string
		gone  string // substring that must NOT survive
	}{
		{"paired script", `<svg><script>alert(1)</script></svg>`, "alert"},
		{"self-closing script with url", `<svg><script href="https://evil.example/x.js"/></svg>`, "<script"},
		{"self-closing script data uri", `<svg><script href="data:text/javascript,alert(1)"/></svg>`, "<script"},
		{"foreignObject", `<svg><foreignObject><body>x</body></foreignObject></svg>`, "<foreignObject"},
		{"onload attr", `<svg onload="alert(1)"><rect/></svg>`, "onload"},
		{"onerror attr", `<svg><image onerror="alert(1)"/></svg>`, "onerror"},
		{"javascript href", `<svg><a xlink:href="javascript:alert(1)">x</a></svg>`, "javascript:alert"},
	}
	for _, tc := range stripped {
		t.Run(tc.name, func(t *testing.T) {
			out := string(svg_sanitize([]byte(tc.input)))
			if strings.Contains(strings.ToLower(out), strings.ToLower(tc.gone)) {
				t.Errorf("svg_sanitize kept %q\n  in:  %s\n  out: %s", tc.gone, tc.input, out)
			}
		})
	}

	// Legitimate self-contained SVG content must survive intact.
	safe := `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M1 1 L15 15" fill="#333"/><rect x="0" y="0" width="8" height="8"/></svg>`
	out := string(svg_sanitize([]byte(safe)))
	for _, want := range []string{"<path", `d="M1 1 L15 15"`, "<rect", "viewBox"} {
		if !strings.Contains(out, want) {
			t.Errorf("svg_sanitize dropped legitimate content %q from %s -> %s", want, safe, out)
		}
	}
}
