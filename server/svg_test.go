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

// TestSvgSanitizeStripsScripts checks the best-effort sanitizer removes the
// obvious dangerous constructs, including the self-closing <script/> that a "/"
// in an attribute value used to smuggle past. The Content-Security-Policy on
// the SVG response is the real guarantee; this only covers the sanitizer layer.
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
