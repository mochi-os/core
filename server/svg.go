// Mochi server: SVG sanitization
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"regexp"
	"strings"
)

var (
	// Match dangerous elements and their content (case-insensitive, dotall)
	svg_re_dangerous = regexp.MustCompile(`(?is)<\s*(script|foreignObject|iframe|embed|object)\b[^>]*>.*?</\s*(script|foreignObject|iframe|embed|object)\s*>`)
	// Match self-closing dangerous elements. Use [^>]*? (not [^/]*) so a "/" in
	// an attribute value (e.g. href="https://…") doesn't end the match early
	// and let the element survive.
	svg_re_dangerous_sc = regexp.MustCompile(`(?i)<\s*(script|foreignObject|iframe|embed|object)\b[^>]*?/\s*>`)
	// Match on* event handler attributes (onclick, onload, onerror, etc.)
	svg_re_on_attr = regexp.MustCompile(`(?i)\s+on\w+\s*=\s*("[^"]*"|'[^']*'|[^\s>]*)`)
	// Match javascript: in href/xlink:href attributes
	svg_re_js_href    = regexp.MustCompile(`(?i)((?:xlink:)?href\s*=\s*")javascript:[^"]*"`)
	svg_re_js_href_sq = regexp.MustCompile(`(?i)((?:xlink:)?href\s*=\s*')javascript:[^']*'`)
)

// svg_sanitize removes dangerous elements and attributes from SVG content.
// Uses regex-based stripping to preserve the original formatting exactly.
func svg_sanitize(data []byte) []byte {
	s := string(data)
	s = svg_re_dangerous.ReplaceAllString(s, "")
	s = svg_re_dangerous_sc.ReplaceAllString(s, "")
	s = svg_re_on_attr.ReplaceAllString(s, "")
	s = svg_re_js_href.ReplaceAllStringFunc(s, func(m string) string {
		// Replace javascript: URL with empty href
		if i := strings.Index(m, "\""); i >= 0 {
			return m[:i+1] + "\""
		}
		return m
	})
	s = svg_re_js_href_sq.ReplaceAllStringFunc(s, func(m string) string {
		if i := strings.Index(m, "'"); i >= 0 {
			return m[:i+1] + "'"
		}
		return m
	})
	return []byte(s)
}
