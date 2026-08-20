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
	svg_re_on_attribute = regexp.MustCompile(`(?i)\s+on\w+\s*=\s*("[^"]*"|'[^']*'|[^\s>]*)`)
	// Match javascript: in href/xlink:href attributes
	svg_re_js_href    = regexp.MustCompile(`(?i)((?:xlink:)?href\s*=\s*")javascript:[^"]*"`)
	svg_re_js_href_sq = regexp.MustCompile(`(?i)((?:xlink:)?href\s*=\s*')javascript:[^']*'`)
)

// svg_content_policy is the Content-Security-Policy served alongside every SVG
// this server renders inline. svg_sanitize is best-effort and bypassable, so
// this is the actual guarantee: scripts, plugins, frames and network fetches
// are all blocked. Inline styles and data: images stay allowed so ordinary
// self-contained SVGs still render.
const svg_content_policy = "default-src 'none'; style-src 'unsafe-inline'; img-src data:"

// content_type_inline reports whether a content type may be rendered inline in
// this server's origin. SVG is excluded deliberately - it is an executable
// document format, and callers that want to serve one must go through the
// sanitize-and-CSP path rather than handing the bytes straight to the browser.
func content_type_inline(content_type string) bool {
	if content_type == "image/svg+xml" {
		return false
	}
	if strings.HasPrefix(content_type, "image/") ||
		strings.HasPrefix(content_type, "video/") ||
		strings.HasPrefix(content_type, "audio/") {
		return true
	}
	return content_type == "application/pdf"
}

// content_type_base strips parameters and casing from a content type, so
// "IMAGE/SVG+XML; charset=utf-8" compares equal to "image/svg+xml".
func content_type_base(content_type string) string {
	base, _, _ := strings.Cut(content_type, ";")
	return strings.ToLower(strings.TrimSpace(base))
}

// svg_sanitize removes dangerous elements and attributes from SVG content.
// Uses regex-based stripping to preserve the original formatting exactly.
func svg_sanitize(data []byte) []byte {
	s := string(data)
	s = svg_re_dangerous.ReplaceAllString(s, "")
	s = svg_re_dangerous_sc.ReplaceAllString(s, "")
	s = svg_re_on_attribute.ReplaceAllString(s, "")
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
