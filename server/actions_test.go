// Mochi server: a.dump / a.error HTML response tests. Actions run under gin's
// NoRoute handler, which pre-sets 404 — a page written without an explicit
// status ships with it — and every response carries X-Content-Type-Options:
// nosniff, so the Content-Type must be set explicitly rather than left to
// net/http detection.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

func TestDumpResponse(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/app/-/action", nil)
	c.Status(404) // NoRoute pre-sets 404 before the action runs
	a := &Action{web: c}

	a.dump(map[string]any{"field": "<value>"})

	if w.Code != 200 {
		t.Errorf("dump status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("dump content type = %q, want text/html; charset=utf-8", ct)
	}
	body := w.Body.String()
	if !strings.Contains(body, "field") {
		t.Errorf("dump body missing dumped value: %q", body)
	}
	if strings.Contains(body, "<value>") {
		t.Errorf("dump body must not contain unescaped values: %q", body)
	}
}

func TestErrorPageResponse(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/app/-/action", nil)
	a := &Action{web: c}

	a.error(500, "boom <script>alert(1)</script>")

	if w.Code != 500 {
		t.Errorf("error status = %d, want 500", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("error content type = %q, want text/html; charset=utf-8", ct)
	}
	if body := w.Body.String(); strings.Contains(body, "<script>") {
		t.Errorf("error body must HTML-escape the message: %q", body)
	}
}

// TestWriteStreamSvgSanitizes covers the path an app takes when it proxies a
// person's avatar or a feed's image: the bytes and the content type both come
// from the far end, so a peer claiming image/svg+xml must not get its script
// executed in our origin.
func TestWriteStreamSvgSanitizes(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/people/entity/-/avatar", nil)
	a := &Action{web: c}

	payload := `<svg xmlns="http://www.w3.org/2000/svg"><script>alert(document.domain)</script><rect width="10" height="10"/></svg>`
	if _, err := a.write_stream_svg(sl.NewBuiltin("write.stream", nil), strings.NewReader(payload)); err != nil {
		t.Fatalf("write_stream_svg returned %v", err)
	}

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if policy := w.Header().Get("Content-Security-Policy"); policy != svg_content_policy {
		t.Errorf("content policy = %q, want %q", policy, svg_content_policy)
	}
	body := w.Body.String()
	if strings.Contains(body, "<script") || strings.Contains(body, "alert") {
		t.Errorf("script survived sanitizing: %q", body)
	}
	if !strings.Contains(body, "<rect") {
		t.Errorf("legitimate content dropped: %q", body)
	}
}

// TestWriteStreamSvgOversizeDownloads checks the escape hatch: an SVG too big
// to buffer is served as a download rather than being sanitized, truncated, or
// passed through inline.
func TestWriteStreamSvgOversizeDownloads(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/people/entity/-/avatar", nil)
	a := &Action{web: c}

	payload := "<svg>" + strings.Repeat("x", stream_svg_maximum) + "</svg>"
	written, err := a.write_stream_svg(sl.NewBuiltin("write.stream", nil), strings.NewReader(payload))
	if err != nil {
		t.Fatalf("write_stream_svg returned %v", err)
	}

	if disposition := w.Header().Get("Content-Disposition"); disposition != "attachment" {
		t.Errorf("disposition = %q, want attachment", disposition)
	}
	if w.Header().Get("Content-Security-Policy") != "" {
		t.Errorf("oversize SVG must not claim the sanitized-content policy")
	}
	if count, _ := sl.AsInt32(written); int(count) != len(payload) {
		t.Errorf("wrote %d bytes, want %d - the body must not be truncated", count, len(payload))
	}
	if w.Body.Len() != len(payload) {
		t.Errorf("body = %d bytes, want %d", w.Body.Len(), len(payload))
	}
}
