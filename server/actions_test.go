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
