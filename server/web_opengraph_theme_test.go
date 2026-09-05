// Mochi server: a page served through the Open Graph path carries the
// requester's theme like every other HTML page, and is cached as one.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// opengraph_serve serves a minimal app's index.html through the Open Graph
// path to an anonymous requester. With validator set, the request carries the
// file's modification-time entity tag: the validator a static file answers
// with 304.
func opengraph_serve(t *testing.T, validator bool) *httptest.ResponseRecorder {
	t.Helper()
	create_web_test_env(t)
	gin.SetMode(gin.TestMode)
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 32)
	}
	timeout := starlark_default_timeout
	starlark_default_timeout = 30 * time.Second
	t.Cleanup(func() { starlark_default_timeout = timeout })

	base := t.TempDir()
	star := filepath.Join(base, "app.star")
	if err := os.WriteFile(star, []byte("def opengraph_page(parameters):\n    return {\"title\": \"Stamped\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(base, "index.html")
	page := "<!doctype html>\n<html lang=\"en\">\n  <head>\n    <meta property=\"og:title\" content=\"Original\" />\n    <meta property=\"og:type\" content=\"website\" />\n  </head>\n  <body></body>\n</html>\n"
	if err := os.WriteFile(file, []byte(page), 0o600); err != nil {
		t.Fatal(err)
	}

	av := &AppVersion{base: base, Execute: []string{star}}
	aa := &AppAction{File: "index.html", Public: true, OpenGraph: "opengraph_page"}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	if validator {
		information, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		c.Request.Header.Set("If-None-Match", fmt.Sprintf(`"%x"`, information.ModTime().UnixNano()))
	}
	if !web_serve_file_with_opengraph(c, &App{}, av, aa, nil, file) {
		t.Fatal("the Open Graph path declined to serve the page")
	}
	return w
}

var themed_html = regexp.MustCompile(`<html[^>]*\sstyle="[^"]*--background-image: radial-gradient`)

// TestOpenGraphPageCarriesTheRequestersTheme is the finding: the landing page
// moved onto this path and lost the gradient, font and hue for every
// anonymous visitor.
func TestOpenGraphPageCarriesTheRequestersTheme(t *testing.T) {
	w := opengraph_serve(t, false)
	body := w.Body.String()
	if !strings.Contains(body, `content="Stamped"`) {
		t.Fatalf("the Open Graph function did not run:\n%s", body)
	}
	if !themed_html.MatchString(body) {
		t.Errorf("the <html> tag carries no theme style, so an anonymous visitor gets no background gradient, theme font or hue:\n%s", body)
	}
}

// TestOpenGraphPageIsCachedPerRequester. The body now varies by requester
// (theme) and by host (og:url), so it takes the plain HTML path's headers and
// no file-level validator may answer for it.
func TestOpenGraphPageIsCachedPerRequester(t *testing.T) {
	saved := web_cache
	web_cache = true
	defer func() { web_cache = saved }()

	w := opengraph_serve(t, true)
	if w.Code != http.StatusOK {
		t.Fatalf("a file validator was answered with %d; the page varies by requester, so the file's own validator cannot vouch for it", w.Code)
	}
	if got := w.Header().Get("Cache-Control"); got != "private, max-age=60" {
		t.Errorf("Cache-Control = %q, want %q: the requester's theme must not reach a shared cache", got, "private, max-age=60")
	}
	if got := w.Header().Get("Vary"); got != "Cookie" {
		t.Errorf("Vary = %q, want Cookie", got)
	}
	if got := w.Header().Get("ETag"); got != "" {
		t.Errorf("ETag = %q, want none", got)
	}
}
