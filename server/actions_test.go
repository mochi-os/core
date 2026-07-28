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
	"os"
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

// write_file_environment builds a files directory for one user and app, and
// returns a function that serves a path from it the way an action would.
func write_file_environment(t *testing.T, url string) (string, func(string) *httptest.ResponseRecorder) {
	t.Helper()

	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })

	user := &User{UID: "testuser"}
	app := &App{id: "files"}
	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("creating files directory: %v", err)
	}

	serve := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", url, nil)
		a := &Action{web: c}

		thread := &sl.Thread{Name: "test"}
		thread.SetLocal("owner", user)
		thread.SetLocal("app", app)

		if _, err := a.sl_write_file(thread, sl.NewBuiltin("write.file", nil), sl.Tuple{sl.String(path)}, nil); err != nil {
			t.Fatalf("sl_write_file(%q) returned %v", path, err)
		}
		return w
	}

	return base, serve
}

// TestWriteFileRefusesEscapingSymlink is the containment check. The path
// validator inspects the string, and a symlink is not in the string, so a link
// left behind by an rsync or an unpacked tarball must be refused at open time
// instead of serving whatever it points at.
func TestWriteFileRefusesEscapingSymlink(t *testing.T) {
	base, serve := write_file_environment(t, "/files/escape.txt")

	secret := data_dir + "/outside.txt"
	if err := os.WriteFile(secret, []byte("OUTSIDE-SECRET"), 0600); err != nil {
		t.Fatalf("writing outside file: %v", err)
	}
	if err := os.Symlink(secret, base+"/escape.txt"); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	w := serve("escape.txt")

	if w.Code != 404 {
		t.Errorf("status = %d, want 404", w.Code)
	}
	if body := w.Body.String(); strings.Contains(body, "OUTSIDE-SECRET") {
		t.Errorf("symlink escaped the files directory: %q", body)
	}
}

// TestWriteFileFollowsInternalSymlink guards the other side of containment:
// links that stay inside the directory are legitimate, and the "latest -> v2"
// layout of a package repository depends on them resolving.
func TestWriteFileFollowsInternalSymlink(t *testing.T) {
	base, serve := write_file_environment(t, "/files/latest/data.txt")

	if err := os.MkdirAll(base+"/v2", 0755); err != nil {
		t.Fatalf("creating directory: %v", err)
	}
	if err := os.WriteFile(base+"/v2/data.txt", []byte("INSIDE"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	if err := os.Symlink("v2", base+"/latest"); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	w := serve("latest/data.txt")

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if body := w.Body.String(); body != "INSIDE" {
		t.Errorf("body = %q, want INSIDE", body)
	}
}

// TestWriteFileDirectoryDoesNotList covers the enumeration hole: serving a
// directory that has no index must not fall back to a generated HTML index,
// which named every file in the tree to anyone who could reach it.
func TestWriteFileDirectoryDoesNotList(t *testing.T) {
	base, serve := write_file_environment(t, "/files/listing/")

	if err := os.MkdirAll(base+"/listing", 0755); err != nil {
		t.Fatalf("creating directory: %v", err)
	}
	if err := os.WriteFile(base+"/listing/private.txt", []byte("x"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("listing")

	if w.Code != 404 {
		t.Errorf("status = %d, want 404", w.Code)
	}
	if body := w.Body.String(); strings.Contains(body, "private.txt") {
		t.Errorf("directory listing disclosed filenames: %q", body)
	}
}

// TestWriteFileDirectoryServesIndex keeps the behaviour a static file host
// relies on: a directory request is answered from its index.html.
func TestWriteFileDirectoryServesIndex(t *testing.T) {
	base, serve := write_file_environment(t, "/files/site/")

	if err := os.MkdirAll(base+"/site", 0755); err != nil {
		t.Fatalf("creating directory: %v", err)
	}
	if err := os.WriteFile(base+"/site/index.html", []byte("<h1>INDEX</h1>"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("site")

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if body := w.Body.String(); !strings.Contains(body, "INDEX") {
		t.Errorf("body = %q, want the directory index", body)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("content type = %q, want text/html", ct)
	}
}

// TestWriteFileDirectoryRedirectsToSlash covers the redirect http.ServeFile
// used to issue for the same case: relative links in an index resolve against
// the request path, so it has to end in a slash before the page is served.
func TestWriteFileDirectoryRedirectsToSlash(t *testing.T) {
	base, serve := write_file_environment(t, "/files/site")

	if err := os.MkdirAll(base+"/site", 0755); err != nil {
		t.Fatalf("creating directory: %v", err)
	}
	if err := os.WriteFile(base+"/site/index.html", []byte("<h1>INDEX</h1>"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("site")

	if w.Code != 301 {
		t.Errorf("status = %d, want 301", w.Code)
	}
	// http.Redirect resolves the relative target against the request path, so
	// the header carries the absolute form of the same destination.
	if location := w.Header().Get("Location"); location != "/files/site/" {
		t.Errorf("location = %q, want /files/site/", location)
	}
}
