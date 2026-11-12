// Mochi server: a.dump / a.error HTML response tests. Actions run under gin's
// NoRoute handler, which pre-sets 404, and every response carries nosniff, so
// the Content-Type must be set explicitly.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

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

// write_file_environment builds a files directory for one user and app and
// returns a server for it. hosted picks the caller modelled: a domain route,
// which may render a site, or a plain app request, which may not.
func write_file_environment(t *testing.T, url string, hosted bool) (string, func(string) *httptest.ResponseRecorder) {
	t.Helper()

	test_data_directory(t)

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
		if hosted {
			// A site publishes because its action says so. The route is set too,
			// since that is how such a request really arrives, but it is the
			// declaration that grants the exemption.
			a.domain = &DomainInfo{route: &DomainRouteInfo{owner: user}}
			a.definition = &AppAction{Site: true}
		}

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
	base, serve := write_file_environment(t, "/files/escape.txt", false)

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
	base, serve := write_file_environment(t, "/files/latest/data.txt", true)

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

// A link whose final component points at a sibling file resolves by a different
// path through os.Root - a final component is opened, not walked.
func TestWriteFileFollowsSiblingFileSymlink(t *testing.T) {
	base, serve := write_file_environment(t, "/files/mochi-server.msi", true)

	if err := os.WriteFile(base+"/mochi-server-0.4.237.msi", []byte("INSTALLER"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	if err := os.Symlink("mochi-server-0.4.237.msi", base+"/mochi-server.msi"); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	w := serve("mochi-server.msi")

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if body := w.Body.String(); body != "INSTALLER" {
		t.Errorf("body = %q, want INSTALLER", body)
	}
}

// TestWriteFileDirectoryDoesNotList covers the enumeration hole: serving a
// directory that has no index must not fall back to a generated HTML index,
// which named every file in the tree to anyone who could reach it.
func TestWriteFileDirectoryDoesNotList(t *testing.T) {
	base, serve := write_file_environment(t, "/files/listing/", true)

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
	base, serve := write_file_environment(t, "/files/site/", true)

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
	base, serve := write_file_environment(t, "/files/site", true)

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

// TestWriteFileRejectsDotfiles covers hidden files below the root. The path
// validator's leading-character rule stopped ".env" but not "site/.env", so a
// git working tree or a stray .env copied into a hosted directory was readable.
func TestWriteFileRejectsDotfiles(t *testing.T) {
	base, serve := write_file_environment(t, "/files/site/.env", false)

	if err := os.MkdirAll(base+"/site/.git", 0755); err != nil {
		t.Fatalf("creating directory: %v", err)
	}
	if err := os.WriteFile(base+"/site/.env", []byte("DOTENV-SECRET"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	if err := os.WriteFile(base+"/site/.git/config", []byte("GIT-CONFIG-SECRET"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	for _, path := range []string{"site/.env", "site/.git/config"} {
		w := serve(path)
		if w.Code != 400 {
			t.Errorf("%s: status = %d, want 400", path, w.Code)
		}
		if body := w.Body.String(); strings.Contains(body, "SECRET") {
			t.Errorf("%s: hidden file served: %q", path, body)
		}
	}
}

// TestWriteFileSetsCachePolicy checks that a served file carries an explicit
// policy. Which bytes a path yields depends on whose directory is read, so a
// response with no Cache-Control could be held by a shared cache and handed to
// the wrong reader, and one with no validator could outlive the file itself.
func TestWriteFileSetsCachePolicy(t *testing.T) {
	base, serve := write_file_environment(t, "/files/data.txt", false)

	if err := os.WriteFile(base+"/data.txt", []byte("DATA"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("data.txt")

	if control := w.Header().Get("Cache-Control"); !strings.Contains(control, "private") {
		t.Errorf("Cache-Control = %q, want a private policy", control)
	}
	if !strings.Contains(w.Header().Get("Cache-Control"), "must-revalidate") {
		t.Errorf("Cache-Control = %q, want must-revalidate", w.Header().Get("Cache-Control"))
	}
	if w.Header().Get("Etag") == "" {
		t.Error("no ETag, so a revalidation cannot be answered cheaply")
	}
}

// Stored content: anything outside the inline allowlist must arrive as a
// download, or it would run in this origin with the reader's session.
func TestWriteFileDownloadsDocument(t *testing.T) {
	base, serve := write_file_environment(t, "/feeds/entity/-/attachment", false)

	if err := os.WriteFile(base+"/payload.html", []byte("<script>alert(document.domain)</script>"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("payload.html")

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if disposition := w.Header().Get("Content-Disposition"); disposition != "attachment" {
		t.Errorf("disposition = %q, want attachment - an uploaded document must not render in this origin", disposition)
	}
	if body := w.Body.String(); !strings.Contains(body, "alert") {
		t.Errorf("body = %q, want the file served intact as a download", body)
	}
}

// TestWriteFileSanitizesSvg is the same case for the format that renders even
// when it is called an image. Serving it as a download would be safe but would
// stop legitimate SVGs displaying, so it takes the sanitize-and-CSP path the
// cache path already uses for a copy pulled from a peer.
func TestWriteFileSanitizesSvg(t *testing.T) {
	base, serve := write_file_environment(t, "/feeds/entity/-/attachment", false)

	payload := `<svg xmlns="http://www.w3.org/2000/svg"><script>alert(document.domain)</script><rect width="10" height="10"/></svg>`
	if err := os.WriteFile(base+"/drawing.svg", []byte(payload), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("drawing.svg")

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

// TestWriteFileServesImageInline keeps the other half of the policy honest: the
// allowlisted media types still display, or every avatar and attached photo
// would turn into a download.
func TestWriteFileServesImageInline(t *testing.T) {
	base, serve := write_file_environment(t, "/feeds/entity/-/attachment", false)

	if err := os.WriteFile(base+"/photo.png", []byte("\x89PNG\r\n\x1a\n"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("photo.png")

	if disposition := w.Header().Get("Content-Disposition"); disposition != "" {
		t.Errorf("disposition = %q, want none - an image must still display", disposition)
	}
}

// TestWriteFileHostedSiteRendersDocument covers the exemption. A request that
// arrived on a domain route is publishing a site, where serving HTML is the
// entire point and the response answers on the route's own hostname.
func TestWriteFileHostedSiteRendersDocument(t *testing.T) {
	base, serve := write_file_environment(t, "/page.html", true)

	if err := os.WriteFile(base+"/page.html", []byte("<h1>PAGE</h1>"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("page.html")

	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if disposition := w.Header().Get("Content-Disposition"); disposition != "" {
		t.Errorf("disposition = %q, want none - a hosted page has to render", disposition)
	}
	if body := w.Body.String(); !strings.Contains(body, "PAGE") {
		t.Errorf("body = %q, want the page", body)
	}
}

// A domain route must not itself grant site serving: routing is how a reader
// arrived, not what the app meant.
func TestDomainRouteDoesNotGrantSiteServing(t *testing.T) {
	test_data_directory(t)

	user := &User{UID: "testuser"}
	app := &App{id: "files"}
	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("creating files directory: %v", err)
	}
	if err := os.WriteFile(base+"/payload.html", []byte("<script>alert(1)</script>"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	serve := func(definition *AppAction) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/payload.html", nil)
		a := &Action{
			web:        c,
			domain:     &DomainInfo{route: &DomainRouteInfo{owner: user}},
			definition: definition,
		}
		thread := &sl.Thread{Name: "test"}
		thread.SetLocal("owner", user)
		thread.SetLocal("app", app)
		if _, err := a.sl_write_file(thread, sl.NewBuiltin("write.file", nil), sl.Tuple{sl.String("payload.html")}, nil); err != nil {
			t.Fatalf("sl_write_file returned %v", err)
		}
		return w
	}

	// Domain-routed, but the action does not claim to publish a site.
	if disposition := serve(&AppAction{}).Header().Get("Content-Disposition"); disposition != "attachment" {
		t.Errorf("disposition = %q, want attachment: a domain route must not exempt an action that never said it publishes", disposition)
	}
	// An action with no declaration at all is treated the same way.
	if disposition := serve(nil).Header().Get("Content-Disposition"); disposition != "attachment" {
		t.Errorf("disposition = %q, want attachment for an undeclared action", disposition)
	}
	// And the declaration still grants it, or a hosted site could not render.
	if disposition := serve(&AppAction{Site: true}).Header().Get("Content-Disposition"); disposition != "" {
		t.Errorf("disposition = %q, want none: a declared site has to render", disposition)
	}
}

// TestRouteContextValid pins the contexts a route may carry. A context is used
// as one path segment by the app receiving it, so anything core's own path
// validator would later reject has to be refused when the route is written -
// otherwise the route reads as configured and every request to it fails.
func TestRouteContextValid(t *testing.T) {
	valid := []string{"", "apt", "docs", "site_one", "site-two", "v2", strings.Repeat("a", route_context_maximum)}
	invalid := []string{
		"café",      // non-ASCII: isalnum() accepted it, core's path validator does not
		"日本",        // likewise
		"a/b",       // a separator would escape the intended subdirectory
		"..",        // traversal
		".hidden",   // hidden directory
		"has space", // not accepted in a path segment
		strings.Repeat("a", route_context_maximum+1), // eats the filename budget
	}

	for _, context := range valid {
		if !route_context_valid(context) {
			t.Errorf("route_context_valid(%q) = false, want true", context)
		}
	}
	for _, context := range invalid {
		if route_context_valid(context) {
			t.Errorf("route_context_valid(%q) = true, want false", context)
		}
	}
}

// TestWriteFileUsesRouteOwner covers a hosted domain serving one account's
// files to every visitor alike. Resolving to the requester meant the same URL
// gave the route owner's site to anonymous visitors and the visitor's own -
// almost always empty - directory to anyone signed in.
func TestWriteFileUsesRouteOwner(t *testing.T) {
	test_data_directory(t)

	publisher := &User{UID: "publisher"}
	visitor := &User{UID: "visitor"}
	app := &App{id: "files"}

	for _, u := range []*User{publisher, visitor} {
		base := api_file_base(u, app)
		if err := os.MkdirAll(base, 0755); err != nil {
			t.Fatalf("creating files directory: %v", err)
		}
		if err := os.WriteFile(base+"/index.html", []byte(u.UID+"-FILE"), 0600); err != nil {
			t.Fatalf("writing file: %v", err)
		}
	}

	serve := func(requester *User) string {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/index.html", nil)
		a := &Action{
			web:  c,
			user: requester,
			domain: &DomainInfo{
				route: &DomainRouteInfo{owner: publisher},
			},
		}

		thread := &sl.Thread{Name: "test"}
		// The action's owner is the requester, exactly as core resolves it for
		// an authenticated visitor. Only the file lookup may override it.
		thread.SetLocal("owner", requester)
		thread.SetLocal("app", app)

		if _, err := a.sl_write_file(thread, sl.NewBuiltin("write.file", nil), sl.Tuple{sl.String("index.html")}, nil); err != nil {
			t.Fatalf("sl_write_file returned %v", err)
		}
		return w.Body.String()
	}

	if body := serve(nil); body != "publisher-FILE" {
		t.Errorf("anonymous visitor got %q, want publisher-FILE", body)
	}
	if body := serve(visitor); body != "publisher-FILE" {
		t.Errorf("signed-in visitor got %q, want publisher-FILE - a hosted site must not vary with who is reading it", body)
	}
}

// With no route, the action's own owner decides which directory is read.
// Redirecting that generally would authorize a stranger as the published
// account.
func TestWriteFileDirectRequestKeepsOwner(t *testing.T) {
	base, serve := write_file_environment(t, "/files/index.html", false)

	if err := os.WriteFile(base+"/index.html", []byte("OWN-FILE"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := serve("index.html")

	if body := w.Body.String(); body != "OWN-FILE" {
		t.Errorf("body = %q, want OWN-FILE", body)
	}
}

// A hosted domain whose account no longer resolves must fail closed; falling
// back to the requester would make one URL answer with whoever is asking.
func TestWriteFileUnresolvedRouteOwnerFailsClosed(t *testing.T) {
	test_data_directory(t)

	visitor := &User{UID: "visitor"}
	app := &App{id: "files"}

	base := api_file_base(visitor, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("creating files directory: %v", err)
	}
	if err := os.WriteFile(base+"/index.html", []byte("VISITOR-OWN-FILE"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/index.html", nil)
	a := &Action{
		web:  c,
		user: visitor,
		// A route matched, but its owner could not be resolved to an account.
		domain: &DomainInfo{route: &DomainRouteInfo{}},
	}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("owner", visitor)
	thread.SetLocal("app", app)

	if _, err := a.sl_write_file(thread, sl.NewBuiltin("write.file", nil), sl.Tuple{sl.String("index.html")}, nil); err != nil {
		t.Fatalf("sl_write_file returned %v", err)
	}

	if w.Code != 500 {
		t.Errorf("status = %d, want 500", w.Code)
	}
	if body := w.Body.String(); strings.Contains(body, "VISITOR-OWN-FILE") {
		t.Errorf("fell back to the requester's own file: %q", body)
	}
}

// write_stream_serve relays body through a.write.stream for a request with
// the given method and If-None-Match, the action having set etag (none when
// empty), answering the recorder, the status gin will send and whether the far
// end's stream was closed.
func write_stream_serve(t *testing.T, method string, match string, etag string, body string) (*httptest.ResponseRecorder, int, bool) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, "/people/entity/-/avatar", nil)
	if match != "" {
		c.Request.Header.Set("If-None-Match", match)
	}
	a := &Action{web: c}
	a.web.Header("Content-Type", "image/jpeg")
	a.web.Header("Last-Modified", "Mon, 02 Nov 2026 10:00:00 GMT")
	if etag != "" {
		a.web.Header("ETag", etag)
	}
	reader := &write_stream_closer{Reader: strings.NewReader(body)}
	stream := &Stream{reader: reader}
	thread := &sl.Thread{Name: "test"}
	if _, err := a.sl_write_stream(thread, sl.NewBuiltin("write.stream", nil), sl.Tuple{stream}, nil); err != nil {
		t.Fatalf("a.write.stream: %v", err)
	}
	return w, c.Writer.Status(), reader.closed
}

type write_stream_closer struct {
	*strings.Reader
	closed bool
}

func (c *write_stream_closer) Close() error {
	c.closed = true
	return nil
}

// TestWriteStreamRevalidates: a browser revalidating a relayed avatar with the
// ETag the action set is told it has not changed, with no body, and the far
// end's stream is closed unread - as a.write.file and a.write.cache answer
// through http.ServeContent. Every other request is relayed in full.
func TestWriteStreamRevalidates(t *testing.T) {
	const tag = `"1730000000-5"`
	for _, c := range []struct {
		name   string
		method string
		match  string
		etag   string
		status int
	}{
		{"matching", "GET", tag, tag, 304},
		{"weak", "GET", `W/"1730000000-5"`, tag, 304},
		{"in a list", "GET", `"other", "1730000000-5"`, tag, 304},
		{"star", "GET", "*", tag, 304},
		{"head", "HEAD", tag, tag, 304},
		{"another method, which the action has already performed", "POST", tag, tag, 200},
		{"not matching", "GET", `"1730000000-6"`, tag, 200},
		{"no validator sent", "GET", "", tag, 200},
		{"no ETag set", "GET", tag, "", 200},
		{"a malformed list", "GET", `1730000000-5`, tag, 200},
	} {
		t.Run(c.name, func(t *testing.T) {
			w, status, closed := write_stream_serve(t, c.method, c.match, c.etag, "AVATAR")
			if status != c.status {
				t.Fatalf("answered %d, want %d", status, c.status)
			}
			if c.status == 200 {
				if w.Body.String() != "AVATAR" {
					t.Errorf("relayed %q, want the whole body", w.Body.String())
				}
				return
			}
			if w.Body.Len() != 0 {
				t.Errorf("THE POINT: a %d carried %d bytes of body", c.status, w.Body.Len())
			}
			if !closed {
				t.Error("the far end's stream was left open")
			}
			if c.status == 304 {
				if w.Header().Get("Content-Type") != "" || w.Header().Get("Last-Modified") != "" {
					t.Errorf("a 304 carried Content-Type %q and Last-Modified %q", w.Header().Get("Content-Type"), w.Header().Get("Last-Modified"))
				}
				if w.Header().Get("ETag") != tag {
					t.Errorf("a 304 carried ETag %q, want the validator it confirmed", w.Header().Get("ETag"))
				}
			}
		})
	}
}

// write_stream_failing yields its body and then fails with failure, as a
// peer's stream does when its connection drops part way through a relay.
type write_stream_failing struct {
	body    string
	failure string
	sent    bool
}

func (f *write_stream_failing) Read(p []byte) (int, error) {
	if !f.sent {
		f.sent = true
		return copy(p, f.body), nil
	}
	if f.failure == "" {
		return 0, errors.New("stream reset")
	}
	return 0, errors.New(f.failure)
}

func (f *write_stream_failing) Close() error { return nil }

// write_stream_spending yields its body and spends the caller's whole byte
// budget as it does, as the caller's other relays at the same moment would;
// with last, that read is the stream's last.
type write_stream_spending struct {
	body   string
	client string
	last   bool
	sent   bool
}

func (f *write_stream_spending) Read(p []byte) (int, error) {
	if f.sent {
		if f.last {
			return 0, io.EOF
		}
		return copy(p, f.body), nil
	}
	f.sent = true
	stream_bytes_charge(f.client, "", rate_limit_stream_client.limit*1024+1)
	return copy(p, f.body), nil
}

func (f *write_stream_spending) Close() error { return nil }

// write_stream_refusing is a response the client has walked away from.
type write_stream_refusing struct {
	*httptest.ResponseRecorder
}

func (w *write_stream_refusing) Write([]byte) (int, error) { return 0, syscall.EPIPE }

// write_stream_relay relays reader through a.write.stream on c as content (an
// image when empty), with maximum and the declared size when positive,
// answering the action and the error the relay returned.
func write_stream_relay(t *testing.T, c *gin.Context, reader io.ReadCloser, content string, maximum, size int) (*Action, error) {
	t.Helper()
	a := &Action{web: c}
	if content == "" {
		content = "image/png"
	}
	a.web.Header("Content-Type", content)
	a.web.Header("ETag", `"1730000000-65536"`)
	var kwargs []sl.Tuple
	if maximum > 0 {
		kwargs = append(kwargs, sl.Tuple{sl.String("maximum"), sl.MakeInt(maximum)})
	}
	if size >= 0 {
		kwargs = append(kwargs, sl.Tuple{sl.String("size"), sl.MakeInt(size)})
	}
	_, err := a.sl_write_stream(&sl.Thread{Name: "test"}, sl.NewBuiltin("write.stream", nil), sl.Tuple{&Stream{reader: reader}}, kwargs)
	return a, err
}

// write_stream_broken is write_stream_relay on a fresh request answered to w.
func write_stream_broken(t *testing.T, w http.ResponseWriter, reader io.ReadCloser, content string, maximum, size int) (*Action, error) {
	t.Helper()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/people/entity/-/avatar", nil)
	return write_stream_relay(t, c, reader, content, maximum, size)
}

// TestWriteStreamMarksABrokenRelay: each way a relay can stop part way - the
// far end's stream failing, sending past the caller's cap or past the size it
// declared, or ending short of that size, or the caller's budget running out -
// marks the action broken, so web_action_written aborts the response; a whole
// relay does not, nor does a client walking away.
func TestWriteStreamMarksABrokenRelay(t *testing.T) {
	stream_bytes_reset()
	t.Cleanup(stream_bytes_reset)
	probe, _ := gin.CreateTestContext(httptest.NewRecorder())
	probe.Request = httptest.NewRequest("GET", "/people/entity/-/avatar", nil)
	client := "/" + rate_limit_client_ip(probe)
	svg := `<svg xmlns="http://www.w3.org/2000/svg"></svg>`
	closer := func(body string) io.ReadCloser { return &write_stream_closer{Reader: strings.NewReader(body)} }
	reset := "stream reset: connection closed: read tcp 192.0.2.1:4001->192.0.2.2:4001: read: connection reset by peer"
	for _, c := range []struct {
		name    string
		reader  io.ReadCloser
		content string
		maximum int
		size    int
		broken  bool
	}{
		{"the far end's stream fails part way", &write_stream_failing{body: "AVA"}, "", 0, -1, true},
		{"the far end's connection is reset part way, which reads as a reset too", &write_stream_failing{body: "AVA", failure: reset}, "", 0, -1, true},
		{"the far end sends past the cap", closer("AVATAR"), "", 3, -1, true},
		{"the far end ends short of the size it declared", closer("AVA"), "", 0, 6, true},
		{"the far end sends past the size it declared", closer("AVATAR"), "", 0, 3, true},
		{"the caller's byte budget runs out part way", &write_stream_spending{body: "AVA", client: client}, "", 0, -1, true},
		{"an SVG whose stream fails part way", &write_stream_failing{body: "<svg"}, "image/svg+xml", 0, -1, true},
		{"an SVG past the cap", closer(svg), "image/svg+xml", 10, -1, true},
		{"an SVG short of the size declared", closer(svg), "image/svg+xml", 0, len(svg) + 5, true},
		{"an SVG whose budget runs out part way", &write_stream_spending{body: "<svg", client: client}, "image/svg+xml", 0, -1, true},
		{"CONTROL: a whole relay", closer("AVATAR"), "", 0, -1, false},
		{"CONTROL: a relay of exactly the size declared", closer("AVATAR"), "", 0, 6, false},
		{"CONTROL: a whole relay whose last read spends the budget", &write_stream_spending{body: "AVA", client: client, last: true}, "", 0, -1, false},
		{"CONTROL: a whole SVG", closer(svg), "image/svg+xml", 0, -1, false},
		{"CONTROL: an SVG of exactly the size declared", closer(svg), "image/svg+xml", 0, len(svg), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			stream_bytes_reset()
			w := httptest.NewRecorder()
			a, err := write_stream_broken(t, w, c.reader, c.content, c.maximum, c.size)
			if (err != nil) != c.broken {
				t.Fatalf("a.write.stream returned %v, want an error: %v", err, c.broken)
			}
			if a.broken.Load() != c.broken {
				t.Errorf("THE POINT: the action is marked broken: %v, want %v", a.broken.Load(), c.broken)
			}
			if c.size >= 0 && c.content == "" && w.Header().Get("Content-Length") != strconv.Itoa(c.size) {
				t.Errorf("THE POINT: the response declares Content-Length %q, want the %d declared", w.Header().Get("Content-Length"), c.size)
			}
		})
	}

	// The client walking away ends the relay quietly: nothing reached it to be
	// mistaken for the whole.
	a, err := write_stream_broken(t, &write_stream_refusing{httptest.NewRecorder()}, closer("AVATAR"), "", 0, -1)
	if err != nil || a.broken.Load() {
		t.Errorf("CONTROL: a client that walked away marked the relay broken (%v, %v)", err, a.broken.Load())
	}

	// CONTROL: a HEAD is answered whole with its headers, net/http discarding
	// the body, and is not a relay cut short.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := gin.CreateTestContext(w)
		c.Request = r
		a, err := write_stream_relay(t, c, closer("AVATAR"), "", 0, 6)
		if err != nil || a.broken.Load() {
			t.Errorf("THE POINT: a HEAD marked the relay broken (%v, %v)", err, a.broken.Load())
		}
	}))
	defer server.Close()
	if response, err := http.Head(server.URL); err != nil || response.StatusCode != 200 {
		t.Errorf("CONTROL: a HEAD answered %v, %v", response, err)
	}

	// A declared size past the cap is refused before anything is written.
	w := httptest.NewRecorder()
	if _, err := write_stream_broken(t, w, closer("AVATAR"), "", 3, 6); err == nil || w.Body.Len() != 0 {
		t.Errorf("a size past the cap answered %v with %d bytes written", err, w.Body.Len())
	}
}

// web_relay_engine serves /relay through the router's own recovery: a relay
// of the reader made for each request, ended as web_action ends one, and
// /panic, a handler that fails.
func web_relay_engine(t *testing.T, reader func() io.ReadCloser) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(web_recovery())
	engine.GET("/relay", func(c *gin.Context) {
		a, err := write_stream_relay(t, c, reader(), "", 0, -1)
		if err != nil && c.Writer.Written() {
			web_action_written(a)
		}
	})
	engine.GET("/panic", func(c *gin.Context) { panic("a handler fault") })
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	return server
}

// TestBrokenRelayReachesTheClientAsAFailure: over a real connection and through
// the router's recovery, a relay that stops part way after its 200 and some of
// the body are on the wire is seen by the client as a failed transfer, not as
// a complete image it would keep under the validator it came with. A whole
// relay arrives whole, and any other fault is still answered 500.
func TestBrokenRelayReachesTheClientAsAFailure(t *testing.T) {
	body := strings.Repeat("A", 64*1024)
	for _, c := range []struct {
		name   string
		reader func() io.ReadCloser
		failed bool
	}{
		{"broken part way", func() io.ReadCloser { return &write_stream_failing{body: body} }, true},
		{"CONTROL: whole", func() io.ReadCloser { return &write_stream_closer{Reader: strings.NewReader(body)} }, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := web_relay_engine(t, c.reader)
			response, err := http.Get(server.URL + "/relay")
			if err == nil {
				var received []byte
				received, err = io.ReadAll(response.Body)
				response.Body.Close()
				if err == nil && string(received) != body {
					t.Fatalf("received %d bytes with no error, want the %d sent", len(received), len(body))
				}
			}
			if (err != nil) != c.failed {
				t.Errorf("THE POINT: the client's transfer failed: %v (%v), want %v", err != nil, err, c.failed)
			}
		})
	}

	server := web_relay_engine(t, func() io.ReadCloser { return nil })
	for range 2 {
		response, err := http.Get(server.URL + "/panic")
		if err != nil {
			t.Fatalf("CONTROL: a handler fault reached the client as %v, want a 500", err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusInternalServerError {
			t.Errorf("CONTROL: a handler fault answered %d, want 500", response.StatusCode)
		}
	}
}

// TestWebActionAbortsABrokenRelay: web_action hands an action that failed after
// writing to web_action_written, and the router recovers with web_recovery,
// which lets the abort through, not with gin's, which swallows it.
func TestWebActionAbortsABrokenRelay(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("reading web.go: %v", err)
	}
	function := function_source(t, string(source), "web_action")
	branch := regexp.MustCompile(`if c\.Writer\.Written\(\) \{\s*web_action_written\(&action\)\s*return true`)
	if !branch.MatchString(function) {
		t.Error("THE POINT: web_action ends an action that failed after writing without web_action_written(&action)")
	}
	if !strings.Contains(string(source), "r.Use(web_recovery())") || strings.Contains(string(source), "gin.Recovery()") {
		t.Error("THE POINT: the router does not recover through web_recovery, so the abort is swallowed")
	}
}

// TestErrorAnswersCarryNoRepresentation: an action that set an image's type,
// validator and freshness and then fails answers its error without them, so a
// browser does not keep the error as the image.
func TestErrorAnswersCarryNoRepresentation(t *testing.T) {
	for _, c := range []struct {
		name   string
		accept string
		fail   func(a *Action)
	}{
		{"a.error as JSON", "application/json", func(a *Action) { a.error(404, "gone") }},
		{"a.error as a page", "text/html", func(a *Action) { a.error(404, "gone") }},
		{"a failed action", "application/json", func(a *Action) { web_action_error(a.web, "people", "en", errors.New("failed")) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			web, _ := gin.CreateTestContext(w)
			web.Request = httptest.NewRequest("GET", "/people/entity/-/avatar", nil)
			web.Request.Header.Set("Accept", c.accept)
			a := &Action{web: web}
			a.web.Header("Content-Type", "image/png")
			a.web.Header("ETag", `"1730000000-5"`)
			a.web.Header("Last-Modified", "Mon, 02 Nov 2026 10:00:00 GMT")
			a.web.Header("Cache-Control", "private, max-age=300")
			a.web.Header("Content-Disposition", "attachment")
			c.fail(a)
			for _, name := range []string{"ETag", "Last-Modified", "Cache-Control", "Content-Disposition"} {
				if value := w.Header().Get(name); value != "" {
					t.Errorf("THE POINT: the error carries %s %q", name, value)
				}
			}
			if kind := w.Header().Get("Content-Type"); strings.HasPrefix(kind, "image/") {
				t.Errorf("THE POINT: the error is sent as %q", kind)
			}
		})
	}
}

// TestStarlarkMarksAnAbandonedCall: a call that does not stop when its time is
// up - stuck in a builtin that does not check for cancellation - is given up on
// with an error that says so, since what it holds stays in use; a call that
// ends in time, or stops when cancelled, is not.
func TestStarlarkMarksAnAbandonedCall(t *testing.T) {
	// The runtime state the server sets up at startup, set together as every
	// test that needs it does: a test that finds the semaphore made takes the
	// timeout as made too.
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 32)
		starlark_default_timeout = 90 * time.Second
	}
	original := starlark_cancel_grace
	starlark_cancel_grace = 20 * time.Millisecond
	t.Cleanup(func() { starlark_cancel_grace = original })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	stuck := sl.NewBuiltin("stuck", func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error) {
		<-release
		return sl.None, nil
	})
	prompt := sl.NewBuiltin("prompt", func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error) {
		return sl.None, nil
	})
	// A builtin that honours the call's context, as a query in SQLite or a
	// relay tied to it does: it stops when the context goes.
	attentive := sl.NewBuiltin("attentive", func(thread *sl.Thread, _ *sl.Builtin, _ sl.Tuple, _ []sl.Tuple) (sl.Value, error) {
		<-starlark_context(thread).Done()
		return nil, errors.New("stopped by its context")
	})
	s := &Starlark{thread: &sl.Thread{Name: "test"}, globals: sl.StringDict{"stuck": stuck, "prompt": prompt, "attentive": attentive}, timeout: 50 * time.Millisecond}
	if _, err := s.call("stuck", nil); !errors.Is(err, starlark_abandoned) {
		t.Errorf("THE POINT: a call stuck past its time answered %v, not that it was abandoned", err)
	}
	if _, err := s.call("prompt", nil); err != nil {
		t.Errorf("CONTROL: a call that ended in time answered %v", err)
	}
	if _, err := s.call("attentive", nil); err == nil || errors.Is(err, starlark_abandoned) {
		t.Errorf("THE POINT: a call its context stops within the grace answered %v, as though abandoned", err)
	}
}

// TestWebActionAbortsAnAbandonedCall: a request whose action was abandoned is
// aborted, so gin never puts its context back for another request to be
// written into by the call still running; any other failure is answered as
// before. web_action checks before anything else it does with the failure.
func TestWebActionAbortsAnAbandonedCall(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	guard := &action_writer{ResponseWriter: c.Writer}
	aborted := func(err error) (stopped bool) {
		defer func() {
			if failure := recover(); failure != nil {
				if failure != http.ErrAbortHandler {
					t.Fatalf("panicked with %v, want http.ErrAbortHandler", failure)
				}
				stopped = true
			}
		}()
		web_action_abandoned("people", guard, err)
		return false
	}
	if aborted(errors.New("starlark: a script error")) {
		t.Error("CONTROL: an ordinary failure was aborted")
	}
	if _, err := guard.Write([]byte("mine")); err != nil {
		t.Fatalf("CONTROL: a write after an ordinary failure was refused: %v", err)
	}
	if !aborted(fmt.Errorf("starlark: timeout after 30s: %w", starlark_abandoned)) {
		t.Error("THE POINT: an abandoned call's response was not aborted")
	}
	if _, err := guard.Write([]byte("late")); err == nil {
		t.Error("THE POINT: the abandoned call can still write to the aborted response")
	}
	guard.Header().Set("Late", "yes")
	if w.Body.String() != "mine" || w.Header().Get("Late") != "" {
		t.Errorf("THE POINT: the aborted response gained %q and header %q", strings.TrimPrefix(w.Body.String(), "mine"), w.Header().Get("Late"))
	}
	if guard.Unwrap() != c.Writer {
		t.Error("Unwrap does not reach the writer beneath, so write deadlines cannot be set")
	}

	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("reading web.go: %v", err)
	}
	function := function_source(t, string(source), "web_action")
	first := regexp.MustCompile(`guard := &action_writer\{ResponseWriter: c\.Writer\}\s*c\.Writer = guard\s*result, err := s\.call\(aa\.Function, sl\.Tuple\{&action\}\)\s*if err != nil \{\s*web_action_abandoned\(a\.id, guard, err\)\s*\}\s*c\.Writer = guard\.ResponseWriter`)
	if !first.MatchString(function) {
		t.Error("THE POINT: web_action does not run the call through a writer it closes on abandonment, ask first, and set its own back")
	}
	if !regexp.MustCompile(`defer func\(\) \{\s*if c\.Request\.MultipartForm != nil \{\s*_ = c\.Request\.MultipartForm\.RemoveAll\(\)\s*\}\s*\}\(\)\s*if err := c\.Request\.ParseMultipartForm`).MatchString(function) {
		t.Error("THE POINT: web_action leaves an upload's temporary files to net/http, which an aborted response skips")
	}

	api, err := os.ReadFile("api.go")
	if err != nil {
		t.Fatalf("reading api.go: %v", err)
	}
	if !regexp.MustCompile(`if errors\.Is\(err, starlark_abandoned\) \{\s*err = fmt\.Errorf\("mochi\.service\.call: %s: %v", f\.Function, err\)`).Match(api) {
		t.Error("THE POINT: a service call's abandonment is passed to its caller as the caller's own")
	}
}

// write_stream_waiting yields nothing until it is closed, as a far end that
// has stopped sending.
type write_stream_waiting struct {
	closed chan struct{}
	once   sync.Once
}

func (r *write_stream_waiting) Read([]byte) (int, error) {
	<-r.closed
	return 0, errors.New("stream closed")
}

func (r *write_stream_waiting) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}

// TestRelayEndsWithItsCall: a relay whose call is given up on stops, its stream
// closed, rather than copying for as long as the far end keeps it open.
func TestRelayEndsWithItsCall(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/people/entity/-/avatar", nil)
	a := &Action{web: c}
	a.web.Header("Content-Type", "image/png")
	ctx, cancel := context.WithCancel(context.Background())
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("context", ctx)
	reader := &write_stream_waiting{closed: make(chan struct{})}
	finished := make(chan error, 1)
	go func() {
		_, err := a.sl_write_stream(thread, sl.NewBuiltin("write.stream", nil), sl.Tuple{&Stream{reader: reader}}, nil)
		finished <- err
	}()
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		reader.Close()
		t.Fatal("THE POINT: the relay ran on after its call's context ended")
	}
}

// write_stream_deadlined records the read deadlines set on it.
type write_stream_deadlined struct {
	*strings.Reader
	deadlines []time.Time
}

func (r *write_stream_deadlined) SetReadDeadline(at time.Time) error {
	r.deadlines = append(r.deadlines, at)
	return nil
}

func (r *write_stream_deadlined) Close() error { return nil }

// TestRelayReadsWaitTheStreamTimeout: every read of a relay's body is bounded
// by the stream's read timeout, so a relay whose far end stalls ends rather
// than holding its goroutine past the call that started it.
func TestRelayReadsWaitTheStreamTimeout(t *testing.T) {
	reader := &write_stream_deadlined{Reader: strings.NewReader("AVATAR")}
	stream := &Stream{reader: reader}
	stream.timeout.read = 7
	limited := &stream_limit_reader{reader: reader, remaining: 64, stream: stream}
	before := time.Now()
	if _, err := io.ReadAll(limited); err != nil {
		t.Fatalf("reading: %v", err)
	}
	if len(reader.deadlines) == 0 {
		t.Fatal("THE POINT: no read of the body was bounded")
	}
	for _, at := range reader.deadlines {
		if wait := at.Sub(before); wait < 6*time.Second || wait > 8*time.Second {
			t.Errorf("a read was bounded at %v, want the stream's 7 s", wait)
		}
	}
}

// TestEtagMatch is the comparison by itself, against RFC 9110's examples.
func TestEtagMatch(t *testing.T) {
	for _, c := range []struct {
		header, etag string
		want         bool
	}{
		{`"xyzzy"`, `"xyzzy"`, true},
		{`W/"xyzzy"`, `"xyzzy"`, true},
		{`"xyzzy"`, `W/"xyzzy"`, true},
		{`"r2d2xxxx", "xyzzy", "c3piozzzz"`, `"xyzzy"`, true},
		{` , "xyzzy"`, `"xyzzy"`, true},
		{`*`, `"xyzzy"`, true},
		{`"xyzzy"`, `"xyzzz"`, false},
		{`"xyzzy`, `"xyzzy"`, false},
		{`xyzzy`, `"xyzzy"`, false},
		{``, `"xyzzy"`, false},
		{`"xyzzy"`, `xyzzy`, false},
		{`*`, `xyzzy`, false},
		{`"xyzzy"`, `"xyzzy" trailing`, false},
	} {
		if got := etag_match(c.header, c.etag); got != c.want {
			t.Errorf("etag_match(%q, %q) = %v, want %v", c.header, c.etag, got, c.want)
		}
	}
}
