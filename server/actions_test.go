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
// returns a function that serves a path from it the way an action would. hosted
// picks which of the two callers is being modelled: a request that arrived on a
// domain route, which publishes a site and may render it, or a plain request to
// the app, which serves stored content and may not.
func write_file_environment(t *testing.T, url string, hosted bool) (string, func(string) *httptest.ResponseRecorder) {
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

// TestWriteFileFollowsSiblingFileSymlink covers the other in-directory shape:
// a link whose final component points at a sibling file rather than at a
// directory, which resolves by a different path through os.Root - a final
// component is opened, not walked. The release publishes each installer under
// a version-stamped name and leaves the stable download name beside it as a
// link, so it is uploaded once instead of twice; if this stopped resolving the
// download URL would 404 while the updater, which fetches the stamped name
// directly, kept working and hid it.
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

// TestWriteFileDownloadsDocument covers the stored-content case: a files
// directory holds what its app was given, and most apps are given it by someone
// other than the reader - an attachment, a photo, a purchased asset. A document
// among them served inline would run in this origin with the reader's session,
// so anything outside the inline allowlist has to arrive as a download.
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

// TestDomainRouteDoesNotGrantSiteServing is the point of moving the exemption
// onto the declaration. A domain route pointed at an app that serves uploads -
// rather than at one publishing a site - used to carry the exemption with it,
// so that app served uploaded documents raw on the route's hostname. Routing is
// how a reader arrived, not what the app meant.
func TestDomainRouteDoesNotGrantSiteServing(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })

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
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })

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

// TestWriteFileDirectRequestKeepsOwner is the other half: with no route, the
// action's own owner still decides which directory is read. Redirecting that
// generally would be an escalation - apps read owner == user as "the requester
// owns this data", so an authenticated stranger handed the published account's
// owner is authorized as that account.
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

// TestWriteFileUnresolvedRouteOwnerFailsClosed covers a hosted domain whose
// account no longer resolves - most often because it was deleted after the
// route was made, leaving a live hostname pointing at nobody. Falling back to
// the requester would put back the behaviour the route owner lookup exists to
// remove: one URL answering with whoever happens to be asking.
func TestWriteFileUnresolvedRouteOwnerFailsClosed(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })

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
