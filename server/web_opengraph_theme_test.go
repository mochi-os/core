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
	source := "def opengraph_page(parameters):\n    return {\"title\": \"Stamped\"}\n"
	page := "<!doctype html>\n<html lang=\"en\">\n  <head>\n    <meta property=\"og:title\" content=\"Original\" />\n    <meta property=\"og:type\" content=\"website\" />\n  </head>\n  <body></body>\n</html>\n"
	return opengraph_serve_page(t, validator, source, page)
}

// opengraph_serve_page is opengraph_serve with the app's Starlark source,
// which defines opengraph_page, and its index.html given.
func opengraph_serve_page(t *testing.T, validator bool, source string, page string) *httptest.ResponseRecorder {
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
	if err := os.WriteFile(star, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(base, "index.html")
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

// TestOpenGraphEntryLeavesTheRootEmpty: a page whose Open Graph function
// returns an entry, as a feed post's does, is served with the app's root
// still empty and the entry beside it. Every app mounts only into an empty
// root, so an entry inside it left the post's page as bare text.
func TestOpenGraphEntryLeavesTheRootEmpty(t *testing.T) {
	source := "def opengraph_page(parameters):\n    return {\"title\": \"Post\", \"entry\": {\"content\": \"<p>Hello there</p>\", \"author\": \"Notes\"}}\n"
	page := "<!doctype html>\n<html lang=\"en\">\n  <head>\n    <meta property=\"og:title\" content=\"Original\" />\n    <meta property=\"og:type\" content=\"website\" />\n  </head>\n  <body>\n    <div id=\"root\"></div>\n  </body>\n</html>\n"
	body := opengraph_serve_page(t, false, source, page).Body.String()
	if !strings.Contains(body, `content="Post"`) {
		t.Fatalf("the Open Graph function did not run:\n%s", body)
	}
	if !strings.Contains(body, `<div id="root"></div>`) {
		t.Errorf("THE POINT: the app's root is not empty, so the app never mounts:\n%s", body)
	}
	if !strings.Contains(body, `<div hidden><article class="h-entry">`) || !strings.Contains(body, "Hello there") {
		t.Errorf("the entry is not on the page beside the root:\n%s", body)
	}
}

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

// A page served to a request with no session says so, and its scripts then
// skip asking /_/token for an app token that can only be refused. A signed-in
// requester's page carries no mark: its token is real.
func TestAnonymousPageIsMarked(t *testing.T) {
	mark := `<meta name="mochi:anonymous" content="true">`

	if body := opengraph_serve(t, false).Body.String(); !strings.Contains(body, mark) {
		t.Errorf("THE POINT: the Open Graph path marks an anonymous page:\n%s", body)
	}

	create_web_test_env(t)
	gin.SetMode(gin.TestMode)
	users := db_open("db/users.db")
	users.exec("alter table users add column methods text not null default ''")
	users.exec("alter table users add column disabled text not null default ''")
	users.exec("alter table users add column status text not null default 'active'")
	n := now()
	users.exec("insert into users (uid, username, role, created, updated) values ('u1', 'test@example.com', 'user', ?, ?)", n, n)
	sessions := db_open("db/sessions.db")
	sessions.exec("create table if not exists sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("create unique index if not exists sessions_code on sessions(code)")
	sessions.exec("insert into sessions (user, code, secret, expires, created, address, agent) values ('u1', 'marked-session', 'secret-for-marked-test-123456', ?, ?, '127.0.0.1', 'test')", n+86400, n)

	file := filepath.Join(t.TempDir(), "index.html")
	if err := os.WriteFile(file, []byte("<!doctype html>\n<html>\n  <head>\n    <title>Page</title>\n  </head>\n  <body></body>\n</html>\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	serve := func(session string) string {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/feeds/", nil)
		if session != "" {
			c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
		}
		web_serve_html(c, &App{}, &AppVersion{}, &AppAction{File: "index.html"}, nil, file)
		return w.Body.String()
	}

	if body := serve(""); !strings.Contains(body, mark) {
		t.Errorf("THE POINT: an anonymous page is marked:\n%s", body)
	}
	if body := serve("marked-session"); strings.Contains(body, "mochi:anonymous") {
		t.Errorf("a signed-in requester's page is marked anonymous:\n%s", body)
	}
}
