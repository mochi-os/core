package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// web_static_context builds a request context for the static-file helpers,
// with the client's validator when etag is set.
func web_static_context(t *testing.T, etag string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/asset.js", nil)
	if etag != "" {
		c.Request.Header.Set("If-None-Match", etag)
	}
	return c, w
}

// A conditional hit is answered by the helper itself, and the caller is told
// so: the 304 is already on the wire and the body must not be produced.
func TestWebCacheStaticAnswersTheConditionalRequest(t *testing.T) {
	saved := web_cache
	web_cache = true
	defer func() { web_cache = saved }()
	file := filepath.Join(t.TempDir(), "asset.js")
	if err := os.WriteFile(file, []byte("console.log(1)"), 0o644); err != nil {
		t.Fatal(err)
	}

	c, w := web_static_context(t, "")
	if web_cache_static(c, file, "revalidate") {
		t.Fatal("a request without If-None-Match was reported as answered")
	}
	etag := w.Header().Get("ETag")
	if etag == "" {
		t.Fatal("no ETag on the first answer, so the test proves nothing")
	}
	if c.IsAborted() {
		t.Fatal("the first request was aborted")
	}

	c, w = web_static_context(t, etag)
	if !web_cache_static(c, file, "revalidate") {
		t.Fatal("a matching If-None-Match was not reported as answered")
	}
	if w.Code != http.StatusNotModified {
		t.Errorf("conditional hit answered %d, want 304", w.Code)
	}

	// The auto-detected html policy takes the same path.
	page := filepath.Join(t.TempDir(), "index.html")
	if err := os.WriteFile(page, []byte("<html></html>"), 0o644); err != nil {
		t.Fatal(err)
	}
	c, w = web_static_context(t, "")
	web_cache_static(c, page, "")
	etag = w.Header().Get("ETag")
	c, w = web_static_context(t, etag)
	if !web_cache_static(c, page, "") || w.Code != http.StatusNotModified {
		t.Errorf("html conditional hit: answered=%v code=%d", w.Code == http.StatusNotModified, w.Code)
	}

	// A policy with no validator never answers.
	c, _ = web_static_context(t, etag)
	if web_cache_static(c, file, "immutable") {
		t.Error("the immutable policy reported a conditional answer")
	}
}

// static_function_source returns one top-level function's text from a Go source file.
func static_function_source(t *testing.T, text, signature string) string {
	t.Helper()
	a := strings.Index(text, signature)
	if a < 0 {
		t.Fatalf("%q not found", signature)
	}
	b := strings.Index(text[a:], "\n}\n")
	if b < 0 {
		t.Fatalf("end of %q not found", signature)
	}
	return text[a : a+b]
}

// Every caller stops once the helper has answered, and the OpenGraph page
// path asks before it runs the app's function and builds the page. Pinned at
// source level: the callers sit inside web_action and the page builder runs
// Starlark, neither of which a unit test drives.
func TestStaticCallersStopAfterAConditionalAnswer(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	calls := strings.Count(text, "web_cache_static(c, file, aa.Cache)")
	guarded := strings.Count(text, "if web_cache_static(c, file, aa.Cache) {")
	if calls != 3 || guarded != calls {
		t.Errorf("web.go: %d web_cache_static callers, %d guarded; want 3 and 3", calls, guarded)
	}
	opengraph := static_function_source(t, text, "func web_serve_file_with_opengraph(")
	check := strings.Index(opengraph, "web_cache_static(")
	call := strings.Index(opengraph, "s.call(aa.OpenGraph")
	if check < 0 || call < 0 || check > call {
		t.Error("web_serve_file_with_opengraph runs the OpenGraph function before answering the conditional request")
	}
}

// robots.txt and the sitemap name the server by its own domain, never by the
// Host header the client sent: the matched hosted domain first, then the
// configured [web] domain, and no absolute URL at all when neither is set.
func TestSiteHostNeverEchoesTheRequestHost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("MOCHI_WEB_DOMAIN", "")
	request := func(domain string) (*gin.Context, *httptest.ResponseRecorder) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/robots.txt", nil)
		c.Request.Host = "attacker.example"
		if domain != "" {
			c.Set("domain_route", &route{Domain: domain})
		}
		return c, w
	}

	c, w := request("")
	web_robots(c)
	if body := w.Body.String(); strings.Contains(body, "Sitemap") || strings.Contains(body, "attacker.example") {
		t.Errorf("robots.txt with nothing configured: %q", body)
	}
	c, w = request("")
	web_sitemap(c)
	if body := w.Body.String(); strings.Contains(body, "attacker.example") || !strings.Contains(body, "<urlset") {
		t.Errorf("sitemap with nothing configured: %q", body)
	}

	c, w = request("hosted.example")
	web_robots(c)
	if body := w.Body.String(); !strings.Contains(body, "Sitemap: https://hosted.example/sitemap.xml") {
		t.Errorf("robots.txt on a hosted domain: %q", body)
	}

	t.Setenv("MOCHI_WEB_DOMAIN", "configured.example")
	c, w = request("")
	web_robots(c)
	if body := w.Body.String(); !strings.Contains(body, "Sitemap: https://configured.example/sitemap.xml") {
		t.Errorf("robots.txt with a configured domain: %q", body)
	}
	c, w = request("")
	web_sitemap(c)
	if body := w.Body.String(); !strings.Contains(body, "<loc>https://configured.example/</loc>") {
		t.Errorf("sitemap with a configured domain: %q", body)
	}
	c, w = request("hosted.example")
	web_robots(c)
	if body := w.Body.String(); !strings.Contains(body, "https://hosted.example/") {
		t.Errorf("the matched domain should win over the configured one: %q", body)
	}

	t.Setenv("MOCHI_WEB_DOMAIN", "localhost")
	c, w = request("")
	web_robots(c)
	if body := w.Body.String(); strings.Contains(body, "Sitemap") {
		t.Errorf("localhost is not a name to advertise: %q", body)
	}
}
