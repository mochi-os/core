// Mochi server: tests of the files a hosted fediverse account carries
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// TestMediaStore: an upload through a hosted fediverse account is kept and served
// under the account as the Document the answer names; a type that is not one
// is refused; the file is nobody else's and goes with the account.
func TestMediaStore(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	thread := create_acting_thread(alice)
	fn := sl.NewBuiltin("test", nil)

	upload := decoded(t)(api_account_upload(thread, fn, sl.Tuple{sl.String("f1"), sl.Bytes("\x89PNG\r\n\x1a\nimage"), sl.String("image/png")}, []sl.Tuple{{sl.String("name"), sl.String("cat.png")}, {sl.String("description"), sl.String("A cat")}}))
	id, _ := upload["id"].(string)
	url, _ := upload["url"].(string)
	document, _ := upload["data"].(map[string]any)
	if id == "" || url != "https://example.com/~f1/media/"+id || document["type"] != "Image" || document["mediaType"] != "image/png" || document["url"] != url || document["name"] != "A cat" {
		t.Fatalf("upload = %v", upload)
	}
	if _, err := os.Stat(media_path(alice, id)); err != nil {
		t.Fatalf("file not kept: %v", err)
	}

	r := httptest.NewRequest("GET", url, nil)
	w := activitypub_request(t, r)
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "image/png" || w.Body.String() != "\x89PNG\r\n\x1a\nimage" || w.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("served = %d %q %q", w.Code, w.Header().Get("Content-Type"), w.Body.String())
	}
	// Another fediverse account's path and a made-up id answer not found.
	db_user(alice, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('f2', 'fediverse', '', '@second@example.com', '{\"hosted\":true,\"handle\":\"second\",\"domain\":\"example.com\"}', 1, 1)")
	actor_register(alice, "f2", "fediverse", "example.com", "second")
	if w := activitypub_request(t, httptest.NewRequest("GET", "https://example.com/~f2/media/"+id, nil)); w.Code != http.StatusNotFound {
		t.Errorf("another account's media = %d", w.Code)
	}
	if w := activitypub_request(t, httptest.NewRequest("GET", "https://example.com/~f1/media/nothing", nil)); w.Code != http.StatusNotFound {
		t.Errorf("unknown media = %d", w.Code)
	}

	if _, err := api_account_upload(thread, fn, sl.Tuple{sl.String("f1"), sl.Bytes("x"), sl.String("png")}, nil); err == nil || !strings.Contains(err.Error(), "invalid type") {
		t.Errorf("a bare type: %v", err)
	}

	activitypub_account_remove(alice, "f1")
	if _, err := os.Stat(media_path(alice, id)); err == nil {
		t.Error("the file outlived the account")
	}
	if row, _ := db_user(alice, "user").row("select id from media where id=?", id); row != nil {
		t.Error("the media row outlived the account")
	}
}

// discard is a response writer that counts the body it is sent and keeps
// none of it, so a test can measure what serving a file allocates.
type discard struct {
	header http.Header
	code   int
	size   int64
}

func (d *discard) Header() http.Header { return d.header }

func (d *discard) WriteHeader(code int) {
	if d.code == 0 {
		d.code = code
	}
}

func (d *discard) Write(p []byte) (int, error) {
	if d.code == 0 {
		d.code = http.StatusOK
	}
	d.size += int64(len(p))
	return len(p), nil
}

// media_allocated is how many bytes the process allocates while serve runs n
// times.
func media_allocated(n int, serve func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range n {
		serve()
	}
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestMediaServedFromTheFile: a hosted account's file is sent from its
// descriptor, not read whole into memory first, and a range and a
// revalidation are answered as asked; a directory where a file should be is
// not found.
func TestMediaServedFromTheFile(t *testing.T) {
	alice, _ := create_activitypub_test_env(t)
	thread := create_acting_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	video := bytes.Repeat([]byte("0123456789abcdef"), media_maximum/16-1)
	upload := decoded(t)(api_account_upload(thread, fn, sl.Tuple{sl.String("f1"), sl.Bytes(video), sl.String("video/mp4")}, nil))
	address, _ := upload["url"].(string)
	get := func(method string, headers ...string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, address, nil)
		for i := 0; i+1 < len(headers); i += 2 {
			r.Header.Set(headers[i], headers[i+1])
		}
		return activitypub_request(t, r)
	}
	w := get("GET")
	tag := w.Header().Get("ETag")
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), video) || w.Header().Get("Content-Type") != "video/mp4" || w.Header().Get("Content-Length") != strconv.Itoa(len(video)) || w.Header().Get("Accept-Ranges") != "bytes" || tag == "" || w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Cache-Control") != "public, max-age=86400" {
		t.Fatalf("THE POINT: the file = %d, %d bytes, %v", w.Code, w.Body.Len(), w.Header())
	}
	if w := get("GET", "Range", "bytes=0-1"); w.Code != http.StatusPartialContent || w.Body.String() != "01" || w.Header().Get("Content-Range") != fmt.Sprintf("bytes 0-1/%d", len(video)) {
		t.Errorf("THE POINT: a range = %d %q %q", w.Code, w.Body.String(), w.Header().Get("Content-Range"))
	}
	if w := get("GET", "If-None-Match", tag); w.Code != http.StatusNotModified || w.Body.Len() != 0 {
		t.Errorf("THE POINT: a revalidation = %d, %d bytes", w.Code, w.Body.Len())
	}
	allocated := media_allocated(5, func() {
		d := &discard{header: http.Header{}}
		c, _ := gin.CreateTestContext(d)
		c.Request = httptest.NewRequest("GET", address, nil)
		activitypub_route(c)
		if d.code != http.StatusOK || d.size != int64(len(video)) {
			t.Fatalf("a fetch = %d, %d bytes", d.code, d.size)
		}
	})
	if allocated > 5<<20 {
		t.Errorf("THE POINT: five fetches of a %d byte file allocated %d bytes", len(video), allocated)
	}

	id, _ := upload["id"].(string)
	os.Remove(media_path(alice, id))
	os.Mkdir(media_path(alice, id), 0o755)
	if w := get("GET"); w.Code != http.StatusNotFound {
		t.Errorf("THE POINT: a directory where the file was = %d", w.Code)
	}
}

// media_engine serves one file by media_send, with the type and tag given,
// behind the compression middleware as the server's routes are, and answers
// a fetch as it reaches the wire.
func media_engine(t *testing.T, path string, kind string, tag string) func(method string, headers ...string) (*http.Response, string) {
	t.Helper()
	saved := web_compress
	web_compress = "auto"
	t.Cleanup(func() { web_compress = saved })
	engine := gin.New()
	engine.Use(web_compress_middleware)
	handler := func(c *gin.Context) {
		file, information := media_open(path)
		if file == nil {
			c.Status(http.StatusNotFound)
			return
		}
		defer file.Close()
		media_send(c, file, information, kind, tag)
	}
	engine.GET("/file", handler)
	engine.HEAD("/file", handler)
	return func(method string, headers ...string) (*http.Response, string) {
		t.Helper()
		r := httptest.NewRequest(method, "/file", nil)
		for i := 0; i+1 < len(headers); i += 2 {
			r.Header.Set(headers[i], headers[i+1])
		}
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, r)
		return w.Result(), w.Body.String()
	}
}

// TestMediaSentCompressed: a file encoded as it is sent is tagged as the same
// content rather than the same bytes, with no ranges or time offered, and its
// 304 carries the 200's tag and Vary, so a resumed download cannot join stored
// bytes to encoded ones; a file sent as stored keeps every validator; a
// caller's own tag sends no time; an answer with no body names no coding.
func TestMediaSentCompressed(t *testing.T) {
	folder := t.TempDir()
	text := filepath.Join(folder, "notes")
	content := strings.Repeat("a line of a text file\n", 2000)
	if err := os.WriteFile(text, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	get := media_engine(t, text, "text/plain", "")

	response, body := get("GET", "Accept-Encoding", "br")
	tag := response.Header.Get("ETag")
	decoded, _ := io.ReadAll(brotli.NewReader(strings.NewReader(body)))
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != "br" || string(decoded) != content {
		t.Fatalf("the encoded file = %d %q, %d bytes", response.StatusCode, response.Header.Get("Content-Encoding"), len(decoded))
	}
	if !strings.HasPrefix(tag, `W/"`) || response.Header.Get("Accept-Ranges") != "" || response.Header.Get("Last-Modified") != "" || len(response.Header.Values("Vary")) != 1 || !compress_varies(response.Header) {
		t.Errorf("THE POINT: the encoded file's validators = %v", response.Header)
	}
	if response, _ := get("GET", "Accept-Encoding", "br", "If-None-Match", tag); response.StatusCode != http.StatusNotModified || response.Header.Get("ETag") != tag || !compress_varies(response.Header) {
		t.Errorf("THE POINT: a revalidation of the encoded file = %d %v", response.StatusCode, response.Header)
	}
	if response, body := get("GET", "Accept-Encoding", "br", "Range", "bytes=0-5", "If-Range", tag); response.StatusCode != http.StatusOK || body != content {
		t.Errorf("THE POINT: a range resumed on the encoded file's tag = %d, %d bytes", response.StatusCode, len(body))
	}
	response, body = get("GET", "Range", "bytes=0-5")
	stored := response.Header.Get("ETag")
	if response.StatusCode != http.StatusPartialContent || body != "a line" || strings.HasPrefix(stored, "W/") || response.Header.Get("Last-Modified") == "" {
		t.Errorf("a range of the stored bytes = %d %q %v", response.StatusCode, body, response.Header)
	}
	if response, body := get("GET", "Range", "bytes=0-5", "If-Range", stored); response.StatusCode != http.StatusPartialContent || body != "a line" {
		t.Errorf("a range resumed on the stored bytes' tag = %d %q", response.StatusCode, body)
	}
	if response, _ := get("GET", "Accept-Encoding", "br", "If-Match", `"another"`); response.StatusCode != http.StatusPreconditionFailed || response.Header.Get("Content-Encoding") != "" {
		t.Errorf("THE POINT: a refused precondition = %d, coding %q", response.StatusCode, response.Header.Get("Content-Encoding"))
	}
	if response, body := get("HEAD", "Accept-Encoding", "br"); response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != "br" || response.Header.Get("ETag") != tag || body != "" {
		t.Errorf("THE POINT: a HEAD of the encoded file = %d %v", response.StatusCode, response.Header)
	}
	empty := filepath.Join(folder, "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if response, body := media_engine(t, empty, "text/plain", "")("GET", "Accept-Encoding", "br"); response.StatusCode != http.StatusOK || body != "" || response.Header.Get("Content-Encoding") != "" {
		t.Errorf("THE POINT: an empty file = %d, coding %q", response.StatusCode, response.Header.Get("Content-Encoding"))
	}

	image := filepath.Join(folder, "image")
	if err := os.WriteFile(image, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	response, _ = media_engine(t, image, "image/png", "")("GET", "Accept-Encoding", "br")
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != "" || strings.HasPrefix(response.Header.Get("ETag"), "W/") || response.Header.Get("Accept-Ranges") != "bytes" || response.Header.Get("Last-Modified") == "" || compress_varies(response.Header) {
		t.Errorf("THE POINT: a file sent as stored = %d %v", response.StatusCode, response.Header)
	}

	tagged := media_engine(t, image, "image/png", `"server/id"`)
	if response, _ := tagged("GET"); response.Header.Get("ETag") != `"server/id"` || response.Header.Get("Last-Modified") != "" {
		t.Errorf("THE POINT: a file with its caller's tag = %v", response.Header)
	}
	if response, _ := tagged("GET", "Range", "bytes=0-5", "If-Unmodified-Since", time.Now().Add(-48*time.Hour).UTC().Format(http.TimeFormat)); response.StatusCode != http.StatusPartialContent {
		t.Errorf("THE POINT: a range conditioned on a time the caller's tag replaces = %d", response.StatusCode)
	}
	if response, _ := tagged("GET", "If-Modified-Since", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)); response.StatusCode != http.StatusOK {
		t.Errorf("THE POINT: a revalidation by a time the caller's tag replaces = %d", response.StatusCode)
	}

	// The middleware is found under another writer, and not taken to encode an
	// answer already encoded.
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Writer = &resource_writer{ResponseWriter: &compress_writer{ResponseWriter: c.Writer, encoding: "br"}}
	if !compress_planned(c, "text/plain") || compress_planned(c, "image/png") {
		t.Errorf("THE POINT: the middleware under another writer not found")
	}
	c.Writer.Header().Set("Content-Encoding", "gzip")
	if compress_planned(c, "text/plain") {
		t.Errorf("THE POINT: an answer already encoded taken to be encoded again")
	}
	bare, _ := gin.CreateTestContext(httptest.NewRecorder())
	if compress_planned(bare, "text/plain") {
		t.Errorf("an answer with no middleware taken to be encoded")
	}
}
