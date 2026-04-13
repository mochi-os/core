// Mochi server: Response compression middleware
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"compress/gzip"
	"strings"

	"github.com/gin-gonic/gin"
)

// web_compress_middleware transparently gzips eligible responses when
// the client accepts gzip. Skips WebSocket upgrades and non-text content.
func web_compress_middleware(c *gin.Context) {
	if c.Request.URL.Path == "/_/websocket" {
		c.Next()
		return
	}

	if !strings.Contains(c.GetHeader("Accept-Encoding"), "gzip") {
		c.Next()
		return
	}

	w := &gzip_writer{ResponseWriter: c.Writer, level: web_gzip_level}
	c.Writer = w
	c.Next()
	w.close()
}

type gzip_writer struct {
	gin.ResponseWriter
	level    int
	gz       *gzip.Writer
	decided  bool
	compress bool
}

// decide sets up compression headers based on Content-Type. Called from
// both WriteHeader (so HEAD responses get correct headers) and Write
// (so Gin render flows, which set Content-Type after c.Status, work).
// No-op if Content-Type is not yet set — caller will retry.
// The gzip.Writer itself is created lazily on first Write so HEAD
// responses don't emit the empty-stream 20-byte envelope.
func (w *gzip_writer) decide() {
	if w.decided {
		return
	}
	ct := w.Header().Get("Content-Type")
	if ct == "" {
		return
	}
	w.decided = true
	if w.Header().Get("Content-Encoding") != "" {
		return
	}
	if !gzip_compressible(ct) {
		return
	}
	w.compress = true
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Add("Vary", "Accept-Encoding")
	// Length is unknown once compressed; let Go use chunked encoding.
	w.Header().Del("Content-Length")
}

func (w *gzip_writer) WriteHeader(code int) {
	w.decide()
	w.ResponseWriter.WriteHeader(code)
}

func (w *gzip_writer) Write(p []byte) (int, error) {
	w.decide()
	if !w.decided {
		w.decided = true
	}
	if !w.compress {
		return w.ResponseWriter.Write(p)
	}
	if w.gz == nil {
		gz, err := gzip.NewWriterLevel(w.ResponseWriter, w.level)
		if err != nil {
			gz = gzip.NewWriter(w.ResponseWriter)
		}
		w.gz = gz
	}
	return w.gz.Write(p)
}

func (w *gzip_writer) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

func (w *gzip_writer) Flush() {
	if w.compress && w.gz != nil {
		w.gz.Flush()
	}
	w.ResponseWriter.Flush()
}

func (w *gzip_writer) close() {
	if w.compress && w.gz != nil {
		w.gz.Close()
	}
}

func gzip_compressible(ct string) bool {
	ct = strings.ToLower(ct)
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	if strings.HasPrefix(ct, "text/") {
		return true
	}
	switch ct {
	case "application/javascript", "application/x-javascript",
		"application/json", "application/xml",
		"application/wasm", "image/svg+xml":
		return true
	}
	return false
}
