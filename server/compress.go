// Mochi server: Response compression middleware
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"compress/gzip"
	"io"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
)

// web_compress_middleware transparently compresses eligible responses,
// negotiating brotli or gzip based on the client's Accept-Encoding and
// the server's web.compress setting. Skips WebSocket upgrades, range
// requests, and non-text content.
func web_compress_middleware(c *gin.Context) {
	if c.Request.URL.Path == "/_/websocket" {
		c.Next()
		return
	}

	// Range requests are satisfied over raw bytes by gin's static handler.
	// Compressing the body would desync Content-Range (raw-byte offsets)
	// from the actual payload and browsers reject the response.
	if c.GetHeader("Range") != "" {
		c.Next()
		return
	}

	encoding := negotiate_encoding(c.GetHeader("Accept-Encoding"))
	if encoding == "" {
		c.Next()
		return
	}

	w := &compress_writer{ResponseWriter: c.Writer, encoding: encoding}
	c.Writer = w
	c.Next()
	w.close()
}

// negotiate_encoding picks brotli or gzip based on the server config and
// what the client accepts. Returns "" when no compression should be used.
func negotiate_encoding(accept string) string {
	accept = strings.ToLower(accept)
	wants_br := strings.Contains(accept, "br")
	wants_gz := strings.Contains(accept, "gzip")
	switch web_compress {
	case "br":
		if wants_br {
			return "br"
		}
	case "gzip":
		if wants_gz {
			return "gzip"
		}
	case "auto":
		if wants_br {
			return "br"
		}
		if wants_gz {
			return "gzip"
		}
	}
	return ""
}

type compress_writer struct {
	gin.ResponseWriter
	encoding string
	w        io.WriteCloser
	decided  bool
	compress bool
}

// decide sets up compression headers based on Content-Type. Called from
// both WriteHeader (so HEAD responses get correct headers) and Write
// (so Gin render flows, which set Content-Type after c.Status, work).
// No-op if Content-Type is not yet set — caller will retry.
// The underlying writer is created lazily on first Write so HEAD
// responses don't emit an empty-stream envelope.
func (w *compress_writer) decide() {
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
	if !compressible(ct) {
		return
	}
	w.compress = true
	w.Header().Set("Content-Encoding", w.encoding)
	w.Header().Add("Vary", "Accept-Encoding")
	// Length is unknown once compressed; let Go use chunked encoding.
	w.Header().Del("Content-Length")
}

func (w *compress_writer) WriteHeader(code int) {
	w.decide()
	w.ResponseWriter.WriteHeader(code)
}

func (w *compress_writer) Write(p []byte) (int, error) {
	w.decide()
	if !w.decided {
		w.decided = true
	}
	if !w.compress {
		return w.ResponseWriter.Write(p)
	}
	if w.w == nil {
		w.w = new_encoder(w.encoding, w.ResponseWriter)
	}
	return w.w.Write(p)
}

func (w *compress_writer) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

func (w *compress_writer) Flush() {
	if w.compress && w.w != nil {
		if f, ok := w.w.(interface{ Flush() error }); ok {
			f.Flush()
		}
	}
	w.ResponseWriter.Flush()
}

func (w *compress_writer) close() {
	if w.compress && w.w != nil {
		w.w.Close()
	}
}

func new_encoder(encoding string, dst io.Writer) io.WriteCloser {
	if encoding == "br" {
		return brotli.NewWriterLevel(dst, web_brotli_level)
	}
	gz, err := gzip.NewWriterLevel(dst, web_gzip_level)
	if err != nil {
		return gzip.NewWriter(dst)
	}
	return gz
}

func compressible(ct string) bool {
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
