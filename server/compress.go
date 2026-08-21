// Mochi server: Response compression middleware
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"compress/gzip"
	"io"
	"strconv"
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

// accept_encoding_weights parses an Accept-Encoding header into per-coding
// weights (RFC 9110 12.5.3): absent q is 1.0, q=0 is a refusal, "*" is kept as
// an ordinary key for codings the header does not name. Weights are hundredths,
// so integer.
func accept_encoding_weights(accept string) map[string]int {
	weights := map[string]int{}
	for _, element := range strings.Split(strings.ToLower(accept), ",") {
		parts := strings.Split(element, ";")
		coding := strings.TrimSpace(parts[0])
		if coding == "" {
			continue
		}
		weight := 100
		for _, parameter := range parts[1:] {
			parameter = strings.TrimSpace(parameter)
			if !strings.HasPrefix(parameter, "q=") {
				continue
			}
			weight = 0
			// An unparseable q reads as an absent q, not a refusal - treating a
			// malformed header as q=0 would silently stop compressing for that client.
			value := strings.TrimPrefix(parameter, "q=")
			if number, err := strconv.ParseFloat(value, 64); err == nil {
				weight = int(number*100 + 0.5)
			} else {
				weight = 100
			}
		}
		weights[coding] = weight
	}
	return weights
}

// accept_encoding_allows reports the weight a client gave one coding: its own
// entry when the header names it, otherwise the "*" entry, otherwise zero.
// Zero means the client will not take it - either because it said so with q=0
// or because it never offered it.
func accept_encoding_allows(weights map[string]int, coding string) int {
	if weight, named := weights[coding]; named {
		return weight
	}
	if weight, wildcard := weights["*"]; wildcard {
		return weight
	}
	return 0
}

// negotiate_encoding picks brotli or gzip from the server config and the
// client's weights, or "" for none. In auto the client's preference decides;
// the explicit modes send their named coding or nothing.
func negotiate_encoding(accept string) string {
	weights := accept_encoding_weights(accept)
	brotli := accept_encoding_allows(weights, "br")
	gzip := accept_encoding_allows(weights, "gzip")

	switch web_compress {
	case "br":
		if brotli > 0 {
			return "br"
		}
	case "gzip":
		if gzip > 0 {
			return "gzip"
		}
	case "auto":
		// Brotli on a tie: it compresses better, and a client that cared would
		// have said so with a weight.
		if brotli > 0 && brotli >= gzip {
			return "br"
		}
		if gzip > 0 {
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
	written  bool
}

// Written reports whether any payload reached this writer, including bytes
// still buffered inside the encoder. Without it, web.go's !c.Writer.Written()
// check overwrites a buffered a.error() status with 200.
func (w *compress_writer) Written() bool {
	return w.written || w.ResponseWriter.Written()
}

// decide sets the compression headers once Content-Type is known; it is a no-op
// until then and the caller retries. Called from both WriteHeader and Write.
// The underlying writer is created on first Write so HEAD emits no empty
// stream.
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
	w.written = true
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
