// Mochi server: Response compression middleware
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

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

// Unwrap hands the ResponseController the writer underneath. Without it the
// chain stops here - the embedded gin.ResponseWriter is an INTERFACE, which
// does not declare Unwrap - so http.NewResponseController(...).SetWriteDeadline
// in starlark_serving_set answered ErrNotSupported and the download had no
// deadline at all. gin's own responseWriter implements Unwrap, so returning the
// embedded value completes the chain.
func (w *compress_writer) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
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
		"application/wasm", "image/svg+xml", "model/gltf-binary":
		return true
	}
	return false
}

// Precompressed copies. Brotli at the level a request can afford is no smaller
// than gzip; level 11 is 15-25% smaller for code but takes about two seconds
// per megabyte, far too slow for a request. So the first request for a hashed
// asset of an installed app is compressed on the fly as before and queues the
// file, one background worker writes a level-11 copy, and every later request
// that accepts brotli is sent the copy. Only files someone fetches are
// compressed: most of an app's code is translations nobody loads.
//
// A copy lives at <version>/.compressed/<path>.br, so it goes when its version
// does. It carries its source's modification time and is used only while the
// two match and it is the smaller, so a source rewritten in place is served
// raw until the worker replaces the copy. Development apps get none: their
// folder is the source tree.
//
// The worker compresses into data_dir/tmp and puts nothing in a version
// directory except under compressed_lock, which the code that removes or
// replaces a version directory also holds: an entry created while RemoveAll
// walks the tree makes it fail half way, leaving the live version gutted.

const (
	compressed_level = 11
	// compressed_retry is how long a file whose copy failed is left alone, so
	// a full disk does not cost a level-11 pass on every request for it.
	compressed_retry = time.Hour
)

type compressed_job struct {
	base string
	file string
}

var (
	compressed_queue   = make(chan compressed_job, 1024)
	compressed_pending sync.Map
	compressed_failed  sync.Map
	compressed_lock    sync.Mutex
)

// compressed_temporary is where copies are written before they are moved into
// their version: data_dir/tmp, on the same filesystem as the versions.
func compressed_temporary() string {
	return filepath.Join(data_dir, "tmp")
}

// compressed_path is where the copy of file, inside the version at base, is
// kept, or "" for a file outside it.
func compressed_path(base string, file string) string {
	relative, err := filepath.Rel(base, file)
	if err != nil || relative == "." || strings.HasPrefix(relative, "..") {
		return ""
	}
	return filepath.Join(base, ".compressed", relative+".br")
}

// compressed_eligible reports whether file gets a copy: a hashed asset of a
// type the middleware would compress. SVG is left out: it is served through
// web_serve_svg_path's own headers.
func compressed_eligible(file string) bool {
	if !match_hashed.MatchString(file) {
		return false
	}
	kind := mime.TypeByExtension(filepath.Ext(file))
	return kind != "" && compressible(kind) && !strings.HasPrefix(kind, "image/svg+xml")
}

// compressed_request queues file for a copy, once while it waits. A full
// queue drops the request: the next fetch of the file asks again.
func compressed_request(base string, file string) {
	if failed, ok := compressed_failed.Load(file); ok && time.Since(failed.(time.Time)) < compressed_retry {
		return
	}
	if _, waiting := compressed_pending.LoadOrStore(file, true); waiting {
		return
	}
	select {
	case compressed_queue <- compressed_job{base: base, file: file}:
	default:
		compressed_pending.Delete(file)
	}
}

// compressed_manager writes the queued copies one at a time, so they cost at
// most one core. Temporaries left by a process that stopped mid-copy are
// removed first: nothing else is writing them yet.
func compressed_manager() {
	if leftovers, err := filepath.Glob(filepath.Join(compressed_temporary(), "compressed-*")); err == nil {
		for _, leftover := range leftovers {
			_ = os.Remove(leftover)
		}
	}
	for job := range compressed_queue {
		compressed_job_run(job)
	}
}

// compressed_job_run makes one copy. A panic is the copy's alone: it must not
// take the server down with it.
func compressed_job_run(job compressed_job) {
	defer compressed_pending.Delete(job.file)
	defer func() {
		if r := recover(); r != nil {
			warn("Compressed copy of %q panicked: %v", job.file, r)
			compressed_failed.Store(job.file, time.Now())
		}
	}()
	if err := compressed_make(job.base, job.file); err != nil {
		debug("Compressed copy of %q failed: %v", job.file, err)
		compressed_failed.Store(job.file, time.Now())
		return
	}
	compressed_failed.Delete(job.file)
}

// compressed_make writes the level-11 copy of file, unless a current one
// exists. A source that changes or goes while it is compressed - its version
// reinstalled or removed - is not an error: nothing is written for it, and the
// next request for the file asks again.
func compressed_make(base string, file string) error {
	path := compressed_path(base, file)
	if path == "" {
		return fmt.Errorf("%q is outside %q", file, base)
	}
	source, err := os.Stat(file)
	if err != nil {
		return nil
	}
	if stored, err := os.Stat(path); err == nil && stored.ModTime().Equal(source.ModTime()) {
		return nil
	}
	input, err := os.Open(file)
	if err != nil {
		return nil
	}
	defer input.Close()
	if err := os.MkdirAll(compressed_temporary(), 0755); err != nil {
		return err
	}
	output, err := os.CreateTemp(compressed_temporary(), "compressed-*")
	if err != nil {
		return err
	}
	temporary := output.Name()
	defer os.Remove(temporary)
	writer := brotli.NewWriterLevel(output, compressed_level)
	_, err = io.Copy(writer, input)
	if closed := writer.Close(); err == nil {
		err = closed
	}
	// On disk before it is renamed into place: after a crash a copy must be
	// whole or absent, never a short file under the right name and time.
	if synced := output.Sync(); err == nil {
		err = synced
	}
	if closed := output.Close(); err == nil {
		err = closed
	}
	if err != nil {
		return err
	}

	compressed_lock.Lock()
	defer compressed_lock.Unlock()
	// The source as it was read, still in its version: otherwise the version
	// was reinstalled or removed meanwhile and the copy belongs to nothing.
	if after, err := os.Stat(file); err != nil || !after.ModTime().Equal(source.ModTime()) || after.Size() != source.Size() {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := os.Chtimes(temporary, source.ModTime(), source.ModTime()); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// web_serve_compressed sends file's copy when the client takes brotli and a
// current copy exists, reporting whether it answered. Without a copy it queues
// one and leaves the request to the plain path.
func web_serve_compressed(c *gin.Context, a *App, base string, file string) bool {
	if a == nil || a.development || !compressed_eligible(file) {
		return false
	}
	// Ranges count bytes of the file itself: the plain path serves them, as
	// the middleware leaves them alone.
	if c.GetHeader("Range") != "" || negotiate_encoding(c.GetHeader("Accept-Encoding")) != "br" {
		return false
	}
	path := compressed_path(base, file)
	if path == "" {
		return false
	}
	source, err := os.Stat(file)
	if err != nil || !source.Mode().IsRegular() {
		return false
	}
	stored, err := os.Stat(path)
	if err != nil || !stored.ModTime().Equal(source.ModTime()) {
		compressed_request(base, file)
		return false
	}
	if stored.Size() >= source.Size() {
		return false
	}
	content, err := os.Open(path)
	if err != nil {
		return false
	}
	defer content.Close()
	// The length of the file opened, not of whatever the path named before.
	opened, err := content.Stat()
	if err != nil {
		return false
	}

	header := c.Writer.Header()
	header.Add("Vary", "Accept-Encoding")
	header.Set("Last-Modified", source.ModTime().UTC().Format(http.TimeFormat))
	if since, err := http.ParseTime(c.GetHeader("If-Modified-Since")); err == nil && !source.ModTime().Truncate(time.Second).After(since) {
		c.Status(http.StatusNotModified)
		c.Writer.WriteHeaderNow()
		return true
	}
	header.Set("Content-Type", mime.TypeByExtension(filepath.Ext(file)))
	header.Set("Content-Encoding", "br")
	header.Set("Content-Length", strconv.FormatInt(opened.Size(), 10))
	c.Status(http.StatusOK)
	if c.Request.Method == http.MethodHead {
		c.Writer.WriteHeaderNow()
		return true
	}
	_, _ = io.Copy(c.Writer, content)
	return true
}
