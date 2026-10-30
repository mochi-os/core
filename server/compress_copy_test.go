// Mochi server: Precompressed copy tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
)

// compressed_fixture is an installed version holding one hashed script large
// enough that its copy is the smaller, with a fresh queue and auto encoding.
func compressed_fixture(t *testing.T) (base string, file string, source []byte) {
	t.Helper()
	saved, queue := web_compress, compressed_queue
	web_compress = "auto"
	compressed_queue = make(chan compressed_job, 16)
	clear := func() {
		compressed_pending.Range(func(key, _ any) bool { compressed_pending.Delete(key); return true })
		compressed_failed.Range(func(key, _ any) bool { compressed_failed.Delete(key); return true })
	}
	clear()
	t.Cleanup(func() {
		web_compress, compressed_queue = saved, queue
		clear()
	})
	test_bare_directory(t)
	base = filepath.Join(data_dir, "apps", "app", "1.0")
	file = filepath.Join(base, "web", "dist", "assets", "index-AbCd12_-.js")
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	source = []byte(strings.Repeat("export function greet(name) { return 'Hello, ' + name }\n", 400))
	if err := os.WriteFile(file, source, 0o644); err != nil {
		t.Fatal(err)
	}
	return base, file, source
}

// compressed_fetch sends one request through the compression middleware to a
// handler that serves file the way web_action's file branches do.
func compressed_fetch(t *testing.T, a *App, base string, file string, method string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(web_compress_middleware)
	r.Handle(method, "/asset", func(c *gin.Context) {
		if !web_serve_compressed(c, a, base, file) {
			web_serve_path(c, file)
		}
	})
	request := httptest.NewRequest(method, "/asset", nil)
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, request)
	return w
}

func compressed_decode(t *testing.T, encoding string, body []byte) []byte {
	t.Helper()
	var reader io.Reader
	switch encoding {
	case "br":
		reader = brotli.NewReader(bytes.NewReader(body))
	case "gzip":
		z, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		reader = z
	default:
		return body
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("decoding %s: %v", encoding, err)
	}
	return decoded
}

// compressed_run makes the copies for every queued request, as the manager does.
func compressed_run(t *testing.T) int {
	t.Helper()
	made := 0
	for {
		select {
		case job := <-compressed_queue:
			if err := compressed_make(job.base, job.file); err != nil {
				t.Fatalf("compressed_make: %v", err)
			}
			compressed_pending.Delete(job.file)
			made++
		default:
			return made
		}
	}
}

var compressed_brotli = map[string]string{"Accept-Encoding": "gzip, deflate, br"}

// The first fetch is compressed on the fly and queues the file once, however
// often it is asked for meanwhile; once the copy exists it is what is sent,
// with its length, and it decodes to the file.
func TestCompressedCopyIsMadeOnceAndThenServed(t *testing.T) {
	base, file, source := compressed_fixture(t)
	a := &App{id: "test"}

	for i := 0; i < 2; i++ {
		w := compressed_fetch(t, a, base, file, http.MethodGet, compressed_brotli)
		if w.Header().Get("Content-Encoding") != "br" || w.Header().Get("Content-Length") != "" {
			t.Fatalf("fetch %d before the copy: encoding %q length %q, want on-the-fly brotli", i, w.Header().Get("Content-Encoding"), w.Header().Get("Content-Length"))
		}
		if !bytes.Equal(compressed_decode(t, "br", w.Body.Bytes()), source) {
			t.Fatalf("fetch %d before the copy did not decode to the file", i)
		}
	}
	if n := len(compressed_queue); n != 1 {
		t.Fatalf("two fetches queued %d copies, want 1", n)
	}
	if made := compressed_run(t); made != 1 {
		t.Fatalf("made %d copies, want 1", made)
	}

	path := compressed_path(base, file)
	stored, err := os.Stat(path)
	if err != nil {
		t.Fatalf("no copy at %s: %v", path, err)
	}
	information, _ := os.Stat(file)
	if !stored.ModTime().Equal(information.ModTime()) {
		t.Errorf("copy time %v, file time %v: they must match", stored.ModTime(), information.ModTime())
	}
	packed, _ := os.ReadFile(path)
	if !bytes.Equal(compressed_decode(t, "br", packed), source) {
		t.Fatal("the copy does not decode to the file")
	}

	w := compressed_fetch(t, a, base, file, http.MethodGet, compressed_brotli)
	if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "br" {
		t.Fatalf("fetch with the copy: %d %q", w.Code, w.Header().Get("Content-Encoding"))
	}
	if w.Header().Get("Content-Length") != strconv.Itoa(len(packed)) || !bytes.Equal(w.Body.Bytes(), packed) {
		t.Fatalf("fetch with the copy sent %d bytes with length %q, want the %d-byte copy", w.Body.Len(), w.Header().Get("Content-Length"), len(packed))
	}
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "text/javascript") {
		t.Errorf("content type %q, want the script's", w.Header().Get("Content-Type"))
	}
	if len(compressed_queue) != 0 {
		t.Error("a fetch served from a current copy queued another")
	}

	// HEAD: the copy's length, no body.
	w = compressed_fetch(t, a, base, file, http.MethodHead, compressed_brotli)
	if w.Header().Get("Content-Length") != strconv.Itoa(len(packed)) || w.Body.Len() != 0 {
		t.Errorf("HEAD: length %q body %d, want %d and none", w.Header().Get("Content-Length"), len(packed), w.Body.Len())
	}

	// A conditional request for an unchanged file is answered 304, varying on
	// the encoding like the 200 it stands for.
	headers := map[string]string{"Accept-Encoding": "br", "If-Modified-Since": information.ModTime().UTC().Format(http.TimeFormat)}
	w = compressed_fetch(t, a, base, file, http.MethodGet, headers)
	if w.Code != http.StatusNotModified || w.Body.Len() != 0 {
		t.Errorf("conditional fetch: %d with %d bytes, want 304 and none", w.Code, w.Body.Len())
	}
	if !strings.Contains(w.Header().Get("Vary"), "Accept-Encoding") {
		t.Errorf("304 Vary %q, want Accept-Encoding", w.Header().Get("Vary"))
	}
}

// A current copy is left as it is.
func TestCompressedCopyIsNotRemade(t *testing.T) {
	base, file, _ := compressed_fixture(t)
	if err := compressed_make(base, file); err != nil {
		t.Fatal(err)
	}
	path := compressed_path(base, file)
	information, _ := os.Stat(path)
	marker := []byte("left alone")
	if err := os.WriteFile(path, marker, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, information.ModTime(), information.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := compressed_make(base, file); err != nil {
		t.Fatal(err)
	}
	if content, _ := os.ReadFile(path); !bytes.Equal(content, marker) {
		t.Error("a current copy was compressed again")
	}
}

// The copy is sent only to a client that takes brotli, never for a range, never
// once the file has changed, and never when it is not the smaller.
func TestCompressedCopyIsSentOnlyWhenItFits(t *testing.T) {
	base, file, source := compressed_fixture(t)
	a := &App{id: "test"}
	if err := compressed_make(base, file); err != nil {
		t.Fatal(err)
	}
	path := compressed_path(base, file)
	packed, _ := os.ReadFile(path)
	from_copy := func(w *httptest.ResponseRecorder) bool {
		return w.Header().Get("Content-Length") == strconv.Itoa(len(packed)) && bytes.Equal(w.Body.Bytes(), packed)
	}

	w := compressed_fetch(t, a, base, file, http.MethodGet, map[string]string{"Accept-Encoding": "gzip"})
	if from_copy(w) || w.Header().Get("Content-Encoding") != "gzip" || !bytes.Equal(compressed_decode(t, "gzip", w.Body.Bytes()), source) {
		t.Errorf("a gzip-only client: encoding %q, from the copy %v", w.Header().Get("Content-Encoding"), from_copy(w))
	}

	w = compressed_fetch(t, a, base, file, http.MethodGet, map[string]string{"Accept-Encoding": "br", "Range": "bytes=0-9"})
	if from_copy(w) || w.Header().Get("Content-Encoding") != "" {
		t.Errorf("a range: encoding %q, from the copy %v, want the file's own bytes", w.Header().Get("Content-Encoding"), from_copy(w))
	}

	// The file rewritten in place: served on the fly and queued again.
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(file, later, later); err != nil {
		t.Fatal(err)
	}
	w = compressed_fetch(t, a, base, file, http.MethodGet, compressed_brotli)
	if from_copy(w) || !bytes.Equal(compressed_decode(t, w.Header().Get("Content-Encoding"), w.Body.Bytes()), source) {
		t.Error("a stale copy was sent")
	}
	if len(compressed_queue) != 1 {
		t.Errorf("a stale copy queued %d replacements, want 1", len(compressed_queue))
	}
	compressed_run(t)

	// A copy no smaller than its file is not sent.
	information, _ := os.Stat(file)
	large := bytes.Repeat([]byte{1}, len(source)+1)
	if err := os.WriteFile(path, large, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, information.ModTime(), information.ModTime()); err != nil {
		t.Fatal(err)
	}
	w = compressed_fetch(t, a, base, file, http.MethodGet, compressed_brotli)
	if bytes.Equal(w.Body.Bytes(), large) || !bytes.Equal(compressed_decode(t, w.Header().Get("Content-Encoding"), w.Body.Bytes()), source) {
		t.Error("a copy larger than its file was sent")
	}
}

// A development app's folder is its source tree: nothing is queued or written.
func TestCompressedCopyNeverForDevelopmentApps(t *testing.T) {
	base, file, source := compressed_fixture(t)
	a := &App{id: "test", development: true}
	w := compressed_fetch(t, a, base, file, http.MethodGet, compressed_brotli)
	if !bytes.Equal(compressed_decode(t, w.Header().Get("Content-Encoding"), w.Body.Bytes()), source) {
		t.Fatal("the development app's file was not served")
	}
	if len(compressed_queue) != 0 {
		t.Error("a development app's file was queued")
	}
	if _, err := os.Stat(filepath.Join(base, ".compressed")); err == nil {
		t.Error("a copy folder was made in a development app's tree")
	}
}

// Which files get copies: hashed assets of compressible types, not SVG, not
// unhashed names, not types already compressed.
func TestCompressedEligible(t *testing.T) {
	web_types()
	for file, want := range map[string]bool{
		"/v/web/dist/assets/index-AbCd1234.js":    true,
		"/v/web/dist/assets/index-AbCd1234.css":   true,
		"/v/web/dist/assets/flight-AbCd1234.wasm": true,
		"/v/web/dist/assets/nimitz-AbCd1234.glb":  true,
		"/v/web/dist/assets/icon-AbCd1234.svg":    false,
		"/v/web/dist/assets/index.js":             false,
		"/v/web/dist/assets/font-AbCd1234.woff2":  false,
		"/v/web/dist/assets/moon-AbCd1234.png":    false,
		"/v/web/dist/index-AbCd1234.js":           false,
	} {
		if got := compressed_eligible(file); got != want {
			t.Errorf("compressed_eligible(%q) = %v, want %v", file, got, want)
		}
	}
	if compressed_path("/v", "/elsewhere/a-AbCd1234.js") != "" || compressed_path("/v", "/v") != "" {
		t.Error("a path outside the version was given a copy location")
	}
}

// Models are compressed, and their type does not depend on the host.
func TestModelsAreCompressed(t *testing.T) {
	if !compressible("model/gltf-binary") {
		t.Error("model/gltf-binary is not compressible")
	}
	mime.AddExtensionType(".glb", "application/x-unregistered")
	web_types()
	if kind := mime.TypeByExtension(".glb"); kind != "model/gltf-binary" {
		t.Errorf(".glb is %q after web_types, want model/gltf-binary", kind)
	}
}

// web_action's two file branches, a directory of files and a single file, both
// send an installed app's hashed assets through the copy server.
func TestCompressedCopyThroughWebAction(t *testing.T) {
	base, file, _ := compressed_fixture(t)
	if err := compressed_make(base, file); err != nil {
		t.Fatal(err)
	}
	packed, _ := os.ReadFile(compressed_path(base, file))
	av := &AppVersion{Version: "1.0", Actions: map[string]AppAction{
		"assets": {Files: "web/dist/assets"},
		"script": {File: "web/dist/assets/index-AbCd12_-.js"},
	}}
	av.base = base
	app := &App{id: "copytest", internal: av}
	av.app = app
	gin.SetMode(gin.TestMode)
	for _, action := range []string{"assets/index-AbCd12_-.js", "script"} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/copytest/"+action, nil)
		c.Request.Header.Set("Accept-Encoding", "br")
		if !web_action(c, app, action, nil, routing_class) {
			t.Fatalf("%s: not handled", action)
		}
		if w.Code != http.StatusOK || w.Header().Get("Content-Encoding") != "br" || !bytes.Equal(w.Body.Bytes(), packed) {
			t.Errorf("%s: %d %q with %d bytes, want the %d-byte copy", action, w.Code, w.Header().Get("Content-Encoding"), w.Body.Len(), len(packed))
		}
	}
}

// The worker makes each queued copy and lets the file be asked for again.
func TestCompressedManagerMakesQueuedCopies(t *testing.T) {
	base, file, source := compressed_fixture(t)
	// A temporary left by a process that stopped mid-copy.
	if err := os.MkdirAll(compressed_temporary(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(compressed_temporary(), "compressed-left"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	queue := compressed_queue
	done := make(chan struct{})
	go func() { compressed_manager(); close(done) }()
	t.Cleanup(func() { close(queue); <-done })

	compressed_request(base, file)
	path := compressed_path(base, file)
	leftover := filepath.Join(compressed_temporary(), "compressed-left")
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, waiting := compressed_pending.Load(file)
		if _, err := os.Stat(path); err == nil && !waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no copy at %s after 10 s (still queued: %v)", path, waiting)
		}
		time.Sleep(10 * time.Millisecond)
	}
	packed, _ := os.ReadFile(path)
	if !bytes.Equal(compressed_decode(t, "br", packed), source) {
		t.Error("the worker's copy does not decode to the file")
	}
	if _, err := os.Stat(leftover); err == nil {
		t.Error("a leftover temporary was not removed when the worker started")
	}
}

// The worker puts nothing in a version directory while the code that removes
// or replaces one holds compressed_lock: an entry created while RemoveAll
// walks the tree fails the removal half way. Nor does it recreate a version
// removed meanwhile.
func TestCompressedCopyWaitsForVersionRemoval(t *testing.T) {
	base, file, _ := compressed_fixture(t)
	compressed_lock.Lock()
	locked := true
	defer func() {
		if locked {
			compressed_lock.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- compressed_make(base, file) }()
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(base, ".compressed")); err == nil {
		t.Fatal("the worker wrote into the version while the lock was held")
	}
	if err := os.RemoveAll(base); err != nil {
		t.Fatalf("removing the version: %v", err)
	}
	compressed_lock.Unlock()
	locked = false
	if err := <-done; err != nil {
		t.Errorf("a copy whose version went is not an error: %v", err)
	}
	if _, err := os.Stat(base); err == nil {
		t.Error("the worker recreated the removed version")
	}
	if leftovers, _ := filepath.Glob(filepath.Join(compressed_temporary(), "compressed-*")); len(leftovers) != 0 {
		t.Errorf("temporaries left behind: %v", leftovers)
	}
}

// Every place that removes or replaces a version directory holds the lock.
func TestVersionRemovalHoldsCompressedLock(t *testing.T) {
	data, err := os.ReadFile("apps.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	install := function_source(t, source, "app_install")
	lock := strings.Index(install, "compressed_lock.Lock()")
	if lock < 0 || lock > strings.Index(install, "os.RemoveAll(av.base)") || lock > strings.Index(install, "os.Rename(tmp, av.base)") {
		t.Error("app_install removes or replaces a version without compressed_lock")
	}
	if strings.Count(source, "os.RemoveAll(dir)") != 1 || !strings.Contains(source, "compressed_lock.Lock()\n\t\t\t\terr := os.RemoveAll(dir)") {
		t.Error("the unused-version cleanup removes a version without compressed_lock")
	}
}

// A file whose copy failed is left alone for compressed_retry, then asked for
// again.
func TestCompressedFailureBacksOff(t *testing.T) {
	base, file, _ := compressed_fixture(t)
	// data_dir/tmp a file: the copy cannot be written.
	if err := os.WriteFile(compressed_temporary(), []byte("in the way"), 0o644); err != nil {
		t.Fatal(err)
	}
	compressed_request(base, file)
	compressed_job_run(<-compressed_queue)
	if _, failed := compressed_failed.Load(file); !failed {
		t.Fatal("a failed copy was not recorded")
	}
	if _, waiting := compressed_pending.Load(file); waiting {
		t.Error("a failed copy is still marked as queued")
	}
	compressed_request(base, file)
	if len(compressed_queue) != 0 {
		t.Error("a file that just failed was queued again")
	}
	compressed_failed.Store(file, time.Now().Add(-compressed_retry-time.Minute))
	compressed_request(base, file)
	if len(compressed_queue) != 1 {
		t.Error("a file was not asked for again after the retry interval")
	}
}

// An account that may load only /login is sent there instead of the shell.
func TestWebLoginDetour(t *testing.T) {
	for _, c := range []struct {
		user *User
		want string
	}{
		{nil, ""},
		{&User{Status: "active"}, ""},
		{&User{Status: "closing"}, app_login_route("closing")},
		{&User{Status: "pending-restore"}, app_login_route("restore")},
	} {
		if got := web_login_detour(c.user); got != c.want {
			status := "nil"
			if c.user != nil {
				status = c.user.Status
			}
			t.Errorf("web_login_detour(%s) = %q, want %q", status, got, c.want)
		}
	}
	data, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(function_source(t, string(data), "web_path"), "web_login_detour(web_auth(c))") {
		t.Error("web_path does not send such accounts to /login before the shell")
	}
}

// A package's own .compressed folder is dropped at install: copies there are
// sent to brotli clients in place of the files, so only core may write them.
func TestInstallDropsPackagedCopies(t *testing.T) {
	_, _, _, id := package_install_setup(t)
	path := filepath.Join(t.TempDir(), "package.zip")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := zip.NewWriter(f)
	for name, content := range map[string]string{
		"app.json":                          `{"version": "1.0", "label": "planted", "architecture": {"engine": "starlark", "version": 4}}`,
		"web/dist/assets/index-AbCd1234.js": "console.log('inspected')",
		".compressed/web/dist/assets/index-AbCd1234.js.br": "not what was inspected",
	} {
		entry, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	av, err := app_install(id, "", path, false)
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if _, err := os.Stat(filepath.Join(av.base, "web", "dist", "assets", "index-AbCd1234.js")); err != nil {
		t.Fatalf("the package's own file was not installed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(av.base, ".compressed")); err == nil {
		t.Error("the package's .compressed folder was installed")
	}
}
