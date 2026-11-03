// Mochi server: a.write.file sends the gzipped copy kept beside a file to a
// whole-file request that accepts gzip, and the file itself otherwise.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// packed_fixture lays an APK-like file, under an extension no MIME table
// names so its type comes from core and not from sniffing, and, unless stale is set, a gzipped
// copy carrying its modification time; a stale copy carries another.
func packed_fixture(t *testing.T, app *App, owner *User, stale bool) []byte {
	t.Helper()
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })

	base := api_file_base(owner, app)
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatalf("creating %s: %v", base, err)
	}
	content := []byte(strings.Repeat("translation table entry ", 4000))
	file := filepath.Join(base, "mochi-1.0.package")
	if err := os.WriteFile(file, content, 0600); err != nil {
		t.Fatalf("writing the file: %v", err)
	}
	var packed bytes.Buffer
	writer, _ := gzip.NewWriterLevel(&packed, gzip.BestCompression)
	writer.Write(content)
	writer.Close()
	if err := os.WriteFile(file+".gz", packed.Bytes(), 0600); err != nil {
		t.Fatalf("writing the copy: %v", err)
	}
	when := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	os.Chtimes(file, when, when)
	if stale {
		when = when.Add(-time.Hour)
	}
	os.Chtimes(file+".gz", when, when)
	return content
}

func packed_request(t *testing.T, app *App, owner *User, headers map[string]string) *httptest.ResponseRecorder {
	return packed_request_typed(t, app, owner, headers, "")
}

// packed_request_typed is packed_request with a Content-Type the action set
// before serving, when typed is not empty.
func packed_request_typed(t *testing.T, app *App, owner *User, headers map[string]string, typed string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/mochi-1.0.package", nil)
	for name, value := range headers {
		c.Request.Header.Set(name, value)
	}
	if typed != "" {
		c.Writer.Header().Set("Content-Type", typed)
	}
	a := &Action{web: c, user: owner, owner: owner, active: &AppVersion{}}
	thread := &sl.Thread{}
	thread.SetLocal("owner", owner)
	thread.SetLocal("user", owner)
	thread.SetLocal("app", app)
	thread.SetLocal("action", a)
	fn := sl.NewBuiltin("write.file", a.sl_write_file)
	if _, err := a.sl_write_file(thread, fn, sl.Tuple{sl.String("mochi-1.0.package")}, nil); err != nil {
		t.Fatalf("a.write.file returned %v", err)
	}
	return recorder
}

func TestWriteFileSendsTheGzippedCopy(t *testing.T) {
	app := &App{id: "files"}
	owner := &User{UID: "packages"}
	content := packed_fixture(t, app, owner, false)

	answer := packed_request(t, app, owner, map[string]string{"Accept-Encoding": "gzip"})
	if answer.Code != 200 || answer.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("a whole-file gzip request: %d %q, want 200 gzip", answer.Code, answer.Header().Get("Content-Encoding"))
	}
	if answer.Body.Len() >= len(content)/4 {
		t.Fatalf("sent %d bytes of a %d-byte file", answer.Body.Len(), len(content))
	}
	reader, err := gzip.NewReader(answer.Body)
	if err != nil {
		t.Fatalf("the answer is not gzip: %v", err)
	}
	inflated, _ := io.ReadAll(reader)
	if !bytes.Equal(inflated, content) {
		t.Fatalf("the copy inflates to %d bytes that are not the file", len(inflated))
	}
	if !strings.Contains(answer.Header().Get("Vary"), "Accept-Encoding") {
		t.Fatalf("Vary %q does not name Accept-Encoding", answer.Header().Get("Vary"))
	}
	if strings.Contains(answer.Header().Get("Content-Type"), "gzip") {
		t.Fatalf("typed as the copy, %q, not as the file", answer.Header().Get("Content-Type"))
	}
}

func TestWriteFileSendsTheFileWithoutGzip(t *testing.T) {
	app := &App{id: "files"}
	owner := &User{UID: "packages"}
	content := packed_fixture(t, app, owner, false)

	answer := packed_request(t, app, owner, nil)
	if answer.Header().Get("Content-Encoding") != "" || !bytes.Equal(answer.Body.Bytes(), content) {
		t.Fatalf("a request without gzip got %q and %d bytes", answer.Header().Get("Content-Encoding"), answer.Body.Len())
	}
	if !strings.Contains(answer.Header().Get("Vary"), "Accept-Encoding") {
		t.Fatalf("the plain answer does not say it varies: %q", answer.Header().Get("Vary"))
	}
}

func TestWriteFileSendsARangeOfTheFile(t *testing.T) {
	app := &App{id: "files"}
	owner := &User{UID: "packages"}
	content := packed_fixture(t, app, owner, false)

	// A resumed download asks for the rest of the file it holds part of.
	answer := packed_request(t, app, owner, map[string]string{"Accept-Encoding": "gzip", "Range": "bytes=100-"})
	if answer.Code != 206 || answer.Header().Get("Content-Encoding") != "" || !bytes.Equal(answer.Body.Bytes(), content[100:]) {
		t.Fatalf("a range got %d %q and %d bytes, want 206 of the file itself", answer.Code, answer.Header().Get("Content-Encoding"), answer.Body.Len())
	}
}

func TestWriteFileIgnoresAStaleCopy(t *testing.T) {
	app := &App{id: "files"}
	owner := &User{UID: "packages"}
	content := packed_fixture(t, app, owner, true)

	answer := packed_request(t, app, owner, map[string]string{"Accept-Encoding": "gzip"})
	if answer.Header().Get("Content-Encoding") != "" || !bytes.Equal(answer.Body.Bytes(), content) {
		t.Fatalf("a copy of another version was sent: %q, %d bytes", answer.Header().Get("Content-Encoding"), answer.Body.Len())
	}
}

func TestWriteFileKeepsTheActionsType(t *testing.T) {
	app := &App{id: "files"}
	owner := &User{UID: "packages"}
	packed_fixture(t, app, owner, false)

	answer := packed_request_typed(t, app, owner, map[string]string{"Accept-Encoding": "gzip"}, "application/vnd.android.package-archive")
	if answer.Header().Get("Content-Encoding") != "gzip" || answer.Header().Get("Content-Type") != "application/vnd.android.package-archive" {
		t.Fatalf("the action's type was replaced: %q %q", answer.Header().Get("Content-Encoding"), answer.Header().Get("Content-Type"))
	}
}
