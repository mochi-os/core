// Mochi server: a closed server refuses a restore before reading the bundle.
//
// web_auth_restore parsed the multipart body - up to 2 GiB + 64 MiB - and only
// then asked whether signups were enabled at all. The check takes no argument
// and reads one indexed row from settings.db, so every byte of that spool was
// read for a request the server was always going to refuse. ParseMultipartForm
// spools to os.TempDir(), which is a tmpfs on a systemd host (32G on yuzu), so
// the cost was resident memory rather than disk, and the route's rate limit
// bounds requests per IP but not how many are in flight at once.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// counting_body reports how much of the request body the handler consumed.
// Zero is the whole point: a refusal that has already read the bundle has not
// saved anything.
type counting_body struct {
	reader io.Reader
	read   int64
}

func (b *counting_body) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += int64(n)
	return n, err
}

func (b *counting_body) Close() error { return nil }

// restore_request builds a well-formed multipart restore POST and returns the
// gin context, the recorder, and the body counter.
func restore_request(t *testing.T) (*gin.Context, *httptest.ResponseRecorder, *counting_body) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	var raw bytes.Buffer
	form := multipart.NewWriter(&raw)
	_ = form.WriteField("email", "restored@example.com")
	_ = form.WriteField("passphrase", "correct horse battery staple")
	_ = form.WriteField("code", "123456")
	part, err := form.CreateFormFile("bundle", "bundle.zip")
	if err != nil {
		t.Fatalf("building the multipart body: %v", err)
	}
	// Small enough that the Content-Length guard cannot be what refuses it -
	// otherwise the test would pass without the signup check running at all.
	part.Write(bytes.Repeat([]byte("bundle bytes "), 4096))
	form.Close()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/_/auth/restore", nil)
	c.Request.Header.Set("Content-Type", form.FormDataContentType())
	c.Request.ContentLength = int64(raw.Len())
	body := &counting_body{reader: bytes.NewReader(raw.Bytes())}
	c.Request.Body = body
	return c, recorder, body
}

// TestClosedSignupRefusesBeforeReadingTheBody is the defect.
func TestClosedSignupRefusesBeforeReadingTheBody(t *testing.T) {
	defer create_web_test_env(t)()
	load_core_labels()
	setting_set("signup_enabled", "false")

	c, _, body := restore_request(t)
	web_auth_restore(c)

	// gin buffers the status until the engine finishes, so the recorder still
	// reads 200 here; c.Writer is what the handler actually set.
	if got := c.Writer.Status(); got != 403 {
		t.Fatalf("a restore against a closed server answered %d, want 403", got)
	}
	if body.read != 0 {
		t.Errorf("the handler read %d bytes of a bundle it was always going to refuse; on a tmpfs /tmp that is resident memory, and the route's per-IP rate limit does not bound how many such requests are in flight", body.read)
	}
}

// TestOpenSignupStillReadsTheBody keeps the refusal from becoming
// unconditional, and proves the zero above is the check firing rather than the
// handler having stopped parsing altogether.
func TestOpenSignupStillReadsTheBody(t *testing.T) {
	defer create_web_test_env(t)()
	load_core_labels()
	setting_set("signup_enabled", "true")

	c, _, body := restore_request(t)
	web_auth_restore(c)

	if got := c.Writer.Status(); got == 403 {
		t.Fatal("an open server refused the restore as signup_disabled")
	}
	if body.read == 0 {
		t.Error("the handler read nothing with signups enabled, so it never reached the multipart parse and the test above proves nothing")
	}
}

// TestSignupCheckPrecedesTheBodyGuards pins the ORDER in source. The
// behavioural test above would still pass if the check moved to sit between
// the Content-Length guard and the parse, which reads a body on any request
// whose declared length happens to be over the cap.
func TestSignupCheckPrecedesTheBodyGuards(t *testing.T) {
	body := function_body(t, "auth_restore.go", "func web_auth_restore(")

	signup := strings.Index(body, "setting_signup_enabled()")
	if signup < 0 {
		t.Fatal("web_auth_restore no longer checks whether signups are enabled")
	}
	for _, later := range []string{"ContentLength", "MaxBytesReader", "ParseMultipartForm"} {
		at := strings.Index(body, later)
		if at < 0 {
			t.Errorf("web_auth_restore no longer uses %s; the upload bound is gone", later)
			continue
		}
		if signup > at {
			t.Errorf("the signup check runs after %s, so a closed server still handles the body", later)
		}
	}
}

// TestSignupCheckAppearsOnce: the hoist must move the check, not duplicate it.
// A leftover copy is harmless at runtime and exactly the kind of thing that
// survives review, then confuses the next reader about which one is load-bearing.
func TestSignupCheckAppearsOnce(t *testing.T) {
	body := function_body(t, "auth_restore.go", "func web_auth_restore(")
	if n := strings.Count(body, "setting_signup_enabled()"); n != 1 {
		t.Errorf("web_auth_restore checks setting_signup_enabled %d times, want 1", n)
	}
}
