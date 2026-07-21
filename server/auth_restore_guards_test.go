// Mochi server: backup-restore unzip guards (#21).
//
// The signup-via-restore bundle is uploaded by an unauthenticated caller, so
// restore_unzip must reject path traversal (zip-slip) and bound decompression
// so a zip-bomb can't exhaust the disk. The byte cap is the per-user storage
// quota for an ordinary restore (admins get a generous ceiling, set by the
// caller). Cross-user containment is separately ensured by the destination
// using a fresh server-generated uid, never the bundle's.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRestoreUnzipGuards(t *testing.T) {
	makeZip := func(entries map[string]int) string {
		zp := filepath.Join(t.TempDir(), "b.zip")
		f, err := os.Create(zp)
		if err != nil {
			t.Fatal(err)
		}
		zw := zip.NewWriter(f)
		for name, size := range entries {
			w, err := zw.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(bytes.Repeat([]byte("a"), size)); err != nil {
				t.Fatal(err)
			}
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		f.Close()
		return zp
	}

	// Path traversal (zip-slip) is rejected.
	if _, err := restore_unzip(makeZip(map[string]int{"top/ok.txt": 1, "../escape.txt": 1}), t.TempDir(), 1<<20); err == nil {
		t.Error("traversal entry (../escape.txt) must be rejected")
	}

	// A bundle decompressing past maxBytes is rejected (zip-bomb guard).
	if _, err := restore_unzip(makeZip(map[string]int{"top/big.bin": 4096}), t.TempDir(), 1024); err == nil {
		t.Error("bundle exceeding maxBytes must be rejected")
	}

	// Within the cap it extracts cleanly.
	if _, err := restore_unzip(makeZip(map[string]int{"top/small.bin": 256}), t.TempDir(), 1024); err != nil {
		t.Errorf("within-cap bundle must extract: %v", err)
	}
}

// The public restore upload is capped before multipart parsing (the route is
// exempt from the global body limit): an oversized declared Content-Length is
// rejected outright, and a body that exceeds the cap without declaring a
// length is cut off by MaxBytesReader during the parse. Both answer 413.
func TestRestoreUploadCap(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	db := db_open("db/users.db")
	db.exec("insert into users (uid, username) values ('u1', 'first@example.com')")

	// The upload cap is the operator setting (not the storage quota); a real
	// user exists so the ordinary-user cap applies, not the admin ceiling.
	settings := db_open("db/settings.db")
	settings.exec("create table settings (name text primary key, value text not null)")
	setting_set("restore_upload_maximum", "1048576") // 1 MiB
	limit := int64(1<<20) + 64*1024*1024

	form := func(email string, payload int) (*bytes.Buffer, string) {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		if email != "" {
			writer.WriteField("email", email)
		}
		writer.WriteField("passphrase", "pp")
		part, err := writer.CreateFormFile("bundle", "bundle.zip")
		if err != nil {
			t.Fatal(err)
		}
		part.Write(bytes.Repeat([]byte("a"), payload))
		writer.Close()
		return body, writer.FormDataContentType()
	}

	// Declared Content-Length past the cap: rejected before any read.
	body, content := form("new@example.com", 16)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/restore", body)
	c.Request.Header.Set("Content-Type", content)
	c.Request.ContentLength = limit + 1
	web_auth_restore(c)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized Content-Length: got %d, want 413", w.Code)
	}

	// Undeclared length with an oversized body: cut off during the parse.
	body, content = form("new@example.com", int(limit)+(1<<20))
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/restore", struct{ io.Reader }{body})
	c.Request.Header.Set("Content-Type", content)
	web_auth_restore(c)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized undeclared-length body: got %d, want 413", w.Code)
	}

	// A small upload parses cleanly through the cap and fails on its own
	// validation (missing email), not on size.
	body, content = form("", 64)
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/auth/restore", body)
	c.Request.Header.Set("Content-Type", content)
	web_auth_restore(c)
	if w.Code != http.StatusBadRequest {
		t.Errorf("small body must pass the cap: got %d, want 400", w.Code)
	}
}

// An empty (not-yet-bootstrapped) server keeps the ordinary 2 GiB public cap by
// default — it does NOT hand the unauthenticated first-user path a large spool
// allowance — and only lifts it when the operator sets restore_upload_bootstrap.
func TestRestoreUploadCapBootstrap(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	// No users inserted: web_auth_restore takes the administrator/bootstrap path.
	settings := db_open("db/settings.db")
	settings.exec("create table settings (name text primary key, value text not null)")

	oversized := func() *gin.Context {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		writer.WriteField("email", "boot@example.com")
		writer.Close()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/_/auth/restore", body)
		c.Request.Header.Set("Content-Type", writer.FormDataContentType())
		// Just over the 2 GiB default + framing headroom.
		c.Request.ContentLength = 3*1024*1024*1024 + 1
		return c
	}

	// Default: the bootstrap path is capped at the ordinary 2 GiB, so 3 GiB is
	// rejected before any spool — no fresh-server 50 GiB allowance.
	c := oversized()
	web_auth_restore(c)
	if c.Writer.Status() != http.StatusRequestEntityTooLarge {
		t.Errorf("empty-server default: got %d, want 413 (no large bootstrap allowance)", c.Writer.Status())
	}

	// Operator opts in to a larger bootstrap: the same 3 GiB is now admitted
	// past the size gate (and fails later on its own validation, not on size).
	setting_set("restore_upload_bootstrap", "8589934592") // 8 GiB
	c = oversized()
	web_auth_restore(c)
	if c.Writer.Status() == http.StatusRequestEntityTooLarge {
		t.Error("with restore_upload_bootstrap set, the larger bootstrap upload must pass the size gate")
	}
}
