// Mochi server: a.upload writes through os.Root, as every other write to app
// storage does.
//
// It was the last one using gin's SaveUploadedFile, which is MkdirAll plus
// Create - both symlink-following. The path string is validated, but a symlink
// is not in the string, so a link left in the app's file directory redirected
// the write outside it. a.write.file records the same reasoning for the read
// side, in the same file.
//
// It also read a.user, the RAW requester, where every mochi.file.* call reads
// the thread's user. Those differ on an anonymous request to a public action:
// the auth gate admits one (it refuses only when user AND owner are absent),
// a.user is nil, and user_storage_dir dereferenced it.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"mime/multipart"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// upload_test_action builds an action carrying a one-file multipart body, plus
// the Starlark thread it would run on. requester is what lands in a.user, so a
// test can pass nil for the anonymous-public case; effective is what web.go
// puts on the thread.
func upload_test_action(t *testing.T, requester, effective *User, app *App, field, name, body string) (*Action, *sl.Thread) {
	t.Helper()

	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	part, err := writer.CreateFormFile(field, name)
	if err != nil {
		t.Fatalf("multipart: %v", err)
	}
	if _, err := part.Write([]byte(body)); err != nil {
		t.Fatalf("multipart write: %v", err)
	}
	writer.Close()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/app/-/upload", &buffer)
	c.Request.Header.Set("Content-Type", writer.FormDataContentType())

	thread := &sl.Thread{}
	thread.SetLocal("user", effective)
	thread.SetLocal("app", app)
	return &Action{web: c, user: requester, app: app}, thread
}

func upload_test_setup(t *testing.T) (*User, *App, func()) {
	t.Helper()
	cleanup := setup_replication_test(t) // sets data_dir under a temp directory
	user := &User{UID: "u-upload"}
	// The account's own directory, which signup creates and the storage
	// measurement needs. Not the app's files directory - a first upload has to
	// find that missing.
	if err := os.MkdirAll(user_storage_dir(user), 0755); err != nil {
		t.Fatalf("mkdir user directory: %v", err)
	}
	return user, &App{id: "uploader"}, cleanup
}

func upload_test_call(a *Action, thread *sl.Thread, field, file string) (sl.Value, error) {
	fn := sl.NewBuiltin("a.upload", nil)
	return a.sl_upload(thread, fn, nil, []sl.Tuple{
		{sl.String("field"), sl.String(field)},
		{sl.String("file"), sl.String(file)},
	})
}

// TestUploadRefusesToFollowASymlink is the finding. A link inside the app's
// file directory must not redirect the write to whatever it points at.
func TestUploadRefusesToFollowASymlink(t *testing.T) {
	user, app, cleanup := upload_test_setup(t)
	defer cleanup()

	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("mkdir base: %v", err)
	}
	// Something outside the app's directory that must survive untouched - the
	// shape of the real case is a link to the server's own databases.
	outside := filepath.Join(data_dir, "outside.txt")
	if err := os.WriteFile(outside, []byte("original"), 0644); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "link.txt")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	a, thread := upload_test_action(t, user, user, app, "f", "link.txt", "overwritten")
	_, err := upload_test_call(a, thread, "f", "link.txt")
	if err == nil {
		t.Error("a write through a symlink was accepted")
	}

	after, readErr := os.ReadFile(outside)
	if readErr != nil {
		t.Fatalf("read outside: %v", readErr)
	}
	if string(after) != "original" {
		t.Errorf("the file outside the app directory was overwritten: %q", after)
	}
}

// TestUploadAnonymousPublicActionDoesNotPanic. a.user is nil for an anonymous
// caller to a public action; the thread's user is the owner web.go resolved,
// which is what the upload must use.
func TestUploadAnonymousPublicActionDoesNotPanic(t *testing.T) {
	owner, app, cleanup := upload_test_setup(t)
	defer cleanup()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("anonymous public upload panicked: %v", r)
		}
	}()

	a, thread := upload_test_action(t, nil, owner, app, "f", "anon.txt", "hello")
	size, err := upload_test_call(a, thread, "f", "anon.txt")
	if err != nil {
		t.Fatalf("anonymous public upload failed: %v", err)
	}
	if size != sl.MakeInt64(5) {
		t.Errorf("returned size %v, want 5", size)
	}
	// And it landed in the owner's directory, which is where a later read looks.
	written, readErr := os.ReadFile(filepath.Join(api_file_base(owner, app), "anon.txt"))
	if readErr != nil {
		t.Fatalf("upload did not land in the owner's directory: %v", readErr)
	}
	if string(written) != "hello" {
		t.Errorf("stored %q, want %q", written, "hello")
	}
}

// TestUploadCreatesTheBaseDirectory. os.OpenRoot will not create it, and the
// first upload for a user and app arrives before anything else has - so
// swapping SaveUploadedFile's MkdirAll for a root had to keep the mkdir.
func TestUploadCreatesTheBaseDirectory(t *testing.T) {
	user, app, cleanup := upload_test_setup(t)
	defer cleanup()

	base := api_file_base(user, app)
	if _, err := os.Stat(base); err == nil {
		t.Fatal("base directory already exists; the test is not exercising a first upload")
	}

	a, thread := upload_test_action(t, user, user, app, "f", "first.txt", "data")
	if _, err := upload_test_call(a, thread, "f", "first.txt"); err != nil {
		t.Fatalf("first upload for a user and app failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "first.txt")); err != nil {
		t.Errorf("first upload did not land: %v", err)
	}
}

// TestUploadCreatesNestedDirectories. root_write_file makes parents through
// the root; SaveUploadedFile's MkdirAll did it outside one.
func TestUploadCreatesNestedDirectories(t *testing.T) {
	user, app, cleanup := upload_test_setup(t)
	defer cleanup()

	a, thread := upload_test_action(t, user, user, app, "f", "nested.txt", "deep")
	if _, err := upload_test_call(a, thread, "f", "a/b/nested.txt"); err != nil {
		t.Fatalf("nested upload failed: %v", err)
	}
	written, err := os.ReadFile(filepath.Join(api_file_base(user, app), "a/b/nested.txt"))
	if err != nil {
		t.Fatalf("nested upload did not land: %v", err)
	}
	if string(written) != "deep" {
		t.Errorf("stored %q, want %q", written, "deep")
	}
}

// TestUploadLandsWhereFileReadLooks. The write and the read must resolve the
// same account, or an app stores something it cannot then find.
func TestUploadLandsWhereFileReadLooks(t *testing.T) {
	user, app, cleanup := upload_test_setup(t)
	defer cleanup()

	a, thread := upload_test_action(t, user, user, app, "f", "shared.txt", "round trip")
	if _, err := upload_test_call(a, thread, "f", "shared.txt"); err != nil {
		t.Fatalf("upload failed: %v", err)
	}

	read := sl.NewBuiltin("mochi.file.read", api_file_read)
	value, err := api_file_read(thread, read, sl.Tuple{sl.String("shared.txt")}, nil)
	if err != nil {
		t.Fatalf("mochi.file.read could not find the upload: %v", err)
	}
	if got := string(value.(sl.Bytes)); got != "round trip" {
		t.Errorf("read back %q, want %q", got, "round trip")
	}
}
