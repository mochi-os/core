// Mochi server: a.write.file(path, storage=True) reads the caller's directory.
//
// a.write.file resolved the directory with principal_owner - the owner of the
// addressed entity - while every other storage primitive (mochi.db, a.upload,
// mochi.cache.*, mochi.image.variant) resolves with principal_storage, the
// caller. A file a non-owner uploaded therefore lives in one account's
// directory and is served from another's, so it is found only while the owner
// happens to hold a copy of it.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// write_file_fixture gives two accounts a file of the same name and different
// contents in the same app's directory, so which account was read is decidable
// from the response body alone.
func write_file_fixture(t *testing.T, app *App, owner, caller *User, wrote map[*User]string) {
	t.Helper()
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })

	for user, content := range wrote {
		base := api_file_base(user, app)
		if err := os.MkdirAll(base, 0700); err != nil {
			t.Fatalf("creating %s: %v", base, err)
		}
		if err := os.WriteFile(filepath.Join(base, "file.txt"), []byte(content), 0600); err != nil {
			t.Fatalf("writing the fixture for %s: %v", user.UID, err)
		}
	}
}

// write_file calls a.write.file("file.txt") and returns the response.
func write_file(t *testing.T, owner, caller *User, app *App, storage bool) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/", nil)

	a := &Action{web: c, user: caller, owner: owner, active: &AppVersion{}}
	thread := &sl.Thread{}
	thread.SetLocal("owner", owner)
	thread.SetLocal("user", caller)
	thread.SetLocal("app", app)
	thread.SetLocal("action", a)

	kwargs := sl.Tuple{}
	if storage {
		kwargs = sl.Tuple{sl.String("storage"), sl.True}
	}
	var pairs []sl.Tuple
	if len(kwargs) > 0 {
		pairs = []sl.Tuple{kwargs}
	}
	fn := sl.NewBuiltin("write.file", a.sl_write_file)
	if _, err := a.sl_write_file(thread, fn, sl.Tuple{sl.String("file.txt")}, pairs); err != nil {
		t.Fatalf("a.write.file returned %v", err)
	}
	return recorder
}

// TestWriteFileStorageReadsTheCaller is the fix. The caller is not the entity
// owner and only the caller holds the file; without storage=True the read goes
// to the owner's directory and 404s.
func TestWriteFileStorageReadsTheCaller(t *testing.T) {
	app := &App{id: "attachmenttest"}
	owner := &User{UID: "owneruser"}
	caller := &User{UID: "calleruser"}
	write_file_fixture(t, app, owner, caller, map[*User]string{caller: "the caller's bytes"})

	recorder := write_file(t, owner, caller, app, true)
	if recorder.Code != 200 {
		t.Fatalf("a.write.file(storage=True) answered %d; the caller holds the file", recorder.Code)
	}
	if body := recorder.Body.String(); body != "the caller's bytes" {
		t.Errorf("served %q, want the caller's bytes", body)
	}
}

// TestWriteFileWithoutStorageReadsTheOwner. The default must not move: a
// hosted domain and every published-asset caller depend on the owner's
// directory being the one read.
func TestWriteFileWithoutStorageReadsTheOwner(t *testing.T) {
	app := &App{id: "attachmenttest"}
	owner := &User{UID: "owneruser"}
	caller := &User{UID: "calleruser"}
	write_file_fixture(t, app, owner, caller, map[*User]string{
		owner:  "the owner's bytes",
		caller: "the caller's bytes",
	})

	recorder := write_file(t, owner, caller, app, false)
	if recorder.Code != 200 {
		t.Fatalf("a.write.file answered %d; the owner holds the file", recorder.Code)
	}
	if body := recorder.Body.String(); body != "the owner's bytes" {
		t.Errorf("served %q, want the owner's bytes - the default resolution moved", body)
	}
}

// TestWriteFileStorageStillPrefersTheCallersCopy. Both accounts hold a file of
// the same name; storage=True must read the caller's, not fall through.
func TestWriteFileStorageStillPrefersTheCallersCopy(t *testing.T) {
	app := &App{id: "attachmenttest"}
	owner := &User{UID: "owneruser"}
	caller := &User{UID: "calleruser"}
	write_file_fixture(t, app, owner, caller, map[*User]string{
		owner:  "the owner's bytes",
		caller: "the caller's bytes",
	})

	recorder := write_file(t, owner, caller, app, true)
	if body := recorder.Body.String(); body != "the caller's bytes" {
		t.Errorf("served %q, want the caller's bytes", body)
	}
}

// TestWriteFileStorageIsTheOwnerWhenAnonymous. principal_storage falls back to
// the owner when there is no caller, so a public action serving an anonymous
// visitor keeps reading the owner's directory whichever way it asks.
func TestWriteFileStorageIsTheOwnerWhenAnonymous(t *testing.T) {
	app := &App{id: "attachmenttest"}
	owner := &User{UID: "owneruser"}
	write_file_fixture(t, app, owner, nil, map[*User]string{owner: "the owner's bytes"})

	recorder := write_file(t, owner, nil, app, true)
	if recorder.Code != 200 {
		t.Fatalf("a.write.file(storage=True) answered %d for an anonymous caller", recorder.Code)
	}
	if body := recorder.Body.String(); body != "the owner's bytes" {
		t.Errorf("served %q, want the owner's bytes", body)
	}
}

// TestWriteFileDomainRouteIgnoresStorage. A hosted domain publishes one
// account's files to every visitor alike; storage=True must not let a
// signed-in visitor redirect that read to their own directory.
func TestWriteFileDomainRouteIgnoresStorage(t *testing.T) {
	app := &App{id: "attachmenttest"}
	owner := &User{UID: "owneruser"}
	caller := &User{UID: "calleruser"}
	write_file_fixture(t, app, owner, caller, map[*User]string{
		owner:  "the owner's bytes",
		caller: "the caller's bytes",
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/", nil)
	a := &Action{
		web: c, user: caller, owner: owner, active: &AppVersion{},
		domain: &DomainInfo{route: &DomainRouteInfo{owner: owner}},
	}
	thread := &sl.Thread{}
	thread.SetLocal("owner", owner)
	thread.SetLocal("user", caller)
	thread.SetLocal("app", app)
	thread.SetLocal("action", a)

	fn := sl.NewBuiltin("write.file", a.sl_write_file)
	pairs := []sl.Tuple{{sl.String("storage"), sl.True}}
	if _, err := a.sl_write_file(thread, fn, sl.Tuple{sl.String("file.txt")}, pairs); err != nil {
		t.Fatalf("a.write.file returned %v", err)
	}
	if body := recorder.Body.String(); body != "the owner's bytes" {
		t.Errorf("served %q on a domain route, want the owner's bytes", body)
	}
}
