// Copyright © 2026 Mochisoft OÜ
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

// TestCacheCopyStaysInOneNamespace. Under domain routing an action runs for a
// visitor while serving the route owner's site, and the two user resolutions
// diverge: cache_base goes through db_user_for_thread and returns the OWNER,
// while api_cache_copy read its destination from t.Local("user") and got the
// VISITOR. So the copy read the owner's cached bytes and wrote them into the
// visitor's own file storage, charged against the visitor's quota.
func TestCacheCopyStaysInOneNamespace(t *testing.T) {
	tmp := t.TempDir()
	original_data, original_cache := data_dir, cache_dir
	data_dir, cache_dir = tmp, filepath.Join(tmp, "cache")
	defer func() { data_dir, cache_dir = original_data, original_cache }()

	owner := &User{UID: "owner", Username: "owner@example.com"}
	visitor := &User{UID: "visitor", Username: "visitor@example.com"}
	app := create_external_app("copier")
	for _, u := range []*User{owner, visitor} {
		if err := os.MkdirAll(filepath.Join(data_dir, "users", u.UID), 0755); err != nil {
			t.Fatalf("mkdir %s: %v", u.UID, err)
		}
	}

	web := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(web)
	c.Request = httptest.NewRequest("GET", "/copier/-/fetch", nil)

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", visitor)
	thread.SetLocal("owner", owner)
	thread.SetLocal("app", app)
	thread.SetLocal("action", &Action{
		web:    c,
		domain: &DomainInfo{route: &DomainRouteInfo{context: "hosted", owner: owner}},
	})

	// The owner's cache holds a secret; the visitor's is empty.
	source, err := cache_file(thread, "secret.txt")
	if err != nil {
		t.Fatalf("cache_file: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(source), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(source, []byte("owner's bytes"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !filepath.IsAbs(source) || !path_has(source, "owner") {
		t.Fatalf("the source resolved to %q, which is not the owner's cache - the premise of this test is wrong", source)
	}

	resolved, err := db_user_for_thread(thread)
	if err != nil {
		t.Fatalf("db_user_for_thread: %v", err)
	}
	if resolved.UID != owner.UID {
		t.Fatalf("db_user_for_thread returned %q, want the owner - domain routing is not active in this thread", resolved.UID)
	}

	// The destination must land in the same account the source came from.
	base_used := api_file_base(resolved, app)
	if path_has(base_used, visitor.UID) {
		t.Errorf("destination base %q is the visitor's storage", base_used)
	}

	if _, err := api_cache_copy(thread, sl.NewBuiltin("mochi.cache.copy", api_cache_copy),
		nil, []sl.Tuple{
			{sl.String("name"), sl.String("secret.txt")},
			{sl.String("destination"), sl.String("copied.txt")},
		}); err != nil {
		t.Fatalf("api_cache_copy: %v", err)
	}

	if _, err := os.Stat(filepath.Join(api_file_base(visitor, app), "copied.txt")); err == nil {
		t.Error("the owner's cached bytes were written into the visitor's file storage")
	}
	if _, err := os.Stat(filepath.Join(api_file_base(owner, app), "copied.txt")); err != nil {
		t.Errorf("the copy did not land in the owner's storage either: %v", err)
	}
}

// path_has reports whether any element of path is exactly element.
func path_has(path, element string) bool {
	for path != "/" && path != "." && path != "" {
		if filepath.Base(path) == element {
			return true
		}
		path = filepath.Dir(path)
	}
	return false
}
