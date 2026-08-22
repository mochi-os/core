// Mochi server: Cache tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// cache_test_write writes a file under cache_dir/apps/<user>/<app>/<name> with a
// given size and modification time, so eviction order (LRU) is deterministic.
func cache_test_write(t *testing.T, user, app, name string, size int, modified time.Time) string {
	t.Helper()
	dir := filepath.Join(cache_dir, "apps", user, app)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, make([]byte, size), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Chtimes(path, modified, modified); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	return path
}

func cache_test_total(t *testing.T) int64 {
	t.Helper()
	var total int64
	filepath.Walk(filepath.Join(cache_dir, "apps"), func(_ string, information os.FileInfo, err error) error {
		if err == nil && !information.IsDir() {
			total += information.Size()
		}
		return nil
	})
	return total
}

// TestCacheEvictLRU: over budget, the least recently used entries go first, and
// the sweep stops once the total is within budget.
func TestCacheEvictLRU(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget }()

	now := time.Now()
	// One user, four 100-byte entries at descending ages. Budget 250 bytes.
	oldest := cache_test_write(t, "userA", "app", "oldest", 100, now.Add(-40*time.Minute))
	older := cache_test_write(t, "userA", "app", "older", 100, now.Add(-30*time.Minute))
	newer := cache_test_write(t, "userA", "app", "newer", 100, now.Add(-20*time.Minute))
	newest := cache_test_write(t, "userA", "app", "newest", 100, now.Add(-10*time.Minute))
	cache_budget = 250

	cache_evict()

	if file_exists(oldest) {
		t.Error("oldest entry should have been evicted")
	}
	if file_exists(older) {
		t.Error("second-oldest entry should have been evicted")
	}
	if !file_exists(newer) || !file_exists(newest) {
		t.Error("the two most recent entries should survive")
	}
	if total := cache_test_total(t); total > cache_budget {
		t.Errorf("total %d still exceeds budget %d after eviction", total, cache_budget)
	}
}

// TestCacheEvictFairShare: when two users share the cache, the user over their
// fair share of the budget is evicted from first, protecting the smaller user.
func TestCacheEvictFairShare(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget }()

	now := time.Now()
	// Hog holds 400 bytes; the small user holds 100 - and its entry is the
	// OLDEST, so pure global LRU would evict it first. Fair share must protect
	// it because the hog is the one over budget/2.
	small := cache_test_write(t, "small", "app", "keep", 100, now.Add(-60*time.Minute))
	cache_test_write(t, "hog", "app", "h1", 100, now.Add(-50*time.Minute))
	cache_test_write(t, "hog", "app", "h2", 100, now.Add(-40*time.Minute))
	cache_test_write(t, "hog", "app", "h3", 100, now.Add(-30*time.Minute))
	cache_test_write(t, "hog", "app", "h4", 100, now.Add(-20*time.Minute))
	cache_budget = 300 // fair share = 150 per user; hog (400) is over, small (100) is not

	cache_evict()

	if !file_exists(small) {
		t.Error("the under-share user's entry must be protected, even though it is the oldest")
	}
	if total := cache_test_total(t); total > cache_budget {
		t.Errorf("total %d still exceeds budget %d", total, cache_budget)
	}
}

// TestCacheEvictAggressive: budget zero (the aggressive-eviction development
// mode) removes every entry on a sweep.
func TestCacheEvictAggressive(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget }()

	now := time.Now()
	cache_test_write(t, "u", "app", "a", 100, now)
	cache_test_write(t, "u", "app", "b", 100, now)
	cache_budget = 0

	cache_evict()

	if total := cache_test_total(t); total != 0 {
		t.Errorf("aggressive mode should evict everything, %d bytes remain", total)
	}
}

// TestCacheEvictDisabled: an unconfigured budget (negative) evicts nothing.
func TestCacheEvictDisabled(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget }()

	now := time.Now()
	kept := cache_test_write(t, "u", "app", "a", 1000, now)
	cache_budget = -1

	cache_evict()

	if !file_exists(kept) {
		t.Error("eviction must not run when the budget is unconfigured")
	}
}

// TestCacheQuotaExempt: cache storage lives outside the per-user storage tree,
// so a cache write does not count against the user's quota while a file write
// does. This is the core promise that lets cached copies be quota-free.
func TestCacheQuotaExempt(t *testing.T) {
	orig_data, orig_cache := data_dir, cache_dir
	data_dir = t.TempDir()
	cache_dir = t.TempDir()
	defer func() { data_dir, cache_dir = orig_data, orig_cache }()

	user := &User{UID: "quotauid"}
	os.MkdirAll(user_storage_dir(user), 0755)

	before, err := user_storage_remaining(user)
	if err != nil {
		t.Fatalf("storage remaining: %v", err)
	}

	// A cache write must not move the needle.
	cachedir := filepath.Join(cache_dir, "apps", user.UID, "app")
	os.MkdirAll(cachedir, 0755)
	os.WriteFile(filepath.Join(cachedir, "copy"), make([]byte, 100000), 0644)

	after_cache, err := user_storage_remaining(user)
	if err != nil {
		t.Fatalf("storage remaining: %v", err)
	}
	if after_cache != before {
		t.Errorf("cache write changed quota: before %d, after %d", before, after_cache)
	}

	// A file write in the user's storage tree must reduce the remaining quota.
	userdir := filepath.Join(user_storage_dir(user), "app", "files")
	os.MkdirAll(userdir, 0755)
	os.WriteFile(filepath.Join(userdir, "own"), make([]byte, 100000), 0644)

	after_file, err := user_storage_remaining(user)
	if err != nil {
		t.Fatalf("storage remaining: %v", err)
	}
	if after_file >= before {
		t.Errorf("file write should have reduced quota: before %d, after %d", before, after_file)
	}
}

// TestCacheCopyStaysInOneNamespace. Under domain routing the two user
// resolutions diverge - cache_base takes the route OWNER, t.Local("user") the
// VISITOR - so the copy read the owner's bytes into the visitor's storage.
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

	resolved, err := principal_storage(thread)
	if err != nil {
		t.Fatalf("principal_storage: %v", err)
	}
	if resolved.UID != owner.UID {
		t.Fatalf("principal_storage returned %q, want the owner - domain routing is not active in this thread", resolved.UID)
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
