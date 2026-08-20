// Mochi server: Cache tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
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
