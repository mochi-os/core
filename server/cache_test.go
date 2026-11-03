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
	// The writes below go straight to disk, which a walk sees and the
	// storage figure does not: walk on every measurement.
	lifetime := storage_lifetime
	storage_lifetime = 0
	defer func() { storage_lifetime = lifetime }()

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

// cache_read_thread builds the Starlark context mochi.cache.read runs under,
// over a fresh data and cache directory.
func cache_read_thread(t *testing.T) *sl.Thread {
	t.Helper()
	tmp := t.TempDir()
	original_data, original_cache := data_dir, cache_dir
	data_dir, cache_dir = tmp, filepath.Join(tmp, "cache")
	t.Cleanup(func() { data_dir, cache_dir = original_data, original_cache })
	user := &User{UID: "reader", Username: "reader@example.com"}
	if err := os.MkdirAll(filepath.Join(data_dir, "users", user.UID), 0755); err != nil {
		t.Fatal(err)
	}
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", create_external_app("reader"))
	return thread
}

// TestCacheReadIsBoundedByDefault. An entry can be as large as object_maximum
// and the read lands whole on the heap, so a call with no maximum gets the
// default ceiling; an explicit larger one is honoured, since the caller asked.
func TestCacheReadIsBoundedByDefault(t *testing.T) {
	thread := cache_read_thread(t)
	saved := cache_read_maximum
	cache_read_maximum = 1024
	defer func() { cache_read_maximum = saved }()

	write := func(name string, size int) {
		t.Helper()
		path, err := cache_file(thread, name)
		if err != nil {
			t.Fatalf("cache_file: %v", err)
		}
		os.MkdirAll(filepath.Dir(path), 0755)
		if err := os.WriteFile(path, make([]byte, size), 0644); err != nil {
			t.Fatal(err)
		}
	}
	read := func(name string, maximum int64) sl.Value {
		t.Helper()
		var kwargs []sl.Tuple
		if maximum > 0 {
			kwargs = append(kwargs, sl.Tuple{sl.String("maximum"), sl.MakeInt64(maximum)})
		}
		value, err := api_cache_read(thread, sl.NewBuiltin("mochi.cache.read", api_cache_read), sl.Tuple{sl.String(name)}, kwargs)
		if err != nil {
			t.Fatalf("cache.read %s: %v", name, err)
		}
		return value
	}
	write("large", int(cache_read_maximum)+1)
	write("small", 16)
	if value := read("large", 0); value != sl.None {
		t.Errorf("an entry over the default ceiling was read with no maximum: %T", value)
	}
	if value := read("large", cache_read_maximum*2); value == sl.None {
		t.Error("an explicit larger maximum was not honoured")
	}
	if value := read("small", 0); value == sl.None {
		t.Error("a small entry was refused with no maximum")
	}
}

// TestCacheEvictSparesLiveTransfers. A .partial or .lock inside cache_grace is
// a transfer in flight: not counted and not a candidate, however old the
// entries around it. Past the grace it is abandoned and reclaimed, since
// nothing else ever removes one.
func TestCacheEvictSparesLiveTransfers(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget }()

	moment := time.Now()
	live := cache_test_write(t, "u", "app", "big.abc.partial", 100, moment.Add(-10*time.Minute))
	lock := cache_test_write(t, "u", "app", "log.lock", 1, moment.Add(-10*time.Minute))
	stale := cache_test_write(t, "u", "app", "old.def.partial", 100, moment.Add(-2*time.Hour))
	entry := cache_test_write(t, "u", "app", "entry", 100, moment.Add(-time.Minute))
	cache_budget = 0

	if measured := cache_measure(); measured != 200 {
		t.Errorf("cache_measure counted %d bytes, want 200: the entry and the abandoned partial", measured)
	}
	cache_evict()
	if !file_exists(live) {
		t.Error("a partial ten minutes old was evicted from under its transfer")
	}
	if !file_exists(lock) {
		t.Error("a live append lock was evicted, letting a second appender past it")
	}
	if file_exists(stale) {
		t.Error("a partial two hours old was not reclaimed")
	}
	if file_exists(entry) {
		t.Error("the finished entry survived a zero budget")
	}
}

// TestCacheUseKeepsTheWrittenTime. Using an entry - reading it, asking for it,
// serving it - marks its access time, which eviction orders by, and leaves its
// modification time the time it was written. Marked in the modification time,
// every view gave a served image a new ETag and Last-Modified, so no browser
// cache could revalidate it, and mochi.cache.age restarted on every read of a
// backoff marker.
func TestCacheUseKeepsTheWrittenTime(t *testing.T) {
	thread := cache_read_thread(t)
	written := time.Now().Add(-time.Hour).Truncate(time.Second)
	write := func(name string) string {
		t.Helper()
		path, err := cache_file(thread, name)
		if err != nil {
			t.Fatalf("cache_file: %v", err)
		}
		os.MkdirAll(filepath.Dir(path), 0755)
		if err := os.WriteFile(path, []byte("\xff\xd8\xffpicture"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, written, written); err != nil {
			t.Fatal(err)
		}
		return path
	}
	check := func(what, path string) {
		t.Helper()
		information, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !information.ModTime().Equal(written) {
			t.Errorf("THE POINT: %s moved the written time to %v", what, information.ModTime())
		}
		if since := time.Since(file_accessed(information)); since > time.Minute {
			t.Errorf("%s did not mark the entry used: last used %v ago", what, since)
		}
	}

	path := write("read")
	api_cache_read(thread, sl.NewBuiltin("mochi.cache.read", api_cache_read), sl.Tuple{sl.String("read")}, nil)
	check("mochi.cache.read", path)

	path = write("path")
	api_cache_path(thread, sl.NewBuiltin("mochi.cache.path", api_cache_path), sl.Tuple{sl.String("path")}, nil)
	check("mochi.cache.path", path)
	age, _ := api_cache_age(thread, sl.NewBuiltin("mochi.cache.age", api_cache_age), sl.Tuple{sl.String("path")}, nil)
	if seconds, _ := sl.AsInt32(age); seconds < 3500 {
		t.Errorf("mochi.cache.age reads %v seconds after a use, not the hour since it was written", age)
	}

	// Served twice, the entry answers with the same validators, and a browser
	// revalidating with them is told nothing changed.
	path = write("served.jpg")
	// gin writes the status of a response with no body only as its handler
	// ends, so the status is read from gin, not the recorder.
	status := 0
	serve := func(match string) *httptest.ResponseRecorder {
		t.Helper()
		web := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(web)
		defer func() { status = c.Writer.Status() }()
		c.Request = httptest.NewRequest("GET", "/reader/-/served", nil)
		if match != "" {
			c.Request.Header.Set("If-None-Match", match)
		}
		action := &Action{web: c}
		if _, err := action.sl_write_cache(thread, sl.NewBuiltin("a.write.cache", nil), sl.Tuple{sl.String("served.jpg")}, nil); err != nil {
			t.Fatalf("a.write.cache: %v", err)
		}
		return web
	}
	first := serve("")
	check("a.write.cache", path)
	second := serve("")
	tag := first.Header().Get("ETag")
	if tag == "" || second.Header().Get("ETag") != tag {
		t.Errorf("THE POINT: the entry's ETag moved between views: %q then %q", tag, second.Header().Get("ETag"))
	}
	if first.Header().Get("Last-Modified") != second.Header().Get("Last-Modified") {
		t.Errorf("Last-Modified moved between views: %q then %q", first.Header().Get("Last-Modified"), second.Header().Get("Last-Modified"))
	}
	if serve(tag); status != 304 {
		t.Errorf("a revalidation with the entry's own ETag answered %d, want 304", status)
	}
}

// TestCacheEvictByUse: eviction goes by when an entry was last used, not when
// it was written - an old entry still in use outlives a newer one nobody reads.
func TestCacheEvictByUse(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget }()

	now := time.Now()
	used := cache_test_write(t, "userA", "app", "used", 100, now.Add(-40*time.Minute))
	idle := cache_test_write(t, "userA", "app", "idle", 100, now.Add(-20*time.Minute))
	cache_used(used)

	cache_budget = 150
	cache_evict()
	if _, err := os.Stat(used); err != nil {
		t.Error("THE POINT: the entry in use was evicted for being written first")
	}
	if _, err := os.Stat(idle); err == nil {
		t.Error("the idle entry survived")
	}
}
