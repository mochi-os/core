// Mochi server: Cache tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

// TestCacheEvictSparesLiveTransfers. A .partial inside cache_grace is a
// transfer in flight: not counted and not a candidate, however old the entries
// around it. Past the grace it is abandoned and reclaimed, since nothing else
// ever removes one.
func TestCacheEvictSparesLiveTransfers(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget }()

	moment := time.Now()
	live := cache_test_write(t, "u", "app", "big.abc.partial", 100, moment.Add(-10*time.Minute))
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

// TestCacheServesAnSVGRevalidation: a kept SVG is sanitised rather than served
// by http.ServeContent, and is answered 304 on a revalidation all the same -
// with its own validator, or with the one the action set from the far end's.
func TestCacheServesAnSVGRevalidation(t *testing.T) {
	thread := cache_read_thread(t)
	path, err := cache_file(thread, "face.svg")
	if err != nil {
		t.Fatalf("cache_file: %v", err)
	}
	os.MkdirAll(filepath.Dir(path), 0755)
	if err := os.WriteFile(path, []byte(`<svg xmlns="http://www.w3.org/2000/svg"></svg>`), 0644); err != nil {
		t.Fatal(err)
	}
	serve := func(set, match string) (*httptest.ResponseRecorder, int) {
		t.Helper()
		web := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(web)
		c.Request = httptest.NewRequest("GET", "/reader/-/face", nil)
		if match != "" {
			c.Request.Header.Set("If-None-Match", match)
		}
		action := &Action{web: c}
		if set != "" {
			action.web.Header("ETag", set)
		}
		if _, err := action.sl_write_cache(thread, sl.NewBuiltin("a.write.cache", nil), sl.Tuple{sl.String("face.svg")}, nil); err != nil {
			t.Fatalf("a.write.cache: %v", err)
		}
		return web, c.Writer.Status()
	}
	first, status := serve("", "")
	tag := first.Header().Get("ETag")
	if status != 200 || tag == "" || !strings.Contains(first.Body.String(), "<svg") {
		t.Fatalf("CONTROL: the SVG answered %d with ETag %q and %d bytes", status, tag, first.Body.Len())
	}
	if revalidated, status := serve("", tag); status != 304 || revalidated.Body.Len() != 0 {
		t.Errorf("THE POINT: a revalidation with the entry's ETag answered %d with %d bytes", status, revalidated.Body.Len())
	}
	if revalidated, status := serve(`"1730000000-5"`, `"1730000000-5"`); status != 304 || revalidated.Body.Len() != 0 {
		t.Errorf("THE POINT: a revalidation with the far end's ETag answered %d with %d bytes", status, revalidated.Body.Len())
	}
	if _, status := serve(`"1730000000-5"`, `"1730000000-6"`); status != 200 {
		t.Errorf("CONTROL: a validator that does not match answered %d", status)
	}
}

// TestWriteStreamKeepsOnARevalidation: a relay asked to keep the body and met
// by a revalidation it answers 304 - the client holds the bytes - keeps them all
// the same, so the next revalidation is answered from the entry rather than by
// the far end; a body short of its declared size is not kept.
func TestWriteStreamKeepsOnARevalidation(t *testing.T) {
	thread := cache_read_thread(t)
	for _, c := range []struct {
		name string
		body string
		kept bool
	}{
		{"whole", "AVATAR", true},
		{"short of its declared size", "AVA", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			stream_bytes_reset()
			web := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(web)
			ctx.Request = httptest.NewRequest("GET", "/people/entity/-/avatar", nil)
			ctx.Request.Header.Set("If-None-Match", `"1730000000-6"`)
			action := &Action{web: ctx}
			action.web.Header("ETag", `"1730000000-6"`)
			entry := "revalidated-" + strings.ReplaceAll(c.name, " ", "-")
			kwargs := []sl.Tuple{{sl.String("cache"), sl.String(entry)}, {sl.String("size"), sl.MakeInt(6)}}
			reader := &write_stream_closer{Reader: strings.NewReader(c.body)}
			if _, err := action.sl_write_stream(thread, sl.NewBuiltin("write.stream", nil), sl.Tuple{&Stream{reader: reader}}, kwargs); err != nil {
				t.Fatalf("a.write.stream: %v", err)
			}
			if status := ctx.Writer.Status(); status != 304 || web.Body.Len() != 0 {
				t.Fatalf("CONTROL: the revalidation answered %d with %d bytes", status, web.Body.Len())
			}
			path, err := cache_file(thread, entry)
			if err != nil {
				t.Fatalf("cache_file: %v", err)
			}
			data, err := os.ReadFile(path)
			if c.kept && (err != nil || string(data) != c.body) {
				t.Errorf("THE POINT: the body was not kept for the next revalidation (%q, %v)", data, err)
			}
			if !c.kept && err == nil {
				t.Errorf("THE POINT: a body short of its declared size was kept: %q", data)
			}
		})
	}
}

// write_stream_held yields its body only once released: a far end still
// sending when the client already has its answer.
type write_stream_held struct {
	body    *strings.Reader
	release chan struct{}
}

func (r *write_stream_held) Read(p []byte) (int, error) {
	<-r.release
	return r.body.Read(p)
}

func (r *write_stream_held) Close() error { return nil }

// TestRevalidationIsAnsweredBeforeTheKeep: a revalidation that keeps the body
// is answered 304 at once, not after the far end's whole body has arrived to
// be kept; the body is kept all the same.
func TestRevalidationIsAnsweredBeforeTheKeep(t *testing.T) {
	thread := cache_read_thread(t)
	stream_bytes_reset()
	reader := &write_stream_held{body: strings.NewReader("AVATAR"), release: make(chan struct{})}
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		ctx, _ := gin.CreateTestContext(w)
		ctx.Request = r
		action := &Action{web: ctx}
		action.web.Header("ETag", `"1730000000-6"`)
		kwargs := []sl.Tuple{{sl.String("cache"), sl.String("answered-first")}, {sl.String("size"), sl.MakeInt(6)}}
		if _, err := action.sl_write_stream(thread, sl.NewBuiltin("write.stream", nil), sl.Tuple{&Stream{reader: reader}}, kwargs); err != nil {
			t.Errorf("a.write.stream: %v", err)
		}
	}))
	defer server.Close()
	request, _ := http.NewRequest("GET", server.URL, nil)
	request.Header.Set("If-None-Match", `"1730000000-6"`)
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	close(reader.release)
	if err != nil {
		t.Fatalf("THE POINT: the revalidation was not answered while the far end was still sending: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotModified {
		t.Errorf("the revalidation answered %d, want 304", response.StatusCode)
	}
	<-done
	path, _ := cache_file(thread, "answered-first")
	if data, err := os.ReadFile(path); err != nil || string(data) != "AVATAR" {
		t.Errorf("the body was not kept after the answer (%q, %v)", data, err)
	}
}

// cache_test_hold takes path as a transfer would, answering the function that
// releases it, or false when a transfer already holds it.
func cache_test_hold(path string) (func(), bool) {
	running, taken := cache_hold_take(path)
	if !taken {
		return nil, false
	}
	return func() { cache_hold_release(path, running) }, true
}

// TestCacheWaitFollowsTheTransferInFlight: a request that finds another
// transfer filling the entry it wants waits for that transfer and is told the
// entry is there, rather than failing on its hold; it is told the transfer is
// still running when its time runs out, or when as many requests already wait
// on it as may; and that nothing is filling the entry when no running append
// holds it - a lock file on disk included - so it fetches the entry itself.
func TestCacheWaitFollowsTheTransferInFlight(t *testing.T) {
	directory := t.TempDir()
	entry := filepath.Join(directory, "original")
	partial := filepath.Join(directory, "original.partial")
	reset := func() {
		os.Remove(entry)
		os.Remove(partial)
		os.Remove(partial + ".lock")
	}
	ctx := context.Background()
	hold := func() func() {
		release, held := cache_test_hold(partial)
		if !held {
			t.Fatal("CONTROL: the partial was still held by an earlier section's transfer")
		}
		return release
	}

	// The transfer in flight finishes: its append ends, then its caller moves
	// the partial into place a moment later.
	reset()
	release := hold()
	go func(release func()) {
		time.Sleep(150 * time.Millisecond)
		os.WriteFile(partial, []byte("AVATAR"), 0644)
		release()
		time.Sleep(150 * time.Millisecond)
		os.Rename(partial, entry)
	}(release)
	if got := cache_wait(ctx, entry, partial, 5*time.Second); got != sl.True {
		t.Errorf("THE POINT: waiting on a transfer that finished answered %v, not that the entry is there", got)
	}

	// The transfer is still running when the time runs out.
	reset()
	release = hold()
	started := time.Now()
	if got := cache_wait(ctx, entry, partial, 200*time.Millisecond); got != sl.None {
		t.Errorf("THE POINT: a transfer still running answered %v, not that it is still running", got)
	}
	if waited := time.Since(started); waited < 200*time.Millisecond || waited > 2*time.Second {
		t.Errorf("the wait for a running transfer took %v, not the time given", waited)
	}

	// As many requests as may already wait on it: the next is told at once.
	done := make(chan struct{})
	for range cache_wait_waiters {
		go func() {
			cache_wait(ctx, entry, partial, 5*time.Second)
			done <- struct{}{}
		}()
	}
	for {
		cache_holds.Lock()
		waiting := cache_holds.running[partial].waiting
		cache_holds.Unlock()
		if waiting == cache_wait_waiters {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	started = time.Now()
	if got := cache_wait(ctx, entry, partial, 5*time.Second); got != sl.None || time.Since(started) > time.Second {
		t.Errorf("THE POINT: a request past the waiters allowed answered %v after %v, not at once", got, time.Since(started))
	}
	release()
	for range cache_wait_waiters {
		<-done
	}

	// Nothing fills the entry: answered at once, so the caller fetches it.
	reset()
	started = time.Now()
	if got := cache_wait(ctx, entry, partial, 5*time.Second); got != sl.False || time.Since(started) > time.Second {
		t.Errorf("CONTROL: nothing filling the entry answered %v after %v", got, time.Since(started))
	}

	// A lock file on disk - left by a crash, a restart or an older core -
	// holds nothing.
	reset()
	if err := os.WriteFile(partial+".lock", nil, 0644); err != nil {
		t.Fatal(err)
	}
	if got := cache_wait(ctx, entry, partial, 5*time.Second); got != sl.False {
		t.Errorf("THE POINT: a lock file on disk was waited on, answering %v", got)
	}

	// A transfer cut short ends without its entry, leaving its partial for the
	// next attempt: it is waited on for the moment allowed for the move, and no
	// longer.
	reset()
	os.WriteFile(partial, []byte("AVA"), 0644)
	release = hold()
	go func(release func()) {
		time.Sleep(100 * time.Millisecond)
		release()
	}(release)
	started = time.Now()
	if got, waited := cache_wait(ctx, entry, partial, 30*time.Second), time.Since(started); got != sl.False || waited < cache_wait_settle || waited > cache_wait_settle+time.Second {
		t.Errorf("a transfer cut short answered %v after %v, not False after the %v allowed for its move", got, waited, cache_wait_settle)
	}

	// A transfer whose partial is gone and whose entry is not there - the
	// entry moved away again at once, as an attachment adopted into the app's
	// own storage is, or the partial judged bad - is not waited on past the
	// partial's going: the waiter looks again for itself rather than waiting
	// out the settling time.
	reset()
	os.WriteFile(partial, []byte("AVATAR"), 0644)
	release = hold()
	go func(release func()) {
		time.Sleep(100 * time.Millisecond)
		os.Remove(partial)
		release()
	}(release)
	started = time.Now()
	if got := cache_wait(ctx, entry, partial, 30*time.Second); got != sl.False || time.Since(started) > cache_wait_settle/2 {
		t.Errorf("THE POINT: a transfer whose entry was taken away again answered %v after %v, not at once", got, time.Since(started))
	}

	// The entry already there is answered at once, held or not.
	reset()
	release = hold()
	os.WriteFile(entry, []byte("AVATAR"), 0644)
	if got := cache_wait(ctx, entry, partial, 5*time.Second); got != sl.True {
		t.Errorf("CONTROL: an entry already there answered %v", got)
	}
	release()
	reset()
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

// TestCacheEvictSparesHeldPartials. A partial an append is filling is a
// transfer in flight: never evicted, however old, since deleting it would break
// the transfer writing it, and not counted, or one larger than the budget would
// have every admission evict everything else. One no transfer holds is an
// ordinary entry, so a broken transfer's bytes cannot sit outside the budget.
// A file a transfer takes after the walk began is judged again as it is
// removed.
func TestCacheEvictSparesHeldPartials(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget }()

	old := time.Now().Add(-2 * time.Hour)
	filling := cache_test_write(t, "u", "app", "filling", 100, old)
	abandoned := cache_test_write(t, "u", "app", "abandoned", 100, old)
	release, _ := cache_test_hold(filling)

	remaining := cache_evict_to(0)
	if !file_exists(filling) {
		t.Error("THE POINT: a partial an append is filling was evicted from under it")
	}
	if file_exists(abandoned) {
		t.Error("THE POINT: a partial no transfer holds was spared, outside the budget")
	}
	defer release()
	if remaining != 0 {
		t.Errorf("THE POINT: eviction left %d bytes counted, want none: the held partial is in flight", remaining)
	}
	if measured := cache_measure(); measured != 0 {
		t.Errorf("measurement counted %d bytes, want none: the held partial is in flight", measured)
	}

	later := cache_test_write(t, "u", "app", "later", 100, old)
	release, _ = cache_test_hold(later)
	if cache_remove_unheld(later) || !file_exists(later) {
		t.Error("THE POINT: a file a transfer took after the walk began was removed from under it")
	}
	release()
	if !cache_remove_unheld(later) || file_exists(later) {
		t.Error("CONTROL: a file no transfer holds was not removed")
	}
}

// TestCacheCopyIsSharedAndWhole. A copy is written under a name of its own
// and moved over its destination, so the file a reader has open is never
// rewritten under it, and no temporary is left. A copy into a destination
// another copy is still writing waits for that copy and answers its size,
// writing nothing itself.
func TestCacheCopyIsSharedAndWhole(t *testing.T) {
	thread := cache_read_thread(t)
	source, err := cache_file(thread, "remote/original")
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Dir(source), 0755)
	if err := os.WriteFile(source, []byte("ORIGINAL"), 0644); err != nil {
		t.Fatal(err)
	}
	user := thread.Local("user").(*User)
	base := api_file_base(user, principal_app(thread))
	os.MkdirAll(base, 0755)
	builtin := sl.NewBuiltin("mochi.cache.copy", api_cache_copy)
	copy := func(destination string) (sl.Value, error) {
		return api_cache_copy(thread, builtin, sl.Tuple{sl.String("remote/original"), sl.String(destination)}, nil)
	}

	if err := os.WriteFile(filepath.Join(base, "adopted.jpg"), []byte("OLD"), 0644); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(filepath.Join(base, "adopted.jpg"))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if value, err := copy("adopted.jpg"); err != nil || value != sl.MakeInt64(8) {
		t.Fatalf("a copy = (%v, %v), want (8, nil)", value, err)
	}
	if data, _ := io.ReadAll(reader); string(data) != "OLD" {
		t.Errorf("THE POINT: the file a reader had open was rewritten under it, reading %q", data)
	}
	if data, _ := os.ReadFile(filepath.Join(base, "adopted.jpg")); string(data) != "ORIGINAL" {
		t.Errorf("the destination holds %q, want the copy", data)
	}
	if left, _ := filepath.Glob(filepath.Join(base, "*_copy")); len(left) != 0 {
		t.Errorf("the copy left its temporary behind: %v", left)
	}

	target := filepath.Join(base, "shared.jpg")
	running, _ := cache_hold_take(target)
	result := make(chan sl.Value, 1)
	go func() {
		value, _ := copy("shared.jpg")
		result <- value
	}()
	time.Sleep(100 * time.Millisecond)
	select {
	case value := <-result:
		t.Fatalf("THE POINT: a copy into a destination another copy is writing did not wait for it, answering %v", value)
	default:
	}
	running.size, running.ok = 42, true
	cache_hold_release(target, running)
	if value := <-result; value != sl.MakeInt64(42) {
		t.Errorf("a shared copy answered %v, not the size of the copy it waited for", value)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Error("a shared copy wrote the destination itself")
	}
}

// TestCacheWaitTakesTheTransferBoundOnlyToWait. Waiting on a transfer moves
// the call to the transfer bound, as the transfer itself does; a wait on
// nothing does not, or any call could lift its own time limit.
func TestCacheWaitTakesTheTransferBoundOnlyToWait(t *testing.T) {
	thread := cache_read_thread(t)
	serving := new(atomic.Bool)
	thread.SetLocal("file_serving", serving)
	builtin := sl.NewBuiltin("mochi.cache.wait", api_cache_wait)
	wait := func() sl.Value {
		value, err := api_cache_wait(thread, builtin, sl.Tuple{sl.String("remote/x"), sl.String("partial/x")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if got := wait(); got != sl.False || serving.Load() {
		t.Errorf("THE POINT: a wait on nothing answered %v and took the transfer bound %v", got, serving.Load())
	}
	partial, err := cache_file(thread, "partial/x")
	if err != nil {
		t.Fatal(err)
	}
	release, _ := cache_test_hold(partial)
	defer release()
	if got := wait(); got != sl.None || !serving.Load() {
		t.Errorf("CONTROL: a wait on a running transfer answered %v and took the transfer bound %v", got, serving.Load())
	}
}

// TestCacheHeadReadsOnlyTheStart: mochi.cache.head answers an entry's first
// bytes, however large the entry - past the bound a whole read refuses - fewer
// for a shorter one, and None for a missing one; a length out of range is
// refused.
func TestCacheHeadReadsOnlyTheStart(t *testing.T) {
	thread := cache_read_thread(t)
	saved := cache_read_maximum
	cache_read_maximum = 1024
	defer func() { cache_read_maximum = saved }()
	head := func(name string, length int) (sl.Value, error) {
		return api_cache_head(thread, sl.NewBuiltin("mochi.cache.head", api_cache_head), sl.Tuple{sl.String(name), sl.MakeInt(length)}, nil)
	}
	path, err := cache_file(thread, "large")
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Dir(path), 0755)
	data := append([]byte{0xff, 0xd8, 0xff, 0xe0}, make([]byte, 4096)...)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	value, err := head("large", 4)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := value.(sl.Bytes); !ok || string(got) != string(data[:4]) {
		t.Errorf("THE POINT: the head of a %d-byte entry, past the %d a read takes, is %v", len(data), cache_read_maximum, value)
	}
	if value, _ := head("large", 10000); value == nil {
		t.Error("a head longer than the entry answered nothing")
	} else if got, ok := value.(sl.Bytes); !ok || len(got) != len(data) {
		t.Errorf("a head longer than the entry answered %s, want its %d bytes", value.Type(), len(data))
	}
	if value, err := head("missing", 4); err != nil || value != sl.None {
		t.Errorf("a missing entry's head answered %v, %v; want None", value, err)
	}
	if _, err := head("large", 0); err == nil {
		t.Error("a length of 0 was not refused")
	}
	if _, err := head("large", file_head_maximum+1); err == nil {
		t.Error("a length past the maximum was not refused")
	}
}
