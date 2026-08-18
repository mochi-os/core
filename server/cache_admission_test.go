// Mochi server: the cache budget is enforced when a write lands, not an hour later.
//
// The cache is exempt from the per-user storage quota in exchange for being
// unconditionally evictable - cache.go says so at the top of the file - but that
// bargain was honoured only by an hourly sweep. cache_write_file never consulted
// cache_budget, and cache_evict had exactly one caller, at the end of the hourly
// cache sweep. Between two sweeps an app could write until the disk filled, and
// cache_dir is shared with every user on the host.
//
// Evicting rather than refusing keeps the other half of the bargain: an app is
// promised that a miss re-obtains, so a write that fails is a contract this code
// does not have.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// cache_admission_setup gives a test its own cache directory and budget, and
// resets the running total so one test's accounting cannot leak into another's.
func cache_admission_setup(t *testing.T, budget int64) {
	t.Helper()
	original_dir, original_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	cache_budget = budget
	cache_total_set(-1)
	t.Cleanup(func() {
		cache_dir, cache_budget = original_dir, original_budget
		cache_total_set(-1)
	})
}

func cache_admission_write(t *testing.T, name string, size int) string {
	t.Helper()
	path := filepath.Join(cache_dir, "apps", "userA", "app", name)
	if _, err := cache_write_file(path, bytes.NewReader(make([]byte, size))); err != nil {
		t.Fatalf("cache_write_file(%s): %v", name, err)
	}
	return path
}

// TestCacheWriteEvictsWhenOverBudget is the finding. Nothing on the write path
// looked at the budget, so the only bound was a sweep up to an hour away.
func TestCacheWriteEvictsWhenOverBudget(t *testing.T) {
	cache_admission_setup(t, 1000)

	// Well past the budget in one run of writes, none of which the hourly sweep
	// would have seen.
	for i := 0; i < 30; i++ {
		cache_admission_write(t, fmt.Sprintf("entry%d", i), 100)
	}

	if total := cache_test_total(t); total > cache_budget {
		t.Errorf("cache holds %d bytes against a budget of %d; the write path still lets an app past it until the hourly sweep",
			total, cache_budget)
	}
}

// TestCacheWriteKeepsWhatItJustWrote. Evicting on admission must not evict the
// entry being admitted - an app that writes and immediately reads back would
// otherwise miss on the value it just stored.
func TestCacheWriteKeepsWhatItJustWrote(t *testing.T) {
	cache_admission_setup(t, 1000)

	for i := 0; i < 20; i++ {
		cache_admission_write(t, fmt.Sprintf("entry%d", i), 100)
	}
	newest := cache_admission_write(t, "newest", 100)

	if !file_exists(newest) {
		t.Error("the entry that triggered the eviction was itself evicted; a write followed by a read would miss")
	}
}

// TestCacheWriteClearsHeadroom. cache_evict stops the instant it reaches the
// budget, so evicting to exactly that would leave the very next write over
// again and walk the whole tree once per write. Admission evicts to a margin
// below instead, so one walk serves many writes.
//
// Measured on the write that crosses, not at the end of a run: the margin is
// meant to be refilled by the writes that follow, so a total near the budget
// later is correct and says nothing about the target used.
func TestCacheWriteClearsHeadroom(t *testing.T) {
	cache_admission_setup(t, 1000)

	now := time.Now()
	for i := 0; i < 10; i++ {
		cache_test_write(t, "userA", "app", fmt.Sprintf("entry%d", i), 100, now.Add(-time.Duration(20-i)*time.Minute))
	}
	cache_total_set(-1)

	cache_admission_write(t, "crosses", 100)

	total := cache_test_total(t)
	if total > cache_budget-cache_budget/cache_headroom_divisor {
		t.Errorf("cache holds %d bytes, want at most %d: admission evicted to the budget rather than below it, so every following write walks the tree",
			total, cache_budget-cache_budget/cache_headroom_divisor)
	}
}

// TestCacheWriteEvictsLeastRecentlyUsed. Admission must use the same order as
// the sweep, or a write would discard a hot entry while a cold one survives.
func TestCacheWriteEvictsLeastRecentlyUsed(t *testing.T) {
	cache_admission_setup(t, 500)

	now := time.Now()
	cold := cache_test_write(t, "userA", "app", "cold", 200, now.Add(-time.Hour))
	warm := cache_test_write(t, "userA", "app", "warm", 200, now.Add(-time.Minute))
	cache_total_set(-1) // the fixture wrote behind the accounting; reseed by walking

	cache_admission_write(t, "new", 200)

	if file_exists(cold) {
		t.Error("the least recently used entry survived an admission eviction")
	}
	if !file_exists(warm) {
		t.Error("a recently used entry was evicted ahead of a colder one")
	}
}

// TestCacheRunningTotalTracksWritesAndDeletes. The total exists so admission
// does not walk the tree on every write; if it drifts, admission either evicts
// when it need not or fails to when it must.
func TestCacheRunningTotalTracksWritesAndDeletes(t *testing.T) {
	cache_admission_setup(t, 100000)

	first := cache_admission_write(t, "first", 300)
	cache_admission_write(t, "second", 200)

	if got, want := cache_total_read(), int64(500); got != want {
		t.Errorf("running total is %d after two writes, want %d", got, want)
	}

	// A rewrite replaces, so only the difference counts.
	cache_admission_write(t, "first", 100)
	if got, want := cache_total_read(), int64(300); got != want {
		t.Errorf("running total is %d after shrinking an entry, want %d: a rewrite was counted as new bytes", got, want)
	}

	os.Remove(first)
	cache_admit(-100)
	if got, want := cache_total_read(), int64(200); got != want {
		t.Errorf("running total is %d after a delete, want %d", got, want)
	}
}

// TestCacheAdmissionIsSkippedWhenUnconfigured. A negative budget means the
// operator has not configured one and eviction is off; admission must not
// invent a limit, or a fresh install would evict its own cache.
func TestCacheAdmissionIsSkippedWhenUnconfigured(t *testing.T) {
	cache_admission_setup(t, -1)

	for i := 0; i < 10; i++ {
		cache_admission_write(t, fmt.Sprintf("entry%d", i), 1000)
	}

	if total := cache_test_total(t); total != 10000 {
		t.Errorf("cache holds %d bytes, want all 10000: an unconfigured budget must not evict", total)
	}
}

// cache_total_read reports the running total without racing the writers.
func cache_total_read() int64 {
	cache_total_lock.Lock()
	defer cache_total_lock.Unlock()
	return cache_total
}

// TestCacheApiWriteEvictsWhenOverBudget drives mochi.cache.write itself rather
// than the helper beneath it, so the admission is proved to be on the path an
// app actually reaches and not merely on a function near it.
func TestCacheApiWriteEvictsWhenOverBudget(t *testing.T) {
	_, thread := file_api_environment(t)
	cache_admission_setup(t, 1000)

	for i := 0; i < 30; i++ {
		_, err := api_cache_write(thread, sl.NewBuiltin("mochi.cache.write", nil),
			sl.Tuple{sl.String(fmt.Sprintf("entry%d", i)), sl.String(string(make([]byte, 100)))}, nil)
		if err != nil {
			t.Fatalf("api_cache_write: %v", err)
		}
	}

	if total := cache_test_total(t); total > cache_budget {
		t.Errorf("cache holds %d bytes against a budget of %d: mochi.cache.write still writes without admission",
			total, cache_budget)
	}
}

// TestCacheApiDeleteReleasesBudget. A delete that is not accounted for leaves
// the running total permanently high, so admission evicts entries that are
// within budget - the cache silently shrinks for the rest of the process.
func TestCacheApiDeleteReleasesBudget(t *testing.T) {
	_, thread := file_api_environment(t)
	cache_admission_setup(t, 100000)

	write := func(name string, size int) {
		t.Helper()
		if _, err := api_cache_write(thread, sl.NewBuiltin("mochi.cache.write", nil),
			sl.Tuple{sl.String(name), sl.String(string(make([]byte, size)))}, nil); err != nil {
			t.Fatalf("api_cache_write: %v", err)
		}
	}
	write("first", 300)
	write("second", 200)

	if _, err := api_cache_delete(thread, sl.NewBuiltin("mochi.cache.delete", nil),
		sl.Tuple{sl.String("first")}, nil); err != nil {
		t.Fatalf("api_cache_delete: %v", err)
	}

	if got, want := cache_total_read(), int64(200); got != want {
		t.Errorf("running total is %d after deleting a 300-byte entry, want %d: mochi.cache.delete does not release the bytes it frees",
			got, want)
	}
}

// TestCacheArchiveIsAdmitted. mochi.archive.write(cache=True) is the one cache
// write that does not go through cache_write_file - it opens the destination
// itself and sets its byte limit to MaxInt64, the cache being exempt from the
// per-user quota. An admission rule on mochi.cache.write alone leaves this path
// as an unmetered way to the same directory.
func TestCacheArchiveIsAdmitted(t *testing.T) {
	base, thread := file_api_environment(t)
	cache_admission_setup(t, 1000000)

	if _, err := cache_write_file(filepath.Join(cache_dir, "apps", "testuser", "files", "seed"),
		bytes.NewReader(make([]byte, 100))); err != nil {
		t.Fatalf("seeding the cache: %v", err)
	}
	if got, want := cache_total_read(), int64(100); got != want {
		t.Fatalf("running total is %d before the archive, want %d", got, want)
	}

	// Incompressible content, so the archive is not so small that a broken
	// accounting is indistinguishable from a correct one.
	content := make([]byte, 4096)
	rand.New(rand.NewSource(2)).Read(content)
	if err := os.WriteFile(base+"/payload.bin", content, 0600); err != nil {
		t.Fatalf("writing payload: %v", err)
	}
	entries := sl_encode([]map[string]any{{"name": "payload.bin", "file": "payload.bin"}})

	archive := func() {
		t.Helper()
		if _, err := api_archive_write(thread, sl.NewBuiltin("mochi.archive.write", nil),
			sl.Tuple{sl.String("export.zip"), entries}, []sl.Tuple{{sl.String("cache"), sl.Bool(true)}}); err != nil {
			t.Fatalf("api_archive_write: %v", err)
		}
	}
	archive()

	information, err := os.Stat(filepath.Join(cache_dir, "apps", "testuser", "files", "export.zip"))
	if err != nil {
		t.Fatalf("the archive did not land in the cache: %v", err)
	}
	if got, want := cache_total_read(), 100+information.Size(); got != want {
		t.Errorf("running total is %d after a cached archive of %d bytes, want %d: mochi.archive.write(cache=True) writes to the cache unaccounted",
			got, information.Size(), want)
	}

	// Rewriting the same archive replaces it, so only the difference counts -
	// otherwise a nightly export would inflate the total until admission
	// evicted a cache that was never over budget.
	archive()
	if got, want := cache_total_read(), 100+information.Size(); got != want {
		t.Errorf("running total is %d after rewriting the archive, want %d: the replaced bytes were counted twice", got, want)
	}
}

// TestCacheArchiveAccountedWhenItFails. An archive that errors partway leaves
// what the zip writer has already flushed standing in the cache. Accounting only
// on the success path would lose those bytes from the running total for the rest
// of the process, and a total that reads low under-evicts.
func TestCacheArchiveAccountedWhenItFails(t *testing.T) {
	base, thread := file_api_environment(t)
	cache_admission_setup(t, 100000000)
	cache_total_set(0)

	// Incompressible, and far past the zip writer's buffer, so the partial
	// archive on disk is substantial rather than a header the buffer still
	// holds. A small partial would let the test pass without the accounting.
	content := make([]byte, 256*1024)
	rand.New(rand.NewSource(1)).Read(content)
	if err := os.WriteFile(base+"/payload.bin", content, 0600); err != nil {
		t.Fatalf("writing payload: %v", err)
	}
	items := []map[string]any{}
	for i := 0; i < 4; i++ {
		items = append(items, map[string]any{"name": fmt.Sprintf("payload%d.bin", i), "file": "payload.bin"})
	}
	items = append(items, map[string]any{"name": "", "data": "x"}) // rejected
	entries := sl_encode(items)

	if _, err := api_archive_write(thread, sl.NewBuiltin("mochi.archive.write", nil),
		sl.Tuple{sl.String("export.zip"), entries}, []sl.Tuple{{sl.String("cache"), sl.Bool(true)}}); err == nil {
		t.Fatal("api_archive_write accepted an entry with no name")
	}

	information, err := os.Stat(filepath.Join(cache_dir, "apps", "testuser", "files", "export.zip"))
	if err != nil {
		t.Fatalf("the failed archive left no file to account for: %v", err)
	}
	if information.Size() < 128*1024 {
		t.Fatalf("the partial archive is only %d bytes, too small to tell a correct accounting from a missing one", information.Size())
	}
	if got := cache_total_read(); got != information.Size() {
		t.Errorf("running total is %d with a %d-byte partial archive in the cache, want %d: a failed archive leaks its bytes past admission",
			got, information.Size(), information.Size())
	}
}

// TestCacheImageVariantIsAdmitted. mochi.image.variant renders straight into
// the cache directory, so it is a third way to add bytes there that does not
// pass cache_write_file. An app that renders a variant per image would fill the
// cache with the budget checked only by the hourly sweep.
func TestCacheImageVariantIsAdmitted(t *testing.T) {
	base, thread := file_api_environment(t)
	cache_admission_setup(t, 100000000)
	cache_total_set(0)

	// A photograph-sized source, so the rendered variant is unambiguously
	// larger than nothing.
	source := image.NewRGBA(image.Rect(0, 0, 1200, 900))
	generator := rand.New(rand.NewSource(3))
	for y := 0; y < 900; y++ {
		for x := 0; x < 1200; x++ {
			source.Set(x, y, color.RGBA{uint8(generator.Intn(256)), uint8(generator.Intn(256)), uint8(generator.Intn(256)), 255})
		}
	}
	handle, err := os.Create(base + "/photo.jpg")
	if err != nil {
		t.Fatalf("creating source image: %v", err)
	}
	if err := jpeg.Encode(handle, source, nil); err != nil {
		t.Fatalf("encoding source image: %v", err)
	}
	handle.Close()

	value, err := api_image_variant(thread, sl.NewBuiltin("mochi.image.variant", nil),
		sl.Tuple{sl.String("photo.jpg"), sl.String("preview")}, nil)
	if err != nil {
		t.Fatalf("api_image_variant: %v", err)
	}
	name, ok := sl.AsString(value)
	if !ok || name == "" {
		t.Fatalf("api_image_variant returned %v, want a cache entry name", value)
	}

	information, err := os.Stat(filepath.Join(cache_dir, "apps", "testuser", "files", name))
	if err != nil {
		t.Fatalf("the variant did not land in the cache: %v", err)
	}
	if got := cache_total_read(); got != information.Size() {
		t.Errorf("running total is %d after a %d-byte variant landed in the cache, want %d: mochi.image.variant writes to the cache unaccounted",
			got, information.Size(), information.Size())
	}
}
