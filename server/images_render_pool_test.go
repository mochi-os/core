// Mochi server: bound how many image decodes run at once.
//
// The file and pixel caps bound ONE render - 100 MB plus 400 MB of RGBA - and
// the HTTP path decodes in the request goroutine, so nothing bounded the total.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// render_test_image writes a small PNG and returns its path.
func render_test_image(t *testing.T, directory string, name string, size int) string {
	t.Helper()
	picture := image.NewRGBA(image.Rect(0, 0, size, size))
	for y := 0; y < size; y++ {
		for x := 0; x < size; x++ {
			picture.Set(x, y, color.RGBA{uint8(x), uint8(y), 0, 255})
		}
	}
	path := filepath.Join(directory, name)
	handle, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating %s: %v", path, err)
	}
	defer handle.Close()
	if err := png.Encode(handle, picture); err != nil {
		t.Fatalf("encoding %s: %v", path, err)
	}
	return path
}

// render_pool_fill takes every slot, so the next acquire has to wait. Returns
// the release for all of them.
func render_pool_fill(t *testing.T) func() {
	t.Helper()
	taken := 0
	for i := 0; i < image_render_parallel; i++ {
		select {
		case image_render_slots <- struct{}{}:
			taken++
		case <-time.After(time.Second):
			t.Fatalf("could not take slot %d; the pool was not empty at the start of this test", i)
		}
	}
	return func() {
		for i := 0; i < taken; i++ {
			<-image_render_slots
		}
	}
}

// TestRenderPoolBoundsConcurrency is the defect: nothing stopped every request
// decoding at once.
func TestRenderPoolBoundsConcurrency(t *testing.T) {
	if cap(image_render_slots) != image_render_parallel {
		t.Fatalf("the pool holds %d slots, want %d", cap(image_render_slots), image_render_parallel)
	}
	if image_render_parallel < 1 || image_render_parallel > 8 {
		t.Errorf("image_render_parallel is %d: a slot is worth about half a gigabyte, so this wants to stay small", image_render_parallel)
	}

	// With every slot held, an acquire must give up rather than pile on.
	release := render_pool_fill(t)
	defer release()

	original := image_render_wait
	image_render_wait = 50 * time.Millisecond
	defer func() { image_render_wait = original }()

	start := time.Now()
	if _, err := image_render_acquire(); err == nil {
		t.Error("a fifth concurrent decode was admitted; the pool bounds nothing")
	}
	if waited := time.Since(start); waited > time.Second {
		t.Errorf("the acquire waited %v, far past image_render_wait: a request would hold its goroutine indefinitely", waited)
	}
}

// TestRenderWaitsRatherThanFailingImmediately. A brief burst should queue, not
// be refused - four slots would otherwise turn an ordinary gallery load into
// missing thumbnails.
func TestRenderWaitsRatherThanFailingImmediately(t *testing.T) {
	release := render_pool_fill(t)

	original := image_render_wait
	image_render_wait = 2 * time.Second
	defer func() { image_render_wait = original }()

	// Free a slot shortly; the acquire should pick it up rather than time out.
	go func() {
		time.Sleep(100 * time.Millisecond)
		release()
	}()

	done, err := image_render_acquire()
	if err != nil {
		t.Fatalf("an acquire that only had to wait 100ms gave up: %v", err)
	}
	done()
}

// TestRenderRefusalIsNotAnError. Every caller already handles "no variant":
// the HTTP path serves the original bytes and mochi.image.variant answers
// None. Returning an error instead would turn a busy moment into a failed
// request.
func TestRenderRefusalIsNotAnError(t *testing.T) {
	directory := t.TempDir()
	source := render_test_image(t, directory, "photo.png", 64)

	release := render_pool_fill(t)
	defer release()

	original := image_render_wait
	image_render_wait = 50 * time.Millisecond
	defer func() { image_render_wait = original }()

	result, err := variant_render(source, "thumbnail", filepath.Join(directory, "out.png"))
	if err != nil {
		t.Errorf("a busy pool produced an error (%v); callers read an error as a broken image, not as a busy server", err)
	}
	if result != "" {
		t.Errorf("a busy pool produced %q, want the empty result that means no variant", result)
	}
	if _, err := os.Stat(filepath.Join(directory, "out.png")); err == nil {
		t.Error("a refused render still wrote its destination")
	}
}

// TestRenderReleasesItsSlot. A slot leaked per render would take four renders
// to wedge every future one, and the wedge would outlive whatever caused it.
func TestRenderReleasesItsSlot(t *testing.T) {
	directory := t.TempDir()
	source := render_test_image(t, directory, "photo.png", 64)

	// Short wait: a leaked slot should fail this test in milliseconds, not
	// stall it for the full production wait on every render after the fourth.
	original := image_render_wait
	image_render_wait = 100 * time.Millisecond
	defer func() { image_render_wait = original }()

	for i := 0; i < image_render_parallel*3; i++ {
		out := filepath.Join(directory, "out.png")
		os.Remove(out)
		result, err := variant_render(source, "thumbnail", out)
		if err != nil {
			t.Fatalf("render %d failed: %v", i, err)
		}
		if result == "" {
			t.Fatalf("render %d produced no variant: the pool ran out, so slots are not being released", i)
		}
	}

	// Every slot must be free again.
	release := render_pool_fill(t)
	release()
}

// render_function returns one function's body from images.go. Indexing the
// whole file finds the DEFINITION of image_render_acquire before any call to
// it, which is how both assertions below first failed against correct code.
func render_function(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile("images.go")
	if err != nil {
		t.Fatalf("reading images.go: %v", err)
	}
	source := string(body)
	at := strings.Index(source, "func "+name+"(")
	if at < 0 {
		t.Fatalf("%s not found", name)
	}
	rest := source[at:]
	end := strings.Index(rest, "\n}")
	if end < 0 {
		return rest
	}
	return rest[:end]
}

// TestRenderSlotTakenAfterTheCheapRefusals. An oversized file is turned away by
// a stat, so it must not occupy a slot a renderable image could use.
func TestRenderSlotTakenAfterTheCheapRefusals(t *testing.T) {
	source := render_function(t, "variant_render")

	acquire := strings.Index(source, "image_render_acquire()")
	size := strings.Index(source, "image_file_bytes_maximum {")
	read := strings.Index(source, "io.ReadAll(f)")
	if acquire < 0 || size < 0 || read < 0 {
		t.Fatal("the render path no longer has the shape this test checks")
	}
	if acquire < size {
		t.Error("a slot is taken before the file-size refusal, so an oversized upload occupies one for nothing")
	}
	if acquire > read {
		t.Error("the slot is taken after the file is read into memory, which is one of the two allocations it exists to bound")
	}
}

// TestRenderPoolIsSharedByBothCallers. The HTTP path and mochi.image.variant
// both decode, so a pool only one of them passes through bounds nothing.
func TestRenderPoolIsSharedByBothCallers(t *testing.T) {
	if n := strings.Count(render_function(t, "variant_render"), "image_render_acquire()"); n != 1 {
		t.Errorf("variant_render acquires a slot %d times, want 1; both callers reach the decode through it, so that is where the bound belongs", n)
	}

	// And a concurrent pair through the real function still bounds itself.
	directory := t.TempDir()
	first := render_test_image(t, directory, "one.png", 48)
	second := render_test_image(t, directory, "two.png", 48)

	var group sync.WaitGroup
	for _, source := range []string{first, second} {
		group.Add(1)
		go func(path string) {
			defer group.Done()
			variant_render(path, "thumbnail", path+".out")
		}(source)
	}
	group.Wait()

	release := render_pool_fill(t)
	release()
}

// variant_render_shared_test_image writes a PNG large enough that encoding its
// preview takes a while, so a second render can start inside the first's.
func variant_render_shared_test_image(t *testing.T, directory string) string {
	t.Helper()
	picture := image.NewRGBA(image.Rect(0, 0, 2000, 1500))
	for y := 0; y < 1500; y++ {
		for x := 0; x < 2000; x++ {
			picture.Set(x, y, color.RGBA{uint8(x * y), uint8(x ^ y), uint8(x + y), 255})
		}
	}
	path := filepath.Join(directory, "photo.png")
	handle, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err := png.Encode(handle, picture); err != nil {
		t.Fatal(err)
	}
	return path
}

// variant_render_decodes reports whether the file at path is a whole image.
func variant_render_decodes(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, _, err = image.Decode(bytes.NewReader(data))
	return err
}

// TestVariantRenderTemporaryIsItsOwn: two renders of one destination, the
// second starting while the first writes. They shared one temporary name, and
// the second began by deleting it, so the first moved the second's
// half-written file into place (served truncated) or found nothing to move,
// and the loser served the original in the variant's place.
func TestVariantRenderTemporaryIsItsOwn(t *testing.T) {
	directory := t.TempDir()
	source := variant_render_shared_test_image(t, directory)
	destination := filepath.Join(directory, "variants", "photo_preview.png")

	type outcome struct {
		result string
		err    error
		whole  error
	}
	render := func(out chan<- outcome) {
		result, err := variant_render(source, "preview", destination)
		var whole error
		if err == nil && result != "" {
			whole = variant_render_decodes(result)
		}
		out <- outcome{result, err, whole}
	}

	first := make(chan outcome, 1)
	go render(first)
	// The first is writing once its temporary exists beside the destination.
	deadline := time.Now().Add(10 * time.Second)
	for {
		names, _ := filepath.Glob(destination + ".*")
		if len(names) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the first render never began writing")
		}
		time.Sleep(time.Millisecond)
	}
	second := make(chan outcome, 1)
	go render(second)

	for name, out := range map[string]chan outcome{"first": first, "second": second} {
		got := <-out
		if got.err != nil || got.result != destination {
			t.Errorf("THE POINT: the %s render failed beside another: %q, %v", name, got.result, got.err)
		} else if got.whole != nil {
			t.Errorf("THE POINT: the %s render's file does not decode when it returns: %v", name, got.whole)
		}
	}
	if left, _ := filepath.Glob(destination + ".*"); len(left) > 0 {
		t.Errorf("temporaries left behind: %v", left)
	}
}

// variant_made_cache counts cache admissions for the test: a budget no render
// reaches, and a running total from zero.
func variant_made_cache(t *testing.T) {
	t.Helper()
	original_dir, original_budget, original_total := cache_dir, cache_budget, cache_total
	cache_dir, cache_budget, cache_total = t.TempDir(), 1<<40, 0
	t.Cleanup(func() {
		cache_dir, cache_budget = original_dir, original_budget
		cache_total_set(original_total)
	})
}

// TestVariantMadeOnceForCallersTogether: four callers ask for one variant at
// once, as a new photo's first viewers, a gallery and its lightbox, or
// subscribers' servers do. Each rendered it again (four decodes, about 300 ms
// of CPU each) and counted its bytes into the cache again.
func TestVariantMadeOnceForCallersTogether(t *testing.T) {
	variant_made_cache(t)
	directory := t.TempDir()
	source := render_test_image(t, directory, "photo.png", 900)
	destination := filepath.Join(cache_dir, "apps", "user", "app", "variants", "photo_preview.png")

	// Every slot held, so all four are asking before any render can start.
	release := render_pool_fill(t)
	var group sync.WaitGroup
	made := make([]bool, 4)
	for i := range made {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			made[i] = variant_made(source, "preview", destination, time.Now().Add(-time.Hour))
		}(i)
	}
	time.Sleep(200 * time.Millisecond)
	release()
	group.Wait()

	for i, ok := range made {
		if !ok {
			t.Errorf("caller %d was told the variant could not be made", i)
		}
	}
	if err := variant_render_decodes(destination); err != nil {
		t.Fatalf("the variant does not decode: %v", err)
	}
	information, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	cache_total_lock.Lock()
	total := cache_total
	cache_total_lock.Unlock()
	if total != information.Size() {
		t.Errorf("THE POINT: four callers together rendered and counted the variant %.1f times (%d bytes counted for a %d-byte file); want once", float64(total)/float64(information.Size()), total, information.Size())
	}
}

// TestVariantMadeWaitersShareAFailure: callers waiting on a render that fails
// are told it failed, and fall back as the caller that rendered does.
func TestVariantMadeWaitersShareAFailure(t *testing.T) {
	variant_made_cache(t)
	directory := t.TempDir()
	source := filepath.Join(directory, "broken.png")
	if err := os.WriteFile(source, []byte("not a picture"), 0o644); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(cache_dir, "apps", "user", "app", "variants", "broken_preview.png")

	release := render_pool_fill(t)
	var group sync.WaitGroup
	made := make([]bool, 3)
	for i := range made {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			made[i] = variant_made(source, "preview", destination, time.Now().Add(-time.Hour))
		}(i)
	}
	time.Sleep(200 * time.Millisecond)
	release()
	group.Wait()
	for i, ok := range made {
		if ok {
			t.Errorf("caller %d was told a variant of an undecodable file was made", i)
		}
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Errorf("a failed render left a file at the destination: %v", err)
	}
}

// TestVariantMadeKeepsAFreshVariant: a caller that looked before another's
// render finished finds the variant made when its turn comes, and keeps it
// rather than rendering it again.
func TestVariantMadeKeepsAFreshVariant(t *testing.T) {
	variant_made_cache(t)
	directory := t.TempDir()
	source := render_test_image(t, directory, "photo.png", 900)
	destination := filepath.Join(cache_dir, "apps", "user", "app", "variants", "photo_preview.png")
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("made a moment ago"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !variant_made(source, "preview", destination, time.Now().Add(-time.Hour)) {
		t.Fatal("a variant newer than its source was reported as not made")
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "made a moment ago" {
		t.Errorf("THE POINT: a variant made since the caller looked was rendered again")
	}
}
