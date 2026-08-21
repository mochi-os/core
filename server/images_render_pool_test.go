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
