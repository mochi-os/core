// Mochi server: Image variant memory bounds
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

// image_write encodes a solid PNG of the given dimensions to a temporary file.
func image_write(t *testing.T, dir string, name string, width int, height int) string {
	t.Helper()
	i := image.NewRGBA(image.Rect(0, 0, width, height))
	for x := 0; x < width; x++ {
		for y := 0; y < height; y++ {
			i.Set(x, y, color.RGBA{uint8(x), uint8(y), 0, 255})
		}
	}
	buffer := &bytes.Buffer{}
	if err := png.Encode(buffer, i); err != nil {
		t.Fatalf("encode: %v", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, buffer.Bytes(), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

// TestVariantRenderRefusesOversizeFile — the defect this bounds. The pixel cap
// below it cannot cover this case: dimensions are only readable once the bytes
// are in memory, so an arbitrarily large file was already resident by the time
// anything had looked at it. Uploads are bounded only by the uploader's
// remaining storage quota, so "large" here means gigabytes, not megabytes.
func TestVariantRenderRefusesOversizeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.png")

	// Sparse: the apparent size is what the check reads, and writing 100 MB of
	// real bytes would only slow the test down.
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := f.Truncate(image_file_bytes_maximum + 1); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	f.Close()

	thumb, err := variant_render(path, "thumbnail", filepath.Join(dir, "out.png"))
	if err != nil {
		t.Errorf("refusal reported an error, so the caller stops instead of serving the original: %v", err)
	}
	if thumb != "" {
		t.Errorf("an oversized file was read and rendered to %q", thumb)
	}
	if _, err := os.Stat(filepath.Join(dir, "out.png")); err == nil {
		t.Error("a variant was written for a file that should not have been read")
	}
}

// TestVariantRenderAllowsFileWithinCap — the cap must not break a normal photo.
// A real image well under the cap has to render, or the fix has traded a memory
// bound for broken thumbnails.
func TestVariantRenderAllowsFileWithinCap(t *testing.T) {
	dir := t.TempDir()
	path := image_write(t, dir, "photo.png", 900, 600)
	out := filepath.Join(dir, "out.png")

	thumb, err := variant_render(path, "thumbnail", out)
	if err != nil {
		t.Fatalf("a normal image was refused: %v", err)
	}
	if thumb != out {
		t.Fatalf("variant path = %q, want %q", thumb, out)
	}

	// It must be an actual downscaled image, not an empty or copied file.
	f, err := os.Open(out)
	if err != nil {
		t.Fatalf("open variant: %v", err)
	}
	defer f.Close()
	config, _, err := image.DecodeConfig(f)
	if err != nil {
		t.Fatalf("variant does not decode: %v", err)
	}
	if config.Width > thumbnail_size || config.Height > thumbnail_size {
		t.Errorf("variant is %dx%d, larger than the %dpx thumbnail bound", config.Width, config.Height, thumbnail_size)
	}
}

// TestVariantRenderRefusalDegradesToOriginal — the refusal must return
// ("", nil), not an error. web.go serves the original when the variant comes
// back empty and no error was raised; an error on this path would be
// indistinguishable from a corrupt file and is equally survivable, but ("", nil)
// is what the neighbouring pixel-cap refusal returns and the two should agree.
func TestVariantRenderRefusalDegradesToOriginal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.png")
	f, _ := os.Create(path)
	f.Truncate(image_file_bytes_maximum + 1)
	f.Close()

	thumb, err := variant_render(path, "thumbnail", filepath.Join(dir, "out.png"))

	// This is the condition at web.go:2197 - it must select the fallback.
	if err == nil && thumb != "" {
		t.Error("the caller would serve a variant that was never rendered")
	}
	if err != nil {
		t.Errorf("the caller treats an error as a hard failure rather than falling back: %v", err)
	}
}

// TestImageCapsAreCoherent — the two caps are documented as a pair, so guard
// the relationship rather than the numbers. A file cap below what a legitimate
// image at the pixel cap needs would make the pixel cap unreachable for every
// format; one far above it would leave the file cap doing nothing.
func TestImageCapsAreCoherent(t *testing.T) {
	// A 100 megapixel JPEG is roughly 20-30 MB. The file cap has to clear that
	// comfortably or ordinary photographs start being refused.
	if image_file_bytes_maximum < 32<<20 {
		t.Errorf("file cap %d is too low to admit a photograph at the pixel cap", image_file_bytes_maximum)
	}
	// And it has to stay below what the pixel cap itself permits in memory
	// (100 Mpx x 4 bytes = 400 MB), or the file read becomes the larger of the
	// two allocations and the pixel cap is no longer the binding constraint.
	if image_file_bytes_maximum >= image_decode_pixels_maximum*4 {
		t.Errorf("file cap %d exceeds the decoded allocation the pixel cap allows", image_file_bytes_maximum)
	}
}
