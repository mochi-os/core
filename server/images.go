// Mochi: Images
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"github.com/disintegration/imaging"
	"github.com/nfnt/resize"
	"github.com/rwcarlsen/goexif/exif"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func is_image(file string) bool {
	ext := filepath.Ext(file)

	switch ext {
	case ".gif":
		return true
	case ".jpeg":
		return true
	case ".jpg":
		return true
	case ".png":
		return true
	case ".webp":
		return true
	}

	return false
}

// Image variant sizes (longest side, never upscaled). Thumbnails suit small
// chips and lists; previews suit full-width card and gallery tiles on
// high-density displays.
const (
	thumbnail_size = 250
	preview_size   = 1280
)

func variant_size(variant string) uint {
	if variant == "preview" {
		return preview_size
	}
	return thumbnail_size
}

// These two caps bound the memory a single variant render can hold, and are
// meant to be read together: the file cap covers the compressed bytes, the
// pixel cap covers what they decode to.
//
// image_decode_pixels_maximum caps the pixel count of an image decoded from a
// possibly-hostile source. A small file can declare enormous dimensions (a
// decompression bomb) and exhaust memory when decoded, so DecodeConfig is
// consulted before image.Decode allocates. 100 megapixels is far above any
// legitimate photo and still bounds the allocation.
//
// image_file_bytes_maximum caps the file itself, which the pixel cap cannot:
// the dimensions are only readable once the bytes are in memory, so a large
// file is resident before anything has looked at it. An upload is bounded only
// by the uploader's remaining storage quota, so without this a multi-gigabyte
// file becomes multi-gigabyte of heap the moment a thumbnail is requested.
//
// A 100 megapixel JPEG runs to roughly 20-30 MB, so 100 MB clears any real
// photograph several times over. Lossless formats are denser and a 100
// megapixel PNG can exceed this, meaning the file cap binds first for those -
// deliberately, since such a file is not one to be generating a 250px preview
// from.
const (
	image_decode_pixels_maximum = 100_000_000
	image_file_bytes_maximum    = 100 << 20
)

// variant_create generates the variant beside the original, in a per-variant
// subdirectory. Kept for callers that serve variants from owned storage.
func variant_create(path string, variant string) (string, error) {
	dir, file := filepath.Split(path)
	thumb := dir + variant + "s/" + variant_name(file, variant)
	if file_exists(thumb) {
		return thumb, nil
	}
	return variant_render(path, variant, thumb)
}

// variant_render decodes the image at source, downscales it for the named
// variant, and writes it to destination (atomically, via a temporary). The
// decode is guarded against decompression bombs. Returns "" without error for
// a format it does not re-encode.
func variant_render(path string, variant string, thumb string) (string, error) {
	tmp := thumb + ".tmp"

	// Clean up any leftover temp file from a previous failed attempt
	_ = os.Remove(tmp)

	f, err := os.Open(path)
	if err != nil {
		warn("Unable to open image file %q to create %s variant: %v", path, variant, err)
		return "", err
	}
	defer f.Close()

	// Refuse an oversized file before reading it. Stat rather than a limited
	// read: the size is known from the open handle, so nothing is read at all,
	// and asking the handle rather than the path leaves no window for the file
	// to change between the check and the read.
	if information, err := f.Stat(); err == nil && information.Size() > image_file_bytes_maximum {
		info("Refusing to read image file %q for %s variant: %d bytes exceeds file cap", path, variant, information.Size())
		return "", nil
	}

	// Read the file into memory so we can inspect EXIF and decode the image
	b, err := io.ReadAll(f)
	if err != nil {
		info("Unable to decode image file %q to create %s variant: %v", path, variant, err)
		return "", err
	}

	// Try to parse EXIF orientation (best-effort)
	var orientation int
	if ex, err := exif.Decode(bytes.NewReader(b)); err == nil && ex != nil {
		if tag, err := ex.Get(exif.Orientation); err == nil && tag != nil {
			if iv, err := tag.Int(0); err == nil {
				orientation = iv
			}
		}
	}

	// Reject a decompression bomb before decoding allocates for it: the config
	// header carries the dimensions cheaply, so an oversized image is refused
	// without ever holding its pixels in memory.
	if config, _, err := image.DecodeConfig(bytes.NewReader(b)); err == nil {
		if int64(config.Width)*int64(config.Height) > image_decode_pixels_maximum {
			info("Refusing to decode image file %q for %s variant: %dx%d exceeds pixel cap", path, variant, config.Width, config.Height)
			return "", nil
		}
	}

	i, format, err := image.Decode(bytes.NewReader(b))
	if err != nil {
		// A decode failure reflects the input bytes (corrupt/truncated upload, or
		// an image-extensioned file in a format the decoder doesn't support), not
		// an admin-actionable server fault, so info() rather than warn() (no email).
		info("Unable to decode image file %q to create %s variant: %v", path, variant, err)
		return "", err
	}

	// Fix orientation according to EXIF tag before resizing
	if orientation != 0 {
		i = fix_orientation(i, orientation)
	}

	size := variant_size(variant)
	t := resize.Thumbnail(size, size, i, resize.Lanczos3)

	if err := os.MkdirAll(filepath.Dir(thumb), 0755); err != nil {
		warn("Unable to create %s variant directory %q: %v", variant, filepath.Dir(thumb), err)
		return "", err
	}

	o, err := os.Create(tmp)
	if err != nil {
		warn("Unable to create temp %s variant file %q: %v", variant, tmp, err)
		return "", err
	}
	// ensure tmp is closed and removed on error
	close_and_remove_tmp := func(remove bool) {
		_ = o.Close()
		if remove {
			_ = os.Remove(tmp)
		}
	}

	switch format {
	case "gif":
		err = gif.Encode(o, t, nil)
	case "jpeg":
		err = jpeg.Encode(o, t, &jpeg.Options{Quality: 80})
	case "png":
		err = png.Encode(o, t)
	case "webp":
		err = imaging.Encode(o, t, imaging.JPEG, imaging.JPEGQuality(80))
	default:
		close_and_remove_tmp(true)
		return "", nil
	}

	if err != nil {
		close_and_remove_tmp(true)
		info("Unable to encode image file %q to create %s variant: %v", path, variant, err)
		return "", err
	}

	if err := o.Close(); err != nil {
		_ = os.Remove(tmp)
		info("Unable to close %s variant file %q: %v", variant, tmp, err)
		return "", err
	}

	if err := os.Rename(tmp, thumb); err != nil {
		_ = os.Remove(tmp)
		info("Unable to move %s variant into place %q: %v", variant, thumb, err)
		return "", err
	}

	return thumb, nil
}

// api_image is the mochi.image namespace.
var api_image = sls.FromStringDict(sl.String("mochi.image"), sl.StringDict{
	"variant": sl.NewBuiltin("mochi.image.variant", api_image_variant),
})

// mochi.image.variant(file, kind) -> name or None: Generate a downscaled
// variant ("thumbnail" or "preview") of an image in the app's file storage,
// returning a cache entry name for a.write.cache / e.write.cache. Variants are
// re-computable, so they live in cache space and may be evicted; the next call
// regenerates. Returns None for a non-image, a missing source, or a decode
// failure. The decode is capped against decompression bombs.
func api_image_variant(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file, kind string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "kind", &kind); err != nil {
		return sl_error(fn, "syntax: variant(file, kind)")
	}
	if !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}
	if kind != "thumbnail" && kind != "preview" {
		return sl_error(fn, "kind must be thumbnail or preview")
	}
	if !is_image(file) {
		return sl.None, nil
	}

	user, err := principal_storage(t)
	if err != nil || user == nil {
		return sl_error(fn, "no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	source := filepath.Join(api_file_base(user, app), file)
	if !file_exists(source) {
		return sl.None, nil
	}

	name := "variants/" + variant_name(filepath.Base(file), kind)
	destination, err := cache_file(t, name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if file_exists(destination) {
		return sl.String(name), nil
	}
	if result, err := variant_render(source, kind, destination); err != nil || result == "" {
		return sl.None, nil
	}
	return sl.String(name), nil
}

func variant_name(name string, variant string) string {
	ext := filepath.Ext(name)
	return strings.TrimSuffix(name, ext) + "_" + variant + ext
}

func fix_orientation(img image.Image, orientation int) image.Image {
	switch orientation {
	case 2:
		return imaging.FlipH(img)
	case 3:
		return imaging.Rotate180(img)
	case 4:
		return imaging.FlipV(img)
	case 5:
		// transpose: rotate 270 then flip horizontal (EXIF semantics)
		return imaging.FlipH(imaging.Rotate270(img))
	case 6:
		// rotate 90 CW (EXIF semantics) -> use Rotate270 to achieve 90 CW
		return imaging.Rotate270(img)
	case 7:
		// transverse: rotate 90 then flip horizontal (EXIF semantics)
		return imaging.FlipH(imaging.Rotate90(img))
	case 8:
		// rotate 270 CW (EXIF semantics) -> use Rotate90 to achieve 270 CW
		return imaging.Rotate90(img)
	default:
		return img
	}
}
