// Mochi: Images
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/disintegration/imaging"
	"github.com/nfnt/resize"
	"github.com/rwcarlsen/goexif/exif"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
	_ "golang.org/x/image/webp"
	"image"
	"image/draw"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func is_image(file string) bool {
	// Folded, because a name is whatever the user's filesystem allowed and
	// PHOTO.PNG is a PNG. The attachments library already lowercases, so an
	// exact-case switch here made it advertise a thumbnail this refused to
	// render and the route then served the full-size original instead.
	ext := strings.ToLower(filepath.Ext(file))

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
// chips and lists; mediums suit card and gallery tiles, which a 1280 pixel
// preview oversizes several times over; previews suit full-width views on
// high-density displays.
const (
	thumbnail_size = 250
	medium_size    = 640
	preview_size   = 1280
)

// image_kinds lists the variants mochi.image.variant renders, smallest first.
var image_kinds = []string{"thumbnail", "medium", "preview"}

func variant_size(variant string) uint {
	switch variant {
	case "medium":
		return medium_size
	case "preview":
		return preview_size
	}
	return thumbnail_size
}

// image_kind reports whether kind is a variant mochi.image.variant renders.
func image_kind(kind string) bool {
	for _, k := range image_kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// Bound the memory one variant render can hold: the file cap covers the
// compressed bytes, the pixel cap what they decode to. DecodeConfig is
// consulted before image.Decode, so a small file declaring huge dimensions
// cannot allocate.
const (
	image_decode_pixels_maximum = 100_000_000
	image_file_bytes_maximum    = 100 << 20
)

// image_render_parallel bounds concurrent decodes. One render can hold 100 MB
// of file bytes plus 400 MB of pixels, and the HTTP path decodes in the request
// goroutine - so a slot here is worth half a gigabyte, not a goroutine.
const image_render_parallel = 4

// image_render_wait is how long a request waits for a slot before giving up on
// the variant. A var so tests need not wait it out.
var image_render_wait = 30 * time.Second

var image_render_slots = make(chan struct{}, image_render_parallel)

// image_render_acquire takes a decode slot, returning the release. On error the
// caller reports no variant, so a busy server degrades to full-size images.
func image_render_acquire() (func(), error) {
	timer := time.NewTimer(image_render_wait)
	defer timer.Stop()
	select {
	case image_render_slots <- struct{}{}:
		return func() { <-image_render_slots }, nil
	case <-timer.C:
		return nil, fmt.Errorf("image render pool busy")
	}
}

// image_decode decodes an image. A JPEG with no end marker, its data running
// straight into a trailer some phone cameras append, is decoded as if the
// marker were there: browsers show it whole, where the decoder alone reads on
// through the trailer and fails.
func image_decode(b []byte) (image.Image, error) {
	i, format, err := image.Decode(bytes.NewReader(b))
	if errors.Is(err, io.ErrUnexpectedEOF) && format == "jpeg" {
		i, _, err = image.Decode(io.MultiReader(bytes.NewReader(b), bytes.NewReader([]byte{0xff, 0xd9})))
	}
	return i, err
}

// variant_render decodes the image at source, downscales it for the named
// variant, and writes it to one of destinations (atomically, via a temporary),
// returning which. The first destination's extension chooses the encoder; an
// opaque PNG given a second, JPEG, destination is kept there instead when JPEG
// encodes it smaller (variant_names). The decode is guarded against
// decompression bombs. Returns "" without error for a format it does not
// re-encode.
func variant_render(path string, variant string, destinations ...string) (string, error) {
	thumb := destinations[0]
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

	// Held from here, not from the open: everything above is a stat and a
	// refusal, so an oversized file is turned away without ever occupying a
	// slot that a renderable image could use.
	release, err := image_render_acquire()
	if err != nil {
		info("Deferring %s variant for %q: %v", variant, path, err)
		return "", nil
	}
	defer release()

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

	i, err := image_decode(b)
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

	// A temporary of its own: two renders of one destination at once used to
	// share one name, so the second deleted the first's and the first moved
	// the second's half-written file into place. Named .partial so the cache
	// counts it as a write in flight, and a crash's leftover ages out.
	o, err := os.CreateTemp(filepath.Dir(thumb), filepath.Base(thumb)+".*.partial")
	if err != nil {
		warn("Unable to create temp %s variant file for %q: %v", variant, thumb, err)
		return "", err
	}
	tmp := o.Name()
	// The mode os.Create gave the variant before; a temporary is made 0600.
	_ = o.Chmod(0o644)

	// The destination's extension chooses the encoder, never the format the
	// source decoded as: the names are fixed before the render, and serving
	// paths type the variant by the one it is kept under. A source whose bytes
	// are not what its name says is re-encoded as the name says.
	counted := &variant_counter{writer: o}
	switch strings.ToLower(filepath.Ext(thumb)) {
	case ".gif":
		err = gif.Encode(counted, t, nil)
	case ".jpg", ".jpeg":
		err = jpeg.Encode(counted, variant_opaque(t), &jpeg.Options{Quality: 80})
	case ".png":
		err = png.Encode(counted, t)
	default:
		_ = o.Close()
		_ = os.Remove(tmp)
		return "", nil
	}
	if err == nil {
		err = o.Close()
	} else {
		_ = o.Close()
	}
	if err != nil {
		_ = os.Remove(tmp)
		info("Unable to encode image file %q to create %s variant: %v", path, variant, err)
		return "", err
	}

	// An opaque PNG may instead be kept as a JPEG under its second name: a
	// photograph's JPEG is a tenth of its PNG, while flat graphics and text are
	// smaller, and crisper, as PNG. JPEG wins only by a clear margin, since Go's
	// encoder subsamples colour and smears coloured text and edges.
	// Transparency stays PNG.
	if len(destinations) > 1 && strings.EqualFold(filepath.Ext(thumb), ".png") && variant_is_opaque(t) {
		var lossy bytes.Buffer
		if jpeg.Encode(&lossy, t, &jpeg.Options{Quality: 80}) == nil && variant_lossy_worth(counted.written, int64(lossy.Len())) {
			if other, err := variant_temporary(destinations[1], lossy.Bytes()); err == nil {
				_ = os.Remove(tmp)
				tmp, thumb = other, destinations[1]
			} else {
				info("Unable to write %s variant file for %q: %v", variant, destinations[1], err)
			}
		}
	}

	if err := os.Rename(tmp, thumb); err != nil {
		_ = os.Remove(tmp)
		info("Unable to move %s variant into place %q: %v", variant, thumb, err)
		return "", err
	}

	return thumb, nil
}

// variant_counter counts the bytes an encoder writes through it.
type variant_counter struct {
	writer  io.Writer
	written int64
}

func (c *variant_counter) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	c.written += int64(n)
	return n, err
}

// variant_lossy_worth reports whether a JPEG of lossy bytes saves enough over a
// PNG of lossless bytes to be kept instead: at most three quarters of it.
func variant_lossy_worth(lossless int64, lossy int64) bool {
	return lossy*4 <= lossless*3
}

// variant_temporary writes data to a temporary of its own beside destination,
// returning its path.
func variant_temporary(destination string, data []byte) (string, error) {
	o, err := os.CreateTemp(filepath.Dir(destination), filepath.Base(destination)+".*.partial")
	if err != nil {
		return "", err
	}
	_ = o.Chmod(0o644)
	if _, err := o.Write(data); err != nil {
		_ = o.Close()
		_ = os.Remove(o.Name())
		return "", err
	}
	if err := o.Close(); err != nil {
		_ = os.Remove(o.Name())
		return "", err
	}
	return o.Name(), nil
}

// variant_is_opaque reports whether an image has no transparent pixel, so a
// format without an alpha channel loses nothing of it.
func variant_is_opaque(i image.Image) bool {
	o, ok := i.(interface{ Opaque() bool })
	return ok && o.Opaque()
}

// variant_opaque composites an image with transparency onto white for an
// encoder with no alpha channel: JPEG would otherwise write a transparent
// WebP's clear pixels as black.
func variant_opaque(i image.Image) image.Image {
	if variant_is_opaque(i) {
		return i
	}
	canvas := image.NewRGBA(i.Bounds())
	draw.Draw(canvas, canvas.Bounds(), image.White, image.Point{}, draw.Src)
	draw.Draw(canvas, canvas.Bounds(), i, i.Bounds().Min, draw.Over)
	return canvas
}

// api_image is the mochi.image namespace.
var api_image = sls.FromStringDict(sl.String("mochi.image"), sl.StringDict{
	"kinds":   sl.NewBuiltin("mochi.image.kinds", api_image_kinds),
	"variant": sl.NewBuiltin("mochi.image.variant", api_image_variant),
})

// mochi.image.kinds() -> tuple: The variant kinds mochi.image.variant renders,
// smallest first. An app asking for a kind a core added later checks for it
// here, since variant raises for a kind it does not know.
func api_image_kinds(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := sl.UnpackArgs(fn.Name(), args, kwargs); err != nil {
		return sl_error(fn, "syntax: kinds()")
	}
	return sl_encode(image_kinds), nil
}

// mochi.image.variant(file, kind) -> name or None: Generate a downscaled
// variant ("thumbnail", "medium" or "preview") of an image in the app's file
// storage, returning a cache entry name for a.write.cache / e.write.cache. The
// name ends in the extension of the format the variant is encoded in: the
// source's own, except that a WebP's variants are JPEG and an opaque PNG's are
// JPEG where that is clearly smaller. Variants are re-computable, so they
// live in cache space and may be evicted; the next call regenerates. Returns
// None for a non-image, a missing source, or a decode failure. The decode is
// capped against decompression bombs.
func api_image_variant(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file, kind string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "kind", &kind); err != nil {
		return sl_error(fn, "syntax: variant(file, kind)")
	}
	if !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}
	if !image_kind(kind) {
		return sl_error(fn, "kind must be one of %s", strings.Join(image_kinds, ", "))
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
	original, err := os.Stat(source)
	if err != nil || original.IsDir() {
		return sl.None, nil
	}

	// The whole relative path, not its base: two images with the same file name in
	// different directories are different images. lib/starlark/attachments.star
	// derives these same names to invalidate the entry, so they must not move.
	names := variant_names(file, kind)
	destinations := make([]string, len(names))
	for i, name := range names {
		destination, err := cache_file(t, "variants/"+name)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
		destinations[i] = destination
	}
	// A variant made before its source was last written is of the image the
	// source replaced - a contact's photo fetched again into the same name,
	// in the same format or another whose variants share it (WebP's are JPEG)
	// - and is made again. A read moves an entry's time on; callers ask for
	// the variant before they read it.
	if index := variant_fresh(destinations, original.ModTime()); index >= 0 {
		return sl.String("variants/" + names[index]), nil
	}
	index := variant_made(source, kind, destinations, original.ModTime())
	if index < 0 {
		return sl.None, nil
	}
	return sl.String("variants/" + names[index]), nil
}

// variant_fresh is the index of the first of destinations holding a variant
// made no earlier than its source was last written, or -1.
func variant_fresh(destinations []string, modified time.Time) int {
	for i, destination := range destinations {
		if information, err := os.Stat(destination); err == nil && !information.ModTime().Before(modified) {
			return i
		}
	}
	return -1
}

// variant_flight is a render of one variant in flight, which later callers for
// the same variant wait on instead of rendering it again: the index of the
// destination it was kept under, or -1.
type variant_flight struct {
	done chan struct{}
	made int
}

var (
	variant_flights      = map[string]*variant_flight{}
	variant_flights_lock sync.Mutex
)

// variant_made renders the variant of source at one of destinations, once
// however many callers ask for it together: the first renders it and the rest
// wait and share its outcome, the index of the destination it was kept under,
// or -1. The first viewers of a new photo, a gallery and its lightbox, or
// subscribers' servers pulling the variant ask at the same moment, and each
// used to render it again. A destination already newer than modified is made.
// The render removes what the other destinations hold, so a stale variant in a
// format it did not choose is never found again.
func variant_made(source string, kind string, destinations []string, modified time.Time) int {
	key := destinations[0]
	variant_flights_lock.Lock()
	if flight, ok := variant_flights[key]; ok {
		variant_flights_lock.Unlock()
		<-flight.done
		return flight.made
	}
	flight := &variant_flight{done: make(chan struct{}), made: -1}
	variant_flights[key] = flight
	variant_flights_lock.Unlock()
	defer func() {
		variant_flights_lock.Lock()
		delete(variant_flights, key)
		variant_flights_lock.Unlock()
		close(flight.done)
	}()

	// A render that finished between the caller's look and this one's turn.
	if index := variant_fresh(destinations, modified); index >= 0 {
		flight.made = index
		return index
	}
	var previous int64
	for _, destination := range destinations {
		if information, err := os.Stat(destination); err == nil {
			previous += information.Size()
		}
	}
	result, err := variant_render(source, kind, destinations...)
	if err != nil || result == "" {
		return -1
	}
	index := -1
	for i, destination := range destinations {
		if destination == result {
			index = i
		} else {
			_ = os.Remove(destination)
		}
	}
	// Adds bytes to the cache without going through cache_write_file, so it
	// accounts for them itself, less the stale entries it replaced. Once, by the
	// render that wrote them, not by each caller that waited on it.
	if information, err := os.Stat(result); err == nil {
		cache_admit(information.Size() - previous)
	}
	flight.made = index
	return index
}

// variant_names are the cache entry names a variant of the source name may be
// kept under, the one it takes when it has no choice first: the source's name
// with the kind spliced in ahead of its extension, followed by ".jpg" where the
// variant is a JPEG of another format. Every format re-encodes as itself
// except WebP, which Go can decode but not encode, so its variants are JPEG;
// an opaque PNG's are kept as a JPEG where that is clearly smaller. Keeping
// the source's extension in a converted name means no two sources in one
// directory - a.png and a.jpg, a.webp and a.jpg - ever share an entry.
// lib/starlark/attachments.star derives the same strings to invalidate them.
func variant_names(name string, variant string) []string {
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext) + "_" + variant
	switch strings.ToLower(ext) {
	case ".webp":
		return []string{stem + ext + ".jpg"}
	case ".png":
		return []string{stem + ext, stem + ext + ".jpg"}
	}
	return []string{stem + ext}
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
