// Mochi server: Images unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// Test is_image function
func TestIsImage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Valid image extensions
		{"gif", "image.gif", true},
		{"jpeg", "photo.jpeg", true},
		{"jpg", "photo.jpg", true},
		{"png", "image.png", true},
		{"webp", "image.webp", true},

		// Invalid/non-image extensions
		{"pdf", "document.pdf", false},
		{"txt", "readme.txt", false},
		{"html", "page.html", false},
		{"mp4", "video.mp4", false},
		{"doc", "document.doc", false},

		// Edge cases
		{"no extension", "README", false},
		{"empty string", "", false},
		{"dot only", ".", false},
		{"hidden file", ".gitignore", false},

		// Path with image extension
		{"path with png", "/path/to/image.png", true},
		{"path with jpg", "folder/subfolder/photo.jpg", true},

		// Case is folded: a name is whatever the user's filesystem allowed, and
		// the attachments library already lowercases - an exact-case switch here
		// made it advertise a thumbnail this refused to render.
		{"uppercase PNG", "image.PNG", true},
		{"uppercase JPG", "photo.JPG", true},
		{"mixed case Png", "image.Png", true},
		{"uppercase non-image", "document.PDF", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := is_image(tt.input)
			if result != tt.expected {
				t.Errorf("is_image(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// Test variant_name function
func TestVariantName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		variant  string
		expected string
	}{
		{"simple png", "image.png", "thumbnail", "image_thumbnail.png"},
		{"simple jpg", "photo.jpg", "thumbnail", "photo_thumbnail.jpg"},
		{"simple gif", "animation.gif", "thumbnail", "animation_thumbnail.gif"},
		{"with spaces", "my photo.jpg", "thumbnail", "my photo_thumbnail.jpg"},
		{"with underscores", "my_image.png", "thumbnail", "my_image_thumbnail.png"},
		{"with dashes", "my-image.png", "thumbnail", "my-image_thumbnail.png"},
		{"multiple dots", "file.name.png", "thumbnail", "file.name_thumbnail.png"},
		{"no extension", "README", "thumbnail", "README_thumbnail"},
		{"empty string", "", "thumbnail", "_thumbnail"},
		{"preview png", "image.png", "preview", "image_preview.png"},
		{"preview jpg", "photo.jpg", "preview", "photo_preview.jpg"},
		{"medium png", "image.png", "medium", "image_medium.png"},
		{"medium jpg", "photo.jpg", "medium", "photo_medium.jpg"},

		// A WebP source's variants are JPEG, and named for it.
		{"webp thumbnail", "photo.webp", "thumbnail", "photo_thumbnail.jpg"},
		{"webp medium", "photo.webp", "medium", "photo_medium.jpg"},
		{"webp preview", "photo.webp", "preview", "photo_preview.jpg"},
		{"uppercase webp", "PHOTO.WEBP", "preview", "PHOTO_preview.jpg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := variant_name(tt.input, tt.variant)
			if result != tt.expected {
				t.Errorf("variant_name(%q, %q) = %q, want %q", tt.input, tt.variant, result, tt.expected)
			}
		})
	}
}

// Test fix_orientation function
func TestFixOrientation(t *testing.T) {
	// Create a simple 10x5 test image (wider than tall)
	img := image.NewRGBA(image.Rect(0, 0, 10, 5))
	// Fill with a recognizable pattern - top-left red, rest blue
	for y := 0; y < 5; y++ {
		for x := 0; x < 10; x++ {
			if x == 0 && y == 0 {
				img.Set(x, y, color.RGBA{255, 0, 0, 255}) // Red
			} else {
				img.Set(x, y, color.RGBA{0, 0, 255, 255}) // Blue
			}
		}
	}

	tests := []struct {
		name         string
		orientation  int
		check_bounds func(image.Image) bool
	}{
		{
			name:        "orientation 0 (no change)",
			orientation: 0,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				return b.Dx() == 10 && b.Dy() == 5
			},
		},
		{
			name:        "orientation 1 (no change)",
			orientation: 1,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				return b.Dx() == 10 && b.Dy() == 5
			},
		},
		{
			name:        "orientation 2 (flip horizontal)",
			orientation: 2,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				return b.Dx() == 10 && b.Dy() == 5
			},
		},
		{
			name:        "orientation 3 (rotate 180)",
			orientation: 3,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				return b.Dx() == 10 && b.Dy() == 5
			},
		},
		{
			name:        "orientation 4 (flip vertical)",
			orientation: 4,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				return b.Dx() == 10 && b.Dy() == 5
			},
		},
		{
			name:        "orientation 5 (transpose)",
			orientation: 5,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				// Transpose swaps dimensions
				return b.Dx() == 5 && b.Dy() == 10
			},
		},
		{
			name:        "orientation 6 (rotate 90 CW)",
			orientation: 6,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				// Rotation swaps dimensions
				return b.Dx() == 5 && b.Dy() == 10
			},
		},
		{
			name:        "orientation 7 (transverse)",
			orientation: 7,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				// Transverse swaps dimensions
				return b.Dx() == 5 && b.Dy() == 10
			},
		},
		{
			name:        "orientation 8 (rotate 270 CW)",
			orientation: 8,
			check_bounds: func(i image.Image) bool {
				b := i.Bounds()
				// Rotation swaps dimensions
				return b.Dx() == 5 && b.Dy() == 10
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fix_orientation(img, tt.orientation)
			if result == nil {
				t.Error("fix_orientation returned nil")
				return
			}
			if !tt.check_bounds(result) {
				b := result.Bounds()
				t.Errorf("fix_orientation bounds check failed: got %dx%d", b.Dx(), b.Dy())
			}
		})
	}
}

// Benchmark is_image
func BenchmarkIsImage(b *testing.B) {
	inputs := []string{
		"image.png",
		"photo.jpg",
		"document.pdf",
		"README",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		is_image(inputs[i%len(inputs)])
	}
}

// TestVariantRender renders both variants of a large image to a named
// destination, bounded to the variant's box, atomically.
func TestVariantRender(t *testing.T) {
	// Create temp directory
	tmp_dir := t.TempDir()

	// Create a test image (1500x900 PNG, larger than both variant sizes)
	img := image.NewRGBA(image.Rect(0, 0, 1500, 900))
	for y := 0; y < 900; y++ {
		for x := 0; x < 1500; x++ {
			img.Set(x, y, color.RGBA{uint8(x % 256), uint8(y % 256), 128, 255})
		}
	}

	img_path := filepath.Join(tmp_dir, "test_image.png")
	f, err := os.Create(img_path)
	if err != nil {
		t.Fatalf("Failed to create test image file: %v", err)
	}
	if err := png.Encode(f, img); err != nil {
		f.Close()
		t.Fatalf("Failed to encode test image: %v", err)
	}
	f.Close()

	for _, tt := range []struct {
		variant string
		size    int
	}{
		{"thumbnail", 250},
		{"medium", 640},
		{"preview", 1280},
	} {
		expected := filepath.Join(tmp_dir, "variants", variant_name("test_image.png", tt.variant))
		thumb_path, err := variant_render(img_path, tt.variant, expected)
		if err != nil {
			t.Fatalf("variant_render(%s) failed: %v", tt.variant, err)
		}
		if thumb_path != expected {
			t.Errorf("Expected %s path %q, got %q", tt.variant, expected, thumb_path)
		}

		// Verify file exists
		if _, err := os.Stat(thumb_path); os.IsNotExist(err) {
			t.Fatalf("%s file was not created", tt.variant)
		}

		// Verify dimensions fit the variant's bounding box
		thumb_f, err := os.Open(thumb_path)
		if err != nil {
			t.Fatalf("Failed to open %s: %v", tt.variant, err)
		}
		thumb_img, format, err := image.Decode(thumb_f)
		thumb_f.Close()
		if err != nil {
			t.Fatalf("Failed to decode %s: %v", tt.variant, err)
		}
		if format != "png" {
			t.Errorf("%s of a PNG is %s; PNG variants stay lossless PNG", tt.variant, format)
		}

		bounds := thumb_img.Bounds()
		if bounds.Dx() > tt.size || bounds.Dy() > tt.size {
			t.Errorf("%s too large: %dx%d", tt.variant, bounds.Dx(), bounds.Dy())
		}
		// The longest side reaches the box: a kind rendering at another kind's
		// size passes the bound above and still serves the wrong image.
		if bounds.Dx() != tt.size {
			t.Errorf("%s is %d wide; a 1500x900 source scales to the box's %d", tt.variant, bounds.Dx(), tt.size)
		}
		if bounds.Dx() == 0 || bounds.Dy() == 0 {
			t.Errorf("%s has zero dimension", tt.variant)
		}

		// Verify no temp file left behind
		if left, _ := filepath.Glob(thumb_path + ".*"); len(left) > 0 {
			t.Errorf("Temp files %v should not exist", left)
		}
	}
}

// Benchmark variant_name
func BenchmarkVariantName(b *testing.B) {
	inputs := []string{
		"image.png",
		"my photo with spaces.jpg",
		"complex.file.name.gif",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		variant_name(inputs[i%len(inputs)], "thumbnail")
	}
}

// image_variant_thread is a thread whose storage and app point at fresh
// directories, for driving api_image_variant end to end.
func image_variant_thread(t *testing.T) (*sl.Thread, *User, *App) {
	t.Helper()
	original_data, original_cache := data_dir, cache_dir
	data_dir, cache_dir = t.TempDir(), t.TempDir()
	t.Cleanup(func() { data_dir, cache_dir = original_data, original_cache })
	user := &User{UID: "user-one"}
	app := &App{id: "app-one"}
	thread := &sl.Thread{}
	thread.SetLocal("storage", user)
	thread.SetLocal("app", app)
	return thread, user, app
}

// image_variant_source writes bytes as file in the app's storage.
func image_variant_source(t *testing.T, user *User, app *App, file string, data []byte) {
	t.Helper()
	path := filepath.Join(api_file_base(user, app), file)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// image_variant_call runs mochi.image.variant and decodes the entry it names,
// answering the entry name, the decoded format and the image.
func image_variant_call(t *testing.T, thread *sl.Thread, file string, kind string) (string, string, image.Image) {
	t.Helper()
	value, err := api_image_variant(thread, sl.NewBuiltin("mochi.image.variant", api_image_variant), sl.Tuple{sl.String(file), sl.String(kind)}, nil)
	if err != nil {
		t.Fatalf("variant(%q, %q): %v", file, kind, err)
	}
	name, ok := sl.AsString(value)
	if !ok || name == "" {
		t.Fatalf("variant(%q, %q) returned %v, want a cache entry name", file, kind, value)
	}
	path, err := cache_file(thread, name)
	if err != nil {
		t.Fatalf("cache_file(%q): %v", name, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", name, err)
	}
	decoded, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decoding %s: %v", name, err)
	}
	return name, format, decoded
}

// image_variant_solid encodes a solid 1500x900 image as png or jpeg.
func image_variant_solid(t *testing.T, format string) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 1500, 900))
	for y := 0; y < 900; y++ {
		for x := 0; x < 1500; x++ {
			img.Set(x, y, color.RGBA{uint8(x % 256), uint8(y % 256), 128, 255})
		}
	}
	buffer := &bytes.Buffer{}
	var err error
	if format == "jpeg" {
		err = jpeg.Encode(buffer, img, nil)
	} else {
		err = png.Encode(buffer, img)
	}
	if err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// TestImageMediumKeepsTheSourceFormat: the medium kind is 640 pixels on its
// longest side, and a PNG's stays a PNG and a JPEG's a JPEG, named for it.
func TestImageMediumKeepsTheSourceFormat(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	for _, c := range []struct{ file, format, name string }{
		{"listing.png", "png", "variants/listing_medium.png"},
		{"listing.jpg", "jpeg", "variants/listing_medium.jpg"},
	} {
		image_variant_source(t, user, app, c.file, image_variant_solid(t, c.format))
		name, format, decoded := image_variant_call(t, thread, c.file, "medium")
		if name != c.name {
			t.Errorf("the medium of %s is named %q, want %q", c.file, name, c.name)
		}
		if format != c.format {
			t.Errorf("THE POINT: the medium of %s is %s, want %s", c.file, format, c.format)
		}
		if bounds := decoded.Bounds(); bounds.Dx() != medium_size || bounds.Dy() > medium_size {
			t.Errorf("the medium of %s is %dx%d, want %d on its longest side", c.file, bounds.Dx(), bounds.Dy(), medium_size)
		}
		if got := file_name_type(name); got != "image/"+c.format {
			t.Errorf("the medium of %s would be served as %s", c.file, got)
		}
	}
}

// image_variant_webp is a 400x300 lossless WebP whose left half is transparent
// and right half opaque blue.
const image_variant_webp = "UklGRjAAAABXRUJQVlA4TCQAAAAvj8FKEA8wKEMx3PMf8CCTtql/03urYI3o/wTgz89/+S//HU0="

// TestImageWebpVariantIsAJpegNamedForIt: a WebP source decodes, and its
// variants are JPEG bytes under a .jpg name, so every path that types a
// variant by its name serves image/jpeg - not image/webp over JPEG bytes. Its
// transparency lands on white, not black.
func TestImageWebpVariantIsAJpegNamedForIt(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	data, err := base64.StdEncoding.DecodeString(image_variant_webp)
	if err != nil {
		t.Fatal(err)
	}
	image_variant_source(t, user, app, "sticker.webp", data)

	name, format, decoded := image_variant_call(t, thread, "sticker.webp", "thumbnail")
	if name != "variants/sticker_thumbnail.jpg" {
		t.Errorf("THE POINT: the variant of a WebP is named %q, want variants/sticker_thumbnail.jpg", name)
	}
	if format != "jpeg" {
		t.Errorf("the variant of a WebP is %s, want jpeg", format)
	}
	if got := file_name_type(name); got != "image/jpeg" {
		t.Errorf("THE POINT: the variant of a WebP would be served as %s", got)
	}

	bounds := decoded.Bounds()
	left := color.RGBAModel.Convert(decoded.At(bounds.Dx()/4, bounds.Dy()/2)).(color.RGBA)
	right := color.RGBAModel.Convert(decoded.At(bounds.Dx()*3/4, bounds.Dy()/2)).(color.RGBA)
	if left.R < 240 || left.G < 240 || left.B < 240 {
		t.Errorf("the transparent half rendered %v, want white", left)
	}
	if right.B < 180 || right.R > 60 {
		t.Errorf("the opaque half rendered %v, want blue", right)
	}
}

// TestImageVariantEncodesAsItsName: bytes that are not what their name says
// are re-encoded as the name says, so the entry's name and its bytes agree. A
// JPEG uploaded as photo.png renders a PNG preview.
func TestImageVariantEncodesAsItsName(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	image_variant_source(t, user, app, "photo.png", image_variant_solid(t, "jpeg"))
	name, format, _ := image_variant_call(t, thread, "photo.png", "preview")
	if name != "variants/photo_preview.png" || format != "png" {
		t.Errorf("JPEG bytes named photo.png rendered %s as %s, want a PNG", name, format)
	}
}

// TestImageKinds: mochi.image.kinds answers the kinds variant renders, which
// is how an app learns this core renders a medium; variant refuses any other.
func TestImageKinds(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	value, err := api_image_kinds(thread, sl.NewBuiltin("mochi.image.kinds", api_image_kinds), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := value.String(); got != `("thumbnail", "medium", "preview")` {
		t.Errorf("mochi.image.kinds() = %s", got)
	}
	image_variant_source(t, user, app, "listing.png", image_variant_solid(t, "png"))
	if _, err := api_image_variant(thread, sl.NewBuiltin("mochi.image.variant", api_image_variant), sl.Tuple{sl.String("listing.png"), sl.String("huge")}, nil); err == nil {
		t.Error("an unknown kind was accepted")
	}
}

// image_variant_colour is a 300x200 image of one colour, encoded as format.
func image_variant_colour(t *testing.T, format string, fill color.RGBA) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 300, 200))
	for y := 0; y < 200; y++ {
		for x := 0; x < 300; x++ {
			img.Set(x, y, fill)
		}
	}
	buffer := &bytes.Buffer{}
	var err error
	if format == "jpeg" {
		err = jpeg.Encode(buffer, img, nil)
	} else {
		err = png.Encode(buffer, img)
	}
	if err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// image_variant_later moves a source's time past the variant made of the
// file it replaced, as a later write does.
func image_variant_later(t *testing.T, user *User, app *App, file string) {
	t.Helper()
	later := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(api_file_base(user, app), file), later, later); err != nil {
		t.Fatal(err)
	}
}

// TestImageVariantFollowsItsSource. A variant made before its source was
// written again is of the image the source replaced, and is made again: a
// contact's photo is fetched into one name per contact, in the format the far
// end sends, and its thumbnail kept showing the first photo for good. WebP and
// JPEG sources share their variants' name, so a change of format too.
func TestImageVariantFollowsItsSource(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	red := color.RGBA{255, 0, 0, 255}
	green := color.RGBA{0, 255, 0, 255}
	centre := func(decoded image.Image) color.RGBA {
		bounds := decoded.Bounds()
		return color.RGBAModel.Convert(decoded.At(bounds.Dx()*3/4, bounds.Dy()/2)).(color.RGBA)
	}

	image_variant_source(t, user, app, "photos/c/source.png", image_variant_colour(t, "png", red))
	if _, _, decoded := image_variant_call(t, thread, "photos/c/source.png", "thumbnail"); centre(decoded).R < 200 {
		t.Fatalf("setup: the first thumbnail is %v, want red", centre(decoded))
	}
	image_variant_source(t, user, app, "photos/c/source.png", image_variant_colour(t, "png", green))
	image_variant_later(t, user, app, "photos/c/source.png")
	if _, _, decoded := image_variant_call(t, thread, "photos/c/source.png", "thumbnail"); centre(decoded).G < 200 || centre(decoded).R > 60 {
		t.Errorf("THE POINT: the thumbnail of a source written again is %v, want its new green", centre(decoded))
	}

	image_variant_source(t, user, app, "photos/d/source.jpg", image_variant_colour(t, "jpeg", red))
	if _, _, decoded := image_variant_call(t, thread, "photos/d/source.jpg", "thumbnail"); centre(decoded).R < 200 {
		t.Fatalf("setup: the JPEG's thumbnail is %v, want red", centre(decoded))
	}
	if err := os.Remove(filepath.Join(api_file_base(user, app), "photos/d/source.jpg")); err != nil {
		t.Fatal(err)
	}
	data, err := base64.StdEncoding.DecodeString(image_variant_webp)
	if err != nil {
		t.Fatal(err)
	}
	image_variant_source(t, user, app, "photos/d/source.webp", data)
	image_variant_later(t, user, app, "photos/d/source.webp")
	if _, _, decoded := image_variant_call(t, thread, "photos/d/source.webp", "thumbnail"); centre(decoded).B < 180 || centre(decoded).R > 60 {
		t.Errorf("THE POINT: the thumbnail of a WebP that replaced a JPEG is %v, want the WebP's blue", centre(decoded))
	}
}
