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
	"image/draw"
	"image/jpeg"
	"image/png"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
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
		{"webp thumbnail", "photo.webp", "thumbnail", "photo_thumbnail.webp.jpg"},
		{"webp medium", "photo.webp", "medium", "photo_medium.webp.jpg"},
		{"webp preview", "photo.webp", "preview", "photo_preview.webp.jpg"},
		{"uppercase webp", "PHOTO.WEBP", "preview", "PHOTO_preview.WEBP.jpg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := variant_names(tt.input, tt.variant)[0]
			if result != tt.expected {
				t.Errorf("variant_names(%q, %q)[0] = %q, want %q", tt.input, tt.variant, result, tt.expected)
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
		expected := filepath.Join(tmp_dir, "variants", variant_names("test_image.png", tt.variant)[0])
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
		variant_names(inputs[i%len(inputs)], "thumbnail")
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
// longest side, and a flat PNG's stays a PNG and a JPEG's a JPEG, named for it.
func TestImageMediumKeepsTheSourceFormat(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	for _, c := range []struct{ file, format, name string }{
		{"listing.png", "png", "variants/listing_medium.png"},
		{"listing.jpg", "jpeg", "variants/listing_medium.jpg"},
	} {
		data := image_variant_solid(t, c.format)
		if c.format == "png" {
			data = variant_choice_image(t, "flat", 1500, 900)
		}
		image_variant_source(t, user, app, c.file, data)
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
	if name != "variants/sticker_thumbnail.webp.jpg" {
		t.Errorf("THE POINT: the variant of a WebP is named %q, want variants/sticker_thumbnail.webp.jpg", name)
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
// are re-encoded as a name the source's allows, so the entry's name and its
// bytes agree. A JPEG uploaded as photo.png renders a preview named for PNG or
// JPEG, holding that format; a JPEG uploaded as photo.gif renders a GIF.
func TestImageVariantEncodesAsItsName(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	for _, c := range []struct{ file string }{{"photo.png"}, {"photo.gif"}} {
		image_variant_source(t, user, app, c.file, image_variant_solid(t, "jpeg"))
		name, format, _ := image_variant_call(t, thread, c.file, "preview")
		named := map[string]string{".png": "png", ".jpg": "jpeg", ".gif": "gif"}[filepath.Ext(name)]
		allowed := false
		for _, candidate := range variant_names(c.file, "preview") {
			allowed = allowed || "variants/"+candidate == name
		}
		if !allowed || format != named {
			t.Errorf("JPEG bytes named %s rendered %s as %s, want the format its name says", c.file, name, format)
		}
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

// TestVariantRenderJPEGWithoutEndMarker: a JPEG whose data runs straight into
// a camera's trailer with no end marker, which browsers show whole, gets its
// variant, decoded whole rather than refused.
func TestVariantRenderJPEGWithoutEndMarker(t *testing.T) {
	directory := t.TempDir()
	source := image.NewRGBA(image.Rect(0, 0, 1500, 900))
	draw.Draw(source, source.Bounds(), &image.Uniform{color.RGBA{200, 30, 40, 255}}, image.Point{}, draw.Src)
	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, source, &jpeg.Options{Quality: 90}); err != nil {
		t.Fatal(err)
	}
	b := encoded.Bytes()
	if !bytes.HasSuffix(b, []byte{0xff, 0xd9}) {
		t.Fatalf("the encoded JPEG does not end with its end marker")
	}
	b = append(b[:len(b)-2:len(b)-2], []byte("\x00\x00Photo_Info\x00SEFH\x01\x00\x00\x00SEFT")...)
	path := filepath.Join(directory, "camera.jpg")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}

	expected := filepath.Join(directory, "variants", variant_names("camera.jpg", "preview")[0])
	made, err := variant_render(path, "preview", expected)
	if err != nil || made != expected {
		t.Fatalf("THE POINT: variant_render = %q, %v; want %q", made, err, expected)
	}
	f, err := os.Open(made)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	variant, _, err := image.Decode(f)
	if err != nil {
		t.Fatal(err)
	}
	// The last row is the source's colour, not the grey of data run out.
	bounds := variant.Bounds()
	r, g, _, _ := variant.At(bounds.Max.X/2, bounds.Max.Y-1).RGBA()
	if r>>8 < 150 || g>>8 > 80 {
		t.Errorf("the variant's last row = %d,%d; want the source's red", r>>8, g>>8)
	}
}

// variant_choice_image encodes a width x height PNG of one of three kinds:
// "photo", a gradient with grain, which JPEG encodes far smaller; "flat", solid
// blocks with thin lines, as a screenshot or diagram is, which PNG encodes
// smaller; "clear", the photo with a translucent corner.
func variant_choice_image(t *testing.T, kind string, width int, height int) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	grain := rand.New(rand.NewSource(86))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			switch kind {
			case "flat":
				shade := color.NRGBA{240, 240, 240, 255}
				if x < width/2 && y < height/2 {
					shade = color.NRGBA{30, 90, 200, 255}
				} else if x >= width/2 && y >= height/2 {
					shade = color.NRGBA{250, 200, 40, 255}
				}
				if y%40 < 3 && x%300 < 220 {
					shade = color.NRGBA{20, 20, 20, 255}
				}
				img.SetNRGBA(x, y, shade)
			default:
				noise := func(v int) uint8 { return uint8(max(0, min(255, v+grain.Intn(49)-24))) }
				alpha := uint8(255)
				if kind == "clear" && x < width/3 && y < height/3 {
					alpha = 128
				}
				img.SetNRGBA(x, y, color.NRGBA{noise(x * 255 / width), noise(y * 255 / height), noise(128), alpha})
			}
		}
	}
	buffer := &bytes.Buffer{}
	if err := png.Encode(buffer, img); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// TestVariantNamesGiveAnOpaquePngAChoice: a PNG's variant may be kept as a PNG
// or a JPEG, named for whichever it is; every other format has one name, a
// WebP's JPEG. A converted name keeps the source's extension, so it is no
// other source's.
func TestVariantNamesGiveAnOpaquePngAChoice(t *testing.T) {
	for _, c := range []struct {
		input string
		want  []string
	}{
		{"photos/cat.png", []string{"photos/cat_medium.png", "photos/cat_medium.png.jpg"}},
		{"CAT.PNG", []string{"CAT_medium.PNG", "CAT_medium.PNG.jpg"}},
		{"cat.jpg", []string{"cat_medium.jpg"}},
		{"cat.webp", []string{"cat_medium.webp.jpg"}},
		{"cat.gif", []string{"cat_medium.gif"}},
	} {
		if got := variant_names(c.input, "medium"); strings.Join(got, " ") != strings.Join(c.want, " ") {
			t.Errorf("THE POINT: variant_names(%q) = %v, want %v", c.input, got, c.want)
		}
	}
}

// TestVariantRenderKeepsTheSmaller: an opaque PNG's variant is kept as
// whichever of PNG and JPEG is smaller - a photograph as a JPEG, a flat
// graphic as a PNG - and one with transparency stays a PNG whatever the sizes.
func TestVariantRenderKeepsTheSmaller(t *testing.T) {
	directory := t.TempDir()
	size := func(path string) int64 {
		information, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return information.Size()
	}
	for _, c := range []struct {
		kind, format string
	}{
		{"photo", "jpeg"},
		{"flat", "png"},
		{"clear", "png"},
	} {
		source := filepath.Join(directory, c.kind+".png")
		if err := os.WriteFile(source, variant_choice_image(t, c.kind, 1500, 900), 0o644); err != nil {
			t.Fatal(err)
		}
		choice := []string{filepath.Join(directory, "choice", c.kind+"_medium.png"), filepath.Join(directory, "choice", c.kind+"_medium.jpg")}
		result, err := variant_render(source, "medium", choice...)
		if err != nil || result == "" {
			t.Fatalf("rendering %s: %q, %v", c.kind, result, err)
		}
		data, err := os.ReadFile(result)
		if err != nil {
			t.Fatal(err)
		}
		_, format, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		if format != c.format || filepath.Ext(result) != map[string]string{"jpeg": ".jpg", "png": ".png"}[c.format] {
			t.Errorf("THE POINT: the %s's medium was kept as %s at %s, want %s", c.kind, format, result, c.format)
		}
		// Each format alone, to show the choice took the smaller where it had one.
		png_only, err := variant_render(source, "medium", filepath.Join(directory, "png", c.kind+"_medium.png"))
		if err != nil {
			t.Fatal(err)
		}
		jpeg_only, err := variant_render(source, "medium", filepath.Join(directory, "jpeg", c.kind+"_medium.jpg"))
		if err != nil {
			t.Fatal(err)
		}
		as_png, as_jpeg := size(png_only), size(jpeg_only)
		switch c.kind {
		case "photo":
			if size(result) != as_jpeg || as_jpeg >= as_png {
				t.Errorf("the photo's medium is %d bytes; as PNG %d, as JPEG %d", size(result), as_png, as_jpeg)
			}
		case "flat":
			if size(result) != as_png || as_png >= as_jpeg {
				t.Errorf("the flat graphic's medium is %d bytes; as PNG %d, as JPEG %d", size(result), as_png, as_jpeg)
			}
		case "clear":
			if as_jpeg >= as_png {
				t.Errorf("the translucent photo is smaller as PNG (%d against %d), so its staying PNG proves nothing", as_png, as_jpeg)
			}
		}
	}
}

// TestImageVariantFollowsItsSourceAcrossFormats drives mochi.image.variant: a
// photograph's medium is a JPEG named for it, and when the source is replaced
// by a flat graphic the medium becomes a PNG and the JPEG goes, so no stale
// entry is ever found under the other name.
func TestImageVariantFollowsItsSourceAcrossFormats(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	image_variant_source(t, user, app, "listing.png", variant_choice_image(t, "photo", 1500, 900))
	name, format, _ := image_variant_call(t, thread, "listing.png", "medium")
	if name != "variants/listing_medium.png.jpg" || format != "jpeg" {
		t.Errorf("THE POINT: the photograph's medium is %s at %q, want jpeg at variants/listing_medium.png.jpg", format, name)
	}
	if got := file_name_type(name); got != "image/jpeg" {
		t.Errorf("the photograph's medium would be served as %s", got)
	}
	// Asked again, it is found under the name it was kept under, not rendered
	// again.
	kept, err := cache_file(thread, name)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(kept)
	if err != nil {
		t.Fatal(err)
	}
	if again, _, _ := image_variant_call(t, thread, "listing.png", "medium"); again != name {
		t.Errorf("asked again, the medium is %q, want %q", again, name)
	}
	if after, err := os.Stat(kept); err != nil || !after.ModTime().Equal(before.ModTime()) {
		t.Errorf("THE POINT: a medium kept under its second name was rendered again on the next ask")
	}

	source := filepath.Join(api_file_base(user, app), "listing.png")
	image_variant_source(t, user, app, "listing.png", variant_choice_image(t, "flat", 1500, 900))
	later := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(source, later, later); err != nil {
		t.Fatal(err)
	}
	name, format, _ = image_variant_call(t, thread, "listing.png", "medium")
	if name != "variants/listing_medium.png" || format != "png" {
		t.Errorf("THE POINT: the replacing graphic's medium is %s at %q, want png at variants/listing_medium.png", format, name)
	}
	stale, err := cache_file(thread, "variants/listing_medium.png.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("THE POINT: the photograph's JPEG medium outlived its source: %v", err)
	}
}

// TestVariantMadeCountsWhatItReplaced: a render that keeps a variant under the
// other name removes the stale entry and takes its bytes off the cache total.
func TestVariantMadeCountsWhatItReplaced(t *testing.T) {
	variant_made_cache(t)
	directory := t.TempDir()
	source := filepath.Join(directory, "photo.png")
	if err := os.WriteFile(source, variant_choice_image(t, "photo", 1500, 900), 0o644); err != nil {
		t.Fatal(err)
	}
	destinations := []string{
		filepath.Join(cache_dir, "apps", "user", "app", "variants", "photo_medium.png"),
		filepath.Join(cache_dir, "apps", "user", "app", "variants", "photo_medium.jpg"),
	}
	if err := os.MkdirAll(filepath.Dir(destinations[0]), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := bytes.Repeat([]byte{1}, 50000)
	if err := os.WriteFile(destinations[0], stale, 0o644); err != nil {
		t.Fatal(err)
	}
	earlier := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(destinations[0], earlier, earlier); err != nil {
		t.Fatal(err)
	}
	cache_total_set(int64(len(stale)))

	if index := variant_made(source, "medium", destinations, time.Now().Add(-time.Hour)); index != 1 {
		t.Fatalf("the photograph's medium was kept under destination %d, want 1 (JPEG)", index)
	}
	if _, err := os.Stat(destinations[0]); !os.IsNotExist(err) {
		t.Errorf("THE POINT: the stale PNG entry was left beside the new JPEG: %v", err)
	}
	information, err := os.Stat(destinations[1])
	if err != nil {
		t.Fatal(err)
	}
	cache_total_lock.Lock()
	total := cache_total
	cache_total_lock.Unlock()
	if total != information.Size() {
		t.Errorf("THE POINT: the cache counts %d bytes for a %d-byte variant that replaced a %d-byte entry", total, information.Size(), len(stale))
	}
}

// TestVariantLossyWorth: a JPEG is kept over a PNG only when it is at most
// three quarters of the PNG's size.
func TestVariantLossyWorth(t *testing.T) {
	for _, c := range []struct {
		lossless, lossy int64
		want            bool
	}{
		{1000, 750, true},
		{1000, 751, false},
		{1000, 999, false},
		{1000, 100, true},
	} {
		if got := variant_lossy_worth(c.lossless, c.lossy); got != c.want {
			t.Errorf("THE POINT: a %d-byte JPEG over a %d-byte PNG kept=%v, want %v", c.lossy, c.lossless, got, c.want)
		}
	}
}

// TestVariantsOfOneStemStayApart: a PNG and a JPEG (or a WebP and a JPEG) of
// one name in one directory are two images, and each is served its own
// variants, whichever is asked first.
func TestVariantsOfOneStemStayApart(t *testing.T) {
	thread, user, app := image_variant_thread(t)
	red, blue := color.RGBA{220, 20, 20, 255}, color.RGBA{20, 20, 220, 255}
	centre := func(decoded image.Image) color.RGBA {
		bounds := decoded.Bounds()
		return color.RGBAModel.Convert(decoded.At(bounds.Dx()/2, bounds.Dy()/2)).(color.RGBA)
	}
	grain := func(shade color.RGBA) []byte {
		img := image.NewNRGBA(image.Rect(0, 0, 1500, 900))
		noise := rand.New(rand.NewSource(59))
		for y := 0; y < 900; y++ {
			for x := 0; x < 1500; x++ {
				vary := func(v uint8) uint8 { return uint8(max(0, min(255, int(v)+noise.Intn(17)-8))) }
				img.SetNRGBA(x, y, color.NRGBA{vary(shade.R), vary(shade.G), vary(shade.B), 255})
			}
		}
		buffer := &bytes.Buffer{}
		if err := png.Encode(buffer, img); err != nil {
			t.Fatal(err)
		}
		return buffer.Bytes()
	}
	image_variant_source(t, user, app, "photos/a.png", grain(red))
	image_variant_source(t, user, app, "photos/a.jpg", image_variant_colour(t, "jpeg", blue))
	png_name, png_format, png_decoded := image_variant_call(t, thread, "photos/a.png", "medium")
	if png_format != "jpeg" {
		t.Fatalf("setup: the grainy PNG's medium is %s at %q, want a JPEG", png_format, png_name)
	}
	jpg_name, _, jpg_decoded := image_variant_call(t, thread, "photos/a.jpg", "medium")
	if png_name == jpg_name {
		t.Errorf("THE POINT: a.png and a.jpg share the variant entry %q", png_name)
	}
	if c := centre(jpg_decoded); c.B < 180 || c.R > 60 {
		t.Errorf("THE POINT: a.jpg's medium is %v, want its own blue", c)
	}
	if _, _, again := image_variant_call(t, thread, "photos/a.png", "medium"); centre(again).R < 180 || centre(png_decoded).R < 180 {
		t.Errorf("THE POINT: a.png's medium is %v, want its own red", centre(again))
	}
}
