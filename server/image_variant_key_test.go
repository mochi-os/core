// Mochi server: a variant's cache entry names the image it came from.
//
// The entry keys on the whole relative path: two images sharing a base name in
// different directories are different images. A flat name must key exactly as
// before, since lib/starlark/attachments.star derives the same string.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestVariantNameKeepsTheDirectory is the regression, at the level the defect
// lives: two images that differ only in directory must not produce one name.
func TestVariantNameKeepsTheDirectory(t *testing.T) {
	first := variant_name("photos/a/cat.jpg", "thumbnail")
	second := variant_name("photos/b/cat.jpg", "thumbnail")

	if first == second {
		t.Fatalf("both directories produced %q; the second image would be served the first one's render", first)
	}
	if first != "photos/a/cat_thumbnail.jpg" {
		t.Errorf("variant_name(photos/a/cat.jpg) = %q, want photos/a/cat_thumbnail.jpg", first)
	}
	if second != "photos/b/cat_thumbnail.jpg" {
		t.Errorf("variant_name(photos/b/cat.jpg) = %q, want photos/b/cat_thumbnail.jpg", second)
	}
}

// TestVariantNameIsUnchangedForAFlatName: a flat name must key exactly as it
// does today - lib/starlark/attachments.star derives the same string to
// invalidate the entry, and attachment_variant_room reserves len("_thumbnail").
func TestVariantNameIsUnchangedForAFlatName(t *testing.T) {
	for _, c := range []struct{ input, kind, want string }{
		{"image.png", "thumbnail", "image_thumbnail.png"},
		{"photo.jpg", "preview", "photo_preview.jpg"},
		{"abc123_my photo.jpg", "thumbnail", "abc123_my photo_thumbnail.jpg"},
		{"file.name.png", "thumbnail", "file.name_thumbnail.png"},
		{"README", "thumbnail", "README_thumbnail"},
	} {
		if got := variant_name(c.input, c.kind); got != c.want {
			t.Errorf("variant_name(%q, %q) = %q, want %q - a flat name must key exactly as it did before", c.input, c.kind, got, c.want)
		}
	}
}

// TestVariantNameDoesNotMistakeADirectoryForAnExtension. The suffix goes on
// the file name, never on a directory that happens to contain a dot -
// filepath.Ext stops at a separator, which is why the path can be passed
// through unchanged.
func TestVariantNameDoesNotMistakeADirectoryForAnExtension(t *testing.T) {
	for _, c := range []struct{ input, want string }{
		{"photos/a.b/cat.jpg", "photos/a.b/cat_thumbnail.jpg"},
		{"photos/a.b/cat", "photos/a.b/cat_thumbnail"},
		{"v1.2/img.png", "v1.2/img_thumbnail.png"},
	} {
		if got := variant_name(c.input, "thumbnail"); got != c.want {
			t.Errorf("variant_name(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

// TestVariantEntryNameIsAValidCacheName: the name is handed to cache_file,
// which refuses anything the "filepath" validator rejects. A nested name has
// to survive that, and a traversal attempt has to keep being refused.
func TestVariantEntryNameIsAValidCacheName(t *testing.T) {
	for _, file := range []string{"cat.jpg", "photos/cat.jpg", "photos/a/b/cat.jpg"} {
		name := "variants/" + variant_name(file, "thumbnail")
		if !valid(name, "filepath") {
			t.Errorf("the entry name %q for source %q is not a valid cache name, so cache_file would refuse it", name, file)
		}
	}
	// The source is validated the same way before it ever reaches here, so a
	// traversing source cannot smuggle one out through the entry name.
	for _, file := range []string{"../escape.jpg", "photos/../../escape.jpg", "/abs/cat.jpg"} {
		if valid(file, "filepath") {
			t.Errorf("valid(%q, \"filepath\") = true; api_image_variant gates the source on this before naming the entry", file)
		}
	}
}

// TestDifferentDirectoriesGetDifferentCacheFiles states the consequence in
// terms of the file that actually gets written, which is what an app's second
// entity was being served.
func TestDifferentDirectoriesGetDifferentCacheFiles(t *testing.T) {
	seen := map[string]string{}
	for _, file := range []string{
		"public/cat.jpg",
		"private/cat.jpg",
		"entity-one/logo.png",
		"entity-two/logo.png",
		"cat.jpg",
	} {
		name := "variants/" + variant_name(file, "thumbnail")
		if other, clash := seen[name]; clash {
			t.Errorf("%q and %q both key to %q", other, file, name)
			continue
		}
		seen[name] = file
	}
}

// TestVariantKeyIsNotTheBasename is the gate. Taking the base is the defect,
// not a detail of it: any narrowing of the key below the full source path
// makes two distinct images share an entry again.
func TestVariantKeyIsNotTheBasename(t *testing.T) {
	source, err := os.ReadFile("images.go")
	if err != nil {
		t.Fatalf("reading images.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func api_image_variant(")
	if at < 0 {
		t.Fatal("images.go no longer defines api_image_variant")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	// Strip line comments: the explanation above the fix names filepath.Base.
	var code []string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "//") {
			code = append(code, line)
		}
	}
	body = strings.Join(code, "\n")

	if strings.Contains(body, "filepath.Base(file)") {
		t.Error("api_image_variant keys the cache entry on the base name again; two images with the same name in different directories share an entry, and the existing-entry short-circuit serves whichever was rendered first")
	}
	if !strings.Contains(body, `"variants/" + variant_name(file, kind)`) {
		t.Error("the entry name is no longer the whole source path; that name is the only record of which image a variant came from")
	}
}

// variant_key_image writes a solid-colour PNG at file under the app's storage,
// large enough that a thumbnail is a real resize rather than a copy.
func variant_key_image(t *testing.T, user *User, app *App, file string, shade color.RGBA) {
	t.Helper()
	path := filepath.Join(api_file_base(user, app), file)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(path), err)
	}
	img := image.NewRGBA(image.Rect(0, 0, 600, 400))
	draw.Draw(img, img.Bounds(), &image.Uniform{shade}, image.Point{}, draw.Src)

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating %s: %v", path, err)
	}
	defer f.Close()
	if err := png.Encode(f, img); err != nil {
		t.Fatalf("encoding %s: %v", path, err)
	}
}

// variant_key_shade decodes a rendered variant and returns its centre pixel,
// which is how one solid-colour source is told from another.
func variant_key_shade(t *testing.T, path string) color.RGBA {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer f.Close()
	img, _, err := image.Decode(f)
	if err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
	bounds := img.Bounds()
	r, g, b, a := img.At(bounds.Dx()/2, bounds.Dy()/2).RGBA()
	return color.RGBA{uint8(r >> 8), uint8(g >> 8), uint8(b >> 8), uint8(a >> 8)}
}

// TestTwoImagesWithOneNameRenderSeparately drives api_image_variant end to end:
// the key is taken at the call site, not inside variant_name.
func TestTwoImagesWithOneNameRenderSeparately(t *testing.T) {
	original_data, original_cache := data_dir, cache_dir
	data_dir, cache_dir = t.TempDir(), t.TempDir()
	t.Cleanup(func() { data_dir, cache_dir = original_data, original_cache })

	user := &User{UID: "user-one"}
	app := &App{id: "app-one"}
	red := color.RGBA{220, 20, 20, 255}
	blue := color.RGBA{20, 20, 220, 255}

	// The same file name, in two directories - one entity's image and another's.
	variant_key_image(t, user, app, "public/photo.png", red)
	variant_key_image(t, user, app, "private/photo.png", blue)

	thread := &sl.Thread{}
	thread.SetLocal("storage", user)
	thread.SetLocal("app", app)
	builtin := sl.NewBuiltin("mochi.image.variant", api_image_variant)

	render := func(file string) string {
		t.Helper()
		value, err := api_image_variant(thread, builtin, sl.Tuple{sl.String(file), sl.String("thumbnail")}, nil)
		if err != nil {
			t.Fatalf("variant(%q): %v", file, err)
		}
		name, ok := sl.AsString(value)
		if !ok || name == "" {
			t.Fatalf("variant(%q) returned %v, want a cache entry name", file, value)
		}
		return name
	}

	first := render("public/photo.png")
	second := render("private/photo.png")

	if first == second {
		t.Fatalf("both images key to %q; the second is served the first's render", first)
	}

	// And each entry holds ITS OWN image, which is the part a name check alone
	// cannot show.
	for _, c := range []struct {
		name  string
		want  color.RGBA
		which string
	}{
		{first, red, "public/photo.png"},
		{second, blue, "private/photo.png"},
	} {
		path, err := cache_file(thread, c.name)
		if err != nil {
			t.Fatalf("cache_file(%q): %v", c.name, err)
		}
		got := variant_key_shade(t, path)
		if got != c.want {
			t.Errorf("the variant for %s (%s) rendered %v, want %v - it holds the other image", c.which, c.name, got, c.want)
		}
	}
}

// TestRepeatedVariantRequestIsStillCached: the short-circuit has to keep
// working for the same source, or every request re-renders.
func TestRepeatedVariantRequestIsStillCached(t *testing.T) {
	original_data, original_cache := data_dir, cache_dir
	data_dir, cache_dir = t.TempDir(), t.TempDir()
	t.Cleanup(func() { data_dir, cache_dir = original_data, original_cache })

	user := &User{UID: "user-one"}
	app := &App{id: "app-one"}
	variant_key_image(t, user, app, "photos/one.png", color.RGBA{10, 200, 10, 255})

	thread := &sl.Thread{}
	thread.SetLocal("storage", user)
	thread.SetLocal("app", app)
	builtin := sl.NewBuiltin("mochi.image.variant", api_image_variant)

	first, err := api_image_variant(thread, builtin, sl.Tuple{sl.String("photos/one.png"), sl.String("thumbnail")}, nil)
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	name, _ := sl.AsString(first)
	path, err := cache_file(thread, name)
	if err != nil {
		t.Fatalf("cache_file: %v", err)
	}
	information, err := os.Stat(path)
	if err != nil {
		t.Fatalf("the first render wrote no file: %v", err)
	}

	// Make the cached entry distinguishable, then ask again: a re-render would
	// overwrite it.
	if err := os.Chtimes(path, information.ModTime(), information.ModTime()); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	marker := information.ModTime()

	second, err := api_image_variant(thread, builtin, sl.Tuple{sl.String("photos/one.png"), sl.String("thumbnail")}, nil)
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if repeat_name, _ := sl.AsString(second); repeat_name != name {
		t.Errorf("the same source keyed to %q then %q", name, repeat_name)
	}
	again, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after the second call: %v", err)
	}
	if !again.ModTime().Equal(marker) {
		t.Error("the second request re-rendered an entry that was already cached; the short-circuit is gone and every view pays for a resize")
	}
}
