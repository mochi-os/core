// Mochi server: Images unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
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

		// Case sensitivity (extensions are case-sensitive)
		{"uppercase PNG", "image.PNG", false},
		{"uppercase JPG", "photo.JPG", false},
		{"mixed case Png", "image.Png", false},
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

// Test thumbnail_name function
func TestThumbnailName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple png", "image.png", "image_thumbnail.png"},
		{"simple jpg", "photo.jpg", "photo_thumbnail.jpg"},
		{"simple gif", "animation.gif", "animation_thumbnail.gif"},
		{"with spaces", "my photo.jpg", "my photo_thumbnail.jpg"},
		{"with underscores", "my_image.png", "my_image_thumbnail.png"},
		{"with dashes", "my-image.png", "my-image_thumbnail.png"},
		{"multiple dots", "file.name.png", "file.name_thumbnail.png"},
		{"no extension", "README", "README_thumbnail"},
		{"empty string", "", "_thumbnail"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := thumbnail_name(tt.input)
			if result != tt.expected {
				t.Errorf("thumbnail_name(%q) = %q, want %q", tt.input, result, tt.expected)
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

// Test thumbnail_create function
func TestThumbnailCreate(t *testing.T) {
	// Create temp directory
	tmp_dir, err := os.MkdirTemp("", "thumbnail_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	// Create a test image (500x300 PNG)
	img := image.NewRGBA(image.Rect(0, 0, 500, 300))
	for y := 0; y < 300; y++ {
		for x := 0; x < 500; x++ {
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

	// Test thumbnail creation
	thumb_path, err := thumbnail_create(img_path)
	if err != nil {
		t.Fatalf("thumbnail_create failed: %v", err)
	}

	// Verify thumbnail was created
	if thumb_path == "" {
		t.Fatal("thumbnail_create returned empty path")
	}

	expected_thumb := filepath.Join(tmp_dir, "thumbnails", "test_image_thumbnail.png")
	if thumb_path != expected_thumb {
		t.Errorf("Expected thumbnail path %q, got %q", expected_thumb, thumb_path)
	}

	// Verify file exists
	if _, err := os.Stat(thumb_path); os.IsNotExist(err) {
		t.Fatal("Thumbnail file was not created")
	}

	// Verify thumbnail dimensions (should be max 250x250)
	thumb_f, err := os.Open(thumb_path)
	if err != nil {
		t.Fatalf("Failed to open thumbnail: %v", err)
	}
	defer thumb_f.Close()

	thumb_img, _, err := image.Decode(thumb_f)
	if err != nil {
		t.Fatalf("Failed to decode thumbnail: %v", err)
	}

	bounds := thumb_img.Bounds()
	if bounds.Dx() > 250 || bounds.Dy() > 250 {
		t.Errorf("Thumbnail too large: %dx%d", bounds.Dx(), bounds.Dy())
	}
	if bounds.Dx() == 0 || bounds.Dy() == 0 {
		t.Error("Thumbnail has zero dimension")
	}

	// Test that calling again returns cached thumbnail
	thumb_path2, err := thumbnail_create(img_path)
	if err != nil {
		t.Fatalf("Second thumbnail_create failed: %v", err)
	}
	if thumb_path2 != thumb_path {
		t.Errorf("Expected cached path %q, got %q", thumb_path, thumb_path2)
	}

	// Verify no temp file left behind
	tmp_file := thumb_path + ".tmp"
	if _, err := os.Stat(tmp_file); !os.IsNotExist(err) {
		t.Errorf("Temp file %q should not exist", tmp_file)
	}
}

// Benchmark thumbnail_name
func BenchmarkThumbnailName(b *testing.B) {
	inputs := []string{
		"image.png",
		"my photo with spaces.jpg",
		"complex.file.name.gif",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		thumbnail_name(inputs[i%len(inputs)])
	}
}
