// Mochi server: File unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Test file_name_safe function
func TestFileNameSafe(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Normal filenames
		{"normal", "document.pdf", "document.pdf"},
		{"with spaces", "my document.pdf", "my document.pdf"},
		{"numbers", "file123.txt", "file123.txt"},

		// Unsafe characters removal
		{"forward slash", "path/file.txt", "pathfile.txt"},
		{"backslash", "path\\file.txt", "pathfile.txt"},
		{"colon", "file:name.txt", "filename.txt"},
		{"asterisk", "file*.txt", "file.txt"},
		{"question mark", "file?.txt", "file.txt"},
		{"quotes", "file\"name.txt", "filename.txt"},
		{"angle brackets", "file<name>.txt", "filename.txt"},
		{"pipe", "file|name.txt", "filename.txt"},
		{"null char", "file\x00name.txt", "filename.txt"},
		{"control chars", "file\x1fname.txt", "filename.txt"},

		// Repeated separators
		{"double dash", "file--name.txt", "file_name.txt"},
		{"double underscore", "file__name.txt", "file_name.txt"},
		{"double space", "file  name.txt", "file_name.txt"},
		{"mixed repeats", "file-_name.txt", "file_name.txt"},

		// Trimming
		{"leading dot", ".hidden", "hidden"},
		{"leading dots", "...hidden", "hidden"},
		{"trailing spaces", "file.txt   ", "file.txt"},
		{"leading spaces", "   file.txt", "file.txt"},
		{"trailing dot", "file.", "file"},

		// Empty/minimal
		{"empty string", "", "unnamed"},
		{"only dots", "...", "unnamed"},
		{"only spaces", "   ", "unnamed"},

		// Windows reserved names
		{"CON", "CON", "_CON"},
		{"PRN", "PRN", "_PRN"},
		{"AUX", "AUX", "_AUX"},
		{"NUL", "NUL", "_NUL"},
		{"COM1", "COM1", "_COM1"},
		{"LPT1", "LPT1", "_LPT1"},
		{"con lowercase", "con", "_con"},
		{"CON with extension", "CON.txt", "_CON.txt"},

		// Length truncation
		{"long name", strings.Repeat("a", 300), strings.Repeat("a", 240)},
		{"long with extension", strings.Repeat("a", 300) + ".txt", strings.Repeat("a", 236) + ".txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := file_name_safe(tt.input)
			if result != tt.expected {
				t.Errorf("file_name_safe(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test file_name_safe preserves extension on truncation
func TestFileNameSafePreservesExtension(t *testing.T) {
	// Long name with extension should preserve extension
	long_name := strings.Repeat("x", 250) + ".pdf"
	result := file_name_safe(long_name)

	if !strings.HasSuffix(result, ".pdf") {
		t.Errorf("file_name_safe should preserve extension, got %q", result)
	}

	if len(result) > 240 {
		t.Errorf("file_name_safe should truncate to <= 240 chars, got %d", len(result))
	}
}

// Test file_name_type function
func TestFileNameType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Images
		{"gif", "image.gif", "image/gif"},
		{"jpeg", "photo.jpeg", "image/jpeg"},
		{"jpg", "photo.jpg", "image/jpeg"},
		{"png", "image.png", "image/png"},
		{"webp", "image.webp", "image/webp"},

		// Documents
		{"pdf", "document.pdf", "application/pdf"},
		{"txt", "readme.txt", "text/plain"},

		// Unknown/default
		{"unknown", "file.xyz", "application/octet-stream"},
		{"no extension", "README", "application/octet-stream"},
		{"empty", "", "application/octet-stream"},

		// Case sensitivity (extensions should be lowercase typically)
		{"uppercase GIF", "image.GIF", "application/octet-stream"}, // only lowercase matched
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := file_name_type(tt.input)
			if result != tt.expected {
				t.Errorf("file_name_type(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test file_exists function
func TestFileExists(t *testing.T) {
	// Create temp file
	tmp_file, err := os.CreateTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmp_file.Close()
	defer os.Remove(tmp_file.Name())

	// Existing file should return true
	if !file_exists(tmp_file.Name()) {
		t.Errorf("file_exists(%q) = false, want true", tmp_file.Name())
	}

	// Non-existing file should return false
	if file_exists("/nonexistent/path/file.txt") {
		t.Error("file_exists for non-existent file = true, want false")
	}
}

// Test file_is_directory function
func TestFileIsDirectory(t *testing.T) {
	// Create temp directory
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	// Create temp file
	tmp_file, err := os.CreateTemp(tmp_dir, "file")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmp_file.Close()

	// Directory should return true
	if !file_is_directory(tmp_dir) {
		t.Errorf("file_is_directory(%q) = false, want true", tmp_dir)
	}

	// File should return false
	if file_is_directory(tmp_file.Name()) {
		t.Errorf("file_is_directory(%q) = true, want false", tmp_file.Name())
	}

	// Non-existent path should return false
	if file_is_directory("/nonexistent/path") {
		t.Error("file_is_directory for non-existent path = true, want false")
	}
}

// Test file_list function
func TestFileList(t *testing.T) {
	// Create temp directory with files
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	// Create some files
	files := []string{"alpha.txt", "beta.txt", "gamma.txt"}
	for _, f := range files {
		path := filepath.Join(tmp_dir, f)
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
	}

	// Test listing
	result := file_list(tmp_dir)

	if len(result) != 3 {
		t.Fatalf("file_list returned %d files, want 3", len(result))
	}

	// Results should be sorted
	if result[0] != "alpha.txt" || result[1] != "beta.txt" || result[2] != "gamma.txt" {
		t.Errorf("file_list not sorted correctly: %v", result)
	}
}

// Test file_list with empty directory
func TestFileListEmpty(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	result := file_list(tmp_dir)
	if result != nil && len(result) != 0 {
		t.Errorf("file_list on empty dir = %v, want empty", result)
	}
}

// Test file_read and file_write
func TestFileReadWrite(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	path := filepath.Join(tmp_dir, "test.txt")
	content := []byte("Hello, World!")

	// Write file
	file_write(path, content)

	// Read it back
	result := file_read(path)

	if string(result) != string(content) {
		t.Errorf("file_read = %q, want %q", result, content)
	}
}

// Test file_write creates parent directories
func TestFileWriteCreatesParentDirs(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	// Write to nested path that doesn't exist
	path := filepath.Join(tmp_dir, "subdir1", "subdir2", "file.txt")
	content := []byte("nested content")

	file_write(path, content)

	// Verify file exists and has correct content
	if !file_exists(path) {
		t.Error("file_write did not create file in nested path")
	}

	result := file_read(path)
	if string(result) != string(content) {
		t.Errorf("file_read = %q, want %q", result, content)
	}
}

// Test file_size function
func TestFileSize(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	path := filepath.Join(tmp_dir, "test.txt")
	content := []byte("12345678901234567890") // 20 bytes

	file_write(path, content)

	size := file_size(path)
	if size != 20 {
		t.Errorf("file_size = %d, want 20", size)
	}
}

// Test file_delete function
func TestFileDelete(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	path := filepath.Join(tmp_dir, "to_delete.txt")
	file_write(path, []byte("delete me"))

	if !file_exists(path) {
		t.Fatal("file should exist before delete")
	}

	file_delete(path)

	if file_exists(path) {
		t.Error("file should not exist after delete")
	}
}

// Test file_delete_all function
func TestFileDeleteAll(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	// Create nested structure
	subdir := filepath.Join(tmp_dir, "subdir")
	os.MkdirAll(subdir, 0755)
	file_write(filepath.Join(subdir, "file1.txt"), []byte("1"))
	file_write(filepath.Join(subdir, "file2.txt"), []byte("2"))

	if !file_exists(subdir) {
		t.Fatal("subdir should exist before delete")
	}

	file_delete_all(subdir)

	if file_exists(subdir) {
		t.Error("subdir should not exist after delete_all")
	}
}

// Test file_move function
func TestFileMove(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	old_path := filepath.Join(tmp_dir, "old.txt")
	new_path := filepath.Join(tmp_dir, "new.txt")
	content := []byte("move me")

	file_write(old_path, content)
	file_move(old_path, new_path)

	if file_exists(old_path) {
		t.Error("old file should not exist after move")
	}

	if !file_exists(new_path) {
		t.Error("new file should exist after move")
	}

	result := file_read(new_path)
	if string(result) != string(content) {
		t.Errorf("moved file content = %q, want %q", result, content)
	}
}

// Benchmark file_name_safe
func BenchmarkFileNameSafe(b *testing.B) {
	inputs := []string{
		"normal.txt",
		"file with spaces.pdf",
		"unsafe/\\:*?\"<>|chars.doc",
		strings.Repeat("a", 300) + ".txt",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file_name_safe(inputs[i%len(inputs)])
	}
}

// Benchmark file_name_type
func BenchmarkFileNameType(b *testing.B) {
	inputs := []string{
		"image.png",
		"document.pdf",
		"file.unknown",
		"README",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file_name_type(inputs[i%len(inputs)])
	}
}

// Test dir_size calculation
func TestDirSize(t *testing.T) {
	testDir := t.TempDir()

	file_write(testDir+"/file1.txt", []byte("hello"))
	file_write(testDir+"/file2.txt", []byte("world!"))
	file_mkdir(testDir + "/subdir")
	file_write(testDir+"/subdir/file3.txt", []byte("test"))

	size := dir_size(testDir)
	expected := int64(5 + 6 + 4)

	if size != expected {
		t.Errorf("dir_size() = %d, expected %d", size, expected)
	}
}

// Test file storage limit is 10GB per user
func TestFileStorageLimitConstant(t *testing.T) {
	expectedLimit := int64(10 * 1024 * 1024 * 1024)
	if file_max_storage != expectedLimit {
		t.Errorf("file_max_storage = %d, expected %d (10GB)", file_max_storage, expectedLimit)
	}
}
