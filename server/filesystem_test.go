// Mochi server: Filesystem unit tests
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
	longName := strings.Repeat("x", 250) + ".pdf"
	result := file_name_safe(longName)

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
	tmpFile, err := os.CreateTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Existing file should return true
	if !file_exists(tmpFile.Name()) {
		t.Errorf("file_exists(%q) = false, want true", tmpFile.Name())
	}

	// Non-existing file should return false
	if file_exists("/nonexistent/path/file.txt") {
		t.Error("file_exists for non-existent file = true, want false")
	}
}

// Test file_is_directory function
func TestFileIsDirectory(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create temp file
	tmpFile, err := os.CreateTemp(tmpDir, "file")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()

	// Directory should return true
	if !file_is_directory(tmpDir) {
		t.Errorf("file_is_directory(%q) = false, want true", tmpDir)
	}

	// File should return false
	if file_is_directory(tmpFile.Name()) {
		t.Errorf("file_is_directory(%q) = true, want false", tmpFile.Name())
	}

	// Non-existent path should return false
	if file_is_directory("/nonexistent/path") {
		t.Error("file_is_directory for non-existent path = true, want false")
	}
}

// Test file_list function
func TestFileList(t *testing.T) {
	// Create temp directory with files
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create some files
	files := []string{"alpha.txt", "beta.txt", "gamma.txt"}
	for _, f := range files {
		path := filepath.Join(tmpDir, f)
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
	}

	// Test listing
	result := file_list(tmpDir)

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
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	result := file_list(tmpDir)
	if result != nil && len(result) != 0 {
		t.Errorf("file_list on empty dir = %v, want empty", result)
	}
}

// Test file_read and file_write
func TestFileReadWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, "test.txt")
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
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write to nested path that doesn't exist
	path := filepath.Join(tmpDir, "subdir1", "subdir2", "file.txt")
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
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, "test.txt")
	content := []byte("12345678901234567890") // 20 bytes

	file_write(path, content)

	size := file_size(path)
	if size != 20 {
		t.Errorf("file_size = %d, want 20", size)
	}
}

// Test file_delete function
func TestFileDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, "to_delete.txt")
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
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create nested structure
	subdir := filepath.Join(tmpDir, "subdir")
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
	tmpDir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	oldPath := filepath.Join(tmpDir, "old.txt")
	newPath := filepath.Join(tmpDir, "new.txt")
	content := []byte("move me")

	file_write(oldPath, content)
	file_move(oldPath, newPath)

	if file_exists(oldPath) {
		t.Error("old file should not exist after move")
	}

	if !file_exists(newPath) {
		t.Error("new file should exist after move")
	}

	result := file_read(newPath)
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
