// Mochi server: File unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"
)

var (
	match_repeated_separators = regexp.MustCompile(`[-_ ]{2,}`)
	match_unsafe_chars        = regexp.MustCompile(`[\x00-\x1f\x7f/\\:*?"<>|]+`)
	reserved_names            = map[string]bool{"CON": true, "PRN": true, "AUX": true, "NUL": true, "COM1": true, "COM2": true, "COM3": true, "COM4": true, "COM5": true, "COM6": true, "COM7": true, "COM8": true, "COM9": true, "LPT1": true, "LPT2": true, "LPT3": true, "LPT4": true, "LPT5": true, "LPT6": true, "LPT7": true, "LPT8": true, "LPT9": true}
)

// file_name_safe sanitizes a filename (test helper)
func file_name_safe(s string) string {
	s = match_unsafe_chars.ReplaceAllString(s, "")

	s = strings.TrimFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || r == '.'
	})

	s = match_repeated_separators.ReplaceAllString(s, "_")

	s = strings.TrimLeft(s, ".")

	if s == "" {
		return "unnamed"
	}

	base := s
	i := strings.LastIndex(s, ".")
	if i > 0 {
		base = s[:i]
	}
	if reserved_names[strings.ToUpper(base)] {
		s = "_" + s
	}

	if len(s) > 240 {
		ext := ""
		i := strings.LastIndex(s, ".")
		if i > 0 && len(s)-i <= 10 {
			ext = s[i:]
			s = s[:i]
		}
		if len(s) > 240-len(ext) {
			s = s[:240-len(ext)]
		}
		s = strings.TrimRight(s, " ._-") + ext
	}

	return s
}

// file_size returns the size of a file in bytes (test helper)
func file_size(path string) int64 {
	f := must(os.Stat(path))
	return f.Size()
}

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

// Test api_file_base helper function
func TestApiFileBase(t *testing.T) {
	origDataDir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = origDataDir }()

	user := &User{ID: 42}
	app := &App{id: "testapp"}

	result := api_file_base(user, app)
	expected := "/var/lib/mochi/users/42/testapp/files"

	if result != expected {
		t.Errorf("api_file_base() = %q, want %q", result, expected)
	}
}

// Test api_file_path helper function
func TestApiFilePath(t *testing.T) {
	origDataDir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = origDataDir }()

	user := &User{ID: 42}
	app := &App{id: "testapp"}

	result := api_file_path(user, app, "subdir/file.txt")
	expected := "/var/lib/mochi/users/42/testapp/files/subdir/file.txt"

	if result != expected {
		t.Errorf("api_file_path() = %q, want %q", result, expected)
	}
}

// Test os.Root prevents path traversal for file operations
func TestOsRootPathTraversalProtection(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "target")
	outsideFile := filepath.Join(tmpDir, "outside.txt")

	// Create target directory
	os.MkdirAll(targetDir, 0755)

	// Create a file outside the target that we'll try to access
	os.WriteFile(outsideFile, []byte("secret data"), 0644)

	// Open root at target directory
	root, err := os.OpenRoot(targetDir)
	if err != nil {
		t.Fatalf("Failed to open root: %v", err)
	}
	defer root.Close()

	// Try to read file outside root using path traversal
	_, err = root.Open("../outside.txt")
	if err == nil {
		t.Error("os.Root should prevent path traversal with ../")
	}

	// Try to create file outside root using path traversal
	_, err = root.OpenFile("../escape.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		t.Error("os.Root should prevent creating files outside root with ../")
	}

	// Verify the escape file was not created
	if file_exists(filepath.Join(tmpDir, "escape.txt")) {
		t.Error("File was created outside root despite os.Root protection")
	}
}

// Test os.Root prevents absolute path access
func TestOsRootAbsolutePathProtection(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "target")

	os.MkdirAll(targetDir, 0755)

	root, err := os.OpenRoot(targetDir)
	if err != nil {
		t.Fatalf("Failed to open root: %v", err)
	}
	defer root.Close()

	// Try to access absolute path
	_, err = root.Open("/etc/passwd")
	if err == nil {
		t.Error("os.Root should prevent absolute path access")
	}
}

// Test os.Root prevents symlink escape
func TestOsRootSymlinkProtection(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "target")
	outsideDir := filepath.Join(tmpDir, "outside")

	os.MkdirAll(targetDir, 0755)
	os.MkdirAll(outsideDir, 0755)

	// Create a secret file outside target
	secretFile := filepath.Join(outsideDir, "secret.txt")
	os.WriteFile(secretFile, []byte("secret"), 0644)

	// Create a symlink inside target pointing outside
	symlinkPath := filepath.Join(targetDir, "link")
	err := os.Symlink(outsideDir, symlinkPath)
	if err != nil {
		t.Skipf("Symlink creation failed (may require privileges): %v", err)
	}

	root, err := os.OpenRoot(targetDir)
	if err != nil {
		t.Fatalf("Failed to open root: %v", err)
	}
	defer root.Close()

	// Try to access file through symlink
	_, err = root.Open("link/secret.txt")
	if err == nil {
		t.Error("os.Root should prevent symlink escape")
	}
}

// Test os.Root allows normal operations within root
func TestOsRootNormalOperations(t *testing.T) {
	tmpDir := t.TempDir()

	root, err := os.OpenRoot(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open root: %v", err)
	}
	defer root.Close()

	// Create a file
	f, err := root.OpenFile("test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Write([]byte("hello"))
	f.Close()

	// Read the file
	f, err = root.Open("test.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	data := make([]byte, 100)
	n, _ := f.Read(data)
	f.Close()

	if string(data[:n]) != "hello" {
		t.Errorf("Read data = %q, want %q", string(data[:n]), "hello")
	}

	// Create subdirectory
	err = root.Mkdir("subdir", 0755)
	if err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	// Create file in subdirectory
	f, err = root.OpenFile("subdir/nested.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to create nested file: %v", err)
	}
	f.Write([]byte("nested"))
	f.Close()

	// Stat the file
	info, err := root.Stat("subdir/nested.txt")
	if err != nil {
		t.Fatalf("Failed to stat nested file: %v", err)
	}
	if info.Size() != 6 {
		t.Errorf("File size = %d, want 6", info.Size())
	}

	// Remove the file
	err = root.Remove("test.txt")
	if err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}

	// Verify it's gone
	_, err = root.Stat("test.txt")
	if err == nil {
		t.Error("File should not exist after removal")
	}
}

// Test cache cleanup removes old files
func TestCacheCleanup(t *testing.T) {
	// Save and restore cache_dir
	origCacheDir := cache_dir
	cache_dir = t.TempDir()
	defer func() { cache_dir = origCacheDir }()

	// Create test files
	oldFile := filepath.Join(cache_dir, "old.txt")
	newFile := filepath.Join(cache_dir, "new.txt")
	file_write(oldFile, []byte("old"))
	file_write(newFile, []byte("new"))

	// Set old file to 8 days ago (older than cache_max_age of 7 days)
	oldTime := time.Now().Add(-8 * 24 * time.Hour)
	os.Chtimes(oldFile, oldTime, oldTime)

	// Run cleanup
	cache_cleanup()

	// Old file should be removed
	if file_exists(oldFile) {
		t.Error("cache_cleanup should have removed old file")
	}

	// New file should still exist
	if !file_exists(newFile) {
		t.Error("cache_cleanup should not have removed new file")
	}
}
