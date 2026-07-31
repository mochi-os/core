// Mochi server: File unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"

	sl "go.starlark.net/starlark"
)

// file_api_environment sets up one user's app file storage and a thread with
// the locals the mochi.file.* builtins read, returning the storage base.
func file_api_environment(t *testing.T) (string, *sl.Thread) {
	t.Helper()

	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })

	user := &User{UID: "testuser"}
	app := &App{id: "files"}
	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("creating files directory: %v", err)
	}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", app)
	return base, thread
}

// TestFileCopy covers the primitive that lets an app duplicate an attachment
// without materialising it. Reading and then writing would build the whole
// object as a Starlark value first, which is what makes a large attachment a
// memory problem for every user sharing the process.
func TestFileCopy(t *testing.T) {
	base, thread := file_api_environment(t)

	content := []byte("ORIGINAL-BYTES")
	if err := os.WriteFile(base+"/source.txt", content, 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	copy := func(source, destination string) (sl.Value, error) {
		return api_file_copy(thread, sl.NewBuiltin("mochi.file.copy", nil),
			sl.Tuple{sl.String(source), sl.String(destination)}, nil)
	}

	// A copy into a directory that does not exist yet still lands, as a write
	// does - an app should not have to create the tree first.
	value, err := copy("source.txt", "nested/copy.txt")
	if err != nil {
		t.Fatalf("api_file_copy returned %v", err)
	}
	written, err := sl.AsInt32(value)
	if err != nil || int(written) != len(content) {
		t.Errorf("copied %v bytes, want %d", value, len(content))
	}
	landed, err := os.ReadFile(base + "/nested/copy.txt")
	if err != nil {
		t.Fatalf("reading copy: %v", err)
	}
	if string(landed) != string(content) {
		t.Errorf("copy = %q, want %q", landed, content)
	}
	// The source survives - this is a copy, not the move mochi.file.move is.
	if _, err := os.Stat(base + "/source.txt"); err != nil {
		t.Errorf("source gone after copy: %v", err)
	}

	if _, err := copy("missing.txt", "out.txt"); err == nil {
		t.Error("copying a missing file returned no error")
	}
	if _, err := copy("../../escape.txt", "out.txt"); err == nil {
		t.Error("a traversing source was accepted")
	}
	if _, err := copy("source.txt", "../../escape.txt"); err == nil {
		t.Error("a traversing destination was accepted")
	}
}

// TestFileCopyRefusesEscapingSymlink is the containment check for the write
// side. The path validator inspects the string, and a symlink is not in the
// string, so a link planted at the destination must not redirect the copy
// outside the app's directory.
func TestFileCopyRefusesEscapingSymlink(t *testing.T) {
	base, thread := file_api_environment(t)

	if err := os.WriteFile(base+"/source.txt", []byte("INSIDE"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	outside := data_dir + "/outside.txt"
	if err := os.WriteFile(outside, []byte("UNTOUCHED"), 0600); err != nil {
		t.Fatalf("writing outside file: %v", err)
	}
	if err := os.Symlink(outside, base+"/link.txt"); err != nil {
		t.Fatalf("creating symlink: %v", err)
	}

	api_file_copy(thread, sl.NewBuiltin("mochi.file.copy", nil),
		sl.Tuple{sl.String("source.txt"), sl.String("link.txt")}, nil)

	after, err := os.ReadFile(outside)
	if err != nil {
		t.Fatalf("reading outside file: %v", err)
	}
	if string(after) != "UNTOUCHED" {
		t.Errorf("the copy followed a symlink out of the directory: %q", after)
	}
}

// TestCacheCopy covers the other direction a forward takes: an attachment
// pulled from a peer lives in cache, which is evictable, and keeping it means
// moving the bytes into file storage without reading them into Starlark.
func TestCacheCopy(t *testing.T) {
	base, thread := file_api_environment(t)

	original := cache_dir
	cache_dir = t.TempDir()
	t.Cleanup(func() { cache_dir = original })

	entry := filepath.Join(cache_dir, "apps", "testuser", "files")
	if err := os.MkdirAll(entry, 0755); err != nil {
		t.Fatalf("creating cache directory: %v", err)
	}
	if err := os.WriteFile(entry+"/remote", []byte("PULLED-BYTES"), 0600); err != nil {
		t.Fatalf("writing cache entry: %v", err)
	}

	value, err := api_cache_copy(thread, sl.NewBuiltin("mochi.cache.copy", nil),
		sl.Tuple{sl.String("remote"), sl.String("kept.bin")}, nil)
	if err != nil {
		t.Fatalf("api_cache_copy returned %v", err)
	}
	if written, err := sl.AsInt32(value); err != nil || int(written) != len("PULLED-BYTES") {
		t.Errorf("copied %v bytes, want %d", value, len("PULLED-BYTES"))
	}
	landed, err := os.ReadFile(base + "/kept.bin")
	if err != nil {
		t.Fatalf("reading copy: %v", err)
	}
	if string(landed) != "PULLED-BYTES" {
		t.Errorf("copy = %q, want PULLED-BYTES", landed)
	}

	// A miss reads as None rather than an error: cache entries are evictable at
	// any moment, so the caller's answer is to re-obtain and retry.
	value, err = api_cache_copy(thread, sl.NewBuiltin("mochi.cache.copy", nil),
		sl.Tuple{sl.String("absent"), sl.String("out.bin")}, nil)
	if err != nil {
		t.Fatalf("api_cache_copy on a miss returned %v", err)
	}
	if value != sl.None {
		t.Errorf("miss = %v, want None", value)
	}
}

// TestFileAge covers the reading the attachment sweep depends on to tell a file
// that has settled from one another request may still be writing. A listing
// cannot: a part-written upload and an abandoned one have the same name.
func TestFileAge(t *testing.T) {
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })

	user := &User{UID: "testuser"}
	app := &App{id: "files"}
	base := api_file_base(user, app)
	if err := os.MkdirAll(base+"/directory", 0755); err != nil {
		t.Fatalf("creating files directory: %v", err)
	}
	if err := os.WriteFile(base+"/fresh.txt", []byte("x"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	if err := os.WriteFile(base+"/stale.txt", []byte("x"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(base+"/stale.txt", old, old); err != nil {
		t.Fatalf("setting modification time: %v", err)
	}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", app)

	age := func(name string) sl.Value {
		value, err := api_file_age(thread, sl.NewBuiltin("mochi.file.age", nil), sl.Tuple{sl.String(name)}, nil)
		if err != nil {
			t.Fatalf("api_file_age(%q) returned %v", name, err)
		}
		return value
	}

	seconds, err := sl.AsInt32(age("fresh.txt"))
	if err != nil || seconds > 60 {
		t.Errorf("fresh file age = %v, want a small number of seconds", age("fresh.txt"))
	}

	seconds, err = sl.AsInt32(age("stale.txt"))
	if err != nil || seconds < 7000 {
		t.Errorf("stale file age = %v, want about 7200 seconds", age("stale.txt"))
	}

	// Absent and directory both read as no answer rather than an age of zero,
	// which a caller comparing against a threshold would take as "settled".
	if value := age("missing.txt"); value != sl.None {
		t.Errorf("missing file age = %v, want None", value)
	}
	if value := age("directory"); value != sl.None {
		t.Errorf("directory age = %v, want None", value)
	}
}

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
	result, err := file_list(tmp_dir)
	if err != nil {
		t.Fatalf("file_list returned error: %v", err)
	}

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

	result, err := file_list(tmp_dir)
	if err != nil {
		t.Fatalf("file_list returned error: %v", err)
	}
	if result != nil && len(result) != 0 {
		t.Errorf("file_list on empty dir = %v, want empty", result)
	}
}

// Test file_write (and round-trip via os.ReadFile)
func TestFileWrite(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	path := filepath.Join(tmp_dir, "test.txt")
	content := []byte("Hello, World!")

	if err := file_write(path, content); err != nil {
		t.Fatalf("file_write failed: %v", err)
	}

	result, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile failed: %v", err)
	}

	if string(result) != string(content) {
		t.Errorf("ReadFile = %q, want %q", result, content)
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

	if err := file_write(path, content); err != nil {
		t.Fatalf("file_write failed: %v", err)
	}

	// Verify file exists and has correct content
	if !file_exists(path) {
		t.Error("file_write did not create file in nested path")
	}

	result, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile failed: %v", err)
	}
	if string(result) != string(content) {
		t.Errorf("ReadFile = %q, want %q", result, content)
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

	if err := file_write(path, content); err != nil {
		t.Fatalf("file_write failed: %v", err)
	}

	size := file_size(path)
	if size != 20 {
		t.Errorf("file_size = %d, want 20", size)
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
	test_dir := t.TempDir()

	if err := file_write(test_dir+"/file1.txt", []byte("hello")); err != nil {
		t.Fatalf("file_write failed: %v", err)
	}
	if err := file_write(test_dir+"/file2.txt", []byte("world!")); err != nil {
		t.Fatalf("file_write failed: %v", err)
	}
	if err := os.MkdirAll(test_dir+"/subdir", 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := file_write(test_dir+"/subdir/file3.txt", []byte("test")); err != nil {
		t.Fatalf("file_write failed: %v", err)
	}

	size, err := dir_size(test_dir)
	if err != nil {
		t.Fatalf("dir_size failed: %v", err)
	}
	expected := int64(5 + 6 + 4)

	if size != expected {
		t.Errorf("dir_size() = %d, expected %d", size, expected)
	}
}

// Test file storage limit is 10GB per user
func TestFileStorageLimitConstant(t *testing.T) {
	expected_limit := int64(10 * 1024 * 1024 * 1024)
	if file_max_storage != expected_limit {
		t.Errorf("file_max_storage = %d, expected %d (10GB)", file_max_storage, expected_limit)
	}
}

// Test api_file_base helper function
func TestApiFileBase(t *testing.T) {
	orig_data_dir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = orig_data_dir }()

	user := &User{UID: "u42"}
	app := &App{id: "testapp"}

	result := api_file_base(user, app)
	expected := "/var/lib/mochi/users/u42/testapp/files"

	if result != expected {
		t.Errorf("api_file_base() = %q, want %q", result, expected)
	}
}

// Test api_file_path helper function
func TestApiFilePath(t *testing.T) {
	orig_data_dir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = orig_data_dir }()

	user := &User{UID: "u42"}
	app := &App{id: "testapp"}

	result := api_file_path(user, app, "subdir/file.txt")
	expected := "/var/lib/mochi/users/u42/testapp/files/subdir/file.txt"

	if result != expected {
		t.Errorf("api_file_path() = %q, want %q", result, expected)
	}
}

// Test os.Root prevents path traversal for file operations
func TestOsRootPathTraversalProtection(t *testing.T) {
	tmp_dir := t.TempDir()
	target_dir := filepath.Join(tmp_dir, "target")
	outside_file := filepath.Join(tmp_dir, "outside.txt")

	// Create target directory
	os.MkdirAll(target_dir, 0755)

	// Create a file outside the target that we'll try to access
	os.WriteFile(outside_file, []byte("secret data"), 0644)

	// Open root at target directory
	root, err := os.OpenRoot(target_dir)
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
	if file_exists(filepath.Join(tmp_dir, "escape.txt")) {
		t.Error("File was created outside root despite os.Root protection")
	}
}

// Test os.Root prevents absolute path access
func TestOsRootAbsolutePathProtection(t *testing.T) {
	tmp_dir := t.TempDir()
	target_dir := filepath.Join(tmp_dir, "target")

	os.MkdirAll(target_dir, 0755)

	root, err := os.OpenRoot(target_dir)
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
	tmp_dir := t.TempDir()
	target_dir := filepath.Join(tmp_dir, "target")
	outside_dir := filepath.Join(tmp_dir, "outside")

	os.MkdirAll(target_dir, 0755)
	os.MkdirAll(outside_dir, 0755)

	// Create a secret file outside target
	secret_file := filepath.Join(outside_dir, "secret.txt")
	os.WriteFile(secret_file, []byte("secret"), 0644)

	// Create a symlink inside target pointing outside
	symlink_path := filepath.Join(target_dir, "link")
	err := os.Symlink(outside_dir, symlink_path)
	if err != nil {
		t.Skipf("Symlink creation failed (may require privileges): %v", err)
	}

	root, err := os.OpenRoot(target_dir)
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
	tmp_dir := t.TempDir()

	root, err := os.OpenRoot(tmp_dir)
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
	orig_cache_dir := cache_dir
	cache_dir = t.TempDir()
	defer func() { cache_dir = orig_cache_dir }()

	// Create test files
	old_file := filepath.Join(cache_dir, "old.txt")
	new_file := filepath.Join(cache_dir, "new.txt")
	if err := file_write(old_file, []byte("old")); err != nil {
		t.Fatalf("file_write failed: %v", err)
	}
	if err := file_write(new_file, []byte("new")); err != nil {
		t.Fatalf("file_write failed: %v", err)
	}

	// Set old file to 8 days ago (older than cache_max_age of 7 days)
	old_time := time.Now().Add(-8 * 24 * time.Hour)
	os.Chtimes(old_file, old_time, old_time)

	// Run cleanup
	cache_cleanup()

	// Old file should be removed
	if file_exists(old_file) {
		t.Error("cache_cleanup should have removed old file")
	}

	// New file should still exist
	if !file_exists(new_file) {
		t.Error("cache_cleanup should not have removed new file")
	}
}

// TestArchiveRoundTrip covers the container an export uses instead of embedding
// bytes in its own JSON. The manifest goes in from a string and the content
// streams in from file storage, which is the whole point: an attachment may be
// as large as the uploader's quota, and the old shape held every one of them
// base64-expanded in memory at once.
func TestArchiveRoundTrip(t *testing.T) {
	base, thread := file_api_environment(t)

	if err := os.WriteFile(base+"/photo.png", []byte("PNG-CONTENT"), 0600); err != nil {
		t.Fatalf("writing file: %v", err)
	}

	manifest := `{"format":3,"attachments":["files/photo.png"]}`
	entries := sl_encode([]map[string]any{
		{"name": "manifest.json", "data": manifest},
		{"name": "files/photo.png", "file": "photo.png"},
		{"name": "files/gone.png", "file": "missing.png"},
	})

	value, err := api_archive_write(thread, sl.NewBuiltin("mochi.archive.write", nil),
		sl.Tuple{sl.String("export.zip"), entries}, nil)
	if err != nil {
		t.Fatalf("api_archive_write returned %v", err)
	}
	if size, err := sl.AsInt32(value); err != nil || size <= 0 {
		t.Errorf("archive size = %v, want a positive count", value)
	}

	listed, err := api_archive_list(thread, sl.NewBuiltin("mochi.archive.list", nil),
		sl.Tuple{sl.String("export.zip")}, nil)
	if err != nil {
		t.Fatalf("api_archive_list returned %v", err)
	}
	names := map[string]bool{}
	for _, item := range sl_decode(listed).([]any) {
		entry := item.(map[string]any)
		names[entry["name"].(string)] = true
	}
	if !names["manifest.json"] || !names["files/photo.png"] {
		t.Errorf("entries = %v, want the manifest and the photo", names)
	}
	// A file that vanished between listing and archiving is skipped, not fatal:
	// one deleted blob must not cost the whole export.
	if names["files/gone.png"] {
		t.Errorf("a missing source produced an entry: %v", names)
	}

	read, err := api_archive_read(thread, sl.NewBuiltin("mochi.archive.read", nil),
		sl.Tuple{sl.String("export.zip"), sl.String("manifest.json")}, nil)
	if err != nil {
		t.Fatalf("api_archive_read returned %v", err)
	}
	if got := string(sl_decode(read).([]byte)); got != manifest {
		t.Errorf("manifest = %q, want %q", got, manifest)
	}

	extracted, err := api_archive_extract(thread, sl.NewBuiltin("mochi.archive.extract", nil),
		sl.Tuple{sl.String("export.zip"), sl.String("files/photo.png"), sl.String("restored/photo.png")}, nil)
	if err != nil {
		t.Fatalf("api_archive_extract returned %v", err)
	}
	if written, err := sl.AsInt32(extracted); err != nil || int(written) != len("PNG-CONTENT") {
		t.Errorf("extracted %v bytes, want %d", extracted, len("PNG-CONTENT"))
	}
	landed, err := os.ReadFile(base + "/restored/photo.png")
	if err != nil {
		t.Fatalf("reading extracted file: %v", err)
	}
	if string(landed) != "PNG-CONTENT" {
		t.Errorf("extracted = %q, want PNG-CONTENT", landed)
	}

	// An absent entry reads as None on both paths rather than erroring, so an
	// import can tell "not in this archive" from "this archive is broken".
	missing, _ := api_archive_read(thread, sl.NewBuiltin("mochi.archive.read", nil),
		sl.Tuple{sl.String("export.zip"), sl.String("absent")}, nil)
	if missing != sl.None {
		t.Errorf("reading an absent entry = %v, want None", missing)
	}
	missing, _ = api_archive_extract(thread, sl.NewBuiltin("mochi.archive.extract", nil),
		sl.Tuple{sl.String("export.zip"), sl.String("absent"), sl.String("out.bin")}, nil)
	if missing != sl.None {
		t.Errorf("extracting an absent entry = %v, want None", missing)
	}
}

// TestArchiveExtractIgnoresEntryName is the zip-slip check. A received archive
// names its own entries, and this one names a traversal - but extract writes to
// the destination the CALLER passed, so the entry name addresses nothing.
func TestArchiveExtractIgnoresEntryName(t *testing.T) {
	base, thread := file_api_environment(t)

	hostile := base + "/hostile.zip"
	f, err := os.Create(hostile)
	if err != nil {
		t.Fatalf("creating archive: %v", err)
	}
	writer := zip.NewWriter(f)
	w, err := writer.Create("../../../escaped.txt")
	if err != nil {
		t.Fatalf("creating entry: %v", err)
	}
	io.WriteString(w, "ESCAPED")
	writer.Close()
	f.Close()

	value, err := api_archive_extract(thread, sl.NewBuiltin("mochi.archive.extract", nil),
		sl.Tuple{sl.String("hostile.zip"), sl.String("../../../escaped.txt"), sl.String("safe.txt")}, nil)
	if err != nil {
		t.Fatalf("api_archive_extract returned %v", err)
	}
	if written, err := sl.AsInt32(value); err != nil || int(written) != len("ESCAPED") {
		t.Errorf("extracted %v bytes, want %d", value, len("ESCAPED"))
	}
	if _, err := os.Stat(base + "/safe.txt"); err != nil {
		t.Errorf("the entry did not land at the caller's destination: %v", err)
	}
	if _, err := os.Stat(data_dir + "/escaped.txt"); err == nil {
		t.Error("an entry name escaped the app's directory")
	}

	// And a traversing destination is refused outright, since that argument is
	// the one the caller controls.
	if _, err := api_archive_extract(thread, sl.NewBuiltin("mochi.archive.extract", nil),
		sl.Tuple{sl.String("hostile.zip"), sl.String("../../../escaped.txt"), sl.String("../../out.txt")}, nil); err == nil {
		t.Error("a traversing destination was accepted")
	}
}
