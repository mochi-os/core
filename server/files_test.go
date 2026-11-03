// Mochi server: File unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// file_api_environment sets up one user's app file storage and a thread with
// the locals the mochi.file.* builtins read, returning the storage base.
func file_api_environment(t *testing.T) (string, *sl.Thread) {
	t.Helper()

	test_data_directory(t)

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
	test_data_directory(t)

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

		// Case is folded, for the reason is_image folds it: reporting
		// application/octet-stream for PHOTO.PNG turned a viewable image into a
		// download, and disagreed with the library that had already lowercased.
		{"uppercase GIF", "image.GIF", "image/gif"},
		{"uppercase PNG", "PHOTO.PNG", "image/png"},
		{"mixed case Pdf", "document.Pdf", "application/pdf"},
		{"still unknown", "file.XYZ", "application/octet-stream"},
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
	tmp_dir := t.TempDir()

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
	tmp_dir := t.TempDir()

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
	tmp_dir := t.TempDir()

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
	tmp_dir := t.TempDir()

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
	tmp_dir := t.TempDir()

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
	tmp_dir := t.TempDir()

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
	if file_maximum_storage != expected_limit {
		t.Errorf("file_max_storage = %d, expected %d (10GB)", file_maximum_storage, expected_limit)
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
	information, err := root.Stat("subdir/nested.txt")
	if err != nil {
		t.Fatalf("Failed to stat nested file: %v", err)
	}
	if information.Size() != 6 {
		t.Errorf("File size = %d, want 6", information.Size())
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

	// Set old file to 8 days ago (older than cache_age_maximum of 7 days)
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

// TestStorageLimitsAgree pins the chain the sizes depend on: what the platform
// stores must not exceed what it transfers, or a stored object truncates
// silently on the way out.
func TestStorageLimitsAgree(t *testing.T) {
	if int64(stream_maximum_default) < int64(object_maximum) {
		t.Errorf("stream cap = %d is below the largest storable object (%d), so that object cannot be transferred whole - the owner keeps it and every subscriber receives a prefix",
			int64(stream_maximum_default), int64(object_maximum))
	}
	// The far end of the chain. The per-client byte budget is a denial-of-service
	// control, and one honest transfer of the largest object has to fit inside
	// it or the budget truncates legitimate traffic. Raising object_maximum
	// without raising this is what this check exists to refuse.
	budget := int64(rate_limit_stream_client.limit) * 1024
	if budget < int64(stream_maximum_default) {
		t.Errorf("per-client relay budget = %d bytes, below one maximum relay (%d): a single honest transfer of the largest object cannot complete",
			budget, int64(stream_maximum_default))
	}
	// The app-wide breaker has to stay clear of the per-client budget, or one
	// client's honest transfer trips the circuit for every other user of that
	// app - shared fate, which is what the breaker's own comment rules out.
	if breaker := int64(rate_limit_stream_app.limit) * 1024; breaker <= budget {
		t.Errorf("app breaker = %d bytes, not above one client's budget (%d): one honest transfer would refuse every other caller",
			breaker, budget)
	}
	// A transfer of the largest object has to be able to finish inside the
	// bound that applies to it. The compute timeout is not that bound - a pull
	// marks itself as transferring - so it is the file timeout that has to
	// clear it at some defensible rate.
	rate := float64(object_maximum) / starlark_file_timeout.Seconds() / (1024 * 1024)
	if rate > 25 {
		t.Errorf("the largest object needs %.1f MB/s sustained to transfer inside the %s bound; that is not an ordinary link speed",
			rate, starlark_file_timeout)
	}
}

// TestTransferSetUsesTheTransferBound covers the marker a bulk pull sets: the
// copy runs inside a built-in that never checks for cancellation, so a call
// moving bytes must take the transfer timeout rather than the compute one.
func TestTransferSetUsesTheTransferBound(t *testing.T) {
	thread := &sl.Thread{Name: "test"}
	var serving atomic.Bool
	thread.SetLocal("file_serving", &serving)

	if starlark_serving_get(thread) {
		t.Fatal("a fresh thread already claims to be transferring")
	}

	starlark_transfer_set(thread)

	if !starlark_serving_get(thread) {
		t.Error("a bulk transfer is still bounded by the compute timeout, which cannot clear the largest object on an ordinary link")
	}
	if starlark_file_timeout <= starlark_default_timeout {
		t.Errorf("transfer bound %s is not longer than the compute bound %s, so marking a transfer buys nothing",
			starlark_file_timeout, starlark_default_timeout)
	}
}

// TestCacheReadBoundSurvivesReplacement pins why mochi.cache.read holds one
// handle: cache writes rename into place, so a Stat and a later ReadFile of the
// same path can describe different files. It demonstrates the descriptor
// property only - it does not exercise api_cache_read and would not fail on a
// rewrite.
func TestCacheReadBoundSurvivesReplacement(t *testing.T) {
	original := cache_dir
	cache_dir = t.TempDir()
	t.Cleanup(func() { cache_dir = original })

	directory := filepath.Join(cache_dir, "apps", "testuser", "files")
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatalf("creating cache directory: %v", err)
	}
	entry := filepath.Join(directory, "remote")
	if err := os.WriteFile(entry, []byte("small"), 0600); err != nil {
		t.Fatalf("writing cache entry: %v", err)
	}

	// The read side opens first, as api_cache_read now does.
	f, err := os.Open(entry)
	if err != nil {
		t.Fatalf("opening entry: %v", err)
	}
	defer f.Close()

	// A concurrent fill replaces the name atomically, exactly as
	// cache_write_file does, with something far larger.
	replacement := make([]byte, 4096)
	if _, err := cache_write_file(entry, bytes.NewReader(replacement)); err != nil {
		t.Fatalf("replacing entry: %v", err)
	}

	// The handle still describes and returns the file it opened. A path-based
	// Stat here would report 4096 and a path-based read would return it, which
	// is the bypass: a bound satisfied against one file, bytes taken from
	// another.
	information, err := f.Stat()
	if err != nil {
		t.Fatalf("stat through the handle: %v", err)
	}
	if information.Size() != int64(len("small")) {
		t.Errorf("size through the handle = %d, want %d: the measurement must describe the opened file, not whatever the name means now",
			information.Size(), len("small"))
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("reading through the handle: %v", err)
	}
	if string(data) != "small" {
		t.Errorf("read %d bytes through the handle, want the 5 that were measured", len(data))
	}

	// And the path really was replaced, so the check above is about the handle
	// rather than about nothing having happened.
	if by_path, err := os.Stat(entry); err != nil || by_path.Size() != 4096 {
		t.Errorf("the replacement did not land; this test would pass vacuously")
	}
}

// storage_test_environment sets up file storage as file_api_environment does,
// with a quota of maximum bytes and the walks of users' storage counted.
func storage_test_environment(t *testing.T, maximum int64) (string, *sl.Thread, *User, *atomic.Int64) {
	t.Helper()
	base, thread := file_api_environment(t)
	previous := file_maximum_storage
	file_maximum_storage = maximum
	walk := storage_walk
	walks := &atomic.Int64{}
	storage_walk = func(path string) (int64, error) {
		walks.Add(1)
		return dir_size(path)
	}
	t.Cleanup(func() {
		file_maximum_storage = previous
		storage_walk = walk
	})
	user, _ := thread.Local("user").(*User)
	return base, thread, user, walks
}

// storage_test_write calls mochi.file.write.
func storage_test_write(thread *sl.Thread, name string, size int) error {
	_, err := api_file_write(thread, sl.NewBuiltin("mochi.file.write", nil), sl.Tuple{sl.String(name), sl.String(strings.Repeat("x", size))}, nil)
	return err
}

// storage_test_upload writes as a.upload does, through root_write_file.
func storage_test_upload(t *testing.T, base string, name string, size int) error {
	t.Helper()
	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	_, err = root_write_file(root, name, strings.NewReader(strings.Repeat("u", size)))
	return err
}

// storage_test_figure answers the bytes a user's figure holds.
func storage_test_figure(t *testing.T, uid string) int64 {
	t.Helper()
	found, ok := storage_figures.Load(uid)
	if !ok {
		t.Fatalf("no storage figure for %s", uid)
	}
	figure := found.(*storage_figure)
	figure.lock.Lock()
	defer figure.lock.Unlock()
	return figure.used
}

// TestStorageFigureFollowsTrackedWrites. The figure stands in for a walk of
// the user's storage between walks, so after the writes and deletes core makes
// it must be what a walk would find - having walked once.
func TestStorageFigureFollowsTrackedWrites(t *testing.T) {
	base, thread, user, walks := storage_test_environment(t, 1<<20)
	if err := os.WriteFile(filepath.Join(base, "before"), make([]byte, 777), 0644); err != nil {
		t.Fatal(err)
	}
	call := func(builtin func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error), arguments ...string) {
		t.Helper()
		values := sl.Tuple{}
		for _, argument := range arguments {
			values = append(values, sl.String(argument))
		}
		if _, err := builtin(thread, sl.NewBuiltin("mochi.file", nil), values, nil); err != nil {
			t.Fatalf("%v: %v", arguments, err)
		}
	}

	steps := []struct {
		name string
		run  func()
	}{
		{"a write", func() {
			if err := storage_test_write(thread, "a", 1000); err != nil {
				t.Fatal(err)
			}
		}},
		{"a write over a file", func() {
			if err := storage_test_write(thread, "a", 300); err != nil {
				t.Fatal(err)
			}
		}},
		{"an upload", func() {
			if err := storage_test_upload(t, base, "b", 5000); err != nil {
				t.Fatal(err)
			}
		}},
		{"an upload over a file", func() {
			if err := storage_test_upload(t, base, "b", 4000); err != nil {
				t.Fatal(err)
			}
		}},
		{"a copy", func() { call(api_file_copy, "b", "c") }},
		{"a move over a file", func() { call(api_file_move, "c", "a") }},
		{"a move onto itself", func() { call(api_file_move, "a", "a") }},
		{"a delete", func() { call(api_file_delete, "b") }},
		{"a delete of a file walked", func() { call(api_file_delete, "before") }},
	}
	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		step.run()
		walked, err := dir_size(user_storage_dir(user))
		if err != nil {
			t.Fatal(err)
		}
		if figure := storage_test_figure(t, user.UID); figure != walked {
			t.Errorf("THE POINT: after %s the figure is %d bytes and a walk finds %d", step.name, figure, walked)
		}
	}
	if count := walks.Load(); count != 1 {
		t.Errorf("THE POINT: the writes walked the storage %d times, want once", count)
	}
}

// TestStorageQuotaRefusedFromTheFigure. A write past the quota is refused on
// the figure the writes before it left, without walking again: a write, and
// an upload, which removes what it wrote and returns the charge.
func TestStorageQuotaRefusedFromTheFigure(t *testing.T) {
	base, thread, user, walks := storage_test_environment(t, 10000)
	if err := storage_test_write(thread, "first", 6000); err != nil {
		t.Fatal(err)
	}
	if err := storage_test_write(thread, "second", 5000); err == nil || !strings.Contains(err.Error(), "storage limit exceeded") {
		t.Errorf("THE POINT: a write past the quota was not refused: %v", err)
	}
	if err := storage_test_upload(t, base, "third", 5000); !errors.Is(err, storage_exceeded) {
		t.Errorf("THE POINT: an upload past the quota was not refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "third")); !os.IsNotExist(err) {
		t.Errorf("the refused upload's bytes were left: %v", err)
	}
	if figure := storage_test_figure(t, user.UID); figure != 6000 {
		t.Errorf("the figure is %d after refusals, want the 6000 written", figure)
	}
	if count := walks.Load(); count != 1 {
		t.Errorf("THE POINT: the refusals walked the storage %d times, want the one walk", count)
	}
}

// TestStorageFigureExpires. What grows outside core's writes - a database, a
// repository - is counted by the next walk once the figure has expired.
func TestStorageFigureExpires(t *testing.T) {
	base, _, user, walks := storage_test_environment(t, 1<<20)
	before, err := user_storage_remaining(user)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "database"), make([]byte, 1234), 0644); err != nil {
		t.Fatal(err)
	}
	if remaining, _ := user_storage_remaining(user); remaining != before || walks.Load() != 1 {
		t.Fatalf("setup: within its lifetime the figure walked again (%d walks, %d remaining)", walks.Load(), remaining)
	}

	found, _ := storage_figures.Load(user.UID)
	figure := found.(*storage_figure)
	figure.lock.Lock()
	figure.measured = figure.measured.Add(-storage_lifetime)
	figure.lock.Unlock()
	remaining, err := user_storage_remaining(user)
	if err != nil {
		t.Fatal(err)
	}
	if walks.Load() != 2 || remaining != before-1234 {
		t.Errorf("THE POINT: an expired figure answered %d remaining after %d walks, want %d after 2", remaining, walks.Load(), before-1234)
	}
}

// TestStorageWritesHeldTogether. Writes running at once near the quota cannot
// both pass on one figure: what one write is charged is gone for the next
// before a byte of it lands.
func TestStorageWritesHeldTogether(t *testing.T) {
	base, thread, user, _ := storage_test_environment(t, 10000)
	// A write stored before, its bytes charged.
	if err := storage_test_write(thread, "first", 6000); err != nil {
		t.Fatalf("setup: writing: %v", err)
	}
	if err := storage_test_write(thread, "second", 6000); err == nil {
		t.Error("THE POINT: a write passed on a figure another write had already charged")
	}
	if err := storage_test_upload(t, base, "third", 6000); err == nil {
		t.Error("THE POINT: an upload passed on a figure another write had already charged")
	}

	// And truly at once.
	if _, err := api_file_delete(thread, sl.NewBuiltin("mochi.file.delete", nil), sl.Tuple{sl.String("first")}, nil); err != nil {
		t.Fatal(err)
	}
	var passed atomic.Int64
	var group sync.WaitGroup
	start := make(chan struct{})
	for i := range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			name := fmt.Sprintf("racer-%d", i)
			var err error
			if i%2 == 0 {
				err = storage_test_write(thread, name, 6000)
			} else {
				err = storage_test_upload(t, base, name, 6000)
			}
			if err == nil {
				passed.Add(1)
			}
		}()
	}
	close(start)
	group.Wait()
	if count := passed.Load(); count != 1 {
		t.Errorf("THE POINT: %d writes of 6000 bytes passed a quota of 10000 at once, want 1", count)
	}
	if walked, _ := dir_size(user_storage_dir(user)); walked > 10000 {
		t.Errorf("THE POINT: %d bytes stored past a quota of 10000", walked)
	}
}

// TestStorageWalkedOncePerRequest. However many writes a request makes, and
// however long it runs, it walks a user's storage once: the multipart check
// and the action's writes share the request's walk.
func TestStorageWalkedOncePerRequest(t *testing.T) {
	_, thread, user, walks := storage_test_environment(t, 1<<30)
	lifetime := storage_lifetime
	storage_lifetime = 0
	defer func() { storage_lifetime = lifetime }()

	measured := &storage_request{}
	thread.SetLocal("measured", measured)
	web_multipart_maximum(measured, user)
	for i := range 3 {
		if err := storage_test_write(thread, fmt.Sprintf("file-%d", i), 100); err != nil {
			t.Fatal(err)
		}
	}
	if count := walks.Load(); count != 1 {
		t.Errorf("THE POINT: one request walked the storage %d times, want once", count)
	}

	// Another request walks again: the figure has expired.
	other := &sl.Thread{Name: "other"}
	other.SetLocal("user", user)
	other.SetLocal("app", thread.Local("app"))
	if err := storage_test_write(other, "other", 100); err != nil {
		t.Fatal(err)
	}
	if count := walks.Load(); count != 2 {
		t.Errorf("the next request walked %d times in all, want 2", count-1)
	}
}

// TestStorageForgottenWithTheUser. A purged user's figure goes with them: the
// uid may come back, to an account that starts empty.
func TestStorageForgottenWithTheUser(t *testing.T) {
	purge_test_env(t, "u-gone")
	user := &User{UID: "u-gone"}
	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}
	if _, err := user_purge_local("u-gone"); err != nil {
		t.Fatal(err)
	}
	if _, kept := storage_figures.Load("u-gone"); kept {
		t.Error("THE POINT: a purged user's storage figure was kept")
	}
}

// TestStorageRequestSharedWithTheAction. A multipart request walks the
// caller's storage to bound its body before the action runs; the action's own
// writes share that walk rather than making another.
func TestStorageRequestSharedWithTheAction(t *testing.T) {
	_, user, session := web_bearer_env(t)
	if err := os.MkdirAll(user_storage_dir(user), 0755); err != nil {
		t.Fatal(err)
	}
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 32)
	}
	timeout, lifetime, walk := starlark_default_timeout, storage_lifetime, storage_walk
	walks := &atomic.Int64{}
	starlark_default_timeout = 30 * time.Second
	storage_lifetime = 0
	storage_walk = func(path string) (int64, error) {
		walks.Add(1)
		return dir_size(path)
	}
	t.Cleanup(func() { starlark_default_timeout, storage_lifetime, storage_walk = timeout, lifetime, walk })

	star := filepath.Join(t.TempDir(), "app.star")
	source := `
def action_store(a):
    for name in ["one", "two", "three"]:
        mochi.file.write(name, "stored")
    return {"stored": True}
`
	if err := os.WriteFile(star, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	version := &AppVersion{Version: "1.0", Execute: []string{star}, Actions: map[string]AppAction{
		"-/store": {Function: "action_store"},
	}}
	version.Architecture.Engine = "starlark"
	app := &App{id: "storagetest", internal: version, latest: version, versions: map[string]*AppVersion{"1.0": version}}
	version.app = app
	token, err := jwt_app_token(user.UID, app.id, session, "bearer-test-secret-0123456789")
	if err != nil {
		t.Fatal(err)
	}

	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	form.WriteField("note", "stored three ways")
	form.Close()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/storagetest/-/store", &body)
	c.Request.Header.Set("Content-Type", form.FormDataContentType())
	c.Request.Header.Set("Accept", "application/json")
	c.Request.Header.Set("Authorization", "Bearer "+token)
	c.Request.AddCookie(&http.Cookie{Name: "session", Value: session})
	web_action(c, app, "-/store", nil, routing_class)

	if _, err := os.Stat(filepath.Join(api_file_base(user, app), "three")); err != nil {
		t.Fatalf("the action did not store its files: %d %s", w.Code, w.Body.String())
	}
	if count := walks.Load(); count != 1 {
		t.Errorf("THE POINT: the request walked the storage %d times, want once", count)
	}
}

// TestStorageUploadsShareTheFigure. a.upload stores each file of an
// attachment; it measures from the figure and charges what it writes, so a
// request storing several files walks once and leaves the figure true.
func TestStorageUploadsShareTheFigure(t *testing.T) {
	user, app := upload_test_setup(t)
	walk := storage_walk
	walks := &atomic.Int64{}
	storage_walk = func(path string) (int64, error) {
		walks.Add(1)
		return dir_size(path)
	}
	t.Cleanup(func() { storage_walk = walk })

	action, thread := upload_test_action(t, user, user, app, "file", "photo.jpg", strings.Repeat("p", 3000))
	for _, name := range []string{"first.jpg", "second.jpg"} {
		if _, err := upload_test_call(action, thread, "file", name); err != nil {
			t.Fatalf("a.upload: %v", err)
		}
	}
	if count := walks.Load(); count != 1 {
		t.Errorf("THE POINT: two uploads walked the storage %d times, want once", count)
	}
	walked, err := dir_size(user_storage_dir(user))
	if err != nil {
		t.Fatal(err)
	}
	if figure := storage_test_figure(t, user.UID); figure != walked {
		t.Errorf("THE POINT: after two uploads the figure is %d bytes and a walk finds %d", figure, walked)
	}
}

// TestStorageOwnerThroughALink. A repository's filesystem names its root with
// links resolved, so a data directory reached through a link must still
// place a path in its user's storage.
func TestStorageOwnerThroughALink(t *testing.T) {
	test_data_directory(t)
	link := filepath.Join(t.TempDir(), "data")
	if err := os.Symlink(data_dir, link); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}
	previous := data_dir
	data_dir = link
	t.Cleanup(func() { data_dir = previous })
	real := filepath.Join(previous, "users", "u-linked", "repositories", "repository")
	if err := os.MkdirAll(real, 0755); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(real)
	if err != nil {
		t.Fatal(err)
	}
	if owner := storage_owner(resolved); owner != "u-linked" {
		t.Errorf("THE POINT: a path through the resolved data directory belongs to %q, want u-linked", owner)
	}
	if owner := storage_owner(filepath.Join(link, "users", "u-linked", "files")); owner != "u-linked" {
		t.Errorf("a path through the link belongs to %q, want u-linked", owner)
	}
}

// storage_test_stale ages a user's figure past its lifetime, as a minute
// passing would, so the next measurement walks.
func storage_test_stale(t *testing.T, uid string) {
	t.Helper()
	found, ok := storage_figures.Load(uid)
	if !ok {
		t.Fatal("setup: no figure to age")
	}
	figure := found.(*storage_figure)
	figure.lock.Lock()
	figure.measured = figure.measured.Add(-storage_lifetime)
	figure.lock.Unlock()
}

// storage_test_agree fails the test when a user's figure is not what a walk
// finds.
func storage_test_agree(t *testing.T, user *User, what string) {
	t.Helper()
	walked, err := dir_size(user_storage_dir(user))
	if err != nil {
		t.Fatal(err)
	}
	if figure := storage_test_figure(t, user.UID); figure != walked {
		t.Errorf("THE POINT: after %s the figure is %d bytes and a walk finds %d", what, figure, walked)
	}
}

// storage_test_writer is an io.Writer made of a function.
type storage_test_writer func([]byte) (int, error)

func (w storage_test_writer) Write(data []byte) (int, error) { return w(data) }

// TestStorageChangeDuringAWalkCountedOnce. A delete, an overwrite or a move
// made while a walk runs, before the walk reaches the file, must not be
// credited after a walk that never counted the file: the user would be given
// its bytes twice, once a walk, and could store past the quota by that much
// again each minute.
func TestStorageChangeDuringAWalkCountedOnce(t *testing.T) {
	builtin := sl.NewBuiltin("mochi.file", nil)
	for _, c := range []struct {
		name   string
		change func(thread *sl.Thread, base string) error
	}{
		{"a delete", func(thread *sl.Thread, base string) error {
			_, err := api_file_delete(thread, builtin, sl.Tuple{sl.String("zz-big")}, nil)
			return err
		}},
		{"an upload over a file", func(thread *sl.Thread, base string) error {
			root, err := os.OpenRoot(base)
			if err != nil {
				return err
			}
			defer root.Close()
			_, err = root_write_file(root, "zz-big", strings.NewReader("small"))
			return err
		}},
		{"a move over a file", func(thread *sl.Thread, base string) error {
			_, err := api_file_move(thread, builtin, sl.Tuple{sl.String("zz-small"), sl.String("zz-big")}, nil)
			return err
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			base, thread, user, _ := storage_test_environment(t, 1<<30)
			for name, size := range map[string]int{"keep": 100000, "zz-big": 50000, "zz-small": 7000} {
				if err := os.WriteFile(filepath.Join(base, name), make([]byte, size), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := user_storage_remaining(user); err != nil {
				t.Fatal(err)
			}
			storage_test_stale(t, user.UID)
			done := make(chan error, 1)
			storage_walk = func(path string) (int64, error) {
				// The change is made once the walk has begun, before it reaches
				// the file; a change that has to wait for the walk waits.
				go func() { done <- c.change(thread, base) }()
				select {
				case err := <-done:
					done <- err
				case <-time.After(200 * time.Millisecond):
				}
				return dir_size(path)
			}
			if _, err := user_storage_remaining(user); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatalf("the change: %v", err)
			}
			storage_test_agree(t, user, c.name+" made while a walk ran")
		})
	}
}

// TestStorageWalkKeepsAWriteUnderWay. A walk that runs while a write is part
// way through must not lose the write's charge: one charged before the walk
// and landing after it is missing from both the walk and the figure it sets.
func TestStorageWalkKeepsAWriteUnderWay(t *testing.T) {
	base, _, user, _ := storage_test_environment(t, 1<<30)
	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(base, "landing"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	started := make(chan struct{})
	walked := make(chan struct{})
	writer := storage_test_writer(func(data []byte) (int, error) {
		close(started)
		select {
		case <-walked:
		case <-time.After(200 * time.Millisecond):
		}
		return file.Write(data)
	})
	done := make(chan error, 1)
	go func() {
		_, err := (&storage_meter{writer: writer, owner: user.UID}).Write(make([]byte, 40000))
		done <- err
	}()
	<-started
	storage_test_stale(t, user.UID)
	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}
	close(walked)
	if err := <-done; err != nil {
		t.Fatalf("the write: %v", err)
	}
	storage_test_agree(t, user, "a write a walk ran beside")
}

// TestStorageFigureDatedFromTheWalkStart. A file credited only when it is
// older than the walk (a repository's objects) must be older than the walk's
// start: one written while the walk ran may be missing from it.
func TestStorageFigureDatedFromTheWalkStart(t *testing.T) {
	_, _, user, _ := storage_test_environment(t, 1<<30)
	var start time.Time
	storage_walk = func(path string) (int64, error) {
		start = time.Now()
		time.Sleep(10 * time.Millisecond)
		return dir_size(path)
	}
	if _, err := user_storage_remaining(user); err != nil {
		t.Fatal(err)
	}
	found, _ := storage_figures.Load(user.UID)
	figure := found.(*storage_figure)
	figure.lock.Lock()
	measured := figure.measured
	figure.lock.Unlock()
	if measured.After(start) {
		t.Errorf("THE POINT: the figure is dated %v, after its walk began at %v", measured, start)
	}
}

// TestArchiveRefusedPastTheQuotaLeavesNothing. An archive or an extraction
// the quota stops is removed whole and given back, and says the storage limit
// was exceeded: a partial archive the size of all the room left otherwise
// stayed, filling the account.
func TestArchiveRefusedPastTheQuotaLeavesNothing(t *testing.T) {
	refused := func(t *testing.T, err error, what string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "storage limit exceeded") || strings.Contains(err.Error(), "unable to") {
			t.Errorf("THE POINT: %s past the quota answered %v, want the storage limit exceeded", what, err)
		}
	}

	t.Run("write", func(t *testing.T) {
		base, thread, user, _ := storage_test_environment(t, 20000)
		if err := os.WriteFile(filepath.Join(base, "source"), push_noise(t, 12000), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := user_storage_remaining(user); err != nil {
			t.Fatal(err)
		}
		entry := sl.NewDict(2)
		entry.SetKey(sl.String("name"), sl.String("source"))
		entry.SetKey(sl.String("file"), sl.String("source"))
		_, err := api_archive_write(thread, sl.NewBuiltin("mochi.archive.write", nil), sl.Tuple{sl.String("out.zip"), sl.NewList([]sl.Value{entry})}, nil)
		refused(t, err, "an archive")
		if _, err := os.Stat(filepath.Join(base, "out.zip")); !os.IsNotExist(err) {
			t.Errorf("THE POINT: the refused archive was left: %v", err)
		}
		storage_test_agree(t, user, "a refused archive")
	})

	t.Run("extract", func(t *testing.T) {
		base, thread, user, _ := storage_test_environment(t, 25000)
		var buffer bytes.Buffer
		writer := zip.NewWriter(&buffer)
		entry, err := writer.CreateHeader(&zip.FileHeader{Name: "big", Method: zip.Store})
		if err != nil {
			t.Fatal(err)
		}
		entry.Write(push_noise(t, 15000))
		writer.Close()
		if err := os.WriteFile(filepath.Join(base, "bundle.zip"), buffer.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := user_storage_remaining(user); err != nil {
			t.Fatal(err)
		}
		_, err = api_archive_extract(thread, sl.NewBuiltin("mochi.archive.extract", nil), sl.Tuple{sl.String("bundle.zip"), sl.String("big"), sl.String("out/big")}, nil)
		refused(t, err, "an extraction")
		if _, err := os.Stat(filepath.Join(base, "out", "big")); !os.IsNotExist(err) {
			t.Errorf("THE POINT: the refused extraction was left: %v", err)
		}
		storage_test_agree(t, user, "a refused extraction")
	})
}

// TestStorageUploadSharesTheRequestWalk. a.upload measures on the walk its
// request already made - the multipart check's, or an earlier upload's - so a
// request that outlives the figure's lifetime does not walk again per file.
func TestStorageUploadSharesTheRequestWalk(t *testing.T) {
	user, app := upload_test_setup(t)
	lifetime := storage_lifetime
	storage_lifetime = 0
	walk := storage_walk
	walks := &atomic.Int64{}
	storage_walk = func(path string) (int64, error) {
		walks.Add(1)
		return dir_size(path)
	}
	t.Cleanup(func() {
		storage_lifetime = lifetime
		storage_walk = walk
	})

	action, thread := upload_test_action(t, user, user, app, "file", "photo.jpg", strings.Repeat("p", 3000))
	for _, name := range []string{"first.jpg", "second.jpg"} {
		if _, err := upload_test_call(action, thread, "file", name); err != nil {
			t.Fatalf("a.upload: %v", err)
		}
	}
	if count := walks.Load(); count != 1 {
		t.Errorf("THE POINT: two uploads in one request walked the storage %d times, want once", count)
	}
}

// TestStorageFigureDroppedOnPromotion. An administrator's storage is exempt
// and never measured, so the figure kept from before a promotion must go: a
// write would otherwise be held to the quota the role exempts it from.
func TestStorageFigureDroppedOnPromotion(t *testing.T) {
	username_test_setup(t)
	db_open("db/users.db").exec("insert into users (uid, username, role) values ('u2', 'bob@example.com', 'user')")
	full := storage_figure_of("u2")
	full.lock.Lock()
	full.used, full.measured = file_maximum_storage, time.Now()
	full.lock.Unlock()

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: "u1", Username: "alice@example.com", Role: "administrator"})
	thread.SetLocal("app", &App{id: "settings", internal: &AppVersion{}})
	if _, err := api_user_update(thread, sl.NewBuiltin("mochi.user.update", nil), sl.Tuple{sl.String("u2"), sl.None, sl.String("administrator")}, nil); err != nil {
		t.Fatalf("promoting: %v", err)
	}
	if _, kept := storage_figures.Load("u2"); kept {
		t.Error("THE POINT: the figure from before the promotion was kept")
	}
	base := filepath.Join(data_dir, "users", "u2", "files")
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := root_write_file(root, "upload", strings.NewReader("an administrator's upload")); err != nil {
		t.Errorf("THE POINT: the promoted administrator's write was refused: %v", err)
	}
}
