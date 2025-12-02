// Mochi server: Attachment unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Test attachment_content_type detection
func TestAttachmentContentType(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected string
	}{
		{"text file", "document.txt", "text/plain; charset=utf-8"},
		{"PNG image", "image.png", "image/png"},
		{"JPEG image", "photo.jpg", "image/jpeg"},
		{"JPEG alt", "photo.jpeg", "image/jpeg"},
		{"GIF image", "animation.gif", "image/gif"},
		{"PDF document", "report.pdf", "application/pdf"},
		{"JSON file", "data.json", "application/json"},
		{"HTML file", "page.html", "text/html; charset=utf-8"},
		{"CSS file", "style.css", "text/css; charset=utf-8"},
		{"JavaScript", "script.js", "text/javascript; charset=utf-8"},
		{"no extension", "README", "application/octet-stream"},
		{"unknown ext", "file.xyz", "application/octet-stream"},
		{"empty name", "", "application/octet-stream"},
		{"dot only", ".", "application/octet-stream"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := attachment_content_type(tt.filename)
			if result != tt.expected {
				t.Errorf("attachment_content_type(%q) = %q, want %q", tt.filename, result, tt.expected)
			}
		})
	}
}

// Test Attachment.to_map conversion
func TestAttachmentToMap(t *testing.T) {
	att := &Attachment{
		ID:          "test123",
		Object:      "post/abc",
		Entity:      "entity456",
		Name:        "document.pdf",
		Size:        1024,
		ContentType: "application/pdf",
		Creator:     "user789",
		Caption:     "Test caption",
		Description: "Test description",
		Rank:        1,
		Created:     1700000000,
	}

	m := att.to_map()

	// Verify all fields are present and correct
	if m["id"] != "test123" {
		t.Errorf("id = %v, want test123", m["id"])
	}
	if m["object"] != "post/abc" {
		t.Errorf("object = %v, want post/abc", m["object"])
	}
	if m["entity"] != "entity456" {
		t.Errorf("entity = %v, want entity456", m["entity"])
	}
	if m["name"] != "document.pdf" {
		t.Errorf("name = %v, want document.pdf", m["name"])
	}
	if m["size"] != int64(1024) {
		t.Errorf("size = %v, want 1024", m["size"])
	}
	if m["content_type"] != "application/pdf" {
		t.Errorf("content_type = %v, want application/pdf", m["content_type"])
	}
	if m["type"] != "application/pdf" {
		t.Errorf("type = %v, want application/pdf", m["type"])
	}
	if m["creator"] != "user789" {
		t.Errorf("creator = %v, want user789", m["creator"])
	}
	if m["caption"] != "Test caption" {
		t.Errorf("caption = %v, want Test caption", m["caption"])
	}
	if m["description"] != "Test description" {
		t.Errorf("description = %v, want Test description", m["description"])
	}
	if m["rank"] != 1 {
		t.Errorf("rank = %v, want 1", m["rank"])
	}
	if m["created"] != int64(1700000000) {
		t.Errorf("created = %v, want 1700000000", m["created"])
	}
	if m["image"] != false {
		t.Errorf("image = %v, want false for pdf", m["image"])
	}
	// Without app_path, url should not be set
	if _, ok := m["url"]; ok {
		t.Errorf("url should not be set without app_path, got %v", m["url"])
	}
}

// Test Attachment.to_map with app_path for URL generation
func TestAttachmentToMapWithURL(t *testing.T) {
	// Test non-image attachment
	att := &Attachment{
		ID:   "abc123",
		Name: "document.pdf",
	}

	m := att.to_map("chat")

	if m["url"] != "/chat/files/abc123" {
		t.Errorf("url = %v, want /chat/files/abc123", m["url"])
	}
	if m["image"] != false {
		t.Errorf("image = %v, want false", m["image"])
	}
	if _, ok := m["thumbnail_url"]; ok {
		t.Errorf("thumbnail_url should not be set for non-image")
	}

	// Test image attachment
	img_att := &Attachment{
		ID:   "img456",
		Name: "photo.jpg",
	}

	img_m := img_att.to_map("feeds")

	if img_m["url"] != "/feeds/files/img456" {
		t.Errorf("url = %v, want /feeds/files/img456", img_m["url"])
	}
	if img_m["image"] != true {
		t.Errorf("image = %v, want true", img_m["image"])
	}
	if img_m["thumbnail_url"] != "/feeds/files/img456/thumbnail" {
		t.Errorf("thumbnail_url = %v, want /feeds/files/img456/thumbnail", img_m["thumbnail_url"])
	}
}

// Test Attachment.attachment_url
func TestAttachmentURL(t *testing.T) {
	att := &Attachment{ID: "test123"}

	tests := []struct {
		name     string
		app_path string
		expected string
	}{
		{"chat app", "chat", "/chat/files/test123"},
		{"feeds app", "feeds", "/feeds/files/test123"},
		{"forums app", "forums", "/forums/files/test123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := att.attachment_url(tt.app_path)
			if result != tt.expected {
				t.Errorf("attachment_url(%q) = %q, want %q", tt.app_path, result, tt.expected)
			}
		})
	}
}

// Test DB.attachment_path sanitization
func TestDBAttachmentPath(t *testing.T) {
	// Create a minimal DB struct for testing
	db := &DB{
		user: &User{ID: 42},
	}

	tests := []struct {
		name     string
		id       string
		filename string
		expected string
	}{
		{"normal file", "abc123", "document.pdf", "users/42/files/abc123_document.pdf"},
		{"with spaces", "abc123", "my document.pdf", "users/42/files/abc123_my document.pdf"},
		{"path traversal attempt", "abc123", "../../../etc/passwd", "users/42/files/abc123_passwd"},
		{"absolute path attempt", "abc123", "/etc/passwd", "users/42/files/abc123_passwd"},
		{"empty name", "abc123", "", "users/42/files/abc123_file"},
		{"dot only", "abc123", ".", "users/42/files/abc123_file"},
		{"dot dot", "abc123", "..", "users/42/files/abc123_file"},
		{"nested path", "abc123", "subdir/file.txt", "users/42/files/abc123_file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := db.attachment_path(tt.id, tt.filename)
			if result != tt.expected {
				t.Errorf("attachment_path(%q, %q) = %q, want %q", tt.id, tt.filename, result, tt.expected)
			}
		})
	}
}

// Test migration helper: get_app_from_object
func TestGetAppFromObject(t *testing.T) {
	tests := []struct {
		name     string
		object   string
		expected string
	}{
		{"chat object", "chat/conv123/msg456", "chat"},
		{"forums object", "forums/forum123/post456", "forums"},
		{"feeds object", "feeds/post123", "feeds"},
		{"unknown app", "unknown/something", "unknown"},
		{"single segment", "chat", "chat"},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := get_app_from_object(tt.object)
			if result != tt.expected {
				t.Errorf("get_app_from_object(%q) = %q, want %q", tt.object, result, tt.expected)
			}
		})
	}
}

// Test migration helper: normalize_object
func TestNormalizeObject(t *testing.T) {
	tests := []struct {
		name     string
		app      string
		object   string
		expected string
	}{
		{"forums full path", "forums", "forums/forum123/post456", "post456"},
		{"forums short path", "forums", "forums/post456", "forums/post456"},
		{"feeds with prefix", "feeds", "feeds/post123", "post123"},
		{"feeds no prefix", "feeds", "post123", "post123"},
		{"chat unchanged", "chat", "chat/conv123/msg456", "chat/conv123/msg456"},
		{"other app unchanged", "other", "other/something", "other/something"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalize_object(tt.app, tt.object)
			if result != tt.expected {
				t.Errorf("normalize_object(%q, %q) = %q, want %q", tt.app, tt.object, result, tt.expected)
			}
		})
	}
}

// Test copy_file function
func TestCopyFile(t *testing.T) {
	// Create temp directory
	tmp_dir, err := os.MkdirTemp("", "mochi_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)

	src_path := filepath.Join(tmp_dir, "source.txt")
	dst_path := filepath.Join(tmp_dir, "dest.txt")
	content := []byte("test content for copy")

	// Create source file
	if err := os.WriteFile(src_path, content, 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	// Test copy
	if err := copy_file(src_path, dst_path); err != nil {
		t.Fatalf("copy_file failed: %v", err)
	}

	// Verify destination
	result, err := os.ReadFile(dst_path)
	if err != nil {
		t.Fatalf("Failed to read destination: %v", err)
	}

	if string(result) != string(content) {
		t.Errorf("Content mismatch: got %q, want %q", result, content)
	}

	// Test copy of non-existent file
	err = copy_file(filepath.Join(tmp_dir, "nonexistent"), dst_path)
	if err == nil {
		t.Error("Expected error copying non-existent file, got nil")
	}
}

// Test AppAction.Attachments route expansion
func TestAppActionAttachmentsExpansion(t *testing.T) {
	// Simulate what app_version_load does with attachments actions
	actions := map[string]AppAction{
		"files":  {Attachments: true, Public: false},
		"public": {Attachments: true, Public: true},
		"normal": {Function: "some_function"},
	}

	// Collect keys to expand first (safe iteration)
	var to_expand []string
	for name, action := range actions {
		if action.Attachments {
			to_expand = append(to_expand, name)
		}
	}

	// Expand attachment actions
	for _, name := range to_expand {
		action := actions[name]
		actions[name+"/:id"] = AppAction{Attachments: true, Public: action.Public}
		actions[name+"/:id/thumbnail"] = AppAction{Attachments: true, Public: action.Public}
		delete(actions, name)
	}

	// Verify expansion
	if _, ok := actions["files"]; ok {
		t.Error("Original 'files' action should be deleted")
	}
	if _, ok := actions["files/:id"]; !ok {
		t.Error("'files/:id' action should exist")
	}
	if _, ok := actions["files/:id/thumbnail"]; !ok {
		t.Error("'files/:id/thumbnail' action should exist")
	}

	// Verify Public flag is preserved
	if actions["files/:id"].Public != false {
		t.Error("files/:id should not be public")
	}
	if actions["public/:id"].Public != true {
		t.Error("public/:id should be public")
	}

	// Verify normal actions are unchanged
	if actions["normal"].Function != "some_function" {
		t.Error("normal action should be unchanged")
	}
}

// Benchmark attachment_content_type
func BenchmarkAttachmentContentType(b *testing.B) {
	filenames := []string{
		"document.pdf",
		"image.png",
		"README",
		"script.js",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		attachment_content_type(filenames[i%len(filenames)])
	}
}

// Benchmark Attachment.to_map
func BenchmarkAttachmentToMap(b *testing.B) {
	att := &Attachment{
		ID:          "test123",
		Object:      "post/abc",
		Entity:      "entity456",
		Name:        "document.pdf",
		Size:        1024,
		ContentType: "application/pdf",
		Creator:     "user789",
		Caption:     "Test caption",
		Description: "Test description",
		Rank:        1,
		Created:     1700000000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		att.to_map()
	}
}
