// Mochi server: Attachment unit tests
// Copyright Alistair Cunningham 2025

package main

import (
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
	// Test non-image attachment with default action_path
	att := &Attachment{
		ID:   "abc123",
		Name: "document.pdf",
	}

	m := att.to_map("chat")

	if m["url"] != "/chat/attachments/abc123" {
		t.Errorf("url = %v, want /chat/attachments/abc123", m["url"])
	}
	if m["image"] != false {
		t.Errorf("image = %v, want false", m["image"])
	}
	if _, ok := m["thumbnail_url"]; ok {
		t.Errorf("thumbnail_url should not be set for non-image")
	}

	// Test image attachment with default action_path
	img_att := &Attachment{
		ID:   "img456",
		Name: "photo.jpg",
	}

	img_m := img_att.to_map("feeds")

	if img_m["url"] != "/feeds/attachments/img456" {
		t.Errorf("url = %v, want /feeds/attachments/img456", img_m["url"])
	}
	if img_m["image"] != true {
		t.Errorf("image = %v, want true", img_m["image"])
	}
	if img_m["thumbnail_url"] != "/feeds/attachments/img456/thumbnail" {
		t.Errorf("thumbnail_url = %v, want /feeds/attachments/img456/thumbnail", img_m["thumbnail_url"])
	}

	// Test with custom action_path
	custom_att := &Attachment{
		ID:   "custom789",
		Name: "file.txt",
	}

	custom_m := custom_att.to_map("myapp", "files")

	if custom_m["url"] != "/myapp/files/custom789" {
		t.Errorf("url = %v, want /myapp/files/custom789", custom_m["url"])
	}

	// Test image with custom action_path
	custom_img := &Attachment{
		ID:   "img999",
		Name: "photo.png",
	}

	custom_img_m := custom_img.to_map("gallery", "media")

	if custom_img_m["url"] != "/gallery/media/img999" {
		t.Errorf("url = %v, want /gallery/media/img999", custom_img_m["url"])
	}
	if custom_img_m["thumbnail_url"] != "/gallery/media/img999/thumbnail" {
		t.Errorf("thumbnail_url = %v, want /gallery/media/img999/thumbnail", custom_img_m["thumbnail_url"])
	}
}

// Test Attachment.attachment_url
func TestAttachmentURL(t *testing.T) {
	att := &Attachment{ID: "test123"}

	tests := []struct {
		name        string
		app_path    string
		action_path string
		entity      string
		expected    string
	}{
		{"chat default", "chat", "attachments", "", "/chat/attachments/test123"},
		{"feeds default", "feeds", "attachments", "", "/feeds/attachments/test123"},
		{"forums default", "forums", "attachments", "", "/forums/attachments/test123"},
		{"custom files", "myapp", "files", "", "/myapp/files/test123"},
		{"custom media", "gallery", "media", "", "/gallery/media/test123"},
		{"with entity", "feeds", "attachments", "alice@example.com", "/feeds/alice@example.com/-/attachments/test123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := att.attachment_url(tt.app_path, tt.action_path, tt.entity)
			if result != tt.expected {
				t.Errorf("attachment_url(%q, %q, %q) = %q, want %q", tt.app_path, tt.action_path, tt.entity, result, tt.expected)
			}
		})
	}
}

// Test attachment_path sanitization
func TestAttachmentPath(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		filename string
		expected string
	}{
		{"normal file", "abc123", "document.pdf", "users/42/wiki/files/abc123_document.pdf"},
		{"with spaces", "abc123", "my document.pdf", "users/42/wiki/files/abc123_my document.pdf"},
		{"path traversal attempt", "abc123", "../../../etc/passwd", "users/42/wiki/files/abc123_passwd"},
		{"absolute path attempt", "abc123", "/etc/passwd", "users/42/wiki/files/abc123_passwd"},
		{"empty name", "abc123", "", "users/42/wiki/files/abc123_file"},
		{"dot only", "abc123", ".", "users/42/wiki/files/abc123_file"},
		{"dot dot", "abc123", "..", "users/42/wiki/files/abc123_file"},
		{"nested path", "abc123", "subdir/file.txt", "users/42/wiki/files/abc123_file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := attachment_path(42, "wiki", tt.id, tt.filename)
			if result != tt.expected {
				t.Errorf("attachment_path(42, \"wiki\", %q, %q) = %q, want %q", tt.id, tt.filename, result, tt.expected)
			}
		})
	}
}

// Test attachment_files_base helper function
func TestAttachmentFilesBase(t *testing.T) {
	origDataDir := data_dir
	data_dir = "/var/lib/mochi"
	defer func() { data_dir = origDataDir }()

	tests := []struct {
		name     string
		user_id  int
		app_id   string
		expected string
	}{
		{"basic", 42, "chat", "/var/lib/mochi/users/42/chat/files"},
		{"user 1", 1, "forums", "/var/lib/mochi/users/1/forums/files"},
		{"large user id", 999999, "feeds", "/var/lib/mochi/users/999999/feeds/files"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := attachment_files_base(tt.user_id, tt.app_id)
			if result != tt.expected {
				t.Errorf("attachment_files_base(%d, %q) = %q, want %q", tt.user_id, tt.app_id, result, tt.expected)
			}
		})
	}
}

// Test attachment_filename helper function
func TestAttachmentFilename(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		filename string
		expected string
	}{
		{"normal file", "abc123", "document.pdf", "abc123_document.pdf"},
		{"with spaces", "xyz789", "my file.txt", "xyz789_my file.txt"},
		{"path traversal blocked", "id1", "../../../etc/passwd", "id1_passwd"},
		{"absolute path blocked", "id2", "/etc/shadow", "id2_shadow"},
		{"empty name", "id3", "", "id3_file"},
		{"dot only", "id4", ".", "id4_file"},
		{"dot dot", "id5", "..", "id5_file"},
		{"nested path", "id6", "subdir/nested/file.txt", "id6_file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := attachment_filename(tt.id, tt.filename)
			if result != tt.expected {
				t.Errorf("attachment_filename(%q, %q) = %q, want %q", tt.id, tt.filename, result, tt.expected)
			}
		})
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
