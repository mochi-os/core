package main

import (
	"strings"
	"testing"
)

func TestAttachmentDisposition(t *testing.T) {
	tests := []struct {
		name       string
		wantInline bool
	}{
		{"photo.jpg", true},
		{"photo.jpeg", true},
		{"image.png", true},
		{"image.gif", true},
		{"image.webp", true},
		{"video.mp4", true},
		{"audio.mp3", true},
		{"document.pdf", true},
		{"evil.html", false},
		{"evil.htm", false},
		{"evil.svg", false},
		{"script.js", false},
		{"style.css", false},
		{"data.json", false},
		{"archive.zip", false},
		{"program.exe", false},
		{"noextension", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct := attachment_content_type(tt.name)
			disposition := "attachment"
			if (strings.HasPrefix(ct, "image/") && ct != "image/svg+xml") ||
				strings.HasPrefix(ct, "video/") || strings.HasPrefix(ct, "audio/") ||
				ct == "application/pdf" {
				disposition = "inline"
			}
			isInline := disposition == "inline"
			if isInline != tt.wantInline {
				t.Errorf("%s: ct=%q disposition=%q, wantInline=%v", tt.name, ct, disposition, tt.wantInline)
			}
		})
	}
}
