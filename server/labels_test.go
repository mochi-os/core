// Mochi server: Label resolution tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestParseAcceptLanguage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{"empty", "", nil},
		{"single tag", "en", []string{"en"}},
		{"single tag with region", "en-GB", []string{"en-gb"}},
		{"multiple tags ordered", "fr,en;q=0.5", []string{"fr", "en"}},
		{"explicit quality", "en;q=0.9,fr;q=1.0", []string{"fr", "en"}},
		{"complex priority", "en-GB,en;q=0.9,fr;q=0.5,de;q=0.7", []string{"en-gb", "en", "de", "fr"}},
		{"wildcard dropped", "*,en;q=0.5", []string{"en"}},
		{"whitespace tolerated", " fr , en ; q=0.8 ", []string{"fr", "en"}},
		{"malformed q ignored treated as 1", "fr;q=invalid,en;q=0.5", []string{"fr", "en"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parse_accept_language(tt.input)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("parse_accept_language(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLanguageFallbacks(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		// Bare and empty
		{"", []string{"en"}},
		{"en", []string{"en"}},
		{"EN", []string{"en"}}, // normalised to lowercase
		{" en ", []string{"en"}},

		// English variants -> en directly (source is Commonwealth-flavoured)
		{"en-gb", []string{"en-gb", "en"}},
		{"en-us", []string{"en-us", "en"}},
		{"en-au", []string{"en-au", "en"}},
		{"en-ca", []string{"en-ca", "en"}},
		{"en-nz", []string{"en-nz", "en"}},

		// Variant routing table: redirect to the nearest translated catalog,
		// then walk that target's own parents (mirrors web fallbackLocales).
		{"en-ph", []string{"en-ph", "en-us", "en"}},
		{"zh-hk", []string{"zh-hk", "zh-hant", "zh", "en"}},
		{"yue", []string{"yue", "zh-hant", "zh", "en"}},
		{"es-ar", []string{"es-ar", "es-419", "es", "en"}},
		{"nn", []string{"nn", "nb", "en"}},

		// Generic parent stripping
		{"fr", []string{"fr", "en"}},
		{"pt-br", []string{"pt-br", "pt", "en"}},
		{"zh-hant", []string{"zh-hant", "zh", "en"}},
		{"zh-hant-hk", []string{"zh-hant-hk", "zh-hant", "zh", "en"}},

		// Pseudo-locale (private use)
		{"en-x-pseudo", []string{"en-x-pseudo", "en-x", "en"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := language_fallbacks(tt.input)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("language_fallbacks(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestFormatMessage(t *testing.T) {
	tests := []struct {
		name     string
		format   string
		locale   string
		args     map[string]any
		expected string
	}{
		{
			name:     "no placeholders no args",
			format:   "Hello, world!",
			locale:   "en",
			args:     nil,
			expected: "Hello, world!",
		},
		{
			name:     "named placeholder",
			format:   "Hello, {name}!",
			locale:   "en",
			args:     map[string]any{"name": "Alice"},
			expected: "Hello, Alice!",
		},
		{
			name:     "english plural one",
			format:   "{count, plural, one {# unread post} other {# unread posts}}",
			locale:   "en",
			args:     map[string]any{"count": int64(1)},
			expected: "1 unread post",
		},
		{
			name:     "english plural other",
			format:   "{count, plural, one {# unread post} other {# unread posts}}",
			locale:   "en",
			args:     map[string]any{"count": int64(5)},
			expected: "5 unread posts",
		},
		{
			name:     "japanese plural collapses to other",
			format:   "{count, plural, other {#件の未読}}",
			locale:   "ja",
			args:     map[string]any{"count": int64(3)},
			expected: "3件の未読",
		},
		{
			name:     "no args returns format unchanged",
			format:   "Hello, {name}!",
			locale:   "en",
			args:     nil,
			expected: "Hello, {name}!",
		},
		{
			name:     "unknown locale falls back to english plurals",
			format:   "{count, plural, one {# item} other {# items}}",
			locale:   "xx",
			args:     map[string]any{"count": int64(1)},
			expected: "1 item",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := format_message(tt.format, tt.locale, tt.args)
			if got != tt.expected {
				t.Errorf("format_message(%q, %q, %v) = %q, want %q",
					tt.format, tt.locale, tt.args, got, tt.expected)
			}
		})
	}
}

// The mochi_language cookie is the server's to write. It used to be written by
// the shell, from a message any app in the frame could send, which put an
// origin-wide cookie write behind a postMessage.
func TestLanguageCookieIsWrittenByTheServer(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Written when the request carries no cookie yet.
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/shell", nil)
	language_cookie(c, "fr")
	set := w.Header().Get("Set-Cookie")
	if !strings.Contains(set, "mochi_language=fr") {
		t.Errorf("no language cookie written: %q", set)
	}
	if !strings.Contains(set, "; Path=/;") {
		t.Errorf("cookie is not origin-wide, so a page outside the app would not see it: %q", set)
	}
	// The login page's picker has no session and must be able to write this
	// cookie for itself; an httpOnly cookie of the same name would silently
	// refuse that write.
	if strings.Contains(set, "HttpOnly") {
		t.Errorf("cookie is HttpOnly, which blocks the signed-out picker's own write: %q", set)
	}

	// Rewritten when the request's cookie disagrees with the resolved language.
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/shell", nil)
	c.Request.AddCookie(&http.Cookie{Name: "mochi_language", Value: "de"})
	language_cookie(c, "fr")
	if !strings.Contains(w.Header().Get("Set-Cookie"), "mochi_language=fr") {
		t.Error("a stale cookie was not brought into step with the resolved language")
	}

	// Left alone when it already agrees — an unchanged language costs no header.
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/shell", nil)
	c.Request.AddCookie(&http.Cookie{Name: "mochi_language", Value: "fr"})
	language_cookie(c, "fr")
	if got := w.Header().Get("Set-Cookie"); got != "" {
		t.Errorf("rewrote an unchanged cookie: %q", got)
	}

	// An unresolved language must not clear the user's stored choice.
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/_/shell", nil)
	language_cookie(c, "")
	if got := w.Header().Get("Set-Cookie"); got != "" {
		t.Errorf("wrote a cookie for an empty language: %q", got)
	}
}
