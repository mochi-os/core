// Mochi server: Label resolution tests
// Copyright Alistair Cunningham 2026

package main

import (
	"reflect"
	"testing"
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

		// English variant routing table: en-PH goes through en-us
		{"en-ph", []string{"en-ph", "en-us", "en"}},

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
