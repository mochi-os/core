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

// request_language consults the stored preference ahead of the cookie and the
// Accept-Language header, and returns it directly. The write paths validate a
// tag before storing it, but that is validation at one moment: a preference
// written under an older validator keeps whatever it was given, and every
// later request reads it. Re-validating on the read is what makes the bound
// on "locale" apply to values already on disk.
func TestRequestLanguageValidatesTheStoredPreference(t *testing.T) {
	tests := []struct {
		name       string
		preference string
		want       string
	}{
		{"a usable tag is returned", "pt-br", "pt-br"},
		{"the auto sentinel falls through", "auto", "en"},
		{"an empty preference falls through", "", "en"},
		{"a tag past the subtag bound falls through", "en" + strings.Repeat("-aa", 1360), "en"},
		{"a malformed tag falls through", "en_GB", "en"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u := &User{Preferences: map[string]string{
				"language": test.preference,
				// Pre-seeded so the fall-through cases resolve to the value
				// already stored and request_language skips its last_language
				// write, which would need a user database this test has none of.
				"last_language": "en",
			}}
			if got := request_language(nil, u); got != test.want {
				t.Errorf("request_language with preference %q = %q, want %q",
					truncate_tag(test.preference), got, test.want)
			}
		})
	}
}

// truncate_tag keeps a failure message readable when the preference under test
// is thousands of characters long.
func truncate_tag(tag string) string {
	if len(tag) <= 40 {
		return tag
	}
	return tag[:40] + "..."
}

// user_language is the no-gin.Context reader - notification and email
// composers, queued jobs, and the account-provider field labels. It consults
// the same two preferences request_language does, so it needs the same
// re-validation: a.user.preference.set writes any string with no shape check
// and no permission gating it, so an app can leave a tag on disk that
// language_fallbacks would expand one chain entry per subtag, for every label
// resolved for that user.
func TestUserLanguageValidatesBothStoredPreferences(t *testing.T) {
	tests := []struct {
		name     string
		language string
		last     string
		want     string
	}{
		{"a usable tag is returned", "pt-br", "en", "pt-br"},
		{"the auto sentinel falls through to last_language", "auto", "de", "de"},
		{"an empty preference falls through to last_language", "", "de", "de"},
		{"a language past the subtag bound falls through", "en" + strings.Repeat("-aa", 1360), "de", "de"},
		{"a malformed language falls through", "en_GB", "de", "de"},
		{"a last_language past the subtag bound falls through to en", "", "en" + strings.Repeat("-aa", 1360), "en"},
		{"a malformed last_language falls through to en", "", "en_GB", "en"},
		{"neither preference set", "", "", "en"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u := &User{Preferences: map[string]string{
				"language":      test.language,
				"last_language": test.last,
			}}
			if got := user_language(u); got != test.want {
				t.Errorf("user_language with language %q, last_language %q = %q, want %q",
					truncate_tag(test.language), truncate_tag(test.last), got, test.want)
			}
		})
	}

	if got := user_language(nil); got != "en" {
		t.Errorf("user_language(nil) = %q, want \"en\"", got)
	}
}

// TestLoadedLabelsMatchTheFilesByteForByte is the gate on the parser options.
//
// go-ini's default treats ";" and "#" as inline comment delimiters, so any
// label value carrying one was truncated at runtime with no error and no log
// line: errors.populated_server resolved to "This server has users" and lost
// the sentence saying what to do instead, in English and in every translation.
//
// Comparing the loaded map against the file itself is the assertion that
// generalises - it fails for any value the loader shortens, not only for the
// keys that happen to carry a semicolon today.
func TestLoadedLabelsMatchTheFilesByteForByte(t *testing.T) {
	load_core_labels()

	if len(core_labels) == 0 {
		t.Fatal("no catalogues loaded; the rest of this test would be vacuous")
	}
	checked := 0
	for tag, loaded := range core_labels {
		onfile, ok := catalogue_cells(t, tag)
		if !ok {
			t.Errorf("%s is loaded but labels/%s.conf cannot be read", tag, tag)
			continue
		}
		for key, want := range onfile {
			got, present := loaded[key]
			if !present {
				t.Errorf("%s.conf holds %q but the loader dropped it", tag, key)
				continue
			}
			if got != want {
				t.Errorf("%s.conf %q loads as %q, want %q: the parser is shortening the value",
					tag, key, got, want)
			}
			checked++
		}
	}
	if checked < 1000 {
		t.Errorf("compared only %d cells across %d catalogues; the catalogues hold thousands, so this run measured almost nothing",
			checked, len(core_labels))
	}
}

// TestLabelParserStillIgnoresWholeLineComments is the other half: disabling
// inline comments must not turn a comment line into a label. Every catalogue
// carries "#" headers, and the app-side loader (apps.go) has the opposite
// defect - it splits every line on "=" with no comment handling at all - so
// this is worth pinning rather than assuming.
func TestLabelParserStillIgnoresWholeLineComments(t *testing.T) {
	body := []byte("[labels]\n# hash comment = not a label\n; semicolon comment = also not a label\n" +
		"errors.example = has users; and a second half\nerrors.hashed = a value with a # inside\n")

	cfg, err := ini_parse(body)
	if err != nil {
		t.Fatalf("ini_parse: %v", err)
	}
	section := cfg.Section("labels")

	if got, want := section.Key("errors.example").String(), "has users; and a second half"; got != want {
		t.Errorf("errors.example = %q, want %q: truncated at the semicolon", got, want)
	}
	if got, want := section.Key("errors.hashed").String(), "a value with a # inside"; got != want {
		t.Errorf("errors.hashed = %q, want %q: truncated at the hash", got, want)
	}

	want := []string{"errors.example", "errors.hashed"}
	if got := section.KeyStrings(); !reflect.DeepEqual(got, want) {
		t.Errorf("keys = %v, want %v: a comment line became a label", got, want)
	}
}
