// Mochi server: Utilities unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

// Test atoi function
func TestAtoi(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		def      int64
		expected int64
	}{
		{"valid positive", "123", 0, 123},
		{"valid negative", "-456", 0, -456},
		{"valid zero", "0", 99, 0},
		{"empty string", "", 42, 42},
		{"invalid string", "abc", 99, 99},
		{"mixed content", "12abc", 99, 99},
		{"float string", "12.34", 99, 99},
		{"whitespace", " 123", 99, 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := atoi(tt.input, tt.def)
			if result != tt.expected {
				t.Errorf("atoi(%q, %d) = %d, want %d", tt.input, tt.def, result, tt.expected)
			}
		})
	}
}

// Test any_to_string function
func TestAnyToString(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"string", "hello", "hello"},
		{"nil", nil, ""},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int", 42, "42"},
		{"int64", int64(123456789), "123456789"},
		{"float whole", float64(42), "42"},
		{"float decimal", float64(3.14159), "3.14159"},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := any_to_string(tt.input)
			if result != tt.expected {
				t.Errorf("any_to_string(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test fingerprint function
func TestFingerprint(t *testing.T) {
	// Fingerprints should be deterministic
	fp1 := fingerprint("test")
	fp2 := fingerprint("test")
	if fp1 != fp2 {
		t.Errorf("fingerprint should be deterministic: %q != %q", fp1, fp2)
	}

	// Different inputs should produce different fingerprints
	fp3 := fingerprint("other")
	if fp1 == fp3 {
		t.Errorf("different inputs should produce different fingerprints")
	}

	// Fingerprint should be 9 characters
	if len(fp1) != 9 {
		t.Errorf("fingerprint length = %d, want 9", len(fp1))
	}

	// Empty string should still produce a fingerprint
	fp_empty := fingerprint("")
	if len(fp_empty) != 9 {
		t.Errorf("empty string fingerprint length = %d, want 9", len(fp_empty))
	}
}

// Test fingerprint_hyphens function
func TestFingerprintHyphens(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"normal", "ABCDEFGHI", "ABC-DEF-GHI"},
		{"numbers", "123456789", "123-456-789"},
		{"mixed", "A1B2C3D4E", "A1B-2C3-D4E"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fingerprint_hyphens(tt.input)
			if result != tt.expected {
				t.Errorf("fingerprint_hyphens(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test fingerprint_no_hyphens function
func TestFingerprintNoHyphens(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"with hyphens", "ABC-DEF-GHI", "ABCDEFGHI"},
		{"no hyphens", "ABCDEFGHI", "ABCDEFGHI"},
		{"multiple hyphens", "A-B-C-D-E", "ABCDE"},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fingerprint_no_hyphens(tt.input)
			if result != tt.expected {
				t.Errorf("fingerprint_no_hyphens(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test itoa function
func TestItoa(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{42, "42"},
		{-123, "-123"},
		{1000000, "1000000"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := itoa(tt.input)
			if result != tt.expected {
				t.Errorf("itoa(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test i64toa function
func TestI64toa(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{42, "42"},
		{-123, "-123"},
		{9223372036854775807, "9223372036854775807"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := i64toa(tt.input)
			if result != tt.expected {
				t.Errorf("i64toa(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test like_escape function
func TestLikeEscape(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"no special chars", "hello", "hello"},
		{"percent", "50%", "50\\%"},
		{"underscore", "hello_world", "hello\\_world"},
		{"backslash", "path\\file", "path\\\\file"},
		{"all special", "%_\\", "\\%\\_\\\\"},
		{"mixed", "100% complete_now", "100\\% complete\\_now"},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := like_escape(tt.input)
			if result != tt.expected {
				t.Errorf("like_escape(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test random_alphanumeric function
func TestRandomAlphanumeric(t *testing.T) {
	// Test length
	lengths := []int{1, 5, 10, 32, 100}
	for _, length := range lengths {
		result := random_alphanumeric(length)
		if len(result) != length {
			t.Errorf("random_alphanumeric(%d) length = %d, want %d", length, len(result), length)
		}
	}

	// Test that results are alphanumeric
	result := random_alphanumeric(100)
	for _, r := range result {
		if !strings.ContainsRune(alphanumeric, r) {
			t.Errorf("random_alphanumeric produced non-alphanumeric char: %q", r)
		}
	}

	// Test randomness (two calls should produce different results with high probability)
	r1 := random_alphanumeric(32)
	r2 := random_alphanumeric(32)
	if r1 == r2 {
		t.Errorf("random_alphanumeric produced identical results: %q", r1)
	}
}

// Test uid function
func TestUid(t *testing.T) {
	// UID should be 32 characters (UUID without hyphens)
	id := uid()
	if len(id) != 32 {
		t.Errorf("uid() length = %d, want 32", len(id))
	}

	// Should only contain hex characters
	for _, r := range id {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Errorf("uid() contains non-hex char: %q", r)
		}
	}

	// Two calls should produce different results
	id2 := uid()
	if id == id2 {
		t.Errorf("uid() produced identical results: %q", id)
	}
}

// Test valid function
func TestValid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		match    string
		expected bool
	}{
		// constant pattern
		{"constant valid", "my-constant_1", "constant", true},
		{"constant valid dots", "my.constant", "constant", true},
		{"constant empty", "", "constant", false},
		{"constant special chars", "const<>ant", "constant", false},

		// entity pattern (49-51 word chars)
		{"entity valid 49", strings.Repeat("a", 49), "entity", true},
		{"entity valid 50", strings.Repeat("a", 50), "entity", true},
		{"entity valid 51", strings.Repeat("a", 51), "entity", true},
		{"entity too short", strings.Repeat("a", 48), "entity", false},
		{"entity too long", strings.Repeat("a", 52), "entity", false},
		// traversal must be rejected — the git builtins validate their entity
		// argument as "entity" before using it as a repo directory name.
		{"entity traversal dotdot", "../../../../tmp/x", "entity", false},
		{"entity traversal slash", strings.Repeat("a", 24) + "/" + strings.Repeat("b", 24), "entity", false},
		{"entity traversal backslash", strings.Repeat("a", 48) + `\.`, "entity", false},

		// fingerprint pattern
		{"fingerprint valid", "ABCDEF123", "fingerprint", true},
		{"fingerprint too short", "ABCDEF12", "fingerprint", false},
		{"fingerprint too long", "ABCDEF1234", "fingerprint", false},

		// integer pattern
		{"integer positive", "12345", "integer", true},
		{"integer negative", "-12345", "integer", true},
		{"integer zero", "0", "integer", true},
		{"integer with letters", "123abc", "integer", false},

		// natural pattern (non-negative integer)
		{"natural valid", "12345", "natural", true},
		{"natural zero", "0", "natural", true},
		{"natural negative", "-1", "natural", false},

		// positive pattern — unlike natural, excludes zero
		{"positive valid", "12345", "positive", true},
		{"positive one", "1", "positive", true},
		{"positive zero", "0", "positive", false},
		{"positive zeroes", "000", "positive", false},
		{"positive leading zero", "007", "positive", false},
		{"positive negative", "-1", "positive", false},
		{"positive maximum", "999999999", "positive", true},
		{"positive too long", "1000000000", "positive", false},

		// numeric pattern (signed integer/decimal)
		{"numeric integer", "-3", "numeric", true},
		{"numeric zero", "0", "numeric", true},
		{"numeric decimal", "6.5", "numeric", true},
		{"numeric alpha", "abc", "numeric", false},
		{"numeric comma", "6,5", "numeric", false},
		{"numeric scientific", "1e3", "numeric", false},
		{"numeric empty", "", "numeric", false},

		// privacy pattern
		{"privacy public", "public", "privacy", true},
		{"privacy private", "private", "privacy", true},
		{"privacy other", "secret", "privacy", false},

		// text pattern (length check)
		{"text normal", "Hello, world!", "text", true},
		{"text empty", "", "text", true},
		{"text too long", strings.Repeat("a", 1000001), "text", false},

		// name pattern (excludes < > \r \n)
		{"name valid", "John Doe", "name", true},
		{"name with quote", "O'Connor", "name", true},
		{"name with angle brackets", "User<script>", "name", false},
		{"name with parens", "John (Jr)", "name", true},

		// id pattern (exactly 32 lowercase hex chars)
		{"id valid", "abcdef01234567890abcdef012345678", "id", true},
		{"id too short", "abcdef0123456789", "id", false},
		{"id with trailing content", "abcdef01234567890abcdef012345678../../etc", "id", false},
		{"id uppercase", "ABCDEF01234567890ABCDEF012345678", "id", false},

		// filename pattern (no angle brackets or backslash)
		{"filename valid", "hello-world.txt", "filename", true},
		{"filename with spaces", "my file (1).txt", "filename", true},
		{"filename with tilde", "~backup.txt", "filename", true},
		{"filename angle brackets", "file<script>.txt", "filename", false},
		{"filename backslash", "file\\path.txt", "filename", false},
		{"filename caret", "file^name.txt", "filename", false},

		// filepath: ASCII by exact list
		{"filepath plain", "photo.png", "filepath", true},
		{"filepath nested", "reports/2026/summary.pdf", "filepath", true},
		{"filepath punctuation", "Report (final), v2 [draft] ~1 100%.pdf", "filepath", true},
		{"filepath interior double dot", "report..final.pdf", "filepath", true},
		{"filepath dollar", "price$.txt", "filepath", false},
		{"filepath semicolon", "a;b.txt", "filepath", false},
		{"filepath backtick", "a`b.txt", "filepath", false},
		{"filepath caret", "a^b.txt", "filepath", false},
		{"filepath braces", "a{b}.txt", "filepath", false},
		{"filepath windows forbidden", "a:b.txt", "filepath", false},
		{"filepath quote", "a\"b.txt", "filepath", false},
		{"filepath asterisk", "a*.txt", "filepath", false},
		{"filepath pipe", "a|b.txt", "filepath", false},
		{"filepath question", "a?.txt", "filepath", false},
		{"filepath backslash", "a\\b.txt", "filepath", false},

		// filepath: non-ASCII by category
		{"filepath accented", "café.png", "filepath", true},
		{"filepath cjk", "写真.png", "filepath", true},
		{"filepath cyrillic", "отчёт.pdf", "filepath", true},
		{"filepath arabic", "ملف.pdf", "filepath", true},
		{"filepath emoji", "party 🎉.png", "filepath", true},
		{"filepath curly quote", "John’s file.txt", "filepath", true},
		{"filepath em dash", "notes — final.txt", "filepath", true},
		{"filepath cjk punctuation", "第1章。草稿.txt", "filepath", true},
		{"filepath circled digit", "chapter ①.txt", "filepath", true},
		{"filepath currency", "price €10.txt", "filepath", true},
		{"filepath decomposed", "café.png", "filepath", false},
		{"filepath bell", "ring\x07.txt", "filepath", false},
		{"filepath newline", "line1\nline2.txt", "filepath", false},
		{"filepath carriage return", "a\rb.txt", "filepath", false},
		{"filepath escape", "a\x1bb.txt", "filepath", false},
		{"filepath delete char", "a\x7fb.txt", "filepath", false},
		{"filepath bidi override", "photo‮gnp.exe", "filepath", false},
		{"filepath zero width space", "a​b.txt", "filepath", false},
		{"filepath zero width joiner", "a‍b.txt", "filepath", false},
		{"filepath no-break space", "a b.txt", "filepath", false},
		{"filepath ideographic space", "a　b.txt", "filepath", false},
		{"filepath fullwidth solidus", "photos／2026.png", "filepath", false},
		{"filepath fullwidth colon", "time：now.txt", "filepath", false},
		{"filepath care of", "a℅b.txt", "filepath", false},
		{"filepath private use", "ab.txt", "filepath", false},

		// filepath: structure
		{"filepath empty", "", "filepath", false},
		{"filepath traversal", "../etc/passwd", "filepath", false},
		{"filepath interior traversal", "a/../b.txt", "filepath", false},
		{"filepath rooted", "/etc/passwd", "filepath", false},
		{"filepath hidden root", ".env", "filepath", false},
		{"filepath hidden nested", "apt/.git/config", "filepath", false},
		{"filepath leading hyphen", "-rf.txt", "filepath", false},
		{"filepath leading tilde", "~root.txt", "filepath", false},
		{"filepath leading space", " a.txt", "filepath", false},
		{"filepath leading combining mark", "́a.txt", "filepath", false},
		{"filepath trailing space", "a.txt ", "filepath", false},
		{"filepath trailing dot", "file.", "filepath", false},
		{"filepath device", "CON", "filepath", false},
		{"filepath device lowercase", "con.txt", "filepath", false},
		{"filepath device nested", "logs/NUL.log", "filepath", false},
		{"filepath device lookalike ok", "console.txt", "filepath", true},
		{"filepath long component", strings.Repeat("x", 256) + ".txt", "filepath", false},
		{"filepath component at limit", strings.Repeat("x", 251) + ".txt", "filepath", true},
		{"filepath empty component", "a//b.txt", "filepath", false},
		{"filepath trailing separator", "a/", "filepath", false},
		{"filepath invalid utf8", "a\xffb.txt", "filepath", false},

		// Control characters should fail all patterns
		{"control chars", "hello\x00world", "constant", false},
		{"control chars name", "hello\x01world", "name", false},

		// locale pattern (BCP 47, lowercase canonical form on disk)
		{"locale 2-letter", "en", "locale", true},
		{"locale 3-letter", "cmn", "locale", true},
		{"locale lang-region", "en-gb", "locale", true},
		{"locale lang-script", "zh-hant", "locale", true},
		{"locale lang-script-region", "zh-hant-hk", "locale", true},
		{"locale lang-numeric-region", "es-419", "locale", true},
		{"locale private-use pseudo", "en-x-pseudo", "locale", true},
		{"locale private-use multi", "en-x-pseudo-rtl", "locale", true},
		{"locale uppercase region rejected", "en-GB", "locale", false},
		{"locale uppercase script rejected", "zh-Hant", "locale", false},
		{"locale underscore rejected", "en_GB", "locale", false},
		{"locale 1-letter rejected", "e", "locale", false},
		{"locale empty rejected", "", "locale", false},
		{"locale subtag too long rejected", "en-toolongsubtag", "locale", false},

		// version pattern (app version; becomes a path component under
		// data_dir/apps, so it must reject path traversal)
		{"version semver", "1.2.3", "version", true},
		{"version keyword", "minor", "version", true},
		{"version numeric", "0.102", "version", true},
		{"version with hyphen", "1.0.0-beta", "version", true},
		{"version leading v", "v1", "version", true},
		{"version traversal slash", "../../../tmp/x", "version", false},
		{"version dotdot", "..", "version", false},
		{"version dotdot embedded", "1..2", "version", false},
		{"version forward slash", "a/b", "version", false},
		{"version backslash", "a\\b", "version", false},
		{"version colon", "a:b", "version", false},
		{"version bare dot", ".", "version", false},
		{"version leading dot", ".5", "version", false},
		{"version too long", strings.Repeat("1", 21), "version", false},
		{"version empty", "", "version", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valid(tt.input, tt.match)
			if result != tt.expected {
				t.Errorf("valid(%q, %q) = %v, want %v", tt.input, tt.match, result, tt.expected)
			}
		})
	}
}

func TestPathClean(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Identity for legitimate names in any script
		{"ascii", "photo.png", "photo.png"},
		{"accented", "café.png", "café.png"},
		{"cjk", "写真.png", "写真.png"},
		{"cyrillic", "отчёт.pdf", "отчёт.pdf"},
		{"punctuation", "Report (final), v2 [draft].pdf", "Report (final), v2 [draft].pdf"},
		{"emoji", "party 🎉.png", "party 🎉.png"},
		{"interior double dot", "report..final.pdf", "report..final.pdf"},

		// Repairs
		{"decomposed to composed", "cafe\u0301.png", "café.png"},
		{"bell dropped", "ring\x07.txt", "ring.txt"},
		{"newline dropped", "line1\nline2.txt", "line1line2.txt"},
		{"bidi override dropped", "photo\u202egnp.exe", "photognp.exe"},
		{"zero width space dropped", "a\u200bb.txt", "ab.txt"},
		{"no-break space to space", "a\u00a0b.txt", "a b.txt"},
		{"ideographic space to space", "a\u3000b.txt", "a b.txt"},
		{"colon scarred", "my:file.txt", "my_file.txt"},
		{"dollar scarred", "price$.txt", "price_.txt"},
		{"fullwidth solidus scarred", "photos\uff0f2026.png", "photos_2026.png"},
		{"windows path to base", "C:\\Users\\x\\photo.png", "photo.png"},
		{"unix path to base", "/etc/passwd", "passwd"},
		{"traversal to base", "../../etc/passwd", "passwd"},
		{"hidden unhidden", ".env", "env"},
		{"leading hyphen trimmed", "-rf.txt", "rf.txt"},
		{"trailing dot trimmed", "file.", "file"},
		{"trailing space trimmed", "file.txt ", "file.txt"},
		{"device prefixed", "CON.txt", "_CON.txt"},
		{"device lowercase prefixed", "nul.log", "_nul.log"},
		{"device lookalike untouched", "console.txt", "console.txt"},
		{"empty", "", "file"},
		{"only dots", "...", "file"},
		{"only invisible", "\u202e\u200b", "file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := path_clean(tt.input, 255)
			if result != tt.expected {
				t.Errorf("path_clean(%q) = %q, want %q", tt.input, result, tt.expected)
			}
			// Whatever clean returns, the validator accepts: the pair share one
			// implementation, and this is the property that keeps them honest.
			if !path_valid(result) {
				t.Errorf("path_clean(%q) = %q which path_valid refuses", tt.input, result)
			}
			// Idempotent: cleaning a cleaned name changes nothing.
			if again := path_clean(result, 255); again != result {
				t.Errorf("path_clean not idempotent: %q -> %q -> %q", tt.input, result, again)
			}
		})
	}

	// Truncation: fits the bound, keeps the extension, stays valid.
	long := strings.Repeat("x", 300) + ".png"
	short := path_clean(long, 100)
	if len(short) > 100 || !strings.HasSuffix(short, ".png") || !path_valid(short) {
		t.Errorf("path_clean truncation produced %q (%d bytes)", short, len(short))
	}
	// Truncation on a multibyte name never splits a code point.
	wide := strings.Repeat("写", 200) + ".png"
	cut := path_clean(wide, 100)
	if len(cut) > 100 || !utf8.ValidString(cut) || !path_valid(cut) {
		t.Errorf("path_clean multibyte truncation produced %q (%d bytes)", cut, len(cut))
	}
}

// Test valid with custom regex
func TestValidCustomRegex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		match    string
		expected bool
	}{
		{"custom match", "abc", "^[a-z]+$", true},
		{"custom no match", "ABC", "^[a-z]+$", false},
		{"custom email-like", "user@example", "^[a-z]+@[a-z]+$", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valid(tt.input, tt.match)
			if result != tt.expected {
				t.Errorf("valid(%q, %q) = %v, want %v", tt.input, tt.match, result, tt.expected)
			}
		})
	}
}

// Benchmark fingerprint
func BenchmarkFingerprint(b *testing.B) {
	inputs := []string{"short", "medium length string", strings.Repeat("long", 100)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fingerprint(inputs[i%len(inputs)])
	}
}

// Benchmark valid
func BenchmarkValid(b *testing.B) {
	inputs := []struct {
		s     string
		match string
	}{
		{"my-constant", "constant"},
		{strings.Repeat("a", 50), "entity"},
		{"Hello, world!", "text"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tt := inputs[i%len(inputs)]
		valid(tt.s, tt.match)
	}
}

// Benchmark random_alphanumeric
func BenchmarkRandomAlphanumeric(b *testing.B) {
	for i := 0; i < b.N; i++ {
		random_alphanumeric(32)
	}
}

// Benchmark like_escape
func BenchmarkLikeEscape(b *testing.B) {
	inputs := []string{
		"normal string",
		"50% complete",
		"path\\to\\file_name",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		like_escape(inputs[i%len(inputs)])
	}
}

// Test unzip function
func TestUnzip(t *testing.T) {
	// Create a temporary directory for testing
	tmp_dir := t.TempDir()

	// Create a simple test zip file
	zip_path := tmp_dir + "/test.zip"
	dest_dir := tmp_dir + "/dest"

	// Create zip with a normal file
	zip_file, err := os.Create(zip_path)
	if err != nil {
		t.Fatalf("Failed to create zip file: %v", err)
	}

	zip_writer := zip.NewWriter(zip_file)

	// Add a normal file
	w, err := zip_writer.Create("hello.txt")
	if err != nil {
		t.Fatalf("Failed to create file in zip: %v", err)
	}
	w.Write([]byte("Hello, World!"))

	// Add a file in a subdirectory
	w, err = zip_writer.Create("subdir/nested.txt")
	if err != nil {
		t.Fatalf("Failed to create nested file in zip: %v", err)
	}
	w.Write([]byte("Nested content"))

	zip_writer.Close()
	zip_file.Close()

	// Test normal extraction
	err = unzip(zip_path, dest_dir, unzip_maximum_bytes)
	if err != nil {
		t.Fatalf("unzip failed: %v", err)
	}

	// Verify files were extracted
	content, err := os.ReadFile(dest_dir + "/hello.txt")
	if err != nil {
		t.Errorf("Failed to read extracted file: %v", err)
	}
	if string(content) != "Hello, World!" {
		t.Errorf("Extracted content = %q, want %q", string(content), "Hello, World!")
	}

	content, err = os.ReadFile(dest_dir + "/subdir/nested.txt")
	if err != nil {
		t.Errorf("Failed to read nested extracted file: %v", err)
	}
	if string(content) != "Nested content" {
		t.Errorf("Nested extracted content = %q, want %q", string(content), "Nested content")
	}
}

// Test unzip path traversal protection
func TestUnzipPathTraversal(t *testing.T) {
	tmp_dir := t.TempDir()
	zip_path := tmp_dir + "/malicious.zip"
	dest_dir := tmp_dir + "/dest"
	outside_file := tmp_dir + "/outside.txt"

	// Create zip with path traversal attempt
	zip_file, err := os.Create(zip_path)
	if err != nil {
		t.Fatalf("Failed to create zip file: %v", err)
	}

	zip_writer := zip.NewWriter(zip_file)

	// Try to create a file outside the destination using ../
	w, err := zip_writer.Create("../outside.txt")
	if err != nil {
		t.Fatalf("Failed to create file in zip: %v", err)
	}
	w.Write([]byte("malicious content"))

	zip_writer.Close()
	zip_file.Close()

	// Create destination directory
	os.MkdirAll(dest_dir, 0755)

	// Attempt extraction - os.Root should prevent the traversal
	err = unzip(zip_path, dest_dir, unzip_maximum_bytes)

	// os.Root returns an error for path traversal attempts
	if err == nil {
		// If no error, verify the file was NOT created outside
		if _, stat_error := os.Stat(outside_file); stat_error == nil {
			t.Errorf("Path traversal succeeded - file created outside destination")
		}
	}
	// If err != nil, that's also acceptable - os.Root rejected the traversal
}

// Test url_is_cloud_metadata function
func TestUrlIsCloudMetadata(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		// Should block
		{"AWS metadata", "http://169.254.169.254/latest/meta-data/", true},
		{"AWS metadata https", "https://169.254.169.254/latest/meta-data/", true},
		{"AWS metadata with path", "http://169.254.169.254/latest/api/token", true},
		{"GCP metadata", "http://metadata.google.internal/computeMetadata/v1/", true},
		{"GCP metadata https", "https://metadata.google.internal/computeMetadata/v1/", true},

		// Should allow
		{"normal URL", "https://example.com/api", false},
		{"localhost", "http://localhost:8080/api", false},
		{"private IP", "http://192.168.1.1/admin", false},
		{"similar but different", "http://169.254.169.253/", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := url_is_cloud_metadata(tt.url)
			if result != tt.expected {
				t.Errorf("url_is_cloud_metadata(%q) = %v, want %v", tt.url, result, tt.expected)
			}
		})
	}
}

// allow_private_for_test lets a test reach an httptest server on 127.0.0.1
// through url_request, which otherwise refuses non-public destinations. Scoped
// to the calling test so the SSRF guard stays on everywhere else.
func allow_private_for_test(t *testing.T) {
	t.Helper()
	previous := url_allow_private
	url_allow_private = true
	t.Cleanup(func() { url_allow_private = previous })
}

// TestURLAddressBlocked pins the SSRF guard: app-supplied URLs must not be able
// to reach loopback, private, link-local (including the cloud metadata address,
// by whatever notation), unspecified or multicast destinations.
func TestURLAddressBlocked(t *testing.T) {
	blocked := []string{
		"127.0.0.1:80", "[::1]:80", // loopback
		"10.1.2.3:80", "192.168.1.1:80", "172.16.0.1:80", // RFC 1918
		"[fc00::1]:80",                // IPv6 unique-local
		"169.254.169.254:80",          // cloud metadata
		"[::ffff:169.254.169.254]:80", // 4-in-6 metadata
		"[fe80::1]:80",                // IPv6 link-local
		"0.0.0.0:80", "[::]:80",       // unspecified
		"224.0.0.1:80", // multicast
		// Special-purpose ranges the standard library's helpers do not
		// classify. Carrier-grade NAT is the one that bites in practice:
		// hosting providers and mesh VPNs run internal services there.
		"100.64.0.1:80",          // RFC 6598 carrier-grade NAT
		"100.127.255.255:80",     // RFC 6598 upper bound
		"[::ffff:100.64.0.1]:80", // 4-in-6 carrier-grade NAT
		"192.0.0.1:80",           // IETF protocol assignments
		"198.18.0.1:80",          // benchmarking
		"192.0.2.1:80",           // TEST-NET-1
		"198.51.100.1:80",        // TEST-NET-2
		"203.0.113.1:80",         // TEST-NET-3
		"240.0.0.1:80",           // reserved
		"255.255.255.255:80",     // broadcast
		"[64:ff9b::7f00:1]:80",   // NAT64-mapped loopback
		"[100::1]:80",            // discard-only
		"[2001:db8::1]:80",       // documentation
	}
	for _, address := range blocked {
		if err := url_address_allowed(address); err == nil {
			t.Errorf("url_address_allowed(%q) allowed a non-public destination", address)
		}
	}

	for _, address := range []string{"93.184.216.34:80", "[2606:2800:220:1:248:1893:25c8:1946]:443"} {
		if err := url_address_allowed(address); err != nil {
			t.Errorf("url_address_allowed(%q) blocked a public destination: %v", address, err)
		}
	}
}

// path_scrub must drop the data_dir root and per-user segment while keeping
// the app-relative remainder, so client-visible errors stay fully diagnostic
// without revealing the server's disk layout or the owning user's id.
func TestPathScrub(t *testing.T) {
	orig := data_dir
	data_dir = "/srv/mochi"
	defer func() { data_dir = orig }()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"user database path", "unable to open database file: /srv/mochi/users/8f3a9c/feeds/db/posts.db", "unable to open database file: feeds/db/posts.db"},
		{"core database path", "database is locked: /srv/mochi/db/sessions.db", "database is locked: db/sessions.db"},
		{"no path", "no such table: posts", "no such table: posts"},
		{"two paths", "/srv/mochi/users/u1/a/x -> /srv/mochi/users/u2/b/y", "a/x -> b/y"},
		{"user directory itself", "stat /srv/mochi/users/u1", "stat users/u1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := path_scrub(tt.input); got != tt.want {
				t.Errorf("path_scrub(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
