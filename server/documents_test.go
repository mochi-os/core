// Mochi server: Documents resolver tests
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"strings"
	"testing"
)

// setup_documents_test redirects data_dir to a temp directory and seeds the
// minimum schema needed for the documents resolver and its dependencies
// (the `documents` and `settings` tables in db/settings.db). Returns a
// cleanup function the caller must defer.
func setup_documents_test(t *testing.T) func() {
	t.Helper()

	tmp, err := os.MkdirTemp("", "mochi_documents_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}

	orig_data_dir := data_dir
	data_dir = tmp

	settings := db_open("db/settings.db")
	settings.exec("create table settings ( name text not null primary key, value text not null, ts integer not null default 0, peer text not null default '' )")
	settings.exec("create table documents ( name text not null, language text not null, body text not null, updated integer not null, primary key ( name, language ) )")

	// Load the core_labels map from the embedded FS so document_setting can
	// resolve `document.not_configured` per-locale.
	load_core_labels()

	return func() {
		settings.close()
		data_dir = orig_data_dir
		os.RemoveAll(tmp)
	}
}

// TestDocumentBundledDefault verifies that with no operator override the
// embedded bundled default in the requested language is returned.
func TestDocumentBundledDefault(t *testing.T) {
	defer setup_documents_test(t)()

	body := document_get("rules", "en")
	if body == "" {
		t.Fatal("expected non-empty bundled en rules body")
	}
	if !strings.Contains(body, "Mochi server rules") {
		t.Fatalf("expected en bundled body to mention 'Mochi server rules', got: %q", first_line(body))
	}

	body_fr := document_get("rules", "fr")
	if body_fr == "" {
		t.Fatal("expected non-empty bundled fr rules body")
	}
	if !strings.Contains(body_fr, "Règles") {
		t.Fatalf("expected fr bundled body to mention 'Règles', got: %q", first_line(body_fr))
	}
}

// TestDocumentOperatorOverride verifies that an operator override beats the
// bundled default for the same (name, language).
func TestDocumentOperatorOverride(t *testing.T) {
	defer setup_documents_test(t)()

	override := "# Custom French rules\n\nOperator-edited body."
	if err := document_set("rules", "fr", override); err != nil {
		t.Fatalf("document_set: %v", err)
	}

	body := document_get("rules", "fr")
	if !strings.Contains(body, "Custom French rules") {
		t.Fatalf("expected operator override to be returned, got: %q", first_line(body))
	}
}

// TestDocumentLanguageFallback verifies that a request for a language with
// no bundled default falls back to en.
func TestDocumentLanguageFallback(t *testing.T) {
	defer setup_documents_test(t)()

	// "zz" is not a real BCP 47 tag and no bundled file exists for it.
	body := document_get("rules", "zz")
	if body == "" {
		t.Fatal("expected fallback to bundled en, got empty body")
	}
	if !strings.Contains(body, "Mochi server rules") {
		t.Fatalf("expected en fallback, got: %q", first_line(body))
	}
}

// TestDocumentOverrideEnFallback verifies that an operator override in en is
// only used when neither operator nor bundled default exist for the requested
// language. (For our shipped locales, bundled defaults always exist, so en
// override only wins for unsupported language tags.)
func TestDocumentOverrideEnFallback(t *testing.T) {
	defer setup_documents_test(t)()

	if err := document_set("rules", "en", "# en override"); err != nil {
		t.Fatalf("document_set: %v", err)
	}

	// Unsupported tag → no operator override in `zz`, no bundled default in
	// `zz` → en operator override wins (step 3 of the chain).
	body := document_get("rules", "zz")
	if !strings.Contains(body, "en override") {
		t.Fatalf("expected en operator override, got: %q", first_line(body))
	}

	// Supported tag with bundled default → bundled fr beats en override
	// (step 2 wins before step 3).
	body_fr := document_get("rules", "fr")
	if strings.Contains(body_fr, "en override") {
		t.Fatalf("expected bundled fr to beat en operator override, got: %q", first_line(body_fr))
	}
}

// TestDocumentUnknownName verifies that names outside the allowlist return
// an empty string rather than a fallback or error.
func TestDocumentUnknownName(t *testing.T) {
	defer setup_documents_test(t)()

	if body := document_get("nonexistent", "en"); body != "" {
		t.Fatalf("expected empty body for unknown name, got: %q", first_line(body))
	}
}

// TestDocumentPlaceholderInterpolation verifies that {{operator.*}}
// placeholders are substituted from system settings.
func TestDocumentPlaceholderInterpolation(t *testing.T) {
	defer setup_documents_test(t)()

	setting_set("operator_name", "Acme")
	setting_set("operator_email", "ops@acme.example")
	setting_set("operator_jurisdiction", "England and Wales")

	if err := document_set("rules", "en", "Operated by **{{operator.name}}** in {{operator.jurisdiction}}. Contact {{operator.email}}."); err != nil {
		t.Fatalf("document_set: %v", err)
	}

	body := document_get("rules", "en")
	for _, want := range []string{"Acme", "ops@acme.example", "England and Wales"} {
		if !strings.Contains(body, want) {
			t.Errorf("expected interpolated body to contain %q, got: %q", want, body)
		}
	}
	for _, leak := range []string{"{{operator.name}}", "{{operator.email}}", "{{operator.jurisdiction}}"} {
		if strings.Contains(body, leak) {
			t.Errorf("placeholder %q leaked through unrendered", leak)
		}
	}
}

// TestDocumentNotConfiguredFallback verifies that missing operator settings
// render as the [not configured] sentinel so empty operator info is
// visually obvious.
func TestDocumentNotConfiguredFallback(t *testing.T) {
	defer setup_documents_test(t)()

	if err := document_set("rules", "en", "Operated by {{operator.name}}."); err != nil {
		t.Fatalf("document_set: %v", err)
	}

	body := document_get("rules", "en")
	if !strings.Contains(body, "[not configured]") {
		t.Fatalf("expected [not configured] when operator_name is unset, got: %q", body)
	}
}

// TestDocumentSetRejectsUnknownName verifies that document_set won't write
// rows for names outside the allowlist.
func TestDocumentSetRejectsUnknownName(t *testing.T) {
	defer setup_documents_test(t)()

	if err := document_set("nonexistent", "en", "body"); err == nil {
		t.Fatal("expected document_set to reject unknown name, got nil error")
	}
}

// TestDocumentSetRejectsEmptyLanguage verifies that document_set requires
// a non-empty language.
func TestDocumentSetRejectsEmptyLanguage(t *testing.T) {
	defer setup_documents_test(t)()

	if err := document_set("rules", "", "body"); err == nil {
		t.Fatal("expected document_set to reject empty language, got nil error")
	}
}

// TestDocumentLanguages verifies that document_languages enumerates the
// bundled locale set from the embedded FS, sorted alphabetically.
func TestDocumentLanguages(t *testing.T) {
	defer setup_documents_test(t)()

	langs := document_languages()
	if len(langs) == 0 {
		t.Fatal("expected non-empty bundled language list")
	}
	for i := 1; i < len(langs); i++ {
		if langs[i-1] >= langs[i] {
			t.Errorf("languages not sorted: %q before %q", langs[i-1], langs[i])
		}
	}
	// en is always shipped — sanity check the embed FS path is right.
	found_en := false
	for _, l := range langs {
		if l == "en" {
			found_en = true
			break
		}
	}
	if !found_en {
		t.Errorf("expected 'en' in bundled languages, got %v", langs)
	}
}

// first_line returns the first line of body, trimmed, for tidy assertion
// failure messages.
func first_line(body string) string {
	for i := 0; i < len(body); i++ {
		if body[i] == '\n' {
			return body[:i]
		}
	}
	return body
}
