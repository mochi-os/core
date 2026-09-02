// Mochi server: Documents resolver tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// setup_documents_test redirects data_dir to a temp directory and seeds the
// minimum schema needed for the documents resolver and its dependencies
// (the `documents` and `settings` tables in db/settings.db). Returns a
// cleanup function the caller must defer.
func setup_documents_test(t *testing.T) {
	t.Helper()

	test_data_directory(t)

	settings := db_open("db/settings.db")
	settings.exec("create table settings ( name text not null primary key, value text not null )")
	settings.exec("create table documents ( name text not null, language text not null, body text not null, updated integer not null, primary key ( name, language ) )")

	// Load the core_labels map from the embedded FS so document_setting can
	// resolve `document.not_configured` per-locale.
	load_core_labels()

	t.Cleanup(func() {
		settings.close()
	})

}

// TestDocumentBundledDefault verifies that with no operator override the
// embedded bundled default in the requested language is returned.
func TestDocumentBundledDefault(t *testing.T) {
	setup_documents_test(t)

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
	setup_documents_test(t)

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
	setup_documents_test(t)

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
	setup_documents_test(t)

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
	setup_documents_test(t)

	if body := document_get("nonexistent", "en"); body != "" {
		t.Fatalf("expected empty body for unknown name, got: %q", first_line(body))
	}
}

// TestDocumentPlaceholderInterpolation verifies that {{operator.*}}
// placeholders are substituted from system settings.
func TestDocumentPlaceholderInterpolation(t *testing.T) {
	setup_documents_test(t)

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
	setup_documents_test(t)

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
	setup_documents_test(t)

	if err := document_set("nonexistent", "en", "body"); err == nil {
		t.Fatal("expected document_set to reject unknown name, got nil error")
	}
}

// TestDocumentSetRejectsEmptyLanguage verifies that document_set requires
// a non-empty language.
func TestDocumentSetRejectsEmptyLanguage(t *testing.T) {
	setup_documents_test(t)

	if err := document_set("rules", "", "body"); err == nil {
		t.Fatal("expected document_set to reject empty language, got nil error")
	}
}

// TestDocumentLanguages verifies that document_languages enumerates the
// bundled locale set from the embedded FS, sorted alphabetically.
func TestDocumentLanguages(t *testing.T) {
	setup_documents_test(t)

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

// TestDocumentIndexCarriesNoBodies verifies the index lists every shipped
// (name, language) pair and nothing else. The bodies are what made this
// response ~4 MB; a regression that puts them back would show up here.
func TestDocumentIndexCarriesNoBodies(t *testing.T) {
	setup_documents_test(t)

	index := document_index()
	if len(index) == 0 {
		t.Fatal("expected a non-empty document index")
	}
	expected := len(document_names) * len(document_languages())
	if len(index) != expected {
		t.Fatalf("expected %d entries (%d names x %d languages), got %d",
			expected, len(document_names), len(document_languages()), len(index))
	}
	for _, entry := range index {
		if len(entry) != 2 {
			t.Fatalf("index entry should carry name and language only, got %d keys: %v", len(entry), entry)
		}
		for _, key := range []string{"body", "default", "updated"} {
			if _, present := entry[key]; present {
				t.Fatalf("index entry must not carry %q: %v", key, entry)
			}
		}
		if entry["name"] == "" || entry["language"] == "" {
			t.Fatalf("index entry missing name or language: %v", entry)
		}
	}
}

// TestDocumentIndexIsBounded pins the index's size against the whole-corpus
// response it replaced. The editor shows one document; the index exists so
// the page does not pay for 294 of them on every window focus.
func TestDocumentIndexIsBounded(t *testing.T) {
	setup_documents_test(t)

	encoded, err := json.Marshal(document_index())
	if err != nil {
		t.Fatalf("index does not encode: %v", err)
	}
	// Every bundled body summed. The index must not be within an order of
	// magnitude of it - if it is, the bodies are back.
	corpus := 0
	for _, name := range document_names {
		for _, language := range document_languages() {
			corpus += len(document_bundled(name, language))
		}
	}
	if corpus == 0 {
		t.Fatal("expected a non-empty bundled corpus")
	}
	if len(encoded) > corpus/50 {
		t.Fatalf("index is %d bytes against a %d byte corpus; expected well under %d",
			len(encoded), corpus, corpus/50)
	}
}

// TestDocumentSourceIsRaw verifies the editor is handed the source text, not
// the rendered output. document_get interpolates {{operator.*}}; saving that
// back would replace the placeholder with its resolved value permanently.
func TestDocumentSourceIsRaw(t *testing.T) {
	setup_documents_test(t)

	if err := document_set("rules", "en", "Contact {{operator.email}} for help."); err != nil {
		t.Fatalf("document_set: %v", err)
	}
	source := document_source("rules", "en")
	if source == nil {
		t.Fatal("expected a source for rules/en")
	}
	body, _ := source["body"].(string)
	if !strings.Contains(body, "{{operator.email}}") {
		t.Fatalf("expected the raw placeholder in the editable body, got: %q", body)
	}
	if strings.Contains(document_get("rules", "en"), "{{operator.email}}") {
		t.Fatal("document_get should have rendered the placeholder; the two accessors are not distinguishable")
	}
}

// TestDocumentSourceCarriesDefaultAndTimestamp verifies the three fields the
// editor needs beyond the body: the bundled default it diffs and reverts
// against, and the override timestamp it displays.
func TestDocumentSourceCarriesDefaultAndTimestamp(t *testing.T) {
	setup_documents_test(t)

	before := document_source("terms", "en")
	if before == nil {
		t.Fatal("expected a source for terms/en")
	}
	bundled, _ := before["default"].(string)
	if bundled == "" {
		t.Fatal("expected the bundled default to be carried")
	}
	if before["body"] != before["default"] {
		t.Fatal("with no override the body should be the bundled default")
	}
	if before["updated"] != int64(0) {
		t.Fatalf("expected updated 0 with no override, got %v", before["updated"])
	}

	if err := document_set("terms", "en", "Operator terms."); err != nil {
		t.Fatalf("document_set: %v", err)
	}
	after := document_source("terms", "en")
	if after["body"] != "Operator terms." {
		t.Fatalf("expected the override as the body, got %v", after["body"])
	}
	if after["default"] != bundled {
		t.Fatal("the bundled default must survive an override, so Revert has something to revert to")
	}
	if updated, _ := after["updated"].(int64); updated == 0 {
		t.Fatal("expected a non-zero timestamp after an override")
	}
}

// TestDocumentSourceRefusesUnknown verifies the accessor returns nothing for
// a pair it does not ship, so the action answers 404 rather than an empty
// editor.
func TestDocumentSourceRefusesUnknown(t *testing.T) {
	setup_documents_test(t)

	if document_source("passwords", "en") != nil {
		t.Fatal("expected nil for a name outside the allowlist")
	}
	if document_source("rules", "zz") != nil {
		t.Fatal("expected nil for a language with no bundled default")
	}
	if document_source("rules", "") != nil {
		t.Fatal("expected nil for an empty language")
	}
}

// An override is keyed by the lowercase tag the request path produces, so the
// key is normalised on the way in, and anything that cannot be a tag is
// refused rather than stored where nothing will read it.
func TestDocumentSetNormalisesTheLanguageKey(t *testing.T) {
	setup_documents_test(t)

	if err := document_set("rules", "EN", "# Upper"); err != nil {
		t.Fatalf("EN: %v", err)
	}
	if body := document_get("rules", "en"); !strings.Contains(body, "Upper") {
		t.Errorf("EN was not stored under en: %q", first_line(body))
	}
	if err := document_set("rules", " en-GB ", "# British"); err != nil {
		t.Fatalf("en-GB: %v", err)
	}
	if body := document_get("rules", "en-gb"); !strings.Contains(body, "British") {
		t.Errorf("en-GB was not stored under en-gb: %q", first_line(body))
	}
	for _, bad := range []string{"en_GB", "not a tag", "", "EN--"} {
		if err := document_set("rules", bad, "# nope"); err == nil {
			t.Errorf("language %q was accepted", bad)
		}
	}
	if normalised, ok := document_language("PT-br"); !ok || normalised != "pt-br" {
		t.Errorf("document_language(PT-br) = %q, %v", normalised, ok)
	}
}
