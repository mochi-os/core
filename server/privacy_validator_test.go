// Mochi server: one place decides what an entity's privacy may be.
//
// utilities.go has held a "privacy" validator all along. Core checked the
// field four ways anyway:
//
//   - entity_create said valid(privacy, "privacy") - correct.
//   - api_entity_create restated the pattern as a raw regex, and spelled the
//     alternation the other way round ("^(private|public)$" against the
//     validator's "^(public|private)$"), which is the tell that it was written
//     without reference to the other.
//   - api_entity_update and entity_privacy_set each restated it again as a
//     string comparison, reaching no validator at all.
//   - mochi.user.settings checks nothing and relies on entity_privacy_set.
//
// None of that was a hole. An exact comparison against two literals is at
// least as strict as the regex, and valid()'s leading control-character filter
// is redundant when the accepted set is two ASCII words. The cost is drift:
// four encodings of "the allowed privacy values" means a third value has to be
// added in four places, and missing one leaves create accepting what update
// rejects with no test failing.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestPrivacyValidatorAcceptsExactlyTwoValues pins the accepted set. Adding a
// third is then a deliberate edit here rather than a silent divergence between
// the paths.
func TestPrivacyValidatorAcceptsExactlyTwoValues(t *testing.T) {
	for _, good := range []string{"public", "private"} {
		if !valid(good, "privacy") {
			t.Errorf("valid(%q, \"privacy\") = false; that is one of the two values every path accepts", good)
		}
	}
	for _, bad := range []string{
		"", "Public", "PRIVATE", "public ", " private", "publicprivate",
		"unlisted", "public|private", "^(public|private)$", "public\nprivate",
	} {
		if valid(bad, "privacy") {
			t.Errorf("valid(%+q, \"privacy\") = true, want false", bad)
		}
	}
}

// TestPrivacySetRejectsEverythingElse drives entity_privacy_set, which is the
// only gate on the mochi.user.settings path - users.go passes the caller's
// string through untouched.
func TestPrivacySetRejectsEverythingElse(t *testing.T) {
	for _, bad := range []string{"", "Public", "unlisted", "public "} {
		e := &Entity{ID: "entity-one", Privacy: "public"}
		if err := entity_privacy_set(e, bad); err == nil {
			t.Errorf("entity_privacy_set accepted %+q; mochi.user.settings does no check of its own, so this is that path's only gate", bad)
		}
		if e.Privacy != "public" {
			t.Errorf("entity_privacy_set wrote %q from the rejected input %+q", e.Privacy, bad)
		}
	}
}

// TestPrivacySetIsANoOpForTheCurrentValue: setting the value it already holds
// must not fall through to the directory work, which broadcasts.
func TestPrivacySetIsANoOpForTheCurrentValue(t *testing.T) {
	e := &Entity{ID: "entity-one", Privacy: "private"}
	if err := entity_privacy_set(e, "private"); err != nil {
		t.Errorf("entity_privacy_set rejected the value the entity already holds: %v", err)
	}
	if e.Privacy != "private" {
		t.Errorf("entity privacy became %q", e.Privacy)
	}
}

// privacy_checks finds every place a source file decides whether a privacy
// value is allowed - both the validator call and the hand-rolled comparison
// this replaced.
var privacy_pattern_literal = regexp.MustCompile(`\^\((public\|private|private\|public)\)\$`)
var privacy_comparison = regexp.MustCompile(`privacy != "(public|private)" && privacy != "(public|private)"`)

// TestNoPathRestatesThePrivacyRule is the gate. Two shapes to catch: the
// pattern written out where the named type would do, and the string comparison
// that reaches no validator at all.
func TestNoPathRestatesThePrivacyRule(t *testing.T) {
	for _, file := range package_source_files(t) {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		text := string(source)

		for number, line := range strings.Split(text, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			// utilities.go is where the rule is allowed to be written out.
			if privacy_pattern_literal.MatchString(line) && file != "utilities.go" {
				t.Errorf("%s:%d writes the privacy pattern out where valid(x, \"privacy\") would do: %s", file, number+1, trimmed)
			}
			if privacy_comparison.MatchString(line) {
				t.Errorf("%s:%d compares against the privacy literals instead of validating: %s", file, number+1, trimmed)
			}
		}
	}
}

// TestPrivacyValidatorIsNamedInUtilities is the other half: the rule has to
// still live somewhere, or the gate above passes over an empty package.
func TestPrivacyValidatorIsNamedInUtilities(t *testing.T) {
	source, err := os.ReadFile("utilities.go")
	if err != nil {
		t.Fatalf("reading utilities.go: %v", err)
	}
	if !strings.Contains(string(source), `case "privacy":`) {
		t.Fatal("utilities.go no longer defines the \"privacy\" validator; every caller names it, so they would all fall through to compiling \"privacy\" as a regex - which matches the substring, not the value")
	}
}

// TestUnknownValidatorNameCompilesAsAPattern records why the test above is a
// Fatal rather than an Error. valid() reassigns match for a known type and
// otherwise compiles the string as a PATTERN, so deleting a case does not fail
// loudly anywhere - every caller naming it silently becomes an unanchored
// match on the name's own text.
func TestUnknownValidatorNameCompilesAsAPattern(t *testing.T) {
	// A name no case defines is compiled as a pattern, so it matches its own
	// text and nothing else in particular.
	if !valid("notavalidatorname", "notavalidatorname") {
		t.Error("an unknown validator name no longer compiles as a pattern; if valid() now rejects unknown names outright, TestPrivacyValidatorIsNamedInUtilities can be relaxed to an Error")
	}
	if valid("something else", "notavalidatorname") {
		t.Error("an unknown validator name matched an unrelated string")
	}

	// The consequence, stated concretely: were `case "privacy"` deleted,
	// valid(x, "privacy") would accept any value CONTAINING "privacy" and
	// reject "public" - the opposite of the rule, with no compile error and no
	// test failure anywhere but here.
	if valid("public", "notavalidatorname") {
		t.Error("the unknown-name fallthrough accepted a real privacy value, so this demonstration is not showing what it claims")
	}
	// And the pattern is unanchored, so it matches its name as a substring:
	// that is what "every caller becomes a substring match" means.
	if !valid("xxnotavalidatornameyy", "notavalidatorname") {
		t.Error("an unknown validator name is anchored; the fallthrough is less dangerous than documented, and the note above should be corrected")
	}
}
