// Mochi server: a doc comment sits on the thing it names.
//
// Go attaches a comment block to whatever declaration follows it, so a deleted
// or moved function silently leaves its doc as the opening paragraph of the
// NEXT function's documentation, where go doc and every IDE hover present it.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// doc_opening matches a doc comment's conventional first line, "<name> <verb>".
// The verb list is what the package actually uses; a doc phrased another way is
// not checked, so this test is a floor rather than a complete audit.
var doc_opening = regexp.MustCompile(`^//\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(is|are|reports|returns|extracts|adds|gives|deals|removes|walks|bounds|lets|creates|writes|tells|checks|holds|tracks|throttles|wakes|applies|seeds|sets|binds|does|makes|converts|builds|deletes|upserts|counts|drains|marks|records|resolves|yields|opens|closes|reads|sends|starts|stops)\b`)

// doc_declaration pulls the declared name out of a func / var / const / type
// line, including a method's receiver form.
var doc_declaration = regexp.MustCompile(`^(?:func\s+(?:\([^)]*\)\s*)?|var\s+|const\s+|type\s+)([a-zA-Z_][a-zA-Z0-9_]*)`)

// doc_group matches a grouped declaration. A comment above `var (` or `const (`
// legitimately names the first member INSIDE the group rather than the group
// itself, so those are not misattachments - this is the one case the detector
// has to know about rather than flag. Six of the fourteen raw hits were this.
var doc_group = regexp.MustCompile(`^(?:var|const|type)\s+\($`)

// doc_mismatch is one doc block whose opening names something other than the
// declaration it is attached to.
type doc_mismatch struct {
	file    string
	line    int
	named   string
	attched string
}

// doc_mismatches scans a file for comment blocks that name one thing and sit on
// another.
func doc_mismatches(t *testing.T, file string) []doc_mismatch {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("reading %s: %v", file, err)
	}
	lines := strings.Split(string(data), "\n")

	var found []doc_mismatch
	for i := 0; i < len(lines); {
		if !strings.HasPrefix(lines[i], "//") {
			i++
			continue
		}
		start := i
		for i < len(lines) && strings.HasPrefix(lines[i], "//") {
			i++
		}
		opening := doc_opening.FindStringSubmatch(lines[start])
		if opening == nil {
			continue
		}
		next := ""
		if i < len(lines) {
			next = lines[i]
		}
		if doc_group.MatchString(strings.TrimSpace(next)) {
			continue
		}
		declaration := doc_declaration.FindStringSubmatch(next)
		if declaration == nil {
			// Not a declaration at all - a free-standing comment, or a doc
			// followed by another doc. The latter is exactly the defect, and
			// it is caught on the following block's own iteration.
			continue
		}
		if declaration[1] != opening[1] {
			found = append(found, doc_mismatch{file, start + 1, opening[1], declaration[1]})
		}
	}
	return found
}

// TestEveryDocCommentSitsOnWhatItNames is the gate. Written against the shape
// rather than the eight sites, because the defect recurs every time a function
// is moved or renamed and its doc is left behind.
func TestEveryDocCommentSitsOnWhatItNames(t *testing.T) {
	for _, file := range package_source_files(t) {
		for _, m := range doc_mismatches(t, file) {
			t.Errorf("%s:%d documents %q but is attached to %q; go doc will print this text under %q",
				m.file, m.line, m.named, m.attched, m.attched)
		}
	}
}

// package_source_files lists the non-test .go files in the package.
func package_source_files(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
			files = append(files, name)
		}
	}
	if len(files) == 0 {
		t.Fatal("no source files found")
	}
	return files
}

// TestRemovedFunctionsHaveNoSurvivingDoc names the two whose functions are gone
// outright. The gate above cannot catch these once the text is deleted - there
// is nothing left to mismatch - so this states that they must stay gone.
func TestRemovedFunctionsHaveNoSurvivingDoc(t *testing.T) {
	for _, gone := range []struct{ file, name string }{
		{"db.go", "sql_target_table"},
		{"protocol2_worker.go", "worker_inbox_count"},
	} {
		data, err := os.ReadFile(gone.file)
		if err != nil {
			t.Fatalf("reading %s: %v", gone.file, err)
		}
		if strings.Contains(string(data), gone.name) {
			t.Errorf("%s mentions %s again; no such function exists, so any comment naming it documents nothing", gone.file, gone.name)
		}
	}
}

// TestDetectorFindsAPlantedMismatch proves the gate can fail. A text scanner
// that silently matched nothing would pass this suite for ever, so the detector
// is run against a known-bad input rather than trusted.
func TestDetectorFindsAPlantedMismatch(t *testing.T) {
	planted := t.TempDir() + "/planted.go"
	source := "package main\n\n" +
		"// alpha_function returns a thing.\n" +
		"func beta_function() int { return 0 }\n"
	if err := os.WriteFile(planted, []byte(source), 0o644); err != nil {
		t.Fatalf("writing the planted file: %v", err)
	}

	found := doc_mismatches(t, planted)
	if len(found) != 1 {
		t.Fatalf("the detector found %d mismatches in a file with exactly one", len(found))
	}
	if found[0].named != "alpha_function" || found[0].attched != "beta_function" {
		t.Errorf("detector reported %q on %q, want alpha_function on beta_function", found[0].named, found[0].attched)
	}
}

// TestDetectorAcceptsAGroupedDeclaration is the other half: the six false
// positives it must NOT report. A doc above `var (` names the first member.
func TestDetectorAcceptsAGroupedDeclaration(t *testing.T) {
	clean := t.TempDir() + "/clean.go"
	source := "package main\n\n" +
		"// cache_total is the running byte total.\n" +
		"var (\n\tcache_total_lock int\n\tcache_total      int\n)\n\n" +
		"// real_function returns a thing.\n" +
		"func real_function() int { return 0 }\n"
	if err := os.WriteFile(clean, []byte(source), 0o644); err != nil {
		t.Fatalf("writing the clean file: %v", err)
	}

	if found := doc_mismatches(t, clean); len(found) != 0 {
		t.Errorf("the detector reported %d mismatches in a clean file: %+v; a doc above a grouped declaration names a member of the group, not the group", len(found), found)
	}
}
