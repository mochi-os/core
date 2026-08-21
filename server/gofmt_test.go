// Mochi server: the tree stays gofmt-clean.
//
// Deliberately a gate, not a `gofmt -w` fixer: Go 1.19's doc-comment rules
// smart-quote, so a comment writing an empty value as a pair of single quotes
// has it replaced by one curly quote and stops meaning what it said.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestSourceIsFormatted lists every unformatted file in the package rather than
// failing on the first, so one run says how much there is to do.
func TestSourceIsFormatted(t *testing.T) {
	gofmt, err := exec.LookPath("gofmt")
	if err != nil {
		t.Skipf("gofmt not on PATH: %v", err)
	}

	names, err := filepath.Glob("*.go")
	if err != nil || len(names) == 0 {
		t.Fatalf("no Go files found to check: %v", err)
	}

	out, err := exec.Command(gofmt, append([]string{"-l"}, names...)...).Output()
	if err != nil {
		t.Fatalf("gofmt -l: %v", err)
	}

	unformatted := strings.Fields(string(out))
	if len(unformatted) == 0 {
		return
	}
	t.Errorf("%d file(s) are not gofmt-clean: %s", len(unformatted), strings.Join(unformatted, " "))
	t.Log("Run gofmt -w on the named files. Read the diff first where it touches a doc comment: " +
		"the doc-comment rules smart-quote, so '' in a comment becomes a curly quote and stops reading as an empty value.")
}

// TestDocCommentsDoNotDocumentEmptyWithQuotePairs: gofmt rewrites a pair of
// single quotes or backticks in a doc comment into one curly quote. Inside a
// function body the same text is left alone, so only doc comments are checked.
func TestDocCommentsDoNotDocumentEmptyWithQuotePairs(t *testing.T) {
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	for _, name := range names {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for number, line := range strings.Split(string(body), "\n") {
			trimmed := strings.TrimSpace(line)
			// A doc comment starts at column zero; an indented // is inside a
			// function body, where gofmt does not rewrite the text.
			if !strings.HasPrefix(line, "//") {
				continue
			}
			if strings.Contains(trimmed, "''") || strings.Contains(trimmed, "``") {
				t.Errorf("%s:%d documents an empty value with a quote pair, which gofmt turns into a curly quote: %s",
					name, number+1, trimmed)
			}
		}
	}
}
