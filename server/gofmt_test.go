// Mochi server: the tree stays gofmt-clean.
//
// This drift kept resurfacing during unrelated work - a file would be listed by
// gofmt -l, get noticed mid-task, and be left alone because reformatting it
// would have buried the change under review. A gate turns that into a failure
// at the moment it is introduced, when it is one file and obvious, rather than
// a periodic sweep of two dozen.
//
// It is deliberately not a `gofmt -w` fixer. Go 1.19's doc-comment rules do
// more than align: they smart-quote, so a doc comment that writes an empty value
// as a pair of single quotes has that pair replaced by one curly quote, and the
// comment stops meaning what it said. Three comments in this package needed
// rewording before formatting was safe, which is judgement no gate should be
// making unattended.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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

// TestDocCommentsDoNotDocumentEmptyWithQuotePairs is the specific trap that made
// the sweep unsafe to run blind. A doc comment writing an empty value as a pair
// of single quotes, or of backticks, has that pair rewritten by gofmt into one
// curly quote, so the comment silently stops saying what it said. Inside a
// function body the same text is left alone, which is why this checks doc
// comments only - and why it went unnoticed for so long.
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
