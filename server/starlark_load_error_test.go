// Mochi server: a file that fails to load says which file, and says it loudly.
//
// An error from a mochi.* call at module level is a bare Go error with no
// position, so the format must interpolate the path. warn, not info: ExecFile
// loses every definition in the file, and starlark_once caches that partial set
// for the process lifetime wherever dev_reload is off.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// starlark_file writes one .star file into a temporary directory.
func starlark_file(t *testing.T, directory, name, body string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

// TestLoadErrorNamesTheFileForAResolveError. This one carried its position
// already; the assertion is that naming the file did not somehow lose it.
func TestLoadErrorNamesTheFileForAResolveError(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)
	capture := log_captured(t)

	directory := t.TempDir()
	path := starlark_file(t, directory, "handlers.star",
		"def handler(a):\n    return 1\n\nCONFIG = undefined_name_here\n")

	starlark([]string{path})

	line := strings.Join(capture.lines, "\n")
	if !strings.Contains(line, "handlers.star") {
		t.Errorf("the load error does not name the file: %q", line)
	}
}

func TestLoadErrorNamesTheFileForABuiltinError(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)
	capture := log_captured(t)

	directory := t.TempDir()
	path := starlark_file(t, directory, "config.star",
		"def handler(a):\n    return 1\n\nCONFIG = mochi.entity.get(\"nope\")\n")

	starlark([]string{path})

	line := strings.Join(capture.lines, "\n")
	if !strings.Contains(line, "config.star") {
		t.Errorf("a builtin error at module level produced a log line naming no file, so an operator has nothing to grep for: %q", line)
	}
	if !strings.Contains(line, "no app context") {
		t.Errorf("the underlying error was lost from the message: %q", line)
	}
}

// TestAFailedFileLosesEveryDefinition pins the consequence the level change is
// justified by. A def that executed BEFORE the failing statement is gone too -
// this is not "the failing statement is skipped", it is "the file did not
// load".
func TestAFailedFileLosesEveryDefinition(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)
	log_captured(t)

	directory := t.TempDir()
	path := starlark_file(t, directory, "handlers.star",
		"def before(a):\n    return 1\n\nBOOM = undefined_name_here\n\ndef after(a):\n    return 2\n")

	s := starlark([]string{path})

	if s.has("before") {
		t.Error("a definition before the failing statement survived; if ExecFile now returns partial results the level of this log line is worth revisiting")
	}
	if s.has("after") {
		t.Error("a definition after the failing statement survived")
	}
}

// TestAnEarlierFileSurvivesALaterFailure bounds the damage: one bad file does
// not take the app's other files with it. That is what makes "partial global
// set" the right description, and why the app keeps serving.
func TestAnEarlierFileSurvivesALaterFailure(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)
	log_captured(t)

	directory := t.TempDir()
	good := starlark_file(t, directory, "a.star", "def alpha(a):\n    return 1\n")
	bad := starlark_file(t, directory, "b.star", "BOOM = undefined_name_here\n")

	s := starlark([]string{good, bad})

	if !s.has("alpha") {
		t.Error("an earlier file's definitions were lost when a later file failed; the loop is meant to continue with what it has")
	}
}

// TestLoadErrorIsNotRepeatSuppressed: info() runs through log_repeat_allow and
// stops after the threshold, warn() does not. Every failing file shares one
// format key, and the twenty-first missing file is as broken as the first.
func TestLoadErrorIsNotRepeatSuppressed(t *testing.T) {
	log_tables_reset(t)
	defer log_tables_reset(t)
	capture := log_captured(t)

	directory := t.TempDir()
	var files []string
	count := log_repeat_threshold + 5
	for i := 0; i < count; i++ {
		files = append(files, starlark_file(t, directory,
			"bad"+string(rune('a'+i%26))+string(rune('a'+i/26))+".star",
			"BOOM = undefined_name_here\n"))
	}

	starlark(files)

	reported := 0
	for _, line := range capture.lines {
		if strings.Contains(line, "Starlark error reading") {
			reported++
		}
	}
	if reported < count {
		t.Errorf("only %d of %d failing files were reported; at info() the repeat window swallows the rest, and each one is a file whose definitions are missing", reported, count)
	}
}

// TestLoadErrorIsAWarning is the gate on both halves at once.
func TestLoadErrorIsAWarning(t *testing.T) {
	source, err := os.ReadFile("starlark.go")
	if err != nil {
		t.Fatalf("reading starlark.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func starlark(files []string) *Starlark {")
	if at < 0 {
		t.Fatal("starlark.go no longer defines starlark(files)")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}
	// Strip line comments: the explanation above the call quotes the old line.
	var code []string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "//") {
			code = append(code, line)
		}
	}
	body = strings.Join(code, "\n")

	if strings.Contains(body, `info("Starlark error`) {
		t.Error("the load failure is logged at info again; it means the app is running with functions missing, and outside dev_reload that state is cached for the process lifetime")
	}
	if !strings.Contains(body, `warn("Starlark error reading %s: %v", file, err)`) {
		t.Error("the load failure does not warn with the file named; a builtin error carries no position, so without the file the line identifies nothing")
	}
}
