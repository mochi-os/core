// Mochi mochictl: check subcommand tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCheckStarlarkValid passes a syntactically valid file. The
// subcommand must return nil.
func TestCheckStarlarkValid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ok.star")
	if err := os.WriteFile(path, []byte("def foo(): return 1\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cmd_check_starlark([]string{path}); err != nil {
		t.Errorf("valid file: got error %v, want nil", err)
	}
}

// TestCheckStarlarkImplicitConcat reproduces the projects 2.29
// failure mode (Python-style adjacent-string concatenation; valid
// Python, Starlark parse error). The subcommand must return an
// error mentioning the file path so deploy.sh's exit-1 carries
// enough context for the operator to locate the bad line.
func TestCheckStarlarkImplicitConcat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.star")
	if err := os.WriteFile(path, []byte("def foo():\n    return (\"a\"\n            \"b\")\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	err := cmd_check_starlark([]string{path})
	if err == nil {
		t.Fatal("implicit concat: got nil error, want parse failure")
	}
	if !strings.Contains(err.Error(), "bad.star") {
		t.Errorf("error message missing file path: %v", err)
	}
}

// TestCheckStarlarkDirectoryWalk: a single bad file under a directory
// halts the whole pass at the first error.
func TestCheckStarlarkDirectoryWalk(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.star"), []byte("def a(): return 1\n"), 0644); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.star"), []byte("def b(\n"), 0644); err != nil {
		t.Fatalf("write b: %v", err)
	}
	err := cmd_check_starlark([]string{dir})
	if err == nil {
		t.Fatal("directory walk with one bad file: got nil error, want failure")
	}
	if !strings.Contains(err.Error(), "b.star") {
		t.Errorf("error should name the failing file (b.star); got %v", err)
	}
}

// TestCheckStarlarkSkipNonStar: non-.star files in a directory are
// not parsed.
func TestCheckStarlarkSkipNonStar(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ok.star"), []byte("def a(): return 1\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A garbage .json file alongside must not block the deploy.
	if err := os.WriteFile(filepath.Join(dir, "stuff.json"), []byte("not valid syntax (((("), 0644); err != nil {
		t.Fatalf("write json: %v", err)
	}
	if err := cmd_check_starlark([]string{dir}); err != nil {
		t.Errorf("non-.star files must be skipped; got %v", err)
	}
}

// TestCheckStarlarkSkipExcludedDirs: the walk skips .git and node_modules. A
// bad .star in either does NOT fail the check - neither is the app's own
// source.
//
// `web` used to be in this list, and this test used to assert it. It was
// removed on purpose: see TestCheckStarlarkChecksWebDirectory.
func TestCheckStarlarkSkipExcludedDirs(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ok.star"), []byte("def a(): return 1\n"), 0644); err != nil {
		t.Fatalf("write ok: %v", err)
	}
	for _, skip := range []string{".git", "node_modules"} {
		sub := filepath.Join(dir, skip)
		if err := os.MkdirAll(sub, 0755); err != nil {
			t.Fatalf("mkdir %s: %v", sub, err)
		}
		if err := os.WriteFile(filepath.Join(sub, "syntax.star"), []byte("def b(\n"), 0644); err != nil {
			t.Fatalf("write %s/syntax.star: %v", sub, err)
		}
	}
	if err := cmd_check_starlark([]string{dir}); err != nil {
		t.Errorf("excluded dirs should be skipped; got %v", err)
	}
}

// TestCheckStarlarkChecksWebDirectory is the regression. The walk used to skip
// any directory named `web`, on the grounds that a frontend directory holds no
// runtime Starlark - true of every app today, and exactly what made the check
// unable to say so. This walk is deploy.sh's blocking pre-deploy gate, so a
// .star it skips ships without ever being parsed.
func TestCheckStarlarkChecksWebDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ok.star"), []byte("def a(): return 1\n"), 0644); err != nil {
		t.Fatalf("write ok: %v", err)
	}
	web := filepath.Join(dir, "web")
	if err := os.MkdirAll(web, 0755); err != nil {
		t.Fatalf("mkdir web: %v", err)
	}
	if err := os.WriteFile(filepath.Join(web, "syntax.star"), []byte("def b(\n"), 0644); err != nil {
		t.Fatalf("write web/syntax.star: %v", err)
	}

	err := cmd_check_starlark([]string{dir})
	if err == nil {
		t.Fatal("a broken .star under web/ passed the check; it would deploy unparsed")
	}
	if !strings.Contains(err.Error(), "syntax.star") {
		t.Errorf("error should name the failing file; got %v", err)
	}
}

// TestCheckStarlarkChecksNestedWebDirectory: the skip matched at any depth, so
// the fix has to reach below the frontend root too.
func TestCheckStarlarkChecksNestedWebDirectory(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "web", "src", "lib")
	if err := os.MkdirAll(nested, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, "deep.star"), []byte("def b(\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := cmd_check_starlark([]string{dir}); err == nil {
		t.Error("a broken .star under web/src/lib passed the check")
	}
}

// TestCheckStarlarkStillPrunesDependenciesUnderWeb is the other half of the
// fix. Walking web/ must not mean walking its installed dependencies: that is
// where the "bulk-up the walk" concern was real, and node_modules nests, so it
// has to keep matching at any depth.
func TestCheckStarlarkStillPrunesDependenciesUnderWeb(t *testing.T) {
	dir := t.TempDir()
	buried := filepath.Join(dir, "web", "node_modules", "some-package", "vendor")
	if err := os.MkdirAll(buried, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(buried, "syntax.star"), []byte("def b(\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A good file under web/ so the walk has a reason to descend at all.
	if err := os.WriteFile(filepath.Join(dir, "web", "fine.star"), []byte("def a(): return 1\n"), 0644); err != nil {
		t.Fatalf("write fine: %v", err)
	}

	if err := cmd_check_starlark([]string{dir}); err != nil {
		t.Errorf("a broken .star inside web/node_modules failed the check; dependencies are not the app's source: %v", err)
	}
}

// TestCheckStarlarkSkipListNamesNoSourceDirectory is the gate. The defect was a
// directory name in the skip list that holds the app's own files; the two that
// remain hold git's object store and installed dependencies.
func TestCheckStarlarkSkipListNamesNoSourceDirectory(t *testing.T) {
	source, err := os.ReadFile("check.go")
	if err != nil {
		t.Fatalf("reading check.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func cmd_check_starlark(")
	if at < 0 {
		t.Fatal("check.go no longer defines cmd_check_starlark")
	}
	body := text[at:]

	for _, name := range []string{`"web"`, `"src"`, `"dist"`, `"lib"`, `"labels"`, `"templates"`} {
		if strings.Contains(body, "name == "+name) {
			t.Errorf("cmd_check_starlark skips %s; that directory holds app files, and this walk is the blocking pre-deploy gate, so anything it skips deploys unparsed", name)
		}
	}
	if !strings.Contains(body, `name == ".git" || name == "node_modules"`) {
		t.Error("cmd_check_starlark no longer prunes .git and node_modules; both nest and neither is app source, so both should still be skipped at any depth")
	}
}

// TestCheckStarlarkMissingPath: missing arg / bad path returns a
// clear error rather than panicking.
func TestCheckStarlarkMissingPath(t *testing.T) {
	if err := cmd_check_starlark(nil); err == nil {
		t.Error("missing path: got nil error, want usage")
	}
	if err := cmd_check_starlark([]string{"/does/not/exist"}); err == nil {
		t.Error("nonexistent path: got nil error, want stat failure")
	}
}
