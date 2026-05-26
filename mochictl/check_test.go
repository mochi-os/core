// Mochi mochictl: check subcommand tests
// Copyright Alistair Cunningham 2026

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

// TestCheckStarlarkSkipExcludedDirs: the walk skips .git, node_modules,
// and web subdirectories. A bad .star in any of those does NOT fail
// the check - those dirs never contain runtime Starlark.
func TestCheckStarlarkSkipExcludedDirs(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ok.star"), []byte("def a(): return 1\n"), 0644); err != nil {
		t.Fatalf("write ok: %v", err)
	}
	for _, skip := range []string{".git", "node_modules", "web"} {
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
