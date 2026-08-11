// Mochi server: Archive extraction bounds
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// bomb_zip writes an archive of `entries` files, each `size` bytes of a single
// repeated byte. Highly compressible on purpose: the point of a bomb is that
// the archive on disk says nothing about what it expands to.
func bomb_zip(t *testing.T, entries int, size int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bomb.zip")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	w := zip.NewWriter(f)
	payload := bytes.Repeat([]byte{'A'}, size)
	for i := 0; i < entries; i++ {
		e, err := w.Create(filepath.Join("pkg", "file"+itoa(i)+".bin"))
		if err != nil {
			t.Fatalf("create entry: %v", err)
		}
		if _, err := e.Write(payload); err != nil {
			t.Fatalf("write entry: %v", err)
		}
	}
	w.Close()
	f.Close()
	return path
}

// TestUnzipRefusesDecompressionBomb — the running total is what matters: a
// bomb declares a small compressed size, so trusting the header would let one
// entry expand without limit.
func TestUnzipRefusesDecompressionBomb(t *testing.T) {
	zip_path := bomb_zip(t, 4, 64*1024)
	dest := t.TempDir()

	err := unzip(zip_path, dest, 100*1024) // 256 KB of payload against a 100 KB budget
	if err == nil {
		t.Fatal("an archive expanding past the budget was extracted")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("error does not name the limit, so an operator cannot tell a bomb from an outgrown cap: %v", err)
	}
}

// TestUnzipAllowsArchiveWithinBudget — the cap must not break a legitimate
// package. Real ones expand 1.3x to 3x; only a bomb is anywhere near the cap.
func TestUnzipAllowsArchiveWithinBudget(t *testing.T) {
	zip_path := bomb_zip(t, 4, 16*1024)
	dest := t.TempDir()

	if err := unzip(zip_path, dest, unzip_maximum_bytes); err != nil {
		t.Fatalf("a 64 KB archive was refused under the real cap: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(dest, "pkg", "file0.bin"))
	if err != nil {
		t.Fatalf("expected file missing: %v", err)
	}
	if len(content) != 16*1024 {
		t.Errorf("extracted %d bytes, want %d", len(content), 16*1024)
	}
}

// TestUnzipRefusesTooManyEntries — the other bomb shape: millions of tiny
// files exhaust inodes rather than disk, so a byte cap alone does not catch it.
func TestUnzipRefusesTooManyEntries(t *testing.T) {
	zip_path := bomb_zip(t, unzip_maximum_entries+1, 1)
	dest := t.TempDir()

	err := unzip(zip_path, dest, unzip_maximum_bytes)
	if err == nil {
		t.Fatal("an archive past the entry cap was extracted")
	}
	if !strings.Contains(err.Error(), "entries") {
		t.Errorf("error does not identify the entry cap: %v", err)
	}
	// Refused before writing: the check is on the central directory, so
	// nothing should have been created.
	if entries, _ := os.ReadDir(dest); len(entries) != 0 {
		t.Errorf("wrote %d path(s) before refusing", len(entries))
	}
}

// TestZipEntryReadInPlace — reading one entry writes nothing to disk, which is
// what removes the bomb surface from the metadata path rather than bounding it.
func TestZipEntryReadInPlace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pkg.zip")
	f, _ := os.Create(path)
	w := zip.NewWriter(f)
	e, _ := w.Create("app.json")
	e.Write([]byte(`{"version":"1.0"}`))
	e, _ = w.Create("labels/en.conf")
	e.Write([]byte("app.name = Test\n"))
	w.Close()
	f.Close()

	r, err := zip.OpenReader(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	got, err := zip_entry_read(&r.Reader, "app.json", app_metadata_maximum)
	if err != nil {
		t.Fatalf("app.json: %v", err)
	}
	if string(got) != `{"version":"1.0"}` {
		t.Errorf("app.json = %q", got)
	}
	if _, err := zip_entry_read(&r.Reader, "labels/en.conf", app_metadata_maximum); err != nil {
		t.Errorf("labels/en.conf: %v", err)
	}
	if _, err := zip_entry_read(&r.Reader, "nope.txt", app_metadata_maximum); err == nil {
		t.Error("a missing entry read as present")
	}
}

// TestZipEntryReadRefusesOversizeEntry — a package whose app.json alone runs to
// megabytes is not one to describe to a caller, and reading it into memory
// unbounded is the same bomb by another route.
func TestZipEntryReadRefusesOversizeEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.zip")
	f, _ := os.Create(path)
	w := zip.NewWriter(f)
	e, _ := w.Create("app.json")
	e.Write(bytes.Repeat([]byte{'x'}, 4096))
	w.Close()
	f.Close()

	r, err := zip.OpenReader(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	if _, err := zip_entry_read(&r.Reader, "app.json", 1024); err == nil {
		t.Error("an entry past the budget was read")
	}
	// Exactly at the budget is fine; only past it is refused.
	if _, err := zip_entry_read(&r.Reader, "app.json", 4096); err != nil {
		t.Errorf("an entry exactly filling the budget was refused: %v", err)
	}
}
