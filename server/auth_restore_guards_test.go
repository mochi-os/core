// Mochi server: backup-restore unzip guards (#21).
//
// The signup-via-restore bundle is uploaded by an unauthenticated caller, so
// restore_unzip must reject path traversal (zip-slip) and bound decompression
// so a zip-bomb can't exhaust the disk. The byte cap is the per-user storage
// quota for an ordinary restore (admins get a generous ceiling, set by the
// caller). Cross-user containment is separately ensured by the destination
// using a fresh server-generated uid, never the bundle's.
//
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
	"testing"
)

func TestRestoreUnzipGuards(t *testing.T) {
	makeZip := func(entries map[string]int) string {
		zp := filepath.Join(t.TempDir(), "b.zip")
		f, err := os.Create(zp)
		if err != nil {
			t.Fatal(err)
		}
		zw := zip.NewWriter(f)
		for name, size := range entries {
			w, err := zw.Create(name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(bytes.Repeat([]byte("a"), size)); err != nil {
				t.Fatal(err)
			}
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		f.Close()
		return zp
	}

	// Path traversal (zip-slip) is rejected.
	if _, err := restore_unzip(makeZip(map[string]int{"top/ok.txt": 1, "../escape.txt": 1}), t.TempDir(), 1<<20); err == nil {
		t.Error("traversal entry (../escape.txt) must be rejected")
	}

	// A bundle decompressing past maxBytes is rejected (zip-bomb guard).
	if _, err := restore_unzip(makeZip(map[string]int{"top/big.bin": 4096}), t.TempDir(), 1024); err == nil {
		t.Error("bundle exceeding maxBytes must be rejected")
	}

	// Within the cap it extracts cleanly.
	if _, err := restore_unzip(makeZip(map[string]int{"top/small.bin": 256}), t.TempDir(), 1024); err != nil {
		t.Errorf("within-cap bundle must extract: %v", err)
	}
}
