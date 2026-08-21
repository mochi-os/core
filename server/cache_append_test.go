// Mochi server: the resumable half of the cache - append, and its guarantees.
//
// cache.write commits whole or not at all, which is right for a served entry
// and wrong for a transfer that must survive its own interruption: a pull
// that dies at byte N should leave N bytes for the next attempt to continue
// from. cache_append_file writes straight into the entry for exactly that
// reason, refuses an offset the entry disagrees with (two racers become one
// clean loser), and holds a sidecar lock meanwhile.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// broken yields its bytes and then fails, like a transfer cut mid-copy.
type broken struct {
	reader io.Reader
	failed bool
}

func (b *broken) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF && !b.failed {
		b.failed = true
		return n, errors.New("transfer broke off")
	}
	return n, err
}

func TestAppendBuildsAnEntryAcrossAttempts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial", "entry")

	total, err := cache_append_file(path, strings.NewReader("hello "), 0, 1<<20)
	if err != nil || total != 6 {
		t.Fatalf("first append = (%d, %v), want (6, nil)", total, err)
	}
	total, err = cache_append_file(path, strings.NewReader("world"), 6, 1<<20)
	if err != nil || total != 11 {
		t.Fatalf("second append = (%d, %v), want (11, nil)", total, err)
	}
	data, _ := os.ReadFile(path)
	if string(data) != "hello world" {
		t.Errorf("entry holds %q", data)
	}
}

func TestAppendKeepsBytesFromABrokenTransfer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "entry")

	total, err := cache_append_file(path, &broken{reader: strings.NewReader("part")}, 0, 1<<20)
	if err == nil {
		t.Fatal("a broken transfer must report its error")
	}
	if total != 4 {
		t.Errorf("total = %d, want the 4 bytes that arrived", total)
	}
	data, _ := os.ReadFile(path)
	if string(data) != "part" {
		t.Errorf("the arrived bytes did not survive: %q", data)
	}
	// The next attempt continues from what survived.
	total, err = cache_append_file(path, strings.NewReader("ial"), 4, 1<<20)
	if err != nil || total != 7 {
		t.Fatalf("resume = (%d, %v), want (7, nil)", total, err)
	}
}

func TestAppendRefusesAnOffsetTheEntryDisagreesWith(t *testing.T) {
	path := filepath.Join(t.TempDir(), "entry")
	if _, err := cache_append_file(path, strings.NewReader("abcd"), 0, 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := cache_append_file(path, strings.NewReader("x"), 2, 1<<20); err == nil {
		t.Fatal("an offset behind the entry's size must be refused, or a racer interleaves")
	}
	if _, err := cache_append_file(path, strings.NewReader("x"), 9, 1<<20); err == nil {
		t.Fatal("an offset past the entry's size must be refused")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "abcd" {
		t.Errorf("a refused append changed the entry: %q", data)
	}
}

func TestAppendHoldsItsLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "entry")
	lock := path + ".lock"
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lock, nil, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := cache_append_file(path, strings.NewReader("x"), 0, 1<<20); err == nil {
		t.Fatal("a held lock must refuse a second appender")
	}
	// A stale lock - older than any live transfer - is overridden.
	old := time.Now().Add(-starlark_file_timeout - time.Minute)
	if err := os.Chtimes(lock, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := cache_append_file(path, strings.NewReader("x"), 0, 1<<20); err != nil {
		t.Fatalf("a stale lock must be overridden: %v", err)
	}
	if _, err := os.Stat(lock); !os.IsNotExist(err) {
		t.Error("the lock outlived its append")
	}
}
