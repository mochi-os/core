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

	sl "go.starlark.net/starlark"
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

// TestCacheAppendAnswersNoneForATransferItCouldNotRun. An offset the entry
// disagrees with, a lock another transfer holds, a write that failed: outcomes
// the Starlark caller degrades on, answered as None. Raising them aborted the
// handler that was serving, since Starlark cannot catch. A caller mistake - a
// source that is not a stream - still raises.
func TestCacheAppendAnswersNoneForATransferItCouldNotRun(t *testing.T) {
	thread := cache_read_thread(t)
	builtin := sl.NewBuiltin("mochi.cache.append", api_cache_append)
	path, err := cache_file(thread, "partial/entry")
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Dir(path), 0755)
	if err := os.WriteFile(path, []byte("abcd"), 0644); err != nil {
		t.Fatal(err)
	}
	append := func(source sl.Value, offset int64) (sl.Value, error) {
		return api_cache_append(thread, builtin, sl.Tuple{sl.String("partial/entry"), source, sl.MakeInt64(offset)}, nil)
	}
	stream := func() *Stream { return &Stream{reader: io.NopCloser(strings.NewReader("x"))} }

	value, err := append(stream(), 2)
	if err != nil || value != sl.None {
		t.Errorf("a disagreeing offset = (%v, %v), want (None, nil)", value, err)
	}
	if err := os.WriteFile(path+".lock", nil, 0644); err != nil {
		t.Fatal(err)
	}
	value, err = append(stream(), 4)
	if err != nil || value != sl.None {
		t.Errorf("a held entry = (%v, %v), want (None, nil)", value, err)
	}
	os.Remove(path + ".lock")
	if _, err := append(sl.String("x"), 4); err == nil {
		t.Error("a source that is not a stream is the caller's mistake and must raise")
	}
	value, err = append(stream(), 4)
	if err != nil || value != sl.MakeInt64(5) {
		t.Errorf("an honest append = (%v, %v), want (5, nil)", value, err)
	}
}

// TestCacheWriteAnswersNoneForAWriteItCouldNotCommit. The same contract for
// write: an entry the cache cannot create - here its parent is a file - is
// None, while a source of the wrong type still raises.
func TestCacheWriteAnswersNoneForAWriteItCouldNotCommit(t *testing.T) {
	thread := cache_read_thread(t)
	builtin := sl.NewBuiltin("mochi.cache.write", api_cache_write)
	path, err := cache_file(thread, "blocked/entry")
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Dir(filepath.Dir(path)), 0755)
	if err := os.WriteFile(filepath.Dir(path), []byte("in the way"), 0644); err != nil {
		t.Fatal(err)
	}
	value, err := api_cache_write(thread, builtin, sl.Tuple{sl.String("blocked/entry"), sl.String("bytes")}, nil)
	if err != nil || value != sl.None {
		t.Errorf("an entry that cannot be created = (%v, %v), want (None, nil)", value, err)
	}
	if _, err := api_cache_write(thread, builtin, sl.Tuple{sl.String("plain/entry"), sl.MakeInt(1)}, nil); err == nil {
		t.Error("a source of the wrong type is the caller's mistake and must raise")
	}
	value, err = api_cache_write(thread, builtin, sl.Tuple{sl.String("plain/entry"), sl.String("bytes")}, nil)
	if err != nil || value != sl.MakeInt64(5) {
		t.Errorf("an honest write = (%v, %v), want (5, nil)", value, err)
	}
}
