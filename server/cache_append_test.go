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
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

	total, err := cache_append_file(path, strings.NewReader("hello "), 0, 1<<20, "", false, nil)
	if err != nil || total != 6 {
		t.Fatalf("first append = (%d, %v), want (6, nil)", total, err)
	}
	total, err = cache_append_file(path, strings.NewReader("world"), 6, 1<<20, "", false, nil)
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

	total, err := cache_append_file(path, &broken{reader: strings.NewReader("part")}, 0, 1<<20, "", false, nil)
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
	total, err = cache_append_file(path, strings.NewReader("ial"), 4, 1<<20, "", false, nil)
	if err != nil || total != 7 {
		t.Fatalf("resume = (%d, %v), want (7, nil)", total, err)
	}
}

func TestAppendRefusesAnOffsetTheEntryDisagreesWith(t *testing.T) {
	path := filepath.Join(t.TempDir(), "entry")
	if _, err := cache_append_file(path, strings.NewReader("abcd"), 0, 1<<20, "", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := cache_append_file(path, strings.NewReader("x"), 2, 1<<20, "", false, nil); err == nil {
		t.Fatal("an offset behind the entry's size must be refused, or a racer interleaves")
	}
	if _, err := cache_append_file(path, strings.NewReader("x"), 9, 1<<20, "", false, nil); err == nil {
		t.Fatal("an offset past the entry's size must be refused")
	}
	gone := filepath.Join(filepath.Dir(path), "gone")
	if _, err := cache_append_file(gone, strings.NewReader("x"), 3, 1<<20, "", false, nil); !errors.Is(err, cache_append_refused) {
		t.Errorf("an append continuing an entry that is not there = %v, want a refusal", err)
	}
	if _, err := os.Stat(gone); !os.IsNotExist(err) {
		t.Error("THE POINT: a refused append made an empty entry where the one it was to continue had gone")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "abcd" {
		t.Errorf("a refused append changed the entry: %q", data)
	}
}

func TestAppendHoldsItsEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "entry")
	release, held := cache_test_hold(path)
	if !held {
		t.Fatal("CONTROL: an entry nothing appends to could not be taken")
	}
	if _, err := cache_append_file(path, strings.NewReader("x"), 0, 1<<20, "", false, nil); err == nil {
		t.Fatal("THE POINT: a second appender was not refused while the first holds the entry")
	}
	release()
	if _, err := cache_append_file(path, strings.NewReader("x"), 0, 1<<20, "", false, nil); err != nil {
		t.Fatalf("an entry released by its appender could not be appended to: %v", err)
	}
	if _, held := cache_holds.running[path]; held {
		t.Error("the hold outlived its append")
	}
	// Nothing on disk holds an entry: a lock file a crash or an older core left
	// behind is ignored.
	if err := os.WriteFile(path+".lock", nil, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := cache_append_file(path, strings.NewReader("y"), 1, 1<<20, "", false, nil); err != nil {
		t.Errorf("THE POINT: a lock file on disk held the entry: %v", err)
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
	release, _ := cache_test_hold(path)
	value, err = append(stream(), 4)
	if err != nil || value != sl.None {
		t.Errorf("a held entry = (%v, %v), want (None, nil)", value, err)
	}
	release()
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

// TestAppendFinishesUnderItsHold. Given its destination, an append that took
// exactly the bytes that finish the entry moves it into place before letting
// go of its hold, so a request woken by the hold's end finds the entry, and no
// request can find the finished partial not yet moved and delete it from under
// its mover. An overrun is deleted under the hold; a short transfer keeps its
// partial for the next attempt.
func TestAppendFinishesUnderItsHold(t *testing.T) {
	directory := t.TempDir()
	partial := filepath.Join(directory, "partial", "entry")
	entry := filepath.Join(directory, "remote", "entry")

	reader, writer := io.Pipe()
	result := make(chan error, 1)
	go func() {
		_, err := cache_append_file(partial, reader, 0, 5, entry, false, nil)
		result <- err
	}()
	var running *cache_hold
	for running == nil {
		cache_holds.Lock()
		running = cache_holds.running[partial]
		cache_holds.Unlock()
		time.Sleep(time.Millisecond)
	}
	seen := make(chan bool, 1)
	go func() {
		<-running.done
		_, err := os.Stat(entry)
		seen <- err == nil
	}()
	writer.Write([]byte("abcd"))
	writer.Close()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if !<-seen {
		t.Error("THE POINT: the hold was let go before the finished entry was in place")
	}
	if data, _ := os.ReadFile(entry); string(data) != "abcd" {
		t.Errorf("the finished entry holds %q, want %q", data, "abcd")
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Error("the finished partial was left behind")
	}

	overrun := filepath.Join(directory, "remote", "overrun")
	total, err := cache_append_file(partial, strings.NewReader("abcdefgh"), 0, 5, overrun, false, nil)
	if err != nil || total != 5 {
		t.Errorf("an overrun = (%d, %v), want (5, nil): the bound plus the byte that shows it", total, err)
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Error("THE POINT: an overrun partial was kept, though nothing in it can be trusted")
	}
	if _, err := os.Stat(overrun); !os.IsNotExist(err) {
		t.Error("an overrun was moved into place")
	}

	short := filepath.Join(directory, "remote", "short")
	total, err = cache_append_file(partial, strings.NewReader("ab"), 0, 5, short, false, nil)
	if err != nil || total != 2 {
		t.Errorf("a short transfer = (%d, %v), want (2, nil)", total, err)
	}
	if data, _ := os.ReadFile(partial); string(data) != "ab" {
		t.Errorf("a short transfer's partial holds %q, want it kept for the next attempt", data)
	}
	if _, err := os.Stat(short); !os.IsNotExist(err) {
		t.Error("a short transfer was moved into place")
	}
}

// TestAppendRestartsUnderItsHold. A restart truncates the partial and appends
// from its start, under the hold: a source that would not continue from the
// partial's bytes starts over without the partial being deleted from under
// another transfer appending to it.
func TestAppendRestartsUnderItsHold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial")
	if err := os.WriteFile(path, []byte("xyz"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := cache_append_file(path, strings.NewReader("abcd"), 0, 1<<20, "", false, nil); err == nil {
		t.Fatal("CONTROL: an offset of 0 against three bytes was not refused")
	}
	total, err := cache_append_file(path, strings.NewReader("abcd"), 0, 1<<20, "", true, nil)
	if err != nil || total != 4 {
		t.Errorf("a restart = (%d, %v), want (4, nil)", total, err)
	}
	if data, _ := os.ReadFile(path); string(data) != "abcd" {
		t.Errorf("a restart left %q, want %q", data, "abcd")
	}
	release, _ := cache_test_hold(path)
	if _, err := cache_append_file(path, strings.NewReader("zz"), 0, 1<<20, "", true, nil); err == nil {
		t.Error("THE POINT: a restart ran over a partial another transfer holds")
	}
	release()
	if data, _ := os.ReadFile(path); string(data) != "abcd" {
		t.Errorf("THE POINT: a restart refused for another transfer's hold still changed its partial, to %q", data)
	}
}

// TestAppendEndsWithItsCall. An append whose source sends a little and then
// stays silent ends when its call does: the stream is closed, so the copy
// stops and the hold is let go. Held on, every later request for the entry
// would wait on a transfer that never ends until the process restarted.
func TestAppendEndsWithItsCall(t *testing.T) {
	thread := cache_read_thread(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	thread.SetLocal("context", ctx)
	reader, writer := io.Pipe()
	defer writer.Close()
	builtin := sl.NewBuiltin("mochi.cache.append", api_cache_append)
	result := make(chan sl.Value, 1)
	go func() {
		value, _ := api_cache_append(thread, builtin, sl.Tuple{sl.String("partial/stalled"), &Stream{reader: reader}, sl.MakeInt64(0)}, nil)
		result <- value
	}()
	writer.Write([]byte("ab"))
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case value := <-result:
		if value != sl.MakeInt64(2) {
			t.Errorf("an append cut short by its call answered %v, want the 2 bytes it took", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("THE POINT: an append whose call ended kept reading from its silent source")
	}
	path, err := cache_file(thread, "partial/stalled")
	if err != nil {
		t.Fatal(err)
	}
	if cache_held(path) {
		t.Error("the hold outlived its call")
	}
}

// TestCacheAppendRefusesAnUnfinishableRequest. A destination without the
// size that finishes the entry, or a restart from anywhere but the start, is
// the caller's mistake and raises.
func TestCacheAppendRefusesAnUnfinishableRequest(t *testing.T) {
	thread := cache_read_thread(t)
	builtin := sl.NewBuiltin("mochi.cache.append", api_cache_append)
	stream := func() *Stream { return &Stream{reader: io.NopCloser(strings.NewReader("x"))} }
	if _, err := api_cache_append(thread, builtin, sl.Tuple{sl.String("partial/a"), stream(), sl.MakeInt64(0)},
		[]sl.Tuple{{sl.String("destination"), sl.String("remote/a")}}); err == nil {
		t.Error("a destination with no maximum did not raise")
	}
	if _, err := api_cache_append(thread, builtin, sl.Tuple{sl.String("partial/a"), stream(), sl.MakeInt64(3)},
		[]sl.Tuple{{sl.String("restart"), sl.True}}); err == nil {
		t.Error("a restart from offset 3 did not raise")
	}
	if value, err := api_cache_append(thread, builtin, sl.Tuple{sl.String("partial/b"), stream(), sl.MakeInt64(0)},
		[]sl.Tuple{{sl.String("destination"), sl.String("")}}); err != nil || value != sl.MakeInt64(1) {
		t.Errorf("the default destination given outright = (%v, %v), want (1, nil)", value, err)
	}
}

// TestAppendRefusesAReplacedEntry. An entry deleted while it is appended to
// - and so, perhaps, made again by someone else - is neither moved into place
// nor deleted: the append answers 0, a transfer it could not finish, and the
// bytes it wrote went to a file no name reaches.
func TestAppendRefusesAReplacedEntry(t *testing.T) {
	directory := t.TempDir()
	partial := filepath.Join(directory, "partial", "entry")
	entry := filepath.Join(directory, "remote", "entry")
	reader, writer := io.Pipe()
	result := make(chan error, 1)
	totals := make(chan int64, 1)
	go func() {
		total, err := cache_append_file(partial, reader, 0, 5, entry, false, nil)
		totals <- total
		result <- err
	}()
	writer.Write([]byte("ab"))
	os.Remove(partial)
	os.WriteFile(partial, []byte("someone else's"), 0644)
	writer.Write([]byte("cd"))
	writer.Close()
	if total, err := <-totals, <-result; total != 0 || err == nil || errors.Is(err, cache_append_refused) {
		t.Errorf("THE POINT: an entry replaced under its append answered (%d, %v), not 0 and a transfer it could not finish", total, err)
	}
	if _, err := os.Stat(entry); !os.IsNotExist(err) {
		t.Error("THE POINT: a replaced entry was moved into place")
	}
	if data, _ := os.ReadFile(partial); string(data) != "someone else's" {
		t.Errorf("the file now at the name was touched: %q", data)
	}
}

// TestCacheAppendRefusedKeepsTheComputeBound. An append that cannot run -
// its entry held by another, or holding other bytes than offset says - does
// not move its call to the transfer bound: only a transfer does.
func TestCacheAppendRefusedKeepsTheComputeBound(t *testing.T) {
	thread := cache_read_thread(t)
	serving := new(atomic.Bool)
	thread.SetLocal("file_serving", serving)
	builtin := sl.NewBuiltin("mochi.cache.append", api_cache_append)
	stream := &Stream{reader: io.NopCloser(strings.NewReader("x"))}
	value, err := api_cache_append(thread, builtin, sl.Tuple{sl.String("partial/c"), stream, sl.MakeInt64(7)}, nil)
	if err != nil || value != sl.None {
		t.Fatalf("CONTROL: an append at a wrong offset = (%v, %v), want (None, nil)", value, err)
	}
	if serving.Load() {
		t.Error("THE POINT: a refused append took the transfer bound")
	}
	stream = &Stream{reader: io.NopCloser(strings.NewReader("x"))}
	if value, _ := api_cache_append(thread, builtin, sl.Tuple{sl.String("partial/c"), stream, sl.MakeInt64(0)}, nil); value != sl.MakeInt64(1) || !serving.Load() {
		t.Errorf("CONTROL: an append that ran = %v, transfer bound %v", value, serving.Load())
	}
}

// TestAppendFinishesASourceThatErrsAtTheEnd. The read past the bound is only
// there to see an overrun: a source that sent every byte and then answered it
// with an error - a reset, its call's time limit closing the stream - has
// sent the whole entry, which is moved into place.
func TestAppendFinishesASourceThatErrsAtTheEnd(t *testing.T) {
	directory := t.TempDir()
	partial := filepath.Join(directory, "partial", "entry")
	entry := filepath.Join(directory, "remote", "entry")
	total, err := cache_append_file(partial, &broken{reader: strings.NewReader("abcd")}, 0, 5, entry, false, nil)
	if total != 4 || err != nil {
		t.Errorf("a whole entry ending in an error = (%d, %v), want (4, nil)", total, err)
	}
	if data, _ := os.ReadFile(entry); string(data) != "abcd" {
		t.Errorf("THE POINT: a whole entry ending in an error was not moved into place: %q", data)
	}
}

// TestAppendCannotFinishAnswersZero. A transfer that ran but could not be
// moved into place is short, 0, for its caller to back off from, and its
// partial is removed: answered None, as an append that never ran, every view
// would fetch the whole original again.
func TestAppendCannotFinishAnswersZero(t *testing.T) {
	directory := t.TempDir()
	partial := filepath.Join(directory, "partial", "entry")
	if err := os.WriteFile(filepath.Join(directory, "blocked"), nil, 0644); err != nil {
		t.Fatal(err)
	}
	total, err := cache_append_file(partial, strings.NewReader("abcd"), 0, 5, filepath.Join(directory, "blocked", "entry"), false, nil)
	if total != 0 || err == nil || errors.Is(err, cache_append_refused) {
		t.Errorf("THE POINT: a transfer that could not be moved into place = (%d, %v), want 0 and a transfer it could not finish", total, err)
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Error("the unfinished partial was left behind")
	}
}

// TestAppendIsCountedOnceInPlace. The bytes an append adds are admitted once
// its entry is in place: admitted while it was held, the eviction they set off
// left the entry out of the total it then set, and the cache sat over its
// budget by every such entry until the next walk.
func TestAppendIsCountedOnceInPlace(t *testing.T) {
	orig_dir, orig_budget := cache_dir, cache_budget
	cache_dir = t.TempDir()
	cache_total_set(-1)
	defer func() { cache_dir, cache_budget = orig_dir, orig_budget; cache_total_set(-1) }()
	cache_budget = 100
	old := time.Now().Add(-time.Hour)
	for _, name := range []string{"a", "b", "c", "d"} {
		cache_test_write(t, "u", "app", name, 20, old)
	}
	base := filepath.Join(cache_dir, "apps", "u", "app")
	if _, err := cache_append_file(filepath.Join(base, "partial"), strings.NewReader(strings.Repeat("x", 50)), 0, 51, filepath.Join(base, "entry"), false, nil); err != nil {
		t.Fatal(err)
	}
	cache_total_lock.Lock()
	running := cache_total
	cache_total_lock.Unlock()
	if measured := cache_measure(); running != measured {
		t.Errorf("THE POINT: the running total is %d where the cache holds %d", running, measured)
	}
}
