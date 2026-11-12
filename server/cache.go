// Mochi server: Cache
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.
//
// Server-managed byte cache for apps. Entries hold anything re-obtainable and
// are exempt from the per-user storage quota in exchange for unconditional
// evictability: the sweep may remove any entry at any time and a miss
// re-obtains. Originals never belong here.
//
// Entries live under cache_dir/apps/<user>/<app>/<name>. A file's modification
// time is its last access, and eviction removes least-recently-used entries
// beyond the budget, users over their fair share first. Writes are atomic.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// cache_budget is the byte budget for the apps namespace of the cache.
// Negative means not yet configured (eviction skips). Zero evicts every
// entry on each sweep - the aggressive development mode for proving apps
// tolerate eviction. Set from [cache] maximum in mochi.conf (megabytes; -1
// selects the aggressive mode), defaulting to a tenth of the disk holding
// cache_dir.
var cache_budget int64 = -1

// cache_read_maximum bounds mochi.cache.read when the caller sets no maximum:
// an entry can be as large as object_maximum, and the read lands whole on the
// heap. Larger entries are mochi.cache.copy's job. A var only so tests can
// lower it.
var cache_read_maximum int64 = 64 << 20

// cache_grace is how long a write's .partial temporary counts as a transfer in
// flight: eviction and measurement leave it alone, so a write is not deleted
// from under its rename. Past it the file is abandoned - no Starlark call runs
// that long - and is reclaimed like any entry, since nothing else ever removes
// one. A partial an append fills is spared only while the append holds it
// (cache_holds).
const cache_grace = time.Hour

// cache_total is the running byte total of the apps namespace, so admission
// does not walk the tree on every write. Negative means not yet measured; every
// walk replaces it with an exact figure, so drift cannot outlive an hour.
var (
	cache_total_lock sync.Mutex
	cache_total      int64 = -1
)

// cache_headroom_divisor sets how far below the budget an admission eviction
// clears: budget/8. cache_evict stops the instant it reaches the budget, so
// evicting to exactly that would leave the very next write over again and walk
// the whole tree per write. Clearing a margin means one walk serves many.
const cache_headroom_divisor = 8

var api_cache = sls.FromStringDict(sl.String("mochi.cache"), sl.StringDict{
	"write":  sl.NewBuiltin("mochi.cache.write", api_cache_write),
	"append": sl.NewBuiltin("mochi.cache.append", api_cache_append),
	"size":   sl.NewBuiltin("mochi.cache.size", api_cache_size),
	"rename": sl.NewBuiltin("mochi.cache.rename", api_cache_rename),
	"read":   sl.NewBuiltin("mochi.cache.read", api_cache_read),
	"path":   sl.NewBuiltin("mochi.cache.path", api_cache_path),
	"age":    sl.NewBuiltin("mochi.cache.age", api_cache_age),
	"copy":   sl.NewBuiltin("mochi.cache.copy", api_cache_copy),
	"delete": sl.NewBuiltin("mochi.cache.delete", api_cache_delete),
	"wait":   sl.NewBuiltin("mochi.cache.wait", api_cache_wait),
})

func cache_configure() {
	m := ini_int("cache", "maximum", 0)
	if m < 0 {
		cache_budget = 0
		return
	}
	if m > 0 {
		cache_budget = int64(m) << 20
		return
	}
	capacity := disk_capacity(cache_dir)
	if capacity > 0 {
		cache_budget = capacity / 10
	} else {
		cache_budget = 10 << 30
	}
}

// cache_base resolves the calling app's cache directory, creating it. User
// resolution is principal_storage, matching mochi.db, mochi.file and a.upload:
// the requesting user when there is one, otherwise the entity owner.
func cache_base(t *sl.Thread) (string, error) {
	user, err := principal_storage(t)
	if err != nil || user == nil {
		return "", fmt.Errorf("no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return "", fmt.Errorf("no app")
	}
	base := filepath.Join(cache_dir, "apps", user.UID, app.id)
	if err := os.MkdirAll(base, 0755); err != nil {
		return "", fmt.Errorf("unable to create cache directory: %v", err)
	}
	return base, nil
}

// cache_file joins a validated entry name onto the app's cache base. The
// "filepath" validator refuses traversal, so the join stays under the base.
func cache_file(t *sl.Thread, name string) (string, error) {
	if !valid(name, "filepath") {
		return "", fmt.Errorf("invalid name %q", name)
	}
	base, err := cache_base(t)
	if err != nil {
		return "", err
	}
	return filepath.Join(base, name), nil
}

// cache_value extracts a Starlark string argument as a cache entry path.
func cache_value(t *sl.Thread, v sl.Value) (string, error) {
	name, ok := sl.AsString(v)
	if !ok {
		return "", fmt.Errorf("invalid name")
	}
	return cache_file(t, name)
}

// mochi.cache.write(name, source, maximum=0) -> int or None: Store bytes or a
// stream as a cache entry, returns size. None when the entry could not be
// written - the copy or the commit failed - which is an outcome for the caller
// to degrade on, not a mistake: Starlark cannot catch a raised error, so
// raising aborted the handler that was serving a request. A bad name or source
// still raises.
func api_cache_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, source sl.Value
	var maximum int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "source", &source, "maximum?", &maximum); err != nil {
		return nil, err
	}
	path, err := cache_value(t, name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	// The bound is maximum+1, not maximum: a source that overruns must be
	// distinguishable from one that fits exactly. Cutting at maximum would
	// truncate an oversized transfer to precisely the declared size.
	limit := int64(object_maximum)
	if maximum > 0 && maximum < limit {
		limit = maximum + 1
	}
	var reader io.Reader
	switch v := source.(type) {
	case sl.String:
		reader = strings.NewReader(string(v))
	case sl.Bytes:
		reader = strings.NewReader(string(v))
	case *Stream:
		defer v.close_read()
		// A stream source is a bulk transfer from a peer, not computation, and
		// at the largest object the platform stores it cannot complete inside
		// the compute budget on an ordinary link. It ends with its call: given
		// up on at the time limit, its stream is closed and the copy stops.
		starlark_transfer_set(t)
		stop := context.AfterFunc(starlark_context(t), v.close)
		defer stop()
		reader = v.raw_reader()
	default:
		return sl_error(fn, "source must be bytes or a stream")
	}
	n, err := cache_write_file(path, io.LimitReader(reader, limit))
	if err != nil {
		debug("cache write %s failed: %v", path, err)
		return sl.None, nil
	}
	return sl.MakeInt64(n), nil
}

// mochi.cache.append(name, source, offset, maximum=0, destination="",
// restart=False) -> int or None: Append a stream's bytes to a cache entry at
// offset, returning the entry's new total size, or None when the append did not
// run: another append is filling the entry, its size is not offset, or it
// could not be opened. None moved nothing, so the caller may try again at once.
// A total short of what was asked is a transfer that ran and stopped - the
// source broke off, this server could not write the bytes, or, given a
// destination, could not move them into place, which answers 0 - and is the
// caller's to back off from. Those are outcomes
// for the caller to degrade on, as for write; a bad name, source or offset
// still raises. Built for resumable transfers: unlike write, which commits whole or
// not at all, append writes straight into the entry, so bytes that arrived
// before a broken or abandoned transfer SURVIVE for the next attempt to
// continue from. The entry must therefore never be a name anything serves -
// callers keep partials under their own prefix and rename into the served
// name once complete.
//
// offset is the caller's belief about the entry's current size, and the
// append refuses when the entry disagrees - that is what turns two callers
// racing the same partial into one clean loser instead of an interleaved
// corruption. The entry is held meanwhile, in this process (cache_holds): a
// transfer never outlives it, so a crash leaves nothing to hold the entry.
// destination, with maximum, finishes the transfer under the same hold: an
// entry that took exactly maximum bytes is moved to destination, and one that
// overran is deleted, so no other request can find a finished partial not yet
// moved, or delete one from under its mover. restart truncates the entry and
// appends from its start (offset 0), under the hold: a source that would not
// continue from the entry's bytes starts over without another request's
// partial being deleted from under it. The append ends with its call: given up
// on at the time limit, its stream is closed and the hold released, rather
// than kept for as long as the source trickles bytes.
// maximum bounds the bytes accepted THIS call,
// maximum+1 as everywhere, so an overrun is detectable.
func api_cache_append(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, source sl.Value
	var destination sl.Value = sl.None
	var offset int64
	var maximum int64
	restart := false
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "source", &source, "offset", &offset, "maximum?", &maximum, "destination?", &destination, "restart?", &restart); err != nil {
		return nil, err
	}
	path, err := cache_value(t, name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	stream, ok := source.(*Stream)
	if !ok || stream == nil {
		return sl_error(fn, "source must be a stream")
	}
	defer stream.close_read()
	if offset < 0 {
		return sl_error(fn, "invalid offset %d", offset)
	}
	if restart && offset != 0 {
		return sl_error(fn, "a restart appends from offset 0")
	}
	finished := ""
	if destination != sl.None && destination != sl.String("") {
		if maximum <= 0 || maximum >= object_maximum {
			return sl_error(fn, "a destination needs a maximum: the bytes that finish the entry")
		}
		finished, err = cache_value(t, destination)
		if err != nil {
			return sl_error(fn, "%v", err)
		}
	}
	limit := int64(object_maximum)
	if maximum > 0 && maximum < limit {
		limit = maximum + 1
	}
	stop := context.AfterFunc(starlark_context(t), stream.close)
	defer stop()
	// A bulk transfer from a peer, not computation: the transfer bound, as
	// for a stream source to write - once the append runs, so a refused one
	// cannot lift its call's time limit.
	total, err := cache_append_file(path, stream.raw_reader(), offset, limit, finished, restart, func() { starlark_transfer_set(t) })
	if errors.Is(err, cache_append_refused) {
		debug("cache append %s did not run: %v", path, err)
		return sl.None, nil
	}
	if err != nil {
		debug("cache append %s stopped short: %v", path, err)
	}
	return sl.MakeInt64(total), nil
}

// cache_wait_maximum bounds how long one mochi.cache.wait may wait, so a script
// cannot hold its request indefinitely on another transfer.
const cache_wait_maximum = 120

// cache_wait_waiters bounds how many requests wait on one transfer at once:
// each holds a Starlark slot while it waits, and a flood of requests for one
// slowly arriving entry must not take them all. One past the bound is told the
// transfer is still running.
const cache_wait_waiters = 4

// cache_wait_settle is how long a waiter that saw a transfer end allows for the
// entry to appear when the transfer's caller moves it into place itself; an
// append given its destination has moved it before the hold ends.
const cache_wait_settle = 2 * time.Second

// cache_wait_poll is how often a waiter looks for the entry while it settles.
const cache_wait_poll = 50 * time.Millisecond

// cache_holds are the transfers running in this process, by the path each
// writes: an append filling a cache partial, or a copy filling a file in an
// app's storage. A transfer never outlives the process running it, so this is
// the whole truth about which paths are held: nothing a crash or restart leaves
// on disk holds one, and nothing is judged stale while its transfer still runs.
var cache_holds = struct {
	sync.Mutex
	running map[string]*cache_hold
}{running: map[string]*cache_hold{}}

// cache_hold is one running transfer. done closes when it ends; waiting counts
// the requests waiting on it; size and ok are what a copy wrote and whether it
// succeeded, for the requests that shared it, read once done has closed.
type cache_hold struct {
	done    chan struct{}
	waiting int
	size    int64
	ok      bool
}

// cache_hold_take takes path for a transfer, answering the new hold and true,
// or the hold of the transfer already running and false.
func cache_hold_take(path string) (*cache_hold, bool) {
	cache_holds.Lock()
	defer cache_holds.Unlock()
	if running, held := cache_holds.running[path]; held {
		return running, false
	}
	running := &cache_hold{done: make(chan struct{})}
	cache_holds.running[path] = running
	return running, true
}

// cache_hold_release ends the transfer holding path.
func cache_hold_release(path string, running *cache_hold) {
	cache_holds.Lock()
	delete(cache_holds.running, path)
	cache_holds.Unlock()
	close(running.done)
}

// cache_held reports whether a transfer is running on path.
func cache_held(path string) bool {
	cache_holds.Lock()
	defer cache_holds.Unlock()
	_, held := cache_holds.running[path]
	return held
}

// cache_remove_unheld removes path unless a transfer is running on it, judged
// under the holds' lock so no transfer can take it in between.
func cache_remove_unheld(path string) bool {
	cache_holds.Lock()
	defer cache_holds.Unlock()
	if _, held := cache_holds.running[path]; held {
		return false
	}
	return os.Remove(path) == nil
}

// cache_held_paths answers the paths transfers are running on, for a walk that
// must leave them alone.
func cache_held_paths() map[string]bool {
	cache_holds.Lock()
	defer cache_holds.Unlock()
	paths := make(map[string]bool, len(cache_holds.running))
	for path := range cache_holds.running {
		paths[path] = true
	}
	return paths
}

// mochi.cache.wait(name, held, seconds=0) -> bool or None: Wait for another
// transfer filling an entry. held is the partial that transfer appends to
// (mochi.cache.append), and name the entry it becomes. While held is being
// appended to, wait up to seconds, then answer: True when name is present, now
// or once that transfer finished; None when the transfer was still running as
// the time ran out, or when as many requests already wait on it as may; False
// when name is absent and nothing is filling it, so the caller fetches it
// itself. Two requests for one original - a second tab, a lightbox over its
// own thumbnail, a retry - then share one transfer rather than the second
// failing on the first one's hold.
func api_cache_wait(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, held sl.Value
	seconds := 0
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "held", &held, "seconds?", &seconds); err != nil {
		return nil, err
	}
	if seconds < 0 || seconds > cache_wait_maximum {
		return sl_error(fn, "seconds must be between 0 and %d", cache_wait_maximum)
	}
	path, err := cache_value(t, name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	partial, err := cache_value(t, held)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	// Waiting on a transfer, not computing: the transfer bound, as for the
	// transfer itself - but only with one to wait on, so a call cannot lift
	// its own time limit by waiting on nothing.
	if cache_held(partial) {
		starlark_transfer_set(t)
	}
	return cache_wait(starlark_context(t), path, partial, time.Duration(seconds)*time.Second), nil
}

// cache_wait is mochi.cache.wait on paths, ending early when ctx does.
func cache_wait(ctx context.Context, path string, partial string, limit time.Duration) sl.Value {
	if _, err := os.Stat(path); err == nil {
		return sl.True
	}
	cache_holds.Lock()
	running := cache_holds.running[partial]
	if running == nil {
		cache_holds.Unlock()
		return sl.False
	}
	if running.waiting >= cache_wait_waiters {
		cache_holds.Unlock()
		return sl.None
	}
	running.waiting++
	cache_holds.Unlock()
	defer func() {
		cache_holds.Lock()
		running.waiting--
		cache_holds.Unlock()
	}()

	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case <-running.done:
	case <-timer.C:
		if _, err := os.Stat(path); err == nil {
			return sl.True
		}
		return sl.None
	case <-ctx.Done():
		return sl.None
	}

	// The append has ended; its caller renames the finished partial a moment
	// later, or, cut short, leaves it for the next attempt to resume. Once the
	// partial is gone the caller has done with it: the entry is there, or it
	// was taken away again at once - an attachment's adoption moves it into the
	// app's own storage - or it was judged bad.
	settle := time.Now().Add(cache_wait_settle)
	for {
		if _, err := os.Stat(path); err == nil {
			return sl.True
		}
		if _, err := os.Stat(partial); os.IsNotExist(err) {
			// The rename may have landed between the two looks.
			if _, err := os.Stat(path); err == nil {
				return sl.True
			}
			return sl.False
		}
		if !time.Now().Before(settle) {
			return sl.False
		}
		select {
		case <-ctx.Done():
			return sl.None
		case <-time.After(cache_wait_poll):
		}
	}
}

// cache_append_refused marks an append that could not run, as distinct from a
// transfer that ran and stopped short.
var cache_append_refused = errors.New("append could not run")

// cache_append_file appends reader to path at offset, holding the entry for
// this process's other appends, and returns the entry's total size afterwards.
// started, when given, is called once the append is ready to copy. The write goes
// straight into the file - durability across a broken transfer is the point -
// and the copy uses a small buffer so every accepted chunk is on its way to
// disk before the next is read.
func cache_append_file(path string, reader io.Reader, offset int64, limit int64, destination string, restart bool, started func()) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return 0, fmt.Errorf("%w: %v", cache_append_refused, err)
	}
	// The bytes this append adds or removes are admitted once its hold is let
	// go and its entry is in place - deferred ahead of the release, so they run
	// after it. Admitted while held, an eviction they set off would leave the
	// entry out of the total it then sets.
	var delta int64
	defer func() {
		if delta != 0 {
			cache_admit(delta)
		}
	}()
	// Held for this append's length - its truncation, its bytes and its move
	// into place - so a second appender is refused and a request for the
	// finished entry can wait on this one (mochi.cache.wait).
	running, taken := cache_hold_take(path)
	if !taken {
		return 0, fmt.Errorf("%w: entry is being appended by another transfer", cache_append_refused)
	}
	defer cache_hold_release(path, running)

	// Continuing from an offset needs the entry that holds those bytes: one
	// gone since the caller looked is not made again, empty, by the refusal.
	flags := os.O_WRONLY | os.O_CREATE | os.O_APPEND
	if restart {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	} else if offset > 0 {
		flags = os.O_WRONLY | os.O_APPEND
	}
	var previous int64
	if information, err := os.Stat(path); err == nil {
		previous = information.Size()
	}
	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", cache_append_refused, err)
	}
	// The size is read from the file opened, not the name: the name may have
	// been deleted and made again in between, and bytes for offset X must not
	// land at the start of a new, empty file.
	opened, err := f.Stat()
	if err != nil {
		f.Close()
		return 0, fmt.Errorf("%w: %v", cache_append_refused, err)
	}
	if restart {
		delta -= previous
		offset = 0
	} else if opened.Size() != offset {
		f.Close()
		return 0, fmt.Errorf("%w: entry holds %d bytes, not the expected %d", cache_append_refused, opened.Size(), offset)
	}
	if started != nil {
		started()
	}
	written, copy_error := io.Copy(f, io.LimitReader(reader, limit))
	delta += written
	total := offset + written
	// The limit is one byte past the bytes that finish the entry, so an
	// overrun is seen. A source that sent them all and then answered the read
	// for that one more byte with an error rather than an end has still sent
	// the whole entry: the error says nothing about the bytes.
	finished := destination != "" && written == limit-1
	if copy_error != nil && !finished {
		f.Close()
		return total, copy_error
	}
	if destination == "" {
		return total, f.Close()
	}
	// The file is checked to be the one at the name, while it is still open,
	// before it is moved there or deleted: a name deleted from under the
	// append now holds some other file, or none.
	final, err := f.Stat()
	named, named_error := os.Stat(path)
	ours := err == nil && named_error == nil && os.SameFile(named, final) && final.Size() == total
	close_error := f.Close()
	if !ours {
		return 0, fmt.Errorf("entry was replaced while it was appended to")
	}
	unfinished := func(reason error) (int64, error) {
		if os.Remove(path) == nil {
			delta -= total
		}
		return 0, reason
	}
	if close_error != nil {
		return unfinished(close_error)
	}
	switch {
	case finished:
		if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
			return unfinished(err)
		}
		if err := os.Rename(path, destination); err != nil {
			return unfinished(err)
		}
	case written >= limit:
		if os.Remove(path) == nil {
			delta -= total
		}
	}
	return total, nil
}

// mochi.cache.size(name) -> int or None: A cache entry's size in bytes, or
// None when absent. Reads the metadata only - unlike path it does not mark
// the entry used, so measuring a partial does not keep it warm.
func api_cache_size(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <name: string>")
	}
	path, err := cache_value(t, args[0])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	information, err := os.Stat(path)
	if err != nil {
		return sl.None, nil
	}
	return sl.MakeInt64(information.Size()), nil
}

// mochi.cache.rename(from, to) -> None: Atomically rename a cache entry. How
// a completed partial becomes the served entry: the bytes never exist under
// the served name until they are whole.
func api_cache_rename(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <from: string>, <to: string>")
	}
	source, err := cache_value(t, args[0])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	destination, err := cache_value(t, args[1])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	var previous int64
	if information, err := os.Stat(destination); err == nil {
		previous = information.Size()
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return sl_error(fn, "unable to rename cache entry: %v", err)
	}
	if err := os.Rename(source, destination); err != nil {
		return sl_error(fn, "unable to rename cache entry: %v", err)
	}
	// The rename replaces whatever was at the destination; the source's bytes
	// were admitted when written, so only a replaced destination adjusts.
	cache_admit(-previous)
	return sl.None, nil
}

// cache_write_file writes reader to path atomically: a temporary in the same
// directory, renamed into place on success, removed on failure.
func cache_write_file(path string, reader io.Reader) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return 0, err
	}
	var previous int64
	if information, err := os.Stat(path); err == nil {
		previous = information.Size()
	}
	temporary := fmt.Sprintf("%s.%s.partial", path, uid())
	f, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(f, reader)
	f.Close()
	if err != nil {
		os.Remove(temporary)
		return 0, err
	}
	if err := os.Rename(temporary, path); err != nil {
		os.Remove(temporary)
		return 0, err
	}
	// The rename replaces whatever was at path, so only the difference is new.
	cache_admit(n - previous)
	return n, nil
}

// mochi.cache.read(name, maximum=0) -> bytes or None: Read a cache entry,
// marking it used. maximum refuses an entry larger than that many bytes; zero
// is unbounded. Checked against the entry itself - the caller's size came from
// the peer that filled it.
func api_cache_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	var maximum int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "maximum?", &maximum); err != nil {
		return sl_error(fn, "syntax: <name: string>, [maximum: integer]")
	}
	path, err := cache_file(t, name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	// One handle for both the measurement and the read: resolving the path twice
	// lets a rename land between them, so the bound would describe bytes other
	// than the ones returned.
	f, err := os.Open(path)
	if err != nil {
		return sl.None, nil
	}
	defer f.Close()

	if maximum <= 0 {
		maximum = cache_read_maximum
	}
	information, err := f.Stat()
	if err != nil {
		return sl.None, nil
	}
	if information.Size() > maximum {
		debug("mochi.cache.read refusing %q: %d bytes exceeds the limit of %d", name, information.Size(), maximum)
		return sl.None, nil
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return sl.None, nil
	}
	cache_used(path)
	return sl_encode(data), nil
}

// mochi.cache.path(name) -> string or None: Report whether an entry is present, marking it used; returns the name
func api_cache_path(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <name: string>")
	}
	path, err := cache_value(t, args[0])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	information, err := os.Stat(path)
	if err != nil || information.IsDir() {
		return sl.None, nil
	}
	cache_used(path)
	name, _ := sl.AsString(args[0])
	return sl.String(name), nil
}

// mochi.cache.age(name) -> int or None: Seconds since an entry was last
// written, or None if absent. Does NOT mark the entry used (unlike path/read),
// so it suits time-based bookkeeping such as retry backoff without keeping the
// entry alive against eviction.
func api_cache_age(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <name: string>")
	}
	path, err := cache_value(t, args[0])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	information, err := os.Stat(path)
	if err != nil || information.IsDir() {
		return sl.None, nil
	}
	return sl.MakeInt64(int64(time.Since(information.ModTime()).Seconds())), nil
}

// mochi.cache.copy(name, destination) -> integer or None: Copy a cache entry
// into the app's file storage, returning the bytes copied, or None on a miss so
// the caller can fill and retry. The bytes stream and are never held in memory.
// They are written beside the destination under a name of their own and moved
// into place, so the destination is never seen part-written; and a copy into a
// destination another copy is still writing waits for that copy and answers
// its size, so two requests adopting one attachment write it once.
func api_cache_copy(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, destination string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "destination", &destination); err != nil {
		return sl_error(fn, "syntax: <name: string>, <destination: string>")
	}
	if !valid(destination, "filepath") {
		return sl_error(fn, "invalid destination %q", destination)
	}

	path, err := cache_file(t, name)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	// The same user the source was resolved for. cache_file goes through
	// cache_base, which uses principal_storage and so returns the OWNER under
	// domain routing; reading the destination from t.Local("user") instead
	// copied the owner's cached bytes into a visitor's own file storage.
	user, err := principal_storage(t)
	if err != nil || user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	target := filepath.Join(base, filepath.FromSlash(destination))
	running, taken := cache_hold_take(target)
	if !taken {
		select {
		case <-running.done:
		case <-starlark_context(t).Done():
			return sl.None, nil
		}
		if !running.ok {
			return sl.None, nil
		}
		return sl.MakeInt64(running.size), nil
	}
	defer cache_hold_release(target, running)

	source, err := os.Open(path)
	if err != nil {
		return sl.None, nil
	}
	defer source.Close()

	information, err := source.Stat()
	if err != nil || information.IsDir() {
		return sl.None, nil
	}

	// Cache space is quota-exempt and file storage is not, so the copy is
	// charged even though the bytes already exist on this disk.
	remaining, err := storage_remaining(storage_request_of(t), user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if information.Size() > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}

	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	// The name it is written under looks like an orphaned attachment's, so a
	// copy a crash cuts short is reclaimed by the sweep of its app's files.
	temporary := filepath.Join(filepath.Dir(destination), uid()+"_copy")
	owner := storage_owner(root.Name())
	written, err := root_write_file(root, temporary, source)
	if err != nil {
		storage_remove(root, owner, temporary)
		return sl_error(fn, "unable to write file")
	}
	if err := storage_rename(root, owner, temporary, destination); err != nil {
		storage_remove(root, owner, temporary)
		return sl_error(fn, "unable to write file")
	}
	running.size, running.ok = written, true
	return sl.MakeInt64(written), nil
}

// mochi.cache.delete(name) -> bool: Remove a cache entry
func api_cache_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <name: string>")
	}
	path, err := cache_value(t, args[0])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	var size int64
	if information, err := os.Stat(path); err == nil {
		size = information.Size()
	}
	err = os.Remove(path)
	if err == nil {
		cache_admit(-size)
	}
	return sl.Bool(err == nil), nil
}

// cache_serve_file resolves a cache entry for a serving primitive
// (a.write.cache, e.write.cache): present entries are touched and returned,
// absent ones report a miss.
func cache_serve_file(t *sl.Thread, name string) (string, error) {
	path, err := cache_file(t, name)
	if err != nil {
		return "", err
	}
	information, err := os.Stat(path)
	if err != nil || information.IsDir() {
		return "", fmt.Errorf("cache miss")
	}
	cache_used(path)
	return path, nil
}

// cache_used marks an entry used, for eviction's least recently used order,
// in its access time. Its modification time stays the time it was written:
// mochi.cache.age reads that, and so do the ETag and Last-Modified a served
// entry carries, which a browser can revalidate against only while they hold
// still. Marked in the modification time, every read gave a served image new
// validators, so no cache could keep it, and a marker read inside its own
// backoff window restarted the window. The zero time leaves the modification
// time alone rather than writing back one read earlier, which a replacement
// landing in between would have taken.
func cache_used(path string) {
	os.Chtimes(path, time.Now(), time.Time{})
}

// cache_tee accumulates a copy of a stream being relayed to a client, so the
// first successful view fills the cache as a side effect. finish(true) after
// a complete body renames the copy into place; anything else discards it, so
// a curtailed or aborted relay never becomes a cache entry.
type cache_tee struct {
	file      *os.File
	temporary string
	target    string
}

func cache_tee_start(t *sl.Thread, name string) (*cache_tee, error) {
	target, err := cache_file(t, name)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return nil, err
	}
	temporary := fmt.Sprintf("%s.%s.partial", target, uid())
	f, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	return &cache_tee{file: f, temporary: temporary, target: target}, nil
}

func (ct *cache_tee) finish(complete bool) {
	ct.file.Close()
	if complete {
		// The entry it replaces, if any, leaves the budget as this one joins it.
		var previous int64
		if information, err := os.Stat(ct.target); err == nil {
			previous = information.Size()
		}
		if information, err := os.Stat(ct.temporary); err == nil && os.Rename(ct.temporary, ct.target) == nil {
			cache_admit(information.Size() - previous)
			return
		}
	}
	os.Remove(ct.temporary)
}

// cache_evict enforces the budget over the apps namespace: least recently
// used first, users over their fair share first. Runs from cache_cleanup.
func cache_evict() {
	if cache_budget < 0 {
		return
	}
	cache_total_set(cache_evict_to(cache_budget))
}

// cache_transient reports whether path is a .partial still inside
// cache_grace: a transfer in flight, which neither eviction nor measurement may
// touch.
func cache_transient(path string, modified time.Time) bool {
	if !strings.HasSuffix(path, ".partial") {
		return false
	}
	return time.Since(modified) < cache_grace
}

// cache_evict_to enforces a byte target over the apps namespace, least recently
// used first, and returns the total that remains. Separate from cache_evict so
// admission can clear headroom below the budget rather than exactly to it.
// Transfers in flight are neither counted nor candidates.
func cache_evict_to(target int64) int64 {
	type entry struct {
		path string
		size int64
		used time.Time
		user string
	}
	root := filepath.Join(cache_dir, "apps")
	var entries []entry
	var total int64
	users := map[string]int64{}
	// A partial an append is filling is a transfer in flight: neither counted
	// nor a candidate, since deleting it would break the transfer writing it.
	// Each removal judges it again, as an append may have begun since.
	held := cache_held_paths()
	filepath.Walk(root, func(path string, information os.FileInfo, err error) error {
		if err != nil || information.IsDir() || cache_transient(path, information.ModTime()) {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		if held[path] {
			return nil
		}
		u := strings.SplitN(filepath.ToSlash(relative), "/", 2)[0]
		entries = append(entries, entry{path, information.Size(), file_accessed(information), u})
		total += information.Size()
		users[u] += information.Size()
		return nil
	})
	if total <= target {
		return total
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].used.Before(entries[j].used) })
	share := target
	if len(users) > 0 {
		share = target / int64(len(users))
	}
	for _, e := range entries {
		if total <= target {
			return total
		}
		if users[e.user] > share && cache_remove_unheld(e.path) {
			total -= e.size
			users[e.user] -= e.size
		}
	}
	for _, e := range entries {
		if total <= target {
			return total
		}
		if cache_remove_unheld(e.path) {
			total -= e.size
		}
	}
	return total
}

// cache_total_set replaces the running total with a figure a walk just measured.
func cache_total_set(total int64) {
	cache_total_lock.Lock()
	cache_total = total
	cache_total_lock.Unlock()
}

// cache_measure walks the apps namespace and returns its byte total, to seed
// the running total on the first admission.
func cache_measure() int64 {
	var total int64
	held := cache_held_paths()
	filepath.Walk(filepath.Join(cache_dir, "apps"), func(path string, information os.FileInfo, err error) error {
		if err == nil && !information.IsDir() && !cache_transient(path, information.ModTime()) && !held[path] {
			total += information.Size()
		}
		return nil
	})
	return total
}

// cache_admit accounts for bytes a write added - or, negative, a delete
// released
// - and evicts when that puts the namespace over budget. Every caller admits
// AFTER the change has landed: the seeding walk already counts it, so the delta
// must not be added on top. Evicting rather than refusing keeps the promise
// that a miss re-obtains.
func cache_admit(delta int64) {
	if cache_budget < 0 {
		return
	}

	cache_total_lock.Lock()
	if cache_total < 0 {
		// Deliberately under the lock. It is one walk per process, and letting
		// concurrent admissions each walk instead would have every one of them
		// add its own delta on top of a measurement that already counted it.
		cache_total = cache_measure()
	} else {
		cache_total += delta
	}
	over := cache_total > cache_budget
	cache_total_lock.Unlock()

	if over {
		cache_total_set(cache_evict_to(cache_budget - cache_budget/cache_headroom_divisor))
	}
}
