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
// resolution matches mochi.db and mochi.file: the requesting user when there
// is one, otherwise the entity owner.
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

// mochi.cache.write(name, source, maximum=0) -> int: Store bytes or a stream as a cache entry, returns size
func api_cache_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, source sl.Value
	maximum := 0
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
	if maximum > 0 && int64(maximum) < limit {
		limit = int64(maximum) + 1
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
		// the compute budget on an ordinary link.
		starlark_transfer_set(t)
		reader = v.raw_reader()
	default:
		return sl_error(fn, "source must be bytes or a stream")
	}
	n, err := cache_write_file(path, io.LimitReader(reader, limit))
	if err != nil {
		return sl_error(fn, "unable to write cache entry: %v", err)
	}
	return sl.MakeInt64(n), nil
}

// mochi.cache.append(name, source, offset, maximum=0) -> int: Append a
// stream's bytes to a cache entry at offset, returning the entry's new total
// size. Built for resumable transfers: unlike write, which commits whole or
// not at all, append writes straight into the entry, so bytes that arrived
// before a broken or abandoned transfer SURVIVE for the next attempt to
// continue from. The entry must therefore never be a name anything serves -
// callers keep partials under their own prefix and rename into the served
// name once complete.
//
// offset is the caller's belief about the entry's current size, and the
// append refuses when the entry disagrees - that is what turns two callers
// racing the same partial into one clean loser instead of an interleaved
// corruption. A sidecar lock file (O_EXCL, stale after the transfer bound)
// holds the entry meanwhile. maximum bounds the bytes accepted THIS call,
// maximum+1 as everywhere, so an overrun is detectable.
func api_cache_append(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name, source sl.Value
	var offset int64
	maximum := 0
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "source", &source, "offset", &offset, "maximum?", &maximum); err != nil {
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
	limit := int64(object_maximum)
	if maximum > 0 && int64(maximum) < limit {
		limit = int64(maximum) + 1
	}
	// A bulk transfer from a peer, not computation: the transfer bound, as
	// for a stream source to write.
	starlark_transfer_set(t)
	total, err := cache_append_file(path, stream.raw_reader(), offset, limit)
	if err != nil {
		return sl_error(fn, "unable to append cache entry: %v", err)
	}
	return sl.MakeInt64(total), nil
}

// cache_append_file appends reader to path at offset, holding the entry's
// sidecar lock, and returns the entry's total size afterwards. The write goes
// straight into the file - durability across a broken transfer is the point -
// and the copy uses a small buffer so every accepted chunk is on its way to
// disk before the next is read.
func cache_append_file(path string, reader io.Reader, offset int64, limit int64) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return 0, err
	}
	lock := path + ".lock"
	handle, err := os.OpenFile(lock, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		// A crashed appender leaves its lock behind; one older than the
		// transfer bound cannot belong to a live transfer.
		if information, stat := os.Stat(lock); stat == nil && time.Since(information.ModTime()) > starlark_file_timeout {
			os.Remove(lock)
			handle, err = os.OpenFile(lock, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		}
		if err != nil {
			return 0, fmt.Errorf("entry is being appended by another transfer")
		}
	}
	handle.Close()
	defer os.Remove(lock)

	var current int64
	if information, err := os.Stat(path); err == nil {
		current = information.Size()
	}
	if current != offset {
		return 0, fmt.Errorf("entry holds %d bytes, not the expected %d", current, offset)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return 0, err
	}
	written, copy_err := io.Copy(f, io.LimitReader(reader, limit))
	close_err := f.Close()
	cache_admit(written)
	if copy_err != nil {
		return offset + written, copy_err
	}
	if close_err != nil {
		return offset + written, close_err
	}
	return offset + written, nil
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

	if maximum > 0 {
		information, err := f.Stat()
		if err != nil {
			return sl.None, nil
		}
		if information.Size() > maximum {
			debug("mochi.cache.read refusing %q: %d bytes exceeds the caller's limit of %d", name, information.Size(), maximum)
			return sl.None, nil
		}
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return sl.None, nil
	}
	moment := time.Now()
	os.Chtimes(path, moment, moment)
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
	moment := time.Now()
	os.Chtimes(path, moment, moment)
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
	remaining, err := user_storage_remaining(user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if information.Size() > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}

	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	written, err := root_write_file(root, destination, source)
	if err != nil {
		return sl_error(fn, "unable to write file")
	}

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
	moment := time.Now()
	os.Chtimes(path, moment, moment)
	return path, nil
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
	if complete && os.Rename(ct.temporary, ct.target) == nil {
		return
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

// cache_evict_to enforces a byte target over the apps namespace, least recently
// used first, and returns the total that remains. Separate from cache_evict so
// admission can clear headroom below the budget rather than exactly to it.
func cache_evict_to(target int64) int64 {
	type entry struct {
		path     string
		size     int64
		modified time.Time
		user     string
	}
	root := filepath.Join(cache_dir, "apps")
	var entries []entry
	var total int64
	users := map[string]int64{}
	filepath.Walk(root, func(path string, information os.FileInfo, err error) error {
		if err != nil || information.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		u := strings.SplitN(filepath.ToSlash(relative), "/", 2)[0]
		entries = append(entries, entry{path, information.Size(), information.ModTime(), u})
		total += information.Size()
		users[u] += information.Size()
		return nil
	})
	if total <= target {
		return total
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].modified.Before(entries[j].modified) })
	share := target
	if len(users) > 0 {
		share = target / int64(len(users))
	}
	for _, e := range entries {
		if total <= target {
			return total
		}
		if users[e.user] > share && os.Remove(e.path) == nil {
			total -= e.size
			users[e.user] -= e.size
		}
	}
	for _, e := range entries {
		if total <= target {
			return total
		}
		if os.Remove(e.path) == nil {
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
	filepath.Walk(filepath.Join(cache_dir, "apps"), func(path string, information os.FileInfo, err error) error {
		if err == nil && !information.IsDir() {
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
