// Mochi server: Cache
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Server-managed byte cache for apps. Entries hold anything re-obtainable -
// pulled remote copies, generated image variants - and are exempt from the
// per-user storage quota in exchange for unconditional evictability: the
// budget sweep may remove any entry at any time, and a miss re-obtains.
// Originals never belong here.
//
// Entries live under cache_dir/apps/<user>/<app>/<name>. A file's
// modification time is its last access; path() and read() touch it, and
// eviction removes least-recently-used entries beyond the budget, users over
// their fair share first. Writes are atomic - a temporary in the same
// directory renamed into place on completion - so concurrent same-name fills
// are wasteful but safe.

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
// does not walk the tree on every write. Negative means not yet measured; the
// first admission seeds it with one walk, and every walk that follows (the
// hourly sweep, or an admission eviction) replaces it with an exact figure, so
// drift from a path that forgets to account cannot accumulate past an hour.
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
	// The global cap holds regardless; a positive maximum tightens it to what
	// the caller expects. The bound is maximum+1, not maximum: a source that
	// overruns must be distinguishable from one that fits exactly, so one byte
	// beyond the bound survives for the caller's size check to find. Cutting
	// at maximum would truncate a peer's oversized transfer to precisely the
	// declared size, and a completeness check downstream would then pass a
	// file whose tail was silently discarded.
	limit := int64(attachment_max_size_default)
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
// marking it used. maximum refuses an entry larger than that many bytes; zero,
// the default, is unbounded. As with mochi.file.read the check is against the
// entry itself, because a caller's idea of the size may have come from the peer
// that filled it.
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
	// One handle for both the measurement and the read. Resolving the path
	// twice would let a cache fill land between them - writes here are a
	// temporary renamed into place, so the name can point at a different, larger
	// file than the one just measured, and the bound would describe bytes that
	// are no longer the bytes being returned.
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
// the caller can fill and retry. The bytes stream across and are never held in
// memory. This is how a re-obtainable copy becomes a kept one: an attachment
// pulled from a peer lives in cache, and forwarding it makes the bytes ours.
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

// cache_admit accounts for bytes a write just added - or, negative, a delete
// just released - and, when that puts the namespace over budget, evicts before
// returning.
//
// Every caller admits *after* the change has landed on disk. That is what makes
// the first-call seeding correct: the walk it does already counts the change
// being reported, so the delta must not then be added on top of it.
//
// The cache is exempt from the per-user storage quota in exchange for being
// unconditionally evictable, and that bargain was honoured only by an hourly
// sweep: between two sweeps an app could write until the disk filled, which
// cache_dir shares with every user on the host. Evicting rather than refusing
// keeps the other half of the bargain - an app is promised that a miss
// re-obtains, so a write that fails is a contract this code does not have.
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
