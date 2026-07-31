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
	user, err := db_user_for_thread(t)
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

// mochi.cache.write(name, source) -> int: Store bytes or a stream as a cache entry, returns size
func api_cache_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <name: string>, <source: bytes or Stream>")
	}
	path, err := cache_value(t, args[0])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	var reader io.Reader
	switch v := args[1].(type) {
	case sl.String:
		reader = strings.NewReader(string(v))
	case sl.Bytes:
		reader = strings.NewReader(string(v))
	case *Stream:
		defer v.close_read()
		reader = io.LimitReader(v.raw_reader(), attachment_max_size_default)
	default:
		return sl_error(fn, "source must be bytes or a stream")
	}
	n, err := cache_write_file(path, reader)
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
	return n, nil
}

// mochi.cache.read(name) -> bytes or None: Read a cache entry, marking it used
func api_cache_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <name: string>")
	}
	path, err := cache_value(t, args[0])
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	data, err := os.ReadFile(path)
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

	user := t.Local("user").(*User)
	if user == nil {
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
	err = os.Remove(path)
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
	if total <= cache_budget {
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].modified.Before(entries[j].modified) })
	share := cache_budget
	if len(users) > 0 {
		share = cache_budget / int64(len(users))
	}
	for _, e := range entries {
		if total <= cache_budget {
			return
		}
		if users[e.user] > share && os.Remove(e.path) == nil {
			total -= e.size
			users[e.user] -= e.size
		}
	}
	for _, e := range entries {
		if total <= cache_budget {
			return
		}
		if os.Remove(e.path) == nil {
			total -= e.size
		}
	}
}
