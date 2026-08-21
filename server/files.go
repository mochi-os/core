// Mochi server: File utilities
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"archive/zip"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// object_maximum is the largest single object the platform stores, and so the
// largest it must transfer whole. The chain object_maximum <=
// stream_maximum_default <= the per-client relay byte budget must hold, so
// raising this means raising both others. TestStorageLimitsAgree pins it.
const object_maximum = 10 * 1024 * 1024 * 1024 // 10GB

// Maximum file storage per user (10GB). Equal to object_maximum, so a single
// object may fill a user's whole allowance - a video is one file, not many.
var file_maximum_storage int64 = 10 * 1024 * 1024 * 1024

// How long an unread cache entry survives before cache_cleanup removes it.
const cache_age_maximum = 7 * 24 * time.Hour

// user_storage_remaining reports how many more bytes the user may store before
// reaching the per-user storage quota (file_maximum_storage). Administrators are
// exempt from the quota and always have effectively unlimited space; returning
// early also spares them the full-tree dir_size walk on every write.
func user_storage_remaining(u *User) (int64, error) {
	if u != nil && u.administrator() {
		return math.MaxInt64, nil
	}
	current, err := dir_size(user_storage_dir(u))
	if err != nil {
		return 0, err
	}
	return file_maximum_storage - current, nil
}

var (
	api_file = sls.FromStringDict(sl.String("mochi.file"), sl.StringDict{
		"age":     sl.NewBuiltin("mochi.file.age", api_file_age),
		"clean":   sl.NewBuiltin("mochi.file.clean", api_file_clean),
		"copy":    sl.NewBuiltin("mochi.file.copy", api_file_copy),
		"maximum": sl.NewBuiltin("mochi.file.maximum", api_file_maximum),
		"delete":  sl.NewBuiltin("mochi.file.delete", api_file_delete),
		"exists":  sl.NewBuiltin("mochi.file.exists", api_file_exists),
		"list":    sl.NewBuiltin("mochi.file.list", api_file_list),
		"move":    sl.NewBuiltin("mochi.file.move", api_file_move),
		"read":    sl.NewBuiltin("mochi.file.read", api_file_read),
		"type":    sl.NewBuiltin("mochi.file.type", api_file_type),
		"write":   sl.NewBuiltin("mochi.file.write", api_file_write),
	})
)

func file_exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// data_dir_writable_check probes data_dir for writability so main_serve can
// fail early with an actionable message rather than deep inside a later write.
func data_dir_writable_check() error {
	if err := os.MkdirAll(data_dir, 0755); err != nil {
		return err
	}
	probe := filepath.Join(data_dir, ".mochi_write_check")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	f.Close()
	os.Remove(probe)
	return nil
}

func file_is_directory(path string) bool {
	information, err := os.Stat(path)
	if err != nil {
		return false
	}
	return information.IsDir()
}

func file_list(path string) ([]string, error) {
	found, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, f := range found {
		files = append(files, f.Name())
	}
	sort.Strings(files)
	return files, nil
}

func file_name_type(name string) string {
	switch path.Ext(name) {
	case ".css":
		return "text/css"
	case ".gif":
		return "image/gif"
	case ".html":
		return "text/html"
	case ".jpeg":
		return "image/jpeg"
	case ".jpg":
		return "image/jpeg"
	case ".js":
		return "text/javascript"
	case ".json":
		return "application/json"
	case ".pdf":
		return "application/pdf"
	case ".png":
		return "image/png"
	case ".svg":
		return "image/svg+xml"
	case ".txt":
		return "text/plain"
	case ".webp":
		return "image/webp"
	case ".xml":
		return "application/xml"

	// Media types decide inline playback versus download: content_type_inline
	// admits image/*, video/* and audio/*; octet-stream becomes an attachment.
	case ".avif":
		return "image/avif"
	case ".flac":
		return "audio/flac"
	case ".heic":
		return "image/heif"
	case ".m4a":
		return "audio/mp4"
	case ".m4v":
		return "video/mp4"
	case ".mov":
		return "video/quicktime"
	case ".mp3":
		return "audio/mpeg"
	case ".mp4":
		return "video/mp4"
	case ".oga":
		return "audio/ogg"
	case ".ogg":
		return "audio/ogg"
	case ".ogv":
		return "video/ogg"
	case ".opus":
		return "audio/opus"
	case ".wav":
		return "audio/wav"
	case ".webm":
		return "video/webm"
	}

	return "application/octet-stream"
}

func file_write(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func file_write_from_reader(path string, r io.Reader) bool {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		warn("Unable to create directory for %q: %v", path, err)
		return false
	}

	f, err := os.Create(path)
	if err != nil {
		warn("Unable to open file %q for writing: %v", path, err)
		return false
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	if err != nil {
		warn("Unable to write to file %q: %v", path, err)
		return false
	}

	return true
}

// Helper function to get the path of a file
func api_file_path(u *User, a *App, file string) string {
	return fmt.Sprintf("%s/users/%s/%s/files/%s", data_dir, u.UID, a.id, file)
}

// Helper function to get the base directory for a user's app files (for use with os.Root)
func api_file_base(u *User, a *App) string {
	return fmt.Sprintf("%s/users/%s/%s/files", data_dir, u.UID, a.id)
}

// app_asset_root opens the calling app's active version directory as an
// os.Root, through which every app-asset read is done. os.Root refuses to
// traverse a symlink at any depth; an os.Lstat on the joined path does not, and
// a dev app's directory is writable by whoever authors it.
func app_asset_root(a *App, u *User) (*os.Root, error) {
	av := a.active(u)
	if av == nil {
		return nil, fmt.Errorf("no active app version")
	}
	return os.OpenRoot(av.base)
}

// app_asset_path returns the on-disk path of an app asset, or "" when it does
// not resolve to a real file inside the app's directory (containment comes from
// opening through app_asset_root). Prefer app_asset_root and its handle; this
// is for the callers that must hand a path to something that opens it for them.
func app_asset_path(a *App, u *User, file string) string {
	root, err := app_asset_root(a, u)
	if err != nil {
		return ""
	}
	defer root.Close()
	f, err := root.Open(file)
	if err != nil {
		return ""
	}
	f.Close()
	return filepath.Join(a.active(u).base, file)
}

// Helper function to get the base directory for a user's storage
func user_storage_dir(u *User) string {
	return fmt.Sprintf("%s/users/%s", data_dir, u.UID)
}

// Calculate total size of files in a directory
func dir_size(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, information os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if information != nil && !information.IsDir() {
			size += information.Size()
		}
		return nil
	})
	return size, err
}

// mochi.file.type(name) -> string: The content type a file name's extension
// implies, application/octet-stream when it implies none. Same map as every
// serve path, so callers need not keep their own copy.
func api_file_type(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name); err != nil {
		return nil, err
	}
	return sl.String(file_name_type(name)), nil
}

// mochi.file.clean(name, maximum=255) -> string: Reduce a client-supplied name
// to one every platform accepts and the "filepath" validator passes. Strips
// invisible and forbidden characters, device stems and path components, and
// bounds the byte length; shares its implementation with the validator.
func api_file_clean(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var name string
	maximum := 255
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name, "maximum?", &maximum); err != nil {
		return nil, err
	}
	return sl.String(path_clean(name, maximum)), nil
}

// mochi.file.delete(file) -> None: Delete a file
func api_file_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl.None, nil // Directory doesn't exist, nothing to delete
	}
	defer root.Close()

	root.Remove(file)
	return sl.None, nil
}

// root_write_file streams source into name inside root, creating any parent
// directories, and returns the number of bytes written. The destination is
// opened through the root so a symlink left in the tree cannot redirect the
// write outside it, which a plain path join would allow.
func root_write_file(root *os.Root, name string, source io.Reader) (int64, error) {
	dir := filepath.Dir(name)
	if dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			return 0, err
		}
	}

	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return io.Copy(f, source)
}

// mochi.file.copy(source, destination) -> integer: Copy a file within the app's
// directory, returning the bytes copied. The bytes stream rather than being
// held in memory, so this is the way to duplicate an attachment or other large
// object.
func api_file_copy(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var source, destination string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "source", &source, "destination", &destination); err != nil {
		return sl_error(fn, "syntax: <source: string>, <destination: string>")
	}
	if !valid(source, "filepath") {
		return sl_error(fn, "invalid source %q", source)
	}
	if !valid(destination, "filepath") {
		return sl_error(fn, "invalid destination %q", destination)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	f, err := root.Open(source)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer f.Close()

	information, err := f.Stat()
	if err != nil || information.IsDir() {
		return sl_error(fn, "file not found")
	}

	// A copy adds its own size to the account, so it is checked like a write.
	remaining, err := user_storage_remaining(user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if information.Size() > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	written, err := root_write_file(root, destination, f)
	if err != nil {
		return sl_error(fn, "unable to write file")
	}

	return sl.MakeInt64(written), nil
}

// mochi.file.max() -> integer: The largest single object the platform stores, in
// bytes. Exposed so an app refuses an oversized upload against the same figure
// core enforces rather than keeping its own copy.
func api_file_maximum(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl.MakeInt64(object_maximum), nil
}

// mochi.file.age(file) -> integer or None: Seconds since a file was last
// modified, or None if it does not exist or is a directory. A listing alone
// cannot tell a settled file from one another request is still writing.
func api_file_age(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl.None, nil
	}
	defer root.Close()

	information, err := root.Stat(file)
	if err != nil || information.IsDir() {
		return sl.None, nil
	}

	return sl.MakeInt64(int64(time.Since(information.ModTime()).Seconds())), nil
}

// mochi.file.exists(file) -> bool: Check whether a file exists
func api_file_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl.False, nil // Directory doesn't exist
	}
	defer root.Close()

	_, err = root.Stat(file)
	if err != nil {
		return sl.False, nil
	}
	return sl.True, nil
}

// mochi.file.list(subdirectory) -> list: List files in a subdirectory
func api_file_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <subdirectory: string>")
	}

	dir, ok := sl.AsString(args[0])
	// "" and "." name the app's file root - a valid listing target that the
	// filepath validator (which requires a leading alphanumeric) would reject.
	if dir == "" || dir == "." {
		dir = "."
	} else if !ok || !valid(dir, "filepath") {
		return sl_error(fn, "invalid directory %q", dir)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "does not exist")
	}
	defer root.Close()

	information, err := root.Stat(dir)
	if err != nil {
		return sl_error(fn, "does not exist")
	}
	if !information.IsDir() {
		return sl_error(fn, "not a directory")
	}

	// Open the directory within the root
	d, err := root.OpenFile(dir, os.O_RDONLY, 0)
	if err != nil {
		return sl_error(fn, "does not exist")
	}
	defer d.Close()

	entries, err := d.ReadDir(-1)
	if err != nil {
		return sl_error(fn, "unable to list directory")
	}

	var files []string
	for _, e := range entries {
		files = append(files, e.Name())
	}
	sort.Strings(files)

	return sl_encode(files), nil
}

// mochi.file.read(file, maximum=0) -> bytes or None: Read a file into memory.
// maximum refuses a larger file, answering None so the caller can fall back to
// a streaming path; zero, the default, is unbounded. The bound is checked
// against the file itself, never a size the caller supplies - it may come from
// a peer.
func api_file_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file string
	var maximum int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "maximum?", &maximum); err != nil {
		return sl_error(fn, "syntax: <file: string>, [maximum: integer]")
	}
	if !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer root.Close()

	f, err := root.Open(file)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer f.Close()

	if maximum > 0 {
		information, err := f.Stat()
		if err != nil {
			return sl_error(fn, "unable to read file")
		}
		if information.Size() > maximum {
			debug("mochi.file.read refusing %q: %d bytes exceeds the caller's limit of %d", file, information.Size(), maximum)
			return sl.None, nil
		}
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return sl_error(fn, "unable to read file")
	}

	return sl_encode(data), nil
}

// mochi.file.write(file, data) -> None: Write a file from memory
func api_file_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 2 {
		return sl_error(fn, "syntax: <file: string>, <data: array of bytes>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	var data string
	switch v := args[1].(type) {
	case sl.String:
		data = string(v)
	case sl.Bytes:
		data = string(v)
	default:
		return sl_error(fn, "invalid file data")
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	// Check storage limit (10GB per user across all apps; admins exempt)
	remaining, err := user_storage_remaining(user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if int64(len(data)) > remaining {
		return sl_error(fn, "storage limit exceeded")
	}

	// Ensure base directory exists before opening root
	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		return sl_error(fn, "unable to create files directory: %v", err)
	}

	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	// Create parent directories within the root if needed
	dir := filepath.Dir(file)
	if dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			return sl_error(fn, "unable to create directory")
		}
	}

	f, err := root.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return sl_error(fn, "unable to write file")
	}

	// WriteString writes directly from the string's backing bytes — no
	// []byte() conversion, no duplicate allocation. Important for large
	// writes: a 5 GiB string via Write([]byte(data)) needs 10 GiB heap
	// during the write; WriteString stays at 5 GiB.
	_, err = f.WriteString(data)
	f.Close()
	if err != nil {
		return sl_error(fn, "unable to write file")
	}

	return sl.None, nil
}

// mochi.file.move(from, to) -> None: Rename a file within the app's file storage.
// Both paths are relative to the app's files directory and the destination's
// parents are created. No bytes are copied and no quota is charged.
func api_file_move(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var from, to string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "from", &from, "to", &to); err != nil {
		return sl_error(fn, "syntax: move(from, to)")
	}
	if !valid(from, "filepath") {
		return sl_error(fn, "invalid source %q", from)
	}
	if !valid(to, "filepath") {
		return sl_error(fn, "invalid destination %q", to)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	if dir := filepath.Dir(to); dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			return sl_error(fn, "unable to create directory")
		}
	}

	if err := root.Rename(from, to); err != nil {
		return sl_error(fn, "unable to move file: %v", err)
	}
	return sl.None, nil
}

// temporary_configure points the process's temporary directory at
// cache_dir/tmp. Go spools large multipart uploads to os.TempDir(), which on a
// systemd host is usually a tmpfs - spending RAM rather than relieving it. Set
// here so it follows directories.cache; failure is not fatal and falls back to
// the system default.
func temporary_configure() {
	temporary := filepath.Join(cache_dir, "tmp")
	if err := os.MkdirAll(temporary, 0o700); err != nil {
		warn("Unable to create temporary directory %s, falling back to the system default: %v", temporary, err)
		return
	}
	// Tighten explicitly: MkdirAll leaves an existing directory's mode alone,
	// and this one predates this function — app staging created it 0755. The
	// spooled parts of an upload land here, so other local users have no
	// business listing it.
	if err := os.Chmod(temporary, 0o700); err != nil {
		warn("Unable to restrict permissions on %s: %v", temporary, err)
	}
	if err := os.Setenv("TMPDIR", temporary); err != nil {
		warn("Unable to set TMPDIR to %s: %v", temporary, err)
	}
}

// Periodically clean expired cache files
func cache_manager() {
	for range time.Tick(1 * time.Hour) {
		cache_cleanup()
	}
}

// Remove cache files older than cache_age_maximum, then enforce the byte budget
// over the apps namespace (cache_evict).
func cache_cleanup() {
	cutoff := time.Now().Add(-cache_age_maximum)
	filepath.Walk(cache_dir, func(path string, information os.FileInfo, err error) error {
		if err != nil || information.IsDir() {
			return nil
		}
		if information.ModTime().Before(cutoff) {
			os.Remove(path)
		}
		return nil
	})
	cache_evict()
}

// ---------------------------------------------------------------------------
// Archives
// ---------------------------------------------------------------------------
//
// A zip container an app builds and reads without the entries passing through
// Starlark, so an export streams attachments instead of holding them all
// base64-expanded in memory. Entry names inside an archive are never used as
// paths - write() and extract() both take their paths from the caller - so
// zip-slip does not arise.

// archive_read_maximum bounds mochi.archive.read, which exists for a manifest
// rather than for content. Anything larger is what extract() is for.
const archive_read_maximum = 64 << 20

var api_archive = sls.FromStringDict(sl.String("mochi.archive"), sl.StringDict{
	"write":   sl.NewBuiltin("mochi.archive.write", api_archive_write),
	"list":    sl.NewBuiltin("mochi.archive.list", api_archive_list),
	"read":    sl.NewBuiltin("mochi.archive.read", api_archive_read),
	"extract": sl.NewBuiltin("mochi.archive.extract", api_archive_extract),
})

// mochi.archive.write(destination, entries) -> integer: Build a zip archive at
// destination and return its size. entries is a list of dicts, each with a
// "name" (the path inside the archive) and one of "file" (an app file), "cache"
// (a cache entry, where a copy pulled from a peer lives) or "data" (a string,
// for a manifest). File and cache entries stream in without being read into
// memory; a source that has gone missing is skipped rather than fatal.
func api_archive_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var destination string
	var entries sl.Value
	var cache bool
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "destination", &destination, "entries", &entries, "cache?", &cache); err != nil {
		return sl_error(fn, "syntax: <destination: string>, <entries: list>, [cache: bool]")
	}
	if !valid(destination, "filepath") {
		return sl_error(fn, "invalid destination %q", destination)
	}

	list, ok := sl_decode(entries).([]any)
	if !ok {
		return sl_error(fn, "entries must be a list")
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
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

	// The archive is charged to the account like any other write, and its
	// contents are already stored, so the budget is checked before building
	// rather than discovering it part-way through.
	remaining, err := user_storage_remaining(user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}

	// Cache rather than file storage: an archive built for immediate download is
	// re-obtainable, quota-exempt and evictable, and it resolves for the calling
	// user - a.write.file would read as the entity owner.
	var out io.WriteCloser
	var cache_path string
	var cache_previous int64
	if cache {
		path, err := cache_file(t, destination)
		if err != nil {
			return sl_error(fn, "invalid destination %q", destination)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return sl_error(fn, "unable to create cache directory: %v", err)
		}
		if information, err := os.Stat(path); err == nil {
			cache_previous = information.Size()
		}
		out, err = os.Create(path)
		if err != nil {
			return sl_error(fn, "unable to write archive")
		}
		cache_path = path
		// The cache is exempt from the per-user quota, so nothing bounds this
		// archive's size - which is why the bytes are admitted below rather
		// than left for the hourly sweep to notice.
		remaining = math.MaxInt64
	} else {
		dir := filepath.Dir(destination)
		if dir != "." && dir != "" {
			if err := root_mkdir_all(root, dir); err != nil {
				return sl_error(fn, "unable to create directory")
			}
		}
		out, err = root.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return sl_error(fn, "unable to write archive")
		}
	}
	defer out.Close()

	// Account for a cached archive however this returns: a partial archive still
	// occupies cache, and a running total that never learns of those bytes
	// under-evicts. Measured from the file, since counter.written counts bytes fed
	// in rather than the compressed result.
	if cache_path != "" {
		defer func() {
			if information, err := os.Stat(cache_path); err == nil {
				cache_admit(information.Size() - cache_previous)
			}
		}()
	}

	counter := &archive_counter{limit: remaining}
	writer := zip.NewWriter(io.MultiWriter(out, counter))

	for _, item := range list {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := entry["name"].(string)
		if name == "" {
			return sl_error(fn, "archive entry has no name")
		}

		if source, ok := entry["file"].(string); ok && source != "" {
			if !valid(source, "filepath") {
				return sl_error(fn, "invalid file %q", source)
			}
			// Opened before the entry is created: a missing file is skipped rather than
			// fatal, and creating the entry first would leave an empty one that an
			// import reads back as a zero-byte attachment.
			f, err := root.Open(source)
			if err != nil {
				continue
			}
			w, err := writer.Create(name)
			if err != nil {
				f.Close()
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
			_, err = io.Copy(w, f)
			f.Close()
			if err != nil {
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
		} else if source, ok := entry["cache"].(string); ok && source != "" {
			// A copy pulled from a peer lives in cache rather than file
			// storage, so an export of someone else's attachment reads from
			// there. Cache space is evictable, and an entry that has gone is
			// skipped exactly as a missing file is.
			path, err := cache_file(t, source)
			if err != nil {
				return sl_error(fn, "invalid cache entry %q", source)
			}
			f, err := os.Open(path)
			if err != nil {
				continue
			}
			w, err := writer.Create(name)
			if err != nil {
				f.Close()
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
			_, err = io.Copy(w, f)
			f.Close()
			if err != nil {
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
		} else if data, ok := entry["data"].(string); ok {
			w, err := writer.Create(name)
			if err != nil {
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
			if _, err := io.WriteString(w, data); err != nil {
				return sl_error(fn, "unable to add %q: %v", name, err)
			}
		}

		if counter.exceeded {
			writer.Close()
			root.Remove(destination)
			return sl_error(fn, "storage limit exceeded")
		}
	}

	if err := writer.Close(); err != nil {
		return sl_error(fn, "unable to finish archive: %v", err)
	}
	if counter.exceeded {
		root.Remove(destination)
		return sl_error(fn, "storage limit exceeded")
	}

	return sl.MakeInt64(counter.written), nil
}

// archive_counter tallies bytes as an archive is built, so a quota overrun is
// caught while writing rather than after a multi-gigabyte file has landed.
type archive_counter struct {
	written  int64
	limit    int64
	exceeded bool
}

func (c *archive_counter) Write(p []byte) (int, error) {
	c.written += int64(len(p))
	if c.written > c.limit {
		c.exceeded = true
	}
	return len(p), nil
}

// archive_open opens an archive inside the app's file storage.
func archive_open(t *sl.Thread, fn *sl.Builtin, file string) (*zip.ReadCloser, error) {
	if !valid(file, "filepath") {
		return nil, fmt.Errorf("invalid file %q", file)
	}
	user := principal_caller(t)
	if user == nil {
		return nil, fmt.Errorf("no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return nil, fmt.Errorf("no app")
	}
	root, err := os.OpenRoot(api_file_base(user, app))
	if err != nil {
		return nil, fmt.Errorf("file not found")
	}
	defer root.Close()

	// Resolve through the root first so a symlink cannot point the reader at a
	// file outside the app's directory, then hand the real path to zip.
	f, err := root.Open(file)
	if err != nil {
		return nil, fmt.Errorf("file not found")
	}
	information, err := f.Stat()
	f.Close()
	if err != nil || information.IsDir() {
		return nil, fmt.Errorf("file not found")
	}

	return zip.OpenReader(filepath.Join(api_file_base(user, app), file))
}

// mochi.archive.list(file) -> list or None: The archive's entries, each a dict
// of name and size. None if the file is not a readable archive.
func api_archive_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file); err != nil {
		return sl_error(fn, "syntax: <file: string>")
	}
	reader, err := archive_open(t, fn, file)
	if err != nil {
		return sl.None, nil
	}
	defer reader.Close()

	entries := []map[string]any{}
	for _, f := range reader.File {
		if f.FileInfo().IsDir() {
			continue
		}
		entries = append(entries, map[string]any{
			"name": f.Name,
			"size": int64(f.UncompressedSize64),
		})
	}
	return sl_encode(entries), nil
}

// mochi.archive.read(file, name) -> bytes or None: One entry's contents. For a
// manifest; bulk content belongs in extract(), and an entry beyond
// archive_read_maximum is refused rather than held in memory.
func api_archive_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file, name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "name", &name); err != nil {
		return sl_error(fn, "syntax: <file: string>, <name: string>")
	}
	reader, err := archive_open(t, fn, file)
	if err != nil {
		return sl.None, nil
	}
	defer reader.Close()

	for _, f := range reader.File {
		if f.Name != name || f.FileInfo().IsDir() {
			continue
		}
		if f.UncompressedSize64 > archive_read_maximum {
			return sl_error(fn, "entry %q is too large to read; use extract", name)
		}
		rc, err := f.Open()
		if err != nil {
			return sl.None, nil
		}
		defer rc.Close()
		data, err := io.ReadAll(io.LimitReader(rc, archive_read_maximum+1))
		if err != nil || int64(len(data)) > archive_read_maximum {
			return sl.None, nil
		}
		return sl_encode(data), nil
	}
	return sl.None, nil
}

// mochi.archive.extract(file, name, destination) -> integer or None: Stream one
// entry out to an app file, returning the bytes written, or None if the entry
// is absent. The destination is the caller's, never the archive's own entry
// name, so a hostile name in a received archive addresses nothing.
func api_archive_extract(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var file, name, destination string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "name", &name, "destination", &destination); err != nil {
		return sl_error(fn, "syntax: <file: string>, <name: string>, <destination: string>")
	}
	if !valid(destination, "filepath") {
		return sl_error(fn, "invalid destination %q", destination)
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	reader, err := archive_open(t, fn, file)
	if err != nil {
		return sl.None, nil
	}
	defer reader.Close()

	remaining, err := user_storage_remaining(user)
	if err != nil {
		return sl_error(fn, "unable to measure storage: %v", err)
	}

	for _, f := range reader.File {
		if f.Name != name || f.FileInfo().IsDir() {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return sl.None, nil
		}
		defer rc.Close()

		root, err := os.OpenRoot(api_file_base(user, app))
		if err != nil {
			return sl_error(fn, "unable to access files directory")
		}
		defer root.Close()

		// Bounded by the quota rather than by the entry's declared size: a zip
		// header can claim any size it likes, and the decompressed stream is
		// what actually lands on disk.
		written, err := root_write_file(root, destination, io.LimitReader(rc, remaining+1))
		if err != nil {
			return sl_error(fn, "unable to write file")
		}
		if written > remaining {
			root.Remove(destination)
			return sl_error(fn, "storage limit exceeded")
		}
		return sl.MakeInt64(written), nil
	}
	return sl.None, nil
}
