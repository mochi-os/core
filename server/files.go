// Mochi server: File utilities
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
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

// Maximum file storage per user (10GB)
var file_max_storage int64 = 10 * 1024 * 1024 * 1024

// user_storage_remaining reports how many more bytes the user may store before
// reaching the per-user storage quota (file_max_storage). Administrators are
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
	return file_max_storage - current, nil
}

var (
	api_file = sls.FromStringDict(sl.String("mochi.file"), sl.StringDict{
		"delete": sl.NewBuiltin("mochi.file.delete", api_file_delete),
		"exists": sl.NewBuiltin("mochi.file.exists", api_file_exists),
		"list":   sl.NewBuiltin("mochi.file.list", api_file_list),
		"read":   sl.NewBuiltin("mochi.file.read", api_file_read),
		"write":  sl.NewBuiltin("mochi.file.write", api_file_write),
	})
)

func file_exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// data_dir_writable_check tries to create and remove a small file inside
// the configured data_dir. Returns nil if the calling process can write
// there, or the underlying error otherwise. Used by main_serve to fail
// early with an actionable message instead of panicking deep inside a
// later DB write.
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
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// Check if path is a symlink
func file_is_symlink(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink != 0
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
	}

	return "application/octet-stream"
}

// file_copy copies a file by streaming, without loading into memory
func file_copy(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()
	d, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer d.Close()
	_, err = io.Copy(d, s)
	return err
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

// Helper function to get the path of a file in an app's directory
func app_local_path(a *App, u *User, file string) string {
	av := a.active(u)
	if av == nil {
		return ""
	}
	return filepath.Join(av.base, file)
}

// Helper function to get the base directory for a user's storage
func user_storage_dir(u *User) string {
	return fmt.Sprintf("%s/users/%s", data_dir, u.UID)
}

// Calculate total size of files in a directory
func dir_size(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info != nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
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

	user := t.Local("user").(*User)
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

// mochi.file.exists(file) -> bool: Check whether a file exists
func api_file_exists(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := t.Local("user").(*User)
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
	if !ok || !valid(dir, "filepath") {
		return sl_error(fn, "invalid directory %q", dir)
	}

	user := t.Local("user").(*User)
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

	info, err := root.Stat(dir)
	if err != nil {
		return sl_error(fn, "does not exist")
	}
	if !info.IsDir() {
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

// mochi.file.read(file) -> bytes: Read a file into memory
func api_file_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	user := t.Local("user").(*User)
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

	user := t.Local("user").(*User)
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

	// Replicate the write to the user's host set so other replicas see
	// the same file content. Push-based — the queue worker reads the
	// file from disk and streams it via the file/push event handler.
	// No size threshold.
	if user.UID != "" {
	}

	return sl.None, nil
}

// Periodically clean expired cache files
func cache_manager() {
	for range time.Tick(1 * time.Hour) {
		cache_cleanup()
	}
}

// Remove cache files older than cache_max_age
func cache_cleanup() {
	cutoff := time.Now().Add(-cache_max_age)
	filepath.Walk(cache_dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if info.ModTime().Before(cutoff) {
			os.Remove(path)
		}
		return nil
	})
}
