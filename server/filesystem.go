// Mochi server: Filesystem utilities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// Maximum file storage per app per user (1GB)
var file_max_storage int64 = 1024 * 1024 * 1024

var (
	match_repeated_separators = regexp.MustCompile(`[-_ ]{2,}`)
	match_unsafe_chars        = regexp.MustCompile(`[\x00-\x1f\x7f/\\:*?"<>|]+`)
	reserved_names            = map[string]bool{"CON": true, "PRN": true, "AUX": true, "NUL": true, "COM1": true, "COM2": true, "COM3": true, "COM4": true, "COM5": true, "COM6": true, "COM7": true, "COM8": true, "COM9": true, "LPT1": true, "LPT2": true, "LPT3": true, "LPT4": true, "LPT5": true, "LPT6": true, "LPT7": true, "LPT8": true, "LPT9": true}

	api_file = sls.FromStringDict(sl.String("mochi.file"), sl.StringDict{
		"delete": sl.NewBuiltin("mochi.file.delete", api_file_delete),
		"exists": sl.NewBuiltin("mochi.file.exists", api_file_exists),
		"list":   sl.NewBuiltin("mochi.file.list", api_file_list),
		"read":   sl.NewBuiltin("mochi.file.read", api_file_read),
		"write":  sl.NewBuiltin("mochi.file.write", api_file_write),
	})
)

func file_create(path string) {
	file_mkdir_for_file(path)
	f := must(os.Create(path))
	f.Close()
}

func file_delete(path string) {
	os.Remove(path)
}

func file_delete_all(path string) {
	os.RemoveAll(path)
}

func file_exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}
	panic(err)
	return false
}

func file_is_directory(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func file_list(path string) []string {
	var files []string
	found, _ := os.ReadDir(path)
	for _, f := range found {
		files = append(files, f.Name())
	}
	sort.Strings(files)
	return files
}

func file_mkdir(path string) {
	must(os.MkdirAll(path, 0755))
}

func file_mkdir_for_file(path string) {
	file_mkdir(filepath.Dir(path))
}

func file_move(old string, new string) {
	file_mkdir_for_file(new)
	must(os.Rename(old, new))
}

func file_name_safe(s string) string {
	s = match_unsafe_chars.ReplaceAllString(s, "")

	s = strings.TrimFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || r == '.'
	})

	s = match_repeated_separators.ReplaceAllString(s, "_")

	s = strings.TrimLeft(s, ".")

	if s == "" {
		return "unnamed"
	}

	base := s
	i := strings.LastIndex(s, ".")
	if i > 0 {
		base = s[:i]
	}
	if reserved_names[strings.ToUpper(base)] {
		s = "_" + s
	}

	if len(s) > 240 {
		ext := ""
		i := strings.LastIndex(s, ".")
		if i > 0 && len(s)-i <= 10 {
			ext = s[i:]
			s = s[:i]
		}
		if len(s) > 240-len(ext) {
			s = s[:240-len(ext)]
		}
		s = strings.TrimRight(s, " ._-") + ext
	}

	return s
}

func file_name_type(name string) string {
	switch path.Ext(name) {
	case ".gif":
		return "image/gif"
	case ".jpeg":
		return "image/jpeg"
	case ".jpg":
		return "image/jpeg"
	case ".pdf":
		return "application/pdf"
	case ".png":
		return "image/png"
	case ".txt":
		return "text/plain"
	case ".webp":
		return "image/webp"
	}

	return "application/octet-stream"
}

func file_read(path string) []byte {
	return must(os.ReadFile(path))
}

func file_size(path string) int64 {
	f := must(os.Stat(path))
	return f.Size()
}

func file_write(path string, data []byte) {
	file_mkdir_for_file(path)
	must(os.WriteFile(path, data, 0644))
}

func file_write_from_reader(path string, r io.Reader) bool {
	file_mkdir_for_file(path)

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
	return fmt.Sprintf("%s/users/%d/%s/files/%s", data_dir, u.ID, a.id, file)
}

// Helper function to get the base files directory for an app
func api_file_dir(u *User, a *App) string {
	return fmt.Sprintf("%s/users/%d/%s/files", data_dir, u.ID, a.id)
}

// Calculate total size of files in a directory
func dir_size(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
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

	file_delete(api_file_path(user, app, file))
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

	if file_exists(api_file_path(user, app, file)) {
		return sl.True, nil
	} else {
		return sl.False, nil
	}
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

	path := api_file_path(user, app, dir)
	if !file_exists(path) {
		return sl_error(fn, "does not exist")
	}
	if !file_is_directory(path) {
		return sl_error(fn, "not a directory")
	}

	return sl_encode(file_list(path)), nil
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

	return sl_encode(file_read(api_file_path(user, app, file))), nil
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

	data, ok := sl.AsString(args[1])
	if !ok {
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

	// Check storage limit
	current := dir_size(api_file_dir(user, app))
	if current+int64(len(data)) > file_max_storage {
		return sl_error(fn, "storage limit exceeded")
	}

	file_write(api_file_path(user, app, file), []byte(data))

	return sl.None, nil
}
