// Mochi server: Filesystem utilities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"syscall"
)

var (
	match_leading_dots = regexp.MustCompile("^\\.+")
	match_spaces       = regexp.MustCompile("\\s+")
	match_unsafe       = regexp.MustCompile("[^0-9a-zA-Z-._]+")
)

func file_create(path string) {
	file_mkdir_for_file(path)
	f := must(os.Create(path))
	f.Close()
}

func file_delete(path string) {
	must(os.Remove(path))
}

func file_delete_all(path string) {
	must(os.RemoveAll(path))
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

func file_glob(match string) []string {
	return must(filepath.Glob(match))
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

func file_mkfifo(path string) {
	must(syscall.Mkfifo(path, 0600))
}

func file_move(old string, new string) {
	file_mkdir_for_file(new)
	must(os.Rename(old, new))
}

func file_name_safe(s string) string {
	s = match_spaces.ReplaceAllString(s, "_")
	s = match_unsafe.ReplaceAllString(s, "")
	s = match_leading_dots.ReplaceAllString(s, "")
	if len(s) > 254 {
		return s[:254]
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
	defer f.Close()
	if err != nil {
		warn("Unable to open file %q for writing: %v", path, err)
		return false
	}

	_, err = io.Copy(f, r)
	if err != nil {
		warn("Unable to write to file %q: %v", path, err)
		return false
	}

	return true
}
