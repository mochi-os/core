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
	"strings"
	"syscall"
	"unicode"
)

var (
	match_repeated_separators = regexp.MustCompile(`[-_ ]{2,}`)
	match_unsafe_chars        = regexp.MustCompile(`[\x00-\x1f\x7f/\\:*?"<>|]+`)
	reserved_names            = map[string]bool{"CON": true, "PRN": true, "AUX": true, "NUL": true, "COM1": true, "COM2": true, "COM3": true, "COM4": true, "COM5": true, "COM6": true, "COM7": true, "COM8": true, "COM9": true, "LPT1": true, "LPT2": true, "LPT3": true, "LPT4": true, "LPT5": true, "LPT6": true, "LPT7": true, "LPT8": true, "LPT9": true}
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
	if idx := strings.LastIndex(s, "."); idx > 0 {
		base = s[:idx]
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
