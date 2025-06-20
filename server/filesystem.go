// Comms server: Filesystem utilities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"syscall"
)

var (
	match_leading_dots = regexp.MustCompile("^\\.+")
	match_spaces       = regexp.MustCompile("\\s+")
	match_unsafe       = regexp.MustCompile("[^0-9a-zA-Z-._]+")
)

func file_create(path string) {
	file_mkdir(filepath.Dir(path))
	f, err := os.Create(path)
	check(err)
	f.Close()
}

func file_delete(path string) {
	os.Remove(path)
}

func files_dir(path string) []string {
	var files []string
	found, _ := os.ReadDir(path)
	for _, f := range found {
		files = append(files, f.Name())
	}
	return files
}

func file_exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}
	check(err)
	return false
}

func file_mkdir(path string) {
	err := os.MkdirAll(path, 0755)
	check(err)
}

func file_mkfifo(path string) {
	err := syscall.Mkfifo(path, 0600)
	check(err)
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
	data, err := os.ReadFile(path)
	check(err)
	return data
}

func file_size(path string) int64 {
	f, err := os.Stat(path)
	check(err)
	return f.Size()
}

func file_write(path string, data []byte) {
	err := os.WriteFile(path, data, 0644)
	check(err)
}
