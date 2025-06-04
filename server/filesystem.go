// Comms server: Filesystem utilities
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func file_create(path string) {
	file_mkdir(filepath.Dir(path))
	f, err := os.Create(data_dir + "/" + path)
	check(err)
	f.Close()
}

func file_delete(path string) {
	os.Remove(data_dir + "/" + path)
}

func files_dir(path string) []string {
	var files []string
	found, _ := os.ReadDir(data_dir + "/" + path)
	for _, f := range found {
		files = append(files, f.Name())
	}
	return files
}

func file_exists(path string) bool {
	_, err := os.Stat(data_dir + "/" + path)
	if err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}
	check(err)
	return false
}

func file_mkdir(path string) {
	err := os.MkdirAll(data_dir+"/"+path, 0755)
	check(err)
}

func file_mkfifo(path string) {
	err := syscall.Mkfifo(data_dir+"/"+path, 0600)
	check(err)
}

func file_read(path string) []byte {
	data, err := os.ReadFile(data_dir + "/" + path)
	check(err)
	return data
}

func file_write(path string, data []byte) {
	err := os.WriteFile(data_dir+"/"+path, data, 0644)
	check(err)
}
