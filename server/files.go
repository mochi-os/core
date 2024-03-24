// Comms server: Files
// Copyright Alistair Cunningham 2024

package main

import (
	"errors"
	"os"
)

const base_dir = "/var/lib/comms/"

func file_create(path string) {
	f, err := os.Create(base_dir + path)
	fatal(err)
	f.Close()
}

func file_exists(path string) bool {
	_, err := os.Stat(base_dir + path)
	if err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}
	fatal(err)
	return false
}

func file_mkdir(path string) {
	err := os.MkdirAll(base_dir+path, 0755)
	fatal(err)
}

func file_read(path string) []byte {
	data, err := os.ReadFile(base_dir + path)
	fatal(err)
	return data
}

func file_write(path string, data []byte) {
	err := os.WriteFile(base_dir+path, data, 0644)
	fatal(err)
}
