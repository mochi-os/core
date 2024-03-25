// Comms server: Files
// Copyright Alistair Cunningham 2024

package main

import (
	"errors"
	"os"
)

func file_create(path string) {
	f, err := os.Create(data_dir + "/" + path)
	fatal(err)
	f.Close()
}

func file_exists(path string) bool {
	_, err := os.Stat(data_dir + "/" + path)
	if err == nil {
		return true
	} else if errors.Is(err, os.ErrNotExist) {
		return false
	}
	fatal(err)
	return false
}

func file_mkdir(path string) {
	err := os.MkdirAll(data_dir+"/"+path, 0755)
	fatal(err)
}

func file_read(path string) []byte {
	data, err := os.ReadFile(data_dir + "/" + path)
	fatal(err)
	return data
}

func file_write(path string, data []byte) {
	err := os.WriteFile(data_dir+"/"+path, data, 0644)
	fatal(err)
}
