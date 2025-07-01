// Comms: Cache
// Copyright Alistair Cunningham 2025

package main

import (
	"io/fs"
	"path/filepath"
	"strings"
	"time"
)

type CacheFile struct {
	User     int
	Identity string
	Entity   string
	ID       string
	Extra    string
	Name     string
	Path     string
	Size     int64
	Created  int64
}

// Clean up stale cache entries
func cache_manager() {
	db := db_open("db/cache.db")
	for {
		time.Sleep(24 * time.Hour)
		log_debug("Cache cleaning files older than 30 days")
		db.exec("delete from files where created<?", now()-30*86400)
		filepath.WalkDir(cache_dir, cache_manager_walk)
	}
}

// Check if a file or directory needs cleaned
func cache_manager_walk(path string, d fs.DirEntry, err error) error {
	if err != nil {
		return err
	}

	if path == cache_dir {
		return nil
	}

	if d.IsDir() {
		// Delete directory, but only if empty
		file_delete(path)

	} else {
		// Delete file if no matching entry in database
		db := db_open("db/cache.db")
		if !db.exists("select * from files where path=?", strings.TrimLeft(path, cache_dir+"/")) {
			file_delete(path)
		}
	}

	return nil
}
