// Mochi server: SQLite online-backup helper
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Cross-platform wrapper around the ncruces driver's online-backup
// API (sqlite3_backup_init). Page-copying preserves byte offsets
// across snapshots, so rsync delta stays tight. Originally lived in
// admin_linux.go alongside the admin backup HTTP handler; extracted
// here so the bulk-bootstrap protocol (which runs on every platform
// because it ships in core) can also call it.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
)

// snapshot_copy_db copies srcPath to dstPath using SQLite's online
// backup API. Returns the size of the resulting file.
func snapshot_copy_db(srcPath, dstPath string) (int64, error) {
	src, err := sql.Open("sqlite3", "file:"+srcPath+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return 0, fmt.Errorf("open source %s: %w", srcPath, err)
	}
	defer src.Close()

	_ = os.Remove(dstPath)

	ctx := context.Background()
	source_connection, err := src.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("source conn: %w", err)
	}
	defer source_connection.Close()

	raw_error := source_connection.Raw(func(driverConn any) error {
		dc, ok := driverConn.(sqlitedrv.Conn)
		if !ok {
			return fmt.Errorf("driver conn does not implement sqlitedrv.Conn")
		}
		return dc.Raw().Backup("main", dstPath)
	})
	if raw_error != nil {
		return 0, fmt.Errorf("backup: %w", raw_error)
	}

	info, err := os.Stat(dstPath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
