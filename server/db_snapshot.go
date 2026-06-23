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
	"strings"

	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
)

// db_quick_check runs PRAGMA quick_check on the database file at path, read-only
// (so it can't touch a WAL). It returns the check result and whether the check
// actually ran to a verdict. ran=false means the check couldn't complete — an
// open or lock error, i.e. transient, NOT a corruption finding. ran=true with
// result != "ok" is a definitive corruption finding. A malformed DB makes
// quick_check itself error rather than return a row; that error text IS the
// corruption verdict (the prod 2026-06-23 signature), so it maps to ran=true.
func db_quick_check(path string) (result string, ran bool) {
	d, err := sql.Open("sqlite3", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return "", false
	}
	defer d.Close()
	var r string
	if err := d.QueryRow("PRAGMA quick_check(1)").Scan(&r); err != nil {
		msg := err.Error()
		if strings.Contains(msg, "malformed") || strings.Contains(msg, "not a database") || strings.Contains(msg, "corrupt") {
			return msg, true
		}
		return "", false
	}
	return r, true
}

// snapshot_integrity_ok reports whether the SQLite database at path is provably
// clean. Gates a freshly-fetched bootstrap snapshot before it is landed, so a
// corrupt source or transfer is rejected and retried rather than installed and
// then re-propagated to other peers — the corruption ping-pong that wrecked
// feeds.db (#6). A check that couldn't run (ran=false) is treated as not-ok: an
// unverifiable snapshot must not be landed.
func snapshot_integrity_ok(path string) bool {
	result, ran := db_quick_check(path)
	return ran && result == "ok"
}

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
