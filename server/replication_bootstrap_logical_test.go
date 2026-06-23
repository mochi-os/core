// Mochi server: logical bootstrap engine round-trip (#15).
//
// Dump a database through the loader and confirm the rebuilt scratch file is an
// exact copy: finish() verifies per-table count + checksum + quick_check, and
// the test independently re-reads values, a blob, NULL, and an index.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestBootstrapLogicalRoundTrip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	src := db_open("logical-src.db")
	src.exec("create table t (id integer primary key, name text, score real, data blob, opt text)")
	src.exec("create index t_name on t(name)")
	src.exec("create table empty (a integer primary key, b text)")
	for i := 1; i <= 25; i++ {
		src.exec("insert into t (id, name, score, data, opt) values (?, ?, ?, ?, ?)",
			i, fmt.Sprintf("row-%d", i), float64(i)+0.5, []byte{byte(i), 0x00, 0xff}, nil)
	}

	scratch := filepath.Join(data_dir, "logical-rebuilt.db")
	loader, err := bootstrap_logical_loader(scratch)
	if err != nil {
		t.Fatal(err)
	}
	// Small batch size so the per-table batch boundary is exercised.
	if err := bootstrap_logical_dump(src, nil, 7, 3, func(msg any) error { return loader.apply(msg) }); err != nil {
		t.Fatalf("dump: %v", err)
	}
	if err := loader.finish(); err != nil {
		t.Fatalf("finish (count/checksum/quick_check verify failed): %v", err)
	}

	// Independently re-read the rebuilt file.
	out, err := sql.Open("sqlite3", "file:"+scratch+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	var n int
	if err := out.QueryRow("select count(*) from t").Scan(&n); err != nil || n != 25 {
		t.Fatalf("rebuilt t has %d rows (err %v), want 25", n, err)
	}
	var name string
	var score float64
	var data []byte
	var opt sql.NullString
	if err := out.QueryRow("select name, score, data, opt from t where id=13").Scan(&name, &score, &data, &opt); err != nil {
		t.Fatal(err)
	}
	if name != "row-13" || score != 13.5 || len(data) != 3 || data[0] != 13 || data[2] != 0xff || opt.Valid {
		t.Errorf("row 13 mismatch: name=%q score=%v data=%v optValid=%v", name, score, data, opt.Valid)
	}
	var idx int
	out.QueryRow("select count(*) from sqlite_master where type='index' and name='t_name'").Scan(&idx)
	if idx != 1 {
		t.Error("rebuilt missing index t_name")
	}
}

// A short/garbled batch (count mismatch) must fail finish — the verify gate that
// makes an incomplete transfer non-adoptable.
func TestBootstrapLogicalRejectsShortTransfer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	scratch := filepath.Join(data_dir, "short-rebuilt.db")
	loader, err := bootstrap_logical_loader(scratch)
	if err != nil {
		t.Fatal(err)
	}
	// Schema for one table, then claim 5 rows but deliver 2.
	if err := loader.apply(&BootstrapSchema{Tables: []string{"create table t (id integer primary key, v text)"}}); err != nil {
		t.Fatal(err)
	}
	if err := loader.apply(&BootstrapRowBatch{Table: "t", Columns: []string{"id", "v"}, Rows: [][]any{{int64(1), "a"}, {int64(2), "b"}}}); err != nil {
		t.Fatal(err)
	}
	if err := loader.apply(&BootstrapTableDone{Table: "t", Count: 5, Checksum: 0}); err != nil {
		t.Fatal(err)
	}
	if err := loader.finish(); err == nil {
		t.Error("finish must reject a short transfer (delivered 2 rows, declared 5)")
	}
}
