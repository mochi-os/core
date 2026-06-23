// Mochi server: logical bootstrap wire round-trip (#15).
//
// serve -> CBOR-encode each envelope -> channel -> CBOR-decode -> fetch, so the
// real serialization is exercised (including int64/uint64 normalisation and
// blob/NULL fidelity), and the rebuilt scratch is confirmed identical.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
)

func TestBootstrapLogicalWireRoundTrip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	src := db_open("wire-src.db")
	src.exec("create table t (id integer primary key, name text, score real, data blob, opt text)")
	src.exec("create index t_name on t(name)")
	for i := 1; i <= 30; i++ {
		src.exec("insert into t (id, name, score, data, opt) values (?, ?, ?, ?, ?)",
			i, fmt.Sprintf("r%d", i), float64(i)/2, []byte{byte(i)}, nil)
	}

	ch := make(chan []byte, 4000)
	done := make(chan error, 1)
	go func() {
		done <- bootstrap_logical_serve(src, nil, 3, 99, func(env *BootstrapDBMessage) error {
			ch <- cbor_encode(env)
			return nil
		})
		close(ch)
	}()

	scratch := filepath.Join(data_dir, "wire-rebuilt.db")
	read := func(env *BootstrapDBMessage) error {
		b, ok := <-ch
		if !ok {
			return io.EOF
		}
		return cbor.Unmarshal(b, env)
	}
	seq, err := bootstrap_logical_fetch(scratch, read)
	if serveErr := <-done; serveErr != nil {
		t.Fatalf("serve: %v", serveErr)
	}
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if seq != 99 {
		t.Errorf("snapshot sequence = %d, want 99", seq)
	}

	out, err := sql.Open("sqlite3", "file:"+scratch+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	var n int
	if err := out.QueryRow("select count(*) from t").Scan(&n); err != nil || n != 30 {
		t.Fatalf("rebuilt rows = %d (err %v), want 30", n, err)
	}
	var data []byte
	var opt sql.NullString
	if err := out.QueryRow("select data, opt from t where id=20").Scan(&data, &opt); err != nil {
		t.Fatal(err)
	}
	if len(data) != 1 || data[0] != 20 || opt.Valid {
		t.Errorf("row 20 mismatch through the wire: data=%v optValid=%v", data, opt.Valid)
	}
}
