// Mochi server: logical bootstrap large-DB transfer (#15/#16).
//
// Reproduces the P2 rig case: a feeds-shaped DB with 200k rows (text PK, a
// dangling foreign key, several indexes) dumped through the real CBOR
// serialization into a verified scratch. Catches batching/checksum/limit bugs
// that only appear at scale, and exercises the memory profile the logical path
// was built to bound (no whole-file page copy).
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"io"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
)

func TestBootstrapLogicalLargeTransfer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	const rows = 200000

	// Seed the source FILE via a raw connection with foreign_keys OFF — exactly
	// how the rig DB was built (sqlite3 CLI, FK off), so it carries dangling-FK
	// rows ('tf' has no feeds row) the server's own FK-on connection would have
	// rejected. This is the faithful reproduction of the missing rig DB.
	srcPath := data_dir + "/large-src.db"
	raw, err := sql.Open("sqlite3", "file:"+srcPath+"?_pragma=foreign_keys(off)")
	if err != nil {
		t.Fatal(err)
	}
	raw.Exec("create table feeds (id text not null primary key, name text not null default '')")
	raw.Exec(`create table posts ( id text not null primary key, feed references feeds( id ), body text not null, created integer not null, updated integer not null, novelty integer not null default 100 )`)
	raw.Exec("create index posts_feed on posts( feed )")
	raw.Exec("create index posts_created on posts( created )")
	if _, err := raw.Exec("with recursive c(n) as (select 1 union all select n+1 from c where n < ?) insert into posts (id, feed, body, created, updated) select 'p' || printf('%07d', n), 'tf', 'body ' || n || ' xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 1700000000 + n, 1700000000 + n from c", rows); err != nil {
		t.Fatalf("seed: %v", err)
	}
	raw.Close()

	src := db_open("large-src.db")

	// Decode with the SAME limited decoder the libp2p stream uses, so an
	// over-limit array (cbor_max_elements) would fail here exactly as on the wire.
	dec, err := cbor.DecOptions{MaxArrayElements: cbor_max_elements, MaxMapPairs: cbor_max_pairs}.DecMode()
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan []byte, 256)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- bootstrap_logical_serve(src, nil, 1, 7, func(env *BootstrapDBMessage) error {
			ch <- cbor_encode(env)
			return nil
		})
		close(ch)
	}()

	scratch := data_dir + "/large-rebuilt.db"
	read := func(env *BootstrapDBMessage) error {
		b, ok := <-ch
		if !ok {
			return io.EOF
		}
		return dec.Unmarshal(b, env)
	}
	seq, err := bootstrap_logical_fetch(scratch, read)
	if e := <-serveErr; e != nil {
		t.Fatalf("serve: %v", e)
	}
	if err != nil {
		t.Fatalf("fetch (this is the rig failure if it reproduces): %v", err)
	}
	if seq != 7 {
		t.Errorf("sequence = %d, want 7", seq)
	}

	out, err := sql.Open("sqlite3", "file:"+scratch+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	var n int
	if err := out.QueryRow("select count(*) from posts").Scan(&n); err != nil || n != rows {
		t.Fatalf("rebuilt posts = %d (err %v), want %d", n, err, rows)
	}
}

// TestBootstrapLogicalPreservesSequence pins that AUTOINCREMENT high-water marks
// (sqlite_sequence) survive the dump/rebuild even when the top row was deleted —
// so the destination won't reissue an id the source already consumed.
func TestBootstrapLogicalPreservesSequence(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	src := db_open("seq-src.db")
	src.exec("create table items (id integer primary key autoincrement, name text)")
	src.exec("insert into items (name) values ('a'), ('b'), ('c')") // ids 1,2,3 → seq=3
	src.exec("delete from items where id = 3")                       // seq stays 3, max(id)=2
	src.exec("pragma user_version = 42")                             // app schema version
	var srcSeq int64
	if err := src.internal.Get(&srcSeq, "select seq from sqlite_sequence where name='items'"); err != nil || srcSeq != 3 {
		t.Fatalf("source seq=%d err=%v, want 3", srcSeq, err)
	}

	ch := make(chan []byte, 64)
	go func() {
		bootstrap_logical_serve(src, nil, 1, 1, func(env *BootstrapDBMessage) error { ch <- cbor_encode(env); return nil })
		close(ch)
	}()
	scratch := data_dir + "/seq-rebuilt.db"
	read := func(env *BootstrapDBMessage) error {
		b, ok := <-ch
		if !ok {
			return io.EOF
		}
		return cbor.Unmarshal(b, env)
	}
	if _, err := bootstrap_logical_fetch(scratch, read); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	out, err := sql.Open("sqlite3", "file:"+scratch+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	var dstSeq int64
	if err := out.QueryRow("select seq from sqlite_sequence where name='items'").Scan(&dstSeq); err != nil {
		t.Fatalf("rebuilt sqlite_sequence missing: %v", err)
	}
	if dstSeq != 3 {
		t.Errorf("rebuilt seq=%d, want 3 (sqlite_sequence not preserved)", dstSeq)
	}
	var uv int
	if err := out.QueryRow("pragma user_version").Scan(&uv); err != nil || uv != 42 {
		t.Errorf("rebuilt user_version=%d err=%v, want 42 (not preserved → app re-runs database_upgrade)", uv, err)
	}
}
