// Mochi server: per-stream re-seed (#9) unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestReseedSourceMissingOps (#9) covers the direction-aware re-seed safety gate.
// A re-seed overwrites the local DB with the source's snapshot, so it must block
// only when the source actually lacks ops THIS host originated: un-sent `pending`
// journal ops, or shipped ops the source has not acked up to our emitted tail. A
// pure-receiver stream, or one the source has fully acked (even with retained-
// shipped journal rows it already holds), is safe — the old count-all gate wrongly
// refused those.
func TestReseedSourceMissingOps(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}

	rel := "users/u1/app1/db/feeds.db"
	peer := "peerS"
	stream := bootstrap_stream_key(rel) // app:app1
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}

	// Missing DB file → nothing local to lose.
	if reseed_source_missing_ops(rel, peer) {
		t.Fatal("missing DB file: want false")
	}

	// DB present, no journal table → pure receiver.
	db := db_open(rel)
	db.exec("create table posts (id text primary key)")
	if reseed_source_missing_ops(rel, peer) {
		t.Fatal("no journal table (pure receiver): want false")
	}

	// A pending (un-sent) op → the source lacks it → block.
	db.exec("create table journal (id text primary key, state text not null default 'pending')")
	db.exec("insert into journal (id, state) values ('p1', 'pending')")
	if !reseed_source_missing_ops(rel, peer) {
		t.Fatal("pending op present: want true")
	}

	// Only retained-shipped ops now; record our emitted tail = 5.
	db.exec("delete from journal")
	db.exec("insert into journal (id, state) values ('s1', 'shipped'), ('s2', 'shipped')")
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")
	rdb.exec("insert into tail (user, scope, db, last) values ('u1', ?, ?, 5)", repl_scope_app, stream)
	rdb.exec("create table if not exists journal_delivery (user text not null, peer text not null, stream text not null, sequence integer not null, primary key (user, peer, stream))")

	// Source acked below our tail (2 < 5) → it dropped our shipped ops → block.
	rdb.exec("insert into journal_delivery (user, peer, stream, sequence) values ('u1', ?, ?, 2)", peer, stream)
	if !reseed_source_missing_ops(rel, peer) {
		t.Fatal("source acked below tail: want true")
	}

	// Source acked up to our tail (5 ≥ 5) → it holds everything → allow, even
	// though retained-shipped journal rows remain.
	rdb.exec("update journal_delivery set sequence = 5 where user = 'u1' and peer = ? and stream = ?", peer, stream)
	if reseed_source_missing_ops(rel, peer) {
		t.Fatal("source acked up to tail: want false")
	}
}

// TestReseedFinalize (#9) checks the post-fetch cleanup. The inherited
// journal — the source's sender-state, carried in the snapshot — must be
// emptied so the receiver never re-ships those ops as its own. And the
// stream's stale pending (rows at/below the freshly re-anchored cursor,
// which buffered behind the jumped gap) must be cleared while later
// buffered ops are kept.
func TestReseedFinalize(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}

	rel := "users/u1/app1/db/feeds.db"
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	peer := "peerX"
	stream := bootstrap_stream_key(rel)

	// App DB carries an inherited journal (from the source snapshot).
	app := db_open(rel)
	app.exec("create table journal (id text primary key, state text)")
	app.exec("insert into journal (id, state) values ('a', 'shipped'), ('b', 'pending')")

	// replication.db: cursor at 100, pending at 50 (stale) and 150 (live).
	rdb := db_open("db/replication.db")
	rdb.exec("create table cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")
	rdb.exec("create table pending (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null, prev integer not null default 0, schema integer not null default 0, payload blob not null, received integer not null, primary key (peer, scope, user, sequence))")
	rdb.exec("insert into cursor (peer, scope, user, db, sequence) values (?, ?, 'u1', ?, 100)", peer, repl_scope_app, stream)
	rdb.exec("insert into pending (peer, scope, user, db, sequence, payload, received) values (?, ?, 'u1', ?, 50, x'00', 0)", peer, repl_scope_app, stream)
	rdb.exec("insert into pending (peer, scope, user, db, sequence, payload, received) values (?, ?, 'u1', ?, 150, x'00', 0)", peer, repl_scope_app, stream)

	reseed_finalize(peer, full)

	if n := app.integer("select count(*) from journal"); n != 0 {
		t.Fatalf("inherited journal not cleared: %d rows remain", n)
	}
	if n := rdb.integer("select count(*) from pending where sequence <= 100"); n != 0 {
		t.Fatalf("stale pending not cleared: %d rows remain", n)
	}
	if n := rdb.integer("select count(*) from pending where sequence = 150"); n != 1 {
		t.Fatalf("live pending wrongly cleared: want 1, got %d", n)
	}
}
