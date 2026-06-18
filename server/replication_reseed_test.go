// Mochi server: per-stream re-seed (#9) unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestReseedLocalJournalCount (#9) covers the re-seed safety gate. A
// stream this host has never written has no replication journal and is
// safe to re-seed (count 0); a stream it HAS written carries journal
// rows, so a re-seed — which overwrites the local DB with the source's
// snapshot — would lose any un-shipped local writes, and the admin path
// refuses unless forced.
func TestReseedLocalJournalCount(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	rel := "users/u1/app1/db/feeds.db"
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}

	// Missing DB file → 0 (nothing local to lose).
	if n := reseed_local_journal_count(rel); n != 0 {
		t.Fatalf("missing DB file: got %d, want 0", n)
	}

	// DB present but no journal table → pure receiver → 0.
	db := db_open(rel)
	db.exec("create table posts (id text primary key)")
	if n := reseed_local_journal_count(rel); n != 0 {
		t.Fatalf("no journal table (pure receiver): got %d, want 0", n)
	}

	// Journal rows present → local writes → non-zero (re-seed refused).
	db.exec("create table journal (id text primary key, state text)")
	db.exec("insert into journal (id, state) values ('a', 'shipped'), ('b', 'pending')")
	if n := reseed_local_journal_count(rel); n != 2 {
		t.Fatalf("local writes present: got %d, want 2", n)
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
