// Mochi server: per-stream re-seed (#9) unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBootstrapFetchLockSerializesSameTarget proves the per-target build lock
// (#146) makes two concurrent fetches/reseeds of the SAME target run one at a
// time (so they can't race on the shared "<target>.rebuild" scratch), while two
// DIFFERENT targets are free to run concurrently.
func TestBootstrapFetchLockSerializesSameTarget(t *testing.T) {
	// Same target: observed concurrency must never exceed 1.
	var live, peak int32
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := bootstrap_fetch_lock("/data/users/u/feeds/db/feeds.db")
			defer unlock()
			n := atomic.AddInt32(&live, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&live, -1)
		}()
	}
	wg.Wait()
	if peak != 1 {
		t.Fatalf("same-target fetches must serialize, but observed %d running at once", peak)
	}

	// Different targets must NOT block each other: both enter their critical
	// section and rendezvous; a shared/global lock would deadlock here.
	both := make(chan struct{}, 2)
	proceed := make(chan struct{})
	for _, target := range []string{"/data/a.db", "/data/b.db"} {
		go func(tg string) {
			unlock := bootstrap_fetch_lock(tg)
			defer unlock()
			both <- struct{}{}
			<-proceed
		}(target)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-both:
		case <-time.After(2 * time.Second):
			t.Fatal("distinct-target fetches must not block each other (both should hold their lock simultaneously)")
		}
	}
	close(proceed)
}

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

// TestReseedSourceMissingOpsUserDB (#63) covers the non-journal scopes the guard
// now vets via the users-row delivery cursor: the per-user user.db (checked on
// BOTH core:user and the pair-only system:users) and a conservative block for
// server-level db/ DBs that span many users' streams.
func TestReseedSourceMissingOpsUserDB(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	peer := "peerS"

	rel := "users/u1/user.db" // bootstrap_stream_key -> core:user
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	db_open(rel).exec("create table accounts (id text primary key)") // no journal — a core DB

	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")
	rdb.exec("create table if not exists journal_delivery (user text not null, peer text not null, stream text not null, sequence integer not null, primary key (user, peer, stream))")

	// No emissions on either stream → pure receiver → safe.
	if reseed_source_missing_ops(rel, peer) {
		t.Fatal("user.db, no emissions: want false")
	}

	// Emitted on core:user (tail 5), source acked only to 2 → block.
	rdb.exec("insert into tail (user, scope, db, last) values ('u1', ?, 'core:user', 5)", repl_scope_app)
	rdb.exec("insert into journal_delivery (user, peer, stream, sequence) values ('u1', ?, 'core:user', 2)", peer)
	if !reseed_source_missing_ops(rel, peer) {
		t.Fatal("user.db core:user source behind: want true")
	}

	// Source caught up on core:user → safe again.
	rdb.exec("update journal_delivery set sequence = 5 where user='u1' and peer=? and stream='core:user'", peer)
	if reseed_source_missing_ops(rel, peer) {
		t.Fatal("user.db core:user source caught up: want false")
	}

	// Also emitted on the pair-only system:users (tail 3) with no ack at all → block.
	rdb.exec("insert into tail (user, scope, db, last) values ('u1', ?, 'system:users', 3)", repl_scope_app)
	if !reseed_source_missing_ops(rel, peer) {
		t.Fatal("user.db system:users source behind: want true")
	}

	// Source acks system:users too → both streams caught up → safe.
	rdb.exec("insert into journal_delivery (user, peer, stream, sequence) values ('u1', ?, 'system:users', 3)", peer)
	if reseed_source_missing_ops(rel, peer) {
		t.Fatal("user.db both streams caught up: want false")
	}

	// A server-level db/ DB spans many users' streams → conservative block.
	db_open("db/users.db").exec("create table users (uid text primary key)")
	if !reseed_source_missing_ops("db/users.db", peer) {
		t.Fatal("db/ system DB: want true (conservative block)")
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

	// The app DB's journal is the receiver's OWN (the logical bootstrap skips
	// journal on the wire, so a reseed never carries the source's). finalize must
	// NOT wipe it — that would only destroy the receiver's own ops (#171). Model a
	// row a local write journaled just after the swap.
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

	if n := app.integer("select count(*) from journal"); n != 2 {
		t.Fatalf("receiver's own journal wrongly touched (#171): want 2 rows, got %d", n)
	}
	if n := rdb.integer("select count(*) from pending where sequence <= 100"); n != 0 {
		t.Fatalf("stale pending not cleared: %d rows remain", n)
	}
	if n := rdb.integer("select count(*) from pending where sequence = 150"); n != 1 {
		t.Fatalf("live pending wrongly cleared: want 1, got %d", n)
	}
}
