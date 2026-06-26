// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
)

// journal_test_dir points data_dir at a fresh temp tree with the db/ and the
// per-user app data dir created, the minimum for db_open + the journal paths.
func journal_test_dir(t *testing.T, userUID, appID string) func() {
	t.Helper()
	orig := data_dir
	data_dir = t.TempDir()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatalf("mkdir db: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(data_dir, "users", userUID, appID, "db"), 0o755); err != nil {
		t.Fatalf("mkdir app db: %v", err)
	}
	journal_tables_test() // production creates these eagerly in db_create()/db_upgrade_90
	return func() { data_dir = orig }
}

// journal_tables_test creates the journal cursor tables in the current temp
// data_dir — sequence/tail/journal_sequence/journal_delivery in replication.db
// and journal_inflight in queue.db. Production creates these eagerly in
// db_create()/db_upgrade_90; tests that don't run the full db_create() seed them
// directly.
func journal_tables_test() {
	rdb := db_open("db/replication.db")
	if rdb == nil {
		return
	}
	rdb.exec("create table if not exists sequence (user text not null default '', scope text not null, next integer not null default 0, primary key (user, scope))")
	rdb.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")
	rdb.exec("create table if not exists journal_sequence (id text primary key, user text not null, scope text not null, stream text not null, sequence integer not null, prev integer not null)")
	rdb.exec("create table if not exists journal_delivery (user text not null default '', peer text not null, stream text not null, sequence integer not null default 0, primary key (user, peer, stream))")
	qdb := db_open("db/queue.db")
	if qdb != nil {
		qdb.exec("create table if not exists journal_inflight (id text primary key, user text not null, peer text not null, stream text not null, sequence integer not null, created integer not null)")
	}
}

// TestJournalAssignIdempotent is the Gap-B core: a sequence is allocated once
// per journal id and the binding is reused on a re-drain, so a crash between
// assign and ship can never burn a sequence. Distinct ids chain (prev = the
// previous op's sequence) on the same stream.
func TestJournalAssignIdempotent(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	op := &ReplicationOp{Scope: repl_scope_app, Database: "testapp", Operation: repl_op_exec}

	s1, p1 := replication_journal_assign("u1", op, "id1")
	if s1 == 0 {
		t.Fatalf("first assign returned sequence 0")
	}
	if p1 != 0 {
		t.Fatalf("first op of a stream must have prev 0, got %d", p1)
	}

	// Re-assign the SAME id: must reuse the binding, not consume a new seq.
	s1b, p1b := replication_journal_assign("u1", op, "id1")
	if s1b != s1 || p1b != p1 {
		t.Fatalf("re-assign of id1 returned (%d,%d), want (%d,%d) — sequence burned", s1b, p1b, s1, p1)
	}

	// A different id on the same stream chains onto s1.
	s2, p2 := replication_journal_assign("u1", op, "id2")
	if s2 != s1+1 {
		t.Fatalf("second op sequence = %d, want %d", s2, s1+1)
	}
	if p2 != s1 {
		t.Fatalf("second op prev = %d, want %d (chains onto first)", p2, s1)
	}
}

// TestJournalDrainShipsOnceAndMarksShipped: the drainer builds the op from the
// row, ships it exactly once, marks it shipped, and a re-drain re-ships
// nothing (idempotent at the drain level).
func TestJournalDrainShipsOnceAndMarksShipped(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.journal_setup()
	db.exec("insert into journal (id, operation, statement, args, target, uid, schema, created, state) values (?,?,?,?,?,?,?,?,'pending')",
		"o1", repl_op_exec, "insert into items (id) values (?)", cbor_encode([]any{"x1"}), "items", "x1", 3, 100)

	var captured []*ReplicationOp
	orig := replication_emit_journal
	replication_emit_journal = func(userUID string, op *ReplicationOp, journalID string) bool {
		captured = append(captured, op)
		return true
	}
	defer func() { replication_emit_journal = orig }()

	journal_drain("u1", "testapp", db)

	if len(captured) != 1 {
		t.Fatalf("drain shipped %d ops, want 1", len(captured))
	}
	op := captured[0]
	if op.Operation != repl_op_exec || op.Database != "testapp" || op.Table != "items" || op.UID != "x1" || op.Schema != 3 {
		t.Fatalf("shipped op fields wrong: %+v", op)
	}
	var cmd SQLCommand
	if err := cbor.Unmarshal(op.Payload, &cmd); err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	if cmd.Statement != "insert into items (id) values (?)" || len(cmd.Args) != 1 {
		t.Fatalf("shipped statement wrong: %q args=%v", cmd.Statement, cmd.Args)
	}

	if has, _ := db.exists("select 1 from journal where id='o1' and state='shipped'"); !has {
		t.Fatalf("row not marked shipped after drain")
	}

	// Re-drain: nothing pending, nothing re-shipped.
	captured = nil
	journal_drain("u1", "testapp", db)
	if len(captured) != 0 {
		t.Fatalf("re-drain shipped %d ops, want 0", len(captured))
	}
}

// TestDbExecuteJournalAtomic: a replicated write commits the data row AND the
// journal row together; a failing write commits NEITHER (Gap B — no half-state).
func TestDbExecuteJournalAtomic(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.exec("create table items (id text primary key)")
	db.journal_setup() // db_app does this at open; this test opens the data DB directly

	av := &AppVersion{}
	av.Database.Schema = 3

	ctx := context.Background()
	conn, err := db.starlark.Connx(ctx)
	if err != nil {
		t.Fatalf("connx: %v", err)
	}
	defer conn.Close()

	affected, recorded, err := db_execute_journal(ctx, conn, db, av, false, "insert into items (id) values (?)", []any{"a"})
	if err != nil || !recorded {
		t.Fatalf("first write: recorded=%v err=%v", recorded, err)
	}
	if affected != 1 {
		t.Fatalf("insert affected %d rows, want 1", affected)
	}
	if n := db.integer("select count(*) from items"); n != 1 {
		t.Fatalf("items count = %d, want 1", n)
	}
	if n := db.integer("select count(*) from journal"); n != 1 {
		t.Fatalf("journal count = %d, want 1", n)
	}

	// Duplicate PK -> the data write fails; the tx rolls back, so NO new
	// journal row and NO partial data row.
	_, recorded, err = db_execute_journal(ctx, conn, db, av, false, "insert into items (id) values (?)", []any{"a"})
	if err == nil {
		t.Fatalf("duplicate insert unexpectedly succeeded")
	}
	if n := db.integer("select count(*) from items"); n != 1 {
		t.Fatalf("items count after failed write = %d, want 1", n)
	}
	if n := db.integer("select count(*) from journal"); n != 1 {
		t.Fatalf("journal count after failed write = %d, want 1 (no half-state)", n)
	}
}

// TestDbExecuteJournalReturnsRowsAffected (#5): mochi.db.execute returns the
// number of rows the statement changed — 1 per insert, the match count for an
// update/delete, 0 when nothing matches.
func TestDbExecuteJournalReturnsRowsAffected(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.exec("create table items (id text primary key, flag integer)")
	db.journal_setup() // db_app does this at open; this test opens the data DB directly
	av := &AppVersion{}
	av.Database.Schema = 1

	ctx := context.Background()
	conn, err := db.starlark.Connx(ctx)
	if err != nil {
		t.Fatalf("connx: %v", err)
	}
	defer conn.Close()

	exec := func(q string, args ...any) int64 {
		n, _, err := db_execute_journal(ctx, conn, db, av, false, q, args)
		if err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
		return n
	}

	for _, id := range []string{"a", "b", "c"} {
		if n := exec("insert into items (id, flag) values (?, 0)", id); n != 1 {
			t.Fatalf("insert %s affected %d rows, want 1", id, n)
		}
	}
	if n := exec("update items set flag=1"); n != 3 {
		t.Fatalf("update-all affected %d rows, want 3", n)
	}
	if n := exec("update items set flag=2 where id='zzz'"); n != 0 {
		t.Fatalf("no-match update affected %d rows, want 0", n)
	}
	if n := exec("delete from items"); n != 3 {
		t.Fatalf("delete affected %d rows, want 3", n)
	}
}

// TestJournalBackfillResendsShippedToPeer (Gap A, #23): a retained 'shipped'
// row is re-shipped to a returning peer reusing its already-assigned
// (sequence, prev), so the receiver's chain lines up and it dedups the rest.
func TestJournalBackfillResendsShippedToPeer(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.journal_setup()
	db.exec("insert into journal (id, operation, statement, args, target, uid, schema, created, state) values (?,?,?,?,?,?,?,?,'shipped')",
		"o1", repl_op_exec, "insert into items (id) values (?)", cbor_encode([]any{"x1"}), "items", "x1", 0, 100)

	// Bind o1 -> (sequence 5, prev 4) as a prior drain would have.
	rdb := db_open("db/replication.db")
	rdb.exec("insert into journal_sequence (id, user, scope, stream, sequence, prev) values (?,?,?,?,?,?)",
		"o1", "u1", repl_scope_app, repl_stream_key(repl_stream_class_app, "testapp"), 5, 4)

	type shipped struct {
		peers     []string
		seq, prev int64
	}
	var got []shipped
	orig := journal_ship
	journal_ship = func(userUID string, op *ReplicationOp, peers []string) {
		got = append(got, shipped{peers, op.Sequence, op.Prev})
	}
	defer func() { journal_ship = orig }()

	journal_backfill_peer("u1", "peerX")

	if len(got) != 1 {
		t.Fatalf("backfill shipped %d ops, want 1", len(got))
	}
	if got[0].seq != 5 || got[0].prev != 4 {
		t.Fatalf("backfill used (seq=%d,prev=%d), want (5,4) — must reuse the binding", got[0].seq, got[0].prev)
	}
	if len(got[0].peers) != 1 || got[0].peers[0] != "peerX" {
		t.Fatalf("backfill peers = %v, want [peerX]", got[0].peers)
	}
}

// TestJournalPruneRetainsRecentAndPending (#24): prune drops shipped ops past
// the retention age (and their bindings), keeps recent shipped ops, and never
// touches pending ops.
func TestJournalPruneRetainsRecentAndPending(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.journal_setup()
	rdb := db_open("db/replication.db")

	origAge, origMin := journal_retention_age, journal_retention_minimum
	journal_retention_age = 100
	journal_retention_minimum = 0 // age-only for the test
	defer func() { journal_retention_age, journal_retention_minimum = origAge, origMin }()

	old := now() - 1000  // older than the 100s retention
	recent := now() - 10 // within retention
	ins := func(id, state string, created int64) {
		db.exec("insert into journal (id, operation, statement, args, target, uid, schema, created, state) values (?,?,?,?,?,?,?,?,?)",
			id, repl_op_exec, "insert into items (id) values (1)", cbor_encode([]any{}), "items", "", 0, created, state)
	}
	ins("old", "shipped", old)
	rdb.exec("insert into journal_sequence (id, user, scope, stream, sequence, prev) values ('old','u1','app','s',1,0)")
	ins("recent", "shipped", recent)
	ins("oldpending", "pending", old)

	journal_prune(db)

	if n := db.integer("select count(*) from journal where id='old'"); n != 0 {
		t.Fatalf("old shipped op not pruned (count=%d)", n)
	}
	if n := db.integer("select count(*) from journal_sequence where id='old'"); n != 0 {
		t.Fatalf("pruned op's binding not removed (count=%d)", n)
	}
	if n := db.integer("select count(*) from journal where id='recent'"); n != 1 {
		t.Fatalf("recent shipped op wrongly pruned (count=%d)", n)
	}
	if n := db.integer("select count(*) from journal where id='oldpending'"); n != 1 {
		t.Fatalf("pending op wrongly pruned (count=%d)", n)
	}
}

// TestJournalConcurrentAssignChainsContiguously (#25): N goroutines assigning
// on the same stream at once must produce a contiguous prev-chain with no gaps,
// no duplicate sequences, and the global counter walking 1..N exactly. This is
// the race the per-stream mutex + single-tx allocation exist to prevent — a
// SELECT-then-UPDATE allocator would double-allocate here and corrupt the chain.
func TestJournalConcurrentAssignChainsContiguously(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	const N = 50
	type pair struct{ seq, prev int64 }
	results := make([]pair, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			op := &ReplicationOp{Scope: repl_scope_app, Database: "testapp", Operation: repl_op_exec}
			s, p := replication_journal_assign("u1", op, fmt.Sprintf("id-%d", i))
			results[i] = pair{s, p}
		}(i)
	}
	wg.Wait()

	prevOf := map[int64]int64{} // seq -> prev
	for _, r := range results {
		if r.seq == 0 {
			t.Fatalf("an assign returned sequence 0")
		}
		if _, dup := prevOf[r.seq]; dup {
			t.Fatalf("duplicate sequence %d allocated", r.seq)
		}
		prevOf[r.seq] = r.prev
	}
	ordered := make([]int64, 0, len(prevOf))
	for s := range prevOf {
		ordered = append(ordered, s)
	}
	sort.Slice(ordered, func(a, b int) bool { return ordered[a] < ordered[b] })
	for idx, s := range ordered {
		if s != int64(idx+1) {
			t.Fatalf("sequence at position %d is %d, want %d (not 1..N contiguous)", idx, s, idx+1)
		}
		want := int64(0)
		if idx > 0 {
			want = ordered[idx-1]
		}
		if prevOf[s] != want {
			t.Fatalf("op seq=%d has prev=%d, want %d (chain gap)", s, prevOf[s], want)
		}
	}
}

// TestJournalCrashRecoveryReusesSequence (#25, Gap B): a row whose sequence was
// already bound (a crash between assign and mark-shipped) is re-drained with the
// SAME sequence — never a fresh one — and the global counter is not bumped twice.
func TestJournalCrashRecoveryReusesSequence(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.journal_setup()
	db.exec("insert into journal (id, operation, statement, args, target, uid, schema, created, state) values (?,?,?,?,?,?,?,?,'pending')",
		"o1", repl_op_exec, "insert into items (id) values (?)", cbor_encode([]any{"x1"}), "items", "x1", 0, 100)

	// Simulate "crashed after assign, before ship/mark": bind o1 -> a sequence.
	op := &ReplicationOp{Scope: repl_scope_app, Database: "testapp", Operation: repl_op_exec}
	seq0, _ := replication_journal_assign("u1", op, "o1")
	if seq0 == 0 {
		t.Fatal("initial assign returned 0")
	}

	var shipped []int64
	orig := journal_ship
	journal_ship = func(userUID string, op *ReplicationOp, peers []string) { shipped = append(shipped, op.Sequence) }
	defer func() { journal_ship = orig }()

	journal_drain("u1", "testapp", db) // recovery re-drain

	if len(shipped) != 1 {
		t.Fatalf("re-drain shipped %d ops, want 1", len(shipped))
	}
	if shipped[0] != seq0 {
		t.Fatalf("re-drain assigned sequence %d, want %d (reused) — sequence burned", shipped[0], seq0)
	}
	if has, _ := db.exists("select 1 from journal where id='o1' and state='shipped'"); !has {
		t.Fatal("row not marked shipped after recovery drain")
	}
	rdb := db_open("db/replication.db")
	if next := rdb.integer("select next from sequence where user='u1' and scope='app'"); int64(next) != seq0 {
		t.Fatalf("global counter = %d, want %d (no second allocation)", next, seq0)
	}
}

// journal_gate_fixture (#13) drives the FULL journal pipeline end to end:
// db_execute_journal writes on a SENDER, then journal_drain produces a real
// (sequence, prev) op stream via the production replication_journal_assign. It
// returns those three ops (re-targeted at the receiver user so the gate's apply
// lands in the receiver's DB), the app, and the receiver UID — so a caller can
// deliver them through the real receive gate in any (faulty) order and assert
// convergence. Teardown is registered via t.Cleanup.
func journal_gate_fixture(t *testing.T) (ops []*ReplicationOp, app *App, receiverUID string) {
	t.Helper()
	t.Cleanup(setup_replication_test(t))
	setup_users_test_schema()

	// db_app's first-touch fires a background post-migration drain that races
	// the cleanup's data_dir restore; stub it for the test.
	origDrain := post_migration_drain_async
	post_migration_drain_async = func(user, appID string) {}
	t.Cleanup(func() { post_migration_drain_async = origDrain })

	udb := db_open("db/users.db")
	senderUID := "uid-journal-sender"
	receiverUID = "uid-journal-receiver"
	udb.exec("insert into users (uid, username) values (?, ?)", senderUID, "bob")
	udb.exec("insert into users (uid, username) values (?, ?)", receiverUID, "alice")

	app_id := "journalapp"
	av := &AppVersion{Version: "1"}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	av.Database.File = "journalapp.db"
	av.Database.Schema = 1
	av.Database.create_function = func(db *DB) {
		db.exec("create table posts (id text primary key, title text not null)")
	}
	app = &App{id: app_id, versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = app
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps[app_id] = app
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, app_id)
		apps_lock.Unlock()
	})

	// SENDER: real journal writes, then drain to capture the emitted op stream.
	sdb := db_app(&User{UID: senderUID}, app)
	if sdb == nil {
		t.Fatal("sender db nil")
	}
	ctx := context.Background()
	conn, err := sdb.starlark.Connx(ctx)
	if err != nil {
		t.Fatalf("connx: %v", err)
	}
	for _, args := range [][]any{{"p1", "One"}, {"p2", "Two"}, {"p3", "Three"}} {
		if _, _, werr := db_execute_journal(ctx, conn, sdb, av, false, "insert into posts (id, title) values (?, ?)", args); werr != nil {
			conn.Close()
			t.Fatalf("journal write: %v", werr)
		}
	}
	conn.Close()

	origShip := journal_ship
	journal_ship = func(userUID string, op *ReplicationOp, peers []string) { ops = append(ops, op) }
	journal_drain(senderUID, app_id, sdb)
	journal_ship = origShip

	if len(ops) != 3 {
		t.Fatalf("journal emitted %d ops, want 3", len(ops))
	}
	for i, op := range ops { // the drain produced a contiguous prev-chain
		if op.Sequence != int64(i+1) {
			t.Fatalf("op %d sequence = %d, want %d", i, op.Sequence, i+1)
		}
		op.User = receiverUID // route the apply at the receiver
	}
	return ops, app, receiverUID
}

// assert_journal_converged checks the receiver holds all three rows.
func assert_journal_converged(t *testing.T, app *App, receiverUID string) {
	t.Helper()
	rdb := db_app(&User{UID: receiverUID}, app)
	if n := rdb.integer("select count(*) from posts"); n != 3 {
		t.Fatalf("receiver posts = %d, want 3 (converged)", n)
	}
	for _, id := range []string{"p1", "p2", "p3"} {
		if n := rdb.integer("select count(*) from posts where id=?", id); n != 1 {
			t.Fatalf("receiver missing row %q", id)
		}
	}
}

// TestJournalStreamConvergesUnderReorder (#13): the journal-produced op stream
// delivered REVERSED through the real gate buffers the later ops in `pending`
// and drains them once the Prev==0 anchor lands — the receiver converges.
func TestJournalStreamConvergesUnderReorder(t *testing.T) {
	ops, app, receiverUID := journal_gate_fixture(t)
	for i := len(ops) - 1; i >= 0; i-- {
		replication_op_receive("sender-peer", ops[i])
	}
	assert_journal_converged(t, app, receiverUID)
}

// TestJournalStreamConvergesAfterDroppedOpRedelivered (#13): a middle op is
// "dropped" — op1 anchors and applies, op3 buffers behind the gap (so only one
// row lands), then redelivering op2 applies it and drains op3. The receiver
// converges. Models a lost-then-retransmitted packet.
func TestJournalStreamConvergesAfterDroppedOpRedelivered(t *testing.T) {
	ops, app, receiverUID := journal_gate_fixture(t)

	replication_op_receive("sender-peer", ops[0]) // anchors, applies p1
	replication_op_receive("sender-peer", ops[2]) // prev > cursor -> buffered

	rdb := db_app(&User{UID: receiverUID}, app)
	if n := rdb.integer("select count(*) from posts"); n != 1 {
		t.Fatalf("after op1+op3 (op2 dropped): posts = %d, want 1 (op3 buffered behind the gap)", n)
	}

	replication_op_receive("sender-peer", ops[1]) // fills the gap, drains op3
	assert_journal_converged(t, app, receiverUID)
}

// TestReplicationSequenceNextAtomic (#6): N goroutines allocating on the same
// (user, scope) with NO mutex must each get a distinct sequence forming 1..N —
// the property the UPSERT...RETURNING rewrite guarantees and the old
// SELECT-then-UPDATE did not (it handed out duplicates under concurrency, the
// pair-membership emit's latent race).
func TestReplicationSequenceNextAtomic(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	const N = 50
	seqs := make([]int64, N)
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seqs[i] = replication_sequence_next("u1", "app")
		}(i)
	}
	wg.Wait()

	seen := map[int64]bool{}
	for _, s := range seqs {
		if s == 0 {
			t.Fatalf("allocation returned sequence 0")
		}
		if seen[s] {
			t.Fatalf("duplicate sequence %d allocated under concurrency", s)
		}
		seen[s] = true
	}
	for i := int64(1); i <= N; i++ {
		if !seen[i] {
			t.Fatalf("sequence %d missing — not 1..N contiguous (got %v)", i, seqs)
		}
	}
}

// TestReplicationStallAlertDedup (#3): the early stall alert fires once per
// stall episode (not every tick), re-arms after the stream drains, and never
// fires for a stream that hasn't been stalled long enough.
func TestReplicationStallAlertDedup(t *testing.T) {
	stall_alerted_mutex.Lock()
	stall_alerted = map[string]bool{}
	stall_alerted_mutex.Unlock()

	const threshold = int64(1000)
	s := StalledStream{Peer: "p", Scope: "app", User: "u", Database: "app:feeds", Oldest: 100} // stalled long enough

	if fresh := replication_stall_alert_new([]StalledStream{s}, threshold); len(fresh) != 1 {
		t.Fatalf("first pass: fresh = %d, want 1 (alert)", len(fresh))
	}
	if fresh := replication_stall_alert_new([]StalledStream{s}, threshold); len(fresh) != 0 {
		t.Fatalf("second pass (still stalled): fresh = %d, want 0 (deduped)", len(fresh))
	}

	// Stream drains (absent) -> alert cleared; a re-stall alerts again.
	replication_stall_alert_new(nil, threshold)
	if fresh := replication_stall_alert_new([]StalledStream{s}, threshold); len(fresh) != 1 {
		t.Fatalf("after drain + re-stall: fresh = %d, want 1 (re-armed)", len(fresh))
	}

	// A stream stalled but not yet past the threshold never alerts.
	young := StalledStream{Peer: "p2", Scope: "app", User: "u", Database: "app:wikis", Oldest: threshold + 1}
	if fresh := replication_stall_alert_new([]StalledStream{young}, threshold); len(fresh) != 0 {
		t.Fatalf("young stream: fresh = %d, want 0 (not stalled long enough)", len(fresh))
	}
}

// TestSQLIsMutating (#7) backs the guard that keeps mutations out of the
// read-only row/rows/exists APIs (which don't journal the write).
func TestSQLIsMutating(t *testing.T) {
	mutating := []string{
		"insert into t (a) values (1)",
		"INSERT OR IGNORE INTO t (a) values (1)",
		"update t set a=1",
		"  UPDATE t SET a=1 WHERE b=2",
		"delete from t",
		"replace into t (a) values (1)",
		"\n\tdelete from t where a=1",
	}
	for _, q := range mutating {
		if !sql_is_mutating(q) {
			t.Errorf("sql_is_mutating(%q) = false, want true", q)
		}
	}
	readonly := []string{
		"select * from t",
		"  SELECT a from t where b='update'",
		"select count(*) from t",
		"",
	}
	for _, q := range readonly {
		if sql_is_mutating(q) {
			t.Errorf("sql_is_mutating(%q) = true, want false", q)
		}
	}
}

// TestJournalReplicatesSuppression: migration writes (replication_suppressed)
// and writes with no app version create no journal row, mirroring the #18 rule.
func TestJournalReplicatesSuppression(t *testing.T) {
	av := &AppVersion{}
	av.Database.Schema = 1

	if journal_replicates(true, av, "insert into items (id) values (1)") {
		t.Fatalf("suppressed write must not replicate")
	}
	if journal_replicates(false, nil, "insert into items (id) values (1)") {
		t.Fatalf("write with no app version must not replicate")
	}
	if !journal_replicates(false, av, "insert into items (id) values (1)") {
		t.Fatalf("normal write to a non-excluded table must replicate")
	}
}
