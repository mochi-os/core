// Mochi server: per-peer journal delivery cursor (#28) unit tests
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

func deliveryTestSetup(t *testing.T) func() {
	orig := data_dir
	data_dir = t.TempDir()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	journal_tables_test() // creates journal_delivery in this temp replication.db
	return func() {
		data_dir = orig
		journal_inflight_active.Store(false) // don't leak the global into other tests
	}
}

// TestJournalDeliveryCursorAdvances (#28): recording inflight sends then acking
// them advances the peer's delivery cursor to the highest acked sequence and
// clears the inflight rows; a non-journal id in the ack batch is ignored.
func TestJournalDeliveryCursorAdvances(t *testing.T) {
	defer deliveryTestSetup(t)()
	rdb := db_open("db/replication.db")

	journal_inflight_record("msg1", "u1", "peerA", "app:feeds", 100)
	journal_inflight_record("msg2", "u1", "peerA", "app:feeds", 105)
	if c := journal_delivery_cursor(rdb, "u1", "peerA", "app:feeds"); c != 0 {
		t.Fatalf("cursor before ack: got %d, want 0", c)
	}

	journal_inflight_acked([]string{"msg1", "msg2", "not-a-journal-msg"})

	if c := journal_delivery_cursor(rdb, "u1", "peerA", "app:feeds"); c != 105 {
		t.Fatalf("cursor after ack: got %d, want 105 (highest acked)", c)
	}
	qdb := db_open("db/queue.db")
	if n := qdb.integer("select count(*) from journal_inflight"); n != 0 {
		t.Fatalf("inflight not cleared: %d rows remain", n)
	}
}

// TestJournalDeliveryCursorMonotonic (#28): the cursor only moves forward — a
// later ack of an older sequence (out-of-order delivery) must not rewind it.
func TestJournalDeliveryCursorMonotonic(t *testing.T) {
	defer deliveryTestSetup(t)()
	rdb := db_open("db/replication.db")

	journal_inflight_record("hi", "u1", "peerA", "app:feeds", 200)
	journal_inflight_acked([]string{"hi"})
	journal_inflight_record("lo", "u1", "peerA", "app:feeds", 150)
	journal_inflight_acked([]string{"lo"})

	if c := journal_delivery_cursor(rdb, "u1", "peerA", "app:feeds"); c != 200 {
		t.Fatalf("cursor: got %d, want 200 (no rewind on older ack)", c)
	}
}

// TestJournalDeliveryCursorPerPeer (#28): cursors are independent per peer, so
// one slow peer doesn't make the fast peer's backfill skip ops.
func TestJournalDeliveryCursorPerPeer(t *testing.T) {
	defer deliveryTestSetup(t)()
	rdb := db_open("db/replication.db")

	journal_inflight_record("a", "u1", "fast", "app:feeds", 300)
	journal_inflight_record("b", "u1", "slow", "app:feeds", 120)
	journal_inflight_acked([]string{"a", "b"})

	if c := journal_delivery_cursor(rdb, "u1", "fast", "app:feeds"); c != 300 {
		t.Fatalf("fast peer cursor: got %d, want 300", c)
	}
	if c := journal_delivery_cursor(rdb, "u1", "slow", "app:feeds"); c != 120 {
		t.Fatalf("slow peer cursor: got %d, want 120", c)
	}
}

// TestJournalInflightSweep (#28): inflight rows whose message never acked within
// the TTL are dropped; recent rows are kept.
func TestJournalInflightSweep(t *testing.T) {
	defer deliveryTestSetup(t)()
	qdb := db_open("db/queue.db")

	journal_inflight_record("recent", "u1", "peerA", "app:feeds", 10) // created = now
	qdb.exec("insert into journal_inflight (id, user, peer, stream, sequence, created) values ('stale','u1','peerA','app:feeds',5,?)",
		now()-journal_inflight_ttl-100)

	journal_inflight_sweep()

	if has, _ := qdb.exists("select 1 from journal_inflight where id='stale'"); has {
		t.Fatal("stale inflight row should be swept")
	}
	if has, _ := qdb.exists("select 1 from journal_inflight where id='recent'"); !has {
		t.Fatal("recent inflight row should be kept")
	}
}

// TestJournalBackfillSkipsBelowDeliveryCursor (#28, headline): a reconnect
// backfill re-ships only ops ABOVE the peer's delivery cursor, not the whole
// retained window — the op the peer already confirmed is skipped.
func TestJournalBackfillSkipsBelowDeliveryCursor(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()

	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.journal_setup()
	db.exec("insert into journal (id, operation, statement, args, target, uid, schema, created, state) values (?,?,?,?,?,?,?,?,'shipped')",
		"o5", repl_op_exec, "insert into items (id) values (?)", cbor_encode([]any{"x5"}), "items", "x5", 0, 100)
	db.exec("insert into journal (id, operation, statement, args, target, uid, schema, created, state) values (?,?,?,?,?,?,?,?,'shipped')",
		"o6", repl_op_exec, "insert into items (id) values (?)", cbor_encode([]any{"x6"}), "items", "x6", 0, 101)

	rdb := db_open("db/replication.db")
	stream := repl_stream_key(repl_stream_class_app, "testapp")
	rdb.exec("insert into journal_sequence (id, user, scope, stream, sequence, prev) values (?,?,?,?,?,?)", "o5", "u1", repl_scope_app, stream, 5, 4)
	rdb.exec("insert into journal_sequence (id, user, scope, stream, sequence, prev) values (?,?,?,?,?,?)", "o6", "u1", repl_scope_app, stream, 6, 5)
	// peerX has already confirmed delivery up to sequence 5.
	rdb.exec("insert into journal_delivery (user, peer, stream, sequence) values (?,?,?,?)", "u1", "peerX", stream, 5)

	var shipped []int64
	orig := journal_ship
	journal_ship = func(userUID string, op *ReplicationOp, peers []string) { shipped = append(shipped, op.Sequence) }
	defer func() { journal_ship = orig }()

	journal_backfill_peer("u1", "peerX")

	if len(shipped) != 1 || shipped[0] != 6 {
		t.Fatalf("backfill shipped %v, want [6] — only the delta above the delivery cursor (5)", shipped)
	}
}
