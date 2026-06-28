// Mochi server: receiver-initiated gap-fill. A wedged inbound stream
// asks the peer to re-ship the exact missing op range so it self-heals without
// an operator reseed. journal_reship_range is the server side of that request:
// re-ship the [from, to] window for one stream, un-gated by the delivery cursor,
// only to a host that is genuinely in the user's host set.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func gapfill_seed_op(db, rdb *DB, stream, id string, seq, prev int64) {
	db.exec("insert into journal (id, operation, statement, args, target, uid, schema, created, state) values (?,?,?,?,?,?,?,?,'shipped')",
		id, repl_op_exec, "insert into items (id) values (?)", cbor_encode([]any{id}), "items", id, 0, 100)
	rdb.exec("insert into journal_sequence (id, user, scope, stream, sequence, prev) values (?,?,?,?,?,?)",
		id, "u1", repl_scope_app, stream, seq, prev)
}

// Re-ships only the requested [from, to] window, reusing each op's (sequence,
// prev), to a peer in the user's host set — and is NOT delivery-cursor-gated.
func TestJournalReshipRangeWindow(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()
	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.journal_setup()
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists hosts (user text not null, peer text not null, added integer not null, primary key (user, peer))")
	stream := repl_stream_key(repl_stream_class_app, "testapp")
	gapfill_seed_op(db, rdb, stream, "o5", 5, 4)
	gapfill_seed_op(db, rdb, stream, "o6", 6, 5)
	gapfill_seed_op(db, rdb, stream, "o7", 7, 6)
	rdb.exec("insert into hosts (user, peer, added) values ('u1', 'peerX', 0)")

	var got []int64
	orig := journal_ship
	journal_ship = func(userUID string, op *ReplicationOp, peers []string) {
		if len(peers) == 1 && peers[0] == "peerX" {
			got = append(got, op.Sequence)
		}
	}
	defer func() { journal_ship = orig }()

	// Request the gap [6,7] — re-ship o6 + o7, never o5 (below `from`).
	if n := journal_reship_range("u1", "peerX", stream, 6, 7); n != 2 {
		t.Fatalf("reship returned %d, want 2", n)
	}
	if len(got) != 2 || got[0] != 6 || got[1] != 7 {
		t.Fatalf("re-shipped seqs = %v, want [6 7] (window-filtered, un-gated)", got)
	}
}

// A peer NOT in the user's host set gets nothing — the gate stops a non-member
// pulling a user's ops.
func TestJournalReshipRangeRejectsNonRecipient(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()
	db := db_open("users/u1/testapp/db/data.db")
	db.journal_setup()
	rdb := db_open("db/replication.db")
	stream := repl_stream_key(repl_stream_class_app, "testapp")
	gapfill_seed_op(db, rdb, stream, "o5", 5, 4)
	// No hosts/pair row → peerY is not a recipient.

	shipped := 0
	orig := journal_ship
	journal_ship = func(userUID string, op *ReplicationOp, peers []string) { shipped++ }
	defer func() { journal_ship = orig }()
	if n := journal_reship_range("u1", "peerY", stream, 5, 5); n != 0 || shipped != 0 {
		t.Fatalf("non-recipient got n=%d shipped=%d, want 0/0 (security gate)", n, shipped)
	}
}

// gapfill_seq coerces the JSON-decoded numeric wire field (float64) to int64.
func TestGapfillSeq(t *testing.T) {
	if gapfill_seq(float64(5)) != 5 || gapfill_seq(int64(7)) != 7 || gapfill_seq(3) != 3 {
		t.Error("gapfill_seq numeric coercion wrong")
	}
	if gapfill_seq("nope") != 0 || gapfill_seq(nil) != 0 {
		t.Error("gapfill_seq should be 0 for non-numeric")
	}
}
