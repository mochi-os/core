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

// The actual wire path: cbor decodes positive integers into interface{} as
// uint64, not int64 — so gapfill_seq MUST handle uint64, or the gap-fill handler
// reads from=0 and silently early-returns (the bug the rig e2e surfaced — the
// direct-value test above never exercised the cbor round-trip).
func TestGapfillSeqCborWire(t *testing.T) {
	var decoded map[string]any
	if err := cbor_decode_mode.Unmarshal(cbor_encode(map[string]any{"from": int64(44)}), &decoded); err != nil {
		t.Fatal(err)
	}
	if got := gapfill_seq(decoded["from"]); got != 44 {
		t.Fatalf("cbor-wire from decoded as %T -> gapfill_seq=%d, want 44", decoded["from"], got)
	}
}

// Full gap-fill cycle through the REAL cbor wire: a stalled stream (detect) ->
// the request payload the requester builds -> cbor encode/decode (the wire, where
// from/to become uint64) -> the serve handler -> the re-ship. This is the
// regression guard the original unit tests lacked: they passed Go ints straight
// to gapfill_seq and never exercised cbor, so the uint64 bug (serve handler
// early-returning on every request -> the committed gap-fill never served) shipped
// green. If gapfill_seq stops handling uint64, this fails. (Apply/drain of the
// re-shipped op is the standard replication_op_receive path, covered elsewhere.)
func TestGapfillRequestServeCycleThroughCbor(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()
	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("db_open returned nil")
	}
	db.journal_setup()
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists hosts (user text not null, peer text not null, added integer not null, primary key (user, peer))")
	rdb.exec("insert into hosts (user, peer, added) values ('u1', 'peerX', 0)")
	stream := repl_stream_key(repl_stream_class_app, "testapp")
	// The missing predecessor the receiver needs: seq 6, prev 5.
	gapfill_seed_op(db, rdb, stream, "o6", 6, 5)

	// DETECT: the stalled stream the receiver would compute — applied up to
	// cursor 5, a buffered op whose predecessor is seq 6, so the gap is exactly 6.
	s := StalledStream{
		Peer: "peerX", Scope: repl_scope_app, User: "u1", Database: stream,
		Cursor: 5, Anchored: true,
		Predecessor: PredecessorRange{Minimum: 6, Maximum: 6},
	}
	// REQUEST -> WIRE: the real payload, through real cbor (from/to come back uint64).
	var decoded map[string]any
	if err := cbor_decode_mode.Unmarshal(cbor_encode(gapfill_request_content(s)), &decoded); err != nil {
		t.Fatal(err)
	}

	// SERVE: feed the wire-decoded request to the handler; capture the re-ship.
	var shipped []int64
	orig := journal_ship
	journal_ship = func(userUID string, op *ReplicationOp, peers []string) {
		if len(peers) == 1 && peers[0] == "peerX" {
			shipped = append(shipped, op.Sequence)
		}
	}
	defer func() { journal_ship = orig }()

	replication_gapfill_event(&Event{content: decoded, peer: "peerX"})

	// RESHIP: op 6 (the missing predecessor, range [cursor+1=6 .. max_prev=6]) re-shipped.
	if len(shipped) != 1 || shipped[0] != 6 {
		t.Fatalf("gap-fill cycle re-shipped seqs %v, want [6] (request->cbor wire->serve->reship)", shipped)
	}
}

// Bounded retries (#80) + unfillability detection (#79): a stream whose cursor
// never advances stops being re-requested after gapfill_max_attempts, reads as
// re-ship-exhausted (so the stall alert escalates to an operator reseed), and a
// cursor advance resets the counter and re-enables requests.
func TestGapfillBoundedRetries(t *testing.T) {
	orig := gapfill_requested
	gapfill_requested = map[string]gapfill_attempt{}
	defer func() { gapfill_requested = orig }()

	key := "peerX|app|u1|app:test"
	s := StalledStream{Peer: "peerX", Scope: "app", User: "u1", Database: "app:test"}
	base := int64(1_000_000)

	// No cursor progress (always 5), spaced past the backoff: exactly max_attempts fire.
	sent := 0
	for i := 0; i < gapfill_max_attempts+3; i++ {
		if gapfill_should_request(key, 5, base+int64(i)*(gapfill_backoff_seconds+1)) {
			sent++
		}
	}
	if sent != gapfill_max_attempts {
		t.Fatalf("bounded retries: sent %d requests, want %d", sent, gapfill_max_attempts)
	}
	if !gapfill_reship_exhausted(s) {
		t.Fatal("stream should read re-ship-exhausted after max no-progress attempts")
	}

	// A cursor advance (re-ship or normal delivery made progress) resets the count.
	if !gapfill_should_request(key, 6, base+9_000_000) {
		t.Fatal("a cursor advance must reset the counter and re-enable requests")
	}
	if gapfill_reship_exhausted(s) {
		t.Fatal("after progress the stream must no longer be exhausted")
	}
	// Backoff still applies: a second request within the window is refused.
	if gapfill_should_request(key, 6, base+9_000_000+1) {
		t.Fatal("a request within the backoff window must be refused")
	}
}
