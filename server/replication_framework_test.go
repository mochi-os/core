// Mochi server: framework-layer multi-master scenarios
// Copyright Alistair Cunningham 2026
//
// The harness in replication_harness_test.go captures emits into
// queues, and the scenarios in replication_multimaster_test.go drive
// row convergence. Both bypass the framework layer - the dedup /
// fence / per-stream gate / pending-buffer / cursor-advance machinery
// that lives in replication_op_receive (extracted from
// replication_op_event so tests can call it directly).
//
// These tests construct ReplicationOps with explicit Sequence / Prev
// values and call replication_op_receive directly, then assert the
// expected framework state (seen, cursor, pending). Failure modes
// covered:
//   - Duplicate delivery dedup via seen
//   - Out-of-order arrival: later op buffered in pending, fills when
//     the gap-filling earlier op lands and drain triggers
//   - Stream restart (Prev==0) anchors a fresh cursor
//   - Below-cursor op (already-applied retry) is silently dropped

package main

import (
	"strconv"
	"testing"
)

const (
	fwUID = "uid-framework"
)

// setup_framework_test seeds the current host with the user and
// schedule schema the framework tests use as a vehicle for landing
// real apply-side state. Returns a cleanup.
func setup_framework_test(t *testing.T) func() {
	cleanup := setup_replication_test(t)
	setup_users_test_schema()
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", fwUID, "fw@example.com")
	schedule_db().exec("create table if not exists schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	return cleanup
}

// build_schedule_op constructs a schedule-row.set op with explicit
// Sequence and Prev so the framework gate can be exercised directly.
// The natural-key fields produce a deterministic row identity per
// (event, created) tuple - convenient for distinguishing which op
// landed.
func build_schedule_op(seq, prev int64, event string, created int64) *ReplicationOp {
	payload := cbor_encode(&ScheduleRow{
		Key: map[string]string{
			"user": fwUID, "app": "feeds", "event": event,
			"created": strconv.FormatInt(created, 10),
		},
		Cols: map[string]string{
			"due": "1000", "data": "{}", "interval": "0",
		},
	})
	return &ReplicationOp{
		Scope: repl_scope_app, User: fwUID,
		Database: "schedule", Table: "schedule",
		Operation: "schedule-row.set",
		Sequence:  seq, Prev: prev,
		Payload: payload,
	}
}

// TestFrameworkDuplicateDelivery: the same op arriving twice is
// deduped via the seen table. Apply runs once; second arrival is a
// no-op.
func TestFrameworkDuplicateDelivery(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	op := build_schedule_op(1, 0, "dup", 100)
	replication_op_receive("peerA", op)
	replication_op_receive("peerA", op) // second delivery

	row, _ := schedule_db().row(
		"select count(*) as n from schedule where user=? and event='dup'", fwUID)
	if n, _ := row["n"].(int64); n != 1 {
		t.Errorf("duplicate apply: rows = %d, want 1 (dedup via seen)", n)
	}

	// Cursor should be at seq=1, single seen row.
	rdb := db_open("db/replication.db")
	cursor, anchored := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, "schedule")
	if !anchored || cursor != 1 {
		t.Errorf("cursor: anchored=%v seq=%d, want anchored=true seq=1", anchored, cursor)
	}
	srow, _ := rdb.row("select count(*) as n from seen where peer='peerA' and scope=? and user=?",
		repl_scope_app, fwUID)
	if n, _ := srow["n"].(int64); n != 1 {
		t.Errorf("seen rows = %d, want 1", n)
	}
}

// TestFrameworkOutOfOrderArrivalDrainsPending: seq=2 (Prev=1) arrives
// before seq=1 (Prev=0). seq=2 is buffered in pending; seq=1 applies
// and triggers the pending drain so seq=2 also lands. Final state:
// both rows present, cursor=2, pending empty.
func TestFrameworkOutOfOrderArrivalDrainsPending(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	op2 := build_schedule_op(2, 1, "second", 200)
	op1 := build_schedule_op(1, 0, "first", 100)

	// seq=2 arrives first - no anchor yet, so buffered in pending.
	replication_op_receive("peerA", op2)
	rdb := db_open("db/replication.db")
	pending, _ := rdb.row("select count(*) as n from pending where peer='peerA'")
	if n, _ := pending["n"].(int64); n != 1 {
		t.Errorf("after seq=2: pending = %d, want 1 (buffered behind gap)", n)
	}
	row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 0 {
		t.Errorf("after seq=2: rows = %d, want 0 (op buffered, not applied)", n)
	}

	// seq=1 arrives - applies, then drain re-applies seq=2.
	replication_op_receive("peerA", op1)
	row, _ = schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 2 {
		t.Errorf("after seq=1: rows = %d, want 2 (seq=1 applied, drain landed seq=2)", n)
	}
	pending, _ = rdb.row("select count(*) as n from pending where peer='peerA'")
	if n, _ := pending["n"].(int64); n != 0 {
		t.Errorf("after drain: pending = %d, want 0 (cleared)", n)
	}
	cursor, _ := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, "schedule")
	if cursor != 2 {
		t.Errorf("cursor = %d, want 2", cursor)
	}
}

// TestFrameworkBelowCursorOpDropped: an op with Prev<cursor (already
// applied) is silently dropped. Doesn't crash, doesn't double-apply,
// doesn't move the cursor backward.
func TestFrameworkBelowCursorOpDropped(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	replication_op_receive("peerA", build_schedule_op(1, 0, "one", 100))
	replication_op_receive("peerA", build_schedule_op(2, 1, "two", 200))
	replication_op_receive("peerA", build_schedule_op(3, 2, "three", 300))

	// Replay of seq=2 with Prev=1 - already below cursor (which is at 3).
	// Different sequence number so seen dedup doesn't catch it; the
	// below-cursor gate is what drops it.
	replication_op_receive("peerA", build_schedule_op(99, 1, "replay-pretender", 200))

	row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 3 {
		t.Errorf("rows = %d, want 3 (below-cursor replay must not apply)", n)
	}
	rdb := db_open("db/replication.db")
	cursor, _ := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, "schedule")
	if cursor != 3 {
		t.Errorf("cursor = %d, want 3 (must not rewind)", cursor)
	}
}

// TestFrameworkStreamRestart: an op with Prev=0 anchors a fresh
// cursor regardless of the prior cursor position - what happens after
// a sender restart that resets its outbound sequence.
func TestFrameworkStreamRestart(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	replication_op_receive("peerA", build_schedule_op(1, 0, "one", 100))
	replication_op_receive("peerA", build_schedule_op(2, 1, "two", 200))

	rdb := db_open("db/replication.db")
	cursor, _ := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, "schedule")
	if cursor != 2 {
		t.Fatalf("pre-restart cursor = %d, want 2", cursor)
	}

	// Sender restart: a fresh Prev=0 op anchors regardless of cursor.
	replication_op_receive("peerA", build_schedule_op(50, 0, "restart-anchor", 500))

	row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 3 {
		t.Errorf("rows = %d, want 3 (restart op anchored and applied)", n)
	}
	cursor, _ = replication_cursor(rdb, "peerA", repl_scope_app, fwUID, "schedule")
	if cursor != 50 {
		t.Errorf("post-restart cursor = %d, want 50", cursor)
	}
}

// TestFrameworkPerPeerStreamsIndependent: two peers' sequence streams
// for the same (scope, user, db) are tracked independently. Cursor on
// peerA at 5 doesn't affect peerB's gate logic.
func TestFrameworkPerPeerStreamsIndependent(t *testing.T) {
	cleanup := setup_framework_test(t)
	defer cleanup()

	replication_op_receive("peerA", build_schedule_op(1, 0, "from-A", 100))
	replication_op_receive("peerB", build_schedule_op(1, 0, "from-B", 200))
	replication_op_receive("peerA", build_schedule_op(2, 1, "from-A2", 300))

	row, _ := schedule_db().row("select count(*) as n from schedule where user=?", fwUID)
	if n, _ := row["n"].(int64); n != 3 {
		t.Errorf("rows = %d, want 3 (1 per stream + 1 chain on A)", n)
	}
	rdb := db_open("db/replication.db")
	cA, _ := replication_cursor(rdb, "peerA", repl_scope_app, fwUID, "schedule")
	cB, _ := replication_cursor(rdb, "peerB", repl_scope_app, fwUID, "schedule")
	if cA != 2 {
		t.Errorf("peerA cursor = %d, want 2", cA)
	}
	if cB != 1 {
		t.Errorf("peerB cursor = %d, want 1", cB)
	}
}
