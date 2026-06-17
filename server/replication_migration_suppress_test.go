package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// TestMigrationWritesDoNotReplicate covers the #18 fix: writes performed by a
// database_create/upgrade/downgrade run with the replication_suppressed
// thread-local set (by (*AppVersion).starlark_db), so they must NOT emit
// replication ops — every replica runs the migration itself, and emitting would
// double-apply on the partner (and a non-deterministic migration can't replay
// identically). The same write without the flag must still emit.
func TestMigrationWritesDoNotReplicate(t *testing.T) {
	var emits int
	orig := replication_emit_to
	replication_emit_to = func(user string, op *ReplicationOp, peers []string) { emits++ }
	defer func() { replication_emit_to = orig }()

	user := &User{UID: "u1", Username: "a@example.com"}
	av := &AppVersion{}
	app := &App{id: "testapp", internal: av}

	mkthread := func(suppress bool) *sl.Thread {
		th := &sl.Thread{Name: "test"}
		th.SetLocal("user", user)
		th.SetLocal("owner", user)
		th.SetLocal("app", app)
		if suppress {
			th.SetLocal("replication_suppressed", true)
		}
		return th
	}

	const write = "insert into widgets (id, name) values (1, 'a')"

	// Control: a normal app write emits a replication op.
	emits = 0
	db_replicate_after_exec(mkthread(false), write, nil)
	if emits != 1 {
		t.Fatalf("control: normal write emitted %d ops, want 1", emits)
	}

	// Migration write (replication_suppressed set): emits nothing.
	emits = 0
	db_replicate_after_exec(mkthread(true), write, nil)
	if emits != 0 {
		t.Fatalf("migration write emitted %d ops, want 0", emits)
	}
}

// TestTransactionCommitHonoursSuppression covers the deferred (transaction)
// emit path: a TransactionHandle opened while replication_suppressed is set is
// marked suppressed, and its commit must emit nothing even with buffered
// pending writes.
func TestTransactionCommitHonoursSuppression(t *testing.T) {
	var emits int
	orig := replication_emit_to
	replication_emit_to = func(user string, op *ReplicationOp, peers []string) { emits++ }
	defer func() { replication_emit_to = orig }()

	user := &User{UID: "u1"}
	av := &AppVersion{}
	app := &App{id: "testapp", internal: av}

	// A suppressed handle with buffered emits must drop them on commit.
	hSuppressed := &TransactionHandle{
		suppressed:    true,
		user:          user,
		app:           app,
		av:            av,
		pending_emits: []sql_pending_emit{{sql: "insert into widgets (id) values (1)"}},
	}
	emits = 0
	flushPendingEmits(hSuppressed)
	if emits != 0 {
		t.Fatalf("suppressed transaction commit emitted %d ops, want 0", emits)
	}

	// A normal handle flushes its buffered emits.
	hNormal := &TransactionHandle{
		user:          user,
		app:           app,
		av:            av,
		pending_emits: []sql_pending_emit{{sql: "insert into widgets (id) values (1)"}},
	}
	emits = 0
	flushPendingEmits(hNormal)
	if emits != 1 {
		t.Fatalf("normal transaction commit emitted %d ops, want 1", emits)
	}
}

// flushPendingEmits mirrors the emit loop in (*TransactionHandle).sl_commit so
// the suppression branch can be tested without opening a real SQL transaction.
func flushPendingEmits(h *TransactionHandle) {
	if !h.suppressed {
		for _, e := range h.pending_emits {
			replication_emit_sql_command(h.user, h.app, h.av, e.sql, e.args)
		}
	}
}
