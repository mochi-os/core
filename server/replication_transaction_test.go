// Mochi server: transaction emit-buffer lifecycle tests
// Copyright Alistair Cunningham 2026
//
// mochi.db.transaction() returns a TransactionHandle that buffers each
// successful sl_execute as a `sql_pending_emit`, flushing on commit
// and dropping on rollback (or on auto-cleanup via transaction_close,
// for handles abandoned by the Starlark thread).

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// new_tx_handle builds a TransactionHandle backed by a real *sqlx.Tx
// on the test app's per-(user, app) DB. Lets us exercise commit /
// rollback / close paths without spinning up a full Starlark context.
func new_tx_handle(t *testing.T) (h *TransactionHandle, cleanup func()) {
	t.Helper()
	clean, user_uid, app_id := setup_sql_replication_test(t)

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	av := a.internal
	db := db_app(u, a)
	tx, err := db.starlark.Beginx()
	if err != nil {
		clean()
		t.Fatalf("begin tx: %v", err)
	}
	h = &TransactionHandle{tx: tx, user: u, app: a, av: av}
	return h, clean
}

func TestTransactionCommitFlushesPendingEmits(t *testing.T) {
	h, cleanup := new_tx_handle(t)
	defer cleanup()

	h.pending_emits = []sql_pending_emit{
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"a", "A"}},
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"b", "B"}},
	}

	if _, err := h.sl_commit(&sl.Thread{}, nil, sl.Tuple{}, nil); err != nil {
		t.Fatalf("sl_commit: %v", err)
	}
	if !h.closed {
		t.Error("after commit: handle must be closed")
	}
	if h.pending_emits != nil {
		t.Errorf("after commit: pending_emits must be nil, got %d entries", len(h.pending_emits))
	}
}

func TestTransactionRollbackDropsPendingEmits(t *testing.T) {
	h, cleanup := new_tx_handle(t)
	defer cleanup()

	h.pending_emits = []sql_pending_emit{
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"a", "A"}},
	}

	if _, err := h.sl_rollback(&sl.Thread{}, nil, sl.Tuple{}, nil); err != nil {
		t.Fatalf("sl_rollback: %v", err)
	}
	if !h.closed {
		t.Error("after rollback: handle must be closed")
	}
	if h.pending_emits != nil {
		t.Errorf("after rollback: pending_emits must be nil, got %d entries", len(h.pending_emits))
	}
}

func TestTransactionCloseDropsPendingEmits(t *testing.T) {
	// transaction_close (auto-cleanup at thread tear-down) iterates
	// every uncommitted handle and rolls back. After the cleanup any
	// pending_emits on those handles must be cleared so a forgotten
	// commit doesn't leak emits.
	h, cleanup := new_tx_handle(t)
	defer cleanup()

	h.pending_emits = []sql_pending_emit{
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"a", "A"}},
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"b", "B"}},
	}

	th := &sl.Thread{}
	th.SetLocal("transactions", []*TransactionHandle{h})
	transaction_close(th)

	if !h.closed {
		t.Error("after close: handle must be closed")
	}
	if h.pending_emits != nil {
		t.Errorf("after close: pending_emits must be nil, got %d entries", len(h.pending_emits))
	}
}

func TestTransactionCloseSkipsAlreadyClosed(t *testing.T) {
	// An already-committed handle in the auto-cleanup list must be
	// skipped (we'd panic calling Rollback on a committed tx).
	h, cleanup := new_tx_handle(t)
	defer cleanup()

	if _, err := h.sl_commit(&sl.Thread{}, nil, sl.Tuple{}, nil); err != nil {
		t.Fatalf("sl_commit: %v", err)
	}
	th := &sl.Thread{}
	th.SetLocal("transactions", []*TransactionHandle{h})
	transaction_close(th) // must not panic
}
