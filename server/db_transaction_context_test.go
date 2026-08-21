// Mochi server: a timed-out call must not leave a write lock held.
//
// thread.Cancel is only observed between interpreter steps, so a statement
// already inside SQLite runs to completion - the transaction must carry the
// call's context.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	sl "go.starlark.net/starlark"
)

// transaction_context_env opens a database with the production Starlark
// authoriser and returns it plus a thread carrying a context the test cancels.
func transaction_context_env(t *testing.T) (*sqlx.DB, *sl.Thread, context.CancelFunc) {
	t.Helper()
	raw, err := sqlitedrv.Open(filepath.Join(t.TempDir(), "app.db"), db_setup_conn_starlark)
	if err != nil {
		t.Fatalf("opening the database: %v", err)
	}
	db := sqlx.NewDb(raw, "sqlite3")
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("create table t (a integer)"); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	thread := &sl.Thread{}
	thread.SetLocal("context", ctx)
	return db, thread, cancel
}

// transaction_handle opens a transaction the way api_db_transaction does.
func transaction_handle(t *testing.T, db *sqlx.DB, thread *sl.Thread) *TransactionHandle {
	t.Helper()
	tx, err := db.BeginTxx(starlark_context(thread), nil)
	if err != nil {
		t.Fatalf("beginning a transaction: %v", err)
	}
	return &TransactionHandle{tx: tx}
}

// transaction_execute runs one statement through the production method.
func transaction_execute(thread *sl.Thread, h *TransactionHandle, query string) error {
	fn := sl.NewBuiltin("mochi.db.transaction.execute", h.sl_execute)
	_, err := h.sl_execute(thread, fn, sl.Tuple{sl.String(query)}, nil)
	return err
}

// TestACancelledCallStopsATransactionStatement is the defect: the statement
// never saw the cancellation, so it ran on holding the write lock.
func TestACancelledCallStopsATransactionStatement(t *testing.T) {
	db, thread, cancel := transaction_context_env(t)
	h := transaction_handle(t, db, thread)

	if err := transaction_execute(thread, h, "insert into t (a) values (1)"); err != nil {
		t.Fatalf("the first statement failed before any cancellation: %v", err)
	}

	cancel()

	if err := transaction_execute(thread, h, "insert into t (a) values (2)"); err == nil {
		t.Error("a statement ran after the call was cancelled; a timed-out call keeps its write lock until the statement finishes on its own")
	}
}

// TestACancelledCallReleasesTheTransaction is why the Beginx mattered as much
// as the statements: with a context on the transaction, database/sql rolls it
// back and returns the connection rather than waiting for a cleanup hook that
// is itself blocked.
func TestACancelledCallReleasesTheTransaction(t *testing.T) {
	db, thread, cancel := transaction_context_env(t)
	h := transaction_handle(t, db, thread)

	if err := transaction_execute(thread, h, "insert into t (a) values (7)"); err != nil {
		t.Fatalf("the insert failed: %v", err)
	}
	cancel()

	// database/sql rolls back asynchronously when the context is done, so give
	// it a moment rather than racing it. Each read is bounded: while the
	// transaction is still open it holds the connection, so an unbounded Get
	// waits on the pool instead of on this loop's deadline.
	deadline := time.Now().Add(5 * time.Second)
	for {
		read, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
		var total int
		err := db.GetContext(read, &total, "select count(*) from t where a=7")
		stop()
		if err == nil && total == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the uncommitted row is still pending after the call was cancelled; the transaction was not opened under the call's context, so nothing rolls it back")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAnUncancelledTransactionStillCommits guards against the context turning
// ordinary work into an error.
func TestAnUncancelledTransactionStillCommits(t *testing.T) {
	db, thread, _ := transaction_context_env(t)
	h := transaction_handle(t, db, thread)

	for _, query := range []string{
		"insert into t (a) values (3)",
		"update t set a=4 where a=3",
	} {
		if err := transaction_execute(thread, h, query); err != nil {
			t.Fatalf("%q failed: %v", query, err)
		}
	}

	fn := sl.NewBuiltin("mochi.db.transaction.commit", h.sl_commit)
	if _, err := h.sl_commit(thread, fn, nil, nil); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	var total int
	if err := db.Get(&total, "select count(*) from t where a=4"); err != nil || total != 1 {
		t.Errorf("after commit the row count is %d (err %v), want 1", total, err)
	}
}

// TestEveryReadMethodTakesTheContext: execute is not the only one that blocks
// inside SQLite, and a read holding a snapshot open is the same problem.
func TestEveryReadMethodTakesTheContext(t *testing.T) {
	db, thread, cancel := transaction_context_env(t)
	h := transaction_handle(t, db, thread)
	cancel()

	for name, method := range map[string]func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error){
		"exists": h.sl_exists,
		"row":    h.sl_row,
		"rows":   h.sl_rows,
	} {
		fn := sl.NewBuiltin("mochi.db.transaction."+name, method)
		if _, err := method(thread, fn, sl.Tuple{sl.String("select a from t")}, nil); err == nil {
			t.Errorf("%s ran after the call was cancelled", name)
		}
	}
}

// TestNoTransactionCallDropsTheContext pins the shape, including the begin.
// Every one of these methods already receives the thread; using the
// non-Context variant is dropping a context that is in hand.
func TestNoTransactionCallDropsTheContext(t *testing.T) {
	data, err := os.ReadFile("db.go")
	if err != nil {
		t.Fatalf("reading db.go: %v", err)
	}
	source := string(data)
	for _, dropped := range []string{
		"h.tx.Exec(",
		"h.tx.Query(",
		"h.tx.Queryx(",
		"db.starlark.Beginx(",
	} {
		if strings.Contains(source, dropped) {
			t.Errorf("db.go still calls %s, which runs on context.Background(); a cancelled call cannot stop it", dropped)
		}
	}
	for _, wanted := range []string{
		"h.tx.ExecContext(starlark_context(t)",
		"h.tx.QueryContext(starlark_context(t)",
		"h.tx.QueryxContext(starlark_context(t)",
		"db.starlark.BeginTxx(starlark_context(t)",
	} {
		if !strings.Contains(source, wanted) {
			t.Errorf("db.go no longer contains %s", wanted)
		}
	}
}
