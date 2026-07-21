// Mochi server: Starlark nested-call concurrency tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// TestStarlarkNestedCallReleasesSlot pins that a Starlark call which re-enters
// Starlark.call — which mochi.service.call does, at api.go's `s.call(...)` —
// cannot permanently consume a concurrency slot.
//
// The semaphore is not reentrant: the outer call holds a slot while the inner
// one tries to acquire another. When every slot is taken, the inner acquire
// blocks, and a channel send is not interrupted by thread.Cancel, so the
// outer goroutine can never reach its cleanup. Whether the slot comes back
// decides between a transient stall and a permanent loss of capacity, and
// losing every slot locks the whole Starlark engine: the acquire at the top
// of Starlark.call runs before any timeout handling, so callers block forever
// with no timeout to rescue them.
func TestStarlarkNestedCallReleasesSlot(t *testing.T) {
	original_sem := starlark_sem
	original_timeout := starlark_default_timeout
	starlark_sem = make(chan struct{}, 1) // one slot makes exhaustion deterministic
	starlark_default_timeout = 1 * time.Second
	t.Cleanup(func() {
		starlark_sem = original_sem
		starlark_default_timeout = original_timeout
	})

	// The callee, standing in for the app a service call dispatches to.
	inner := &Starlark{thread: &sl.Thread{Name: "inner"}, globals: sl.StringDict{}}
	inner_globals, err := sl.ExecFile(inner.thread, "inner.star", "def inner_function():\n    return 1\n", inner.globals)
	if err != nil {
		t.Fatalf("load inner: %v", err)
	}
	inner.globals = inner_globals

	// A builtin that re-enters Starlark.call, exactly as mochi.service.call does.
	nested := sl.NewBuiltin("nested", func(_ *sl.Thread, _ *sl.Builtin, _ sl.Tuple, _ []sl.Tuple) (sl.Value, error) {
		return inner.call("inner_function", nil)
	})
	outer := &Starlark{thread: &sl.Thread{Name: "outer"}, globals: sl.StringDict{"nested": nested}}
	outer_globals, err := sl.ExecFile(outer.thread, "outer.star", "def outer_function():\n    return nested()\n", outer.globals)
	if err != nil {
		t.Fatalf("load outer: %v", err)
	}
	outer.globals = outer_globals

	// Run the nested call. It is expected to fail — the point is what it
	// leaves behind, not what it returns.
	done := make(chan struct{})
	go func() {
		defer close(done)
		outer.call("outer_function", nil)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the nested call never returned to its caller")
	}

	// The slot must be available again. A later, unrelated call is how a real
	// server would discover it is not.
	after := &Starlark{thread: &sl.Thread{Name: "after"}, globals: sl.StringDict{}}
	after_globals, err := sl.ExecFile(after.thread, "after.star", "def after_function():\n    return 1\n", after.globals)
	if err != nil {
		t.Fatalf("load after: %v", err)
	}
	after.globals = after_globals

	recovered := make(chan struct{})
	go func() {
		defer close(recovered)
		after.call("after_function", nil)
	}()
	select {
	case <-recovered:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrency slot was never released: a later Starlark call could not acquire one, so the engine is deadlocked")
	}
}

// TestStarlarkQueryInterrupted pins that a timed-out call stops its database
// statement. thread.Cancel is only observed between interpreter steps, so a
// query already inside SQLite would otherwise run to completion, holding a
// connection and a concurrency slot long after the caller gave up.
func TestStarlarkQueryInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	thread := &sl.Thread{Name: "query"}
	thread.SetLocal("context", ctx)

	if got := starlark_context(thread); got != ctx {
		t.Fatal("starlark_context did not return the thread's context")
	}
	// Outside a Starlark call there is no context to inherit; a background
	// one keeps such callers working rather than failing closed.
	if got := starlark_context(&sl.Thread{Name: "bare"}); got == nil {
		t.Fatal("starlark_context returned nil for a thread with no context")
	}

	// A cancelled context must actually stop a statement on this driver, or
	// the plumbing above is decoration.
	database := db_open(filepath.Join(t.TempDir(), "interrupt.db"))
	if database == nil {
		t.Skip("could not open a test database")
	}
	conn, err := database.starlark.Connx(context.Background())
	if err != nil {
		t.Skipf("could not check out a connection: %v", err)
	}
	defer conn.Close()

	cancel()
	// recursive CTE that would otherwise run essentially forever
	_, err = conn.ExecContext(ctx, "with recursive c(x) as (select 1 union all select x+1 from c) select count(*) from c")
	if err == nil {
		t.Fatal("a cancelled context did not interrupt the statement")
	}
	if !errors.Is(err, context.Canceled) {
		t.Logf("statement stopped with: %v", err)
	}
}
