// Mochi server: Database pool lifecycle tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	sl "go.starlark.net/starlark"
)

// pool_test_database opens a real *DB under a temporary data directory, the way
// the server does, and gives it one table to work with.
func pool_test_database(t *testing.T, name string) *DB {
	t.Helper()
	test_data_directory(t)
	db, _, _ := db_open_work(name)
	if db == nil {
		t.Fatalf("opening %q", name)
	}
	t.Cleanup(func() { db_purge_prefix("") })
	if err := db.exec_e("create table t (a integer)"); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	return db
}

// busy_timeouts holds every connection the internal pool can give out at once
// and reports each one's busy timeout, so a value left behind on any of them is
// visible. Holding them together is what makes the check exhaustive - releasing
// between reads could hand back the same clean connection twice.
func busy_timeouts(t *testing.T, db *DB, count int) []int64 {
	t.Helper()
	out := []int64{}
	for i := 0; i < count; i++ {
		conn, err := db.internal.Conn(context.Background())
		if err != nil {
			t.Fatalf("checkout: %v", err)
		}
		defer conn.Close()
		out = append(out, db_busy_timeout(conn))
	}
	return out
}

// TestStarlarkExecuteCannotLeakATransaction drives the real api_db_query path:
// an app opening a transaction it never closes must not hand it to the next
// caller, who could read and commit another request's uncommitted work.
func TestStarlarkExecuteCannotLeakATransaction(t *testing.T) {
	db := pool_test_database(t, "leak.db")
	// One connection makes the next checkout deterministically the one the call
	// just released.
	db.starlark.SetMaxOpenConns(1)

	thread := &sl.Thread{}
	thread.SetLocal("context", context.Background())
	thread.SetLocal("database", db) // db_for_thread short-circuits on this

	execute := func(query string) error {
		fn := sl.NewBuiltin("mochi.db.execute", api_db_query)
		_, err := api_db_query(thread, fn, sl.Tuple{sl.String(query)}, nil)
		return err
	}

	if err := execute("begin immediate"); err != nil {
		t.Fatalf("the authoriser refused BEGIN, so this test no longer measures the leak: %v", err)
	}

	conn, err := db.starlark.Connx(context.Background())
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	defer conn.Close()
	settled := false
	_ = conn.Raw(func(driver any) error {
		type autocommit interface{ GetAutocommit() bool }
		if c, ok := driver.(autocommit); ok {
			settled = c.GetAutocommit()
		}
		return nil
	})
	if !settled {
		t.Error("the connection returned to the pool inside a transaction; the next request would read and could commit another request's uncommitted work")
	}
}

// TestVacuumRestoresTheBusyTimeout: vacuum lowers the timeout to 30 s for its own
// work. busy_timeout is per connection and nothing in the driver resets it, so
// leaving it lowered hands every later caller on that connection the short wait.
func TestVacuumRestoresTheBusyTimeout(t *testing.T) {
	db := pool_test_database(t, "vacuum.db")
	// vacuum checks out a dedicated connection and calls db.integer (which takes
	// another) while holding it, so a cap of one deadlocks. Two is the smallest
	// cap that lets it run and still lets the test inspect every connection.
	db.internal.SetMaxOpenConns(2)
	db.internal.SetMaxIdleConns(2)

	before := busy_timeouts(t, db, 2)[0]
	if before <= 0 {
		t.Fatalf("busy_timeout unreadable (%d)", before)
	}

	// Both vacuum gates: a quarter of the pages free and over 8 MB reclaimable.
	if err := db.exec_e("create table bulk (a text)"); err != nil {
		t.Fatalf("bulk table: %v", err)
	}
	// One statement: a row-at-a-time loop here is tens of thousands of separate
	// transactions and takes minutes.
	if err := db.exec_e("insert into bulk (a) with recursive counter(x) as (select 1 union all select x+1 from counter where x<24000) select hex(randomblob(512)) from counter"); err != nil {
		t.Fatalf("bulk fill: %v", err)
	}
	if err := db.exec_e("delete from bulk"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	db.vacuum()

	for i, after := range busy_timeouts(t, db, 2) {
		if after != before {
			t.Errorf("connection %d reports busy_timeout %d ms after vacuum, want the %d ms it started at: the value rides back into the pool and every later caller on that connection inherits it", i, after, before)
		}
	}
}

// TestWatchdogRestoresTheBusyTimeout: same defect on the checkpoint path, which
// lowers the wait to 1 s. db.internal is the pool core's own writes use, and
// db.exec wraps must(), so a connection left at 1 s turns an ordinary contended
// write into a panicked request.
func TestWatchdogRestoresTheBusyTimeout(t *testing.T) {
	db := pool_test_database(t, "watchdog.db")
	db.internal.SetMaxOpenConns(2)
	db.internal.SetMaxIdleConns(2)

	before := busy_timeouts(t, db, 2)[0]
	if before <= 0 {
		t.Fatalf("busy_timeout unreadable (%d)", before)
	}

	// The watchdog acts only past db_wal_warn_bytes. Lower it rather than build
	// a 256 MB write-ahead log.
	original := db_wal_warn_bytes
	db_wal_warn_bytes = 1
	t.Cleanup(func() { db_wal_warn_bytes = original })
	if err := db.exec_e("insert into t (a) values (1)"); err != nil {
		t.Fatalf("seeding the WAL: %v", err)
	}
	if information, err := os.Stat(db.path + "-wal"); err != nil || information.Size() == 0 {
		t.Skipf("no write-ahead log to checkpoint (%v)", err)
	}

	db_wal_watchdog()

	for i, after := range busy_timeouts(t, db, 2) {
		if after != before {
			t.Errorf("connection %d reports busy_timeout %d ms after the watchdog ran, want the %d ms it started at", i, after, before)
		}
	}
}

// TestSystemSweepReleasesItsHandles: db_app_system_sweep walks every app.db on
// the host at startup. db_open_work creates handles as in-use, and db_manager
// evicts only released ones, so a sweep that never releases pins every database
// it touched for the life of the process.
func TestSystemSweepReleasesItsHandles(t *testing.T) {
	directory := test_data_directory(t)
	t.Cleanup(func() { db_purge_prefix("") })

	paths := []string{}
	for _, app := range []string{"alpha", "beta", "gamma"} {
		path := fmt.Sprintf("users/sweepuser/%s/app.db", app)
		if err := os.MkdirAll(filepath.Dir(filepath.Join(directory, path)), 0755); err != nil {
			t.Fatalf("directory: %v", err)
		}
		db, _, _ := db_open_work(path)
		if db == nil {
			t.Fatalf("creating %q", path)
		}
		db.close() // created here, not by the sweep; the sweep will reuse it
		paths = append(paths, filepath.Join(directory, path))
	}

	// Drop them from the cache so the sweep is the opener, which is the case the
	// fix is about.
	databases_lock.Lock()
	for _, path := range paths {
		delete(databases, path)
	}
	databases_lock.Unlock()

	db_app_system_sweep()

	databases_lock.Lock()
	defer databases_lock.Unlock()
	held := []string{}
	for _, path := range paths {
		db, found := databases[path]
		if !found {
			continue // already evicted, which is the same outcome
		}
		if db.closed == 0 {
			held = append(held, path)
		}
	}
	if len(held) > 0 {
		t.Errorf("the sweep left %d of %d handles marked in use, so db_manager can never evict them: %v", len(held), len(paths), held)
	}
}

// cache_size reports how many prepared statements a handle currently holds.
func cache_size(db *DB) int {
	db.statement_lock.Lock()
	defer db.statement_lock.Unlock()
	return len(db.statement_cache)
}

// TestUserSetupRunsOncePerHandle: db_user's table block is DDL, and every DDL
// statement flushes the handle's prepared statements. Re-running it on each call
// left user.db - reached from the access check and every routing lookup - with a
// permanently empty cache.
func TestUserSetupRunsOncePerHandle(t *testing.T) {
	test_data_directory(t)
	t.Cleanup(func() { db_purge_prefix("") })
	u := &User{UID: "setupuser"}

	db := db_user(u, "user")
	if db == nil {
		t.Fatal("db_user returned nil")
	}
	// The setup must still have run: the tables it creates have to exist.
	for _, table := range []string{"preferences", "accounts", "devices", "interests", "settings"} {
		if found, _ := db.exists("select 1 from sqlite_master where type='table' and name=?", table); !found {
			t.Fatalf("table %q missing - the setup block did not run on first open", table)
		}
	}

	// Warm the cache the way a request does.
	for i := 0; i < 5; i++ {
		_, _ = db.exists("select 1 from preferences where name=?", "x")
		_ = db.exec_e("insert or replace into preferences (name, value) values (?, ?)", "k", "v")
		_, _ = db.row("select value from preferences where name=?", "k")
	}
	warm := cache_size(db)
	if warm == 0 {
		t.Fatal("cache never warmed, so this test cannot measure the flush")
	}

	if again := db_user(u, "user"); again != db {
		t.Fatal("db_user returned a different handle; this test assumes the cached one")
	}
	if after := cache_size(db); after != warm {
		t.Errorf("cache went from %d to %d statements across one more db_user call; the setup block is still running per call and flushing it", warm, after)
	}
}

// TestCommitsSetupLeavesTheHandleInUse: commits_setup hands the handle to
// commit_hook_fire, which keeps using it for the drain and the append. Marking
// it idle here lets db_manager evict it - closing both pools - out from under a
// drain that outlives the 60 s window.
func TestCommitsSetupLeavesTheHandleInUse(t *testing.T) {
	app, _ := lifecycle_test_app(t, `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key)")
`)
	u := &User{UID: "commitsuser"}

	sys := commits_setup(u, app)
	if sys == nil {
		t.Fatal("commits_setup returned nil")
	}
	databases_lock.Lock()
	closed := sys.closed
	databases_lock.Unlock()
	if closed != 0 {
		t.Errorf("commits_setup returned a handle marked idle (closed=%d); db_manager evicts it after 60 s and closes both pools while commit_hook_fire is still draining through it", closed)
	}
}

// TestSystemDatabaseOwnsTheCommitsTable: creating it once here is what keeps
// commit_hook_fire from running the same DDL three times per fired commit.
func TestSystemDatabaseOwnsTheCommitsTable(t *testing.T) {
	app, _ := lifecycle_test_app(t, `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key)")
`)
	u := &User{UID: "commitstable"}

	sys := db_app_system(u, app)
	if sys == nil {
		t.Fatal("db_app_system returned nil")
	}
	if found, _ := sys.exists("select 1 from sqlite_master where type='table' and name='commits'"); !found {
		t.Fatal("db_app_system did not create the commits table, so the commit hook has to create it itself")
	}

	for i := 0; i < 5; i++ {
		_, _ = sys.exists("select 1 from commits where seq=?", 1)
		_, _ = sys.row("select count(*) as n from commits")
	}
	warm := cache_size(sys)
	if warm == 0 {
		t.Fatal("cache never warmed, so this test cannot measure the flush")
	}

	commits_append(sys, "alpha", "insert", "row1")
	if after := cache_size(sys); after != warm {
		t.Errorf("cache went from %d to %d statements across one commits_append; it is still running the table DDL per call", warm, after)
	}
}
