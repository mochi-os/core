package main

// Tests for the app-database lifecycle path in db_app (#227): the ready gate,
// the re-entrancy hand-off for database_create's own mochi.db.* calls, and the
// transactional atomicity of a failed creation.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	sqlitedrv "github.com/ncruces/go-sqlite3/driver"
)

// lifecycle_test_app writes a Starlark app source file and returns an
// internal-style App whose single version executes it. The caller mutates
// av.Database.* fields per test.
func lifecycle_test_app(t *testing.T, source string) (*App, *AppVersion) {
	t.Helper()

	// Starlark.call needs the runtime state the server normally sets up at
	// startup via starlark_configure: a non-nil concurrency semaphore (a nil
	// channel blocks forever) and a non-zero timeout (zero cancels instantly).
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 32)
		starlark_default_timeout = 90 * time.Second
	}

	directory := test_data_directory(t)

	star := filepath.Join(directory, "app.star")
	if err := os.WriteFile(star, []byte(source), 0644); err != nil {
		t.Fatalf("write starlark source: %v", err)
	}

	av := &AppVersion{Version: "1.0", Execute: []string{star}}
	av.Database.File = "lifecycle.db"
	av.Database.Schema = 1
	av.Database.Create.Function = "database_create"
	app := &App{id: "lifecycletest", internal: av}
	av.app = app

	t.Cleanup(func() {
		db_purge_prefix("users")
	})
	return app, av
}

// TestAppDatabaseConcurrentFirstAccess (#227): many goroutines open a fresh app
// DB at once - every opener gets a queryable handle, creation runs once, the
// version is stamped.
func TestAppDatabaseConcurrentFirstAccess(t *testing.T) {
	app, _ := lifecycle_test_app(t, `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key autoincrement, name text not null)")
    n = 0
    for i in range(200000):
        n += i
    mochi.db.execute("create table beta (marks integer not null)")
    mochi.db.execute("insert into beta (marks) values (1)")
`)
	u := &User{UID: "racetestuser"}

	const openers = 24
	errs := make([]error, openers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(openers)
	for i := 0; i < openers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			db := db_app(u, app)
			if db == nil {
				errs[i] = fmt.Errorf("db_app returned nil")
				return
			}
			// The schema must be complete the moment db_app returns —
			// this query is what 500ed before the fix.
			if _, err := db.exists("select marks from beta"); err != nil {
				errs[i] = err
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("opener %d: %v", i, err)
		}
	}

	db := db_app(u, app)
	if db == nil {
		t.Fatal("post-race db_app returned nil")
	}
	if marks := db.integer("select count(*) from beta"); marks != 1 {
		t.Fatalf("database_create ran %d times, want exactly once", marks)
	}
	if v := db.integer("pragma user_version"); v != 1 {
		t.Fatalf("user_version = %d, want 1 (stamped atomically with the DDL)", v)
	}
}

// TestAppDatabaseCreateReentrancy: database_create's own mochi.db.* calls must
// not re-enter db_app, and mochi.db.table must see the transaction's
// uncommitted DDL.
func TestAppDatabaseCreateReentrancy(t *testing.T) {
	app, _ := lifecycle_test_app(t, `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key, name text not null)")
    columns = mochi.db.table("alpha")
    if len(columns) != 2:
        fail("mochi.db.table saw %d columns, want 2 (in-transaction DDL invisible)" % len(columns))
    if "alpha" not in mochi.db.tables():
        fail("mochi.db.tables did not see in-transaction DDL")
    mochi.db.execute("insert into alpha (id, name) values (1, 'seed')")
    row = mochi.db.row("select name from alpha where id = 1")
    if row["name"] != "seed":
        fail("read-your-writes failed inside database_create")
`)
	u := &User{UID: "reentrancyuser"}

	db := db_app(u, app)
	if db == nil {
		t.Fatal("db_app returned nil")
	}
	if n := db.integer("select count(*) from alpha"); n != 1 {
		t.Fatalf("alpha rows = %d, want 1", n)
	}
}

// TestAppDatabaseCreateFailureAtomic: a database_create that fails partway
// persists nothing, and the next opener retries rather than reusing a partial
// schema.
func TestAppDatabaseCreateFailureAtomic(t *testing.T) {
	app, av := lifecycle_test_app(t, `
def database_create_bad():
    mochi.db.execute("create table alpha (id integer primary key)")
    fail("simulated create crash")

def database_create():
    mochi.db.execute("create table alpha (id integer primary key)")
    mochi.db.execute("create table beta (marks integer not null)")
    mochi.db.execute("insert into beta (marks) values (1)")
`)
	u := &User{UID: "atomicuser"}

	av.Database.Create.Function = "database_create_bad"
	if db := db_app(u, app); db != nil {
		t.Fatal("db_app succeeded with a failing database_create")
	}

	// The failed attempt must have rolled back: no tables, no version stamp.
	raw := db_open(fmt.Sprintf("users/%s/%s/db/%s", u.UID, app.id, av.Database.File))
	if n := raw.integer("select count(*) from sqlite_master where type='table'"); n != 0 {
		t.Fatalf("failed create persisted %d tables, want 0 (rollback)", n)
	}
	if v := raw.integer("pragma user_version"); v != 0 {
		t.Fatalf("failed create stamped user_version=%d, want 0", v)
	}

	// The next opener retries with the fixed function and succeeds — the
	// handle must not be wedged by the earlier failure.
	av.Database.Create.Function = "database_create"
	db := db_app(u, app)
	if db == nil {
		t.Fatal("db_app still failing after the create function was fixed")
	}
	if marks := db.integer("select count(*) from beta"); marks != 1 {
		t.Fatalf("beta rows = %d, want 1", marks)
	}
	if v := db.integer("pragma user_version"); v != 1 {
		t.Fatalf("user_version = %d, want 1", v)
	}
}

// TestAppDatabaseUpgradeStepAtomic: each database_upgrade step runs in its
// own transaction. A failing step's DDL rolls back, but the version bump is
// preserved (the established failed-migration-consumes-the-version repair
// convention), and mochi.db.table inside a step sees that step's own DDL.
func TestAppDatabaseUpgradeStepAtomic(t *testing.T) {
	app, av := lifecycle_test_app(t, `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key)")

def database_upgrade(version):
    if version == 2:
        mochi.db.execute("alter table alpha add column extra text")
        columns = mochi.db.table("alpha")
        if len(columns) != 2:
            fail("mochi.db.table saw %d columns mid-migration, want 2" % len(columns))
    if version == 3:
        mochi.db.execute("create table gamma (id integer primary key)")
        fail("simulated migration crash")
`)
	u := &User{UID: "upgradeuser"}

	if db := db_app(u, app); db == nil {
		t.Fatal("initial create failed")
	}

	// Ship "2.0" with schema 3: step 2 succeeds, step 3 fails.
	av.Version = "2.0"
	av.Database.Schema = 3
	av.Database.Upgrade.Function = "database_upgrade"
	db := db_app(u, app)
	if db == nil {
		t.Fatal("db_app returned nil after migration wave")
	}
	if v := db.integer("pragma user_version"); v != 3 {
		t.Fatalf("user_version = %d, want 3 (failed step still consumes the version)", v)
	}
	if n := db.integer("select count(*) from pragma_table_info('alpha')"); n != 2 {
		t.Fatalf("alpha has %d columns, want 2 (step 2 committed)", n)
	}
	if n := db.integer("select count(*) from sqlite_master where type='table' and name='gamma'"); n != 0 {
		t.Fatalf("gamma exists — failed step 3's DDL was not rolled back")
	}
}

// TestAppDatabaseUpgradeAbort: a step calling mochi.db.abort() holds the schema version
// and retries verbatim, unlike an ordinary failure, which consumes it.
func TestAppDatabaseUpgradeAbort(t *testing.T) {
	app, av := lifecycle_test_app(t, `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key)")

def database_upgrade(version):
    if version == 2:
        mochi.db.execute("alter table alpha add column extra text")
    if version == 3:
        mochi.db.execute("create table gamma (id integer primary key)")
        mochi.db.abort("transient precondition missing")
`)
	u := &User{UID: "abortuser"}

	if db := db_app(u, app); db == nil {
		t.Fatal("initial create failed")
	}

	// Ship "2.0" with schema 3: step 2 commits, step 3 aborts.
	av.Version = "2.0"
	av.Database.Schema = 3
	av.Database.Upgrade.Function = "database_upgrade"
	db := db_app(u, app)
	if db == nil {
		t.Fatal("db_app returned nil after migration wave")
	}

	// The version is HELD at 2 - the abort did not advance it, so step 3
	// retries next time (contrast the failure case, which reaches 3).
	if v := db.integer("pragma user_version"); v != 2 {
		t.Fatalf("user_version = %d, want 2 (abort must hold the version, not consume it)", v)
	}
	// Step 2's committed DDL survives; step 3's rolled back.
	if n := db.integer("select count(*) from pragma_table_info('alpha')"); n != 2 {
		t.Fatalf("alpha has %d columns, want 2 (step 2 committed)", n)
	}
	if n := db.integer("select count(*) from sqlite_master where type='table' and name='gamma'"); n != 0 {
		t.Fatalf("gamma exists - the aborted step's DDL was not rolled back")
	}

	// A second open retries the aborted step, and it aborts again: still 2.
	db2 := db_app(u, app)
	if v := db2.integer("pragma user_version"); v != 2 {
		t.Fatalf("user_version = %d after retry, want 2 (a persistently aborting step never advances)", v)
	}
}

// TestLifecycleAuthoriser: the dedicated lifecycle connection allows the
// user_version stamp but keeps every other Starlark-pool restriction.
func TestLifecycleAuthoriser(t *testing.T) {
	tmp := test_data_directory(t)

	pool, err := sqlitedrv.Open(filepath.Join(tmp, "authoriser.db"), db_setup_conn_lifecycle)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec("pragma user_version = 7"); err != nil {
		t.Fatalf("user_version write must be allowed on the lifecycle connection: %v", err)
	}
	if _, err := pool.Exec("create table alpha (id integer primary key)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	rows, err := pool.Query("PRAGMA table_info(alpha)")
	if err != nil {
		t.Fatalf("table_info must be allowed on the lifecycle connection: %v", err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if n != 1 {
		t.Fatalf("table_info returned %d rows, want 1", n)
	}
	if _, err := pool.Exec("pragma max_page_count = 999999999"); err == nil {
		t.Fatal("max_page_count write must stay denied on the lifecycle connection")
	}
	if _, err := pool.Exec("create trigger t1 after insert on sqlite_master begin select 1; end"); err == nil {
		t.Fatal("trigger creation must stay denied on the lifecycle connection")
	}
}

// TestAppDatabaseUncommittedDiscarded simulates a process death mid-create:
// an uncommitted transaction on a lifecycle connection must leave nothing
// behind when the connection is torn down without commit.
func TestAppDatabaseUncommittedDiscarded(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "crash.db")

	pool, err := sqlitedrv.Open(path, db_setup_conn_lifecycle)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := pool.Exec("begin immediate"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := pool.Exec("create table alpha (id integer primary key)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := pool.Exec("pragma user_version = 1"); err != nil {
		t.Fatalf("stamp: %v", err)
	}
	// No commit — tear the connection down as a crash would.
	pool.Close()

	after, err := sqlitedrv.Open(path, db_setup_conn_lifecycle)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer after.Close()
	var tables int
	if err := after.QueryRow("select count(*) from sqlite_master where type='table'").Scan(&tables); err != nil {
		t.Fatalf("count: %v", err)
	}
	if tables != 0 {
		t.Fatalf("uncommitted create persisted %d tables, want 0", tables)
	}
	var version int
	if err := after.QueryRow("pragma user_version").Scan(&version); err != nil {
		t.Fatalf("user_version: %v", err)
	}
	if version != 0 {
		t.Fatalf("uncommitted stamp persisted user_version=%d, want 0", version)
	}
}

// TestAppDatabaseCreateTransactionBlocked: mochi.db.transaction inside a
// lifecycle function must fail cleanly (the lifecycle already runs in a
// transaction; a nested one would block on its own write lock), and the
// aborted create must roll back.
func TestAppDatabaseCreateTransactionBlocked(t *testing.T) {
	app, av := lifecycle_test_app(t, `
def database_create():
    mochi.db.execute("create table alpha (id integer primary key)")
    tx = mochi.db.transaction()
`)
	u := &User{UID: "txblockuser"}

	if db := db_app(u, app); db != nil {
		t.Fatal("db_app succeeded despite mochi.db.transaction inside database_create")
	}
	raw := db_open(fmt.Sprintf("users/%s/%s/db/%s", u.UID, app.id, av.Database.File))
	if n := raw.integer("select count(*) from sqlite_master where type='table'"); n != 0 {
		t.Fatalf("aborted create persisted %d tables, want 0", n)
	}
}
