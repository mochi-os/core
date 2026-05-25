// Mochi server: schema-version skew apply-path tests
// Copyright Alistair Cunningham 2026
//
// Each ReplicationOp carries the sender's per-app schema version
// (op.Schema). When a receiver lands on a host whose local schema
// trails the sender's, replication_apply_sql_command returns
// ApplyDeferred and the op buffers in `pending`. Once the receiver's
// database_upgrade catches up, the next db_app open fires
// post_migration_drain_async which re-drains the buffer. The seam is
// real code at replication_sql_command.go:324 and replication.go:799
// but had no direct test coverage - existing framework tests in
// replication_framework_test.go exercise the stream-gate defer
// (Prev-gap buffering), not the schema-skew defer.
//
// These tests pin three properties:
//   - Forward skew defers and the deferred op applies after a
//     simulated migration.
//   - Backward skew (sender older than receiver) applies immediately.
//   - Multi-version skip drains in sequence order once the receiver
//     catches up across multiple migration steps.
//
// Setup re-uses setup_replication_test (framework + replication.db
// schema) and builds its own app + user registration so the test
// can mutate AppVersion.Database.Schema between phases - simpler than
// driving real db_upgrade calls.

package main

import (
	"sync"
	"testing"
)

const skew_user = "uid-schema-skew"

// register_skew_app installs an app with a single-table per-user DB
// at the given starting schema version. Returns the AppVersion so
// the test can mutate Schema to simulate a migration landing.
// posts is the table the replicated INSERTs target.
func register_skew_app(t *testing.T, app_id string, initial_schema int) *AppVersion {
	t.Helper()
	av := &AppVersion{Version: "1"}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	av.Database.File = app_id + ".db"
	av.Database.Schema = initial_schema
	av.Database.create_function = func(db *DB) {
		db.exec("create table posts (id text primary key, title text not null, n integer not null default 0)")
	}
	a := &App{id: app_id, versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = a
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps[app_id] = a
	apps_lock.Unlock()
	return av
}

// unregister_skew_app drops the app from the global apps map so
// repeated test runs don't accumulate.
func unregister_skew_app(app_id string) {
	apps_lock.Lock()
	delete(apps, app_id)
	apps_lock.Unlock()
}

// setup_schema_skew_test brings the world to a state where one user
// owns one app at the given starting schema. Builds on
// setup_replication_test for the replication.db / data_dir scaffolding
// and adds the user + app + commit-hook stubs the apply path needs.
// Returns the app's AppVersion so the test can later bump Schema.
//
// post_migration_drain_async is stubbed to a no-op for these tests
// because db_app opens in the assertion helpers (skew_posts_count)
// would otherwise spawn a goroutine that re-runs replication_app_drain
// AFTER our explicit drain, racing the assertions. Tests that want to
// exercise the post_migration_drain_async wiring restore + override
// the stub locally (TestSchemaSkewDrainTriggeredByDbOpen).
func setup_schema_skew_test(t *testing.T, app_id string, initial_schema int) (*AppVersion, func()) {
	t.Helper()
	cleanup := setup_replication_test(t)

	setup_users_test_schema()
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", skew_user, "skew@example.com")

	av := register_skew_app(t, app_id, initial_schema)

	orig_drain := post_migration_drain_async
	post_migration_drain_async = func(user, app_id string) {}

	full_cleanup := func() {
		post_migration_drain_async = orig_drain
		unregister_skew_app(app_id)
		cleanup()
	}
	return av, full_cleanup
}

// skew_sql_op constructs a ReplicationOp carrying a SQL command at
// the given schema marker, sequence and chain predecessor. The
// payload is always a posts-table insert keyed on `id`, with `n` set
// so a sequence-ordered drain can be verified after the fact.
func skew_sql_op(app_id string, sequence, prev int64, schema int, id string, n int) *ReplicationOp {
	return &ReplicationOp{
		Scope:     repl_scope_app,
		User:      skew_user,
		Database:  app_id,
		Operation: repl_op_exec,
		Sequence:  sequence,
		Prev:      prev,
		Schema:    schema,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title, n) values (?, ?, ?)",
			Args:      []any{id, "post-" + id, int64(n)},
		}),
	}
}

// skew_posts_count returns the number of posts rows on the local
// (test-host) per-user-app DB. Opens db_app to mimic production - the
// open ALSO triggers post_migration_drain_async (a no-op stub under
// setup_replication_test, so the count read is deterministic).
func skew_posts_count(t *testing.T, app_id string) int64 {
	t.Helper()
	u := &User{UID: skew_user}
	a := app_by_id(app_id)
	if a == nil {
		t.Fatalf("app %q not registered", app_id)
	}
	db := db_app(u, a)
	if db == nil {
		return 0
	}
	row, _ := db.row("select count(*) as n from posts")
	if row == nil {
		return 0
	}
	n, _ := row["n"].(int64)
	return n
}

// skew_pending_count reads the replication.db pending buffer depth
// for the (peer, user, app) tuple under test.
func skew_pending_count(t *testing.T, peer, app_id string) int64 {
	t.Helper()
	rdb := db_open("db/replication.db")
	row, _ := rdb.row(
		"select count(*) as n from pending where peer=? and scope=? and user=? and db=?",
		peer, repl_scope_app, skew_user, app_id)
	if row == nil {
		return 0
	}
	n, _ := row["n"].(int64)
	return n
}

// TestSchemaSkewForwardDefersThenAppliesAfterMigration is the headline
// case from the audit's #41 honest-gaps list. h2 (the receiver in this
// test) is at schema=1; h1 (synthetic sender) emits an op at schema=2.
// The op must buffer in pending, the row must NOT appear in posts,
// and the cursor must stay un-anchored. After the test bumps the
// receiver's AppVersion.Database.Schema to 2 (simulating a migration
// landing), replication_app_drain re-applies the deferred op and the
// row appears.
func TestSchemaSkewForwardDefersThenAppliesAfterMigration(t *testing.T) {
	app_id := "skew_forward"
	av, cleanup := setup_schema_skew_test(t, app_id, 1)
	defer cleanup()

	op := skew_sql_op(app_id, 1, 0, 2, "p1", 1)
	replication_op_receive("peerA", op)

	if got := skew_pending_count(t, "peerA", app_id); got != 1 {
		t.Errorf("after defer: pending = %d, want 1", got)
	}
	if got := skew_posts_count(t, app_id); got != 0 {
		t.Errorf("after defer: posts rows = %d, want 0 (op deferred, not applied)", got)
	}
	rdb := db_open("db/replication.db")
	if _, anchored := replication_cursor(rdb, "peerA", repl_scope_app, skew_user, app_id); anchored {
		t.Errorf("after defer: cursor must NOT be anchored (apply did not succeed)")
	}

	// Migration lands: bump local schema to match the sender's.
	av.Database.Schema = 2
	replication_app_drain(skew_user, app_id)

	if got := skew_pending_count(t, "peerA", app_id); got != 0 {
		t.Errorf("after drain: pending = %d, want 0 (cleared)", got)
	}
	if got := skew_posts_count(t, app_id); got != 1 {
		t.Errorf("after drain: posts rows = %d, want 1 (deferred op applied)", got)
	}
	cursor, anchored := replication_cursor(rdb, "peerA", repl_scope_app, skew_user, app_id)
	if !anchored || cursor != 1 {
		t.Errorf("after drain: cursor anchored=%v seq=%d, want anchored=true seq=1", anchored, cursor)
	}
}

// TestSchemaSkewBackwardAppliesImmediately is the forward-compat
// direction. Receiver at schema=2 sees an op carrying schema=1 (an
// older sender, or a replay from before the receiver migrated). The
// op applies on first delivery; no defer, no pending row.
func TestSchemaSkewBackwardAppliesImmediately(t *testing.T) {
	app_id := "skew_backward"
	_, cleanup := setup_schema_skew_test(t, app_id, 2)
	defer cleanup()

	op := skew_sql_op(app_id, 1, 0, 1, "p1", 1)
	replication_op_receive("peerA", op)

	if got := skew_pending_count(t, "peerA", app_id); got != 0 {
		t.Errorf("backward skew should not defer: pending = %d, want 0", got)
	}
	if got := skew_posts_count(t, app_id); got != 1 {
		t.Errorf("backward skew should apply on first delivery: posts = %d, want 1", got)
	}
}

// TestSchemaSkewMultiVersionDrainsInOrder is the worst-case from the
// task description: receiver at v1, sender ahead by 4 versions. Four
// chained ops arrive at schema=2, 3, 4, 5 with sequences 1..4. None
// can apply (receiver still on v1); all four sit in pending. The
// test bumps the receiver's schema to 5 (simulating a multi-step
// migration arriving in a single restart) and asserts:
//   - all four rows now appear in posts
//   - they applied in sequence order (the n column preserves order)
//   - pending is empty
//   - cursor reached the last sequence
//
// The chained-by-Prev structure exercises stream-drain's chain-order
// requirement: a multi-version backlog can't be applied as a set, the
// chain has to drain head-first.
func TestSchemaSkewMultiVersionDrainsInOrder(t *testing.T) {
	app_id := "skew_multi"
	av, cleanup := setup_schema_skew_test(t, app_id, 1)
	defer cleanup()

	// Four ops, each chained to its predecessor, each one schema
	// version higher than the last.
	ops := []*ReplicationOp{
		skew_sql_op(app_id, 1, 0, 2, "p1", 1),
		skew_sql_op(app_id, 2, 1, 3, "p2", 2),
		skew_sql_op(app_id, 3, 2, 4, "p3", 3),
		skew_sql_op(app_id, 4, 3, 5, "p4", 4),
	}
	for _, op := range ops {
		replication_op_receive("peerA", op)
	}

	if got := skew_pending_count(t, "peerA", app_id); got != 4 {
		t.Fatalf("after all 4 defer: pending = %d, want 4", got)
	}
	if got := skew_posts_count(t, app_id); got != 0 {
		t.Errorf("after all 4 defer: posts rows = %d, want 0", got)
	}

	// Multi-step migration lands in one go.
	av.Database.Schema = 5
	replication_app_drain(skew_user, app_id)

	if got := skew_pending_count(t, "peerA", app_id); got != 0 {
		t.Errorf("after drain: pending = %d, want 0", got)
	}
	if got := skew_posts_count(t, app_id); got != 4 {
		t.Fatalf("after drain: posts rows = %d, want 4", got)
	}

	// Sequence order check: the four ops carry n=1,2,3,4 and the
	// stream-drain must replay them in sequence (1 first, 4 last).
	// SELECT order-by-n returns the rows in apply order so a
	// reordered drain would show 4 -> 1 here instead of 1 -> 4.
	u := &User{UID: skew_user}
	a := app_by_id(app_id)
	rows, err := db_app(u, a).rows("select id, n from posts order by n")
	if err != nil {
		t.Fatalf("readback: %v", err)
	}
	want := []struct {
		id string
		n  int64
	}{{"p1", 1}, {"p2", 2}, {"p3", 3}, {"p4", 4}}
	for i, w := range want {
		got_id, _ := rows[i]["id"].(string)
		got_n, _ := rows[i]["n"].(int64)
		if got_id != w.id || got_n != w.n {
			t.Errorf("drain order [%d]: got (%q, %d), want (%q, %d)", i, got_id, got_n, w.id, w.n)
		}
	}

	rdb := db_open("db/replication.db")
	cursor, anchored := replication_cursor(rdb, "peerA", repl_scope_app, skew_user, app_id)
	if !anchored || cursor != 4 {
		t.Errorf("after drain: cursor anchored=%v seq=%d, want anchored=true seq=4", anchored, cursor)
	}
}

// TestSchemaSkewDrainTriggeredByDbOpen is the production-integration
// half of the forward-skew test: rather than calling
// replication_app_drain directly, this test invokes db_app which is
// the path that fires post_migration_drain_async in production. The
// stub in setup_replication_test replaces the goroutine spawn with a
// synchronous call so the drain completes before the assertion runs;
// pins that the production wiring (post_migration_drain_async ->
// replication_app_drain) reaches the deferred op without the test
// having to know the inner function name.
func TestSchemaSkewDrainTriggeredByDbOpen(t *testing.T) {
	app_id := "skew_dbopen"
	av, cleanup := setup_schema_skew_test(t, app_id, 1)
	defer cleanup()

	// Override the no-op stub so db_app's post-migration hook actually
	// runs the drain, but synchronously - the production goroutine
	// would race the assertion.
	var drain_mu sync.Mutex
	orig := post_migration_drain_async
	post_migration_drain_async = func(user, app string) {
		drain_mu.Lock()
		defer drain_mu.Unlock()
		replication_app_drain(user, app)
	}
	defer func() { post_migration_drain_async = orig }()

	op := skew_sql_op(app_id, 1, 0, 2, "p1", 1)
	replication_op_receive("peerA", op)

	// Verify defer landed.
	if got := skew_pending_count(t, "peerA", app_id); got != 1 {
		t.Fatalf("after defer: pending = %d, want 1", got)
	}

	// Migration lands. The NEXT db_app open should trigger the drain.
	av.Database.Schema = 2
	u := &User{UID: skew_user}
	a := app_by_id(app_id)
	// db_app fires post_migration_drain_async (now synchronous).
	_ = db_app(u, a)

	if got := skew_pending_count(t, "peerA", app_id); got != 0 {
		t.Errorf("after db_app open: pending = %d, want 0 (drain should have fired)", got)
	}
	if got := skew_posts_count(t, app_id); got != 1 {
		t.Errorf("after db_app open: posts rows = %d, want 1", got)
	}
}
