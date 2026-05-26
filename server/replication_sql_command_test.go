// Mochi server: SQL command replication tests
// Copyright Alistair Cunningham 2026
//
// Per-app SQL command replication: apply paths, emit gates,
// loop prevention, idempotent replay, and transaction emit-buffer
// lifecycle. Originally four separate files; merged because they all
// test the same SQLCommand pipeline at different angles.

package main

import (
	"os"
	"sort"
	"sync"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
	sl "go.starlark.net/starlark"
)

// ============================================================
// Basic apply tests (was replication_sql_command_test.go)
// ============================================================

func TestSQLTargetTable(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"INSERT INTO posts VALUES (1)", "posts"},
		{"insert into posts values (1)", "posts"},
		{"INSERT OR IGNORE INTO posts VALUES (1)", "posts"},
		{"INSERT OR REPLACE INTO posts (id) VALUES (1)", "posts"},
		{"REPLACE INTO posts (id) VALUES (1)", "posts"},
		{"UPDATE posts SET title = ? WHERE id = ?", "posts"},
		{"update posts set title = ?", "posts"},
		{"UPDATE OR REPLACE posts SET x = 1", "posts"},
		{"DELETE FROM posts WHERE id = ?", "posts"},
		{"delete from posts", "posts"},

		// Identifiers with quoting.
		{`INSERT INTO "posts" VALUES (1)`, "posts"},
		{"INSERT INTO `posts` VALUES (1)", "posts"},
		{"INSERT INTO [posts] VALUES (1)", "posts"},

		// Leading whitespace / comments.
		{"  \n\t INSERT INTO posts VALUES (1)", "posts"},
		{"-- header\nINSERT INTO posts VALUES (1)", "posts"},
		{"/* header */ INSERT INTO posts VALUES (1)", "posts"},

		// Non-mutating statements: not replicated.
		{"SELECT * FROM posts", ""},
		{"PRAGMA user_version", ""},
		{"CREATE TABLE posts (id INTEGER)", ""},
		{"DROP TABLE posts", ""},
		{"ALTER TABLE posts ADD COLUMN x", ""},

		// CTE: deliberately not recognised; caller must reshape.
		{"WITH cte AS (SELECT 1) INSERT INTO posts SELECT * FROM cte", ""},

		// Garbled input.
		{"", ""},
		{"   ", ""},
		{"INSERT", ""},
		{"UPDATE", ""},
		{"DELETE FROM", ""},
		{"INSERT INTO", ""},
	}
	for _, c := range cases {
		got := sql_target_table(c.sql)
		if got != c.want {
			t.Errorf("sql_target_table(%q) = %q; want %q", c.sql, got, c.want)
		}
	}
}

func TestSQLTargetUID(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		args []any
		want string
	}{
		// Explicit (id, ...) column list - first column is id, args[0]
		// is the row uid.
		{"insert id-first",
			"INSERT INTO posts (id, title) VALUES (?, ?)",
			[]any{"abc123", "hello"},
			"abc123"},
		{"replace id-first",
			"REPLACE INTO posts (id, title) VALUES (?, ?)",
			[]any{"xyz", "hi"},
			"xyz"},
		{"insert or ignore id-first",
			"INSERT OR IGNORE INTO posts (id, title) VALUES (?, ?)",
			[]any{"u1", "t"},
			"u1"},
		{"insert or replace id-first",
			"INSERT OR REPLACE INTO posts (id, n) VALUES (?, ?)",
			[]any{"u2", 1},
			"u2"},

		// Implicit positional values - args[0] is the row uid by
		// convention (Mochi's CREATE TABLE puts id first).
		{"insert positional",
			"INSERT INTO posts VALUES (?, ?, ?)",
			[]any{"pos1", "t", "b"},
			"pos1"},
		{"replace positional",
			"REPLACE INTO posts VALUES (?, ?)",
			[]any{"rp1", "n"},
			"rp1"},

		// (id, ...) column list with quoted table and case variations.
		{"insert quoted table id-first",
			`INSERT INTO "posts" (id, title) VALUES (?, ?)`,
			[]any{"quoted", "t"},
			"quoted"},
		{"lowercase keywords",
			"insert into posts (id, title) values (?, ?)",
			[]any{"lower", "t"},
			"lower"},

		// First column is NOT id - no extraction (apps using non-id
		// PK fall back to empty uid).
		{"insert non-id first column",
			"INSERT INTO posts (slug, title) VALUES (?, ?)",
			[]any{"hello-world", "t"},
			""},

		// UPDATE / DELETE with WHERE id = ?.
		{"update where id",
			"UPDATE posts SET title = ? WHERE id = ?",
			[]any{"new", "abc"},
			"abc"},
		{"delete where id",
			"DELETE FROM posts WHERE id = ?",
			[]any{"abc"},
			"abc"},
		{"update where id no spaces",
			"UPDATE posts SET title=? WHERE id=?",
			[]any{"new", "row7"},
			"row7"},
		{"update multiple set args",
			"UPDATE posts SET title = ?, body = ?, updated = ? WHERE id = ?",
			[]any{"t", "b", 123, "row9"},
			"row9"},

		// WHERE clause keyed on a different column or compound - no
		// extraction.
		{"update where non-id",
			"UPDATE posts SET title = ? WHERE slug = ?",
			[]any{"t", "hello"},
			""},
		{"update where compound",
			"UPDATE posts SET title = ? WHERE id = ? AND author = ?",
			[]any{"t", "abc", "user"},
			""},
		{"delete no where",
			"DELETE FROM posts",
			[]any{},
			""},

		// Non-string args (e.g. integer PK) aren't returned as row
		// uids - Mochi uses string uids via mochi.uid().
		{"insert integer pk",
			"INSERT INTO posts (id, title) VALUES (?, ?)",
			[]any{int64(42), "t"},
			""},

		// Empty / unparseable input.
		{"empty sql", "", nil, ""},
		{"select read-only", "SELECT * FROM posts WHERE id = ?", []any{"x"}, ""},
		{"create table", "CREATE TABLE posts (id INTEGER)", nil, ""},
	}
	for _, c := range cases {
		got := sql_target_uid(c.sql, c.args)
		if got != c.want {
			t.Errorf("%s: sql_target_uid(%q, %v) = %q; want %q",
				c.name, c.sql, c.args, got, c.want)
		}
	}
}

// TestReplicationOpUIDRoundtrip verifies the UID field survives a
// cbor encode / decode cycle (the wire path between sender and
// receiver) and that an op encoded by an older sender (no UID field)
// decodes cleanly with an empty UID.
func TestReplicationOpUIDRoundtrip(t *testing.T) {
	sent := ReplicationOp{
		Scope:     repl_scope_app,
		User:      "uid-user",
		Database:  "posts",
		Table:     "posts",
		UID:       "row-abc",
		Operation: repl_op_exec,
		Payload:   []byte("body"),
		Sequence:  1,
		Prev:      0,
	}
	encoded := cbor_encode(&sent)
	var received ReplicationOp
	if err := cbor.Unmarshal(encoded, &received); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if received.UID != "row-abc" {
		t.Errorf("UID lost in roundtrip: got %q want %q", received.UID, sent.UID)
	}

	// Older sender shape: encode without setting UID, decode, expect "".
	older := ReplicationOp{
		Scope:    repl_scope_app,
		User:     "uid-user",
		Database: "posts",
		Table:    "posts",
		// UID intentionally unset.
		Operation: repl_op_exec,
		Payload:   []byte("body"),
		Sequence:  2,
		Prev:      1,
	}
	var olderDecoded ReplicationOp
	if err := cbor.Unmarshal(cbor_encode(&older), &olderDecoded); err != nil {
		t.Fatalf("older-shape decode failed: %v", err)
	}
	if olderDecoded.UID != "" {
		t.Errorf("missing UID must decode as empty string, got %q", olderDecoded.UID)
	}
}

func TestSQLTableExcluded(t *testing.T) {
	// Default exclusions.
	if !sql_table_excluded(nil, "sqlite_master") {
		t.Error("sqlite_master must be excluded by default")
	}
	if !sql_table_excluded(nil, "sqlite_sequence") {
		t.Error("sqlite_sequence must be excluded by default")
	}
	if !sql_table_excluded(nil, "_commit_log") {
		t.Error("_commit_log must be excluded by default")
	}
	if sql_table_excluded(nil, "posts") {
		t.Error("posts must NOT be excluded by default")
	}

	// Empty / unparseable target: treated as excluded so we don't emit.
	if !sql_table_excluded(nil, "") {
		t.Error("empty table must be treated as excluded")
	}

	// App-declared exclusion.
	av := &AppVersion{}
	av.Database.Replicate.Exclude.Tables = []string{"cache_search", "session_local"}
	if !sql_table_excluded(av, "cache_search") {
		t.Error("app-excluded table must be excluded")
	}
	if !sql_table_excluded(av, "session_local") {
		t.Error("app-excluded table must be excluded")
	}
	if sql_table_excluded(av, "posts") {
		t.Error("non-excluded app table must replicate")
	}
}

// setup_sql_replication_test wires up just enough server state for an
// apply-side test: a temp data_dir, a registered user, a registered app
// pointing at a per-(user, app) DB the apply path will exec against,
// and a fresh schema in that DB.
func setup_sql_replication_test(t *testing.T) (cleanup func(), user_uid, app_id string) {
	t.Helper()
	tmp, err := os.MkdirTemp("", "mochi_sql_repl")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	orig := data_dir
	data_dir = tmp
	// Suppress the post-migration background drain: it reads data_dir
	// asynchronously and races with the cleanup's data_dir restore.
	orig_drain := post_migration_drain_async
	post_migration_drain_async = func(user, app_id string) {}

	udb := db_open("db/users.db")
	udb.exec(`create table if not exists users (id integer primary key, uid text not null unique, username text not null unique)`)
	user_uid = "uid-test-sql"
	udb.exec("insert into users (uid, username) values (?, ?)", user_uid, "alice")

	app_id = "myapp"
	av := &AppVersion{Version: "1"}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	av.Database.File = "myapp.db"
	av.Database.Schema = 1
	av.Database.create_function = func(db *DB) {
		db.exec(`create table posts (id text primary key, title text not null)`)
	}
	a := &App{id: app_id, versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = a
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps[app_id] = a
	apps_lock.Unlock()

	cleanup = func() {
		apps_lock.Lock()
		delete(apps, app_id)
		apps_lock.Unlock()
		data_dir = orig
		post_migration_drain_async = orig_drain
		os.RemoveAll(tmp)
	}
	return
}

func TestReplicationApplySQLCommandInsert(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  app_id,
		Operation: repl_op_exec,
		Schema:    1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"p1", "Hello"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	row, _ := db.row("select title from posts where id = ?", "p1")
	if row == nil {
		t.Fatal("inserted row missing")
	}
	if got, _ := row["title"].(string); got != "Hello" {
		t.Errorf("title: want Hello, got %q", got)
	}
}

func TestReplicationApplySQLCommandUpdateThenDelete(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	db.exec("insert into posts (id, title) values (?, ?)", "p1", "Old")

	upd := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "update posts set title = ? where id = ?",
			Args:      []any{"New", "p1"},
		}),
	}
	if got := replication_apply_op(upd); got != ApplyApplied {
		t.Fatalf("update apply: want ApplyApplied, got %v", got)
	}
	row, _ := db.row("select title from posts where id = ?", "p1")
	if got, _ := row["title"].(string); got != "New" {
		t.Errorf("after update: want New, got %q", got)
	}

	del := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "delete from posts where id = ?",
			Args:      []any{"p1"},
		}),
	}
	if got := replication_apply_op(del); got != ApplyApplied {
		t.Fatalf("delete apply: want ApplyApplied, got %v", got)
	}
	if r, _ := db.row("select 1 from posts where id = ?", "p1"); r != nil {
		t.Error("row not deleted")
	}
}

func TestReplicationApplySQLCommandDeferralPaths(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	// Unknown user → deferred.
	unknown_user := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-missing",
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(unknown_user); got != ApplyDeferred {
		t.Errorf("unknown user: want ApplyDeferred, got %v", got)
	}

	// Unknown app → deferred.
	unknown_app := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: "missingapp", Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(unknown_app); got != ApplyDeferred {
		t.Errorf("unknown app: want ApplyDeferred, got %v", got)
	}

	// Sender schema newer than receiver → deferred.
	newer_schema := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 99,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(newer_schema); got != ApplyDeferred {
		t.Errorf("newer schema: want ApplyDeferred, got %v", got)
	}
}

func TestReplicationApplySQLCommandInvalid(t *testing.T) {
	cleanup, _, _ := setup_sql_replication_test(t)
	defer cleanup()

	// Bad cbor → Invalid.
	bad := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-test-sql",
		Database: "myapp", Operation: repl_op_exec, Schema: 1,
		Payload: []byte{0xff, 0xff, 0xff},
	}
	if got := replication_apply_op(bad); got != ApplyInvalid {
		t.Errorf("bad cbor: want ApplyInvalid, got %v", got)
	}

	// Empty statement → Invalid.
	empty := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-test-sql",
		Database: "myapp", Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: ""}),
	}
	if got := replication_apply_op(empty); got != ApplyInvalid {
		t.Errorf("empty statement: want ApplyInvalid, got %v", got)
	}
}

func TestReplicationApplySQLCommandRoundTrip(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	// Two writers replay each other's ops; both ends should converge.
	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)

	apply := func(sql string, args ...any) {
		op := &ReplicationOp{
			Scope: repl_scope_app, User: user_uid,
			Database: app_id, Operation: repl_op_exec, Schema: 1,
			Payload: cbor_encode(&SQLCommand{Statement: sql, Args: args}),
		}
		if got := replication_apply_op(op); got != ApplyApplied {
			t.Fatalf("apply %q: %v", sql, got)
		}
	}

	apply("insert into posts (id, title) values (?, ?)", "p1", "A")
	apply("insert into posts (id, title) values (?, ?)", "p2", "B")
	apply("update posts set title = ? where id = ?", "A-updated", "p1")
	apply("delete from posts where id = ?", "p2")

	rows, _ := db.rows("select id, title from posts order by id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if id, _ := rows[0]["id"].(string); id != "p1" {
		t.Errorf("row id: want p1, got %q", id)
	}
	if title, _ := rows[0]["title"].(string); title != "A-updated" {
		t.Errorf("row title: want A-updated, got %q", title)
	}
}

// TestReplicationEmitConcurrentChainIntact is the regression test
// for task #93. Before the fix, replication_sequence_next and
// replication_tail_advance used SELECT-then-UPDATE patterns that
// raced under concurrent emit. Two goroutines could both see the
// same pre-update value and emit ops with identical sequence
// numbers or identical `prev` pointers. The receiver applied one
// op cleanly and silently dropped the duplicate as "below cursor",
// then everything past the lost link buffered forever waiting for
// it. Surfaced live as 668/272 stalled entries on mochi2's
// feeds/projects streams after ~40 minutes of normal traffic.
//
// Fix in replication_emit_to_real wraps the (sequence_next,
// tail_advance) pair in a per-(user, scope, db) mutex. This test
// drives the same critical section directly from N goroutines and
// asserts: every sequence is unique, every prev chains onto the
// preceding emit's sequence, the tail row's final last matches the
// max emitted sequence.
//
// Verified the test catches the regression: with the mutex removed,
// failure messages include "sequence N emitted twice" and "chain
// broken at idx M: seq=X prev=Y, want prev=Z".
func TestReplicationEmitConcurrentChainIntact(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	// setup_sql_replication_test gives a temp data_dir but doesn't
	// initialise db/replication.db. The emit critical section reads
	// from `sequence` and `tail` tables there. Create the minimal
	// schema directly.
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists sequence (user text not null default '', scope text not null, next integer not null default 0, primary key (user, scope))")
	rdb.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")

	type allocated struct{ seq, prev int64 }
	const N = 200
	results := make([]allocated, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			// Mirror the production critical section in
			// replication_emit_to_real.
			mu := replication_emit_lock(user_uid, "app", "testdb")
			mu.Lock()
			seq := replication_sequence_next(user_uid, "app")
			prev := replication_tail_advance(user_uid, "app", "testdb", seq)
			mu.Unlock()
			results[i] = allocated{seq, prev}
		}(i)
	}
	wg.Wait()

	// Sort by allocated sequence; the chain must be intact after
	// sort: every prev equals the predecessor's seq, no duplicates.
	sort.Slice(results, func(i, j int) bool { return results[i].seq < results[j].seq })
	seen := map[int64]bool{}
	for i, r := range results {
		if seen[r.seq] {
			t.Errorf("sequence %d emitted twice (race regression)", r.seq)
		}
		seen[r.seq] = true
		if i == 0 {
			if r.prev != 0 {
				t.Errorf("first op should have prev=0, got %d", r.prev)
			}
		} else {
			want := results[i-1].seq
			if r.prev != want {
				t.Errorf("chain broken at idx %d: seq=%d prev=%d, want prev=%d", i, r.seq, r.prev, want)
			}
		}
	}

	// Final tail.last must equal the highest sequence emitted.
	if row, err := rdb.row("select last from tail where user=? and scope=? and db=?", user_uid, "app", "testdb"); err == nil && row != nil {
		if last, _ := row["last"].(int64); last != results[N-1].seq {
			t.Errorf("final tail.last = %d, want %d", last, results[N-1].seq)
		}
	} else {
		t.Errorf("tail row missing or read failed: %v", err)
	}
}


// ============================================================
// Extra apply / loop-prevention / replay tests
// (was replication_sql_command_extra_test.go)
// ============================================================

// TestReplicationApplySQLCommandDoesNotReEmit locks the no-loop
// invariant: when a SQL exec op is applied on the receiver, the apply
// path must not call replication_emit_sql_command on the way through.
// If it did, two-host replication would ping-pong forever.
//
// Probe: replication_emit increments a per-(user, scope) sequence row
// in replication.db.sequence as its first side effect. If apply
// re-emitted, that row would exist with next>=1. We assert it doesn't.
func TestReplicationApplySQLCommandDoesNotReEmit(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()
	db_upgrade_50() // creates replication.db.sequence

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  app_id,
		Operation: repl_op_exec,
		Schema:    1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"loop-1", "Hello"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}

	repl := db_open("db/replication.db")
	row, _ := repl.row("select next from sequence where user=? and scope=?", user_uid, repl_scope_app)
	if row != nil {
		if next, _ := row["next"].(int64); next > 0 {
			t.Errorf("apply re-emitted: replication.db.sequence row for user=%q scope=%q advanced to %d (expected 0/absent)", user_uid, repl_scope_app, next)
		}
	}
}

// TestReplicationApplySQLCommandIdempotentReplay re-applies the same
// op and verifies the receiver doesn't blow up. INSERT replay produces
// a PK uniqueness violation which the apply path logs and treats as
// ApplyApplied (so the deduper doesn't keep retrying forever); the
// row state matches what one apply would have produced.
func TestReplicationApplySQLCommandIdempotentReplay(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"idem", "Once"},
		}),
	}
	for i := 0; i < 3; i++ {
		if got := replication_apply_op(op); got != ApplyApplied {
			t.Fatalf("apply #%d: want ApplyApplied, got %v", i, got)
		}
	}

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	count := db.integer("select count(*) from posts where id='idem'")
	if count != 1 {
		t.Errorf("replay must be idempotent; row count = %d, want 1", count)
	}
	title, _ := db.row("select title from posts where id='idem'")
	if v, _ := title["title"].(string); v != "Once" {
		t.Errorf("title: want 'Once', got %q", v)
	}
}

// TestReplicationApplySQLCommandReceiverFailureLogged exercises the
// schema-drift path: a receiver missing a column referenced by the
// op's SQL. The apply must not panic; it logs and returns ApplyApplied
// so the deduper marks it seen and doesn't re-deliver.
func TestReplicationApplySQLCommandReceiverFailureLogged(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title, missing) values (?, ?, ?)",
			Args:      []any{"bad", "X", "Y"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("receiver-failure: want ApplyApplied (logged), got %v", got)
	}
}

// TestReplicationSQLCommandMixedArgTypesRoundTrip exercises the CBOR
// encode→decode→exec path with the parameter types apps actually pass:
// strings, integers, []byte (blob), and nil. The receiver's SQL
// driver must accept whatever Go types CBOR produces on the other side.
//
// CBOR's `any` decode returns positive ints as uint64; the SQL driver
// accepts both, so the wire format normalising to uint64 is fine. The
// test checks stored values, not the intermediate Go types.
func TestReplicationSQLCommandMixedArgTypesRoundTrip(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	db.exec("create table mixed (id text primary key, n integer, blob blob, opt text)")

	original := &SQLCommand{
		Statement: "insert into mixed (id, n, blob, opt) values (?, ?, ?, ?)",
		Args:      []any{"m1", int64(42), []byte{0x01, 0x02, 0x03}, nil},
	}
	payload := cbor_encode(original)

	// Probe the decoded Args shape before the apply runs, so we can
	// pinpoint where any type confusion happens.
	var decoded SQLCommand
	if err := cbor.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Args) != 4 {
		t.Fatalf("args len: want 4, got %d", len(decoded.Args))
	}
	t.Logf("decoded arg types: %T %T %T %T", decoded.Args[0], decoded.Args[1], decoded.Args[2], decoded.Args[3])

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: payload,
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}

	// DB.row() helpfully converts []byte to string for app code, so we
	// can't .([]byte) the blob column directly. Use length + hex to
	// verify the stored bytes are correct.
	row, _ := db.row("select n, length(blob) as blen, hex(blob) as bhex, opt from mixed where id='m1'")
	if row == nil {
		t.Fatal("row missing after apply")
	}
	if n, _ := row["n"].(int64); n != 42 {
		t.Errorf("integer column: want 42, got %d (raw %v)", n, row["n"])
	}
	if blen, _ := row["blen"].(int64); blen != 3 {
		t.Errorf("blob length: want 3, got %d", blen)
	}
	if bhex, _ := row["bhex"].(string); bhex != "010203" {
		t.Errorf("blob hex: want 010203, got %q", bhex)
	}
	if v := row["opt"]; v != nil {
		t.Errorf("nil arg: want nil, got %v (%T)", v, v)
	}
}

// TestReplicationSQLCommandNoParamsStatement covers a statement that
// uses no bound parameters at all (e.g. a bulk delete).
func TestReplicationSQLCommandNoParamsStatement(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	db.exec("insert into posts (id, title) values ('a', 'A')")
	db.exec("insert into posts (id, title) values ('b', 'B')")

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "delete from posts"}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}
	if n := db.integer("select count(*) from posts"); n != 0 {
		t.Errorf("post-delete count: want 0, got %d", n)
	}
}

// TestReplicationSQLCommandSchemaDefer exercises the cross-host schema
// gate: a sender at schema v3 cannot apply on a receiver still at v1.
// The op must defer, not error out.
func TestReplicationSQLCommandSchemaDefer(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 99,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"future", "From v99"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyDeferred {
		t.Errorf("op carrying higher sender schema: want ApplyDeferred, got %v", got)
	}
}

// ============================================================
// Emit-side gate tests (was replication_sql_emit_test.go)
// ============================================================

// emit_gate_setup wires up a tmp data_dir + replication.db so we can
// probe the sequence side-effect, but does NOT register the user or
// app. Individual tests set up just what they need.
func emit_gate_setup(t *testing.T) (cleanup func(), user_uid, app_id string) {
	t.Helper()
	cleanup, user_uid, app_id = setup_sql_replication_test(t)
	db_upgrade_50() // creates replication.db.sequence
	return
}

func sequence_row_exists(user string) bool {
	repl := db_open("db/replication.db")
	row, _ := repl.row("select next from sequence where user=? and scope=?", user, repl_scope_app)
	return row != nil
}

func TestReplicationEmitSQLCommandSilentWithNoUser(t *testing.T) {
	cleanup, _, app_id := emit_gate_setup(t)
	defer cleanup()

	a := app_by_id(app_id)
	replication_emit_sql_command(nil, a, a.internal, "insert into posts (id, title) values (?, ?)", []any{"x", "y"})
	if sequence_row_exists("") {
		t.Error("emit must not advance sequence when user is nil")
	}
}

func TestReplicationEmitSQLCommandSilentWithEmptyUID(t *testing.T) {
	cleanup, _, app_id := emit_gate_setup(t)
	defer cleanup()

	a := app_by_id(app_id)
	u := &User{UID: ""}
	replication_emit_sql_command(u, a, a.internal, "insert into posts (id, title) values (?, ?)", []any{"x", "y"})
	if sequence_row_exists("") {
		t.Error("emit must not advance sequence when UID is empty")
	}
}

func TestReplicationEmitSQLCommandSilentWithNoApp(t *testing.T) {
	cleanup, user_uid, _ := emit_gate_setup(t)
	defer cleanup()

	u := &User{UID: user_uid}
	replication_emit_sql_command(u, nil, nil, "insert into posts (id, title) values (?, ?)", []any{"x", "y"})
	if sequence_row_exists(user_uid) {
		t.Error("emit must not advance sequence when app is nil")
	}
}

func TestReplicationEmitSQLCommandSilentForExcludedTable(t *testing.T) {
	cleanup, user_uid, app_id := emit_gate_setup(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	av := a.internal
	av.Database.Replicate.Exclude.Tables = []string{"posts"}

	replication_emit_sql_command(u, a, av, "insert into posts (id, title) values (?, ?)", []any{"x", "y"})
	if sequence_row_exists(user_uid) {
		t.Error("emit must not advance sequence for excluded table")
	}
}

func TestReplicationEmitSQLCommandSilentForDefaultExcludedTable(t *testing.T) {
	cleanup, user_uid, app_id := emit_gate_setup(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)

	// sqlite_* writes (rare but possible if an app does raw bookkeeping)
	// must never replicate — they're SQLite internals.
	replication_emit_sql_command(u, a, a.internal, "insert into sqlite_master (name) values (?)", []any{"x"})
	if sequence_row_exists(user_uid) {
		t.Error("emit must not advance sequence for sqlite_* tables")
	}
}

func TestReplicationEmitSQLCommandSilentForNonMutatingStatement(t *testing.T) {
	cleanup, user_uid, app_id := emit_gate_setup(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)

	// SELECT / CREATE TABLE / DROP / ALTER aren't mutating data rows.
	for _, sql := range []string{
		"select 1",
		"create table foo (id text)",
		"drop table foo",
		"alter table posts add column x text",
	} {
		replication_emit_sql_command(u, a, a.internal, sql, nil)
	}
	if sequence_row_exists(user_uid) {
		t.Error("emit must not advance sequence for non-mutating SQL")
	}
}

// ============================================================
// Transaction emit-buffer lifecycle tests
// (was replication_transaction_test.go)
// ============================================================

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
