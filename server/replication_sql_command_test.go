// Mochi server: Tests for per-app SQL command replication
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"testing"

	cbor "github.com/fxamacker/cbor/v2"
)

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
func setup_sql_replication_test(t *testing.T) (cleanup func(), userUID, appID string) {
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
	userUID = "uid-test-sql"
	udb.exec("insert into users (uid, username) values (?, ?)", userUID, "alice")

	appID = "myapp"
	av := &AppVersion{Version: "1"}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	av.Database.File = "myapp.db"
	av.Database.Schema = 1
	av.Database.create_function = func(db *DB) {
		db.exec(`create table posts (id text primary key, title text not null)`)
	}
	a := &App{id: appID, versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = a
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps[appID] = a
	apps_lock.Unlock()

	cleanup = func() {
		apps_lock.Lock()
		delete(apps, appID)
		apps_lock.Unlock()
		data_dir = orig
		post_migration_drain_async = orig_drain
		os.RemoveAll(tmp)
	}
	return
}

func TestReplicationApplySQLCommandInsert(t *testing.T) {
	cleanup, userUID, appID := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      userUID,
		Database:  appID,
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

	u := &User{UID: userUID}
	a := app_by_id(appID)
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
	cleanup, userUID, appID := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: userUID}
	a := app_by_id(appID)
	db := db_app(u, a)
	db.exec("insert into posts (id, title) values (?, ?)", "p1", "Old")

	upd := &ReplicationOp{
		Scope: repl_scope_app, User: userUID,
		Database: appID, Operation: repl_op_exec, Schema: 1,
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
		Scope: repl_scope_app, User: userUID,
		Database: appID, Operation: repl_op_exec, Schema: 1,
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
	cleanup, userUID, appID := setup_sql_replication_test(t)
	defer cleanup()

	// Unknown user → deferred.
	unknownUser := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-missing",
		Database: appID, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(unknownUser); got != ApplyDeferred {
		t.Errorf("unknown user: want ApplyDeferred, got %v", got)
	}

	// Unknown app → deferred.
	unknownApp := &ReplicationOp{
		Scope: repl_scope_app, User: userUID,
		Database: "missingapp", Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(unknownApp); got != ApplyDeferred {
		t.Errorf("unknown app: want ApplyDeferred, got %v", got)
	}

	// Sender schema newer than receiver → deferred.
	newerSchema := &ReplicationOp{
		Scope: repl_scope_app, User: userUID,
		Database: appID, Operation: repl_op_exec, Schema: 99,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(newerSchema); got != ApplyDeferred {
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
	cleanup, userUID, appID := setup_sql_replication_test(t)
	defer cleanup()

	// Two writers replay each other's ops; both ends should converge.
	u := &User{UID: userUID}
	a := app_by_id(appID)
	db := db_app(u, a)

	apply := func(sql string, args ...any) {
		op := &ReplicationOp{
			Scope: repl_scope_app, User: userUID,
			Database: appID, Operation: repl_op_exec, Schema: 1,
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

