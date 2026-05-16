// Mochi server: Tests for per-app SQL command replication
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"testing"
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
		os.RemoveAll(tmp)
	}
	return
}

func TestReplicationApplySQLCommandInsert(t *testing.T) {
	cleanup, userUID, appID := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Class:     repl_class_sql,
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
		Class: repl_class_sql, Scope: repl_scope_app, User: userUID,
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
		Class: repl_class_sql, Scope: repl_scope_app, User: userUID,
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
		Class: repl_class_sql, Scope: repl_scope_app, User: "uid-missing",
		Database: appID, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(unknownUser); got != ApplyDeferred {
		t.Errorf("unknown user: want ApplyDeferred, got %v", got)
	}

	// Unknown app → deferred.
	unknownApp := &ReplicationOp{
		Class: repl_class_sql, Scope: repl_scope_app, User: userUID,
		Database: "missingapp", Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(unknownApp); got != ApplyDeferred {
		t.Errorf("unknown app: want ApplyDeferred, got %v", got)
	}

	// Sender schema newer than receiver → deferred.
	newerSchema := &ReplicationOp{
		Class: repl_class_sql, Scope: repl_scope_app, User: userUID,
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
		Class: repl_class_sql, Scope: repl_scope_app, User: "uid-test-sql",
		Database: "myapp", Operation: repl_op_exec, Schema: 1,
		Payload: []byte{0xff, 0xff, 0xff},
	}
	if got := replication_apply_op(bad); got != ApplyInvalid {
		t.Errorf("bad cbor: want ApplyInvalid, got %v", got)
	}

	// Empty statement → Invalid.
	empty := &ReplicationOp{
		Class: repl_class_sql, Scope: repl_scope_app, User: "uid-test-sql",
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
			Class: repl_class_sql, Scope: repl_scope_app, User: userUID,
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

