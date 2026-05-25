// Mochi server: emit-side gates for SQL command replication
// Copyright Alistair Cunningham 2026
//
// These tests probe the negative cases of replication_emit_sql_command:
// it must silently no-op when the user has no UID, the app or app
// version is missing, or the target table is on the exclusion list.
// Probe via the side effect: emit always lands in replication.db.sequence
// before any peer fan-out, so an absent sequence row means the gate
// fired correctly.

package main

import (
	"testing"
)

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
