// Mochi server: Tests for Go-side per-user system DB replication
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// TestReplicationApplyAppSystemExec verifies that a replicated app-system
// write (the path mochi.access.* now uses) lands in the receiver's
// users/<uid>/<app>/app.db.
func TestReplicationApplyAppSystemExec(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  app_id,
		Table:     "access",
		Operation: repl_op_exec_app_system,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into access (subject, resource, operation, grant, granter, created) values (?, ?, ?, ?, ?, ?)`,
			Args:      []any{"alice", "feed/F1", "view", int64(1), "alice", int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app_system(u, a)
	row, err := db.row("select grant from access where subject=? and resource=? and operation=?", "alice", "feed/F1", "view")
	if err != nil {
		t.Fatalf("row error: %v", err)
	}
	if row == nil {
		t.Fatal("replicated access row missing on receiver")
	}
}

// TestReplicationApplyAppSystemExecMissingApp confirms the apply defers
// when the receiver doesn't have the app installed yet (the bootstrap
// drain will retry once the app sync lands).
func TestReplicationApplyAppSystemExecMissingApp(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  "no-such-app",
		Operation: repl_op_exec_app_system,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into access (subject, resource, operation, grant, granter, created) values (?, ?, ?, ?, ?, ?)`,
			Args:      []any{"alice", "feed/F1", "view", int64(1), "alice", int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyDeferred {
		t.Fatalf("expected ApplyDeferred for missing app, got %v", got)
	}
}

// TestReplicationApplyUserCoreExec verifies that a replicated user-core
// write (the path mochi.group.* now uses) lands in the receiver's
// users/<uid>/user.db.
func TestReplicationApplyUserCoreExec(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  repl_db_user_core_sentinel,
		Table:     "groups",
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into groups (id, name, description, created) values (?, ?, ?, ?)`,
			Args:      []any{"g-engineering", "Engineering", "", int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	db := db_user(u, "user")
	row, err := db.row("select name from groups where id=?", "g-engineering")
	if err != nil {
		t.Fatalf("row error: %v", err)
	}
	if row == nil {
		t.Fatal("replicated groups row missing on receiver")
	}
	if got, _ := row["name"].(string); got != "Engineering" {
		t.Errorf("name: want Engineering, got %q", got)
	}
}

// TestReplicationApplyUserCoreExecPreferences: a user preference write
// replicates via the user-core exec path and lands in the receiver's
// users/<uid>/user.db `preferences` table. Regression for a language
// preference changed on one host of an account not reaching the other
// hosts — user_preference_set / user_preference_delete now use
// exec_replicated, and `preferences` is not in sql_default_excluded,
// so the write fans out and applies.
func TestReplicationApplyUserCoreExecPreferences(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  repl_db_user_core_sentinel,
		Table:     "preferences",
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into preferences (name, value) values (?, ?)`,
			Args:      []any{"language", "fr"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	db := db_user(u, "user")
	row, err := db.row("select value from preferences where name=?", "language")
	if err != nil {
		t.Fatalf("row error: %v", err)
	}
	if row == nil {
		t.Fatal("replicated preferences row missing on receiver")
	}
	if got, _ := row["value"].(string); got != "fr" {
		t.Errorf("language preference: want fr, got %q", got)
	}

	// A delete also replicates and converges.
	del := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  repl_db_user_core_sentinel,
		Table:     "preferences",
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `delete from preferences where name = ?`,
			Args:      []any{"language"},
		}),
	}
	if got := replication_apply_op(del); got != ApplyApplied {
		t.Fatalf("delete: expected ApplyApplied, got %v", got)
	}
	if n := db.integer("select count(*) from preferences where name='language'"); n != 0 {
		t.Errorf("preference rows after replicated delete = %d, want 0", n)
	}
}

// TestReplicationApplyUserCoreExecInterests: an interest-profile write
// replicates via the user-core exec path and lands in the receiver's
// users/<uid>/user.db `interests` table. The personalised ranking is
// account-global, so mochi.interests.* now uses exec_replicated and
// `interests` is not in sql_default_excluded — the write fans out.
func TestReplicationApplyUserCoreExecInterests(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  repl_db_user_core_sentinel,
		Table:     "interests",
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `insert or replace into interests (qid, weight, updated) values (?, ?, ?)`,
			Args:      []any{"Q42", int64(75), int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	db := db_user(u, "user")
	row, err := db.row("select weight from interests where qid=?", "Q42")
	if err != nil {
		t.Fatalf("row error: %v", err)
	}
	if row == nil {
		t.Fatal("replicated interests row missing on receiver")
	}
	if got, _ := row["weight"].(int64); got != 75 {
		t.Errorf("weight: want 75, got %d", got)
	}
}

// TestReplicationApplyUserCoreExecMissingUser confirms the apply defers
// when the user record hasn't yet landed locally.
func TestReplicationApplyUserCoreExecMissingUser(t *testing.T) {
	cleanup, _, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      "uid-not-here",
		Database:  repl_db_user_core_sentinel,
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into groups (id, name, description, created) values (?, ?, ?, ?)`,
			Args:      []any{"g1", "G", "", int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyDeferred {
		t.Fatalf("expected ApplyDeferred for missing user, got %v", got)
	}
}
