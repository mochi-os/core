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
	cleanup, userUID, appID := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      userUID,
		Database:  appID,
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

	u := &User{UID: userUID}
	a := app_by_id(appID)
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
	cleanup, userUID, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      userUID,
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
	cleanup, userUID, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      userUID,
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

	u := &User{UID: userUID}
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
