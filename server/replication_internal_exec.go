// Mochi server: Replication of Go-side writes to per-user system DBs
// Copyright Alistair Cunningham 2026

package main

import (
	"strings"

	cbor "github.com/fxamacker/cbor/v2"
)

// Go-side APIs (mochi.access.*, mochi.group.*, attachments helpers, etc)
// hit per-user databases via *DB.exec — the Starlark-side
// db_replicate_after_exec interception doesn't see them. This file
// emits + applies replicated SQL commands for those writes.
//
// Two flavours, one per file role:
//
//   - exec-app-system: writes to users/<uid>/<app>/app.db (access,
//     attachments). On the receiver, Database carries the app id;
//     the apply path resolves db_app_system(user, app).
//
//   - exec-user-core : writes to users/<uid>/user.db (groups,
//     accounts, interests, permissions, settings). No app context;
//     Database is the sentinel "user". The apply path opens
//     db_user(user, "user"), which is idempotent and creates tables
//     on first call so the receiver schema is always ready.
//
// Same SQLCommand payload as the Starlark exec path, same FK-defer
// behaviour, same parallel-queue serialization per (target, entity).

func replication_emit_app_system_exec(user *User, app *App, sql string, args []any) {
	if user == nil || user.UID == "" || app == nil {
		return
	}
	table := sql_target_table(sql)
	if table == "" {
		return
	}
	for _, prefix := range sql_default_excluded {
		if strings.HasPrefix(table, prefix) {
			return
		}
	}
	payload := cbor_encode(&SQLCommand{Statement: sql, Args: args})
	replication_emit(user.UID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user.UID,
		Database:  app.id,
		Table:     table,
		Operation: repl_op_exec_app_system,
		Payload:   payload,
	})
}

func replication_emit_user_core_exec(user *User, sql string, args []any) {
	if user == nil || user.UID == "" {
		return
	}
	table := sql_target_table(sql)
	if table == "" {
		return
	}
	for _, prefix := range sql_default_excluded {
		if strings.HasPrefix(table, prefix) {
			return
		}
	}
	payload := cbor_encode(&SQLCommand{Statement: sql, Args: args})
	replication_emit(user.UID, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user.UID,
		Database:  repl_db_user_core_sentinel,
		Table:     table,
		Operation: repl_op_exec_user_core,
		Payload:   payload,
	})
}

func replication_apply_app_system_exec(op *ReplicationOp) ApplyResult {
	var cmd SQLCommand
	if err := cbor.Unmarshal(op.Payload, &cmd); err != nil {
		info("Replication exec-app-system: decode failed: %v", err)
		return ApplyInvalid
	}
	if cmd.Statement == "" {
		return ApplyInvalid
	}
	if !user_exists(op.User) {
		return ApplyDeferred
	}
	u := &User{UID: op.User}
	a := app_by_id(op.Database)
	if a == nil {
		return ApplyDeferred
	}
	db := db_app_system(u, a)
	if db == nil {
		return ApplyDeferred
	}
	if _, err := db.internal.Exec(cmd.Statement, cmd.Args...); err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			debug("Replication exec-app-system deferred (FK): user=%q app=%q table=%q sql=%q", op.User, op.Database, op.Table, cmd.Statement)
			return ApplyDeferred
		}
		warn("Replication exec-app-system failed on user=%q app=%q sql=%q: %v", op.User, op.Database, cmd.Statement, err)
		return ApplyApplied
	}
	return ApplyApplied
}

func replication_apply_user_core_exec(op *ReplicationOp) ApplyResult {
	var cmd SQLCommand
	if err := cbor.Unmarshal(op.Payload, &cmd); err != nil {
		info("Replication exec-user-core: decode failed: %v", err)
		return ApplyInvalid
	}
	if cmd.Statement == "" {
		return ApplyInvalid
	}
	if !user_exists(op.User) {
		return ApplyDeferred
	}
	u := &User{UID: op.User}
	db := db_user(u, "user")
	if db == nil {
		return ApplyDeferred
	}
	if _, err := db.internal.Exec(cmd.Statement, cmd.Args...); err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			debug("Replication exec-user-core deferred (FK): user=%q table=%q sql=%q", op.User, op.Table, cmd.Statement)
			return ApplyDeferred
		}
		warn("Replication exec-user-core failed on user=%q sql=%q: %v", op.User, cmd.Statement, err)
		return ApplyApplied
	}
	return ApplyApplied
}
