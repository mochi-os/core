// The replication apply path runs inbound SQL under db_authorise_starlark_strict
// (DML-only): a replicated op is always DML, so an inbound DROP/ALTER/CREATE is a
// malicious op (e.g. a compromised host sending "DROP TABLE posts") and is denied.
// The denial is via the SQLite authorizer (per-action), so it can't be bypassed by
// smuggling DDL after a DML statement (db.starlark.Exec runs every statement). App
// migrations, which legitimately run DDL through the same pool, are unaffected.
// Threat-model #93, claude/plans/replication-threat-model.md.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestExecReplicatedApplyBlocksDDL(t *testing.T) {
	defer journal_test_dir(t, "u1", "testapp")()
	db := db_open("users/u1/testapp/db/data.db")
	if db == nil {
		t.Fatal("nil db")
	}
	// One connection only, so the apply path's authorizer mutate+restore is exercised
	// by the subsequent normal Exec (it reuses the very connection the apply used).
	db.starlark.SetMaxOpenConns(1)

	// Normal path creates the table — proves legitimate DDL (migrations) is unaffected.
	if _, err := db.starlark.Exec("create table posts (id text primary key, body text)"); err != nil {
		t.Fatalf("normal create failed: %v", err)
	}
	tableExists := func() bool {
		var n int
		db.starlark.QueryRow("select count(*) from sqlite_master where type='table' and name='posts'").Scan(&n)
		return n == 1
	}
	rowCount := func() int {
		var n int
		db.starlark.QueryRow("select count(*) from posts").Scan(&n)
		return n
	}

	// DML through the apply helper works.
	if _, err := db.exec_replicated_apply("insert into posts (id, body) values ('a', 'hi')"); err != nil {
		t.Fatalf("replicated INSERT should succeed: %v", err)
	}
	if rowCount() != 1 {
		t.Fatalf("INSERT did not land: rows=%d", rowCount())
	}

	// DROP through the apply helper is DENIED; the table survives.
	if _, err := db.exec_replicated_apply("drop table posts"); err == nil {
		t.Fatal("replicated DROP TABLE should be denied")
	}
	if !tableExists() {
		t.Fatal("DROP TABLE was applied — gate failed")
	}

	// Multi-statement: a DROP smuggled after a DML statement is still denied
	// (the per-action authorizer, not a leading-verb check, is why).
	if _, err := db.exec_replicated_apply("insert into posts (id) values ('b'); drop table posts"); err == nil {
		t.Fatal("multi-statement DROP should be denied")
	}
	if !tableExists() {
		t.Fatal("multi-statement DROP was applied — gate bypassed via the 2nd statement")
	}

	// ALTER and CREATE are denied too.
	if _, err := db.exec_replicated_apply("alter table posts add column extra text"); err == nil {
		t.Fatal("replicated ALTER should be denied")
	}
	if _, err := db.exec_replicated_apply("create table evil (x text)"); err == nil {
		t.Fatal("replicated CREATE should be denied")
	}

	// After all that apply-path use, the normal pool still permits DDL — proving the
	// strict authorizer was restored and the connection returned to the pool clean,
	// so app migrations are not collaterally broken.
	if _, err := db.starlark.Exec("create table normal_ok (x text)"); err != nil {
		t.Fatalf("normal DDL broke after apply-path use (authorizer not restored): %v", err)
	}
}
