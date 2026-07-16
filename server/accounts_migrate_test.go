// Mochi server: the accounts id→uid migration. A legacy integer-autoincrement
// accounts table must rebuild into the text-id register form, preserving each
// row's identity as the legacy integer rendered as a string (deterministic, so
// paired hosts converge and stored references keep matching).
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestAccountsMigrate(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	// Legacy schema: integer autoincrement id, no register bookkeeping columns.
	db.exec("create table accounts ( id integer primary key, type text not null, label text not null default '', identifier text not null default '', data text not null default '', created integer not null, verified integer not null default 0, enabled integer not null default 1, \"default\" text not null default '', last_delivered integer not null default 0 )")
	db.exec("insert into accounts (type, label, created) values ('email', 'x@y', 100)") // id=1
	db.exec("insert into accounts (type, label, created) values ('ai', 'Claude', 100)") // id=2

	db.accounts_migrate()

	if db.has_column("accounts", "id") {
		// id stays — but it must now be text, not integer autoincrement.
	}
	var first struct{ Id string }
	if !db.scan(&first, "select id from accounts where type='email'") {
		t.Fatal("migrated email account lost")
	}
	if first.Id != "1" {
		t.Errorf("migrated id = %q, want \"1\" (legacy integer rendered as string)", first.Id)
	}
	// A new account now takes a uid, not an autoincrement integer — and the text
	// PK accepts it.
	db.exec("insert into accounts (id, type, label, created) values (?, 'email', 'new', 100)", uid())
	if n := db.integer("select count(*) from accounts"); n != 3 {
		t.Errorf("accounts after a uid insert = %d, want 3", n)
	}
}

// account_set must update both account kinds, and row_remove must delete.
func TestAccountSet(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	db := db_user(&User{UID: "u-acct"}, "user")
	db.exec("insert into accounts (id, type, label, created) values ('e1', 'email', 'old', 100)")
	db.exec("insert into accounts (id, type, label, created) values ('b1', 'browser', 'dev', 100)")

	db.account_set("e1", map[string]any{"label": "new"})
	var e struct{ Label string }
	db.scan(&e, "select label from accounts where id='e1'")
	if e.Label != "new" {
		t.Errorf("email label not updated: %q", e.Label)
	}

	db.account_set("b1", map[string]any{"label": "dev2"})
	var b struct{ Label string }
	db.scan(&b, "select label from accounts where id='b1'")
	if b.Label != "dev2" {
		t.Errorf("device label not updated: %q", b.Label)
	}

	db.row_remove(reg_accounts, map[string]any{"id": "e1"})
	if n := db.integer("select count(*) from accounts where id='e1'"); n != 0 {
		t.Errorf("removed email account still present")
	}
}
