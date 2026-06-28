// Mochi server: access control as a versioned LWW-Register. Concurrent
// allow/deny/revoke writes from different hosts must converge to the same rule on
// every host, regardless of replication arrival order, and resolve fail-closed.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// apply_op replays one already-originated access write (the journaled upsert)
// against db, exactly as a replica would — explicit version/writer, no local
// version computation. created is fixed (100) on purpose: the merge must NOT
// depend on wall-clock time.
func apply_op(t *testing.T, db *DB, subject, resource, operation string, grant, removed int, writer string, version int64) {
	t.Helper()
	if err := db.exec_e(access_upsert_sql, subject, resource, operation, grant, removed, "g", writer, version, int64(100)); err != nil {
		t.Fatalf("upsert (%s/%s/%s grant=%d removed=%d v=%d) failed: %v", subject, resource, operation, grant, removed, version, err)
	}
}

// effective returns the grant access_check would see: 1 allow, 0 deny, -1 none
// (tombstoned or absent).
func effective(db *DB, subject, resource, operation string) int {
	a := Access{Grant: -1}
	if db.scan(&a, "select grant from access where subject=? and resource=? and operation=? and removed=0", subject, resource, operation) {
		return a.Grant
	}
	return -1
}

// Two hosts apply the SAME concurrent ops in OPPOSITE orders → identical final
// state, and allow+deny at the same version resolves fail-closed (deny wins).
func TestAccessLWWConverges(t *testing.T) {
	a, ca := create_test_db(t)
	defer ca()
	b, cb := create_test_db(t)
	defer cb()
	a.access_setup()
	b.access_setup()

	// host A grants (v1, writer A); host B denies (v1, writer B) — concurrent.
	apply_op(t, a, "u", "r", "x", 1, 0, "A", 1)
	apply_op(t, a, "u", "r", "x", 0, 0, "B", 1)
	apply_op(t, b, "u", "r", "x", 0, 0, "B", 1)
	apply_op(t, b, "u", "r", "x", 1, 0, "A", 1)

	ea, eb := effective(a, "u", "r", "x"), effective(b, "u", "r", "x")
	if ea != eb {
		t.Fatalf("diverged across arrival order: A=%d B=%d", ea, eb)
	}
	if ea != 0 {
		t.Errorf("fail-closed violated: allow+deny @ same version resolved to %d, want 0 (deny)", ea)
	}
}

// A revoke tombstones the rule; a stale lower-version grant can't resurrect it,
// but a re-grant that saw the revoke (higher version) wins.
func TestAccessLWWRevokeThenRegrant(t *testing.T) {
	a, ca := create_test_db(t)
	defer ca()
	a.access_setup()

	apply_op(t, a, "u", "r", "x", 1, 0, "A", 1) // allow v1
	apply_op(t, a, "u", "r", "x", 0, 1, "A", 2) // revoke v2 (tombstone)
	if e := effective(a, "u", "r", "x"); e != -1 {
		t.Fatalf("after revoke want no active rule, got %d", e)
	}
	apply_op(t, a, "u", "r", "x", 1, 0, "B", 1) // stale grant v1 arrives late
	if e := effective(a, "u", "r", "x"); e != -1 {
		t.Errorf("stale grant v1 resurrected a revoked rule: got %d, want -1", e)
	}
	apply_op(t, a, "u", "r", "x", 1, 0, "A", 3) // re-grant that saw the revoke
	if e := effective(a, "u", "r", "x"); e != 1 {
		t.Errorf("re-grant v3 should win: got %d, want 1 (allow)", e)
	}
}

// A legacy access table (autoincrement id, no version) is rebuilt into the
// versioned register, preserving its rules; a post-migration write then converges.
func TestAccessMigrate(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	db.exec("create table access ( id integer primary key autoincrement, subject text not null, resource text not null, operation text not null, grant integer not null, granter text not null, created integer not null, unique( subject, resource, operation ) )")
	db.exec("insert into access ( subject, resource, operation, grant, granter, created ) values ( 'u', 'r', 'x', 1, 'g', 100 )")

	db.access_setup() // triggers access_migrate

	if !db.has_column("access", "version") {
		t.Fatal("migrate did not add the version column")
	}
	if db.has_column("access", "id") {
		t.Error("migrate did not drop the autoincrement id")
	}
	if e := effective(db, "u", "r", "x"); e != 1 {
		t.Errorf("migrated rule lost: got %d, want 1 (allow)", e)
	}
	db.access_set("u", "r", "x", false, "g2") // deny, version 2 via the upsert
	if e := effective(db, "u", "r", "x"); e != 0 {
		t.Errorf("post-migration deny should win (v2 > v1): got %d, want 0", e)
	}
}
