// Mochi server: app permissions as a versioned LWW-Register. Concurrent
// grant/revoke writes from different hosts must converge to the same state on
// every host regardless of replication arrival order, resolve fail-closed (deny
// wins on a version tie), and an app default must never override a user's choice.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// apply_perm replays one already-originated permissions write (the journaled
// upsert) against db, exactly as a replica would — explicit version/writer, no
// local version computation. created is fixed (100): the merge must NOT depend on
// wall-clock time.
func apply_perm(t *testing.T, db *DB, app, permission, object string, granted int, writer string, version int64) {
	t.Helper()
	if err := db.exec_e(permissions_upsert_sql, app, permission, object, granted, writer, version, int64(100)); err != nil {
		t.Fatalf("upsert (%s/%s/%s granted=%d v=%d) failed: %v", app, permission, object, granted, version, err)
	}
}

// perm_granted returns 1 if the permission is granted, 0 if explicitly denied,
// -1 if absent.
func perm_granted(db *DB, app, permission, object string) int {
	var row struct{ Granted int }
	if db.scan(&row, "select granted from permissions where app=? and permission=? and object=?", app, permission, object) {
		return row.Granted
	}
	return -1
}

// Two hosts apply the SAME concurrent ops in OPPOSITE orders → identical final
// state, and grant+deny at the same version resolves fail-closed (deny wins).
func TestPermissionsLWWConverges(t *testing.T) {
	a, ca := create_test_db(t)
	defer ca()
	b, cb := create_test_db(t)
	defer cb()
	a.permissions_setup()
	b.permissions_setup()

	// host A grants (v1, writer A); host B denies (v1, writer B) — concurrent.
	apply_perm(t, a, "app", "url:x", "", 1, "A", 1)
	apply_perm(t, a, "app", "url:x", "", 0, "B", 1)
	apply_perm(t, b, "app", "url:x", "", 0, "B", 1)
	apply_perm(t, b, "app", "url:x", "", 1, "A", 1)

	ga, gb := perm_granted(a, "app", "url:x", ""), perm_granted(b, "app", "url:x", "")
	if ga != gb {
		t.Fatalf("diverged across arrival order: A=%d B=%d", ga, gb)
	}
	if ga != 0 {
		t.Errorf("fail-closed violated: grant+deny @ same version resolved to %d, want 0 (deny)", ga)
	}
}

// A user revoke (version >= 2) beats the app's default grant (version 1), and a
// re-applied default after the revoke cannot resurrect it — exercising the real
// permissions_default / permissions_upsert version computation.
func TestPermissionsRevokeBeatsDefault(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	db.permissions_setup()

	db.permissions_default("app", "settings/write", "") // default grant, v1
	if g := perm_granted(db, "app", "settings/write", ""); g != 1 {
		t.Fatalf("default grant should be active: got %d, want 1", g)
	}
	db.permissions_upsert("app", "settings/write", "", 0) // user revoke, v2
	if g := perm_granted(db, "app", "settings/write", ""); g != 0 {
		t.Fatalf("user revoke v2 should win over default v1: got %d, want 0", g)
	}
	db.permissions_default("app", "settings/write", "") // re-setup re-applies default v1
	if g := perm_granted(db, "app", "settings/write", ""); g != 0 {
		t.Errorf("re-applied default v1 resurrected a revoked permission: got %d, want 0", g)
	}
}

// A legacy permissions table (no version column, host-local direct rows) is
// rebuilt into the versioned register, preserving its rows; a post-migration
// versioned write then converges.
func TestPermissionsMigrate(t *testing.T) {
	db, cleanup := create_test_db(t)
	defer cleanup()
	db.exec("create table permissions (app text not null, permission text not null, object text not null default '', granted integer not null default 0, primary key (app, permission, object))")
	db.exec("insert into permissions (app, permission, object, granted) values ('app', 'url:x', '', 1)")

	db.permissions_setup() // triggers permissions_migrate

	if !db.has_column("permissions", "version") {
		t.Fatal("migrate did not add the version column")
	}
	if g := perm_granted(db, "app", "url:x", ""); g != 1 {
		t.Errorf("migrated row lost: got %d, want 1 (granted)", g)
	}
	apply_perm(t, db, "app", "url:x", "", 0, "A", 2) // post-migration revoke v2
	if g := perm_granted(db, "app", "url:x", ""); g != 0 {
		t.Errorf("post-migration revoke should win (v2 > v1): got %d, want 0", g)
	}
}
