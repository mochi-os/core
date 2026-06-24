// Mochi server: #27 SEV1 regression — wiped-replica recovery must never
// overwrite a populated host.
//
// The 2026-06-24 incident: a re-pair misaligned yuzu's cursors, so its FULL
// DBs looked "unanchored"; replication_wiped_rebootstrap re-pulled them from
// the empty/mid-bootstrap wasabi and wiped 28 DBs on the live primary. The fix
// gates the re-pull on the user genuinely having no local data.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestWipedRebootstrapSkipsPopulatedUser(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Mock the re-pull so we observe who would be overwritten.
	var pulled []string
	orig := bootstrap_start_user
	bootstrap_start_user = func(peer, uid string) { pulled = append(pulled, uid) }
	defer func() { bootstrap_start_user = orig }()

	rdb := db_open("db/replication.db")
	aged := now() - (rebootstrap_unanchored_seconds + 100) // past the cutoff

	// (1) POPULATED user — a misaligned cursor makes it look unanchored, but its
	// local DB has real data. Must be SKIPPED (re-pulling would wipe it).
	pUser := "user-populated"
	if err := os.MkdirAll(filepath.Join(data_dir, "users", pUser), 0o755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("sqlite3", "file:"+filepath.Join(data_dir, "users", pUser, "feeds.db"))
	if err != nil {
		t.Fatal(err)
	}
	d.Exec("create table posts (id integer primary key, body text)")
	d.Exec("insert into posts (body) values ('real data that must survive')")
	d.Close()
	rdb.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peerX', 'app', ?, 'app:entityX', 5, 4, 0, x'', ?)",
		pUser, aged)

	// (2) GENUINELY WIPED user — empty local dir. Must still be PULLED.
	wUser := "user-wiped"
	if err := os.MkdirAll(filepath.Join(data_dir, "users", wUser), 0o755); err != nil {
		t.Fatal(err)
	}
	rdb.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peerX', 'app', ?, 'app:entityY', 5, 4, 0, x'', ?)",
		wUser, aged)

	// Both must classify as stalled+unanchored for the test to mean anything.
	seen := map[string]bool{}
	for _, s := range replication_pending_stalled() {
		seen[s.User] = true
	}
	if !seen[pUser] || !seen[wUser] {
		t.Fatalf("setup: expected both users stalled+unanchored; got %v", seen)
	}

	replication_wiped_rebootstrap()

	for _, u := range pulled {
		if u == pUser {
			t.Errorf("SEV1 REGRESSION (#27): wiped-replica recovery re-pulled POPULATED user %q — that overwrites real data (the 28-DB wipe)", pUser)
		}
	}
	pulledWiped := false
	for _, u := range pulled {
		if u == wUser {
			pulledWiped = true
		}
	}
	if !pulledWiped {
		t.Errorf("genuine wiped replica %q was NOT re-pulled — recovery broken for the real (empty-local) case", wUser)
	}
}

// TestUserHasLocalDataPredicate pins the guard predicate directly.
func TestUserHasLocalDataPredicate(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Empty user dir -> no data.
	empty := "empty-user"
	os.MkdirAll(filepath.Join(data_dir, "users", empty), 0o755)
	if replication_user_has_local_data(empty) {
		t.Errorf("empty user reported as having data")
	}

	// Schema-only DB (no rows) -> no data.
	schemaOnly := "schema-only-user"
	dir := filepath.Join(data_dir, "users", schemaOnly, "12entity", "db")
	os.MkdirAll(dir, 0o755)
	d, _ := sql.Open("sqlite3", "file:"+filepath.Join(dir, "feeds.db"))
	d.Exec("create table posts (id integer primary key, body text)")
	d.Close()
	if replication_user_has_local_data(schemaOnly) {
		t.Errorf("schema-only (zero-row) user reported as having data")
	}

	// Add a row -> has data.
	d2, _ := sql.Open("sqlite3", "file:"+filepath.Join(dir, "feeds.db"))
	d2.Exec("insert into posts (body) values ('x')")
	d2.Close()
	if !replication_user_has_local_data(schemaOnly) {
		t.Errorf("populated user reported as having NO data — guard would wipe it")
	}
}
