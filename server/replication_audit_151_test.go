// Convergence-audit blind-spot fixes (#151): the content hash must not XOR-cancel
// duplicate rows, colliding stream keys must aggregate rather than clobber, and the
// per-user system-scope streams must appear in the manifest so the auth-critical
// liveness pass isn't dead.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #151.1: two identical rows must NOT hash the same as an empty table. Under the
// old XOR fold they cancel to zero (h(X) ^ h(X) == 0), so a count-matched table of
// duplicates read as converged. The additive fold can't cancel.
func TestContentHashAdditiveFoldCatchesDuplicates(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rel := "users/u1/testapp/db/x.db"
	full := filepath.Join(data_dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	db := db_open(rel)
	db.exec("create table t (v text)")

	emptyHash := db_replicated_content_hash(rel)

	db.exec("insert into t (v) values ('x')")
	db.exec("insert into t (v) values ('x')") // the SAME row twice
	dupHash := db_replicated_content_hash(rel)

	if dupHash == emptyHash {
		t.Fatal("a table with two identical rows hashed the same as empty — the XOR-cancel blind spot is still present")
	}
}

// #151.2: entries that collide on the (user, stream) key must aggregate (sum
// counts / fold hashes), not clobber — otherwise a multi-DB app is only partially
// audited.
func TestAuditMapsAggregateColliding(t *testing.T) {
	counts := audit_manifest_map([]AuditStream{
		{User: "u1", Stream: "app:x", Count: 3},
		{User: "u1", Stream: "app:x", Count: 5},
	})
	if counts["u1|app:x"] != 8 {
		t.Errorf("colliding counts = %d, want 8 (summed, not clobbered)", counts["u1|app:x"])
	}

	z := strings.Repeat("00", 32)
	one := strings.Repeat("00", 31) + "01"
	h := audit_hash_map([]AuditHashEntry{{User: "u1", Stream: "app:x", Hash: z}, {User: "u1", Stream: "app:x", Hash: one}})
	if h["u1|app:x"] != one { // 0 + 1
		t.Errorf("folded colliding hashes = %q, want %q", h["u1|app:x"], one)
	}
	// Order-independent (both hosts may walk the DBs in different order).
	h2 := audit_hash_map([]AuditHashEntry{{User: "u1", Stream: "app:x", Hash: one}, {User: "u1", Stream: "app:x", Hash: z}})
	if h2["u1|app:x"] != h["u1|app:x"] {
		t.Error("hash fold must be order-independent")
	}
}

// #151.3: the per-user system-scope streams must appear in the manifest (so the
// auth-critical system:users liveness check has data), and must NOT be offered for
// content hashing (their shared core DBs carry per-host columns).
func TestAuditSystemStreamsInManifestNotContent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	db_open("db/users.db").exec("insert into users (uid, username, role, status) values ('u1', 'a', 'user', 'active')")

	found := false
	for _, s := range replication_audit_local_manifest() {
		if s.User == "u1" && s.Stream == "system:users" {
			found = true
		}
	}
	if !found {
		t.Fatal("manifest must include system:users for a local user — the liveness pass had nothing to check (dead code)")
	}

	// system scope is count/liveness-only: never a content-hash candidate.
	cands := audit_content_candidates(
		map[string]int64{"u1|system:users": 0, "u1|app:feeds": 5},
		map[string]int64{"u1|system:users": 0, "u1|app:feeds": 5},
	)
	appSeen := false
	for _, c := range cands {
		if strings.HasPrefix(c.Stream, "system:") {
			t.Errorf("system-scope stream %q must not be a content-hash candidate", c.Stream)
		}
		if c.Stream == "app:feeds" {
			appSeen = true
		}
	}
	if !appSeen {
		t.Error("a normal app stream should still be a content candidate")
	}
}
