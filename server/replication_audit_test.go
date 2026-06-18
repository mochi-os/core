// Mochi server: convergence audit (#29) unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAppHighestVersionDir: version dirs are compared numeric-aware, so 3.100
// sorts above 3.95 (the comptroller case), and non-numeric dirs are ignored.
func TestAppHighestVersionDir(t *testing.T) {
	dir := t.TempDir()
	for _, v := range []string{"2.0", "2.1", "3.95", "3.100", "notaversion"} {
		if err := os.MkdirAll(filepath.Join(dir, v), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if got := app_highest_version_dir(dir); got != "3.100" {
		t.Fatalf("highest version: got %q, want 3.100 (numeric-aware)", got)
	}
}

// TestAuditTableReplicates: host-local infra tables are excluded from the
// content count; app data tables are included.
func TestAuditTableReplicates(t *testing.T) {
	for _, name := range []string{"journal", "_commit_log", "sequence", "received", "log", "acknowledged", "pending", "sqlite_master", ""} {
		if audit_table_replicates(name) {
			t.Errorf("%q should be host-local (excluded)", name)
		}
	}
	for _, name := range []string{"posts", "feeds", "sources", "items"} {
		if !audit_table_replicates(name) {
			t.Errorf("%q should count as replicated", name)
		}
	}
}

// TestReplicationStaleApps (a): an app whose apps.db-claimed version has no
// directory on disk is flagged stale (with the highest dir present); one whose
// claimed version IS present is fine; one with no install dir at all is skipped.
func TestReplicationStaleApps(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}

	adb := db_apps()
	adb.exec("create table if not exists versions (app text not null primary key, version text, track text)")
	adb.exec("insert into versions (app, version) values ('A', '3.100'), ('B', '1.0'), ('C', '2.0')")

	os.MkdirAll(filepath.Join(data_dir, "apps", "A", "3.95"), 0o755) // A: claims 3.100, only 3.95 -> stale
	os.MkdirAll(filepath.Join(data_dir, "apps", "B", "1.0"), 0o755)  // B: claims 1.0, present -> fine
	// C: no install dir -> skipped

	stale := replication_stale_apps()
	if len(stale) != 1 || stale[0].App != "A" || stale[0].Claimed != "3.100" || stale[0].OnDisk != "3.95" {
		t.Fatalf("stale apps: got %+v, want one entry {A 3.100 3.95}", stale)
	}
}

// TestDbReplicatedRowCount (b): the content count sums only replicated tables —
// the host-local journal and broadcast bookkeeping must not contribute, or two
// hosts would look permanently diverged.
func TestDbReplicatedRowCount(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	rel := "users/u1/app1/db/feeds.db"
	if err := os.MkdirAll(filepath.Join(data_dir, filepath.FromSlash("users/u1/app1/db")), 0o755); err != nil {
		t.Fatal(err)
	}

	db := db_open(rel)
	db.exec("create table posts (id text primary key)")
	db.exec("insert into posts (id) values ('a'), ('b'), ('c')") // replicated: 3
	db.exec("create table journal (id text primary key)")
	db.exec("insert into journal (id) values ('x'), ('y')") // host-local: excluded
	db.exec("create table sequence (key text primary key)")
	db.exec("insert into sequence (key) values ('s')") // host-local: excluded

	if got := db_replicated_row_count(rel); got != 3 {
		t.Fatalf("replicated row count: got %d, want 3 (posts only)", got)
	}
}

// TestReplicationAuditCompare (b): the heart of the lag-filtering — a count is
// only judged when it is STABLE on both hosts, and only a stable-but-unequal
// count alerts. Covers equal, unstable, absent-on-peer, and re-arm-on-converge.
func TestReplicationAuditCompare(t *testing.T) {
	peer := "peerX"
	reset := func() {
		audit_mutex.Lock()
		audit_alerted = map[string]bool{}
		audit_mutex.Unlock()
	}
	alerted := func(streamKey string) bool {
		audit_mutex.Lock()
		defer audit_mutex.Unlock()
		return audit_alerted[peer+"|"+streamKey]
	}
	m := func(v int64) map[string]int64 { return map[string]int64{"u|s": v} }

	// Stable on both, unequal -> divergence.
	reset()
	replication_audit_compare(peer, m(100), m(100), m(99), m(99))
	if !alerted("u|s") {
		t.Fatal("stable + unequal should alert")
	}

	// Equal -> no alert.
	reset()
	replication_audit_compare(peer, m(100), m(100), m(100), m(100))
	if alerted("u|s") {
		t.Fatal("equal counts should not alert")
	}

	// Local count moved since last round (still settling) -> no alert.
	reset()
	replication_audit_compare(peer, m(100), m(95), m(99), m(99))
	if alerted("u|s") {
		t.Fatal("unstable stream should not alert")
	}

	// Stream absent on peer (maybe bootstrap lag) -> no alert.
	reset()
	replication_audit_compare(peer, m(100), m(100), map[string]int64{}, map[string]int64{})
	if alerted("u|s") {
		t.Fatal("stream absent on peer should not alert")
	}

	// Re-arm: a diverged stream that converges clears its alert.
	reset()
	replication_audit_compare(peer, m(100), m(100), m(99), m(99))
	if !alerted("u|s") {
		t.Fatal("setup: should be alerted")
	}
	replication_audit_compare(peer, m(100), m(100), m(100), m(100))
	if alerted("u|s") {
		t.Fatal("converged stream should clear its alert (re-arm)")
	}
}
