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
	for _, name := range []string{"journal", "commits", "idempotency", "sequence", "received", "log", "acknowledged", "pending", "email_delivered", "sqlite_master", ""} {
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
		audit_convergence_mutex.Lock()
		audit_alerted = map[string]bool{}
		audit_convergence_mutex.Unlock()
	}
	alerted := func(streamKey string) bool {
		audit_convergence_mutex.Lock()
		defer audit_convergence_mutex.Unlock()
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

// TestReplicationAuditLiveness (#liveness): a stream whose apply cursor is below
// the peer's emitted tail AND frozen since the previous round alerts as "not
// advancing"; one that is behind but still advancing (lag), caught up, or not
// originated by the peer (tail 0) does not; and the alert re-arms on catch-up.
func TestReplicationAuditLiveness(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() {
		data_dir = orig
		audit_convergence_mutex.Lock()
		audit_cursor_previous = map[string]int64{}
		audit_liveness_alerted = map[string]bool{}
		audit_convergence_mutex.Unlock()
	}()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")

	peer, stream := "peerP", "app:feeds"
	setCursor := func(seq int64) {
		rdb.exec("insert into cursor (peer, scope, user, db, sequence) values (?, ?, 'u1', ?, ?) on conflict(peer, scope, user, db) do update set sequence=excluded.sequence",
			peer, repl_scope_app, stream, seq)
	}
	manifest := func(tail int64) []AuditStream { return []AuditStream{{User: "u1", Stream: stream, Tail: tail}} }
	alerted := func() bool {
		audit_convergence_mutex.Lock()
		defer audit_convergence_mutex.Unlock()
		return audit_liveness_alerted[peer+"|u1|"+stream]
	}
	reset := func() {
		audit_convergence_mutex.Lock()
		audit_cursor_previous = map[string]int64{}
		audit_liveness_alerted = map[string]bool{}
		audit_convergence_mutex.Unlock()
	}

	// Behind + frozen -> alert (only after a second round confirms the freeze).
	reset()
	setCursor(100)
	replication_audit_liveness(peer, manifest(200))
	if alerted() {
		t.Fatal("first round must not alert (freeze needs a second round)")
	}
	replication_audit_liveness(peer, manifest(200))
	if !alerted() {
		t.Fatal("behind + frozen should alert")
	}

	// Behind but advancing (lag) -> no alert.
	reset()
	setCursor(100)
	replication_audit_liveness(peer, manifest(200))
	setCursor(150)
	replication_audit_liveness(peer, manifest(200))
	if alerted() {
		t.Fatal("advancing stream (lag, not stuck) should not alert")
	}

	// Caught up -> no alert.
	reset()
	setCursor(200)
	replication_audit_liveness(peer, manifest(200))
	replication_audit_liveness(peer, manifest(200))
	if alerted() {
		t.Fatal("caught-up stream should not alert")
	}

	// Peer doesn't originate it (tail 0) -> no alert.
	reset()
	setCursor(100)
	replication_audit_liveness(peer, manifest(0))
	replication_audit_liveness(peer, manifest(0))
	if alerted() {
		t.Fatal("stream not originated by peer (tail 0) should not alert")
	}

	// Re-arm: alerted, then catches up -> alert cleared.
	reset()
	setCursor(100)
	replication_audit_liveness(peer, manifest(200))
	replication_audit_liveness(peer, manifest(200))
	if !alerted() {
		t.Fatal("setup: should be alerted")
	}
	setCursor(200)
	replication_audit_liveness(peer, manifest(200))
	if alerted() {
		t.Fatal("catching up should clear the liveness alert (re-arm)")
	}
}

// TestReplicationManagerHung (external-monitor dead-man's-switch): a fresh
// heartbeat reads as not-hung with a small age; a heartbeat aged past the stall
// threshold reads as hung — what /_/health exposes so an external monitor can
// catch a manager that has stopped running scans/alerts.
func TestReplicationManagerHung(t *testing.T) {
	orig := replication_manager_heartbeat.Load()
	defer replication_manager_heartbeat.Store(orig)

	replication_manager_heartbeat.Store(now())
	if hung, age := replication_manager_hung(); hung || age < 0 || age > 5 {
		t.Fatalf("fresh heartbeat: hung=%v age=%d, want not-hung with small age", hung, age)
	}

	replication_manager_heartbeat.Store(now() - int64(replication_manager_stall_seconds) - 30)
	if hung, age := replication_manager_hung(); !hung || age <= replication_manager_stall_seconds {
		t.Fatalf("stale heartbeat: hung=%v age=%d, want hung with age > %d", hung, age, replication_manager_stall_seconds)
	}
}
