// Mochi server: convergence audit (#29) unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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

// TestAuditTableReplicates: core host-local infra tables are excluded from the
// content count; app data tables are included. post_scores is NOT core anymore
// (it's app-declared) so against the core set it counts — see
// TestAuditExcludesForPath for the per-app derivation.
func TestAuditTableReplicates(t *testing.T) {
	core := audit_local_tables_core
	for _, name := range []string{"journal", "commits", "idempotency", "sequence", "received", "log", "acknowledged", "pending", "email_delivered", "sqlite_master", ""} {
		if audit_table_replicates(name, core) {
			t.Errorf("%q should be host-local (excluded)", name)
		}
	}
	for _, name := range []string{"posts", "feeds", "sources", "items", "post_scores"} {
		if !audit_table_replicates(name, core) {
			t.Errorf("%q should count as replicated against the core set", name)
		}
	}
}

// TestAuditExcludesForPath: the resolver merges core infra with each app's
// app.json declarations, keyed by the owning app so same-named tables in
// different apps don't collide, and uses core columns for core DBs.
func TestAuditExcludesForPath(t *testing.T) {
	// Path parsing: only users/<u>/<app>/db/<file> resolves an app id.
	for path, want := range map[string]string{
		"users/U/APPID/db/feeds.db": "APPID",
		"users/U/APPID/app.db":      "",
		"users/U/user.db":           "",
		"db/sessions.db":            "",
	} {
		if got := audit_app_id_from_path(path); got != want {
			t.Errorf("audit_app_id_from_path(%q) = %q, want %q", path, got, want)
		}
	}

	// Two apps with a same-named `tags` table: APP1 declares tags.relevance
	// host-local, APP2 declares nothing. The resolver must scope to the owner.
	mk := func(id string, tables []string, cols map[string][]string) *App {
		av := &AppVersion{}
		av.Database.Replicate.Exclude.Tables = tables
		av.Database.Replicate.Exclude.Columns = cols
		return &App{id: id, internal: av}
	}
	apps_lock.Lock()
	apps["AUDITT1"] = mk("AUDITT1", []string{"post_scores"}, map[string][]string{"tags": {"relevance"}})
	apps["AUDITT2"] = mk("AUDITT2", nil, nil)
	apps_lock.Unlock()
	defer func() {
		apps_lock.Lock()
		delete(apps, "AUDITT1")
		delete(apps, "AUDITT2")
		apps_lock.Unlock()
	}()

	tables1, columns1 := audit_excludes_for_path("users/U/AUDITT1/db/feeds.db")
	if !tables1["journal"] || !tables1["commits"] {
		t.Error("APP1 data DB should still carry the core infra table exclusions")
	}
	if !tables1["post_scores"] {
		t.Error("APP1 data DB should pick up the declared post_scores table exclusion")
	}
	if columns1["tags"] == nil || !columns1["tags"]["relevance"] {
		t.Error("APP1 should exclude its declared tags.relevance column")
	}

	_, columns2 := audit_excludes_for_path("users/U/AUDITT2/db/feeds.db")
	if columns2["tags"]["relevance"] {
		t.Error("APP2's tags.relevance must NOT be excluded — collision across apps")
	}

	// Core DB path: no app, core columns apply (user.db accounts.last_delivered).
	tablesCore, columnsCore := audit_excludes_for_path("users/U/user.db")
	if !tablesCore["journal"] {
		t.Error("core DB should carry the core infra table exclusions")
	}
	if columnsCore["accounts"] == nil || !columnsCore["accounts"]["last_delivered"] {
		t.Error("core DB should exclude accounts.last_delivered")
	}

	// App-system DB (app.db) also carries the core host-local columns, so the
	// per-host attachments.entity (owned "" vs foreign source) pointer is
	// excluded from the convergence hash. (#68)
	_, columnsAppSys := audit_excludes_for_path("users/U/APPID/app.db")
	if columnsAppSys["attachments"] == nil || !columnsAppSys["attachments"]["entity"] {
		t.Error("app.db should exclude attachments.entity")
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
	replication_audit_liveness(peer, nil, manifest(200))
	if alerted() {
		t.Fatal("first round must not alert (freeze needs a second round)")
	}
	replication_audit_liveness(peer, nil, manifest(200))
	if !alerted() {
		t.Fatal("behind + frozen should alert")
	}

	// Behind but advancing (lag) -> no alert.
	reset()
	setCursor(100)
	replication_audit_liveness(peer, nil, manifest(200))
	setCursor(150)
	replication_audit_liveness(peer, nil, manifest(200))
	if alerted() {
		t.Fatal("advancing stream (lag, not stuck) should not alert")
	}

	// Caught up -> no alert.
	reset()
	setCursor(200)
	replication_audit_liveness(peer, nil, manifest(200))
	replication_audit_liveness(peer, nil, manifest(200))
	if alerted() {
		t.Fatal("caught-up stream should not alert")
	}

	// Peer doesn't originate it (tail 0) -> no alert.
	reset()
	setCursor(100)
	replication_audit_liveness(peer, nil, manifest(0))
	replication_audit_liveness(peer, nil, manifest(0))
	if alerted() {
		t.Fatal("stream not originated by peer (tail 0) should not alert")
	}

	// Re-arm: alerted, then catches up -> alert cleared.
	reset()
	setCursor(100)
	replication_audit_liveness(peer, nil, manifest(200))
	replication_audit_liveness(peer, nil, manifest(200))
	if !alerted() {
		t.Fatal("setup: should be alerted")
	}
	setCursor(200)
	replication_audit_liveness(peer, nil, manifest(200))
	if alerted() {
		t.Fatal("catching up should clear the liveness alert (re-arm)")
	}
}

// TestReplicationAuditLivenessConverged (#55): a frozen cursor whose CONTENT is
// already converged must NOT alert — the multi-host false positive where the same
// rows arrived via another path, leaving this per-peer cursor permanently behind
// with no data missing. The convergence verdict (audit_stream_converged) is stubbed
// so the gate is exercised without a live peer: stubbed converged -> silent even
// when frozen below tail; stubbed not-converged -> the genuine-gap path still alerts.
func TestReplicationAuditLivenessConverged(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	origConverged := audit_stream_converged
	defer func() {
		data_dir = orig
		audit_stream_converged = origConverged
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
	rdb.exec("insert into cursor (peer, scope, user, db, sequence) values (?, ?, 'u1', ?, 0) on conflict(peer, scope, user, db) do update set sequence=0",
		peer, repl_scope_app, stream)
	manifest := []AuditStream{{User: "u1", Stream: stream, Count: 5, Tail: 19}}
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

	// Converged content -> suppress, even though the cursor (0) is frozen below tail (19).
	audit_stream_converged = func(string, AuditStream, map[string]int64) bool { return true }
	reset()
	replication_audit_liveness(peer, nil, manifest) // round 1: record cursor
	replication_audit_liveness(peer, nil, manifest) // round 2: frozen, but converged
	if alerted() {
		t.Fatal("converged stream (content matches peer) must not alert despite a frozen cursor")
	}

	// Not converged (genuine gap or dropped UPDATE) -> still alerts.
	audit_stream_converged = func(string, AuditStream, map[string]int64) bool { return false }
	reset()
	replication_audit_liveness(peer, nil, manifest)
	replication_audit_liveness(peer, nil, manifest)
	if !alerted() {
		t.Fatal("non-converged frozen stream must still alert")
	}
}

// TestReplicationAuditLivenessHomeHostSkipped (#62) checks that a stream THIS
// host originates (it has a local emitted tail) is never treated as a
// not-advancing candidate: the peer's tail is just our own ops echoed back, so
// our per-peer cursor lags by design. It must be skipped before the convergence
// check — otherwise a home-host cursor=0 stream stays a permanent frozen
// candidate that re-alerts whenever the phase-2 hash fetch flaps (the #60 chat
// false positive). A pure receiver (no local tail) must still alert.
func TestReplicationAuditLivenessHomeHostSkipped(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	origConverged := audit_stream_converged
	defer func() {
		data_dir = orig
		audit_stream_converged = origConverged
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
	rdb.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")

	peer, stream := "peerP", "app:chat"
	rdb.exec("insert into cursor (peer, scope, user, db, sequence) values (?, ?, 'u1', ?, 0)", peer, repl_scope_app, stream)
	manifest := []AuditStream{{User: "u1", Stream: stream, Count: 5, Tail: 19}}
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

	called := false
	audit_stream_converged = func(string, AuditStream, map[string]int64) bool { called = true; return false }

	// Pure receiver (no local tail): frozen + not converged -> alerts (no regression).
	reset()
	replication_audit_liveness(peer, nil, manifest)
	replication_audit_liveness(peer, nil, manifest)
	if !alerted() {
		t.Fatal("pure-receiver frozen stream must still alert")
	}

	// This host originates the stream (local tail > 0): skipped entirely.
	rdb.exec("insert into tail (user, scope, db, last) values ('u1', ?, ?, 7)", repl_scope_app, stream)
	reset()
	called = false
	replication_audit_liveness(peer, nil, manifest)
	replication_audit_liveness(peer, nil, manifest)
	if alerted() {
		t.Fatal("home-host-originated stream (local tail>0) must not be flagged as not-advancing")
	}
	if called {
		t.Fatal("home-host stream must be skipped before the convergence check")
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

// TestAuditRowHash (#36): the per-row hash is independent of column map order and
// keeps int 0, string "0", and NULL distinct (a type tag), so a value-type change
// can't masquerade as identical content.
func TestAuditRowHash(t *testing.T) {
	if audit_row_hash(map[string]any{"x": int64(1), "y": "two"}, nil) != audit_row_hash(map[string]any{"y": "two", "x": int64(1)}, nil) {
		t.Fatal("row hash must be independent of map key order")
	}
	h0 := audit_row_hash(map[string]any{"v": int64(0)}, nil)
	hs := audit_row_hash(map[string]any{"v": "0"}, nil)
	hn := audit_row_hash(map[string]any{"v": nil}, nil)
	if h0 == hs || h0 == hn || hs == hn {
		t.Fatal(`int 0, string "0", and NULL must hash distinctly`)
	}
	// Excluded host-local columns don't affect the hash (#45): two rows differing
	// ONLY in an excluded column hash equal; differing in a non-excluded one don't.
	excl := map[string]bool{"score": true}
	if audit_row_hash(map[string]any{"id": "a", "score": int64(1)}, excl) != audit_row_hash(map[string]any{"id": "a", "score": int64(99)}, excl) {
		t.Fatal("rows differing only in an excluded column must hash equal")
	}
	if audit_row_hash(map[string]any{"id": "a", "score": int64(1)}, excl) == audit_row_hash(map[string]any{"id": "b", "score": int64(1)}, excl) {
		t.Fatal("rows differing in a non-excluded column must hash differently")
	}
}

// TestDbReplicatedContentHash (#36): the content hash is order-independent, is
// SENSITIVE to a diverged UPDATE that leaves the row count unchanged (the class a
// count audit misses), and ignores host-local tables.
func TestDbReplicatedContentHash(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	mk := func(rel string, setup func(db *DB)) string {
		if err := os.MkdirAll(filepath.Join(data_dir, filepath.FromSlash(filepath.Dir(rel))), 0o755); err != nil {
			t.Fatal(err)
		}
		setup(db_open(rel))
		return db_replicated_content_hash(rel)
	}
	h1 := mk("users/u1/app1/db/feeds.db", func(db *DB) {
		db.exec("create table posts (id text primary key, body text)")
		db.exec("insert into posts (id, body) values ('a','x'),('b','y'),('c','z')")
	})
	// Same rows inserted in a DIFFERENT order -> SAME hash (order-independent).
	h2 := mk("users/u2/app1/db/feeds.db", func(db *DB) {
		db.exec("create table posts (id text primary key, body text)")
		db.exec("insert into posts (id, body) values ('c','z'),('a','x'),('b','y')")
	})
	if h1 == "" || h1 != h2 {
		t.Fatalf("same content in different order must hash equal: %q vs %q", h1, h2)
	}
	// A diverged UPDATE (same row count, one value changed) -> DIFFERENT hash.
	h3 := mk("users/u3/app1/db/feeds.db", func(db *DB) {
		db.exec("create table posts (id text primary key, body text)")
		db.exec("insert into posts (id, body) values ('a','x'),('b','y'),('c','DIFFERENT')")
	})
	if h3 == h1 {
		t.Fatal("a diverged UPDATE with equal row count must change the content hash")
	}
	// A host-local table (journal) must not affect the content hash.
	h4 := mk("users/u4/app1/db/feeds.db", func(db *DB) {
		db.exec("create table posts (id text primary key, body text)")
		db.exec("insert into posts (id, body) values ('a','x'),('b','y'),('c','z')")
		db.exec("create table journal (id text primary key)")
		db.exec("insert into journal (id) values ('j1'),('j2')")
	})
	if h4 != h1 {
		t.Fatal("host-local journal table must not affect the content hash")
	}
}

// TestReplicationAuditContentCompare (#36): alerts only when counts MATCH, hashes
// are stable on both sides since the last round, and the hashes DIFFER. Count
// divergence is the count compare's job; a still-settling hash is lag; convergence
// re-arms.
func TestReplicationAuditContentCompare(t *testing.T) {
	peer := "peerX"
	reset := func() {
		audit_convergence_mutex.Lock()
		audit_content_alerted = map[string]bool{}
		audit_convergence_mutex.Unlock()
	}
	alerted := func() bool {
		audit_convergence_mutex.Lock()
		defer audit_convergence_mutex.Unlock()
		return audit_content_alerted[peer+"|u|s"]
	}
	hm := func(v string) map[string]string { return map[string]string{"u|s": v} }
	cm := func(v int64) map[string]int64 { return map[string]int64{"u|s": v} }

	// Counts match, hashes stable-but-different on both -> content divergence.
	reset()
	replication_audit_content_compare(peer, hm("A"), hm("A"), hm("B"), hm("B"), cm(100), cm(100))
	if !alerted() {
		t.Fatal("count-equal + stable-but-different hashes should alert")
	}

	// Equal hashes -> no alert.
	reset()
	replication_audit_content_compare(peer, hm("A"), hm("A"), hm("A"), hm("A"), cm(100), cm(100))
	if alerted() {
		t.Fatal("equal hashes should not alert")
	}

	// Counts differ -> deferred to the count compare, not alerted here.
	reset()
	replication_audit_content_compare(peer, hm("A"), hm("A"), hm("B"), hm("B"), cm(100), cm(99))
	if alerted() {
		t.Fatal("count divergence is the count compare's job")
	}

	// Hash still settling on the local side -> lag, no alert.
	reset()
	replication_audit_content_compare(peer, hm("A"), hm("Z"), hm("B"), hm("B"), cm(100), cm(100))
	if alerted() {
		t.Fatal("a still-settling hash should not alert")
	}

	// Peer didn't hash it (empty) -> can't compare.
	reset()
	replication_audit_content_compare(peer, hm("A"), hm("A"), hm(""), hm(""), cm(100), cm(100))
	if alerted() {
		t.Fatal("empty peer hash should not alert")
	}

	// Re-arm: a content-diverged stream that converges clears its alert.
	reset()
	replication_audit_content_compare(peer, hm("A"), hm("A"), hm("B"), hm("B"), cm(100), cm(100))
	if !alerted() {
		t.Fatal("setup: should be alerted")
	}
	replication_audit_content_compare(peer, hm("A"), hm("A"), hm("A"), hm("A"), cm(100), cm(100))
	if alerted() {
		t.Fatal("converged content should clear its alert (re-arm)")
	}
}

// TestAuditContentCandidates (#48): only streams present on BOTH hosts with equal
// counts are content-hash candidates — count-diverging or one-sided streams aren't.
func TestAuditContentCandidates(t *testing.T) {
	local := map[string]int64{"u|a": 10, "u|b": 5, "u|c": 3}
	remote := map[string]int64{"u|a": 10, "u|b": 7, "u|d": 1}
	got := audit_content_candidates(local, remote)
	if len(got) != 1 || got[0].User != "u" || got[0].Stream != "a" {
		t.Fatalf("want exactly [{u a}] (equal count, both present), got %v", got)
	}
}

// TestReplicationAuditContentHash (#48): the mtime-cached hash is stable for
// unchanged content, recomputes when the DB changes, and is "" for a missing DB.
func TestReplicationAuditContentHash(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	if replication_audit_content_hash("db/missing.db") != "" {
		t.Fatal("missing DB must hash to \"\"")
	}
	db := db_open("db/test.db")
	db.exec("create table t (id text primary key, v text)")
	db.exec("insert into t values ('1','a')")
	h1 := replication_audit_content_hash("db/test.db")
	if h1 == "" {
		t.Fatal("expected a non-empty hash")
	}
	if replication_audit_content_hash("db/test.db") != h1 {
		t.Fatal("unchanged content must hash the same")
	}
	db.exec("insert into t values ('2','b')")
	if replication_audit_content_hash("db/test.db") == h1 {
		t.Fatal("changed content must produce a different hash (cache keyed on mtime)")
	}
}
