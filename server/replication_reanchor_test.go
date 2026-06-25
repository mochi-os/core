// Mochi server: #33 regression — a populated host with a cursor-misaligned
// (unanchored) stream must AUTO re-anchor via the safe targeted reseed, not sit
// stalled forever (the prod yuzu↔wasabi SKIP-loop) nor get whole-user re-pulled
// (the #27 SEV1 wipe). The reseed is gated per-DB on reseed_source_missing_ops:
// fire only when the source is authoritative (not missing any op we
// originated); a source that is behind us is genuine divergence and must NOT be
// overwritten.
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
	"time"
)

// reanchor_reset clears the package-global recovery state so each test starts
// from a clean backoff map and concurrency table.
func reanchor_reset() {
	rebootstrap_mutex.Lock()
	rebootstrap_attempts = map[string]rebootstrap_state{}
	rebootstrap_mutex.Unlock()
	reanchor_mutex.Lock()
	reanchor_inflight = map[string]bool{}
	reanchor_mutex.Unlock()
}

// mkapp creates an app data DB at the standard users/<u>/<entity>/db/<file>
// path. journalPending>0 adds a journal table with that many pending ops, which
// makes reseed_source_missing_ops report the source is missing our un-shipped
// writes (divergence); journalPending==0 with no journal table leaves the host
// a pure receiver (source authoritative).
func mkapp(t *testing.T, uid, entity, file string, rows, journalPending int) {
	t.Helper()
	dir := filepath.Join(data_dir, "users", uid, entity, "db")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	d, err := sql.Open("sqlite3", "file:"+filepath.Join(dir, file))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Exec("create table posts (id integer primary key, body text)")
	for i := 0; i < rows; i++ {
		d.Exec("insert into posts (body) values ('row')")
	}
	if journalPending > 0 {
		d.Exec("create table journal (id integer primary key, state text)")
		for i := 0; i < journalPending; i++ {
			d.Exec("insert into journal (state) values ('pending')")
		}
	}
}

// TestReanchorReseedsCleanCatchup: populated host, unanchored stream, source
// authoritative → the auto-recovery launches the targeted reseed for the DB.
// Without the fix the populated branch just SKIPs and the stream never anchors.
func TestReanchorReseedsCleanCatchup(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	reanchor_reset()

	type call struct{ peer, scope, path string }
	got := make(chan call, 4)
	orig := bootstrap_db_reseed
	bootstrap_db_reseed = func(peer, scope, path string) error {
		got <- call{peer, scope, path}
		return nil
	}
	defer func() { bootstrap_db_reseed = orig }()

	uid, entity := "uA", "12entityA"
	mkapp(t, uid, entity, "feeds.db", 3, 0) // rows, no journal → source authoritative

	replication_reanchor_misaligned(StalledStream{
		Peer: "peerX", Scope: repl_scope_app, User: uid, Database: "app:" + entity,
	})

	select {
	case c := <-got:
		want := "users/" + uid + "/" + entity + "/db/feeds.db"
		if c.path != want {
			t.Errorf("reseed path = %q, want %q", c.path, want)
		}
		if c.peer != "peerX" {
			t.Errorf("reseed peer = %q, want peerX", c.peer)
		}
		if c.scope != bootstrap_scope_userdbs {
			t.Errorf("reseed scope = %q, want %q", c.scope, bootstrap_scope_userdbs)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("#33: clean catch-up did NOT trigger a targeted reseed — the misaligned stream stays stalled forever (the prod SKIP-loop)")
	}
}

// TestReanchorSkipsDivergentSource: populated host whose DB carries un-shipped
// local writes the source lacks → reseed_source_missing_ops is true → the
// overwrite must be refused (it would lose local-origin rows — the #27 hazard).
func TestReanchorSkipsDivergentSource(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	reanchor_reset()

	called := make(chan struct{}, 4)
	orig := bootstrap_db_reseed
	bootstrap_db_reseed = func(peer, scope, path string) error { called <- struct{}{}; return nil }
	defer func() { bootstrap_db_reseed = orig }()

	uid, entity := "uB", "12entityB"
	mkapp(t, uid, entity, "feeds.db", 3, 2) // 2 pending journal ops → source behind us

	replication_reanchor_misaligned(StalledStream{
		Peer: "peerX", Scope: repl_scope_app, User: uid, Database: "app:" + entity,
	})

	select {
	case <-called:
		t.Error("#27: reseed OVERWROTE a divergent DB whose source lacks our un-shipped ops — data loss")
	case <-time.After(500 * time.Millisecond):
		// good: no reseed launched for a behind-source DB
	}
}

// TestWipedRebootstrapReanchorsPopulated is the wiring test: through the actual
// recovery loop, a populated host's misaligned (unanchored) stream must be
// re-anchored via the targeted reseed (#33) and must NEVER be whole-user
// re-pulled (#27). Reverting the populated branch to a bare SKIP turns this red.
func TestWipedRebootstrapReanchorsPopulated(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	reanchor_reset()

	var pulled []string
	op := bootstrap_start_user
	bootstrap_start_user = func(peer, uid string) { pulled = append(pulled, uid) }
	defer func() { bootstrap_start_user = op }()

	reseeded := make(chan string, 4)
	or := bootstrap_db_reseed
	bootstrap_db_reseed = func(peer, scope, path string) error { reseeded <- path; return nil }
	defer func() { bootstrap_db_reseed = or }()

	uid, entity := "uPop", "12entityPop"
	mkapp(t, uid, entity, "feeds.db", 3, 0) // populated, source authoritative

	rdb := db_open("db/replication.db")
	aged := now() - (rebootstrap_unanchored_seconds + 100)
	rdb.exec("insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peerX', 'app', ?, ?, 5, 4, 0, x'', ?)",
		uid, "app:"+entity, aged)

	found := false
	for _, s := range replication_pending_stalled() {
		if s.User == uid {
			found = true
		}
	}
	if !found {
		t.Fatal("setup: populated stream not stalled+unanchored")
	}

	replication_wiped_rebootstrap()

	for _, u := range pulled {
		if u == uid {
			t.Errorf("#27: populated user %q was whole-user re-pulled (would wipe real data)", uid)
		}
	}
	select {
	case p := <-reseeded:
		if p != "users/"+uid+"/"+entity+"/db/feeds.db" {
			t.Errorf("reseed path = %q, want the feeds.db path", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("#33: populated misaligned stream was NOT re-anchored via reseed — it would stall forever")
	}
}

// TestStreamDbPaths pins the stream-key → DB-path inverse used to target the
// reseed (only existing files returned).
func TestStreamDbPaths(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	uid := "uC"
	touch := func(parts ...string) {
		p := filepath.Join(append([]string{data_dir, "users", uid}, parts...)...)
		os.MkdirAll(filepath.Dir(p), 0o755)
		os.WriteFile(p, nil, 0o644)
	}
	touch("12appC", "db", "feeds.db")
	touch("12appC", "db", "extra.db")
	touch("12appC", "app.db")
	touch("user.db")

	has := func(list []string, want string) bool {
		for _, v := range list {
			if v == want {
				return true
			}
		}
		return false
	}

	app := replication_stream_db_paths(uid, "app:12appC")
	if len(app) != 2 || !has(app, "users/uC/12appC/db/feeds.db") || !has(app, "users/uC/12appC/db/extra.db") {
		t.Errorf("app:<id> → %v, want both db/*.db files", app)
	}
	if sys := replication_stream_db_paths(uid, "app:12appC/system"); len(sys) != 1 || sys[0] != "users/uC/12appC/app.db" {
		t.Errorf("app:<id>/system → %v, want [users/uC/12appC/app.db]", sys)
	}
	if core := replication_stream_db_paths(uid, "core:user"); len(core) != 1 || core[0] != "users/uC/user.db" {
		t.Errorf("core:user → %v, want [users/uC/user.db]", core)
	}
	if missing := replication_stream_db_paths(uid, "core:nonexistent"); missing != nil {
		t.Errorf("core:nonexistent (no file) → %v, want nil", missing)
	}
}
