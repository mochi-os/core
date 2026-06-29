// Operator-free auto-reseed trigger (#101, Phase 2). The wipe-prevention lives in the
// row-level subset swap-guard (replication_subset_guard_test.go); these cover the
// trigger's gating: it is OFF by default (the automation must be inert until
// explicitly enabled), and when enabled it dispatches a reseed of the stalled stream's
// DB from the peer. claude/plans/replication-auto-reseed.md.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// Safety-by-default: with the flag off (the shipped default), the trigger must never
// dispatch — an exhausted gap escalates to an operator exactly as before.
func TestReplicationAutoReseedDefaultOff(t *testing.T) {
	if replication_auto_reseed_enabled {
		t.Fatal("auto-reseed MUST default to OFF (staged rollout) — inert until explicitly enabled")
	}
	if replication_auto_reseed_try(StalledStream{Peer: "p", Scope: "app", User: "u1", Database: "app:x/system"}) {
		t.Fatal("auto-reseed must not dispatch while disabled")
	}
}

// When enabled, the trigger reseeds the stalled stream's DB from the peer.
func TestReplicationAutoReseedDispatchesWhenEnabled(t *testing.T) {
	defer journal_test_dir(t, "u1", "12app")()
	// The DB the stream resolves to (app:12app/system -> users/<u>/12app/app.db).
	appdb := filepath.Join(data_dir, "users", "u1", "12app", "app.db")
	d, err := sql.Open("sqlite3", "file:"+appdb)
	if err != nil {
		t.Fatal(err)
	}
	d.Exec("create table t (id text primary key)")
	d.Close()

	done := make(chan string, 1)
	orig := auto_reseed_dispatch
	auto_reseed_dispatch = func(peer, scope, path string) error { done <- scope + "|" + path; return nil }
	defer func() { auto_reseed_dispatch = orig }()
	replication_auto_reseed_enabled = true
	defer func() { replication_auto_reseed_enabled = false }()

	s := StalledStream{Peer: "peerX", User: "u1", Database: "app:12app/system"}
	if !replication_auto_reseed_try(s) {
		t.Fatal("should dispatch when enabled, path resolvable, and no un-shipped local writes")
	}
	select {
	case got := <-done:
		want := bootstrap_scope_userdbs + "|users/u1/12app/app.db"
		if got != want {
			t.Fatalf("dispatched %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reseed was not dispatched within 2s")
	}
}
