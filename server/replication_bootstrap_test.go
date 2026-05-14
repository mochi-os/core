// Mochi server: bulk bootstrap unit tests (V1 — scaffolding)
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// TestBootstrapSetAndGetState: round-trip a state + position cursor
// through the bootstrap table; subsequent set replaces the prior row.
func TestBootstrapSetAndGetState(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Absent row → empty state + position.
	state, pos := bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != "" || pos != "" {
		t.Errorf("absent row: got state=%q pos=%q, want both empty", state, pos)
	}

	// First write — queued, no position yet.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_queued, "")
	state, pos = bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_queued || pos != "" {
		t.Errorf("after seed: state=%q pos=%q, want (%q, %q)", state, pos, bootstrap_state_queued, "")
	}

	// Advance to active with a resume cursor.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_active, "cursor-1")
	state, pos = bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_active || pos != "cursor-1" {
		t.Errorf("after advance: state=%q pos=%q, want (%q, %q)", state, pos, bootstrap_state_active, "cursor-1")
	}

	// Transition to done; position retained as the final marker.
	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_done, "cursor-final")
	state, pos = bootstrap_get_state(bootstrap_scope_files, "peer-A")
	if state != bootstrap_state_done || pos != "cursor-final" {
		t.Errorf("after done: state=%q pos=%q, want (%q, %q)", state, pos, bootstrap_state_done, "cursor-final")
	}
}

// TestBootstrapClear: removes the row entirely; subsequent reads
// behave as if the (scope, peer) was never tracked.
func TestBootstrapClear(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	bootstrap_set_state(bootstrap_scope_apps, "peer-A", bootstrap_state_done, "cursor-final")
	bootstrap_clear(bootstrap_scope_apps, "peer-A")

	state, pos := bootstrap_get_state(bootstrap_scope_apps, "peer-A")
	if state != "" || pos != "" {
		t.Errorf("after clear: got state=%q pos=%q, want both empty", state, pos)
	}
}

// TestBootstrapScopesForPeer: every scope for a peer is returned in
// stable order; rows for other peers are excluded.
func TestBootstrapScopesForPeer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	bootstrap_set_state(bootstrap_scope_files, "peer-A", bootstrap_state_done, "f")
	bootstrap_set_state(bootstrap_scope_apps, "peer-A", bootstrap_state_active, "a")
	bootstrap_set_state(bootstrap_scope_userdbs, "peer-A", bootstrap_state_queued, "")
	bootstrap_set_state(bootstrap_scope_files, "peer-OTHER", bootstrap_state_done, "x")

	rows := bootstrap_scopes_for_peer("peer-A")
	if len(rows) != 3 {
		t.Fatalf("rows for peer-A: got %d, want 3", len(rows))
	}
	// Stable order = ORDER BY scope ASC → apps, files, userdbs.
	want := []string{bootstrap_scope_apps, bootstrap_scope_files, bootstrap_scope_userdbs}
	for i, w := range want {
		if rows[i]["scope"] != w {
			t.Errorf("row[%d].scope = %q, want %q", i, rows[i]["scope"], w)
		}
	}

	// Spot-check the states came through.
	if rows[0]["state"] != bootstrap_state_active {
		t.Errorf("apps state = %q, want active", rows[0]["state"])
	}
	if rows[1]["position"] != "f" {
		t.Errorf("files position = %q, want %q", rows[1]["position"], "f")
	}

	// peer-OTHER's row not included.
	for _, r := range rows {
		if r["scope"] == "" {
			t.Errorf("got an empty-scope row: %+v", r)
		}
	}
}
