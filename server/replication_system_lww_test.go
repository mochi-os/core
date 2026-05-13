// Mochi server: system-LWW replication unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// setup_system_lww_test prepares the data_dir + settings.db with the
// LWW schema and stubs the replication-emit so apply tests can run
// without spawning goroutines that outlive cleanup. Returns a cleanup.
func setup_system_lww_test(t *testing.T) func() {
	cleanup := setup_replication_test(t)
	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings (name text primary key, value text not null, ts integer not null default 0, peer text not null default '')")
	return cleanup
}

// TestSystemLWWApplySettingsFreshRow: a system-lww op for a row that
// doesn't exist locally inserts cleanly.
func TestSystemLWWApplySettingsFreshRow(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	replication_system_lww_apply("peer-A", &SystemLWWSet{
		Database: "settings", Table: "settings",
		Row: "signup_enabled", Field: "value",
		Value: "true", TS: 100, Peer: "peer-A",
	})

	if got := setting_get("signup_enabled", ""); got != "true" {
		t.Errorf("setting_get = %q, want %q", got, "true")
	}
}

// TestSystemLWWApplyHigherTSWins: an incoming op with a higher ts
// overwrites the local value.
func TestSystemLWWApplyHigherTSWins(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	db := db_open("db/settings.db")
	db.exec("replace into settings (name, value, ts, peer) values ('k', 'old', 100, 'peer-A')")

	replication_system_lww_apply("peer-B", &SystemLWWSet{
		Database: "settings", Table: "settings",
		Row: "k", Field: "value",
		Value: "new", TS: 200, Peer: "peer-B",
	})

	if got := setting_get("k", ""); got != "new" {
		t.Errorf("after higher-ts apply = %q, want %q", got, "new")
	}
}

// TestSystemLWWApplyLowerTSLoses: an incoming op with a lower ts is
// ignored — local row stays.
func TestSystemLWWApplyLowerTSLoses(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	db := db_open("db/settings.db")
	db.exec("replace into settings (name, value, ts, peer) values ('k', 'current', 200, 'peer-A')")

	replication_system_lww_apply("peer-B", &SystemLWWSet{
		Database: "settings", Table: "settings",
		Row: "k", Field: "value",
		Value: "stale", TS: 100, Peer: "peer-B",
	})

	if got := setting_get("k", ""); got != "current" {
		t.Errorf("after lower-ts apply = %q, want %q (unchanged)", got, "current")
	}
}

// TestSystemLWWApplyEqualTSPeerTiebreak: equal ts → higher peer-id lex
// wins (deterministic conflict resolution).
func TestSystemLWWApplyEqualTSPeerTiebreak(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	db := db_open("db/settings.db")
	db.exec("replace into settings (name, value, ts, peer) values ('k', 'from-aaa', 100, 'peer-aaa')")

	// Incoming peer-bbb > peer-aaa lex, equal ts → wins.
	replication_system_lww_apply("peer-bbb", &SystemLWWSet{
		Database: "settings", Table: "settings",
		Row: "k", Field: "value",
		Value: "from-bbb", TS: 100, Peer: "peer-bbb",
	})
	if got := setting_get("k", ""); got != "from-bbb" {
		t.Errorf("after equal-ts higher-peer apply = %q, want %q", got, "from-bbb")
	}

	// Now reverse: incoming peer-000 < peer-bbb lex, same ts → loses.
	replication_system_lww_apply("peer-000", &SystemLWWSet{
		Database: "settings", Table: "settings",
		Row: "k", Field: "value",
		Value: "from-000", TS: 100, Peer: "peer-000",
	})
	if got := setting_get("k", ""); got != "from-bbb" {
		t.Errorf("after equal-ts lower-peer apply = %q, want %q (unchanged)", got, "from-bbb")
	}
}

// TestSystemLWWApplyIdempotent: re-applying the same op is a no-op.
func TestSystemLWWApplyIdempotent(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	op := &SystemLWWSet{
		Database: "settings", Table: "settings",
		Row: "k", Field: "value",
		Value: "once", TS: 100, Peer: "peer-A",
	}
	replication_system_lww_apply("peer-A", op)
	replication_system_lww_apply("peer-A", op)
	replication_system_lww_apply("peer-A", op)

	if got := setting_get("k", ""); got != "once" {
		t.Errorf("after repeated apply = %q, want %q", got, "once")
	}
}

// TestSystemLWWApplyRejectsUnknownDestination: an op for a table we
// don't handle is silently dropped (warn-logged).
func TestSystemLWWApplyRejectsUnknownDestination(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	replication_system_lww_apply("peer-A", &SystemLWWSet{
		Database: "nope", Table: "nope",
		Row: "k", Field: "value", Value: "v", TS: 100, Peer: "peer-A",
	})
	// No row should be created in the settings table.
	db := db_open("db/settings.db")
	if exists, _ := db.exists("select 1 from settings where name='k'"); exists {
		t.Error("unknown destination should not affect settings")
	}
}

// TestSystemLWWApplyRejectsMissingFields: required-field validation.
func TestSystemLWWApplyRejectsMissingFields(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	cases := []SystemLWWSet{
		{Database: "", Table: "settings", Row: "k", Field: "value", Value: "v", TS: 100},
		{Database: "settings", Table: "", Row: "k", Field: "value", Value: "v", TS: 100},
		{Database: "settings", Table: "settings", Row: "", Field: "value", Value: "v", TS: 100},
		{Database: "settings", Table: "settings", Row: "k", Field: "", Value: "v", TS: 100},
	}
	for _, c := range cases {
		replication_system_lww_apply("peer-A", &c)
	}
	db := db_open("db/settings.db")
	if exists, _ := db.exists("select 1 from settings where name='k'"); exists {
		t.Error("missing-field op should not write")
	}
}

// TestSettingSetWritesTSAndPeer: setting_set populates ts and peer
// in the row alongside value.
func TestSettingSetWritesTSAndPeer(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	before := now()
	setting_set("k", "v")
	after := now()

	db := db_open("db/settings.db")
	row, _ := db.row("select value, ts, peer from settings where name='k'")
	if row == nil {
		t.Fatal("row should exist after setting_set")
	}
	if got, _ := row["value"].(string); got != "v" {
		t.Errorf("value = %q, want %q", got, "v")
	}
	ts, _ := row["ts"].(int64)
	if ts < before || ts > after {
		t.Errorf("ts = %d, want in [%d, %d]", ts, before, after)
	}
	if got, _ := row["peer"].(string); got != p2p_id {
		t.Errorf("peer = %q, want %q", got, p2p_id)
	}
}
