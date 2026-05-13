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

// TestSystemLWWApplyAppsClassesFresh: a class-binding op for a row
// that doesn't exist locally inserts cleanly.
func TestSystemLWWApplyAppsClassesFresh(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	db_apps() // create tables

	replication_system_lww_apply("peer-A", &SystemLWWSet{
		Database: "apps", Table: "classes",
		Row: "feed", Field: "app",
		Value: "feeds", TS: 100, Peer: "peer-A",
	})

	if got := apps_class_get("feed"); got != "feeds" {
		t.Errorf("apps_class_get = %q, want %q", got, "feeds")
	}
}

// TestSystemLWWApplyAppsClassesLWW: lower ts loses against the existing row.
func TestSystemLWWApplyAppsClassesLWW(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	db := db_apps()
	db.exec("replace into classes (class, app, ts, peer) values ('feed', 'current', 200, 'peer-X')")

	replication_system_lww_apply("peer-Y", &SystemLWWSet{
		Database: "apps", Table: "classes",
		Row: "feed", Field: "app",
		Value: "stale", TS: 100, Peer: "peer-Y",
	})
	if got := apps_class_get("feed"); got != "current" {
		t.Errorf("after stale apply = %q, want unchanged %q", got, "current")
	}
}

// TestSystemLWWApplyAppsClassesDelete: empty-value op deletes the row.
func TestSystemLWWApplyAppsClassesDelete(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	db := db_apps()
	db.exec("replace into classes (class, app, ts, peer) values ('feed', 'current', 100, 'peer-X')")

	replication_system_lww_apply("peer-Y", &SystemLWWSet{
		Database: "apps", Table: "classes",
		Row: "feed", Field: "app",
		Value: "", TS: 200, Peer: "peer-Y",
	})
	if got := apps_class_get("feed"); got != "" {
		t.Errorf("after delete-via-lww = %q, want empty", got)
	}
}

// TestSystemLWWApplyAppsServicesAndPaths: same machinery for services /
// paths.
func TestSystemLWWApplyAppsServicesAndPaths(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	db_apps()

	replication_system_lww_apply("peer-A", &SystemLWWSet{
		Database: "apps", Table: "services",
		Row: "feeds", Field: "app",
		Value: "feeds", TS: 100, Peer: "peer-A",
	})
	if got := apps_service_get("feeds"); got != "feeds" {
		t.Errorf("services apply: get = %q, want %q", got, "feeds")
	}

	replication_system_lww_apply("peer-A", &SystemLWWSet{
		Database: "apps", Table: "paths",
		Row: "/feeds/", Field: "app",
		Value: "feeds", TS: 100, Peer: "peer-A",
	})
	if got := apps_path_get("/feeds/"); got != "feeds" {
		t.Errorf("paths apply: get = %q, want %q", got, "feeds")
	}
}

// TestSystemLWWApplyAppsAppsInstall: apps.apps row insert via LWW.
func TestSystemLWWApplyAppsAppsInstall(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	db_apps()

	replication_system_lww_apply("peer-A", &SystemLWWSet{
		Database: "apps", Table: "apps",
		Row: "feeds", Field: "installed",
		Value: "1234567890", TS: 100, Peer: "peer-A",
	})

	if got := apps_installed("feeds"); got != 1234567890 {
		t.Errorf("apps_installed after apply = %d, want %d", got, int64(1234567890))
	}
}

// setup_domains_test_schema creates a minimal domains.db schema for
// row-level LWW tests. Mirrors db_open's domains schema with the LWW
// columns from v58.
func setup_domains_test_schema() {
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null, ts integer not null default 0, peer text not null default '')")
}

// TestSystemLWWRowApplyDomainsFresh: a row-level op for a domain not
// present locally inserts cleanly.
func TestSystemLWWRowApplyDomainsFresh(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	setup_domains_test_schema()

	replication_system_lww_row_apply("peer-A", &SystemLWWRow{
		Database: "domains", Table: "domains",
		Key: map[string]string{"domain": "example.com"},
		Cols: map[string]string{
			"verified": "0",
			"token":    "tok123",
			"tls":      "1",
			"created":  "100",
			"updated":  "100",
		},
		TS: 100, Peer: "peer-A",
	})

	db := db_open("db/domains.db")
	row, _ := db.row("select domain, token, ts, peer from domains where domain='example.com'")
	if row == nil {
		t.Fatal("row should exist after row-level apply")
	}
	if got, _ := row["token"].(string); got != "tok123" {
		t.Errorf("token = %q, want tok123", got)
	}
}

// TestSystemLWWRowApplyDomainsHigherTSWins: incoming higher-ts op
// overwrites the local row.
func TestSystemLWWRowApplyDomainsHigherTSWins(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	setup_domains_test_schema()
	db := db_open("db/domains.db")
	db.exec("replace into domains (domain, verified, token, tls, created, updated, ts, peer) values ('example.com', 0, 'old', 1, 100, 100, 100, 'peer-X')")

	replication_system_lww_row_apply("peer-Y", &SystemLWWRow{
		Database: "domains", Table: "domains",
		Key:  map[string]string{"domain": "example.com"},
		Cols: map[string]string{"verified": "1", "token": "new", "tls": "1", "created": "100", "updated": "200"},
		TS:   200, Peer: "peer-Y",
	})

	row, _ := db.row("select token from domains where domain='example.com'")
	if got, _ := row["token"].(string); got != "new" {
		t.Errorf("token after higher-ts apply = %q, want new", got)
	}
}

// TestSystemLWWRowApplyDomainsLowerTSLoses: lower-ts incoming is
// ignored.
func TestSystemLWWRowApplyDomainsLowerTSLoses(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	setup_domains_test_schema()
	db := db_open("db/domains.db")
	db.exec("replace into domains (domain, verified, token, tls, created, updated, ts, peer) values ('example.com', 0, 'current', 1, 100, 100, 200, 'peer-X')")

	replication_system_lww_row_apply("peer-Y", &SystemLWWRow{
		Database: "domains", Table: "domains",
		Key:  map[string]string{"domain": "example.com"},
		Cols: map[string]string{"token": "stale"},
		TS:   100, Peer: "peer-Y",
	})

	row, _ := db.row("select token from domains where domain='example.com'")
	if got, _ := row["token"].(string); got != "current" {
		t.Errorf("after lower-ts apply = %q, want unchanged current", got)
	}
}

// TestSystemLWWRowApplyDomainsDelete: incoming op with Delete=true
// removes the local row.
func TestSystemLWWRowApplyDomainsDelete(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	setup_domains_test_schema()
	db := db_open("db/domains.db")
	db.exec("replace into domains (domain, verified, token, tls, created, updated, ts, peer) values ('example.com', 0, 't', 1, 100, 100, 100, 'peer-X')")

	replication_system_lww_row_apply("peer-Y", &SystemLWWRow{
		Database: "domains", Table: "domains",
		Key:    map[string]string{"domain": "example.com"},
		Delete: true,
		TS:     200, Peer: "peer-Y",
	})

	exists, _ := db.exists("select 1 from domains where domain='example.com'")
	if exists {
		t.Error("domain should be deleted after delete-op apply")
	}
}

// TestSystemLWWRowApplyRejectsUnknownDestination: unknown destination
// is silently dropped.
func TestSystemLWWRowApplyRejectsUnknownDestination(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()
	setup_domains_test_schema()

	replication_system_lww_row_apply("peer-A", &SystemLWWRow{
		Database: "nope", Table: "nope",
		Key: map[string]string{"k": "v"},
		TS:  100, Peer: "peer-A",
	})
	// No side effect — just verify no panic.
}

// TestAppsClassSetEmitsAndStamps: apps_class_set writes (ts, peer) and
// fires the system-LWW emit.
func TestAppsClassSetEmitsAndStamps(t *testing.T) {
	cleanup := setup_system_lww_test(t)
	defer cleanup()

	// Override the emit stub to capture the call.
	emit_called := 0
	orig := replication_emit_system_lww
	replication_emit_system_lww = func(database, table, row, field, value string, ts int64) {
		emit_called++
		if database != "apps" || table != "classes" || row != "feed" || field != "app" || value != "feeds" {
			t.Errorf("emit called with unexpected args: db=%q table=%q row=%q field=%q value=%q",
				database, table, row, field, value)
		}
	}
	defer func() { replication_emit_system_lww = orig }()

	apps_class_set("feed", "feeds")

	if emit_called != 1 {
		t.Errorf("emit_called = %d, want 1", emit_called)
	}

	db := db_apps()
	row, _ := db.row("select app, ts, peer from classes where class='feed'")
	if row == nil {
		t.Fatal("row should exist after set")
	}
	ts, _ := row["ts"].(int64)
	if ts == 0 {
		t.Error("ts should be non-zero after set")
	}
	if peer, _ := row["peer"].(string); peer != p2p_id {
		t.Errorf("peer = %q, want %q", peer, p2p_id)
	}
}
