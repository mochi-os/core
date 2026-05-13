// Mochi server: system-set / system-row replication unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// setup_system_replication_test prepares data_dir + settings.db with
// the schema replication_emit/_apply expects. Returns a cleanup.
func setup_system_replication_test(t *testing.T) func() {
	cleanup := setup_replication_test(t)
	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings (name text primary key, value text not null)")
	return cleanup
}

// TestSystemSetApplySettings: a settings.settings op with a non-empty
// value replaces / inserts the row.
func TestSystemSetApplySettings(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "settings", Table: "settings",
		Row: "signup_enabled", Field: "value", Value: "true",
	})
	if got := setting_get("signup_enabled", ""); got != "true" {
		t.Errorf("setting_get = %q, want %q", got, "true")
	}
}

// TestSystemSetApplySettingsDeleteOnEmpty: an empty value removes the row.
func TestSystemSetApplySettingsDeleteOnEmpty(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	db := db_open("db/settings.db")
	db.exec("replace into settings (name, value) values ('k', 'v')")

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "settings", Table: "settings",
		Row: "k", Field: "value", Value: "",
	})
	if exists, _ := db.exists("select 1 from settings where name='k'"); exists {
		t.Error("empty-value op should delete the row")
	}
}

// TestSystemSetApplyApps verifies apps.classes / services / paths
// dispatch and write correctly.
func TestSystemSetApplyApps(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	db_apps()

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "apps", Table: "classes",
		Row: "feed", Field: "app", Value: "feeds",
	})
	if got := apps_class_get("feed"); got != "feeds" {
		t.Errorf("classes apply: get = %q, want feeds", got)
	}

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "apps", Table: "services",
		Row: "feeds", Field: "app", Value: "feeds",
	})
	if got := apps_service_get("feeds"); got != "feeds" {
		t.Errorf("services apply: get = %q, want feeds", got)
	}

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "apps", Table: "paths",
		Row: "/feeds/", Field: "app", Value: "feeds",
	})
	if got := apps_path_get("/feeds/"); got != "feeds" {
		t.Errorf("paths apply: get = %q, want feeds", got)
	}
}

// TestSystemSetApplyAppsInstall: apps.apps install registry write.
func TestSystemSetApplyAppsInstall(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	db_apps()

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "apps", Table: "apps",
		Row: "feeds", Field: "installed", Value: "1234567890",
	})
	if got := apps_installed("feeds"); got != 1234567890 {
		t.Errorf("apps_installed = %d, want 1234567890", got)
	}
}

// TestSystemSetApplyRejectsUnknownDestination: dispatch warn-drops
// unknown destinations without affecting other tables.
func TestSystemSetApplyRejectsUnknownDestination(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "nope", Table: "nope",
		Row: "k", Field: "value", Value: "v",
	})
	db := db_open("db/settings.db")
	if exists, _ := db.exists("select 1 from settings where name='k'"); exists {
		t.Error("unknown destination should not touch settings")
	}
}

// TestSystemSetApplyRejectsMissingFields validates required-field
// gating: any missing key field silently drops the op.
func TestSystemSetApplyRejectsMissingFields(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	cases := []SystemSet{
		{Database: "", Table: "settings", Row: "k", Field: "value", Value: "v"},
		{Database: "settings", Table: "", Row: "k", Field: "value", Value: "v"},
		{Database: "settings", Table: "settings", Row: "", Field: "value", Value: "v"},
		{Database: "settings", Table: "settings", Row: "k", Field: "", Value: "v"},
	}
	for _, c := range cases {
		replication_system_set_apply("peer-A", &c)
	}
	db := db_open("db/settings.db")
	if exists, _ := db.exists("select 1 from settings where name='k'"); exists {
		t.Error("missing-field op should not write")
	}
}

// TestSettingSetEmits: setting_set fires the system-set emit with the
// expected arguments.
func TestSettingSetEmits(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	calls := 0
	orig := replication_emit_system_set
	replication_emit_system_set = func(database, table, row, field, value string) {
		calls++
		if database != "settings" || table != "settings" || row != "k" || field != "value" || value != "v" {
			t.Errorf("emit args: db=%q table=%q row=%q field=%q value=%q",
				database, table, row, field, value)
		}
	}
	defer func() { replication_emit_system_set = orig }()

	setting_set("k", "v")

	if calls != 1 {
		t.Errorf("emit calls = %d, want 1", calls)
	}
}

// TestAppsClassSetEmits: apps_class_set fires system-set.
func TestAppsClassSetEmits(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	calls := 0
	orig := replication_emit_system_set
	replication_emit_system_set = func(database, table, row, field, value string) {
		calls++
	}
	defer func() { replication_emit_system_set = orig }()

	apps_class_set("feed", "feeds")
	if calls != 1 {
		t.Errorf("emit calls = %d, want 1", calls)
	}
}

// setup_domains_test_schema creates a minimal domains.db schema for
// row-level tests.
func setup_domains_test_schema() {
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
}

// TestSystemRowApplyDomainsFresh: a row-level op for a new domain
// inserts cleanly.
func TestSystemRowApplyDomainsFresh(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "domains",
		Key: map[string]string{"domain": "example.com"},
		Cols: map[string]string{
			"verified": "0", "token": "tok123", "tls": "1",
			"created": "100", "updated": "100",
		},
	})
	db := db_open("db/domains.db")
	row, _ := db.row("select token from domains where domain='example.com'")
	if row == nil {
		t.Fatal("row should exist after apply")
	}
	if got, _ := row["token"].(string); got != "tok123" {
		t.Errorf("token = %q, want tok123", got)
	}
}

// TestSystemRowApplyDomainsReplacesExisting: a subsequent op
// overwrites the existing row (last-applier-wins).
func TestSystemRowApplyDomainsReplacesExisting(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()
	db := db_open("db/domains.db")
	db.exec("replace into domains (domain, verified, token, tls, created, updated) values ('example.com', 0, 'old', 1, 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "domains",
		Key: map[string]string{"domain": "example.com"},
		Cols: map[string]string{
			"verified": "1", "token": "new", "tls": "1",
			"created": "100", "updated": "200",
		},
	})
	row, _ := db.row("select token from domains where domain='example.com'")
	if got, _ := row["token"].(string); got != "new" {
		t.Errorf("token = %q, want new", got)
	}
}

// TestSystemRowApplyDomainsDelete: Delete=true removes the row.
func TestSystemRowApplyDomainsDelete(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()
	db := db_open("db/domains.db")
	db.exec("replace into domains (domain, verified, token, tls, created, updated) values ('example.com', 0, 't', 1, 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "domains",
		Key:    map[string]string{"domain": "example.com"},
		Delete: true,
	})
	if exists, _ := db.exists("select 1 from domains where domain='example.com'"); exists {
		t.Error("domain should be deleted after delete-op")
	}
}

// TestSystemRowApplyRoutes: composite-key route apply.
func TestSystemRowApplyRoutes(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path))")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "routes",
		Key: map[string]string{"domain": "example.com", "path": "/feeds"},
		Cols: map[string]string{
			"method": "app", "target": "feeds", "context": "",
			"owner": "u1", "priority": "10", "enabled": "1",
			"created": "100", "updated": "100",
		},
	})
	row, _ := domains.row("select target from routes where domain='example.com' and path='/feeds'")
	if got, _ := row["target"].(string); got != "feeds" {
		t.Errorf("target = %q, want feeds", got)
	}
}

// TestSystemRowApplyRoutesDelete: composite-key delete.
func TestSystemRowApplyRoutesDelete(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path))")
	domains.exec("replace into routes (domain, path, method, target, context, owner, priority, enabled, created, updated) values ('example.com', '/x', 'app', 'wikis', '', '', 0, 1, 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "routes",
		Key:    map[string]string{"domain": "example.com", "path": "/x"},
		Delete: true,
	})
	if exists, _ := domains.exists("select 1 from routes where domain='example.com' and path='/x'"); exists {
		t.Error("route should be deleted")
	}
}

// TestSystemRowApplyAppsVersions: apps.versions row apply (single
// key, two data columns).
func TestSystemRowApplyAppsVersions(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	db_apps()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "apps", Table: "versions",
		Key:  map[string]string{"app": "feeds"},
		Cols: map[string]string{"version": "1.2.3", "track": "stable"},
	})
	db := db_apps()
	row, _ := db.row("select version, track from versions where app='feeds'")
	if row == nil {
		t.Fatal("versions row should exist after apply")
	}
	if got, _ := row["version"].(string); got != "1.2.3" {
		t.Errorf("version = %q, want 1.2.3", got)
	}
	if got, _ := row["track"].(string); got != "stable" {
		t.Errorf("track = %q, want stable", got)
	}
}

// TestSystemRowApplyAppsTracks: apps.tracks composite-key apply.
func TestSystemRowApplyAppsTracks(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	db_apps()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "apps", Table: "tracks",
		Key:  map[string]string{"app": "feeds", "track": "beta"},
		Cols: map[string]string{"version": "2.0.0-rc1"},
	})
	db := db_apps()
	row, _ := db.row("select version from tracks where app='feeds' and track='beta'")
	if got, _ := row["version"].(string); got != "2.0.0-rc1" {
		t.Errorf("version = %q, want 2.0.0-rc1", got)
	}
}

// TestSystemRowApplyDelegations: domains.delegations composite-key
// apply with timestamps.
func TestSystemRowApplyDelegations(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner text not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("insert into domains (domain, verified, token, tls, created, updated) values ('example.com', 1, 't', 1, 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "delegations",
		Key: map[string]string{"domain": "example.com", "path": "/feeds", "owner": "u1"},
		Cols: map[string]string{
			"created": "100", "updated": "100",
		},
	})
	row, _ := domains.row("select created, updated from delegations where domain='example.com' and path='/feeds' and owner='u1'")
	if row == nil {
		t.Fatal("delegation should exist after apply")
	}
	if got, _ := row["created"].(int64); got != 100 {
		t.Errorf("created = %d, want 100", got)
	}
}

// TestSystemRowApplyDelegationsDelete: composite-key delete.
func TestSystemRowApplyDelegationsDelete(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner text not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("insert into domains (domain, verified, token, tls, created, updated) values ('example.com', 1, 't', 1, 100, 100)")
	domains.exec("insert into delegations (domain, path, owner, created, updated) values ('example.com', '/x', 'u1', 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "delegations",
		Key:    map[string]string{"domain": "example.com", "path": "/x", "owner": "u1"},
		Delete: true,
	})
	if exists, _ := domains.exists("select 1 from delegations where domain='example.com'"); exists {
		t.Error("delegation should be deleted after delete-op")
	}
}

// TestSystemRowApplyRejectsMissingKey: empty key map drops silently.
func TestSystemRowApplyRejectsMissingKey(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "domains",
		Key:  map[string]string{},
		Cols: map[string]string{"verified": "1"},
	})
	// No write should happen.
	db := db_open("db/domains.db")
	rows, _ := db.rows("select 1 from domains")
	if len(rows) != 0 {
		t.Errorf("empty-key op should not write; got %d rows", len(rows))
	}
}
