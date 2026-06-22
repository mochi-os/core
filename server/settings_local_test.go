package main

import "testing"

// TestSettingLocalNotEmitted: a host-local setting (schema, server_started) must
// NOT be emitted for replication, while a normal setting is (#75).
func TestSettingLocalNotEmitted(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	sdb := db_open("db/settings.db")
	sdb.exec("create table if not exists settings (name text primary key, value text)")

	var emitted []string
	orig := replication_emit_system_set
	replication_emit_system_set = func(database, table, row, field, value string) {
		if database == "settings" && table == "settings" {
			emitted = append(emitted, row)
		}
	}
	defer func() { replication_emit_system_set = orig }()

	setting_set("schema", "89")
	setting_set("server_started", "1")
	setting_set("email_from", "x@y")

	for _, name := range emitted {
		if name == "schema" || name == "server_started" {
			t.Fatalf("host-local setting %q must not be replicated", name)
		}
	}
	if len(emitted) != 1 || emitted[0] != "email_from" {
		t.Fatalf("normal setting email_from should be the only emit, got %v", emitted)
	}
}

// TestSettingLocalApplyIgnored: an incoming system-set for a host-local setting
// must be ignored (even from an old peer still emitting it), while a normal one
// applies (#75).
func TestSettingLocalApplyIgnored(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	sdb := db_open("db/settings.db")
	sdb.exec("create table if not exists settings (name text primary key, value text)")
	sdb.exec("replace into settings (name, value) values ('schema', '88')")

	replication_system_set_apply_settings("peerX", &SystemSet{
		Database: "settings", Table: "settings", Row: "schema", Field: "value", Value: "99"})
	if v := setting_get("schema", ""); v != "88" {
		t.Fatalf("host-local schema must not be overwritten by a peer: got %q, want 88", v)
	}

	replication_system_set_apply_settings("peerX", &SystemSet{
		Database: "settings", Table: "settings", Row: "email_from", Field: "value", Value: "x@y"})
	if v := setting_get("email_from", ""); v != "x@y" {
		t.Fatalf("normal setting should apply: got %q", v)
	}
}
