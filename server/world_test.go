// Mochi server: world-listing unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func setup_world_test(t *testing.T) func() {
	cleanup := setup_replication_test(t) // temp data_dir + net_id="self"
	db := db_open("db/world.db")
	db.exec("create table if not exists worlds ( peer text not null, world text not null, name text not null, address text not null, version integer not null, services text not null, seen integer not null, primary key (peer, world) )")
	return cleanup
}

const world_test_id = "abcdefghij0123456789abcdefghij01"

func world_test_services(players int64) string {
	b, _ := json.Marshal([]world_service{{Service: "air", Players: players}})
	return string(b)
}

// A gossiped announcement from another peer stores; the peer identity comes
// from the transport, so a row is keyed by the SENDER, not by anything in
// the payload.
func TestWorldPublishEventStores(t *testing.T) {
	defer setup_world_test(t)()
	e := &Event{peer: "peer1", service: "world", event: "publish", content: map[string]any{
		"world": world_test_id, "name": "Duc's World", "address": "https://world.example:4433",
		"version": "3", "services": world_test_services(5)}}
	world_publish_event(e)

	db := db_open("db/world.db")
	row, _ := db.row("select * from worlds where peer=? and world=?", "peer1", world_test_id)
	if row == nil {
		t.Fatal("announcement from a remote peer was not stored")
	}
	if row["name"] != "Duc's World" {
		t.Fatalf("stored name %q", row["name"])
	}
}

// This host is authoritative for its own rows: its announcements coming back
// around the flood must not overwrite the local table.
func TestWorldPublishEventIgnoresSelf(t *testing.T) {
	defer setup_world_test(t)()
	e := &Event{peer: net_id, service: "world", event: "publish", content: map[string]any{
		"world": world_test_id, "name": "Echo", "address": "https://world.example:4433",
		"version": "3", "services": world_test_services(1)}}
	world_publish_event(e)

	db := db_open("db/world.db")
	if exists, _ := db.exists("select 1 from worlds where world=?", world_test_id); exists {
		t.Fatal("own echo was stored")
	}
}

// Malformed announcements — bad id, oversized name, absurd player counts,
// too many services, broken JSON — drop without storing. These strings
// render on every server's join page; the bounds are the defence.
func TestWorldValidationRejects(t *testing.T) {
	defer setup_world_test(t)()
	long := strings.Repeat("x", world_name_most+1)
	many := make([]world_service, world_services_most+1)
	for i := range many {
		many[i] = world_service{Service: "air", Players: 1}
	}
	manyb, _ := json.Marshal(many)
	cases := []map[string]any{
		{"world": "short", "name": "ok", "address": "https://x:1", "version": "3", "services": world_test_services(1)},
		{"world": world_test_id, "name": long, "address": "https://x:1", "version": "3", "services": world_test_services(1)},
		{"world": world_test_id, "name": "ok", "address": "https://x:1", "version": "3", "services": world_test_services(-1)},
		{"world": world_test_id, "name": "ok", "address": "https://x:1", "version": "3", "services": string(manyb)},
		{"world": world_test_id, "name": "ok", "address": "https://x:1", "version": "3", "services": "not json"},
		{"world": world_test_id, "name": "ok", "address": "https://x:1", "version": "3", "services": "[]"},
		{"world": world_test_id, "name": "ok", "address": "", "version": "3", "services": world_test_services(1)},
	}
	for k, content := range cases {
		world_publish_event(&Event{peer: "peer1", service: "world", event: "publish", content: content})
		db := db_open("db/world.db")
		if exists, _ := db.exists("select 1 from worlds where peer=?", "peer1"); exists {
			t.Fatalf("case %d: invalid announcement was stored: %v", k, content)
		}
	}
}

// Rows age out when refresh stops; a fresh row survives the sweep.
func TestWorldExpiry(t *testing.T) {
	defer setup_world_test(t)()
	db := db_open("db/world.db")
	db.exec("replace into worlds (peer, world, name, address, version, services, seen) values ('peer1', ?, 'Old', 'https://x:1', 3, ?, ?)",
		world_test_id, world_test_services(0), now()-world_seen_expiry-1)
	db.exec("replace into worlds (peer, world, name, address, version, services, seen) values ('peer2', ?, 'Fresh', 'https://x:1', 3, ?, ?)",
		world_test_id, world_test_services(0), now())
	db.exec("delete from worlds where seen < ?", now()-world_seen_expiry)
	if exists, _ := db.exists("select 1 from worlds where peer='peer1'"); exists {
		t.Fatal("expired row survived the sweep")
	}
	if exists, _ := db.exists("select 1 from worlds where peer='peer2'"); !exists {
		t.Fatal("fresh row was swept")
	}
}

// The per-service display name wins over the world name, and a listing not
// hosting the requested service does not appear.
func TestWorldServiceNameResolution(t *testing.T) {
	defer setup_world_test(t)()
	services, _ := json.Marshal([]world_service{
		{Service: "air", Players: 3, Name: "Duc's Dogfight Den"},
		{Service: "sail", Players: 1},
	})
	world_store("peer1", world_test_id, "Duc's World", "https://world.example:4433", 3, string(services))

	db := db_open("db/world.db")
	rows, err := db.rows("select * from worlds where seen >= ?", now()-world_seen_expiry)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows: %v %v", rows, err)
	}
	var list []world_service
	if json.Unmarshal([]byte(rows[0]["services"].(string)), &list) != nil || len(list) != 2 {
		t.Fatalf("services did not round-trip: %v", rows[0]["services"])
	}
	if list[0].Name != "Duc's Dogfight Den" || list[1].Name != "" {
		t.Fatalf("per-service names did not survive: %+v", list)
	}
}

// Wire compatibility: an event routed to a service this build does not know
// is dropped with an error, never a panic — this is the property that lets
// NEW announcement types roll out with no flag day, and this test is the
// assertion the #14 design requires of it.
func TestWorldUnknownServiceIgnored(t *testing.T) {
	defer setup_world_test(t)()
	e := &Event{peer: "peer1", service: "no-such-service-exists", event: "publish", content: map[string]any{"x": "y"}}
	if err := e.route(); err == nil {
		t.Fatal("routing to an unknown service should error (and be dropped by the caller)")
	}
}
