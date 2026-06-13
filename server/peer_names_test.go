// Mochi server: peer display-name unit tests
// Copyright Alistair Cunningham 2026
//
// Tests for the hostname a peer announces in peers/publish: name
// validation, the store/clear semantics, that a served-domain list is
// ignored, the announce source, load, and the schema-86 migration.

package main

import (
	"fmt"
	"os"
	"testing"
)

// setup_peer_names_test gives a fresh data_dir with settings, domains and
// peers databases and empty peer registries. Returns a cleanup restoring
// everything.
func setup_peer_names_test(t *testing.T) func() {
	t.Helper()

	tmp, err := os.MkdirTemp("", "mochi_peer_names_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	orig_data_dir := data_dir
	data_dir = tmp

	db_open("db/settings.db").exec("create table if not exists settings (name text primary key, value text not null)")
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	pdb := db_open("db/peers.db")
	pdb.exec("create table if not exists peers ( id text not null, address text not null, updated integer not null, success integer not null default 0, failure integer not null default 0, primary key ( id, address ) )")
	pdb.exec("create table if not exists records ( id text not null primary key, record blob not null, sequence integer not null, updated integer not null )")
	pdb.exec("create table if not exists names ( id text not null, name text not null, updated integer not null, primary key ( id, name ) )")

	peers_lock.Lock()
	saved_peers := peers
	peers = map[string]Peer{}
	peers_lock.Unlock()

	peer_names_lock.Lock()
	saved_names := peer_names
	peer_names = map[string][]PeerName{}
	peer_names_lock.Unlock()

	return func() {
		peer_names_lock.Lock()
		peer_names = saved_names
		peer_names_lock.Unlock()
		peers_lock.Lock()
		peers = saved_peers
		peers_lock.Unlock()
		data_dir = orig_data_dir
		os.RemoveAll(tmp)
	}
}

// names_event builds the Event shape pubsub_receive hands to
// peer_publish_event, with a name (and an optional, ignored domains field)
// and no addresses.
func names_event(origin, name, domains string) *Event {
	content := map[string]any{}
	if name != "" {
		content["name"] = name
	}
	if domains != "" {
		content["domains"] = domains
	}
	return &Event{origin: origin, content: content}
}

func TestPeerNameValid(t *testing.T) {
	good := []string{"wasabi", "mochi-os.org", "a", "host-1.example.com", "9front.org"}
	for _, n := range good {
		if !peer_name_valid(n) {
			t.Errorf("peer_name_valid(%q) = false, want true", n)
		}
	}
	bad := []string{"", "Wasabi", "under_score", "tröjan.example", "-lead.example", "trail-.example", "dot..dot", ".lead", "trail.", "white space",
		fmt.Sprintf("%0254d", 1), "label-over-63-chars-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.example"}
	for _, n := range bad {
		if peer_name_valid(n) {
			t.Errorf("peer_name_valid(%q) = true, want false", n)
		}
	}
}

func TestPeerPublishEventAppliesNames(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	peer_publish_event(names_event(origin, "Wasabi", "mochi-os.org, Example.COM"))

	// Only the announced hostname is applied; a peer's served-domain list
	// is ignored.
	if name := peer_name(origin); name != "wasabi" {
		t.Errorf("peer_name = %q, want wasabi", name)
	}

	var rows []peer_name_row
	db_open("db/peers.db").scans(&rows, "select id, name, updated from names where id=? order by name", origin)
	if len(rows) != 1 {
		t.Fatalf("stored names = %d, want 1 (served domains must be ignored)", len(rows))
	}
	if rows[0].Name != "wasabi" {
		t.Errorf("stored name = %q, want wasabi", rows[0].Name)
	}
}

func TestPeerPublishEventClearsNames(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	peer_publish_event(names_event(origin, "wasabi", ""))
	if name := peer_name(origin); name != "wasabi" {
		t.Fatalf("name not applied")
	}

	// A publish with no name from a peer that previously announced one
	// means its operator wants anonymity.
	peer_publish_event(names_event(origin, "", ""))
	if name := peer_name(origin); name != "" {
		t.Errorf("name survived a nameless publish: %q", name)
	}
	if exists, _ := db_open("db/peers.db").exists("select 1 from names where id=?", origin); exists {
		t.Error("name survived in peers.db")
	}
}

func TestPeerPublishEventIgnoresDomains(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	domains := ""
	for i := 0; i < 8; i++ {
		if i > 0 {
			domains += ","
		}
		domains += fmt.Sprintf("d%d.example.com", i)
	}
	peer_publish_event(names_event(origin, "wasabi", domains))

	// However many domains a peer lists, none are stored — only the name.
	peer_names_lock.Lock()
	stored := len(peer_names[origin])
	peer_names_lock.Unlock()
	if stored != 1 {
		t.Errorf("stored names = %d, want 1 (served domains ignored)", stored)
	}
	if name := peer_name(origin); name != "wasabi" {
		t.Errorf("peer_name = %q, want wasabi", name)
	}
}

func TestPeerNameFields(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	peer_names_apply(origin, []string{"wasabi"})

	// The announced name shows as a plain label; the fingerprint is the
	// authoritative identifier. No verified field exists any more.
	m := map[string]any{}
	peer_name_fields(m, origin)
	if m["name"] != "wasabi" {
		t.Errorf("name = %v, want wasabi", m["name"])
	}
	if fp, _ := m["fingerprint"].(string); len(fp) != 9 || fp != fingerprint(origin) {
		t.Errorf("fingerprint = %v, want %s", m["fingerprint"], fingerprint(origin))
	}
	if _, ok := m["verified"]; ok {
		t.Error("verified field should no longer be set")
	}
}

func TestPeerNamesAnnounce(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	setting_set("hostname", "test-box")
	// Served domains are present but must never reach the announcement.
	db := db_open("db/domains.db")
	db.exec("insert into domains (domain, created, updated) values ('example.com', 1, 1), ('*.wild.example', 1, 1)")

	if name := peer_names_announce(); name != "test-box" {
		t.Errorf("announced name = %q, want test-box", name)
	}

	// The administrator opt-out silences the announcement entirely.
	setting_set("hostname_publish", "false")
	if name := peer_names_announce(); name != "" {
		t.Errorf("opt-out still announces %q", name)
	}
}

func TestPeerNamesLoad(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	db := db_open("db/peers.db")
	db.exec("insert into names (id, name, updated) values (?, 'mochi-os.org', 5)", origin)

	peer_names_load()
	if name := peer_name(origin); name != "mochi-os.org" {
		t.Errorf("loaded peer_name = %q, want mochi-os.org", name)
	}
}

func TestDbUpgrade86(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	db := db_open("db/peers.db")
	// Recreate the pre-86 shape (verified/checked columns) with a cached row.
	db.exec("drop table names")
	db.exec("create table names ( id text not null, name text not null, verified integer not null default 0, checked integer not null default 0, updated integer not null, primary key ( id, name ) )")
	db.exec("insert into names (id, name, verified, checked, updated) values ('x', 'y', 1, 5, 5)")

	db_upgrade_86()
	db_upgrade_86() // idempotent

	// New shape: the table takes the three-column insert and is usable.
	db.exec("insert into names (id, name, updated) values ('a', 'b', 1)")
	if ok, _ := db.exists("select 1 from names where id='a'"); !ok {
		t.Error("names table not usable after upgrade")
	}
}
