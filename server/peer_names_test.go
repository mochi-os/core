// Mochi server: peer display-name unit tests
// Copyright Alistair Cunningham 2026
//
// Tests for the hostname/domain claims carried in peers/publish: claim
// validation, the merge/clear semantics, DNS verification against
// authenticated-connection evidence, the display selection rule (dotted
// claims never show unverified), and the approval-context withholding.

package main

import (
	"fmt"
	"net"
	"os"
	"testing"
)

// setup_peer_names_test gives a fresh data_dir with settings, domains
// and peers databases, empty peer registries, and stubbed resolver /
// evidence hooks. Returns a cleanup restoring everything.
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
	pdb.exec("create table if not exists names ( id text not null, name text not null, verified integer not null default 0, checked integer not null default 0, updated integer not null, primary key ( id, name ) )")

	peers_lock.Lock()
	saved_peers := peers
	peers = map[string]Peer{}
	peers_lock.Unlock()

	peer_names_lock.Lock()
	saved_names := peer_names
	peer_names = map[string][]PeerName{}
	peer_names_lock.Unlock()

	saved_resolve := peer_names_resolve
	saved_evidence := peer_names_evidence

	return func() {
		peer_names_resolve = saved_resolve
		peer_names_evidence = saved_evidence
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
// peer_publish_event, with name claims and no addresses.
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

	// The dotless hostname displays unverified; dotted claims are
	// stored but never selected while unverified.
	name, verified := peer_name(origin)
	if name != "wasabi" || verified {
		t.Errorf("peer_name = %q/%v, want wasabi/false", name, verified)
	}

	var rows []peer_name_row
	db_open("db/peers.db").scans(&rows, "select id, name, verified, checked, updated from names where id=? order by name", origin)
	if len(rows) != 3 {
		t.Fatalf("stored claims = %d, want 3", len(rows))
	}
}

func TestPeerPublishEventClearsNames(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	peer_publish_event(names_event(origin, "wasabi", ""))
	if name, _ := peer_name(origin); name != "wasabi" {
		t.Fatalf("claim not applied")
	}

	// A publish with no claims from a peer that previously claimed
	// means its operator wants anonymity.
	peer_publish_event(names_event(origin, "", ""))
	if name, _ := peer_name(origin); name != "" {
		t.Errorf("claims survived a claimless publish: %q", name)
	}
	if exists, _ := db_open("db/peers.db").exists("select 1 from names where id=?", origin); exists {
		t.Error("claims survived in peers.db")
	}
}

func TestPeerNamesCap(t *testing.T) {
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

	peer_names_lock.Lock()
	stored := len(peer_names[origin])
	peer_names_lock.Unlock()
	if stored != peer_names_maximum {
		t.Errorf("stored claims = %d, want cap %d", stored, peer_names_maximum)
	}
}

func TestPeerNamesVerify(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	peer_names_evidence = func(string) []string { return []string{"192.0.2.7"} }
	resolved := map[string][]net.IP{"mochi-os.org": {net.ParseIP("192.0.2.7")}, "evil.example": {net.ParseIP("203.0.113.9")}}
	var resolve_error error
	peer_names_resolve = func(host string) ([]net.IP, error) {
		if resolve_error != nil {
			return nil, resolve_error
		}
		return resolved[host], nil
	}

	peer_names_apply(origin, []string{"wasabi", "mochi-os.org", "evil.example"})
	peer_names_verify(origin)

	// Matching claim verifies and wins selection; mismatching stays out.
	name, verified := peer_name(origin)
	if name != "mochi-os.org" || !verified {
		t.Fatalf("peer_name = %q/%v, want mochi-os.org/true", name, verified)
	}

	// Verdicts persist to peers.db (survive restarts).
	if ok, _ := db_open("db/peers.db").exists("select 1 from names where id=? and name='mochi-os.org' and verified=1", origin); !ok {
		t.Error("verified verdict not persisted")
	}

	// A transient resolver failure keeps the verdict ...
	resolve_error = &net.DNSError{Err: "timeout", IsTimeout: true}
	peer_names_lock.Lock()
	for i := range peer_names[origin] {
		peer_names[origin][i].Checked = 0
	}
	peer_names_lock.Unlock()
	peer_names_verify(origin)
	if name, verified := peer_name(origin); name != "mochi-os.org" || !verified {
		t.Errorf("transient failure demoted: %q/%v", name, verified)
	}

	// ... but a definitive not-found demotes.
	resolve_error = &net.DNSError{Err: "no such host", IsNotFound: true}
	peer_names_lock.Lock()
	for i := range peer_names[origin] {
		peer_names[origin][i].Checked = 0
	}
	peer_names_lock.Unlock()
	peer_names_verify(origin)
	if name, verified := peer_name(origin); name != "wasabi" || verified {
		t.Errorf("not-found kept verification: %q/%v", name, verified)
	}
}

func TestPeerNamesNewClaimUnverified(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	peer_names_evidence = func(string) []string { return []string{"192.0.2.7"} }
	peer_names_resolve = func(host string) ([]net.IP, error) { return []net.IP{net.ParseIP("192.0.2.7")}, nil }

	peer_names_apply(origin, []string{"a.example.com"})
	peer_names_verify(origin)
	if _, verified := peer_name(origin); !verified {
		t.Fatal("claim should verify")
	}

	// A changed claim set: the kept claim retains its verdict, the new
	// claim starts unverified.
	peer_names_apply(origin, []string{"b.example.com", "a.example.com"})
	peer_names_lock.Lock()
	for _, c := range peer_names[origin] {
		switch c.Name {
		case "a.example.com":
			if !c.Verified {
				t.Error("kept claim lost its verdict")
			}
		case "b.example.com":
			if c.Verified {
				t.Error("new claim born verified")
			}
		}
	}
	peer_names_lock.Unlock()
}

func TestPeerNameApprovalContext(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	peer_names_apply(origin, []string{"wasabi"})

	// Unverified names are withheld where a human approves trust.
	m := map[string]any{}
	peer_name_fields(m, origin, true)
	if m["name"] != "" || m["verified"] != false {
		t.Errorf("approval context leaked unverified name: %v", m)
	}
	if fp, _ := m["fingerprint"].(string); len(fp) != 9 || fp != fingerprint(origin) {
		t.Errorf("fingerprint = %v, want %s", m["fingerprint"], fingerprint(origin))
	}

	// Informational contexts show it muted (verified=false).
	m = map[string]any{}
	peer_name_fields(m, origin, false)
	if m["name"] != "wasabi" || m["verified"] != false {
		t.Errorf("informational context = %v", m)
	}
}

func TestPeerNamesAnnounce(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	setting_set("hostname", "test-box")
	db := db_open("db/domains.db")
	db.exec("insert into domains (domain, created, updated) values ('example.com', 1, 1), ('*.wild.example', 1, 1)")

	name, domains := peer_names_announce()
	if name != "test-box" {
		t.Errorf("announced name = %q, want test-box", name)
	}
	if domains != "example.com,wild.example" && domains != "wild.example,example.com" {
		t.Errorf("announced domains = %q", domains)
	}

	// The administrator opt-out silences the announcement entirely.
	setting_set("hostname_publish", "false")
	if name, domains := peer_names_announce(); name != "" || domains != "" {
		t.Errorf("opt-out still announces %q/%q", name, domains)
	}
}

func TestPeerNamesLoad(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	origin, _ := test_host(t)
	db := db_open("db/peers.db")
	db.exec("insert into names (id, name, verified, checked, updated) values (?, 'mochi-os.org', 1, 5, 5), (?, 'wasabi', 0, 0, 5)", origin, origin)

	peer_names_load()
	if name, verified := peer_name(origin); name != "mochi-os.org" || !verified {
		t.Errorf("loaded peer_name = %q/%v, want mochi-os.org/true", name, verified)
	}
}

func TestDbUpgrade82(t *testing.T) {
	cleanup := setup_peer_names_test(t)
	defer cleanup()

	db := db_open("db/peers.db")
	db.exec("drop table names")
	db_upgrade_82()
	db_upgrade_82() // idempotent
	db.exec("insert into names (id, name, updated) values ('x', 'y', 1)")
	if ok, _ := db.exists("select 1 from names where id='x'"); !ok {
		t.Error("names table not usable after upgrade")
	}
}
