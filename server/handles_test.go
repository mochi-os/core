// Mochi server: Handle registry tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
)

// create_handles_test_env builds users.db with users and handles, the domains
// and settings databases, a configured server domain, and two users: an
// administrator and an ordinary user. Restores the globals it changes.
func create_handles_test_env(t *testing.T) (*User, *User) {
	t.Helper()
	create_domains_test_env(t)
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', disabled text not null default '', status text not null default 'active')")
	// create_domains_test_env has made the handles table already.
	// user_by_uid reads the user's entities as well.
	users.exec("create table entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("insert into users (uid, username, role) values ('u-alice', 'alice@example.com', 'user')")
	users.exec("insert into users (uid, username, role) values ('u-root', 'root@example.com', 'administrator')")
	users.exec("insert into entities (id, fingerprint, user, class, name) values ('e-alice', 'fp-alice1', 'u-alice', 'person', 'Alice')")
	users.exec("insert into entities (id, fingerprint, user, class, name) values ('e-root', 'fp-root12', 'u-root', 'person', 'Root')")

	saved_domain, saved_development := server_domain_name, networks_development
	server_domain_name = "Example.com"
	networks_development = true
	t.Cleanup(func() {
		server_domain_name, networks_development = saved_domain, saved_development
	})
	alice, root := user_by_uid("u-alice"), user_by_uid("u-root")
	if alice == nil || root == nil {
		t.Fatal("test users did not load")
	}
	return alice, root
}

// TestHandleNames: the shape rules and normalisation.
func TestHandleNames(t *testing.T) {
	for name, want := range map[string]bool{
		"alice": true, "al1ce": true, "abc": true, "alice_news": true, "a2345678901234567890": true,
		"ab": false, "1alice": false, "Alice": false, "alice-news": false, "alice__x": false,
		"alice_news_more": false, "a23456789012345678901": false, "al ice": false, "": false,
	} {
		if got := handle_name_valid(name); got != want {
			t.Errorf("handle_name_valid(%q) = %v, want %v", name, got, want)
		}
	}
	if handle_normalise(" Alice-News ") != "alice_news" {
		t.Errorf("handle_normalise should lowercase and write a hyphen as an underscore")
	}
	if handle_bluesky("alice_news") != "alice-news" {
		t.Errorf("handle_bluesky should write the separator as a hyphen")
	}
	if handle_parent("alice_news") != "alice" || handle_parent("alice") != "" {
		t.Errorf("handle_parent wrong")
	}
	if network_address("fediverse", "alice_news", "example.com") != "alice_news@example.com" ||
		network_address("matrix", "alice", "example.com") != "@alice:example.com" ||
		network_address("bluesky", "alice_news", "example.com") != "alice-news.example.com" ||
		network_address("xmpp", "alice", "example.com") != "alice@example.com" {
		t.Errorf("network_address wrong")
	}
}

// TestHandleReserve: a user reserves a bare name, a name under it, is refused
// another bare name at the default limit of one, and is refused blocked,
// protected and taken names; an administrator is bound by none of those but
// the blocked names.
func TestHandleReserve(t *testing.T) {
	alice, root := create_handles_test_env(t)

	h, err := handle_reserve(alice, "", "Alice", "handle")
	if err != nil || h.Domain != "example.com" || h.Name != "alice" || h.Parent != "" {
		t.Fatalf("reserve alice: %+v, %v", h, err)
	}
	if _, _, reason := handle_available(alice, "", "alice", "handle"); reason != "taken" {
		t.Errorf("a reserved name should be taken, got %q", reason)
	}
	if _, _, reason := handle_available(root, "", "alice", "handle"); reason != "taken" {
		t.Errorf("a reserved name is taken for an administrator too, got %q", reason)
	}
	if _, err := handle_reserve(alice, "", "alice-news", "handle"); err != nil {
		t.Errorf("a name under the user's own handle should be free: %v", err)
	}
	if _, _, reason := handle_available(root, "", "alice_other", "handle"); reason != "" {
		t.Errorf("an administrator may add a name under any handle, got %q", reason)
	}
	if _, _, reason := handle_available(alice, "", "bob_news", "handle"); reason != "parent" {
		t.Errorf("a name under a handle nobody holds should be refused as parent, got %q", reason)
	}
	if _, _, reason := handle_available(alice, "", "second", "handle"); reason != "limit" {
		t.Errorf("a second bare name at a limit of one should be refused as limit, got %q", reason)
	}
	if n := handle_counted(alice); n != 1 {
		t.Errorf("counted = %d, want 1 (names under a handle are free)", n)
	}
	if _, _, reason := handle_available(root, "", "second", "handle"); reason != "" {
		t.Errorf("an administrator has no limit, got %q", reason)
	}
	if _, _, reason := handle_available(alice, "", "www", "handle"); reason != "blocked" {
		t.Errorf("www should be blocked, got %q", reason)
	}
	if _, _, reason := handle_available(root, "", "xrpc", "handle"); reason != "blocked" {
		t.Errorf("an engine segment is blocked for everyone, got %q", reason)
	}
	if _, _, reason := handle_available(alice, "", "admin", "handle"); reason != "protected" {
		t.Errorf("admin should be protected from a user, got %q", reason)
	}
	if _, _, reason := handle_available(root, "", "admin", "handle"); reason != "" {
		t.Errorf("an administrator may take a protected name, got %q", reason)
	}
	if _, _, reason := handle_available(alice, "", "Bad Name", "handle"); reason != "invalid" {
		t.Errorf("a malformed name should be invalid, got %q", reason)
	}
	if _, _, reason := handle_available(alice, "other.example", "fresh", "handle"); reason != "domain" {
		t.Errorf("a domain the user may not reserve on should be refused as domain, got %q", reason)
	}
	if _, _, reason := handle_available(alice, "", "fresh", "feed"); reason != "invalid" {
		t.Errorf("an unknown kind should be invalid, got %q", reason)
	}
}

// TestHandleLimitSetting: the owner's setting is the limit, zero leaves names
// to administrators, and a community name counts like a handle.
func TestHandleLimitSetting(t *testing.T) {
	alice, _ := create_handles_test_env(t)
	setting_set("handles", "2")
	if _, err := handle_reserve(alice, "", "alice", "handle"); err != nil {
		t.Fatal(err)
	}
	if _, err := handle_reserve(alice, "", "gardening", "community"); err != nil {
		t.Fatalf("a community name is the second of two: %v", err)
	}
	if _, _, reason := handle_available(alice, "", "third", "handle"); reason != "limit" {
		t.Errorf("a community name counts like a handle, got %q", reason)
	}
	setting_set("handles", "0")
	if _, _, reason := handle_available(alice, "", "third", "handle"); reason != "limit" {
		t.Errorf("zero leaves bare names to administrators, got %q", reason)
	}
}

// TestHandleOwnDomain: a user's own domain is their namespace: protected names
// are theirs, and handles there do not count against the limit.
func TestHandleOwnDomain(t *testing.T) {
	alice, _ := create_handles_test_env(t)
	route_normalise_domain(t, "alice.example")
	domains := db_open("db/domains.db")
	domains.exec("insert into routes (domain, path, method, target, owner, created, updated) values ('alice.example', '', 'app', 'feeds', 'u-alice', 1, 1)")

	if list := handle_domains(alice); len(list) != 2 || list[0] != "example.com" || list[1] != "alice.example" {
		t.Fatalf("handle_domains = %v, want the server's domain then the user's own", list)
	}
	if _, err := handle_reserve(alice, "", "alice", "handle"); err != nil {
		t.Fatal(err)
	}
	if _, err := handle_reserve(alice, "alice.example", "admin", "handle"); err != nil {
		t.Errorf("a protected name on the user's own domain is theirs: %v", err)
	}
	if _, err := handle_reserve(alice, "alice.example", "shop", "handle"); err != nil {
		t.Errorf("a handle on the user's own domain is free of the limit: %v", err)
	}
	if n := handle_counted(alice); n != 1 {
		t.Errorf("counted = %d, want 1: own-domain names do not count", n)
	}
	if h := handle_lookup("ALICE.example", "Admin"); h == nil || h.User != "u-alice" {
		t.Errorf("handle_lookup should normalise case: %+v", h)
	}
}

// TestHandleRelease: releasing a handle deletes the names under it and frees
// the name; only the holder or an administrator may; deletion and demotion
// free what they should.
func TestHandleRelease(t *testing.T) {
	alice, root := create_handles_test_env(t)
	if _, err := handle_reserve(alice, "", "alice", "handle"); err != nil {
		t.Fatal(err)
	}
	if _, err := handle_reserve(alice, "", "alice_news", "handle"); err != nil {
		t.Fatal(err)
	}
	if _, err := handle_reserve(root, "", "admin", "handle"); err != nil {
		t.Fatal(err)
	}
	if _, err := handle_reserve(root, "", "rootname", "handle"); err != nil {
		t.Fatal(err)
	}
	bob := &User{UID: "u-bob", Username: "bob@example.com", Role: "user"}
	if err := handle_release(bob, "", "alice"); err == nil {
		t.Error("another user must not release a handle")
	}
	if err := handle_release(alice, "", "alice"); err != nil {
		t.Fatalf("the holder releases: %v", err)
	}
	if handle_get("example.com", "alice") != nil || handle_get("example.com", "alice_news") != nil {
		t.Error("release should delete the handle and the names under it")
	}
	if _, _, reason := handle_available(root, "", "alice", "handle"); reason != "" {
		t.Errorf("a released name is free for anyone, got %q", reason)
	}
	if err := handle_release(root, "", "rootname"); err != nil {
		t.Fatalf("an administrator releases their own: %v", err)
	}
	if _, err := handle_reserve(root, "", "rootname", "handle"); err != nil {
		t.Fatal(err)
	}
	handles_demoted("u-root")
	if handle_get("example.com", "admin") != nil {
		t.Error("demotion should free a protected role handle")
	}
	if handle_get("example.com", "rootname") == nil {
		t.Error("demotion should keep an ordinary handle")
	}
	handles_purge("u-root")
	if len(handles_user("u-root")) != 0 {
		t.Error("deleting the user should free every handle")
	}
}

// TestHandleHostAnswers: a certificate under a wildcard domain is answered for
// a handle's host only once its holder has a Bluesky account on it.
func TestHandleHostAnswers(t *testing.T) {
	alice, _ := create_handles_test_env(t)
	if _, err := handle_reserve(alice, "", "alice", "handle"); err != nil {
		t.Fatal(err)
	}
	if handle_host_answers("alice.example.com") {
		t.Error("a handle with no Bluesky account answers for no host")
	}
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('a1', 'bluesky', '', 'alice.example.com', '{\"hosted\":true,\"handle\":\"alice\",\"domain\":\"example.com\"}', 1, 1)")
	if !handle_host_answers("alice.example.com") {
		t.Error("a handle whose holder has a Bluesky account answers for its host")
	}
	if handle_host_answers("bob.example.com") || handle_host_answers("example.com") {
		t.Error("other hosts are not answered")
	}
	if got := account_hosted(alice, "bluesky", "example.com", "alice"); got == nil || got["id"] != "a1" {
		t.Errorf("account_hosted = %v", got)
	}
	accounts_hosted_remove(alice, "example.com", "alice")
	if account_hosted(alice, "bluesky", "example.com", "alice") != nil {
		t.Error("accounts_hosted_remove should delete the hosted account")
	}
}
