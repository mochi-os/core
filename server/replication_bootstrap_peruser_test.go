// Mochi server: per-user replication bootstrap authorization (#19).
//
// A per-user host (in the hosts table, run by a different operator) is
// authorized for exactly one user. The bootstrap must confine both what it
// WRITES from such a peer and what it SERVES to one, to users/<that-uid>/ and
// the userdbs/files scopes — never another user, never sysdbs/apps. A pair
// member (whole-server) stays unrestricted.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestBootstrapPerUserClamp(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")
	// 'puser' is a per-user host authorized for alice AND carol (a peer can host
	// several users, #152). 'ppair' (added below) is a real pair member —
	// unrestricted. 'pstranger' is in neither hosts nor pair and must be served
	// nothing (#144).
	rdb.exec("insert into hosts (user, peer, added, ack) values ('alice', 'puser', 1, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('carol', 'puser', 2, 0)")

	// Set-membership authorizer: a peer may reach the users it hosts, and only
	// those (the #19 confinement, generalised from one user to the host set, #152).
	if !bootstrap_peer_hosts_user("puser", "alice") || !bootstrap_peer_hosts_user("puser", "carol") {
		t.Error("per-user: both hosted users must be authorized (multi-user peer, #152)")
	}
	if bootstrap_peer_hosts_user("puser", "bob") {
		t.Error("per-user: a non-hosted user must be rejected (#19)")
	}
	if bootstrap_peer_hosts_user("puser", "") {
		t.Error("per-user: empty user must be rejected")
	}
	if bootstrap_path_user("alicebob/x") != "alicebob" || bootstrap_path_user("alice/feeds/x") != "alice" || bootstrap_path_user("") != "" {
		t.Error("bootstrap_path_user: wrong leading-segment extraction")
	}

	// Serve gates (peer level).
	if !bootstrap_serve_files_ok("puser", bootstrap_scope_files, "alice/feeds/files/x") {
		t.Error("per-user serve: own files ok")
	}
	if bootstrap_serve_files_ok("puser", bootstrap_scope_files, "bob/feeds/files/x") {
		t.Error("per-user serve: another user's files must be refused")
	}
	if bootstrap_serve_files_ok("puser", bootstrap_scope_apps, "alice/x") {
		t.Error("per-user serve: server-wide apps scope must be refused")
	}
	if !bootstrap_serve_db_ok("puser", bootstrap_scope_userdbs, "alice") {
		t.Error("per-user serve: own userdbs ok")
	}
	if bootstrap_serve_db_ok("puser", bootstrap_scope_userdbs, "bob") {
		t.Error("per-user serve: another user's db must be refused")
	}
	if bootstrap_serve_db_ok("puser", bootstrap_scope_sysdbs, "alice") {
		t.Error("per-user serve: sysdbs must be refused")
	}
	// The SECOND hosted user (carol) must also be served — the dead-end #152 fixes.
	if !bootstrap_serve_db_ok("puser", bootstrap_scope_userdbs, "carol") {
		t.Error("per-user serve: a second hosted user's userdbs must be allowed (#152)")
	}
	if !bootstrap_serve_files_ok("puser", bootstrap_scope_files, "carol/feeds/files/x") {
		t.Error("per-user serve: a second hosted user's files must be allowed (#152)")
	}

	// A real pair member (whole-server) keeps full scope.
	rdb.exec("insert into pair (peer, added, role) values ('ppair', 1, '')")
	pair_membership_refresh()
	if !bootstrap_serve_files_ok("ppair", bootstrap_scope_apps, "anything") {
		t.Error("pair serve: apps ok")
	}
	if !bootstrap_serve_db_ok("ppair", bootstrap_scope_sysdbs, "anyuser") {
		t.Error("pair serve: sysdbs ok")
	}

	// A stranger — in NEITHER hosts nor pair — must be served NOTHING. Before
	// #144 this peer's uid=="" was conflated with the pair case and it was
	// served the sysdbs scope (sessions.db/users.db) and every user's data.
	if bootstrap_serve_db_ok("pstranger", bootstrap_scope_sysdbs, "alice") {
		t.Error("stranger serve: sysdbs (sessions.db/users.db) must be refused")
	}
	if bootstrap_serve_db_ok("pstranger", bootstrap_scope_userdbs, "alice") {
		t.Error("stranger serve: another user's userdbs must be refused")
	}
	if bootstrap_serve_files_ok("pstranger", bootstrap_scope_files, "alice/feeds/files/x") {
		t.Error("stranger serve: files must be refused")
	}
	if bootstrap_serve_files_ok("pstranger", bootstrap_scope_apps, "anything") {
		t.Error("stranger serve: apps must be refused")
	}

	// Receiver db-fetch rejects a per-user peer fetching another user / sysdbs,
	// before any network round-trip.
	if err := bootstrap_db_fetch_impl("puser", bootstrap_scope_userdbs, "", "bob", "feeds", "feeds.db"); err == nil {
		t.Error("db-fetch: per-user peer fetching another user's db must error")
	}
	if err := bootstrap_db_fetch_impl("puser", bootstrap_scope_sysdbs, "", "alice", "", "sessions.db"); err == nil {
		t.Error("db-fetch: per-user peer fetching sysdbs must error")
	}
}
