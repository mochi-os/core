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
	// 'puser' is a per-user host authorized for alice. 'ppair' (added below) is
	// a real pair member — unrestricted. 'pstranger' is in neither hosts nor
	// pair and must be served nothing (#144).
	rdb.exec("insert into hosts (user, peer, added, ack) values ('alice', 'puser', 1, 0)")

	// Path authorizer (uid level).
	if !bootstrap_files_relpath_authorized("", "anything/at/all") {
		t.Error("pair (uid==\"\"): every path allowed")
	}
	if !bootstrap_files_relpath_authorized("alice", "alice/feeds/files/x") {
		t.Error("per-user: own subtree allowed")
	}
	if !bootstrap_files_relpath_authorized("alice", "alice") {
		t.Error("per-user: own root allowed")
	}
	if bootstrap_files_relpath_authorized("alice", "bob/feeds/files/x") {
		t.Error("per-user: another user's subtree must be rejected")
	}
	if bootstrap_files_relpath_authorized("alice", "") {
		t.Error("per-user: empty/all-users path must be rejected")
	}
	if bootstrap_files_relpath_authorized("alice", "alicebob/x") {
		t.Error("per-user: prefix-collision (alicebob) must be rejected")
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
