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
	// 'puser' is a per-user host authorized for alice. 'pother' is in neither
	// hosts nor pair, so bootstrap_peer_user returns "" — i.e. unrestricted,
	// the whole-server/pair case.
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

	// Unrestricted peer (pair/whole-server) keeps full scope.
	if !bootstrap_serve_files_ok("pother", bootstrap_scope_apps, "anything") {
		t.Error("unrestricted serve: apps ok")
	}
	if !bootstrap_serve_db_ok("pother", bootstrap_scope_sysdbs, "anyuser") {
		t.Error("unrestricted serve: sysdbs ok")
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
