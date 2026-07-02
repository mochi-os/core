// #170: an abandoned (irreparable) scope must not wedge the receiver-complete
// epoch handshake — completion fires when every scope is done OR irreparable, so a
// gap in one scope doesn't silently break replication with the whole peer.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestBootstrapReceiverCompleteIgnoresIrreparable(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	rdb := db_open("db/replication.db")

	// p1: one scope done, one abandoned (irreparable). Completion MUST fire — the
	// handshake marks the source's epoch baseline pending (peer_epoch.pending=1).
	rdb.exec("insert into bootstrap (scope, peer, state) values ('files', 'p1', 'done')")
	rdb.exec("insert into bootstrap (scope, peer, state) values ('userdbs', 'p1', 'irreparable')")
	bootstrap_receiver_complete("p1")
	if has, _ := rdb.exists("select 1 from peer_epoch where peer='p1' and pending=1"); !has {
		t.Error("receiver-complete must fire when every scope is done-or-irreparable (#170); an irreparable scope must not wedge the handshake")
	}

	// p2: a scope still actively pulling (queued) DOES block completion.
	rdb.exec("insert into bootstrap (scope, peer, state) values ('files', 'p2', 'done')")
	rdb.exec("insert into bootstrap (scope, peer, state) values ('userdbs', 'p2', 'queued')")
	bootstrap_receiver_complete("p2")
	if has, _ := rdb.exists("select 1 from peer_epoch where peer='p2'"); has {
		t.Error("receiver-complete must NOT fire while a scope is still pulling (queued)")
	}
}
