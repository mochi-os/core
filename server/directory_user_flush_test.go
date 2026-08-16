// Mochi server: the learned directory's cap must not be flushable.
//
// Eviction ordered on seen alone handed the spammer the win. Contact is free -
// entity ids are ed25519 public keys and peer identities are as cheap - so a
// flood's rows are always the freshest in the table, and `order by seen asc`
// deleted the user's own quiet counterparts first. For a private entity, which
// the public directory deliberately never lists, those rows are the only route
// to it, so a flushed table means the user can no longer send to the people
// they actually correspond with.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"strings"
	"testing"
)

// directory_flood_entity mints a distinct well-formed entity id.
func directory_flood_entity(n int) string {
	suffix := fmt.Sprintf("%08d", n)
	return strings.Repeat("b", 50-len(suffix)) + suffix
}

// TestDirectoryUserFloodKeepsConfirmedRows is the finding. A quiet but real
// counterpart - learned long ago, delivered to, silent since - must survive a
// flood large enough to turn the whole table over.
func TestDirectoryUserFloodKeepsConfirmedRows(t *testing.T) {
	user, cleanup := test_directory_user(t)
	defer cleanup()

	quiet := strings.Repeat("c", 50)
	quiet_peer := "12D3KooWQuietCounterpart"
	directory_user_learn(user, quiet, quiet_peer)

	db := db_user(user, "user")
	// The relationship is real: this server has delivered to it. Then it goes
	// quiet - six months of silence, which this file's design note is explicit
	// is not the same as dead.
	directory_user_confirm(user, quiet, quiet_peer)
	db.exec("update directory set seen=? where entity=?", now()-180*86400, quiet)

	// A flood from one peer, each row freshly seen. Spread across peers so the
	// per-peer admission cap does not do the work the ordering must do.
	for i := 0; i < directory_user_cap+directory_user_peer_cap; i++ {
		directory_user_learn(user, directory_flood_entity(i), fmt.Sprintf("12D3KooWFlood%d", i/directory_user_peer_cap))
	}

	if n := db.integer("select count(*) from directory where entity=?", quiet); n != 1 {
		t.Fatalf("the quiet confirmed counterpart was evicted by the flood; it is the only route to a private entity")
	}
	if total := db.integer("select count(*) from directory"); total > directory_user_cap {
		t.Errorf("table holds %d rows, above the cap of %d", total, directory_user_cap)
	}
}

// TestDirectoryUserEvictsNeverConfirmedFirst pins the ordering directly: with
// both kinds present and the confirmed row the STALER of the two, the
// unconfirmed one still goes first.
func TestDirectoryUserEvictsNeverConfirmedFirst(t *testing.T) {
	user, cleanup := test_directory_user(t)
	defer cleanup()
	db := db_user(user, "user")

	confirmed := strings.Repeat("d", 50)
	unconfirmed := strings.Repeat("e", 50)
	directory_user_learn(user, confirmed, "12D3KooWConfirmedPeer")
	directory_user_confirm(user, confirmed, "12D3KooWConfirmedPeer")
	directory_user_learn(user, unconfirmed, "12D3KooWUnconfirmedPeer")

	// The confirmed row is older, so a seen-only ordering would take it first.
	db.exec("update directory set seen=? where entity=?", now()-90*86400, confirmed)
	db.exec("update directory set seen=? where entity=?", now()-1, unconfirmed)

	db.exec("delete from directory where (entity, peer) in (select entity, peer from directory order by confirmed asc, seen asc limit 1)")

	if n := db.integer("select count(*) from directory where entity=?", confirmed); n != 1 {
		t.Error("the confirmed row was evicted ahead of a never-confirmed one")
	}
	if n := db.integer("select count(*) from directory where entity=?", unconfirmed); n != 0 {
		t.Error("the never-confirmed row survived; it must be evicted first")
	}
}

// TestDirectoryUserConfirmMarksInsideTheThrottle. The seen rewrite is
// throttled because user.db is the cold, backup-critical store, but the mark
// decides eviction priority - so a relationship whose only delivery ever fell
// inside the refresh window must still be marked, or the cap would evict
// exactly the quiet counterpart it is meant to protect.
func TestDirectoryUserConfirmMarksInsideTheThrottle(t *testing.T) {
	user, cleanup := test_directory_user(t)
	defer cleanup()
	db := db_user(user, "user")

	entity := strings.Repeat("f", 50)
	peer := "12D3KooWFreshlyLearned"
	directory_user_learn(user, entity, peer)

	// Delivered to immediately: seen is inside the refresh window, so the
	// throttled update matches nothing.
	directory_user_confirm(user, entity, peer)

	if c := db.integer("select confirmed from directory where entity=?", entity); c == 0 {
		t.Error("a delivery inside the refresh window left the row unmarked, so the cap would treat it as flood")
	}
}

// TestDirectoryUserPeerCap bounds one peer's share. Secondary to the ordering
// - peer identities are free, so this limits an identity's churn rather than
// the attack - but a single peer must not be able to hold the whole table.
func TestDirectoryUserPeerCap(t *testing.T) {
	user, cleanup := test_directory_user(t)
	defer cleanup()
	db := db_user(user, "user")

	peer := "12D3KooWOnePeer"
	for i := 0; i < directory_user_peer_cap+50; i++ {
		directory_user_learn(user, directory_flood_entity(i), peer)
	}

	n := db.integer("select count(*) from directory where peer=?", peer)
	if n > directory_user_peer_cap {
		t.Errorf("one peer holds %d rows, above the per-peer cap of %d", n, directory_user_peer_cap)
	}
	// A different peer keeps its own budget - one flooder must not deny
	// learning to everyone else.
	other := strings.Repeat("g", 50)
	directory_user_learn(user, other, "12D3KooWAnotherPeer")
	if db.integer("select count(*) from directory where entity=?", other) != 1 {
		t.Error("a different peer was refused; the cap must be per peer")
	}
}

// TestDirectoryUserConfirmedColumnMigrates. The table is created lazily per
// user, so an existing user.db predates the column and every query naming it
// would fail on a real upgrade.
func TestDirectoryUserConfirmedColumnMigrates(t *testing.T) {
	user, cleanup := test_directory_user(t)
	defer cleanup()

	db := db_user(user, "user")
	db.exec("drop table if exists directory")
	db.exec("create table directory (entity text not null, peer text not null, fingerprint text not null default '', created integer not null, seen integer not null, primary key (entity, peer))")

	directory_user_table(db)

	if exists, _ := db.exists("select 1 from pragma_table_info('directory') where name='confirmed'"); !exists {
		t.Fatal("confirmed column was not added to a pre-existing table")
	}
	// And the path still works end to end on the migrated table.
	entity := strings.Repeat("h", 50)
	directory_user_learn(user, entity, "12D3KooWMigrated")
	if len(directory_user_peers(user, entity)) != 1 {
		t.Error("learning failed against the migrated table")
	}
}
