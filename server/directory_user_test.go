// Mochi server: the per-user learned directory. Rows are learned from
// claim-verified contact, refreshed with a write throttle, confirmed on
// delivery success, evicted only on terminal send failure or beyond the
// LRU cap — never by age (a quiet relationship is not a dead host).
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"strings"
	"testing"
)

func test_directory_user(t *testing.T) (*User, func()) {
	cleanup := create_test_users_db(t)
	db := db_open("db/users.db")
	db.exec("create table if not exists entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	return &User{UID: "u-dir"}, cleanup
}

func TestDirectoryUserLifecycle(t *testing.T) {
	user, cleanup := test_directory_user(t)
	defer cleanup()
	entity := strings.Repeat("a", 50)
	peer := "12D3KooWTestPeerOne"

	directory_user_learn(user, entity, peer)
	rows := directory_user_peers(user, entity)
	if len(rows) != 1 || rows[0]["peer"] != peer {
		t.Fatalf("learned rows = %v, want one row for %q", rows, peer)
	}

	// Throttle: an immediate relearn must not move seen.
	db := db_user(user, "user")
	db.exec("update directory set seen=? where entity=?", now()-10, entity)
	directory_user_learn(user, entity, peer)
	if seen := db.integer("select seen from directory where entity=?", entity); int64(seen) > now()-5 {
		t.Errorf("seen moved within the refresh window: %d", seen)
	}

	// A stale row IS refreshed.
	db.exec("update directory set seen=? where entity=?", now()-2*directory_user_refresh, entity)
	directory_user_learn(user, entity, peer)
	if seen := db.integer("select seen from directory where entity=?", entity); int64(seen) < now()-5 {
		t.Errorf("stale row not refreshed: %d", seen)
	}

	// Confirm bumps a stale row the same way.
	db.exec("update directory set seen=? where entity=?", now()-2*directory_user_refresh, entity)
	directory_user_confirm(user, entity, peer)
	if seen := db.integer("select seen from directory where entity=?", entity); int64(seen) < now()-5 {
		t.Errorf("confirm did not refresh: %d", seen)
	}

	// A second peer for the same entity coexists (mid-move); freshest first.
	peer2 := "12D3KooWTestPeerTwo"
	directory_user_learn(user, entity, peer2)
	db.exec("update directory set seen=? where entity=? and peer=?", now()-1000, entity, peer)
	rows = directory_user_peers(user, entity)
	if len(rows) != 2 || rows[0]["peer"] != peer2 {
		t.Fatalf("freshest-first ordering broken: %v", rows)
	}

	// Terminal failure evicts exactly the dead row.
	directory_user_forget(user, entity, peer)
	rows = directory_user_peers(user, entity)
	if len(rows) != 1 || rows[0]["peer"] != peer2 {
		t.Fatalf("forget removed the wrong row: %v", rows)
	}

	// Local entities and self-peer rows are never learned.
	local := strings.Repeat("b", 50)
	db_open("db/users.db").exec("insert into entities (id, user) values (?, 'u-dir')", local)
	directory_user_learn(user, local, peer2)
	if got := directory_user_peers(user, local); len(got) != 0 {
		t.Errorf("learned a local entity: %v", got)
	}
}

func TestDirectoryUserCap(t *testing.T) {
	user, cleanup := test_directory_user(t)
	defer cleanup()
	db := db_user(user, "user")
	// Seed cap-1 old rows directly, then learn two fresh ones: total exceeds
	// the cap by one, and the eviction must take the oldest-seen row.
	for i := 0; i < directory_user_cap-1; i++ {
		db.exec("insert into directory (entity, peer, created, seen) values (?, 'p', ?, ?)",
			fmt.Sprintf("%050d", i), now(), int64(i))
	}
	fresh := strings.Repeat("c", 50)
	directory_user_learn(user, fresh, "12D3KooWFresh")
	fresh2 := strings.Repeat("d", 50)
	directory_user_learn(user, fresh2, "12D3KooWFresh")
	if total := db.integer("select count(*) from directory"); total != directory_user_cap {
		t.Errorf("cap not enforced: %d rows", total)
	}
	if got := directory_user_peers(user, fresh2); len(got) != 1 {
		t.Errorf("freshest row evicted instead of oldest")
	}
	if got := directory_user_peers(user, "%050d"); len(got) != 0 {
		_ = got
	}
	if remaining := db.integer("select count(*) from directory where entity=?", fmt.Sprintf("%050d", 0)); remaining != 0 {
		t.Errorf("oldest row survived the cap eviction")
	}
}

func TestEntityPeersForMerge(t *testing.T) {
	user, cleanup := test_directory_user(t)
	defer cleanup()
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-dir', 'dir@test')")
	sender := strings.Repeat("e", 50)
	udb.exec("insert into entities (id, user, class) values (?, 'u-dir', 'person')", sender)

	// Public directory knows nothing about this private target.
	ddb := db_open("db/directory.db")
	ddb.exec("create table if not exists entries ( entity text not null, peer text not null, seen integer not null )")

	target := strings.Repeat("f", 50)
	directory_user_learn(user, target, "12D3KooWPrivateHome")

	peers := entity_peers_for(sender, target)
	if len(peers) != 1 || peers[0] != "12D3KooWPrivateHome" {
		t.Fatalf("merge did not resolve the private target: %v", peers)
	}

	// A fresher public row outranks the learned one.
	ddb.exec("insert into entries (entity, peer, seen) values (?, '12D3KooWPublicNew', ?)", target, now()+100)
	peers = entity_peers_for(sender, target)
	if len(peers) != 2 || peers[0] != "12D3KooWPublicNew" {
		t.Fatalf("freshest-first merge broken: %v", peers)
	}

	// No sender context: public rows only.
	peers = entity_peers_for("", target)
	if len(peers) != 1 || peers[0] != "12D3KooWPublicNew" {
		t.Fatalf("anonymous resolution leaked user rows: %v", peers)
	}
}

// A private local entity must not be reachable via the bare self-loop by an
// unrelated caller (ping/request/stream) — it would confirm existence the
// unlisting is meant to hide. Its owner still reaches it; a public local
// entity is unaffected.
func TestFailoverForPrivateLocalGate(t *testing.T) {
	cleanup := create_test_users_db(t)
	defer cleanup()
	db := db_open("db/users.db")
	db.exec("create table if not exists entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")

	priv := strings.Repeat("a", 50)
	pub := strings.Repeat("b", 50)
	owner_ent := strings.Repeat("c", 50)
	other_ent := strings.Repeat("d", 50)
	db.exec("insert into entities (id, user, class, privacy) values (?, 'owner', 'feed', 'private')", priv)
	db.exec("insert into entities (id, user, class, privacy) values (?, 'owner', 'feed', 'public')", pub)
	db.exec("insert into entities (id, user, class, privacy) values (?, 'owner', 'person', 'public')", owner_ent)
	db.exec("insert into entities (id, user, class, privacy) values (?, 'other', 'person', 'public')", other_ent)

	// Unrelated caller: private local entity is NOT reachable.
	if got := entity_peers_failover_for(other_ent, priv); len(got) != 0 {
		t.Errorf("private local entity reachable by an unrelated caller: %v", got)
	}
	// Owner reaching their own private entity: self-loop stands.
	if got := entity_peers_failover_for(owner_ent, priv); len(got) != 1 || got[0] != net_id {
		t.Errorf("owner cannot reach their own private entity: %v", got)
	}
	// Public local entity: reachable by anyone (unchanged).
	if got := entity_peers_failover_for(other_ent, pub); len(got) != 1 || got[0] != net_id {
		t.Errorf("public local entity not reachable: %v", got)
	}
	// Delivery resolver is deliberately NOT gated (app handler is the gate).
	if got := entity_peers_for(other_ent, priv); len(got) != 1 || got[0] != net_id {
		t.Errorf("delivery to a private local entity must still self-loop: %v", got)
	}
}
