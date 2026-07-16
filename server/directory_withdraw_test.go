// Mochi server: directory ghost-withdrawal unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Tests covering entry_store's self-row echo handling: a received row
// naming this host for an entity that no longer exists locally is a
// pre-wipe ghost, answered with a host-signed entry_delete_self; a row
// for a live entity is dropped without withdrawal.

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

// create_test_directory_db builds the users.db entities table and the
// directory.db entries table inside the create_test_users_db temp
// data_dir, and points net_id at a fixed test peer. Returns a cleanup
// restoring net_id (the data_dir cleanup comes from create_test_users_db).
func create_test_directory_db(t *testing.T) func() {
	t.Helper()
	users_cleanup := create_test_users_db(t)

	users := db_open("db/users.db")
	users.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")

	db := db_open("db/directory.db")
	db.exec("create table entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, signature text not null default '', attestation text not null default '', primary key ( entity, peer ) )")

	orig_net_id := net_id
	net_id = "12D3KooWDirectoryWithdrawTestPeer"

	return func() {
		net_id = orig_net_id
		users_cleanup()
	}
}

// withdraw_test_entity returns a fresh valid entity id (base58 ed25519 public key).
func withdraw_test_entity(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	return base58_encode(pub)
}

// withdraw_test_row builds a received-row Entry naming this host, with
// valid fields and timestamps. Signatures are irrelevant: the self-row
// branch runs before signature verification.
func withdraw_test_row(entity string) *Entry {
	t := now()
	return &Entry{Entity: entity, Peer: net_id, Name: "Ghost", Class: "person", Version: t, Created: t, Seen: t}
}

// TestEntryStoreWithdrawsGhostSelfRow confirms an echoed self-row for a
// nonexistent entity triggers entry_delete_self (observable as the local
// row for that (entity, net_id) pair being deleted) and is not stored.
func TestEntryStoreWithdrawsGhostSelfRow(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	ghost := withdraw_test_entity(t)
	db := db_open("db/directory.db")
	db.exec("insert into entries (entity, peer, name, class, created, seen) values (?, ?, 'Ghost', 'person', 1, 1)", ghost, net_id)

	if entry_store(withdraw_test_row(ghost), "test") {
		t.Error("self-row echo must not be stored")
	}
	exists, _ := db.exists("select 1 from entries where entity=? and peer=?", ghost, net_id)
	if exists {
		t.Error("ghost self-row must be withdrawn (entry_delete_self not called)")
	}
}

// TestEntryStoreKeepsLiveSelfRow confirms an echoed self-row for an
// entity that exists locally is dropped WITHOUT withdrawal.
func TestEntryStoreKeepsLiveSelfRow(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	live := withdraw_test_entity(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-live', 'live@example.com')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-live', 'person', 'Live')", live, fingerprint(live))

	db := db_open("db/directory.db")
	db.exec("insert into entries (entity, peer, name, class, created, seen) values (?, ?, 'Live', 'person', 1, 1)", live, net_id)

	if entry_store(withdraw_test_row(live), "test") {
		t.Error("self-row echo must not be stored")
	}
	exists, _ := db.exists("select 1 from entries where entity=? and peer=?", live, net_id)
	if !exists {
		t.Error("live entity's self-row must not be withdrawn")
	}
}

// TestEntryStoreRefusesForeignRowForLocalEntity confirms a row naming a
// DIFFERENT peer for an entity this host owns is refused before signature
// verification (owner-authoritative): clones and restored backups hold the
// entity's keys, so their rows VERIFY — ownership, not the signature, is
// the deciding check. Storing one would offer delivery fan-out a foreign
// route for a local subscriber (the 2026-07-06 News feed wedge trigger).
func TestEntryStoreRefusesForeignRowForLocalEntity(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	owned := withdraw_test_entity(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-own', 'own@example.com')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-own', 'person', 'Owned')", owned, fingerprint(owned))

	ts := now()
	foreign := &Entry{Entity: owned, Peer: "12D3KooWSomeForeignClonePeer", Name: "Owned", Class: "person", Version: ts, Created: ts, Seen: ts}
	if entry_store(foreign, "test") {
		t.Error("foreign row for a locally-owned entity must be refused")
	}
	db := db_open("db/directory.db")
	exists, _ := db.exists("select 1 from entries where entity=? and peer<>?", owned, net_id)
	if exists {
		t.Error("foreign row for a locally-owned entity must not be stored")
	}
}

// TestEntryStoreWithdrawalRateLimited confirms repeated echoes of the
// same ghost within the window trigger only one withdrawal.
func TestEntryStoreWithdrawalRateLimited(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	ghost := withdraw_test_entity(t)
	db := db_open("db/directory.db")

	entry_store(withdraw_test_row(ghost), "test")

	// Re-insert a marker row; a second echo inside the rate window must
	// NOT delete it, because the withdrawal budget is spent.
	db.exec("insert into entries (entity, peer, name, class, created, seen) values (?, ?, 'Ghost', 'person', 1, 1)", ghost, net_id)
	entry_store(withdraw_test_row(ghost), "test")
	exists, _ := db.exists("select 1 from entries where entity=? and peer=?", ghost, net_id)
	if !exists {
		t.Error("second echo within the rate window must not trigger another withdrawal")
	}
}
