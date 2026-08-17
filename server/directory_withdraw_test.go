// Mochi server: directory ghost-withdrawal unit tests
// Copyright © 2026 Mochisoft OÜ
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
	"os"
	"strings"
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
	db.exec("create table entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, message text not null default '', expires text not null default '', signature text not null default '', primary key ( entity, peer ) )")

	orig_net_id := net_id
	net_id = "12D3KooWDirectoryWithdrawTestPeer"

	return func() {
		net_id = orig_net_id
		users_cleanup()
	}
}

// withdraw_test_entity returns a fresh valid entity id (base58 ed25519
// public key). Used across the directory and broadcast tests wherever only
// the id matters.
func withdraw_test_entity(t *testing.T) string {
	t.Helper()
	id, _ := withdraw_test_signer(t)
	return id
}

// withdraw_test_signer returns the same, plus the private key that signs
// rows for it. Needed wherever the row has to verify, which since the
// verify-first reordering is every path into entry_store's later branches.
func withdraw_test_signer(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	return base58_encode(public), private
}

// withdraw_test_row builds a received-row Entry naming this host, signed by
// the entity, as a genuine echo of a row this host once published is.
func withdraw_test_row(t *testing.T, entity string, key ed25519.PrivateKey) *Entry {
	t.Helper()
	return withdraw_test_row_for(t, entity, key, net_id)
}

// withdraw_test_row_for is the same for an arbitrary peer, so a test can
// build the foreign-peer row the owner-authoritative branch refuses.
func withdraw_test_row_for(t *testing.T, entity string, key ed25519.PrivateKey, peer string) *Entry {
	t.Helper()
	ts := now()
	en := &Entry{
		Entity:  entity,
		Peer:    peer,
		Name:    "Ghost",
		Class:   "person",
		Version: ts,
		Created: ts,
		Seen:    ts,
		Message: "msg-" + entity,
		Expires: i64toa(now() + pubsub_expires_ttl),
	}
	signable, err := pubsub_signable(en.Message, entity, "directory", "publish", en.Expires, entry_content(en))
	if err != nil {
		t.Fatalf("pubsub_signable: %v", err)
	}
	en.Signature = base58_encode(ed25519.Sign(key, signable))
	return en
}

// withdraw_test_row_unsigned is the attacker's row: the same shape, with a
// signature that verifies against nothing. On push and sync every field
// here is read straight off the wire, so an unauthenticated peer chooses
// the entity id freely.
func withdraw_test_row_unsigned(entity string) *Entry {
	ts := now()
	return &Entry{
		Entity:    entity,
		Peer:      net_id,
		Name:      "Ghost",
		Class:     "person",
		Version:   ts,
		Created:   ts,
		Seen:      ts,
		Message:   "msg-" + entity,
		Expires:   i64toa(now() + pubsub_expires_ttl),
		Signature: base58_encode(make([]byte, ed25519.SignatureSize)),
	}
}

// TestEntryStoreWithdrawsGhostSelfRow confirms an echoed self-row for a
// nonexistent entity triggers entry_delete_self (observable as the local
// row for that (entity, net_id) pair being deleted) and is not stored.
func TestEntryStoreWithdrawsGhostSelfRow(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	ghost, key := withdraw_test_signer(t)
	db := db_open("db/directory.db")
	db.exec("insert into entries (entity, peer, name, class, created, seen) values (?, ?, 'Ghost', 'person', 1, 1)", ghost, net_id)

	if entry_store(withdraw_test_row(t, ghost, key), "test") {
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

	live, key := withdraw_test_signer(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-live', 'live@example.com')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-live', 'person', 'Live')", live, fingerprint(live))

	db := db_open("db/directory.db")
	db.exec("insert into entries (entity, peer, name, class, created, seen) values (?, ?, 'Live', 'person', 1, 1)", live, net_id)

	if entry_store(withdraw_test_row(t, live, key), "test") {
		t.Error("self-row echo must not be stored")
	}
	exists, _ := db.exists("select 1 from entries where entity=? and peer=?", live, net_id)
	if !exists {
		t.Error("live entity's self-row must not be withdrawn")
	}
}

// TestEntryStoreRefusesForeignRowForLocalEntity confirms a row naming a
// DIFFERENT peer for an entity this host owns is refused even though it
// verifies (owner-authoritative): clones and restored backups hold the
// entity's keys, so their rows VERIFY — ownership, not the signature, is
// the deciding check. The row here is genuinely signed, which is what
// makes the test meaningful; an unsigned one would now be refused a step
// earlier and prove nothing about ownership. Storing one would offer
// delivery fan-out a foreign route for a local subscriber (the 2026-07-06
// News feed wedge trigger).
func TestEntryStoreRefusesForeignRowForLocalEntity(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	owned, key := withdraw_test_signer(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-own', 'own@example.com')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-own', 'person', 'Owned')", owned, fingerprint(owned))

	foreign := withdraw_test_row_for(t, owned, key, "12D3KooWSomeForeignClonePeer")
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

	ghost, key := withdraw_test_signer(t)
	db := db_open("db/directory.db")

	entry_store(withdraw_test_row(t, ghost, key), "test")

	// Re-insert a marker row; a second echo inside the rate window must
	// NOT delete it, because the withdrawal budget is spent.
	db.exec("insert into entries (entity, peer, name, class, created, seen) values (?, ?, 'Ghost', 'person', 1, 1)", ghost, net_id)
	entry_store(withdraw_test_row(t, ghost, key), "test")
	exists, _ := db.exists("select 1 from entries where entity=? and peer=?", ghost, net_id)
	if !exists {
		t.Error("second echo within the rate window must not trigger another withdrawal")
	}
}

// TestEntryStoreIgnoresUnsignedGhostRow is the finding. The ghost branch
// answers an echo with a host-signed directory/delete broadcast, and it ran
// before entry_verify - so on push and sync, where the whole Entry is read
// off the wire, an invented entity id bought a signature and a publish from
// us. Observable here as the marker row surviving: entry_delete_self must
// never have run.
func TestEntryStoreIgnoresUnsignedGhostRow(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	ghost, _ := withdraw_test_signer(t)
	db := db_open("db/directory.db")
	db.exec("insert into entries (entity, peer, name, class, created, seen) values (?, ?, 'Ghost', 'person', 1, 1)", ghost, net_id)

	if entry_store(withdraw_test_row_unsigned(ghost), "push") {
		t.Error("unsigned row must not be stored")
	}
	exists, _ := db.exists("select 1 from entries where entity=? and peer=?", ghost, net_id)
	if !exists {
		t.Error("an unsigned row reached the ghost-withdrawal branch: it deleted the row, so it also signed and published a directory/delete")
	}
}

// TestEntryStoreUnsignedGhostSpendsNoRateLimit is the other half of the
// cost. The limiter is keyed on the entity, so an unverified row that
// reaches the branch also inserts an attacker-chosen key that lives for the
// full window - unbounded in breadth however tight the per-entity limit is.
// Spending no budget is what proves the branch was never entered.
func TestEntryStoreUnsignedGhostSpendsNoRateLimit(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	ghost, key := withdraw_test_signer(t)
	db := db_open("db/directory.db")

	// An unsigned echo first. If it spends the entity's once-per-hour
	// budget, the genuine echo below is refused and the ghost is never
	// withdrawn - the fix turning into a denial of the real feature.
	entry_store(withdraw_test_row_unsigned(ghost), "push")

	db.exec("insert into entries (entity, peer, name, class, created, seen) values (?, ?, 'Ghost', 'person', 1, 1)", ghost, net_id)
	entry_store(withdraw_test_row(t, ghost, key), "test")
	exists, _ := db.exists("select 1 from entries where entity=? and peer=?", ghost, net_id)
	if exists {
		t.Error("the unsigned row spent the entity's withdrawal budget, so the genuine echo was refused")
	}
}

// TestEntryStoreIgnoresUnsignedForeignRow pins the same ordering for the
// owner-authoritative branch. It has no side effect of its own, so this is
// a cheaper claim than the ghost case - but it keeps the two branches
// reasoning from a signature that has actually been checked.
func TestEntryStoreIgnoresUnsignedForeignRow(t *testing.T) {
	cleanup := create_test_directory_db(t)
	defer cleanup()

	owned, _ := withdraw_test_signer(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-own', 'own@example.com')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-own', 'person', 'Owned')", owned, fingerprint(owned))

	foreign := withdraw_test_row_unsigned(owned)
	foreign.Peer = "12D3KooWSomeForeignClonePeer"
	if entry_store(foreign, "push") {
		t.Error("unsigned foreign row must not be stored")
	}
}

// TestEntryStoreVerifiesBeforeSideEffects is the ordering itself. The
// behavioural tests above see the outcome; this one fails if a later change
// moves either side-effecting branch back above the signature check.
func TestEntryStoreVerifiesBeforeSideEffects(t *testing.T) {
	source, err := os.ReadFile("directory.go")
	if err != nil {
		t.Fatalf("read directory.go: %v", err)
	}
	body := string(source)
	body = body[strings.Index(body, "func entry_store("):]
	body = body[:strings.Index(body, "\n}\n")]

	verify := strings.Index(body, "if !entry_verify(en)")
	ghost := strings.Index(body, "if en.Peer == net_id")
	local := strings.Index(body, "entity_local(en.Entity)")
	if verify < 0 || ghost < 0 || local < 0 {
		t.Fatalf("entry_store no longer has the expected checks (verify=%d ghost=%d local=%d)", verify, ghost, local)
	}
	if verify > ghost {
		t.Error("the ghost-withdrawal branch runs before entry_verify: it signs and publishes on unauthenticated input")
	}
	if verify > local {
		t.Error("the owner-authoritative branch runs before entry_verify")
	}
}
