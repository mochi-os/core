// Mochi server: Replication unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"testing"
)

// setup_replication_test creates a fresh data_dir with replication.db
// initialised via the v50 migration. Sets p2p_id to "self" so the
// self-exclusion paths can be exercised. Returns a cleanup function.
func setup_replication_test(t *testing.T) func() {
	tmp_dir, err := os.MkdirTemp("", "mochi_repl_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	orig_data_dir := data_dir
	data_dir = tmp_dir
	orig_p2p_id := p2p_id
	p2p_id = "self"

	db_upgrade_50()

	return func() {
		data_dir = orig_data_dir
		p2p_id = orig_p2p_id
		os.RemoveAll(tmp_dir)
	}
}

// setup_users_test_schema creates a minimal users.db schema for tests that
// exercise the keys-transfer apply path. Mirrors the relevant subset of
// db_create() — only the users and entities tables.
func setup_users_test_schema() {
	users := db_open("db/users.db")
	users.exec("create table users (id integer primary key, username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")
	users.exec("create table entities ( id text not null primary key, private text not null, fingerprint text not null, user references users( id ), parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0 )")
	users.exec("create index entities_user on entities( user )")
}

// 50-character pseudo-entity-id used in tests where valid("entity") needs
// to pass (49-51 word chars). The first character varies so different
// fixtures produce distinct IDs.
func test_entity_id(prefix byte) string {
	out := make([]byte, 50)
	out[0] = prefix
	for i := 1; i < 50; i++ {
		out[i] = 'a'
	}
	return string(out)
}

func TestReplicationRecipientsEmpty(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	peers := recipients("user1")
	if len(peers) != 0 {
		t.Errorf("expected empty recipients, got %v", peers)
	}
}

func TestReplicationRecipientsHosts(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec("insert into hosts (user, peer, added) values ('user1', 'peerA', 0)")
	db.exec("insert into hosts (user, peer, added) values ('user1', 'peerB', 0)")

	peers := recipients("user1")
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d: %v", len(peers), peers)
	}
	got := map[string]bool{}
	for _, p := range peers {
		got[p] = true
	}
	if !got["peerA"] || !got["peerB"] {
		t.Errorf("expected peerA + peerB, got %v", peers)
	}
}

func TestReplicationRecipientsPair(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added) values ('peerX', 0)")
	db.exec("insert into pair (peer, added) values ('peerY', 0)")

	peers := recipients("user-doesnt-matter")
	if len(peers) != 2 {
		t.Errorf("pair members go to every user; expected 2, got %d: %v", len(peers), peers)
	}
}

func TestReplicationRecipientsUnionDedup(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec("insert into hosts (user, peer, added) values ('user1', 'peerA', 0)")
	db.exec("insert into hosts (user, peer, added) values ('user1', 'peerB', 0)")
	db.exec("insert into pair (peer, added) values ('peerB', 0)")
	db.exec("insert into pair (peer, added) values ('peerC', 0)")

	peers := recipients("user1")
	if len(peers) != 3 {
		t.Errorf("union should dedup peerB; expected 3, got %d: %v", len(peers), peers)
	}
}

func TestReplicationRecipientsExcludesSelf(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec("insert into hosts (user, peer, added) values ('user1', 'self', 0)")
	db.exec("insert into hosts (user, peer, added) values ('user1', 'other', 0)")
	db.exec("insert into pair (peer, added) values ('self', 0)")

	peers := recipients("user1")
	if len(peers) != 1 || peers[0] != "other" {
		t.Errorf("self must be filtered; expected ['other'], got %v", peers)
	}
}

func TestReplicationSequenceMonotonic(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s1 := replication_sequence_next("user1", "app")
	s2 := replication_sequence_next("user1", "app")
	s3 := replication_sequence_next("user1", "app")

	if s1 != 1 || s2 != 2 || s3 != 3 {
		t.Errorf("expected 1, 2, 3 sequence; got %d, %d, %d", s1, s2, s3)
	}
}

func TestReplicationSequenceIndependentScopes(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	a := replication_sequence_next("user1", "app")
	b := replication_sequence_next("user1", "core")
	c := replication_sequence_next("user2", "app")

	if a != 1 || b != 1 || c != 1 {
		t.Errorf("each (user, scope) starts at 1; got app/u1=%d core/u1=%d app/u2=%d", a, b, c)
	}

	if next := replication_sequence_next("user1", "app"); next != 2 {
		t.Errorf("user1/app should be 2 on second call, got %d", next)
	}
}

func TestReplicationMembershipApplyFresh(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	mc := &MembershipChange{User: "user1", Hosts: []string{"peerA", "peerB"}, Sequence: 1}
	replication_membership_apply("origin", mc)

	db := db_open("db/replication.db")
	count := db.integer("select count(*) from hosts where user='user1'")
	if count != 2 {
		t.Errorf("expected 2 hosts after fresh apply, got %d", count)
	}
}

func TestReplicationMembershipApplyStaleIgnored(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	mc1 := &MembershipChange{User: "user1", Hosts: []string{"peerA", "peerB"}, Sequence: 5}
	replication_membership_apply("origin1", mc1)

	mc2 := &MembershipChange{User: "user1", Hosts: []string{"peerC"}, Sequence: 3}
	replication_membership_apply("origin2", mc2)

	db := db_open("db/replication.db")
	count := db.integer("select count(*) from hosts where user='user1'")
	if count != 2 {
		t.Errorf("stale apply must not overwrite; expected 2 hosts, got %d", count)
	}

	// Stale messages still get recorded as seen so the sender's queue drops them.
	exists, _ := db.exists("select 1 from seen where peer='origin2' and scope='membership' and user='user1' and sequence=3")
	if !exists {
		t.Errorf("stale membership change must still be recorded as seen")
	}
}

func TestReplicationMembershipApplyDuplicateIgnored(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	mc1 := &MembershipChange{User: "user1", Hosts: []string{"peerA", "peerB"}, Sequence: 1}
	replication_membership_apply("origin", mc1)

	// Same (peer, scope, user, sequence) — must be a no-op even though the
	// payload differs.
	mc2 := &MembershipChange{User: "user1", Hosts: []string{"peerX"}, Sequence: 1}
	replication_membership_apply("origin", mc2)

	db := db_open("db/replication.db")
	rows, _ := db.rows("select peer from hosts where user='user1' order by peer")
	if len(rows) != 2 {
		t.Fatalf("expected 2 hosts (first apply wins), got %d", len(rows))
	}
	if p, _ := rows[0]["peer"].(string); p != "peerA" {
		t.Errorf("expected peerA in first row, got %q", p)
	}
	if p, _ := rows[1]["peer"].(string); p != "peerB" {
		t.Errorf("expected peerB in second row, got %q", p)
	}
}

func TestReplicationMembershipExcludesSelf(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	mc := &MembershipChange{User: "user1", Hosts: []string{"peerA", "self", "peerB"}, Sequence: 1}
	replication_membership_apply("origin", mc)

	db := db_open("db/replication.db")
	count := db.integer("select count(*) from hosts where user='user1' and peer='self'")
	if count != 0 {
		t.Errorf("self peer must be filtered from hosts; got %d rows", count)
	}
	total := db.integer("select count(*) from hosts where user='user1'")
	if total != 2 {
		t.Errorf("expected 2 hosts (peerA, peerB), got %d", total)
	}
}

func TestReplicationKeysTransferFresh(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	a := test_entity_id('a')
	b := test_entity_id('b')
	kt := &KeysTransfer{
		Username: "alice@example.com",
		Role:     "user",
		Methods:  "email",
		Status:   "active",
		Entities: []KeysEntity{
			{ID: a, Private: "priv-a", Fingerprint: "fp-a", Class: "user", Name: "Alice"},
			{ID: b, Private: "priv-b", Fingerprint: "fp-b", Class: "device", Name: "phone"},
		},
	}
	n := replication_keys_transfer_apply(a, "origin", kt)
	if n != 2 {
		t.Fatalf("expected 2 entities inserted, got %d", n)
	}

	udb := db_open("db/users.db")
	count := udb.integer("select count(*) from users where username='alice@example.com'")
	if count != 1 {
		t.Errorf("expected user inserted; got %d rows", count)
	}
	count = udb.integer("select count(*) from entities where user=(select id from users where username='alice@example.com')")
	if count != 2 {
		t.Errorf("expected 2 entities linked to user; got %d", count)
	}
}

func TestReplicationKeysTransferIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	a := test_entity_id('a')
	kt := &KeysTransfer{
		Username: "bob@example.com",
		Entities: []KeysEntity{{ID: a, Private: "p", Fingerprint: "f", Class: "user", Name: "Bob"}},
	}

	if n := replication_keys_transfer_apply(a, "origin", kt); n != 1 {
		t.Errorf("first apply should insert 1, got %d", n)
	}
	if n := replication_keys_transfer_apply(a, "origin", kt); n != 0 {
		t.Errorf("second apply should be a no-op, got %d inserts", n)
	}

	udb := db_open("db/users.db")
	count := udb.integer("select count(*) from users where username='bob@example.com'")
	if count != 1 {
		t.Errorf("expected exactly 1 user row, got %d", count)
	}
}

func TestReplicationKeysTransferRejectsUnauthorisedSigner(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	a := test_entity_id('a')
	intruder := test_entity_id('z')
	kt := &KeysTransfer{
		Username: "charlie@example.com",
		Entities: []KeysEntity{{ID: a, Private: "p", Fingerprint: "f", Class: "user", Name: "Charlie"}},
	}

	// signer is not in the transferred set — reject
	if n := replication_keys_transfer_apply(intruder, "origin", kt); n != 0 {
		t.Errorf("unauthorised signer must be rejected, got %d inserts", n)
	}

	udb := db_open("db/users.db")
	count := udb.integer("select count(*) from users where username='charlie@example.com'")
	if count != 0 {
		t.Errorf("unauthorised transfer must not create user; got %d rows", count)
	}
}

func TestReplicationKeysTransferEmptyUsername(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	a := test_entity_id('a')
	kt := &KeysTransfer{
		Username: "",
		Entities: []KeysEntity{{ID: a, Private: "p", Fingerprint: "f", Class: "user", Name: "X"}},
	}
	if n := replication_keys_transfer_apply(a, "origin", kt); n != 0 {
		t.Errorf("empty username must be rejected, got %d inserts", n)
	}
}

func TestReplicationKeysTransferExistingUser(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	// User already exists locally (e.g. they signed up here before opt-in).
	udb := db_open("db/users.db")
	udb.exec("insert into users (username, role, methods, status) values ('dave@example.com', 'user', 'email', 'active')")
	localID := udb.integer("select id from users where username='dave@example.com'")

	a := test_entity_id('a')
	kt := &KeysTransfer{
		Username: "dave@example.com",
		Entities: []KeysEntity{{ID: a, Private: "p", Fingerprint: "f", Class: "user", Name: "Dave"}},
	}
	if n := replication_keys_transfer_apply(a, "origin", kt); n != 1 {
		t.Errorf("expected 1 entity insert for existing user, got %d", n)
	}

	owner := udb.integer("select user from entities where id=?", a)
	if owner != localID {
		t.Errorf("entity must be linked to existing local user %d, got %d", localID, owner)
	}
}

func TestReplicationMembershipNewerOverwrites(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	mc1 := &MembershipChange{User: "user1", Hosts: []string{"peerA"}, Sequence: 1}
	replication_membership_apply("origin1", mc1)

	mc2 := &MembershipChange{User: "user1", Hosts: []string{"peerB", "peerC"}, Sequence: 2}
	replication_membership_apply("origin2", mc2)

	db := db_open("db/replication.db")
	rows, _ := db.rows("select peer from hosts where user='user1' order by peer")
	if len(rows) != 2 {
		t.Fatalf("expected 2 hosts after newer apply, got %d", len(rows))
	}
	got := map[string]bool{}
	for _, r := range rows {
		if p, ok := r["peer"].(string); ok {
			got[p] = true
		}
	}
	if got["peerA"] || !got["peerB"] || !got["peerC"] {
		t.Errorf("newer state must replace older; got %v", got)
	}
}
