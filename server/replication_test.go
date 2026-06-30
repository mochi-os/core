// Mochi server: Replication unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	"github.com/gin-gonic/gin"
	p2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
	sl "go.starlark.net/starlark"
)

// setup_replication_test creates a fresh data_dir with replication.db
// initialised via the v50 migration. Sets net_id to "self" so the
// self-exclusion paths can be exercised. Returns a cleanup function.
func setup_replication_test(t *testing.T) func() {
	tmp_dir, err := os.MkdirTemp("", "mochi_repl_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	orig_data_dir := data_dir
	data_dir = tmp_dir
	orig_p2p_id := net_id
	net_id = "self"

	db_upgrade_50()
	db_upgrade_55()
	db_upgrade_62()
	db_upgrade_66()
	db_upgrade_67()
	db_upgrade_76()
	db_upgrade_77()
	db_upgrade_78()
	db_upgrade_81() // hosts: seen + attestation columns (membership v2)
	db_upgrade_87() // bootstrap: progress + attempts columns (universal retry)
	db_upgrade_89() // epoch + peer_epoch tables (replication generations, #65)
	db_upgrade_90() // journal_sequence/journal_delivery (replication.db) + journal_inflight (queue.db), #28/#424

	// queue.db is touched by Message.send_work via send_peer goroutines —
	// approve / deny tests fire emits asynchronously and would otherwise
	// panic on missing table. Create the queue schema to absorb the
	// goroutine writes (no actual delivery happens in unit tests; queue
	// rows just accumulate and are torn down with the temp dir).
	queue := db_open("db/queue.db")
	queue.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20 )")

	// Stub system-scope emits so setting_set / domain_* / apps_class_set
	// calls (which many handlers trigger indirectly) don't spawn
	// goroutines that outlive cleanup.
	orig_emit_system_set := replication_emit_system_set
	orig_emit_system_row := replication_emit_system_row
	replication_emit_system_set = func(database, table, row, field, value string) {}
	replication_emit_system_row = func(database, table string, key, cols map[string]string, del bool) {}

	// link-denied emit spawns send_peer goroutines too — same problem.
	orig_emit_link_denied := replication_emit_link_denied
	replication_emit_link_denied = func(destinationPeer, placeholder, reason string) {}

	// Pair backfill (row-by-row sysdbs replication on join-approve)
	// enumerates users + system tables and fires queue-based emits.
	// Stub here; tests that exercise the backfill itself override
	// locally.
	orig_pair_backfill := replication_pair_backfill
	replication_pair_backfill = func(peer string) {}

	// Bootstrap sync-RPC fetches spawn goroutines that hit libp2p; stub
	// them out for the same reason. Tests that need to observe the
	// emit override these again locally (see
	// TestBootstrapStartSeedsScopesAndEmitsManifests).
	orig_file_manifest_fetch := replication_bootstrap_file_manifest_fetch
	orig_db_manifest_fetch := replication_bootstrap_db_manifest_fetch
	orig_file_chunk_fetch := bootstrap_file_chunk_fetch
	orig_file_scope_driver := bootstrap_file_scope_driver
	orig_db_fetch := bootstrap_db_fetch
	orig_db_scope_driver := bootstrap_db_scope_driver
	replication_bootstrap_file_manifest_fetch = func(peer, scope, prefix string) {}
	replication_bootstrap_db_manifest_fetch = func(peer, scope, user string) {}
	bootstrap_file_chunk_fetch = func(peer, scope, path string, offset, length int64) (*BootstrapFileChunk, error) {
		return nil, nil
	}
	bootstrap_file_scope_driver = func(peer, scope string, needed []BootstrapFileEntry) {}
	bootstrap_db_fetch = func(peer, scope, path, user, app, db string) error { return nil }
	bootstrap_db_scope_driver = func(peer, scope string, entries []BootstrapDBEntry) {}

	// bootstrap_scope_settled fires this emit as a goroutine; queue.db
	// may be torn down before it runs.
	orig_emit_bootstrap_scope_done := replication_bootstrap_emit_scope_done
	replication_bootstrap_emit_scope_done = func(peer, scope string) {}

	// The scope-settled hook also spawns replication_pending_drain /
	// apps_load_published goroutines that read data_dir via db_open and
	// would race the data_dir reset on cleanup. No-op the whole hook; the
	// done-state it follows is already set by the caller. Tests that need
	// the side effects call bootstrap_scope_settled_impl directly.
	orig_scope_settled := bootstrap_scope_settled
	bootstrap_scope_settled = func(peer, scope string) {}

	// db_upgrade_63 adds the bootstrap_served table that
	// replication_join_approve_core populates. Without it the
	// approve path errors out.
	db_upgrade_63()

	return func() {
		replication_bootstrap_emit_scope_done = orig_emit_bootstrap_scope_done
		bootstrap_scope_settled = orig_scope_settled
		replication_emit_system_set = orig_emit_system_set
		replication_emit_system_row = orig_emit_system_row
		replication_emit_link_denied = orig_emit_link_denied
		replication_bootstrap_file_manifest_fetch = orig_file_manifest_fetch
		replication_bootstrap_db_manifest_fetch = orig_db_manifest_fetch
		bootstrap_file_chunk_fetch = orig_file_chunk_fetch
		bootstrap_file_scope_driver = orig_file_scope_driver
		bootstrap_db_fetch = orig_db_fetch
		bootstrap_db_scope_driver = orig_db_scope_driver
		replication_pair_backfill = orig_pair_backfill
		// Drain any /mochi/2 self-loop workers spawned by this test
		// before we mutate global state (data_dir, net_id) — otherwise
		// a worker mid-handler would race the assignments below.
		// Production code never tears down data_dir; this is a
		// test-only concern.
		workers_drain_test(500 * time.Millisecond)
		data_dir = orig_data_dir
		net_id = orig_p2p_id
		os.RemoveAll(tmp_dir)
	}
}

// setup_users_test_schema creates a minimal users.db schema for tests that
// exercise the keys-transfer or session-replication apply paths. Mirrors
// the v53 schema: uid is the PK on users, FKs reference users(uid).
func setup_users_test_schema() {
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', disabled text not null default '', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")
	users.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null references users(uid) on delete cascade, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create index entities_user on entities(user)")
	// Auth-factor tables — mirrors db.go's uid-keyed schema. Needed by
	// the per-user link keys-transfer tests (auth factors travel in the
	// payload).
	users.exec("create table credentials (id blob primary key, user text not null references users(uid) on delete cascade, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create table recovery (id integer primary key, user text not null references users(uid) on delete cascade, hash text not null, created integer not null)")
	users.exec("create table totp (user text primary key references users(uid) on delete cascade, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("create table oauth (id integer primary key, user text not null references users(uid) on delete cascade, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create table tokens (hash text primary key not null, user text not null references users(uid) on delete cascade, app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")
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

// setup_sessions_test_schema creates the sessions table for tests that
// exercise session-replication apply paths.
func setup_sessions_test_schema() {
	sessions := db_open("db/sessions.db")
	sessions.exec("create table sessions (user text not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
	sessions.exec("create unique index sessions_code on sessions(code)")
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
	db.exec("insert into hosts (user, peer, added) values ('user1', 'peer_b', 0)")

	peers := recipients("user1")
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d: %v", len(peers), peers)
	}
	got := map[string]bool{}
	for _, p := range peers {
		got[p] = true
	}
	if !got["peerA"] || !got["peer_b"] {
		t.Errorf("expected peerA + peer_b, got %v", peers)
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
	db.exec("insert into hosts (user, peer, added) values ('user1', 'peer_b', 0)")
	db.exec("insert into pair (peer, added) values ('peer_b', 0)")
	db.exec("insert into pair (peer, added) values ('peerC', 0)")

	peers := recipients("user1")
	if len(peers) != 3 {
		t.Errorf("union should dedup peer_b; expected 3, got %d: %v", len(peers), peers)
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

// Membership v2: per-host self-assertion. A receiver only reacts to
// membership ops for a user it holds locally, so these tests seed a users
// row. Real libp2p host identities (test_host, from directory_test.go) sign
// asserts/leaves so server_verify runs for real.

// membership_user seeds a local users row + a hosts row for `peer`, so this
// server is a host of `user` and already knows `peer`.
func membership_user(t *testing.T, user, peer string, seen int64) {
	t.Helper()
	udb := db_open("db/users.db")
	udb.exec("create table if not exists users (uid text primary key, username text not null default '', status text not null default 'active', purge integer not null default 0)")
	udb.exec("insert or replace into users (uid, username) values (?, 'u@x')", user)
	udb.exec("create table if not exists entities (id text primary key, user text not null default '')")
	if peer != "" {
		db_open("db/replication.db").exec("insert or replace into hosts (user, peer, added, ack, seen) values (?, ?, 1, 0, ?)", user, peer, seen)
	}
}

func membership_assert_event(t *testing.T, user, peer string, key p2p_crypto.PrivKey, seen int64) *Event {
	t.Helper()
	signable, _ := membership_assert_signable(user, peer, seen)
	sig, err := key.Sign(signable)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return &Event{service: "replication", event: "membership/assert", content: map[string]any{
		"user": user, "peer": peer, "seen": i64toa(seen), "attestation": base58_encode(sig)}}
}

func membership_leave_event(t *testing.T, user, peer string, key p2p_crypto.PrivKey, when int64) *Event {
	t.Helper()
	signable, _ := membership_leave_signable(user, peer, when)
	sig, err := key.Sign(signable)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return &Event{service: "replication", event: "membership/leave", content: map[string]any{
		"user": user, "peer": peer, "time": i64toa(when), "attestation": base58_encode(sig)}}
}

// TestMembershipJoinAdds: a user-authorised join (signer is the user's
// identity) adds the named peer; it is additive and never removes.
func TestMembershipJoinAdds(t *testing.T) {
	cleanup := setup_directory_test(t) // protocol2_init + users/entities tables
	defer cleanup()
	user, _ := test_identity(t)
	peerB, _ := test_host(t)
	membership_user(t, user, "", 0)
	db := db_open("db/users.db")
	db.exec("insert or replace into entities (id, user) values (?, ?)", user, user) // signer is a user identity

	replication_membership_join_event(&Event{from: user, service: "replication", event: "membership/join",
		content: map[string]any{"user": user, "peer": peerB}})

	if n := db_open("db/replication.db").integer("select count(*) from hosts where user=? and peer=?", user, peerB); n != 1 {
		t.Errorf("join did not add peer; rows=%d want 1", n)
	}
}

// TestMembershipLeaveSelfOnly: a host's own signed leave removes only its
// row; a leave forged for another peer (wrong host key) is rejected. This is
// the strip-resistance property.
func TestMembershipLeaveSelfOnly(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()
	user, _ := test_identity(t)
	peerB, keyB := test_host(t)
	peerC, keyC := test_host(t)
	membership_user(t, user, peerB, 100)
	db_open("db/replication.db").exec("insert or replace into hosts (user, peer, added, ack, seen) values (?, ?, 1, 0, 100)", user, peerC)
	db := db_open("db/replication.db")

	// C tries to remove B (signs a leave naming B with C's key) → rejected.
	bad := membership_leave_event(t, user, peerB, keyC, 200)
	replication_membership_leave_event(bad)
	if n := db.integer("select count(*) from hosts where user=? and peer=?", user, peerB); n != 1 {
		t.Error("leave with another host's key stripped peerB; want kept")
	}

	// B removes itself (B's key) → applied.
	replication_membership_leave_event(membership_leave_event(t, user, peerB, keyB, 200))
	if n := db.integer("select count(*) from hosts where user=? and peer=?", user, peerB); n != 0 {
		t.Error("self-signed leave did not remove peerB")
	}
	// C's row is untouched.
	if n := db.integer("select count(*) from hosts where user=? and peer=?", user, peerC); n != 1 {
		t.Error("leave removed an unrelated peer")
	}
}

// TestMembershipAssertRefreshesKnownOnly: a host-signed assert refreshes
// `seen` for a known peer, and is ignored for an unknown peer (no self-join).
func TestMembershipAssertRefreshesKnownOnly(t *testing.T) {
	cleanup := setup_directory_test(t)
	defer cleanup()
	user, _ := test_identity(t)
	peerB, keyB := test_host(t)
	unknown, keyU := test_host(t)
	membership_user(t, user, peerB, 100)
	db := db_open("db/replication.db")

	replication_membership_assert_event(membership_assert_event(t, user, peerB, keyB, 500))
	if s := db.integer("select seen from hosts where user=? and peer=?", user, peerB); s != 500 {
		t.Errorf("assert did not refresh seen; got %d want 500", s)
	}
	// Unknown peer self-asserting must NOT join.
	replication_membership_assert_event(membership_assert_event(t, user, unknown, keyU, 500))
	if n := db.integer("select count(*) from hosts where user=? and peer=?", user, unknown); n != 0 {
		t.Error("assert from an unknown peer self-joined; want ignored")
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
	count = udb.integer("select count(*) from entities where user=(select uid from users where username='alice@example.com')")
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
	udb.exec("insert into users (uid, username, role, methods, status) values (?, ?, ?, ?, ?)", "uid-dave", "dave@example.com", "user", "email", "active")
	local_uid := "uid-dave"

	a := test_entity_id('a')
	kt := &KeysTransfer{
		Username: "dave@example.com",
		Entities: []KeysEntity{{ID: a, Private: "p", Fingerprint: "f", Class: "user", Name: "Dave"}},
	}
	if n := replication_keys_transfer_apply(a, "origin", kt); n != 1 {
		t.Errorf("expected 1 entity insert for existing user, got %d", n)
	}

	var owner string
	if row, _ := udb.row("select user from entities where id=?", a); row != nil {
		owner, _ = row["user"].(string)
	}
	if owner != local_uid {
		t.Errorf("entity must be linked to existing local user %q, got %q", local_uid, owner)
	}
}

func TestReplicationSessionApplyInsert(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")

	p := &SessionInsert{
		UserUID: "uid-alice", Code: "sess-1", Secret: "secret-1",
		Expires: 100, Created: 50, Accessed: 50,
		Address: "127.0.0.1", Agent: "test",
	}
	replication_session_apply_insert(p)

	sdb := db_open("db/sessions.db")
	count := sdb.integer("select count(*) from sessions where code='sess-1' and user='uid-alice'")
	if count != 1 {
		t.Errorf("expected session inserted with local user=uid-alice; got %d rows", count)
	}
}

func TestReplicationSessionApplyInsertUnknownUser(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	// No matching user row — apply must skip without panicking.
	p := &SessionInsert{
		UserUID: "uid-unknown", Code: "sess-1", Secret: "x",
		Expires: 100, Created: 50, Accessed: 50,
	}
	replication_session_apply_insert(p)

	sdb := db_open("db/sessions.db")
	count := sdb.integer("select count(*) from sessions where code='sess-1'")
	if count != 0 {
		t.Errorf("session must not be inserted when user is unknown locally; got %d rows", count)
	}
}

func TestReplicationSessionApplyInsertReplaces(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")

	// Pre-existing local row.
	sdb := db_open("db/sessions.db")
	sdb.exec("insert into sessions (user, code, secret, expires, created, accessed) values (1, 'sess-1', 'old-secret', 100, 50, 50)")

	// Replicated insert with same code but updated fields.
	p := &SessionInsert{
		UserUID: "uid-alice", Code: "sess-1", Secret: "new-secret",
		Expires: 200, Created: 50, Accessed: 75,
	}
	replication_session_apply_insert(p)

	row, _ := sdb.row("select secret, accessed from sessions where code='sess-1'")
	if row == nil {
		t.Fatalf("session row missing after replace")
	}
	if s, _ := row["secret"].(string); s != "new-secret" {
		t.Errorf("replace should update secret; got %q", s)
	}
	if a, _ := row["accessed"].(int64); a != 75 {
		t.Errorf("replace should update accessed; got %d", a)
	}
}

func TestReplicationSessionApplyDelete(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_sessions_test_schema()

	sdb := db_open("db/sessions.db")
	sdb.exec("insert into sessions (user, code, secret, expires, created, accessed) values (1, 'sess-1', 's', 100, 50, 50)")

	replication_session_apply_delete(&SessionDelete{Code: "sess-1"})

	count := sdb.integer("select count(*) from sessions where code='sess-1'")
	if count != 0 {
		t.Errorf("delete must remove the session; got %d rows", count)
	}
}

func TestReplicationSessionApplyDeleteNonExistent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_sessions_test_schema()

	// Delete a code that doesn't exist — must be a no-op, not a panic.
	replication_session_apply_delete(&SessionDelete{Code: "never-existed"})
}

// The v51 dual-write trigger test was removed when v53 rebuilt the users
// table with uid as the sole identifier. The transitional v51 state is no
// longer a stable target — v53 immediately drops the parallel user_uid
// columns and triggers and recreates every FK table with `user` as TEXT
// referencing `users(uid)`.

func TestReplicationPendingBufferAndDrain(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	// Session insert arrives BEFORE the user is replicated — must defer.
	p := &SessionInsert{
		UserUID: "uid-late", Code: "sess-late", Secret: "x",
		Expires: 100, Created: 50, Accessed: 50,
	}
	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-late",
		Database: "sessions", Table: "sessions", Operation: repl_op_insert,
		Sequence: 1, Payload: cbor_encode(p),
	}
	if got := replication_apply_op(op); got != ApplyDeferred {
		t.Fatalf("expected ApplyDeferred for unknown user, got %v", got)
	}

	// Buffer it manually (mimicking what the event handler does).
	db := db_open("db/replication.db")
	db.exec(
		"insert into pending (peer, scope, user, sequence, schema, payload, received) values (?, ?, ?, ?, ?, ?, ?)",
		"origin", op.Scope, op.User, op.Sequence, op.Schema, cbor_encode(op), now())

	count := db.integer("select count(*) from pending where user='uid-late'")
	if count != 1 {
		t.Fatalf("expected op in pending, got %d", count)
	}

	// Now the user lands locally.
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-late", "late@example.com")

	// Drain — the op should now apply.
	replication_pending_drain()

	count = db.integer("select count(*) from pending where user='uid-late'")
	if count != 0 {
		t.Errorf("pending must be empty after successful drain; got %d rows", count)
	}

	count = db.integer("select count(*) from seen where user='uid-late' and sequence=1")
	if count != 1 {
		t.Errorf("drained op must be recorded in seen; got %d rows", count)
	}

	sdb := db_open("db/sessions.db")
	count = sdb.integer("select count(*) from sessions where code='sess-late' and user='uid-late'")
	if count != 1 {
		t.Errorf("session must be in sessions.db after drain; got %d rows", count)
	}
}

// TestReplicationPendingStalled verifies the stalled-stream detector
// classifies pending rows correctly: anchored streams with the next-
// op present and unanchored streams with a Prev==0 op present are
// not stalled (they'll drain on the next tick); anchored streams with
// a gap and unanchored streams whose pending all has prev>0 are.
func TestReplicationPendingStalled(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	ts := now()

	// Stream 1: unanchored, all pending rows have prev>0 (the Prev==0
	// stream-start never arrived). Stalled.
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peerA', 'app', 'u1', 'dbA', 7, 6, 1, ?, ?)",
		[]byte{0x00}, ts)
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peerA', 'app', 'u1', 'dbA', 8, 7, 1, ?, ?)",
		[]byte{0x00}, ts)

	// Stream 2: unanchored but has a Prev==0 stream-start in pending.
	// Will drain naturally, not stalled.
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_b', 'app', 'u2', 'dbB', 1, 0, 1, ?, ?)",
		[]byte{0x00}, ts)

	// Stream 3: anchored at cursor=5, has the next op (prev=5) in
	// pending. Will drain naturally, not stalled.
	db.exec(
		"insert into cursor (peer, scope, user, db, sequence) values ('peerC', 'app', 'u3', 'dbC', 5)")
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peerC', 'app', 'u3', 'dbC', 6, 5, 1, ?, ?)",
		[]byte{0x00}, ts)

	// Stream 4: anchored at cursor=10, but pending starts at prev=15.
	// Stalled (gap between cursor and the chain head).
	db.exec(
		"insert into cursor (peer, scope, user, db, sequence) values ('peerD', 'app', 'u4', 'dbD', 10)")
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peerD', 'app', 'u4', 'dbD', 16, 15, 1, ?, ?)",
		[]byte{0x00}, ts)

	stalled := replication_pending_stalled()

	// Expect exactly stream 1 (peerA) and stream 4 (peerD).
	got := map[string]bool{}
	for _, s := range stalled {
		got[s.Peer+"/"+s.Database] = true
	}
	if !got["peerA/dbA"] {
		t.Errorf("expected peerA/dbA to be stalled (unanchored, no Prev==0)")
	}
	if got["peer_b/dbB"] {
		t.Errorf("peer_b/dbB should NOT be stalled (has Prev==0 stream-start)")
	}
	if got["peerC/dbC"] {
		t.Errorf("peerC/dbC should NOT be stalled (next op prev=cursor present)")
	}
	if !got["peerD/dbD"] {
		t.Errorf("expected peerD/dbD to be stalled (gap between cursor=10 and min_prev=15)")
	}
	if len(stalled) != 2 {
		t.Errorf("expected exactly 2 stalled streams, got %d: %+v", len(stalled), stalled)
	}
}

// setup_replication_test creates replication.db but not settings.db;
// the GC reads its TTL via setting_get so the tests need both. Single
// helper to keep the per-test setup compact.
func setup_replication_pending_gc_test(t *testing.T) func() {
	cleanup := setup_replication_test(t)
	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings (name text primary key, value text not null)")
	return cleanup
}

// TestReplicationPendingGcDropsAgedUnfillable: rows in a stalled
// stream older than the TTL get purged. Recent rows in the same
// stream stay. Same stream classification as
// replication_pending_stalled.
func TestReplicationPendingGcDropsAgedUnfillable(t *testing.T) {
	cleanup := setup_replication_pending_gc_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	// 1-day TTL via setting override.
	setting_set("replication.pending.unfillable_ttl_days", "1")
	old_ts := now() - 5*86400   // 5 days old -> dropped
	recent_ts := now() - 1*3600 // 1 hour old -> kept

	// Stalled stream (unanchored, no Prev==0). Two old rows + one
	// recent row.
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_a', 'app', 'u1', 'db_a', 7, 6, 1, ?, ?)",
		[]byte{0x00}, old_ts)
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_a', 'app', 'u1', 'db_a', 8, 7, 1, ?, ?)",
		[]byte{0x00}, old_ts)
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_a', 'app', 'u1', 'db_a', 9, 8, 1, ?, ?)",
		[]byte{0x00}, recent_ts)

	dropped := replication_pending_gc()
	if dropped != 2 {
		t.Errorf("dropped = %d, want 2 (two old rows in a stalled stream)", dropped)
	}

	var remaining int64
	row, _ := db.row("select count(*) as n from pending where peer='peer_a'")
	if row != nil {
		remaining, _ = row["n"].(int64)
	}
	if remaining != 1 {
		t.Errorf("remaining rows for peer_a = %d, want 1 (the recent row)", remaining)
	}
}

// TestReplicationPendingGcKeepsRecentRows: even on a stalled stream,
// rows younger than the TTL stay - the operator might still recover
// them via resync.
func TestReplicationPendingGcKeepsRecentRows(t *testing.T) {
	cleanup := setup_replication_pending_gc_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	setting_set("replication.pending.unfillable_ttl_days", "30")
	// Two recent rows in a stalled stream.
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_a', 'app', 'u1', 'db_a', 7, 6, 1, ?, ?)",
		[]byte{0x00}, now()-1*3600)
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_a', 'app', 'u1', 'db_a', 8, 7, 1, ?, ?)",
		[]byte{0x00}, now()-2*3600)

	dropped := replication_pending_gc()
	if dropped != 0 {
		t.Errorf("dropped = %d, want 0 (rows younger than TTL)", dropped)
	}
}

// TestReplicationPendingGcKeepsHealthyStreams: a stream that's NOT
// stalled (has its Prev==0 stream-start in pending, or next-op
// prev=cursor) keeps all its rows regardless of age. The classifier
// must match replication_pending_stalled exactly.
func TestReplicationPendingGcKeepsHealthyStreams(t *testing.T) {
	cleanup := setup_replication_pending_gc_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	setting_set("replication.pending.unfillable_ttl_days", "1")
	old_ts := now() - 5*86400

	// Stream WITH Prev==0 stream-start -> not stalled even though
	// rows are old.
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_a', 'app', 'u1', 'db_a', 1, 0, 1, ?, ?)",
		[]byte{0x00}, old_ts)
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_a', 'app', 'u1', 'db_a', 2, 1, 1, ?, ?)",
		[]byte{0x00}, old_ts)

	dropped := replication_pending_gc()
	if dropped != 0 {
		t.Errorf("dropped = %d, want 0 (healthy stream, no GC even on old rows)", dropped)
	}
}

// TestReplicationPendingGcRespectsSetting: setting override changes
// the cutoff. Default is 30 days; override to 1 day cuts more.
func TestReplicationPendingGcRespectsSetting(t *testing.T) {
	cleanup := setup_replication_pending_gc_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	// 5-day-old row in a stalled stream.
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values ('peer_a', 'app', 'u1', 'db_a', 7, 6, 1, ?, ?)",
		[]byte{0x00}, now()-5*86400)

	// Default TTL (30 days): the 5-day-old row is NOT yet aged out.
	if dropped := replication_pending_gc(); dropped != 0 {
		t.Errorf("default TTL: dropped = %d, want 0 (row only 5 days old)", dropped)
	}

	// Operator shrinks the TTL to 1 day.
	setting_set("replication.pending.unfillable_ttl_days", "1")
	if dropped := replication_pending_gc(); dropped != 1 {
		t.Errorf("after TTL=1: dropped = %d, want 1", dropped)
	}
}

func TestReplicationPendingDrainMalformedDropped(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec(
		"insert into pending (peer, scope, user, sequence, schema, payload, received) values ('origin', 'app', 'u', 1, 0, ?, ?)",
		[]byte{0xff, 0xff, 0xff}, now())

	replication_pending_drain()

	count := db.integer("select count(*) from pending")
	if count != 0 {
		t.Errorf("malformed payload must be dropped from pending; got %d rows", count)
	}
}

// TestReplicationGatedStreamAppliesInSequenceOrder is the Stage 19
// regression: an inbound db-stream applies buffered ops in chain
// order, so a create→delete pair on one row can't be reordered by a
// backlog draining out of order. The pending rows are inserted
// delete-first (the bug's arrival order); a correct drain still
// applies insert(seq 1, Prev 0) then delete(seq 2, Prev 1), leaving
// the row gone. See claude/plans/replication-test.md Stage 19.
func TestReplicationGatedStreamAppliesInSequenceOrder(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-gate", "gate@example.com")

	db := db_open("db/replication.db")

	insert := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-gate",
		Database: "sessions", Table: "sessions", Operation: repl_op_insert,
		Sequence: 1, Prev: 0,
		Payload: cbor_encode(&SessionInsert{
			UserUID: "uid-gate", Code: "sess-G", Secret: "x",
			Expires: 100, Created: 50, Accessed: 50,
		}),
	}
	del := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-gate",
		Database: "sessions", Table: "sessions", Operation: repl_op_delete,
		Sequence: 2, Prev: 1,
		Payload: cbor_encode(&SessionDelete{Code: "sess-G"}),
	}

	// Buffer them delete-first, so received order is the wrong order.
	// The insert is the Prev==0 stream start; the delete chains on it.
	for _, o := range []*ReplicationOp{del, insert} {
		db.exec(
			"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values (?, ?, ?, ?, ?, ?, 0, ?, ?)",
			"peerG", o.Scope, o.User, repl_stream_key(repl_stream_class_system, "sessions"), o.Sequence, o.Prev, cbor_encode(o), now())
	}

	replication_pending_drain()

	sdb := db_open("db/sessions.db")
	if n := sdb.integer("select count(*) from sessions where code='sess-G'"); n != 0 {
		t.Errorf("session must be gone — delete must apply after insert; got %d rows", n)
	}
	if n := db.integer("select count(*) from pending where peer='peerG'"); n != 0 {
		t.Errorf("pending must be empty after in-order drain; got %d rows", n)
	}
	if seq, anchored := replication_cursor(db, "peerG", repl_scope_app, "uid-gate", repl_stream_key(repl_stream_class_system, "sessions")); !anchored || seq != 2 {
		t.Errorf("cursor must advance to 2; got seq=%d anchored=%v", seq, anchored)
	}
}

// TestReplicationGatedStreamGapStopsDrain: a db-stream stops at the
// first missing chain link (head-of-line). With seq 1 (Prev 0) and
// seq 3 (Prev 2) buffered but the seq-2 op absent, the drain applies
// 1, leaves 3 in pending, and the cursor does not advance past the gap.
func TestReplicationGatedStreamGapStopsDrain(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-gap", "gap@example.com")

	db := db_open("db/replication.db")

	op := func(seq, prev int64, code string) *ReplicationOp {
		return &ReplicationOp{
			Scope: repl_scope_app, User: "uid-gap",
			Database: "sessions", Table: "sessions", Operation: repl_op_insert,
			Sequence: seq, Prev: prev,
			Payload: cbor_encode(&SessionInsert{
				UserUID: "uid-gap", Code: code, Secret: "x",
				Expires: 100, Created: 50, Accessed: 50,
			}),
		}
	}
	for _, o := range []*ReplicationOp{op(1, 0, "sess-1"), op(3, 2, "sess-3")} {
		db.exec(
			"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values (?, ?, ?, ?, ?, ?, 0, ?, ?)",
			"peerH", o.Scope, o.User, repl_stream_key(repl_stream_class_system, "sessions"), o.Sequence, o.Prev, cbor_encode(o), now())
	}

	replication_pending_drain()

	sdb := db_open("db/sessions.db")
	if n := sdb.integer("select count(*) from sessions where code='sess-1'"); n != 1 {
		t.Errorf("seq 1 must apply; got %d rows", n)
	}
	if n := sdb.integer("select count(*) from sessions where code='sess-3'"); n != 0 {
		t.Errorf("seq 3 must stay buffered behind the gap; got %d rows", n)
	}
	if n := db.integer("select count(*) from pending where peer='peerH' and sequence=3"); n != 1 {
		t.Errorf("seq 3 must remain in pending; got %d rows", n)
	}
	if seq, _ := replication_cursor(db, "peerH", repl_scope_app, "uid-gap", repl_stream_key(repl_stream_class_system, "sessions")); seq != 1 {
		t.Errorf("cursor must stop at 1 (the gap is at 2); got seq=%d", seq)
	}
}

// TestReplicationPerDBStreamsIndependent is the increment-2 headline
// fix: a gap in one db-stream no longer head-of-line-blocks another.
// Increment 1 keyed one cursor per (peer, scope, user), so a stuck op
// in any DB stalled every other DB of that user.
func TestReplicationPerDBStreamsIndependent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-multi", "multi@example.com")

	db := db_open("db/replication.db")
	buffer := func(stream string, o *ReplicationOp) {
		db.exec(
			"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values (?, ?, ?, ?, ?, ?, 0, ?, ?)",
			"peerM", o.Scope, o.User, stream, o.Sequence, o.Prev, cbor_encode(o), now())
	}
	// The "users" stream has a gap (seq 7 chains on an absent seq 4).
	buffer("users", &ReplicationOp{
		Scope: repl_scope_app, User: "uid-multi", Database: "users",
		Sequence: 7, Prev: 4,
	})
	// The "sessions" stream is intact — its first op is a Prev==0 start.
	buffer("sessions", &ReplicationOp{
		Scope: repl_scope_app, User: "uid-multi",
		Database: "sessions", Table: "sessions", Operation: repl_op_insert,
		Sequence: 1, Prev: 0,
		Payload: cbor_encode(&SessionInsert{
			UserUID: "uid-multi", Code: "sess-ok", Secret: "x",
			Expires: 100, Created: 50, Accessed: 50,
		}),
	})

	replication_pending_drain()

	sdb := db_open("db/sessions.db")
	if n := sdb.integer("select count(*) from sessions where code='sess-ok'"); n != 1 {
		t.Errorf("sessions stream must drain despite the users-stream gap; got %d", n)
	}
	if n := db.integer("select count(*) from pending where db='users'"); n != 1 {
		t.Errorf("the gapped users-stream op must stay buffered; got %d", n)
	}
}

// TestReplicationStreamStartReanchors covers the schema-67 migration
// seam: post-upgrade the sender's tail is empty, so the first op per
// stream carries Prev==0 — a stream (re)start that applies whether or
// not the stream is anchored. The monotonic cursor must not rewind
// when such a straggler carries a lower sequence.
func TestReplicationStreamStartReanchors(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-seam", "seam@example.com")

	db := db_open("db/replication.db")
	// Stream already advanced to 5 by earlier ops.
	replication_cursor_set(db, "peerS", repl_scope_app, "uid-seam", repl_stream_key(repl_stream_class_system, "sessions"), 5)

	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-seam",
		Database: "sessions", Table: "sessions", Operation: repl_op_insert,
		Sequence: 2, Prev: 0,
		Payload: cbor_encode(&SessionInsert{
			UserUID: "uid-seam", Code: "sess-seam", Secret: "x",
			Expires: 100, Created: 50, Accessed: 50,
		}),
	}
	db.exec(
		"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values (?, ?, ?, ?, ?, ?, 0, ?, ?)",
		"peerS", op.Scope, op.User, repl_stream_key(repl_stream_class_system, "sessions"), op.Sequence, op.Prev, cbor_encode(op), now())

	replication_stream_drain(db, "peerS", repl_scope_app, "uid-seam", repl_stream_key(repl_stream_class_system, "sessions"))

	sdb := db_open("db/sessions.db")
	if n := sdb.integer("select count(*) from sessions where code='sess-seam'"); n != 1 {
		t.Errorf("Prev==0 op must apply even on an anchored stream; got %d rows", n)
	}
	if seq, _ := replication_cursor(db, "peerS", repl_scope_app, "uid-seam", repl_stream_key(repl_stream_class_system, "sessions")); seq != 5 {
		t.Errorf("monotonic cursor must not rewind below 5; got %d", seq)
	}
}

// TestBootstrapStreamKey checks the DB-file → stream-key mapping that
// keeps the bootstrap cursor-seed per-physical-DB exact, including the
// 1:many app case (app data DB vs the per-app config DB app.db, which
// both travel under op.Database = app.id on the wire).
func TestBootstrapStreamKey(t *testing.T) {
	cases := []struct{ path, want string }{
		{"users/uid-1/feeds/db/feeds.db", "app:feeds"},
		{"users/uid-1/feeds/app.db", "app:feeds/system"},
		{"users/uid-1/user.db", "core:user"},
		{"users/uid-1/notifications.db", "core:notifications"},
		// An app whose data file is itself named app.db still keys on
		// the app (not the config-DB suffix) — keyed off path structure.
		{"users/uid-1/feeds/db/app.db", "app:feeds"},
		{"db/users.db", ""},
		{"db/sessions.db", ""},
	}
	for _, c := range cases {
		if got := bootstrap_stream_key(c.path); got != c.want {
			t.Errorf("bootstrap_stream_key(%q) = %q, want %q", c.path, got, c.want)
		}
	}
}

// TestBootstrapDBSeedCursor: bootstrap_db_seed_cursor derives the
// stream key and owning user from a just-landed DB path and writes the
// apply cursor at the snapshot's sequence point — so a freshly
// bootstrapped replica's streams are gated, not un-anchored (piece 3).
func TestBootstrapDBSeedCursor(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	cases := []struct {
		rel, stream, user string
	}{
		{"users/uid-a/feeds/db/feeds.db", "app:feeds", "uid-a"},
		{"users/uid-a/feeds/app.db", "app:feeds/system", "uid-a"},
		{"users/uid-b/user.db", "core:user", "uid-b"},
		{"users/uid-b/notifications.db", "core:notifications", "uid-b"},
	}
	for i, c := range cases {
		seed := int64(10 + i)
		bootstrap_db_seed_cursor("src", filepath.Join(data_dir, c.rel), seed)
		got, anchored := replication_cursor(db, "src", repl_scope_app, c.user, c.stream)
		if !anchored || got != seed {
			t.Errorf("%s: cursor = (%d, %v), want (%d, true)", c.rel, got, anchored, seed)
		}
	}

	// A DB path no replication stream targets seeds nothing.
	bootstrap_db_seed_cursor("src", filepath.Join(data_dir, "db/users.db"), 99)
	if _, anchored := replication_cursor(db, "src", repl_scope_app, "", "users.db"); anchored {
		t.Error("non-stream DB path must not seed a cursor")
	}
}

// TestReplicationFreshReplicaSeedThenDrain models piece 3 end to end:
// live ops reach a fresh replica before its cursor is seeded — with no
// cursor and Prev>0 they buffer, never applied onto a not-yet-landed
// DB — then the bootstrap / keys-transfer seed lands and the buffered
// ops chain from it and drain.
func TestReplicationFreshReplicaSeedThenDrain(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-fresh", "fresh@example.com")

	db := db_open("db/replication.db")
	// Two ops chained from sequence 40 — as if the source stream was
	// already well past 0 when this replica joined. No cursor: buffered.
	mk := func(seq, prev int64, code string) *ReplicationOp {
		return &ReplicationOp{
			Scope: repl_scope_app, User: "uid-fresh",
			Database: "sessions", Table: "sessions", Operation: repl_op_insert,
			Sequence: seq, Prev: prev,
			Payload: cbor_encode(&SessionInsert{
				UserUID: "uid-fresh", Code: code, Secret: "x",
				Expires: 100, Created: 50, Accessed: 50,
			}),
		}
	}
	for _, o := range []*ReplicationOp{mk(41, 40, "s41"), mk(42, 41, "s42")} {
		db.exec(
			"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values (?, ?, ?, ?, ?, ?, 0, ?, ?)",
			"src", o.Scope, o.User, repl_stream_key(repl_stream_class_system, "sessions"), o.Sequence, o.Prev, cbor_encode(o), now())
	}

	// Before the seed: a drain can't place Prev>0 ops on an un-anchored
	// stream — they stay buffered.
	replication_stream_drain(db, "src", repl_scope_app, "uid-fresh", repl_stream_key(repl_stream_class_system, "sessions"))
	sdb := db_open("db/sessions.db")
	if n := sdb.integer("select count(*) from sessions where code in ('s41','s42')"); n != 0 {
		t.Fatalf("un-seeded stream must not apply Prev>0 ops; got %d", n)
	}

	// The seed lands — cursor at the source's snapshot point, 40.
	replication_cursor_set(db, "src", repl_scope_app, "uid-fresh", repl_stream_key(repl_stream_class_system, "sessions"), 40)
	replication_stream_drain(db, "src", repl_scope_app, "uid-fresh", repl_stream_key(repl_stream_class_system, "sessions"))

	if n := sdb.integer("select count(*) from sessions where code in ('s41','s42')"); n != 2 {
		t.Errorf("after seeding cursor=40 the buffered ops must chain and apply; got %d", n)
	}
	if n := db.integer("select count(*) from pending where peer='src'"); n != 0 {
		t.Errorf("pending must be empty after drain; got %d", n)
	}
	if seq, _ := replication_cursor(db, "src", repl_scope_app, "uid-fresh", repl_stream_key(repl_stream_class_system, "sessions")); seq != 42 {
		t.Errorf("cursor must advance to 42; got %d", seq)
	}
}

// TestReplicationPendingKickTTL: replication_pending_kick fires at
// most once per app id within the TTL window. Without this gate, a
// drain finding 50 stuck ops for the same app would fan out 50
// app_check_install goroutines on each 30s tick.
func TestReplicationPendingKickTTL(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Use a valid-shaped entity id so the kick gate's `valid(_, "entity")`
	// check passes.
	app_id := test_entity_id('a')

	// Fresh state: first kick is due.
	replication_pending_kick_mu.Lock()
	delete(replication_pending_kick_last, app_id)
	replication_pending_kick_mu.Unlock()

	if !replication_pending_kick_due(app_id) {
		t.Error("first kick on fresh app: want true, got false")
	}
	// Immediately re-asking: gated by TTL.
	if replication_pending_kick_due(app_id) {
		t.Error("repeat kick within TTL: want false, got true")
	}

	// Backdate the last-kick timestamp past the TTL → next call is due.
	replication_pending_kick_mu.Lock()
	replication_pending_kick_last[app_id] = now() - replication_pending_kick_ttl_s - 1
	replication_pending_kick_mu.Unlock()
	if !replication_pending_kick_due(app_id) {
		t.Error("kick after TTL elapsed: want true, got false")
	}
}

// TestReplicationPendingKickRejectsNonEntityApp: dev / internal apps
// (string ids) don't have a publisher download path, so kicking
// app_check_install for them is pointless. Verify the gate filters them.
func TestReplicationPendingKickRejectsNonEntityApp(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Snapshot the kick map; ensure it doesn't gain an entry for a
	// non-entity app id even after a Deferred call goes through.
	replication_pending_kick_mu.Lock()
	before := len(replication_pending_kick_last)
	replication_pending_kick_mu.Unlock()

	replication_pending_kick(&ReplicationOp{
		Scope:    repl_scope_app,
		Database: "feeds", // string id, not a fingerprint
		User:     "u1",
	})

	replication_pending_kick_mu.Lock()
	after := len(replication_pending_kick_last)
	replication_pending_kick_mu.Unlock()
	if after != before {
		t.Errorf("non-entity app id must not be tracked; before=%d after=%d", before, after)
	}
}

func TestReplicationKeysTransferDrainsPending(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	// A session insert for a user we don't know yet is deferred.
	p := &SessionInsert{
		UserUID: "uid-via-keys", Code: "sess-keys", Secret: "x",
		Expires: 100, Created: 50, Accessed: 50,
	}
	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-via-keys",
		Database: "sessions", Table: "sessions", Operation: repl_op_insert,
		Sequence: 1, Payload: cbor_encode(p),
	}
	db := db_open("db/replication.db")
	db.exec(
		"insert into pending (peer, scope, user, sequence, schema, payload, received) values ('origin', ?, ?, ?, ?, ?, ?)",
		op.Scope, op.User, op.Sequence, op.Schema, cbor_encode(op), now())

	// Pre-create the user with the uid the pending op references so
	// keys-transfer finds it and only inserts the entity.
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-via-keys", "kuser@example.com")

	// Keys-transfer arrives — keys are added and pending drain fires.
	a := test_entity_id('a')
	kt := &KeysTransfer{
		Username: "kuser@example.com",
		Entities: []KeysEntity{{ID: a, Private: "p", Fingerprint: "f", Class: "user", Name: "K"}},
	}
	if n := replication_keys_transfer_apply(a, "origin", kt); n != 1 {
		t.Fatalf("keys-transfer should insert 1 entity; got %d", n)
	}

	sdb := db_open("db/sessions.db")
	count := sdb.integer("select count(*) from sessions where code='sess-keys'")
	if count != 1 {
		t.Errorf("session must be applied after user uid matches; got %d rows", count)
	}
}

func TestDirectoryMigrationRenameAndBackfill(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_dir_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	// Simulate a pre-v52 directory.db: the old single 'directory' table.
	db := db_open("db/directory.db")
	db.exec("create table directory ( id text not null primary key, name text not null, class text not null, location text not null default '', data text not null default '', fingerprint text not null default '', created integer not null, updated integer not null )")
	db.exec("create index directory_location on directory(location)")
	db.exec("insert into directory (id, name, class, location, fingerprint, created, updated) values ('ent1', 'Alice', 'person', 'p2p/peerA', 'fp1', 100, 100)")
	db.exec("insert into directory (id, name, class, location, fingerprint, created, updated) values ('ent2', 'Bob', 'person', '', 'fp2', 200, 200)")

	db_upgrade_52()

	// Table was renamed.
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='entities'"); !exists {
		t.Fatalf("v52 must rename directory to entities")
	}
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='directory'"); exists {
		t.Errorf("the old directory table should no longer exist")
	}

	// locations table exists.
	if exists, _ := db.exists("select 1 from sqlite_master where type='table' and name='locations'"); !exists {
		t.Fatalf("v52 must create locations table")
	}

	// Backfill: ent1 had a location, ent2 didn't. Only ent1 should land in locations.
	count := db.integer("select count(*) from locations")
	if count != 1 {
		t.Errorf("expected 1 backfilled location (ent1 only); got %d", count)
	}
	if row, _ := db.row("select peer from locations where entity='ent1'"); row == nil {
		t.Errorf("ent1 location missing from backfill")
	} else if p, _ := row["peer"].(string); p != "peerA" {
		t.Errorf("backfilled peer should strip the p2p/ prefix; got %q", p)
	}

	// Re-running the migration is idempotent.
	db_upgrade_52()
	count = db.integer("select count(*) from locations")
	if count != 1 {
		t.Errorf("idempotent migration changed locations count: now %d", count)
	}
}

func TestDirectoryEntityPeersMultiHost(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_peers_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()
	orig_p2p := net_id
	net_id = "selfpeer"
	defer func() { net_id = orig_p2p }()

	// Set up the directory schema.
	dir := db_open("db/directory.db")
	dir.exec("create table entries ( entity text not null, peer text not null, name text not null, class text not null, data text not null default '', fingerprint text not null default '', version integer not null default 0, created integer not null, seen integer not null, signature text not null default '', attestation text not null default '', primary key ( entity, peer ) )")
	// Also need a users.db entities table so entity_peers can do its local check.
	udb := db_open("db/users.db")
	udb.exec("create table entities (id text primary key, user text not null default '')")

	now_ts := now()
	dir.exec("insert into entries (entity, peer, name, class, version, created, seen) values ('e1', 'peerA', 'n', 'person', 1, 1, ?)", now_ts-100)
	dir.exec("insert into entries (entity, peer, name, class, version, created, seen) values ('e1', 'peer_b', 'n', 'person', 1, 1, ?)", now_ts-50)
	dir.exec("insert into entries (entity, peer, name, class, version, created, seen) values ('e1', 'peerC', 'n', 'person', 1, 1, ?)", now_ts) // most recent

	peers := entity_peers("e1")
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d: %v", len(peers), peers)
	}
	// Most-recent-first ordering.
	if peers[0] != "peerC" {
		t.Errorf("expected peerC first (most recent); got %q", peers[0])
	}

	// entity_peer returns the most recent.
	if p := entity_peer("e1"); p != "peerC" {
		t.Errorf("entity_peer should return most recent peer; got %q", p)
	}

	// Local entity short-circuits.
	udb.exec("insert into entities (id, user) values ('local-e', 'u1')")
	if p := entity_peer("local-e"); p != "selfpeer" {
		t.Errorf("local entity must resolve to self; got %q", p)
	}
	if ps := entity_peers("local-e"); len(ps) != 1 || ps[0] != "selfpeer" {
		t.Errorf("local entity must resolve to [self]; got %v", ps)
	}

	// Aged-out peers (older than 30 days) are not returned.
	dir.exec("insert into entries (entity, peer, name, class, version, created, seen) values ('e2', 'oldpeer', 'n', 'person', 1, 1, ?)", now_ts-31*86400)
	if ps := entity_peers("e2"); len(ps) != 0 {
		t.Errorf("aged-out peers must not be returned; got %v", ps)
	}
}

func TestLeaderClaimFromVacant(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	if !replication_leader_claim("user:u1", "k1", false) {
		t.Errorf("first claim on a vacant lease must succeed")
	}

	db := db_open("db/replication.db")
	row, _ := db.row("select peer, fence from leadership where scope='user:u1' and key='k1'")
	if row == nil {
		t.Fatal("lease row missing after claim")
	}
	if p, _ := row["peer"].(string); p != "self" {
		t.Errorf("expected peer='self', got %q", p)
	}
	if f, _ := row["fence"].(int64); f != 1 {
		t.Errorf("first lease fence should be 1, got %d", f)
	}
}

func TestLeaderClaimRenewalIncrementsFence(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	if !replication_leader_claim("platform", "tick", false) {
		t.Fatal("first claim failed")
	}
	if !replication_leader_claim("platform", "tick", false) {
		t.Fatal("renewal must succeed when we already hold the lease")
	}

	db := db_open("db/replication.db")
	row, _ := db.row("select fence from leadership where scope='platform' and key='tick'")
	if f, _ := row["fence"].(int64); f != 2 {
		t.Errorf("renewal must bump fence to 2, got %d", f)
	}
}

func TestLeaderClaimBlockedByActivePeer(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Someone else holds an active lease.
	db := db_open("db/replication.db")
	expires := now() + 60
	db.exec("insert into leadership (scope, key, peer, expires, fence) values ('user:u', 'job', 'other-peer', ?, 5)", expires)

	if replication_leader_claim("user:u", "job", false) {
		t.Errorf("must NOT claim while another peer holds an active lease")
	}

	row, _ := db.row("select peer, fence from leadership where scope='user:u' and key='job'")
	if p, _ := row["peer"].(string); p != "other-peer" {
		t.Errorf("active lease must not be overwritten; peer is now %q", p)
	}
	if f, _ := row["fence"].(int64); f != 5 {
		t.Errorf("blocked attempt must not bump fence; got %d", f)
	}
}

func TestLeaderClaimTakesOverExpired(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Other peer's lease has expired.
	db := db_open("db/replication.db")
	db.exec("insert into leadership (scope, key, peer, expires, fence) values ('user:u', 'job', 'other-peer', ?, 3)", now()-1)

	if !replication_leader_claim("user:u", "job", false) {
		t.Errorf("must claim an expired lease")
	}

	row, _ := db.row("select peer, fence from leadership where scope='user:u' and key='job'")
	if p, _ := row["peer"].(string); p != "self" {
		t.Errorf("expected peer='self' after takeover, got %q", p)
	}
	if f, _ := row["fence"].(int64); f != 4 {
		t.Errorf("takeover must bump fence (was 3, expected 4); got %d", f)
	}
}

func TestLeaderFenceAndRelease(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	replication_leader_claim("scope", "key", false)
	if f := replication_leader_fence("scope", "key"); f != 1 {
		t.Errorf("fence after first claim: expected 1, got %d", f)
	}

	replication_leader_release("scope", "key")
	if f := replication_leader_fence("scope", "key"); f != 0 {
		t.Errorf("fence after release must be 0, got %d", f)
	}

	// Re-claim after release succeeds and starts a fresh fence.
	if !replication_leader_claim("scope", "key", false) {
		t.Errorf("re-claim after release must succeed")
	}
}

// ----------------------------------------------------------------------
// Cross-host election RPC tests (Stage 22 follow-up).
// net_id in the test stub is "self"; alphabetic order: "aaa" < "self" < "zzz".
// ----------------------------------------------------------------------

func TestLeaderVoteVacantGrants(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	granted, leader, fence, exp := replication_leader_vote("platform", "k1", "any-proposer", now()+60)
	if !granted {
		t.Errorf("vacant row must grant any proposer")
	}
	if leader != "" || fence != 0 || exp != 0 {
		t.Errorf("vacant vote should report empty state, got leader=%q fence=%d exp=%d", leader, fence, exp)
	}
}

func TestLeaderVoteRenewalGrants(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec("insert into leadership (scope, key, peer, expires, fence) values ('platform', 'k1', 'p-aaa', ?, 5)", now()+60)

	granted, leader, fence, _ := replication_leader_vote("platform", "k1", "p-aaa", now()+60)
	if !granted {
		t.Errorf("renewal by current holder must grant")
	}
	if leader != "p-aaa" || fence != 5 {
		t.Errorf("renewal response must report current state (leader=%q fence=%d)", leader, fence)
	}
}

// TestLeaderVoteHashTieBreak — vote agrees with the hash tie-break
// (sha256(scope|key|peer) lowest wins), and an expired lease grants
// any proposer regardless of hash.
func TestLeaderVoteHashTieBreak(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	// We (self) hold an alive lease.
	db.exec("insert into leadership (scope, key, peer, expires, fence) values ('platform', 'k1', ?, ?, 1)", net_id, now()+60)

	// For each candidate, the vote outcome must equal the prefer
	// helper's decision (the source of truth for the tie-break).
	for _, candidate := range []string{"aaa", "zzz", "peerA", "peer_b", "peerC"} {
		want := replication_leader_prefer("platform", "k1", candidate, net_id)
		got, _, _, _ := replication_leader_vote("platform", "k1", candidate, now()+60)
		if got != want {
			t.Errorf("candidate %q: vote=%v, expected %v from hash tie-break",
				candidate, got, want)
		}
	}

	// Expired lease grants every proposer regardless of hash.
	db.exec("update leadership set expires=? where scope='platform' and key='k1'", now()-1)
	if g, _, _, _ := replication_leader_vote("platform", "k1", "zzz", now()+60); !g {
		t.Errorf("expired lease must grant any proposer")
	}
}

// TestLeaderHashDistribution — across many keys, the hash tie-break
// spreads winners across the peerset rather than concentrating on one
// peer. Guards against the V2 lex regression where the lowest-id host
// always won.
func TestLeaderHashDistribution(t *testing.T) {
	candidates := []string{"peer-alpha", "peer-bravo", "peer-charlie", "peer-delta"}
	wins := map[string]int{}
	const keys = 200
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("k%d", i)
		winner := candidates[0]
		for _, c := range candidates[1:] {
			if replication_leader_prefer("platform", key, c, winner) {
				winner = c
			}
		}
		wins[winner]++
	}
	// Uniform-ish: each candidate should win at least 10% of keys
	// (uniform would be 25%; 10% guards against pathological).
	for _, c := range candidates {
		if wins[c] < keys/10 {
			t.Errorf("candidate %q won %d / %d keys; hash tie-break not distributing (wins map: %v)",
				c, wins[c], keys, wins)
		}
	}
}

// TestLeaderPreferDeterministic — the same inputs produce the same
// preference every call; (a, b) and (b, a) are mirror outcomes.
func TestLeaderPreferDeterministic(t *testing.T) {
	for _, scope := range []string{"platform", "user:abc"} {
		for _, key := range []string{"k1", "k2", "long-key-name"} {
			forward := replication_leader_prefer(scope, key, "peer-a", "peer-b")
			if forward != replication_leader_prefer(scope, key, "peer-a", "peer-b") {
				t.Errorf("not deterministic for (%q, %q, peer-a, peer-b)", scope, key)
			}
			reverse := replication_leader_prefer(scope, key, "peer-b", "peer-a")
			if forward == reverse {
				t.Errorf("(a,b) and (b,a) must be mirror outcomes for (%q, %q); both returned %v", scope, key, forward)
			}
		}
	}
}

func TestLeaderMembershipFromScopePrefix(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added) values ('pair-peer', ?)", now())
	db.exec("insert into pair (peer, added) values (?, ?)", net_id, now()) // self should be filtered
	db.exec("insert into hosts (user, peer, added) values ('user-x', 'host-peer', ?)", now())
	db.exec("insert into hosts (user, peer, added) values ('user-x', ?, ?)", net_id, now()) // self filtered

	if m := replication_leader_membership("user:user-x"); len(m) != 1 || m[0] != "host-peer" {
		t.Errorf("user-scoped membership: got %v, want [host-peer]", m)
	}
	if m := replication_leader_membership("user:unknown"); len(m) != 0 {
		t.Errorf("unknown user membership: got %v, want empty", m)
	}
	if m := replication_leader_membership("platform"); len(m) != 1 || m[0] != "pair-peer" {
		t.Errorf("platform membership: got %v, want [pair-peer]", m)
	}
	if m := replication_leader_membership("credential:abc"); len(m) != 1 || m[0] != "pair-peer" {
		t.Errorf("non-user-prefix scope must fall back to pair; got %v", m)
	}
}

func TestLeaderClaimRPCGrantedSucceeds(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	orig_rpc := replication_leader_claim_rpc
	orig_notify := replication_leader_notify
	defer func() {
		replication_leader_claim_rpc = orig_rpc
		replication_leader_notify = orig_notify
	}()
	notified := 0
	replication_leader_notify = func(scope, key string, fence, expires int64) { notified++ }
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		return &LeaderClaimResponse{Granted: true}
	}

	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added) values ('remote', ?)", now())

	if !replication_leader_claim("platform", "k1", false) {
		t.Fatalf("claim must succeed when remote grants")
	}
	if notified != 1 {
		t.Errorf("notify must fire exactly once on success, got %d", notified)
	}

	// Fast-path renewal on the next call — no fan-out, fence bumps to 2.
	calls := 0
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		calls++
		return &LeaderClaimResponse{Granted: true}
	}
	if !replication_leader_claim("platform", "k1", false) {
		t.Errorf("fast-path renewal must succeed")
	}
	if calls != 0 {
		t.Errorf("fast-path renewal must not fire RPC; got %d calls", calls)
	}
	if f := replication_leader_fence("platform", "k1"); f != 2 {
		t.Errorf("renewal must bump fence to 2, got %d", f)
	}
}

func TestLeaderClaimRPCDeniedMirrorsAndFails(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	orig_rpc := replication_leader_claim_rpc
	orig_notify := replication_leader_notify
	defer func() {
		replication_leader_claim_rpc = orig_rpc
		replication_leader_notify = orig_notify
	}()
	replication_leader_notify = func(scope, key string, fence, expires int64) {
		t.Errorf("notify must NOT fire on a denied claim")
	}
	deny_exp := now() + 60
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		return &LeaderClaimResponse{Granted: false, CurrentLeader: "remote-leader", CurrentFence: 7, CurrentExpires: deny_exp}
	}

	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added) values ('remote', ?)", now())

	if replication_leader_claim("platform", "k1", false) {
		t.Fatalf("claim must NOT succeed when remote denies with a current leader")
	}
	row, _ := db.row("select peer, fence, expires from leadership where scope='platform' and key='k1'")
	if p, _ := row["peer"].(string); p != "remote-leader" {
		t.Errorf("mirror failed: expected peer='remote-leader', got %q", p)
	}
	if f, _ := row["fence"].(int64); f != 7 {
		t.Errorf("mirror failed: expected fence=7, got %d", f)
	}

	// Fast-path-deny on the next call — no RPC fired.
	calls := 0
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		calls++
		return nil
	}
	if replication_leader_claim("platform", "k1", false) {
		t.Errorf("fast-path-deny expected after mirroring active peer lease")
	}
	if calls != 0 {
		t.Errorf("fast-path-deny must not fire RPC; got %d calls", calls)
	}
}

func TestLeaderClaimRPCPartitionFallbackSucceeds(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	orig_rpc := replication_leader_claim_rpc
	orig_notify := replication_leader_notify
	defer func() {
		replication_leader_claim_rpc = orig_rpc
		replication_leader_notify = orig_notify
	}()
	replication_leader_notify = func(scope, key string, fence, expires int64) {}
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		return nil // every peer unreachable
	}

	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added) values ('remote', ?)", now())

	if !replication_leader_claim("platform", "k1", false) {
		t.Errorf("partition fallback: claim must succeed when no peers respond")
	}
}

// TestLeaderClaimStrictRequiresMajority — strict mode demands more
// than half of (self + membership) to grant. Nil RPC responses
// (unreachable peers) count against the proposer.
func TestLeaderClaimStrictRequiresMajority(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	orig_rpc := replication_leader_claim_rpc
	orig_notify := replication_leader_notify
	defer func() {
		replication_leader_claim_rpc = orig_rpc
		replication_leader_notify = orig_notify
	}()
	replication_leader_notify = func(scope, key string, fence, expires int64) {}

	db := db_open("db/replication.db")

	// Three peers in the pair (total participants with self = 4;
	// strict majority needs ceil(5/2) = 3 grants total, so need 2
	// peer grants since self contributes 1).
	db.exec("insert into pair (peer, added) values ('peer-a', ?)", now())
	db.exec("insert into pair (peer, added) values ('peer-b', ?)", now())
	db.exec("insert into pair (peer, added) values ('peer-c', ?)", now())

	// Case: 2 peers grant, 1 unreachable - clears the threshold.
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		if peer == "peer-c" {
			return nil
		}
		return &LeaderClaimResponse{Granted: true}
	}
	if !replication_leader_claim("platform", "ka", true) {
		t.Errorf("strict: 2 of 3 peer grants must win (self + 2 = majority of 4)")
	}
	db.exec("delete from leadership")

	// Case: 1 peer grants, 2 unreachable - below threshold; strict
	// claim fails. Optimistic claim still wins (no veto).
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		if peer == "peer-a" {
			return &LeaderClaimResponse{Granted: true}
		}
		return nil
	}
	if replication_leader_claim("platform", "kb", true) {
		t.Errorf("strict: 1 of 3 peer grants must fail (self + 1 = 2, below majority of 4)")
	}
	db.exec("delete from leadership")
	if !replication_leader_claim("platform", "kb", false) {
		t.Errorf("optimistic: 1 of 3 peer grants (rest nil = no veto) must win")
	}
	db.exec("delete from leadership")

	// Case: zero peers in membership - strict trivially wins (just
	// self).
	db.exec("delete from pair")
	replication_leader_claim_rpc = func(peer, scope, key string, expires int64) *LeaderClaimResponse {
		t.Errorf("rpc must not be called when membership is empty")
		return nil
	}
	if !replication_leader_claim("platform", "kc", true) {
		t.Errorf("strict: single-host (no peers) must always win")
	}
}

func TestLeaderGrantedNoticeMirrors(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Build a synthetic Event for the granted handler.
	expires := now() + 60
	e := &Event{
		content: map[string]any{
			"scope":   "platform",
			"key":     "k1",
			"peer":    "remote-leader",
			"fence":   int64(9),
			"expires": expires,
		},
	}
	replica_leader_granted_event(e)

	db := db_open("db/replication.db")
	row, _ := db.row("select peer, fence, expires from leadership where scope='platform' and key='k1'")
	if row == nil {
		t.Fatal("granted handler must insert a leadership row")
	}
	if p, _ := row["peer"].(string); p != "remote-leader" {
		t.Errorf("mirrored peer: got %q, want 'remote-leader'", p)
	}
	if f, _ := row["fence"].(int64); f != 9 {
		t.Errorf("mirrored fence: got %d, want 9", f)
	}

	// A later notice with a higher fence overwrites.
	e.content["fence"] = int64(10)
	e.content["expires"] = expires + 30
	replica_leader_granted_event(e)
	row, _ = db.row("select fence from leadership where scope='platform' and key='k1'")
	if f, _ := row["fence"].(int64); f != 10 {
		t.Errorf("higher-fence notice must overwrite, got %d", f)
	}

	// A stale notice with an older fence/expires is ignored.
	e.content["fence"] = int64(8)
	e.content["expires"] = expires - 30
	replica_leader_granted_event(e)
	row, _ = db.row("select fence from leadership where scope='platform' and key='k1'")
	if f, _ := row["fence"].(int64); f != 10 {
		t.Errorf("stale notice must NOT overwrite (expected 10, got %d)", f)
	}
}

func TestBroadcastNextMonotonic(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/test.db")

	if n := broadcast_next_local(db, "votes", "peerA"); n != 1 {
		t.Errorf("first allocation: expected 1, got %d", n)
	}
	if n := broadcast_next_local(db, "votes", "peerA"); n != 2 {
		t.Errorf("second allocation: expected 2, got %d", n)
	}
	if n := broadcast_next_local(db, "votes", "peerA"); n != 3 {
		t.Errorf("third allocation: expected 3, got %d", n)
	}
}

func TestBroadcastNextSeparateByKeyAndPeer(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_separate")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/test.db")

	// Different keys have independent counters.
	if n := broadcast_next_local(db, "key1", "peerA"); n != 1 {
		t.Errorf("key1/peerA first: got %d", n)
	}
	if n := broadcast_next_local(db, "key2", "peerA"); n != 1 {
		t.Errorf("key2/peerA first (independent of key1): got %d", n)
	}
	// Different peers on the same key are also independent.
	if n := broadcast_next_local(db, "key1", "peer_b"); n != 1 {
		t.Errorf("key1/peer_b first (independent of peerA): got %d", n)
	}
	if n := broadcast_next_local(db, "key1", "peerA"); n != 2 {
		t.Errorf("key1/peerA second: got %d", n)
	}
}

func TestBroadcastReceivedAndAdvance(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_recv")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/test.db")

	// Before any messages, received returns 0.
	if n := broadcast_received_get(db, "senderA", "votes"); n != 0 {
		t.Errorf("empty state: expected 0, got %d", n)
	}

	// Advance recorded.
	broadcast_advance_local(db, "senderA", "votes", 5)
	if n := broadcast_received_get(db, "senderA", "votes"); n != 5 {
		t.Errorf("after advance to 5: got %d", n)
	}

	// Stale (lower) advance does NOT regress.
	broadcast_advance_local(db, "senderA", "votes", 3)
	if n := broadcast_received_get(db, "senderA", "votes"); n != 5 {
		t.Errorf("stale advance must not regress: got %d", n)
	}

	// Higher advance updates.
	broadcast_advance_local(db, "senderA", "votes", 10)
	if n := broadcast_received_get(db, "senderA", "votes"); n != 10 {
		t.Errorf("after advance to 10: got %d", n)
	}

	// Independent per (sender, key).
	if n := broadcast_received_get(db, "senderB", "votes"); n != 0 {
		t.Errorf("different sender unaffected: got %d", n)
	}
	if n := broadcast_received_get(db, "senderA", "comments"); n != 0 {
		t.Errorf("different key unaffected: got %d", n)
	}
}

func TestBroadcastGapDetection(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_bcast_gap")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/test.db")

	// Receive sequence 1, 2 — no gap.
	broadcast_advance_local(db, "s", "k", 1)
	broadcast_advance_local(db, "s", "k", 2)
	last := broadcast_received_get(db, "s", "k")
	if last != 2 {
		t.Fatalf("expected last=2, got %d", last)
	}

	// Sequence 5 arrives — gap of {3, 4} detected (app would request
	// replay; we just check the math).
	incoming := int64(5)
	gap := incoming > last+1
	if !gap {
		t.Errorf("gap should be detected when incoming > last+1")
	}

	// Sequence 3 arrives — no gap.
	last = broadcast_received_get(db, "s", "k")
	incoming = 3
	gap = incoming > last+1
	if gap {
		t.Errorf("gap should NOT be detected when incoming == last+1")
	}
}

func TestWebpushDedupBasic(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_webpush_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	os.MkdirAll(filepath.Join(tmp_dir, "users/uid-alice"), 0755)
	user := &User{UID: "uid-alice"}

	if webpush_already_delivered(user, "https://fcm.example/a", "evt-1") {
		t.Errorf("fresh state must not be marked delivered")
	}

	webpush_mark_delivered(user, "https://fcm.example/a", "evt-1")

	if !webpush_already_delivered(user, "https://fcm.example/a", "evt-1") {
		t.Errorf("after mark, must be delivered")
	}

	// Different event_id on same endpoint → not yet delivered.
	if webpush_already_delivered(user, "https://fcm.example/a", "evt-2") {
		t.Errorf("different event_id must not dedup")
	}

	// Different endpoint on same event_id → each subscription tracked
	// independently so all of a user's devices still get the push.
	if webpush_already_delivered(user, "https://fcm.example/b", "evt-1") {
		t.Errorf("different endpoint must not dedup (each device gets the push)")
	}
}

func TestWebpushDedupTTLExpires(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_webpush_ttl")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	os.MkdirAll(filepath.Join(tmp_dir, "users/uid-alice"), 0755)
	user := &User{UID: "uid-alice"}

	// Manually insert a row with a stale timestamp.
	db := webpush_dedup_db(user)
	stale := now() - webpush_dedup_ttl - 1
	db.exec("insert into webpush_delivered (endpoint, event_id, ts) values (?, ?, ?)", "https://fcm.example/x", "evt-old", stale)

	if webpush_already_delivered(user, "https://fcm.example/x", "evt-old") {
		t.Errorf("a row older than the TTL must not dedup")
	}

	// A fresh mark prunes the stale row as a side effect.
	webpush_mark_delivered(user, "https://fcm.example/x", "evt-new")
	count := db.integer("select count(*) from webpush_delivered where endpoint='https://fcm.example/x' and event_id='evt-old'")
	if count != 0 {
		t.Errorf("mark_delivered must prune stale rows; got %d remaining", count)
	}
}

// =====================================================================
// Two-host integration scenarios (task #11)
// =====================================================================
//
// These tests simulate two hosts in a single process by swapping
// data_dir and net_id between turns. Replication ops "travel" between
// hosts by being constructed once and applied via replication_apply_op
// (or its underlying helpers) under each host's context. The real Net
// transport is bypassed — its role is at-least-once delivery + dedup,
// both already covered by transport-level unit tests. What these
// scenarios prove is that the apply pipelines on different hosts
// converge to the same end state for the patterns the apps use.

// integration_setup mints two host contexts (data_dirs + p2p_ids) and
// returns a switch() function: switch("h1") runs subsequent code under
// host 1's context. Cleanup removes both temp dirs and restores the
// originals.
func integration_setup(t *testing.T) (func(string), func()) {
	t.Helper()
	dir1, err := os.MkdirTemp("", "mochi_int_h1")
	if err != nil {
		t.Fatalf("temp dir 1: %v", err)
	}
	dir2, err := os.MkdirTemp("", "mochi_int_h2")
	if err != nil {
		os.RemoveAll(dir1)
		t.Fatalf("temp dir 2: %v", err)
	}
	orig_data := data_dir
	orig_p2p := net_id
	// Replace the post-migration background drain with a no-op for the
	// duration of the test. The production goroutine reads data_dir
	// asynchronously, which races with switch_to's host swap; the drain
	// itself is a performance optimization that the test doesn't need.
	orig_drain := post_migration_drain_async
	post_migration_drain_async = func(user, app_id string) {}

	hosts := map[string]struct {
		dir string
		id  string
	}{
		"h1": {dir1, "peer1"},
		"h2": {dir2, "peer2"},
	}

	switch_to := func(name string) {
		h, ok := hosts[name]
		if !ok {
			t.Fatalf("unknown host %q", name)
		}
		data_dir = h.dir
		net_id = h.id
		// Lazy-create the per-host replication schema on first use.
		db_upgrade_50()
	}

	cleanup := func() {
		data_dir = orig_data
		net_id = orig_p2p
		post_migration_drain_async = orig_drain
		os.RemoveAll(dir1)
		os.RemoveAll(dir2)
	}
	return switch_to, cleanup
}

func TestIntegrationKeysTransferThenSessionInsert(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	// Host 1: alice exists locally and creates a session.
	switch_to("h1")
	setup_users_test_schema()
	setup_sessions_test_schema()
	udb1 := db_open("db/users.db")
	udb1.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")
	a := test_entity_id('a')
	udb1.exec("insert into entities (id, private, fingerprint, user, class, name, privacy) values (?, 'priv', 'fp1', 'uid-alice', 'person', 'Alice', 'private')", a)

	// Host 2: pre-seed alice so the keys-transfer falls into the "user
	// already exists" branch and matches the canonical uid the wire op
	// expects.
	switch_to("h2")
	setup_users_test_schema()
	setup_sessions_test_schema()
	udb2 := db_open("db/users.db")
	udb2.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")
	kt := &KeysTransfer{
		Username: "alice@example.com",
		Role:     "user",
		Methods:  "email",
		Status:   "active",
		Entities: []KeysEntity{
			{ID: a, Private: "priv", Fingerprint: "fp1", Class: "person", Name: "Alice"},
		},
	}
	if n := replication_keys_transfer_apply(a, "peer1", kt); n != 1 {
		t.Fatalf("keys-transfer on h2: expected 1 entity inserted, got %d", n)
	}

	// Host 2 receives a session-insert op from Host 1.
	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-alice",
		Database: "sessions", Table: "sessions", Operation: repl_op_insert,
		Sequence: 1,
		Payload: cbor_encode(&SessionInsert{
			UserUID: "uid-alice", Code: "sess-x", Secret: "s",
			Expires: 100, Created: 50, Accessed: 50,
			Address: "1.2.3.4", Agent: "test",
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("session apply on h2: expected ApplyApplied, got %v", got)
	}

	sdb := db_open("db/sessions.db")
	count := sdb.integer("select count(*) from sessions where code='sess-x'")
	if count != 1 {
		t.Errorf("expected session present on h2; got %d rows", count)
	}
}

func TestEmailDedupBasic(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_email_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	os.MkdirAll(filepath.Join(tmp_dir, "users/uid-alice"), 0755)
	user := &User{UID: "uid-alice"}

	if email_already_delivered(user, "alice@example.com", "evt-1") {
		t.Errorf("fresh state must not be marked delivered")
	}

	email_mark_delivered(user, "alice@example.com", "evt-1")

	if !email_already_delivered(user, "alice@example.com", "evt-1") {
		t.Errorf("after mark, must be delivered")
	}

	// Different event_id same address — not yet delivered.
	if email_already_delivered(user, "alice@example.com", "evt-2") {
		t.Errorf("different event_id must not dedup")
	}

	// Different address same event_id — each address tracked independently
	// so if a user has multiple addresses they all get emailed.
	if email_already_delivered(user, "alice-work@example.com", "evt-1") {
		t.Errorf("different address must not dedup")
	}
}

func TestEmailDedupTTLExpires(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_email_ttl")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	os.MkdirAll(filepath.Join(tmp_dir, "users/uid-alice"), 0755)
	user := &User{UID: "uid-alice"}

	db := email_dedup_db(user)
	stale := now() - email_dedup_ttl - 1
	db.exec("insert into email_delivered (address, event_id, ts) values (?, ?, ?)", "bob@example.com", "evt-old", stale)

	if email_already_delivered(user, "bob@example.com", "evt-old") {
		t.Errorf("rows older than TTL must not dedup")
	}

	email_mark_delivered(user, "bob@example.com", "evt-new")
	count := db.integer("select count(*) from email_delivered where address='bob@example.com' and event_id='evt-old'")
	if count != 0 {
		t.Errorf("mark_delivered must prune stale rows; got %d remaining", count)
	}
}

func TestFenceObserveFreshAccepts(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	if !replication_fence_observe("credential:abc", "fence", "peerA", 1) {
		t.Errorf("first observation should be accepted")
	}
	fence, peer := replication_fence_current("credential:abc", "fence")
	if fence != 1 || peer != "peerA" {
		t.Errorf("current after first observe: expected (1, peerA), got (%d, %q)", fence, peer)
	}
}

func TestFenceObserveStaleRejects(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	replication_fence_observe("credential:x", "fence", "peerA", 5)
	// Stale fence from another peer — must be rejected.
	if replication_fence_observe("credential:x", "fence", "peer_b", 3) {
		t.Errorf("stale fence (3 < 5) must be rejected")
	}
	// Witness must not have been overwritten.
	fence, peer := replication_fence_current("credential:x", "fence")
	if fence != 5 || peer != "peerA" {
		t.Errorf("rejected stale must not overwrite witness: got (%d, %q)", fence, peer)
	}
}

func TestFenceObserveNewerWins(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	replication_fence_observe("scope", "key", "peerA", 3)
	if !replication_fence_observe("scope", "key", "peer_b", 7) {
		t.Errorf("newer fence (7 > 3) must be accepted")
	}
	fence, peer := replication_fence_current("scope", "key")
	if fence != 7 || peer != "peer_b" {
		t.Errorf("after newer observe: expected (7, peer_b), got (%d, %q)", fence, peer)
	}
}

func TestFenceObserveEqualAccepts(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	replication_fence_observe("scope", "key", "peerA", 5)
	// Equal fence from the same peer (e.g. retry of the same op) is accepted.
	if !replication_fence_observe("scope", "key", "peerA", 5) {
		t.Errorf("equal fence retry from same peer must be accepted")
	}
}

func TestFenceObserveNoOpForUnstamped(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Empty scope/key or fence<=0 means "no leader info", which passes
	// through unconditionally so non-leader ops aren't blocked.
	if !replication_fence_observe("", "", "peerA", 5) {
		t.Errorf("unstamped op (empty scope) must pass")
	}
	if !replication_fence_observe("s", "k", "peerA", 0) {
		t.Errorf("unstamped op (fence=0) must pass")
	}
	if !replication_fence_observe("s", "k", "peerA", -1) {
		t.Errorf("unstamped op (fence<0) must pass")
	}
}

func TestFenceCurrentEmptyState(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// No observations yet — current returns (0, "").
	fence, peer := replication_fence_current("nope", "nada")
	if fence != 0 || peer != "" {
		t.Errorf("empty state: expected (0, \"\"), got (%d, %q)", fence, peer)
	}
}

func TestReplicationHealthEndpoint(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	// Populate replication.db with representative state.
	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added) values ('peerA', ?)", now())
	db.exec("insert into pair (peer, added) values ('peer_b', ?)", now())
	db.exec("insert into hosts (user, peer, added) values ('uid-a', 'peerX', ?)", now())
	db.exec("insert into hosts (user, peer, added) values ('uid-a', 'peerY', ?)", now())
	db.exec("insert into hosts (user, peer, added) values ('uid-b', 'peerX', ?)", now())
	db.exec("insert into pending (peer, scope, user, sequence, schema, payload, received) values ('peerX', 'app', 'uid-c', 1, 0, ?, ?)",
		[]byte{0xa0}, now()-100)
	db.exec("insert into seen (peer, scope, user, sequence, applied) values ('peerX', 'app', 'uid-a', 1, ?)", now())
	db.exec("insert into leadership (scope, key, peer, expires, fence) values ('user:uid-a', 'tick', 'self', ?, 1)", now()+60)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/_/replication/health", web_replication_health)

	req := httptest.NewRequest("GET", "/_/replication/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}

	if peer, _ := resp["peer_id"].(string); peer != "self" {
		t.Errorf("peer_id: expected 'self', got %q", peer)
	}

	pair_list, _ := resp["pair"].([]any)
	if len(pair_list) != 2 {
		t.Errorf("pair: expected 2 entries, got %d (%v)", len(pair_list), pair_list)
	}

	if h, _ := resp["hosts"].(float64); int(h) != 3 {
		t.Errorf("hosts: expected 3, got %v", resp["hosts"])
	}
	if u, _ := resp["users_with_hosts"].(float64); int(u) != 2 {
		t.Errorf("users_with_hosts: expected 2, got %v", resp["users_with_hosts"])
	}
	if p, _ := resp["pending"].(float64); int(p) != 1 {
		t.Errorf("pending: expected 1, got %v", resp["pending"])
	}
	if age, _ := resp["pending_oldest_age"].(float64); age < 50 {
		t.Errorf("pending_oldest_age: expected >= 50, got %v", age)
	}
	if s, _ := resp["seen_total"].(float64); int(s) != 1 {
		t.Errorf("seen_total: expected 1, got %v", resp["seen_total"])
	}
	if l, _ := resp["leases_held"].(float64); int(l) != 1 {
		t.Errorf("leases_held: expected 1, got %v", resp["leases_held"])
	}
}

func TestIntegrationFenceWitnessLifecycle(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	// Host 1 claims the lease for (scope, key) — fence=1.
	switch_to("h1")
	if !replication_leader_claim("user:u", "tick", false) {
		t.Fatal("h1 lease claim failed")
	}
	h1_fence := replication_leader_fence("user:u", "tick")
	if h1_fence != 1 {
		t.Fatalf("h1 fence after first claim: expected 1, got %d", h1_fence)
	}

	// Host 2 sees an op from h1 stamped with fence=1 — accepts and
	// records the witness.
	switch_to("h2")
	if !replication_fence_observe("user:u", "tick", "peer1", h1_fence) {
		t.Fatal("h2 must accept the fresh fence-1 op from peer1")
	}
	witnessed, peer := replication_fence_current("user:u", "tick")
	if witnessed != 1 || peer != "peer1" {
		t.Errorf("h2 witness after first observe: expected (1, peer1), got (%d, %q)", witnessed, peer)
	}

	// Unstamped ops still pass (non-leader patterns like LWW / counter).
	if !replication_fence_observe("user:u", "tick", "peer3", 0) {
		t.Errorf("h2 must accept fence=0 (unstamped op)")
	}

	// Host 3 takes over (hypothetically; cross-host claim coordination
	// isn't built in V1 so we just simulate by observing a higher fence
	// from another peer). h2 observes peer3's fence=2 — wins.
	if !replication_fence_observe("user:u", "tick", "peer3", 2) {
		t.Fatal("h2 must accept newer fence=2 from peer3")
	}
	witnessed, peer = replication_fence_current("user:u", "tick")
	if witnessed != 2 || peer != "peer3" {
		t.Errorf("h2 witness after takeover observe: expected (2, peer3), got (%d, %q)", witnessed, peer)
	}

	// h2 receives a delayed op from peer1 still stamped with fence=1 —
	// rejected because the witness has moved on to 2.
	if replication_fence_observe("user:u", "tick", "peer1", 1) {
		t.Error("h2 must reject stale fence=1 after the witness moved to 2")
	}

	// peer3 emitting again with the same fence=2 is fine (renewal /
	// retry of the same lease, not a regression).
	if !replication_fence_observe("user:u", "tick", "peer3", 2) {
		t.Errorf("h2 must accept fence=2 retry from the same peer")
	}
}

func TestIntegrationWebpushDedupReplicates(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	// Host 1: alice's webpush_mark_delivered records the row locally.
	switch_to("h1")
	setup_users_test_schema()
	udb1 := db_open("db/users.db")
	udb1.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")
	os.MkdirAll(filepath.Join(data_dir, "users/uid-alice"), 0755)

	u1 := &User{UID: "uid-alice", Username: "alice@example.com"}
	webpush_mark_delivered(u1, "https://fcm.example/a", "evt-1")
	if !webpush_already_delivered(u1, "https://fcm.example/a", "evt-1") {
		t.Fatal("h1 mark didn't take")
	}

	// Host 2: alice is local too (keys-transfer landed). Apply the
	// replicated webpush_delivered op directly via the apply path.
	switch_to("h2")
	setup_users_test_schema()
	udb2 := db_open("db/users.db")
	udb2.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")
	os.MkdirAll(filepath.Join(data_dir, "users/uid-alice"), 0755)

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      "uid-alice",
		Database:  "notifications",
		Table:     "webpush_delivered",
		Operation: repl_op_insert,
		Sequence:  1,
		Payload: cbor_encode(&WebpushDelivered{
			Endpoint: "https://fcm.example/a", EventID: "evt-1", TS: now(),
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("h2 apply: expected ApplyApplied, got %v", got)
	}

	u2 := &User{UID: "uid-alice"}
	if !webpush_already_delivered(u2, "https://fcm.example/a", "evt-1") {
		t.Error("h2 must see the replicated webpush_delivered row")
	}
	// Different endpoint isn't deduped — each subscription tracked
	// independently, replication preserves that.
	if webpush_already_delivered(u2, "https://fcm.example/b", "evt-1") {
		t.Error("different endpoint must not be deduped from one replicate")
	}
}

func TestIntegrationEmailDedupReplicates(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	switch_to("h1")
	setup_users_test_schema()
	udb1 := db_open("db/users.db")
	udb1.exec("insert into users (uid, username) values (?, ?)", "uid-bob", "bob@example.com")
	os.MkdirAll(filepath.Join(data_dir, "users/uid-alice"), 0755)
	u1 := &User{UID: "uid-bob"}
	email_mark_delivered(u1, "bob@example.com", "login:abc")

	switch_to("h2")
	setup_users_test_schema()
	udb2 := db_open("db/users.db")
	udb2.exec("insert into users (uid, username) values (?, ?)", "uid-bob", "bob@example.com")
	os.MkdirAll(filepath.Join(data_dir, "users/uid-alice"), 0755)

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      "uid-bob",
		Database:  "notifications",
		Table:     "email_delivered",
		Operation: repl_op_insert,
		Sequence:  1,
		Payload: cbor_encode(&EmailDelivered{
			Address: "bob@example.com", EventID: "login:abc", TS: now(),
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("h2 apply: expected ApplyApplied, got %v", got)
	}

	u2 := &User{UID: "uid-bob"}
	if !email_already_delivered(u2, "bob@example.com", "login:abc") {
		t.Error("h2 must see the replicated email_delivered row")
	}
}

// ============================================================
// Cross-host integration tests (in-process two-host harness)
// (was replication_integration_test.go)
// ============================================================

// install_test_app registers a fake "myapp" with the schema used by
// the SQL command apply path. Returns a cleanup that removes the
// registration when the test finishes.
func install_test_app(t *testing.T) (cleanup func()) {
	t.Helper()
	av := &AppVersion{Version: "1"}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	av.Database.File = "myapp.db"
	av.Database.Schema = 1
	av.Database.create_function = func(db *DB) {
		db.exec(`create table posts (id text primary key, title text not null)`)
	}
	a := &App{id: "myapp", versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = a
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps["myapp"] = a
	apps_lock.Unlock()
	return func() {
		apps_lock.Lock()
		delete(apps, "myapp")
		apps_lock.Unlock()
	}
}

func TestIntegrationSQLCommandAcrossHosts(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()
	defer install_test_app(t)()

	// h1: register user, create the app DB by doing a local write.
	switch_to("h1")
	setup_users_test_schema()
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")
	a := app_by_id("myapp")
	u := &User{UID: "uid-alice"}
	db_app(u, a).exec("insert into posts (id, title) values ('p1', 'From h1')")

	// The op h1 would emit.
	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-alice",
		Database: "myapp", Operation: repl_op_exec, Schema: 1, Sequence: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"p1", "From h1"},
		}),
	}

	// h2: register the user, apply, verify the row landed.
	switch_to("h2")
	setup_users_test_schema()
	udb = db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")

	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("h2 apply: want ApplyApplied, got %v", got)
	}
	got, _ := db_app(&User{UID: "uid-alice"}, app_by_id("myapp")).row(
		"select title from posts where id='p1'")
	if got == nil {
		t.Fatal("h2 row missing after apply")
	}
	if v, _ := got["title"].(string); v != "From h1" {
		t.Errorf("h2 title: want 'From h1', got %q", v)
	}
}

// TestIntegrationUsersUsersRoleAcrossHosts verifies that role
// propagation between paired hosts goes via the pair-only system-row
// path (not the per-user users-row.set path). Role replicates between
// the same operator's paired hosts but not across per-user link
// partners - admin authority is per-operator.
func TestIntegrationUsersUsersRoleAcrossHosts(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	switch_to("h2")
	setup_users_test_schema()
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, role) values (?, ?, 'user')", "uid-alice", "alice@example.com")

	replication_system_row_apply("h1", &SystemRow{
		Database: "users", Table: "users",
		Key:  map[string]string{"uid": "uid-alice"},
		Cols: map[string]string{"role": "administrator"},
	})
	row, _ := udb.row("select role from users where uid=?", "uid-alice")
	if v, _ := row["role"].(string); v != "administrator" {
		t.Errorf("role: want administrator, got %q", v)
	}
}

// TestIntegrationUsersUsersRoleNotOnPerUserPath defends the
// other side of the rule: a role op arriving on the per-user
// users-row.set pipeline (e.g. a misbehaving per-user link partner) is
// silently dropped. Protects against cross-operator privilege
// escalation.
func TestIntegrationUsersUsersRoleNotOnPerUserPath(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	switch_to("h2")
	setup_users_test_schema()
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username, role) values (?, ?, 'user')", "uid-alice", "alice@example.com")

	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-alice",
		Database: "users", Operation: "users-row.set", Sequence: 1,
		Payload: cbor_encode(&UsersRow{
			Table: "users",
			Cols:  map[string]string{"role": "administrator"},
		}),
	}
	// The apply path returns ApplyInvalid for an op whose only column
	// is outside the per-user whitelist - the dispatcher logs and
	// drops without touching the row.
	_ = replication_apply_op(op)
	row, _ := udb.row("select role from users where uid=?", "uid-alice")
	if v, _ := row["role"].(string); v == "administrator" {
		t.Error("role MUST NOT escalate via the per-user replication path")
	}
}

func TestIntegrationUsersEntitiesCreateAcrossHosts(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	switch_to("h2")
	setup_users_test_schema()
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")

	entity_id := test_entity_id('z')
	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-alice",
		Database: "users", Operation: "users-row.set", Sequence: 1,
		Payload: cbor_encode(&UsersRow{
			Table: "entities",
			Cols: map[string]string{
				"id":          entity_id,
				"private":     "priv-bytes",
				"fingerprint": "fp-xyz",
				"parent":      "",
				"class":       "feed",
				"name":        "Alice's Feed",
				"privacy":     "public",
				"data":        "",
				"published":   "0",
			},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}
	row, _ := udb.row("select user, class, name from entities where id=?", entity_id)
	if row == nil {
		t.Fatal("entity row missing after apply")
	}
	if v, _ := row["user"].(string); v != "uid-alice" {
		t.Errorf("user FK: want uid-alice, got %q", v)
	}
	if v, _ := row["name"].(string); v != "Alice's Feed" {
		t.Errorf("name: want 'Alice's Feed', got %q", v)
	}
}

func TestIntegrationUsersEntitiesUpdateAcrossHosts(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	switch_to("h2")
	setup_users_test_schema()
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")
	id := test_entity_id('y')
	udb.exec("insert into entities (id, private, fingerprint, user, class, name, privacy) values (?, 'p', 'fp', 'uid-alice', 'feed', 'Orig', 'public')", id)

	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-alice",
		Database: "users", Operation: "users-row.set", Sequence: 1,
		Payload: cbor_encode(&UsersRow{
			Table: "entities",
			Cols:  map[string]string{"id": id, "name": "Renamed", "privacy": "private"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}
	row, _ := udb.row("select name, privacy from entities where id=?", id)
	if v, _ := row["name"].(string); v != "Renamed" {
		t.Errorf("name: want Renamed, got %q", v)
	}
	if v, _ := row["privacy"].(string); v != "private" {
		t.Errorf("privacy: want private, got %q", v)
	}
}

func TestIntegrationUsersEntitiesDeleteAcrossHosts(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	switch_to("h2")
	setup_users_test_schema()
	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values (?, ?)", "uid-alice", "alice@example.com")
	id := test_entity_id('x')
	udb.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, 'p', 'fp', 'uid-alice', 'feed', 'Doomed')", id)

	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-alice",
		Database: "users", Operation: "users-row.delete", Sequence: 1,
		Payload: cbor_encode(&UsersRow{
			Table:  "entities",
			Cols:   map[string]string{"id": id},
			Delete: true,
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}
	if exists, _ := udb.exists("select 1 from entities where id=?", id); exists {
		t.Error("entity row must be removed on h2")
	}
}

// ============================================================
// mochi.replication.* Starlark API tests
// ============================================================

// TestApiReplicationStatusEmpty: with no pair / no hosts / no pending
// requests, status returns zeros for the counts and the local peer-id.
func TestApiReplicationStatusEmpty(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	thread := &sl.Thread{}
	v, err := api_replication_status(thread, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("api_replication_status error: %v", err)
	}
	d, ok := v.(*sl.Dict)
	if !ok {
		t.Fatalf("result is not a dict: %T", v)
	}

	peer, _, _ := d.Get(sl.String("peer"))
	if s, _ := peer.(sl.String); string(s) != "self" {
		t.Errorf("peer = %v, want self", peer)
	}

	for _, key := range []string{"hosts_count", "links_pending", "joins_pending"} {
		v, _, _ := d.Get(sl.String(key))
		n, ok := v.(sl.Int)
		if !ok {
			t.Errorf("%s is not an Int: %T", key, v)
			continue
		}
		count, _ := n.Int64()
		if count != 0 {
			t.Errorf("%s = %d, want 0", key, count)
		}
	}
}

// TestApiReplicationStatusPopulated: rows in each table reflect in the
// returned dict.
func TestApiReplicationStatusPopulated(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u1', 'peer-X', 0, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u2', 'peer-Y', 0, 0)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u1', 'peer-K', '', 'ph-1', 0, 9999999999)")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-J', '', 0, 9999999999)")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-K', '', 0, 9999999999)")

	thread := &sl.Thread{}
	v, err := api_replication_status(thread, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("api_replication_status error: %v", err)
	}
	d := v.(*sl.Dict)

	pair_value, _, _ := d.Get(sl.String("pair"))
	pair_list, ok := pair_value.(*sl.List)
	if !ok {
		t.Fatalf("pair is not a list: %T", pair_value)
	}
	if pair_list.Len() != 2 {
		t.Errorf("pair list len = %d, want 2", pair_list.Len())
	}

	want := map[string]int64{
		"hosts_count":   2,
		"links_pending": 1,
		"joins_pending": 2,
	}
	for k, expected := range want {
		v, _, _ := d.Get(sl.String(k))
		n, _ := v.(sl.Int).Int64()
		if n != expected {
			t.Errorf("%s = %d, want %d", k, n, expected)
		}
	}
}

// with_user_thread runs fn with t.Local("user") set to u.
func with_user_thread(u *User, fn func(*sl.Thread)) {
	th := &sl.Thread{}
	th.SetLocal("user", u)
	fn(th)
}

// TestApiReplicationBootstrapProgress: returns per-(peer, scope) rows;
// peer-filtered arg narrows to one peer.
func TestApiReplicationBootstrapProgress(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	bootstrap_set_state("files", "peer-A", "done", "")
	bootstrap_set_state("apps", "peer-A", "active", "12")
	bootstrap_set_state("files", "peer-B", "queued", "")

	th := &sl.Thread{}

	// No filter — every row.
	v, err := api_replication_bootstrap_progress(th, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	all := v.(*sl.List)
	if all.Len() != 3 {
		t.Errorf("all rows = %d, want 3", all.Len())
	}

	// Filtered to peer-A.
	v, err = api_replication_bootstrap_progress(th, sl.NewBuiltin("bootstrap_progress", api_replication_bootstrap_progress), sl.Tuple{sl.String("peer-A")}, nil)
	if err != nil {
		t.Fatalf("progress filtered: %v", err)
	}
	filtered := v.(*sl.List)
	if filtered.Len() != 2 {
		t.Errorf("peer-A rows = %d, want 2", filtered.Len())
	}

	// Check entry shape — iterate the first row and look up keys.
	it := filtered.Iterate()
	defer it.Done()
	var first sl.Value
	if !it.Next(&first) {
		t.Fatalf("filtered list is empty")
	}
	d := first.(*sl.Dict)
	for _, key := range []string{"peer", "scope", "state", "position"} {
		v, ok, _ := d.Get(sl.String(key))
		if !ok {
			t.Errorf("entry missing key %q", key)
			continue
		}
		if _, ok := v.(sl.String); !ok {
			t.Errorf("entry %q value is not a string: %T", key, v)
		}
	}
}

// TestApiReplicationLinksAndHosts: per-user link/host queries scope to
// the calling user. Inserts rows for two users and asserts the API
// returns only the calling user's rows.
func TestApiReplicationLinksAndHosts(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-alice', 'peer-A', 'a.example', 'ph-1', 0, 9999999999)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-alice', 'peer-B', 'b.example', 'ph-2', 0, 9999999999)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-bob', 'peer-Z', 'z.example', 'ph-9', 0, 9999999999)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-alice', 'peer-A', 100, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-bob', 'peer-Z', 200, 1)")

	alice := &User{UID: "u-alice"}
	with_user_thread(alice, func(th *sl.Thread) {
		v, err := api_replication_links(th, nil, sl.Tuple{}, nil)
		if err != nil {
			t.Fatalf("links: %v", err)
		}
		links := v.(*sl.List)
		if links.Len() != 2 {
			t.Errorf("links len = %d, want 2 (alice has 2 pending)", links.Len())
		}

		v, err = api_replication_hosts(th, nil, sl.Tuple{}, nil)
		if err != nil {
			t.Fatalf("hosts: %v", err)
		}
		hosts := v.(*sl.List)
		if hosts.Len() != 1 {
			t.Errorf("hosts len = %d, want 1 (alice has 1 host)", hosts.Len())
		}
	})

	// No user — both APIs should error.
	th := &sl.Thread{}
	if _, err := api_replication_links(th, sl.NewBuiltin("links", api_replication_links), sl.Tuple{}, nil); err == nil {
		t.Error("links: expected error for no user")
	}
	if _, err := api_replication_hosts(th, sl.NewBuiltin("hosts", api_replication_hosts), sl.Tuple{}, nil); err == nil {
		t.Error("hosts: expected error for no user")
	}
}

// TestApiReplicationLinkDeny: deny removes the link row for the calling
// user but leaves rows for other users untouched.
func TestApiReplicationLinkDeny(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-alice', 'peer-A', '', 'ph-1', 0, 9999999999)")
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values ('u-bob', 'peer-A', '', 'ph-2', 0, 9999999999)")

	alice := &User{UID: "u-alice"}
	with_user_thread(alice, func(th *sl.Thread) {
		v, err := api_replication_link_deny(th, sl.NewBuiltin("link_deny", api_replication_link_deny), sl.Tuple{sl.String("peer-A")}, nil)
		if err != nil {
			t.Fatalf("link_deny: %v", err)
		}
		if s, _ := v.(sl.String); string(s) != "denied" {
			t.Errorf("link_deny first call = %v, want denied", v)
		}

		// Idempotent: second call returns already-handled.
		v, _ = api_replication_link_deny(th, sl.NewBuiltin("link_deny", api_replication_link_deny), sl.Tuple{sl.String("peer-A")}, nil)
		if s, _ := v.(sl.String); string(s) != "already-handled" {
			t.Errorf("link_deny repeat = %v, want already-handled", v)
		}
	})

	// Bob's row must be untouched.
	exists, _ := rdb.exists("select 1 from links where user='u-bob' and peer='peer-A'")
	if !exists {
		t.Error("bob's link row was incorrectly removed by alice's deny")
	}
}

// TestApiReplicationJoinsAndDeny: joins() lists all pending whole-server
// join-requests (no user filter; system-wide). join_deny removes one
// row idempotently.
func TestApiReplicationJoinsAndDeny(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	orig_emit_join_denied := replication_emit_join_denied
	replication_emit_join_denied = func(peer, reason string) {}
	defer func() { replication_emit_join_denied = orig_emit_join_denied }()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-A', 'a.example', 0, 9999999999)")
	rdb.exec("insert into joins (peer, label, received, expires) values ('peer-B', 'b.example', 1, 9999999999)")

	th := &sl.Thread{}
	v, err := api_replication_joins(th, nil, sl.Tuple{}, nil)
	if err != nil {
		t.Fatalf("joins: %v", err)
	}
	joins := v.(*sl.List)
	if joins.Len() != 2 {
		t.Errorf("joins len = %d, want 2", joins.Len())
	}

	// Deny peer-A.
	v, err = api_replication_join_deny(th, sl.NewBuiltin("join_deny", api_replication_join_deny), sl.Tuple{sl.String("peer-A")}, nil)
	if err != nil {
		t.Fatalf("join_deny: %v", err)
	}
	if s, _ := v.(sl.String); string(s) != "denied" {
		t.Errorf("join_deny first call = %v, want denied", v)
	}

	// Idempotent.
	v, _ = api_replication_join_deny(th, sl.NewBuiltin("join_deny", api_replication_join_deny), sl.Tuple{sl.String("peer-A")}, nil)
	if s, _ := v.(sl.String); string(s) != "already-handled" {
		t.Errorf("join_deny repeat = %v, want already-handled", v)
	}

	// peer-B still pending.
	v, _ = api_replication_joins(th, nil, sl.Tuple{}, nil)
	if v.(*sl.List).Len() != 1 {
		t.Errorf("after denying A, joins len = %d, want 1", v.(*sl.List).Len())
	}
}

// TestApiReplicationPairRemove: removing a pair member drops the row
// and informs remaining members via the emit hook.
func TestApiReplicationPairRemove(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	emitted := false
	orig_emit := admin_replication_emit_pair_membership
	admin_replication_emit_pair_membership = func(full, recipients []string) { emitted = true }
	defer func() { admin_replication_emit_pair_membership = orig_emit }()

	rdb := db_open("db/replication.db")
	rdb.exec("insert into pair (peer, added, role) values ('peer-A', 0, '')")
	rdb.exec("insert into pair (peer, added, role) values ('peer-B', 0, '')")

	th := &sl.Thread{}
	v, err := api_replication_pair_remove(th, sl.NewBuiltin("pair_remove", api_replication_pair_remove), sl.Tuple{sl.String("peer-A")}, nil)
	if err != nil {
		t.Fatalf("pair_remove: %v", err)
	}
	if s, _ := v.(sl.String); string(s) != "removed" {
		t.Errorf("pair_remove = %v, want removed", v)
	}
	if !emitted {
		t.Error("admin_replication_emit_pair_membership was not called for remaining members")
	}

	// peer-B still present.
	if exists, _ := rdb.exists("select 1 from pair where peer='peer-B'"); !exists {
		t.Error("peer-B was incorrectly removed alongside peer-A")
	}

	// not-found path.
	v, _ = api_replication_pair_remove(th, sl.NewBuiltin("pair_remove", api_replication_pair_remove), sl.Tuple{sl.String("peer-unknown")}, nil)
	if s, _ := v.(sl.String); string(s) != "not-found" {
		t.Errorf("pair_remove unknown = %v, want not-found", v)
	}
}

// TestApiReplicationHostRemove: removing a host removes only the calling
// user's row, leaves other users untouched, and returns not-found when
// the peer wasn't a host.
func TestApiReplicationHostRemove(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (uid, username) values ('u-alice', 'alice')")
	udb.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-alice', '', 'fpa', 'u-alice', 'identity', 'Alice')")

	rdb := db_open("db/replication.db")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-alice', 'peer-A', 100, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-alice', 'peer-B', 200, 0)")
	rdb.exec("insert into hosts (user, peer, added, ack) values ('u-bob', 'peer-A', 300, 0)")

	alice := &User{UID: "u-alice"}
	with_user_thread(alice, func(th *sl.Thread) {
		v, err := api_replication_host_remove(th, sl.NewBuiltin("host_remove", api_replication_host_remove), sl.Tuple{sl.String("peer-A")}, nil)
		if err != nil {
			t.Fatalf("host_remove: %v", err)
		}
		if s, _ := v.(sl.String); string(s) != "removed" {
			t.Errorf("host_remove = %v, want removed", v)
		}

		// not-found path.
		v, _ = api_replication_host_remove(th, sl.NewBuiltin("host_remove", api_replication_host_remove), sl.Tuple{sl.String("peer-unknown")}, nil)
		if s, _ := v.(sl.String); string(s) != "not-found" {
			t.Errorf("host_remove unknown peer = %v, want not-found", v)
		}
	})

	// Alice's other host and Bob's row must be intact.
	if exists, _ := rdb.exists("select 1 from hosts where user='u-alice' and peer='peer-B'"); !exists {
		t.Error("alice's peer-B host was incorrectly removed")
	}
	if exists, _ := rdb.exists("select 1 from hosts where user='u-bob' and peer='peer-A'"); !exists {
		t.Error("bob's host was incorrectly removed by alice's call")
	}
}

// ============================================================
// Per-DB row replication tests (users / schedule / sessions)
// + Go-side internal-exec replication tests
// ============================================================

// ============================================================
// users.db row tests
// ============================================================

// setup_users_row_apply_test wires up the data_dir + a registered user
// with a known UID. Returns a cleanup to restore state, plus the test
// UID.
func setup_users_row_apply_test(t *testing.T) (cleanup func(), uid string) {
	t.Helper()
	tmp, err := os.MkdirTemp("", "mochi_users_row")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	orig := data_dir
	data_dir = tmp
	setup_users_test_schema()
	uid = "uid-users-row"
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", uid, "alice@example.com")
	cleanup = func() {
		data_dir = orig
		os.RemoveAll(tmp)
	}
	return
}

// TestReplicationUsersUsersApplyRoleIgnoredOnPerUserPath asserts that
// role does NOT flow via the per-user (host-set) path - it must arrive
// via the pair-only system-row pipeline so it doesn't leak across
// operators (different operators decide admin authority independently).
func TestReplicationUsersUsersApplyRoleIgnoredOnPerUserPath(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "users", Cols: map[string]string{"role": "administrator"}}
	if got := replication_users_row_apply(uid, op); got != ApplyInvalid {
		t.Fatalf("role on per-user path: want ApplyInvalid (silently ignored), got %v", got)
	}
	row, _ := db_open("db/users.db").row("select role from users where uid=?", uid)
	if got, _ := row["role"].(string); got == "administrator" {
		t.Error("role MUST NOT apply via the per-user path - pair-only column")
	}
}

// TestReplicationUsersUsersApplyUsernameIgnoredOnPerUserPath asserts
// the same exclusion for username, which is a per-operator namespace
// affordance rather than per-user data.
func TestReplicationUsersUsersApplyUsernameIgnoredOnPerUserPath(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "users", Cols: map[string]string{"username": "evil@elsewhere"}}
	if got := replication_users_row_apply(uid, op); got != ApplyInvalid {
		t.Fatalf("username on per-user path: want ApplyInvalid, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select username from users where uid=?", uid)
	if got, _ := row["username"].(string); got == "evil@elsewhere" {
		t.Error("username MUST NOT apply via the per-user path - pair-only column")
	}
}

// TestReplicationUsersUsersApplyStatus covers the per-user path's
// remaining valid column - status (suspend / activate) does propagate
// to every host in the user's set, including per-user link partners,
// because the user is suspended everywhere or active everywhere.
func TestReplicationUsersUsersApplyStatus(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "users", Cols: map[string]string{"status": "suspended"}}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("status apply: want ApplyApplied, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select status from users where uid=?", uid)
	if got, _ := row["status"].(string); got != "suspended" {
		t.Errorf("status: want suspended, got %q", got)
	}
}

func TestReplicationUsersUsersApplyMultipleColumns(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "users", Cols: map[string]string{
		"status":  "suspended",
		"methods": "email,passkey",
	}}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("multi-col apply: want ApplyApplied, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select status, methods from users where uid=?", uid)
	if got, _ := row["status"].(string); got != "suspended" {
		t.Errorf("status: want suspended, got %q", got)
	}
	if got, _ := row["methods"].(string); got != "email,passkey" {
		t.Errorf("methods: want email,passkey, got %q", got)
	}
}

func TestReplicationUsersUsersApplyIgnoresUnknownColumn(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	// "evil" isn't a real column. The apply must skip it (and skip the
	// whole UPDATE if no whitelisted columns remain).
	op := &UsersRow{Table: "users", Cols: map[string]string{"evil": "x"}}
	if got := replication_users_row_apply(uid, op); got != ApplyInvalid {
		t.Errorf("unknown-only column: want ApplyInvalid, got %v", got)
	}

	// A real column alongside an unknown column applies just the known.
	op = &UsersRow{Table: "users", Cols: map[string]string{"status": "suspended", "evil": "x"}}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("mixed: want ApplyApplied, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select status from users where uid=?", uid)
	if got, _ := row["status"].(string); got != "suspended" {
		t.Errorf("status: want suspended, got %q", got)
	}
}

func TestReplicationUsersUsersApplyDeferUnknownUser(t *testing.T) {
	cleanup, _ := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "users", Cols: map[string]string{"status": "suspended"}}
	if got := replication_users_row_apply("uid-missing", op); got != ApplyDeferred {
		t.Errorf("unknown user: want ApplyDeferred, got %v", got)
	}
}

func TestReplicationUsersUsersApplyDeleteIsNoop(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "users", Delete: true}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Errorf("delete: want ApplyApplied (noop), got %v", got)
	}
	// User row must still exist.
	exists, _ := db_open("db/users.db").exists("select 1 from users where uid=?", uid)
	if !exists {
		t.Error("delete op must NOT remove user row")
	}
}

func TestReplicationUsersEntitiesApplyCreate(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "entities", Cols: map[string]string{
		"id":          test_entity_id('a'),
		"private":     "private-key-bytes",
		"fingerprint": "fp-abc",
		"parent":      "",
		"class":       "feed",
		"name":        "Alice's Feed",
		"privacy":     "public",
		"data":        "",
		"published":   "0",
	}}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("create: want ApplyApplied, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select user, class, name, privacy from entities where id=?", test_entity_id('a'))
	if row == nil {
		t.Fatal("entity row missing after apply")
	}
	if got, _ := row["user"].(string); got != uid {
		t.Errorf("user FK: want %q, got %q", uid, got)
	}
	if got, _ := row["name"].(string); got != "Alice's Feed" {
		t.Errorf("name: want \"Alice's Feed\", got %q", got)
	}
}

func TestReplicationUsersEntitiesApplyUpdate(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	id := test_entity_id('b')
	db_open("db/users.db").exec(
		"insert into entities (id, private, fingerprint, user, class, name, privacy) values (?, 'priv', 'fp', ?, 'feed', 'Original', 'public')",
		id, uid)

	op := &UsersRow{Table: "entities", Cols: map[string]string{
		"id":      id,
		"name":    "Renamed",
		"privacy": "private",
	}}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("update: want ApplyApplied, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select name, privacy from entities where id=?", id)
	if got, _ := row["name"].(string); got != "Renamed" {
		t.Errorf("name: want Renamed, got %q", got)
	}
	if got, _ := row["privacy"].(string); got != "private" {
		t.Errorf("privacy: want private, got %q", got)
	}
}

func TestReplicationUsersEntitiesApplyDelete(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	id := test_entity_id('c')
	db_open("db/users.db").exec(
		"insert into entities (id, private, fingerprint, user, class, name) values (?, 'p', 'fp', ?, 'feed', 'X')",
		id, uid)

	op := &UsersRow{Table: "entities", Cols: map[string]string{"id": id}, Delete: true}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("delete: want ApplyApplied, got %v", got)
	}
	if exists, _ := db_open("db/users.db").exists("select 1 from entities where id=?", id); exists {
		t.Error("entity row must be removed")
	}
}

func TestReplicationUsersEntitiesApplyScopedToUser(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	// A row owned by user "other-uid" must not be touched by an op
	// arriving with op.User = uid (our test user).
	db_open("db/users.db").exec("insert into users (uid, username) values ('other-uid', 'other@example.com')")
	id := test_entity_id('d')
	db_open("db/users.db").exec(
		"insert into entities (id, private, fingerprint, user, class, name) values (?, 'p', 'fp', 'other-uid', 'feed', 'Theirs')",
		id)

	op := &UsersRow{Table: "entities", Cols: map[string]string{"id": id, "name": "Hijacked"}}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select name from entities where id=?", id)
	if got, _ := row["name"].(string); got != "Theirs" {
		t.Errorf("apply must be scoped to op.User; want untouched 'Theirs', got %q", got)
	}
}

func TestReplicationUsersEntitiesApplyIgnoresPublished(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	id := test_entity_id('e')
	db_open("db/users.db").exec(
		"insert into entities (id, private, fingerprint, user, class, name, published) values (?, 'p', 'fp', ?, 'feed', 'X', 1000)",
		id, uid)

	op := &UsersRow{Table: "entities", Cols: map[string]string{"id": id, "published": "9999"}}
	if got := replication_users_row_apply(uid, op); got != ApplyInvalid {
		t.Errorf("published-only update: want ApplyInvalid (per-host state), got %v", got)
	}
	row, _ := db_open("db/users.db").row("select published from entities where id=?", id)
	if got, _ := row["published"].(int64); got != 1000 {
		t.Errorf("published must not replicate; want 1000, got %d", got)
	}
}

// ============================================================
// schedule.db row tests
// ============================================================

// setup_schedule_row_apply_test wires up data_dir, a registered user,
// and the schedule.db schema. Returns a cleanup and the user UID.
func setup_schedule_row_apply_test(t *testing.T) (cleanup func(), uid string) {
	t.Helper()
	cleanup = setup_replication_test(t)
	setup_users_test_schema()
	uid = "uid-sched"
	db_open("db/users.db").exec("insert into users (uid, username) values (?, ?)", uid, "sched@example.com")
	schedule_db().exec("create table schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	return
}

func TestReplicationScheduleRowApplyInsert(t *testing.T) {
	cleanup, uid := setup_schedule_row_apply_test(t)
	defer cleanup()

	r := &ScheduleRow{
		Key: map[string]string{
			"user": uid, "app": "feeds", "event": "refresh", "created": "100",
		},
		Cols: map[string]string{
			"due": "130", "data": "{}", "interval": "30",
		},
	}
	if got := replication_schedule_row_apply(uid, r); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}
	row, _ := schedule_db().row(
		"select due, interval from schedule where user=? and app=? and event=? and created=?",
		uid, "feeds", "refresh", 100)
	if row == nil {
		t.Fatal("schedule row missing after apply")
	}
	if got, _ := row["due"].(int64); got != 130 {
		t.Errorf("due = %d, want 130", got)
	}
	if got, _ := row["interval"].(int64); got != 30 {
		t.Errorf("interval = %d, want 30", got)
	}
}

// TestReplicationScheduleRowApplyInsertIdempotent: re-applying the
// same op is a no-op. Same natural key already exists, INSERT is
// skipped.
func TestReplicationScheduleRowApplyInsertIdempotent(t *testing.T) {
	cleanup, uid := setup_schedule_row_apply_test(t)
	defer cleanup()

	r := &ScheduleRow{
		Key:  map[string]string{"user": uid, "app": "feeds", "event": "refresh", "created": "100"},
		Cols: map[string]string{"due": "130", "data": "{}", "interval": "30"},
	}
	replication_schedule_row_apply(uid, r)
	replication_schedule_row_apply(uid, r) // re-deliver

	rows, _ := schedule_db().rows(
		"select 1 from schedule where user=? and app=? and event=? and created=?",
		uid, "feeds", "refresh", 100)
	if len(rows) != 1 {
		t.Errorf("re-apply created duplicate; rows = %d, want 1", len(rows))
	}
}

func TestReplicationScheduleRowApplyDelete(t *testing.T) {
	cleanup, uid := setup_schedule_row_apply_test(t)
	defer cleanup()
	sdb := schedule_db()
	sdb.exec("insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, ?, ?, ?, ?)",
		uid, "feeds", 130, "refresh", "{}", 30, 100)

	r := &ScheduleRow{
		Key:    map[string]string{"user": uid, "app": "feeds", "event": "refresh", "created": "100"},
		Delete: true,
	}
	if got := replication_schedule_row_apply(uid, r); got != ApplyApplied {
		t.Fatalf("delete apply: want ApplyApplied, got %v", got)
	}
	exists, _ := sdb.exists(
		"select 1 from schedule where user=? and app=? and event=? and created=?",
		uid, "feeds", "refresh", 100)
	if exists {
		t.Error("row should have been deleted")
	}
}

// TestReplicationScheduleRowApplyDeleteNonExistent: deleting a row
// that's already gone returns Applied (idempotent).
func TestReplicationScheduleRowApplyDeleteNonExistent(t *testing.T) {
	cleanup, uid := setup_schedule_row_apply_test(t)
	defer cleanup()

	r := &ScheduleRow{
		Key:    map[string]string{"user": uid, "app": "feeds", "event": "refresh", "created": "100"},
		Delete: true,
	}
	if got := replication_schedule_row_apply(uid, r); got != ApplyApplied {
		t.Errorf("delete-nonexistent: want ApplyApplied, got %v", got)
	}
}

// TestReplicationScheduleRowApplyDeferUnknownUser: when the user
// hasn't landed yet (per-user link bootstrap incomplete), the op
// defers so it can replay after the user row arrives.
func TestReplicationScheduleRowApplyDeferUnknownUser(t *testing.T) {
	cleanup, _ := setup_schedule_row_apply_test(t)
	defer cleanup()

	r := &ScheduleRow{
		Key:  map[string]string{"user": "uid-missing", "app": "feeds", "event": "refresh", "created": "100"},
		Cols: map[string]string{"due": "130", "data": "{}", "interval": "30"},
	}
	if got := replication_schedule_row_apply("uid-missing", r); got != ApplyDeferred {
		t.Errorf("unknown user: want ApplyDeferred, got %v", got)
	}
}

// TestReplicationScheduleRowApplyMissingKey: a payload that's missing
// any natural-key field is dropped.
func TestReplicationScheduleRowApplyMissingKey(t *testing.T) {
	cleanup, uid := setup_schedule_row_apply_test(t)
	defer cleanup()

	cases := []map[string]string{
		{"app": "feeds", "event": "refresh", "created": "100"}, // user missing
		{"user": uid, "event": "refresh", "created": "100"},    // app missing
		{"user": uid, "app": "feeds", "created": "100"},        // event missing
		{"user": uid, "app": "feeds", "event": "refresh"},      // created missing
	}
	for i, key := range cases {
		r := &ScheduleRow{
			Key:  key,
			Cols: map[string]string{"due": "130", "data": "{}", "interval": "30"},
		}
		if got := replication_schedule_row_apply(uid, r); got != ApplyInvalid {
			t.Errorf("case %d (%v): want ApplyInvalid, got %v", i, key, got)
		}
	}
}

// TestScheduleCreateEmits: schedule_create fires a schedule-row.set
// op with the right Key + Cols.
func TestScheduleCreateEmits(t *testing.T) {
	cleanup, uid := setup_schedule_row_apply_test(t)
	defer cleanup()
	// Need an app row for schedule_valid - not exercised here, but the
	// real schedule_create path doesn't check app existence so we only
	// need the schedule table itself, already set up.

	calls := 0
	orig := replication_emit_schedule_row
	replication_emit_schedule_row = func(user string, r *ScheduleRow) {
		calls++
		if user != uid {
			t.Errorf("emit user = %q, want %q", user, uid)
		}
		if r.Delete {
			t.Error("emit delete=true on create")
		}
		if r.Key["app"] != "feeds" || r.Key["event"] != "refresh" {
			t.Errorf("emit key: %v", r.Key)
		}
		if r.Cols["interval"] != "30" {
			t.Errorf("emit interval: %q, want 30", r.Cols["interval"])
		}
	}
	defer func() { replication_emit_schedule_row = orig }()

	id := schedule_create(uid, "feeds", now()+60, "refresh", "{}", 30)
	if id == 0 {
		t.Fatal("schedule_create returned id=0")
	}
	if calls != 1 {
		t.Errorf("emit calls = %d, want 1", calls)
	}
}

// TestScheduleCreateSystemEventDoesNotEmit: system events (empty
// user) stay local - no emit.
func TestScheduleCreateSystemEventDoesNotEmit(t *testing.T) {
	cleanup, _ := setup_schedule_row_apply_test(t)
	defer cleanup()

	calls := 0
	orig := replication_emit_schedule_row
	replication_emit_schedule_row = func(user string, r *ScheduleRow) { calls++ }
	defer func() { replication_emit_schedule_row = orig }()

	if id := schedule_create("", "platform", now()+60, "tick", "{}", 0); id == 0 {
		t.Fatal("schedule_create returned id=0 for system event")
	}
	if calls != 0 {
		t.Errorf("system event emit calls = %d, want 0", calls)
	}
}

// TestScheduleDeleteEmits: schedule_delete fires a schedule-row.delete
// op keyed on the row's natural identifier.
func TestScheduleDeleteEmits(t *testing.T) {
	cleanup, uid := setup_schedule_row_apply_test(t)
	defer cleanup()
	sdb := schedule_db()
	res := must(sdb.internal.Exec(
		"insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, ?, ?, ?, ?)",
		uid, "crm", 200, "reminder", "{}", 0, 100))
	id, _ := res.LastInsertId()

	calls := 0
	orig := replication_emit_schedule_row
	replication_emit_schedule_row = func(user string, r *ScheduleRow) {
		calls++
		if !r.Delete {
			t.Error("emit delete=false on delete")
		}
		if r.Key["user"] != uid || r.Key["app"] != "crm" || r.Key["event"] != "reminder" || r.Key["created"] != "100" {
			t.Errorf("emit key: %v", r.Key)
		}
	}
	defer func() { replication_emit_schedule_row = orig }()

	schedule_delete(id)
	if calls != 1 {
		t.Errorf("emit calls = %d, want 1", calls)
	}
}

// ============================================================
// Go-side internal-exec replication tests
// (matches replication_internal_exec helpers now folded into
// replication.go for per-user system DB writes from Go callers)
// ============================================================

// TestReplicationApplyAppSystemExec verifies that a replicated app-system
// write (the path mochi.access.* now uses) lands in the receiver's
// users/<uid>/<app>/app.db.
func TestReplicationApplyAppSystemExec(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  app_id,
		Table:     "access",
		Operation: repl_op_exec_app_system,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into access (subject, resource, operation, grant, granter, created) values (?, ?, ?, ?, ?, ?)`,
			Args:      []any{"alice", "feed/F1", "view", int64(1), "alice", int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app_system(u, a)
	row, err := db.row("select grant from access where subject=? and resource=? and operation=?", "alice", "feed/F1", "view")
	if err != nil {
		t.Fatalf("row error: %v", err)
	}
	if row == nil {
		t.Fatal("replicated access row missing on receiver")
	}
}

// TestReplicationApplyAppSystemExecMissingApp confirms the apply defers
// when the receiver doesn't have the app installed yet (the bootstrap
// drain will retry once the app sync lands).
func TestReplicationApplyAppSystemExecMissingApp(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  "no-such-app",
		Operation: repl_op_exec_app_system,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into access (subject, resource, operation, grant, granter, created) values (?, ?, ?, ?, ?, ?)`,
			Args:      []any{"alice", "feed/F1", "view", int64(1), "alice", int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyDeferred {
		t.Fatalf("expected ApplyDeferred for missing app, got %v", got)
	}
}

// TestReplicationApplyUserCoreExec verifies that a replicated user-core
// write (the path mochi.group.* now uses) lands in the receiver's
// users/<uid>/user.db.
func TestReplicationApplyUserCoreExec(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  repl_db_user_core_sentinel,
		Table:     "groups",
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into groups (id, name, description, created) values (?, ?, ?, ?)`,
			Args:      []any{"g-engineering", "Engineering", "", int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	db := db_user(u, "user")
	row, err := db.row("select name from groups where id=?", "g-engineering")
	if err != nil {
		t.Fatalf("row error: %v", err)
	}
	if row == nil {
		t.Fatal("replicated groups row missing on receiver")
	}
	if got, _ := row["name"].(string); got != "Engineering" {
		t.Errorf("name: want Engineering, got %q", got)
	}
}

// TestReplicationApplyUserCoreExecPreferences: a user preference write
// replicates via the user-core exec path and lands in the receiver's
// users/<uid>/user.db `preferences` table. Regression for a language
// preference changed on one host of an account not reaching the other
// hosts — user_preference_set / user_preference_delete now use
// exec_replicated, and `preferences` is not in sql_default_excluded,
// so the write fans out and applies.
func TestReplicationApplyUserCoreExecPreferences(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  repl_db_user_core_sentinel,
		Table:     "preferences",
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into preferences (name, value) values (?, ?)`,
			Args:      []any{"language", "fr"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	db := db_user(u, "user")
	row, err := db.row("select value from preferences where name=?", "language")
	if err != nil {
		t.Fatalf("row error: %v", err)
	}
	if row == nil {
		t.Fatal("replicated preferences row missing on receiver")
	}
	if got, _ := row["value"].(string); got != "fr" {
		t.Errorf("language preference: want fr, got %q", got)
	}

	// A delete also replicates and converges.
	del := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  repl_db_user_core_sentinel,
		Table:     "preferences",
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `delete from preferences where name = ?`,
			Args:      []any{"language"},
		}),
	}
	if got := replication_apply_op(del); got != ApplyApplied {
		t.Fatalf("delete: expected ApplyApplied, got %v", got)
	}
	if n := db.integer("select count(*) from preferences where name='language'"); n != 0 {
		t.Errorf("preference rows after replicated delete = %d, want 0", n)
	}
}

// TestReplicationApplyUserCoreExecInterests: an interest-profile write
// replicates via the user-core exec path and lands in the receiver's
// users/<uid>/user.db `interests` table. The personalised ranking is
// account-global, so mochi.interests.* now uses exec_replicated and
// `interests` is not in sql_default_excluded — the write fans out.
func TestReplicationApplyUserCoreExecInterests(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  repl_db_user_core_sentinel,
		Table:     "interests",
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `insert or replace into interests (qid, weight, updated) values (?, ?, ?)`,
			Args:      []any{"Q42", int64(75), int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	db := db_user(u, "user")
	row, err := db.row("select weight from interests where qid=?", "Q42")
	if err != nil {
		t.Fatalf("row error: %v", err)
	}
	if row == nil {
		t.Fatal("replicated interests row missing on receiver")
	}
	if got, _ := row["weight"].(int64); got != 75 {
		t.Errorf("weight: want 75, got %d", got)
	}
}

// TestReplicationApplyUserCoreExecMissingUser confirms the apply defers
// when the user record hasn't yet landed locally.
func TestReplicationApplyUserCoreExecMissingUser(t *testing.T) {
	cleanup, _, _ := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      "uid-not-here",
		Database:  repl_db_user_core_sentinel,
		Operation: repl_op_exec_user_core,
		Payload: cbor_encode(&SQLCommand{
			Statement: `replace into groups (id, name, description, created) values (?, ?, ?, ?)`,
			Args:      []any{"g1", "G", "", int64(1700000000)},
		}),
	}
	if got := replication_apply_op(op); got != ApplyDeferred {
		t.Fatalf("expected ApplyDeferred for missing user, got %v", got)
	}
}

// ============================================================
// SQL command replication tests
// (basic apply / extra invariants / emit-side gates / transactions)
// ============================================================

// ============================================================
// Basic apply tests (was replication_sql_command_test.go)
// ============================================================

func TestSQLTargetTable(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"INSERT INTO posts VALUES (1)", "posts"},
		{"insert into posts values (1)", "posts"},
		{"INSERT OR IGNORE INTO posts VALUES (1)", "posts"},
		{"INSERT OR REPLACE INTO posts (id) VALUES (1)", "posts"},
		{"REPLACE INTO posts (id) VALUES (1)", "posts"},
		{"UPDATE posts SET title = ? WHERE id = ?", "posts"},
		{"update posts set title = ?", "posts"},
		{"UPDATE OR REPLACE posts SET x = 1", "posts"},
		{"DELETE FROM posts WHERE id = ?", "posts"},
		{"delete from posts", "posts"},

		// Identifiers with quoting.
		{`INSERT INTO "posts" VALUES (1)`, "posts"},
		{"INSERT INTO `posts` VALUES (1)", "posts"},
		{"INSERT INTO [posts] VALUES (1)", "posts"},

		// Leading whitespace / comments.
		{"  \n\t INSERT INTO posts VALUES (1)", "posts"},
		{"-- header\nINSERT INTO posts VALUES (1)", "posts"},
		{"/* header */ INSERT INTO posts VALUES (1)", "posts"},

		// Non-mutating statements: not replicated.
		{"SELECT * FROM posts", ""},
		{"PRAGMA user_version", ""},
		{"CREATE TABLE posts (id INTEGER)", ""},
		{"DROP TABLE posts", ""},
		{"ALTER TABLE posts ADD COLUMN x", ""},

		// CTE: deliberately not recognised; caller must reshape.
		{"WITH cte AS (SELECT 1) INSERT INTO posts SELECT * FROM cte", ""},

		// Garbled input.
		{"", ""},
		{"   ", ""},
		{"INSERT", ""},
		{"UPDATE", ""},
		{"DELETE FROM", ""},
		{"INSERT INTO", ""},
	}
	for _, c := range cases {
		got := sql_target_table(c.sql)
		if got != c.want {
			t.Errorf("sql_target_table(%q) = %q; want %q", c.sql, got, c.want)
		}
	}
}

func TestSQLTargetUID(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		args []any
		want string
	}{
		// Explicit (id, ...) column list - first column is id, args[0]
		// is the row uid.
		{"insert id-first",
			"INSERT INTO posts (id, title) VALUES (?, ?)",
			[]any{"abc123", "hello"},
			"abc123"},
		{"replace id-first",
			"REPLACE INTO posts (id, title) VALUES (?, ?)",
			[]any{"xyz", "hi"},
			"xyz"},
		{"insert or ignore id-first",
			"INSERT OR IGNORE INTO posts (id, title) VALUES (?, ?)",
			[]any{"u1", "t"},
			"u1"},
		{"insert or replace id-first",
			"INSERT OR REPLACE INTO posts (id, n) VALUES (?, ?)",
			[]any{"u2", 1},
			"u2"},

		// Implicit positional values - args[0] is the row uid by
		// convention (Mochi's CREATE TABLE puts id first).
		{"insert positional",
			"INSERT INTO posts VALUES (?, ?, ?)",
			[]any{"pos1", "t", "b"},
			"pos1"},
		{"replace positional",
			"REPLACE INTO posts VALUES (?, ?)",
			[]any{"rp1", "n"},
			"rp1"},

		// (id, ...) column list with quoted table and case variations.
		{"insert quoted table id-first",
			`INSERT INTO "posts" (id, title) VALUES (?, ?)`,
			[]any{"quoted", "t"},
			"quoted"},
		{"lowercase keywords",
			"insert into posts (id, title) values (?, ?)",
			[]any{"lower", "t"},
			"lower"},

		// First column is NOT id - no extraction (apps using non-id
		// PK fall back to empty uid).
		{"insert non-id first column",
			"INSERT INTO posts (slug, title) VALUES (?, ?)",
			[]any{"hello-world", "t"},
			""},

		// UPDATE / DELETE with WHERE id = ?.
		{"update where id",
			"UPDATE posts SET title = ? WHERE id = ?",
			[]any{"new", "abc"},
			"abc"},
		{"delete where id",
			"DELETE FROM posts WHERE id = ?",
			[]any{"abc"},
			"abc"},
		{"update where id no spaces",
			"UPDATE posts SET title=? WHERE id=?",
			[]any{"new", "row7"},
			"row7"},
		{"update multiple set args",
			"UPDATE posts SET title = ?, body = ?, updated = ? WHERE id = ?",
			[]any{"t", "b", 123, "row9"},
			"row9"},

		// WHERE clause keyed on a different column or compound - no
		// extraction.
		{"update where non-id",
			"UPDATE posts SET title = ? WHERE slug = ?",
			[]any{"t", "hello"},
			""},
		{"update where compound",
			"UPDATE posts SET title = ? WHERE id = ? AND author = ?",
			[]any{"t", "abc", "user"},
			""},
		{"delete no where",
			"DELETE FROM posts",
			[]any{},
			""},

		// Non-string args (e.g. integer PK) aren't returned as row
		// uids - Mochi uses string uids via mochi.uid().
		{"insert integer pk",
			"INSERT INTO posts (id, title) VALUES (?, ?)",
			[]any{int64(42), "t"},
			""},

		// Empty / unparseable input.
		{"empty sql", "", nil, ""},
		{"select read-only", "SELECT * FROM posts WHERE id = ?", []any{"x"}, ""},
		{"create table", "CREATE TABLE posts (id INTEGER)", nil, ""},
	}
	for _, c := range cases {
		got := sql_target_uid(c.sql, c.args)
		if got != c.want {
			t.Errorf("%s: sql_target_uid(%q, %v) = %q; want %q",
				c.name, c.sql, c.args, got, c.want)
		}
	}
}

// TestReplicationOpUIDRoundtrip verifies the UID field survives a
// cbor encode / decode cycle (the wire path between sender and
// receiver) and that an op encoded by an older sender (no UID field)
// decodes cleanly with an empty UID.
func TestReplicationOpUIDRoundtrip(t *testing.T) {
	sent := ReplicationOp{
		Scope:     repl_scope_app,
		User:      "uid-user",
		Database:  "posts",
		Table:     "posts",
		UID:       "row-abc",
		Operation: repl_op_exec,
		Payload:   []byte("body"),
		Sequence:  1,
		Prev:      0,
	}
	encoded := cbor_encode(&sent)
	var received ReplicationOp
	if err := cbor.Unmarshal(encoded, &received); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if received.UID != "row-abc" {
		t.Errorf("UID lost in roundtrip: got %q want %q", received.UID, sent.UID)
	}

	// Older sender shape: encode without setting UID, decode, expect "".
	older := ReplicationOp{
		Scope:    repl_scope_app,
		User:     "uid-user",
		Database: "posts",
		Table:    "posts",
		// UID intentionally unset.
		Operation: repl_op_exec,
		Payload:   []byte("body"),
		Sequence:  2,
		Prev:      1,
	}
	var olderDecoded ReplicationOp
	if err := cbor.Unmarshal(cbor_encode(&older), &olderDecoded); err != nil {
		t.Fatalf("older-shape decode failed: %v", err)
	}
	if olderDecoded.UID != "" {
		t.Errorf("missing UID must decode as empty string, got %q", olderDecoded.UID)
	}
}

func TestSQLTableExcluded(t *testing.T) {
	// Default exclusions.
	if !sql_table_excluded(nil, "sqlite_master") {
		t.Error("sqlite_master must be excluded by default")
	}
	if !sql_table_excluded(nil, "sqlite_sequence") {
		t.Error("sqlite_sequence must be excluded by default")
	}
	// commits is the relocated commit-hook log; it lives in app.db, not the
	// data DB, so it must NOT be in the data-DB default-exclude set — a bare
	// name there would wrongly suppress an app table called "commits".
	if sql_table_excluded(nil, "commits") {
		t.Error("commits must NOT be excluded on the data-DB path")
	}
	if sql_table_excluded(nil, "posts") {
		t.Error("posts must NOT be excluded by default")
	}

	// Empty / unparseable target: treated as excluded so we don't emit.
	if !sql_table_excluded(nil, "") {
		t.Error("empty table must be treated as excluded")
	}

	// App-declared exclusion.
	av := &AppVersion{}
	av.Database.Replicate.Exclude.Tables = []string{"cache_search", "session_local"}
	if !sql_table_excluded(av, "cache_search") {
		t.Error("app-excluded table must be excluded")
	}
	if !sql_table_excluded(av, "session_local") {
		t.Error("app-excluded table must be excluded")
	}
	if sql_table_excluded(av, "posts") {
		t.Error("non-excluded app table must replicate")
	}
}

// setup_sql_replication_test wires up just enough server state for an
// apply-side test: a temp data_dir, a registered user, a registered app
// pointing at a per-(user, app) DB the apply path will exec against,
// and a fresh schema in that DB.
func setup_sql_replication_test(t *testing.T) (cleanup func(), user_uid, app_id string) {
	t.Helper()
	tmp, err := os.MkdirTemp("", "mochi_sql_repl")
	if err != nil {
		t.Fatalf("mktemp: %v", err)
	}
	orig := data_dir
	data_dir = tmp
	// Suppress the post-migration background drain: it reads data_dir
	// asynchronously and races with the cleanup's data_dir restore.
	orig_drain := post_migration_drain_async
	post_migration_drain_async = func(user, app_id string) {}

	udb := db_open("db/users.db")
	udb.exec(`create table if not exists users (id integer primary key, uid text not null unique, username text not null unique)`)
	user_uid = "uid-test-sql"
	udb.exec("insert into users (uid, username) values (?, ?)", user_uid, "alice")

	app_id = "myapp"
	av := &AppVersion{Version: "1"}
	av.Architecture.Engine = "starlark"
	av.Architecture.Version = 4
	av.Database.File = "myapp.db"
	av.Database.Schema = 1
	av.Database.create_function = func(db *DB) {
		db.exec(`create table posts (id text primary key, title text not null)`)
	}
	a := &App{id: app_id, versions: map[string]*AppVersion{"1": av}, internal: av}
	av.app = a
	apps_lock.Lock()
	if apps == nil {
		apps = map[string]*App{}
	}
	apps[app_id] = a
	apps_lock.Unlock()

	cleanup = func() {
		apps_lock.Lock()
		delete(apps, app_id)
		apps_lock.Unlock()
		data_dir = orig
		post_migration_drain_async = orig_drain
		os.RemoveAll(tmp)
	}
	return
}

func TestReplicationApplySQLCommandInsert(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  app_id,
		Operation: repl_op_exec,
		Schema:    1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"p1", "Hello"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("expected ApplyApplied, got %v", got)
	}

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	row, _ := db.row("select title from posts where id = ?", "p1")
	if row == nil {
		t.Fatal("inserted row missing")
	}
	if got, _ := row["title"].(string); got != "Hello" {
		t.Errorf("title: want Hello, got %q", got)
	}
}

func TestReplicationApplySQLCommandUpdateThenDelete(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	db.exec("insert into posts (id, title) values (?, ?)", "p1", "Old")

	upd := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "update posts set title = ? where id = ?",
			Args:      []any{"New", "p1"},
		}),
	}
	if got := replication_apply_op(upd); got != ApplyApplied {
		t.Fatalf("update apply: want ApplyApplied, got %v", got)
	}
	row, _ := db.row("select title from posts where id = ?", "p1")
	if got, _ := row["title"].(string); got != "New" {
		t.Errorf("after update: want New, got %q", got)
	}

	del := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "delete from posts where id = ?",
			Args:      []any{"p1"},
		}),
	}
	if got := replication_apply_op(del); got != ApplyApplied {
		t.Fatalf("delete apply: want ApplyApplied, got %v", got)
	}
	if r, _ := db.row("select 1 from posts where id = ?", "p1"); r != nil {
		t.Error("row not deleted")
	}
}

func TestReplicationApplySQLCommandDeferralPaths(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	// Unknown user → deferred.
	unknown_user := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-missing",
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(unknown_user); got != ApplyDeferred {
		t.Errorf("unknown user: want ApplyDeferred, got %v", got)
	}

	// Unknown app → deferred.
	unknown_app := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: "missingapp", Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(unknown_app); got != ApplyDeferred {
		t.Errorf("unknown app: want ApplyDeferred, got %v", got)
	}

	// Sender schema newer than receiver → deferred.
	newer_schema := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 99,
		Payload: cbor_encode(&SQLCommand{Statement: "insert into posts (id, title) values ('x', 'y')"}),
	}
	if got := replication_apply_op(newer_schema); got != ApplyDeferred {
		t.Errorf("newer schema: want ApplyDeferred, got %v", got)
	}
}

func TestReplicationApplySQLCommandInvalid(t *testing.T) {
	cleanup, _, _ := setup_sql_replication_test(t)
	defer cleanup()

	// Bad cbor → Invalid.
	bad := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-test-sql",
		Database: "myapp", Operation: repl_op_exec, Schema: 1,
		Payload: []byte{0xff, 0xff, 0xff},
	}
	if got := replication_apply_op(bad); got != ApplyInvalid {
		t.Errorf("bad cbor: want ApplyInvalid, got %v", got)
	}

	// Empty statement → Invalid.
	empty := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-test-sql",
		Database: "myapp", Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: ""}),
	}
	if got := replication_apply_op(empty); got != ApplyInvalid {
		t.Errorf("empty statement: want ApplyInvalid, got %v", got)
	}
}

func TestReplicationApplySQLCommandRoundTrip(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	// Two writers replay each other's ops; both ends should converge.
	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)

	apply := func(sql string, args ...any) {
		op := &ReplicationOp{
			Scope: repl_scope_app, User: user_uid,
			Database: app_id, Operation: repl_op_exec, Schema: 1,
			Payload: cbor_encode(&SQLCommand{Statement: sql, Args: args}),
		}
		if got := replication_apply_op(op); got != ApplyApplied {
			t.Fatalf("apply %q: %v", sql, got)
		}
	}

	apply("insert into posts (id, title) values (?, ?)", "p1", "A")
	apply("insert into posts (id, title) values (?, ?)", "p2", "B")
	apply("update posts set title = ? where id = ?", "A-updated", "p1")
	apply("delete from posts where id = ?", "p2")

	rows, _ := db.rows("select id, title from posts order by id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if id, _ := rows[0]["id"].(string); id != "p1" {
		t.Errorf("row id: want p1, got %q", id)
	}
	if title, _ := rows[0]["title"].(string); title != "A-updated" {
		t.Errorf("row title: want A-updated, got %q", title)
	}
}

// TestReplicationEmitConcurrentChainIntact is the regression test
// for task #93. Before the fix, replication_sequence_next and
// replication_tail_advance used SELECT-then-UPDATE patterns that
// raced under concurrent emit. Two goroutines could both see the
// same pre-update value and emit ops with identical sequence
// numbers or identical `prev` pointers. The receiver applied one
// op cleanly and silently dropped the duplicate as "below cursor",
// then everything past the lost link buffered forever waiting for
// it. Surfaced live as 668/272 stalled entries on mochi2's
// feeds/projects streams after ~40 minutes of normal traffic.
//
// Fix in replication_emit_to_real wraps the (sequence_next,
// tail_advance) pair in a per-(user, scope, db) mutex. This test
// drives the same critical section directly from N goroutines and
// asserts: every sequence is unique, every prev chains onto the
// preceding emit's sequence, the tail row's final last matches the
// max emitted sequence.
//
// Verified the test catches the regression: with the mutex removed,
// failure messages include "sequence N emitted twice" and "chain
// broken at idx M: seq=X prev=Y, want prev=Z".
func TestReplicationEmitConcurrentChainIntact(t *testing.T) {
	cleanup, user_uid, _ := setup_sql_replication_test(t)
	defer cleanup()

	// setup_sql_replication_test gives a temp data_dir but doesn't
	// initialise db/replication.db. The emit critical section reads
	// from `sequence` and `tail` tables there. Create the minimal
	// schema directly.
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists sequence (user text not null default '', scope text not null, next integer not null default 0, primary key (user, scope))")
	rdb.exec("create table if not exists tail (user text not null default '', scope text not null, db text not null default '', last integer not null default 0, primary key (user, scope, db))")

	type allocated struct{ seq, prev int64 }
	const N = 200
	results := make([]allocated, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			// Mirror the production critical section in
			// replication_emit_to_real.
			mu := replication_emit_lock(user_uid, "app", "testdb")
			mu.Lock()
			seq := replication_sequence_next(user_uid, "app")
			prev := replication_tail_advance(user_uid, "app", "testdb", seq)
			mu.Unlock()
			results[i] = allocated{seq, prev}
		}(i)
	}
	wg.Wait()

	// Sort by allocated sequence; the chain must be intact after
	// sort: every prev equals the predecessor's seq, no duplicates.
	sort.Slice(results, func(i, j int) bool { return results[i].seq < results[j].seq })
	seen := map[int64]bool{}
	for i, r := range results {
		if seen[r.seq] {
			t.Errorf("sequence %d emitted twice (race regression)", r.seq)
		}
		seen[r.seq] = true
		if i == 0 {
			if r.prev != 0 {
				t.Errorf("first op should have prev=0, got %d", r.prev)
			}
		} else {
			want := results[i-1].seq
			if r.prev != want {
				t.Errorf("chain broken at idx %d: seq=%d prev=%d, want prev=%d", i, r.seq, r.prev, want)
			}
		}
	}

	// Final tail.last must equal the highest sequence emitted.
	if row, err := rdb.row("select last from tail where user=? and scope=? and db=?", user_uid, "app", "testdb"); err == nil && row != nil {
		if last, _ := row["last"].(int64); last != results[N-1].seq {
			t.Errorf("final tail.last = %d, want %d", last, results[N-1].seq)
		}
	} else {
		t.Errorf("tail row missing or read failed: %v", err)
	}
}

// ============================================================
// Extra apply / loop-prevention / replay tests
// (was replication_sql_command_extra_test.go)
// ============================================================

// TestReplicationApplySQLCommandDoesNotReEmit locks the no-loop
// invariant: when a SQL exec op is applied on the receiver, the apply
// path must not call replication_emit_sql_command on the way through.
// If it did, two-host replication would ping-pong forever.
//
// Probe: replication_emit increments a per-(user, scope) sequence row
// in replication.db.sequence as its first side effect. If apply
// re-emitted, that row would exist with next>=1. We assert it doesn't.
func TestReplicationApplySQLCommandDoesNotReEmit(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()
	db_upgrade_50() // creates replication.db.sequence

	op := &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user_uid,
		Database:  app_id,
		Operation: repl_op_exec,
		Schema:    1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"loop-1", "Hello"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}

	repl := db_open("db/replication.db")
	row, _ := repl.row("select next from sequence where user=? and scope=?", user_uid, repl_scope_app)
	if row != nil {
		if next, _ := row["next"].(int64); next > 0 {
			t.Errorf("apply re-emitted: replication.db.sequence row for user=%q scope=%q advanced to %d (expected 0/absent)", user_uid, repl_scope_app, next)
		}
	}
}

// TestReplicationApplySQLCommandIdempotentReplay re-applies the same
// op and verifies the receiver doesn't blow up. INSERT replay produces
// a PK uniqueness violation which the apply path logs and treats as
// ApplyApplied (so the deduper doesn't keep retrying forever); the
// row state matches what one apply would have produced.
func TestReplicationApplySQLCommandIdempotentReplay(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"idem", "Once"},
		}),
	}
	for i := 0; i < 3; i++ {
		if got := replication_apply_op(op); got != ApplyApplied {
			t.Fatalf("apply #%d: want ApplyApplied, got %v", i, got)
		}
	}

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	count := db.integer("select count(*) from posts where id='idem'")
	if count != 1 {
		t.Errorf("replay must be idempotent; row count = %d, want 1", count)
	}
	title, _ := db.row("select title from posts where id='idem'")
	if v, _ := title["title"].(string); v != "Once" {
		t.Errorf("title: want 'Once', got %q", v)
	}
}

// TestReplicationApplySQLCommandReceiverFailureLogged exercises the
// schema-drift path: a receiver missing a column referenced by the
// op's SQL. The apply must not panic; it logs and returns ApplyApplied
// so the deduper marks it seen and doesn't re-deliver.
func TestReplicationApplySQLCommandReceiverFailureLogged(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title, missing) values (?, ?, ?)",
			Args:      []any{"bad", "X", "Y"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("receiver-failure: want ApplyApplied (logged), got %v", got)
	}
}

// TestReplicationSQLCommandMixedArgTypesRoundTrip exercises the CBOR
// encode→decode→exec path with the parameter types apps actually pass:
// strings, integers, []byte (blob), and nil. The receiver's SQL
// driver must accept whatever Go types CBOR produces on the other side.
//
// CBOR's `any` decode returns positive ints as uint64; the SQL driver
// accepts both, so the wire format normalising to uint64 is fine. The
// test checks stored values, not the intermediate Go types.
func TestReplicationSQLCommandMixedArgTypesRoundTrip(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	db.exec("create table mixed (id text primary key, n integer, blob blob, opt text)")

	original := &SQLCommand{
		Statement: "insert into mixed (id, n, blob, opt) values (?, ?, ?, ?)",
		Args:      []any{"m1", int64(42), []byte{0x01, 0x02, 0x03}, nil},
	}
	payload := cbor_encode(original)

	// Probe the decoded Args shape before the apply runs, so we can
	// pinpoint where any type confusion happens.
	var decoded SQLCommand
	if err := cbor.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Args) != 4 {
		t.Fatalf("args len: want 4, got %d", len(decoded.Args))
	}
	t.Logf("decoded arg types: %T %T %T %T", decoded.Args[0], decoded.Args[1], decoded.Args[2], decoded.Args[3])

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: payload,
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}

	// DB.row() helpfully converts []byte to string for app code, so we
	// can't .([]byte) the blob column directly. Use length + hex to
	// verify the stored bytes are correct.
	row, _ := db.row("select n, length(blob) as blen, hex(blob) as bhex, opt from mixed where id='m1'")
	if row == nil {
		t.Fatal("row missing after apply")
	}
	if n, _ := row["n"].(int64); n != 42 {
		t.Errorf("integer column: want 42, got %d (raw %v)", n, row["n"])
	}
	if blen, _ := row["blen"].(int64); blen != 3 {
		t.Errorf("blob length: want 3, got %d", blen)
	}
	if bhex, _ := row["bhex"].(string); bhex != "010203" {
		t.Errorf("blob hex: want 010203, got %q", bhex)
	}
	if v := row["opt"]; v != nil {
		t.Errorf("nil arg: want nil, got %v (%T)", v, v)
	}
}

// TestReplicationSQLCommandNoParamsStatement covers a statement that
// uses no bound parameters at all (e.g. a bulk delete).
func TestReplicationSQLCommandNoParamsStatement(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app(u, a)
	db.exec("insert into posts (id, title) values ('a', 'A')")
	db.exec("insert into posts (id, title) values ('b', 'B')")

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 1,
		Payload: cbor_encode(&SQLCommand{Statement: "delete from posts"}),
	}
	if got := replication_apply_op(op); got != ApplyApplied {
		t.Fatalf("apply: want ApplyApplied, got %v", got)
	}
	if n := db.integer("select count(*) from posts"); n != 0 {
		t.Errorf("post-delete count: want 0, got %d", n)
	}
}

// TestReplicationSQLCommandSchemaDefer exercises the cross-host schema
// gate: a sender at schema v3 cannot apply on a receiver still at v1.
// The op must defer, not error out.
func TestReplicationSQLCommandSchemaDefer(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	op := &ReplicationOp{
		Scope: repl_scope_app, User: user_uid,
		Database: app_id, Operation: repl_op_exec, Schema: 99,
		Payload: cbor_encode(&SQLCommand{
			Statement: "insert into posts (id, title) values (?, ?)",
			Args:      []any{"future", "From v99"},
		}),
	}
	if got := replication_apply_op(op); got != ApplyDeferred {
		t.Errorf("op carrying higher sender schema: want ApplyDeferred, got %v", got)
	}
}

// ============================================================
// Emit-side gate tests (was replication_sql_emit_test.go)
// ============================================================

// emit_gate_setup wires up a tmp data_dir + replication.db so we can
// probe the sequence side-effect, but does NOT register the user or
// app. Individual tests set up just what they need.
func emit_gate_setup(t *testing.T) (cleanup func(), user_uid, app_id string) {
	t.Helper()
	cleanup, user_uid, app_id = setup_sql_replication_test(t)
	db_upgrade_50() // creates replication.db.sequence
	return
}

func sequence_row_exists(user string) bool {
	repl := db_open("db/replication.db")
	row, _ := repl.row("select next from sequence where user=? and scope=?", user, repl_scope_app)
	return row != nil
}

func TestReplicationEmitSQLCommandSilentWithNoUser(t *testing.T) {
	cleanup, _, app_id := emit_gate_setup(t)
	defer cleanup()

	a := app_by_id(app_id)
	replication_emit_sql_command(nil, a, a.internal, "insert into posts (id, title) values (?, ?)", []any{"x", "y"})
	if sequence_row_exists("") {
		t.Error("emit must not advance sequence when user is nil")
	}
}

func TestReplicationEmitSQLCommandSilentWithEmptyUID(t *testing.T) {
	cleanup, _, app_id := emit_gate_setup(t)
	defer cleanup()

	a := app_by_id(app_id)
	u := &User{UID: ""}
	replication_emit_sql_command(u, a, a.internal, "insert into posts (id, title) values (?, ?)", []any{"x", "y"})
	if sequence_row_exists("") {
		t.Error("emit must not advance sequence when UID is empty")
	}
}

func TestReplicationEmitSQLCommandSilentWithNoApp(t *testing.T) {
	cleanup, user_uid, _ := emit_gate_setup(t)
	defer cleanup()

	u := &User{UID: user_uid}
	replication_emit_sql_command(u, nil, nil, "insert into posts (id, title) values (?, ?)", []any{"x", "y"})
	if sequence_row_exists(user_uid) {
		t.Error("emit must not advance sequence when app is nil")
	}
}

func TestReplicationEmitSQLCommandSilentForExcludedTable(t *testing.T) {
	cleanup, user_uid, app_id := emit_gate_setup(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	av := a.internal
	av.Database.Replicate.Exclude.Tables = []string{"posts"}

	replication_emit_sql_command(u, a, av, "insert into posts (id, title) values (?, ?)", []any{"x", "y"})
	if sequence_row_exists(user_uid) {
		t.Error("emit must not advance sequence for excluded table")
	}
}

func TestReplicationEmitSQLCommandSilentForDefaultExcludedTable(t *testing.T) {
	cleanup, user_uid, app_id := emit_gate_setup(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)

	// sqlite_* writes (rare but possible if an app does raw bookkeeping)
	// must never replicate — they're SQLite internals.
	replication_emit_sql_command(u, a, a.internal, "insert into sqlite_master (name) values (?)", []any{"x"})
	if sequence_row_exists(user_uid) {
		t.Error("emit must not advance sequence for sqlite_* tables")
	}
}

func TestReplicationEmitSQLCommandSilentForNonMutatingStatement(t *testing.T) {
	cleanup, user_uid, app_id := emit_gate_setup(t)
	defer cleanup()

	u := &User{UID: user_uid}
	a := app_by_id(app_id)

	// SELECT / CREATE TABLE / DROP / ALTER aren't mutating data rows.
	for _, sql := range []string{
		"select 1",
		"create table foo (id text)",
		"drop table foo",
		"alter table posts add column x text",
	} {
		replication_emit_sql_command(u, a, a.internal, sql, nil)
	}
	if sequence_row_exists(user_uid) {
		t.Error("emit must not advance sequence for non-mutating SQL")
	}
}

// ============================================================
// Transaction emit-buffer lifecycle tests
// (was replication_transaction_test.go)
// ============================================================

// new_tx_handle builds a TransactionHandle backed by a real *sqlx.Tx
// on the test app's per-(user, app) DB. Lets us exercise commit /
// rollback / close paths without spinning up a full Starlark context.
func new_tx_handle(t *testing.T) (h *TransactionHandle, cleanup func()) {
	t.Helper()
	clean, user_uid, app_id := setup_sql_replication_test(t)

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	av := a.internal
	db := db_app(u, a)
	tx, err := db.starlark.Beginx()
	if err != nil {
		clean()
		t.Fatalf("begin tx: %v", err)
	}
	h = &TransactionHandle{tx: tx, user: u, app: a, av: av}
	return h, clean
}

func TestTransactionCommitFlushesPendingEmits(t *testing.T) {
	h, cleanup := new_tx_handle(t)
	defer cleanup()

	h.pending_emits = []sql_pending_emit{
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"a", "A"}},
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"b", "B"}},
	}

	if _, err := h.sl_commit(&sl.Thread{}, nil, sl.Tuple{}, nil); err != nil {
		t.Fatalf("sl_commit: %v", err)
	}
	if !h.closed {
		t.Error("after commit: handle must be closed")
	}
	if h.pending_emits != nil {
		t.Errorf("after commit: pending_emits must be nil, got %d entries", len(h.pending_emits))
	}
}

func TestTransactionRollbackDropsPendingEmits(t *testing.T) {
	h, cleanup := new_tx_handle(t)
	defer cleanup()

	h.pending_emits = []sql_pending_emit{
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"a", "A"}},
	}

	if _, err := h.sl_rollback(&sl.Thread{}, nil, sl.Tuple{}, nil); err != nil {
		t.Fatalf("sl_rollback: %v", err)
	}
	if !h.closed {
		t.Error("after rollback: handle must be closed")
	}
	if h.pending_emits != nil {
		t.Errorf("after rollback: pending_emits must be nil, got %d entries", len(h.pending_emits))
	}
}

func TestTransactionCloseDropsPendingEmits(t *testing.T) {
	// transaction_close (auto-cleanup at thread tear-down) iterates
	// every uncommitted handle and rolls back. After the cleanup any
	// pending_emits on those handles must be cleared so a forgotten
	// commit doesn't leak emits.
	h, cleanup := new_tx_handle(t)
	defer cleanup()

	h.pending_emits = []sql_pending_emit{
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"a", "A"}},
		{sql: "insert into posts (id, title) values (?, ?)", args: []any{"b", "B"}},
	}

	th := &sl.Thread{}
	th.SetLocal("transactions", []*TransactionHandle{h})
	transaction_close(th)

	if !h.closed {
		t.Error("after close: handle must be closed")
	}
	if h.pending_emits != nil {
		t.Errorf("after close: pending_emits must be nil, got %d entries", len(h.pending_emits))
	}
}

func TestTransactionCloseSkipsAlreadyClosed(t *testing.T) {
	// An already-committed handle in the auto-cleanup list must be
	// skipped (we'd panic calling Rollback on a committed tx).
	h, cleanup := new_tx_handle(t)
	defer cleanup()

	if _, err := h.sl_commit(&sl.Thread{}, nil, sl.Tuple{}, nil); err != nil {
		t.Fatalf("sl_commit: %v", err)
	}
	th := &sl.Thread{}
	th.SetLocal("transactions", []*TransactionHandle{h})
	transaction_close(th) // must not panic
}

// ============================================================
// System-scope replication tests (core DBs)
// ============================================================

// setup_system_replication_test prepares data_dir + settings.db with
// the schema replication_emit/_apply expects. Returns a cleanup.
func setup_system_replication_test(t *testing.T) func() {
	cleanup := setup_replication_test(t)
	settings := db_open("db/settings.db")
	settings.exec("create table if not exists settings (name text primary key, value text not null)")
	return cleanup
}

// TestSystemSetApplySettings: a settings.settings op with a non-empty
// value replaces / inserts the row.
func TestSystemSetApplySettings(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "settings", Table: "settings",
		Row: "signup_enabled", Field: "value", Value: "true",
	})
	if got := setting_get("signup_enabled", ""); got != "true" {
		t.Errorf("setting_get = %q, want %q", got, "true")
	}
}

// TestSystemSetApplySettingsDeleteOnEmpty: an empty value removes the row.
func TestSystemSetApplySettingsDeleteOnEmpty(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	db := db_open("db/settings.db")
	db.exec("replace into settings (name, value) values ('k', 'v')")

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "settings", Table: "settings",
		Row: "k", Field: "value", Value: "",
	})
	if exists, _ := db.exists("select 1 from settings where name='k'"); exists {
		t.Error("empty-value op should delete the row")
	}
}

// TestSystemSetApplyApps verifies apps.classes / services / paths
// dispatch and write correctly.
func TestSystemSetApplyApps(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	db_apps()

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "apps", Table: "classes",
		Row: "feed", Field: "app", Value: "feeds",
	})
	if got := apps_class_get("feed"); got != "feeds" {
		t.Errorf("classes apply: get = %q, want feeds", got)
	}

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "apps", Table: "services",
		Row: "feeds", Field: "app", Value: "feeds",
	})
	if got := apps_service_get("feeds"); got != "feeds" {
		t.Errorf("services apply: get = %q, want feeds", got)
	}

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "apps", Table: "paths",
		Row: "/feeds/", Field: "app", Value: "feeds",
	})
	if got := apps_path_get("/feeds/"); got != "feeds" {
		t.Errorf("paths apply: get = %q, want feeds", got)
	}
}

// TestSystemSetApplyAppsInstall: apps.apps install registry write.
func TestSystemSetApplyAppsInstall(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	db_apps()

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "apps", Table: "apps",
		Row: "feeds", Field: "installed", Value: "1234567890",
	})
	if got := apps_installed("feeds"); got != 1234567890 {
		t.Errorf("apps_installed = %d, want 1234567890", got)
	}
}

// TestSystemSetApplyRejectsUnknownDestination: dispatch warn-drops
// unknown destinations without affecting other tables.
func TestSystemSetApplyRejectsUnknownDestination(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	replication_system_set_apply("peer-A", &SystemSet{
		Database: "nope", Table: "nope",
		Row: "k", Field: "value", Value: "v",
	})
	db := db_open("db/settings.db")
	if exists, _ := db.exists("select 1 from settings where name='k'"); exists {
		t.Error("unknown destination should not touch settings")
	}
}

// TestSystemSetApplyRejectsMissingFields validates required-field
// gating: any missing key field silently drops the op.
func TestSystemSetApplyRejectsMissingFields(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	cases := []SystemSet{
		{Database: "", Table: "settings", Row: "k", Field: "value", Value: "v"},
		{Database: "settings", Table: "", Row: "k", Field: "value", Value: "v"},
		{Database: "settings", Table: "settings", Row: "", Field: "value", Value: "v"},
		{Database: "settings", Table: "settings", Row: "k", Field: "", Value: "v"},
	}
	for _, c := range cases {
		replication_system_set_apply("peer-A", &c)
	}
	db := db_open("db/settings.db")
	if exists, _ := db.exists("select 1 from settings where name='k'"); exists {
		t.Error("missing-field op should not write")
	}
}

// TestSettingSetEmits: setting_set fires the system-set emit with the
// expected arguments.
func TestSettingSetEmits(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	calls := 0
	orig := replication_emit_system_set
	replication_emit_system_set = func(database, table, row, field, value string) {
		calls++
		if database != "settings" || table != "settings" || row != "k" || field != "value" || value != "v" {
			t.Errorf("emit args: db=%q table=%q row=%q field=%q value=%q",
				database, table, row, field, value)
		}
	}
	defer func() { replication_emit_system_set = orig }()

	setting_set("k", "v")

	if calls != 1 {
		t.Errorf("emit calls = %d, want 1", calls)
	}
}

// TestAppsClassSetEmits: apps_class_set fires system-set.
func TestAppsClassSetEmits(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()

	calls := 0
	orig := replication_emit_system_set
	replication_emit_system_set = func(database, table, row, field, value string) {
		calls++
	}
	defer func() { replication_emit_system_set = orig }()

	apps_class_set("feed", "feeds")
	if calls != 1 {
		t.Errorf("emit calls = %d, want 1", calls)
	}
}

// setup_domains_test_schema creates a minimal domains.db schema for
// row-level tests.
func setup_domains_test_schema() {
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
}

// TestSystemRowApplyDomainsFresh: a row-level op for a new domain
// inserts cleanly.
func TestSystemRowApplyDomainsFresh(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "domains",
		Key: map[string]string{"domain": "example.com"},
		Cols: map[string]string{
			"verified": "0", "token": "tok123", "tls": "1",
			"created": "100", "updated": "100",
		},
	})
	db := db_open("db/domains.db")
	row, _ := db.row("select token from domains where domain='example.com'")
	if row == nil {
		t.Fatal("row should exist after apply")
	}
	if got, _ := row["token"].(string); got != "tok123" {
		t.Errorf("token = %q, want tok123", got)
	}
}

// TestSystemRowApplyDomainsReplacesExisting: a subsequent op
// overwrites the existing row (last-applier-wins).
func TestSystemRowApplyDomainsReplacesExisting(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()
	db := db_open("db/domains.db")
	db.exec("replace into domains (domain, verified, token, tls, created, updated) values ('example.com', 0, 'old', 1, 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "domains",
		Key: map[string]string{"domain": "example.com"},
		Cols: map[string]string{
			"verified": "1", "token": "new", "tls": "1",
			"created": "100", "updated": "200",
		},
	})
	row, _ := db.row("select token from domains where domain='example.com'")
	if got, _ := row["token"].(string); got != "new" {
		t.Errorf("token = %q, want new", got)
	}
}

// TestSystemRowApplyDomainsDelete: Delete=true removes the row.
func TestSystemRowApplyDomainsDelete(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()
	db := db_open("db/domains.db")
	db.exec("replace into domains (domain, verified, token, tls, created, updated) values ('example.com', 0, 't', 1, 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "domains",
		Key:    map[string]string{"domain": "example.com"},
		Delete: true,
	})
	if exists, _ := db.exists("select 1 from domains where domain='example.com'"); exists {
		t.Error("domain should be deleted after delete-op")
	}
}

// TestSystemRowApplyRoutes: composite-key route apply.
func TestSystemRowApplyRoutes(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path))")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "routes",
		Key: map[string]string{"domain": "example.com", "path": "/feeds"},
		Cols: map[string]string{
			"method": "app", "target": "feeds", "context": "",
			"owner": "u1", "priority": "10", "enabled": "1",
			"created": "100", "updated": "100",
		},
	})
	row, _ := domains.row("select target from routes where domain='example.com' and path='/feeds'")
	if got, _ := row["target"].(string); got != "feeds" {
		t.Errorf("target = %q, want feeds", got)
	}
}

// TestSystemRowApplyRoutesDelete: composite-key delete.
func TestSystemRowApplyRoutesDelete(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists routes (domain text not null, path text not null default '', method text not null default 'app', target text not null, context text not null default '', owner text not null default '', priority integer not null default 0, enabled integer not null default 1, created integer not null, updated integer not null, primary key (domain, path))")
	domains.exec("replace into routes (domain, path, method, target, context, owner, priority, enabled, created, updated) values ('example.com', '/x', 'app', 'wikis', '', '', 0, 1, 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "routes",
		Key:    map[string]string{"domain": "example.com", "path": "/x"},
		Delete: true,
	})
	if exists, _ := domains.exists("select 1 from routes where domain='example.com' and path='/x'"); exists {
		t.Error("route should be deleted")
	}
}

// TestSystemRowApplyAppsVersions: apps.versions row apply (single
// key, two data columns).
func TestSystemRowApplyAppsVersions(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	db_apps()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "apps", Table: "versions",
		Key:  map[string]string{"app": "feeds"},
		Cols: map[string]string{"version": "1.2.3", "track": "stable"},
	})
	db := db_apps()
	row, _ := db.row("select version, track from versions where app='feeds'")
	if row == nil {
		t.Fatal("versions row should exist after apply")
	}
	if got, _ := row["version"].(string); got != "1.2.3" {
		t.Errorf("version = %q, want 1.2.3", got)
	}
	if got, _ := row["track"].(string); got != "stable" {
		t.Errorf("track = %q, want stable", got)
	}
}

// TestSystemRowApplyAppsTracks: apps.tracks composite-key apply.
func TestSystemRowApplyAppsTracks(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	db_apps()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "apps", Table: "tracks",
		Key:  map[string]string{"app": "feeds", "track": "beta"},
		Cols: map[string]string{"version": "2.0.0-rc1"},
	})
	db := db_apps()
	row, _ := db.row("select version from tracks where app='feeds' and track='beta'")
	if got, _ := row["version"].(string); got != "2.0.0-rc1" {
		t.Errorf("version = %q, want 2.0.0-rc1", got)
	}
}

// TestSystemRowApplyDelegations: domains.delegations composite-key
// apply with timestamps.
func TestSystemRowApplyDelegations(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner text not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("insert into domains (domain, verified, token, tls, created, updated) values ('example.com', 1, 't', 1, 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "delegations",
		Key: map[string]string{"domain": "example.com", "path": "/feeds", "owner": "u1"},
		Cols: map[string]string{
			"created": "100", "updated": "100",
		},
	})
	row, _ := domains.row("select created, updated from delegations where domain='example.com' and path='/feeds' and owner='u1'")
	if row == nil {
		t.Fatal("delegation should exist after apply")
	}
	if got, _ := row["created"].(int64); got != 100 {
		t.Errorf("created = %d, want 100", got)
	}
}

// TestSystemRowApplyDelegationsDelete: composite-key delete.
func TestSystemRowApplyDelegationsDelete(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists delegations (id integer primary key, domain text not null, path text not null, owner text not null, created integer not null, updated integer not null, unique(domain, path, owner), foreign key (domain) references domains(domain) on delete cascade)")
	domains.exec("insert into domains (domain, verified, token, tls, created, updated) values ('example.com', 1, 't', 1, 100, 100)")
	domains.exec("insert into delegations (domain, path, owner, created, updated) values ('example.com', '/x', 'u1', 100, 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "delegations",
		Key:    map[string]string{"domain": "example.com", "path": "/x", "owner": "u1"},
		Delete: true,
	})
	if exists, _ := domains.exists("select 1 from delegations where domain='example.com'"); exists {
		t.Error("delegation should be deleted after delete-op")
	}
}

// TestSystemRowApplyRejectsMissingKey: empty key map drops silently.
func TestSystemRowApplyRejectsMissingKey(t *testing.T) {
	cleanup := setup_system_replication_test(t)
	defer cleanup()
	setup_domains_test_schema()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "domains", Table: "domains",
		Key:  map[string]string{},
		Cols: map[string]string{"verified": "1"},
	})
	// No write should happen.
	db := db_open("db/domains.db")
	rows, _ := db.rows("select 1 from domains")
	if len(rows) != 0 {
		t.Errorf("empty-key op should not write; got %d rows", len(rows))
	}
}

// setup_users_users_system_test seeds db/users.db with the columns the
// pair-only system-row path writes against. Matches setup_users_row_apply_test
// but lives in this file so the system-row tests don't depend on the
// other file's helper.
func setup_users_users_system_test(t *testing.T) (cleanup func(), uid string) {
	t.Helper()
	cleanup = setup_system_replication_test(t)
	setup_users_test_schema()
	uid = "uid-system-users"
	db_open("db/users.db").exec("insert into users (uid, username, role) values (?, ?, ?)", uid, "alice", "user")
	return
}

// TestSystemRowApplyUsersUsersRole: role applies via the pair-only
// system-row path.
func TestSystemRowApplyUsersUsersRole(t *testing.T) {
	cleanup, uid := setup_users_users_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "users", Table: "users",
		Key:  map[string]string{"uid": uid},
		Cols: map[string]string{"role": "administrator"},
	})
	row, _ := db_open("db/users.db").row("select role from users where uid=?", uid)
	if got, _ := row["role"].(string); got != "administrator" {
		t.Errorf("role = %q, want administrator", got)
	}
}

// TestSystemRowApplyUsersUsersUsername: username applies via the
// pair-only system-row path.
func TestSystemRowApplyUsersUsersUsername(t *testing.T) {
	cleanup, uid := setup_users_users_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "users", Table: "users",
		Key:  map[string]string{"uid": uid},
		Cols: map[string]string{"username": "alicia"},
	})
	row, _ := db_open("db/users.db").row("select username from users where uid=?", uid)
	if got, _ := row["username"].(string); got != "alicia" {
		t.Errorf("username = %q, want alicia", got)
	}
}

// TestSystemRowApplyUsersUsersIgnoresUnknownColumn: arbitrary columns
// outside the pair-scope whitelist are silently skipped. Prevents a
// misbehaving peer from injecting writes against (for example) status
// or preferences via the wrong pipeline.
func TestSystemRowApplyUsersUsersIgnoresUnknownColumn(t *testing.T) {
	cleanup, uid := setup_users_users_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "users", Table: "users",
		Key:  map[string]string{"uid": uid},
		Cols: map[string]string{"status": "suspended", "evil": "x"},
	})
	row, _ := db_open("db/users.db").row("select status from users where uid=?", uid)
	if got, _ := row["status"].(string); got == "suspended" {
		t.Error("status MUST NOT apply via the system-row path - per-user column")
	}
}

// TestSystemRowApplyUsersUsersMissingUID: an op without a uid key drops
// silently rather than UPDATE-ing every row.
func TestSystemRowApplyUsersUsersMissingUID(t *testing.T) {
	cleanup, uid := setup_users_users_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "users", Table: "users",
		Key:  map[string]string{},
		Cols: map[string]string{"role": "administrator"},
	})
	row, _ := db_open("db/users.db").row("select role from users where uid=?", uid)
	if got, _ := row["role"].(string); got == "administrator" {
		t.Error("missing-uid op MUST NOT promote the seeded user")
	}
}

// TestSystemRowApplyUsersUsersDeleteIsNoop: a delete-flag op against
// users.users is a no-op. User deletion is a server-pair operation,
// never a row replication op.
func TestSystemRowApplyUsersUsersDeleteIsNoop(t *testing.T) {
	cleanup, uid := setup_users_users_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "users", Table: "users",
		Key:    map[string]string{"uid": uid},
		Delete: true,
	})
	exists, _ := db_open("db/users.db").exists("select 1 from users where uid=?", uid)
	if !exists {
		t.Error("delete-op MUST NOT remove the user row")
	}
}

// TestSystemRowApplyUsersUsersCreatesMissingRow is the #34 receiver half: a
// pair users.users op carrying username (the only NOT-NULL-without-default
// column) creates the row if absent — so an entity-less user's bare-row seed
// lands on the partner. status is NOT carried, so the new row takes the schema
// default 'active'.
func TestSystemRowApplyUsersUsersCreatesMissingRow(t *testing.T) {
	cleanup, _ := setup_users_users_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "users", Table: "users",
		Key:  map[string]string{"uid": "uid-seed"},
		Cols: map[string]string{"username": "dzung@example.com", "role": "user"},
	})
	row, _ := db_open("db/users.db").row("select username, status from users where uid=?", "uid-seed")
	if row == nil {
		t.Fatal("entity-less bare-row seed did not create the user row")
	}
	if got, _ := row["username"].(string); got != "dzung@example.com" {
		t.Errorf("created username = %q, want dzung@example.com", got)
	}
	if got, _ := row["status"].(string); got != "active" {
		t.Errorf("created status = %q, want active (schema default)", got)
	}
}

// TestSystemRowApplyUsersUsersRoleOnlyDoesNotCreate: a change op without
// username can't satisfy username NOT NULL, so it stays update-only and never
// creates a row — only an existing row would get the role change.
func TestSystemRowApplyUsersUsersRoleOnlyDoesNotCreate(t *testing.T) {
	cleanup, _ := setup_users_users_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "users", Table: "users",
		Key:  map[string]string{"uid": "uid-absent"},
		Cols: map[string]string{"role": "administrator"},
	})
	if exists, _ := db_open("db/users.db").exists("select 1 from users where uid=?", "uid-absent"); exists {
		t.Error("role-only op (no username) MUST NOT create a row")
	}
}

// setup_documents_system_test seeds db/settings.db with the documents
// table the apply path writes against. Settings DB already exists from
// the parent helper.
func setup_documents_system_test(t *testing.T) func() {
	cleanup := setup_system_replication_test(t)
	db_open("db/settings.db").exec("create table if not exists documents ( name text not null, language text not null, body text not null, updated integer not null, primary key ( name, language ) )")
	return cleanup
}

// TestSystemRowApplySettingsDocumentsFresh: a brand-new
// (name, language) row lands on the receiver.
func TestSystemRowApplySettingsDocumentsFresh(t *testing.T) {
	cleanup := setup_documents_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "settings", Table: "documents",
		Key:  map[string]string{"name": "terms", "language": "en"},
		Cols: map[string]string{"body": "Custom operator terms.", "updated": "150"},
	})
	row, _ := db_open("db/settings.db").row("select body, updated from documents where name=? and language=?", "terms", "en")
	if row == nil {
		t.Fatal("documents row missing after apply")
	}
	if got, _ := row["body"].(string); got != "Custom operator terms." {
		t.Errorf("body = %q, want %q", got, "Custom operator terms.")
	}
	if got, _ := row["updated"].(int64); got != 150 {
		t.Errorf("updated = %d, want 150", got)
	}
}

// TestSystemRowApplySettingsDocumentsReplacesExisting: a subsequent
// op overwrites the row (LWW per name+language; later updated wins
// because the emitter is the operator).
func TestSystemRowApplySettingsDocumentsReplacesExisting(t *testing.T) {
	cleanup := setup_documents_system_test(t)
	defer cleanup()
	db := db_open("db/settings.db")
	db.exec("replace into documents (name, language, body, updated) values ('rules', 'fr', 'old', 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "settings", Table: "documents",
		Key:  map[string]string{"name": "rules", "language": "fr"},
		Cols: map[string]string{"body": "new", "updated": "200"},
	})
	row, _ := db.row("select body, updated from documents where name=? and language=?", "rules", "fr")
	if got, _ := row["body"].(string); got != "new" {
		t.Errorf("body = %q, want new", got)
	}
	if got, _ := row["updated"].(int64); got != 200 {
		t.Errorf("updated = %d, want 200", got)
	}
}

// TestSystemRowApplySettingsDocumentsDelete: Delete=true removes the
// row. Lets an operator revert a customised page back to the bundled
// default by removing the override on every paired host.
func TestSystemRowApplySettingsDocumentsDelete(t *testing.T) {
	cleanup := setup_documents_system_test(t)
	defer cleanup()
	db := db_open("db/settings.db")
	db.exec("replace into documents (name, language, body, updated) values ('privacy', 'en', 'override', 100)")

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "settings", Table: "documents",
		Key:    map[string]string{"name": "privacy", "language": "en"},
		Delete: true,
	})
	if exists, _ := db.exists("select 1 from documents where name=? and language=?", "privacy", "en"); exists {
		t.Error("document should be removed after delete-op")
	}
}

// TestDocumentSetEmits: an operator document_set fires a system-row
// op so the override reaches paired hosts.
func TestDocumentSetEmits(t *testing.T) {
	cleanup := setup_documents_system_test(t)
	defer cleanup()

	calls := 0
	orig := replication_emit_system_row
	replication_emit_system_row = func(database, table string, key, cols map[string]string, del bool) {
		calls++
		if database != "settings" || table != "documents" {
			t.Errorf("emit destination: db=%q table=%q", database, table)
		}
		if key["name"] != "terms" || key["language"] != "en" {
			t.Errorf("emit key: %v", key)
		}
		if cols["body"] != "Customised terms." {
			t.Errorf("emit body: %q", cols["body"])
		}
		if cols["updated"] == "" {
			t.Error("emit updated is empty")
		}
		if del {
			t.Error("emit delete=true on a set call")
		}
	}
	defer func() { replication_emit_system_row = orig }()

	if err := document_set("terms", "en", "Customised terms."); err != nil {
		t.Fatalf("document_set: %v", err)
	}
	if calls != 1 {
		t.Errorf("emit calls = %d, want 1", calls)
	}
}

// TestSystemRowApplySettingsDocumentsMissingKey: an op without both
// key parts drops silently rather than writing a degenerate row.
func TestSystemRowApplySettingsDocumentsMissingKey(t *testing.T) {
	cleanup := setup_documents_system_test(t)
	defer cleanup()

	replication_system_row_apply("peer-A", &SystemRow{
		Database: "settings", Table: "documents",
		Key:  map[string]string{"name": "terms"}, // language missing
		Cols: map[string]string{"body": "x", "updated": "1"},
	})
	rows, _ := db_open("db/settings.db").rows("select 1 from documents")
	if len(rows) != 0 {
		t.Errorf("missing-language op should not write; got %d rows", len(rows))
	}
}

// TestReplicationExecIdempotentReapply: a UNIQUE failure on the uid PK ("id") is
// classified as a benign idempotent re-apply (debug, advance); a UNIQUE failure
// on a SECONDARY column, and unrelated errors, are not — they stay warn().
func TestReplicationExecIdempotentReapply(t *testing.T) {
	benign := []string{
		"sqlite3: constraint failed: UNIQUE constraint failed: threads.id",
		"UNIQUE constraint failed: posts.id",
	}
	for _, m := range benign {
		if !replication_exec_idempotent_reapply(fmt.Errorf("%s", m)) {
			t.Errorf("expected benign re-apply for %q", m)
		}
	}
	notBenign := []string{
		"UNIQUE constraint failed: accounts.email", // secondary unique — possible real conflict
		"UNIQUE constraint failed: members.handle",
		"FOREIGN KEY constraint failed",
		"no such column: updated",
		"",
	}
	for _, m := range notBenign {
		if replication_exec_idempotent_reapply(fmt.Errorf("%s", m)) {
			t.Errorf("did not expect benign re-apply for %q", m)
		}
	}
	if replication_exec_idempotent_reapply(nil) {
		t.Error("nil error must not be a re-apply")
	}
}

// TestStallAlertSuppress: the stall alert is muted for a stream already marked
// irreparable (the gave-up alert covered it) and for a fully-defunct peer (an
// orphan the GC clears), but NOT for a peer still known via peers.db or a
// per-user host relationship.
func TestStallAlertSuppress(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}

	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists irreparable (peer text not null, scope text not null, user text not null default '', db text not null default '', reason text not null, since integer not null, notified integer not null default 0, primary key (peer, scope, user, db))")
	rdb.exec("create table if not exists hosts (user text not null, peer text not null, added integer not null, ack integer not null default 0, seen integer not null default 0, primary key (user, peer))")
	pdb := db_open("db/peers.db")
	pdb.exec("create table if not exists peers (id text not null, address text not null, updated integer not null, primary key (id, address))")

	stream := func(peer string) StalledStream {
		return StalledStream{Peer: peer, Scope: "app", User: "u1", Database: "app:feeds"}
	}

	if !stall_alert_suppress(stream("defunctpeer")) {
		t.Error("fully-defunct peer (nowhere) should be suppressed")
	}

	pdb.exec("insert into peers (id, address, updated) values ('knownpeer', '/ip4/1.2.3.4', 0)")
	if stall_alert_suppress(stream("knownpeer")) {
		t.Error("peer known in peers.db should NOT be suppressed")
	}

	rdb.exec("insert into hosts (user, peer, added) values ('u9', 'hostpeer', 0)")
	if stall_alert_suppress(stream("hostpeer")) {
		t.Error("per-user host peer should NOT be suppressed")
	}

	rdb.exec("insert into irreparable (peer, scope, user, db, reason, since) values ('irrpeer', 'app', 'u1', 'app:feeds', 'stalled', 0)")
	if !stall_alert_suppress(stream("irrpeer")) {
		t.Error("stream already marked irreparable should be suppressed")
	}

	// Self-healing transient streams (sessions/queue/peers) are always suppressed,
	// even from a known peer — they re-derive after loss, so a stall is benign.
	if !stall_alert_suppress(StalledStream{Peer: "knownpeer", Scope: "app", User: "u1", Database: "system:sessions"}) {
		t.Error("transient system:sessions stream should be suppressed")
	}
	// A non-transient system stream (a cold/critical DB) is NOT suppressed.
	if stall_alert_suppress(StalledStream{Peer: "knownpeer", Scope: "app", User: "u1", Database: "system:users"}) {
		t.Error("system:users (cold DB) should NOT be suppressed")
	}
}

// TestReplicationInboundReset (#34): after serving a peer's bootstrap, this
// host's inbound state for that peer (cursor + seen + pending) is cleared so its
// post-reset low-sequence writes are accepted — without touching any other
// peer's state.
func TestReplicationInboundReset(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")
	rdb.exec("create table if not exists seen (peer text not null, scope text not null, user text not null default '', sequence integer not null, applied integer not null, primary key (peer, scope, user, sequence))")
	rdb.exec("create table if not exists pending (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null, prev integer not null default 0, schema integer not null default 0, payload blob not null, received integer not null, primary key (peer, scope, user, sequence))")

	ins := func(peer string) {
		rdb.exec("insert into cursor (peer, scope, user, db, sequence) values (?, 'app', 'u1', 'app:x', 14255)", peer)
		rdb.exec("insert into seen (peer, scope, user, sequence, applied) values (?, 'app', 'u1', 5, 0)", peer)
		rdb.exec("insert into pending (peer, scope, user, db, sequence, payload, received) values (?, 'app', 'u1', 'app:x', 6, x'00', 0)", peer)
	}
	ins("peerA")
	ins("peerB")

	replication_inbound_reset(rdb, "peerA")

	for _, tbl := range []string{"cursor", "seen", "pending"} {
		if n := rdb.integer("select count(*) from " + tbl + " where peer='peerA'"); n != 0 {
			t.Errorf("%s: peerA rows not cleared (%d remain)", tbl, n)
		}
		if n := rdb.integer("select count(*) from " + tbl + " where peer='peerB'"); n != 1 {
			t.Errorf("%s: peerB rows wrongly affected (got %d, want 1)", tbl, n)
		}
	}
}

// TestReplicationCursorForce (#35): the snapshot/reseed cursor seed must override
// a stale (higher) cursor downward — where the monotonic replication_cursor_set
// would silently keep the stale value (the reseed-doesn't-re-anchor bug). The
// live _set must stay monotonic.
func TestReplicationCursorForce(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")

	seq := func() int {
		return rdb.integer("select sequence from cursor where peer='p' and scope='app' and user='u' and db='app:x'")
	}
	replication_cursor_set(rdb, "p", "app", "u", "app:x", 14255) // stale-high cursor from before a source reset
	replication_cursor_set(rdb, "p", "app", "u", "app:x", 453)   // monotonic: ignores the downward reseed
	if got := seq(); got != 14255 {
		t.Fatalf("monotonic _set should keep 14255 (got %d)", got)
	}
	replication_cursor_force(rdb, "p", "app", "u", "app:x", 453) // authoritative snapshot seed: rewinds
	if got := seq(); got != 453 {
		t.Fatalf("force should rewind to the snapshot point 453 (got %d)", got)
	}
}

// TestReplicationNewUserBootstrapMaybe (#38): a freshly-registered user with no
// local app data triggers a per-user bootstrap from the introducing peer; a user
// that already has app data does not; repeats are rate-limited; empty args no-op.
func TestReplicationNewUserBootstrapMaybe(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()

	var captured [][2]string
	origHook := replication_user_bootstrap_hook
	replication_user_bootstrap_hook = func(peer, uid string) { captured = append(captured, [2]string{peer, uid}) }
	defer func() { replication_user_bootstrap_hook = origHook }()
	reset := func() {
		captured = nil
		new_user_bootstrap_mutex.Lock()
		new_user_bootstrap_recent = map[string]int64{}
		new_user_bootstrap_mutex.Unlock()
	}

	// No local app data -> trigger a per-user bootstrap from the introducing peer.
	reset()
	replication_new_user_bootstrap_maybe("peerA", "u1")
	if len(captured) != 1 || captured[0] != [2]string{"peerA", "u1"} {
		t.Fatalf("absent app data should trigger bootstrap, got %v", captured)
	}

	// User already has app data (an app subdir under users/<uid>/) -> no trigger.
	reset()
	if err := os.MkdirAll(filepath.Join(data_dir, "users", "u2", "app1"), 0o755); err != nil {
		t.Fatal(err)
	}
	replication_new_user_bootstrap_maybe("peerA", "u2")
	if len(captured) != 0 {
		t.Fatalf("user with app data should not trigger, got %v", captured)
	}

	// Rate-limit: a second call for the same user within the cooldown is skipped.
	reset()
	replication_new_user_bootstrap_maybe("peerA", "u3")
	replication_new_user_bootstrap_maybe("peerA", "u3")
	if len(captured) != 1 {
		t.Fatalf("rate-limit should suppress the second trigger, got %v", captured)
	}

	// Empty peer or uid -> no-op.
	reset()
	replication_new_user_bootstrap_maybe("", "u4")
	replication_new_user_bootstrap_maybe("peerA", "")
	if len(captured) != 0 {
		t.Fatalf("empty peer/uid should be a no-op, got %v", captured)
	}
}

// TestReplicationStreamReached (#43): the cross-peer chain check — a stream's
// predecessor sequence counts as reached if ANY peer's cursor for that stream has
// reached it (multi-source relay), and only for the matching stream.
func TestReplicationStreamReached(t *testing.T) {
	orig := data_dir
	data_dir = t.TempDir()
	defer func() { data_dir = orig }()
	if err := os.MkdirAll(filepath.Join(data_dir, "db"), 0o755); err != nil {
		t.Fatal(err)
	}
	db := db_open("db/replication.db")
	db.exec("create table if not exists cursor (peer text not null, scope text not null, user text not null default '', db text not null default '', sequence integer not null default 0, primary key (peer, scope, user, db))")
	// Multi-source stream app:x: peerA (the pair) behind at 10, peerB (the
	// relay / per-user peer) reached 21 — the b8b3 shape.
	db.exec("insert into cursor (peer, scope, user, db, sequence) values ('peerA','app','u','app:x',10)")
	db.exec("insert into cursor (peer, scope, user, db, sequence) values ('peerB','app','u','app:x',21)")

	if !replication_stream_reached(db, "app", "u", "app:x", 21) {
		t.Fatal("seq 21 reached via peerB's cursor — chain should be satisfied")
	}
	if !replication_stream_reached(db, "app", "u", "app:x", 10) {
		t.Fatal("seq 10 (<= both cursors) should be satisfied")
	}
	if replication_stream_reached(db, "app", "u", "app:x", 22) {
		t.Fatal("seq 22 reached by no peer — should NOT be satisfied")
	}
	if replication_stream_reached(db, "app", "u", "app:y", 21) {
		t.Fatal("a different stream (app:y) must not be satisfied by app:x cursors")
	}
}
