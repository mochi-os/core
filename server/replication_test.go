// Mochi server: Replication unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
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
	db_upgrade_55()
	db_upgrade_62()
	db_upgrade_66()
	db_upgrade_67()

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

	// Membership broadcast spawns send_peer goroutines that hit queue.db
	// — keep the local DB write side, drop the broadcast.
	orig_membership := replication_membership_update
	replication_membership_update = func(user string, hosts []string) {
		db := db_open("db/replication.db")
		db.exec("delete from hosts where user=?", user)
		for _, peer := range hosts {
			if peer == "" || peer == p2p_id {
				continue
			}
			db.exec("insert into hosts (user, peer, added, ack) values (?, ?, ?, 0)", user, peer, now())
		}
	}

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

	// db_upgrade_63 adds the bootstrap_served table that
	// replication_join_approve_core populates. Without it the
	// approve path errors out.
	db_upgrade_63()

	return func() {
		replication_bootstrap_emit_scope_done = orig_emit_bootstrap_scope_done
		replication_emit_system_set = orig_emit_system_set
		replication_emit_system_row = orig_emit_system_row
		replication_membership_update = orig_membership
		replication_emit_link_denied = orig_emit_link_denied
		replication_bootstrap_file_manifest_fetch = orig_file_manifest_fetch
		replication_bootstrap_db_manifest_fetch = orig_db_manifest_fetch
		bootstrap_file_chunk_fetch = orig_file_chunk_fetch
		bootstrap_file_scope_driver = orig_file_scope_driver
		bootstrap_db_fetch = orig_db_fetch
		bootstrap_db_scope_driver = orig_db_scope_driver
		replication_pair_backfill = orig_pair_backfill
		data_dir = orig_data_dir
		p2p_id = orig_p2p_id
		os.RemoveAll(tmp_dir)
	}
}

// setup_users_test_schema creates a minimal users.db schema for tests that
// exercise the keys-transfer or session-replication apply paths. Mirrors
// the v53 schema: uid is the PK on users, FKs reference users(uid).
func setup_users_test_schema() {
	users := db_open("db/users.db")
	users.exec("create table users (uid text not null primary key, username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
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

func TestReplicationMembershipApplyFresh(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	mc := &MembershipChange{User: "user1", Hosts: []string{"peerA", "peer_b"}, Sequence: 1}
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

	mc1 := &MembershipChange{User: "user1", Hosts: []string{"peerA", "peer_b"}, Sequence: 5}
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

	mc1 := &MembershipChange{User: "user1", Hosts: []string{"peerA", "peer_b"}, Sequence: 1}
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
	if p, _ := rows[1]["peer"].(string); p != "peer_b" {
		t.Errorf("expected peer_b in second row, got %q", p)
	}
}

func TestReplicationMembershipExcludesSelf(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	mc := &MembershipChange{User: "user1", Hosts: []string{"peerA", "self", "peer_b"}, Sequence: 1}
	replication_membership_apply("origin", mc)

	db := db_open("db/replication.db")
	count := db.integer("select count(*) from hosts where user='user1' and peer='self'")
	if count != 0 {
		t.Errorf("self peer must be filtered from hosts; got %d rows", count)
	}
	total := db.integer("select count(*) from hosts where user='user1'")
	if total != 2 {
		t.Errorf("expected 2 hosts (peerA, peer_b), got %d", total)
	}
}

// TestReplicationMembershipFullSetIncludesOrigin: the broadcast
// membership set must include this server. Callers pass the local
// hosts table, which never lists self — so the set the function emits
// must add p2p_id, or replicas applying the MembershipChange drop the
// origin and can no longer fan writes back to it.
func TestReplicationMembershipFullSetIncludesOrigin(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	// setup_replication_test sets p2p_id = "self".

	got := replication_membership_full_set([]string{"peerA", "peer_b"})
	if len(got) != 3 || got[0] != "self" {
		t.Fatalf("full set = %v, want [self peerA peer_b] (origin first)", got)
	}
	found := map[string]bool{}
	for _, p := range got {
		found[p] = true
	}
	if !found["self"] || !found["peerA"] || !found["peer_b"] {
		t.Errorf("full set %v missing an expected peer", got)
	}

	// Empty caller list still yields just the origin.
	if got := replication_membership_full_set(nil); len(got) != 1 || got[0] != "self" {
		t.Errorf("full set of nil = %v, want [self]", got)
	}

	// De-dupes (defensive — if a caller's list already had self or a
	// repeat) and drops blanks.
	got = replication_membership_full_set([]string{"self", "peerA", "", "peerA"})
	if len(got) != 2 || got[0] != "self" || got[1] != "peerA" {
		t.Errorf("full set with dups/blanks = %v, want [self peerA]", got)
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
		Payload:  cbor_encode(&SessionDelete{Code: "sess-G"}),
	}

	// Buffer them delete-first, so received order is the wrong order.
	// The insert is the Prev==0 stream start; the delete chains on it.
	for _, o := range []*ReplicationOp{del, insert} {
		db.exec(
			"insert into pending (peer, scope, user, db, sequence, prev, schema, payload, received) values (?, ?, ?, ?, ?, ?, 0, ?, ?)",
			"peerG", o.Scope, o.User, "sessions", o.Sequence, o.Prev, cbor_encode(o), now())
	}

	replication_pending_drain()

	sdb := db_open("db/sessions.db")
	if n := sdb.integer("select count(*) from sessions where code='sess-G'"); n != 0 {
		t.Errorf("session must be gone — delete must apply after insert; got %d rows", n)
	}
	if n := db.integer("select count(*) from pending where peer='peerG'"); n != 0 {
		t.Errorf("pending must be empty after in-order drain; got %d rows", n)
	}
	if seq, anchored := replication_cursor(db, "peerG", repl_scope_app, "uid-gate", "sessions"); !anchored || seq != 2 {
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
			"peerH", o.Scope, o.User, "sessions", o.Sequence, o.Prev, cbor_encode(o), now())
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
	if seq, _ := replication_cursor(db, "peerH", repl_scope_app, "uid-gap", "sessions"); seq != 1 {
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
	replication_cursor_set(db, "peerS", repl_scope_app, "uid-seam", "sessions", 5)

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
		"peerS", op.Scope, op.User, "sessions", op.Sequence, op.Prev, cbor_encode(op), now())

	replication_stream_drain(db, "peerS", repl_scope_app, "uid-seam", "sessions")

	sdb := db_open("db/sessions.db")
	if n := sdb.integer("select count(*) from sessions where code='sess-seam'"); n != 1 {
		t.Errorf("Prev==0 op must apply even on an anchored stream; got %d rows", n)
	}
	if seq, _ := replication_cursor(db, "peerS", repl_scope_app, "uid-seam", "sessions"); seq != 5 {
		t.Errorf("monotonic cursor must not rewind below 5; got %d", seq)
	}
}

// TestBootstrapStreamKey checks the DB-file → stream-key mapping that
// keeps the bootstrap cursor-seed per-physical-DB exact, including the
// 1:many app case (app data DB vs the per-app config DB app.db, which
// both travel under op.Database = app.id on the wire).
func TestBootstrapStreamKey(t *testing.T) {
	cases := []struct{ path, want string }{
		{"users/uid-1/feeds/db/feeds.db", "feeds"},
		{"users/uid-1/feeds/app.db", "feeds/app"},
		{"users/uid-1/user.db", "user"},
		{"users/uid-1/notifications.db", "notifications"},
		// An app whose data file is itself named app.db still keys on
		// the app (not the config-DB suffix) — keyed off path structure.
		{"users/uid-1/feeds/db/app.db", "feeds"},
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
		{"users/uid-a/feeds/db/feeds.db", "feeds", "uid-a"},
		{"users/uid-a/feeds/app.db", "feeds/app", "uid-a"},
		{"users/uid-b/user.db", "user", "uid-b"},
		{"users/uid-b/notifications.db", "notifications", "uid-b"},
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
			"src", o.Scope, o.User, "sessions", o.Sequence, o.Prev, cbor_encode(o), now())
	}

	// Before the seed: a drain can't place Prev>0 ops on an un-anchored
	// stream — they stay buffered.
	replication_stream_drain(db, "src", repl_scope_app, "uid-fresh", "sessions")
	sdb := db_open("db/sessions.db")
	if n := sdb.integer("select count(*) from sessions where code in ('s41','s42')"); n != 0 {
		t.Fatalf("un-seeded stream must not apply Prev>0 ops; got %d", n)
	}

	// The seed lands — cursor at the source's snapshot point, 40.
	replication_cursor_set(db, "src", repl_scope_app, "uid-fresh", "sessions", 40)
	replication_stream_drain(db, "src", repl_scope_app, "uid-fresh", "sessions")

	if n := sdb.integer("select count(*) from sessions where code in ('s41','s42')"); n != 2 {
		t.Errorf("after seeding cursor=40 the buffered ops must chain and apply; got %d", n)
	}
	if n := db.integer("select count(*) from pending where peer='src'"); n != 0 {
		t.Errorf("pending must be empty after drain; got %d", n)
	}
	if seq, _ := replication_cursor(db, "src", repl_scope_app, "uid-fresh", "sessions"); seq != 42 {
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
	orig_p2p := p2p_id
	p2p_id = "selfpeer"
	defer func() { p2p_id = orig_p2p }()

	// Set up the v52 directory schema.
	db_create_directory := func() {
		db := db_open("db/directory.db")
		db.exec("create table entities (id text not null primary key, name text not null, class text not null, location text not null default '', data text not null default '', fingerprint text not null default '', created integer not null, updated integer not null)")
		db.exec("create table locations (entity text not null, peer text not null, seen integer not null, primary key (entity, peer))")
		db.exec("create index locations_seen on locations(seen)")
	}
	db_create_directory()
	// Also need a users.db entities table so entity_peers can do its local check.
	udb := db_open("db/users.db")
	udb.exec("create table entities (id text primary key, user text not null default '')")

	dir := db_open("db/directory.db")
	now_ts := now()
	dir.exec("insert into locations (entity, peer, seen) values ('e1', 'peerA', ?)", now_ts-100)
	dir.exec("insert into locations (entity, peer, seen) values ('e1', 'peer_b', ?)", now_ts-50)
	dir.exec("insert into locations (entity, peer, seen) values ('e1', 'peerC', ?)", now_ts) // most recent

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
	dir.exec("insert into locations (entity, peer, seen) values ('e2', 'oldpeer', ?)", now_ts-31*86400)
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
// p2p_id in the test stub is "self"; alphabetic order: "aaa" < "self" < "zzz".
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
	db.exec("insert into leadership (scope, key, peer, expires, fence) values ('platform', 'k1', ?, ?, 1)", p2p_id, now()+60)

	// For each candidate, the vote outcome must equal the prefer
	// helper's decision (the source of truth for the tie-break).
	for _, candidate := range []string{"aaa", "zzz", "peerA", "peer_b", "peerC"} {
		want := replication_leader_prefer("platform", "k1", candidate, p2p_id)
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
	db.exec("insert into pair (peer, added) values (?, ?)", p2p_id, now()) // self should be filtered
	db.exec("insert into hosts (user, peer, added) values ('user-x', 'host-peer', ?)", now())
	db.exec("insert into hosts (user, peer, added) values ('user-x', ?, ?)", p2p_id, now()) // self filtered

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
// data_dir and p2p_id between turns. Replication ops "travel" between
// hosts by being constructed once and applied via replication_apply_op
// (or its underlying helpers) under each host's context. The real P2P
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
	orig_p2p := p2p_id
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
		p2p_id = h.id
		// Lazy-create the per-host replication schema on first use.
		db_upgrade_50()
	}

	cleanup := func() {
		data_dir = orig_data
		p2p_id = orig_p2p
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

func TestIntegrationMembershipChangePropagates(t *testing.T) {
	switch_to, cleanup := integration_setup(t)
	defer cleanup()

	// Host 1 announces a host set {peer1, peer2, peer3} for user alice.
	// Host 2 receives and replaces its local view.
	switch_to("h1")
	mc := &MembershipChange{User: "uid-alice", Hosts: []string{"peer1", "peer2", "peer3"}, Sequence: 5}
	replication_membership_apply("peer1", mc)
	db1 := db_open("db/replication.db")
	count := db1.integer("select count(*) from hosts where user='uid-alice'")
	// h1's peer (peer1=self on h1) is filtered out → 2 rows.
	if count != 2 {
		t.Errorf("h1 hosts count: expected 2 (self excluded), got %d", count)
	}

	switch_to("h2")
	replication_membership_apply("peer1", mc)
	db2 := db_open("db/replication.db")
	count = db2.integer("select count(*) from hosts where user='uid-alice'")
	// h2's peer (peer2=self on h2) is filtered out → 2 rows.
	if count != 2 {
		t.Errorf("h2 hosts count: expected 2 (self excluded), got %d", count)
	}

	// Stale change must not overwrite either host.
	switch_to("h1")
	mc2 := &MembershipChange{User: "uid-alice", Hosts: []string{"peer1"}, Sequence: 3}
	replication_membership_apply("peer1", mc2)
	count = db1.integer("select count(*) from hosts where user='uid-alice'")
	if count != 2 {
		t.Errorf("h1 host count after stale apply: expected 2, got %d", count)
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

func TestReplicationMembershipNewerOverwrites(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	mc1 := &MembershipChange{User: "user1", Hosts: []string{"peerA"}, Sequence: 1}
	replication_membership_apply("origin1", mc1)

	mc2 := &MembershipChange{User: "user1", Hosts: []string{"peer_b", "peerC"}, Sequence: 2}
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
	if got["peerA"] || !got["peer_b"] || !got["peerC"] {
		t.Errorf("newer state must replace older; got %v", got)
	}
}
