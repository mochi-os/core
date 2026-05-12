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
// exercise the keys-transfer or session-replication apply paths. Includes
// the uid / user_uid columns from migration v51 so cross-host identifier
// lookups work.
func setup_users_test_schema() {
	users := db_open("db/users.db")
	users.exec("create table users (id integer primary key, uid text not null default '', username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")
	users.exec("create unique index users_uid on users (uid)")
	users.exec(`create trigger users_uid_insert after insert on users
		when new.uid is null or new.uid = ''
		begin
			update users set uid = lower(hex(randomblob(16))) where id = new.id;
		end`)
	users.exec("create table entities ( id text not null primary key, private text not null, fingerprint text not null, user references users( id ), user_uid text not null default '', parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0 )")
	users.exec("create index entities_user on entities( user )")
	users.exec("create index entities_user_uid on entities( user_uid )")
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
	sessions.exec("create table sessions (user integer not null, code text not null, secret text not null default '', expires integer not null, created integer not null default 0, accessed integer not null default 0, address text not null default '', agent text not null default '', primary key (user, code))")
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

func TestReplicationSessionApplyInsert(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	setup_sessions_test_schema()

	udb := db_open("db/users.db")
	udb.exec("insert into users (id, uid, username) values (1, 'uid-alice', 'alice@example.com')")

	p := &SessionInsert{
		UserUID: "uid-alice", Code: "sess-1", Secret: "secret-1",
		Expires: 100, Created: 50, Accessed: 50,
		Address: "127.0.0.1", Agent: "test",
	}
	replication_session_apply_insert(p)

	sdb := db_open("db/sessions.db")
	count := sdb.integer("select count(*) from sessions where code='sess-1' and user=1")
	if count != 1 {
		t.Errorf("expected session inserted with local user=1; got %d rows", count)
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
	udb.exec("insert into users (id, uid, username) values (1, 'uid-alice', 'alice@example.com')")

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

func TestUsersUIDMigrationBackfillAndTriggers(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_uid_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	// Simulate a pre-v51 database: users + entities with no uid columns.
	users := db_open("db/users.db")
	users.exec("create table users (id integer primary key, username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	users.exec("create unique index users_username on users (username)")
	users.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user references users( id ), parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")
	users.exec("create index entities_user on entities( user )")
	// Pre-existing rows that the migration must backfill.
	users.exec("insert into users (id, username) values (1, 'alice@example.com')")
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-alice', 'priv', 'fp1', 1, 'person', 'Alice')")
	// Tables that v51 also touches but without rows referencing the user.
	users.exec("create table credentials (id blob primary key, user integer not null, public_key blob not null, sign_count integer not null default 0, name text not null default '', transports text not null default '', backup_eligible integer not null default 0, backup_state integer not null default 0, created integer not null)")
	users.exec("create table recovery (id integer primary key, user integer not null, hash text not null, created integer not null)")
	users.exec("create table totp (user integer primary key, secret text not null, verified integer not null default 0, created integer not null)")
	users.exec("create table oauth (id integer primary key, user integer not null, provider text not null, subject text not null, email text not null default '', verified integer not null default 0, name text not null default '', created integer not null, unique(provider, subject))")
	users.exec("create table tokens (hash text primary key not null, user integer not null, app text not null, name text not null default '', scopes text not null default '', created integer not null, expires integer not null default 0)")

	db_upgrade_51()

	// users.uid backfilled for existing users.
	aliceUID := ""
	if row, _ := users.row("select uid from users where id=1"); row != nil {
		aliceUID, _ = row["uid"].(string)
	}
	if aliceUID == "" {
		t.Fatalf("v51 must backfill users.uid for existing rows")
	}

	// entities.user_uid backfilled from the join.
	if row, _ := users.row("select user_uid from entities where id='e-alice'"); row == nil {
		t.Fatalf("entities.user_uid query failed after v51")
	} else if v, _ := row["user_uid"].(string); v != aliceUID {
		t.Errorf("entities.user_uid not backfilled correctly: got %q want %q", v, aliceUID)
	}

	// New user insert via the auto-uid trigger gets a non-empty uid.
	users.exec("insert into users (id, username) values (2, 'bob@example.com')")
	bobUID := ""
	if row, _ := users.row("select uid from users where id=2"); row != nil {
		bobUID, _ = row["uid"].(string)
	}
	if bobUID == "" {
		t.Errorf("users_uid_insert trigger must populate uid on new rows")
	}
	if bobUID == aliceUID {
		t.Errorf("trigger-generated uid must be unique per row (got %q for both)", bobUID)
	}

	// New entity insert via the trigger gets user_uid populated from the join.
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values ('e-bob', 'priv2', 'fp2', 2, 'person', 'Bob')")
	if row, _ := users.row("select user_uid from entities where id='e-bob'"); row == nil {
		t.Fatalf("entities query failed for new row")
	} else if v, _ := row["user_uid"].(string); v != bobUID {
		t.Errorf("entities_user_uid_insert trigger must mirror users.uid: got %q want %q", v, bobUID)
	}

	// Re-running the migration on a partially-migrated DB is idempotent.
	db_upgrade_51()
	if row, _ := users.row("select uid from users where id=1"); row != nil {
		if v, _ := row["uid"].(string); v != aliceUID {
			t.Errorf("idempotent migration changed an already-backfilled uid (%q → %q)", aliceUID, v)
		}
	}
}

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
		Database: "sessions", Table: "sessions", Kind: repl_op_insert,
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
	udb.exec("insert into users (id, uid, username) values (42, 'uid-late', 'late@example.com')")

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
	count = sdb.integer("select count(*) from sessions where code='sess-late' and user=42")
	if count != 1 {
		t.Errorf("session must be in sessions.db after drain; got %d rows", count)
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
		Database: "sessions", Table: "sessions", Kind: repl_op_insert,
		Sequence: 1, Payload: cbor_encode(p),
	}
	db := db_open("db/replication.db")
	db.exec(
		"insert into pending (peer, scope, user, sequence, schema, payload, received) values ('origin', ?, ?, ?, ?, ?, ?)",
		op.Scope, op.User, op.Sequence, op.Schema, cbor_encode(op), now())

	// Keys-transfer lands the user. We have to use a real users.id that
	// keys-transfer will pick, then patch uid to match the pending op's
	// expected user_uid before triggering drain.
	a := test_entity_id('a')
	kt := &KeysTransfer{
		Username: "kuser@example.com",
		Entities: []KeysEntity{{ID: a, Private: "p", Fingerprint: "f", Class: "user", Name: "K"}},
	}
	if n := replication_keys_transfer_apply(a, "origin", kt); n != 1 {
		t.Fatalf("keys-transfer should insert 1 entity; got %d", n)
	}

	// Force the new user's uid to match what the pending op expects
	// (the auto-generated random uid won't match by chance).
	udb := db_open("db/users.db")
	udb.exec("update users set uid='uid-via-keys' where username='kuser@example.com'")

	// Trigger drain explicitly (keys-transfer already drained once before
	// our uid override; do it again now that the uid matches).
	replication_pending_drain()

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
	udb.exec("create table entities (id text primary key, user integer, user_uid text default '')")

	dir := db_open("db/directory.db")
	now_ts := now()
	dir.exec("insert into locations (entity, peer, seen) values ('e1', 'peerA', ?)", now_ts-100)
	dir.exec("insert into locations (entity, peer, seen) values ('e1', 'peerB', ?)", now_ts-50)
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
	udb.exec("insert into entities (id, user) values ('local-e', 1)")
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
