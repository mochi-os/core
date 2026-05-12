// Mochi server: Replication unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"encoding/json"
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

func TestCounterLocalApplyConverges(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_counter_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/test.db")

	// Two hosts each apply some adds.
	counter_local_apply(db, "votes", "peerA", 5)
	counter_local_apply(db, "votes", "peerA", 3)   // +8 on peerA's slot
	counter_local_apply(db, "votes", "peerB", -2)  // -2 on peerB
	counter_local_apply(db, "votes", "peerB", 10)  // +10 on peerB
	counter_local_apply(db, "votes", "peerC", -1)  // -1 on peerC
	// Final logical value: 8 + 8 - 1 = 15

	n := db.integer("select coalesce(sum(pos) - sum(neg), 0) from _counters where name='votes'")
	if n != 15 {
		t.Errorf("expected counter value 15 (8 + 8 - 1), got %d", n)
	}

	// Each peer has its own slot.
	count := db.integer("select count(*) from _counters where name='votes'")
	if count != 3 {
		t.Errorf("expected 3 per-peer rows, got %d", count)
	}

	// pos and neg are separately tracked.
	row, _ := db.row("select pos, neg from _counters where name='votes' and peer='peerB'")
	if row == nil {
		t.Fatal("missing peerB row")
	}
	if p, _ := row["pos"].(int64); p != 10 {
		t.Errorf("peerB pos: expected 10, got %d", p)
	}
	if neg, _ := row["neg"].(int64); neg != 2 {
		t.Errorf("peerB neg: expected 2, got %d", neg)
	}
}

func TestCounterLocalApplyCommutative(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_counter_comm")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	// Same set of deltas applied in two different orders to two DBs
	// must produce the same final value (PN-counter convergence).
	deltas := []struct {
		peer  string
		delta int64
	}{
		{"a", 5}, {"b", -3}, {"c", 7}, {"a", -2}, {"b", 4}, {"c", -1},
	}

	dbA := db_open("db/a.db")
	for _, d := range deltas {
		counter_local_apply(dbA, "x", d.peer, d.delta)
	}

	dbB := db_open("db/b.db")
	// Reverse order
	for i := len(deltas) - 1; i >= 0; i-- {
		d := deltas[i]
		counter_local_apply(dbB, "x", d.peer, d.delta)
	}

	a := dbA.integer("select coalesce(sum(pos) - sum(neg), 0) from _counters where name='x'")
	b := dbB.integer("select coalesce(sum(pos) - sum(neg), 0) from _counters where name='x'")
	if a != b {
		t.Errorf("non-commutative: forward=%d reverse=%d", a, b)
	}
	if a != 10 {
		t.Errorf("expected 10 (5-3+7-2+4-1), got %d", a)
	}
}

func TestLWWLocalApplyHigherWins(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_lww_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/test.db")

	// Initial write
	lww_local_apply(db, "settings", "u1", "theme", "dark", 100, "peerA")
	row, _ := db.row("select value from _lww where tbl='settings' and row='u1' and field='theme'")
	if v, _ := row["value"].(string); v != "dark" {
		t.Errorf("initial value: expected 'dark', got %q", v)
	}

	// Earlier timestamp must NOT overwrite.
	lww_local_apply(db, "settings", "u1", "theme", "light", 50, "peerB")
	row, _ = db.row("select value from _lww where tbl='settings' and row='u1' and field='theme'")
	if v, _ := row["value"].(string); v != "dark" {
		t.Errorf("older write must not overwrite: got %q", v)
	}

	// Later timestamp DOES overwrite.
	lww_local_apply(db, "settings", "u1", "theme", "auto", 200, "peerC")
	row, _ = db.row("select value from _lww where tbl='settings' and row='u1' and field='theme'")
	if v, _ := row["value"].(string); v != "auto" {
		t.Errorf("newer write must overwrite: got %q", v)
	}
}

func TestLWWLocalApplyPeerTiebreak(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_lww_tie")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	db := db_open("db/test.db")

	// Same timestamp, different peer: higher peer-id (lex) wins.
	lww_local_apply(db, "t", "r", "f", "valueA", 100, "peerA")
	lww_local_apply(db, "t", "r", "f", "valueB", 100, "peerB")
	row, _ := db.row("select value, peer from _lww where tbl='t' and row='r' and field='f'")
	if v, _ := row["value"].(string); v != "valueB" {
		t.Errorf("peer tiebreak: expected 'valueB' (higher peer-id), got %q", v)
	}

	// peerB already won; peerA at the same ts must NOT overwrite.
	lww_local_apply(db, "t", "r", "f", "valueA-again", 100, "peerA")
	row, _ = db.row("select value from _lww where tbl='t' and row='r' and field='f'")
	if v, _ := row["value"].(string); v != "valueB" {
		t.Errorf("losing peer at same ts must not overwrite: got %q", v)
	}
}

func TestLWWLocalApplyConvergesUnderReorder(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_lww_reorder")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig_data_dir := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig_data_dir }()

	type op struct {
		value string
		ts    int64
		peer  string
	}
	ops := []op{
		{"a", 100, "p1"},
		{"b", 200, "p2"},
		{"c", 150, "p3"},
		{"d", 200, "p1"}, // same ts as b, lower peer
		{"e", 300, "p2"},
	}

	dbForward := db_open("db/forward.db")
	for _, o := range ops {
		lww_local_apply(dbForward, "t", "r", "f", o.value, o.ts, o.peer)
	}

	dbReverse := db_open("db/reverse.db")
	for i := len(ops) - 1; i >= 0; i-- {
		o := ops[i]
		lww_local_apply(dbReverse, "t", "r", "f", o.value, o.ts, o.peer)
	}

	rowF, _ := dbForward.row("select value from _lww where tbl='t' and row='r' and field='f'")
	rowR, _ := dbReverse.row("select value from _lww where tbl='t' and row='r' and field='f'")
	if rowF == nil || rowR == nil {
		t.Fatal("missing row in one of the DBs")
	}
	vF, _ := rowF["value"].(string)
	vR, _ := rowR["value"].(string)
	if vF != vR {
		t.Errorf("non-convergent under reorder: forward=%q reverse=%q", vF, vR)
	}
	// Expected winner: ts=300/p2 ("e"). It dominates everything.
	if vF != "e" {
		t.Errorf("expected winner 'e' (highest ts), got %q", vF)
	}
}

func TestLeaderClaimFromVacant(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	if !replication_leader_claim("user:u1", "k1") {
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

	if !replication_leader_claim("platform", "tick") {
		t.Fatal("first claim failed")
	}
	if !replication_leader_claim("platform", "tick") {
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

	if replication_leader_claim("user:u", "job") {
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

	if !replication_leader_claim("user:u", "job") {
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

	replication_leader_claim("scope", "key")
	if f := replication_leader_fence("scope", "key"); f != 1 {
		t.Errorf("fence after first claim: expected 1, got %d", f)
	}

	replication_leader_release("scope", "key")
	if f := replication_leader_fence("scope", "key"); f != 0 {
		t.Errorf("fence after release must be 0, got %d", f)
	}

	// Re-claim after release succeeds and starts a fresh fence.
	if !replication_leader_claim("scope", "key") {
		t.Errorf("re-claim after release must succeed")
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
	if n := broadcast_next_local(db, "key1", "peerB"); n != 1 {
		t.Errorf("key1/peerB first (independent of peerA): got %d", n)
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
	incoming := 5
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

	os.MkdirAll(filepath.Join(tmp_dir, "users/1"), 0755)
	user := &User{ID: 1}

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

	os.MkdirAll(filepath.Join(tmp_dir, "users/1"), 0755)
	user := &User{ID: 1}

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

	hosts := map[string]struct {
		dir string
		id  string
	}{
		"h1": {dir1, "peer1"},
		"h2": {dir2, "peer2"},
	}

	switchTo := func(name string) {
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
		os.RemoveAll(dir1)
		os.RemoveAll(dir2)
	}
	return switchTo, cleanup
}

func TestIntegrationCounterConvergesAcrossTwoHosts(t *testing.T) {
	switchTo, cleanup := integration_setup(t)
	defer cleanup()

	// Host 1 increments by 5, Host 2 decrements by 3 — independent local
	// writes plus a replication op exchange. Both hosts must converge to
	// the same logical value (5 - 3 = 2).
	switchTo("h1")
	db1 := db_open("db/test-app.db")
	counter_local_apply(db1, "votes", "peer1", 5)

	switchTo("h2")
	db2 := db_open("db/test-app.db")
	counter_local_apply(db2, "votes", "peer2", -3)

	// Replication: each host's local delta arrives at the other.
	switchTo("h2")
	db2 = db_open("db/test-app.db")
	counter_local_apply(db2, "votes", "peer1", 5)

	switchTo("h1")
	db1 = db_open("db/test-app.db")
	counter_local_apply(db1, "votes", "peer2", -3)

	v1 := db1.integer("select coalesce(sum(pos) - sum(neg), 0) from _counters where name='votes'")
	switchTo("h2")
	db2 = db_open("db/test-app.db")
	v2 := db2.integer("select coalesce(sum(pos) - sum(neg), 0) from _counters where name='votes'")

	if v1 != 2 {
		t.Errorf("host1 value: expected 2, got %d", v1)
	}
	if v2 != 2 {
		t.Errorf("host2 value: expected 2, got %d", v2)
	}
	if v1 != v2 {
		t.Errorf("hosts disagree: h1=%d h2=%d", v1, v2)
	}
}

func TestIntegrationLWWConvergesAcrossTwoHosts(t *testing.T) {
	switchTo, cleanup := integration_setup(t)
	defer cleanup()

	// Host 1 writes "dark" at ts=100; Host 2 writes "light" at ts=200.
	// After cross-replication both hosts see "light" (higher ts).
	switchTo("h1")
	db1 := db_open("db/test-app.db")
	lww_local_apply(db1, "settings", "u1", "theme", "dark", 100, "peer1")

	switchTo("h2")
	db2 := db_open("db/test-app.db")
	lww_local_apply(db2, "settings", "u1", "theme", "light", 200, "peer2")

	// Cross-replicate.
	switchTo("h1")
	db1 = db_open("db/test-app.db")
	lww_local_apply(db1, "settings", "u1", "theme", "light", 200, "peer2")
	switchTo("h2")
	db2 = db_open("db/test-app.db")
	lww_local_apply(db2, "settings", "u1", "theme", "dark", 100, "peer1")

	row1, _ := db1.row("select value from _lww where tbl='settings' and row='u1' and field='theme'")
	switchTo("h2")
	db2 = db_open("db/test-app.db")
	row2, _ := db2.row("select value from _lww where tbl='settings' and row='u1' and field='theme'")

	v1, _ := row1["value"].(string)
	v2, _ := row2["value"].(string)
	if v1 != "light" {
		t.Errorf("h1 winner: expected 'light' (ts=200), got %q", v1)
	}
	if v2 != "light" {
		t.Errorf("h2 winner: expected 'light', got %q", v2)
	}
	if v1 != v2 {
		t.Errorf("hosts disagree: h1=%q h2=%q", v1, v2)
	}
}

func TestIntegrationKeysTransferThenSessionInsert(t *testing.T) {
	switchTo, cleanup := integration_setup(t)
	defer cleanup()

	// Host 1: alice exists locally and creates a session.
	switchTo("h1")
	setup_users_test_schema()
	setup_sessions_test_schema()
	udb1 := db_open("db/users.db")
	udb1.exec("insert into users (id, uid, username) values (1, 'uid-alice', 'alice@example.com')")
	a := test_entity_id('a')
	udb1.exec("insert into entities (id, private, fingerprint, user, user_uid, class, name, privacy) values (?, 'priv', 'fp1', 1, 'uid-alice', 'person', 'Alice', 'private')", a)

	// Host 2: receives keys-transfer for alice (introducing her).
	switchTo("h2")
	setup_users_test_schema()
	setup_sessions_test_schema()
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
	// Patch alice's uid on h2 to match the canonical uid so the session
	// op's user_uid resolves (in production the keys-transfer would
	// carry the canonical uid too; the wire format doesn't yet).
	udb2 := db_open("db/users.db")
	udb2.exec("update users set uid='uid-alice' where username='alice@example.com'")

	// Host 2 receives a session-insert op from Host 1.
	op := &ReplicationOp{
		Scope: repl_scope_app, User: "uid-alice",
		Database: "sessions", Table: "sessions", Kind: repl_op_insert,
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
	switchTo, cleanup := integration_setup(t)
	defer cleanup()

	// Host 1 announces a host set {peer1, peer2, peer3} for user alice.
	// Host 2 receives and replaces its local view.
	switchTo("h1")
	mc := &MembershipChange{User: "uid-alice", Hosts: []string{"peer1", "peer2", "peer3"}, Sequence: 5}
	replication_membership_apply("peer1", mc)
	db1 := db_open("db/replication.db")
	count := db1.integer("select count(*) from hosts where user='uid-alice'")
	// h1's peer (peer1=self on h1) is filtered out → 2 rows.
	if count != 2 {
		t.Errorf("h1 hosts count: expected 2 (self excluded), got %d", count)
	}

	switchTo("h2")
	replication_membership_apply("peer1", mc)
	db2 := db_open("db/replication.db")
	count = db2.integer("select count(*) from hosts where user='uid-alice'")
	// h2's peer (peer2=self on h2) is filtered out → 2 rows.
	if count != 2 {
		t.Errorf("h2 hosts count: expected 2 (self excluded), got %d", count)
	}

	// Stale change must not overwrite either host.
	switchTo("h1")
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

	os.MkdirAll(filepath.Join(tmp_dir, "users/1"), 0755)
	user := &User{ID: 1}

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

	os.MkdirAll(filepath.Join(tmp_dir, "users/1"), 0755)
	user := &User{ID: 1}

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
	if replication_fence_observe("credential:x", "fence", "peerB", 3) {
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
	if !replication_fence_observe("scope", "key", "peerB", 7) {
		t.Errorf("newer fence (7 > 3) must be accepted")
	}
	fence, peer := replication_fence_current("scope", "key")
	if fence != 7 || peer != "peerB" {
		t.Errorf("after newer observe: expected (7, peerB), got (%d, %q)", fence, peer)
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
	db.exec("insert into pair (peer, added) values ('peerB', ?)", now())
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

	pairList, _ := resp["pair"].([]any)
	if len(pairList) != 2 {
		t.Errorf("pair: expected 2 entries, got %d (%v)", len(pairList), pairList)
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
