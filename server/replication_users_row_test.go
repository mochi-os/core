// Mochi server: Tests for users-row apply paths added for replication
// Copyright Alistair Cunningham 2026

package main

import (
	"os"
	"testing"
)

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

func TestReplicationUsersUsersApplyRole(t *testing.T) {
	cleanup, uid := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "users", Cols: map[string]string{"role": "administrator"}}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("role apply: want ApplyApplied, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select role from users where uid=?", uid)
	if got, _ := row["role"].(string); got != "administrator" {
		t.Errorf("role: want administrator, got %q", got)
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
	op = &UsersRow{Table: "users", Cols: map[string]string{"role": "administrator", "evil": "x"}}
	if got := replication_users_row_apply(uid, op); got != ApplyApplied {
		t.Fatalf("mixed: want ApplyApplied, got %v", got)
	}
	row, _ := db_open("db/users.db").row("select role from users where uid=?", uid)
	if got, _ := row["role"].(string); got != "administrator" {
		t.Errorf("role: want administrator, got %q", got)
	}
}

func TestReplicationUsersUsersApplyDeferUnknownUser(t *testing.T) {
	cleanup, _ := setup_users_row_apply_test(t)
	defer cleanup()

	op := &UsersRow{Table: "users", Cols: map[string]string{"role": "administrator"}}
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
