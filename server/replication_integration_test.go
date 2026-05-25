// Mochi server: cross-host integration tests for the new replication paths
// Copyright Alistair Cunningham 2026
//
// Uses the in-process `integration_setup` harness which switches data_dir
// and p2p_id between two simulated hosts (h1, h2). Each test arranges
// state on h1, manually constructs the replication op the emit path
// would produce, switches to h2, and asserts apply lands the change.
// The transport itself (signing, peer fan-out, recipients) is not in
// scope here — that's covered by separate unit tests.

package main

import (
	"testing"
)

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
