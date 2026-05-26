// Mochi server: per-DB row replication tests
// Copyright Alistair Cunningham 2026
//
// Tests for the apply + emit helpers in replication_rows.go plus the
// Go-side internal-exec replication path. Each table type's tests are
// grouped together; the shared setup_replication_test fixture comes
// from replication_test.go.

package main

import (
	"os"
	"testing"
)

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
