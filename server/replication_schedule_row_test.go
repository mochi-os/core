// Mochi server: Tests for schedule-row replication apply + emit paths
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

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
