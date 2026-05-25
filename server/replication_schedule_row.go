// Mochi server: schedule.db row replication
// Copyright Alistair Cunningham 2026
//
// Replicates inserts and deletes against db/schedule.db so paired
// hosts agree on what is scheduled. Without this, a schedule edit on
// host A (new recurring event, cancellation, etc.) never reaches its
// pair partner; if A then dies before per-user bootstrap re-runs, the
// schedule entry is lost and the existing leader-gate (which only
// works if both hosts have the same row) has nothing to fire.
//
// Recurring-event due advancement (schedule_claim's "due = due +
// interval" UPDATE) is NOT replicated. Both replicas hit the local
// schedule_due() at the same instant; the update is a deterministic
// function of (old_due, interval); both arrive at the same new value.
// Replicating it would multiply pair-broadcast traffic for no
// convergence benefit.
//
// Row identity: the schedule.id column is a local autoincrement and
// is not portable. We use the natural composite key (user, app, event,
// created) the per-user link bootstrap (KeysSchedule, see
// replication_link.go) already uses for idempotent re-application. A
// rapid-fire same-second duplicate (two mochi.schedule.after calls in
// the same second with the same event name from the same user/app) is
// indistinguishable on the wire and merges into one row at the
// receiver - documented limitation, acceptable because the semantics
// ("one reminder, not two") are defensible and the case is rare.
//
// System events (user == "") stay local: they have no scope identifier
// for the replication pipeline and the leader-gate handles cross-host
// firing dedup independently.

package main

import (
	"fmt"

	cbor "github.com/fxamacker/cbor/v2"
)

// ScheduleRow is the wire payload for one insert or delete against
// db/schedule.db. Key carries the natural composite identifier; Cols
// carries the mutable fields (omitted on delete). Wire shape mirrors
// UsersRow / SessionsRow.
type ScheduleRow struct {
	Key    map[string]string `cbor:"key"`
	Cols   map[string]string `cbor:"cols,omitempty"`
	Delete bool              `cbor:"delete,omitempty"`
}

// replication_emit_schedule_insert fans out a schedule insert to every
// host in the user's set (operator-paired members and per-user link
// partners both). Called from schedule_create after the local INSERT
// commits.
func replication_emit_schedule_insert(user, app string, due int64, event, data string, interval, created int64) {
	if user == "" || app == "" || event == "" {
		return
	}
	replication_emit_schedule_row(user, &ScheduleRow{
		Key: map[string]string{
			"user":    user,
			"app":     app,
			"event":   event,
			"created": fmt.Sprintf("%d", created),
		},
		Cols: map[string]string{
			"due":      fmt.Sprintf("%d", due),
			"data":     data,
			"interval": fmt.Sprintf("%d", interval),
		},
	})
}

// replication_emit_schedule_delete fans out a schedule delete keyed
// on the natural composite identifier. The caller looks up
// (user, app, event, created) before performing the local DELETE.
func replication_emit_schedule_delete(user, app, event string, created int64) {
	if user == "" || app == "" || event == "" {
		return
	}
	replication_emit_schedule_row(user, &ScheduleRow{
		Key: map[string]string{
			"user":    user,
			"app":     app,
			"event":   event,
			"created": fmt.Sprintf("%d", created),
		},
		Delete: true,
	})
}

// replication_emit_schedule_row is the underlying emit. Package-level
// variable so tests can stub.
var replication_emit_schedule_row = replication_emit_schedule_row_real

func replication_emit_schedule_row_real(user string, r *ScheduleRow) {
	if user == "" || r == nil {
		return
	}
	payload := cbor_encode(r)
	operation := "schedule-row.set"
	if r.Delete {
		operation = "schedule-row.delete"
	}
	replication_emit(user, &ReplicationOp{
		Scope:     repl_scope_app,
		User:      user,
		Database:  "schedule",
		Table:     "schedule",
		Operation: operation,
		Payload:   payload,
	})
}

// schedule_row_decode_and_apply decodes a ScheduleRow payload and
// dispatches to the apply path. Called from replication_apply_op.
func schedule_row_decode_and_apply(payload []byte, user string) ApplyResult {
	var r ScheduleRow
	if err := cbor.Unmarshal(payload, &r); err != nil {
		info("Replication schedule-row decode failed: %v", err)
		return ApplyInvalid
	}
	return replication_schedule_row_apply(user, &r)
}

// replication_schedule_row_apply lands a schedule insert or delete
// against db/schedule.db. Inserts use exists-check + INSERT (same
// pattern as the KeysSchedule bootstrap path) so a re-applied op or a
// row already imported by bootstrap is silently skipped. Deletes are
// unconditional on the natural key.
func replication_schedule_row_apply(userUID string, r *ScheduleRow) ApplyResult {
	if !user_exists(userUID) {
		return ApplyDeferred
	}
	user := r.Key["user"]
	app := r.Key["app"]
	event := r.Key["event"]
	created_raw := r.Key["created"]
	if user == "" || app == "" || event == "" || created_raw == "" {
		return ApplyInvalid
	}
	var created int64
	if _, err := fmt.Sscanf(created_raw, "%d", &created); err != nil {
		return ApplyInvalid
	}
	sdb := schedule_db()
	if r.Delete {
		sdb.exec("delete from schedule where user=? and app=? and event=? and created=?",
			user, app, event, created)
		return ApplyApplied
	}
	exists, _ := sdb.exists(
		"select 1 from schedule where user=? and app=? and event=? and created=?",
		user, app, event, created)
	if exists {
		return ApplyApplied
	}
	var due, interval int64
	_, _ = fmt.Sscanf(r.Cols["due"], "%d", &due)
	_, _ = fmt.Sscanf(r.Cols["interval"], "%d", &interval)
	sdb.exec(
		"insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, ?, ?, ?, ?)",
		user, app, due, event, r.Cols["data"], interval, created)
	schedule_notify()
	return ApplyApplied
}
