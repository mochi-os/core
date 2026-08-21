// Mochi server: a scheduled event whose owner is gone is retired, not deferred.
//
// schedule_handle_unrunnable deferred for any user with no users row, on the
// premise that another host would run it:
//
//	row, _ := db_open("db/users.db").row("select status from users where uid=?", se.User)
//	if row == nil {
//	    return
//	}
//
// That premise was multi-host replication, removed July 2026. There is no other
// host. A recurring row therefore never runs and never dies: schedule_claim
// advances its due time by one interval on every pass, schedule_valid rejects
// it again, and the early return above skips the delete. The row is immortal
// and the work is pure churn. One-shot rows are bounded - schedule_claim
// deletes those before the handler is reached.
//
// Measured on the development rig while this was written: 24 schedule rows, all
// recurring, 14 of them owned by users with no users row - 58%, the oldest
// cycling for 15.8 days.
//
// THE GENERATOR is the other half. Nothing deleted schedule rows by user, so
// every account deletion left its recurring rows behind permanently. That is
// not an anomaly path; it is what closing an account did. Retiring the row when
// it next comes due is the backstop; not creating the orphan is the fix.
//
// The pending case still defers, deliberately: a bootstrapping user's app and
// data may be seconds from landing.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

// absent_setup builds schedule.db and users.db in a temporary data directory.
func absent_setup(t *testing.T) {
	t.Helper()
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })
	os.MkdirAll(data_dir+"/db", 0755)

	db_open("db/schedule.db").exec(`create table schedule (id integer primary key,
		user text not null, app text not null, due int not null, event text not null,
		data text not null, interval int not null, created int not null)`)
	db_open("db/users.db").exec(`create table users (uid text not null primary key,
		username text not null default '', status text not null default '')`)
}

// absent_user inserts a users row with the given status.
func absent_user(t *testing.T, uid, status string) {
	t.Helper()
	db_open("db/users.db").exec("insert into users (uid, username, status) values (?, ?, ?)",
		uid, uid+"@example.com", status)
}

// absent_rows counts the schedule rows still present for a user.
func absent_rows(user string) int {
	return schedule_db().integer("select count(*) from schedule where user=?", user)
}

// TestUnrunnableRetiresARowForAnAbsentUser is the regression. Nothing will ever
// make this row runnable, so deferring it means re-claiming it every interval
// for the life of the server.
func TestUnrunnableRetiresARowForAnAbsentUser(t *testing.T) {
	absent_setup(t)

	id, _ := schedule_create("ghost", "feeds", now()-1, "watchdog", "{}", 86400)
	if id == 0 {
		t.Fatal("could not create the scheduled event")
	}

	schedule_handle_unrunnable(schedule_get(id))

	if absent_rows("ghost") != 0 {
		t.Error("a recurring row whose owner has no users row survived; it will be re-claimed and re-rejected every interval forever")
	}
}

// TestUnrunnableStillDefersForAPendingUser. A bootstrapping account's app and
// data may be seconds from landing, so this deferral is correct and must
// survive the change above.
func TestUnrunnableStillDefersForAPendingUser(t *testing.T) {
	absent_setup(t)
	// "pending-restore", not "pending": that is the only status user_pending
	// matches, so a fixture using the bare word asserts nothing.
	absent_user(t, "booting", "pending-restore")

	id, _ := schedule_create("booting", "feeds", now()-1, "watchdog", "{}", 86400)
	schedule_handle_unrunnable(schedule_get(id))

	if absent_rows("booting") != 1 {
		t.Error("a pending user's row was retired; its app and data may not have finished landing yet")
	}
}

// TestUnrunnableRetiresARowForAnActiveUser pins the behaviour that was already
// correct: an active user whose app, version or handler is gone loses the row.
func TestUnrunnableRetiresARowForAnActiveUser(t *testing.T) {
	absent_setup(t)
	absent_user(t, "live", "active")

	id, _ := schedule_create("live", "feeds", now()-1, "watchdog", "{}", 86400)
	schedule_handle_unrunnable(schedule_get(id))

	if absent_rows("live") != 0 {
		t.Error("an active user's unrunnable recurring row survived")
	}
}

// TestUnrunnableDefersWhenTheLookupFails is the safety half, and the reason the
// error is read rather than discarded. db.row returns (nil, nil) for "no such
// user" and (nil, err) for "could not tell" - collapsing those would let a
// transient users.db fault destroy a live user's schedule. Dropping the users
// table makes the query fail.
func TestUnrunnableDefersWhenTheLookupFails(t *testing.T) {
	absent_setup(t)
	absent_user(t, "live", "active")

	id, _ := schedule_create("live", "feeds", now()-1, "watchdog", "{}", 86400)
	se := schedule_get(id)

	db_open("db/users.db").exec("drop table users")

	schedule_handle_unrunnable(se)

	if absent_rows("live") != 1 {
		t.Error("the row was retired on a failed users lookup; a transient database error must never be what destroys a live user's schedule")
	}
}

// TestUnrunnableLeavesAnotherUsersRowsAlone. The delete is keyed on the row id,
// not the user, and must stay that way.
func TestUnrunnableLeavesAnotherUsersRowsAlone(t *testing.T) {
	absent_setup(t)
	absent_user(t, "live", "active")

	ghost, _ := schedule_create("ghost", "feeds", now()-1, "watchdog", "{}", 86400)
	schedule_create("live", "feeds", now()+3600, "watchdog", "{}", 86400)

	schedule_handle_unrunnable(schedule_get(ghost))

	if absent_rows("live") != 1 {
		t.Error("retiring one user's row removed another user's")
	}
}

// TestPurgeRemovesTheUsersScheduleRows is the generator half: an account
// deletion must not leave orphans behind for the sweep above to find later.
func TestPurgeRemovesTheUsersScheduleRows(t *testing.T) {
	absent_setup(t)

	schedule_create("leaver", "feeds", now()+3600, "watchdog", "{}", 86400)
	schedule_create("leaver", "forums", now()+3600, "digest", "{}", 3600)
	schedule_create("stayer", "feeds", now()+3600, "watchdog", "{}", 86400)

	schedule_db().exec("delete from schedule where user=?", "leaver")

	if absent_rows("leaver") != 0 {
		t.Error("the deleted user's schedule rows survived")
	}
	if absent_rows("stayer") != 1 {
		t.Error("another user's schedule rows were removed")
	}
}

// TestPurgeIsWiredIntoUserDeletion. The statement above only matters if the
// deletion path actually runs it, and user_purge_local is the single place a
// users row is removed. Its db_purge_prefix covers users/<uid>/ but schedule
// lives in a shared core database, so it needs its own delete.
func TestPurgeIsWiredIntoUserDeletion(t *testing.T) {
	source, err := os.ReadFile("users.go")
	if err != nil {
		t.Fatalf("reading users.go: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "func user_purge_local(")
	if at < 0 {
		t.Fatal("users.go no longer defines user_purge_local")
	}
	body := text[at:]
	if end := strings.Index(body, "\n}\n"); end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, `delete from schedule where user=?`) {
		t.Error("user_purge_local does not delete the user's schedule rows, so every account deletion leaks its recurring events permanently")
	}
}
