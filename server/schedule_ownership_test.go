// Mochi server: a scheduled event belongs to one user, and a caller who is
// nobody is not that user.
//
// Both guards read `user != nil && se.User != user.UID`, so a nil user skipped
// the ownership test entirely and only the app test remained. An anonymous
// request reaches Starlark with no user on the thread - web_action and
// web_serve_file_with_opengraph both bind whatever web_auth returned, which is
// nil without a session - so any app serving a public action could read every
// other user's scheduled events for that app, or delete them. Row ids are
// sequential rowids and no schedule API requires a permission, so enumeration
// costs nothing.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// owner_test_thread builds the Starlark context an action runs under. Passing
// nil for user models the anonymous caller; absent and typed-nil are both
// exercised below because they are different shapes of the same thing.
func owner_test_thread(app string, user *User, absent bool) *sl.Thread {
	thread := &sl.Thread{Name: "test"}
	if !absent {
		thread.SetLocal("user", user)
	}
	thread.SetLocal("app", create_external_app(app))
	return thread
}

func owner_test_get(t *testing.T, thread *sl.Thread, id int64) sl.Value {
	t.Helper()
	value, err := api_schedule_get(thread, sl.NewBuiltin("schedule.get", nil), sl.Tuple{sl.MakeInt64(id)}, nil)
	if err != nil {
		t.Fatalf("schedule.get: %v", err)
	}
	return value
}

func owner_test_cancel(t *testing.T, thread *sl.Thread, id int64) sl.Value {
	t.Helper()
	value, err := api_schedule_cancel(thread, sl.NewBuiltin("schedule.cancel", nil), sl.Tuple{sl.MakeInt64(id)}, nil)
	if err != nil {
		t.Fatalf("schedule.cancel: %v", err)
	}
	return value
}

// owner_test_row schedules one event for a named user and returns its id.
func owner_test_row(t *testing.T, uid, app string) int64 {
	t.Helper()
	id := schedule_create(uid, app, now()+3600, "remind", `{"secret":"the payload"}`, 0)
	if id == 0 {
		t.Fatal("schedule_create returned 0")
	}
	return id
}

// TestScheduleGetRefusesAnAnonymousCaller is the finding: no user on the
// thread at all, which is what an anonymous request to a public action gives.
func TestScheduleGetRefusesAnAnonymousCaller(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	id := owner_test_row(t, "victim", "reader")
	got := owner_test_get(t, owner_test_thread("reader", nil, true), id)
	if got != sl.None {
		t.Errorf("an anonymous caller read another user's scheduled event: %v\nthe payload, due time and event name are all exposed, and ids are sequential", got)
	}
}

// TestScheduleGetRefusesATypedNilUser. web_action calls s.set("user", user)
// with a nil *User, so the thread local is a typed nil rather than absent -
// the shape that actually occurs in production.
func TestScheduleGetRefusesATypedNilUser(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	id := owner_test_row(t, "victim", "reader")
	var nobody *User
	if got := owner_test_get(t, owner_test_thread("reader", nobody, false), id); got != sl.None {
		t.Errorf("a typed-nil user read another user's scheduled event: %v", got)
	}
}

// TestScheduleCancelRefusesAnAnonymousCaller, and leaves the row alone. A
// refusal that still deletes would be worse than the read.
func TestScheduleCancelRefusesAnAnonymousCaller(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	id := owner_test_row(t, "victim", "reader")
	if got := owner_test_cancel(t, owner_test_thread("reader", nil, true), id); got != sl.False {
		t.Errorf("cancel returned %v for an anonymous caller", got)
	}
	if schedule_get(id) == nil {
		t.Error("the row was deleted anyway: an anonymous caller can destroy every user's scheduled events for the app")
	}
}

// TestScheduleRefusesAnotherUser. The arm that already worked, pinned so the
// fix did not trade one hole for another.
func TestScheduleRefusesAnotherUser(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	id := owner_test_row(t, "victim", "reader")
	stranger := &User{UID: "stranger", Username: "stranger@example.com"}
	if got := owner_test_get(t, owner_test_thread("reader", stranger, false), id); got != sl.None {
		t.Errorf("a different signed-in user read the event: %v", got)
	}
	if got := owner_test_cancel(t, owner_test_thread("reader", stranger, false), id); got != sl.False {
		t.Errorf("a different signed-in user cancelled the event: %v", got)
	}
}

// TestScheduleAllowsTheOwner is the control. Without it every test above
// passes on a build where nobody can read anything.
func TestScheduleAllowsTheOwner(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	owner := &User{UID: "victim", Username: "victim@example.com"}
	id := owner_test_row(t, owner.UID, "reader")

	if got := owner_test_get(t, owner_test_thread("reader", owner, false), id); got == sl.None {
		t.Fatal("the owner cannot read their own scheduled event")
	}
	if got := owner_test_cancel(t, owner_test_thread("reader", owner, false), id); got != sl.True {
		t.Fatalf("the owner cannot cancel their own scheduled event: %v", got)
	}
	if schedule_get(id) != nil {
		t.Error("cancel returned True but the row survives")
	}
}

// TestScheduleStillScopesByApp. The app test is the other half of the pair and
// must survive the change: the owner of an event must not reach it through a
// different app.
func TestScheduleStillScopesByApp(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	db_create()

	owner := &User{UID: "victim", Username: "victim@example.com"}
	id := owner_test_row(t, owner.UID, "reader")
	if got := owner_test_get(t, owner_test_thread("other", owner, false), id); got != sl.None {
		t.Errorf("a different app read the event: %v", got)
	}
}

// TestScheduleGuardsDoNotFailOpenOnNil pins the shape, because the failing
// form reads as a deliberate nil-check and would survive review again.
func TestScheduleGuardsDoNotFailOpenOnNil(t *testing.T) {
	body, err := os.ReadFile("schedule.go")
	if err != nil {
		t.Fatalf("read schedule.go: %v", err)
	}
	text := string(body)
	if strings.Contains(text, "user != nil && se.User != user.UID") {
		t.Error("a guard reads `user != nil && se.User != user.UID` again: that skips the ownership test for exactly the callers who own nothing")
	}
	for _, name := range []string{"api_schedule_get", "api_schedule_cancel"} {
		fn := text[strings.Index(text, "func "+name+"("):]
		fn = fn[:strings.Index(fn, "\n}")]
		if !strings.Contains(fn, "user == nil || se.User != user.UID") {
			t.Errorf("%s does not refuse a nil user", name)
		}
	}
}
