// Mochi server: the scheduler's payload and dispatch are bounded.
//
// db/schedule.db is a single global table shared by every user and app, and it
// sits outside the per-user storage quota, so an unbounded payload is
// server-wide disk an app fills for free.
//
// Dispatch was worse: schedule_due materialised every due row with no LIMIT and
// schedule_run_due did `go schedule_run(item)` per row, each of which spawns a
// watchdog goroutine of its own and then enters Starlark. N rows made due at
// one instant became 2N goroutines queueing on the 32-slot pool - and `at`
// accepts a past timestamp while `after` accepts a delay of zero, so arranging
// it needs no waiting.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

func schedule_bounds_setup(t *testing.T) *sl.Thread {
	t.Helper()
	data_dir = t.TempDir()
	os.MkdirAll(data_dir+"/db", 0755)
	db := db_open("db/schedule.db")
	db.exec("create table schedule (id integer primary key, user text not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	db.exec("create index schedule_due on schedule(due)")

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: "u-schedule"})
	thread.SetLocal("app", &App{id: "scheduler"})
	return thread
}

// schedule_bounds_payload builds a dict whose JSON encoding is at least size.
func schedule_bounds_payload(size int) *sl.Dict {
	data := sl.NewDict(1)
	_ = data.SetKey(sl.String("blob"), sl.String(strings.Repeat("x", size)))
	return data
}

// TestScheduleRejectsAnOversizedPayload covers all three entry points: they
// share one store, so a cap on only some of them is no cap at all.
func TestScheduleRejectsAnOversizedPayload(t *testing.T) {
	thread := schedule_bounds_setup(t)
	big := schedule_bounds_payload(schedule_data_maximum + 1024)

	for _, c := range []struct {
		name  string
		call  func(*sl.Builtin) (sl.Value, error)
		third sl.Value
	}{
		{"at", nil, sl.MakeInt64(now() + 3600)},
		{"after", nil, sl.MakeInt64(60)},
		{"every", nil, sl.MakeInt64(3600)},
	} {
		var handler func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error)
		switch c.name {
		case "at":
			handler = api_schedule_at
		case "after":
			handler = api_schedule_after
		case "every":
			handler = api_schedule_every
		}
		fn := sl.NewBuiltin("mochi.schedule."+c.name, handler)
		_, err := handler(thread, fn, sl.Tuple{sl.String("evt"), big, c.third}, nil)
		if err == nil {
			t.Errorf("%s accepted a payload past the cap", c.name)
			continue
		}
		if !strings.Contains(err.Error(), "data too large") {
			t.Errorf("%s refused with %q, want the size error", c.name, err)
		}
	}

	// Nothing was stored.
	if n := schedule_db().integer("select count(*) from schedule"); n != 0 {
		t.Errorf("%d rows written for refused payloads", n)
	}
}

// TestScheduleAcceptsAnOrdinaryPayload. The cap must not disturb what a
// scheduled event is actually for - carrying the ids a handler needs.
func TestScheduleAcceptsAnOrdinaryPayload(t *testing.T) {
	thread := schedule_bounds_setup(t)
	data := sl.NewDict(2)
	_ = data.SetKey(sl.String("feed"), sl.String("12abc"))
	_ = data.SetKey(sl.String("post"), sl.MakeInt64(42))

	fn := sl.NewBuiltin("mochi.schedule.after", api_schedule_after)
	if _, err := api_schedule_after(thread, fn, sl.Tuple{sl.String("evt"), data, sl.MakeInt64(60)}, nil); err != nil {
		t.Fatalf("an ordinary payload was refused: %v", err)
	}
	if n := schedule_db().integer("select count(*) from schedule"); n != 1 {
		t.Errorf("%d rows stored, want 1", n)
	}
}

// TestScheduleDueIsBounded. The query used to materialise every due row.
func TestScheduleDueIsBounded(t *testing.T) {
	schedule_bounds_setup(t)
	past := now() - 60
	for i := 0; i < schedule_due_maximum+50; i++ {
		if id, _ := schedule_create("u-schedule", "scheduler", past, "evt", "{}", 0); id == 0 {
			t.Fatalf("row %d not created", i)
		}
	}

	due := schedule_due(now())
	if len(due) > schedule_due_maximum {
		t.Errorf("schedule_due returned %d rows, above the cap of %d", len(due), schedule_due_maximum)
	}
	if len(due) != schedule_due_maximum {
		t.Errorf("schedule_due returned %d rows, want a full batch of %d", len(due), schedule_due_maximum)
	}
	// Capping is not dropping: the rest are still due for the next pass, which
	// the manager takes immediately because its sleep clamps to zero.
	if n := schedule_db().integer("select count(*) from schedule where due<=?", now()); n != schedule_due_maximum+50 {
		t.Errorf("%d rows still due, want all %d", n, schedule_due_maximum+50)
	}
}

// TestScheduleDispatchBlocksAtCapacity. The slot acquire is deliberately
// blocking: skipping at capacity would return to a manager whose next sleep is
// zero, because the skipped rows are still due - a busy spin. Blocking paces
// the loop instead.
func TestScheduleDispatchBlocksAtCapacity(t *testing.T) {
	schedule_bounds_setup(t)
	schedule_create("u-schedule", "scheduler", now()-60, "evt", "{}", 0)

	// Hold every slot, as long-running handlers would.
	for i := 0; i < schedule_concurrency; i++ {
		schedule_slots <- struct{}{}
	}

	done := make(chan struct{})
	go func() {
		schedule_run_due(time.Now())
		close(done)
	}()

	select {
	case <-done:
		// Drain whatever the dispatch left behind before failing.
		for len(schedule_slots) > 0 {
			<-schedule_slots
		}
		t.Fatal("dispatch proceeded with every slot held; concurrency is not bounded")
	case <-time.After(250 * time.Millisecond):
	}

	// Free one slot and it completes.
	<-schedule_slots
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not resume after a slot was freed")
	}

	// schedule_run_due returns as soon as it has ACQUIRED the slot and
	// spawned the handler, so the handler outlives it - and it opens
	// databases under the t.TempDir() this test is about to remove. The
	// handler's slot release is the completion signal: schedule_run_due
	// defers it until schedule_run has fully returned, and schedule_run's
	// only inner goroutine is the stuck-watchdog, which touches no storage.
	// Without this wait the suite fails intermittently on "TempDir RemoveAll
	// cleanup: directory not empty" rather than on any assertion.
	deadline := time.Now().Add(5 * time.Second)
	for len(schedule_slots) > schedule_concurrency-1 {
		if time.Now().After(deadline) {
			t.Fatal("the dispatched handler never released its slot")
		}
		time.Sleep(time.Millisecond)
	}

	for len(schedule_slots) > 0 {
		<-schedule_slots
	}
}

// TestScheduleConcurrencyLeavesRoomForRequests. The bound is only useful if it
// sits below the Starlark pool: at or above it, a batch of scheduled handlers
// could still take every slot and starve interactive traffic, which is the
// failure this exists to prevent.
func TestScheduleConcurrencyLeavesRoomForRequests(t *testing.T) {
	if cap(schedule_slots) != schedule_concurrency {
		t.Errorf("slot channel holds %d, want schedule_concurrency of %d", cap(schedule_slots), schedule_concurrency)
	}
	if schedule_concurrency >= 32 {
		t.Errorf("schedule_concurrency is %d, at or above the default Starlark pool of 32", schedule_concurrency)
	}
	source, err := os.ReadFile("starlark.go")
	if err != nil {
		t.Fatalf("read starlark.go: %v", err)
	}
	if !strings.Contains(string(source), `ini_int("starlark", "concurrency", 32)`) {
		t.Error("the Starlark concurrency default moved; recheck schedule_concurrency against it")
	}
}

// schedule_bounds_fill inserts count rows directly for one (app, user) pair,
// bypassing the API so a test can reach a boundary without making the same
// number of Starlark calls.
func schedule_bounds_fill(t *testing.T, app, user string, count int) {
	t.Helper()
	db := db_open("db/schedule.db")
	for i := 0; i < count; i++ {
		db.exec("insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, 'filler', '{}', 0, ?)",
			user, app, now()+3600, now())
	}
}

func schedule_bounds_count(app, user string) int {
	return db_open("db/schedule.db").integer("select count(*) from schedule where app=? and user=?", app, user)
}

// TestScheduleRefusesPastTheRowCap is the finding. The payload cap bounds a
// row; nothing bounded the count, and db/schedule.db is one global table
// outside the per-user storage quota, so the two multiplied into shared disk an
// app fills for free.
func TestScheduleRefusesPastTheRowCap(t *testing.T) {
	thread := schedule_bounds_setup(t)
	schedule_bounds_fill(t, "scheduler", "u-schedule", schedule_rows_maximum-1)

	// The last row inside the cap is accepted.
	_, err := api_schedule_after(thread, sl.NewBuiltin("mochi.schedule.after", api_schedule_after),
		sl.Tuple{sl.String("event"), sl.NewDict(0), sl.MakeInt(60)}, nil)
	if err != nil {
		t.Fatalf("a call at the cap boundary was refused: %v", err)
	}
	if got := schedule_bounds_count("scheduler", "u-schedule"); got != schedule_rows_maximum {
		t.Fatalf("count = %d, want %d", got, schedule_rows_maximum)
	}

	// The next is refused, and says why rather than "failed to create".
	_, err = api_schedule_after(thread, sl.NewBuiltin("mochi.schedule.after", api_schedule_after),
		sl.Tuple{sl.String("event"), sl.NewDict(0), sl.MakeInt(60)}, nil)
	if err == nil {
		t.Fatal("a call past the cap was accepted; nothing bounds how much shared disk one app can take")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("error was %q, want it to name the limit so an app author can tell this from a database failure", err)
	}
	if got := schedule_bounds_count("scheduler", "u-schedule"); got != schedule_rows_maximum {
		t.Errorf("count = %d after a refusal, want it unchanged at %d", got, schedule_rows_maximum)
	}
}

// TestScheduleCapIsPerAppAndUser. The cap has to be keyed on the pair: keyed on
// the app alone it would punish an app for being installed by many users, and
// on the user alone it could not say which app was responsible.
func TestScheduleCapIsPerAppAndUser(t *testing.T) {
	// The fixture's own thread is unused here: this test drives two others.
	schedule_bounds_setup(t)
	schedule_bounds_fill(t, "scheduler", "u-schedule", schedule_rows_maximum)

	// Same app, a different user.
	other := &sl.Thread{Name: "test"}
	other.SetLocal("user", &User{UID: "u-other"})
	other.SetLocal("app", &App{id: "scheduler"})
	if _, err := api_schedule_after(other, sl.NewBuiltin("mochi.schedule.after", api_schedule_after),
		sl.Tuple{sl.String("event"), sl.NewDict(0), sl.MakeInt(60)}, nil); err != nil {
		t.Errorf("a second user was refused because the first had filled the app's quota: %v", err)
	}

	// Same user, a different app.
	second := &sl.Thread{Name: "test"}
	second.SetLocal("user", &User{UID: "u-schedule"})
	second.SetLocal("app", &App{id: "other"})
	if _, err := api_schedule_after(second, sl.NewBuiltin("mochi.schedule.after", api_schedule_after),
		sl.Tuple{sl.String("event"), sl.NewDict(0), sl.MakeInt(60)}, nil); err != nil {
		t.Errorf("a second app was refused because the first had filled the user's quota: %v", err)
	}
}

// schedule_bounds_log captures what the server logs for the duration of a test.
// warn and info both go through the standard logger, so this is the only place
// the warning is observable without an SMTP fixture - and log_repeat_allow
// cannot be probed instead, because probing it consumes the window and would
// suppress the very line under test.
type schedule_bounds_log struct {
	lock sync.Mutex
	text strings.Builder
}

func (l *schedule_bounds_log) Write(p []byte) (int, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.text.Write(p)
}

func (l *schedule_bounds_log) contains(s string) bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	return strings.Contains(l.text.String(), s)
}

func schedule_bounds_capture(t *testing.T) *schedule_bounds_log {
	t.Helper()
	captured := &schedule_bounds_log{}
	previous := log.Writer()
	log.SetOutput(captured)
	t.Cleanup(func() { log.SetOutput(previous) })
	return captured
}

// TestScheduleWarnsOnCrossingHalfTheCap. The operator hears about it while
// there is still room to act, rather than when the app breaks - a cap reached
// without notice fails as an aborted handler, a 500 and an email, which is the
// worst moment to find out. On the crossing only: a pair legitimately sitting
// above half would otherwise warn for ever.
func TestScheduleWarnsOnCrossingHalfTheCap(t *testing.T) {
	thread := schedule_bounds_setup(t)
	log_tables_reset(t)
	defer log_tables_reset(t)
	captured := schedule_bounds_capture(t)

	schedule_bounds_fill(t, "scheduler", "u-schedule", schedule_rows_warning-2)

	add := func() {
		t.Helper()
		if _, err := api_schedule_after(thread, sl.NewBuiltin("mochi.schedule.after", api_schedule_after),
			sl.Tuple{sl.String("event"), sl.NewDict(0), sl.MakeInt(60)}, nil); err != nil {
			t.Fatalf("schedule.after: %v", err)
		}
	}

	// One below the threshold: nothing said.
	add()
	if captured.contains("scheduled events for one user") {
		t.Error("warned before the threshold was reached")
	}

	// The crossing itself.
	add()
	if !captured.contains("scheduled events for one user") {
		t.Error("crossing half the cap said nothing; the operator only finds out when the app breaks")
	}
}

// TestScheduleWarnsOnceNotForever. Every insert after the crossing is also
// above half, so a warning that fired on the level rather than the crossing
// would repeat for as long as the app stayed there.
func TestScheduleWarnsOnceNotForever(t *testing.T) {
	thread := schedule_bounds_setup(t)
	log_tables_reset(t)
	defer log_tables_reset(t)

	schedule_bounds_fill(t, "scheduler", "u-schedule", schedule_rows_warning)

	captured := schedule_bounds_capture(t)
	for i := 0; i < 5; i++ {
		if _, err := api_schedule_after(thread, sl.NewBuiltin("mochi.schedule.after", api_schedule_after),
			sl.Tuple{sl.String("event"), sl.NewDict(0), sl.MakeInt(60)}, nil); err != nil {
			t.Fatalf("schedule.after: %v", err)
		}
	}
	if captured.contains("scheduled events for one user") {
		t.Error("warned again while already above the threshold; the line fires on the level rather than the crossing")
	}
}

// TestScheduleEveryHasAnIntervalFloor. The clamp was one second, which is
// 86,400 firings a day for as long as the row lives - and a recurring row lives
// until something cancels it, because schedule_claim re-arms it with
// due = due + interval rather than deleting it.
func TestScheduleEveryHasAnIntervalFloor(t *testing.T) {
	thread := schedule_bounds_setup(t)

	for _, asked := range []int{0, 1, 5, schedule_interval_floor - 1} {
		value, err := api_schedule_every(thread, sl.NewBuiltin("mochi.schedule.every", api_schedule_every),
			sl.Tuple{sl.String("tick"), sl.NewDict(0), sl.MakeInt(asked)}, nil)
		if err != nil {
			t.Fatalf("schedule.every(%d): %v", asked, err)
		}
		event, ok := value.(*SlScheduledEvent)
		if !ok {
			t.Fatalf("schedule.every returned %T", value)
		}
		if event.interval < schedule_interval_floor {
			t.Errorf("asking for %ds produced an interval of %ds, below the %ds floor", asked, event.interval, schedule_interval_floor)
		}
	}

	// A legitimate interval is untouched. The only use of every in the tree is
	// a daily watchdog, so a floor that rounded real intervals up would be
	// changing behaviour rather than bounding abuse.
	value, err := api_schedule_every(thread, sl.NewBuiltin("mochi.schedule.every", api_schedule_every),
		sl.Tuple{sl.String("watchdog"), sl.NewDict(0), sl.MakeInt(86400)}, nil)
	if err != nil {
		t.Fatalf("schedule.every(86400): %v", err)
	}
	if event, ok := value.(*SlScheduledEvent); !ok || event.interval != 86400 {
		t.Errorf("a daily interval came back as %v, want 86400 untouched", value)
	}
}
