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
	"os"
	"strings"
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
		if id := schedule_create("u-schedule", "scheduler", past, "evt", "{}", 0); id == 0 {
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
