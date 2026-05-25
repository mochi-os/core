// Mochi server: Scheduled Events Tests
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

// schedule_update_due updates the due time for a scheduled event (test helper)
func schedule_update_due(id int64, due int64) {
	db := schedule_db()
	db.exec("update schedule set due=? where id=?", due, id)
}

func TestScheduleDatabase(t *testing.T) {
	// Setup test environment
	data_dir = t.TempDir()
	os.MkdirAll(data_dir+"/db", 0755)

	// Create schedule database
	db := db_open("db/schedule.db")
	db.exec("create table schedule (id integer primary key, user int not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	db.exec("create index schedule_due on schedule(due)")
	db.exec("create index schedule_app_event on schedule(app, event)")

	t.Run("create and get", func(t *testing.T) {
		data, _ := json.Marshal(map[string]any{"auction": 123})
		id := schedule_create("u1", "test-app", now()+3600, "end_auction", string(data), 0)
		if id == 0 {
			t.Fatal("expected non-zero ID")
		}

		se := schedule_get(id)
		if se == nil {
			t.Fatal("expected scheduled event")
		}
		if se.User != "u1" {
			t.Errorf("expected user u1, got %q", se.User)
		}
		if se.App != "test-app" {
			t.Errorf("expected app test-app, got %s", se.App)
		}
		if se.Event != "end_auction" {
			t.Errorf("expected event end_auction, got %s", se.Event)
		}
		if se.Interval != 0 {
			t.Errorf("expected interval 0, got %d", se.Interval)
		}

		// Verify data
		var dataMap map[string]any
		json.Unmarshal([]byte(se.Data), &dataMap)
		if dataMap["auction"] != float64(123) {
			t.Errorf("expected auction 123, got %v", dataMap["auction"])
		}
	})

	t.Run("create recurring", func(t *testing.T) {
		data, _ := json.Marshal(map[string]any{})
		id := schedule_create("u1", "test-app", now()+300, "cleanup", string(data), 300)
		if id == 0 {
			t.Fatal("expected non-zero ID")
		}

		se := schedule_get(id)
		if se == nil {
			t.Fatal("expected scheduled event")
		}
		if se.Interval != 300 {
			t.Errorf("expected interval 300, got %d", se.Interval)
		}
	})

	t.Run("list by app and user", func(t *testing.T) {
		// Create events for different apps/users
		data, _ := json.Marshal(map[string]any{})
		schedule_create("u1", "app-a", now()+100, "event1", string(data), 0)
		schedule_create("u1", "app-a", now()+200, "event2", string(data), 0)
		schedule_create("u2", "app-a", now()+300, "event3", string(data), 0)
		schedule_create("u1", "app-b", now()+400, "event4", string(data), 0)

		// List for user 1, app-a
		events := schedule_list("app-a", "u1")
		count := 0
		for _, e := range events {
			if e.App == "app-a" && e.User == "u1" {
				count++
			}
		}
		if count < 2 {
			t.Errorf("expected at least 2 events for app-a/user-1, got %d", count)
		}
	})

	t.Run("delete", func(t *testing.T) {
		data, _ := json.Marshal(map[string]any{})
		id := schedule_create("u1", "test-app", now()+3600, "to_delete", string(data), 0)

		se := schedule_get(id)
		if se == nil {
			t.Fatal("expected scheduled event before delete")
		}

		schedule_delete(id)

		se = schedule_get(id)
		if se != nil {
			t.Error("expected nil after delete")
		}
	})

	t.Run("update due", func(t *testing.T) {
		data, _ := json.Marshal(map[string]any{})
		id := schedule_create("u1", "test-app", now()+100, "recurring", string(data), 100)

		se := schedule_get(id)
		original_due := se.Due

		schedule_update_due(id, original_due+100)

		se = schedule_get(id)
		if se.Due != original_due+100 {
			t.Errorf("expected due %d, got %d", original_due+100, se.Due)
		}
	})

	t.Run("due events", func(t *testing.T) {
		// Create some due and future events
		data, _ := json.Marshal(map[string]any{})
		schedule_create("u1", "due-app", now()-10, "past_event", string(data), 0)
		schedule_create("u1", "due-app", now()+3600, "future_event", string(data), 0)

		due_events := schedule_due(now())
		found_past := false
		found_future := false
		for _, e := range due_events {
			if e.Event == "past_event" {
				found_past = true
			}
			if e.Event == "future_event" {
				found_future = true
			}
		}
		if !found_past {
			t.Error("expected to find past_event in due events")
		}
		if found_future {
			t.Error("did not expect to find future_event in due events")
		}
	})

	t.Run("next event", func(t *testing.T) {
		// Clear existing events
		db := schedule_db()
		db.exec("delete from schedule")

		data, _ := json.Marshal(map[string]any{})
		schedule_create("u1", "next-app", now()+200, "second", string(data), 0)
		schedule_create("u1", "next-app", now()+100, "first", string(data), 0)
		schedule_create("u1", "next-app", now()+300, "third", string(data), 0)

		next := schedule_next()
		if next == nil {
			t.Fatal("expected next event")
		}
		if next.Event != "first" {
			t.Errorf("expected first event, got %s", next.Event)
		}
	})
}

func TestScheduleStarlarkObject(t *testing.T) {
	t.Run("SlScheduledEvent properties", func(t *testing.T) {
		se := &ScheduledEvent{
			ID:       123,
			User:     "u1",
			App:      "test-app",
			Due:      1710522000,
			Event:    "test_event",
			Data:     `{"key": "value"}`,
			Interval: 300,
			Created:  1710435600,
		}

		sl_se := new_starlark_scheduled_event(se)

		// Test id
		id_attr, _ := sl_se.Attr("id")
		if id_attr.String() != "123" {
			t.Errorf("expected id 123, got %s", id_attr.String())
		}

		// Test event
		event_attr, _ := sl_se.Attr("event")
		if event_attr.String() != `"test_event"` {
			t.Errorf("expected event test_event, got %s", event_attr.String())
		}

		// Test interval
		interval_attr, _ := sl_se.Attr("interval")
		if interval_attr.String() != "300" {
			t.Errorf("expected interval 300, got %s", interval_attr.String())
		}

		// Test due
		due_attr, _ := sl_se.Attr("due")
		if due_attr.String() != "1710522000" {
			t.Errorf("expected due 1710522000, got %s", due_attr.String())
		}

		// Test created
		created_attr, _ := sl_se.Attr("created")
		if created_attr.String() != "1710435600" {
			t.Errorf("expected created 1710435600, got %s", created_attr.String())
		}

		// Test data
		data_attr, _ := sl_se.Attr("data")
		if data_attr == nil {
			t.Error("expected data attribute")
		}
	})

	t.Run("SlScheduledEvent with empty data", func(t *testing.T) {
		se := &ScheduledEvent{
			ID:       124,
			User:     "u1",
			App:      "test-app",
			Due:      1710522000,
			Event:    "test_event",
			Data:     "",
			Interval: 0,
			Created:  1710435600,
		}

		sl_se := new_starlark_scheduled_event(se)
		data_attr, _ := sl_se.Attr("data")
		if data_attr == nil {
			t.Error("expected data attribute even for empty data")
		}
	})
}

func TestScheduleEventWrapper(t *testing.T) {
	t.Run("ScheduledEventWrapper properties", func(t *testing.T) {
		se := &ScheduledEvent{
			ID:      125,
			Event:   "test_event",
			Due:     1710522000,
			Created: 1710435600,
		}
		data := map[string]any{"auction": 123, "status": "active"}

		wrapper := &ScheduledEventWrapper{
			se:     se,
			data:   data,
			source: "schedule",
		}

		// Test source
		source_attr, _ := wrapper.Attr("source")
		if source_attr.String() != `"schedule"` {
			t.Errorf("expected source schedule, got %s", source_attr.String())
		}

		// Test due
		due_attr, _ := wrapper.Attr("due")
		if due_attr.String() != "1710522000" {
			t.Errorf("expected due 1710522000, got %s", due_attr.String())
		}

		// Test created
		created_attr, _ := wrapper.Attr("created")
		if created_attr.String() != "1710435600" {
			t.Errorf("expected created 1710435600, got %s", created_attr.String())
		}

		// Test from (should be None for scheduled events)
		from_attr, _ := wrapper.Attr("from")
		if from_attr.String() != "None" {
			t.Errorf("expected from None, got %s", from_attr.String())
		}

		// Test headers (should be None for scheduled events)
		headers_attr, _ := wrapper.Attr("headers")
		if headers_attr.String() != "None" {
			t.Errorf("expected headers None, got %s", headers_attr.String())
		}
	})
}

func TestScheduleValid(t *testing.T) {
	// Setup test environment
	data_dir = t.TempDir()
	os.MkdirAll(data_dir+"/db", 0755)

	// Create minimal databases needed
	db_create()

	t.Run("invalid user", func(t *testing.T) {
		se := &ScheduledEvent{
			User: "u99999", // Non-existent user
			App:  "test-app",
		}
		// User doesn't exist, so should be invalid
		if schedule_valid(se) {
			t.Error("expected invalid for non-existent user")
		}
	})

	t.Run("system user with invalid app", func(t *testing.T) {
		se := &ScheduledEvent{
			User: "", // System user (always valid)
			App:  "non-existent-app-12345",
		}
		// System user is valid, but app doesn't exist
		if schedule_valid(se) {
			t.Error("expected invalid for non-existent app")
		}
	})

	t.Run("invalid app", func(t *testing.T) {
		se := &ScheduledEvent{
			User: "",
			App:  "non-existent-app-12345",
		}
		if schedule_valid(se) {
			t.Error("expected invalid for non-existent app")
		}
	})
}

func TestScheduleClaimBeforeExecute(t *testing.T) {
	// Setup test environment
	data_dir = t.TempDir()
	os.MkdirAll(data_dir+"/db", 0755)

	db := db_open("db/schedule.db")
	db.exec("create table schedule (id integer primary key, user int not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	db.exec("create index schedule_due on schedule(due)")

	t.Run("one-shot event deleted on claim", func(t *testing.T) {
		data, _ := json.Marshal(map[string]any{})
		id := schedule_create("u0", "test-app", now(), "one_shot", string(data), 0)

		se := schedule_get(id)
		if se == nil {
			t.Fatal("expected event to exist before claim")
		}

		// Simulate claim (what schedule_run does)
		if se.Interval > 0 {
			schedule_update_due(se.ID, se.Due+se.Interval)
		} else {
			schedule_delete(se.ID)
		}

		se = schedule_get(id)
		if se != nil {
			t.Error("expected one-shot event to be deleted after claim")
		}
	})

	t.Run("recurring event due updated on claim", func(t *testing.T) {
		data, _ := json.Marshal(map[string]any{})
		original_due := now()
		id := schedule_create("u0", "test-app", original_due, "recurring", string(data), 300)

		se := schedule_get(id)
		if se == nil {
			t.Fatal("expected event to exist before claim")
		}

		// Simulate claim (what schedule_run does)
		if se.Interval > 0 {
			schedule_update_due(se.ID, se.Due+se.Interval)
		} else {
			schedule_delete(se.ID)
		}

		se = schedule_get(id)
		if se == nil {
			t.Fatal("expected recurring event to still exist after claim")
		}
		if se.Due != original_due+300 {
			t.Errorf("expected due to be updated to %d, got %d", original_due+300, se.Due)
		}
	})
}

func TestScheduleEdgeCases(t *testing.T) {
	data_dir = t.TempDir()
	os.MkdirAll(data_dir+"/db", 0755)

	db := db_open("db/schedule.db")
	db.exec("create table schedule (id integer primary key, user int not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")

	t.Run("empty data", func(t *testing.T) {
		id := schedule_create("u1", "test-app", now()+100, "event", "", 0)
		se := schedule_get(id)
		if se == nil {
			t.Fatal("expected event with empty data")
		}
		if se.Data != "" {
			t.Errorf("expected empty data, got %s", se.Data)
		}
	})

	t.Run("large data payload", func(t *testing.T) {
		large_data := make(map[string]any)
		for i := 0; i < 100; i++ {
			large_data[string(rune('a'+i%26))+string(rune(i))] = i
		}
		data_json, _ := json.Marshal(large_data)

		id := schedule_create("u1", "test-app", now()+100, "large_event", string(data_json), 0)
		se := schedule_get(id)
		if se == nil {
			t.Fatal("expected event with large data")
		}

		var recovered map[string]any
		json.Unmarshal([]byte(se.Data), &recovered)
		if len(recovered) != 100 {
			t.Errorf("expected 100 keys in recovered data, got %d", len(recovered))
		}
	})

	t.Run("past due time", func(t *testing.T) {
		past_time := now() - 3600 // 1 hour ago
		id := schedule_create("u1", "test-app", past_time, "past_event", "{}", 0)
		se := schedule_get(id)
		if se == nil {
			t.Fatal("expected event with past due time")
		}
		if se.Due != past_time {
			t.Errorf("expected due %d, got %d", past_time, se.Due)
		}

		// Should appear in due events
		due_events := schedule_due(now())
		found := false
		for _, e := range due_events {
			if e.ID == id {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected past event to appear in due events")
		}
	})

	t.Run("zero interval treated as one-shot", func(t *testing.T) {
		id := schedule_create("u1", "test-app", now()+100, "zero_interval", "{}", 0)
		se := schedule_get(id)
		if se.Interval != 0 {
			t.Errorf("expected interval 0, got %d", se.Interval)
		}
	})
}

func TestScheduleManagerTiming(t *testing.T) {
	t.Run("next event timing calculation", func(t *testing.T) {
		data_dir = t.TempDir()
		os.MkdirAll(data_dir+"/db", 0755)

		db := db_open("db/schedule.db")
		db.exec("create table schedule (id integer primary key, user int not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
		db.exec("create index schedule_due on schedule(due)")

		// Create event 30 seconds from now
		due_time := now() + 30
		schedule_create("u1", "test-app", due_time, "soon_event", "{}", 0)

		next := schedule_next()
		if next == nil {
			t.Fatal("expected next event")
		}

		// Check that the event is within 1 minute (scheduler precision window)
		event_time := time.Unix(next.Due, 0)
		if event_time.Before(time.Now().Add(1 * time.Minute)) {
			// This is the condition where scheduler should sleep until exact time
			t.Log("Event is imminent, scheduler would sleep until exact time")
		}
	})
}

func TestScheduleConcurrency(t *testing.T) {
	data_dir = t.TempDir()
	os.MkdirAll(data_dir+"/db", 0755)

	db := db_open("db/schedule.db")
	db.exec("create table schedule (id integer primary key, user int not null, app text not null, due int not null, event text not null, data text not null, interval int not null, created int not null)")
	db.exec("create index schedule_due on schedule(due)")

	t.Run("concurrent creates", func(t *testing.T) {
		// Test sequential creates to verify ID uniqueness
		// (SQLite handles concurrency via locking, so concurrent writes serialize anyway)
		ids := make(map[int64]bool)
		for i := 0; i < 10; i++ {
			data, _ := json.Marshal(map[string]any{"n": i})
			id := schedule_create("u1", "test-app", now()+int64(i*100), "concurrent", string(data), 0)
			if id == 0 {
				t.Errorf("got zero ID for iteration %d", i)
				continue
			}
			if ids[id] {
				t.Errorf("duplicate ID %d at iteration %d", id, i)
			}
			ids[id] = true
		}

		if len(ids) != 10 {
			t.Errorf("expected 10 unique IDs, got %d", len(ids))
		}
	})

	t.Run("list after multiple creates", func(t *testing.T) {
		events := schedule_list("test-app", "u1")
		if len(events) < 10 {
			t.Errorf("expected at least 10 events, got %d", len(events))
		}
	})
}
