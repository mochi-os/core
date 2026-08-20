// Mochi server: Scheduled Events
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"fmt"
	"time"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// ScheduledEvent represents a scheduled event in the database
type ScheduledEvent struct {
	ID       int64  `db:"id"`
	User     string `db:"user"`
	App      string `db:"app"`
	Due      int64  `db:"due"`
	Event    string `db:"event"`
	Data     string `db:"data"`
	Interval int64  `db:"interval"`
	Created  int64  `db:"created"`
}

var api_schedule = sls.FromStringDict(sl.String("mochi.schedule"), sl.StringDict{
	"after":  sl.NewBuiltin("mochi.schedule.after", api_schedule_after),
	"at":     sl.NewBuiltin("mochi.schedule.at", api_schedule_at),
	"cancel": sl.NewBuiltin("mochi.schedule.cancel", api_schedule_cancel),
	"every":  sl.NewBuiltin("mochi.schedule.every", api_schedule_every),
	"get":    sl.NewBuiltin("mochi.schedule.get", api_schedule_get),
	"list":   sl.NewBuiltin("mochi.schedule.list", api_schedule_list),
})

// schedule_wake is used to wake up the scheduler when a new event is created
var schedule_wake = make(chan struct{}, 1)

// schedule_data_maximum bounds one row's payload. db/schedule.db is a single
// global table shared by every user and app, and it sits outside the per-user
// storage quota, so an unbounded blob here is server-wide disk an app fills for
// free. Far above what a payload is for: a scheduled event carries the ids the
// handler needs to find its work, not the work itself - the largest payload any
// app in the tree can construct is about 110 bytes, and every row currently
// stored holds an empty dictionary.
const schedule_data_maximum = 4 * 1024

// schedule_rows_maximum bounds how many rows one (app, user) pair may hold. The
// payload cap bounds a row; without this nothing bounds the count, and the two
// multiply into unbounded shared disk.
//
// Keyed on the pair because that is how the table is keyed and how the abuse
// happens - a per-app cap would punish an app for being installed by many
// users, and a per-user cap could not say which app was responsible. Ten
// thousand matches directory_user_cap, and is chosen well clear of the busiest
// real caller rather than close to it: comptroller's schedule rows are created
// inside P2P event handlers, which run on the marketplace's host as the
// marketplace operator, so every seller's listings and every buyer's bids land
// in one pair - two rows per auctioned listing, plus one per bid extension,
// finalisation and review. A cap that a busy marketplace could reach would fail
// the way an app hitting it fails: an aborted handler, a 500, and an email.
const schedule_rows_maximum = 10000

// schedule_rows_warning is the level at which the operator hears about it,
// while there is still room to act. Reported on the crossing only, so a pair
// sitting legitimately above it does not warn for ever.
const schedule_rows_warning = schedule_rows_maximum / 2

// schedule_interval_floor is the shortest repeat a caller may ask for. The
// clamp it replaces was one second, which is 86,400 firings a day for as long
// as the row lives - and a recurring row lives until something cancels it,
// since schedule_claim re-arms it with due = due + interval. Every use of
// mochi.schedule.every in the tree is a daily watchdog, so a minute is far
// below anything real while removing the per-second case entirely.
const schedule_interval_floor = 60

// schedule_due_maximum bounds one pass, so the due query cannot materialise an
// arbitrary number of rows and the loop cannot dispatch them all at once.
// Leftovers are not delayed: schedule_next then reports a due time already
// past, the manager's sleep clamps to zero, and the next pass takes them.
const schedule_due_maximum = 100

// schedule_concurrency bounds how many scheduled handlers run at once. Each
// one enters Starlark, so without it a batch of rows made due at the same
// instant queues on the 32-slot pool and starves every interactive request on
// the host - and `at` accepts a past timestamp while `after` accepts a delay of
// zero, so nobody has to wait to arrange that. Well under the pool, so
// scheduled work leaves most of it for requests.
const schedule_concurrency = 8

// schedule_slots is acquired before a handler is dispatched and released when
// it returns. Acquired BLOCKING: skipping at capacity would return to a manager
// whose next sleep is zero (the skipped rows are still due), which is a busy
// spin. Blocking paces the loop instead, and a wedged handler holds one slot
// rather than all of them, so the rest keep running.
var schedule_slots = make(chan struct{}, schedule_concurrency)

// schedule_data_encode serialises a payload, refusing one past the size cap.
// It also reports the encoding error the three callers used to discard.
func schedule_data_encode(data map[string]any) (string, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("data is not JSON-encodable: %v", err)
	}
	if len(encoded) > schedule_data_maximum {
		return "", fmt.Errorf("data too large: %d bytes exceeds %d", len(encoded), schedule_data_maximum)
	}
	return string(encoded), nil
}

// schedule_db opens the schedule database
func schedule_db() *DB {
	return db_open("db/schedule.db")
}

// schedule_create inserts a new scheduled event and returns its ID.
// Replicates the new row to every host in the user's set so paired
// replicas agree on what is scheduled; the leader-gate on the firing
// side dedups handler execution. System events (user == "") stay
// local - they have no scope identifier for the replication pipeline.
func schedule_create(user string, app string, due int64, event string, data string, interval int64) (int64, error) {
	created := now()
	db := schedule_db()

	// The cap is enforced here rather than in the three API functions so the
	// count and the insert cannot drift apart, and so a path added later
	// inherits it.
	held := db.integer("select count(*) from schedule where app=? and user=?", app, user)
	if held >= schedule_rows_maximum {
		return 0, fmt.Errorf("scheduled event limit reached: %d events for this app", schedule_rows_maximum)
	}
	if held+1 == schedule_rows_warning {
		// On the crossing only. warn_application keys the admin-email throttle
		// on the app, so one app approaching its limit cannot silence another's
		// first warning.
		warn_application(app, "Schedule: app %q holds %d of its %d scheduled events for one user", app, held+1, schedule_rows_maximum)
	}

	result := must(db.internal.Exec("insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, ?, ?, ?, ?)",
		user, app, due, event, data, interval, created))
	id, _ := result.LastInsertId()
	if id == 0 {
		return 0, fmt.Errorf("failed to create scheduled event")
	}

	// Wake up the scheduler to check for the new event
	schedule_notify()

	return id, nil
}

// schedule_get retrieves a scheduled event by ID
func schedule_get(id int64) *ScheduledEvent {
	db := schedule_db()
	var se ScheduledEvent
	if !db.scan(&se, "select * from schedule where id=?", id) {
		return nil
	}
	return &se
}

// schedule_delete removes a scheduled event by ID.
func schedule_delete(id int64) {
	schedule_db().exec("delete from schedule where id=?", id)
}

// schedule_list returns all scheduled events for an app and user
func schedule_list(app string, user string) []ScheduledEvent {
	db := schedule_db()
	var events []ScheduledEvent
	db.scans(&events, "select * from schedule where app=? and user=? order by due", app, user)
	return events
}

// schedule_due returns all events that are due (due <= now)
func schedule_due(t int64) []ScheduledEvent {
	db := schedule_db()
	var events []ScheduledEvent
	db.scans(&events, "select * from schedule where due<=? order by due limit ?", t, schedule_due_maximum)
	return events
}

// schedule_next returns the next scheduled event, or nil if none
func schedule_next() *ScheduledEvent {
	db := schedule_db()
	var se ScheduledEvent
	if !db.scan(&se, "select * from schedule order by due limit 1") {
		return nil
	}
	return &se
}

// schedule_valid checks if the user and app still exist
// schedule_valid reports whether a due event can actually run on THIS host
// right now: the user, the app, an active version for that user, AND a
// handler for the event must all be present. It is the single source of
// truth used by schedule_run — anything it rejects is routed through
// schedule_handle_unrunnable rather than reaching schedule_run_event,
// whose own equivalent checks are then only a TOCTOU backstop.
func schedule_valid(se *ScheduledEvent) bool {
	// Resolve the user ("" = system, always valid).
	var user *User
	if se.User != "" {
		user = user_by_uid(se.User)
		if user == nil {
			return false
		}
	}

	// App must exist, with an active version for this user...
	app := app_by_id(se.App)
	if app == nil {
		return false
	}
	av := app.active(user)
	if av == nil {
		return false
	}

	// ...that defines a Starlark function of that name. Same lookup
	// schedule_run_event uses, so a typo is rejected when the task is
	// scheduled rather than silently doing nothing when it comes due.
	return av.starlark().has(se.Event)
}

// schedule_handle_unrunnable deals with a due event that schedule_valid
// rejected. It stays quiet (no admin email) either way:
//
//   - User absent, or still bootstrapping (pending): leave the row alone. Its
//     app and data may not have finished landing, so the handler could become
//     runnable shortly.
//   - Active user, or a system event, whose app / version / handler is gone:
//     drop the row. A recurring one would otherwise re-fire every interval
//     forever. One-shot rows were already removed by schedule_claim.
func schedule_handle_unrunnable(se *ScheduledEvent) {
	if se.User != "" {
		// Read the users row directly — NOT user_by_uid, which also returns nil
		// for a user whose identity hasn't loaded and would wrongly look
		// "absent".
		row, _ := db_open("db/users.db").row("select status from users where uid=?", se.User)
		if row == nil {
			return
		}
		status, _ := row["status"].(string)
		if user_pending(&User{Status: status}) {
			return
		}
	}
	if se.Interval > 0 {
		schedule_db().exec("delete from schedule where id=?", se.ID)
	}
}

// schedule_start initializes and starts the scheduler
func schedule_start() {
	// Wait for server to stabilize
	time.Sleep(5 * time.Second)

	// Catch up on overdue events
	schedule_run_due(time.Now())

	// Start the scheduler loop
	schedule_manager()
}

// schedule_manager is the main scheduler loop
func schedule_manager() {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					warn("scheduler panic: %v", r)
				}
			}()

			now := time.Now()
			schedule_run_due(now)

			// Calculate sleep duration
			var sleep_duration time.Duration
			next := schedule_next()
			if next != nil && time.Unix(next.Due, 0).Before(now.Add(1*time.Minute)) {
				sleep_duration = time.Until(time.Unix(next.Due, 0))
				if sleep_duration < 0 {
					sleep_duration = 0
				}
			} else {
				sleep_duration = 1 * time.Minute
			}

			// Wait for either the timer or a wake signal
			if sleep_duration > 0 {
				timer := time.NewTimer(sleep_duration)
				select {
				case <-timer.C:
					// Timer expired, check for due events
				case <-schedule_wake:
					// Woken up by new event creation
					timer.Stop()
				}
			}
		}()
	}
}

// schedule_notify wakes up the scheduler to check for new events
func schedule_notify() {
	select {
	case schedule_wake <- struct{}{}:
	default:
		// Channel already has a pending wake signal
	}
}

// schedule_run_due executes all due events
func schedule_run_due(t time.Time) {
	items := schedule_due(t.Unix())
	for _, item := range items {
		// Claim the event before spawning a goroutine
		// This prevents race conditions where multiple scheduler loops
		// pick up the same event
		if !schedule_claim(item.ID, item.Interval) {
			continue
		}
		schedule_slots <- struct{}{}
		go func(item ScheduledEvent) {
			defer func() { <-schedule_slots }()
			schedule_run(item)
		}(item)
	}
}

// schedule_claim atomically claims a scheduled event for execution. Returns
// true if this call claimed the event, false if another goroutine got there
// first. A recurring event advances its due time by one interval; a one-shot is
// deleted. Both are conditional on due <= now, so the rows-affected count is
// what decides the claim.
func schedule_claim(id int64, interval int64) bool {
	db := schedule_db()
	var result int64
	var err error

	if interval > 0 {
		// Recurring: update due time to next interval
		// Use the current due time + interval to avoid drift
		res, e := db.internal.Exec("update schedule set due = due + ? where id = ? and due <= ?", interval, id, now())
		if e == nil {
			result, err = res.RowsAffected()
		}
	} else {
		res, e := db.internal.Exec("delete from schedule where id = ? and due <= ?", id, now())
		if e == nil {
			result, err = res.RowsAffected()
		}
	}

	return err == nil && result > 0
}

// schedule_run executes a single scheduled event
// The event has already been claimed (deleted or due updated) before this is called
func schedule_run(se ScheduledEvent) {
	defer func() {
		if r := recover(); r != nil {
			warn("schedule panic: %s/%s: %v", se.App, se.Event, r)
		}
	}()

	// Can it run on this host (user + app + active version + handler all
	// present)? If not, handle it quietly — never warn-email; see
	// schedule_handle_unrunnable.
	if !schedule_valid(&se) {
		schedule_handle_unrunnable(&se)
		return
	}

	// Run the event handler. Normal runs are not logged — failures have
	// their own lines (panic recovery above, missing user/app warns and
	// handler errors in schedule_run_event and the app framework). The
	// watchdog covers the one case those miss: a handler that doesn't
	// return. A run past schedule_stuck_seconds gets a stuck line, and a
	// finished line when it eventually returns, so a stuck line with no
	// finished line means the handler is still wedged (or died with the
	// process).
	started := now()
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-time.After(schedule_stuck_seconds * time.Second):
			info("schedule stuck: %s/%s id=%d running over %ds", se.App, se.Event, se.ID, int64(schedule_stuck_seconds))
		}
	}()
	schedule_run_event(&se)
	close(done)
	if now()-started >= schedule_stuck_seconds {
		info("schedule finished: %s/%s id=%d after %ds", se.App, se.Event, se.ID, now()-started)
	}
}

// schedule_stuck_seconds is how long a scheduled event may run before
// the watchdog logs it as stuck. Feed polls and AI calls legitimately
// take tens of seconds under remote rate-limit backoff; minutes is
// pathological.
const schedule_stuck_seconds = 5 * 60

// schedule_run_event dispatches the scheduled event to the app's event handler
func schedule_run_event(se *ScheduledEvent) {
	// Get the user (nil for system events)
	// These four checks duplicate schedule_valid (already run in
	// schedule_run, which routes a rejection to schedule_handle_unrunnable
	// and never reaches here). They survive only as a TOCTOU backstop —
	// the user/app could vanish between the two calls — so they log at
	// debug, never warn-email.
	var user *User
	if se.User != "" {
		user = user_by_uid(se.User)
		if user == nil {
			debug("schedule: user %q not found for event %s/%s", se.User, se.App, se.Event)
			return
		}
	}

	// Get the app
	app := app_by_id(se.App)
	if app == nil {
		debug("schedule: app %q not found for event %s", se.App, se.Event)
		return
	}

	// Get the active version for this user
	av := app.active(user)
	if av == nil {
		debug("schedule: no active version for app %q", se.App)
		return
	}

	// A scheduled task names its Starlark function directly. app.json's events
	// block lists what a REMOTE PEER may invoke, which a scheduled task is not,
	// so the two namespaces stay apart: a scheduled handler is unreachable from
	// the network because it is not in the event namespace at all.
	s := av.starlark()
	if !s.has(se.Event) {
		debug("schedule: handler %q not found in app %q", se.Event, se.App)
		return
	}

	// Parse the data payload
	var data map[string]any
	if se.Data != "" {
		json.Unmarshal([]byte(se.Data), &data)
	}
	if data == nil {
		data = make(map[string]any)
	}

	// Create a scheduled event wrapper for Starlark
	sew := &ScheduledEventWrapper{
		se:     se,
		data:   data,
		source: "schedule",
		user:   user,
	}

	// Run the handler
	s.set("event", sew)
	s.set("app", app)
	s.set("user", user)
	s.set("owner", user)

	// No sender to answer and no watermark to hold back, so a failure here
	// is reported rather than propagated — but it is reported, because the
	// alternative is a scheduled task that silently never does its work.
	if _, err := s.call(se.Event, sl.Tuple{sew}); err != nil {
		warn("Scheduled event %s:%s() failed: %v", app.id, se.Event, err)
	}
}

// ScheduledEventWrapper wraps a ScheduledEvent for Starlark event handlers
type ScheduledEventWrapper struct {
	se     *ScheduledEvent
	data   map[string]any
	source string
	user   *User
}

func (e *ScheduledEventWrapper) AttrNames() []string {
	return []string{"content", "created", "data", "due", "from", "header", "headers", "source", "user"}
}

func (e *ScheduledEventWrapper) Attr(name string) (sl.Value, error) {
	switch name {
	case "content":
		return sl.NewBuiltin("content", e.sl_content), nil
	case "created":
		return sl.MakeInt64(e.se.Created), nil
	case "data":
		return sl_encode(e.data), nil
	case "due":
		return sl.MakeInt64(e.se.Due), nil
	case "from":
		return sl.None, nil
	case "header":
		return sl.NewBuiltin("header", e.sl_header), nil
	case "headers":
		return sl.None, nil
	case "source":
		return sl.String(e.source), nil
	case "user":
		if e.user != nil {
			return e.user, nil
		}
		return sl.None, nil
	default:
		return nil, nil
	}
}

func (e *ScheduledEventWrapper) Freeze()               {}
func (e *ScheduledEventWrapper) Hash() (uint32, error) { return 0, nil }
func (e *ScheduledEventWrapper) String() string        { return "ScheduledEvent" }
func (e *ScheduledEventWrapper) Truth() sl.Bool        { return sl.True }
func (e *ScheduledEventWrapper) Type() string          { return "ScheduledEvent" }

// e.content(field, default?) -> any: Get a content field from the event data
func (e *ScheduledEventWrapper) sl_content(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <field: string>, [default: any]")
	}

	field, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid field %q", field)
	}

	value, found := e.data[field]
	if found {
		return sl_encode(value), nil
	}

	if len(args) > 1 {
		return args[1], nil
	}

	return sl.None, nil
}

// e.header(name) -> string: Get an event header (from, to, service, event)
func (e *ScheduledEventWrapper) sl_header(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <header: string>")
	}

	header, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid header %q", header)
	}

	switch header {
	case "from":
		return sl.None, nil
	case "to":
		return sl.None, nil
	case "service":
		return sl.None, nil
	case "event":
		return sl.String(e.se.Event), nil
	default:
		return sl_error(fn, "invalid header %q", header)
	}
}

// SlScheduledEvent is the Starlark representation of a scheduled event object
type SlScheduledEvent struct {
	id       int64
	event    string
	data     map[string]any
	due      int64
	interval int64
	created  int64
}

func (se *SlScheduledEvent) AttrNames() []string {
	return []string{"cancel", "created", "data", "due", "event", "id", "interval"}
}

func (se *SlScheduledEvent) Attr(name string) (sl.Value, error) {
	switch name {
	case "cancel":
		return sl.NewBuiltin("cancel", se.sl_cancel), nil
	case "created":
		return sl.MakeInt64(se.created), nil
	case "data":
		return sl_encode(se.data), nil
	case "due":
		return sl.MakeInt64(se.due), nil
	case "event":
		return sl.String(se.event), nil
	case "id":
		return sl.MakeInt64(se.id), nil
	case "interval":
		return sl.MakeInt64(se.interval), nil
	default:
		return nil, nil
	}
}

func (se *SlScheduledEvent) Freeze()               {}
func (se *SlScheduledEvent) Hash() (uint32, error) { return uint32(se.id), nil }
func (se *SlScheduledEvent) String() string        { return fmt.Sprintf("ScheduledEvent(%d)", se.id) }
func (se *SlScheduledEvent) Truth() sl.Bool        { return sl.True }
func (se *SlScheduledEvent) Type() string          { return "ScheduledEvent" }

// se.cancel() -> None: Cancel this scheduled event (no-op if already executed/cancelled)
func (se *SlScheduledEvent) sl_cancel(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	schedule_delete(se.id)
	return sl.None, nil
}

// new_starlark_scheduled_event creates a Starlark scheduled event object from database record
func new_starlark_scheduled_event(se *ScheduledEvent) *SlScheduledEvent {
	var data map[string]any
	if se.Data != "" {
		json.Unmarshal([]byte(se.Data), &data)
	}
	if data == nil {
		data = make(map[string]any)
	}

	return &SlScheduledEvent{
		id:       se.ID,
		event:    se.Event,
		data:     data,
		due:      se.Due,
		interval: se.Interval,
		created:  se.Created,
	}
}

// mochi.schedule.at(event, data, time) -> ScheduledEvent: Schedule an event at a specific time
func api_schedule_at(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <event: string>, <data: dict>, <time: int>")
	}

	event, ok := sl.AsString(args[0])
	if !ok || event == "" {
		return sl_error(fn, "invalid event name")
	}

	data_val := sl_decode(args[1])
	data_map, ok := data_val.(map[string]any)
	if !ok {
		return sl_error(fn, "data must be a dictionary")
	}

	due, err := sl.AsInt32(args[2])
	if err != nil {
		return sl_error(fn, "invalid time")
	}

	// Get user and app from context
	user := principal_caller(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	var uid string
	if user != nil {
		uid = user.UID
	}

	// Serialize data
	data_json, err := schedule_data_encode(data_map)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	// If time is in the past, run immediately (but still schedule for audit trail)
	due_time := int64(due)

	id, err := schedule_create(uid, app.id, due_time, event, string(data_json), 0)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return new_starlark_scheduled_event(&ScheduledEvent{
		ID: id, User: uid, App: app.id, Due: due_time,
		Event: event, Data: string(data_json), Created: now(),
	}), nil
}

// mochi.schedule.after(event, data, delay) -> ScheduledEvent: Schedule an event after a delay
func api_schedule_after(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <event: string>, <data: dict>, <delay: int>")
	}

	event, ok := sl.AsString(args[0])
	if !ok || event == "" {
		return sl_error(fn, "invalid event name")
	}

	data_val := sl_decode(args[1])
	data_map, ok := data_val.(map[string]any)
	if !ok {
		return sl_error(fn, "data must be a dictionary")
	}

	delay, err := sl.AsInt32(args[2])
	if err != nil {
		return sl_error(fn, "invalid delay")
	}

	// Get user and app from context
	user := principal_caller(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	var uid string
	if user != nil {
		uid = user.UID
	}

	// Serialize data
	data_json, err := schedule_data_encode(data_map)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	// If delay is zero or negative, run immediately
	due_time := now() + int64(delay)
	if delay <= 0 {
		due_time = now()
	}

	id, err := schedule_create(uid, app.id, due_time, event, string(data_json), 0)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	return new_starlark_scheduled_event(&ScheduledEvent{
		ID: id, User: uid, App: app.id, Due: due_time,
		Event: event, Data: string(data_json), Created: now(),
	}), nil
}

// mochi.schedule.every(event, data, interval) -> ScheduledEvent: Schedule a recurring event
func api_schedule_every(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 3 {
		return sl_error(fn, "syntax: <event: string>, <data: dict>, <interval: int>")
	}

	event, ok := sl.AsString(args[0])
	if !ok || event == "" {
		return sl_error(fn, "invalid event name")
	}

	data_val := sl_decode(args[1])
	data_map, ok := data_val.(map[string]any)
	if !ok {
		return sl_error(fn, "data must be a dictionary")
	}

	interval, err := sl.AsInt32(args[2])
	if err != nil {
		return sl_error(fn, "invalid interval")
	}

	// A repeat shorter than the floor is raised to it rather than refused: the
	// clamp is the established contract here, and nothing in the tree asks for
	// less than a day, so refusing would only ever surprise a future caller.
	if interval < schedule_interval_floor {
		interval = schedule_interval_floor
	}

	// Get user and app from context
	user := principal_caller(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	var uid string
	if user != nil {
		uid = user.UID
	}

	// Serialize data
	data_json, err := schedule_data_encode(data_map)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	// First run is after the interval
	due_time := now() + int64(interval)

	id, err := schedule_create(uid, app.id, due_time, event, string(data_json), int64(interval))
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	se := &ScheduledEvent{
		ID: id, User: uid, App: app.id, Due: due_time,
		Event: event, Data: string(data_json), Interval: int64(interval), Created: now(),
	}

	return new_starlark_scheduled_event(se), nil
}

// mochi.schedule.get(id) -> ScheduledEvent | None: Get a scheduled event by ID
func api_schedule_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	// Get user and app from context
	user := principal_caller(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	se := schedule_get(int64(id))
	if se == nil {
		return sl.None, nil
	}

	// Verify the event belongs to this app and user. A nil user owns nothing:
	// an anonymous caller reaches Starlark with no user on the thread, so
	// testing `user != nil && ...` skipped the ownership check for exactly
	// those callers and left only the app test - every other user's events for
	// this app were readable, and the ids are sequential rowids.
	if se.App != app.id {
		return sl.None, nil
	}
	if user == nil || se.User != user.UID {
		return sl.None, nil
	}

	return new_starlark_scheduled_event(se), nil
}

// mochi.schedule.cancel(id) -> bool: Cancel a previously scheduled event.
// Returns True if the event was found and cancelled, False if not found or
// if it doesn't belong to the calling app and user (silent — same scoping
// pattern as mochi.schedule.get).
func api_schedule_cancel(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := sl.AsInt32(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	user := principal_caller(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	se := schedule_get(int64(id))
	if se == nil {
		return sl.False, nil
	}
	if se.App != app.id {
		return sl.False, nil
	}
	if user == nil || se.User != user.UID {
		return sl.False, nil
	}

	schedule_delete(int64(id))
	return sl.True, nil
}

// mochi.schedule.list() -> list: List scheduled events for current app and user
func api_schedule_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Get user and app from context
	user := principal_caller(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	var uid string
	if user != nil {
		uid = user.UID
	}

	events := schedule_list(app.id, uid)
	result := make([]sl.Value, len(events))
	for i, se := range events {
		result[i] = new_starlark_scheduled_event(&se)
	}

	return sl.NewList(result), nil
}
