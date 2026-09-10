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

// schedule_data_maximum bounds one row's payload. db/schedule.db is one global
// table outside the per-user storage quota, so an unbounded blob is server-wide
// disk an app fills for free. A payload carries ids, not work.
const schedule_data_maximum = 4 * 1024

// schedule_rows_maximum bounds how many rows one (app, user) pair may hold, so
// the payload cap and the row count cannot multiply into unbounded shared disk.
// Keyed on the pair because that is how the table is keyed and how abuse
// happens.
const schedule_rows_maximum = 10000

// schedule_rows_warning is the level at which the operator hears about it,
// while there is still room to act. Reported on the crossing only, so a pair
// sitting legitimately above it does not warn for ever.
const schedule_rows_warning = schedule_rows_maximum / 2

// schedule_interval_floor is the shortest repeat a caller may ask for. A
// recurring row lives until something cancels it, so a one-second interval is
// 86,400 firings a day for ever; every real use in the tree is a daily
// watchdog.
const schedule_interval_floor = 60

// schedule_retry_seconds is how far a one-shot's due time is moved when it is
// claimed. Holding the row instead of deleting it is what lets a due event that
// cannot run yet be seen again: when the check that follows passes, the row is
// deleted and the handler runs; when it defers, the row comes due again after
// this long. Must be positive, or an unclaimed overdue row makes the scheduler
// loop spin.
const schedule_retry_seconds = 60

// schedule_due_maximum bounds one pass, so the due query cannot materialise an
// arbitrary number of rows and the loop cannot dispatch them all at once.
// Leftovers are not delayed: schedule_next then reports a due time already
// past, the manager's sleep clamps to zero, and the next pass takes them.
const schedule_due_maximum = 100

// schedule_concurrency bounds how many scheduled handlers run at once. Each
// enters Starlark, so an unbounded batch made due at one instant would take the
// 32-slot pool and starve interactive requests. Well under the pool
// deliberately.
const schedule_concurrency = 8

// schedule_slots is acquired before a handler is dispatched, released when it
// returns. Acquired BLOCKING: skipping at capacity returns to a manager whose
// next sleep is zero, which is a busy spin. Blocking paces the loop instead.
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

// schedule_create inserts a new scheduled event and returns its ID. A system
// event passes an empty user.
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

// schedule_reason says why a due event cannot run on this host now, so that
// schedule_handle_unrunnable can tell a row nothing will ever run again from
// one caught in a passing state.
type schedule_reason int

const (
	schedule_runnable schedule_reason = iota
	schedule_user_absent
	schedule_app_absent
	schedule_version_absent
	schedule_handler_failed
	schedule_handler_absent
)

// retires reports whether the reason is final: nothing on this host can run the
// event again, so a recurring row is dropped rather than re-claimed every
// interval. No active version is an upgrade or cleanup window, and a handler
// missing while an execute file failed to load may be defined in that file, so
// both defer.
func (r schedule_reason) retires() bool {
	switch r {
	case schedule_user_absent, schedule_app_absent, schedule_handler_absent:
		return true
	}
	return false
}

func (r schedule_reason) String() string {
	switch r {
	case schedule_runnable:
		return "runnable"
	case schedule_user_absent:
		return "user absent"
	case schedule_app_absent:
		return "app absent"
	case schedule_version_absent:
		return "no active version"
	case schedule_handler_failed:
		return "handler absent after a load failure"
	case schedule_handler_absent:
		return "handler absent"
	}
	return "unknown"
}

// schedule_check reports whether a due event can run on this host now: user,
// app, an active version for that user, and a handler for the event must all be
// present. schedule_run routes anything it rejects to
// schedule_handle_unrunnable with the reason.
func schedule_check(se *ScheduledEvent) schedule_reason {
	// Resolve the user ("" = system, always valid).
	var user *User
	if se.User != "" {
		user = user_by_uid(se.User)
		if user == nil {
			return schedule_user_absent
		}
	}

	// App must exist, with an active version for this user...
	app := app_by_id(se.App)
	if app == nil {
		return schedule_app_absent
	}
	av := app.active(user)
	if av == nil {
		return schedule_version_absent
	}

	// ...that defines a Starlark function of that name. Same lookup
	// schedule_run_event uses. A definition can be missing because the file
	// holding it did not load, which is not the app never declaring it.
	s := av.starlark()
	if s.has(se.Event) {
		return schedule_runnable
	}
	if len(s.failed) > 0 {
		return schedule_handler_failed
	}
	return schedule_handler_absent
}

// schedule_handle_unrunnable deals with a due event schedule_check rejected.
// The row is retired only when nothing will ever run it again: the account is
// gone, the app is gone, or the app loaded cleanly and defines no such handler.
// Anything passing - a pending user whose app may still be landing, a user
// whose row exists but did not resolve, no active version during an upgrade or
// cleanup window, a handler behind a load failure, a users.db fault - is
// deferred: a recurring row comes due again next interval, a one-shot after
// schedule_retry_seconds, both set by the claim. Logged at info, never
// warn-email.
func schedule_handle_unrunnable(se *ScheduledEvent, reason schedule_reason) {
	retire := reason.retires()
	if se.User != "" {
		// Read the users row directly — NOT user_by_uid, which also returns nil
		// for a user whose identity hasn't loaded and would wrongly look
		// "absent".
		row, err := db_open("db/users.db").row("select status from users where uid=?", se.User)
		if err != nil {
			// Could not tell whether the account exists. Defer rather than
			// retire: a transient users.db error must never be what destroys
			// a live user's schedule.
			info("schedule: deferring %s/%s for user %q: users lookup failed: %v", se.App, se.Event, se.User, err)
			return
		}
		if row != nil {
			status, _ := row["status"].(string)
			if user_pending(&User{Status: status}) {
				info("schedule: deferring %s/%s for user %q: account pending", se.App, se.Event, se.User)
				return
			}
			if reason == schedule_user_absent {
				// The row exists but user_by_uid answered nil: suspended,
				// or the identity did not load. Neither is final.
				info("schedule: deferring %s/%s for user %q: account (status %q) did not resolve", se.App, se.Event, se.User, status)
				return
			}
		} else {
			// No row and no error: the account is gone, whatever the check
			// said.
			retire = true
		}
	}
	if !retire {
		info("schedule: deferring %s/%s for user %q: %s", se.App, se.Event, se.User, reason)
		return
	}
	info("schedule: retiring %s/%s for user %q: %s", se.App, se.Event, se.User, reason)
	schedule_db().exec("delete from schedule where id=?", se.ID)
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

// schedule_claim atomically claims a due event: recurring rows advance by one
// interval, one-shots are held for schedule_retry_seconds. Both are conditional
// on due <= now, so the rows-affected count is what decides the claim.
func schedule_claim(id int64, interval int64) bool {
	db := schedule_db()
	var result int64
	var err error
	moment := now()

	if interval > 0 {
		// Recurring: advance to the first due + k*interval after now. Stepping
		// by one interval keeps the phase but leaves an overdue row still due,
		// and every missed firing of a long outage would then replay
		// back-to-back, one full Starlark run per pass.
		res, e := db.internal.Exec("update schedule set due=due+((?-due)/?+1)*? where id=? and due<=?", moment, interval, interval, id, moment)
		if e == nil {
			result, err = res.RowsAffected()
		}
	} else {
		// One-shot: hold the row past now rather than delete it. schedule_run
		// deletes it once schedule_check passes or gives a final reason; a
		// passing reason leaves it to come due again after the retry delay.
		res, e := db.internal.Exec("update schedule set due=? where id=? and due<=?", moment+schedule_retry_seconds, id, moment)
		if e == nil {
			result, err = res.RowsAffected()
		}
	}

	return err == nil && result > 0
}

// schedule_run executes a single scheduled event
// The event has already been claimed (due moved forward) before this is called
func schedule_run(se ScheduledEvent) {
	defer func() {
		if r := recover(); r != nil {
			warn("schedule panic: %s/%s: %v", se.App, se.Event, r)
		}
	}()

	// Can it run on this host (user + app + active version + handler all
	// present)? If not, handle it quietly — never warn-email; see
	// schedule_handle_unrunnable.
	if reason := schedule_check(&se); reason != schedule_runnable {
		schedule_handle_unrunnable(&se, reason)
		return
	}
	if se.Interval == 0 {
		// A one-shot fires once: the claim only held the row, so it is
		// removed here, before the handler runs, and a handler that crashes
		// does not fire again.
		schedule_delete(se.ID)
	}

	// Run the handler. Normal runs are not logged - the watchdog covers the one
	// case the other log lines miss, a handler that never returns: a stuck line
	// with no matching finished line means it is still wedged.
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
	// These four checks duplicate schedule_check, which schedule_run already ran.
	// They survive only as a TOCTOU backstop, so they log at debug, never
	// warn-email.
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
	user     string
	app      string
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

// se.cancel() -> None: Cancel this scheduled event. A no-op if already
// executed or cancelled, and if the caller is not the owning app and user: the
// same test as mochi.schedule.cancel, since the object is reachable through
// list.
func (se *SlScheduledEvent) sl_cancel(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	user, _ := principal_storage(t)
	app := principal_app(t)
	if app == nil || se.app != app.id {
		return sl.None, nil
	}
	if user == nil || se.user != user.UID {
		return sl.None, nil
	}
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
		user:     se.User,
		app:      se.App,
		event:    se.Event,
		data:     data,
		due:      se.Due,
		interval: se.Interval,
		created:  se.Created,
	}
}

// schedule_integer reads one integer argument as int64. The due time is a unix
// timestamp, and an int32 parse refused dates past 2038 - eleven years out -
// while capping delay and interval at 68 years.
func schedule_integer(value sl.Value) (int64, error) {
	var n int64
	if err := sl.AsInt(value, &n); err != nil {
		return 0, err
	}
	return n, nil
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

	due, err := schedule_integer(args[2])
	if err != nil {
		return sl_error(fn, "invalid time")
	}

	// The row belongs to the storage account, whose database the handler
	// reads when it fires. A public action has no caller to bind it to; its
	// side effects run on the owner's behalf like its reads do.
	user, _ := principal_storage(t)
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
	due_time := due

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

	delay, err := schedule_integer(args[2])
	if err != nil {
		return sl_error(fn, "invalid delay")
	}

	// Get user and app from context
	user, _ := principal_storage(t)
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
	due_time := now() + delay
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

	interval, err := schedule_integer(args[2])
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
	user, _ := principal_storage(t)
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
	due_time := now() + interval

	id, err := schedule_create(uid, app.id, due_time, event, string(data_json), interval)
	if err != nil {
		return sl_error(fn, "%v", err)
	}

	se := &ScheduledEvent{
		ID: id, User: uid, App: app.id, Due: due_time,
		Event: event, Data: string(data_json), Interval: interval, Created: now(),
	}

	return new_starlark_scheduled_event(se), nil
}

// mochi.schedule.get(id) -> ScheduledEvent | None: Get a scheduled event by ID
func api_schedule_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := schedule_integer(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	// Get user and app from context
	user, _ := principal_storage(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	se := schedule_get(id)
	if se == nil {
		return sl.None, nil
	}

	// Verify the event belongs to this app AND this user. A nil user owns nothing:
	// testing `user != nil && ...` would skip the ownership check for anonymous
	// callers, and the ids are sequential rowids.
	if se.App != app.id {
		return sl.None, nil
	}
	if user == nil || se.User != user.UID {
		return sl.None, nil
	}

	return new_starlark_scheduled_event(se), nil
}

// mochi.schedule.cancel(id) -> bool: Cancel a previously scheduled event.
// False when not found, or not owned by the calling app and user (silent).
func api_schedule_cancel(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <id: int>")
	}

	id, err := schedule_integer(args[0])
	if err != nil {
		return sl_error(fn, "invalid id")
	}

	user, _ := principal_storage(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	se := schedule_get(id)
	if se == nil {
		return sl.False, nil
	}
	if se.App != app.id {
		return sl.False, nil
	}
	if user == nil || se.User != user.UID {
		return sl.False, nil
	}

	schedule_delete(id)
	return sl.True, nil
}

// mochi.schedule.list() -> list: List scheduled events for current app and user
func api_schedule_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Get user and app from context
	user, _ := principal_storage(t)
	app := principal_app(t)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	// A nil user owns nothing. Rows with user '' are what anonymous callers
	// create, and listing them for an anonymous caller would hand every
	// visitor every other visitor's events, with cancel on each.
	if user == nil {
		return sl.NewList(nil), nil
	}

	events := schedule_list(app.id, user.UID)
	result := make([]sl.Value, len(events))
	for i, se := range events {
		result[i] = new_starlark_scheduled_event(&se)
	}

	return sl.NewList(result), nil
}
