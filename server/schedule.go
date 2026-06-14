// Mochi server: Scheduled Events
// Copyright Alistair Cunningham 2025-2026

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
	"leader": sl.NewBuiltin("mochi.schedule.leader", api_schedule_leader),
	"list":   sl.NewBuiltin("mochi.schedule.list", api_schedule_list),
})

// schedule_wake is used to wake up the scheduler when a new event is created
var schedule_wake = make(chan struct{}, 1)

// schedule_db opens the schedule database
func schedule_db() *DB {
	return db_open("db/schedule.db")
}

// schedule_create inserts a new scheduled event and returns its ID.
// Replicates the new row to every host in the user's set so paired
// replicas agree on what is scheduled; the leader-gate on the firing
// side dedups handler execution. System events (user == "") stay
// local - they have no scope identifier for the replication pipeline.
func schedule_create(user string, app string, due int64, event string, data string, interval int64) int64 {
	created := now()
	db := schedule_db()
	result := must(db.internal.Exec("insert into schedule (user, app, due, event, data, interval, created) values (?, ?, ?, ?, ?, ?, ?)",
		user, app, due, event, data, interval, created))
	id, _ := result.LastInsertId()
	if id == 0 {
		return 0
	}

	replication_emit_schedule_insert(user, app, due, event, data, interval, created)

	// Wake up the scheduler to check for the new event
	schedule_notify()

	return id
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

// schedule_delete removes a scheduled event by ID and replicates the
// removal keyed on the natural composite identifier so paired hosts
// drop the matching row. Looks the row up first because the
// autoincrement id is local-only.
func schedule_delete(id int64) {
	db := schedule_db()
	var user, app, event string
	var created int64
	if row, _ := db.row("select user, app, event, created from schedule where id=?", id); row != nil {
		user, _ = row["user"].(string)
		app, _ = row["app"].(string)
		event, _ = row["event"].(string)
		created, _ = row["created"].(int64)
	}
	db.exec("delete from schedule where id=?", id)
	if user != "" && app != "" && event != "" {
		replication_emit_schedule_delete(user, app, event, created)
	}
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
	db.scans(&events, "select * from schedule where due<=? order by due", t)
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

	// ...that has a handler for this event (or a catch-all "").
	apps_lock.Lock()
	_, found := av.Events[se.Event]
	if !found {
		_, found = av.Events[""]
	}
	apps_lock.Unlock()
	return found
}

// schedule_handle_unrunnable deals with a due event that schedule_valid
// rejected. Under replication this is frequently EXPECTED, not an error:
// the host that has the user + app + handler runs it (leader-gating dedups
// the side effects), and a host still bootstrapping a user must not touch
// that user's just-replicated rows. So we stay quiet (no admin email) and,
// crucially, never replicate a delete — a peer at a different app version
// may run this event fine, and propagating a delete would wipe it there.
//
//   - User absent or still bootstrapping (pending): defer silently — a
//     peer runs it; dropping it risks losing a just-replicated schedule
//     before its app/data finishes landing.
//   - Active user, or a system event (host-local), whose app / version /
//     handler is genuinely gone here: a recurring row would otherwise
//     re-fire every interval forever, so drop it LOCALLY (no replicated
//     delete) to stop the churn. One-shot rows were already removed by
//     schedule_claim, so nothing to do for them.
func schedule_handle_unrunnable(se *ScheduledEvent) {
	if se.User != "" {
		// Decide on the user's replication status alone — NOT user_by_uid,
		// which also returns nil for a user whose identity hasn't loaded and
		// would wrongly look "absent". A missing row, or a still-bootstrapping
		// (pending) user, means defer: a peer runs the event, and dropping a
		// just-replicated row before its data lands would lose it.
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
		go schedule_run(item)
	}
}

// schedule_claim atomically claims a scheduled event for execution.
// Returns true if this call claimed the event, false if it was already
// claimed.
//
// Recurring case: the "due = due + interval" UPDATE is intentionally
// NOT replicated. Both replicas hit schedule_due at the same instant
// and apply the same deterministic advance; replicating would just
// duplicate the wire traffic with no convergence benefit.
//
// One-shot case: when the local delete actually fires (rows-affected
// > 0) we look up the natural-key fields and replicate a delete so
// paired replicas drop the row. Skipped if a concurrent claim on
// another goroutine already removed it (rows-affected = 0).
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
		// One-shot: look up natural-key fields first so we can emit a
		// keyed delete after the local DELETE commits.
		var user, app, event string
		var created int64
		if row, _ := db.row("select user, app, event, created from schedule where id=?", id); row != nil {
			user, _ = row["user"].(string)
			app, _ = row["app"].(string)
			event, _ = row["event"].(string)
			created, _ = row["created"].(int64)
		}
		res, e := db.internal.Exec("delete from schedule where id = ? and due <= ?", id, now())
		if e == nil {
			result, err = res.RowsAffected()
		}
		if err == nil && result > 0 && user != "" && app != "" && event != "" {
			replication_emit_schedule_delete(user, app, event, created)
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
	// present)? If not, handle it quietly and replication-safely — never
	// warn-email or replicate a delete; see schedule_handle_unrunnable.
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

	// Find the event handler
	apps_lock.Lock()
	ae, found := av.Events[se.Event]
	if !found {
		ae, found = av.Events[""]
	}
	apps_lock.Unlock()

	if !found {
		debug("schedule: event %q not found in app %q", se.Event, se.App)
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
	s := av.starlark()
	s.set("event", sew)
	s.set("app", app)
	s.set("user", user)
	s.set("owner", user)

	s.call(ae.Function, sl.Tuple{sew})
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
	user := t.Local("user").(*User)
	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	var uid string
	if user != nil {
		uid = user.UID
	}

	// Serialize data
	data_json, _ := json.Marshal(data_map)

	// If time is in the past, run immediately (but still schedule for audit trail)
	due_time := int64(due)

	id := schedule_create(uid, app.id, due_time, event, string(data_json), 0)
	if id == 0 {
		return sl_error(fn, "failed to create scheduled event")
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
	user := t.Local("user").(*User)
	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	var uid string
	if user != nil {
		uid = user.UID
	}

	// Serialize data
	data_json, _ := json.Marshal(data_map)

	// If delay is zero or negative, run immediately
	due_time := now() + int64(delay)
	if delay <= 0 {
		due_time = now()
	}

	id := schedule_create(uid, app.id, due_time, event, string(data_json), 0)
	if id == 0 {
		return sl_error(fn, "failed to create scheduled event")
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

	// Minimum interval is 1 second
	if interval < 1 {
		interval = 1
	}

	// Get user and app from context
	user := t.Local("user").(*User)
	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	var uid string
	if user != nil {
		uid = user.UID
	}

	// Serialize data
	data_json, _ := json.Marshal(data_map)

	// First run is after the interval
	due_time := now() + int64(interval)

	id := schedule_create(uid, app.id, due_time, event, string(data_json), int64(interval))
	if id == 0 {
		return sl_error(fn, "failed to create scheduled event")
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
	user := t.Local("user").(*User)
	app := t.Local("app").(*App)
	if app == nil {
		return sl_error(fn, "no app context")
	}

	se := schedule_get(int64(id))
	if se == nil {
		return sl.None, nil
	}

	// Verify the event belongs to this app and user
	if se.App != app.id {
		return sl.None, nil
	}
	if user != nil && se.User != user.UID {
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

	user := t.Local("user").(*User)
	app := t.Local("app").(*App)
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
	if user != nil && se.User != user.UID {
		return sl.False, nil
	}

	schedule_delete(int64(id))
	return sl.True, nil
}

// mochi.schedule.list() -> list: List scheduled events for current app and user
func api_schedule_list(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// Get user and app from context
	user := t.Local("user").(*User)
	app := t.Local("app").(*App)
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
