// Mochi server: Events
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"fmt"
	sl "go.starlark.net/starlark"
	rd "runtime/debug"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

type Event struct {
	id              int64
	msg_id          string
	from            string
	to              string
	service         string
	event           string
	sender_app      string
	sender_services []string
	peer            string
	content         map[string]any
	user            *User
	app             *App
	db              *DB
	stream          *Stream
}

var (
	events_lock       = &sync.Mutex{}
	event_next  int64 = 1
)

func event_id() int64 {
	events_lock.Lock()
	id := event_next
	event_next = event_next + 1
	events_lock.Unlock()
	return id
}

func init() {
	// Wire the broadcast pending-drain dispatcher (task #82). The
	// drain loop in broadcast_pending_drain_chain calls this for
	// each in-order buffered row; we synthesise an Event from the
	// stored fields and re-run the matching app event handler.
	// Decoupled via a package-level var so broadcast_pending.go
	// doesn't have to depend on the routing graph.
	broadcast_pending_dispatch = broadcast_pending_dispatch_run
}

// broadcast_pending_dispatch_run is the dispatcher installed at
// init. sysdb is the per-app system DB (app.db) where pending rows
// live; the handler itself runs against the app's data DB (opened
// here separately) so mochi.db.* calls reach the app's own tables.
// Returns true if the row's handler ran cleanly (caller advances +
// deletes); false on lookup or handler failure (caller leaves the
// row in pending for a future drain attempt).
func broadcast_pending_dispatch_run(row *broadcast_pending_row, sysdb *DB) bool {
	if sysdb == nil || sysdb.user == nil {
		return false
	}
	a := app_for_service(sysdb.user, row.Service)
	if a == nil {
		return false
	}
	av := a.active(sysdb.user)
	if av == nil {
		return false
	}
	apps_lock.Lock()
	ae, ok := av.Events[row.Event]
	if !ok {
		ae, ok = av.Events[""]
	}
	apps_lock.Unlock()
	if !ok {
		return false
	}
	var content map[string]any
	if err := cbor.Unmarshal(row.Content, &content); err != nil {
		info("Broadcast pending drain: decode content failed for seq=%d (peer=%s, key=%s): %v", row.Sequence, row.Peer, row.Key, err)
		return false
	}
	// Open the data DB so the handler's mochi.db.* writes land in
	// the app's regular tables. Apps without a declared data DB
	// (av.Database.File == "") get e.db == nil; the handler is
	// expected to cope (most don't call mochi.db at all).
	var datadb *DB
	if av.Database.File != "" {
		datadb = db_app(sysdb.user, a)
		if datadb == nil {
			debug("Broadcast pending drain: cannot open data DB for app %q (user %q)", a.id, sysdb.user.UID)
			return false
		}
		defer datadb.close()
	}
	e := &Event{
		id:              event_id(),
		msg_id:          row.MsgID,
		from:            row.Source,
		to:              row.Target,
		service:         row.Service,
		event:           row.Event,
		sender_app:      row.SenderApp,
		sender_services: split_services(row.SenderServices),
		peer:            row.Peer,
		content:         content,
		user:            sysdb.user,
		app:             a,
		db:              datadb,
	}
	if err := e.run_handler(a, av, ae); err != nil {
		debug("Broadcast pending drain: handler failed for seq=%d (peer=%s, key=%s): %v", row.Sequence, row.Peer, row.Key, err)
		return false
	}
	return true
}

// Get a string field from the content segment of a received event
func (e *Event) get(field string, def string) string {
	result, found := e.content[field]
	if found {
		if s, ok := result.(string); ok {
			return s
		}
	}
	return def
}

// Route a received event to the correct app. Returns error on failure.
func (e *Event) route() error {
	// Resolve fingerprint to entity ID
	if e.to != "" && valid(e.to, "fingerprint") {
		ent := entity_by_any(e.to)
		if ent != nil {
			e.to = ent.ID
		}
	}

	if e.to != "" {
		e.user = user_owning_entity(e.to)
		if e.user == nil {
			//debug("Event dropping to unknown user %q", e.to)
			return fmt.Errorf("unknown user %q", e.to)
		}
	}

	// Find which app handles this event
	// First check if the target entity is an app that handles this event
	a := app_by_id(e.to)
	if a != nil {
		// Check if this app actually handles the requested event
		av := a.active(e.user)
		if av != nil {
			apps_lock.Lock()
			_, hasEvent := av.Events[e.event]
			if !hasEvent {
				_, hasEvent = av.Events[""]
			}
			apps_lock.Unlock()
			if !hasEvent {
				// App doesn't handle this event, fall back to service lookup
				a = nil
			}
		} else {
			// No active version for this app
			a = nil
		}
	}
	if a == nil {
		// Fall back to looking up by service (respecting user preferences)
		a = app_for_service(e.user, e.service)
	}
	if a == nil {
		debug("Event dropping to unknown service %q", e.service)
		return fmt.Errorf("unknown service %q", e.service)
	}
	e.app = a
	if dev_reload && a.latest != nil {
		a.latest.reload()
	}

	// Get the version to use for this event
	av := a.active(e.user)

	// Handle built-in attachment events
	// This must happen before the event lookup since _attachment/* events aren't registered in app.json
	// System database (app.db) is always available, even for apps without a declared database file
	if strings.HasPrefix(e.event, "_attachment/") {
		if e.from == "" {
			info("Event dropping unsigned attachment event")
			audit_message_rejected("", "unsigned")
			return fmt.Errorf("unsigned attachment event")
		}
		if e.user == nil {
			info("Event dropping attachment event for nil user")
			return fmt.Errorf("attachment event requires user")
		}
		if !string_in_slice(e.service, e.sender_services) {
			info("Event dropping attachment event: sender does not handle service %q", e.service)
			return fmt.Errorf("sender does not handle service %q", e.service)
		}
		e.db = db_app_system(e.user, a)
		if e.db == nil {
			info("Event app %q failed to open system database", a.id)
			return fmt.Errorf("no system database")
		}
		defer e.db.close()

		switch e.event {
		case "_attachment/create":
			e.attachment_event_create()
			return nil
		case "_attachment/insert":
			e.attachment_event_insert()
			return nil
		case "_attachment/update":
			e.attachment_event_update()
			return nil
		case "_attachment/move":
			e.attachment_event_move()
			return nil
		case "_attachment/delete":
			e.attachment_event_delete()
			return nil
		case "_attachment/clear":
			e.attachment_event_clear()
			return nil
		case "_attachment/data":
			e.attachment_event_data()
			return nil
		case "_attachment/fetch":
			e.attachment_event_fetch()
			return nil
		}
	}

	// System broadcast events. Handled internally, bypassing app-level
	// event registration since every subscription app gets the same
	// mechanism for free.
	if e.event == "broadcast/resync" || e.event == "broadcast/acknowledge" {
		if e.from == "" {
			info("Event dropping unsigned broadcast event %q to app %q", e.event, a.id)
			return fmt.Errorf("unsigned broadcast event")
		}
		if e.user == nil {
			info("Event dropping broadcast event for nil user")
			return fmt.Errorf("broadcast event requires user")
		}
		// Broadcast tables (_log, _received, _acknowledged, _sequence,
		// _broadcast_pending) live in the per-app system DB (app.db),
		// not the app's writable data DB, so apps can't tamper with
		// the metadata the server reads back trustingly. Same pattern
		// as attachments and access. Migration target: claude/sessions/
		// 2026-05-25-broadcast-app-db-move.md.
		e.db = db_app_system(e.user, a)
		if e.db == nil {
			info("Event app %q failed to open system database", a.id)
			return fmt.Errorf("no system database")
		}
		defer e.db.close()
		switch e.event {
		case "broadcast/resync":
			return e.broadcast_resync(a, av)
		case "broadcast/acknowledge":
			return e.broadcast_acknowledge()
		}
	}

	// Find event in app
	apps_lock.Lock()
	ae, found := av.Events[e.event]
	if !found {
		ae, found = av.Events[""]
	}
	apps_lock.Unlock()

	if !found {
		debug("Event dropping to unknown event %q in app %q for service %q", e.event, a.id, e.service)
		return fmt.Errorf("unknown event %q", e.event)
	}

	// Load a database file for the app
	if av.Database.File != "" {
		if e.user == nil {
			info("Event dropping broadcast for service requiring user")
			return fmt.Errorf("broadcast for service requiring user")
		}

		e.db = db_app(e.user, a)
		if e.db == nil {
			info("Event app %q has no database file", a.id)
			return fmt.Errorf("no database file")
		}
		defer e.db.close()
	}

	// Reject events without a verified sender (unless anonymous is allowed)
	if e.from == "" && !ae.Anonymous {
		info("Event dropping unsigned message to app %q", a.id)
		audit_message_rejected("", "unsigned")
		return fmt.Errorf("unsigned message")
	}

	// Check sender app against allowed apps list (if specified)
	if len(ae.Apps) > 0 {
		allowed := false
		for _, entry := range ae.Apps {
			switch v := entry.(type) {
			case bool:
				if v && e.sender_app == a.id {
					allowed = true
				}
			case string:
				if e.sender_app == v {
					allowed = true
				}
			}
		}
		if !allowed {
			info("Event dropping message from app %q not in allowed list for event %q in app %q", e.sender_app, e.event, a.id)
			return fmt.Errorf("app %q not allowed", e.sender_app)
		}
	}

	// Check sender services against allowed services list (if specified)
	// Skip for anonymous senders (e.from == "") since they have no services context
	if len(ae.Services) > 0 && e.from != "" {
		allowed := false
		for _, entry := range ae.Services {
			if entry == "." {
				// "." means sender must handle the same service being called
				if string_in_slice(e.service, e.sender_services) {
					allowed = true
				}
			} else {
				// Named service: sender must handle that specific service
				if string_in_slice(entry, e.sender_services) {
					allowed = true
				}
			}
		}
		if !allowed {
			info("Event dropping message: sender services %v not in allowed list for event %q in app %q", e.sender_services, e.event, a.id)
			return fmt.Errorf("sender services not allowed")
		}
	}

	// Broadcast gap detection. Events carrying _key + _sequence in
	// content are part of a sequenced broadcast stream from e.peer.
	// We dedup duplicates, BUFFER out-of-order events in
	// _broadcast_pending and ACK them (sender can drop the queue
	// row, subscriber's resync path fills the missing predecessors,
	// drain replays the buffered ones in chain order), and advance
	// _received after a successful handler.
	//
	// Broadcast tracking lives in the per-app SYSTEM DB (app.db)
	// alongside attachments and access, so apps can't tamper with
	// the metadata the server reads back trustingly. The handler
	// itself still runs against e.db (the app's data DB) so
	// mochi.db.* calls reach the app's own tables. Two DBs open
	// during a broadcast event; the system DB closes when route()
	// returns.
	bkey, _ := e.content["_key"].(string)
	bseq := event_int64(e.content["_sequence"])
	broadcast_check := bkey != "" && bseq > 0 && e.peer != "" && e.user != nil
	var bdb *DB
	if broadcast_check {
		bdb = db_app_system(e.user, a)
		if bdb == nil {
			info("Event app %q failed to open system database for broadcast tracking", a.id)
			return fmt.Errorf("no system database")
		}
		defer bdb.close()
		last := broadcast_received_get(bdb, e.peer, bkey)
		if bseq <= last {
			//debug("Broadcast duplicate seq=%d <= last=%d for (peer=%s, key=%s)", bseq, last, e.peer, bkey)
			return nil
		}
		if bseq > last+1 {
			debug("Broadcast gap seq=%d > last+1=%d for (peer=%s, key=%s); buffering + requesting resync", bseq, last+1, e.peer, bkey)
			go broadcast_request_resync(e.user, a, e.to, e.from, bkey, e.peer, last)
			stored := broadcast_pending_insert(bdb, e.peer, bkey, bseq,
				e.from, e.to, e.service, e.event, e.msg_id, e.sender_app,
				strings.Join(e.sender_services, ","),
				cbor_encode(e.content))
			if !stored {
				// Buffer is full. NACK with pending-full reason
				// so the sender keeps the row queued and retries
				// with exponential backoff; the buffer drains as
				// resync advances _received and frees slots. ACKing
				// here would silently lose the event - the sender
				// deletes the queue row on ACK and the receiver
				// would never see this seq again unless a later
				// resync round happened to cover it.
				return fmt.Errorf("pending buffer full for (peer=%s, key=%s): %w", e.peer, bkey, ErrBroadcastPendingFull)
			}
			// ACK to the sender (return nil) - we have the event
			// stored; the sender's queue can delete the row. The
			// gap fills as resync replies arrive and advance
			// _received, at which point the buffered tail drains.
			return nil
		}
	}

	// Check which engine the app uses, and run it
	handler_err := e.run_handler(a, av, ae)

	if broadcast_check && handler_err == nil {
		broadcast_advance_local(bdb, e.peer, bkey, bseq)
		broadcast_send_ack(e.user, a, e.to, e.from, bkey, e.peer, bseq)
	}
	return handler_err
}

// run_handler invokes the per-app event handler under the correct
// engine. Factored out so the broadcast wrapper can call it once and
// check the result.
func (e *Event) run_handler(a *App, av *AppVersion, ae AppEvent) (handler_err error) {
	switch av.Architecture.Engine {
	case "": // Internal app
		if ae.internal_function == nil {
			info("Event dropping to event %q in internal app %q for service %q without handler", e.event, a.id, e.service)
			return fmt.Errorf("no handler for event %q", e.event)
		}

		defer func() {
			r := recover()
			if r != nil {
				warn("Event handler error: %v\n\n%s", r, string(rd.Stack()))
				handler_err = fmt.Errorf("handler panic: %v", r)
			}
		}()

		ae.internal_function(e)
		return handler_err

	case "starlark":
		if ae.Function == "" {
			info("Event dropping to event %q in internal app %q for service %q without handler", e.event, a.id, e.service)
			return fmt.Errorf("no handler for event %q", e.event)
		}

		s := av.starlark()
		s.set("event", e)
		s.set("app", a)
		s.set("user", e.user)
		s.set("owner", e.user)

		//debug("App event %s:%s(): %v", a.id, ae.Function, e)
		s.call(ae.Function, sl.Tuple{e})
		return nil

	default:
		info("Event unknown engine %q version %q", av.Architecture.Engine, av.Architecture.Version)
		return fmt.Errorf("unknown engine %q", av.Architecture.Engine)
	}
}

// event_int64 normalises a content field that may decode as int64,
// uint64, or float64 depending on the JSON/CBOR path.
func event_int64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case uint64:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}

// Decode the next segment from a received event. Lazy-creates the
// CBOR decoder if it isn't set up yet — needed for /mochi/2/stream
// callers that reach segment() before any s.read() has constructed
// the decoder.
func (e *Event) segment(v any) bool {
	if e.stream == nil || e.stream.reader == nil {
		return false
	}
	if e.stream.decoder == nil {
		e.stream.decoder = cbor_decode_mode.NewDecoder(e.stream.reader)
	}
	err := e.stream.decoder.Decode(v)
	if err != nil {
		info("Event unable to decode segment: %v", err)
		return false
	}
	return true
}

// Starlark methods
func (e *Event) AttrNames() []string {
	return []string{"content", "dump", "header", "read", "stream", "user", "write"}
}

func (e *Event) Attr(name string) (sl.Value, error) {
	switch name {
	case "content":
		return sl.NewBuiltin("content", e.sl_content), nil
	case "dump":
		return sl.NewBuiltin("dump", e.sl_dump), nil
	case "header":
		return sl.NewBuiltin("header", e.sl_header), nil
	case "read":
		return &EventRead{event: e}, nil
	case "stream":
		return e.stream, nil
	case "user":
		return e.user, nil
	case "write":
		return &EventWrite{event: e}, nil
	default:
		return nil, nil
	}
}

// EventRead is callable as e.read() and exposes e.read.file(path).
type EventRead struct {
	event *Event
}

func (er *EventRead) String() string        { return "event.read" }
func (er *EventRead) Type() string          { return "event.read" }
func (er *EventRead) Freeze()               {}
func (er *EventRead) Truth() sl.Bool        { return true }
func (er *EventRead) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable: event.read") }
func (er *EventRead) Name() string          { return "read" }

// Callable: e.read() -> dict | None: Read the next decoded segment from the event's stream
func (er *EventRead) CallInternal(t *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return er.event.stream.sl_read(t, nil, args, kwargs)
}

func (er *EventRead) AttrNames() []string {
	return []string{"file"}
}

func (er *EventRead) Attr(name string) (sl.Value, error) {
	switch name {
	case "file":
		return sl.NewBuiltin("read.file", er.event.stream.sl_read_file), nil
	}
	return nil, nil
}

// EventWrite is callable as e.write(values...) and exposes e.write.{file, asset}.
type EventWrite struct {
	event *Event
}

func (ew *EventWrite) String() string        { return "event.write" }
func (ew *EventWrite) Type() string          { return "event.write" }
func (ew *EventWrite) Freeze()               {}
func (ew *EventWrite) Truth() sl.Bool        { return true }
func (ew *EventWrite) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable: event.write") }
func (ew *EventWrite) Name() string          { return "write" }

// Callable: e.write(values...) -> bool: Write encoded segments to the event's stream
func (ew *EventWrite) CallInternal(t *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return ew.event.stream.sl_write(t, nil, args, kwargs)
}

func (ew *EventWrite) AttrNames() []string {
	return []string{"asset", "file"}
}

func (ew *EventWrite) Attr(name string) (sl.Value, error) {
	switch name {
	case "asset":
		return sl.NewBuiltin("write.asset", ew.event.stream.sl_write_asset), nil
	case "file":
		return sl.NewBuiltin("write.file", ew.event.stream.sl_write_file), nil
	}
	return nil, nil
}

func (e *Event) Freeze() {}

func (e *Event) Hash() (uint32, error) {
	return sl.String(fmt.Sprintf("%d", e.id)).Hash()
}

func (e *Event) String() string {
	return fmt.Sprintf("Event %d", e.id)
}

func (e *Event) Truth() sl.Bool {
	return sl.True
}

func (e *Event) Type() string {
	return "Event"
}

// e.content(field, default?) -> any: Get a content field from the event
func (e *Event) sl_content(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) < 1 || len(args) > 2 {
		return sl_error(fn, "syntax: <field: string>, [default: any]")
	}

	field, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid field %q", field)
	}

	value, found := e.content[field]
	if found {
		return sl_encode(value), nil
	}

	if len(args) > 1 {
		return args[1], nil
	}

	return sl.None, nil
}

// e.dump() -> dict: Return event details as a dictionary
func (e *Event) sl_dump(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sl_encode(map[string]any{"from": e.from, "to": e.to, "service": e.service, "event": e.event, "app": e.sender_app, "services": e.sender_services, "content": e.content}), nil
}

// e.header(name) -> string: Get an event header (from, to, service, event)
func (e *Event) sl_header(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <header: string>")
	}

	header, ok := sl.AsString(args[0])
	if !ok {
		return sl_error(fn, "invalid header %q", header)
	}

	switch header {
	case "from":
		return sl_encode(e.from), nil
	case "to":
		return sl_encode(e.to), nil
	case "service":
		return sl_encode(e.service), nil
	case "event":
		return sl_encode(e.event), nil
	case "app":
		return sl_encode(e.sender_app), nil
	case "services":
		return sl_encode(e.sender_services), nil
	case "peer":
		return sl_encode(e.peer), nil
	default:
		return sl_error(fn, "invalid header %q", header)
	}
}
