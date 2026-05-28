// Mochi server: server-to-app error events
//
// Core-originated failure callbacks. When an operation an app initiated
// fails terminally, core calls the app's declared handler (the `errors`
// block in app.json) in-process, on the host that observed the failure —
// never queued or routed. See claude/plans/error-events.md.
//
// The catalogue below is fixed and core-owned, like HTTP status codes;
// apps subscribe to codes, they don't invent them. Dispatch mirrors the
// starlark branch of (*Event).run_handler / schedule_run_event: the
// handler's mochi.db.* resolves to db_app(user, app) via the user/app
// thread-locals.
//
// Copyright Alistair Cunningham 2026

package main

import (
	rd "runtime/debug"

	sl "go.starlark.net/starlark"
)

// The fixed error catalogue. Each message/* code is named after the
// mochi.X.send namespace whose send failed.
const (
	error_code_message_unknown  = "message/unknown"  // a host of the recipient reported unknown_user
	error_code_message_timeout  = "message/timeout"  // a send aged out of the queue undelivered
	error_code_message_rejected = "message/rejected" // authoritative refusal from the receiver
	error_code_broadcast_gap    = "broadcast/gap"    // an unfillable broadcast gap was skipped
)

// error_catalogue is the set of codes core emits, used for app-load
// validation (warn on a declared key that isn't here — almost always a typo).
var error_catalogue = map[string]bool{
	error_code_message_unknown:  true,
	error_code_message_timeout:  true,
	error_code_message_rejected: true,
	error_code_broadcast_gap:    true,
}

// ErrorEvent is the Starlark object bound as `e` in an error handler. The
// top-level shape is frozen; per-code specifics live in detail. See the
// handler-object table in claude/plans/error-events.md.
type ErrorEvent struct {
	code     string         // which error — dotted category/leaf
	reason   string         // finer discriminator within a code
	service  string         // app service the error occurred in
	entity   string         // far-end entity the error concerns (not the owner)
	original map[string]any // the operation that triggered it, when tied to a message
	when     int64          // when it occurred, unix seconds, core-computed
	detail   map[string]any // category-specific payload
	user     *User
	app      *App
}

func (e *ErrorEvent) AttrNames() []string {
	return []string{"code", "detail", "entity", "original", "reason", "service", "time"}
}

func (e *ErrorEvent) Attr(name string) (sl.Value, error) {
	switch name {
	case "code":
		return sl.String(e.code), nil
	case "reason":
		return sl.String(e.reason), nil
	case "service":
		return sl.String(e.service), nil
	case "entity":
		return sl.String(e.entity), nil
	case "original":
		return sl_encode(e.original), nil
	case "time":
		return sl.MakeInt64(e.when), nil
	case "detail":
		return sl_encode(e.detail), nil
	default:
		return nil, nil
	}
}

func (e *ErrorEvent) Freeze()               {}
func (e *ErrorEvent) Hash() (uint32, error) { return 0, nil }
func (e *ErrorEvent) String() string        { return "error" }
func (e *ErrorEvent) Truth() sl.Bool        { return sl.True }
func (e *ErrorEvent) Type() string          { return "error" }

// error_dispatch delivers a core-originated error event to the sending
// app's declared handler, in-process on this host. It is a no-op when the
// app declares no handler for code — and crucially the detail thunk is NOT
// invoked in that case, so callers may pass an expensive detail builder
// (e.g. an entity_peers lookup) freely. Fires per host, deliberately
// ungated: each host reacts to its own observation.
func error_dispatch(user *User, app *App, code, reason, service, entity string, original map[string]any, detail func() map[string]any) {
	if user == nil || app == nil {
		return
	}
	av := app.active(user)
	if av == nil {
		return
	}

	apps_lock.Lock()
	ae, ok := av.Errors[code]
	if !ok {
		ae, ok = av.Errors[""]
	}
	apps_lock.Unlock()
	if !ok || ae.Function == "" {
		return
	}
	if av.Architecture.Engine != "starlark" {
		return
	}

	var d map[string]any
	if detail != nil {
		d = detail()
	}
	if d == nil {
		d = map[string]any{}
	}
	if original == nil {
		original = map[string]any{}
	}

	e := &ErrorEvent{
		code:     code,
		reason:   reason,
		service:  service,
		entity:   entity,
		original: original,
		when:     now(),
		detail:   d,
		user:     user,
		app:      app,
	}

	defer func() {
		if r := recover(); r != nil {
			warn("Error handler panic: %s code=%q app=%q: %v\n\n%s", ae.Function, code, app.id, r, string(rd.Stack()))
		}
	}()

	s := av.starlark()
	s.set("event", e)
	s.set("app", app)
	s.set("user", user)
	s.set("owner", user)
	if _, err := s.call(ae.Function, sl.Tuple{e}); err != nil {
		warn("Error handler %s code=%q app=%q failed: %v", ae.Function, code, app.id, err)
	}
}

// error_code_for_nack maps a receiver NACK reason (the fail_* vocabulary)
// to the error code + reason to dispatch. ok is false for reasons that
// dispatch nothing (e.g. fail_dedup — a benign duplicate).
func error_code_for_nack(nack string) (code, reason string, ok bool) {
	switch nack {
	case fail_unknown_user:
		return error_code_message_unknown, "unknown", true
	case fail_unsupported:
		return error_code_message_rejected, "unsupported", true
	case fail_expired:
		return error_code_message_rejected, "expired", true
	case fail_signature_invalid:
		return error_code_message_rejected, "signature", true
	}
	return "", "", false
}

// error_catalogue_validate warns about `errors`-block keys that aren't in
// the fixed catalogue — almost always a typo, since the catalogue is
// core-owned and apps only subscribe to codes core emits. Returns the
// unknown codes (for tests; callers may ignore).
func error_catalogue_validate(av *AppVersion, id string) []string {
	var unknown []string
	for code := range av.Errors {
		if code == "" {
			continue
		}
		if !error_catalogue[code] {
			unknown = append(unknown, code)
			info("App %q declares unknown error code %q (not in the core catalogue)", id, code)
		}
	}
	return unknown
}
