// Mochi server: server-to-app error event unit tests
// Copyright Alistair Cunningham 2026

package main

import (
	"encoding/json"
	"os"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestErrorsBlockParse confirms an app.json `errors` block unmarshals into
// AppVersion.Errors, and that an unrelated unknown key is ignored (the
// inert-on-old-server property relies on json.Unmarshal's leniency).
func TestErrorsBlockParse(t *testing.T) {
	data := []byte(`{
		"version": "1.0",
		"errors": {
			"message/timeout": {"function": "error_message_timeout"},
			"broadcast/gap": {"function": "error_broadcast_gap"}
		},
		"some_future_key": {"ignored": true}
	}`)
	var av AppVersion
	if err := json.Unmarshal(data, &av); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(av.Errors) != 2 {
		t.Fatalf("Errors len = %d, want 2", len(av.Errors))
	}
	if got := av.Errors[error_code_message_timeout].Function; got != "error_message_timeout" {
		t.Errorf("message/timeout function = %q, want error_message_timeout", got)
	}
	if got := av.Errors[error_code_broadcast_gap].Function; got != "error_broadcast_gap" {
		t.Errorf("broadcast/gap function = %q, want error_broadcast_gap", got)
	}
}

// TestErrorCodeForNack covers the fail_* -> (code, reason) mapping,
// including reasons that dispatch nothing.
func TestErrorCodeForNack(t *testing.T) {
	cases := []struct {
		nack       string
		wantCode   string
		wantReason string
		wantOK     bool
	}{
		{fail_unknown_user, error_code_message_unknown, "unknown", true},
		{fail_unsupported, error_code_message_rejected, "unsupported", true},
		{fail_expired, error_code_message_rejected, "expired", true},
		{fail_signature_invalid, error_code_message_rejected, "signature", true},
		{fail_dedup, "", "", false},
		{"", "", "", false},
		{"not_a_reason", "", "", false},
	}
	for _, c := range cases {
		code, reason, ok := error_code_for_nack(c.nack)
		if code != c.wantCode || reason != c.wantReason || ok != c.wantOK {
			t.Errorf("error_code_for_nack(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.nack, code, reason, ok, c.wantCode, c.wantReason, c.wantOK)
		}
	}
}

// TestErrorCatalogue confirms the fixed catalogue and that validation
// accepts known codes and flags unknown ones.
func TestErrorCatalogue(t *testing.T) {
	want := []string{
		error_code_message_unknown,
		error_code_message_timeout,
		error_code_message_rejected,
		error_code_broadcast_gap,
	}
	if len(error_catalogue) != len(want) {
		t.Errorf("catalogue size = %d, want %d", len(error_catalogue), len(want))
	}
	for _, c := range want {
		if !error_catalogue[c] {
			t.Errorf("catalogue missing %q", c)
		}
	}

	// Known codes: no warnings.
	ok := &AppVersion{Errors: map[string]AppError{
		error_code_message_timeout: {Function: "a"},
		error_code_broadcast_gap:   {Function: "b"},
	}}
	if u := error_catalogue_validate(ok, "test"); len(u) != 0 {
		t.Errorf("valid codes flagged as unknown: %v", u)
	}

	// An unknown code is reported (and "" is skipped, not flagged).
	bad := &AppVersion{Errors: map[string]AppError{
		error_code_message_timeout: {Function: "a"},
		"message/bogus":            {Function: "b"},
		"":                         {Function: "c"},
	}}
	u := error_catalogue_validate(bad, "test")
	if len(u) != 1 || u[0] != "message/bogus" {
		t.Errorf("unknown codes = %v, want [message/bogus]", u)
	}
}

// TestErrorEventAttr confirms every handler-object field is exposed to
// Starlark with the right value/type.
func TestErrorEventAttr(t *testing.T) {
	e := &ErrorEvent{
		code:    error_code_message_timeout,
		reason:  "timeout",
		service: "feeds",
		entity:  "ent123",
		original: map[string]any{
			"service": "feeds",
			"event":   "post/create",
			"message": "msg9",
		},
		when:   1748390400,
		detail: map[string]any{"locations": int64(0)},
	}

	str := func(name, want string) {
		t.Helper()
		v, err := e.Attr(name)
		if err != nil {
			t.Fatalf("Attr(%q): %v", name, err)
		}
		s, ok := sl.AsString(v)
		if !ok || s != want {
			t.Errorf("Attr(%q) = %v, want %q", name, v, want)
		}
	}
	str("code", error_code_message_timeout)
	str("reason", "timeout")
	str("service", "feeds")
	str("entity", "ent123")

	tv, err := e.Attr("time")
	if err != nil {
		t.Fatalf("Attr(time): %v", err)
	}
	ti, ok := tv.(sl.Int)
	if !ok {
		t.Fatalf("time is %T, want sl.Int", tv)
	}
	if n, _ := ti.Int64(); n != 1748390400 {
		t.Errorf("time = %d, want 1748390400", n)
	}

	dv, err := e.Attr("detail")
	if err != nil {
		t.Fatalf("Attr(detail): %v", err)
	}
	dd, ok := dv.(*sl.Dict)
	if !ok {
		t.Fatalf("detail is %T, want *sl.Dict", dv)
	}
	if loc, found, _ := dd.Get(sl.String("locations")); !found {
		t.Error("detail.locations missing")
	} else if li, ok := loc.(sl.Int); !ok {
		t.Errorf("detail.locations is %T, want sl.Int", loc)
	} else if n, _ := li.Int64(); n != 0 {
		t.Errorf("detail.locations = %d, want 0", n)
	}

	ov, err := e.Attr("original")
	if err != nil {
		t.Fatalf("Attr(original): %v", err)
	}
	od, ok := ov.(*sl.Dict)
	if !ok {
		t.Fatalf("original is %T, want *sl.Dict", ov)
	}
	if ev, found, _ := od.Get(sl.String("event")); !found {
		t.Error("original.event missing")
	} else if s, _ := sl.AsString(ev); s != "post/create" {
		t.Errorf("original.event = %v, want post/create", ev)
	}

	// Unknown attribute returns (nil, nil) — the Starlark no-such-attr contract.
	if v, err := e.Attr("nope"); v != nil || err != nil {
		t.Errorf("Attr(nope) = (%v, %v), want (nil, nil)", v, err)
	}

	if len(e.AttrNames()) != 7 {
		t.Errorf("AttrNames = %v, want 7 entries", e.AttrNames())
	}
}

// TestErrorDispatchGate covers error_dispatch's handler gate: with no
// declared handler the detail thunk is never invoked (the lazy-detail
// optimisation) and nothing runs; with a declared starlark handler the
// thunk runs and the real engine executes the handler against the wrapper
// (touching every field).
func TestErrorDispatchGate(t *testing.T) {
	cleanup := create_test_apps_db(t)
	defer cleanup()
	udb := db_open("db/users.db")
	udb.exec("create table if not exists users (id integer primary key, uid text not null default '', username text not null, role text not null default 'user', methods text not null default 'email', status text not null default 'active')")
	udb.exec("insert into users (uid, username) values ('u1', 'test@example.com')")
	os.MkdirAll(data_dir+"/users/1", 0755)
	user := &User{UID: "u1", Username: "test@example.com"}

	// No matching handler -> thunk never invoked, no-op.
	none := &AppVersion{Version: "1.0", Errors: map[string]AppError{}}
	none.Architecture.Engine = "starlark"
	aNone := &App{id: "app-none", versions: map[string]*AppVersion{"1.0": none}}
	aNone.latest = none
	if aNone.active(user) == nil {
		t.Fatal("active(user) returned nil; fixture broken")
	}
	invoked := false
	error_dispatch(user, aNone, error_code_message_timeout, "timeout", "feeds", "ent", nil, func() map[string]any {
		invoked = true
		return map[string]any{"locations": int64(0)}
	})
	if invoked {
		t.Error("detail thunk invoked despite no declared handler (lazy gate broken)")
	}

	// Declared starlark handler -> thunk invoked and the handler runs,
	// reading every wrapper field through the real engine.
	have := &AppVersion{Version: "1.0", Errors: map[string]AppError{
		error_code_message_timeout: {Function: "error_message_timeout"},
	}}
	have.Architecture.Engine = "starlark"
	have.Execute = []string{"def error_message_timeout(e):\n    _ = e.code\n    _ = e.reason\n    _ = e.service\n    _ = e.entity\n    _ = e.original\n    _ = e.time\n    _ = e.detail\n"}
	aHave := &App{id: "app-have", versions: map[string]*AppVersion{"1.0": have}}
	aHave.latest = have
	ran := false
	error_dispatch(user, aHave, error_code_message_timeout, "timeout", "feeds", "ent",
		map[string]any{"service": "feeds", "event": "post/create", "message": "m1"},
		func() map[string]any {
			ran = true
			return map[string]any{"locations": int64(0)}
		})
	if !ran {
		t.Error("detail thunk not invoked despite a declared handler")
	}
}
