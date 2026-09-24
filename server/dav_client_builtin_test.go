// Mochi server: the CalDAV builtins as an app calls them
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// TestCaldavBuiltinsAnswerDicts: an app gets a dict whatever happens - a
// missing account, one that cannot hold a calendar, and a Google account with
// no grant each name their error rather than raising - and a working account
// lists the server's calendars through the same call.
func TestCaldavBuiltinsAnswerDicts(t *testing.T) {
	private_endpoints_allowed(t)
	oauth_binding_setup(t)
	user := user_by_uid("u-link")
	db := db_user(user, "user")
	fake := new_dav_fake_app("main")
	server := dav_test_server(t, "caldav", fake)
	db.exec("insert into accounts (id, type, label, identifier, data, created) values ('c1', 'caldav', '', ?, ?, 1)", server.URL+"/people/caldav/", json_encode(map[string]any{"url": server.URL + "/people/caldav/", "username": "u", "password": "p"}))
	db.exec("insert into accounts (id, type, label, identifier, data, created) values ('e1', 'email', '', 'a@b.c', '', 1)")
	db.exec("insert into accounts (id, type, label, identifier, data, created) values ('g1', 'google', '', 'sub', ?, 1)", json_encode(map[string]any{"refresh": "r", "scopes": []string{"openid"}}))

	thread := &sl.Thread{}
	thread.SetLocal("app", &App{id: "internal", internal: &AppVersion{}})
	thread.SetLocal("user", user)
	call := func(name string, fn func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error), args ...sl.Value) map[string]any {
		t.Helper()
		out, err := fn(thread, sl.NewBuiltin(name, fn), sl.Tuple(args), nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return sl_decode(out).(map[string]any)
	}
	if out := call("mochi.caldav.calendars", api_caldav_calendars, sl.String("absent")); out["error"] != "missing" {
		t.Errorf("unknown account = %v, want missing", out)
	}
	if out := call("mochi.caldav.calendars", api_caldav_calendars, sl.String("e1")); out["error"] != "unauthorised" {
		t.Errorf("an email account = %v, want unauthorised", out)
	}
	if out := call("mochi.caldav.calendars", api_caldav_calendars, sl.String("g1")); out["error"] != "unauthorised" {
		t.Errorf("an ungranted google account = %v, want unauthorised", out)
	}
	out := call("mochi.caldav.calendars", api_caldav_calendars, sl.String("c1"))
	calendars, _ := out["calendars"].([]any)
	if len(calendars) != 1 {
		t.Fatalf("calendars = %v", out)
	}
	collection := calendars[0].(map[string]any)["href"].(string)
	if out := call("mochi.caldav.list", api_caldav_list, sl.String("c1"), sl.String(collection)); len(out["objects"].([]any)) != 0 {
		t.Errorf("listing = %v, want empty", out)
	}
	put := call("mochi.caldav.put", api_caldav_put, sl.String("c1"), sl.String(collection+"a.ics"), sl.String(dav_test_event))
	if put["etag"] != `"etag-1"` {
		t.Errorf("put = %v", put)
	}
	hrefs := sl.NewList([]sl.Value{sl.String(collection + "a.ics")})
	got := call("mochi.caldav.get", api_caldav_get, sl.String("c1"), sl.String(collection), hrefs)
	objects, _ := got["objects"].([]any)
	if len(objects) != 1 || objects[0].(map[string]any)["etag"] != `"etag-1"` {
		t.Errorf("get = %v", got)
	}
	if out := call("mochi.caldav.delete", api_caldav_delete, sl.String("c1"), sl.String(collection+"a.ics"), sl.String(`"etag-9"`)); out["error"] != "conflict" {
		t.Errorf("delete of a stale version = %v, want conflict", out)
	}
	if out := call("mochi.caldav.delete", api_caldav_delete, sl.String("c1"), sl.String(collection+"a.ics"), sl.String(`"etag-1"`)); out["error"] != nil {
		t.Errorf("delete = %v", out)
	}
	// Without the permission the call fails as a Starlark error, not a dict.
	thread.SetLocal("app", &App{id: "external"})
	if _, err := api_caldav_calendars(thread, sl.NewBuiltin("mochi.caldav.calendars", api_caldav_calendars), sl.Tuple{sl.String("c1")}, nil); err == nil {
		t.Error("an app without accounts/calendar was served")
	}
}
