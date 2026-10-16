// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// create_presence_test_env is create_linked_test_env with the seen table and
// an empty presence registry, restored afterwards.
func create_presence_test_env(t *testing.T) *User {
	t.Helper()
	alice := create_linked_test_env(t)
	db_open("db/sessions.db").exec("create table if not exists seen (user text primary key, time integer not null)")
	saved := presence_connections
	presence_connections = map[string]map[string]*presence_connection{}
	t.Cleanup(func() { presence_connections = saved })
	return alice
}

// TestPresenceConnections: connections make a user present with the status of
// the most present one; a status set reaches one login session's connections
// and no other; the last connection ending leaves the user offline, seen at
// that moment or at their sessions' last access, whichever is later.
func TestPresenceConnections(t *testing.T) {
	alice := create_presence_test_env(t)
	if status, seen, connections := presence_user(alice.UID); status != "offline" || seen != 0 || len(connections) != 0 {
		t.Fatalf("a user never seen = %s %d %v", status, seen, connections)
	}
	presence_connect(alice.UID, "c1", "s1", "web", "")
	presence_connect(alice.UID, "c2", "", "xmpp", "phone")
	status, seen, connections := presence_user(alice.UID)
	if status != "online" || seen < now()-1 || len(connections) != 2 || connections[0].ID != "c1" || connections[1].Kind != "xmpp" || connections[1].Device != "phone" {
		t.Fatalf("two connections = %s %d %+v", status, seen, connections)
	}

	if changed := presence_set(alice.UID, "s1", "", "busy", "in a call"); changed != 1 {
		t.Errorf("set by session changed %d, want 1", changed)
	}
	if changed := presence_set(alice.UID, "", "", "nonsense", ""); changed != 0 {
		t.Errorf("an invalid status changed %d", changed)
	}
	status, _, connections = presence_user(alice.UID)
	if status != "online" || connections[0].Status != "busy" || connections[0].Message != "in a call" || connections[1].Status != "online" {
		t.Errorf("after one busy = %s %+v", status, connections)
	}
	if changed := presence_set(alice.UID, "", "c2", "away", ""); changed != 1 {
		t.Errorf("set by connection changed %d, want 1", changed)
	}
	if status, _, _ := presence_user(alice.UID); status != "busy" {
		t.Errorf("busy and away = %s, want busy", status)
	}
	if changed := presence_set(alice.UID, "", "", "away", ""); changed != 2 {
		t.Errorf("set for the user changed %d, want 2", changed)
	}
	if status, _, _ := presence_user(alice.UID); status != "away" {
		t.Errorf("all away = %s", status)
	}

	presence_disconnect(alice.UID, "c1")
	if status, _, connections := presence_user(alice.UID); status != "away" || len(connections) != 1 || connections[0].ID != "c2" {
		t.Errorf("one left = %s %+v", status, connections)
	}
	presence_disconnect(alice.UID, "c2")
	presence_disconnect(alice.UID, "c2")
	status, seen, connections = presence_user(alice.UID)
	if status != "offline" || len(connections) != 0 || seen < now()-1 {
		t.Errorf("none left = %s %d %v", status, seen, connections)
	}
	if _, found := presence_connections[alice.UID]; found {
		t.Error("an empty registry entry was kept")
	}

	// A later access of a session is the later moment.
	db := db_open("db/sessions.db")
	db.exec("update seen set time=? where user=?", now()-600, alice.UID)
	db.exec("insert into sessions (user, code, secret, expires, created, accessed) values (?, 'code1', '', ?, ?, ?)", alice.UID, now()+3600, now()-900, now()-300)
	if _, seen, _ := presence_user(alice.UID); seen != presence_seen(alice.UID) || seen < now()-301 || seen > now()-299 {
		t.Errorf("seen = %d, want the session's access %d", seen, now()-300)
	}
	db.exec("update seen set time=? where user=?", now()-100, alice.UID)
	if _, seen, _ := presence_user(alice.UID); seen < now()-101 || seen > now()-99 {
		t.Errorf("seen = %d, want the recorded moment %d", seen, now()-100)
	}
}

// TestPresenceAPI: the user's own presence is read with presence/read - the
// status, its text and when they were last seen - and their connections only
// with user/sessions/read as well; the status of this login session's
// connections is set with user/sessions/write; a bad status or no permission
// is refused.
func TestPresenceAPI(t *testing.T) {
	alice := create_presence_test_env(t)
	app := create_external_app("app1")
	thread := create_test_thread(alice, app)
	thread.SetLocal("session", "s1")
	fn := sl.NewBuiltin("test", nil)
	presence_connect(alice.UID, "c1", "s1", "web", "")
	presence_connect(alice.UID, "c2", "s2", "web", "")

	if _, err := api_presence_get(thread, fn, nil, nil); err == nil || !strings.Contains(err.Error(), "permission") {
		t.Errorf("get without permission: %v", err)
	}
	permission_grant(alice, app.id, "user/sessions/read")
	if _, err := api_presence_get(thread, fn, nil, nil); err == nil || !strings.Contains(err.Error(), "presence/read") {
		t.Errorf("get with the sessions permission alone: %v", err)
	}
	permission_revoke(alice, app.id, "user/sessions/read")
	permission_grant(alice, app.id, "presence/read")
	got := decoded(t)(api_presence_get(thread, fn, nil, nil))
	if _, listed := got["connections"]; got["status"] != "online" || listed {
		t.Errorf("get with presence/read alone = %v, want the status without the connections", got)
	}
	permission_grant(alice, app.id, "user/sessions/read")
	got = decoded(t)(api_presence_get(thread, fn, nil, nil))
	if connections, _ := got["connections"].([]any); got["status"] != "online" || len(connections) != 2 || connections[0].(map[string]any)["session"] != "s1" {
		t.Errorf("get = %v", got)
	}
	if _, err := api_presence_set(thread, fn, sl.Tuple{sl.String("away")}, nil); err == nil || !strings.Contains(err.Error(), "permission") {
		t.Errorf("set without permission: %v", err)
	}
	permission_grant(alice, app.id, "user/sessions/write")
	if value, err := api_presence_set(thread, fn, sl.Tuple{sl.String("away")}, linked_kwargs("message", "back soon")); err != nil || value.String() != "1" {
		t.Errorf("set = %v %v", value, err)
	}
	if _, err := api_presence_set(thread, fn, sl.Tuple{sl.String("invisible")}, nil); err == nil {
		t.Error("an unknown status was set")
	}
	if _, err := api_presence_set(thread, fn, sl.Tuple{sl.String("away")}, linked_kwargs("message", strings.Repeat("x", 201))); err == nil {
		t.Error("an over-long status text was set")
	}
	got = decoded(t)(api_presence_get(thread, fn, nil, nil))
	connections, _ := got["connections"].([]any)
	if got["status"] != "online" || connections[0].(map[string]any)["status"] != "away" || connections[0].(map[string]any)["message"] != "back soon" || connections[1].(map[string]any)["status"] != "online" {
		t.Errorf("after set = %v", got)
	}
	// The status text is the one of the connection that gives the status.
	presence_set(alice.UID, "s2", "", "online", "at my desk")
	if got = decoded(t)(api_presence_get(thread, fn, nil, nil)); got["message"] != "at my desk" {
		t.Errorf("message = %v, want the online connection's text", got["message"])
	}
	// With no login session on the call, every connection.
	thread.SetLocal("session", "")
	if value, _ := api_presence_set(thread, fn, sl.Tuple{sl.String("busy")}, nil); value.String() != "2" {
		t.Errorf("set from no session changed %v, want 2", value)
	}
}

// TestWebsocketPresence: a web socket opening makes the user present under the
// login session the browser presented, and closing it ends the presence.
func TestWebsocketPresence(t *testing.T) {
	alice := create_presence_test_env(t)
	websocket_open(alice, "chat", "w1", nil, "app1", "s1")
	t.Cleanup(func() {
		websockets_lock.Lock()
		delete(websockets, alice.UID)
		websockets_lock.Unlock()
	})
	if !websocket_registered(alice, "chat", "w1") {
		t.Fatal("the connection is not registered")
	}
	status, _, connections := presence_user(alice.UID)
	if status != "online" || len(connections) != 1 || connections[0].ID != "w1" || connections[0].Session != "s1" || connections[0].Kind != "web" {
		t.Fatalf("presence after open = %s %+v", status, connections)
	}
	websocket_terminate(nil, alice, "chat", "w1")
	if status, seen, connections := presence_user(alice.UID); status != "offline" || len(connections) != 0 || seen < now()-1 {
		t.Errorf("presence after close = %s %d %v", status, seen, connections)
	}
}
