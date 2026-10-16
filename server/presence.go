// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Presence
//
// Which of a user's sessions are connected right now, each with its status,
// and when the user was last seen: the record chat reads for conversations
// over P2P and the chat engines read for XMPP and Matrix clients. Core
// records - a web socket opening and closing, an engine's client session
// beginning and ending, a status a client sets - and chat decides what is
// shared, under the user's switches. Connections live in memory, since none
// survives a restart; the time last seen is kept in sessions.db beside the
// sessions' own access times.

package main

import (
	"sort"
	"sync"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// The statuses a connected session may have. A user with no connection is
// offline.
var presence_statuses = []string{"online", "away", "busy"}

// A presence_connection is one connected session of a user: a web socket, or a
// client session of a chat engine.
type presence_connection struct {
	ID      string `json:"id"`      // the connection, unique for the user
	Session string `json:"session"` // the login session it belongs to, where one is known
	Kind    string `json:"kind"`    // web, xmpp, matrix
	Device  string `json:"device"`  // the device it is from, where one is known (devices.go)
	Status  string `json:"status"`
	Message string `json:"message"` // the status text the user set
	Since   int64  `json:"since"`
	Seen    int64  `json:"seen"`
}

var presence_connections = map[string]map[string]*presence_connection{}
var presence_lock sync.Mutex

var api_presence = sls.FromStringDict(sl.String("mochi.presence"), sl.StringDict{
	"get": sl.NewBuiltin("mochi.presence.get", api_presence_get),
	"set": sl.NewBuiltin("mochi.presence.set", api_presence_set),
})

// presence_status_valid reports whether a status is one a connection may have.
func presence_status_valid(status string) bool {
	for _, s := range presence_statuses {
		if s == status {
			return true
		}
	}
	return false
}

// presence_connect records a session connecting, online.
func presence_connect(uid string, connection string, session string, kind string, device string) {
	presence_lock.Lock()
	defer presence_lock.Unlock()
	if presence_connections[uid] == nil {
		presence_connections[uid] = map[string]*presence_connection{}
	}
	presence_connections[uid][connection] = &presence_connection{ID: connection, Session: session, Kind: kind, Device: device, Status: "online", Since: now(), Seen: now()}
}

// presence_disconnect records a session ending, and the user seen at this
// moment.
func presence_disconnect(uid string, connection string) {
	presence_lock.Lock()
	connections := presence_connections[uid]
	if _, found := connections[connection]; !found {
		presence_lock.Unlock()
		return
	}
	delete(connections, connection)
	if len(connections) == 0 {
		delete(presence_connections, uid)
	}
	presence_lock.Unlock()
	presence_seen_record(uid)
}

// presence_seen_record keeps the moment a user was last seen.
func presence_seen_record(uid string) {
	db_open("db/sessions.db").exec("insert into seen (user, time) values (?, ?) on conflict(user) do update set time=excluded.time", uid, now())
}

// presence_touch records activity on a connection.
func presence_touch(uid string, connection string) {
	presence_lock.Lock()
	defer presence_lock.Unlock()
	if c := presence_connections[uid][connection]; c != nil {
		c.Seen = now()
	}
}

// presence_set gives connections a status and a status text: one connection
// when named, else every connection of a login session, else every connection
// of the user. Answers how many changed.
func presence_set(uid string, session string, connection string, status string, message string) int {
	if !presence_status_valid(status) {
		return 0
	}
	presence_lock.Lock()
	defer presence_lock.Unlock()
	changed := 0
	for id, c := range presence_connections[uid] {
		if (connection != "" && id != connection) || (connection == "" && session != "" && c.Session != session) {
			continue
		}
		c.Status, c.Message, c.Seen = status, message, now()
		changed++
	}
	return changed
}

// presence_user answers a user's presence: the status of their most present
// connection (online over busy over away, offline with none), when they were
// last seen (now while connected; else the later of the recorded moment and
// their sessions' last access), and the connections, oldest first.
func presence_user(uid string) (string, int64, []presence_connection) {
	presence_lock.Lock()
	var connections []presence_connection
	for _, c := range presence_connections[uid] {
		connections = append(connections, *c)
	}
	presence_lock.Unlock()
	sort.Slice(connections, func(i, j int) bool {
		if connections[i].Since != connections[j].Since {
			return connections[i].Since < connections[j].Since
		}
		return connections[i].ID < connections[j].ID
	})
	if len(connections) > 0 {
		status := "away"
		for _, c := range connections {
			switch {
			case c.Status == "online":
				status = "online"
			case c.Status == "busy" && status != "online":
				status = "busy"
			}
		}
		return status, now(), connections
	}
	return "offline", presence_seen(uid), []presence_connection{}
}

// presence_seen is when a user not connected was last seen: the moment a
// connection ended, or their sessions' last access, whichever is later.
func presence_seen(uid string) int64 {
	db := db_open("db/sessions.db")
	seen := int64(0)
	if row, _ := db.row("select time from seen where user=?", uid); row != nil {
		seen, _ = row["time"].(int64)
	}
	if row, _ := db.row("select max(accessed) as accessed from sessions where user=?", uid); row != nil {
		if accessed, _ := row["accessed"].(int64); accessed > seen {
			seen = accessed
		}
	}
	return seen
}

// presence_encode writes a user's presence for Starlark: the status, its
// text and when they were last seen, and with sessions their connections.
func presence_encode(uid string, sessions bool) sl.Value {
	status, seen, connections := presence_user(uid)
	message := ""
	for _, c := range connections {
		if c.Status == status && c.Message != "" {
			message = c.Message
		}
	}
	out := map[string]any{"status": status, "message": message, "seen": seen}
	if sessions {
		list := make([]any, 0, len(connections))
		for _, c := range connections {
			list = append(list, map[string]any{"id": c.ID, "session": c.Session, "kind": c.Kind, "device": c.Device, "status": c.Status, "message": c.Message, "since": c.Since, "seen": c.Seen})
		}
		out["connections"] = list
	}
	return sl_encode(out)
}

// mochi.presence.get() -> dict: The user's own presence: status (online, busy, away, or offline with no connection), message (the status text of the connection that gives the status, or "") and seen (now while connected, else when they were last seen). With user/sessions/read also connections, each with id, session, kind (web, xmpp, matrix), device, status, message, since and seen. Requires presence/read
func api_presence_get(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := require_permission(t, fn, "presence/read"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	return presence_encode(user.UID, require_permission(t, fn, "user/sessions/read") == nil), nil
}

// mochi.presence.set(status, message="") -> int: Set the status of the user's connections from this login session - online, away or busy - with a status text, and answer how many connections changed. From a call with no login session, every connection of the user. Requires user/sessions/write
func api_presence_set(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var status, message string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "status", &status, "message?", &message); err != nil {
		return nil, err
	}
	if err := require_permission(t, fn, "user/sessions/write"); err != nil {
		return sl_error(fn, "%v", err)
	}
	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}
	if !presence_status_valid(status) {
		return sl_error(fn, "status must be one of %v", presence_statuses)
	}
	if len(message) > 200 {
		return sl_error(fn, "the status text is over 200 characters")
	}
	session, _ := t.Local("session").(string)
	return sl.MakeInt(presence_set(user.UID, session, "", status, message)), nil
}
