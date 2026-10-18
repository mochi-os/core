// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix signals
//
// The ephemeral events of the Matrix engine (the federation plan, phase 24):
// who is typing, how far someone has read, and presence. What another server
// sends of its own users reaches the apps of the accounts here that share a
// room with them, as the conversation events of phase 23; what a hosted
// account does goes to every other server in its rooms, unless the user's
// switches keep it back. Presence is told when the user's changes, and told
// again with the account's own traffic once twenty minutes have passed, since
// other servers forget a presence they have not heard of in half an hour;
// nothing is sent for an account that does nothing.

package main

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/matrix-org/gomatrixserverlib/spec"
)

// signals_typing_held is how long the engine shows the user typing without a
// fresh word from the app, before telling the room they stopped.
var signals_typing_held = 10 * time.Second

// signals_presence_renewed is how long a presence told stands before the
// account's own traffic tells it again.
const signals_presence_renewed = 20 * 60

// signals_tables makes the engine's presence table: whether each hosted
// account shows the user's presence, what it last told, and when.
func signals_tables(db *DB) {
	db.exec("create table if not exists presence (account text not null primary key, hidden integer not null default 0, shown text not null default '', told integer not null default 0)")
}

// signals_receive takes an ephemeral event another server sent, about its own
// users only, and queues it for the accounts here it concerns.
func signals_receive(origin spec.ServerName, raw json.RawMessage, out *rooms_outbox) {
	var edu struct {
		Type    string          `json:"edu_type"`
		Content json.RawMessage `json:"content"`
	}
	if json.Unmarshal(raw, &edu) != nil {
		return
	}
	theirs := func(user string) bool {
		id, err := spec.NewUserID(user, true)
		return err == nil && id.Domain() == origin
	}
	switch edu.Type {
	case "m.typing":
		var typing struct {
			Room   string `json:"room_id"`
			User   string `json:"user_id"`
			Typing bool   `json:"typing"`
		}
		if json.Unmarshal(edu.Content, &typing) != nil || !theirs(typing.User) || rooms_membership(typing.Room, typing.User) != spec.Join {
			return
		}
		signals_tell(typing.Room, typing.User, out, map[string]any{"kind": "typing", "conversation": typing.Room, "from": typing.User, "typing": typing.Typing})
	case "m.receipt":
		var receipts map[string]map[string]map[string]struct {
			Events []string `json:"event_ids"`
		}
		if json.Unmarshal(edu.Content, &receipts) != nil {
			return
		}
		for room, kinds := range receipts {
			for user, receipt := range kinds["m.read"] {
				if !theirs(user) || rooms_membership(room, user) != spec.Join || len(receipt.Events) == 0 {
					continue
				}
				signals_tell(room, user, out, map[string]any{"kind": "receipt", "conversation": room, "from": user, "id": receipt.Events[len(receipt.Events)-1], "displayed": true})
			}
		}
	case "m.presence":
		var presence struct {
			Push []struct {
				User    string `json:"user_id"`
				State   string `json:"presence"`
				Message string `json:"status_msg"`
			} `json:"push"`
		}
		if json.Unmarshal(edu.Content, &presence) != nil {
			return
		}
		for _, p := range presence.Push {
			if !theirs(p.User) {
				continue
			}
			status := "offline"
			switch p.State {
			case "online":
				status = "online"
			case "unavailable":
				status = "away"
			}
			event := map[string]any{"kind": "presence", "from": p.User, "status": status, "message": p.Message}
			for _, room := range signals_rooms(p.User) {
				signals_tell(room, p.User, out, event)
			}
		}
	}
}

// signals_rooms are the rooms someone is joined to that this server keeps:
// of those holding a membership event of theirs, the ones where the current
// state has them joined.
func signals_rooms(user string) []string {
	rows, _ := federation_database().rows("select distinct room as id from events where type='m.room.member' and key=?", user)
	var out []string
	for _, row := range rows {
		if room := row_string(row, "id"); rooms_membership(room, user) == spec.Join {
			out = append(out, room)
		}
	}
	return out
}

// signals_tell queues an event for every account here joined to a room,
// other than the one it is about, each told once however many rooms they
// share.
func signals_tell(room string, about string, out *rooms_outbox, event map[string]any) {
	state, _ := rooms_current(room)
	members := rooms_members(state)
	var local []string
	for member, membership := range members {
		if membership == spec.Join && member != about {
			local = append(local, member)
		}
	}
	sort.Strings(local)
	for _, member := range local {
		user, account := federation_user(member)
		if user == nil {
			continue
		}
		if event["kind"] == "presence" && signals_queued(out.events[account], event) {
			continue
		}
		out.add(user, account, event)
	}
}

// signals_queued says whether the same presence is already queued.
func signals_queued(events []map[string]any, event map[string]any) bool {
	for _, e := range events {
		if e["kind"] == event["kind"] && e["from"] == event["from"] && e["status"] == event["status"] && e["message"] == event["message"] {
			return true
		}
	}
	return false
}

// === What the account does ===

var signals_timers = map[string]*time.Timer{}
var signals_timers_lock sync.Mutex

// signals_typing tells a room a hosted account is typing, or stopped: the
// other servers in it and the accounts here. Typing stands for a while
// without a fresh word, and then the room is told it stopped.
func signals_typing(h *matrix_hosted, room string, typing bool) {
	key := h.address + "\x00" + room
	signals_timers_lock.Lock()
	if timer := signals_timers[key]; timer != nil {
		timer.Stop()
		delete(signals_timers, key)
	}
	if typing {
		signals_timers[key] = time.AfterFunc(signals_typing_held, func() {
			signals_timers_lock.Lock()
			delete(signals_timers, key)
			signals_timers_lock.Unlock()
			signals_typing_send(h, room, false)
		})
	}
	signals_timers_lock.Unlock()
	signals_typing_send(h, room, typing)
}

func signals_typing_send(h *matrix_hosted, room string, typing bool) {
	if rooms_membership(room, h.address) != spec.Join {
		return
	}
	user, _ := spec.NewUserID(h.address, true)
	state, _ := rooms_current(room)
	federation_signal(rooms_servers(state), string(user.Domain()), "m.typing", map[string]any{"room_id": room, "user_id": h.address, "typing": typing})
	var out rooms_outbox
	signals_tell(room, h.address, &out, map[string]any{"kind": "typing", "conversation": room, "from": h.address, "typing": typing})
	out.flush()
}

// signals_read tells a room how far a hosted account has read it, where the
// user shares that: a read receipt for the event named.
func signals_read(h *matrix_hosted, room string, id string) {
	if rooms_membership(room, h.address) != spec.Join || rooms_event(id) == nil {
		return
	}
	user, _ := spec.NewUserID(h.address, true)
	state, _ := rooms_current(room)
	content := map[string]any{room: map[string]any{"m.read": map[string]any{h.address: map[string]any{"event_ids": []string{id}, "data": map[string]any{"ts": time.Now().UnixMilli()}}}}}
	federation_signal(rooms_servers(state), string(user.Domain()), "m.receipt", content)
	var out rooms_outbox
	signals_tell(room, h.address, &out, map[string]any{"kind": "receipt", "conversation": room, "from": h.address, "id": id, "displayed": true})
	out.flush()
}

// signals_share records whether a hosted account shows the user's presence,
// and tells its rooms what it now shows.
func signals_share(h *matrix_hosted, shared bool) {
	hidden := 0
	if !shared {
		hidden = 1
	}
	federation_database().exec("insert into presence (account, hidden) values (?, ?) on conflict(account) do update set hidden=excluded.hidden", h.id, hidden)
	signals_presence(h, signals_changed)
}

// When signals_presence tells: when what is shown changes, with the account's
// own traffic once what was told is twenty minutes old too, or always, on
// joining a room whose servers have not heard it.
const (
	signals_changed = iota
	signals_renewed
	signals_always
)

// signals_presence tells every server sharing a room with a hosted account
// the presence it shows: the user's status in Mochi and its text, or offline
// where they do not share it.
func signals_presence(h *matrix_hosted, when int) {
	db := federation_database()
	row, _ := db.row("select hidden, shown, told from presence where account=?", h.id)
	status, message := "offline", ""
	if row_int(row, "hidden") == 0 {
		var connections []presence_connection
		status, _, connections = presence_user(h.user.UID)
		for _, c := range connections {
			if c.Status == status && c.Message != "" {
				message = c.Message
			}
		}
	}
	shown := matrix_presence(status) + "\n" + message
	stale := now()-row_int(row, "told") >= signals_presence_renewed
	if row != nil && row_string(row, "shown") == shown && (when == signals_changed || (when == signals_renewed && !stale)) {
		return
	}
	db.exec("insert into presence (account, shown, told) values (?, ?, ?) on conflict(account) do update set shown=excluded.shown, told=excluded.told", h.id, shown, now())
	presence := map[string]any{"user_id": h.address, "presence": matrix_presence(status), "currently_active": status == "online", "last_active_ago": 0}
	if message != "" && status != "offline" {
		presence["status_msg"] = message
	}
	user, _ := spec.NewUserID(h.address, true)
	rooms := signals_rooms(h.address)
	var states []rooms_state
	for _, room := range rooms {
		state, _ := rooms_current(room)
		states = append(states, state)
	}
	federation_signal(rooms_servers(states...), string(user.Domain()), "m.presence", map[string]any{"push": []any{presence}})
	event := map[string]any{"kind": "presence", "from": h.address, "status": status, "message": message}
	var out rooms_outbox
	for _, room := range rooms {
		signals_tell(room, h.address, &out, event)
	}
	out.flush()
}

// signals_presence_notify tells the rooms of a user's hosted Matrix accounts
// that their presence may have changed.
func signals_presence_notify(uid string) {
	user := user_by_uid(uid)
	if user == nil {
		return
	}
	rows, _ := networks_database().rows("select id from actors where user=? and network='matrix' and deleted=0", uid)
	for _, row := range rows {
		if h := matrix_hosted_of(user, account_hosted_get(user, row_string(row, "id"))); h != nil {
			signals_presence(h, signals_changed)
		}
	}
}
