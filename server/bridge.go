// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix bridges
//
// A Mochi group chat that takes people from Matrix (the federation plan,
// phase 24, decision 39). The group stays a Mochi chat among its Mochi
// members, and the server of the member who added the outside people relays
// between it and a Matrix room it makes for them. Each Mochi member appears
// in the room as a stand-in the relaying server keeps, named by a Matrix id
// made from their fingerprint on the server's own domain (else the domain of
// the relaying user's hosted Matrix account), under their plain Mochi name
// and avatar. Mochi members need no account on Matrix and no domain of their
// own. The room is not end-to-end encrypted: the relaying server reads each
// message to pass it on.
//
// The app relays: it tells the engine what the Mochi members do through the
// functions below, and the engine hands it what the outside people do through
// bridge/receive, as the conversation events of phase 23. An outside person
// who invites a stand-in to a conversation of their own reaches the member it
// stands for the same way, as an invitation the app passes on.

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// bridge_tables makes the stand-ins and the rooms relayed.
func bridge_tables(db *DB) {
	db.exec("create table if not exists standins (id text not null primary key, domain text not null, entity text not null, name text not null, avatar text not null default '', user text not null, created integer not null)")
	db.exec("create unique index if not exists standins_entity on standins(domain, entity)")
	db.exec("create table if not exists bridges (room text not null primary key, user text not null, standin text not null, chat text not null default '', created integer not null)")
}

// A bridge is a room relayed for a group: the user whose server relays it, and
// the stand-in of theirs that made or joined it.
type bridge struct {
	room    string
	user    string
	standin string
	chat    string
}

// bridge_of is the bridge a room is, or nil.
func bridge_of(room string) *bridge {
	row, _ := federation_database().row("select room, user, standin, chat from bridges where room=?", room)
	if row == nil {
		return nil
	}
	return &bridge{room: row_string(row, "room"), user: row_string(row, "user"), standin: row_string(row, "standin"), chat: row_string(row, "chat")}
}

// bridge_standin_of is a stand-in by its Matrix id: the member it stands for,
// the name and avatar it goes by, and the user whose server keeps it.
func bridge_standin_of(id string) map[string]any {
	row, _ := federation_database().row("select id, domain, entity, name, avatar, user from standins where id=?", id)
	return row
}

// bridge_domain is the domain a user's server keeps stand-ins on: its own,
// else that of the user's first hosted Matrix account, or "".
func bridge_domain(user *User) string {
	if domain := server_domain(); domain != "" {
		return domain
	}
	row, _ := networks_database().row("select domain from actors where user=? and network='matrix' and deleted=0 order by created limit 1", user.UID)
	return row_string(row, "domain")
}

// bridge_standin is a member's stand-in on a domain, made on first use with
// their avatar, and renamed where their name changed.
func bridge_standin(user *User, domain string, entity string, name string) string {
	db := federation_database()
	if row, _ := db.row("select id, name from standins where domain=? and entity=?", domain, entity); row != nil {
		if name != "" && name != row_string(row, "name") {
			db.exec("update standins set name=? where id=?", name, row_string(row, "id"))
		}
		return row_string(row, "id")
	}
	id := "@mochi-" + hex.EncodeToString([]byte(fingerprint(entity))) + ":" + domain
	db.exec("insert or ignore into standins (id, domain, entity, name, user, created) values (?, ?, ?, ?, ?, ?)", id, domain, entity, name, user.UID, now())
	if data, kind := bridge_avatar(user, entity); len(data) > 0 {
		if stored, err := content_store(user, domain, data, kind, "avatar"); err == nil {
			db.exec("update standins set avatar=? where id=?", stored["url"], id)
		}
	}
	return id
}

// bridge_avatar is a member's avatar, asked of their people service.
var bridge_avatar = func(user *User, entity string) ([]byte, string) {
	if user.Identity == nil {
		return nil, ""
	}
	from := user.Identity.ID
	peer, err := remote_connect(from, entity, "")
	if err != nil {
		return nil, ""
	}
	s, err := stream_to_peer(peer, from, entity, "people", "avatar", "", nil)
	if err != nil {
		return nil, ""
	}
	defer s.close()
	if s.write(map[string]any{}) != nil {
		return nil, ""
	}
	header, err := s.read_content()
	if err != nil || fmt.Sprint(header["status"]) != "200" {
		return nil, ""
	}
	kind, _ := header["content_type"].(string)
	if inline, ok := header["data"].([]byte); ok {
		return inline, kind
	}
	data, err := read_limited(s.raw_reader(), 2<<20)
	if err != nil {
		return nil, ""
	}
	return data, kind
}

// bridge_inbound hands what the outside people of a relayed room did to the
// relaying user's apps declaring bridge/receive. A variable so tests can
// watch it.
var bridge_inbound = func(user *User, room string, events []map[string]any) {
	if user == nil || len(events) == 0 || user_pending(user) {
		return
	}
	list := make([]any, 0, len(events))
	for _, event := range events {
		list = append(list, event)
	}
	for _, a := range apps_for_function(user, "bridge/receive") {
		if _, err := activitypub_call(user, a, "bridge/receive", Map{"room": room, "events": list}); err != nil {
			info("Matrix relay %s: %s did not take %d events: %v", room, a.id, len(events), err)
		}
	}
}

// === Functions ===

var api_bridge = sls.FromStringDict(sl.String("mochi.bridge"), sl.StringDict{
	"available": sl.NewBuiltin("mochi.bridge.available", api_bridge_available),
	"close":     sl.NewBuiltin("mochi.bridge.close", api_bridge_close),
	"create":    sl.NewBuiltin("mochi.bridge.create", api_bridge_create),
	"delete":    sl.NewBuiltin("mochi.bridge.delete", api_bridge_delete),
	"invite":    sl.NewBuiltin("mochi.bridge.invite", api_bridge_invite),
	"join":      sl.NewBuiltin("mochi.bridge.join", api_bridge_join),
	"leave":     sl.NewBuiltin("mochi.bridge.leave", api_bridge_leave),
	"media":     sl.NewBuiltin("mochi.bridge.media", api_bridge_media),
	"member":    sl.NewBuiltin("mochi.bridge.member", api_bridge_member),
	"remove":    sl.NewBuiltin("mochi.bridge.remove", api_bridge_remove),
	"rename":    sl.NewBuiltin("mochi.bridge.rename", api_bridge_rename),
	"send":      sl.NewBuiltin("mochi.bridge.send", api_bridge_send),
	"update":    sl.NewBuiltin("mochi.bridge.update", api_bridge_update),
	"upload":    sl.NewBuiltin("mochi.bridge.upload", api_bridge_upload),
})

// bridge_caller is the user an app relays for, with the relay permission and
// Matrix on.
func bridge_caller(t *sl.Thread, fn *sl.Builtin) (*User, error) {
	if err := require_permission(t, fn, "bridge/write"); err != nil {
		return nil, err
	}
	user := principal_caller(t)
	if user == nil || user.Identity == nil {
		return nil, fmt.Errorf("no user")
	}
	if !network_enabled("matrix") {
		return nil, fmt.Errorf("matrix is off")
	}
	return user, nil
}

// bridge_owned is a relayed room of the caller's, or an error.
func bridge_owned(t *sl.Thread, fn *sl.Builtin, room string) (*User, *bridge, error) {
	user, err := bridge_caller(t, fn)
	if err != nil {
		return nil, nil, err
	}
	r := bridge_of(room)
	if r == nil || r.user != user.UID {
		return nil, nil, fmt.Errorf("no such relay")
	}
	return user, r, nil
}

// bridge_failed is a bridge's failure as the app sees it: None, with the reason
// in the log, so the app's action carries on.
func bridge_failed(fn *sl.Builtin, room string, err error) (sl.Value, error) {
	info("Matrix relay %s: %s: %v", room, fn.Name(), err)
	return sl.None, nil
}

// bridge_members reads a list of {entity, name} for the Mochi members.
func bridge_members(value sl.Value) ([][2]string, error) {
	var out [][2]string
	list, ok := sl_decode(value).([]any)
	if value == nil || value == sl.None {
		return out, nil
	}
	if !ok {
		return nil, fmt.Errorf("members must be a list")
	}
	for _, item := range list {
		member, _ := item.(map[string]any)
		entity, _ := member["entity"].(string)
		name, _ := member["name"].(string)
		if !valid(entity, "entity") {
			return nil, fmt.Errorf("invalid member")
		}
		out = append(out, [2]string{entity, name})
	}
	return out, nil
}

// bridge_seat puts a member's stand-in in a relayed room: invited by the
// relaying member's stand-in, then joined, under their name.
func bridge_seat(ctx context.Context, user *User, r *bridge, domain string, entity string, name string) (string, error) {
	standin := bridge_standin(user, domain, entity, name)
	switch rooms_membership(r.room, standin) {
	case spec.Join:
		// Renamed since: the join again carries the name.
		if row := bridge_standin_of(standin); row != nil {
			state, _ := rooms_current(r.room)
			if id := state[rooms_key(spec.MRoomMember, standin)]; id != "" {
				if event := rooms_event(id); event != nil && !strings.Contains(string(event.Content()), fmt.Sprintf("%q", row_string(row, "name"))) {
					_, err := rooms_send(ctx, r.room, standin, spec.MRoomMember, &standin, rooms_member_content(standin, spec.Join))
					return standin, err
				}
			}
		}
		return standin, nil
	case spec.Ban:
		return "", fmt.Errorf("banned from the room")
	case spec.Invite:
	default:
		version := rooms_version(r.room)
		if version == nil {
			return "", fmt.Errorf("no such room")
		}
		var out rooms_outbox
		unlock := rooms_lock(r.room)
		err := rooms_invite_locked(ctx, version, r.room, r.standin, standin, false, &out)
		unlock()
		out.flush()
		if err != nil {
			return "", err
		}
	}
	_, err := rooms_send(ctx, r.room, standin, spec.MRoomMember, &standin, rooms_member_content(standin, spec.Join))
	return standin, err
}

// mochi.bridge.available() -> bool: Whether the user's server can relay a group's members to Matrix: Matrix is on, and it has a domain to keep their stand-ins on. Requires the bridge/write permission
func api_bridge_available(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if err := sl.UnpackArgs(fn.Name(), args, kwargs); err != nil {
		return nil, err
	}
	user, err := bridge_caller(t, fn)
	if err != nil {
		return sl.False, nil
	}
	return sl.Bool(bridge_domain(user) != ""), nil
}

// mochi.bridge.create(chat, name, people, members=[]) -> dict | None: Make a Matrix room for a Mochi group chat that takes people from Matrix, relayed by the user's server: named name, the user's stand-in its creator, each member of members ({entity, name}) seated as a stand-in, and each of people (Matrix addresses) invited. Returns {room, people}, each person as {id, name}; None where Matrix is unreachable or an address has no one at it. Requires the bridge/write permission
func api_bridge_create(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var chat, name string
	var people *sl.List
	var members sl.Value = sl.None
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "chat", &chat, "name", &name, "people", &people, "members?", &members); err != nil {
		return nil, err
	}
	user, err := bridge_caller(t, fn)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	seated, err := bridge_members(members)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	domain := bridge_domain(user)
	if domain == "" {
		return bridge_failed(fn, chat, fmt.Errorf("this server has no Matrix domain to relay on"))
	}
	ctx := thread_context(t)
	creator := bridge_standin(user, domain, user.Identity.ID, user.Identity.Name)
	var addresses []string
	var found []any
	for i := 0; i < people.Len(); i++ {
		address, _ := sl.AsString(people.Index(i))
		address = matrix_person(strings.TrimSpace(address))
		if !resolve_matrix_re.MatchString(address) {
			return bridge_failed(fn, chat, fmt.Errorf("invalid address"))
		}
		person, err := rooms_profile(ctx, spec.ServerName(domain), address)
		if err != nil {
			return bridge_failed(fn, chat, err)
		}
		if !strings.Contains(strings.Join(addresses, "\n"), address) {
			addresses = append(addresses, address)
			found = append(found, map[string]any{"id": address, "name": person})
		}
	}
	room, err := rooms_create(ctx, creator, addresses, name, false)
	if err != nil {
		return bridge_failed(fn, chat, err)
	}
	federation_database().exec("insert into bridges (room, user, standin, chat, created) values (?, ?, ?, ?, ?) on conflict(room) do update set user=excluded.user, standin=excluded.standin, chat=excluded.chat", room, user.UID, creator, chat, now())
	r := bridge_of(room)
	for _, member := range seated {
		if _, err := bridge_seat(ctx, user, r, domain, member[0], member[1]); err != nil {
			info("Matrix relay %s: member %s not seated: %v", room, member[0], err)
		}
	}
	return sl_encode(map[string]any{"room": room, "people": found}), nil
}

// mochi.bridge.invite(room, person) -> dict | None: Invite a person on Matrix by their address to a relayed room, and return {id, name}; None where no one is at the address. Requires the bridge/write permission
func api_bridge_invite(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, person string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "person", &person); err != nil {
		return nil, err
	}
	_, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	ctx := thread_context(t)
	address := matrix_person(strings.TrimSpace(person))
	if !resolve_matrix_re.MatchString(address) {
		return bridge_failed(fn, room, fmt.Errorf("invalid address"))
	}
	mine, _ := spec.NewUserID(r.standin, true)
	name, err := rooms_profile(ctx, mine.Domain(), address)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	version := rooms_version(room)
	if version == nil {
		return bridge_failed(fn, room, fmt.Errorf("no such room"))
	}
	if membership := rooms_membership(room, address); membership == spec.Join || membership == spec.Invite {
		return sl_encode(map[string]any{"id": address, "name": name}), nil
	}
	var out rooms_outbox
	unlock := rooms_lock(room)
	err = rooms_invite_locked(ctx, version, room, r.standin, address, false, &out)
	unlock()
	out.flush()
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl_encode(map[string]any{"id": address, "name": name}), nil
}

// mochi.bridge.member(room, entity, name) -> string | None: Seat a Mochi member in a relayed room as their stand-in, under the name given, or rename them there; returns the stand-in's Matrix id. Requires the bridge/write permission
func api_bridge_member(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, entity, name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "entity", &entity, "name", &name); err != nil {
		return nil, err
	}
	user, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if !valid(entity, "entity") {
		return sl_error(fn, "invalid entity")
	}
	mine, _ := spec.NewUserID(r.standin, true)
	standin, err := bridge_seat(thread_context(t), user, r, string(mine.Domain()), entity, name)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl.String(standin), nil
}

// bridge_speaker is a member's stand-in on a relayed room's domain. Whether
// it is in the room is the room's to say, as for any of its users.
func bridge_speaker(r *bridge, entity string) (string, error) {
	mine, _ := spec.NewUserID(r.standin, true)
	row, _ := federation_database().row("select id from standins where domain=? and entity=?", string(mine.Domain()), entity)
	if row == nil {
		return "", fmt.Errorf("the member has no stand-in")
	}
	return row_string(row, "id"), nil
}

// mochi.bridge.leave(room, entity) -> None: Take a Mochi member's stand-in out of a relayed room. Requires the bridge/write permission
func api_bridge_leave(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, entity string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "entity", &entity); err != nil {
		return nil, err
	}
	_, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	standin, err := bridge_speaker(r, entity)
	if err != nil {
		return sl.None, nil
	}
	if _, err := rooms_send(thread_context(t), room, standin, spec.MRoomMember, &standin, map[string]any{"membership": spec.Leave}); err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl.None, nil
}

// mochi.bridge.remove(room, person) -> None: Put a person from Matrix out of a relayed room. Requires the bridge/write permission
func api_bridge_remove(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, person string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "person", &person); err != nil {
		return nil, err
	}
	_, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	if membership := rooms_membership(room, person); membership != spec.Join && membership != spec.Invite {
		return sl.None, nil
	}
	if _, err := rooms_send(thread_context(t), room, r.standin, spec.MRoomMember, &person, map[string]any{"membership": spec.Leave}); err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl.None, nil
}

// mochi.bridge.rename(room, name) -> None: Rename a relayed room. Requires the bridge/write permission
func api_bridge_rename(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "name", &name); err != nil {
		return nil, err
	}
	_, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	empty := ""
	if _, err := rooms_send(thread_context(t), room, r.standin, "m.room.name", &empty, map[string]any{"name": name}); err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl.None, nil
}

// mochi.bridge.send(room, entity, object, key="") -> dict | None: Send a Mochi member's message to a relayed room as their stand-in, the object as mochi.account.object.create takes it for Matrix (content, file, reply, thread, or a reaction), and return {id}; key sends it once. Requires the bridge/write permission
func api_bridge_send(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, entity, key string
	var value sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "entity", &entity, "object", &value, "key?", &key); err != nil {
		return nil, err
	}
	_, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	standin, err := bridge_speaker(r, entity)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	object := sl_decode_map(value)
	content, err := matrix_content(object)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	if key != "" {
		key = "bridge|" + key
	}
	id, err := rooms_message(thread_context(t), room, standin, room, matrix_type(object), content, key)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl_encode(map[string]any{"id": id}), nil
}

// mochi.bridge.update(room, entity, id, object) -> dict | None: Send a new version of a message a Mochi member's stand-in sent to a relayed room: {content}. Requires the bridge/write permission
func api_bridge_update(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, entity, id string
	var value sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "entity", &entity, "id", &id, "object", &value); err != nil {
		return nil, err
	}
	_, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	standin, err := bridge_speaker(r, entity)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	object := sl_decode_map(value)
	object["conversation"] = room
	result, err := rooms_object_update(thread_context(t), &matrix_hosted{id: room, address: standin}, id, object)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl_encode(result), nil
}

// mochi.bridge.delete(room, entity, id) -> bool | None: Withdraw a message or reaction a Mochi member's stand-in sent to a relayed room. Requires the bridge/write permission
func api_bridge_delete(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, entity, id string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "entity", &entity, "id", &id); err != nil {
		return nil, err
	}
	_, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	standin, err := bridge_speaker(r, entity)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	if err := rooms_object_delete(thread_context(t), &matrix_hosted{id: room, address: standin}, id, room); err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl.True, nil
}

// mochi.bridge.upload(room, data, type, name="") -> dict | None: Keep a file for a relayed room's messages and return {url, name, type, size}, the url its mxc address, for mochi.bridge.send's file. Requires the bridge/write permission
func api_bridge_upload(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, kind, name string
	var data sl.Value
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "data", &data, "type", &kind, "name?", &name); err != nil {
		return nil, err
	}
	user, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	bytes, ok := data.(sl.Bytes)
	if !ok || len(bytes) == 0 || len(bytes) > media_maximum {
		return sl_error(fn, "data must be between 1 and %d bytes", media_maximum)
	}
	if name == "" {
		name = "file"
	}
	mine, _ := spec.NewUserID(r.standin, true)
	result, err := content_store(user, string(mine.Domain()), []byte(bytes), kind, name)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	return sl_encode(result), nil
}

// mochi.bridge.media(room, url) -> dict | None: Fetch a file a person from Matrix sent to a relayed room, by the address its message named, and return {status, type, data}, kept in core's evictable cache. Requires the bridge/write permission
func api_bridge_media(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, address string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "url", &address); err != nil {
		return nil, err
	}
	user, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	status, kind, data, err := content_fetch(thread_context(t), &matrix_hosted{user: user, id: room, address: r.standin}, address)
	if err != nil {
		return bridge_failed(fn, room, err)
	}
	result := sl.NewDict(3)
	result.SetKey(sl.String("status"), sl.MakeInt(status))
	result.SetKey(sl.String("type"), sl.String(kind))
	result.SetKey(sl.String("data"), sl.Bytes(data))
	return result, nil
}

// mochi.bridge.join(room, entity) -> dict | None: Accept, as a Mochi member's stand-in, an invitation a person from Matrix sent it to a conversation of their own, relayed by the user's server from then on: the stand-in joins, and the room's recent past comes through bridge/receive. Returns {room}. Requires the bridge/write permission
func api_bridge_join(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room, entity, chat string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room, "entity", &entity, "chat?", &chat); err != nil {
		return nil, err
	}
	user, err := bridge_caller(t, fn)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	row, _ := federation_database().row("select s.id from standins s join invites i on i.user=s.id where i.room=? and s.entity=? and s.user=?", room, entity, user.UID)
	if row == nil {
		return bridge_failed(fn, room, fmt.Errorf("no such invitation"))
	}
	standin := row_string(row, "id")
	federation_database().exec("insert into bridges (room, user, standin, chat, created) values (?, ?, ?, ?, ?) on conflict(room) do update set user=excluded.user, standin=excluded.standin, chat=excluded.chat", room, user.UID, standin, chat, now())
	if _, err := rooms_join(thread_context(t), standin, room); err != nil {
		federation_database().exec("delete from bridges where room=?", room)
		return bridge_failed(fn, room, err)
	}
	return sl_encode(map[string]any{"room": room}), nil
}

// mochi.bridge.close(room) -> None: Stop relaying a room: every stand-in leaves it. Requires the bridge/write permission
func api_bridge_close(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var room string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "room", &room); err != nil {
		return nil, err
	}
	_, r, err := bridge_owned(t, fn, room)
	if err != nil {
		return sl_error(fn, "%v", err)
	}
	ctx := thread_context(t)
	state, _ := rooms_current(room)
	var standins []string
	for member, membership := range rooms_members(state) {
		if membership == spec.Join && bridge_standin_of(member) != nil && member != r.standin {
			standins = append(standins, member)
		}
	}
	// The stand-in that made the room leaves last, holding it until then.
	for _, standin := range append(standins, r.standin) {
		if rooms_membership(room, standin) == spec.Join {
			if _, err := rooms_send(ctx, room, standin, spec.MRoomMember, &standin, map[string]any{"membership": spec.Leave}); err != nil {
				info("Matrix relay %s: %s not left: %v", room, standin, err)
			}
		}
	}
	federation_database().exec("delete from bridges where room=?", room)
	unlock := rooms_lock(room)
	rooms_forget(room)
	unlock()
	return sl.None, nil
}

// bridge_events turns what a bridge's own stand-in sees of an event into
// bridge/receive events, or none for what a stand-in did.
func bridge_events(r *bridge, event gomatrixserverlib.PDU, after rooms_state) []map[string]any {
	if bridge_standin_of(string(event.SenderID())) != nil {
		return nil
	}
	return rooms_conversation(r.standin, event, after)
}
