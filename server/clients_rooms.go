// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix clients' rooms
//
// What a Matrix client does in rooms (the federation plan, phase 28): makes,
// joins, knocks on, leaves and upgrades them, changes their state, sends and
// redacts, reads their history, marks what it has read, names them in the
// directory and walks the spaces they make. Each act is the account's, built
// by the room engine (rooms.go) as chat's are, and what a client sends is
// told to the account's own chat as the account's message.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// clients_history_maximum bounds how many events one page of a room's
// history holds.
const clients_history_maximum = 100

// clients_unbounded is the ceiling of a window with no end, one short of the
// largest position so the position after it is one too.
const clients_unbounded = math.MaxInt64 - 1

// === Events as clients see them ===

// clients_event is an event as a client sees it: with its room where the
// answer is not already the room's, its age, the state it replaced, what
// redacted it, and the transaction that sent it where the device asking is
// the one that sent it.
func clients_event(event gomatrixserverlib.PDU, viewer string, device string, room bool) map[string]any {
	var content any
	json.Unmarshal(event.Content(), &content)
	if content == nil {
		content = map[string]any{}
	}
	sender := string(event.SenderID())
	out := map[string]any{
		"type":             event.Type(),
		"content":          content,
		"sender":           sender,
		"origin_server_ts": int64(event.OriginServerTS()),
		"event_id":         event.EventID(),
	}
	if room {
		out["room_id"] = event.RoomID().String()
	}
	unsigned := map[string]any{"age": max(int64(0), time.Now().UnixMilli()-int64(event.OriginServerTS()))}
	if key := event.StateKey(); key != nil {
		out["state_key"] = *key
		if previous := clients_previous(event); previous != nil {
			var before any
			json.Unmarshal(previous.Content(), &before)
			unsigned["prev_content"] = before
			unsigned["replaces_state"] = previous.EventID()
			unsigned["prev_sender"] = string(previous.SenderID())
		}
	}
	if redacts := event.Redacts(); redacts != "" {
		out["redacts"] = redacts
	}
	if event.Redacted() {
		if because := clients_redaction(event); because != nil {
			unsigned["redacted_because"] = clients_event(because, viewer, "", room)
		}
	}
	if sender == viewer && device != "" {
		if transaction := clients_transaction(event.EventID(), device); transaction != "" {
			unsigned["transaction_id"] = transaction
		}
	}
	out["unsigned"] = unsigned
	return out
}

// clients_events is a list of events as clients see them.
func clients_events(events []gomatrixserverlib.PDU, viewer string, device string, room bool) []map[string]any {
	out := make([]map[string]any, 0, len(events))
	for _, event := range events {
		out = append(out, clients_event(event, viewer, device, room))
	}
	return out
}

// clients_previous is the state event a state event replaced, or nil.
func clients_previous(event gomatrixserverlib.PDU) gomatrixserverlib.PDU {
	row, _ := federation_database().row("select before from events where id=?", event.EventID())
	if row == nil || row_int(row, "before") <= 0 {
		return nil
	}
	id := rooms_snapshot(row_int(row, "before"))[rooms_key(event.Type(), *event.StateKey())]
	if id == "" || id == event.EventID() {
		return nil
	}
	return rooms_event(id)
}

// clients_redaction is the event that redacted one, or nil.
func clients_redaction(event gomatrixserverlib.PDU) gomatrixserverlib.PDU {
	rows, _ := federation_database().rows("select id from events where room=? and type=? and body like ? limit 5", event.RoomID().String(), spec.MRoomRedaction, "%"+event.EventID()+"%")
	for _, row := range rows {
		if redaction := rooms_event(row_string(row, "id")); redaction != nil && clients_redacts(redaction) == event.EventID() {
			return redaction
		}
	}
	return nil
}

// clients_redacts is the event a redaction names, in its content from room
// version 11 and beside it before.
func clients_redacts(redaction gomatrixserverlib.PDU) string {
	if target := redaction.Redacts(); target != "" {
		return target
	}
	var content struct {
		Redacts string `json:"redacts"`
	}
	json.Unmarshal(redaction.Content(), &content)
	return content.Redacts
}

// clients_transaction is the transaction a device sent an event by, or "".
func clients_transaction(event string, device string) string {
	row, _ := federation_database().row("select key from sent where event=? and key like ?", event, "client|"+device+"|%")
	return strings.TrimPrefix(row_string(row, "key"), "client|"+device+"|")
}

// === Who sees what ===

// clients_memberships is every room a user has a membership in, as the room
// engine holds it, and the invitations other servers sent.
func clients_memberships(user string) map[string]string {
	out := map[string]string{}
	rows, _ := federation_database().rows("select distinct room from events where type='m.room.member' and key=? and outlier=0", user)
	for _, row := range rows {
		room := row_string(row, "room")
		if membership := rooms_membership(room, user); membership != "" {
			out[room] = membership
		}
	}
	invites, _ := federation_database().rows("select room from invites where user=?", user)
	for _, row := range invites {
		if room := row_string(row, "room"); out[room] == "" || out[room] == spec.Leave {
			out[room] = spec.Invite
		}
	}
	// A knock through another server waits there; an invitation declined
	// or a knock withdrawn there leaves the room left.
	pending, _ := federation_database().rows("select room, kind from pending where user=? and kind in ('knock', 'leave')", user)
	for _, row := range pending {
		room := row_string(row, "room")
		switch row_string(row, "kind") {
		case "knock":
			if out[room] == "" || out[room] == spec.Leave {
				out[room] = "knock"
			}
		case "leave":
			if out[room] == "" {
				out[room] = spec.Leave
			}
		}
	}
	return out
}

// clients_pending records where a user stands with a room this server is
// not in - invited to it, knocking on it, or having declined or withdrawn -
// at a new position, so their syncs are told.
func clients_pending(room string, user string, kind string, server string, state string) {
	federation_database().exec("insert or replace into pending (room, user, kind, server, state, created) values (?, ?, ?, ?, ?, ?)", room, user, kind, server, state, now())
	clients_wake()
}

// clients_leaving is a leave of a room as a user's syncs give it once this
// server keeps nothing of the room: the event, shown as at the leaving.
func clients_leaving(event gomatrixserverlib.PDU, user string) string {
	return json_encode([]any{clients_event(event, user, "", false)})
}

// clients_window is the part of a room's timeline a user may see, by
// position: from the start where its history is shared, else from their
// first invitation or join, as its visibility says; to their leaving where
// they have left.
func clients_window(room string, user string) (int64, int64) {
	db := federation_database()
	rows, _ := db.rows("select s.position, e.membership from stream s join events e on e.id=s.event where s.room=? and e.type='m.room.member' and e.key=? order by s.position", room, user)
	floor, ceiling := int64(0), int64(clients_unbounded)
	visibility := clients_state_content(room, "m.room.history_visibility", "")["history_visibility"]
	invited, joined := int64(0), int64(0)
	for _, row := range rows {
		switch row_string(row, "membership") {
		case spec.Invite:
			if invited == 0 {
				invited = row_int(row, "position")
			}
		case spec.Join:
			if joined == 0 {
				joined = row_int(row, "position")
			}
		}
	}
	switch visibility {
	case "joined":
		floor = joined
	case "invited":
		floor = invited
		if floor == 0 || (joined > 0 && joined < floor) {
			floor = joined
		}
	}
	switch rooms_membership(room, user) {
	case spec.Leave, spec.Ban:
		if len(rows) > 0 {
			ceiling = row_int(rows[len(rows)-1], "position")
		}
	case spec.Join, spec.Invite:
	default:
		ceiling = 0
	}
	return floor, ceiling
}

// clients_state_content is a state event's content in a room's current
// state, or an empty map.
func clients_state_content(room string, kind string, key string) map[string]any {
	state, _ := rooms_current(room)
	content := map[string]any{}
	if id := state[rooms_key(kind, key)]; id != "" {
		if event := rooms_event(id); event != nil {
			json.Unmarshal(event.Content(), &content)
		}
	}
	return content
}

// A clients_entry is one event of a room's timeline at its position.
type clients_entry struct {
	position int64
	event    gomatrixserverlib.PDU
}

// clients_timeline is a room's timeline between two positions, both
// excluded, newest first when backwards, up to a limit.
func clients_timeline(room string, after int64, before int64, limit int, backwards bool) []clients_entry {
	order := "asc"
	if backwards {
		order = "desc"
	}
	rows, _ := federation_database().rows("select position, event from stream where room=? and position>? and position<? order by position "+order+" limit ?", room, after, before, limit)
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row_string(row, "event"))
	}
	loaded := rooms_events(ids)
	out := make([]clients_entry, 0, len(rows))
	for _, row := range rows {
		if event := loaded[row_string(row, "event")]; event != nil {
			out = append(out, clients_entry{row_int(row, "position"), event})
		}
	}
	return out
}

// clients_position is an event's position in the timeline, or 0.
func clients_position(event string) int64 {
	row, _ := federation_database().row("select position from stream where event=?", event)
	return row_int(row, "position")
}

// clients_visible is an event the user may see, by its id, or nil.
func clients_visible(room string, user string, id string) gomatrixserverlib.PDU {
	position := clients_position(id)
	floor, ceiling := clients_window(room, user)
	if position == 0 || position < floor || position > ceiling {
		return nil
	}
	event := rooms_event(id)
	if event == nil || event.RoomID().String() != room {
		return nil
	}
	return event
}

// clients_member checks a user is in a room, or was and may still read its
// past, and writes the refusal.
func clients_member(c *gin.Context, s *clients_session, room string) bool {
	switch rooms_membership(room, s.h.address) {
	case spec.Join, spec.Leave, spec.Ban:
		return true
	}
	clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "You are not in the room")
	return false
}

// === Routing ===

// clients_rooms_dispatch answers a client's room requests, reporting whether
// the request was one.
func clients_rooms_dispatch(c *gin.Context, s *clients_session, parts []string) bool {
	method := c.Request.Method
	route := method + " " + strings.Join(parts, "/")
	switch route {
	case "POST createRoom":
		clients_create(c, s)
		return true
	case "GET joined_rooms":
		joined := []string{}
		for room, membership := range clients_memberships(s.h.address) {
			if membership == spec.Join {
				joined = append(joined, room)
			}
		}
		sort.Strings(joined)
		c.JSON(http.StatusOK, map[string]any{"joined_rooms": joined})
		return true
	case "GET publicRooms", "POST publicRooms":
		clients_public(c, s)
		return true
	case "POST search":
		clients_search(c, s)
		return true
	}
	if taken, ok := clients_match(parts, "join", "*"); ok && method == "POST" {
		clients_join(c, s, taken[0])
		return true
	}
	if taken, ok := clients_match(parts, "knock", "*"); ok && method == "POST" {
		clients_knock(c, s, taken[0])
		return true
	}
	if taken, ok := clients_match(parts, "directory", "room", "*"); ok {
		clients_alias(c, s, taken[0])
		return true
	}
	if taken, ok := clients_match(parts, "directory", "list", "room", "*"); ok {
		clients_listing(c, s, taken[0])
		return true
	}
	if len(parts) < 3 || parts[0] != "rooms" {
		return false
	}
	room := parts[1]
	rest := parts[2:]
	switch {
	case method == "POST" && len(rest) == 1 && rest[0] == "join":
		clients_join(c, s, room)
	case method == "POST" && len(rest) == 1 && (rest[0] == "invite" || rest[0] == "kick" || rest[0] == "ban" || rest[0] == "unban"):
		clients_membership(c, s, room, rest[0])
	case method == "POST" && len(rest) == 1 && rest[0] == "leave":
		if err := rooms_leave(c.Request.Context(), s.h.address, room); err != nil {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
			return true
		}
		c.JSON(http.StatusOK, map[string]any{})
	case method == "POST" && len(rest) == 1 && rest[0] == "forget":
		switch rooms_membership(room, s.h.address) {
		case spec.Join, spec.Invite:
			clients_error(c, http.StatusBadRequest, "M_UNKNOWN", "Leave the room first")
			return true
		}
		clients_setting_set(s.h.address, room, "org.mochi.forgotten", json.RawMessage(`{"forgotten":true}`))
		c.JSON(http.StatusOK, map[string]any{})
	case method == "GET" && len(rest) == 1 && (rest[0] == "members" || rest[0] == "joined_members"):
		clients_members(c, s, room, rest[0] == "joined_members")
	case method == "GET" && len(rest) == 1 && rest[0] == "state":
		if !clients_member(c, s, room) {
			return true
		}
		state, _ := rooms_current(room)
		c.JSON(http.StatusOK, clients_events(rooms_list(state), s.h.address, s.device, true))
	case len(rest) >= 2 && rest[0] == "state":
		key := ""
		if len(rest) >= 3 {
			key = strings.Join(rest[2:], "/")
		}
		clients_state(c, s, room, rest[1], key)
	case method == "PUT" && len(rest) == 3 && rest[0] == "send":
		clients_send(c, s, room, rest[1], rest[2])
	case method == "PUT" && len(rest) == 3 && rest[0] == "redact":
		clients_redact(c, s, room, rest[1], rest[2])
	case method == "GET" && len(rest) == 2 && rest[0] == "event":
		if !clients_member(c, s, room) {
			return true
		}
		event := clients_visible(room, s.h.address, rest[1])
		if event == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such event")
			return true
		}
		c.JSON(http.StatusOK, clients_event(event, s.h.address, s.device, true))
	case method == "GET" && len(rest) == 2 && rest[0] == "context":
		clients_context(c, s, room, rest[1])
	case method == "GET" && len(rest) == 1 && rest[0] == "messages":
		clients_messages(c, s, room)
	case method == "GET" && len(rest) >= 2 && rest[0] == "relations":
		clients_relations(c, s, room, rest[1:])
	case method == "GET" && len(rest) == 1 && rest[0] == "threads":
		clients_threads(c, s, room)
	case method == "GET" && len(rest) == 1 && rest[0] == "timestamp_to_event":
		clients_timestamp(c, s, room)
	case method == "PUT" && len(rest) == 2 && rest[0] == "typing":
		clients_typing(c, s, room, rest[1])
	case method == "POST" && len(rest) == 3 && rest[0] == "receipt":
		clients_receipt(c, s, room, rest[1], rest[2])
	case method == "POST" && len(rest) == 1 && rest[0] == "read_markers":
		clients_markers(c, s, room)
	case method == "GET" && len(rest) == 1 && rest[0] == "aliases":
		if !clients_member(c, s, room) {
			return true
		}
		rows, _ := federation_database().rows("select alias from aliases where room=? order by alias", room)
		aliases := []string{}
		for _, row := range rows {
			aliases = append(aliases, row_string(row, "alias"))
		}
		c.JSON(http.StatusOK, map[string]any{"aliases": aliases})
	case method == "POST" && len(rest) == 1 && rest[0] == "upgrade":
		clients_upgrade(c, s, room)
	case method == "GET" && len(rest) == 1 && rest[0] == "hierarchy":
		clients_hierarchy(c, s, room)
	case method == "POST" && len(rest) >= 1 && rest[0] == "report":
		c.JSON(http.StatusOK, map[string]any{})
	default:
		clients_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognised request")
	}
	return true
}

// === Making rooms ===

// A clients_creation is what a client asks of a new room.
type clients_creation struct {
	Visibility string         `json:"visibility"`
	Alias      string         `json:"room_alias_name"`
	Name       string         `json:"name"`
	Topic      string         `json:"topic"`
	Invite     []string       `json:"invite"`
	Version    string         `json:"room_version"`
	Creation   map[string]any `json:"creation_content"`
	Initial    []struct {
		Type    string         `json:"type"`
		Key     string         `json:"state_key"`
		Content map[string]any `json:"content"`
	} `json:"initial_state"`
	Preset   string         `json:"preset"`
	Direct   bool           `json:"is_direct"`
	Override map[string]any `json:"power_level_content_override"`
}

// clients_create makes a room as a client asks: its version, its kind (a
// space, say), its preset's rules, an alias, its state, a name, a topic and
// the people invited to it.
func clients_create(c *gin.Context, s *clients_session) {
	var body clients_creation
	if !clients_body(c, &body) {
		return
	}
	room, err := clients_make(c.Request.Context(), s.h.address, body, "")
	if err != nil {
		if refusal, ok := err.(*clients_refusal); ok {
			clients_error(c, refusal.status, refusal.code, refusal.message)
			return
		}
		clients_error(c, http.StatusBadRequest, "M_UNKNOWN", err.Error())
		return
	}
	c.JSON(http.StatusOK, map[string]any{"room_id": room})
}

// A clients_refusal is a refusal of what a client asked, with its status and
// Matrix error code.
type clients_refusal struct {
	status  int
	code    string
	message string
}

func (r *clients_refusal) Error() string { return r.message }

// clients_make makes a room: the create event, the creator joining, the
// power levels, the alias, the preset's rules, the state asked, the name and
// topic, and the invitations. A replaced room names its predecessor.
func clients_make(ctx context.Context, creator string, body clients_creation, predecessor string) (string, error) {
	name := body.Version
	if name == "" {
		name = string(rooms_version_default)
	}
	version, err := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersion(name))
	if err != nil {
		return "", &clients_refusal{http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", "That room version is not supported"}
	}
	user, err := spec.NewUserID(creator, true)
	if err != nil {
		return "", err
	}
	if len(body.Name) > 255 || len(body.Topic) > 4096 || len(body.Invite) > 100 || len(body.Initial) > 50 {
		return "", &clients_refusal{http.StatusBadRequest, "M_INVALID_PARAM", "The room asked for is too large"}
	}
	alias := ""
	if body.Alias != "" {
		alias = "#" + body.Alias + ":" + string(user.Domain())
		if strings.ContainsAny(body.Alias, ":# ") || len(alias) > 255 {
			return "", &clients_refusal{http.StatusBadRequest, "M_INVALID_PARAM", "Invalid alias"}
		}
		if taken, _ := federation_database().exists("select 1 from aliases where alias=?", alias); taken {
			return "", &clients_refusal{http.StatusBadRequest, "M_ROOM_IN_USE", "That alias is taken"}
		}
	}
	for _, person := range body.Invite {
		if _, err := spec.NewUserID(person, true); err != nil {
			return "", &clients_refusal{http.StatusBadRequest, "M_INVALID_PARAM", "Invalid user id " + person}
		}
	}
	preset := body.Preset
	if preset == "" {
		preset = "private_chat"
		if body.Visibility == "public" {
			preset = "public_chat"
		}
	}
	room := ""
	if !version.DomainlessRoomIDs() {
		room = "!" + strings.ReplaceAll(uid(), "-", "") + ":" + string(user.Domain())
	}
	create := map[string]any{}
	for key, value := range body.Creation {
		create[key] = value
	}
	create["room_version"] = string(version.Version())
	if version.CreatorInCreateEvent() {
		create["creator"] = creator
	}
	if predecessor != "" {
		create["predecessor"] = map[string]any{"room_id": predecessor}
	}
	var out rooms_outbox
	defer out.flush()
	if room != "" {
		federation_database().exec("insert into rooms (id, version, state, created) values (?, ?, 0, ?)", room, string(version.Version()), now())
	}
	empty := ""
	event, err := rooms_build(version, room, creator, spec.MRoomCreate, &empty, create)
	if err != nil {
		return "", err
	}
	if room == "" {
		room = "!" + strings.TrimPrefix(event.EventID(), "$")
		federation_database().exec("insert into rooms (id, version, state, created) values (?, ?, 0, ?)", room, string(version.Version()), now())
	}
	unlock := rooms_lock(room)
	defer unlock()
	if err := rooms_commit(version, event, &out); err != nil {
		return "", err
	}
	type step struct {
		kind    string
		key     string
		content any
	}
	users := map[string]any{}
	if !version.PrivilegedCreators() {
		users[creator] = 100
	}
	if preset == "trusted_private_chat" {
		for _, person := range body.Invite {
			users[person] = 100
		}
	}
	powers := map[string]any{
		"users": users, "users_default": 0, "events_default": 0, "state_default": 50,
		"ban": 50, "kick": 50, "redact": 50, "invite": 50,
		"events":        map[string]any{"m.room.name": 50, "m.room.power_levels": 100, "m.room.history_visibility": 100, "m.room.canonical_alias": 50, "m.room.avatar": 50, "m.room.encryption": 100, "m.room.tombstone": 100, "m.room.server_acl": 100},
		"notifications": map[string]any{"room": 50},
	}
	// Anyone in a private room may invite, as Synapse's presets have it; in
	// a public one, those with power only.
	if preset == "private_chat" || preset == "trusted_private_chat" {
		powers["invite"] = 0
	}
	for key, value := range body.Override {
		powers[key] = value
	}
	steps := []step{
		{spec.MRoomMember, creator, rooms_member_content(creator, spec.Join)},
		{spec.MRoomPowerLevels, "", powers},
	}
	if alias != "" {
		steps = append(steps, step{"m.room.canonical_alias", "", map[string]any{"alias": alias}})
	}
	join, guest := spec.Invite, "can_join"
	if preset == "public_chat" {
		join, guest = spec.Public, "forbidden"
	}
	steps = append(steps,
		step{spec.MRoomJoinRules, "", map[string]any{"join_rule": join}},
		step{spec.MRoomHistoryVisibility, "", map[string]any{"history_visibility": "shared"}},
		step{"m.room.guest_access", "", map[string]any{"guest_access": guest}},
	)
	for _, initial := range body.Initial {
		if initial.Type == spec.MRoomCreate || initial.Type == spec.MRoomMember || initial.Type == spec.MRoomPowerLevels {
			continue
		}
		content := initial.Content
		if content == nil {
			content = map[string]any{}
		}
		steps = append(steps, step{initial.Type, initial.Key, content})
	}
	if body.Name != "" {
		steps = append(steps, step{"m.room.name", "", map[string]any{"name": body.Name}})
	}
	if body.Topic != "" {
		steps = append(steps, step{"m.room.topic", "", map[string]any{"topic": body.Topic}})
	}
	for _, st := range steps {
		key := st.key
		event, err := rooms_build(version, room, creator, st.kind, &key, st.content)
		if err != nil {
			return room, err
		}
		if err := rooms_commit(version, event, &out); err != nil {
			return room, err
		}
	}
	if alias != "" {
		federation_database().exec("insert or ignore into aliases (alias, room, creator, created) values (?, ?, ?, ?)", alias, room, creator, now())
	}
	if body.Visibility == "public" {
		federation_database().exec("insert or ignore into published (room, created) values (?, ?)", room, now())
	}
	if body.Direct && len(body.Invite) > 0 {
		rooms_direct_add(creator, room, body.Invite[0])
		clients_direct_add(creator, body.Invite[0], room)
	}
	for _, person := range body.Invite {
		if err := rooms_invite_locked(ctx, version, room, creator, person, body.Direct, &out); err != nil {
			return room, &clients_refusal{http.StatusForbidden, "M_FORBIDDEN", err.Error()}
		}
	}
	return room, nil
}

// clients_direct_add records a one-to-one room in the account's m.direct
// data, as clients keep it, unless it is there.
func clients_direct_add(user string, person string, room string) {
	direct := map[string][]string{}
	if content := clients_setting(user, "", "m.direct"); content != nil {
		json.Unmarshal(content, &direct)
	}
	if slices.Contains(direct[person], room) {
		return
	}
	direct[person] = append(direct[person], room)
	clients_setting_set(user, "", "m.direct", json.RawMessage(json_encode(direct)))
}

// === Joining, knocking and membership ===

// clients_join joins a room by its id or an alias, through the servers the
// client names where the room is elsewhere.
func clients_join(c *gin.Context, s *clients_session, address string) {
	var via []spec.ServerName
	for _, server := range append(c.QueryArray("server_name"), c.QueryArray("via")...) {
		via = append(via, spec.ServerName(server))
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	room, err := rooms_join(ctx, s.h.address, address, via...)
	if err != nil {
		clients_refuse(c, err)
		return
	}
	c.JSON(http.StatusOK, map[string]any{"room_id": room})
}

// clients_knock asks to join a room whose rule is to knock: by the room's own
// event where someone here is in it, else through a server in it.
func clients_knock(c *gin.Context, s *clients_session, address string) {
	var body struct {
		Reason string `json:"reason"`
	}
	if !clients_body(c, &body) {
		return
	}
	room := address
	var servers []spec.ServerName
	for _, server := range append(c.QueryArray("server_name"), c.QueryArray("via")...) {
		servers = append(servers, spec.ServerName(server))
	}
	if strings.HasPrefix(address, "#") {
		row, _ := federation_database().row("select room from aliases where alias=?", address)
		if row == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such room")
			return
		}
		room = row_string(row, "room")
	}
	if i := strings.IndexByte(room, ':'); i > 0 {
		servers = append(servers, spec.ServerName(room[i+1:]))
	}
	content := rooms_member_content(s.h.address, "knock")
	if body.Reason != "" {
		content["reason"] = body.Reason
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	if rooms_version(room) != nil && rooms_local(room) != "" {
		key := s.h.address
		if _, err := rooms_send(ctx, room, s.h.address, spec.MRoomMember, &key, content); err != nil {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
			return
		}
		c.JSON(http.StatusOK, map[string]any{"room_id": room})
		return
	}
	if err := clients_knock_remote(ctx, s, room, servers, content); err != nil {
		clients_refuse(c, err)
		return
	}
	c.JSON(http.StatusOK, map[string]any{"room_id": room})
}

// clients_knock_remote knocks on a room elsewhere: the knock is made by a
// server in it, signed here and sent back, and the room's stripped state
// kept as the account's invitations are, until someone lets it in.
func clients_knock_remote(ctx context.Context, s *clients_session, room string, servers []spec.ServerName, content map[string]any) error {
	knocking, err := spec.NewUserID(s.h.address, true)
	if err != nil {
		return err
	}
	id := federation_identity_of(knocking.Domain())
	if id == nil {
		return fmt.Errorf("not a Matrix domain here")
	}
	var failure error = fmt.Errorf("no server to knock through")
	for _, server := range servers {
		if federation_local(server) {
			continue
		}
		// Every version this server speaks, as a join asks: the room's
		// server refuses a knock from a server that does not speak its own.
		made, err := federation_client().MakeKnock(ctx, knocking.Domain(), server, room, s.h.address, slices.Sorted(maps.Keys(gomatrixserverlib.RoomVersions())))
		if err != nil {
			failure = err
			continue
		}
		version, err := gomatrixserverlib.GetRoomVersion(made.RoomVersion)
		if err != nil {
			failure = err
			continue
		}
		proto := made.KnockEvent
		proto.SetContent(content)
		builder := version.NewEventBuilderFromProtoEvent(&proto)
		event, err := builder.Build(time.Now(), id.domain, id.key, id.private)
		if err != nil {
			failure = err
			continue
		}
		answer, err := federation_client().SendKnock(ctx, knocking.Domain(), server, event)
		if err != nil {
			failure = err
			continue
		}
		stripped := answer.KnockRoomState
		stripped = append(stripped, gomatrixserverlib.NewInviteStrippedState(event))
		clients_pending(room, s.h.address, "knock", string(server), json_encode(stripped))
		return nil
	}
	return failure
}

// clients_membership invites, removes, bans or lets back in someone, as the
// account.
func clients_membership(c *gin.Context, s *clients_session, room string, action string) {
	var body struct {
		User   string `json:"user_id"`
		Reason string `json:"reason"`
	}
	if !clients_body(c, &body) {
		return
	}
	if _, err := spec.NewUserID(body.User, true); err != nil {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Invalid user id")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	if action == "invite" {
		version := rooms_version(room)
		if version == nil {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "You are not in the room")
			return
		}
		var out rooms_outbox
		unlock := rooms_lock(room)
		err := rooms_invite_locked(ctx, version, room, s.h.address, body.User, false, &out)
		unlock()
		out.flush()
		if err != nil {
			clients_refuse(c, err)
			return
		}
		c.JSON(http.StatusOK, map[string]any{})
		return
	}
	membership := map[string]string{"kick": spec.Leave, "ban": spec.Ban, "unban": spec.Leave}[action]
	if action == "unban" && rooms_membership(room, body.User) != spec.Ban {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "They are not banned")
		return
	}
	content := map[string]any{"membership": membership}
	if body.Reason != "" {
		content["reason"] = body.Reason
	}
	key := body.User
	if _, err := rooms_send(ctx, room, s.h.address, spec.MRoomMember, &key, content); err != nil {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
		return
	}
	c.JSON(http.StatusOK, map[string]any{})
}

// clients_members answers a room's members: their membership events, or
// those joined with their names and pictures.
func clients_members(c *gin.Context, s *clients_session, room string, joined bool) {
	if !clients_member(c, s, room) {
		return
	}
	state, _ := rooms_current(room)
	var ids []string
	for key, id := range state {
		if strings.HasPrefix(key, spec.MRoomMember+"\x00") {
			ids = append(ids, id)
		}
	}
	loaded := rooms_events(ids)
	if joined {
		out := map[string]any{}
		for _, event := range loaded {
			var content struct {
				Membership string `json:"membership"`
				Name       string `json:"displayname"`
				Avatar     string `json:"avatar_url"`
			}
			json.Unmarshal(event.Content(), &content)
			if content.Membership == spec.Join && event.StateKey() != nil {
				member := map[string]any{}
				if content.Name != "" {
					member["display_name"] = content.Name
				}
				if content.Avatar != "" {
					member["avatar_url"] = content.Avatar
				}
				out[*event.StateKey()] = member
			}
		}
		c.JSON(http.StatusOK, map[string]any{"joined": out})
		return
	}
	only, not := c.Query("membership"), c.Query("not_membership")
	chunk := []map[string]any{}
	for _, id := range ids {
		event := loaded[id]
		if event == nil {
			continue
		}
		membership, _ := event.Membership()
		if (only != "" && membership != only) || (not != "" && membership == not) {
			continue
		}
		chunk = append(chunk, clients_event(event, s.h.address, s.device, true))
	}
	c.JSON(http.StatusOK, map[string]any{"chunk": chunk})
}

// === State ===

// clients_state answers a state event's content, or sets it.
func clients_state(c *gin.Context, s *clients_session, room string, kind string, key string) {
	switch c.Request.Method {
	case "GET":
		if !clients_member(c, s, room) {
			return
		}
		state, _ := rooms_current(room)
		id := state[rooms_key(kind, key)]
		event := rooms_event(id)
		if id == "" || event == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such state")
			return
		}
		c.Data(http.StatusOK, "application/json", event.Content())
	case "PUT":
		var content map[string]any
		if !clients_body(c, &content) {
			return
		}
		if kind == spec.MRoomCreate {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "A room's creation cannot be changed")
			return
		}
		if kind == "m.room.canonical_alias" && !clients_aliases_held(room, content) {
			clients_error(c, http.StatusBadRequest, "M_BAD_ALIAS", "The alias does not name the room")
			return
		}
		event, err := rooms_send(c.Request.Context(), room, s.h.address, kind, &key, content)
		if err != nil {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
			return
		}
		clients_told(s, event)
		c.JSON(http.StatusOK, map[string]any{"event_id": event.EventID()})
	default:
		clients_error(c, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "Unrecognised request")
	}
}

// clients_aliases_held checks a canonical alias names the room: each alias
// on this server's domains must be one of the room's.
func clients_aliases_held(room string, content map[string]any) bool {
	var aliases []string
	if alias, _ := content["alias"].(string); alias != "" {
		aliases = append(aliases, alias)
	}
	if others, ok := content["alt_aliases"].([]any); ok {
		for _, other := range others {
			if alias, _ := other.(string); alias != "" {
				aliases = append(aliases, alias)
			}
		}
	}
	for _, alias := range aliases {
		_, domain, err := gomatrixserverlib.SplitID('#', alias)
		if err != nil {
			return false
		}
		if federation_local(domain) {
			row, _ := federation_database().row("select room from aliases where alias=?", alias)
			if row_string(row, "room") != room {
				return false
			}
		}
	}
	return true
}

// === Sending ===

// clients_send sends an event to a room as the account, once for each of
// the device's transactions.
func clients_send(c *gin.Context, s *clients_session, room string, kind string, transaction string) {
	var content map[string]any
	if !clients_body(c, &content) {
		return
	}
	key := "client|" + s.device + "|" + transaction
	if row, _ := federation_database().row("select event from sent where account=? and key=?", s.h.id, key); row != nil {
		c.JSON(http.StatusOK, map[string]any{"event_id": row_string(row, "event")})
		return
	}
	if rooms_membership(room, s.h.address) != spec.Join {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "You are not in the room")
		return
	}
	event, err := rooms_send(c.Request.Context(), room, s.h.address, kind, nil, content)
	if err != nil {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
		return
	}
	federation_database().exec("insert or ignore into sent (account, key, event, created) values (?, ?, ?, ?)", s.h.id, key, event.EventID(), now())
	clients_told(s, event)
	c.JSON(http.StatusOK, map[string]any{"event_id": event.EventID()})
}

// clients_redact redacts an event of a room as the account, once for each of
// the device's transactions.
func clients_redact(c *gin.Context, s *clients_session, room string, target string, transaction string) {
	var body struct {
		Reason string `json:"reason"`
	}
	if !clients_body(c, &body) {
		return
	}
	key := "client|" + s.device + "|" + transaction
	if row, _ := federation_database().row("select event from sent where account=? and key=?", s.h.id, key); row != nil {
		c.JSON(http.StatusOK, map[string]any{"event_id": row_string(row, "event")})
		return
	}
	content := map[string]any{"redacts": target}
	if body.Reason != "" {
		content["reason"] = body.Reason
	}
	event, err := rooms_send(c.Request.Context(), room, s.h.address, spec.MRoomRedaction, nil, content)
	if err != nil {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
		return
	}
	federation_database().exec("insert or ignore into sent (account, key, event, created) values (?, ?, ?, ?)", s.h.id, key, event.EventID(), now())
	clients_told(s, event)
	c.JSON(http.StatusOK, map[string]any{"event_id": event.EventID()})
}

// clients_told tells the account's own chat what one of its clients did in
// a room, as the account's: the room engine tells every account in a room
// but the one that sent.
func clients_told(s *clients_session, event gomatrixserverlib.PDU) {
	after, ok := rooms_after(event.EventID())
	if !ok {
		return
	}
	var out rooms_outbox
	out.add(s.user, s.h.id, rooms_conversation(s.h.address, event, after)...)
	out.flush()
}

// === History ===

// clients_token reads a position a client gives back: one of /messages'
// own, "p" and the position, or a sync position.
func clients_token(token string) int64 {
	if position, ok := strings.CutPrefix(token, "p"); ok {
		n, _ := strconv.ParseInt(position, 10, 64)
		return n
	}
	return sync_parse(token).stream
}

// clients_messages answers a page of a room's history from a position, back
// or forward, within what the user may see, with the state the page's
// senders need.
func clients_messages(c *gin.Context, s *clients_session, room string) {
	if !clients_member(c, s, room) {
		return
	}
	floor, ceiling := clients_window(room, s.h.address)
	limit, _ := strconv.Atoi(c.Query("limit"))
	if limit <= 0 {
		limit = 10
	}
	limit = min(limit, clients_history_maximum)
	backwards := c.DefaultQuery("dir", "b") == "b"
	from := c.Query("from")
	var position int64
	switch {
	case from != "":
		position = clients_token(from)
	case backwards:
		position = clients_unbounded + 1
	default:
		position = 0
	}
	stop := int64(-1)
	if to := c.Query("to"); to != "" {
		stop = clients_token(to)
	}
	var entries []clients_entry
	if backwards {
		after := floor - 1
		if stop >= 0 {
			after = max(after, stop-1)
		}
		entries = clients_timeline(room, after, min(position, ceiling+1), limit, true)
	} else {
		before := ceiling + 1
		if stop >= 0 {
			before = min(before, stop+1)
		}
		entries = clients_timeline(room, max(position, floor-1), before, limit, false)
	}
	filter := sync_filter_read(s, c.Query("filter"))
	chunk := []map[string]any{}
	var events []gomatrixserverlib.PDU
	for _, entry := range entries {
		if filter.Room.Timeline.allows(entry.event) {
			events = append(events, entry.event)
			chunk = append(chunk, clients_event(entry.event, s.h.address, s.device, true))
		}
	}
	answer := map[string]any{"chunk": chunk, "start": "p" + strconv.FormatInt(position, 10)}
	if from == "" && backwards {
		answer["start"] = "p" + strconv.FormatInt(sync_maximum(), 10)
	}
	if len(entries) == limit {
		answer["end"] = "p" + strconv.FormatInt(entries[len(entries)-1].position, 10)
	}
	if filter.Room.State.Lazy {
		answer["state"] = clients_events(sync_lazy(room, events, s.h.address), s.h.address, s.device, true)
	}
	c.JSON(http.StatusOK, answer)
}

// clients_context answers an event with those around it and the state at it.
func clients_context(c *gin.Context, s *clients_session, room string, id string) {
	if !clients_member(c, s, room) {
		return
	}
	event := clients_visible(room, s.h.address, id)
	if event == nil {
		clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such event")
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	limit = min(max(limit, 0), clients_history_maximum)
	position := clients_position(id)
	floor, ceiling := clients_window(room, s.h.address)
	before := clients_timeline(room, floor-1, position, limit/2+limit%2, true)
	after := clients_timeline(room, position, ceiling+1, limit/2, false)
	answer := map[string]any{"event": clients_event(event, s.h.address, s.device, true)}
	var around []gomatrixserverlib.PDU
	ahead := []map[string]any{}
	for _, entry := range before {
		ahead = append(ahead, clients_event(entry.event, s.h.address, s.device, true))
		around = append(around, entry.event)
	}
	behind := []map[string]any{}
	for _, entry := range after {
		behind = append(behind, clients_event(entry.event, s.h.address, s.device, true))
		around = append(around, entry.event)
	}
	answer["events_before"], answer["events_after"] = ahead, behind
	answer["start"] = "p" + strconv.FormatInt(position, 10)
	answer["end"] = "p" + strconv.FormatInt(position, 10)
	if len(before) > 0 {
		answer["start"] = "p" + strconv.FormatInt(before[len(before)-1].position, 10)
	}
	if len(after) > 0 {
		answer["end"] = "p" + strconv.FormatInt(after[len(after)-1].position, 10)
	}
	if state, ok := rooms_after(id); ok {
		answer["state"] = clients_events(rooms_list(state), s.h.address, s.device, true)
	}
	c.JSON(http.StatusOK, answer)
}

// clients_relations answers the events that relate to one - replies in a
// thread, edits, reactions - of a relation and type where named, newest
// first.
func clients_relations(c *gin.Context, s *clients_session, room string, rest []string) {
	if !clients_member(c, s, room) {
		return
	}
	target := rest[0]
	relation, kind := "", ""
	if len(rest) > 1 {
		relation = rest[1]
	}
	if len(rest) > 2 {
		kind = rest[2]
	}
	floor, ceiling := clients_window(room, s.h.address)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	limit = min(max(limit, 1), clients_history_maximum)
	position := int64(clients_unbounded + 1)
	if from := c.Query("from"); from != "" {
		position = clients_token(from)
	}
	rows, _ := federation_database().rows("select s.position, s.event from stream s join events e on e.id=s.event where s.room=? and s.position>? and s.position<? and e.body like ? order by s.position desc limit 500", room, floor-1, min(position, ceiling+1), "%"+target+"%")
	chunk := []map[string]any{}
	last := int64(0)
	for _, row := range rows {
		event := rooms_event(row_string(row, "event"))
		if event == nil || (kind != "" && event.Type() != kind) {
			continue
		}
		var content struct {
			Relates struct {
				Type  string `json:"rel_type"`
				Event string `json:"event_id"`
			} `json:"m.relates_to"`
		}
		json.Unmarshal(event.Content(), &content)
		if content.Relates.Event != target || (relation != "" && content.Relates.Type != relation) {
			continue
		}
		chunk = append(chunk, clients_event(event, s.h.address, s.device, true))
		last = row_int(row, "position")
		if len(chunk) == limit {
			break
		}
	}
	answer := map[string]any{"chunk": chunk}
	if len(chunk) == limit {
		answer["next_batch"] = "p" + strconv.FormatInt(last, 10)
	}
	c.JSON(http.StatusOK, answer)
}

// clients_threads answers a room's threads: the events replies were made in
// a thread to, newest first.
func clients_threads(c *gin.Context, s *clients_session, room string) {
	if !clients_member(c, s, room) {
		return
	}
	floor, ceiling := clients_window(room, s.h.address)
	rows, _ := federation_database().rows("select s.event from stream s join events e on e.id=s.event where s.room=? and s.position>? and s.position<? and e.body like ? order by s.position desc limit 500", room, floor-1, ceiling+1, `%"m.thread"%`)
	seen := map[string]bool{}
	chunk := []map[string]any{}
	for _, row := range rows {
		event := rooms_event(row_string(row, "event"))
		if event == nil {
			continue
		}
		var content struct {
			Relates struct {
				Type  string `json:"rel_type"`
				Event string `json:"event_id"`
			} `json:"m.relates_to"`
		}
		json.Unmarshal(event.Content(), &content)
		if content.Relates.Type != "m.thread" || seen[content.Relates.Event] {
			continue
		}
		seen[content.Relates.Event] = true
		if root := clients_visible(room, s.h.address, content.Relates.Event); root != nil {
			chunk = append(chunk, clients_event(root, s.h.address, s.device, true))
		}
	}
	c.JSON(http.StatusOK, map[string]any{"chunk": chunk})
}

// clients_timestamp answers the event of a room nearest a time, in a
// direction.
func clients_timestamp(c *gin.Context, s *clients_session, room string) {
	if !clients_member(c, s, room) {
		return
	}
	when, err := strconv.ParseInt(c.Query("ts"), 10, 64)
	if err != nil {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Invalid time")
		return
	}
	floor, ceiling := clients_window(room, s.h.address)
	forward := c.DefaultQuery("dir", "f") == "f"
	entries := clients_timeline(room, floor-1, ceiling+1, 5000, !forward)
	for _, entry := range entries {
		ts := int64(entry.event.OriginServerTS())
		if (forward && ts >= when) || (!forward && ts <= when) {
			c.JSON(http.StatusOK, map[string]any{"event_id": entry.event.EventID(), "origin_server_ts": ts})
			return
		}
	}
	clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No event at that time")
}

// === Reading ===

// clients_typing tells a room the account is typing, or stopped.
func clients_typing(c *gin.Context, s *clients_session, room string, person string) {
	var body struct {
		Typing bool `json:"typing"`
	}
	if !clients_body(c, &body) {
		return
	}
	if person != s.h.address {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Cannot type as another user")
		return
	}
	if rooms_membership(room, s.h.address) != spec.Join {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "You are not in the room")
		return
	}
	signals_typing(s.h, room, body.Typing)
	c.JSON(http.StatusOK, map[string]any{})
}

// clients_receipt records how far the account has read a room: a public
// receipt the room is told of, or a private one only the account's clients
// see. The notifications up to the event are read.
func clients_receipt(c *gin.Context, s *clients_session, room string, kind string, id string) {
	var body struct {
		Thread string `json:"thread_id"`
	}
	if !clients_body(c, &body) {
		return
	}
	if kind != "m.read" && kind != "m.read.private" && kind != "m.fully_read" {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Unknown receipt type")
		return
	}
	if clients_visible(room, s.h.address, id) == nil {
		clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such event")
		return
	}
	clients_read(s, room, kind, body.Thread, id)
	c.JSON(http.StatusOK, map[string]any{})
}

// clients_read records one receipt or marker of the account's in a room.
func clients_read(s *clients_session, room string, kind string, thread string, id string) {
	switch kind {
	case "m.fully_read":
		clients_setting_set(s.h.address, room, "m.fully_read", json.RawMessage(json_encode(map[string]any{"event_id": id})))
		return
	case "m.read":
		if thread == "" || thread == "main" {
			signals_read(s.h, room, id)
		}
		receipts_set(room, s.h.address, kind, clients_thread(thread), id, time.Now().UnixMilli())
	default:
		receipts_set(room, s.h.address, kind, clients_thread(thread), id, time.Now().UnixMilli())
	}
	notifications_read(s.h.address, room, clients_position(id))
}

// clients_thread is a receipt's thread as kept: "" for the whole room.
func clients_thread(thread string) string {
	if thread == "main" {
		return ""
	}
	return thread
}

// clients_markers records the account's read markers and receipts at once.
func clients_markers(c *gin.Context, s *clients_session, room string) {
	var body map[string]string
	if !clients_body(c, &body) {
		return
	}
	for _, kind := range []string{"m.fully_read", "m.read", "m.read.private"} {
		if id := body[kind]; id != "" {
			if clients_visible(room, s.h.address, id) == nil {
				clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such event")
				return
			}
			clients_read(s, room, kind, "", id)
		}
	}
	c.JSON(http.StatusOK, map[string]any{})
}

// === Aliases and the directory ===

// clients_alias answers what room an alias names - one of this server's, or
// one elsewhere asked of its server - or makes or removes one of this
// server's.
func clients_alias(c *gin.Context, s *clients_session, alias string) {
	_, domain, err := gomatrixserverlib.SplitID('#', alias)
	if err != nil {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Invalid alias")
		return
	}
	db := federation_database()
	switch c.Request.Method {
	case "GET":
		if federation_local(domain) {
			row, _ := db.row("select room from aliases where alias=?", alias)
			if row == nil {
				clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such alias")
				return
			}
			state, _ := rooms_current(row_string(row, "room"))
			c.JSON(http.StatusOK, map[string]any{"room_id": row_string(row, "room"), "servers": rooms_servers(state)})
			return
		}
		mine, _ := spec.NewUserID(s.h.address, true)
		found, err := federation_client().LookupRoomAlias(c.Request.Context(), mine.Domain(), domain, alias)
		if err != nil || found.RoomID == "" {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such alias")
			return
		}
		c.JSON(http.StatusOK, map[string]any{"room_id": found.RoomID, "servers": found.Servers})
	case "PUT":
		var body struct {
			Room string `json:"room_id"`
		}
		if !clients_body(c, &body) {
			return
		}
		mine, _ := spec.NewUserID(s.h.address, true)
		if domain != mine.Domain() {
			clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "An alias here is on the account's own domain")
			return
		}
		if rooms_membership(body.Room, s.h.address) != spec.Join {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "You are not in the room")
			return
		}
		if taken, _ := db.exists("select 1 from aliases where alias=?", alias); taken {
			clients_error(c, http.StatusConflict, "M_UNKNOWN", "That alias is taken")
			return
		}
		db.exec("insert into aliases (alias, room, creator, created) values (?, ?, ?, ?)", alias, body.Room, s.h.address, now())
		c.JSON(http.StatusOK, map[string]any{})
	case "DELETE":
		row, _ := db.row("select room, creator from aliases where alias=?", alias)
		if row == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such alias")
			return
		}
		if row_string(row, "creator") != s.h.address && !clients_powerful(row_string(row, "room"), s.h.address, "m.room.canonical_alias") {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Only its maker or the room's moderators may remove it")
			return
		}
		db.exec("delete from aliases where alias=?", alias)
		c.JSON(http.StatusOK, map[string]any{})
	default:
		clients_error(c, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "Unrecognised request")
	}
}

// clients_powerful says whether a user may send a state event of a kind in a
// room by its power levels.
func clients_powerful(room string, user string, kind string) bool {
	levels := clients_state_content(room, spec.MRoomPowerLevels, "")
	need := 50.0
	if events, ok := levels["events"].(map[string]any); ok {
		if level, ok := events[kind].(float64); ok {
			need = level
		}
	} else if level, ok := levels["state_default"].(float64); ok {
		need = level
	}
	have := 0.0
	if users, ok := levels["users"].(map[string]any); ok {
		if level, ok := users[user].(float64); ok {
			have = level
		} else if level, ok := levels["users_default"].(float64); ok {
			have = level
		}
	}
	return have >= need
}

// clients_listing answers whether a room is in this server's directory, or
// lists or unlists it, by its moderators.
func clients_listing(c *gin.Context, s *clients_session, room string) {
	db := federation_database()
	switch c.Request.Method {
	case "GET":
		visibility := "private"
		if listed, _ := db.exists("select 1 from published where room=?", room); listed {
			visibility = "public"
		}
		c.JSON(http.StatusOK, map[string]any{"visibility": visibility})
	case "PUT":
		var body struct {
			Visibility string `json:"visibility"`
		}
		if !clients_body(c, &body) {
			return
		}
		if rooms_membership(room, s.h.address) != spec.Join || !clients_powerful(room, s.h.address, "m.room.canonical_alias") {
			clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Only the room's moderators may list it")
			return
		}
		if body.Visibility == "public" {
			db.exec("insert or ignore into published (room, created) values (?, ?)", room, now())
		} else {
			db.exec("delete from published where room=?", room)
		}
		c.JSON(http.StatusOK, map[string]any{})
	default:
		clients_error(c, http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "Unrecognised request")
	}
}

// clients_public answers a server's directory of rooms: this server's own,
// filtered by a term, or another server's asked of it.
func clients_public(c *gin.Context, s *clients_session) {
	var body struct {
		Limit  int    `json:"limit"`
		Since  string `json:"since"`
		Server string `json:"server"`
		Filter struct {
			Term string `json:"generic_search_term"`
		} `json:"filter"`
	}
	if c.Request.Method == "POST" {
		if !clients_body(c, &body) {
			return
		}
	} else {
		body.Limit, _ = strconv.Atoi(c.Query("limit"))
		body.Since = c.Query("since")
	}
	server := c.Query("server")
	if server == "" {
		server = body.Server
	}
	if server != "" && !federation_local(spec.ServerName(server)) {
		mine, _ := spec.NewUserID(s.h.address, true)
		limit := body.Limit
		if limit <= 0 {
			limit = 30
		}
		found, err := federation_client().GetPublicRoomsFiltered(c.Request.Context(), mine.Domain(), spec.ServerName(server), min(limit, 100), body.Since, body.Filter.Term, false, "")
		if err != nil {
			clients_error(c, http.StatusBadGateway, "M_UNKNOWN", "The server's directory could not be read")
			return
		}
		c.JSON(http.StatusOK, found)
		return
	}
	c.JSON(http.StatusOK, rooms_published(body.Filter.Term, body.Limit, body.Since))
}

// rooms_published is this server's directory of rooms, those whose name,
// topic or alias hold a term, a page from a position.
func rooms_published(term string, limit int, since string) map[string]any {
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	offset, _ := strconv.Atoi(since)
	rows, _ := federation_database().rows("select room from published order by created")
	var chunk []map[string]any
	for _, row := range rows {
		room := row_string(row, "room")
		entry := rooms_entry(room)
		if entry == nil {
			continue
		}
		text := strings.ToLower(fmt.Sprint(entry["name"], " ", entry["topic"], " ", entry["canonical_alias"]))
		if term != "" && !strings.Contains(text, strings.ToLower(term)) {
			continue
		}
		chunk = append(chunk, entry)
	}
	total := len(chunk)
	page := []map[string]any{}
	if offset < len(chunk) {
		page = chunk[offset:min(offset+limit, len(chunk))]
	}
	answer := map[string]any{"chunk": page, "total_room_count_estimate": total}
	if offset+limit < len(chunk) {
		answer["next_batch"] = strconv.Itoa(offset + limit)
	}
	if offset > 0 {
		answer["prev_batch"] = strconv.Itoa(max(0, offset-limit))
	}
	return answer
}

// rooms_entry is a room as a directory lists it, or nil for one the server
// is no longer in.
func rooms_entry(room string) map[string]any {
	if rooms_version(room) == nil {
		return nil
	}
	state, _ := rooms_current(room)
	joined := 0
	for _, membership := range rooms_members(state) {
		if membership == spec.Join {
			joined++
		}
	}
	entry := map[string]any{"room_id": room, "num_joined_members": joined, "guest_can_join": false, "world_readable": false}
	if name, _ := clients_state_content(room, "m.room.name", "")["name"].(string); name != "" {
		entry["name"] = name
	}
	if topic, _ := clients_state_content(room, "m.room.topic", "")["topic"].(string); topic != "" {
		entry["topic"] = topic
	}
	if alias, _ := clients_state_content(room, "m.room.canonical_alias", "")["alias"].(string); alias != "" {
		entry["canonical_alias"] = alias
	}
	if avatar, _ := clients_state_content(room, "m.room.avatar", "")["url"].(string); avatar != "" {
		entry["avatar_url"] = avatar
	}
	if rule, _ := clients_state_content(room, spec.MRoomJoinRules, "")["join_rule"].(string); rule != "" {
		entry["join_rule"] = rule
	}
	if visibility, _ := clients_state_content(room, spec.MRoomHistoryVisibility, "")["history_visibility"].(string); visibility == "world_readable" {
		entry["world_readable"] = true
	}
	if guest, _ := clients_state_content(room, "m.room.guest_access", "")["guest_access"].(string); guest == "can_join" {
		entry["guest_can_join"] = true
	}
	if kind, _ := clients_state_content(room, spec.MRoomCreate, "")["type"].(string); kind != "" {
		entry["room_type"] = kind
	}
	return entry
}

// === Upgrades ===

// clients_upgrade replaces a room with one of a newer version: the new room
// takes the old one's state that carries over, the old one is closed with a
// tombstone naming it, and its aliases move.
func clients_upgrade(c *gin.Context, s *clients_session, room string) {
	var body struct {
		Version string `json:"new_version"`
	}
	if !clients_body(c, &body) {
		return
	}
	if rooms_membership(room, s.h.address) != spec.Join || !clients_powerful(room, s.h.address, "m.room.tombstone") {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", "Only the room's administrators may upgrade it")
		return
	}
	if _, err := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersion(body.Version)); err != nil {
		clients_error(c, http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", "That room version is not supported")
		return
	}
	creation := clients_creation{Version: body.Version, Preset: "private_chat"}
	creation.Creation = map[string]any{}
	if kind, ok := clients_state_content(room, spec.MRoomCreate, "")["type"]; ok {
		creation.Creation["type"] = kind
	}
	for _, kind := range []string{spec.MRoomJoinRules, spec.MRoomHistoryVisibility, "m.room.guest_access", "m.room.name", "m.room.topic", "m.room.avatar", "m.room.encryption", "m.room.server_acl", "m.room.canonical_alias"} {
		content := clients_state_content(room, kind, "")
		if len(content) == 0 {
			continue
		}
		creation.Initial = append(creation.Initial, struct {
			Type    string         `json:"type"`
			Key     string         `json:"state_key"`
			Content map[string]any `json:"content"`
		}{kind, "", content})
	}
	powers := clients_state_content(room, spec.MRoomPowerLevels, "")
	if len(powers) > 0 {
		creation.Override = powers
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()
	replacement, err := clients_make(ctx, s.h.address, creation, room)
	if err != nil {
		clients_error(c, http.StatusBadRequest, "M_UNKNOWN", err.Error())
		return
	}
	empty := ""
	if _, err := rooms_send(ctx, room, s.h.address, "m.room.tombstone", &empty, map[string]any{"body": "This room has been replaced", "replacement_room": replacement}); err != nil {
		clients_error(c, http.StatusForbidden, "M_FORBIDDEN", err.Error())
		return
	}
	db := federation_database()
	db.exec("update aliases set room=? where room=?", replacement, room)
	if listed, _ := db.exists("select 1 from published where room=?", room); listed {
		db.exec("delete from published where room=?", room)
		db.exec("insert or ignore into published (room, created) values (?, ?)", replacement, now())
	}
	c.JSON(http.StatusOK, map[string]any{"replacement_room": replacement})
}

// === Search ===

// clients_search answers the events of the account's rooms whose text holds
// every word of a term - a message's body, a room's name or topic, as the
// keys ask - newest first, a page at a time, each with those around it where
// asked. What is encrypted is the clients' to search.
func clients_search(c *gin.Context, s *clients_session) {
	var body struct {
		Categories struct {
			Events *struct {
				Term    string      `json:"search_term"`
				Keys    []string    `json:"keys"`
				Filter  sync_events `json:"filter"`
				Context *struct {
					Before int `json:"before_limit"`
					After  int `json:"after_limit"`
				} `json:"event_context"`
				State bool `json:"include_state"`
			} `json:"room_events"`
		} `json:"search_categories"`
	}
	if !clients_body(c, &body) {
		return
	}
	asked := body.Categories.Events
	if asked == nil {
		c.JSON(http.StatusOK, map[string]any{"search_categories": map[string]any{}})
		return
	}
	words := strings.Fields(strings.ToLower(asked.Term))
	if len(words) == 0 {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Search for something")
		return
	}
	keys := asked.Keys
	if len(keys) == 0 {
		keys = []string{"content.body", "content.name", "content.topic"}
	}
	limit := asked.Filter.Limit
	if limit <= 0 {
		limit = 10
	}
	limit = min(limit, 50)
	before := int64(clients_unbounded + 1)
	if batch := c.Query("next_batch"); batch != "" {
		before = clients_token(batch)
	}
	type found struct {
		position int64
		room     string
		event    gomatrixserverlib.PDU
	}
	var results []found
	rooms := []string{}
	for room, membership := range clients_memberships(s.h.address) {
		if membership != spec.Join && membership != spec.Leave && membership != spec.Ban {
			continue
		}
		if asked.Filter.Rooms != nil && !slices.Contains(asked.Filter.Rooms, room) || slices.Contains(asked.Filter.NotRooms, room) {
			continue
		}
		rooms = append(rooms, room)
		floor, ceiling := clients_window(room, s.h.address)
		rows, _ := federation_database().rows("select s.position, s.event from stream s join events e on e.id=s.event where s.room=? and s.position>? and s.position<? and e.redacted=0 and e.type in ('m.room.message', 'm.room.name', 'm.room.topic') and lower(e.body) like ? order by s.position desc limit 200", room, floor-1, min(before, ceiling+1), "%"+words[0]+"%")
		for _, row := range rows {
			event := rooms_event(row_string(row, "event"))
			if event == nil || !asked.Filter.allows(event) {
				continue
			}
			var content map[string]any
			json.Unmarshal(event.Content(), &content)
			text := ""
			for _, key := range keys {
				if value, ok := content[strings.TrimPrefix(key, "content.")].(string); ok {
					text += " " + strings.ToLower(value)
				}
			}
			every := true
			for _, word := range words {
				every = every && strings.Contains(text, word)
			}
			if every {
				results = append(results, found{row_int(row, "position"), room, event})
			}
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].position > results[j].position })
	total := len(results)
	next := ""
	if len(results) > limit {
		results = results[:limit]
		next = "p" + strconv.FormatInt(results[limit-1].position, 10)
	}
	out := []map[string]any{}
	for _, result := range results {
		entry := map[string]any{"rank": 1, "result": clients_event(result.event, s.h.address, s.device, true)}
		if asked.Context != nil {
			floor, ceiling := clients_window(result.room, s.h.address)
			ahead := clients_timeline(result.room, floor-1, result.position, min(max(asked.Context.Before, 0), 20), true)
			behind := clients_timeline(result.room, result.position, ceiling+1, min(max(asked.Context.After, 0), 20), false)
			var before, after []gomatrixserverlib.PDU
			for _, e := range ahead {
				before = append(before, e.event)
			}
			for _, e := range behind {
				after = append(after, e.event)
			}
			entry["context"] = map[string]any{"events_before": clients_events(before, s.h.address, s.device, true), "events_after": clients_events(after, s.h.address, s.device, true)}
		}
		out = append(out, entry)
	}
	answer := map[string]any{"count": total, "results": out, "highlights": words}
	if next != "" {
		answer["next_batch"] = next
	}
	if asked.State {
		state := map[string]any{}
		for _, room := range rooms {
			current, _ := rooms_current(room)
			state[room] = clients_events(rooms_list(current), s.h.address, s.device, true)
		}
		answer["state"] = state
	}
	c.JSON(http.StatusOK, map[string]any{"search_categories": map[string]any{"room_events": answer}})
}

// === Spaces ===

// clients_hierarchy walks a space: the room and the rooms its m.space.child
// events name, as deep as asked, those the server is in described from
// their state and those it is not asked of their servers.
func clients_hierarchy(c *gin.Context, s *clients_session, room string) {
	if !clients_member(c, s, room) {
		return
	}
	depth, err := strconv.Atoi(c.DefaultQuery("max_depth", "3"))
	if err != nil {
		depth = 3
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	limit = min(max(limit, 1), 100)
	suggested := c.Query("suggested_only") == "true"
	mine, _ := spec.NewUserID(s.h.address, true)
	rooms := []map[string]any{}
	seen := map[string]bool{}
	type visit struct {
		room  string
		depth int
		via   []string
	}
	queue := []visit{{room, 0, nil}}
	for len(queue) > 0 && len(rooms) < limit {
		v := queue[0]
		queue = queue[1:]
		if seen[v.room] {
			continue
		}
		seen[v.room] = true
		entry := rooms_entry(v.room)
		var children []map[string]any
		if entry != nil {
			state, _ := rooms_current(v.room)
			for key, id := range state {
				if !strings.HasPrefix(key, "m.space.child\x00") {
					continue
				}
				event := rooms_event(id)
				if event == nil {
					continue
				}
				var content map[string]any
				json.Unmarshal(event.Content(), &content)
				if content["via"] == nil || (suggested && content["suggested"] != true) {
					continue
				}
				children = append(children, clients_event(event, s.h.address, "", false))
			}
		} else if len(v.via) > 0 {
			for _, server := range v.via {
				found, err := federation_client().RoomHierarchy(c.Request.Context(), mine.Domain(), spec.ServerName(server), v.room, suggested)
				if err != nil {
					continue
				}
				encoded, _ := json.Marshal(found.Room)
				json.Unmarshal(encoded, &entry)
				for _, child := range found.Room.ChildrenState {
					var event map[string]any
					encoded, _ := json.Marshal(child)
					json.Unmarshal(encoded, &event)
					children = append(children, event)
				}
				break
			}
		}
		if entry == nil {
			continue
		}
		entry["children_state"] = children
		if children == nil {
			entry["children_state"] = []map[string]any{}
		}
		rooms = append(rooms, entry)
		if v.depth >= depth {
			continue
		}
		for _, child := range children {
			key, _ := child["state_key"].(string)
			var via []string
			if content, ok := child["content"].(map[string]any); ok {
				if servers, ok := content["via"].([]any); ok {
					for _, server := range servers {
						if name, ok := server.(string); ok {
							via = append(via, name)
						}
					}
				}
			}
			queue = append(queue, visit{key, v.depth + 1, via})
		}
	}
	c.JSON(http.StatusOK, map[string]any{"rooms": rooms})
}
