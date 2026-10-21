// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix clients' sync
//
// How a Matrix client learns what is new (the federation plan, phase 28):
// sliding sync (MSC4186), which Element X speaks, a window on the rooms
// ordered by their latest activity with each room's state and timeline as
// the client asks; and the older /sync, every room at once from a position.
// Both count in the room timelines' order (the stream table), the key
// store's deliveries and device changes, the receipts', the account data's,
// typing's and the invitations' positions, and wait for something new up to
// the time the client gives.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// sync_wait_maximum bounds how long a sync waits for something new.
const sync_wait_maximum = 60 * time.Second

// === Positions ===

// A sync_position is how far a device's sync has got in each of the orders
// it counts in.
type sync_position struct {
	stream    int64
	delivered int64
	changes   int64
	receipts  int64
	settings  int64
	typing    int64
	invites   int64
	rules     int64
}

// String is the position as a client gives it back.
func (p sync_position) String() string {
	return fmt.Sprintf("m%d_%d_%d_%d_%d_%d_%d_%d", p.stream, p.delivered, p.changes, p.receipts, p.settings, p.typing, p.invites, p.rules)
}

// sync_parse reads a position a client gives back; an unknown one reads as
// the start.
func sync_parse(token string) sync_position {
	var p sync_position
	rest, ok := strings.CutPrefix(token, "m")
	if !ok {
		return p
	}
	values := strings.Split(rest, "_")
	targets := []*int64{&p.stream, &p.delivered, &p.changes, &p.receipts, &p.settings, &p.typing, &p.invites, &p.rules}
	for i, value := range values {
		if i < len(targets) {
			*targets[i], _ = strconv.ParseInt(value, 10, 64)
		}
	}
	return p
}

// sync_last is the last position an order has given, which its table keeps
// even when the rows are gone - a knock or invitation's row goes when the
// user joins - so a token never goes back.
func sync_last(db *DB, table string) int64 {
	row, _ := db.row("select coalesce((select seq from sqlite_sequence where name=?), 0) as position", table)
	return row_int(row, "position")
}

// sync_maximum is the room timelines' latest position.
func sync_maximum() int64 {
	return sync_last(federation_database(), "stream")
}

// sync_latest is the latest position of each order but the key store's,
// which a device's own sync moves on.
func sync_latest() sync_position {
	db := federation_database()
	return sync_position{
		stream:   sync_maximum(),
		receipts: sync_last(db, "receipts"),
		settings: sync_last(db, "settings"),
		typing:   typing_position(),
		invites:  sync_last(db, "pending"),
		rules:    sync_last(db, "rules"),
	}
}

// === Filters ===

// A sync_events is a filter on events.
type sync_events struct {
	Limit      int      `json:"limit"`
	Types      []string `json:"types"`
	NotTypes   []string `json:"not_types"`
	Senders    []string `json:"senders"`
	NotSenders []string `json:"not_senders"`
	Rooms      []string `json:"rooms"`
	NotRooms   []string `json:"not_rooms"`
	Lazy       bool     `json:"lazy_load_members"`
}

// A sync_filter is a client's filter on its syncs.
type sync_filter struct {
	Room struct {
		Rooms     []string    `json:"rooms"`
		NotRooms  []string    `json:"not_rooms"`
		Leave     bool        `json:"include_leave"`
		Timeline  sync_events `json:"timeline"`
		State     sync_events `json:"state"`
		Ephemeral sync_events `json:"ephemeral"`
		Data      sync_events `json:"account_data"`
	} `json:"room"`
	Data     sync_events `json:"account_data"`
	Presence sync_events `json:"presence"`
}

// sync_type_matches matches an event type to a filter's, which may end in
// a wildcard.
func sync_type_matches(kind string, patterns []string) bool {
	for _, pattern := range patterns {
		if pattern == kind || pattern == "*" || (strings.HasSuffix(pattern, "*") && strings.HasPrefix(kind, strings.TrimSuffix(pattern, "*"))) {
			return true
		}
	}
	return false
}

// allows says whether an event passes the filter.
func (f sync_events) allows(event gomatrixserverlib.PDU) bool {
	if f.Types != nil && !sync_type_matches(event.Type(), f.Types) {
		return false
	}
	if sync_type_matches(event.Type(), f.NotTypes) {
		return false
	}
	sender := string(event.SenderID())
	if f.Senders != nil && !slices.Contains(f.Senders, sender) {
		return false
	}
	return !slices.Contains(f.NotSenders, sender)
}

// allows_room says whether a room passes the room filter.
func (f *sync_filter) allows_room(room string) bool {
	if f.Room.Rooms != nil && !slices.Contains(f.Room.Rooms, room) {
		return false
	}
	return !slices.Contains(f.Room.NotRooms, room)
}

// sync_filter_read reads a filter a sync names: a kept filter's id, or the
// filter itself as JSON.
func sync_filter_read(s *clients_session, value string) *sync_filter {
	filter := &sync_filter{}
	value = strings.TrimSpace(value)
	switch {
	case value == "":
	case strings.HasPrefix(value, "{"):
		json.Unmarshal([]byte(value), filter)
	default:
		row, _ := federation_database().row("select body from filters where id=? and user=?", value, s.h.address)
		if row != nil {
			json.Unmarshal([]byte(row_string(row, "body")), filter)
		}
	}
	return filter
}

// sync_lazy is the membership of the senders of events, and the user's own,
// for a client that loads members lazily.
func sync_lazy(room string, events []gomatrixserverlib.PDU, self string) []gomatrixserverlib.PDU {
	state, _ := rooms_current(room)
	wanted := map[string]bool{self: true}
	for _, event := range events {
		wanted[string(event.SenderID())] = true
	}
	var ids []string
	for member := range wanted {
		if id := state[rooms_key(spec.MRoomMember, member)]; id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	loaded := rooms_events(ids)
	out := make([]gomatrixserverlib.PDU, 0, len(ids))
	for _, id := range ids {
		if event := loaded[id]; event != nil {
			out = append(out, event)
		}
	}
	return out
}

// === Routing ===

// sync_dispatch answers a sync, reporting whether the request was one.
func sync_dispatch(c *gin.Context, s *clients_session, parts []string) bool {
	switch c.Request.Method + " " + strings.Join(parts, "/") {
	case "GET sync":
		sync_legacy(c, s)
	case "POST sync":
		sliding_sync(c, s)
	default:
		return false
	}
	return true
}

// sync_timeout reads how long a client lets a sync wait.
func sync_timeout(c *gin.Context) time.Duration {
	ms, _ := strconv.ParseInt(c.Query("timeout"), 10, 64)
	return min(time.Duration(max(ms, 0))*time.Millisecond, sync_wait_maximum)
}

// sync_wait waits for something new, until a deadline or the client leaves;
// answers false when it should stop waiting.
func sync_wait(ctx context.Context, waiter chan struct{}, deadline time.Time) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-waiter:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// === The older sync ===

// sync_legacy answers /sync: what is new since the position given, or
// everything for a device's first sync, waiting for something where nothing
// is new yet.
func sync_legacy(c *gin.Context, s *clients_session) {
	since := c.Query("since")
	full := c.Query("full_state") == "true"
	filter := sync_filter_read(s, c.Query("filter"))
	from := sync_parse(since)
	deadline := time.Now().Add(sync_timeout(c))
	for {
		waiter := clients_waiter()
		answer, empty := sync_build(s, from, since == "", full, filter)
		if !empty || since == "" || full {
			c.JSON(http.StatusOK, answer)
			return
		}
		if !sync_wait(c.Request.Context(), waiter, deadline) {
			if c.Request.Context().Err() == nil {
				c.JSON(http.StatusOK, answer)
			}
			return
		}
	}
}

// sync_build is a sync's answer from a position, and whether it holds
// nothing new.
func sync_build(s *clients_session, from sync_position, initial bool, full bool, filter *sync_filter) (map[string]any, bool) {
	address := s.h.address
	next := sync_latest()
	deliveries, delivered := keyring_deliveries(address, s.device, from.delivered)
	changed, changes := keyring_changes(address, from.changes, initial)
	next.delivered, next.changes = delivered, changes
	limit := filter.Room.Timeline.Limit
	if limit <= 0 {
		limit = 10
	}
	joined, invited, left, knocked := map[string]any{}, map[string]any{}, map[string]any{}, map[string]any{}
	for room, membership := range clients_memberships(address) {
		if !filter.allows_room(room) {
			continue
		}
		switch membership {
		case spec.Join:
			if entry := sync_joined(s, room, from, next, initial || full, limit, filter); entry != nil {
				joined[room] = entry
			}
		case spec.Invite:
			if entry := sync_invited(s, room, from, next, initial); entry != nil {
				invited[room] = entry
			}
		case "knock":
			if events := sync_knocked(s, room, from, initial); events != nil {
				knocked[room] = map[string]any{"knock_state": map[string]any{"events": events}}
			}
		case spec.Leave, spec.Ban:
			if (!initial || filter.Room.Leave) && !clients_forgotten(address, room) {
				if entry := sync_left(s, room, from, next, limit); entry != nil {
					left[room] = entry
				}
			}
		}
	}
	data := sync_data(address, "", from.settings, initial)
	if initial || from.rules < next.rules {
		data = append(data, map[string]any{"type": "m.push_rules", "content": rules_ruleset(address)})
	}
	answer := map[string]any{
		"next_batch":                       next.String(),
		"rooms":                            map[string]any{"join": joined, "invite": invited, "leave": left, "knock": knocked},
		"account_data":                     map[string]any{"events": data},
		"presence":                         map[string]any{"events": []any{}},
		"to_device":                        map[string]any{"events": deliveries},
		"device_lists":                     map[string]any{"changed": changed, "left": []string{}},
		"device_one_time_keys_count":       keyring_counts(address, s.device),
		"device_unused_fallback_key_types": keyring_unused(address, s.device),
		"org.matrix.msc2732.device_unused_fallback_key_types": keyring_unused(address, s.device),
	}
	empty := len(joined) == 0 && len(invited) == 0 && len(left) == 0 && len(knocked) == 0 && len(data) == 0 && len(deliveries) == 0 && len(changed) == 0
	return answer, empty
}

// clients_forgotten says whether the account forgot a room it left.
func clients_forgotten(user string, room string) bool {
	return clients_setting(user, room, "org.mochi.forgotten") != nil
}

// sync_data is the account's data changed since a position, global or a
// room's, all of it for a first sync.
func sync_data(user string, room string, since int64, initial bool) []map[string]any {
	if initial {
		since = 0
	}
	rows, _ := federation_database().rows("select type, content from settings where user=? and room=? and position>? order by position", user, room, since)
	events := []map[string]any{}
	for _, row := range rows {
		if strings.HasPrefix(row_string(row, "type"), "org.mochi.") {
			continue
		}
		var content any
		json.Unmarshal([]byte(row_string(row, "content")), &content)
		events = append(events, map[string]any{"type": row_string(row, "type"), "content": content})
	}
	return events
}

// sync_joined is a joined room's part of a sync, or nil where nothing in it
// is new: its timeline, the state before it, what is ephemeral, the room's
// data, its unread counts and summary. A room joined since the position, or
// a first sync, is given whole.
func sync_joined(s *clients_session, room string, from sync_position, next sync_position, whole bool, limit int, filter *sync_filter) map[string]any {
	address := s.h.address
	floor, ceiling := clients_window(room, address)
	ceiling = min(ceiling, next.stream)
	join := clients_join_position(room, address)
	if join > from.stream {
		whole = true
	}
	var timeline []clients_entry
	limited := false
	if whole {
		timeline = clients_timeline(room, floor-1, ceiling+1, limit+1, true)
	} else {
		timeline = clients_timeline(room, max(from.stream, floor-1), ceiling+1, limit+1, true)
	}
	if len(timeline) > limit {
		limited = true
		timeline = timeline[:limit]
	}
	slices.Reverse(timeline)
	var events []gomatrixserverlib.PDU
	ids := map[string]bool{}
	for _, entry := range timeline {
		if filter.Room.Timeline.allows(entry.event) {
			events = append(events, entry.event)
		}
		ids[entry.event.EventID()] = true
	}
	// The state before the timeline: all of it for a room given whole, else
	// the keys changed by events the timeline left out.
	var state []gomatrixserverlib.PDU
	if whole || limited {
		current, _ := rooms_current(room)
		var skipped map[string]bool
		if !whole && len(timeline) > 0 {
			skipped = map[string]bool{}
			gap := clients_timeline(room, max(from.stream, floor-1), timeline[0].position, 5000, false)
			for _, entry := range gap {
				if key := entry.event.StateKey(); key != nil {
					skipped[rooms_key(entry.event.Type(), *key)] = true
				}
			}
		}
		lazy := filter.Room.State.Lazy
		var members map[string]bool
		if lazy {
			members = map[string]bool{address: true}
			for _, event := range events {
				members[string(event.SenderID())] = true
			}
		}
		for _, event := range rooms_list(current) {
			key := rooms_key(event.Type(), *event.StateKey())
			if ids[event.EventID()] || (skipped != nil && !skipped[key]) {
				continue
			}
			if lazy && event.Type() == spec.MRoomMember && !members[*event.StateKey()] {
				continue
			}
			if filter.Room.State.allows(event) {
				state = append(state, event)
			}
		}
	}
	ephemeral := sync_ephemeral(room, address, from, whole)
	data := sync_data(address, room, from.settings, whole)
	if !whole && len(events) == 0 && len(state) == 0 && len(ephemeral) == 0 && len(data) == 0 {
		return nil
	}
	notify, highlight := notifications_counts(address, room)
	entry := map[string]any{
		"timeline": map[string]any{
			"events":  clients_events(events, address, s.device, false),
			"limited": limited,
		},
		"state":                map[string]any{"events": clients_events(state, address, s.device, false)},
		"ephemeral":            map[string]any{"events": ephemeral},
		"account_data":         map[string]any{"events": data},
		"unread_notifications": map[string]any{"notification_count": notify, "highlight_count": highlight},
		"summary":              sync_summary(room, address),
	}
	if len(timeline) > 0 {
		entry["timeline"].(map[string]any)["prev_batch"] = "p" + strconv.FormatInt(timeline[0].position, 10)
	} else {
		entry["timeline"].(map[string]any)["prev_batch"] = "p" + strconv.FormatInt(ceiling+1, 10)
	}
	return entry
}

// clients_join_position is the position of a user's latest joining a room,
// or 0.
func clients_join_position(room string, user string) int64 {
	row, _ := federation_database().row("select max(s.position) as position from stream s join events e on e.id=s.event where s.room=? and e.type='m.room.member' and e.key=? and e.membership='join'", room, user)
	return row_int(row, "position")
}

// sync_ephemeral is a room's typing and receipts changed since a position,
// or now for a room given whole.
func sync_ephemeral(room string, user string, from sync_position, whole bool) []map[string]any {
	events := []map[string]any{}
	if whole || typing_changed(room, from.typing) {
		events = append(events, map[string]any{"type": "m.typing", "content": map[string]any{"user_ids": typing_users(room)}})
	}
	since := from.receipts
	if whole {
		since = 0
	}
	if content := receipts_content(room, user, since); len(content) > 0 {
		events = append(events, map[string]any{"type": "m.receipt", "content": content})
	}
	return events
}

// receipts_content is a room's receipts kept since a position, as an
// m.receipt event's content; a private one only for the user it is theirs.
func receipts_content(room string, user string, since int64) map[string]any {
	rows, _ := federation_database().rows("select user, type, thread, event, ts from receipts where room=? and position>? order by position", room, since)
	content := map[string]any{}
	for _, row := range rows {
		kind := row_string(row, "type")
		if kind == "m.read.private" && row_string(row, "user") != user {
			continue
		}
		event := row_string(row, "event")
		kinds, _ := content[event].(map[string]any)
		if kinds == nil {
			kinds = map[string]any{}
			content[event] = kinds
		}
		users, _ := kinds[kind].(map[string]any)
		if users == nil {
			users = map[string]any{}
			kinds[kind] = users
		}
		receipt := map[string]any{"ts": row_int(row, "ts")}
		if thread := row_string(row, "thread"); thread != "" {
			receipt["thread_id"] = thread
		}
		users[row_string(row, "user")] = receipt
	}
	return content
}

// sync_summary is a room's summary: its members other than the user, up to
// five, and its counts.
func sync_summary(room string, user string) map[string]any {
	state, _ := rooms_current(room)
	joined, invited := 0, 0
	var heroes []string
	for member, membership := range rooms_members(state) {
		switch membership {
		case spec.Join:
			joined++
		case spec.Invite:
			invited++
		default:
			continue
		}
		if member != user {
			heroes = append(heroes, member)
		}
	}
	sort.Strings(heroes)
	if len(heroes) > 5 {
		heroes = heroes[:5]
	}
	if heroes == nil {
		heroes = []string{}
	}
	return map[string]any{"m.heroes": heroes, "m.joined_member_count": joined, "m.invited_member_count": invited}
}

// sync_invited is an invitation's part of a sync where it is new: the
// invitation and the state it shows of the room.
func sync_invited(s *clients_session, room string, from sync_position, next sync_position, initial bool) map[string]any {
	db := federation_database()
	if row, _ := db.row("select i.body, i.state, coalesce(p.position, 0) as position from invites i left join pending p on p.room=i.room and p.user=i.user where i.room=? and i.user=?", room, s.h.address); row != nil {
		if !initial && row_int(row, "position") <= from.invites {
			return nil
		}
		var stripped []any
		json.Unmarshal([]byte(row_string(row, "state")), &stripped)
		var invite map[string]any
		json.Unmarshal([]byte(row_string(row, "body")), &invite)
		if invite != nil {
			stripped = append(stripped, map[string]any{"type": invite["type"], "state_key": invite["state_key"], "sender": invite["sender"], "content": invite["content"]})
		}
		return map[string]any{"invite_state": map[string]any{"events": stripped}}
	}
	state, _ := rooms_current(room)
	id := state[rooms_key(spec.MRoomMember, s.h.address)]
	if !initial && clients_position(id) <= from.stream {
		return nil
	}
	event := rooms_event(id)
	if event == nil {
		return nil
	}
	var stripped []any
	for _, part := range rooms_stripped(room, string(event.SenderID())) {
		stripped = append(stripped, part)
	}
	stripped = append(stripped, clients_event(event, s.h.address, "", false))
	return map[string]any{"invite_state": map[string]any{"events": stripped}}
}

// sync_knocked is the state a knock shows of a room where it is new: the
// room's stripped state and the knock, from the room here, or as the server
// knocked through answered it.
func sync_knocked(s *clients_session, room string, from sync_position, initial bool) []any {
	if row, _ := federation_database().row("select position, state from pending where room=? and user=? and kind='knock'", room, s.h.address); row != nil {
		if !initial && row_int(row, "position") <= from.invites {
			return nil
		}
		var stripped []any
		json.Unmarshal([]byte(row_string(row, "state")), &stripped)
		return stripped
	}
	state, _ := rooms_current(room)
	id := state[rooms_key(spec.MRoomMember, s.h.address)]
	event := rooms_event(id)
	if event == nil || (!initial && clients_position(id) <= from.stream) {
		return nil
	}
	var stripped []any
	for _, part := range rooms_stripped(room, "") {
		stripped = append(stripped, part)
	}
	return append(stripped, clients_event(event, s.h.address, "", false))
}

// sync_left is a room left since the position: its timeline up to the
// leaving, or nothing but the leaving of a room this server is not in.
func sync_left(s *clients_session, room string, from sync_position, next sync_position, limit int) map[string]any {
	if row, _ := federation_database().row("select position from pending where room=? and user=? and kind='leave'", room, s.h.address); row != nil && rooms_membership(room, s.h.address) == "" {
		if row_int(row, "position") <= from.invites {
			return nil
		}
		return map[string]any{
			"timeline":     map[string]any{"events": []any{}, "limited": false},
			"state":        map[string]any{"events": []any{}},
			"account_data": map[string]any{"events": []any{}},
		}
	}
	_, ceiling := clients_window(room, s.h.address)
	if ceiling <= from.stream || ceiling > next.stream {
		return nil
	}
	timeline := clients_timeline(room, from.stream, ceiling+1, limit, true)
	slices.Reverse(timeline)
	var events []gomatrixserverlib.PDU
	for _, entry := range timeline {
		events = append(events, entry.event)
	}
	return map[string]any{
		"timeline":     map[string]any{"events": clients_events(events, s.h.address, s.device, false), "limited": false},
		"state":        map[string]any{"events": []any{}},
		"account_data": map[string]any{"events": []any{}},
	}
}

// typing_changed says whether a room's typing changed since a position.
func typing_changed(room string, since int64) bool {
	typing_lock.Lock()
	defer typing_lock.Unlock()
	return typing_changes[room] > since
}

// === Sliding sync ===

// A sliding_request is a sliding sync request's body.
type sliding_request struct {
	Connection string                      `json:"conn_id"`
	Lists      map[string]sliding_list     `json:"lists"`
	Rooms      map[string]sliding_interest `json:"room_subscriptions"`
	Extensions sliding_extensions          `json:"extensions"`
}

// A sliding_interest is what a client wants of a room: the state, by type
// and key, and how many timeline events.
type sliding_interest struct {
	State    [][]string `json:"required_state"`
	Timeline int        `json:"timeline_limit"`
}

// A sliding_list is a window on the account's rooms, ordered by activity.
type sliding_list struct {
	sliding_interest
	Ranges  [][]int `json:"ranges"`
	Filters struct {
		Direct  *bool    `json:"is_dm"`
		Invite  *bool    `json:"is_invite"`
		Spaces  []string `json:"spaces"`
		Types   []any    `json:"room_types"`
		Exclude []any    `json:"not_room_types"`
	} `json:"filters"`
}

// sliding_extensions are the extensions a client asks for.
type sliding_extensions struct {
	Device *struct {
		Enabled *bool  `json:"enabled"`
		Since   string `json:"since"`
	} `json:"to_device"`
	Keys *struct {
		Enabled *bool `json:"enabled"`
	} `json:"e2ee"`
	Data *struct {
		Enabled *bool `json:"enabled"`
	} `json:"account_data"`
	Receipts *struct {
		Enabled *bool `json:"enabled"`
	} `json:"receipts"`
	Typing *struct {
		Enabled *bool `json:"enabled"`
	} `json:"typing"`
}

// A sliding_sent is what a connection was last given of a room: the
// position its timeline was given to, the state asked for then, and its
// counts.
type sliding_sent struct {
	position int64
	state    string
	counts   string
}

// A sliding_connection is one of a device's sliding sync connections: its
// position, the rooms it was given as of that position and the one before -
// a client may send the previous position again - and the positions its
// extensions got to, with the last request, which lists and subscriptions
// carry over from.
type sliding_connection struct {
	lock     sync.Mutex
	pos      int64
	rooms    map[string]sliding_sent
	previous map[string]sliding_sent
	marks    sync_position
	before   sync_position
	request  sliding_request
	used     time.Time
}

var sliding_connections = map[string]*sliding_connection{}
var sliding_lock sync.Mutex

// sliding_connection_of finds or makes a device's connection.
func sliding_connection_of(s *clients_session, id string) *sliding_connection {
	key := s.token.Hash + "|" + id
	sliding_lock.Lock()
	defer sliding_lock.Unlock()
	for k, c := range sliding_connections {
		if time.Since(c.used) > 30*time.Minute {
			delete(sliding_connections, k)
		}
	}
	c := sliding_connections[key]
	if c == nil {
		c = &sliding_connection{rooms: map[string]sliding_sent{}}
		sliding_connections[key] = c
	}
	c.used = time.Now()
	return c
}

// sliding_enabled reads an extension's switch.
func sliding_enabled(enabled *bool, held bool) bool {
	if enabled == nil {
		return held
	}
	return *enabled
}

// sliding_sync answers a sliding sync: the lists' counts, the rooms in their
// windows and those subscribed to - whole where the connection has not been
// given them, else what is new - and the extensions, waiting for something
// where nothing is new.
func sliding_sync(c *gin.Context, s *clients_session) {
	var body sliding_request
	if !clients_body(c, &body) {
		return
	}
	conn := sliding_connection_of(s, body.Connection)
	conn.lock.Lock()
	defer conn.lock.Unlock()
	pos := c.Query("pos")
	switch {
	case pos == "":
		conn.rooms, conn.previous = map[string]sliding_sent{}, nil
		conn.marks, conn.before = sync_position{}, sync_position{}
	case pos == strconv.FormatInt(conn.pos, 10):
	case conn.previous != nil && pos == strconv.FormatInt(conn.pos-1, 10):
		// The answer to the previous request was lost: answer it again.
		conn.rooms, conn.marks = conn.previous, conn.before
		conn.pos--
	default:
		clients_error(c, http.StatusBadRequest, "M_UNKNOWN_POS", "Unknown position")
		return
	}
	// Lists, subscriptions and extensions carry over from the last request
	// where a request leaves them out.
	if body.Lists == nil {
		body.Lists = conn.request.Lists
	}
	if body.Rooms == nil {
		body.Rooms = conn.request.Rooms
	}
	if body.Extensions.Device == nil {
		body.Extensions.Device = conn.request.Extensions.Device
	}
	if body.Extensions.Keys == nil {
		body.Extensions.Keys = conn.request.Extensions.Keys
	}
	if body.Extensions.Data == nil {
		body.Extensions.Data = conn.request.Extensions.Data
	}
	if body.Extensions.Receipts == nil {
		body.Extensions.Receipts = conn.request.Extensions.Receipts
	}
	if body.Extensions.Typing == nil {
		body.Extensions.Typing = conn.request.Extensions.Typing
	}
	conn.request = body
	deadline := time.Now().Add(sync_timeout(c))
	for {
		waiter := clients_waiter()
		answer, rooms, marks, empty := sliding_build(s, conn, body, pos == "")
		if !empty || pos == "" || !sync_wait(c.Request.Context(), waiter, deadline) {
			if c.Request.Context().Err() != nil {
				return
			}
			conn.previous, conn.before = conn.rooms, conn.marks
			conn.rooms, conn.marks = rooms, marks
			conn.pos++
			answer["pos"] = strconv.FormatInt(conn.pos, 10)
			c.JSON(http.StatusOK, answer)
			return
		}
	}
}

// sliding_bump is a room's latest activity: the position of its latest
// event that moves a room up a list.
func sliding_bump(room string) int64 {
	row, _ := federation_database().row("select max(s.position) as position from stream s join events e on e.id=s.event where s.room=? and e.type in ('m.room.message','m.room.encrypted','m.sticker','m.call.invite','m.poll.start','m.room.create','m.room.member')", room)
	return row_int(row, "position")
}

// sliding_build is a sliding sync's answer, the rooms it gives as the
// connection then holds them and its extensions' positions, and whether it
// holds nothing new.
func sliding_build(s *clients_session, conn *sliding_connection, body sliding_request, first bool) (map[string]any, map[string]sliding_sent, sync_position, bool) {
	address := s.h.address
	memberships := clients_memberships(address)
	type candidate struct {
		room   string
		bump   int64
		invite bool
	}
	var candidates []candidate
	direct := map[string]bool{}
	if content := clients_setting(address, "", "m.direct"); content != nil {
		var rooms map[string][]string
		json.Unmarshal(content, &rooms)
		for _, ids := range rooms {
			for _, id := range ids {
				direct[id] = true
			}
		}
	}
	for room, membership := range memberships {
		switch membership {
		case spec.Join:
			candidates = append(candidates, candidate{room, sliding_bump(room), false})
		case spec.Invite, "knock":
			bump := int64(math.MaxInt32)
			if rooms_version(room) != nil && rooms_local(room) != "" {
				state, _ := rooms_current(room)
				bump = clients_position(state[rooms_key(spec.MRoomMember, address)])
			}
			candidates = append(candidates, candidate{room, bump, true})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].bump != candidates[j].bump {
			return candidates[i].bump > candidates[j].bump
		}
		return candidates[i].room < candidates[j].room
	})
	// What each room is wanted for: the lists whose window holds it and the
	// subscriptions to it.
	wanted := map[string]sliding_interest{}
	merge := func(room string, interest sliding_interest) {
		held := wanted[room]
		held.State = append(held.State, interest.State...)
		held.Timeline = max(held.Timeline, interest.Timeline)
		wanted[room] = held
	}
	lists := map[string]any{}
	names := make([]string, 0, len(body.Lists))
	for name := range body.Lists {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		list := body.Lists[name]
		var members []candidate
		for _, cand := range candidates {
			if list.Filters.Invite != nil && *list.Filters.Invite != cand.invite {
				continue
			}
			if list.Filters.Direct != nil && *list.Filters.Direct != (direct[cand.room] || rooms_direct(address, cand.room)) {
				continue
			}
			kind, _ := clients_state_content(cand.room, spec.MRoomCreate, "")["type"].(string)
			if list.Filters.Types != nil && !sliding_kind_in(kind, list.Filters.Types) {
				continue
			}
			if sliding_kind_in(kind, list.Filters.Exclude) {
				continue
			}
			members = append(members, cand)
		}
		lists[name] = map[string]any{"count": len(members)}
		ranges := list.Ranges
		if len(ranges) == 0 {
			ranges = [][]int{{0, 19}}
		}
		for _, window := range ranges {
			if len(window) != 2 {
				continue
			}
			for i := max(window[0], 0); i <= window[1] && i < len(members); i++ {
				merge(members[i].room, list.sliding_interest)
			}
		}
	}
	for room, interest := range body.Rooms {
		if membership := memberships[room]; membership == spec.Join || membership == spec.Invite || membership == "knock" {
			merge(room, interest)
		}
	}
	marks := conn.marks
	rooms := map[string]sliding_sent{}
	for room, sent := range conn.rooms {
		rooms[room] = sent
	}
	answered := map[string]any{}
	for room, interest := range wanted {
		entry, sent := sliding_room(s, room, memberships[room], interest, conn.rooms[room], direct[room] || rooms_direct(address, room))
		if entry != nil {
			answered[room] = entry
			rooms[room] = sent
		}
	}
	// A room the connection was given and the user has since left is given
	// its leaving, once.
	for room, sent := range conn.rooms {
		if _, given := wanted[room]; given {
			continue
		}
		switch memberships[room] {
		case spec.Join, spec.Invite, "knock":
			continue
		}
		if entry, _ := sliding_room(s, room, spec.Leave, sliding_interest{Timeline: 1}, sent, false); entry != nil {
			answered[room] = entry
		}
		delete(rooms, room)
	}
	extensions, marks, quiet := sliding_extend(s, body.Extensions, conn.marks, answered, first)
	answer := map[string]any{"lists": lists, "rooms": answered, "extensions": extensions}
	return answer, rooms, marks, len(answered) == 0 && quiet
}

// sliding_kind_in says whether a room's kind is among a filter's, where an
// absent kind is null.
func sliding_kind_in(kind string, kinds []any) bool {
	for _, k := range kinds {
		if (k == nil && kind == "") || k == kind {
			return true
		}
	}
	return false
}

// sliding_room is a room's part of a sliding sync, and what the connection
// then holds of it; nil where nothing in it is new. A room the connection
// has not been given, or is now asked more state of, is given whole.
func sliding_room(s *clients_session, room string, membership string, interest sliding_interest, held sliding_sent, direct bool) (map[string]any, sliding_sent) {
	address := s.h.address
	fingerprint := json_encode(interest.State)
	limit := min(max(interest.Timeline, 0), clients_history_maximum)
	notify, highlight := notifications_counts(address, room)
	counts := fmt.Sprintf("%d_%d", notify, highlight)
	whole := held.position == 0 || held.state != fingerprint || held.counts == spec.Invite || held.counts == "knock"
	// An invitation or a knock shows the room's stripped state, as
	// invite_state for both.
	if membership == spec.Invite || membership == "knock" {
		if !whole && held.counts == membership {
			return nil, held
		}
		var events any
		if membership == spec.Invite {
			invited := sync_invited(s, room, sync_position{}, sync_position{}, true)
			if invited == nil {
				return nil, held
			}
			events = invited["invite_state"].(map[string]any)["events"]
		} else {
			events = sync_knocked(s, room, sync_position{}, true)
		}
		return map[string]any{"initial": true, "invite_state": events, "is_dm": direct, "bump_stamp": 0}, sliding_sent{math.MaxInt32, fingerprint, membership}
	}
	floor, ceiling := clients_window(room, address)
	latest := min(ceiling, sync_maximum())
	var timeline []clients_entry
	limited := false
	if whole {
		timeline = clients_timeline(room, floor-1, latest+1, limit+1, true)
	} else {
		timeline = clients_timeline(room, max(held.position, floor-1), latest+1, max(limit, 1)+1, true)
	}
	if len(timeline) > max(limit, 0) && (whole || len(timeline) > limit) {
		limited = true
		timeline = timeline[:limit]
	}
	slices.Reverse(timeline)
	if !whole && len(timeline) == 0 && held.counts == counts {
		return nil, held
	}
	var events []gomatrixserverlib.PDU
	for _, entry := range timeline {
		events = append(events, entry.event)
	}
	current, _ := rooms_current(room)
	var state []gomatrixserverlib.PDU
	if whole {
		state = sliding_state(room, current, interest.State, events, address)
	} else {
		for _, event := range events {
			if key := event.StateKey(); key != nil && sliding_wants(interest.State, event.Type(), *key, events, address) {
				state = append(state, event)
			}
		}
	}
	entry := map[string]any{
		"initial":            whole,
		"required_state":     clients_events(state, address, s.device, false),
		"timeline":           clients_events(events, address, s.device, false),
		"limited":            limited,
		"num_live":           0,
		"notification_count": notify,
		"highlight_count":    highlight,
		"bump_stamp":         sliding_bump(room),
		"is_dm":              direct,
	}
	if !whole {
		entry["num_live"] = len(events)
	}
	if len(timeline) > 0 {
		entry["prev_batch"] = "p" + strconv.FormatInt(timeline[0].position, 10)
	}
	summary := sync_summary(room, address)
	entry["joined_count"], entry["invited_count"] = summary["m.joined_member_count"], summary["m.invited_member_count"]
	if name, _ := clients_state_content(room, "m.room.name", "")["name"].(string); name != "" {
		entry["name"] = name
	} else {
		var heroes []map[string]any
		names := rooms_names(room, current)
		for _, hero := range summary["m.heroes"].([]string) {
			h := map[string]any{"user_id": hero}
			if display := names[room+" "+hero]; display != "" {
				h["displayname"] = display
			}
			heroes = append(heroes, h)
		}
		if heroes != nil {
			entry["heroes"] = heroes
		}
	}
	if avatar, _ := clients_state_content(room, "m.room.avatar", "")["url"].(string); avatar != "" {
		entry["avatar"] = avatar
	}
	position := held.position
	if len(timeline) > 0 {
		position = max(position, timeline[len(timeline)-1].position)
	}
	if whole {
		position = max(position, latest, 1)
	}
	return entry, sliding_sent{position, fingerprint, counts}
}

// sliding_wants says whether the state a client asks for holds a state
// event: by its type and key, either a wildcard, the members of the
// timeline's senders for $LAZY, the user for $ME.
func sliding_wants(asked [][]string, kind string, key string, timeline []gomatrixserverlib.PDU, user string) bool {
	for _, pair := range asked {
		if len(pair) != 2 {
			continue
		}
		if pair[0] != "*" && pair[0] != kind {
			continue
		}
		switch pair[1] {
		case "*", key:
			return true
		case "$ME":
			if key == user {
				return true
			}
		case "$LAZY":
			if key == user {
				return true
			}
			for _, event := range timeline {
				if string(event.SenderID()) == key {
					return true
				}
			}
		}
	}
	return false
}

// sliding_state is the state a client asks for of a room.
func sliding_state(room string, current rooms_state, asked [][]string, timeline []gomatrixserverlib.PDU, user string) []gomatrixserverlib.PDU {
	var ids []string
	for key, id := range current {
		kind, state, _ := strings.Cut(key, "\x00")
		if sliding_wants(asked, kind, state, timeline, user) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	loaded := rooms_events(ids)
	out := make([]gomatrixserverlib.PDU, 0, len(ids))
	for _, id := range ids {
		if event := loaded[id]; event != nil {
			out = append(out, event)
		}
	}
	return out
}

// sliding_extend is a sliding sync's extensions - what was sent to the
// device, its keys' news, the account's data, receipts and typing - with
// their positions after, and whether they hold nothing new.
func sliding_extend(s *clients_session, asked sliding_extensions, marks sync_position, rooms map[string]any, first bool) (map[string]any, sync_position, bool) {
	address := s.h.address
	extensions := map[string]any{}
	quiet := true
	latest := sync_latest()
	if asked.Device != nil && sliding_enabled(asked.Device.Enabled, false) {
		since, _ := strconv.ParseInt(asked.Device.Since, 10, 64)
		events, delivered := keyring_deliveries(address, s.device, since)
		extensions["to_device"] = map[string]any{"next_batch": strconv.FormatInt(delivered, 10), "events": events}
		quiet = quiet && len(events) == 0
	}
	if asked.Keys != nil && sliding_enabled(asked.Keys.Enabled, false) {
		changed, changes := keyring_changes(address, marks.changes, first)
		marks.changes = changes
		extensions["e2ee"] = map[string]any{
			"device_lists":                     map[string]any{"changed": changed, "left": []string{}},
			"device_one_time_keys_count":       keyring_counts(address, s.device),
			"device_unused_fallback_key_types": keyring_unused(address, s.device),
		}
		quiet = quiet && len(changed) == 0
	}
	if asked.Data != nil && sliding_enabled(asked.Data.Enabled, false) {
		global := sync_data(address, "", marks.settings, first)
		if first || marks.rules < latest.rules {
			global = append(global, map[string]any{"type": "m.push_rules", "content": rules_ruleset(address)})
		}
		perroom := map[string]any{}
		for room, entry := range rooms {
			whole, _ := entry.(map[string]any)["initial"].(bool)
			if data := sync_data(address, room, marks.settings, whole); len(data) > 0 {
				perroom[room] = data
			}
		}
		extensions["account_data"] = map[string]any{"global": global, "rooms": perroom}
		quiet = quiet && len(global) == 0 && len(perroom) == 0
	}
	if asked.Receipts != nil && sliding_enabled(asked.Receipts.Enabled, false) {
		perroom := map[string]any{}
		for room, entry := range rooms {
			since := marks.receipts
			if whole, _ := entry.(map[string]any)["initial"].(bool); whole {
				since = 0
			}
			if content := receipts_content(room, address, since); len(content) > 0 {
				perroom[room] = map[string]any{"type": "m.receipt", "content": content}
			}
		}
		for _, room := range signals_rooms(address) {
			if _, given := rooms[room]; given {
				continue
			}
			if content := receipts_content(room, address, marks.receipts); len(content) > 0 {
				perroom[room] = map[string]any{"type": "m.receipt", "content": content}
			}
		}
		extensions["receipts"] = map[string]any{"rooms": perroom}
		quiet = quiet && len(perroom) == 0
	}
	if asked.Typing != nil && sliding_enabled(asked.Typing.Enabled, false) {
		perroom := map[string]any{}
		for _, room := range signals_rooms(address) {
			_, given := rooms[room]
			if typing_changed(room, marks.typing) || (given && first) {
				perroom[room] = map[string]any{"type": "m.typing", "content": map[string]any{"user_ids": typing_users(room)}}
			}
		}
		extensions["typing"] = map[string]any{"rooms": perroom}
		quiet = quiet && len(perroom) == 0
	}
	marks.settings, marks.receipts, marks.typing, marks.rules = latest.settings, latest.receipts, latest.typing, latest.rules
	return extensions, marks, quiet
}
