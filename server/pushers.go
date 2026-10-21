// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix clients' notifications
//
// Push rules, pushers and the notifications they make (the federation plan,
// phase 28). Each event a room takes is weighed, for each account here in
// the room but the sender, against the account's push rules - the
// specification's defaults, the user's changes to them and the user's own
// rules - and one that notifies is kept, counted unread until a receipt
// passes it, and sent to the account's pushers: each a push gateway a
// client registered, which wakes the client's phone.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// rules_kinds are the kinds of push rule, in the order they are weighed.
var rules_kinds = []string{"override", "content", "room", "sender", "underride"}

// A rules_rule is one push rule.
type rules_rule struct {
	ID         string           `json:"rule_id"`
	Default    bool             `json:"default"`
	Enabled    bool             `json:"enabled"`
	Conditions []map[string]any `json:"conditions,omitempty"`
	Pattern    string           `json:"pattern,omitempty"`
	Actions    []any            `json:"actions"`
}

// rules_notify and the tweaks are the actions the default rules use.
var (
	rules_sound     = map[string]any{"set_tweak": "sound", "value": "default"}
	rules_highlight = map[string]any{"set_tweak": "highlight"}
	rules_low       = map[string]any{"set_tweak": "highlight", "value": false}
)

// rules_defaults are the specification's default rules for a user, by kind.
func rules_defaults(user string) map[string][]rules_rule {
	local := strings.TrimPrefix(strings.SplitN(user, ":", 2)[0], "@")
	match := func(key string, pattern string) map[string]any {
		return map[string]any{"kind": "event_match", "key": key, "pattern": pattern}
	}
	is := func(key string, value any) map[string]any {
		return map[string]any{"kind": "event_property_is", "key": key, "value": value}
	}
	permitted := map[string]any{"kind": "sender_notification_permission", "key": "room"}
	return map[string][]rules_rule{
		"override": {
			{ID: ".m.rule.master", Default: true, Enabled: false, Conditions: []map[string]any{}, Actions: []any{}},
			{ID: ".m.rule.suppress_notices", Default: true, Enabled: true, Conditions: []map[string]any{match("content.msgtype", "m.notice")}, Actions: []any{}},
			{ID: ".m.rule.invite_for_me", Default: true, Enabled: true, Conditions: []map[string]any{match("type", spec.MRoomMember), match("content.membership", spec.Invite), match("state_key", user)}, Actions: []any{"notify", rules_sound}},
			{ID: ".m.rule.member_event", Default: true, Enabled: true, Conditions: []map[string]any{match("type", spec.MRoomMember)}, Actions: []any{}},
			{ID: ".m.rule.is_user_mention", Default: true, Enabled: true, Conditions: []map[string]any{{"kind": "event_property_contains", "key": `content.m\.mentions.user_ids`, "value": user}}, Actions: []any{"notify", rules_sound, rules_highlight}},
			{ID: ".m.rule.contains_display_name", Default: true, Enabled: true, Conditions: []map[string]any{{"kind": "contains_display_name"}}, Actions: []any{"notify", rules_sound, rules_highlight}},
			{ID: ".m.rule.is_room_mention", Default: true, Enabled: true, Conditions: []map[string]any{is(`content.m\.mentions.room`, true), permitted}, Actions: []any{"notify", rules_highlight}},
			{ID: ".m.rule.roomnotif", Default: true, Enabled: true, Conditions: []map[string]any{permitted, match("content.body", "@room")}, Actions: []any{"notify", rules_highlight}},
			{ID: ".m.rule.tombstone", Default: true, Enabled: true, Conditions: []map[string]any{match("type", "m.room.tombstone"), match("state_key", "")}, Actions: []any{"notify", rules_highlight}},
			{ID: ".m.rule.reaction", Default: true, Enabled: true, Conditions: []map[string]any{match("type", "m.reaction")}, Actions: []any{}},
			{ID: ".m.rule.room.server_acl", Default: true, Enabled: true, Conditions: []map[string]any{match("type", "m.room.server_acl"), match("state_key", "")}, Actions: []any{}},
			{ID: ".m.rule.suppress_edits", Default: true, Enabled: true, Conditions: []map[string]any{is(`content.m\.relates_to.rel_type`, "m.replace")}, Actions: []any{}},
		},
		"content": {
			{ID: ".m.rule.contains_user_name", Default: true, Enabled: true, Pattern: local, Actions: []any{"notify", rules_sound, rules_highlight}},
		},
		"room":   {},
		"sender": {},
		"underride": {
			{ID: ".m.rule.call", Default: true, Enabled: true, Conditions: []map[string]any{match("type", "m.call.invite")}, Actions: []any{"notify", map[string]any{"set_tweak": "sound", "value": "ring"}, rules_low}},
			{ID: ".m.rule.encrypted_room_one_to_one", Default: true, Enabled: true, Conditions: []map[string]any{{"kind": "room_member_count", "is": "2"}, match("type", "m.room.encrypted")}, Actions: []any{"notify", rules_sound, rules_low}},
			{ID: ".m.rule.room_one_to_one", Default: true, Enabled: true, Conditions: []map[string]any{{"kind": "room_member_count", "is": "2"}, match("type", "m.room.message")}, Actions: []any{"notify", rules_sound, rules_low}},
			{ID: ".m.rule.message", Default: true, Enabled: true, Conditions: []map[string]any{match("type", "m.room.message")}, Actions: []any{"notify", rules_low}},
			{ID: ".m.rule.encrypted", Default: true, Enabled: true, Conditions: []map[string]any{match("type", "m.room.encrypted")}, Actions: []any{"notify", rules_low}},
		},
	}
}

// rules_rules are a user's rules by kind, in order: the user's own rules of
// a kind come before its default ones, except underride, after the
// override defaults; the defaults with the user's changes to them.
func rules_rules(user string) map[string][]rules_rule {
	defaults := rules_defaults(user)
	rows, _ := federation_database().rows("select kind, id, priority, body, enabled from rules where user=? order by kind, priority desc, position", user)
	changed := map[string]map[string]any{}
	own := map[string][]rules_rule{}
	for _, row := range rows {
		kind, id := row_string(row, "kind"), row_string(row, "id")
		var body map[string]any
		json.Unmarshal([]byte(row_string(row, "body")), &body)
		if strings.HasPrefix(id, ".") {
			body["enabled"] = row_int(row, "enabled") != 0
			changed[kind+"\x00"+id] = body
			continue
		}
		var rule rules_rule
		json.Unmarshal([]byte(row_string(row, "body")), &rule)
		rule.ID, rule.Default, rule.Enabled = id, false, row_int(row, "enabled") != 0
		own[kind] = append(own[kind], rule)
	}
	out := map[string][]rules_rule{}
	for _, kind := range rules_kinds {
		var held []rules_rule
		for _, rule := range defaults[kind] {
			if change := changed[kind+"\x00"+rule.ID]; change != nil {
				if enabled, ok := change["enabled"].(bool); ok {
					rule.Enabled = enabled
				}
				if actions, ok := change["actions"].([]any); ok {
					rule.Actions = actions
				}
			}
			held = append(held, rule)
		}
		if kind == "override" && len(held) > 0 {
			// The master rule stays first.
			out[kind] = append(append([]rules_rule{held[0]}, own[kind]...), held[1:]...)
		} else {
			out[kind] = append(own[kind], held...)
		}
		if out[kind] == nil {
			out[kind] = []rules_rule{}
		}
	}
	return out
}

// rules_ruleset is a user's rules as clients read them.
func rules_ruleset(user string) map[string]any {
	global := map[string]any{}
	for kind, rules := range rules_rules(user) {
		global[kind] = rules
	}
	return map[string]any{"global": global}
}

// === Weighing an event ===

// rules_flatten is an event's fields by their dotted keys, a dot in a key
// escaped, as push rules name them.
func rules_flatten(prefix string, value any, out map[string]any) {
	switch v := value.(type) {
	case map[string]any:
		for key, inner := range v {
			escaped := strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), ".", `\.`)
			name := escaped
			if prefix != "" {
				name = prefix + "." + escaped
			}
			rules_flatten(name, inner, out)
		}
	default:
		out[prefix] = v
	}
}

// rules_glob matches a glob pattern to a value, case-insensitively and
// whole.
func rules_glob(pattern string, value string) bool {
	expression := "(?is)^" + strings.NewReplacer(`\*`, ".*", `\?`, ".").Replace(regexp.QuoteMeta(pattern)) + "$"
	matched, _ := regexp.MatchString(expression, value)
	return matched
}

// rules_word says whether a value holds a pattern as a word.
func rules_word(pattern string, value string) bool {
	expression := `(?is)(^|\W)` + strings.NewReplacer(`\*`, `\S*`, `\?`, `\S`).Replace(regexp.QuoteMeta(pattern)) + `(\W|$)`
	matched, _ := regexp.MatchString(expression, value)
	return matched
}

// A rules_context is what an event is weighed in: its fields, the user's
// name in the room, the room's members and its power levels.
type rules_context struct {
	fields  map[string]any
	name    string
	members int
	powers  map[string]any
	sender  string
}

// matches says whether a rule matches an event.
func (rc *rules_context) matches(kind string, rule rules_rule) bool {
	switch kind {
	case "content":
		body, ok := rc.fields["content.body"].(string)
		return ok && rule.Pattern != "" && rules_word(rule.Pattern, body)
	case "room":
		return rc.fields["room_id"] == rule.ID
	case "sender":
		return rc.sender == rule.ID
	}
	for _, condition := range rule.Conditions {
		if !rc.condition(condition) {
			return false
		}
	}
	return true
}

// condition says whether an event meets one condition of a rule.
func (rc *rules_context) condition(condition map[string]any) bool {
	key, _ := condition["key"].(string)
	switch condition["kind"] {
	case "event_match":
		pattern, _ := condition["pattern"].(string)
		value, ok := rc.fields[key].(string)
		if !ok {
			return false
		}
		if key == "content.body" {
			return rules_word(pattern, value)
		}
		return rules_glob(pattern, value)
	case "event_property_is":
		value, found := rc.fields[key]
		return found && rules_same(value, condition["value"])
	case "event_property_contains":
		list, ok := rc.fields[key].([]any)
		if !ok {
			return false
		}
		for _, item := range list {
			if rules_same(item, condition["value"]) {
				return true
			}
		}
		return false
	case "contains_display_name":
		body, ok := rc.fields["content.body"].(string)
		return ok && rc.name != "" && rules_word(rc.name, body)
	case "room_member_count":
		is, _ := condition["is"].(string)
		return rules_count(is, rc.members)
	case "sender_notification_permission":
		need := 50.0
		if notifications, ok := rc.powers["notifications"].(map[string]any); ok {
			if level, ok := notifications[key].(float64); ok {
				need = level
			}
		}
		have := 0.0
		if users, ok := rc.powers["users"].(map[string]any); ok {
			if level, ok := users[rc.sender].(float64); ok {
				have = level
			} else if level, ok := rc.powers["users_default"].(float64); ok {
				have = level
			}
		}
		return have >= need
	}
	return false
}

// rules_same compares two JSON values, numbers by value.
func rules_same(a any, b any) bool {
	return json_encode(a) == json_encode(b)
}

// rules_count reads a member count condition: a number, optionally after
// ==, <, >, <= or >=.
func rules_count(is string, count int) bool {
	for _, op := range []string{"==", "<=", ">=", "<", ">"} {
		if rest, ok := strings.CutPrefix(is, op); ok {
			n, err := strconv.Atoi(rest)
			if err != nil {
				return false
			}
			switch op {
			case "==":
				return count == n
			case "<=":
				return count <= n
			case ">=":
				return count >= n
			case "<":
				return count < n
			case ">":
				return count > n
			}
		}
	}
	n, err := strconv.Atoi(is)
	return err == nil && count == n
}

// rules_weigh is what an event's first matching rule does for a user, or
// nil where none matches or it does nothing.
func rules_weigh(user string, event gomatrixserverlib.PDU, state rooms_state) []any {
	var raw map[string]any
	json.Unmarshal(event.JSON(), &raw)
	fields := map[string]any{}
	rules_flatten("", raw, fields)
	fields["room_id"] = event.RoomID().String()
	members := 0
	for _, membership := range rooms_members(state) {
		if membership == spec.Join {
			members++
		}
	}
	rc := &rules_context{fields: fields, name: rooms_names(event.RoomID().String(), state)[event.RoomID().String()+" "+user], members: members, sender: string(event.SenderID())}
	if id := state[rooms_key(spec.MRoomPowerLevels, "")]; id != "" {
		if levels := rooms_event(id); levels != nil {
			json.Unmarshal(levels.Content(), &rc.powers)
		}
	}
	rules := rules_rules(user)
	for _, kind := range rules_kinds {
		for _, rule := range rules[kind] {
			if rule.Enabled && rc.matches(kind, rule) {
				return rule.Actions
			}
		}
	}
	return nil
}

// rules_notifies says whether actions notify, and whether they highlight.
func rules_notifies(actions []any) (bool, bool) {
	notify, highlight := false, false
	for _, action := range actions {
		switch a := action.(type) {
		case string:
			if a == "notify" {
				notify = true
			}
		case map[string]any:
			if a["set_tweak"] == "highlight" {
				value, set := a["value"]
				highlight = !set || value == true
			}
		}
	}
	return notify, notify && highlight
}

// === Notifications ===

// notifications_tell weighs an event a room took for each account here in it
// but its sender, keeping a notification where it notifies and sending it to
// the account's pushers. Called with the room held, so the pushing waits for
// nothing.
func notifications_tell(event gomatrixserverlib.PDU, state rooms_state) {
	room := event.RoomID().String()
	position := clients_position(event.EventID())
	if position == 0 {
		return
	}
	recipients := map[string]bool{}
	for member, membership := range rooms_members(state) {
		if membership == spec.Join {
			recipients[member] = true
		}
	}
	if event.Type() == spec.MRoomMember && event.StateKey() != nil {
		recipients[*event.StateKey()] = true
	}
	sender := string(event.SenderID())
	// A user's joining counts as their receipt: what came before it, an
	// invitation among it, is read.
	if membership, _ := event.Membership(); event.Type() == spec.MRoomMember && membership == spec.Join && event.StateKey() != nil {
		federation_database().exec("update notifications set read=1 where user=? and room=? and position<=? and read=0", *event.StateKey(), room, position)
	}
	for member := range recipients {
		if member == sender {
			continue
		}
		user, account := federation_user(member)
		if user == nil || !clients_signed(user, account) {
			continue
		}
		actions := rules_weigh(member, event, state)
		notify, highlight := rules_notifies(actions)
		if !notify {
			continue
		}
		federation_database().exec("insert or ignore into notifications (user, room, event, position, actions, highlight, created) values (?, ?, ?, ?, ?, ?, ?)", member, room, event.EventID(), position, json_encode(actions), highlight, now())
		pushers_send(user, member, event, state, actions)
	}
}

// clients_signed says whether a Matrix client is signed in to an account:
// only then are its notifications kept.
func clients_signed(user *User, account string) bool {
	found, _ := db_open("db/users.db").exists("select 1 from tokens where user=? and account=? and network='matrix' and kind=?", user.UID, account, token_signin)
	return found
}

// notifications_counts is a room's unread notifications for a user, and
// those that highlight.
func notifications_counts(user string, room string) (int64, int64) {
	row, _ := federation_database().row("select count(*) as n, coalesce(sum(highlight), 0) as h from notifications where user=? and room=? and read=0", user, room)
	return row_int(row, "n"), row_int(row, "h")
}

// notifications_read marks a room's notifications for a user read up to a
// position.
func notifications_read(user string, room string, position int64) {
	if position <= 0 {
		return
	}
	db := federation_database()
	db.exec("update notifications set read=1 where user=? and room=? and position<=? and read=0", user, room, position)
	db.exec("delete from notifications where user=? and read=1 and created<?", user, now()-30*86400)
	clients_wake()
}

// === Routing ===

// pushers_dispatch answers push rules, pushers and the notifications list,
// reporting whether the request was one.
func pushers_dispatch(c *gin.Context, s *clients_session, parts []string) bool {
	method := c.Request.Method
	route := method + " " + strings.Join(parts, "/")
	switch {
	case route == "GET pushers":
		pushers_list(c, s)
	case route == "POST pushers/set":
		pushers_set(c, s)
	case route == "GET notifications":
		notifications_list(c, s)
	case len(parts) >= 1 && parts[0] == "pushrules":
		rules_route(c, s, parts[1:])
	default:
		return false
	}
	return true
}

// rules_route answers the push rules: all of them, a scope's, a kind's, or a
// rule, its switch and its actions; and adds, changes and removes them.
func rules_route(c *gin.Context, s *clients_session, parts []string) {
	// A trailing slash leaves an empty part.
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	rules := rules_rules(s.h.address)
	method := c.Request.Method
	if len(parts) == 0 && method == "GET" {
		c.JSON(http.StatusOK, rules_ruleset(s.h.address))
		return
	}
	if parts[0] != "global" {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Push rules are global")
		return
	}
	if len(parts) == 1 && method == "GET" {
		c.JSON(http.StatusOK, rules_ruleset(s.h.address)["global"])
		return
	}
	kind := parts[1]
	if !slices.Contains(rules_kinds, kind) {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Unknown kind of rule")
		return
	}
	if len(parts) == 2 && method == "GET" {
		c.JSON(http.StatusOK, rules[kind])
		return
	}
	if len(parts) < 3 {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Name a rule")
		return
	}
	id := parts[2]
	var found *rules_rule
	for i := range rules[kind] {
		if rules[kind][i].ID == id {
			found = &rules[kind][i]
		}
	}
	db := federation_database()
	attribute := ""
	if len(parts) == 4 {
		attribute = parts[3]
	}
	switch {
	case method == "GET" && attribute == "":
		if found == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such rule")
			return
		}
		c.JSON(http.StatusOK, found)
	case method == "GET" && attribute == "enabled":
		if found == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such rule")
			return
		}
		c.JSON(http.StatusOK, map[string]any{"enabled": found.Enabled})
	case method == "GET" && attribute == "actions":
		if found == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such rule")
			return
		}
		c.JSON(http.StatusOK, map[string]any{"actions": found.Actions})
	case method == "PUT" && (attribute == "enabled" || attribute == "actions"):
		if found == nil {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such rule")
			return
		}
		var body map[string]any
		if !clients_body(c, &body) {
			return
		}
		row, _ := db.row("select body, enabled from rules where user=? and kind=? and id=?", s.h.address, kind, id)
		held := map[string]any{}
		enabled := found.Enabled
		if row != nil {
			json.Unmarshal([]byte(row_string(row, "body")), &held)
		} else if !found.Default {
			held = map[string]any{"conditions": found.Conditions, "pattern": found.Pattern, "actions": found.Actions}
		}
		if attribute == "enabled" {
			value, ok := body["enabled"].(bool)
			if !ok {
				clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "enabled must be true or false")
				return
			}
			enabled = value
		} else {
			actions, ok := body["actions"].([]any)
			if !ok {
				clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "actions must be a list")
				return
			}
			held["actions"] = actions
		}
		rules_keep(s.h.address, kind, id, 0, held, enabled)
		c.JSON(http.StatusOK, map[string]any{})
	case method == "PUT" && attribute == "":
		if strings.HasPrefix(id, ".") {
			clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "A rule's id may not begin with a dot")
			return
		}
		var body struct {
			Actions    []any            `json:"actions"`
			Conditions []map[string]any `json:"conditions"`
			Pattern    string           `json:"pattern"`
		}
		if !clients_body(c, &body) {
			return
		}
		if body.Actions == nil || (kind == "content" && body.Pattern == "") || ((kind == "override" || kind == "underride") && body.Conditions == nil) {
			clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "The rule is not complete")
			return
		}
		priority := rules_priority(rules[kind], c.Query("before"), c.Query("after"))
		rules_keep(s.h.address, kind, id, priority, map[string]any{"actions": body.Actions, "conditions": body.Conditions, "pattern": body.Pattern}, true)
		c.JSON(http.StatusOK, map[string]any{})
	case method == "DELETE" && attribute == "":
		if found == nil || found.Default {
			clients_error(c, http.StatusNotFound, "M_NOT_FOUND", "No such rule of the user's")
			return
		}
		db.exec("delete from rules where user=? and kind=? and id=?", s.h.address, kind, id)
		db.exec("insert into rules (user, kind, id, body, enabled) values (?, '', ?, '{}', 0)", s.h.address, "\x00removed")
		db.exec("delete from rules where user=? and kind='' and id=?", s.h.address, "\x00removed")
		clients_wake()
		c.JSON(http.StatusOK, map[string]any{})
	default:
		clients_error(c, http.StatusNotFound, "M_UNRECOGNIZED", "Unrecognised request")
	}
}

// rules_priority is where a new rule goes among the user's own of its kind:
// before or after one named, else first.
func rules_priority(rules []rules_rule, before string, after string) int64 {
	db := federation_database()
	priority := func(id string) (int64, bool) {
		for _, rule := range rules {
			if rule.ID == id && !rule.Default {
				row, _ := db.row("select priority from rules where id=?", id)
				return row_int(row, "priority"), true
			}
		}
		return 0, false
	}
	if before != "" {
		if p, ok := priority(before); ok {
			return p + 1
		}
	}
	if after != "" {
		if p, ok := priority(after); ok {
			return p - 1
		}
	}
	return time.Now().UnixMilli()
}

// rules_keep keeps a rule of the user's, or their change to a default one, at
// a new position, so their syncs are given their rules again.
func rules_keep(user string, kind string, id string, priority int64, body map[string]any, enabled bool) {
	db := federation_database()
	row, _ := db.row("select priority from rules where user=? and kind=? and id=?", user, kind, id)
	if row != nil && priority == 0 {
		priority = row_int(row, "priority")
	}
	db.exec("delete from rules where user=? and kind=? and id=?", user, kind, id)
	db.exec("insert into rules (user, kind, id, priority, body, enabled) values (?, ?, ?, ?, ?, ?)", user, kind, id, priority, json_encode(body), enabled)
	clients_wake()
}

// === Pushers ===

// pushers_list answers the account's pushers.
func pushers_list(c *gin.Context, s *clients_session) {
	rows, _ := federation_database().rows("select body from pushers where user=? order by created", s.h.address)
	pushers := []any{}
	for _, row := range rows {
		var pusher map[string]any
		if json.Unmarshal([]byte(row_string(row, "body")), &pusher) == nil {
			pushers = append(pushers, pusher)
		}
	}
	c.JSON(http.StatusOK, map[string]any{"pushers": pushers})
}

// pushers_set adds, changes or removes a pusher: a push gateway a client's
// app on a phone is woken through. Unless asked to add beside them, the same
// app and key are taken from any other account.
func pushers_set(c *gin.Context, s *clients_session) {
	var body map[string]any
	if !clients_body(c, &body) {
		return
	}
	key, _ := body["pushkey"].(string)
	app, _ := body["app_id"].(string)
	if key == "" || app == "" || len(key) > 512 || len(app) > 64 {
		clients_error(c, http.StatusBadRequest, "M_MISSING_PARAM", "pushkey and app_id are required")
		return
	}
	db := federation_database()
	if body["kind"] == nil {
		db.exec("delete from pushers where app=? and key=? and user=?", app, key, s.h.address)
		c.JSON(http.StatusOK, map[string]any{})
		return
	}
	kind, _ := body["kind"].(string)
	if kind != "http" {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "Only http pushers are supported")
		return
	}
	data, _ := body["data"].(map[string]any)
	gateway, _ := data["url"].(string)
	if u, err := url.Parse(gateway); err != nil || (u.Scheme != "https" && !(u.Scheme == "http" && network_scheme == "http://")) || u.Host == "" || u.Path != "/_matrix/push/v1/notify" {
		clients_error(c, http.StatusBadRequest, "M_INVALID_PARAM", "The gateway must be an https address ending /_matrix/push/v1/notify")
		return
	}
	if append, _ := body["append"].(bool); !append {
		db.exec("delete from pushers where app=? and key=? and user!=?", app, key, s.h.address)
	}
	delete(body, "append")
	db.exec("insert or replace into pushers (user, app, key, device, kind, body, created) values (?, ?, ?, ?, ?, ?, ?)", s.h.address, app, key, s.device, kind, json_encode(body), now())
	c.JSON(http.StatusOK, map[string]any{})
}

// pushers_send sends a notification to each of the account's pushers, away
// from the room it came in; a gateway that refuses a key has that pusher
// removed.
func pushers_send(user *User, address string, event gomatrixserverlib.PDU, state rooms_state, actions []any) {
	rows, _ := federation_database().rows("select app, key, body from pushers where user=?", address)
	if len(rows) == 0 {
		return
	}
	room := event.RoomID().String()
	unread := int64(0)
	row, _ := federation_database().row("select count(*) as n from notifications where user=? and read=0", address)
	unread = row_int(row, "n")
	var tweaks map[string]any
	for _, action := range actions {
		if a, ok := action.(map[string]any); ok && a["set_tweak"] != nil {
			if tweaks == nil {
				tweaks = map[string]any{}
			}
			name, _ := a["set_tweak"].(string)
			value, set := a["value"]
			if !set {
				value = true
			}
			tweaks[name] = value
		}
	}
	names := rooms_names(room, state)
	roomname, _ := clients_state_content(room, "m.room.name", "")["name"].(string)
	var content any
	json.Unmarshal(event.Content(), &content)
	for _, row := range rows {
		var pusher map[string]any
		json.Unmarshal([]byte(row_string(row, "body")), &pusher)
		data, _ := pusher["data"].(map[string]any)
		gateway, _ := data["url"].(string)
		device := map[string]any{"app_id": row_string(row, "app"), "pushkey": row_string(row, "key"), "pushkey_ts": now()}
		passed := map[string]any{}
		for key, value := range data {
			if key != "url" {
				passed[key] = value
			}
		}
		device["data"] = passed
		if tweaks != nil {
			device["tweaks"] = tweaks
		}
		notification := map[string]any{
			"event_id": event.EventID(),
			"room_id":  room,
			"counts":   map[string]any{"unread": unread},
			"devices":  []any{device},
			"prio":     "high",
		}
		if data["format"] != "event_id_only" {
			notification["type"] = event.Type()
			notification["sender"] = string(event.SenderID())
			if name := names[room+" "+string(event.SenderID())]; name != "" {
				notification["sender_display_name"] = name
			}
			if roomname != "" {
				notification["room_name"] = roomname
			}
			notification["content"] = content
			if key := event.StateKey(); key != nil {
				notification["user_is_target"] = *key == address
			}
		}
		go pushers_post(user, gateway, address, row_string(row, "app"), row_string(row, "key"), map[string]any{"notification": notification})
	}
}

// pushers_post sends one notification to a gateway, as the user's server.
func pushers_post(user *User, gateway string, address string, app string, key string, body any) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "POST", gateway, bytes.NewReader([]byte(json_encode(body))))
	if err != nil {
		return
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := network_request(ctx, user, "matrix", request, 0)
	if err != nil {
		debug("Matrix push to %s failed: %v", gateway, err)
		return
	}
	defer response.Body.Close()
	answer, _ := read_limited(response.Body, 64<<10)
	var refused struct {
		Rejected []string `json:"rejected"`
	}
	if json.Unmarshal(answer, &refused) == nil && slices.Contains(refused.Rejected, key) {
		federation_database().exec("delete from pushers where user=? and app=? and key=?", address, app, key)
	}
}

// notifications_list answers the account's notifications, newest first, a
// page at a time, the highlights alone where asked.
func notifications_list(c *gin.Context, s *clients_session) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	limit = min(max(limit, 1), 100)
	from, err := strconv.ParseInt(c.Query("from"), 10, 64)
	if err != nil || from <= 0 {
		from = 1 << 62
	}
	only := ""
	if c.Query("only") == "highlight" {
		only = " and highlight=1"
	}
	rows, _ := federation_database().rows("select id, room, event, actions, read, created from notifications where user=? and id<?"+only+" order by id desc limit ?", s.h.address, from, limit)
	notifications := []any{}
	last := int64(0)
	for _, row := range rows {
		event := rooms_event(row_string(row, "event"))
		if event == nil {
			continue
		}
		var actions []any
		json.Unmarshal([]byte(row_string(row, "actions")), &actions)
		notifications = append(notifications, map[string]any{
			"actions": actions,
			"event":   clients_event(event, s.h.address, s.device, true),
			"read":    row_int(row, "read") != 0,
			"room_id": row_string(row, "room"),
			"ts":      row_int(row, "created") * 1000,
		})
		last = row_int(row, "id")
	}
	answer := map[string]any{"notifications": notifications}
	if len(rows) == limit {
		answer["next_token"] = fmt.Sprint(last)
	}
	c.JSON(http.StatusOK, answer)
}
