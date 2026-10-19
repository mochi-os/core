// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Mochi's own sessions in XMPP rooms
//
// A hosted XMPP account's rooms, as chat holds them (the federation plan,
// phase 26): Mochi's own sessions join a room as the account's resource
// "mochi", under the account's name, and hand chat what the room says as the
// conversation events a linked account's session does - its messages, who is
// in it and in what role, and the account's own messages coming back with the
// id the room gave them. A room is joined from chat by its address, wherever
// it is hosted, or made on this server's room service for several people; it
// is kept in the account's bookmarks (XEP-0402), so the account's XMPP clients
// join it too, and a room an XMPP client bookmarks to join is joined here as
// well. The rooms are joined again when the server starts, asking for what
// came since, and each is asked every quarter of an hour whether the account
// is still in it (XEP-0410), and joined again where it is not.

package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"mellium.im/xmpp/jid"
)

// How long a room has to send the end of its history after confirming a
// join, and how often a room is asked whether the account is still in it.
var (
	xmpp_history_wait  = 3 * time.Second
	xmpp_presence_ping = 15 * time.Minute
)

// An xmpp_joined is a room Mochi's own sessions on an account are in: the
// nick they go by there, its name, and when its latest message came.
type xmpp_joined struct {
	room string
	nick string
	name string
	seen int64
}

// xmpp_joined_of is a room Mochi's own sessions on an account are in, or nil.
func xmpp_joined_of(a *xmpp_account, room string) *xmpp_joined {
	row, _ := xmpp_database().row("select room, nick, name, seen from joined where account=? and room=?", a.id, strings.ToLower(room))
	if row == nil {
		return nil
	}
	return &xmpp_joined{room: row_string(row, "room"), nick: row_string(row, "nick"), name: row_string(row, "name"), seen: row_int(row, "seen")}
}

// === Asking ===

// The server's own requests waiting for their answers, by id.
var (
	xmpp_asks      = map[string]chan *xmpp_stanza{}
	xmpp_asks_lock sync.Mutex
)

// xmpp_ask sends a request from Mochi's own sessions on an account and waits
// for its answer, failing on an error answer.
func xmpp_ask(a *xmpp_account, to jid.JID, kind string, inner string) (*xmpp_stanza, error) {
	id := "ask-" + uid()
	answer := make(chan *xmpp_stanza, 1)
	xmpp_asks_lock.Lock()
	xmpp_asks[id] = answer
	xmpp_asks_lock.Unlock()
	defer func() {
		xmpp_asks_lock.Lock()
		delete(xmpp_asks, id)
		xmpp_asks_lock.Unlock()
	}()
	xmpp_route(xmpp_make("iq", inner, "type", kind, "id", id), a.mochi(), to)
	select {
	case s := <-answer:
		if s.get("type") == "error" {
			return s, fmt.Errorf("%s answered %s", to, xmpp_condition(s))
		}
		return s, nil
	case <-time.After(xmpp_query_timeout):
		return nil, fmt.Errorf("%s did not answer", to)
	}
}

// xmpp_answered hands an answer to the request of the server's waiting for it.
func xmpp_answered(s *xmpp_stanza) {
	xmpp_asks_lock.Lock()
	answer := xmpp_asks[s.get("id")]
	xmpp_asks_lock.Unlock()
	if answer != nil {
		select {
		case answer <- s:
		default:
		}
	}
}

// xmpp_condition is the condition an error stanza names, or "error".
func xmpp_condition(s *xmpp_stanza) string {
	var e struct {
		Error *struct {
			Conditions []struct {
				XMLName struct{ Space, Local string }
			} `xml:",any"`
		} `xml:"error"`
	}
	if s.decode(&e) == nil && e.Error != nil {
		for _, c := range e.Error.Conditions {
			if c.XMLName.Space == xmpp_ns_stanzas {
				return c.XMLName.Local
			}
		}
	}
	return "error"
}

// === What Mochi sent ===

// The ids Mochi's own sessions sent to rooms, for ten minutes, by which a
// room's echo of a message is told from the account's other clients'.
var (
	xmpp_echoes      = map[string]time.Time{}
	xmpp_echoes_lock sync.Mutex
)

// xmpp_room_sent keeps an id sent to a room.
func xmpp_room_sent(a *xmpp_account, id string) {
	xmpp_echoes_lock.Lock()
	defer xmpp_echoes_lock.Unlock()
	xmpp_echoes[a.id+" "+id] = time.Now()
	for key, at := range xmpp_echoes {
		if time.Since(at) > 10*time.Minute {
			delete(xmpp_echoes, key)
		}
	}
}

// xmpp_room_echo reports whether an id is one Mochi's own sessions sent.
func xmpp_room_echo(a *xmpp_account, id string) bool {
	xmpp_echoes_lock.Lock()
	defer xmpp_echoes_lock.Unlock()
	_, found := xmpp_echoes[a.id+" "+id]
	return found
}

// === Joining ===

// An xmpp_joining is a join waiting for the room: the nick asked for, and
// what the room says until its history ends, kept to be told in one batch;
// once the batch is queued, what the room says goes to the apps after it.
type xmpp_joining struct {
	room    string
	nick    string
	done    chan error
	once    sync.Once
	lock    sync.Mutex
	events  []map[string]any
	entered bool
	closed  bool
}

// The joins waiting, by account and room.
var (
	xmpp_joinings      = map[string]*xmpp_joining{}
	xmpp_joinings_lock sync.Mutex
)

// xmpp_joining_of is the join waiting for a room, or nil.
func xmpp_joining_of(a *xmpp_account, room string) *xmpp_joining {
	xmpp_joinings_lock.Lock()
	defer xmpp_joinings_lock.Unlock()
	return xmpp_joinings[a.id+" "+strings.ToLower(room)]
}

// finish settles a join, once.
func (j *xmpp_joining) finish(err error) {
	j.once.Do(func() { j.done <- err })
}

// add keeps events for the join's batch, or reports the batch gone.
func (j *xmpp_joining) add(events ...map[string]any) bool {
	j.lock.Lock()
	defer j.lock.Unlock()
	if j.closed {
		return false
	}
	j.events = append(j.events, events...)
	return true
}

// xmpp_room_presence is what Mochi's own sessions show a room: the user's
// presence where they share it, else extended away, since an occupant that
// said it was unavailable would have left.
func xmpp_room_presence(a *xmpp_account) string {
	if inner, shown := xmpp_mochi_presence(a); shown {
		return inner
	}
	return `<show>xa</show>`
}

// xmpp_room_enter joins a room under a nick and waits for the room to confirm
// it and send its history - what came since a moment, or its latest messages
// the first time. Once it has, the room is kept, and the apps are told of it,
// of who is in it and of what it sent, in one batch ahead of anything the
// room says after; with wait, the join returns once they have been told.
func xmpp_room_enter(a *xmpp_account, room jid.JID, nick string, name string, since int64, wait bool) error {
	full, err := room.WithResource(nick)
	if err != nil {
		return fmt.Errorf("invalid nick")
	}
	key := a.id + " " + room.String()
	j := &xmpp_joining{room: room.String(), nick: nick, done: make(chan error, 1)}
	xmpp_joinings_lock.Lock()
	xmpp_joinings[key] = j
	xmpp_joinings_lock.Unlock()
	defer func() {
		xmpp_joinings_lock.Lock()
		if xmpp_joinings[key] == j {
			delete(xmpp_joinings, key)
		}
		xmpp_joinings_lock.Unlock()
	}()
	history := `<history maxstanzas="20"/>`
	if since > 0 {
		history = `<history since="` + time.Unix(since, 0).UTC().Format(time.RFC3339) + `"/>`
	}
	xmpp_route(xmpp_make("presence", `<x xmlns="`+xmpp_ns_muc+`">`+history+`</x>`+xmpp_room_presence(a)), a.mochi(), full)
	select {
	case err = <-j.done:
	case <-time.After(xmpp_query_timeout):
		err = fmt.Errorf("the room did not answer")
	}
	j.lock.Lock()
	j.closed = true
	events := j.events
	if err != nil {
		j.lock.Unlock()
		if full, e := room.WithResource(nick); e == nil && !strings.Contains(err.Error(), "refused") {
			xmpp_route(xmpp_make("presence", "", "type", "unavailable"), a.mochi(), full)
		}
		return err
	}
	seen := since
	for _, event := range events {
		if event["kind"] == "message" {
			seen = max(seen, as_int64(event["created"]))
		}
	}
	xmpp_database().exec("insert into joined (account, address, room, nick, name, seen, created) values (?, ?, ?, ?, ?, ?, ?) on conflict(account, room) do update set nick=excluded.nick, name=excluded.name, seen=max(seen, excluded.seen)", a.id, a.address.String(), room.String(), nick, name, seen, now())
	batch := append([]map[string]any{{"kind": "room", "conversation": "xmpp:" + room.String(), "room": true, "name": name}}, events...)
	told := make(chan struct{})
	user, account := a.user, a.id
	xmpp_queue(user, func() {
		linked_inbound(user, account, "xmpp", batch)
		close(told)
	})
	j.lock.Unlock()
	if wait {
		select {
		case <-told:
		case <-time.After(xmpp_query_timeout):
		}
	}
	return nil
}

// xmpp_room_name is a room's name as discovery gives it, else its local
// part, and whether the address is a room at all.
func xmpp_room_name(a *xmpp_account, room jid.JID) (string, bool, error) {
	answer, err := xmpp_ask(a, room, "get", `<query xmlns="`+xmpp_ns_info+`"/>`)
	if err != nil {
		return "", false, err
	}
	var info struct {
		Identities []struct {
			Category string `xml:"category,attr"`
			Name     string `xml:"name,attr"`
		} `xml:"http://jabber.org/protocol/disco#info query>identity"`
	}
	answer.decode(&info)
	for _, i := range info.Identities {
		if i.Category == "conference" {
			name := strings.TrimSpace(i.Name)
			if name == "" {
				name = room.Localpart()
			}
			return name, true, nil
		}
	}
	return "", false, nil
}

// xmpp_room_join joins a room by its address and keeps it, in the account's
// bookmarks too, answering its conversation and name. The apps hear of the
// room, who is in it and its recent past before the join returns. An address
// that is not a room is refused.
func xmpp_room_join(a *xmpp_account, address string) (string, string, error) {
	room, err := xmpp_address(address)
	if err != nil || room.Localpart() == "" {
		return "", "", fmt.Errorf("invalid address")
	}
	room = room.Bare()
	if j := xmpp_joined_of(a, room.String()); j != nil {
		return "xmpp:" + j.room, j.name, nil
	}
	name, conference, err := xmpp_room_name(a, room)
	if err != nil {
		return "", "", fmt.Errorf("the room could not be reached: %v", err)
	}
	if !conference {
		return "", "", fmt.Errorf("not a room")
	}
	nick := a.address.Localpart()
	if err := xmpp_room_enter(a, room, nick, name, 0, true); err != nil {
		return "", "", err
	}
	xmpp_bookmark_keep(a, room.String(), name, nick)
	return "xmpp:" + room.String(), name, nil
}

// xmpp_room_leave leaves a room and stops keeping it, in the bookmarks too.
// The room is no longer kept before it is told, so its answer is not taken
// for the account being put out.
func xmpp_room_leave(a *xmpp_account, room string) {
	j := xmpp_joined_of(a, room)
	if j == nil {
		return
	}
	xmpp_database().exec("delete from joined where account=? and room=?", a.id, j.room)
	xmpp_bookmark_drop(a, j.room)
	if full, err := jid.Parse(j.room + "/" + j.nick); err == nil {
		xmpp_route(xmpp_make("presence", "", "type", "unavailable"), a.mochi(), full)
	}
}

// xmpp_room_create makes a room on the account's domain's room service for
// people: persistent, members only, each person's address shown to the
// others, the account its owner and each person a member, invited by the
// room. Mochi's own sessions join it, and it is bookmarked.
func xmpp_room_create(a *xmpp_account, people []jid.JID, name string) (string, error) {
	service := "rooms." + a.address.Domainpart()
	if xmpp_room_base(service) == "" {
		return "", fmt.Errorf("this server has no room service for %s", a.address.Domainpart())
	}
	room, err := jid.Parse(strings.ToLower(uid()) + "@" + service)
	if err != nil {
		return "", err
	}
	conference_make(room.String(), name, a.address)
	nick := a.address.Localpart()
	if err := xmpp_room_enter(a, room, nick, name, 0, false); err != nil {
		return "", err
	}
	xmpp_bookmark_keep(a, room.String(), name, nick)
	for _, person := range people {
		conference_invite(room.String(), a.address, person, "")
	}
	return room.String(), nil
}

// === What rooms say ===

// xmpp_room_in takes what a room sends Mochi's own sessions: a message of the
// room, the subject that ends a join's history, a private message from
// someone in it, or an error, which where the room no longer has the account
// in it has it join again.
func xmpp_room_in(a *xmpp_account, s *xmpp_stanza, from jid.JID) {
	room := from.Bare().String()
	joining := xmpp_joining_of(a, room)
	joined := xmpp_joined_of(a, room)
	var m xmpp_message
	if s.decode(&m) != nil {
		return
	}
	kind := s.get("type")
	switch {
	case kind == "error":
		if joined != nil && from.Resourcepart() == "" && xmpp_condition(s) == "not-acceptable" {
			go xmpp_room_rejoin(a, joined)
		}
		return
	case kind != "groupchat":
		if from.Resourcepart() == "" || (joined == nil && joining == nil) {
			return
		}
		// Privately, from someone in the room, as they are known there.
		conversation := "xmpp:" + from.String()
		events := xmpp_events(&m, m.Delay, from, conversation, conversation, from.Resourcepart(), "", false)
		xmpp_payload_add(events, s)
		xmpp_tell(a, events)
		return
	}
	if m.Subject != nil && strings.TrimSpace(m.Body) == "" {
		if joining != nil {
			joining.finish(nil)
		}
		return
	}
	nick := ""
	switch {
	case joining != nil:
		nick = joining.nick
	case joined != nil:
		nick = joined.nick
	default:
		return
	}
	conversation := "xmpp:" + room
	archive := m.archived(room)
	own := from.Resourcepart() == nick
	id := m.ID
	if m.Origin != nil && m.Origin.ID != "" {
		id = m.Origin.ID
	}
	var events []map[string]any
	if own && xmpp_room_echo(a, id) {
		// The room's copy of what Mochi sent: only the id it gave is news.
		if archive == "" {
			return
		}
		events = []map[string]any{{"kind": "sent", "conversation": conversation, "id": id, "archive": archive}}
	} else {
		events = xmpp_events(&m, m.Delay, from, conversation, "xmpp:"+from.String(), from.Resourcepart(), archive, own)
		xmpp_payload_add(events, s)
	}
	if joining != nil && joining.add(events...) {
		return
	}
	for _, event := range events {
		if event["kind"] == "message" {
			xmpp_database().exec("update joined set seen=max(seen, ?) where account=? and room=?", as_int64(event["created"]), a.id, room)
		}
	}
	xmpp_tell(a, events)
}

// xmpp_room_heard takes a room's presence for Mochi's own sessions: whether a
// join worked, who is in the room and in what role, and the account being
// put out of it, which the apps hear as the room left. It reports whether the
// presence was a room's.
func xmpp_room_heard(a *xmpp_account, s *xmpp_stanza, from jid.JID) bool {
	room := from.Bare().String()
	joining := xmpp_joining_of(a, room)
	joined := xmpp_joined_of(a, room)
	if joining == nil && joined == nil {
		return false
	}
	var p xmpp_presence
	if s.decode(&p) != nil {
		return true
	}
	nick := ""
	if joining != nil {
		nick = joining.nick
	} else {
		nick = joined.nick
	}
	codes := p.codes()
	self := codes["110"] || from.Resourcepart() == nick
	conversation := "xmpp:" + room
	switch {
	case p.Type == "error":
		if joining != nil {
			joining.finish(fmt.Errorf("the room refused the join: %s", xmpp_condition(s)))
		}
	case self && p.Type == "unavailable":
		if joining != nil {
			joining.finish(fmt.Errorf("the room put the account out"))
			return true
		}
		if codes["303"] {
			return true
		}
		xmpp_database().exec("delete from joined where account=? and room=?", a.id, room)
		xmpp_bookmark_drop(a, room)
		xmpp_tell(a, []map[string]any{{"kind": "room", "conversation": conversation, "left": true}})
	case self:
		if joining != nil {
			joining.lock.Lock()
			first := !joining.entered
			joining.entered = true
			joining.lock.Unlock()
			// The subject ends the history; a room that sends none is taken
			// to have sent it all a moment after confirming the join.
			if first {
				time.AfterFunc(xmpp_history_wait, func() { joining.finish(nil) })
			}
		}
	case from.Resourcepart() != "":
		event := map[string]any{"kind": "member", "conversation": conversation, "id": "xmpp:" + from.String(), "name": from.Resourcepart(), "left": p.Type == "unavailable"}
		if item := p.item(); item != nil {
			event["role"], event["affiliation"] = item.Role, item.Affiliation
		}
		if joining != nil && joining.add(event) {
			return true
		}
		xmpp_tell(a, []map[string]any{event})
	}
	return true
}

// === Keeping the rooms ===

// xmpp_room_rejoin joins a room again, asking for what came since its latest
// message, and tells the apps what it said meanwhile; a room that will not
// have the account is left.
func xmpp_room_rejoin(a *xmpp_account, j *xmpp_joined) {
	room, err := jid.Parse(j.room)
	if err != nil {
		return
	}
	if err := xmpp_room_enter(a, room, j.nick, j.name, j.seen, false); err != nil {
		debug("XMPP account %s: room %s not joined again: %v", a.address, j.room, err)
		if strings.Contains(err.Error(), "refused") {
			xmpp_database().exec("delete from joined where account=? and room=?", a.id, j.room)
			xmpp_tell(a, []map[string]any{{"kind": "room", "conversation": "xmpp:" + j.room, "left": true}})
		}
	}
}

// xmpp_rooms_manager joins each account's rooms again once the server has
// started, and asks each room every quarter of an hour whether the account is
// still in it, joining it again where it is not.
func xmpp_rooms_manager() {
	time.Sleep(10 * time.Second)
	xmpp_rooms_check(true)
	for range time.Tick(xmpp_presence_ping) {
		if network_enabled("xmpp") {
			xmpp_rooms_check(false)
		}
	}
}

// xmpp_rooms_check joins every room kept, or on a later pass asks each room
// whether the account is in it.
func xmpp_rooms_check(start bool) {
	if !network_enabled("xmpp") {
		return
	}
	rows, _ := xmpp_database().rows("select account, address, room, nick, name, seen from joined order by created")
	for _, row := range rows {
		address, err := jid.Parse(row_string(row, "address"))
		if err != nil {
			continue
		}
		a := xmpp_account_of(address)
		if a == nil || a.id != row_string(row, "account") {
			continue
		}
		j := &xmpp_joined{room: row_string(row, "room"), nick: row_string(row, "nick"), name: row_string(row, "name"), seen: row_int(row, "seen")}
		if start {
			xmpp_room_rejoin(a, j)
			continue
		}
		full, err := jid.Parse(j.room + "/" + j.nick)
		if err != nil {
			continue
		}
		if _, err := xmpp_ask(a, full, "get", `<ping xmlns="`+xmpp_ns_ping+`"/>`); err != nil {
			xmpp_room_rejoin(a, j)
		}
	}
}

// xmpp_rooms_announce shows each room Mochi's own sessions on an account are
// in the presence they now have.
func xmpp_rooms_announce(a *xmpp_account) {
	rows, _ := xmpp_database().rows("select room, nick from joined where account=?", a.id)
	inner := xmpp_room_presence(a)
	for _, row := range rows {
		if full, err := jid.Parse(row_string(row, "room") + "/" + row_string(row, "nick")); err == nil {
			xmpp_route(xmpp_make("presence", inner), a.mochi(), full)
		}
	}
}
