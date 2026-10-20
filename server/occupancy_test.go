// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
	"mellium.im/xmpp/jid"
)

// TestXMPPHostedRooms: chat joins a room by its address through a hosted
// account, as Mochi's own sessions under the account's name, and is told of
// the room, who is in it in what role at what address and its recent past,
// in that order, before the join returns; what the room says comes to chat,
// and what chat sends comes back with the room's id only; the room is kept
// in the account's bookmarks, which its clients are told of; leaving it
// withdraws the bookmark. A room a client bookmarks to be joined is joined,
// and left once the bookmark goes. Put out of a room, chat hears it left. A
// room whose service no longer has the account in it is joined again.
func TestXMPPHostedRooms(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.apps(t)
	alice := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "phone")
	client := env.raw(t, "bob@example.com", env.mint(t, env.bob, "x2", "Phone"), "phone")
	client.send(`<presence xmlns="jabber:client"/>`)
	room := "club@rooms.example.com"
	alice.send(`<presence xmlns="jabber:client" to="` + room + `/Alice"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	alice.expect(t, "the room made", `code="201"`)
	xmpp_configure(t, alice, room, "c1", "muc#roomconfig_roomname", "Club", "muc#roomconfig_persistentroom", "1", "muc#roomconfig_whois", "anyone")
	alice.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="g0"><body>Earlier</body></message>`)
	alice.expect(t, "the echo", "Earlier")

	joined := sl_decode(xmpp_call(t, env.bob, api_account_room_join, sl.Tuple{sl.String("x2"), sl.String(room)})).(map[string]any)
	if joined["conversation"] != "xmpp:"+room || joined["name"] != "Club" {
		t.Fatalf("THE POINT: the join: %v", joined)
	}
	env.log.lock.Lock()
	var kinds []string
	for _, e := range env.log.events {
		if e["account"] == "x2" && e["conversation"] == "xmpp:"+room {
			kinds = append(kinds, e["kind"].(string))
		}
	}
	env.log.lock.Unlock()
	if strings.Join(kinds, " ") != "room member message" {
		t.Errorf("THE POINT: what chat was told before the join returned, in order: %v", kinds)
	}
	member := env.log.find(t, "member", func(e map[string]any) bool { return e["account"] == "x2" && e["id"] == "xmpp:"+room+"/Alice" })
	if member["role"] != "moderator" || member["affiliation"] != "owner" {
		t.Errorf("THE POINT: the room's roles: %v", member)
	}
	if member["address"] != "alice@example.com" {
		t.Errorf("THE POINT: the address a room that shows it gives, bare, for encrypting to: %v", member)
	}
	alice.expect(t, "the room told of Mochi's own sessions", `from="`+room+`/bob"`, `jid="bob@example.com/mochi"`)
	client.expect(t, "THE POINT: the account's clients told of the bookmark", "urn:xmpp:bookmarks:1", `id="`+room+`"`, `autojoin="true"`)

	alice.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="g1"><body>Hi Mochi</body></message>`)
	said := env.log.find(t, "message", func(e map[string]any) bool { return e["account"] == "x2" && e["body"] == "Hi Mochi" })
	if said["from"] != "xmpp:"+room+"/Alice" || said["room"] != true || said["archive"] == "" || said["own"] != false {
		t.Errorf("THE POINT: the room's message to chat: %v", said)
	}
	sent := sl_decode(xmpp_call(t, env.bob, api_account_object_create, sl.Tuple{sl.String("x2"), xmpp_object("conversation", "xmpp:"+room, "content", "From Mochi")}, sl.Tuple{sl.String("key"), sl.String("chat|k1")})).(map[string]any)
	if sent["id"] != "chat|k1" {
		t.Errorf("the message's id: %v", sent)
	}
	alice.expect(t, "THE POINT: chat's message in the room", `from="`+room+`/bob"`, "From Mochi")
	echo := env.log.find(t, "sent", func(e map[string]any) bool { return e["account"] == "x2" && e["id"] == "chat|k1" })
	if echo["archive"] == "" {
		t.Errorf("THE POINT: the room's id for chat's message: %v", echo)
	}
	env.log.lock.Lock()
	for _, e := range env.log.events {
		if e["kind"] == "message" && e["body"] == "From Mochi" {
			t.Errorf("chat was told its own message as new: %v", e)
		}
	}
	env.log.lock.Unlock()

	xmpp_call(t, env.bob, api_account_room_leave, sl.Tuple{sl.String("x2"), sl.String("xmpp:" + room)})
	alice.expect(t, "THE POINT: Mochi's own sessions left the room", `from="`+room+`/bob"`, `type="unavailable"`)
	client.expect(t, "the bookmark withdrawn", "urn:xmpp:bookmarks:1", `<retract id="`+room+`"`)
	if xmpp_joined_of(xmpp_account_of(mustjid("bob@example.com")), room) != nil {
		t.Error("the room is still kept")
	}
	time.Sleep(200 * time.Millisecond)
	env.log.lock.Lock()
	for _, e := range env.log.events {
		if e["kind"] == "room" && e["conversation"] == "xmpp:"+room && e["left"] == true {
			t.Errorf("chat told of a leave it asked for as if the account were put out: %v", e)
		}
	}
	env.log.lock.Unlock()

	// A client's bookmark is joined, and left once withdrawn.
	den := "den@rooms.example.com"
	alice.send(`<presence xmlns="jabber:client" to="` + den + `/Alice"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	alice.expect(t, "a second room", `from="`+den+`/Alice"`, `code="201"`)
	xmpp_configure(t, alice, den, "c2", "muc#roomconfig_roomname", "Den", "muc#roomconfig_persistentroom", "1")
	client.send(`<iq xmlns="jabber:client" type="set" id="b1"><pubsub xmlns="http://jabber.org/protocol/pubsub"><publish node="urn:xmpp:bookmarks:1"><item id="` + den + `"><conference xmlns="urn:xmpp:bookmarks:1" name="Den" autojoin="true"><nick>bobby</nick></conference></item></publish></pubsub></iq>`)
	client.expect(t, "the bookmark kept", `id="b1"`, `type="result"`)
	alice.expect(t, "THE POINT: a client's bookmark joined", `from="`+den+`/bobby"`)
	env.log.find(t, "room", func(e map[string]any) bool {
		return e["account"] == "x2" && e["conversation"] == "xmpp:"+den && e["name"] == "Den"
	})
	client.send(`<iq xmlns="jabber:client" type="get" id="b2"><pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="urn:xmpp:bookmarks:1"/></pubsub></iq>`)
	client.expect(t, "the bookmarks read", `id="b2"`, `id="`+den+`"`)
	client.send(`<iq xmlns="jabber:client" type="set" id="b3"><pubsub xmlns="http://jabber.org/protocol/pubsub"><retract node="urn:xmpp:bookmarks:1" notify="true"><item id="` + den + `"/></retract></pubsub></iq>`)
	alice.expect(t, "THE POINT: a withdrawn bookmark left", `from="`+den+`/bobby"`, `type="unavailable"`)
	env.log.find(t, "room", func(e map[string]any) bool {
		return e["account"] == "x2" && e["conversation"] == "xmpp:"+den && e["left"] == true
	})

	// Put out of a room, chat hears it left; a room that forgot the account
	// is joined again.
	xmpp_call(t, env.bob, api_account_room_join, sl.Tuple{sl.String("x2"), sl.String(room)})
	xmpp_affiliate(t, alice, room, "k1", `<item nick="bob" role="none"/>`)
	env.log.find(t, "room", func(e map[string]any) bool {
		return e["account"] == "x2" && e["conversation"] == "xmpp:"+room && e["left"] == true
	})
	xmpp_call(t, env.bob, api_account_room_join, sl.Tuple{sl.String("x2"), sl.String(room)})
	c := conference_of(room)
	c.lock.Lock()
	delete(c.occupants, "bob")
	c.lock.Unlock()
	alice.received = nil
	xmpp_rooms_check(false)
	alice.expect(t, "THE POINT: a room that forgot the account joined again", `from="`+room+`/bob"`)
}

// TestXMPPHostedRoomCreate: chat starting a conversation with several
// people through a hosted account makes a room for members on the account's
// domain's room service, the account its owner and each person a member
// invited by the room, Mochi's own sessions in it.
func TestXMPPHostedRoomCreate(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.bob, "alice@example.com")
	client := env.raw(t, "bob@example.com", env.mint(t, env.bob, "x2", "Phone"), "phone")
	client.send(`<presence xmlns="jabber:client"/>`)
	made := sl_decode(xmpp_call(t, env.alice, api_account_room_create, sl.Tuple{sl.String("x1"), sl.NewList([]sl.Value{sl.String("bob@example.com"), sl.String("carol@remote.test")})}, sl.Tuple{sl.String("name"), sl.String("Planning")})).(map[string]any)
	conversation, _ := made["conversation"].(string)
	room := strings.TrimPrefix(conversation, "xmpp:")
	if made["room"] != true || !strings.HasSuffix(room, "@rooms.example.com") {
		t.Fatalf("THE POINT: the room made: %v", made)
	}
	client.expect(t, "THE POINT: the invitation", `from="`+room+`"`, `<invite from="alice@example.com"`)
	c := conference_of(room)
	c.lock.Lock()
	members, owner, name := c.affiliation("bob@example.com") == "member" && c.affiliation("carol@remote.test") == "member", c.affiliation("alice@example.com"), c.config.name
	in := c.occupants["alice"] != nil
	c.lock.Unlock()
	if !members || owner != "owner" || name != "Planning" || !in {
		t.Errorf("the room: members %v, owner %s, name %s, Mochi in it %v", members, owner, name, in)
	}
	client.send(`<presence xmlns="jabber:client" to="` + room + `/Bob"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	client.expect(t, "a member joins", `from="`+room+`/Bob"`, `code="110"`)
}

// TestXMPPCondition: an error's condition is the one it names in the
// stanzas' namespace.
func TestXMPPCondition(t *testing.T) {
	s, _ := xmpp_parse(`<iq xmlns="jabber:client" id="x" type="error"><error type="cancel"><other xmlns="urn:example"/><item-not-found xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`)
	if condition := xmpp_condition(s); condition != "item-not-found" {
		t.Errorf("THE POINT: the condition read: %q", condition)
	}
	s, _ = xmpp_parse(`<iq xmlns="jabber:client" id="x" type="error"><error type="cancel"/></iq>`)
	if condition := xmpp_condition(s); condition != "error" {
		t.Errorf("an error naming none: %q", condition)
	}
}

// TestXMPPRoomErrorRejoin: a room's error saying the account is not in it
// has Mochi's own sessions join it again; another error does not.
func TestXMPPRoomErrorRejoin(t *testing.T) {
	saved := xmpp_history_wait
	xmpp_history_wait = 100 * time.Millisecond
	t.Cleanup(func() { xmpp_history_wait = saved })
	env := create_xmpp_server_env(t)
	env.apps(t)
	alice := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "phone")
	room := "club@rooms.example.com"
	alice.send(`<presence xmlns="jabber:client" to="` + room + `/Alice"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	alice.expect(t, "the room made", `code="201"`)
	xmpp_configure(t, alice, room, "c1", "muc#roomconfig_persistentroom", "1")
	xmpp_call(t, env.bob, api_account_room_join, sl.Tuple{sl.String("x2"), sl.String(room)})
	alice.expect(t, "bob joined", `from="`+room+`/bob"`)
	a := &xmpp_account{user: env.bob, id: "x2", address: jid.MustParse("bob@example.com")}
	refused := func(condition string) *xmpp_stanza {
		s, _ := xmpp_parse(`<message xmlns="jabber:client" from="` + room + `" to="bob@example.com/mochi" type="error" id="e1"><error type="modify"><` + condition + ` xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></message>`)
		return s
	}
	alice.received = nil
	xmpp_room_in(a, refused("item-not-found"), jid.MustParse(room))
	alice.silent(t, "THE POINT: joined again on another error", `from="`+room+`/bob"`)
	xmpp_room_in(a, refused("not-acceptable"), jid.MustParse(room))
	alice.expect(t, "THE POINT: joined again where the room no longer has the account", `from="`+room+`/bob"`)
	// The join settles before the test's databases go.
	wait(t, "the join settled", func() bool { return xmpp_joining_of(a, room) == nil })
}
