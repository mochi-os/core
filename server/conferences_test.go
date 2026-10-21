// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"
)

// xmpp_configure submits a room's configuration as its owner, the fields
// given as name and value.
func xmpp_configure(t *testing.T, c *xmpp_raw_client, room string, id string, fields ...string) {
	t.Helper()
	form := ""
	for i := 0; i+1 < len(fields); i += 2 {
		form += `<field var="` + fields[i] + `"><value>` + fields[i+1] + `</value></field>`
	}
	c.send(`<iq xmlns="jabber:client" type="set" id="` + id + `" to="` + room + `"><query xmlns="http://jabber.org/protocol/muc#owner"><x xmlns="jabber:x:data" type="submit"><field var="FORM_TYPE"><value>http://jabber.org/protocol/muc#roomconfig</value></field>` + form + `</x></query></iq>`)
	c.expect(t, "the configuration taken", `id="`+id+`"`, `type="result"`)
}

// xmpp_affiliate sets someone's affiliation with a room, or a nick's role.
func xmpp_affiliate(t *testing.T, c *xmpp_raw_client, room string, id string, item string) {
	t.Helper()
	c.send(`<iq xmlns="jabber:client" type="set" id="` + id + `" to="` + room + `"><query xmlns="http://jabber.org/protocol/muc#admin">` + item + `</query></iq>`)
	c.expect(t, "the change taken", `id="`+id+`"`, `type="result"`)
}

// TestXMPPRooms: an account here joining a room that is not makes it, its
// owner, locked to anyone else until taken as it is; someone joining is told
// who is in it, given its history and subject, and the room told of them; a
// message to the room goes to everyone in it with the room's id; a private
// message goes to one occupant as from the sender's nick; a nick someone else
// holds is refused; an occupant asking whether they are in the room is
// answered; the room's archive gives its messages; and discovery tells what
// the room is.
func TestXMPPRooms(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.apps(t)
	alice := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "phone")
	bob := env.raw(t, "bob@example.com", env.mint(t, env.bob, "x2", "Phone"), "phone")
	room := "lounge@rooms.example.com"

	alice.send(`<presence xmlns="jabber:client" to="` + room + `/Alice"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	made := alice.expect(t, "the room made", `from="`+room+`/Alice"`, `code="110"`)
	if !strings.Contains(made, `code="201"`) || !strings.Contains(made, `affiliation="owner"`) || !strings.Contains(made, `role="moderator"`) {
		t.Errorf("THE POINT: a room made by joining it: %s", made)
	}
	bob.send(`<presence xmlns="jabber:client" to="` + room + `/Bob"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	bob.expect(t, "THE POINT: a locked room refused", `from="`+room+`/Bob"`, `type="error"`, "item-not-found")
	xmpp_configure(t, alice, room, "c1", "muc#roomconfig_roomname", "Lounge", "muc#roomconfig_persistentroom", "1", "muc#roomconfig_publicroom", "1", "muc#roomconfig_whois", "anyone")
	alice.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="g0"><body>Before bob</body></message>`)
	alice.expect(t, "the room's echo", `from="`+room+`/Alice"`, "Before bob")

	bob.send(`<presence xmlns="jabber:client" to="` + room + `/Bob"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	bob.expect(t, "THE POINT: who is in the room", `from="`+room+`/Alice"`, `affiliation="owner"`, `jid="alice@example.com/phone"`)
	bob.expect(t, "joined", `from="`+room+`/Bob"`, `code="110"`, `code="100"`)
	history := bob.expect(t, "THE POINT: the room's history", "Before bob", "urn:xmpp:delay")
	if !strings.Contains(history, `from="`+room+`/Alice"`) {
		t.Errorf("the history's sender: %s", history)
	}
	bob.expect(t, "the subject", "<subject")
	alice.expect(t, "THE POINT: the room told of bob", `from="`+room+`/Bob"`, `role="participant"`)

	bob.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="g1"><body>Hello room</body></message>`)
	said := alice.expect(t, "THE POINT: a message to everyone", `from="`+room+`/Bob"`, "Hello room")
	if !strings.Contains(said, `<stanza-id xmlns="urn:xmpp:sid:0"`) || !strings.Contains(said, `by="`+room+`"`) {
		t.Errorf("the room's id: %s", said)
	}
	bob.expect(t, "the sender's own copy", `from="`+room+`/Bob"`, "Hello room")
	archive := regexp.MustCompile(`stanza-id[^>]*id="([^"]+)"`).FindStringSubmatch(said)[1]

	alice.send(`<message xmlns="jabber:client" to="` + room + `/Bob" type="chat" id="p1"><body>Just you</body></message>`)
	private := bob.expect(t, "THE POINT: a private message", "Just you")
	if !strings.Contains(private, `from="`+room+`/Alice"`) || !strings.Contains(private, "muc#user") {
		t.Errorf("the private message: %s", private)
	}

	other := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Tablet"), "tablet")
	other.send(`<presence xmlns="jabber:client" to="` + room + `/Bob"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	other.expect(t, "THE POINT: another's nick refused", `type="error"`, "conflict")
	time.Sleep(100 * time.Millisecond)
	if r := xmpp_resource_get("alice@example.com", "tablet"); r == nil || r.joined(mustjid(room)) {
		t.Error("THE POINT: a client refused a room counted as in it")
	}

	bob.send(`<iq xmlns="jabber:client" type="get" id="s1" to="` + room + `/Bob"><ping xmlns="urn:xmpp:ping"/></iq>`)
	bob.expect(t, "THE POINT: in the room, as asked", `id="s1"`, `type="result"`)
	other.send(`<iq xmlns="jabber:client" type="get" id="s2" to="` + room + `/Bob"><ping xmlns="urn:xmpp:ping"/></iq>`)
	other.expect(t, "someone not in the room asking", `id="s2"`, "not-acceptable")

	bob.send(`<iq xmlns="jabber:client" type="set" id="a1" to="` + room + `"><query xmlns="urn:xmpp:mam:2" queryid="R1"><set xmlns="http://jabber.org/protocol/rsm"><max>10</max></set></query></iq>`)
	bob.expect(t, "THE POINT: the room's archive", `queryid="R1"`, `id="`+archive+`"`, "Hello room")
	bob.expect(t, "the archive's end", `id="a1"`, `complete="true"`)

	bob.send(`<iq xmlns="jabber:client" type="get" id="d1" to="` + room + `"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`)
	information := bob.expect(t, "the room's discovery", `id="d1"`, `category="conference"`)
	for _, feature := range []string{`name="Lounge"`, "muc_persistent", "muc_open", "muc_nonanonymous", "muc_public", "urn:xmpp:mam:2"} {
		if !strings.Contains(information, feature) {
			t.Errorf("the room's discovery lacks %s: %s", feature, information)
		}
	}
	bob.send(`<iq xmlns="jabber:client" type="get" id="d2" to="rooms.example.com"><query xmlns="http://jabber.org/protocol/disco#items"/></iq>`)
	bob.expect(t, "THE POINT: the room listed by its service", `id="d2"`, `jid="`+room+`"`, `name="Lounge"`)
	bob.send(`<iq xmlns="jabber:client" type="get" id="d3" to="example.com"><query xmlns="http://jabber.org/protocol/disco#items"/></iq>`)
	bob.expect(t, "the service listed by the server", `id="d3"`, `jid="rooms.example.com"`)

	bob.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="t1"><subject>Bob's topic</subject></message>`)
	bob.expect(t, "a participant's subject refused", `id="t1"`, "forbidden")
	alice.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="t2"><subject>Our topic</subject></message>`)
	bob.expect(t, "the owner's subject", "Our topic</subject>")

	bob.send(`<presence xmlns="jabber:client" to="` + room + `/Bob" type="unavailable"/>`)
	bob.expect(t, "bob out", `type="unavailable"`, `code="110"`)
	bob.send(`<iq xmlns="jabber:client" type="set" id="a2" to="` + room + `"><query xmlns="urn:xmpp:mam:2"/></iq>`)
	bob.expect(t, "THE POINT: the archive asked by someone neither in the room nor a member", `id="a2"`, "forbidden")
	alice.send(`<presence xmlns="jabber:client" to="` + room + `/Alice" type="unavailable"/>`)
	alice.expect(t, "alice out", `type="unavailable"`, `code="110"`)
	other.send(`<iq xmlns="jabber:client" type="set" id="a3" to="` + room + `"><query xmlns="urn:xmpp:mam:2" queryid="R3"/></iq>`)
	other.expect(t, "THE POINT: the archive answers its owner, with no client in the room", `queryid="R3"`, "Hello room")
}

// TestXMPPRoomPeople: a room for members only puts out those who are not,
// and refuses them until made members; a moderator puts someone out, who may
// come back, and an owner bans them, who may not; members of a room for
// members see who its members are; an invitation through the room makes the
// invited a member; an owner ends the room, everyone in it told; a room that
// does not persist goes once empty.
func TestXMPPRoomPeople(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.apps(t)
	alice := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "phone")
	bob := env.raw(t, "bob@example.com", env.mint(t, env.bob, "x2", "Phone"), "phone")
	room := "team@rooms.example.com"
	join := func(c *xmpp_raw_client, nick string) {
		c.send(`<presence xmlns="jabber:client" to="` + room + `/` + nick + `"><x xmlns="http://jabber.org/protocol/muc"><history maxstanzas="0"/></x></presence>`)
	}
	join(alice, "Alice")
	alice.expect(t, "the room made", `code="201"`)
	xmpp_configure(t, alice, room, "c1", "muc#roomconfig_persistentroom", "1")
	join(bob, "Bob")
	bob.expect(t, "bob in", `from="`+room+`/Bob"`, `code="110"`)

	xmpp_configure(t, alice, room, "c2", "muc#roomconfig_persistentroom", "1", "muc#roomconfig_membersonly", "1")
	bob.expect(t, "THE POINT: someone not a member put out", `type="unavailable"`, `code="322"`)
	bob.received = nil
	join(bob, "Bob")
	bob.expect(t, "THE POINT: someone not a member refused", `type="error"`, "registration-required")
	xmpp_affiliate(t, alice, room, "m1", `<item affiliation="member" jid="bob@example.com"/>`)
	join(bob, "Bob")
	bob.expect(t, "a member in", `from="`+room+`/Bob"`, `code="110"`, `affiliation="member"`)
	bob.send(`<iq xmlns="jabber:client" type="get" id="l1" to="` + room + `"><query xmlns="http://jabber.org/protocol/muc#admin"><item affiliation="member"/></query></iq>`)
	bob.expect(t, "THE POINT: a member sees the members", `id="l1"`, `jid="bob@example.com"`)
	bob.send(`<iq xmlns="jabber:client" type="get" id="l2" to="` + room + `"><query xmlns="http://jabber.org/protocol/muc#admin"><item affiliation="outcast"/></query></iq>`)
	bob.expect(t, "a member asking for the outcasts", `id="l2"`, "forbidden")

	bob.received = nil
	xmpp_affiliate(t, alice, room, "k1", `<item nick="Bob" role="none"/>`)
	bob.expect(t, "THE POINT: put out", `type="unavailable"`, `code="307"`)
	join(bob, "Bob")
	bob.expect(t, "back after being put out", `from="`+room+`/Bob"`, `code="110"`, `role="participant"`)
	bob.received = nil
	xmpp_affiliate(t, alice, room, "b1", `<item affiliation="outcast" jid="bob@example.com"/>`)
	bob.expect(t, "THE POINT: banned", `type="unavailable"`, `code="301"`)
	join(bob, "Bob")
	bob.expect(t, "THE POINT: an outcast refused", `type="error"`, "forbidden")

	alice.send(`<message xmlns="jabber:client" to="` + room + `"><x xmlns="http://jabber.org/protocol/muc#user"><invite to="carol@remote.test"><reason>Join us</reason></invite></x></message>`)
	wait(t, "THE POINT: the invited made a member", func() bool {
		c := conference_of(room)
		c.lock.Lock()
		defer c.lock.Unlock()
		return c.affiliation("carol@remote.test") == "member"
	})

	alice.send(`<iq xmlns="jabber:client" type="set" id="e1" to="` + room + `"><query xmlns="http://jabber.org/protocol/muc#owner"><destroy><reason>Done</reason></destroy></query></iq>`)
	alice.expect(t, "THE POINT: the room's end told", `type="unavailable"`, "<destroy", "Done</reason>")
	if conference_of(room) != nil {
		t.Error("the room is still there")
	}

	brief := "brief@rooms.example.com"
	bob.send(`<presence xmlns="jabber:client" to="` + brief + `/Bob"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	bob.expect(t, "a room made", `from="`+brief+`/Bob"`, `code="201"`)
	bob.send(`<iq xmlns="jabber:client" type="set" id="c3" to="` + brief + `"><query xmlns="http://jabber.org/protocol/muc#owner"><x xmlns="jabber:x:data" type="submit"/></query></iq>`)
	bob.expect(t, "taken as it is", `id="c3"`, `type="result"`)
	bob.send(`<presence xmlns="jabber:client" to="` + brief + `/Bob" type="unavailable"/>`)
	wait(t, "THE POINT: a room that does not persist gone once empty", func() bool { return conference_of(brief) == nil })
}

// TestXMPPRoomsFederated: someone on another server joins a room here over
// their server's link to the room service, which answers them over its own
// link, authenticated as the room service's domain; their messages reach
// those here, and those here reach them. A room is made only by an account
// of this server's.
func TestXMPPRoomsFederated(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.apps(t)
	remote := env.remote(t, "remote.test")
	alice := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "phone")
	room := "open@rooms.example.com"
	alice.send(`<presence xmlns="jabber:client" to="` + room + `/Alice"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	alice.expect(t, "the room made", `code="201"`)
	xmpp_configure(t, alice, room, "c1", "muc#roomconfig_persistentroom", "1")

	link, valid := remote.link_to(t, "rooms.example.com", false)
	if valid != "valid" {
		t.Fatalf("THE POINT: the link to the room service: %s", valid)
	}
	link.write(`<presence xmlns="jabber:server" from="carol@remote.test/laptop" to="` + room + `/Carol"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	wait(t, "THE POINT: the room's answer over its own link", func() bool {
		return len(remote.heard(`from="`+room+`/Carol"`, `to="carol@remote.test/laptop"`, `code="110"`)) > 0
	})
	alice.expect(t, "carol told to alice", `from="`+room+`/Carol"`)
	link.write(`<message xmlns="jabber:server" from="carol@remote.test/laptop" to="` + room + `" type="groupchat" id="r1"><body>From afar</body></message>`)
	alice.expect(t, "THE POINT: a message from another server's person", `from="`+room+`/Carol"`, "From afar")
	alice.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="r2"><body>From here</body></message>`)
	wait(t, "THE POINT: a message to another server's person", func() bool {
		return len(remote.heard(`from="`+room+`/Alice"`, "From here")) > 0
	})

	link.write(`<presence xmlns="jabber:server" from="carol@remote.test/laptop" to="made@rooms.example.com/Carol"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	wait(t, "THE POINT: a room made from another server refused", func() bool {
		return len(remote.heard(`from="made@rooms.example.com/Carol"`, `type="error"`)) > 0
	})
	time.Sleep(50 * time.Millisecond)
	if conference_of("made@rooms.example.com") != nil {
		t.Error("another server's person made a room")
	}
}

// TestXMPPRoomsReached: the Status page shows each XMPP domain's room service
// working where its host leads where the domain does - by its own address or
// its server's SRV record - and unavailable where it does not.
func TestXMPPRoomsReached(t *testing.T) {
	create_xmpp_server_env(t)
	actor_register(user_by_uid("u-bob"), "x3", "xmpp", "other.test", "bob")
	actor_register(user_by_uid("u-bob"), "x4", "xmpp", "third.test", "bob")
	saved_host, saved_srv := xmpp_lookup_host, xmpp_lookup_srv
	xmpp_lookup_host = func(ctx context.Context, host string) ([]string, error) {
		switch host {
		case "example.com", "rooms.example.com", "server.third.test":
			return []string{"192.0.2.1"}, nil
		case "other.test", "third.test":
			return []string{"192.0.2.1"}, nil
		case "rooms.other.test":
			return []string{"198.51.100.7"}, nil
		}
		return nil, fmt.Errorf("no such host")
	}
	xmpp_lookup_srv = func(ctx context.Context, service string, proto string, name string) (string, []*net.SRV, error) {
		if name == "rooms.third.test" {
			return "", []*net.SRV{{Target: "server.third.test.", Port: 5269}}, nil
		}
		return "", nil, fmt.Errorf("no records")
	}
	t.Cleanup(func() {
		xmpp_lookup_host, xmpp_lookup_srv = saved_host, saved_srv
		xmpp_reached = map[string]xmpp_reach{}
	})
	states := map[string]string{}
	for _, entry := range xmpp_rooms_state(context.Background()) {
		e := entry.(map[string]any)
		states[e["host"].(string)] = e["state"].(string) + " " + e["reason"].(string)
	}
	if states["rooms.example.com"] != "working " || states["rooms.third.test"] != "working " {
		t.Errorf("THE POINT: room services that lead here: %v", states)
	}
	if states["rooms.other.test"] != "unavailable dns" {
		t.Errorf("THE POINT: a room service that leads elsewhere: %v", states)
	}

	db_open("db/users.db").exec("update users set role='administrator' where uid='u-bob'")
	app := create_external_app("status")
	permission_grant(user_by_uid("u-bob"), app.id, "server/read")
	rooms, _ := networks_states(t, create_test_thread(user_by_uid("u-bob"), app))["xmpp"]["rooms"].([]any)
	if len(rooms) != 3 {
		t.Errorf("THE POINT: the Status page's room services: %v", rooms)
	}
}

// TestXMPPRoomsCertified: a certificate for a domain's room service is asked
// for under the domain, whose row decides it, and for no other name under it.
func TestXMPPRoomsCertified(t *testing.T) {
	create_xmpp_server_env(t)
	domains := db_open("db/domains.db")
	domains.exec("create table if not exists domains (domain text primary key, verified integer not null default 0, token text not null default '', tls integer not null default 1, created integer not null, updated integer not null)")
	domains.exec("insert or replace into domains (domain, verified, tls, created, updated) values ('example.com', 1, 1, 1, 1)")
	if d := domain_certified("rooms.example.com"); d == nil || d.Domain != "example.com" {
		t.Errorf("THE POINT: the room service certified under its domain: %v", d)
	}
	if err := domains_host_policy(context.Background(), "rooms.example.com"); err != nil {
		t.Errorf("THE POINT: the room service's certificate refused: %v", err)
	}
	for _, host := range []string{"lobby.example.com", "rooms.elsewhere.test"} {
		if d := domain_certified(host); d != nil {
			t.Errorf("%s certified under %s", host, d.Domain)
		}
	}
}
