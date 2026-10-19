// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"regexp"
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// xmpp_object is a dict of strings, as an app passes an object.
func xmpp_object(pairs ...string) *sl.Dict {
	d := sl.NewDict(len(pairs) / 2)
	for i := 0; i+1 < len(pairs); i += 2 {
		d.SetKey(sl.String(pairs[i]), sl.String(pairs[i+1]))
	}
	return d
}

// xmpp_call calls a function as a user's app holding their accounts.
func xmpp_call(t *testing.T, user *User, f func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error), args sl.Tuple, kwargs ...sl.Tuple) sl.Value {
	t.Helper()
	value, err := f(linked_thread(user), sl.NewBuiltin("test", nil), args, kwargs)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// bob_online is bob's client, online, with none of alice's.
func (env *xmpp_env) bob_online(t *testing.T) *xmpp_test_client {
	t.Helper()
	b, err := env.connect(t, "bob@example.com", env.mint(t, env.bob, "x2", "Phone"), "")
	if err != nil {
		t.Fatal(err)
	}
	b.online(t)
	return b
}

// both puts alice and bob on each other's rosters, each seeing the other's
// presence.
func (env *xmpp_env) both() {
	xmpp_roster_save(&xmpp_account{user: env.alice, id: "x1"}, &xmpp_item{address: "bob@example.com", subscription: "both", listed: true})
	xmpp_roster_save(&xmpp_account{user: env.bob, id: "x2"}, &xmpp_item{address: "alice@example.com", subscription: "both", listed: true})
}

// TestXMPPStreamManagement: a client that manages its stream (XEP-0198) is
// told how many stanzas the server has had from it, and what it has not
// acknowledged is kept; its connection breaking leaves its resource bound,
// what comes meanwhile waiting, until it resumes on a new stream and is given
// what it missed. A token that is not the resource's is refused. A client
// that did not ask to resume, or does not within the time allowed, is gone
// with its connection: what it missed goes to the account's other clients
// online, or where none is waits for the next.
func TestXMPPStreamManagement(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	fake.allow(env.bob, "alice@example.com")
	password := env.mint(t, env.alice, "x1", "Phone")
	b := env.bob_online(t)

	c := env.raw(t, "alice@example.com", password, "phone")
	c.send(`<presence xmlns="jabber:client"/>`)
	c.send(`<enable xmlns="urn:xmpp:sm:3" resume="true"/>`)
	enabled := c.expect(t, "stream management enabled", "<enabled", `resume="true"`)
	token := regexp.MustCompile(`id="([^"]+)"`).FindStringSubmatch(enabled)[1]
	c.send(`<iq xmlns="jabber:client" type="get" id="p1" to="example.com"><ping xmlns="urn:xmpp:ping"/></iq>`)
	c.expect(t, "the ping's answer", `id="p1"`)
	c.send(`<r xmlns="urn:xmpp:sm:3"/>`)
	c.expect(t, "THE POINT: the server counts what it had from the client", `<a xmlns="urn:xmpp:sm:3" h="1"`)

	for _, text := range []string{"First", "Second", "Third"} {
		b.send(t, `<message xmlns="jabber:client" to="alice@example.com/phone" type="chat" id="`+text+`"><body>`+text+`</body></message>`)
	}
	c.expect(t, "the third message", "Third</body>")
	// The ping's answer was the first stanza counted, the first message the
	// second.
	c.send(`<a xmlns="urn:xmpp:sm:3" h="2"/>`)
	time.Sleep(200 * time.Millisecond)
	c.conn.Close()
	time.Sleep(300 * time.Millisecond)
	r := xmpp_resource_get("alice@example.com", "phone")
	if r == nil {
		t.Fatal("THE POINT: the resource went with its connection")
	}
	r.lock.Lock()
	waiting, available := r.stream == nil, r.available
	r.lock.Unlock()
	if !waiting || !available {
		t.Errorf("the resource does not wait to be resumed: waiting %v, available %v", waiting, available)
	}
	b.send(t, `<message xmlns="jabber:client" to="alice@example.com/phone" type="chat" id="Fourth"><body>Fourth</body></message>`)

	wrong := env.signed(t, "alice@example.com", password)
	if s := wrong.step(t, `<resume xmlns="urn:xmpp:sm:3" previd="not-the-token" h="2"/>`); s.name != "failed" {
		t.Errorf("a wrong token was taken: %s", s.text(s.space))
	}
	again := env.signed(t, "alice@example.com", password)
	resumed := again.step(t, `<resume xmlns="urn:xmpp:sm:3" previd="`+token+`" h="2"/>`)
	if resumed.name != "resumed" || resumed.get("h") != "1" {
		t.Fatalf("THE POINT: the resumption: %s", resumed.text(resumed.space))
	}
	again.listen()
	again.expect(t, "THE POINT: an unacknowledged message given again", "Second</body>")
	again.expect(t, "the next", "Third</body>")
	again.expect(t, "THE POINT: what came while the connection was down", "Fourth</body>")
	if heard := again.heard("First</body>"); len(heard) > 0 {
		t.Errorf("an acknowledged message came again: %v", heard)
	}

	// Not resumed in time: the resource goes, and what it missed goes to the
	// account's other client online, or where none is waits for the next.
	saved := xmpp_resume_timeout
	xmpp_resume_timeout = 300 * time.Millisecond
	t.Cleanup(func() { xmpp_resume_timeout = saved })
	other := env.raw(t, "alice@example.com", password, "other")
	other.send(`<presence xmlns="jabber:client"/>`)
	again.send(`<enable xmlns="urn:xmpp:sm:3" resume="true"/>`)
	again.expect(t, "management enabled again", "<failed")
	again.conn.Close()
	time.Sleep(100 * time.Millisecond)
	b.send(t, `<message xmlns="jabber:client" to="alice@example.com/phone" type="chat" id="Fifth"><body>Fifth</body></message>`)
	wait(t, "the resource to go once not resumed", func() bool { return xmpp_resource_get("alice@example.com", "phone") == nil })
	other.expect(t, "THE POINT: what the resource missed given to the client online", "Fifth</body>")

	third := env.raw(t, "alice@example.com", password, "phone")
	third.send(`<presence xmlns="jabber:client"/>`)
	third.send(`<enable xmlns="urn:xmpp:sm:3" resume="true"/>`)
	third.expect(t, "management on the third", "<enabled")
	other.conn.Close()
	wait(t, "the other client to go", func() bool { return xmpp_resource_get("alice@example.com", "other") == nil })
	third.conn.Close()
	time.Sleep(100 * time.Millisecond)
	b.send(t, `<message xmlns="jabber:client" to="alice@example.com/phone" type="chat" id="Sixth"><body>Sixth</body></message>`)
	wait(t, "the third to go once not resumed", func() bool { return xmpp_resource_get("alice@example.com", "phone") == nil })
	next := env.raw(t, "alice@example.com", password, "desk")
	next.send(`<presence xmlns="jabber:client"/>`)
	next.expect(t, "THE POINT: what the resource missed kept for the next client", "Sixth</body>")

	// A client that did not ask to resume goes with its connection.
	plain := env.raw(t, "alice@example.com", password, "plain")
	plain.send(`<enable xmlns="urn:xmpp:sm:3"/>`)
	plain.expect(t, "management without resumption", "<enabled")
	plain.conn.Close()
	wait(t, "the resource to go at once", func() bool { return xmpp_resource_get("alice@example.com", "plain") == nil })
}

// TestXMPPClientState: a client that says it is inactive (XEP-0352) is not
// sent its contacts' presence or typing, but is sent their messages; once
// active it is given each contact's latest presence.
func TestXMPPClientState(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	env.both()
	c := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "phone")
	c.send(`<presence xmlns="jabber:client"/>`)
	_, b := env.pair(t)
	c.expect(t, "bob's presence", `from="bob@example.com/`)
	c.send(`<inactive xmlns="urn:xmpp:csi:0"/>`)
	time.Sleep(200 * time.Millisecond)
	b.send(t, `<presence xmlns="jabber:client"><status>Lunch</status></presence>`)
	b.send(t, `<presence xmlns="jabber:client"><status>Back soon</status></presence>`)
	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="s1"><composing xmlns="http://jabber.org/protocol/chatstates"/></message>`)
	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="s2"><body>Still sent</body></message>`)
	c.expect(t, "THE POINT: a message reaches an inactive client", "Still sent")
	c.silent(t, "THE POINT: presence sent to an inactive client", "Lunch")
	if heard := c.heard("Back soon"); len(heard) > 0 {
		t.Errorf("presence sent to an inactive client: %v", heard)
	}
	if heard := c.heard("composing"); len(heard) > 0 {
		t.Errorf("typing sent to an inactive client: %v", heard)
	}
	c.send(`<active xmlns="urn:xmpp:csi:0"/>`)
	c.expect(t, "THE POINT: the latest presence once active", "Back soon")
	if heard := c.heard("Lunch"); len(heard) > 0 {
		t.Errorf("presence since replaced given: %v", heard)
	}
	if heard := c.heard("composing"); len(heard) > 0 {
		t.Errorf("typing given once active: %v", heard)
	}
}

// TestXMPPCarbons: the account's clients that take copies (XEP-0280) are
// given each message of a conversation another of them received or sent,
// and those Mochi's own sessions sent, with the id the account's archive
// gave it; a client that does not take copies is given none, nor any client
// a message marked private.
func TestXMPPCarbons(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	fake.allow(env.bob, "alice@example.com")
	password := env.mint(t, env.alice, "x1", "Phone")
	phone := env.raw(t, "alice@example.com", password, "phone")
	tablet := env.raw(t, "alice@example.com", password, "tablet")
	laptop := env.raw(t, "alice@example.com", password, "laptop")
	for i, c := range []*xmpp_raw_client{phone, tablet, laptop} {
		c.send(`<presence xmlns="jabber:client"/>`)
		if i < 2 {
			c.send(`<iq xmlns="jabber:client" type="set" id="c1"><enable xmlns="urn:xmpp:carbons:2"/></iq>`)
			c.expect(t, "copies enabled", `id="c1"`, `type="result"`)
		}
	}
	_, b := env.pair(t)

	b.send(t, `<message xmlns="jabber:client" to="alice@example.com/laptop" type="chat" id="r1"><body>To the laptop</body></message>`)
	laptop.expect(t, "the message", "To the laptop")
	phone.expect(t, "THE POINT: a copy of a message another client received", `<received xmlns="urn:xmpp:carbons:2"`, "To the laptop")
	tablet.expect(t, "the copy on the tablet", `<received xmlns="urn:xmpp:carbons:2"`, "To the laptop")

	phone.send(`<message xmlns="jabber:client" to="bob@example.com" type="chat" id="s1"><body>From the phone</body></message>`)
	b.expect(t, "the phone's message", "From the phone")
	tablet.expect(t, "THE POINT: a copy of a message another client sent", `<sent xmlns="urn:xmpp:carbons:2"`, "From the phone", `<stanza-id xmlns="urn:xmpp:sid:0"`)
	laptop.silent(t, "a copy to a client that takes none", "From the phone")
	if heard := phone.heard(`<sent xmlns="urn:xmpp:carbons:2"`, "From the phone"); len(heard) > 0 {
		t.Errorf("the sender was given a copy of its own message: %v", heard)
	}

	phone.send(`<message xmlns="jabber:client" to="bob@example.com" type="chat" id="s2"><body>Private</body><private xmlns="urn:xmpp:carbons:2"/></message>`)
	b.expect(t, "the private message", "Private</body>")
	tablet.silent(t, "THE POINT: a copy of a private message", "Private</body>")

	result := xmpp_call(t, env.alice, api_account_object_create, sl.Tuple{sl.String("x1"), xmpp_object("conversation", "xmpp:bob@example.com", "content", "From Mochi")})
	archive, _ := sl_decode(result).(map[string]any)["archive"].(string)
	if archive == "" {
		t.Errorf("no id in the archive for a message Mochi sent: %v", result)
	}
	phone.expect(t, "THE POINT: a copy of a message Mochi's own sessions sent", `<sent xmlns="urn:xmpp:carbons:2"`, "From Mochi", `from="alice@example.com/mochi"`, `id="`+archive+`"`)
}

// TestXMPPPush: a client registered with a push service (XEP-0357) has its
// app woken through the service, with no content, by a message that comes
// while none of the account's clients has a connection, and not while one
// does; a service that answers with an error is not woken again.
func TestXMPPPush(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	service := env.remote(t, "push.remote.test")
	password := env.mint(t, env.alice, "x1", "Phone")
	c := env.raw(t, "alice@example.com", password, "phone")
	c.send(`<presence xmlns="jabber:client"/>`)
	c.send(`<enable xmlns="urn:xmpp:sm:3" resume="true"/>`)
	c.expect(t, "stream management", "<enabled")
	c.send(`<iq xmlns="jabber:client" type="set" id="u1"><enable xmlns="urn:xmpp:push:0" jid="push.remote.test" node="node-1"><x xmlns="jabber:x:data" type="submit"><field var="FORM_TYPE"><value>http://jabber.org/protocol/pubsub#publish-options</value></field><field var="secret"><value>secret-1</value></field></x></enable></iq>`)
	c.expect(t, "push enabled", `id="u1"`, `type="result"`)
	b := env.bob_online(t)

	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="m1"><body>While online</body></message>`)
	c.expect(t, "the message", "While online")
	time.Sleep(300 * time.Millisecond)
	if heard := service.heard("urn:xmpp:push:0"); len(heard) > 0 {
		t.Errorf("a push while a client is online: %v", heard)
	}

	c.conn.Close()
	time.Sleep(300 * time.Millisecond)
	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="m2"><body>While asleep</body></message>`)
	var push string
	wait(t, "THE POINT: the push service woken", func() bool {
		heard := service.heard(`<publish node="node-1"`, "urn:xmpp:push:0")
		if len(heard) > 0 {
			push = heard[0]
		}
		return push != ""
	})
	if !strings.Contains(push, "secret-1") || strings.Contains(push, "While asleep") || !strings.Contains(push, `from="alice@example.com"`) {
		t.Errorf("the push: %s", push)
	}

	link, valid := service.link(t, false)
	if valid != "valid" {
		t.Fatalf("the service's link: %s", valid)
	}
	id := regexp.MustCompile(` id="([^"]+)"`).FindStringSubmatch(push)[1]
	link.write(`<iq xmlns="jabber:server" type="error" id="` + id + `" from="push.remote.test" to="alice@example.com"><error type="cancel"><item-not-found xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`)
	wait(t, "THE POINT: a refused push's registration dropped", func() bool {
		found, _ := xmpp_database().exists("select 1 from pushes where account='x1'")
		return !found
	})
}
