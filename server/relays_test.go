// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// TestXMPPRelays: a group taking someone from XMPP is relayed through a room
// for members on the relaying user's server, the Mochi members in it under
// their names with no address and the person invited and made a member;
// what a member says goes to the room as them, what the person says comes to
// the relaying user, and the person's reactions too; a member's correction
// and withdrawal reach the room; the person put out is no longer a member;
// and the relay closed, the room ends. A Mochi member in the room cannot be
// written to privately.
func TestXMPPRelays(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.bob, "alice@example.com")
	thread, log := matrix_bridge_env(t, env.alice)
	fn := sl.NewBuiltin("test", nil)
	call := func(f func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error), args sl.Tuple, kwargs ...sl.Tuple) sl.Value {
		t.Helper()
		value, err := f(thread, fn, args, kwargs)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if call(api_bridge_available, nil, sl.Tuple{sl.String("network"), sl.String("xmpp")}) != sl.True {
		t.Fatal("no relay to XMPP available")
	}
	carol := sl.NewDict(2)
	carol.SetKey(sl.String("entity"), sl.String("1CaroLEntityForTheRelayTestXXXXXXXXXXXXXXXXXXXXXXX"))
	carol.SetKey(sl.String("name"), sl.String("Carol"))
	made := sl_decode(call(api_bridge_create, sl.Tuple{sl.String("chat-1"), sl.String("Friends"), sl.NewList([]sl.Value{sl.String("bob@example.com")})}, sl.Tuple{sl.String("members"), sl.NewList([]sl.Value{carol})})).(map[string]any)
	relayed, _ := made["room"].(string)
	room := strings.TrimPrefix(relayed, "xmpp:")
	people, _ := made["people"].([]any)
	if !strings.HasPrefix(relayed, "xmpp:") || !strings.HasSuffix(room, "@rooms.example.com") || len(people) != 1 || people[0].(map[string]any)["id"] != "xmpp:bob@example.com" {
		t.Fatalf("THE POINT: the relay made: %v", made)
	}

	bob := env.raw(t, "bob@example.com", env.mint(t, env.bob, "x2", "Phone"), "phone")
	bob.send(`<presence xmlns="jabber:client"/>`)
	c := conference_of(room)
	c.lock.Lock()
	member, members := c.affiliation("bob@example.com"), c.config.members
	c.lock.Unlock()
	if member != "member" || !members {
		t.Errorf("THE POINT: the person made a member of a room for members: %s %v", member, members)
	}
	bob.send(`<presence xmlns="jabber:client" to="` + room + `/Bob"><x xmlns="http://jabber.org/protocol/muc"/></presence>`)
	seated := bob.expect(t, "THE POINT: a Mochi member in the room", `from="`+room+`/Carol"`, `affiliation="member"`)
	if strings.Contains(seated, "jid=") {
		t.Errorf("a Mochi member shown with an address: %s", seated)
	}
	bob.expect(t, "the relaying member in the room", `from="`+room+`/Alice"`)
	bob.expect(t, "bob in", `from="`+room+`/Bob"`, `code="110"`)

	sent := sl_decode(call(api_bridge_send, sl.Tuple{sl.String(relayed), sl.String("1CaroLEntityForTheRelayTestXXXXXXXXXXXXXXXXXXXXXXX"), xmpp_object("content", "Hello from Mochi")}, sl.Tuple{sl.String("key"), sl.String("m1")})).(map[string]any)
	id, _ := sent["id"].(string)
	said := bob.expect(t, "THE POINT: a member's message as them", `from="`+room+`/Carol"`, "Hello from Mochi")
	if !strings.Contains(said, `id="`+id+`"`) {
		t.Errorf("the message's id %s: %s", id, said)
	}
	again := sl_decode(call(api_bridge_send, sl.Tuple{sl.String(relayed), sl.String("1CaroLEntityForTheRelayTestXXXXXXXXXXXXXXXXXXXXXXX"), xmpp_object("content", "Hello from Mochi")}, sl.Tuple{sl.String("key"), sl.String("m1")})).(map[string]any)
	if again["id"] != id || len(bob.heard("Hello from Mochi")) != 1 {
		t.Errorf("a key sent the message twice: %v", again)
	}

	bob.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="b1"><body>Hello from XMPP</body></message>`)
	heard := log.find(t, "message", func(e map[string]any) bool { return e["room"] == relayed && e["body"] == "Hello from XMPP" })
	if heard["from"] != "xmpp:bob@example.com" || heard["name"] != "Bob" || heard["archive"] == "" {
		t.Errorf("THE POINT: the person's message to the relaying user: %v", heard)
	}
	bob.send(`<message xmlns="jabber:client" to="` + room + `" type="groupchat" id="b2"><reactions xmlns="urn:xmpp:reactions:0" id="` + id + `"><reaction>👍</reaction></reactions></message>`)
	reacted := log.find(t, "reaction", func(e map[string]any) bool { return e["room"] == relayed })
	if reacted["id"] != id || reacted["reaction"] != "👍" || reacted["from"] != "xmpp:bob@example.com" {
		t.Errorf("THE POINT: the person's reaction: %v", reacted)
	}

	call(api_bridge_update, sl.Tuple{sl.String(relayed), sl.String("1CaroLEntityForTheRelayTestXXXXXXXXXXXXXXXXXXXXXXX"), sl.String(id), xmpp_object("content", "Hello again")})
	bob.expect(t, "THE POINT: a member's correction", "urn:xmpp:message-correct:0", `id="`+id+`"`, "Hello again")
	call(api_bridge_delete, sl.Tuple{sl.String(relayed), sl.String("1CaroLEntityForTheRelayTestXXXXXXXXXXXXXXXXXXXXXXX"), sl.String(id)})
	bob.expect(t, "THE POINT: a member's withdrawal", "urn:xmpp:message-retract:1", `id="`+id+`"`)
	reaction := sl_decode(call(api_bridge_send, sl.Tuple{sl.String(relayed), sl.String("1CaroLEntityForTheRelayTestXXXXXXXXXXXXXXXXXXXXXXX"), xmpp_object("reaction", "😂", "message", "x1")})).(map[string]any)
	if reaction["id"] != "reaction|x1" {
		t.Errorf("a member's reaction's id: %v", reaction)
	}
	bob.expect(t, "a member's reaction", `id="x1"`, "😂</reaction>")

	bob.send(`<message xmlns="jabber:client" to="` + room + `/Carol" type="chat" id="p1"><body>Privately</body></message>`)
	bob.expect(t, "a private message to a Mochi member refused", `id="p1"`, `type="error"`, "service-unavailable")
	bob.send(`<iq xmlns="jabber:client" to="` + room + `/Carol" type="get" id="v1"><vCard xmlns="vcard-temp"/></iq>`)
	bob.expect(t, "THE POINT: a Mochi member's card given by the room", `id="v1"`, `type="result"`, "<FN>Carol</FN>")
	bob.send(`<iq xmlns="jabber:client" to="` + room + `/Carol" type="get" id="v2"><query xmlns="jabber:iq:version"/></iq>`)
	bob.expect(t, "THE POINT: a request of a Mochi member refused", `id="v2"`, `type="error"`, "service-unavailable")

	uploaded := sl_decode(call(api_bridge_upload, sl.Tuple{sl.String(relayed), sl.Bytes("a file"), sl.String("text/plain")}, sl.Tuple{sl.String("name"), sl.String("note.txt")})).(map[string]any)
	if link, _ := uploaded["url"].(string); !regexp.MustCompile(`^https://example\.com/xmpp/upload/[^/]+/note\.txt$`).MatchString(link) {
		t.Errorf("THE POINT: a member's file kept for the room: %v", uploaded)
	}

	call(api_bridge_remove, sl.Tuple{sl.String(relayed), sl.String("xmpp:bob@example.com")})
	bob.expect(t, "THE POINT: the person put out", `type="unavailable"`, `code="321"`)
	c.lock.Lock()
	member = c.affiliation("bob@example.com")
	c.lock.Unlock()
	if member != "none" {
		t.Errorf("the person is still a member: %s", member)
	}
	call(api_bridge_close, sl.Tuple{sl.String(relayed)})
	if conference_of(room) != nil || bridge_of(relayed) != nil {
		t.Error("THE POINT: the room outlived the relay")
	}
}
