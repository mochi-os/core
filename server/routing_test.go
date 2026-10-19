// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
	"mellium.im/xmpp/jid"
)

// An xmpp_apps is the apps the engine calls, played by the test: chat's
// acceptance rule, people's roster and friends, and the person's profile.
// It records each call.
type xmpp_apps struct {
	lock    sync.Mutex
	accept  map[string]bool
	friends map[string][]any
	avatar  []byte
	calls   []map[string]any
}

func (env *xmpp_env) apps(t *testing.T) *xmpp_apps {
	t.Helper()
	fake := &xmpp_apps{accept: map[string]bool{}, friends: map[string][]any{}}
	saved_call := activitypub_call
	activitypub_call = func(user *User, a *App, function string, args Map) (any, error) {
		fake.lock.Lock()
		defer fake.lock.Unlock()
		call := map[string]any{"user": user.UID, "function": function}
		for key, value := range args {
			call[key] = value
		}
		fake.calls = append(fake.calls, call)
		switch function {
		case "linked/accept":
			return fake.accept[user.UID+" "+args["person"].(string)], nil
		case "xmpp/friends":
			return fake.friends[user.UID], nil
		case "profile/media":
			if fake.avatar != nil {
				return map[string]any{"avatar": map[string]any{"data": fake.avatar, "type": "image/png"}}, nil
			}
			return map[string]any{}, nil
		case "activitypub/profile":
			return map[string]any{}, nil
		}
		return nil, nil
	}
	functions := map[string]AppFunction{}
	for _, name := range []string{"linked/accept", "xmpp/roster", "xmpp/friends", "profile/media", "activitypub/profile"} {
		functions[name] = AppFunction{Function: name}
	}
	apps_lock.Lock()
	saved_apps := apps
	apps = map[string]*App{"fake": {id: "fake", internal: &AppVersion{Functions: functions}}}
	apps_lock.Unlock()
	t.Cleanup(func() {
		xmpp_queues_drain()
		activitypub_call = saved_call
		apps_lock.Lock()
		apps = saved_apps
		apps_lock.Unlock()
	})
	return fake
}

// allow has a user's chat take whoever an address names.
func (fake *xmpp_apps) allow(user *User, address string) {
	fake.lock.Lock()
	fake.accept[user.UID+" xmpp:"+address] = true
	fake.lock.Unlock()
	xmpp_acceptances_lock.Lock()
	xmpp_acceptances = map[string]xmpp_acceptance{}
	xmpp_acceptances_lock.Unlock()
}

// called answers the calls of a function matching every pair given.
func (fake *xmpp_apps) called(function string, pairs ...any) []map[string]any {
	fake.lock.Lock()
	defer fake.lock.Unlock()
	var out []map[string]any
	for _, call := range fake.calls {
		if call["function"] != function {
			continue
		}
		all := true
		for i := 0; i+1 < len(pairs); i += 2 {
			all = all && call[pairs[i].(string)] == pairs[i+1]
		}
		if all {
			out = append(out, call)
		}
	}
	return out
}

// pair is two clients online, alice's and bob's.
func (env *xmpp_env) pair(t *testing.T) (*xmpp_test_client, *xmpp_test_client) {
	t.Helper()
	a, err := env.connect(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := env.connect(t, "bob@example.com", env.mint(t, env.bob, "x2", "Phone"), "")
	if err != nil {
		t.Fatal(err)
	}
	a.online(t)
	b.online(t)
	return a, b
}

// TestXMPPMessages: a message between two accounts here reaches the other's
// client and their chat, and the sender's chat as their own; one from someone
// the recipient's chat does not take is passed over unseen; one that finds no
// client online waits for the next; typing goes through and to chat; and a
// message to oneself goes nowhere but one's other clients.
func TestXMPPMessages(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	a, b := env.pair(t)

	a.send(t, `<message xmlns="jabber:client" to="bob@example.com" type="chat" id="m1"><body>Hello Bob</body></message>`)
	env.log.find(t, "message", func(e map[string]any) bool {
		return e["account"] == "x1" && e["own"] == true && e["conversation"] == "xmpp:bob@example.com" && e["body"] == "Hello Bob"
	})
	b.silent(t, "THE POINT: a stranger's message reached bob though his chat does not take it", "Hello Bob")
	if len(fake.called("linked/accept", "user", "u-bob", "person", "xmpp:alice@example.com")) != 1 {
		t.Errorf("bob's chat was asked %d times", len(fake.called("linked/accept", "user", "u-bob")))
	}

	fake.allow(env.bob, "alice@example.com")
	a.send(t, `<message xmlns="jabber:client" to="bob@example.com" type="chat" id="m2"><body>Hello again</body><markable xmlns="urn:xmpp:chat-markers:0"/></message>`)
	got := b.expect(t, "THE POINT: alice's message at bob's client", "Hello again")
	if !strings.Contains(got, `from="`+a.full.String()+`"`) {
		t.Errorf("message = %s", got)
	}
	env.log.find(t, "message", func(e map[string]any) bool {
		return e["account"] == "x2" && e["own"] == false && e["conversation"] == "xmpp:alice@example.com" && e["from"] == "xmpp:alice@example.com" && e["name"] == "alice" && e["id"] == "m2"
	})

	// Typing reaches bob and his chat.
	a.send(t, `<message xmlns="jabber:client" to="bob@example.com" type="chat"><composing xmlns="http://jabber.org/protocol/chatstates"/></message>`)
	b.expect(t, "the typing", "composing")
	env.log.find(t, "typing", func(e map[string]any) bool { return e["account"] == "x2" && e["typing"] == true })

	// Bob's client goes; the next message waits for the one after.
	b.session.Close()
	wait(t, "bob's client to go", func() bool { return xmpp_resource_get("bob@example.com", b.full.Resourcepart()) == nil })
	a.send(t, `<message xmlns="jabber:client" to="bob@example.com" type="chat" id="m3"><body>While away</body></message>`)
	wait(t, "the message kept", func() bool {
		found, _ := xmpp_database().exists("select 1 from offline where account='x2'")
		return found
	})
	c, err := env.connect(t, "bob@example.com", env.mint(t, env.bob, "x2", "Laptop"), "")
	if err != nil {
		t.Fatal(err)
	}
	c.silent(t, "a kept message before the client was online", "While away")
	c.online(t)
	kept := c.expect(t, "THE POINT: the kept message on coming online", "While away")
	if !strings.Contains(kept, "urn:xmpp:delay") {
		t.Errorf("kept message carries no delay: %s", kept)
	}
	if found, _ := xmpp_database().exists("select 1 from offline where account='x2'"); found {
		t.Error("the kept message was kept after it was handed over")
	}

	// To oneself: alice's other client, and not chat.
	before := len(env.log.events)
	a2, err := env.connect(t, "alice@example.com", env.mint(t, env.alice, "x1", "Laptop"), "")
	if err != nil {
		t.Fatal(err)
	}
	a2.online(t)
	a.expect(t, "the other client online", "<presence", `from="`+a2.full.String()+`"`)
	a.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="m4"><body>Note to self</body></message>`)
	a2.expect(t, "the note at the other client", "Note to self")
	time.Sleep(200 * time.Millisecond)
	env.log.lock.Lock()
	for _, e := range env.log.events[before:] {
		if e["body"] == "Note to self" {
			t.Errorf("a note to self went to chat: %v", e)
		}
	}
	env.log.lock.Unlock()
}

// TestXMPPSubscriptions: a subscription request between two accounts waits
// in the other's client and in their people, and approving it lets presence
// through one way, then both; a contact's presence reaches the client and
// chat, by its most present resource; a client coming online asks for its
// contacts' presence; and ending it all takes the contact off the roster.
func TestXMPPSubscriptions(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	a, b := env.pair(t)

	a.send(t, `<presence xmlns="jabber:client" to="bob@example.com" type="subscribe"/>`)
	b.expect(t, "THE POINT: the request at bob's client", `type="subscribe"`, `from="alice@example.com"`)
	a.expect(t, "the roster push of the request", "jabber:iq:roster", `ask="subscribe"`)
	wait(t, "THE POINT: bob's people told of the invitation", func() bool {
		return len(fake.called("xmpp/roster", "user", "u-bob", "address", "alice@example.com", "event", "invite", "pending", true)) == 1
	})

	b.send(t, `<presence xmlns="jabber:client" to="alice@example.com" type="subscribed"/>`)
	a.expect(t, "the approval", `type="subscribed"`, `from="bob@example.com"`)
	a.expect(t, "the roster push of the approval", "jabber:iq:roster", `subscription="to"`)
	a.expect(t, "THE POINT: bob's presence, once alice may see it", `from="`+b.full.String()+`"`)
	wait(t, "alice's people told of the acceptance", func() bool {
		return len(fake.called("xmpp/roster", "user", "u-alice", "address", "bob@example.com", "event", "accept", "subscription", "to")) == 1
	})
	if item := xmpp_roster_item(&xmpp_account{user: env.bob, id: "x2"}, "alice@example.com"); item == nil || item.subscription != "from" || item.pending {
		t.Errorf("bob's item = %+v", item)
	}

	// Bob's presence changes: alice and her chat are told; alice's own does
	// not reach bob, who does not see it yet.
	b.send(t, `<presence xmlns="jabber:client"><show>away</show><status>Lunch</status></presence>`)
	a.expect(t, "bob away", "away</show>")
	env.log.find(t, "presence", func(e map[string]any) bool {
		return e["account"] == "x1" && e["from"] == "xmpp:bob@example.com" && e["status"] == "away" && e["message"] == "Lunch"
	})
	a.send(t, `<presence xmlns="jabber:client"><show>dnd</show></presence>`)
	b.silent(t, "THE POINT: alice's presence reached bob, who is not subscribed to it", "dnd</show>")

	// The other way: both.
	b.send(t, `<presence xmlns="jabber:client" to="alice@example.com" type="subscribe"/>`)
	a.expect(t, "bob's request", `type="subscribe"`, `from="bob@example.com"`)
	a.send(t, `<presence xmlns="jabber:client" to="bob@example.com" type="subscribed"/>`)
	b.expect(t, "the roster push", "jabber:iq:roster", `subscription="both"`)
	b.expect(t, "alice's presence once bob may see it", "dnd</show>")

	// A new client of alice's asks for her contacts' presence.
	a2, err := env.connect(t, "alice@example.com", env.mint(t, env.alice, "x1", "Laptop"), "")
	if err != nil {
		t.Fatal(err)
	}
	a2.online(t)
	a2.expect(t, "THE POINT: bob's presence, asked for on coming online", `from="`+b.full.String()+`"`, "away</show>")

	// The roster as the client asks for it.
	a.send(t, `<iq xmlns="jabber:client" type="get" id="r1"><query xmlns="jabber:iq:roster"/></iq>`)
	roster := a.expect(t, "the roster", `id="r1"`)
	if !strings.Contains(roster, `jid="bob@example.com"`) || !strings.Contains(roster, `subscription="both"`) {
		t.Errorf("roster = %s", roster)
	}

	// Bob goes; alice and her chat hear he is offline.
	b.send(t, `<presence xmlns="jabber:client" type="unavailable"/>`)
	env.log.find(t, "presence", func(e map[string]any) bool { return e["account"] == "x1" && e["status"] == "offline" })

	// Alice removes bob: both subscriptions end, he is told, and off he goes.
	b.online(t)
	a.send(t, `<iq xmlns="jabber:client" type="set" id="r2"><query xmlns="jabber:iq:roster"><item jid="bob@example.com" subscription="remove"/></query></iq>`)
	a.expect(t, "the removal pushed", `subscription="remove"`)
	b.expect(t, "THE POINT: bob told alice withdrew", `type="unsubscribed"`, `from="alice@example.com"`)
	if item := xmpp_roster_item(&xmpp_account{user: env.alice, id: "x1"}, "bob@example.com"); item != nil {
		t.Errorf("alice still has bob: %+v", item)
	}
	if item := xmpp_roster_item(&xmpp_account{user: env.bob, id: "x2"}, "alice@example.com"); item != nil && item.subscription != "none" {
		t.Errorf("bob's item after the removal = %+v", item)
	}
}

// TestXMPPAccepted: someone on the roster writes without chat being asked;
// a stranger is asked of chat once a minute, and the user writing to them
// asks again.
func TestXMPPAccepted(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	bob := &xmpp_account{user: env.bob, id: "x2"}
	alice := mustjid("alice@example.com")
	if xmpp_accepted(bob, alice) || xmpp_accepted(bob, alice) {
		t.Fatal("a stranger was taken")
	}
	if n := len(fake.called("linked/accept")); n != 1 {
		t.Errorf("chat was asked %d times in the minute", n)
	}
	xmpp_accepted_forget(bob, alice)
	fake.lock.Lock()
	fake.accept["u-bob xmpp:alice@example.com"] = true
	fake.lock.Unlock()
	if !xmpp_accepted(bob, alice) {
		t.Error("THE POINT: once forgotten, chat's new answer was not asked")
	}
	xmpp_roster_save(bob, &xmpp_item{address: "carol@remote.test", subscription: "to"})
	before := len(fake.called("linked/accept"))
	if !xmpp_accepted(bob, mustjid("carol@remote.test")) || len(fake.called("linked/accept")) != before {
		t.Error("THE POINT: a contact was not taken without asking chat")
	}
}

// TestXMPPFriends: the user's friends with an XMPP address are on the roster
// both ways - by their contact card, or by their account here - and come and
// go with the friendship.
func TestXMPPFriends(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.friends["u-alice"] = []any{map[string]any{"person": "e-bob", "name": "Bob", "addresses": []any{}}, map[string]any{"person": "e-remote", "name": "Carol", "addresses": []any{"xmpp:carol@remote.test"}}}
	fake.friends["u-bob"] = []any{map[string]any{"person": "e-alice", "name": "Alice", "addresses": []any{}}}
	a, b := env.pair(t)
	a.send(t, `<iq xmlns="jabber:client" type="get" id="r1"><query xmlns="jabber:iq:roster"/></iq>`)
	roster := a.expect(t, "the roster", `id="r1"`)
	for _, want := range []string{`jid="bob@example.com" subscription="both" name="Bob"`, `jid="carol@remote.test" subscription="both" name="Carol"`} {
		if !strings.Contains(roster, want) {
			t.Errorf("THE POINT: roster lacks %s: %s", want, roster)
		}
	}
	b.send(t, `<presence xmlns="jabber:client"><show>chat</show></presence>`)
	a.expect(t, "a friend's presence", `from="`+b.full.String()+`"`, "chat</show>")
	a.send(t, `<iq xmlns="jabber:client" type="set" id="r2"><query xmlns="jabber:iq:roster"><item jid="bob@example.com" subscription="remove"/></query></iq>`)
	a.expect(t, "a friend cannot be removed here", `id="r2"`, "not-allowed")

	fake.lock.Lock()
	fake.friends["u-alice"] = fake.friends["u-alice"][1:]
	fake.lock.Unlock()
	xmpp_friends_changed(env.alice)
	a.expect(t, "THE POINT: the friendship's end pushed", `jid="bob@example.com" subscription="remove"`)
}

// TestXMPPBlocking: a client blocks and unblocks people and domains on the
// user's own block list, which every client of the account is told of; a
// blocked person's message is refused and the user's to them too.
func TestXMPPBlocking(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	a, b := env.pair(t)
	a.send(t, `<iq xmlns="jabber:client" type="set" id="k1"><block xmlns="urn:xmpp:blocking"><item jid="bob@example.com"/><item jid="spam.test"/></block></iq>`)
	a.expect(t, "the block answered", `id="k1"`, `type="result"`)
	a.expect(t, "the block pushed", "<block", "urn:xmpp:blocking")
	if !actor_blocked(env.alice, "xmpp:bob@example.com") || !blocked_by(db_user(env.alice, "user"), "spam.test") {
		t.Fatal("THE POINT: the blocks are not on the user's block list")
	}
	a.send(t, `<iq xmlns="jabber:client" type="get" id="k2"><blocklist xmlns="urn:xmpp:blocking"/></iq>`)
	list := a.expect(t, "the block list", `id="k2"`)
	if !strings.Contains(list, `jid="bob@example.com"`) || !strings.Contains(list, `jid="spam.test"`) {
		t.Errorf("block list = %s", list)
	}

	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="x1"><body>Blocked hello</body></message>`)
	b.expect(t, "THE POINT: bob's message refused", `id="x1"`, `type="error"`, "service-unavailable")
	a.silent(t, "a blocked message reached alice", "Blocked hello")
	a.send(t, `<message xmlns="jabber:client" to="bob@example.com" type="chat" id="x2"><body>To blocked</body></message>`)
	a.expect(t, "alice's own message to bob refused", `id="x2"`, "urn:xmpp:blocking:errors")
	b.silent(t, "alice's message reached the person she blocked", "To blocked")

	a.send(t, `<iq xmlns="jabber:client" type="set" id="k3"><unblock xmlns="urn:xmpp:blocking"/></iq>`)
	a.expect(t, "the unblock answered", `id="k3"`, `type="result"`)
	if actor_blocked(env.alice, "xmpp:bob@example.com") || blocked_by(db_user(env.alice, "user"), "spam.test") {
		t.Error("unblocking all left blocks")
	}
	if target, ok := actor_target(" xmpp:Bob@Example.com "); !ok || target != "xmpp:bob@example.com" {
		t.Errorf("an XMPP address as a block target = %q %v", target, ok)
	}
}

// TestXMPPAnswers: the server answers an account's vCard - its name and the
// user's avatar - to its own client and to anyone asking, and refuses a
// change; discovery of the server, of an account and of Mochi's own
// sessions, and pings.
func TestXMPPAnswers(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.avatar = []byte("\x89PNG avatar")
	a, b := env.pair(t)
	a.send(t, `<iq xmlns="jabber:client" type="get" id="v1"><vCard xmlns="vcard-temp"/></iq>`)
	card := a.expect(t, "alice's own vCard", `id="v1"`)
	if !strings.Contains(card, "Alice</FN>") || !strings.Contains(card, "image/png</TYPE>") {
		t.Errorf("THE POINT: vCard = %s", card)
	}
	b.send(t, `<iq xmlns="jabber:client" type="get" id="v2" to="alice@example.com"><vCard xmlns="vcard-temp"/></iq>`)
	if got := b.expect(t, "alice's vCard to bob", `id="v2"`); !strings.Contains(got, "Alice</FN>") || !strings.Contains(got, `from="alice@example.com"`) {
		t.Errorf("vCard to bob = %s", got)
	}
	a.send(t, `<iq xmlns="jabber:client" type="set" id="v3"><vCard xmlns="vcard-temp"><FN>Mallory</FN></vCard></iq>`)
	a.expect(t, "a change refused", `id="v3"`, "not-allowed")

	a.send(t, `<iq xmlns="jabber:client" type="get" id="d1" to="example.com"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`)
	if got := a.expect(t, "the server's features", `id="d1"`); !strings.Contains(got, `category="server"`) || !strings.Contains(got, "urn:xmpp:blocking") {
		t.Errorf("server = %s", got)
	}
	b.send(t, `<iq xmlns="jabber:client" type="get" id="d2" to="alice@example.com"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`)
	b.expect(t, "the account", `id="d2"`, `category="account"`)
	b.send(t, `<iq xmlns="jabber:client" type="get" id="d3" to="alice@example.com/mochi"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`)
	b.expect(t, "Mochi's own sessions as a client", `id="d3"`, "urn:xmpp:chat-markers:0")
	a.send(t, `<iq xmlns="jabber:client" type="get" id="p1" to="example.com"><ping xmlns="urn:xmpp:ping"/></iq>`)
	a.expect(t, "the ping", `id="p1"`, `type="result"`)
	a.send(t, `<iq xmlns="jabber:client" type="get" id="n1" to="notices@example.com"><vCard xmlns="vcard-temp"/></iq>`)
	a.expect(t, "the notices address's name", `id="n1"`, "Mochi</FN>")
}

// TestXMPPHostedConversation: chat holds a conversation through a hosted
// account as through a linked one - messages, corrections, withdrawals,
// typing, read markers and presence go out from Mochi's own sessions - and
// a file shared with it is fetched.
func TestXMPPHostedConversation(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.bob, "alice@example.com")
	fake.allow(env.alice, "bob@example.com")
	_, b := env.pair(t)
	thread := linked_thread(env.alice)
	fn := sl.NewBuiltin("test", nil)
	call := func(f func(*sl.Thread, *sl.Builtin, sl.Tuple, []sl.Tuple) (sl.Value, error), args sl.Tuple, kwargs ...sl.Tuple) sl.Value {
		t.Helper()
		value, err := f(thread, fn, args, kwargs)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	object := func(pairs ...string) *sl.Dict {
		d := sl.NewDict(len(pairs) / 2)
		for i := 0; i+1 < len(pairs); i += 2 {
			d.SetKey(sl.String(pairs[i]), sl.String(pairs[i+1]))
		}
		return d
	}
	conversation := sl.String("xmpp:bob@example.com")

	created := sl_decode(call(api_account_object_create, sl.Tuple{sl.String("x1"), object("conversation", "xmpp:bob@example.com", "content", "From Mochi")}, sl.Tuple{sl.String("key"), sl.String("chat|k1")})).(map[string]any)
	if created["id"] != "chat|k1" {
		t.Errorf("created = %v", created)
	}
	got := b.expect(t, "THE POINT: chat's message at bob's client", "From Mochi")
	if !strings.Contains(got, `from="alice@example.com/mochi"`) || !strings.Contains(got, `id="chat|k1"`) {
		t.Errorf("message = %s", got)
	}
	call(api_account_object_update, sl.Tuple{sl.String("x1"), sl.String("chat|k1"), object("conversation", "xmpp:bob@example.com", "content", "From Mochi, corrected")})
	b.expect(t, "the correction", `urn:xmpp:message-correct:0`, "corrected")
	call(api_account_object_delete, sl.Tuple{sl.String("x1"), sl.String("chat|k1")}, sl.Tuple{sl.String("conversation"), conversation})
	b.expect(t, "the withdrawal", "urn:xmpp:message-retract:1", `id="chat|k1"`)

	call(api_account_typing, sl.Tuple{sl.String("x1"), conversation})
	b.expect(t, "typing", "<composing")
	b.expect(t, "the pause after it", "<paused")

	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="b1"><body>Read me</body><markable xmlns="urn:xmpp:chat-markers:0"/></message>`)
	env.log.find(t, "message", func(e map[string]any) bool { return e["account"] == "x1" && e["id"] == "b1" })
	call(api_account_read, sl.Tuple{sl.String("x1"), conversation, sl.String("other")})
	b.silent(t, "a marker for a message that did not ask for one", "<displayed")
	call(api_account_read, sl.Tuple{sl.String("x1"), conversation, sl.String("b1")})
	b.expect(t, "THE POINT: the read marker", "<displayed", `id="b1"`)

	made := sl_decode(call(api_account_room_create, sl.Tuple{sl.String("x1"), sl.NewList([]sl.Value{sl.String("Bob@Example.com")})})).(map[string]any)
	if made["conversation"] != "xmpp:bob@example.com" || made["room"] != false {
		t.Errorf("one-to-one = %v", made)
	}
	files := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("png bytes"))
	}))
	defer files.Close()
	fetched := sl_decode(call(api_account_media, sl.Tuple{sl.String("x1"), sl.String(files.URL + "/a.png")})).(map[string]any)
	if fetched["status"] != int64(200) || string(repository_bytes(fetched["data"])) != "png bytes" {
		t.Errorf("THE POINT: fetched = %v", fetched)
	}

	// Presence: Mochi's own sessions show the user while they are in Mochi
	// and share it, to contacts who may see it.
	xmpp_roster_save(&xmpp_account{user: env.alice, id: "x1"}, &xmpp_item{address: "bob@example.com", subscription: "from", listed: true})
	xmpp_roster_save(&xmpp_account{user: env.bob, id: "x2"}, &xmpp_item{address: "alice@example.com", subscription: "to", listed: true})
	presence_connect(env.alice.UID, "web1", "", "web", "")
	b.expect(t, "THE POINT: Mochi's own sessions online", "<presence", `from="alice@example.com/mochi"`, "http://jabber.org/protocol/caps")
	call(api_account_presence, sl.Tuple{sl.String("x1"), sl.False})
	b.expect(t, "THE POINT: unavailable once not shared", "<presence", `from="alice@example.com/mochi"`, `type="unavailable"`)
	presence_disconnect(env.alice.UID, "web1")
}

// TestXMPPRosterAPI: people adds a contact - its request sent, or the other's
// approved - and removes one, ending both ways; the list shows each state.
func TestXMPPRosterAPI(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.apps(t)
	_, b := env.pair(t)
	app := create_external_app("people1")
	permission_grant(env.alice, app.id, "roster/read")
	permission_grant(env.alice, app.id, "roster/write")
	thread := create_test_thread(env.alice, app)
	fn := sl.NewBuiltin("test", nil)
	list := func() map[string]any {
		t.Helper()
		value, err := api_account_roster_list(thread, fn, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return sl_decode(value).(map[string]any)
	}
	if accounts := list()["accounts"].([]any); len(accounts) != 1 || accounts[0].(map[string]any)["address"] != "alice@example.com" {
		t.Errorf("accounts = %v", accounts)
	}
	if _, err := api_account_roster_add(thread, fn, sl.Tuple{sl.String("x1"), sl.String("xmpp:bob@example.com"), sl.String("Bob")}, nil); err != nil {
		t.Fatal(err)
	}
	b.expect(t, "THE POINT: the request people sent", `type="subscribe"`, `from="alice@example.com"`)
	items := list()["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["ask"] != true || items[0].(map[string]any)["name"] != "Bob" {
		t.Errorf("items = %v", items)
	}
	if _, err := api_account_roster_remove(thread, fn, sl.Tuple{sl.String("x1"), sl.String("bob@example.com")}, nil); err != nil {
		t.Fatal(err)
	}
	b.expect(t, "the request withdrawn", `type="unsubscribe"`, `from="alice@example.com"`)
	if items := list()["items"].([]any); len(items) != 0 {
		t.Errorf("items after removal = %v", items)
	}
	other := create_external_app("other1")
	if _, err := api_account_roster_list(create_test_thread(env.alice, other), fn, nil, nil); err == nil {
		t.Error("an app without the permission read the roster")
	}
}

// mustjid parses an address known to be good.
func mustjid(text string) jid.JID {
	return jid.MustParse(text)
}

// TestXMPPReactions: a reaction (XEP-0444) to a message reaches chat as a
// reaction event, by the message it is to, its first emoji the one kept and
// none withdrawing it; chat's reaction through a hosted account goes as the
// person's whole set, known by the message, and withdrawing it sends none.
func TestXMPPReactions(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	fake.allow(env.bob, "alice@example.com")
	_, b := env.pair(t)

	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="r1"><reactions xmlns="urn:xmpp:reactions:0" id="m1"><reaction>👍</reaction><reaction>😂</reaction></reactions></message>`)
	event := env.log.find(t, "reaction", func(e map[string]any) bool { return e["account"] == "x1" && e["id"] == "m1" })
	if event["reaction"] != "👍" || event["from"] != "xmpp:bob@example.com" || event["event"] != "reaction|m1" {
		t.Errorf("THE POINT: the reaction: %v", event)
	}
	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="r2"><reactions xmlns="urn:xmpp:reactions:0" id="m2"/></message>`)
	withdrawn := env.log.find(t, "reaction", func(e map[string]any) bool { return e["account"] == "x1" && e["id"] == "m2" })
	if withdrawn["reaction"] != "" {
		t.Errorf("THE POINT: a withdrawn reaction: %v", withdrawn)
	}

	result := sl_decode(xmpp_call(t, env.alice, api_account_object_create, sl.Tuple{sl.String("x1"), xmpp_object("conversation", "xmpp:bob@example.com", "reaction", "😍", "message", "m3")})).(map[string]any)
	if result["id"] != "reaction|m3" {
		t.Errorf("the reaction's id: %v", result)
	}
	b.expect(t, "THE POINT: chat's reaction", `urn:xmpp:reactions:0`, `id="m3"`, "😍</reaction>")
	xmpp_call(t, env.alice, api_account_object_delete, sl.Tuple{sl.String("x1"), sl.String("reaction|m3")}, sl.Tuple{sl.String("conversation"), sl.String("xmpp:bob@example.com")})
	wait(t, "THE POINT: the reaction withdrawn", func() bool {
		for _, text := range b.heard(`urn:xmpp:reactions:0`, `id="m3"`) {
			if !strings.Contains(text, "</reaction>") {
				return true
			}
		}
		return false
	})
}
