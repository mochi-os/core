// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
	"nhooyr.io/websocket"
)

// saved_inbound_dispatch is linked_inbound as core has it, before a test
// replaces it.
var saved_inbound_dispatch = linked_inbound

// xml_attribute reads an attribute of a stanza's first element.
func xml_attribute(text string, name string) string {
	head := text
	if i := strings.IndexByte(text, '>'); i >= 0 {
		head = text[:i]
	}
	match := regexp.MustCompile(`\b` + name + `="([^"]*)"`).FindStringSubmatch(head)
	if match == nil {
		return ""
	}
	return match[1]
}

// push sends a stanza from the fake server on its latest session.
func (f *xmpp_fake) push(t *testing.T, text string) {
	t.Helper()
	f.lock.Lock()
	session := f.sessions[len(f.sessions)-1]
	f.lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := session.Send(ctx, xml.NewDecoder(strings.NewReader(text))); err != nil {
		t.Fatalf("push: %v", err)
	}
}

// waits answers when the fake has heard a stanza with every part given.
func (f *xmpp_fake) await(t *testing.T, what string, parts ...string) string {
	t.Helper()
	var found string
	wait(t, what, func() bool {
		heard := f.heard(parts...)
		if len(heard) > 0 {
			found = heard[len(heard)-1]
			return true
		}
		return false
	})
	return found
}

// events answers the events logged that match a condition.
func (log *inbound_log) matching(condition func(map[string]any) bool) []map[string]any {
	log.lock.Lock()
	defer log.lock.Unlock()
	var out []map[string]any
	for _, event := range log.events {
		if condition(event) {
			out = append(out, event)
		}
	}
	return out
}

// TestXMPPConversations: a linked XMPP account's session reads the archive on
// its first connection (refusing a result from anyone but the account), joins
// its bookmarked rooms, and hands each kind of message, receipt, marker, typing,
// correction, retraction, encrypted message, file, carbon (refusing a forged
// one), invitation and presence on as conversation events, answering a
// receipt asked for and a ping. The functions send messages, a room's echo of
// which is only the room's id, corrections, retractions, typing with its
// pause, a read marker only for the message marked as wanting it and only when
// shared, the user's presence as extended away once hidden, and leave a room.
func TestXMPPConversations(t *testing.T) {
	alice := create_linked_test_env(t)
	saved := xmpp_pause
	xmpp_pause = 150 * time.Millisecond
	defer func() { xmpp_pause = saved }()
	room := "room@rooms.example.org"
	fake := create_xmpp_fake(t, "secret")
	fake.answer = func(text string) []string {
		id := xml_attribute(text, "id")
		switch {
		case strings.HasPrefix(text, "<iq") && strings.Contains(text, "urn:xmpp:mam:2"):
			query := regexp.MustCompile(`queryid="([^"]+)"`).FindStringSubmatch(text)[1]
			return []string{
				`<message xmlns="jabber:client" from="alice@example.org"><result xmlns="urn:xmpp:mam:2" queryid="` + query + `" id="a1"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-02T03:04:05Z"/><message xmlns="jabber:client" from="bob@example.org/phone" to="alice@example.org" type="chat" id="old1"><body>earlier</body></message></forwarded></result></message>`,
				`<message xmlns="jabber:client" from="mallory@evil.org"><result xmlns="urn:xmpp:mam:2" queryid="` + query + `" id="evil"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.org/phone" to="alice@example.org" type="chat" id="old9"><body>forged archive</body></message></forwarded></result></message>`,
				`<message xmlns="jabber:client" from="alice@example.org"><result xmlns="urn:xmpp:mam:2" queryid="` + query + `" id="a2"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-02T03:05:00Z"/><message xmlns="jabber:client" from="alice@example.org/laptop" to="bob@example.org" type="chat" id="old2"><body>my reply</body></message></forwarded></result></message>`,
				`<iq xmlns="jabber:client" type="result" id="` + id + `"><fin xmlns="urn:xmpp:mam:2" complete="true"><set xmlns="http://jabber.org/protocol/rsm"><last>a2</last></set></fin></iq>`,
			}
		case strings.HasPrefix(text, "<iq") && strings.Contains(text, "urn:xmpp:bookmarks:1") && strings.Contains(text, `type="get"`):
			return []string{`<iq xmlns="jabber:client" type="result" id="` + id + `"><pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="urn:xmpp:bookmarks:1"><item id="` + room + `"><conference xmlns="urn:xmpp:bookmarks:1" name="The room" autojoin="true"><nick>alice</nick></conference></item></items></pubsub></iq>`}
		case strings.HasPrefix(text, "<iq"):
			return []string{`<iq xmlns="jabber:client" type="result" id="` + id + `"/>`}
		case strings.HasPrefix(text, "<presence") && strings.Contains(text, room+"/alice") && !strings.Contains(text, "unavailable"):
			return []string{`<presence xmlns="jabber:client" from="` + room + `/alice"><x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/></x></presence>`}
		}
		return nil
	}
	log := &inbound_log{}
	log.install(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)

	account := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("xmpp")}, linked_kwargs("address", "alice@example.org", "password", "secret")))
	id := account["id"].(string)
	wait(t, "the connection", func() bool { connected, _ := linked_connected(alice.UID, id); return connected })
	log.find(t, "session", nil)

	// The archive on the first connection: what came, what the account sent
	// from elsewhere, and nothing from anyone but the account.
	earlier := log.find(t, "message", func(e map[string]any) bool { return e["body"] == "earlier" })
	stamp, _ := time.Parse(time.RFC3339, "2026-01-02T03:04:05Z")
	if earlier["conversation"] != "xmpp:bob@example.org" || earlier["from"] != "xmpp:bob@example.org" || earlier["archive"] != "a1" || earlier["created"] != stamp.Unix() || earlier["own"] != false {
		t.Errorf("archived message = %v", earlier)
	}
	reply := log.find(t, "message", func(e map[string]any) bool { return e["body"] == "my reply" })
	if reply["own"] != true || reply["conversation"] != "xmpp:bob@example.org" || reply["from"] != "xmpp:alice@example.org" {
		t.Errorf("archived own message = %v", reply)
	}
	wait(t, "the archive position", func() bool { return linked_get(alice, id).get("archive") == "a2" })
	if forged := log.matching(func(e map[string]any) bool { return e["body"] == "forged archive" }); len(forged) > 0 {
		t.Errorf("a forged archive result was taken: %v", forged)
	}

	// The bookmarked room is joined and named.
	joined := log.find(t, "room", nil)
	if joined["conversation"] != "xmpp:"+room || joined["room"] != true || joined["name"] != "The room" {
		t.Errorf("room = %v", joined)
	}

	fake.push(t, `<message xmlns="jabber:client" type="chat" id="m1" from="bob@example.org/phone" to="alice@example.org/fake"><body>hello</body><markable xmlns="urn:xmpp:chat-markers:0"/><request xmlns="urn:xmpp:receipts"/></message>`)
	fake.push(t, `<message xmlns="jabber:client" type="chat" from="bob@example.org/phone"><composing xmlns="http://jabber.org/protocol/chatstates"/></message>`)
	fake.push(t, `<message xmlns="jabber:client" type="chat" id="m1b" from="bob@example.org/phone"><body>hello again</body><replace xmlns="urn:xmpp:message-correct:0" id="m1"/></message>`)
	fake.push(t, `<message xmlns="jabber:client" type="chat" id="r1" from="bob@example.org/phone"><retract xmlns="urn:xmpp:message-retract:1" id="m1"/></message>`)
	fake.push(t, `<message xmlns="jabber:client" type="chat" id="d1" from="bob@example.org/phone"><displayed xmlns="urn:xmpp:chat-markers:0" id="x1"/></message>`)
	fake.push(t, `<message xmlns="jabber:client" type="chat" id="m2" from="bob@example.org/phone"><body>I sent you an OMEMO encrypted message</body><encrypted xmlns="eu.siacs.conversations.axolotl"><payload>abc</payload></encrypted></message>`)
	fake.push(t, `<message xmlns="jabber:client" type="chat" id="m3" from="bob@example.org/phone"><body>https://files.example.org/a/photo%20one.jpg</body><x xmlns="jabber:x:oob"><url>https://files.example.org/a/photo%20one.jpg</url></x></message>`)
	fake.push(t, `<message xmlns="jabber:client" from="alice@example.org" to="alice@example.org/fake"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="alice@example.org/phone" to="bob@example.org" type="chat" id="c1"><body>from my phone</body></message></forwarded></sent></message>`)
	fake.push(t, `<message xmlns="jabber:client" from="mallory@evil.org" to="alice@example.org/fake"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="alice@example.org/phone" to="bob@example.org" type="chat" id="f1"><body>forged carbon</body></message></forwarded></sent></message>`)
	fake.push(t, `<message xmlns="jabber:client" id="i1" from="carol@example.org/pc"><x xmlns="jabber:x:conference" jid="club@rooms.example.org"/></message>`)
	fake.push(t, `<message xmlns="jabber:client" type="groupchat" id="g1" from="`+room+`/dave"><body>hi all</body><stanza-id xmlns="urn:xmpp:sid:0" by="`+room+`" id="rs1"/></message>`)
	fake.push(t, `<presence xmlns="jabber:client" from="bob@example.org/phone"><show>dnd</show><status>In a meeting</status></presence>`)
	fake.push(t, `<iq xmlns="jabber:client" type="get" id="ping1" from="example.org"><ping xmlns="urn:xmpp:ping"/></iq>`)

	message := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "m1" })
	if message["body"] != "hello" || message["from"] != "xmpp:bob@example.org" || message["name"] != "bob" || message["room"] != false {
		t.Errorf("message = %v", message)
	}
	fake.await(t, "the receipt asked for", "urn:xmpp:receipts", "received", `id="m1"`, "bob@example.org/phone")
	if typing := log.find(t, "typing", nil); typing["typing"] != true || typing["from"] != "xmpp:bob@example.org" {
		t.Errorf("typing = %v", typing)
	}
	if edit := log.find(t, "edit", nil); edit["id"] != "m1" || edit["body"] != "hello again" {
		t.Errorf("edit = %v", edit)
	}
	if deleted := log.find(t, "delete", nil); deleted["id"] != "m1" || deleted["from"] != "xmpp:bob@example.org" {
		t.Errorf("delete = %v", deleted)
	}
	if receipt := log.find(t, "receipt", nil); receipt["id"] != "x1" || receipt["displayed"] != true {
		t.Errorf("receipt = %v", receipt)
	}
	if encrypted := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "m2" }); encrypted["encrypted"] != true || encrypted["body"] != "" {
		t.Errorf("encrypted message = %v", encrypted)
	}
	file := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "m3" })
	files, _ := file["files"].([]any)
	if file["body"] != "" || len(files) != 1 || files[0].(map[string]any)["name"] != "photo one.jpg" {
		t.Errorf("file message = %v", file)
	}
	if carbon := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "c1" }); carbon["own"] != true || carbon["conversation"] != "xmpp:bob@example.org" || carbon["body"] != "from my phone" {
		t.Errorf("carbon = %v", carbon)
	}
	if invite := log.find(t, "invite", nil); invite["conversation"] != "xmpp:club@rooms.example.org" || invite["from"] != "xmpp:carol@example.org" {
		t.Errorf("invite = %v", invite)
	}
	if group := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "g1" }); group["room"] != true || group["conversation"] != "xmpp:"+room || group["from"] != "xmpp:"+room+"/dave" || group["archive"] != "rs1" {
		t.Errorf("room message = %v", group)
	}
	if presence := log.find(t, "presence", nil); presence["status"] != "busy" || presence["message"] != "In a meeting" {
		t.Errorf("presence = %v", presence)
	}
	fake.await(t, "the ping answered", `id="ping1"`, `type="result"`)
	if forged := log.matching(func(e map[string]any) bool { return e["body"] == "forged carbon" }); len(forged) > 0 {
		t.Errorf("a forged carbon was taken: %v", forged)
	}

	// Sending.
	conversation := "xmpp:bob@example.org"
	object := sl.NewDict(2)
	object.SetKey(sl.String("conversation"), sl.String(conversation))
	object.SetKey(sl.String("content"), sl.String("hi bob"))
	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String(id), object}, []sl.Tuple{{sl.String("key"), sl.String("k1")}}); err != nil {
		t.Fatal(err)
	}
	sent := fake.await(t, "the message", "hi bob")
	for _, part := range []string{`to="bob@example.org"`, `type="chat"`, `id="k1"`, "markable", "urn:xmpp:receipts", "origin-id"} {
		if !strings.Contains(sent, part) {
			t.Errorf("sent message lacks %s: %s", part, sent)
		}
	}
	grouped := sl.NewDict(2)
	grouped.SetKey(sl.String("conversation"), sl.String("xmpp:"+room))
	grouped.SetKey(sl.String("content"), sl.String("hi room"))
	if _, err := api_account_object_create(thread, fn, sl.Tuple{sl.String(id), grouped}, []sl.Tuple{{sl.String("key"), sl.String("k2")}}); err != nil {
		t.Fatal(err)
	}
	fake.await(t, "the room message", "hi room", `type="groupchat"`, `id="k2"`)
	fake.push(t, `<message xmlns="jabber:client" type="groupchat" id="k2" from="`+room+`/alice"><body>hi room</body><stanza-id xmlns="urn:xmpp:sid:0" by="`+room+`" id="rs2"/></message>`)
	if echo := log.find(t, "sent", nil); echo["id"] != "k2" || echo["archive"] != "rs2" || echo["conversation"] != "xmpp:"+room {
		t.Errorf("room echo = %v", echo)
	}
	if again := log.matching(func(e map[string]any) bool { return e["kind"] == "message" && e["id"] == "k2" }); len(again) > 0 {
		t.Errorf("the room's echo came back as a message: %v", again)
	}
	edited := sl.NewDict(2)
	edited.SetKey(sl.String("conversation"), sl.String(conversation))
	edited.SetKey(sl.String("content"), sl.String("hi again"))
	if _, err := api_account_object_update(thread, fn, sl.Tuple{sl.String(id), sl.String("k1"), edited}, nil); err != nil {
		t.Fatal(err)
	}
	fake.await(t, "the correction", "hi again", "message-correct", `id="k1"`)
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String(id), sl.String("k1")}, []sl.Tuple{{sl.String("conversation"), sl.String(conversation)}}); err != nil {
		t.Fatal(err)
	}
	fake.await(t, "the retraction", "message-retract:1", `id="k1"`)

	if _, err := api_account_typing(thread, fn, sl.Tuple{sl.String(id), sl.String(conversation)}, nil); err != nil {
		t.Fatal(err)
	}
	fake.await(t, "composing", "composing", `to="bob@example.org"`)
	fake.await(t, "the pause after typing", "paused", `to="bob@example.org"`)

	if _, err := api_account_read(thread, fn, sl.Tuple{sl.String(id), sl.String(conversation), sl.String("m3")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := api_account_read(thread, fn, sl.Tuple{sl.String(id), sl.String(conversation), sl.String("m1")}, []sl.Tuple{{sl.String("shared"), sl.False}}); err != nil {
		t.Fatal(err)
	}
	if _, err := api_account_read(thread, fn, sl.Tuple{sl.String(id), sl.String(conversation), sl.String("m1")}, nil); err != nil {
		t.Fatal(err)
	}
	fake.await(t, "the read marker", "chat-markers", "displayed", `id="m1"`)
	if markers := fake.heard("displayed"); len(markers) != 1 {
		t.Errorf("%d read markers, want 1: only for the marked message, only when shared: %v", len(markers), markers)
	}

	if _, err := api_account_presence(thread, fn, sl.Tuple{sl.String(id), sl.False}, nil); err != nil {
		t.Fatal(err)
	}
	fake.await(t, "extended away", "<presence", "<show", ">xa<")

	if _, err := api_account_room_leave(thread, fn, sl.Tuple{sl.String(id), sl.String("xmpp:" + room)}, nil); err != nil {
		t.Fatal(err)
	}
	fake.await(t, "leaving the room", "<presence", `type="unavailable"`, room+"/alice")
	fake.await(t, "the bookmark withdrawn", "retract", "urn:xmpp:bookmarks:1", room)
}

// A matrix_room_fake is a Matrix home server with one rich sync, recording
// every request after it.
type matrix_room_fake struct {
	server   *httptest.Server
	lock     sync.Mutex
	syncs    []string
	requests []string
	bodies   map[string]map[string]any
	media    []string
	count    int
}

func create_matrix_room_fake(t *testing.T) *matrix_room_fake {
	t.Helper()
	f := &matrix_room_fake{bodies: map[string]map[string]any{}}
	mux := http.NewServeMux()
	record := func(r *http.Request) map[string]any {
		body := json_body(r)
		f.lock.Lock()
		key := r.Method + " " + r.URL.Path
		f.requests = append(f.requests, key)
		f.bodies[key] = body
		f.count++
		f.lock.Unlock()
		return body
	}
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, r *http.Request) {
		host := fake_host(f.server)
		f.lock.Lock()
		f.syncs = append(f.syncs, r.URL.RawQuery)
		first := len(f.syncs) == 1
		f.lock.Unlock()
		if !first {
			time.Sleep(50 * time.Millisecond)
			answer(w, 200, map[string]any{"next_batch": "s2"})
			return
		}
		state := func(kind string, key string, sender string, content map[string]any) map[string]any {
			return map[string]any{"type": kind, "state_key": key, "sender": sender, "event_id": "$st" + kind + key, "origin_server_ts": 1700000000000, "content": content}
		}
		event := func(id string, sender string, content map[string]any) map[string]any {
			return map[string]any{"type": "m.room.message", "event_id": id, "sender": sender, "origin_server_ts": 1700000001000, "content": content}
		}
		bob, me := "@bob:"+host, "@alice:"+host
		answer(w, 200, map[string]any{
			"next_batch":   "s1",
			"account_data": map[string]any{"events": []any{map[string]any{"type": "m.direct", "content": map[string]any{bob: []any{"!dm:" + host}}}}},
			"presence":     map[string]any{"events": []any{map[string]any{"type": "m.presence", "sender": bob, "content": map[string]any{"presence": "unavailable", "status_msg": "Lunch"}}}},
			"rooms": map[string]any{
				"join": map[string]any{
					"!dm:" + host: map[string]any{
						"summary": map[string]any{"m.heroes": []any{bob}, "m.joined_member_count": 2},
						"state":   map[string]any{"events": []any{state("m.room.member", bob, bob, map[string]any{"membership": "join", "displayname": "Bob"})}},
						"timeline": map[string]any{"events": []any{
							event("$m1", bob, map[string]any{"msgtype": "m.text", "body": "hello"}),
							event("$m2", bob, map[string]any{"msgtype": "m.text", "body": "> <@alice> hello\n\nquoted reply", "m.relates_to": map[string]any{"m.in_reply_to": map[string]any{"event_id": "$m1"}}}),
							event("$m3", bob, map[string]any{"msgtype": "m.text", "body": "* hello there", "m.new_content": map[string]any{"msgtype": "m.text", "body": "hello there"}, "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": "$m1"}}),
							event("$m4", bob, map[string]any{"msgtype": "m.image", "body": "cat.png", "url": "mxc://" + host + "/cat", "info": map[string]any{"mimetype": "image/png", "size": 1234}}),
							map[string]any{"type": "m.room.redaction", "event_id": "$r1", "sender": bob, "redacts": "$m2", "origin_server_ts": 1700000002000, "content": map[string]any{}},
							map[string]any{"type": "m.room.message", "event_id": "$m5", "sender": me, "origin_server_ts": 1700000003000, "content": map[string]any{"msgtype": "m.text", "body": "from my phone"}},
							map[string]any{"type": "m.room.message", "event_id": "$m6", "sender": me, "origin_server_ts": 1700000003000, "unsigned": map[string]any{"transaction_id": "mochichat-x"}, "content": map[string]any{"msgtype": "m.text", "body": "sent from here"}},
							event("$m7", bob, map[string]any{"msgtype": "m.text", "body": "in a thread", "m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": "$m1"}}),
						}},
						"ephemeral": map[string]any{"events": []any{
							map[string]any{"type": "m.typing", "content": map[string]any{"user_ids": []any{bob}}},
							map[string]any{"type": "m.receipt", "content": map[string]any{"$m5": map[string]any{"m.read": map[string]any{bob: map[string]any{"ts": 1700000004000}}}}},
						}},
					},
					"!secret:" + host: map[string]any{
						"state":    map[string]any{"events": []any{state("m.room.name", "", bob, map[string]any{"name": "Secrets"}), state("m.room.encryption", "", bob, map[string]any{"algorithm": "m.megolm.v1.aes-sha2"})}},
						"timeline": map[string]any{"events": []any{map[string]any{"type": "m.room.encrypted", "event_id": "$e1", "sender": bob, "origin_server_ts": 1700000005000, "content": map[string]any{"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "x"}}}},
					},
				},
				"invite": map[string]any{"!club:" + host: map[string]any{"invite_state": map[string]any{"events": []any{
					state("m.room.name", "", bob, map[string]any{"name": "The club"}),
					state("m.room.member", me, bob, map[string]any{"membership": "invite"}),
				}}}},
				"leave": map[string]any{"!old:" + host: map[string]any{}},
			},
		})
	})
	mux.HandleFunc("PUT /_matrix/client/v3/rooms/{room}/send/{kind}/{txn}", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		f.lock.Lock()
		n := f.count
		f.lock.Unlock()
		answer(w, 200, map[string]any{"event_id": "$sent" + strconv.Itoa(n)})
	})
	for _, route := range []string{"PUT /_matrix/client/v3/rooms/{room}/redact/{event}/{txn}", "PUT /_matrix/client/v3/rooms/{room}/typing/{user}", "POST /_matrix/client/v3/rooms/{room}/receipt/{kind}/{event}", "PUT /_matrix/client/v3/presence/{user}/status", "POST /_matrix/client/v3/rooms/{room}/leave", "PUT /_matrix/client/v3/user/{user}/account_data/m.direct"} {
		mux.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
			record(r)
			answer(w, 200, map[string]any{})
		})
	}
	mux.HandleFunc("GET /_matrix/client/v3/user/{user}/account_data/m.direct", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 404, map[string]any{"errcode": "M_NOT_FOUND"})
	})
	mux.HandleFunc("POST /_matrix/client/v3/createRoom", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		answer(w, 200, map[string]any{"room_id": "!new:" + fake_host(f.server)})
	})
	mux.HandleFunc("POST /_matrix/client/v3/join/{room}", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		answer(w, 200, map[string]any{"room_id": "!joined:" + fake_host(f.server)})
	})
	mux.HandleFunc("GET /_matrix/client/v3/profile/{user}", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.PathValue("user"), "@nobody") {
			answer(w, 404, map[string]any{"errcode": "M_NOT_FOUND"})
			return
		}
		answer(w, 200, map[string]any{"displayname": "Carol"})
	})
	mux.HandleFunc("POST /_matrix/media/v3/upload", func(w http.ResponseWriter, r *http.Request) {
		f.lock.Lock()
		f.requests = append(f.requests, "POST /_matrix/media/v3/upload?"+r.URL.RawQuery+" "+r.Header.Get("Content-Type"))
		f.lock.Unlock()
		answer(w, 200, map[string]any{"content_uri": "mxc://" + fake_host(f.server) + "/up1"})
	})
	mux.HandleFunc("GET /_matrix/client/v1/media/download/{server}/{media}", func(w http.ResponseWriter, r *http.Request) {
		f.lock.Lock()
		f.media = append(f.media, r.Header.Get("Authorization"))
		f.lock.Unlock()
		w.Write([]byte("png"))
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// sent answers the body of the last request to a path, and whether one came.
func (f *matrix_room_fake) sent(method string, path string) (map[string]any, bool) {
	f.lock.Lock()
	defer f.lock.Unlock()
	body, found := f.bodies[method+" "+path]
	return body, found
}

// TestMatrixConversations: a linked Matrix account's sync is read as
// conversation events - rooms with their names, encryption and one-to-one
// marking, members, messages with replies (their quoted fallback taken off),
// edits, files, redactions, threads, encrypted messages, messages from the
// account's other clients (but not what this device sent), typing, read
// receipts, invitations, rooms left and presence - with the user's presence
// asked for on every sync. The functions send messages and files, edits,
// redactions, typing, read receipts shared or private, the user's presence
// hidden, start one-to-one and group rooms, join and leave; and a file is
// fetched with the account's credential only from the account's own server.
func TestMatrixConversations(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_matrix_room_fake(t)
	host := fake_host(fake.server)
	saved := matrix_sync_timeout
	matrix_sync_timeout = 200 * time.Millisecond
	defer func() { matrix_sync_timeout = saved }()
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m1', 'matrix', 'Alice', ?, ?, 1, 1, 1)", "@alice:"+host, json_encode(map[string]any{"id": "@alice:" + host, "server": host, "base": fake.server.URL, "token": "mt1"}))
	log := &inbound_log{}
	log.install(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	linked_start(linked_get(alice, "m1"))
	log.find(t, "session", nil)

	dm, secret := "!dm:"+host, "!secret:"+host
	bob := "@bob:" + host
	if room := log.find(t, "room", func(e map[string]any) bool { return e["conversation"] == dm }); room["direct"] != true || room["room"] != false || room["count"] != 2 {
		t.Errorf("one-to-one room = %v", room)
	} else if heroes, _ := room["heroes"].([]any); len(heroes) != 1 || heroes[0].(map[string]any)["name"] != "Bob" {
		t.Errorf("one-to-one room's heroes = %v", room["heroes"])
	}
	if room := log.find(t, "room", func(e map[string]any) bool { return e["conversation"] == secret }); room["name"] != "Secrets" || room["encrypted"] != true || room["room"] != true {
		t.Errorf("encrypted room = %v", room)
	}
	if member := log.find(t, "member", nil); member["id"] != bob || member["name"] != "Bob" || member["left"] != false {
		t.Errorf("member = %v", member)
	}
	if hello := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "$m1" }); hello["body"] != "hello" || hello["from"] != bob || hello["name"] != "Bob" || hello["created"] != int64(1700000001) || hello["own"] != false {
		t.Errorf("message = %v", hello)
	}
	if reply := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "$m2" }); reply["body"] != "quoted reply" || reply["reply"] != "$m1" {
		t.Errorf("reply = %v", reply)
	}
	if edit := log.find(t, "edit", nil); edit["id"] != "$m1" || edit["body"] != "hello there" {
		t.Errorf("edit = %v", edit)
	}
	image := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "$m4" })
	files, _ := image["files"].([]any)
	if image["body"] != "" || len(files) != 1 || files[0].(map[string]any)["url"] != fake.server.URL+"/_matrix/client/v1/media/download/"+host+"/cat" || files[0].(map[string]any)["type"] != "image/png" {
		t.Errorf("file = %v", image)
	}
	if redaction := log.find(t, "delete", nil); redaction["id"] != "$m2" || redaction["from"] != bob {
		t.Errorf("redaction = %v", redaction)
	}
	if own := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "$m5" }); own["own"] != true {
		t.Errorf("message from another client = %v", own)
	}
	if echo := log.matching(func(e map[string]any) bool { return e["id"] == "$m6" }); len(echo) > 0 {
		t.Errorf("what this device sent came back: %v", echo)
	}
	if threaded := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "$m7" }); threaded["thread"] != "$m1" {
		t.Errorf("thread = %v", threaded)
	}
	if encrypted := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "$e1" }); encrypted["encrypted"] != true || encrypted["body"] != "" {
		t.Errorf("encrypted = %v", encrypted)
	}
	if typing := log.find(t, "typing", nil); typing["from"] != bob || typing["typing"] != true || typing["conversation"] != dm {
		t.Errorf("typing = %v", typing)
	}
	if receipt := log.find(t, "receipt", nil); receipt["from"] != bob || receipt["id"] != "$m5" || receipt["displayed"] != true {
		t.Errorf("receipt = %v", receipt)
	}
	if invite := log.find(t, "invite", nil); invite["conversation"] != "!club:"+host || invite["from"] != bob || invite["name"] != "The club" {
		t.Errorf("invite = %v", invite)
	}
	if left := log.find(t, "room", func(e map[string]any) bool { return e["conversation"] == "!old:"+host }); left["left"] != true {
		t.Errorf("left = %v", left)
	}
	if presence := log.find(t, "presence", nil); presence["from"] != bob || presence["status"] != "away" || presence["message"] != "Lunch" {
		t.Errorf("presence = %v", presence)
	}
	fake.lock.Lock()
	first := fake.syncs[0]
	fake.lock.Unlock()
	if !strings.Contains(first, "set_presence=offline") || !strings.Contains(first, "lazy_load_members") {
		t.Errorf("sync = %s, want the user's presence (offline, not connected) and the filter", first)
	}

	// Sending.
	message := sl.NewDict(3)
	message.SetKey(sl.String("conversation"), sl.String(dm))
	message.SetKey(sl.String("content"), sl.String("hi bob"))
	message.SetKey(sl.String("reply"), sl.String("$m1"))
	created := decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), message}, []sl.Tuple{{sl.String("key"), sl.String("chat|abc")}}))
	if !strings.HasPrefix(created["id"].(string), "$sent") {
		t.Errorf("create = %v", created)
	}
	body, found := fake.sent("PUT", "/_matrix/client/v3/rooms/"+dm+"/send/m.room.message/mochichat-abc")
	relates, _ := body["m.relates_to"].(map[string]any)
	if !found || body["body"] != "hi bob" || body["msgtype"] != "m.text" || relates["m.in_reply_to"] == nil {
		t.Errorf("sent = %v (found %v)", body, found)
	}
	upload := decoded(t)(api_account_upload(thread, fn, sl.Tuple{sl.String("m1"), sl.Bytes("png"), sl.String("image/png")}, []sl.Tuple{{sl.String("name"), sl.String("cat.png")}}))
	if upload["url"] != "mxc://"+host+"/up1" {
		t.Errorf("upload = %v", upload)
	}
	withfile := sl.NewDict(2)
	withfile.SetKey(sl.String("conversation"), sl.String(dm))
	withfile.SetKey(sl.String("file"), sl_encode(upload))
	decoded(t)(api_account_object_create(thread, fn, sl.Tuple{sl.String("m1"), withfile}, []sl.Tuple{{sl.String("key"), sl.String("chat|file")}}))
	if body, _ := fake.sent("PUT", "/_matrix/client/v3/rooms/"+dm+"/send/m.room.message/mochichat-file"); body["msgtype"] != "m.image" || body["url"] != "mxc://"+host+"/up1" || body["filename"] != "cat.png" {
		t.Errorf("file sent = %v", body)
	}
	edit := sl.NewDict(2)
	edit.SetKey(sl.String("conversation"), sl.String(dm))
	edit.SetKey(sl.String("content"), sl.String("hi again"))
	if _, err := api_account_object_update(thread, fn, sl.Tuple{sl.String("m1"), sl.String("$sent1"), edit}, nil); err != nil {
		t.Fatal(err)
	}
	fake.lock.Lock()
	var editing map[string]any
	for key, b := range fake.bodies {
		if strings.Contains(key, "/send/m.room.message/mochi") && !strings.Contains(key, "mochichat") {
			editing = b
		}
	}
	fake.lock.Unlock()
	if relation, _ := editing["m.relates_to"].(map[string]any); relation["rel_type"] != "m.replace" || relation["event_id"] != "$sent1" || editing["m.new_content"].(map[string]any)["body"] != "hi again" {
		t.Errorf("edit = %v", editing)
	}
	if _, err := api_account_object_delete(thread, fn, sl.Tuple{sl.String("m1"), sl.String("$sent1")}, []sl.Tuple{{sl.String("conversation"), sl.String(dm)}}); err != nil {
		t.Fatal(err)
	}
	fake.lock.Lock()
	redacted := false
	for _, request := range fake.requests {
		redacted = redacted || strings.HasPrefix(request, "PUT /_matrix/client/v3/rooms/"+dm+"/redact/$sent1/")
	}
	fake.lock.Unlock()
	if !redacted {
		t.Error("no redaction")
	}
	if _, err := api_account_typing(thread, fn, sl.Tuple{sl.String("m1"), sl.String(dm)}, nil); err != nil {
		t.Fatal(err)
	}
	if body, _ := fake.sent("PUT", "/_matrix/client/v3/rooms/"+dm+"/typing/@alice:"+host); body["typing"] != true || body["timeout"] != float64(matrix_typing_timeout) {
		t.Errorf("typing = %v", body)
	}
	if _, err := api_account_read(thread, fn, sl.Tuple{sl.String("m1"), sl.String(dm), sl.String("$m1")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := api_account_read(thread, fn, sl.Tuple{sl.String("m1"), sl.String(dm), sl.String("$m4")}, []sl.Tuple{{sl.String("shared"), sl.False}}); err != nil {
		t.Fatal(err)
	}
	if _, found := fake.sent("POST", "/_matrix/client/v3/rooms/"+dm+"/receipt/m.read/$m1"); !found {
		t.Error("no shared read receipt")
	}
	if _, found := fake.sent("POST", "/_matrix/client/v3/rooms/"+dm+"/receipt/m.read.private/$m4"); !found {
		t.Error("no private read receipt")
	}

	db_open("db/sessions.db").exec("create table if not exists seen (user text not null primary key, time integer not null)")
	presence_connect(alice.UID, "w1", "", "web", "")
	wait(t, "the user's presence sent online", func() bool {
		body, _ := fake.sent("PUT", "/_matrix/client/v3/presence/@alice:"+host+"/status")
		return body["presence"] == "online"
	})
	if _, err := api_account_presence(thread, fn, sl.Tuple{sl.String("m1"), sl.False}, nil); err != nil {
		t.Fatal(err)
	}
	wait(t, "the user's presence hidden", func() bool {
		body, _ := fake.sent("PUT", "/_matrix/client/v3/presence/@alice:"+host+"/status")
		return body["presence"] == "offline"
	})
	presence_disconnect(alice.UID, "w1")

	people := sl.NewList([]sl.Value{sl.String("@carol:" + host)})
	room := decoded(t)(api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), people}, nil))
	if room["conversation"] != "!new:"+host || room["room"] != false {
		t.Errorf("room = %v", room)
	}
	if body, _ := fake.sent("POST", "/_matrix/client/v3/createRoom"); body["is_direct"] != true || body["preset"] != "trusted_private_chat" {
		t.Errorf("createRoom = %v", body)
	}
	if body, _ := fake.sent("PUT", "/_matrix/client/v3/user/@alice:"+host+"/account_data/m.direct"); body["@carol:"+host] == nil {
		t.Errorf("m.direct = %v", body)
	}
	if value, err := api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), sl.NewList([]sl.Value{sl.String("@nobody:" + host)})}, nil); err != nil || value != sl.None {
		t.Errorf("room with no one: %v, %v", value, err)
	}
	group := decoded(t)(api_account_room_create(thread, fn, sl.Tuple{sl.String("m1"), sl.NewList([]sl.Value{sl.String("@carol:" + host), sl.String("@dave:" + host)})}, []sl.Tuple{{sl.String("name"), sl.String("Friends")}}))
	if group["room"] != true {
		t.Errorf("group = %v", group)
	}
	if body, _ := fake.sent("POST", "/_matrix/client/v3/createRoom"); body["name"] != "Friends" || body["is_direct"] != nil || len(body["invite"].([]any)) != 2 {
		t.Errorf("group createRoom = %v", body)
	}
	if joined := decoded(t)(api_account_room_join(thread, fn, sl.Tuple{sl.String("m1"), sl.String("#club:" + host)}, nil)); joined["conversation"] != "!joined:"+host {
		t.Errorf("join = %v", joined)
	}
	if body, _ := fake.sent("PUT", "/_matrix/client/v3/user/@alice:"+host+"/account_data/m.direct"); body[bob] != nil {
		t.Errorf("m.direct = %v, want no mark for a room joined with no person", body)
	}
	decoded(t)(api_account_room_join(thread, fn, sl.Tuple{sl.String("m1"), sl.String("!joined:" + host)}, []sl.Tuple{{sl.String("person"), sl.String(bob)}}))
	if body, _ := fake.sent("PUT", "/_matrix/client/v3/user/@alice:"+host+"/account_data/m.direct"); body[bob] == nil {
		t.Errorf("m.direct = %v, want the accepted one-to-one room marked with bob", body)
	}
	if _, err := api_account_room_leave(thread, fn, sl.Tuple{sl.String("m1"), sl.String("!club:" + host)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, found := fake.sent("POST", "/_matrix/client/v3/rooms/!club:"+host+"/leave"); !found {
		t.Error("no leave")
	}

	// A file is fetched with the credential from the account's server, and
	// without it from anywhere else.
	decoded(t)(api_account_media(thread, fn, sl.Tuple{sl.String("m1"), sl.String(fake.server.URL + "/_matrix/client/v1/media/download/" + host + "/cat")}, nil))
	elsewhere := create_matrix_room_fake(t)
	decoded(t)(api_account_media(thread, fn, sl.Tuple{sl.String("m1"), sl.String(strings.Replace(elsewhere.server.URL, "127.0.0.1", "localhost", 1) + "/_matrix/client/v1/media/download/" + host + "/cat")}, nil))
	fake.lock.Lock()
	home := fake.media
	fake.lock.Unlock()
	elsewhere.lock.Lock()
	away := elsewhere.media
	elsewhere.lock.Unlock()
	if len(home) != 1 || home[0] != "Bearer mt1" {
		t.Errorf("home fetch carried %v", home)
	}
	if len(away) != 1 || away[0] != "" {
		t.Errorf("another server's fetch carried %v, want no credential", away)
	}
}

// TestMastodonDirect: a linked Mastodon account's session first reads its
// latest conversations, with the earlier messages each latest replies to,
// then holds the server's stream with the token as its subprotocol, asking for
// the conversations stream too; a direct mention, an edit, a deletion and a
// conversation's new message reach the app, a public status does not, and the
// position is kept so a reconnection reads only what is newer.
func TestMastodonDirect(t *testing.T) {
	alice := create_linked_test_env(t)
	var lock sync.Mutex
	var protocols []string
	var subscribed []string
	push := make(chan string, 8)
	status := func(id string, author string, visibility string, content string, created string, reply string) map[string]any {
		s := map[string]any{"id": id, "visibility": visibility, "content": content, "created_at": created, "account": map[string]any{"id": author, "acct": map[string]string{"b1": "bob@other.example", "a1": "alice"}[author], "display_name": map[string]string{"b1": "Bob", "a1": "Alice"}[author]}, "mentions": []any{}, "media_attachments": []any{}}
		if reply != "" {
			s["in_reply_to_id"] = reply
		}
		return s
	}
	mux := http.NewServeMux()
	var server *httptest.Server
	mux.HandleFunc("GET /api/v1/instance", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"urls": map[string]any{"streaming_api": "ws://" + fake_host(server)}})
	})
	mux.HandleFunc("GET /api/v1/conversations", func(w http.ResponseWriter, r *http.Request) {
		last := status("s2", "b1", "direct", "<p>second</p>", "2026-01-02T03:05:00Z", "s1")
		last["media_attachments"] = []any{map[string]any{"url": "https://media.other.example/a/photo.jpg", "description": "A photo"}}
		answer(w, 200, []any{map[string]any{"id": "c1", "accounts": []any{map[string]any{"id": "b1", "acct": "bob@other.example"}}, "last_status": last}})
	})
	mux.HandleFunc("GET /api/v1/statuses/s2/context", func(w http.ResponseWriter, r *http.Request) {
		answer(w, 200, map[string]any{"ancestors": []any{status("s1", "a1", "direct", "<p>first</p>", "2026-01-02T03:04:00Z", "")}})
	})
	mux.HandleFunc("GET /api/v1/streaming", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		protocols = append(protocols, r.Header.Get("Sec-WebSocket-Protocol"))
		lock.Unlock()
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"tok"}})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		ctx := r.Context()
		go func() {
			for {
				_, data, err := conn.Read(ctx)
				if err != nil {
					return
				}
				lock.Lock()
				subscribed = append(subscribed, string(data))
				lock.Unlock()
			}
		}()
		for {
			select {
			case text := <-push:
				conn.Write(ctx, websocket.MessageText, []byte(text))
			case <-ctx.Done():
				return
			}
		}
	})
	server = httptest.NewServer(mux)
	defer server.Close()
	host := fake_host(server)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('d1', 'mastodon', 'Alice', ?, ?, 1, 1, 1)", "@alice@"+host, json_encode(map[string]any{"id": "https://" + host + "/@alice", "server": host, "token": "tok", "account": "a1"}))
	log := &inbound_log{}
	log.install(t)
	linked_start(linked_get(alice, "d1"))

	bob := "acct:bob@other.example"
	first := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "s1" })
	if first["own"] != true || first["from"] != "acct:alice@"+host || first["html"] != "<p>first</p>" {
		t.Errorf("earlier message = %v", first)
	}
	if people, _ := first["people"].([]any); len(people) != 1 || people[0] != bob {
		t.Errorf("earlier message's people = %v", first["people"])
	}
	second := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "s2" })
	files, _ := second["files"].([]any)
	if second["own"] != false || second["from"] != bob || second["name"] != "Bob" || second["reply"] != "s1" || len(files) != 1 || files[0].(map[string]any)["description"] != "A photo" {
		t.Errorf("latest message = %v", second)
	}
	wait(t, "the position", func() bool { return as_int64(linked_get(alice, "d1").value("position")) > 0 })
	wait(t, "the stream", func() bool {
		lock.Lock()
		defer lock.Unlock()
		return len(subscribed) > 0
	})
	lock.Lock()
	if protocols[0] != "tok" || !strings.Contains(subscribed[0], `"stream":"direct"`) {
		t.Errorf("stream opened with %v, asked for %v", protocols, subscribed)
	}
	lock.Unlock()

	encode := func(event string, payload any) string {
		text := ""
		if s, ok := payload.(string); ok {
			text = s
		} else {
			text = json_encode(payload)
		}
		return json_encode(map[string]any{"event": event, "payload": text})
	}
	mention := status("s3", "b1", "direct", "<p>hey</p>", "2026-01-03T00:00:00Z", "")
	mention["mentions"] = []any{map[string]any{"acct": "alice"}, map[string]any{"acct": "carol@third.example"}}
	push <- encode("update", status("s9", "b1", "public", "<p>public</p>", "2026-01-03T00:00:00Z", ""))
	push <- encode("notification", map[string]any{"type": "mention", "status": mention})
	edited := status("s3", "b1", "direct", "<p>hey there</p>", "2026-01-03T00:00:00Z", "")
	push <- encode("status.update", edited)
	push <- encode("delete", "s3")
	push <- encode("conversation", map[string]any{"id": "c1", "accounts": []any{map[string]any{"acct": "bob@other.example"}}, "last_status": status("s5", "b1", "direct", "<p>more</p>", "2026-01-04T00:00:00Z", "s3")})

	group := log.find(t, "message", func(e map[string]any) bool { return e["id"] == "s3" })
	if people, _ := group["people"].([]any); len(people) != 2 || people[0] != bob || people[1] != "acct:carol@third.example" {
		t.Errorf("mention's people = %v", group["people"])
	}
	if edit := log.find(t, "edit", nil); edit["id"] != "s3" || edit["html"] != "<p>hey there</p>" {
		t.Errorf("edit = %v", edit)
	}
	if deleted := log.find(t, "delete", nil); deleted["id"] != "s3" {
		t.Errorf("delete = %v", deleted)
	}
	log.find(t, "message", func(e map[string]any) bool { return e["id"] == "s5" })
	if public := log.matching(func(e map[string]any) bool { return e["id"] == "s9" }); len(public) > 0 {
		t.Errorf("a public status was taken as a direct message: %v", public)
	}
	stamp, _ := time.Parse(time.RFC3339, "2026-01-04T00:00:00Z")
	wait(t, "the position to follow the stream", func() bool { return as_int64(linked_get(alice, "d1").value("position")) == stamp.Unix() })
}

// TestLinkedSaveKeepsOthers: two loads of one account each save what they
// changed, and neither undoes the other's change.
func TestLinkedSaveKeepsOthers(t *testing.T) {
	alice := create_linked_test_env(t)
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('m1', 'matrix', 'Alice', '@alice:example.org', ?, 1, 1)", json_encode(map[string]any{"id": "@alice:example.org", "token": "old", "since": "s0"}))
	session, app := linked_get(alice, "m1"), linked_get(alice, "m1")
	app.store("hidden", true)
	session.set("token", "new")
	session.set("since", "")
	session.save()
	fresh := linked_get(alice, "m1")
	if fresh.value("hidden") != true || fresh.get("token") != "new" || fresh.get("since") != "" || fresh.get("id") != "@alice:example.org" {
		t.Errorf("data = %v", fresh.data)
	}
}

// TestLinkedInbound: what a session hears goes to every app declaring
// linked/receive, as one call with the account, the network and the events.
func TestLinkedInbound(t *testing.T) {
	alice := create_linked_test_env(t)
	var calls []Map
	saved_call, saved_apps := activitypub_call, apps
	activitypub_call = func(user *User, a *App, function string, args Map) (any, error) {
		if function == "linked/receive" && user.UID == alice.UID {
			calls = append(calls, args)
		}
		return nil, nil
	}
	apps = map[string]*App{
		"chat":  {id: "chat", internal: &AppVersion{Functions: map[string]AppFunction{"linked/receive": {Function: "function_linked_receive"}}}},
		"feeds": {id: "feeds", internal: &AppVersion{Functions: map[string]AppFunction{"activitypub/receive": {Function: "function_activitypub_receive"}}}},
	}
	t.Cleanup(func() { activitypub_call, apps = saved_call, saved_apps })
	inbound := linked_inbound
	t.Cleanup(func() { linked_inbound = inbound })
	linked_inbound = saved_inbound_dispatch
	linked_inbound(alice, "x1", "xmpp", []map[string]any{{"kind": "message", "body": "hi"}, {"kind": "typing"}})
	linked_inbound(alice, "x1", "xmpp", nil)
	if len(calls) != 1 || calls[0]["account"] != "x1" || calls[0]["network"] != "xmpp" || len(calls[0]["events"].([]any)) != 2 {
		t.Errorf("calls = %v", calls)
	}
}

// TestLinkedSessionBudget: a session's syncs draw on a share of the account's
// request budget of their own, so syncing past the budget leaves the user's
// own requests theirs, and the sync loop waits out its window rather than
// ending the session.
func TestLinkedSessionBudget(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_matrix_room_fake(t)
	host := fake_host(fake.server)
	saved_sync, saved_limit := matrix_sync_timeout, rate_limit_host.limit
	matrix_sync_timeout = 50 * time.Millisecond
	rate_limit_host.limit = 4
	defer func() { matrix_sync_timeout, rate_limit_host.limit = saved_sync, saved_limit }()
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('m1', 'matrix', 'Alice', ?, ?, 1, 1, 1)", "@alice:"+host, json_encode(map[string]any{"id": "@alice:" + host, "server": host, "base": fake.server.URL, "token": "mt1"}))
	log := &inbound_log{}
	log.install(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)
	linked_start(linked_get(alice, "m1"))
	// The presence sent and three syncs use the whole of a budget of four.
	wait(t, "the session to use its budget", func() bool {
		fake.lock.Lock()
		defer fake.lock.Unlock()
		return len(fake.syncs) >= 3
	})
	time.Sleep(100 * time.Millisecond)
	if _, err := api_account_typing(thread, fn, sl.Tuple{sl.String("m1"), sl.String("!dm:" + host)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, found := fake.sent("PUT", "/_matrix/client/v3/rooms/!dm:"+host+"/typing/@alice:"+host); !found {
		t.Error("the user's typing was refused for the session's syncs")
	}
	if connected, reason := linked_connected(alice.UID, "m1"); !connected {
		t.Errorf("the session ended at its budget: %s", reason)
	}
}
