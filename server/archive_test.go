// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"regexp"
	"strings"
	"sync"
	"testing"
)

// TestXMPPArchiveIds: a message of a conversation coming to the account is
// given its id in the account's archive (XEP-0359), which its client is told
// and chat is handed, and one it claims the archive gave already is taken
// out; an encrypted message's content goes to chat as it came; a message the
// account's client sends is handed to chat with its id, which the other side
// is not told.
func TestXMPPArchiveIds(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	fake.allow(env.alice, "bob@example.com")
	fake.allow(env.bob, "alice@example.com")
	a, b := env.pair(t)

	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="i1"><body>With an id</body><stanza-id xmlns="urn:xmpp:sid:0" by="alice@example.com" id="forged"/></message>`)
	delivered := a.expect(t, "the message", "With an id")
	ids := regexp.MustCompile(`stanza-id[^>]*id="([^"]+)"`).FindAllStringSubmatch(delivered, -1)
	if len(ids) != 1 || ids[0][1] == "forged" || !strings.Contains(delivered, `by="alice@example.com"`) {
		t.Fatalf("THE POINT: the ids the client was told: %s", delivered)
	}
	event := env.log.find(t, "message", func(e map[string]any) bool { return e["id"] == "i1" && e["account"] == "x1" })
	if event["archive"] != ids[0][1] {
		t.Errorf("THE POINT: chat was handed archive %v, the client told %s", event["archive"], ids[0][1])
	}

	b.send(t, `<message xmlns="jabber:client" to="alice@example.com" type="chat" id="i2"><encrypted xmlns="eu.siacs.conversations.axolotl"><header sid="7"><key rid="9">S0VZ</key><iv>SVY=</iv></header><payload>UEFZ</payload></encrypted><body>I sent you an OMEMO encrypted message</body></message>`)
	encrypted := env.log.find(t, "message", func(e map[string]any) bool { return e["id"] == "i2" && e["account"] == "x1" })
	payload, _ := encrypted["payload"].(string)
	if encrypted["encrypted"] != true || !strings.Contains(payload, `<encrypted xmlns="eu.siacs.conversations.axolotl">`) || !strings.Contains(payload, "UEFZ") || strings.Contains(payload, "<body") {
		t.Errorf("THE POINT: the encrypted content handed to chat: %v", encrypted)
	}

	a.send(t, `<message xmlns="jabber:client" to="bob@example.com" type="chat" id="o1"><body>From the client of alice</body></message>`)
	sent := b.expect(t, "alice's message", "From the client of alice")
	if strings.Contains(sent, `by="alice@example.com"`) {
		t.Errorf("the other side was told alice's archive's id: %s", sent)
	}
	own := env.log.find(t, "message", func(e map[string]any) bool { return e["id"] == "o1" && e["own"] == true && e["account"] == "x1" })
	if archive, _ := own["archive"].(string); archive == "" {
		t.Errorf("THE POINT: no archive id for the client's own message: %v", own)
	}
}

// TestXMPPArchiveQueries: a client's query of its account's archive
// (XEP-0313) is answered from the page chat gives: each message forwarded
// with when it came, from the person or from the account, the encrypted
// content as it came, and a message chat sent with files as one message for
// its text and one for each file; then the answer naming the page's first
// and last and whether it is the last. Paging from an item among a message's
// files carries on from the next of them; the last page is the latest items;
// a place not in the archive is not found.
func TestXMPPArchiveQueries(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.apps(t)
	page := []any{
		map[string]any{"archive": "A1", "id": "m1", "person": "xmpp:bob@example.com", "own": false, "body": "Hello", "created": int64(1700000000)},
		map[string]any{"archive": "A2", "id": "m2", "person": "xmpp:bob@example.com", "own": true, "body": "Look", "created": int64(1700000100), "reply": "m1", "files": []any{
			map[string]any{"id": "f1", "url": "https://example.com/xmpp/upload/1/a.png"},
			map[string]any{"id": "f2", "url": "https://example.com/xmpp/upload/2/b.png"},
		}},
		map[string]any{"archive": "A3", "id": "m3", "person": "xmpp:bob@example.com", "own": false, "body": "", "created": int64(1700000200), "payload": `<encrypted xmlns="eu.siacs.conversations.axolotl"><payload>UEFZ</payload></encrypted>`},
	}
	var lock sync.Mutex
	var asked []Map
	saved := xmpp_archive_ask
	xmpp_archive_ask = func(a *xmpp_account, arguments Map) map[string]any {
		lock.Lock()
		asked = append(asked, arguments)
		lock.Unlock()
		// As chat does: from the message paged from, or to it, inclusive.
		after, _ := arguments["after"].(string)
		before, _ := arguments["before"].(string)
		if after == "nowhere" {
			return map[string]any{"messages": []any{}, "more": false, "found": false}
		}
		messages := []any{}
		within := after == ""
		for _, m := range page {
			archive := m.(map[string]any)["archive"]
			if archive == after {
				within = true
			}
			if within {
				messages = append(messages, m)
			}
			if archive == before {
				break
			}
		}
		return map[string]any{"messages": messages, "more": false, "found": true}
	}
	t.Cleanup(func() { xmpp_archive_ask = saved })
	c := env.raw(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "phone")

	c.send(`<iq xmlns="jabber:client" type="get" id="f0"><query xmlns="urn:xmpp:mam:2"/></iq>`)
	c.expect(t, "the form", `id="f0"`, `var="with"`)

	c.send(`<iq xmlns="jabber:client" type="set" id="q1"><query xmlns="urn:xmpp:mam:2" queryid="Q1"><x xmlns="jabber:x:data" type="submit"><field var="FORM_TYPE" type="hidden"><value>urn:xmpp:mam:2</value></field><field var="with"><value>bob@example.com</value></field></x><set xmlns="http://jabber.org/protocol/rsm"><max>10</max></set></query></iq>`)
	fin := c.expect(t, "the page's end", `id="q1"`, "<fin")
	if !strings.Contains(fin, `complete="true"`) || !strings.Contains(fin, "<first>A1</first>") || !strings.Contains(fin, "<last>A3</last>") {
		t.Errorf("THE POINT: the page's end: %s", fin)
	}
	first := c.expect(t, "the first message", `queryid="Q1"`, `id="A1"`)
	if !strings.Contains(first, `from="bob@example.com"`) || !strings.Contains(first, "Hello</body>") || !strings.Contains(first, `stamp="2023-11-14T22:13:20Z"`) {
		t.Errorf("the first message: %s", first)
	}
	text := c.expect(t, "THE POINT: the text of a message with files", `id="A2"`, "Look</body>")
	if !strings.Contains(text, `from="alice@example.com"`) || !strings.Contains(text, `to="bob@example.com"`) || !strings.Contains(text, `<reply xmlns="urn:xmpp:reply:0" id="m1"`) {
		t.Errorf("the account's own message: %s", text)
	}
	c.expect(t, "THE POINT: each file a message of its own", `id="A2+1"`, "a.png</url>", `id="f1"`)
	c.expect(t, "the second file", `id="A2+2"`, "b.png</url>", `id="f2"`)
	c.expect(t, "THE POINT: the encrypted content as it came", `id="A3"`, "UEFZ</payload>")
	lock.Lock()
	if len(asked) != 1 || asked[0]["conversation"] != "xmpp:bob@example.com" || asked[0]["maximum"] != 10 || asked[0]["account"] != "x1" {
		t.Errorf("chat was asked %v", asked)
	}
	lock.Unlock()

	c.send(`<iq xmlns="jabber:client" type="set" id="q2"><query xmlns="urn:xmpp:mam:2" queryid="Q2"><set xmlns="http://jabber.org/protocol/rsm"><max>10</max><after>A2+1</after></set></query></iq>`)
	second := c.expect(t, "the second page's end", `id="q2"`, "<fin")
	if heard := c.heard(`queryid="Q2"`, `id="A2+1"`); len(heard) > 0 {
		t.Errorf("THE POINT: the item paged from given again: %v", heard)
	}
	if !strings.Contains(second, "<first>A2+2</first>") {
		t.Errorf("the second page starts after the item paged from: %s", second)
	}
	lock.Lock()
	if asked[1]["after"] != "A2" {
		t.Errorf("chat was asked after %v", asked[1]["after"])
	}
	lock.Unlock()

	c.send(`<iq xmlns="jabber:client" type="set" id="q3"><query xmlns="urn:xmpp:mam:2" queryid="Q3"><set xmlns="http://jabber.org/protocol/rsm"><max>2</max><before/></set></query></iq>`)
	last := c.expect(t, "the last page's end", `id="q3"`, "<fin")
	if strings.Contains(last, `complete="true"`) || !strings.Contains(last, "<first>A2+2</first>") || !strings.Contains(last, "<last>A3</last>") {
		t.Errorf("THE POINT: the last page: %s", last)
	}
	lock.Lock()
	if asked[2]["last"] != true {
		t.Errorf("chat was not asked for the last page: %v", asked[2])
	}
	lock.Unlock()

	c.send(`<iq xmlns="jabber:client" type="set" id="q4"><query xmlns="urn:xmpp:mam:2" queryid="Q4"><set xmlns="http://jabber.org/protocol/rsm"><after>nowhere</after></set></query></iq>`)
	c.expect(t, "THE POINT: a place not in the archive", `id="q4"`, `type="error"`, "item-not-found")
}
