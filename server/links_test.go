// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// An xmpp_remote is another XMPP server, for a domain of its own: it takes
// the links this server opens to it, checking this server's dialback key by
// asking it back, and opens its own to send, answering this server's checks
// of its key. It records the stanzas it was sent.
type xmpp_remote struct {
	env      *xmpp_env
	domain   string
	secret   string
	lock     sync.Mutex
	received []string
}

func (env *xmpp_env) remote(t *testing.T, domain string) *xmpp_remote {
	t.Helper()
	f := &xmpp_remote{env: env, domain: domain, secret: random_alphanumeric(20)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	env.lock.Lock()
	env.addresses[domain] = listener.Addr().String()
	env.lock.Unlock()
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	return f
}

// key is the dialback key the remote gives a receiving domain on a stream.
func (f *xmpp_remote) key(receiving string, id string) string {
	m := hmac.New(sha256.New, []byte(f.secret))
	m.Write([]byte(receiving + " " + f.domain + " " + id))
	return hex.EncodeToString(m.Sum(nil))
}

// secured opens a stream to this server's server port, secured by TLS with
// the certificate checked, and answers it with the stream's id.
func (f *xmpp_remote) secured(to string) (*xmpp_conn, error) {
	conn, err := net.Dial("tcp", f.env.server)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	x := xmpp_conn_new(conn, 1<<20)
	x.open(xmpp_ns_server, f.domain, to, "")
	if _, err := x.header(); err != nil {
		return nil, err
	}
	x.next()
	x.write(`<starttls xmlns="` + xmpp_ns_tls + `"/>`)
	if _, err := x.next(); err != nil {
		return nil, err
	}
	secured := tls.Client(conn, &tls.Config{ServerName: to, RootCAs: f.env.roots})
	if err := secured.Handshake(); err != nil {
		return nil, err
	}
	x = xmpp_conn_new(secured, 1<<20)
	x.open(xmpp_ns_server, f.domain, to, "")
	attrs, err := x.header()
	if err != nil {
		return nil, err
	}
	x.id = attrs["id"]
	x.next()
	return x, nil
}

// serve takes a link this server opens to the remote.
func (f *xmpp_remote) serve(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(20 * time.Second))
	x := xmpp_conn_new(conn, 1<<20)
	attrs, err := x.header()
	if err != nil {
		return
	}
	x.open(xmpp_ns_server, f.domain, attrs["from"], "first-"+uid())
	x.write(`<stream:features><starttls xmlns="` + xmpp_ns_tls + `"><required/></starttls></stream:features>`)
	if s, err := x.next(); err != nil || s.name != "starttls" {
		return
	}
	x.write(`<proceed xmlns="` + xmpp_ns_tls + `"/>`)
	if err := x.secure(f.domain); err != nil {
		return
	}
	if attrs, err = x.header(); err != nil {
		return
	}
	x.id = "second-" + uid()
	x.open(xmpp_ns_server, f.domain, attrs["from"], x.id)
	x.write(`<stream:features><dialback xmlns="` + xmpp_ns_dialback_feature + `"/></stream:features>`)
	for {
		s, err := x.next()
		if err != nil {
			return
		}
		switch {
		case s.space == xmpp_ns_dialback && s.name == "result":
			answer := "invalid"
			if f.ask(s.get("from"), x.id, strings.TrimSpace(s.inner)) {
				answer = "valid"
			}
			x.write(`<db:result from="` + f.domain + `" to="` + s.get("from") + `" type="` + answer + `"/>`)
		case s.space == xmpp_ns_dialback && s.name == "verify":
			answer := "invalid"
			if strings.TrimSpace(s.inner) == f.key(s.get("from"), s.get("id")) {
				answer = "valid"
			}
			x.write(`<db:verify from="` + f.domain + `" to="` + s.get("from") + `" id="` + s.get("id") + `" type="` + answer + `"/>`)
		default:
			f.lock.Lock()
			f.received = append(f.received, s.text(s.space))
			f.lock.Unlock()
		}
	}
}

// ask checks the key this server gave on a stream by asking it.
func (f *xmpp_remote) ask(origin string, id string, key string) bool {
	x, err := f.secured(origin)
	if err != nil {
		return false
	}
	defer x.conn.Close()
	x.write(`<db:verify from="` + f.domain + `" to="` + origin + `" id="` + id + `">` + key + `</db:verify>`)
	s, err := x.next()
	return err == nil && s.name == "verify" && s.get("type") == "valid"
}

// link opens the remote's own link to this server with a key, right or
// wrong, and answers the stream and what this server said of the key.
func (f *xmpp_remote) link(t *testing.T, wrong bool) (*xmpp_conn, string) {
	t.Helper()
	return f.link_to(t, "example.com", wrong)
}

// link_to opens the remote's own link to one of this server's domains,
// answering the key's check, or why the stream was refused.
func (f *xmpp_remote) link_to(t *testing.T, to string, wrong bool) (*xmpp_conn, string) {
	t.Helper()
	x, err := f.secured(to)
	if err != nil {
		return nil, "the stream refused: " + err.Error()
	}
	t.Cleanup(func() { x.conn.Close() })
	key := f.key(to, x.id)
	if wrong {
		key = f.key(to, "another")
	}
	x.write(`<db:result from="` + f.domain + `" to="` + to + `">` + key + `</db:result>`)
	s, err := x.next()
	if err != nil {
		t.Fatalf("no answer to the key: %v", err)
	}
	return x, s.get("type")
}

// heard answers the stanzas the remote received that contain every part.
func (f *xmpp_remote) heard(parts ...string) []string {
	f.lock.Lock()
	defer f.lock.Unlock()
	var out []string
	for _, text := range f.received {
		all := true
		for _, part := range parts {
			all = all && strings.Contains(text, part)
		}
		if all {
			out = append(out, text)
		}
	}
	return out
}

// TestXMPPServerLinks: a message to another server goes over a link this
// server authenticates by dialback, the other server checking the key with
// it; that server's own link is taken once its key checks out with it, and
// the stanzas on it reach the account; a wrong key is refused, as is a stanza
// from a domain the link was not authenticated for; a domain the owner blocks
// is not sent to, and a server that cannot be reached answers the sender
// with an error.
func TestXMPPServerLinks(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	remote := env.remote(t, "remote.test")
	a, err := env.connect(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "")
	if err != nil {
		t.Fatal(err)
	}
	a.online(t)

	a.send(t, `<message xmlns="jabber:client" to="carol@remote.test" type="chat" id="o1"><body>Over the link</body></message>`)
	wait(t, "THE POINT: the message at the other server", func() bool { return len(remote.heard("Over the link")) == 1 })
	if got := remote.heard("Over the link")[0]; !strings.Contains(got, `xmlns="jabber:server"`) || !strings.Contains(got, `from="`+a.full.String()+`"`) {
		t.Errorf("message = %s", got)
	}

	fake.allow(env.alice, "carol@remote.test")
	x, answer := remote.link(t, false)
	if answer != "valid" {
		t.Fatalf("THE POINT: the remote's own link was refused: %s", answer)
	}
	x.write(`<message xmlns="jabber:server" from="carol@remote.test/phone" to="alice@example.com" type="chat" id="i1"><body>Back again</body></message>`)
	a.expect(t, "THE POINT: the remote's message at alice's client", "Back again", `from="carol@remote.test/phone"`)
	env.log.find(t, "message", func(e map[string]any) bool {
		return e["account"] == "x1" && e["conversation"] == "xmpp:carol@remote.test" && e["body"] == "Back again"
	})
	x.write(`<presence xmlns="jabber:server" from="carol@remote.test/phone" to="alice@example.com"><show>dnd</show><status>Unasked</status></presence>`)
	a.silent(t, "THE POINT: presence from someone alice does not see reached her", "Unasked")
	env.log.lock.Lock()
	for _, e := range env.log.events {
		if e["kind"] == "presence" && e["from"] == "xmpp:carol@remote.test" {
			t.Errorf("THE POINT: presence from someone alice does not see reached chat: %v", e)
		}
	}
	env.log.lock.Unlock()
	x.write(`<message xmlns="jabber:server" from="mallory@other.test" to="alice@example.com" type="chat"><body>Forged</body></message>`)
	if s, err := x.next(); err != nil || s.name != "error" || !strings.Contains(s.inner, "not-authorized") {
		t.Errorf("THE POINT: a stanza from a domain the link is not for did not end it: %v %v", s, err)
	}
	a.silent(t, "THE POINT: a stanza from another domain was taken", "Forged")

	// Alice's presence goes over the link only to a contact who sees it.
	alice := &xmpp_account{user: env.alice, id: "x1", address: mustjid("alice@example.com")}
	xmpp_roster_save(alice, &xmpp_item{address: "carol@remote.test", subscription: "to", listed: true})
	a.send(t, `<presence xmlns="jabber:client"><status>Not for carol</status></presence>`)
	time.Sleep(500 * time.Millisecond)
	if heard := remote.heard("Not for carol"); len(heard) != 0 {
		t.Errorf("THE POINT: alice's presence went to carol, who does not see it: %v", heard)
	}
	xmpp_roster_save(alice, &xmpp_item{address: "carol@remote.test", subscription: "both", listed: true})
	a.send(t, `<presence xmlns="jabber:client"><status>For carol</status></presence>`)
	wait(t, "alice's presence at carol, who sees it now", func() bool { return len(remote.heard("For carol")) == 1 })

	if _, answer := remote.link(t, true); answer != "invalid" {
		t.Errorf("THE POINT: a wrong key was taken: %s", answer)
	}
	if remote.ask("example.com", "some-stream", strings.Repeat("0", 64)) {
		t.Error("THE POINT: this server vouched for a key it did not make")
	}

	block_add(db_open("db/networks.db"), "remote.test")
	a.send(t, `<message xmlns="jabber:client" to="carol@remote.test" type="chat" id="o2"><body>Blocked domain</body></message>`)
	a.expect(t, "THE POINT: the owner's block refused", `id="o2"`, `type="error"`, "service-unavailable")
	block_remove(db_open("db/networks.db"), "remote.test")

	a.send(t, `<message xmlns="jabber:client" to="dave@nowhere.test" type="chat" id="o3"><body>Nowhere</body></message>`)
	a.expect(t, "THE POINT: an unreachable server answered", `id="o3"`, `type="error"`, "remote-server-not-found")
	a.send(t, `<message xmlns="jabber:client" to="dave@nowhere.test" type="chat" id="o4"><body>Nowhere again</body></message>`)
	a.expect(t, "the backoff answers at once", `id="o4"`, "remote-server-timeout")
}

// TestXMPPNotices: a notification goes to a linked XMPP account from
// notices@ on the server's domain, by name; what a linked session hears from
// the notices address is not handed to the apps; and no one can take the
// notices name as a handle.
func TestXMPPNotices(t *testing.T) {
	env := create_xmpp_server_env(t)
	env.apps(t)
	remote := env.remote(t, "remote.test")
	db_open("db/queue.db").exec("create table if not exists pushes (id text primary key, user text not null, account text not null, type text not null, identifier text not null default '', data text not null default '', app text not null default '', category text not null default '', object text not null default '', title text not null default '', body text not null default '', link text not null default '', event text not null default '', attempts integer not null default 0, next_retry integer not null, created integer not null)")
	db_user(env.alice, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified, enabled) values ('l2', 'xmpp', '', 'carol@remote.test', '{}', 1, 1, 1)")
	thread := linked_thread(env.alice)
	app, _ := thread.Local("app").(*App)
	permission_grant(env.alice, app.id, "accounts/notify")
	kwargs := linked_kwargs("app", "chat", "category", "message", "object", "c1", "title", "New message", "body", "Hello", "link", "/chat/c1", "account", "l2")
	answer := decoded(t)(api_account_notify(thread, sl.NewBuiltin("test", nil), nil, kwargs))
	if answer["sent"] != int64(1) {
		t.Fatalf("notify = %v", answer)
	}
	wait(t, "THE POINT: the notice at the linked account", func() bool { return len(remote.heard("New message")) == 1 })
	got := remote.heard("New message")[0]
	if !strings.Contains(got, `from="notices@example.com"`) || !strings.Contains(got, `to="carol@remote.test"`) || !strings.Contains(got, "Mochi</nick>") || !strings.Contains(got, "https://example.com/chat/c1") {
		t.Errorf("notice = %s", got)
	}

	for event, want := range map[string]bool{"xmpp:notices@example.com": true, "xmpp:notices@example.com/mochi": true, "xmpp:carol@remote.test": false, "xmpp:notices@remote.test": false} {
		if notices_heard(map[string]any{"kind": "message", "from": event}) != want {
			t.Errorf("THE POINT: notices_heard(%s) != %v", event, want)
		}
	}
	if _, _, reason := handle_available(env.alice, "example.com", "notices", "handle"); reason != "blocked" {
		t.Errorf("THE POINT: the notices name was taken as a handle: %q", reason)
	}
}
