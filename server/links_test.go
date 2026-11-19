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
// of its key. It records the stanzas it was sent, and answers a ping over a
// link of its own back when answers is set.
type xmpp_remote struct {
	env      *xmpp_env
	domain   string
	secret   string
	lock     sync.Mutex
	received []string
	answers  bool
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
			answers := f.answers
			f.lock.Unlock()
			if answers && s.name == "iq" && s.get("type") == "get" && strings.Contains(s.inner, xmpp_ns_ping) {
				// The answer goes on the remote's own link, whose key this
				// server checks over the link this ping came on, so it waits
				// on nothing this loop reads.
				go f.pong(s)
			}
		}
	}
}

// pong answers a ping over a link of the remote's own to the domain it came
// from, as a server answers one.
func (f *xmpp_remote) pong(s *xmpp_stanza) {
	to := s.get("from")
	x, err := f.secured(to)
	if err != nil {
		return
	}
	defer x.conn.Close()
	x.write(`<db:result from="` + f.domain + `" to="` + to + `">` + f.key(to, x.id) + `</db:result>`)
	if answer, err := x.next(); err != nil || answer.get("type") != "valid" {
		return
	}
	x.write(`<iq xmlns="jabber:server" type="result" id="` + s.get("id") + `" from="` + f.domain + `" to="` + to + `"/>`)
	time.Sleep(200 * time.Millisecond)
}

// answering sets whether the remote answers pings.
func (f *xmpp_remote) answering(answers bool) {
	f.lock.Lock()
	f.answers = answers
	f.lock.Unlock()
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
		if notices_heard(env.alice, map[string]any{"kind": "message", "from": event}) != want {
			t.Errorf("THE POINT: notices_heard(%s) != %v", event, want)
		}
	}
	// In the notices room of one of the user's accounts, which their own
	// networks.db keeps; another user's is no notices of theirs.
	db_user_brief(env.alice, "networks").exec("insert into notices (account, room, created) values ('m1', '!notices:example.com', 1)")
	if !notices_heard(env.alice, map[string]any{"kind": "message", "from": "@someone:example.com", "conversation": "!notices:example.com"}) {
		t.Error("THE POINT: a message in the user's notices room was not taken for notices")
	}
	if notices_heard(env.alice, map[string]any{"kind": "message", "from": "@someone:example.com", "conversation": "!other:example.com"}) {
		t.Error("a message in another room was taken for notices")
	}
	if _, _, reason := handle_available(env.alice, "example.com", "notices", "handle"); reason != "blocked" {
		t.Errorf("THE POINT: the notices name was taken as a handle: %q", reason)
	}
}

// subscription_rows is the delivery queue's rows of subscription changes for
// a contact: each one's inbox (the side it changes), body and attempts.
func subscription_rows(t *testing.T, address string) []map[string]any {
	t.Helper()
	rows, err := db_open("db/networks.db").rows("select inbox, body, attempts from deliveries where network='xmpp' and address=? order by inbox", address)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// subscription_due makes a host's rows due again, as their backoff ending
// would.
func subscription_due(host string) {
	db := db_open("db/networks.db")
	db.exec("update deliveries set next=0, claimed=0 where host=?", host)
	db.exec("update hosts set next=0 where host=?", host)
}

// TestXMPPLinkOutlivesAQuietPeer: the other server says nothing on a link this
// server opens once it has accepted it. A deadline on reading it failed every
// link in use eleven minutes after it opened, and what was sent to that
// server in the backoff after was refused or lost.
func TestXMPPLinkOutlivesAQuietPeer(t *testing.T) {
	env := create_xmpp_server_env(t)
	remote := env.remote(t, "quiet.test")
	saved_idle, saved_grace := xmpp_link_idle, xmpp_link_grace
	xmpp_link_idle, xmpp_link_grace = time.Second, 300*time.Millisecond
	t.Cleanup(func() { xmpp_link_idle, xmpp_link_grace = saved_idle, saved_grace })

	alice, carol := mustjid("alice@example.com/phone"), mustjid("carol@quiet.test")
	for i := range 16 {
		xmpp_route(xmpp_make("message", "<body>Tick "+itoa(i)+"</body>", "type", "chat"), alice, carol)
		time.Sleep(150 * time.Millisecond)
	}
	wait(t, "the last message at the other server", func() bool { return len(remote.heard("Tick 15")) == 1 })
	l := xmpp_peer_of("example.com", "quiet.test")
	l.lock.Lock()
	failures := l.failures
	l.lock.Unlock()
	if failures != 0 {
		t.Errorf("THE POINT: the link in use failed %d times while the other server said nothing", failures)
	}
	for i := range 16 {
		if len(remote.heard("Tick "+itoa(i)+"<")) != 1 {
			t.Errorf("THE POINT: message %d did not reach the other server", i)
		}
	}
}

// TestXMPPSubscriptionKeptUntilAnswered: a change to a subscription with a
// contact on another server is kept in the delivery queue until that server
// answers the ping sent after it on the same link, and sent again until then.
// Routed once, it was lost whenever the link was down or dropped what was
// written to it, and the two servers went on disagreeing.
func TestXMPPSubscriptionKeptUntilAnswered(t *testing.T) {
	env := create_xmpp_server_env(t)
	remote := env.remote(t, "far.test")
	saved := xmpp_confirm_timeout
	xmpp_confirm_timeout = 2 * time.Second
	t.Cleanup(func() { xmpp_confirm_timeout = saved })

	alice := &xmpp_account{user: env.alice, id: "x1", address: mustjid("alice@example.com")}
	xmpp_roster_save(alice, &xmpp_item{address: "carol@far.test", subscription: "both", listed: true})
	xmpp_roster_remove(alice, mustjid("carol@far.test"))
	if rows := subscription_rows(t, "carol@far.test"); len(rows) != 2 || row_string(rows[0], "inbox") != "xmpp:carol@far.test#from" || row_string(rows[1], "inbox") != "xmpp:carol@far.test#to" ||
		!strings.Contains(row_string(rows[0], "body"), `type="unsubscribed"`) || !strings.Contains(row_string(rows[1], "body"), `type="unsubscribe"`) {
		t.Fatalf("THE POINT: the removal's two changes are not kept in the queue: %v", rows)
	}

	// The other server takes the changes but does not answer: they are kept.
	delivery_host("xmpp:far.test")
	if len(remote.heard(`type="unsubscribe"`)) != 1 || len(remote.heard(`type="unsubscribed"`)) != 1 {
		t.Fatalf("the changes did not reach the other server: %v", remote.received)
	}
	if rows := subscription_rows(t, "carol@far.test"); len(rows) != 2 || row_int(rows[0], "attempts") != 1 || row_int(rows[1], "attempts") != 1 {
		t.Errorf("THE POINT: changes the other server never answered for were taken as delivered: %v", rows)
	}

	// It answers now: they are sent again, and given up as delivered.
	remote.answering(true)
	subscription_due("xmpp:far.test")
	delivery_host("xmpp:far.test")
	if rows := subscription_rows(t, "carol@far.test"); len(rows) != 0 {
		t.Errorf("THE POINT: changes the other server answered for are still kept: %v", rows)
	}
	if len(remote.heard(`type="unsubscribe"`)) != 2 {
		t.Errorf("the kept change was not sent again: %v", remote.heard(`type="unsubscribe"`))
	}
}

// TestXMPPSubscriptionNotTakenFromAFailedLink: the error this server makes for
// a link it cannot open is no answer from the other server.
func TestXMPPSubscriptionNotTakenFromAFailedLink(t *testing.T) {
	env := create_xmpp_server_env(t)
	saved := xmpp_confirm_timeout
	xmpp_confirm_timeout = 10 * time.Second
	t.Cleanup(func() { xmpp_confirm_timeout = saved })

	alice := &xmpp_account{user: env.alice, id: "x1", address: mustjid("alice@example.com")}
	xmpp_subscription_send(alice, "subscribe", mustjid("dave@nowhere.test"))
	started := time.Now()
	delivery_host("xmpp:nowhere.test")
	if rows := subscription_rows(t, "dave@nowhere.test"); len(rows) != 1 || row_int(rows[0], "attempts") != 1 {
		t.Errorf("THE POINT: a change to a server that cannot be reached was taken as delivered: %v", rows)
	}
	if time.Since(started) > 8*time.Second {
		t.Errorf("the failed link was waited out (%v) rather than answered at once", time.Since(started))
	}
	// The failure is the domain's on XMPP: its fediverse deliveries are not
	// held up for it.
	db := db_open("db/networks.db")
	if found, _ := db.exists("select 1 from hosts where host='nowhere.test'"); found {
		t.Error("THE POINT: an XMPP failure was counted against the domain's fediverse deliveries")
	}
	if found, _ := db.exists("select 1 from hosts where host='xmpp:nowhere.test' and failures=1"); !found {
		t.Error("the XMPP failure was not counted against the domain on XMPP")
	}
}

// TestXMPPSubscriptionChangeReplacesTheWaitingOne: a later change to the same
// side of a subscription replaces one still waiting, so a change sent again
// after a later one cannot undo it; the other side's is kept.
func TestXMPPSubscriptionChangeReplacesTheWaitingOne(t *testing.T) {
	env := create_xmpp_server_env(t)
	alice := &xmpp_account{user: env.alice, id: "x1", address: mustjid("alice@example.com")}
	carol := mustjid("carol@slow.test")
	xmpp_subscription_send(alice, "subscribe", carol)
	xmpp_subscription_send(alice, "subscribed", carol)
	xmpp_subscription_send(alice, "unsubscribe", carol)
	rows := subscription_rows(t, "carol@slow.test")
	if len(rows) != 2 || !strings.Contains(row_string(rows[0], "body"), `type="subscribed"`) || !strings.Contains(row_string(rows[1], "body"), `type="unsubscribe"`) {
		t.Errorf("THE POINT: the waiting changes are not the latest of each side: %v", rows)
	}

	// A contact here is told at once, with nothing kept.
	xmpp_subscription_send(alice, "subscribe", mustjid("bob@example.com"))
	if rows := subscription_rows(t, "bob@example.com"); len(rows) != 0 {
		t.Errorf("a change for a contact here was queued: %v", rows)
	}
	_ = env
}

// TestXMPPApprovalBeforePresence: an approval of a contact on another server
// reaches it before the account's presence (RFC 6121 3.1.5). Sent first, the
// presence is dropped there as from someone the contact does not see, and the
// contact sees the account offline until its presence next changes.
func TestXMPPApprovalBeforePresence(t *testing.T) {
	env := create_xmpp_server_env(t)
	remote := env.remote(t, "far.test")
	remote.answering(true)
	saved := xmpp_confirm_timeout
	xmpp_confirm_timeout = 5 * time.Second
	t.Cleanup(func() { xmpp_confirm_timeout = saved })
	c, err := env.connect(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "")
	if err != nil {
		t.Fatal(err)
	}
	c.online(t)

	alice := &xmpp_account{user: env.alice, id: "x1", address: mustjid("alice@example.com")}
	xmpp_roster_save(alice, &xmpp_item{address: "carol@far.test", subscription: "none", pending: true})
	xmpp_subscription_out(alice, "subscribed", mustjid("carol@far.test"))
	delivery_host("xmpp:far.test")
	wait(t, "alice's presence at the other server", func() bool { return len(remote.heard("<presence", `from="`+c.full.String()+`"`)) > 0 })

	approval, presence := -1, -1
	remote.lock.Lock()
	for i, text := range remote.received {
		if approval < 0 && strings.Contains(text, `type="subscribed"`) {
			approval = i
		}
		if presence < 0 && strings.Contains(text, "<presence") && strings.Contains(text, `from="`+c.full.String()+`"`) && !strings.Contains(text, "type=") {
			presence = i
		}
	}
	remote.lock.Unlock()
	if approval < 0 || presence < approval {
		t.Errorf("THE POINT: the presence (at %d) reached the other server before the approval (at %d)", presence, approval)
	}
}

// TestXMPPConfirmationIsOneWrite: a change and the ping that confirms it are
// written to a link as one. Written apart, the change could go into a
// connection the other server had closed and the ping on the next, whose
// answer then confirmed a change that never arrived.
func TestXMPPConfirmationIsOneWrite(t *testing.T) {
	env := create_xmpp_server_env(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	var lock sync.Mutex
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			lock.Lock()
			held = append(held, conn)
			lock.Unlock()
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		lock.Lock()
		for _, conn := range held {
			conn.Close()
		}
		lock.Unlock()
	})
	env.lock.Lock()
	env.addresses["hold.test"] = listener.Addr().String()
	env.lock.Unlock()
	saved := xmpp_confirm_timeout
	xmpp_confirm_timeout = time.Second
	t.Cleanup(func() { xmpp_confirm_timeout = saved })

	done := make(chan bool, 1)
	go func() {
		done <- xmpp_confirmed(xmpp_make("presence", "", "type", "unsubscribed"), mustjid("alice@example.com"), mustjid("carol@hold.test"))
	}()
	l := xmpp_peer_of("example.com", "hold.test")
	var waiting []string
	wait(t, "the change waiting on the link", func() bool {
		l.lock.Lock()
		defer l.lock.Unlock()
		waiting = append([]string(nil), l.waiting...)
		return len(waiting) > 0
	})
	if len(waiting) != 1 || !strings.Contains(waiting[0], `type="unsubscribed"`) || !strings.Contains(waiting[0], xmpp_ns_ping) {
		t.Errorf("THE POINT: the change and its ping wait apart, to be written apart: %q", waiting)
	}
	if <-done {
		t.Error("a ping nobody answered confirmed the change")
	}
}

// TestXMPPStaleApprovalNotSent: a worker sends the rows it has claimed. An
// approval taken back after its row was claimed is replaced in the queue, and
// the claimed copy is not sent: it went out, and the account's presence after
// it, to the contact just refused.
func TestXMPPStaleApprovalNotSent(t *testing.T) {
	env := create_xmpp_server_env(t)
	remote := env.remote(t, "far.test")
	remote.answering(true)
	saved := xmpp_confirm_timeout
	xmpp_confirm_timeout = 5 * time.Second
	t.Cleanup(func() { xmpp_confirm_timeout = saved })

	alice := &xmpp_account{user: env.alice, id: "x1", address: mustjid("alice@example.com")}
	carol := mustjid("carol@far.test")
	xmpp_subscription_send(alice, "subscribed", carol)
	rows, claimed := delivery_claim("xmpp:far.test")
	if !claimed || len(rows) != 1 {
		t.Fatalf("the approval was not claimed: %v", rows)
	}
	xmpp_subscription_send(alice, "unsubscribed", carol)
	if outcome := delivery_attempt(&rows[0]); outcome != delivery_dropped {
		t.Errorf("the replaced approval's outcome = %v, want dropped", outcome)
	}
	if heard := remote.heard(`type="subscribed"`); len(heard) != 0 {
		t.Errorf("THE POINT: the approval taken back was sent: %v", heard)
	}
}

// TestXMPPPresenceOnlyWhileAllowed: the presence after a confirmed approval
// goes only to a contact who may still see it.
func TestXMPPPresenceOnlyWhileAllowed(t *testing.T) {
	env := create_xmpp_server_env(t)
	remote := env.remote(t, "far.test")
	remote.answering(true)
	saved := xmpp_confirm_timeout
	xmpp_confirm_timeout = 5 * time.Second
	t.Cleanup(func() { xmpp_confirm_timeout = saved })
	c, err := env.connect(t, "alice@example.com", env.mint(t, env.alice, "x1", "Phone"), "")
	if err != nil {
		t.Fatal(err)
	}
	c.online(t)

	// The approval is on its way, and the roster no longer lets carol see
	// alice: taken back while it went.
	alice := &xmpp_account{user: env.alice, id: "x1", address: mustjid("alice@example.com")}
	xmpp_roster_save(alice, &xmpp_item{address: "carol@far.test", subscription: "none", listed: true})
	xmpp_subscription_send(alice, "subscribed", mustjid("carol@far.test"))
	delivery_host("xmpp:far.test")
	if len(remote.heard(`type="subscribed"`)) != 1 {
		t.Fatalf("the approval did not reach the other server: %v", remote.received)
	}
	time.Sleep(300 * time.Millisecond)
	if heard := remote.heard("<presence", `from="`+c.full.String()+`"`); len(heard) != 0 {
		t.Errorf("THE POINT: the presence went to a contact the roster no longer lets see it: %v", heard)
	}
}

// TestXMPPSwitchedOffEndsStreams: switching XMPP off ends the streams already
// open, each told why at its next stanza: another server's link is shut down
// rather than left to queue what it carries, which reaches no one, and a
// client's stream is shut down and its resource ended, so it no longer reads
// as signed in.
func TestXMPPSwitchedOffEndsStreams(t *testing.T) {
	env := create_xmpp_server_env(t)
	fake := env.apps(t)
	remote := env.remote(t, "remote.test")
	phone := env.mint(t, env.alice, "x1", "Phone")
	a, err := env.connect(t, "alice@example.com", phone, "")
	if err != nil {
		t.Fatal(err)
	}
	a.online(t)
	fake.allow(env.alice, "carol@remote.test")
	x, answer := remote.link(t, false)
	if answer != "valid" {
		t.Fatalf("the remote's link was refused: %s", answer)
	}
	desk := env.raw(t, "alice@example.com", phone, "desk")

	setting_set("xmpp", "false")
	t.Cleanup(func() { setting_set("xmpp", "true") })
	x.write(`<message xmlns="jabber:server" from="carol@remote.test/phone" to="alice@example.com" type="chat" id="i1"><body>Switched off</body></message>`)
	if s, err := x.next(); err != nil || s.name != "error" || !strings.Contains(s.inner, "system-shutdown") {
		t.Errorf("THE POINT: the link was not shut down when XMPP was switched off: %v %v", s, err)
	}
	a.silent(t, "THE POINT: a stanza on a link open before XMPP was switched off reached alice", "Switched off")
	env.log.lock.Lock()
	for _, e := range env.log.events {
		if e["body"] == "Switched off" {
			t.Errorf("THE POINT: a stanza on a link open before XMPP was switched off reached chat: %v", e)
		}
	}
	env.log.lock.Unlock()

	desk.send(`<presence xmlns="jabber:client"><status>Still here</status></presence>`)
	desk.expect(t, "THE POINT: the client's stream is shut down once XMPP is off", "system-shutdown")
	wait(t, "THE POINT: the client's resource to end", func() bool { return xmpp_resource_get("alice@example.com", "desk") == nil })
}
