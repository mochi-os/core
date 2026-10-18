// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/xml"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
)

// An xmpp_fake is an XMPP server for one account, reached over a TLS pipe:
// every dial makes a new pipe with a server session on its far end that
// accepts the password given, sends what it was told to and then listens.
type xmpp_fake struct {
	password string
	send     []string
	lock     sync.Mutex
	dials    int
	sessions []*xmpp.Session
	cert     tls.Certificate
	received []string
	answer   func(stanza string) []string
}

func create_xmpp_fake(t *testing.T, password string, send ...string) *xmpp_fake {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example.org"}, DNSNames: []string{"example.org"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	certificate, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	f := &xmpp_fake{password: password, send: send, cert: tls.Certificate{Certificate: [][]byte{certificate}, PrivateKey: key}}
	saved := xmpp_dial
	xmpp_dial = f.dial
	t.Cleanup(func() {
		xmpp_dial = saved
		f.lock.Lock()
		defer f.lock.Unlock()
		for _, s := range f.sessions {
			s.Close()
			s.Conn().Close()
		}
	})
	return f
}

// dial hands the client one end of a new TLS pipe and serves the other.
func (f *xmpp_fake) dial(ctx context.Context, address jid.JID) (net.Conn, error) {
	client, server := net.Pipe()
	f.lock.Lock()
	f.dials++
	f.lock.Unlock()
	go f.serve(server, tls.Server(server, &tls.Config{Certificates: []tls.Certificate{f.cert}}))
	return tls.Client(client, &tls.Config{InsecureSkipVerify: true}), nil
}

// serve runs the server's side of a connection. A failed negotiation closes
// the raw pipe, since closing the TLS layer would wait for the client to read
// the alert, which a client closing its own end never does.
func (f *xmpp_fake) serve(raw net.Conn, conn net.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	permissions := func(n *sasl.Negotiator) bool {
		user, password, _ := n.Credentials()
		return string(user) == "alice" && string(password) == f.password
	}
	session, err := xmpp.ReceiveClientSession(ctx, jid.MustParse("example.org"), conn,
		xmpp.SASLServer(permissions, sasl.Plain),
		xmpp.BindCustom(func(j jid.JID, resource string) (jid.JID, error) { return j.WithResource("fake") }),
	)
	if err != nil {
		raw.Close()
		return
	}
	f.lock.Lock()
	f.sessions = append(f.sessions, session)
	f.lock.Unlock()
	// The pipe is synchronous, so the stream is read before anything is sent
	// on it; each stanza goes as its own element.
	go session.Serve(xmpp.HandlerFunc(func(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
		var b bytes.Buffer
		e := xml.NewEncoder(&b)
		e.EncodeToken(start.Copy())
		xmlstream.Copy(e, t)
		e.Flush()
		text := b.String()
		f.lock.Lock()
		f.received = append(f.received, text)
		answer := f.answer
		f.lock.Unlock()
		if answer != nil {
			for _, reply := range answer(text) {
				xmpp_write(t, reply)
			}
		}
		return nil
	}))
	for _, element := range f.send {
		if err := session.Send(ctx, xml.NewDecoder(strings.NewReader(element))); err != nil {
			return
		}
	}
}

// heard answers the stanzas the fake has received that contain every part
// given.
func (f *xmpp_fake) heard(parts ...string) []string {
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

// TestXMPPLink: an XMPP account links by signing in once over TLS, keeping the
// password since the protocol has no token; a refused password is reported;
// the manager's connection sends presence and hands messages and presence to
// linked_inbound; and stopping the session closes the stream.
func TestXMPPLink(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_xmpp_fake(t, "secret",
		`<message xmlns="jabber:client" type="chat" id="m1" from="bob@example.org/phone" to="alice@example.org"><body>hello</body></message>`,
		`<presence xmlns="jabber:client" from="bob@example.org/phone"><show>away</show></presence>`)
	log := &inbound_log{}
	log.install(t)
	thread := linked_thread(alice)
	fn := sl.NewBuiltin("test", nil)

	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("xmpp")}, linked_kwargs("address", "alice@example.org", "password", "wrong")); err == nil || !strings.Contains(err.Error(), "did not accept") {
		t.Errorf("wrong password: %v", err)
	}
	if _, err := api_account_link(thread, fn, sl.Tuple{sl.String("xmpp")}, linked_kwargs("address", "example.org", "password", "secret")); err == nil || !strings.Contains(err.Error(), "invalid address") {
		t.Errorf("address without a name: %v", err)
	}
	account := decoded(t)(api_account_link(thread, fn, sl.Tuple{sl.String("xmpp")}, linked_kwargs("address", "xmpp:Alice@Example.org", "password", "secret")))
	id := account["id"].(string)
	if account["identifier"] != "alice@example.org" || account["label"] != "alice" || account["server"] != "example.org" || account["url"] != "xmpp:alice@example.org" {
		t.Fatalf("linked account = %v", account)
	}
	if granted, _ := account["granted"].([]any); len(granted) != 2 || granted[0] != "chat" || granted[1] != "notify" {
		t.Errorf("granted = %v", account["granted"])
	}
	if l := linked_get(alice, id); l.get("password") != "secret" || l.get("address") != "alice@example.org" || l.get("id") != "xmpp:alice@example.org" {
		t.Fatalf("stored data = %v", l.data)
	}

	wait(t, "the connection", func() bool { connected, _ := linked_connected(alice.UID, id); return connected })
	log.find(t, "session", nil)
	message := log.find(t, "message", nil)
	presence := log.find(t, "presence", nil)
	if message["body"] != "hello" || message["from"] != "xmpp:bob@example.org" || message["conversation"] != "xmpp:bob@example.org" || message["room"] != false || message["id"] != "m1" || message["network"] != "xmpp" || message["account"] != id {
		t.Errorf("message = %v", message)
	}
	if presence["status"] != "away" || presence["from"] != "xmpp:bob@example.org" {
		t.Errorf("presence = %v", presence)
	}
	if present := decoded(t)(api_account_get(thread, fn, sl.Tuple{sl.String(id)}, nil)); present["connected"] != true {
		t.Errorf("present = %v", present)
	}
	fake.lock.Lock()
	dials := fake.dials
	fake.lock.Unlock()
	if dials != 3 {
		t.Errorf("%d dials, want 3: the refused sign-in, the link and the session", dials)
	}

	linked_stop(alice.UID, id)
	if connected, _ := linked_connected(alice.UID, id); connected {
		t.Error("connected after the stop")
	}
	if tested := decoded(t)(api_account_test(thread, fn, sl.Tuple{sl.String(id)}, nil)); tested["success"] != true {
		t.Errorf("test = %v", tested)
	}
}

// TestXMPPRefusedStopsTheSession: a session whose password the server stops
// accepting ends without retrying, marks the account, and starts again when
// the account is linked afresh.
func TestXMPPRefusedStopsTheSession(t *testing.T) {
	alice := create_linked_test_env(t)
	fake := create_xmpp_fake(t, "secret")
	db := db_user(alice, "user")
	db.exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('x1', 'xmpp', 'alice', 'alice@example.org', ?, 1, 1)", json_encode(map[string]any{"id": "xmpp:alice@example.org", "address": "alice@example.org", "server": "example.org", "password": "old"}))
	linked_start(linked_get(alice, "x1"))
	wait(t, "the session to give up", func() bool {
		_, running := linked_sessions[linked_session_key(alice.UID, "x1")]
		return !running
	})
	if l := linked_get(alice, "x1"); !strings.Contains(l.get("error"), "link it again") {
		t.Errorf("error = %q", l.get("error"))
	}
	fake.lock.Lock()
	dials := fake.dials
	fake.lock.Unlock()
	if dials != 1 {
		t.Errorf("%d dials, want 1: no retry of a refused credential", dials)
	}
	if present := account_present(alice, map[string]any{"id": "x1", "type": "xmpp", "data": json_encode(linked_get(alice, "x1").data)}); present["connected"] != false || present["error"] == nil {
		t.Errorf("present = %v", present)
	}
}
