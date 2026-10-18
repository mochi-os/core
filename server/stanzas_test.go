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
	"encoding/base64"
	"encoding/xml"
	"fmt"
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

// An xmpp_env is two users with hosted XMPP accounts on example.com - alice
// (x1) and bob (x2) - the engine's client and server listeners open on
// loopback, every domain's certificate issued by an authority the test
// trusts, other servers reached at the addresses the test names, and the
// inbound log.
type xmpp_env struct {
	alice     *User
	bob       *User
	log       *inbound_log
	client    string
	server    string
	roots     *x509.CertPool
	authority *x509.Certificate
	key       *ecdsa.PrivateKey
	lock      sync.Mutex
	addresses map[string]string
	certs     map[string]*tls.Certificate
}

func create_xmpp_server_env(t *testing.T) *xmpp_env {
	t.Helper()
	alice := create_signin_test_env(t)
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username, role) values ('u-bob', 'bob@example.com', 'user')")
	users.exec("insert into entities (id, fingerprint, user, class, name) values ('e-bob', 'fp-bob123', 'u-bob', 'person', 'Bob')")
	users.exec("insert into handles (domain, name, user, kind, created) values ('example.com', 'bob', 'u-bob', 'handle', 1)")
	bob := user_by_uid("u-bob")
	db_user(bob, "user").exec("insert into accounts (id, type, label, identifier, data, created, verified) values ('x2', 'xmpp', '', 'bob@example.com', '{\"hosted\":true,\"handle\":\"bob\",\"domain\":\"example.com\"}', 1, 1)")
	actor_register(alice, "x1", "xmpp", "example.com", "alice")
	actor_register(bob, "x2", "xmpp", "example.com", "bob")
	db_open("db/sessions.db").exec("create table if not exists seen (user text not null primary key, time integer not null)")
	if !network_enabled("xmpp") {
		t.Fatal("xmpp is off in the test environment")
	}
	env := &xmpp_env{alice: alice, bob: bob, addresses: map[string]string{}, certs: map[string]*tls.Certificate{}}
	env.key, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Test authority"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, _ := x509.CreateCertificate(rand.Reader, template, template, &env.key.PublicKey, env.key)
	env.authority, _ = x509.ParseCertificate(der)
	env.roots = x509.NewCertPool()
	env.roots.AddCert(env.authority)

	saved_certificate, saved_roots, saved_dial, saved_pause := xmpp_certificate, xmpp_roots, xmpp_server_dial, xmpp_pause
	xmpp_certificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		return env.certificate(hello.ServerName), nil
	}
	xmpp_roots = env.roots
	xmpp_server_dial = func(ctx context.Context, domain string) (net.Conn, error) {
		env.lock.Lock()
		address := env.addresses[domain]
		env.lock.Unlock()
		if address == "" {
			return nil, fmt.Errorf("no such host %s", domain)
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", address)
	}
	xmpp_pause = 200 * time.Millisecond

	client, _ := net.Listen("tcp", "127.0.0.1:0")
	server, _ := net.Listen("tcp", "127.0.0.1:0")
	env.client, env.server = client.Addr().String(), server.Addr().String()
	go xmpp_accept(client, "client")
	go xmpp_accept(server, "server")
	env.log = &inbound_log{}
	env.log.install(t)
	t.Cleanup(func() {
		client.Close()
		server.Close()
		xmpp_resources_lock.Lock()
		var running []*xmpp_resource
		for _, resources := range xmpp_resources {
			for _, r := range resources {
				running = append(running, r)
			}
		}
		xmpp_resources = map[string]map[string]*xmpp_resource{}
		xmpp_resources_lock.Unlock()
		for _, r := range running {
			r.stop("")
		}
		xmpp_peers_lock.Lock()
		peers := xmpp_peers
		xmpp_peers = map[string]*xmpp_peer{}
		xmpp_peers_lock.Unlock()
		for _, l := range peers {
			l.lock.Lock()
			if l.conn != nil {
				l.conn.conn.Close()
			}
			l.lock.Unlock()
		}
		xmpp_queues_drain()
		xmpp_settle()
		presence_lock.Lock()
		presence_connections = map[string]map[string]*presence_connection{}
		presence_lock.Unlock()
		xmpp_acceptances, xmpp_friend_lists, xmpp_cards = map[string]xmpp_acceptance{}, map[string]xmpp_friend_list{}, map[string]xmpp_card{}
		xmpp_heard, xmpp_told, xmpp_shown, xmpp_markable = map[string]map[string]map[string][2]string{}, map[string]string{}, map[string]string{}, map[string]string{}
		rate_limit_login.reset("127.0.0.1")
		xmpp_certificate, xmpp_roots, xmpp_server_dial, xmpp_pause = saved_certificate, saved_roots, saved_dial, saved_pause
	})
	return env
}

// xmpp_settle waits, for up to five seconds, for the links still running, the
// work queued for the apps and the presence notifications still going out, so
// none outlives the test's databases.
func xmpp_settle() {
	done := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		xmpp_running.Wait()
		presence_pending.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
}

// xmpp_queues_drain waits for each user's queue to the apps to empty, so no
// work outlives the test's databases.
func xmpp_queues_drain() {
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		xmpp_queues_lock.Lock()
		waiting := 0
		for _, queue := range xmpp_queues {
			waiting += len(queue)
		}
		xmpp_queues_lock.Unlock()
		if waiting == 0 {
			time.Sleep(20 * time.Millisecond)
			return
		}
	}
}

// certificate is a domain's certificate, issued by the test's authority.
func (env *xmpp_env) certificate(domain string) *tls.Certificate {
	env.lock.Lock()
	defer env.lock.Unlock()
	if cert := env.certs[domain]; cert != nil {
		return cert
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: domain}, DNSNames: []string{domain}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, _ := x509.CreateCertificate(rand.Reader, template, env.authority, &key.PublicKey, env.key)
	cert := &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	env.certs[domain] = cert
	return cert
}

// mint mints a password for a user's hosted account.
func (env *xmpp_env) mint(t *testing.T, user *User, account string, name string) string {
	t.Helper()
	app := create_external_app("minter")
	permission_grant(user, app.id, "accounts/write")
	permission_grant(user, app.id, "tokens/create")
	value, err := api_account_password(create_test_thread(user, app), sl.NewBuiltin("test", nil), sl.Tuple{sl.String(account), sl.String(name)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return string(value.(sl.String))
}

// An xmpp_test_client is a client of the engine's, built on another
// implementation of the protocol: the stanzas it has received, as text.
type xmpp_test_client struct {
	session  *xmpp.Session
	full     jid.JID
	lock     sync.Mutex
	received []string
}

// connect signs a client in to an account with a password by the mechanisms
// given, bound to a resource.
func (env *xmpp_env) connect(t *testing.T, address string, password string, resource string, mechanisms ...sasl.Mechanism) (*xmpp_test_client, error) {
	t.Helper()
	if len(mechanisms) == 0 {
		mechanisms = []sasl.Mechanism{sasl.ScramSha256}
	}
	conn, err := net.Dial("tcp", env.client)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := jid.MustParse(address)
	if resource != "" {
		origin, _ = origin.WithResource(resource)
	}
	session, err := xmpp.NewClientSession(ctx, origin, conn,
		xmpp.StartTLS(&tls.Config{ServerName: origin.Domainpart(), RootCAs: env.roots}),
		xmpp.SASL("", password, mechanisms...),
		xmpp.BindResource(),
	)
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &xmpp_test_client{session: session, full: session.LocalAddr()}
	go session.Serve(xmpp.HandlerFunc(func(r xmlstream.TokenReadEncoder, start *xml.StartElement) error {
		var b bytes.Buffer
		e := xml.NewEncoder(&b)
		e.EncodeToken(start.Copy())
		xmlstream.Copy(e, r)
		e.Flush()
		c.lock.Lock()
		c.received = append(c.received, b.String())
		c.lock.Unlock()
		return nil
	}))
	t.Cleanup(func() {
		session.Close()
		session.Conn().Close()
	})
	return c, nil
}

// send writes a stanza from the client.
func (c *xmpp_test_client) send(t *testing.T, text string) {
	t.Helper()
	if err := c.session.Send(context.Background(), xmpp_reader(text)); err != nil {
		t.Fatalf("send: %v", err)
	}
}

// heard answers the stanzas the client received that contain every part.
func (c *xmpp_test_client) heard(parts ...string) []string {
	c.lock.Lock()
	defer c.lock.Unlock()
	var out []string
	for _, text := range c.received {
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

// expect waits for a stanza with every part, and answers it.
func (c *xmpp_test_client) expect(t *testing.T, what string, parts ...string) string {
	t.Helper()
	var found string
	wait(t, what, func() bool {
		heard := c.heard(parts...)
		if len(heard) > 0 {
			found = heard[0]
			return true
		}
		return false
	})
	return found
}

// silent checks that no stanza with every part arrives within a moment.
func (c *xmpp_test_client) silent(t *testing.T, what string, parts ...string) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	if heard := c.heard(parts...); len(heard) > 0 {
		t.Errorf("%s: %v", what, heard)
	}
}

// online sends the client's initial presence.
func (c *xmpp_test_client) online(t *testing.T) {
	t.Helper()
	c.send(t, `<presence xmlns="jabber:client"/>`)
}

// TestXMPPSignIn: a client signs in with a minted password by SCRAM-SHA-256,
// SCRAM-SHA-1 or PLAIN after STARTTLS, and binds the resource it asks for;
// every password of the account shares one salt, so each signs in by SCRAM;
// a wrong password, another account's password and an unknown account are
// refused alike; a stream to a domain with no XMPP accounts is refused; and a
// second client taking a resource ends the first one's stream.
func TestXMPPSignIn(t *testing.T) {
	env := create_xmpp_server_env(t)
	phone := env.mint(t, env.alice, "x1", "Phone")
	laptop := env.mint(t, env.alice, "x1", "Laptop")
	other := env.mint(t, env.bob, "x2", "Phone")

	forms := password_scram("xmpp", "alice@example.com", "sha256")
	if len(forms) != 2 || string(forms[0].salt) != string(forms[1].salt) {
		t.Fatalf("the account's passwords do not share a salt: %d forms", len(forms))
	}
	for _, c := range []struct {
		name      string
		password  string
		mechanism sasl.Mechanism
	}{{"SCRAM-SHA-256 with the first password", phone, sasl.ScramSha256}, {"SCRAM-SHA-256 with the second", laptop, sasl.ScramSha256}, {"SCRAM-SHA-1", laptop, sasl.ScramSha1}, {"PLAIN", phone, sasl.Plain}} {
		client, err := env.connect(t, "alice@example.com", c.password, "desk", c.mechanism)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if !strings.HasPrefix(client.full.String(), "alice@example.com/mochi-") {
			t.Errorf("%s bound %s", c.name, client.full)
		}
		client.session.Close()
	}
	for _, c := range []struct {
		name     string
		address  string
		password string
	}{{"a wrong password", "alice@example.com", "mochi-wrong"}, {"bob's password", "alice@example.com", other}, {"no such account", "carol@example.com", phone}} {
		for _, mechanism := range []sasl.Mechanism{sasl.ScramSha256, sasl.Plain} {
			if _, err := env.connect(t, c.address, c.password, "", mechanism); err == nil || !strings.Contains(err.Error(), "not-authorized") {
				t.Errorf("%s by %s: %v", c.name, mechanism.Name, err)
			}
		}
		rate_limit_login.reset("127.0.0.1")
	}
	if _, err := env.connect(t, "alice@elsewhere.test", phone, ""); err == nil || !strings.Contains(err.Error(), "host-unknown") {
		t.Errorf("a stream to another domain: %v", err)
	}

	// The resource asked for is bound; the same resource again ends the
	// first client's stream, as the newer client holds it.
	older := env.raw(t, "alice@example.com", phone, "desk")
	if older.full != "alice@example.com/desk" {
		t.Errorf("bound %s, not the resource asked for", older.full)
	}
	newer := env.raw(t, "alice@example.com", laptop, "desk")
	older.expect(t, "the conflict", "<conflict")
	if r := xmpp_resource_get("alice@example.com", "desk"); r == nil || r.full.String() != newer.full {
		t.Error("the second client does not hold the resource")
	}
	if strings.HasPrefix(env.raw(t, "alice@example.com", phone, "mochi").full, "alice@example.com/mochi-") == false {
		t.Error("a client took the resource Mochi's own sessions show under")
	}
}

// An xmpp_raw_client is a client written stanza by stanza, for what the
// other implementation cannot ask: what it received, as text.
type xmpp_raw_client struct {
	x        *xmpp_conn
	full     string
	lock     sync.Mutex
	received []string
}

// raw signs a client in by PLAIN and binds the resource asked for.
func (env *xmpp_env) raw(t *testing.T, address string, password string, resource string) *xmpp_raw_client {
	t.Helper()
	conn, err := net.Dial("tcp", env.client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	local, domain, _ := strings.Cut(address, "@")
	x := xmpp_conn_new(conn, 1<<20)
	step := func(text string) *xmpp_stanza {
		t.Helper()
		if text != "" {
			x.write(text)
		}
		s, err := x.next()
		if err != nil {
			t.Fatalf("raw client: %v", err)
		}
		return s
	}
	begin := func() {
		t.Helper()
		x.write(`<?xml version='1.0'?><stream:stream xmlns="jabber:client" xmlns:stream="http://etherx.jabber.org/streams" to="` + domain + `" version="1.0">`)
		if _, err := x.header(); err != nil {
			t.Fatalf("raw client header: %v", err)
		}
		step("")
	}
	begin()
	if s := step(`<starttls xmlns="urn:ietf:params:xml:ns:xmpp-tls"/>`); s.name != "proceed" {
		t.Fatalf("starttls answered %s", s.name)
	}
	secured := tls.Client(conn, &tls.Config{ServerName: domain, RootCAs: env.roots})
	if err := secured.Handshake(); err != nil {
		t.Fatal(err)
	}
	x = xmpp_conn_new(secured, 1<<20)
	begin()
	credentials := base64.StdEncoding.EncodeToString([]byte("\x00" + local + "\x00" + password))
	if s := step(`<auth xmlns="urn:ietf:params:xml:ns:xmpp-sasl" mechanism="PLAIN">` + credentials + `</auth>`); s.name != "success" {
		t.Fatalf("auth answered %s", s.name)
	}
	x.restart()
	begin()
	s := step(`<iq xmlns="jabber:client" type="set" id="b1"><bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"><resource>` + resource + `</resource></bind></iq>`)
	var bound struct {
		JID string `xml:"bind>jid"`
	}
	s.decode(&bound)
	c := &xmpp_raw_client{x: x, full: bound.JID}
	conn.SetDeadline(time.Time{})
	go func() {
		for {
			s, err := x.next()
			if err != nil {
				c.lock.Lock()
				c.received = append(c.received, fmt.Sprintf("<closed>%v</closed>", err))
				c.lock.Unlock()
				return
			}
			c.lock.Lock()
			c.received = append(c.received, s.text(s.space))
			c.lock.Unlock()
		}
	}()
	return c
}

// expect waits for a stanza with every part, and answers it.
func (c *xmpp_raw_client) expect(t *testing.T, what string, parts ...string) string {
	t.Helper()
	var found string
	wait(t, what, func() bool {
		c.lock.Lock()
		defer c.lock.Unlock()
		for _, text := range c.received {
			all := true
			for _, part := range parts {
				all = all && strings.Contains(text, part)
			}
			if all {
				found = text
				return true
			}
		}
		return false
	})
	return found
}

// TestXMPPListeners: a listener whose port is taken makes XMPP unavailable on
// the Status page, with the reason, and is tried again until it opens.
func TestXMPPListeners(t *testing.T) {
	create_xmpp_server_env(t)
	taken, _ := net.Listen("tcp", "127.0.0.1:0")
	defer taken.Close()
	_, port, _ := net.SplitHostPort(taken.Addr().String())
	saved_address, saved_ports := xmpp_listen_address, xmpp_ports
	xmpp_listen_address = "127.0.0.1"
	xmpp_ports = map[string]int{"client": 0, "direct": 0, "server": int(atoi(port, 0))}
	t.Cleanup(func() {
		xmpp_listen_address, xmpp_ports = saved_address, saved_ports
		xmpp_closed_lock.Lock()
		xmpp_closed = nil
		xmpp_closed_lock.Unlock()
	})
	go xmpp_listen_manager()
	wait(t, "the listener to fail", func() bool { return xmpp_unavailable() == "port" })
	if state, reason := network_state("xmpp"); state != "unavailable" || reason != "port" {
		t.Errorf("state = %s %s", state, reason)
	}
}
