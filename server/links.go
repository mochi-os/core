// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP server links
//
// Stanzas for an address on another server go over a link to it: a stream
// from one of this server's domains to that server's, found by its SRV
// records or on port 5269, secured by TLS with its certificate checked for the
// domain, and authenticated by dialback (XEP-0220, keys as XEP-0185 makes
// them). A link carries stanzas one way; the other server opens its own to
// send this way, and that link is authenticated by asking this server's link
// to it whether the key it gave is one the other server made. A stanza on an
// incoming link is taken only from the domain the link was authenticated for,
// to the domain it was opened to.
//
// A link opens when something is to be sent and closes after ten idle
// minutes. What waits while it opens goes once it is authenticated; where the
// other server cannot be reached or refuses the link, what waits is answered
// with an error to whoever sent it, and for a while after a failure new
// stanzas are answered so at once rather than waiting on a server that is
// down.

package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"mellium.im/xmpp/jid"
)

const xmpp_ns_dialback_feature = "urn:xmpp:features:dialback"

// How long a link may stay idle, and the longest wait before a failed one is
// tried again.
var (
	xmpp_link_idle    = 10 * time.Minute
	xmpp_link_backoff = time.Hour
)

// xmpp_database makes the server-wide XMPP database: the secret dialback keys
// are made with, each hosted account's roster, and the messages waiting for an
// account's clients.
var xmpp_database_lock sync.Mutex

func xmpp_database() *DB {
	xmpp_database_lock.Lock()
	defer xmpp_database_lock.Unlock()
	db, _, reused := db_open_work("db/xmpp.db")
	if db != nil && !reused {
		db.exec("create table if not exists secrets (name text not null primary key, value text not null)")
		db.exec("create table if not exists roster (account text not null, address text not null, name text not null default '', subscription text not null default 'none', ask integer not null default 0, pending integer not null default 0, listed integer not null default 0, created integer not null, updated integer not null, primary key (account, address))")
		db.exec("create table if not exists offline (id integer primary key autoincrement, account text not null, stanza text not null, created integer not null)")
		db.exec("create index if not exists offline_account on offline(account, id)")
	}
	return db
}

// xmpp_secret is the server's secret for dialback keys, made on first use.
func xmpp_secret() string {
	db := xmpp_database()
	if row, _ := db.row("select value from secrets where name='dialback'"); row != nil {
		return row_string(row, "value")
	}
	db.exec("insert or ignore into secrets (name, value) values ('dialback', ?)", random_alphanumeric(48))
	row, _ := db.row("select value from secrets where name='dialback'")
	return row_string(row, "value")
}

// xmpp_local reports whether this server answers for a domain on XMPP: one
// with hosted XMPP accounts, or the server's own, which its notices come
// from.
func xmpp_local(domain string) bool {
	domain = strings.ToLower(domain)
	if domain == "" || !network_enabled("xmpp") {
		return false
	}
	if domain == server_domain() {
		return true
	}
	found, _ := networks_database().exists("select 1 from actors where network='xmpp' and domain=? and deleted=0", domain)
	return found
}

// xmpp_dialback_key is the key the originating domain gives the receiving one
// on a stream, which only this server can make again (XEP-0185).
func xmpp_dialback_key(receiving string, originating string, id string) string {
	secret := sha256.Sum256([]byte(xmpp_secret()))
	m := hmac.New(sha256.New, secret[:])
	m.Write([]byte(strings.ToLower(receiving) + " " + strings.ToLower(originating) + " " + id))
	return hex.EncodeToString(m.Sum(nil))
}

// xmpp_server_dial opens the connection to a domain's server: the hosts its
// SRV records name in their order, else the domain itself on port 5269, each
// address checked as every outbound request's is. A variable so tests can
// connect to a server of their own.
var xmpp_server_dial = func(ctx context.Context, domain string) (net.Conn, error) {
	dialer := net.Dialer{
		Timeout: 10 * time.Second,
		Control: func(network string, address string, _ syscall.RawConn) error {
			return url_address_allowed(address)
		},
	}
	var targets []string
	if _, records, err := net.DefaultResolver.LookupSRV(ctx, "xmpp-server", "tcp", domain); err == nil {
		for _, r := range records {
			if r.Target != "." {
				targets = append(targets, net.JoinHostPort(strings.TrimSuffix(r.Target, "."), strconv.Itoa(int(r.Port))))
			}
		}
	}
	if len(targets) == 0 {
		targets = []string{net.JoinHostPort(domain, "5269")}
	}
	var last error
	for _, target := range targets {
		conn, err := dialer.DialContext(ctx, "tcp", target)
		if err == nil {
			return conn, nil
		}
		last = err
	}
	return nil, last
}

// === Links out ===

// An xmpp_peer is a link from one of this server's domains to another
// server's: its stream once open, whether the other server has accepted it,
// the stanzas waiting for it, and the dialback checks asked on it.
type xmpp_peer struct {
	local    string
	remote   string
	lock     sync.Mutex
	conn     *xmpp_conn
	ready    bool
	running  bool
	waiting  []string
	checks   map[string]chan bool
	failures int
	retry    time.Time
	used     time.Time
	wake     chan struct{}
}

var (
	xmpp_peers      = map[string]*xmpp_peer{}
	xmpp_peers_lock sync.Mutex
)

// xmpp_running counts the links running and the work queued for the apps,
// which a test waits for before its databases go.
var xmpp_running sync.WaitGroup

// xmpp_link_of is the link from a domain of this server's to another's.
func xmpp_peer_of(local string, remote string) *xmpp_peer {
	key := strings.ToLower(local) + " " + strings.ToLower(remote)
	xmpp_peers_lock.Lock()
	defer xmpp_peers_lock.Unlock()
	l := xmpp_peers[key]
	if l == nil {
		l = &xmpp_peer{local: strings.ToLower(local), remote: strings.ToLower(remote), checks: map[string]chan bool{}, wake: make(chan struct{}, 1)}
		xmpp_peers[key] = l
	}
	return l
}

// xmpp_send_remote sends a stanza to another server over the link from the
// sender's domain, refusing a domain the owner blocks.
func xmpp_send_remote(s *xmpp_stanza, from jid.JID, to jid.JID) {
	if err := host_allowed(nil, "xmpp", to.Domainpart()); err != nil {
		xmpp_bounce(s, "service-unavailable")
		return
	}
	l := xmpp_peer_of(from.Domainpart(), to.Domainpart())
	text := s.text(xmpp_ns_server)
	l.lock.Lock()
	if !l.running && time.Now().Before(l.retry) {
		l.lock.Unlock()
		xmpp_bounce(s, "remote-server-timeout")
		return
	}
	l.waiting = append(l.waiting, text)
	start := !l.running
	l.running = true
	l.lock.Unlock()
	if start {
		xmpp_running.Add(1)
		go l.run()
	}
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// xmpp_bounce answers a stanza that could not be sent with an error to its
// sender, routed as any stanza is.
func xmpp_bounce(s *xmpp_stanza, condition string) {
	reply := xmpp_failure(s, condition, "cancel", "")
	if reply == nil || s.name == "presence" {
		return
	}
	from, err := jid.Parse(reply.get("from"))
	to, err2 := jid.Parse(reply.get("to"))
	if err != nil || err2 != nil || !xmpp_local(to.Domainpart()) {
		return
	}
	xmpp_deliver(reply, from, to)
}

// run opens the link, authenticates it, and writes what waits until it has
// been idle too long or fails. A failure answers what waits with an error and
// holds new stanzas off for a backoff; what came as the link closed cleanly
// opens it again.
func (l *xmpp_peer) run() {
	defer xmpp_running.Done()
	for {
		err := l.serve()
		l.lock.Lock()
		l.ready = false
		l.conn = nil
		for id, check := range l.checks {
			close(check)
			delete(l.checks, id)
		}
		if err == nil && len(l.waiting) > 0 {
			l.lock.Unlock()
			continue
		}
		waiting := l.waiting
		l.waiting = nil
		l.running = false
		if err != nil {
			l.failures++
			wait := min(time.Duration(1<<min(l.failures, 10))*15*time.Second, xmpp_link_backoff)
			l.retry = time.Now().Add(wait)
			info("XMPP link %s to %s failed: %v; retrying after %v", l.local, l.remote, err, wait)
		}
		l.lock.Unlock()
		for _, text := range waiting {
			if s, err := xmpp_parse(text); err == nil {
				xmpp_bounce(s, "remote-server-not-found")
			}
		}
		return
	}
}

// xmpp_parse reads a stanza from text.
func xmpp_parse(text string) (*xmpp_stanza, error) {
	x := &xmpp_conn{maximum: len(text) + 1}
	x.limited = &xmpp_limited{r: strings.NewReader(text), left: len(text) + 1}
	x.buffer = bufio.NewReader(x.limited)
	x.restart()
	return x.next()
}

// serve is the link's life: open, authenticate, write what waits.
func (l *xmpp_peer) serve() error {
	ctx, cancel := context.WithTimeout(context.Background(), xmpp_negotiate_timeout)
	defer cancel()
	conn, err := xmpp_server_dial(ctx, l.remote)
	if err != nil {
		return fmt.Errorf("the server could not be reached: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(xmpp_negotiate_timeout))
	x := xmpp_conn_new(conn, xmpp_stanza_server)
	features, err := l.begin(x)
	if err != nil {
		return err
	}
	if !strings.Contains(features, `<starttls`) {
		return fmt.Errorf("the server does not offer TLS")
	}
	x.write(`<starttls xmlns="` + xmpp_ns_tls + `"/>`)
	if s, err := x.next(); err != nil || s.name != "proceed" {
		return fmt.Errorf("the server refused TLS")
	}
	secured := tls.Client(conn, &tls.Config{ServerName: l.remote, RootCAs: xmpp_roots, MinVersion: tls.VersionTLS12})
	if err := secured.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("TLS: %v", err)
	}
	secured.SetDeadline(time.Now().Add(xmpp_negotiate_timeout))
	x = xmpp_conn_new(secured, xmpp_stanza_server)
	if _, err := l.begin(x); err != nil {
		return err
	}
	// The stream is open: dialback checks may go on it from now, while it
	// authenticates itself.
	l.lock.Lock()
	l.conn = x
	l.used = time.Now()
	l.lock.Unlock()
	x.write(`<db:result from="` + xmpp_escape(l.local) + `" to="` + xmpp_escape(l.remote) + `">` + xmpp_dialback_key(l.remote, l.local, x.id) + `</db:result>`)
	failed := make(chan error, 1)
	go func() {
		for {
			secured.SetReadDeadline(time.Now().Add(xmpp_link_idle + time.Minute))
			s, err := x.next()
			if err != nil {
				failed <- err
				return
			}
			if s.space != xmpp_ns_dialback {
				continue
			}
			switch s.name {
			case "result":
				if s.get("type") != "valid" {
					failed <- fmt.Errorf("the server refused dialback")
					return
				}
				l.lock.Lock()
				l.ready = true
				l.failures = 0
				l.lock.Unlock()
				select {
				case l.wake <- struct{}{}:
				default:
				}
			case "verify":
				l.lock.Lock()
				check := l.checks[s.get("id")]
				delete(l.checks, s.get("id"))
				l.lock.Unlock()
				if check != nil {
					check <- s.get("type") == "valid"
				}
			}
		}
	}()
	idle := time.NewTimer(xmpp_link_idle)
	defer idle.Stop()
	deadline := time.NewTimer(xmpp_negotiate_timeout)
	defer deadline.Stop()
	for {
		select {
		case err := <-failed:
			l.lock.Lock()
			ready := l.ready
			l.lock.Unlock()
			if ready && errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-deadline.C:
			l.lock.Lock()
			ready := l.ready
			l.lock.Unlock()
			if !ready {
				return fmt.Errorf("dialback was not answered")
			}
		case <-idle.C:
			l.lock.Lock()
			if len(l.waiting) == 0 && len(l.checks) == 0 {
				l.lock.Unlock()
				x.close()
				return nil
			}
			l.lock.Unlock()
			idle.Reset(xmpp_link_idle)
		case <-l.wake:
			l.lock.Lock()
			if !l.ready {
				l.lock.Unlock()
				continue
			}
			waiting := l.waiting
			l.waiting = nil
			l.used = time.Now()
			l.lock.Unlock()
			for _, text := range waiting {
				if err := x.write(text); err != nil {
					l.lock.Lock()
					l.waiting = append(waiting, l.waiting...)
					l.lock.Unlock()
					return err
				}
			}
			idle.Reset(xmpp_link_idle)
		}
	}
}

// begin opens the link's stream and reads the other server's header and
// features, keeping the stream's id.
func (l *xmpp_peer) begin(x *xmpp_conn) (string, error) {
	if err := x.open(xmpp_ns_server, l.local, l.remote, ""); err != nil {
		return "", err
	}
	attrs, err := x.header()
	if err != nil {
		return "", fmt.Errorf("no stream: %v", err)
	}
	if attrs["xmlns"] != xmpp_ns_server {
		return "", fmt.Errorf("not a server stream")
	}
	x.id = attrs["id"]
	if x.id == "" {
		return "", fmt.Errorf("the stream has no id")
	}
	s, err := x.next()
	if err != nil {
		return "", fmt.Errorf("no features: %v", err)
	}
	if s.space != xmpp_ns_stream || s.name != "features" {
		return "", fmt.Errorf("no features")
	}
	return s.inner, nil
}

// xmpp_dialback_check asks the server a domain is on, over this server's link
// to it, whether a key given on a stream is one it made, and answers whether
// it is. The link opens for the asking where it is not open.
func xmpp_dialback_check(local string, remote string, id string, key string) bool {
	l := xmpp_peer_of(local, remote)
	l.lock.Lock()
	if !l.running {
		if time.Now().Before(l.retry) {
			l.lock.Unlock()
			return false
		}
		l.running = true
		xmpp_running.Add(1)
		go l.run()
	}
	check := make(chan bool, 1)
	l.checks[id] = check
	l.lock.Unlock()
	deadline := time.Now().Add(xmpp_negotiate_timeout)
	sent := false
	for time.Now().Before(deadline) {
		if !sent {
			l.lock.Lock()
			conn := l.conn
			l.lock.Unlock()
			if conn != nil {
				conn.write(`<db:verify from="` + xmpp_escape(local) + `" to="` + xmpp_escape(remote) + `" id="` + xmpp_escape(id) + `">` + xmpp_escape(key) + `</db:verify>`)
				sent = true
			}
		}
		select {
		case valid, ok := <-check:
			return ok && valid
		case <-time.After(50 * time.Millisecond):
		}
	}
	l.lock.Lock()
	delete(l.checks, id)
	l.lock.Unlock()
	return false
}

// === Links in ===

// xmpp_server_serve takes a link another server opens: TLS, then dialback,
// then the stanzas it carries, each from a domain the link was authenticated
// for.
func xmpp_server_serve(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(xmpp_negotiate_timeout))
	x := xmpp_conn_new(conn, xmpp_stanza_server)
	secure := false
	local := ""
	var lock sync.Mutex
	authenticated := map[string]bool{}
	for {
		attrs, err := x.header()
		if err != nil {
			return
		}
		to := strings.ToLower(attrs["to"])
		if local == "" {
			local = to
		}
		x.id = uid()
		x.open(xmpp_ns_server, local, attrs["from"], x.id)
		if attrs["xmlns"] != xmpp_ns_server {
			x.fail("invalid-namespace")
			return
		}
		if to != local || !xmpp_local(local) {
			x.fail("host-unknown")
			return
		}
		if !secure {
			x.write(`<stream:features><starttls xmlns="` + xmpp_ns_tls + `"><required/></starttls></stream:features>`)
			s, err := x.next()
			if err != nil || s.space != xmpp_ns_tls || s.name != "starttls" {
				x.write(`<failure xmlns="` + xmpp_ns_tls + `"/></stream:stream>`)
				return
			}
			x.write(`<proceed xmlns="` + xmpp_ns_tls + `"/>`)
			if err := x.secure(local); err != nil {
				return
			}
			secure = true
			continue
		}
		x.write(`<stream:features><dialback xmlns="` + xmpp_ns_dialback_feature + `"><errors/></dialback></stream:features>`)
		break
	}
	for {
		x.conn.SetReadDeadline(time.Now().Add(xmpp_link_idle + time.Minute))
		x.conn.SetWriteDeadline(time.Time{})
		s, err := x.next()
		if err != nil {
			return
		}
		switch {
		case s.space == xmpp_ns_dialback && s.name == "result":
			from, to := strings.ToLower(s.get("from")), strings.ToLower(s.get("to"))
			key := strings.TrimSpace(s.inner)
			if !xmpp_local(to) || from == "" || host_allowed(nil, "xmpp", from) != nil {
				x.write(`<db:result from="` + xmpp_escape(to) + `" to="` + xmpp_escape(from) + `" type="invalid"/>`)
				continue
			}
			// The check waits on another link, so the stream is read meanwhile.
			go func(id string) {
				valid := xmpp_dialback_check(to, from, id, key)
				if valid {
					lock.Lock()
					authenticated[to+" "+from] = true
					lock.Unlock()
				}
				answer := "invalid"
				if valid {
					answer = "valid"
				}
				x.write(`<db:result from="` + xmpp_escape(to) + `" to="` + xmpp_escape(from) + `" type="` + answer + `"/>`)
			}(x.id)
		case s.space == xmpp_ns_dialback && s.name == "verify":
			from, to := strings.ToLower(s.get("from")), strings.ToLower(s.get("to"))
			answer := "invalid"
			if xmpp_local(to) && hmac.Equal([]byte(strings.TrimSpace(s.inner)), []byte(xmpp_dialback_key(from, to, s.get("id")))) {
				answer = "valid"
			}
			x.write(`<db:verify from="` + xmpp_escape(to) + `" to="` + xmpp_escape(from) + `" id="` + xmpp_escape(s.get("id")) + `" type="` + answer + `"/>`)
		case s.space == xmpp_ns_server && (s.name == "message" || s.name == "presence" || s.name == "iq"):
			from, err := jid.Parse(s.get("from"))
			to, err2 := jid.Parse(s.get("to"))
			if err != nil || err2 != nil {
				if reply := xmpp_failure(s, "jid-malformed", "modify", ""); reply != nil {
					x.write(reply.text(xmpp_ns_server))
				}
				continue
			}
			lock.Lock()
			allowed := authenticated[strings.ToLower(to.Domainpart())+" "+strings.ToLower(from.Domainpart())]
			lock.Unlock()
			if !allowed {
				x.fail("not-authorized")
				return
			}
			xmpp_from_server(s, from, to)
		}
	}
}

// === Upkeep ===

// xmpp_links_manager keeps the engine's tables in bounds: messages kept for
// clients that have not come back for thirty days go, as do sign-in answers
// cached for the acceptance rule.
func xmpp_links_manager() {
	for range time.Tick(time.Hour) {
		if !network_enabled("xmpp") {
			continue
		}
		xmpp_database().exec("delete from offline where created<?", now()-30*86400)
		xmpp_accepted_prune()
		xmpp_peers_lock.Lock()
		keys := make([]string, 0, len(xmpp_peers))
		for key := range xmpp_peers {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			l := xmpp_peers[key]
			l.lock.Lock()
			if !l.running && time.Now().After(l.retry) && len(l.checks) == 0 {
				delete(xmpp_peers, key)
			}
			l.lock.Unlock()
		}
		xmpp_peers_lock.Unlock()
	}
}
