// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP streams
//
// The server side of XMPP for hosted accounts (the federation plan, phase 25):
// the listeners, and the streams on them. Clients connect on the client port,
// securing the stream with STARTTLS, or on the direct port in TLS from the
// start (XEP-0368); other servers connect on the server port (links.go). A
// listener the address or port of is already taken marks XMPP unavailable on
// the Status page and is tried again each minute; the server goes on without
// it.
//
// A stream belongs to the domain its header names, which must be one with
// hosted XMPP accounts, and TLS answers with that domain's certificate. A
// client signs in with a password minted for the account (signin.go), by
// SCRAM-SHA-256, SCRAM-SHA-1 or PLAIN, binds a resource and from then on its
// stanzas are routed (routing.go). The account's minted passwords share one
// salt, so one SCRAM exchange tries each of them.
//
// Stanzas are kept as they were written: the element, its attributes and its
// content as text, so what passes through is passed on unchanged but for the
// addresses and the namespace of the stream it goes out on.

package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mellium.im/xmpp/jid"
)

// The namespaces of the stream and of what is negotiated on it.
const (
	xmpp_ns_client   = "jabber:client"
	xmpp_ns_server   = "jabber:server"
	xmpp_ns_stream   = "http://etherx.jabber.org/streams"
	xmpp_ns_streams  = "urn:ietf:params:xml:ns:xmpp-streams"
	xmpp_ns_stanzas  = "urn:ietf:params:xml:ns:xmpp-stanzas"
	xmpp_ns_tls      = "urn:ietf:params:xml:ns:xmpp-tls"
	xmpp_ns_sasl     = "urn:ietf:params:xml:ns:xmpp-sasl"
	xmpp_ns_bind     = "urn:ietf:params:xml:ns:xmpp-bind"
	xmpp_ns_session  = "urn:ietf:params:xml:ns:xmpp-session"
	xmpp_ns_dialback = "jabber:server:dialback"
	xmpp_ns_xml      = "http://www.w3.org/XML/1998/namespace"
)

// The largest stanza a client and another server may send.
const (
	xmpp_stanza_client = 256 << 10
	xmpp_stanza_server = 1 << 20
)

// How long a stream has to negotiate, and how long a stream may be silent
// before it is closed. Clients keep theirs open with whitespace or pings.
var (
	xmpp_negotiate_timeout = 30 * time.Second
	xmpp_idle_timeout      = 15 * time.Minute
)

// The listeners, from mochi.conf's [xmpp] section: the address they take,
// every address by default, and the port for clients, for clients in TLS from
// the start and for other servers; 0 turns one off.
var (
	xmpp_listen_address = ""
	xmpp_ports          = map[string]int{"client": 5222, "direct": 5223, "server": 5269}
)

// The listeners that could not be opened, which make XMPP unavailable.
var (
	xmpp_closed      []string
	xmpp_closed_lock sync.Mutex
)

// xmpp_certificate answers a TLS handshake with the certificate of the domain
// asked for. A variable so tests can answer with one of their own.
var xmpp_certificate = domains_get_certificate

// xmpp_roots are the authorities another server's certificate is checked
// against; nil for the system's. A variable so tests can trust their own.
var xmpp_roots *x509.CertPool

// xmpp_start opens the listeners where the development switch is on, and
// starts the server links' manager.
func xmpp_start() {
	if !networks_development {
		return
	}
	xmpp_listen_address = ini_string("xmpp", "address", "")
	for name, port := range xmpp_ports {
		xmpp_ports[name] = ini_int("xmpp", name, port)
	}
	go xmpp_listen_manager()
	go xmpp_links_manager()
}

// xmpp_unavailable says why XMPP is unavailable on this server, for the
// Status page: "port" while a listener could not be opened.
func xmpp_unavailable() string {
	xmpp_closed_lock.Lock()
	defer xmpp_closed_lock.Unlock()
	if len(xmpp_closed) > 0 {
		return "port"
	}
	return ""
}

// xmpp_listen_manager opens each listener, trying those it could not open
// again each minute until all are open.
func xmpp_listen_manager() {
	open := map[string]bool{}
	for {
		var closed []string
		names := make([]string, 0, len(xmpp_ports))
		for name := range xmpp_ports {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			port := xmpp_ports[name]
			if open[name] || port <= 0 {
				continue
			}
			listener, err := net.Listen("tcp", net.JoinHostPort(xmpp_listen_address, strconv.Itoa(port)))
			if err != nil {
				closed = append(closed, name)
				info("XMPP %s listener on port %d not opened: %v", name, port, err)
				continue
			}
			open[name] = true
			info("XMPP %s listener on port %d", name, port)
			go xmpp_accept(listener, name)
		}
		xmpp_closed_lock.Lock()
		xmpp_closed = closed
		xmpp_closed_lock.Unlock()
		if len(closed) == 0 {
			return
		}
		time.Sleep(time.Minute)
	}
}

// xmpp_accept takes the connections to a listener, each in its own goroutine.
// While XMPP is off they are closed as they come.
func xmpp_accept(listener net.Listener, name string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(time.Second)
			continue
		}
		if !network_enabled("xmpp") {
			conn.Close()
			continue
		}
		switch name {
		case "client":
			go xmpp_client_serve(conn, false)
		case "direct":
			go xmpp_client_serve(tls.Server(conn, xmpp_tls_config("")), true)
		case "server":
			go xmpp_server_serve(conn)
		}
	}
}

// xmpp_tls_config is the TLS a stream is secured with: the certificate of the
// domain asked for by name, else of the domain the stream is for.
func xmpp_tls_config(domain string) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			if hello.ServerName == "" {
				hello.ServerName = domain
			}
			return xmpp_certificate(hello)
		},
	}
}

// === A stream ===

// An xmpp_conn is one side of a stream: the connection, read stanza by stanza
// through a buffer that outlives each restart of the stream, and written
// under a lock.
type xmpp_conn struct {
	conn    net.Conn
	limited *xmpp_limited
	buffer  *bufio.Reader
	decoder *xml.Decoder
	lock    sync.Mutex
	maximum int
	id      string
}

// An xmpp_limited reader refuses more than a stanza's worth of input between
// two resets.
type xmpp_limited struct {
	r    io.Reader
	left int
}

var xmpp_too_large = errors.New("stanza too large")

func (l *xmpp_limited) Read(p []byte) (int, error) {
	if l.left <= 0 {
		return 0, xmpp_too_large
	}
	if len(p) > l.left {
		p = p[:l.left]
	}
	n, err := l.r.Read(p)
	l.left -= n
	return n, err
}

// xmpp_conn_new reads and writes a stream on a connection.
func xmpp_conn_new(conn net.Conn, maximum int) *xmpp_conn {
	x := &xmpp_conn{conn: conn, maximum: maximum}
	x.limited = &xmpp_limited{r: conn, left: maximum}
	x.buffer = bufio.NewReader(x.limited)
	x.restart()
	return x
}

// restart reads a new stream on the same connection, from where the last one
// ended.
func (x *xmpp_conn) restart() {
	x.decoder = xml.NewDecoder(x.buffer)
	x.decoder.Strict = true
}

// header reads a stream's opening: its attributes, and under "db" whether
// it declares the dialback namespace.
func (x *xmpp_conn) header() (map[string]string, error) {
	x.limited.left = x.maximum
	for {
		t, err := x.decoder.Token()
		if err != nil {
			return nil, err
		}
		switch t := t.(type) {
		case xml.ProcInst:
			if t.Target != "xml" {
				return nil, fmt.Errorf("processing instruction in stream")
			}
		case xml.CharData:
		case xml.StartElement:
			if t.Name.Space != xmpp_ns_stream || t.Name.Local != "stream" {
				return nil, fmt.Errorf("not a stream")
			}
			attrs := map[string]string{}
			for _, a := range t.Attr {
				switch {
				case a.Name.Space == "" && a.Name.Local == "xmlns":
					attrs["xmlns"] = a.Value
				case a.Name.Space == "xmlns" && a.Name.Local == "db" && a.Value == xmpp_ns_dialback:
					attrs["db"] = "true"
				case a.Name.Space == "":
					attrs[a.Name.Local] = a.Value
				}
			}
			return attrs, nil
		default:
			return nil, fmt.Errorf("restricted XML in stream")
		}
	}
}

// open writes this side's stream header in a namespace, with the dialback
// namespace for another server.
func (x *xmpp_conn) open(space string, from string, to string, id string) error {
	text := `<?xml version='1.0'?><stream:stream xmlns="` + space + `" xmlns:stream="` + xmpp_ns_stream + `"`
	if space == xmpp_ns_server {
		text += ` xmlns:db="` + xmpp_ns_dialback + `"`
	}
	if from != "" {
		text += ` from="` + xmpp_escape(from) + `"`
	}
	if to != "" {
		text += ` to="` + xmpp_escape(to) + `"`
	}
	if id != "" {
		text += ` id="` + xmpp_escape(id) + `"`
	}
	return x.write(text + ` version="1.0" xml:lang="en">`)
}

// next reads the next element of the stream: a stanza, or one of what is
// negotiated before them. The stream's end is io.EOF.
func (x *xmpp_conn) next() (*xmpp_stanza, error) {
	x.limited.left = x.maximum
	for {
		t, err := x.decoder.Token()
		if err != nil {
			return nil, err
		}
		switch t := t.(type) {
		case xml.StartElement:
			var body struct {
				Inner []byte `xml:",innerxml"`
			}
			if err := x.decoder.DecodeElement(&body, &t); err != nil {
				return nil, err
			}
			s := &xmpp_stanza{space: t.Name.Space, name: t.Name.Local, inner: string(body.Inner)}
			for _, a := range t.Attr {
				if a.Name.Space == "" && a.Name.Local == "xmlns" {
					continue
				}
				s.attrs = append(s.attrs, a)
			}
			return s, nil
		case xml.EndElement:
			return nil, io.EOF
		case xml.CharData:
		default:
			return nil, fmt.Errorf("restricted XML in stream")
		}
	}
}

// write sends text on the stream.
func (x *xmpp_conn) write(text string) error {
	x.lock.Lock()
	defer x.lock.Unlock()
	x.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	_, err := io.WriteString(x.conn, text)
	return err
}

// fail ends the stream with an error.
func (x *xmpp_conn) fail(condition string) {
	x.write(`<stream:error><` + condition + ` xmlns="` + xmpp_ns_streams + `"/></stream:error></stream:stream>`)
	x.conn.Close()
}

// close ends the stream.
func (x *xmpp_conn) close() {
	x.write(`</stream:stream>`)
	x.conn.Close()
}

// tls secures the connection as the server for a domain, and reads the
// stream from it.
func (x *xmpp_conn) secure(domain string) error {
	conn := tls.Server(x.conn, xmpp_tls_config(domain))
	conn.SetDeadline(time.Now().Add(xmpp_negotiate_timeout))
	if err := conn.Handshake(); err != nil {
		return err
	}
	x.conn = conn
	x.limited = &xmpp_limited{r: conn, left: x.maximum}
	x.buffer = bufio.NewReader(x.limited)
	x.restart()
	return nil
}

// === Stanzas ===

// An xmpp_stanza is an element of a stream as it was written: its namespace
// and name, its attributes and its content.
type xmpp_stanza struct {
	space string
	name  string
	attrs []xml.Attr
	inner string
}

// get is the value of one of the stanza's own attributes.
func (s *xmpp_stanza) get(name string) string {
	for _, a := range s.attrs {
		if a.Name.Space == "" && a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// set gives an attribute a value, or removes it for an empty one.
func (s *xmpp_stanza) set(name string, value string) {
	out := s.attrs[:0:0]
	for _, a := range s.attrs {
		if a.Name.Space == "" && a.Name.Local == name {
			continue
		}
		out = append(out, a)
	}
	if value != "" {
		out = append(out, xml.Attr{Name: xml.Name{Local: name}, Value: value})
	}
	s.attrs = out
}

// copy is a stanza of its own to change.
func (s *xmpp_stanza) copy() *xmpp_stanza {
	c := *s
	c.attrs = append([]xml.Attr{}, s.attrs...)
	return &c
}

// text writes the stanza in a stream's namespace: its attributes, the
// namespaces it declares and its language, and its content as it came.
func (s *xmpp_stanza) text(space string) string {
	var b strings.Builder
	b.WriteString("<" + s.name + ` xmlns="` + space + `"`)
	for _, a := range s.attrs {
		switch a.Name.Space {
		case "":
			b.WriteString(" " + a.Name.Local)
		case "xmlns":
			b.WriteString(" xmlns:" + a.Name.Local)
		case xmpp_ns_xml:
			b.WriteString(" xml:" + a.Name.Local)
		default:
			continue
		}
		b.WriteString(`="` + xmpp_escape(a.Value) + `"`)
	}
	if s.inner == "" {
		b.WriteString("/>")
		return b.String()
	}
	b.WriteString(">" + s.inner + "</" + s.name + ">")
	return b.String()
}

// decode reads the stanza into one of the types xmpp.go reads.
func (s *xmpp_stanza) decode(v any) error {
	return xml.Unmarshal([]byte(s.text(xmpp_ns_client)), v)
}

// child is the namespace and name of the stanza's first element: an IQ's
// request.
func (s *xmpp_stanza) child() (string, string) {
	d := xml.NewDecoder(strings.NewReader("<x>" + s.inner + "</x>"))
	depth := 0
	for {
		t, err := d.Token()
		if err != nil {
			return "", ""
		}
		switch t := t.(type) {
		case xml.StartElement:
			depth++
			if depth == 2 {
				return t.Name.Space, t.Name.Local
			}
		case xml.EndElement:
			depth--
		}
	}
}

// xmpp_make is a stanza the server writes: its name, its attributes in pairs,
// and its content.
func xmpp_make(name string, inner string, attrs ...string) *xmpp_stanza {
	s := &xmpp_stanza{space: xmpp_ns_client, name: name, inner: inner}
	for i := 0; i+1 < len(attrs); i += 2 {
		if attrs[i+1] != "" {
			s.attrs = append(s.attrs, xml.Attr{Name: xml.Name{Local: attrs[i]}, Value: attrs[i+1]})
		}
	}
	return s
}

// xmpp_failure is the error answering a stanza: from where it went, to where
// it came from, with the condition, which a stanza of type error never gets.
func xmpp_failure(s *xmpp_stanza, condition string, kind string, extra string) *xmpp_stanza {
	if s.get("type") == "error" {
		return nil
	}
	return xmpp_make(s.name, `<error type="`+kind+`"><`+condition+` xmlns="`+xmpp_ns_stanzas+`"/>`+extra+`</error>`, "from", s.get("to"), "to", s.get("from"), "id", s.get("id"), "type", "error")
}

// === Clients ===

// An xmpp_resource is a client signed in to an account and bound to a
// resource: what it last said of its presence, and what waits to be written
// to it, by a goroutine of its own so that no one waits on a slow client.
type xmpp_resource struct {
	user       *User
	account    string
	address    jid.JID
	full       jid.JID
	conn       *xmpp_conn
	out        chan string
	done       chan struct{}
	once       sync.Once
	lock       sync.Mutex
	available  bool
	priority   int
	presence   string
	connection string
}

// The bound resources, by account address and resource.
var (
	xmpp_resources      = map[string]map[string]*xmpp_resource{}
	xmpp_resources_lock sync.Mutex
)

// send queues text for the client, ending the stream of a client that has
// let too much wait.
func (r *xmpp_resource) send(text string) {
	select {
	case <-r.done:
	case r.out <- text:
	default:
		info("XMPP client %s fell behind; closing its stream", r.full)
		r.stop("policy-violation")
	}
}

// stop ends the client's stream, once, with an error where one is given.
func (r *xmpp_resource) stop(condition string) {
	r.once.Do(func() {
		close(r.done)
		if condition != "" {
			r.conn.fail(condition)
		} else {
			r.conn.close()
		}
	})
}

// writer writes what is queued for the client.
func (r *xmpp_resource) writer() {
	for {
		select {
		case <-r.done:
			return
		case text := <-r.out:
			if err := r.conn.write(text); err != nil {
				r.stop("")
				return
			}
		}
	}
}

// xmpp_resources_of lists an account's bound resources.
func xmpp_resources_of(address string) []*xmpp_resource {
	xmpp_resources_lock.Lock()
	defer xmpp_resources_lock.Unlock()
	var out []*xmpp_resource
	for _, r := range xmpp_resources[address] {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].full.String() < out[j].full.String() })
	return out
}

// xmpp_resource_get is an account's bound resource by its name, or nil.
func xmpp_resource_get(address string, resource string) *xmpp_resource {
	xmpp_resources_lock.Lock()
	defer xmpp_resources_lock.Unlock()
	return xmpp_resources[address][resource]
}

// xmpp_client_serve negotiates a client's stream - TLS, then the sign-in,
// then the resource - and routes its stanzas until it ends.
func xmpp_client_serve(conn net.Conn, direct bool) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(xmpp_negotiate_timeout))
	if direct {
		if err := conn.(*tls.Conn).Handshake(); err != nil {
			return
		}
	}
	x := xmpp_conn_new(conn, xmpp_stanza_client)
	ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	secure := direct
	domain := ""
	var user *User
	var account map[string]any
	for {
		attrs, err := x.header()
		if err != nil {
			return
		}
		to := strings.ToLower(attrs["to"])
		if domain == "" {
			domain = to
		}
		x.id = uid()
		x.open(xmpp_ns_client, domain, "", x.id)
		if attrs["xmlns"] != xmpp_ns_client {
			x.fail("invalid-namespace")
			return
		}
		if to != domain || !xmpp_local(domain) || domain == "" {
			x.fail("host-unknown")
			return
		}
		switch {
		case !secure:
			x.write(`<stream:features><starttls xmlns="` + xmpp_ns_tls + `"><required/></starttls></stream:features>`)
			s, err := x.next()
			if err != nil || s.space != xmpp_ns_tls || s.name != "starttls" {
				x.write(`<failure xmlns="` + xmpp_ns_tls + `"/></stream:stream>`)
				return
			}
			x.write(`<proceed xmlns="` + xmpp_ns_tls + `"/>`)
			if err := x.secure(domain); err != nil {
				return
			}
			secure = true
		case user == nil:
			x.write(`<stream:features><mechanisms xmlns="` + xmpp_ns_sasl + `"><mechanism>SCRAM-SHA-256</mechanism><mechanism>SCRAM-SHA-1</mechanism><mechanism>PLAIN</mechanism></mechanisms></stream:features>`)
			user, account = xmpp_sasl(x, domain, ip)
			if user == nil {
				x.close()
				return
			}
			x.restart()
		default:
			x.write(`<stream:features><bind xmlns="` + xmpp_ns_bind + `"/><session xmlns="` + xmpp_ns_session + `"><optional/></session></stream:features>`)
			r := xmpp_bind(x, user, account, domain)
			if r == nil {
				return
			}
			conn.SetDeadline(time.Time{})
			xmpp_client_run(r)
			return
		}
	}
}

// xmpp_bind waits for the client to bind a resource, answering the requests
// that may come before it, and registers the resource. A resource already
// bound is taken over: the client holding it is told of the conflict and its
// stream ended, as the newer connection is the one in use.
func xmpp_bind(x *xmpp_conn, user *User, account map[string]any, domain string) *xmpp_resource {
	address, err := jid.Parse(row_string(account, "identifier"))
	if err != nil {
		x.fail("internal-server-error")
		return nil
	}
	address = address.Bare()
	for {
		s, err := x.next()
		if err != nil {
			return nil
		}
		if s.name != "iq" || s.space != xmpp_ns_client {
			x.fail("not-authorized")
			return nil
		}
		space, name := s.child()
		switch {
		case space == xmpp_ns_session && name == "session":
			x.write(xmpp_make("iq", "", "type", "result", "id", s.get("id")).text(xmpp_ns_client))
			continue
		case space != xmpp_ns_bind || name != "bind" || s.get("type") != "set":
			x.write(xmpp_failure(s, "not-authorized", "auth", "").text(xmpp_ns_client))
			continue
		}
		var request struct {
			Resource string `xml:"bind>resource"`
		}
		s.decode(&request)
		resource := strings.TrimSpace(request.Resource)
		full, err := address.WithResource(resource)
		if resource == "" || err != nil || len(resource) > 1023 {
			full, _ = address.WithResource("mochi-" + random_alphanumeric(8))
		}
		if full.Resourcepart() == xmpp_mochi_resource {
			full, _ = address.WithResource(xmpp_mochi_resource + "-" + random_alphanumeric(8))
		}
		r := &xmpp_resource{user: user, account: row_string(account, "id"), address: address, full: full, conn: x, out: make(chan string, 512), done: make(chan struct{}), connection: "xmpp-" + uid()}
		xmpp_resources_lock.Lock()
		if xmpp_resources[address.String()] == nil {
			xmpp_resources[address.String()] = map[string]*xmpp_resource{}
		}
		old := xmpp_resources[address.String()][full.Resourcepart()]
		xmpp_resources[address.String()][full.Resourcepart()] = r
		xmpp_resources_lock.Unlock()
		if old != nil {
			old.stop("conflict")
		}
		x.write(xmpp_make("iq", `<bind xmlns="`+xmpp_ns_bind+`"><jid>`+xmpp_escape(full.String())+`</jid></bind>`, "type", "result", "id", s.get("id")).text(xmpp_ns_client))
		return r
	}
}

// xmpp_client_run routes a bound client's stanzas until its stream ends, then
// takes its presence away.
func xmpp_client_run(r *xmpp_resource) {
	go r.writer()
	defer xmpp_resource_end(r)
	for {
		r.conn.conn.SetReadDeadline(time.Now().Add(xmpp_idle_timeout))
		s, err := r.conn.next()
		if err != nil {
			if err == xmpp_too_large {
				r.stop("policy-violation")
			} else {
				r.stop("")
			}
			return
		}
		if s.space != xmpp_ns_client {
			continue
		}
		switch s.name {
		case "message", "presence", "iq":
			xmpp_from_client(r, s)
		}
	}
}

// xmpp_resource_end forgets a client whose stream ended: its presence goes,
// and the account's contacts are told it is unavailable.
func xmpp_resource_end(r *xmpp_resource) {
	xmpp_resources_lock.Lock()
	if xmpp_resources[r.address.String()][r.full.Resourcepart()] == r {
		delete(xmpp_resources[r.address.String()], r.full.Resourcepart())
		if len(xmpp_resources[r.address.String()]) == 0 {
			delete(xmpp_resources, r.address.String())
		}
	}
	xmpp_resources_lock.Unlock()
	r.lock.Lock()
	available := r.available
	r.available = false
	r.lock.Unlock()
	if available {
		xmpp_presence_gone(r)
	}
}

// === Signing in ===

// xmpp_sasl signs a client in by one of the mechanisms offered, against the
// passwords minted for the account the name names on the stream's domain,
// allowing three tries within the sign-in limit per address. It answers the
// user and the account, or nil once the client has had its tries.
func xmpp_sasl(x *xmpp_conn, domain string, ip string) (*User, map[string]any) {
	for range 3 {
		s, err := x.next()
		if err != nil || s.space != xmpp_ns_sasl {
			return nil, nil
		}
		if s.name == "abort" {
			x.write(`<failure xmlns="` + xmpp_ns_sasl + `"><aborted/></failure>`)
			continue
		}
		if s.name != "auth" {
			return nil, nil
		}
		if !rate_limit_login.allow(ip) {
			audit_repeated_failures(ip, rate_limit_login.limit, "xmpp")
			x.write(`<failure xmlns="` + xmpp_ns_sasl + `"><temporary-auth-failure/></failure>`)
			return nil, nil
		}
		initial, ok := xmpp_sasl_data(s.inner)
		if !ok {
			x.write(`<failure xmlns="` + xmpp_ns_sasl + `"><incorrect-encoding/></failure>`)
			continue
		}
		var user *User
		var account map[string]any
		additional := ""
		switch mechanism := s.get("mechanism"); mechanism {
		case "PLAIN":
			user, account = xmpp_plain(initial, domain)
		case "SCRAM-SHA-256", "SCRAM-SHA-1":
			var aborted bool
			user, account, additional, aborted = xmpp_scram(x, mechanism, initial, domain)
			if aborted {
				return nil, nil
			}
		default:
			x.write(`<failure xmlns="` + xmpp_ns_sasl + `"><invalid-mechanism/></failure>`)
			continue
		}
		if user == nil {
			x.write(`<failure xmlns="` + xmpp_ns_sasl + `"><not-authorized/></failure>`)
			continue
		}
		rate_limit_login.reset(ip)
		if additional != "" {
			x.write(`<success xmlns="` + xmpp_ns_sasl + `">` + base64.StdEncoding.EncodeToString([]byte(additional)) + `</success>`)
		} else {
			x.write(`<success xmlns="` + xmpp_ns_sasl + `"/>`)
		}
		return user, account
	}
	return nil, nil
}

// xmpp_sasl_data reads what an auth or response element carries: base64, or
// "=" for nothing.
func xmpp_sasl_data(text string) ([]byte, bool) {
	text = strings.TrimSpace(text)
	if text == "" || text == "=" {
		return []byte{}, true
	}
	data, err := base64.StdEncoding.DecodeString(text)
	return data, err == nil
}

// xmpp_name is the account a name given at sign-in names on a domain: the
// name alone, or the whole address on that domain.
func xmpp_name(name string, domain string) string {
	if local, host, found := strings.Cut(name, "@"); found {
		if !strings.EqualFold(host, domain) {
			return ""
		}
		name = local
	}
	if name == "" {
		return ""
	}
	return name + "@" + domain
}

// xmpp_plain checks a PLAIN sign-in: an identity to act as, which may only be
// the account itself, the name and the password.
func xmpp_plain(data []byte, domain string) (*User, map[string]any) {
	parts := strings.Split(string(data), "\x00")
	if len(parts) != 3 {
		return nil, nil
	}
	address := xmpp_name(parts[1], domain)
	if address == "" || (parts[0] != "" && !strings.EqualFold(parts[0], address)) {
		return nil, nil
	}
	user, account, _ := password_check("xmpp", address, parts[2])
	return user, account
}

// xmpp_scram runs a SCRAM exchange (RFC 5802) against the account's minted
// passwords. They share a salt, so the client's proof is tried against each.
// Someone with no account is given a salt all the same, made from the name,
// so a sign-in says nothing of whether the account exists. It answers the user,
// the account and the server's final message, or aborted where the client
// broke off.
func xmpp_scram(x *xmpp_conn, mechanism string, first []byte, domain string) (*User, map[string]any, string, bool) {
	algorithm, hasher := "sha256", sha256.New
	if mechanism == "SCRAM-SHA-1" {
		algorithm, hasher = "sha1", sha1.New
	}
	// The GS2 header: no channel binding, since none is offered, and an
	// identity to act as only where it is the account itself.
	text := string(first)
	header, bare, found := xmpp_scram_cut(text)
	if !found || (header[0] != "n" && header[0] != "y") {
		return nil, nil, "", false
	}
	fields := xmpp_scram_fields(bare)
	name := strings.ReplaceAll(strings.ReplaceAll(fields["n"], "=2C", ","), "=3D", "=")
	address := xmpp_name(name, domain)
	nonce := fields["r"]
	if address == "" || nonce == "" {
		return nil, nil, "", false
	}
	if header[1] != "" && !strings.EqualFold(header[1], address) {
		return nil, nil, "", false
	}
	salt, iterations, candidates := xmpp_scram_candidates(address, algorithm)
	server := nonce + random_alphanumeric(24)
	reply := "r=" + server + ",s=" + base64.StdEncoding.EncodeToString(salt) + ",i=" + strconv.Itoa(iterations)
	if x.write(`<challenge xmlns="`+xmpp_ns_sasl+`">`+base64.StdEncoding.EncodeToString([]byte(reply))+`</challenge>`) != nil {
		return nil, nil, "", true
	}
	s, err := x.next()
	if err != nil || s.space != xmpp_ns_sasl || s.name != "response" {
		return nil, nil, "", true
	}
	data, ok := xmpp_sasl_data(s.inner)
	if !ok {
		return nil, nil, "", false
	}
	final := string(data)
	at := strings.LastIndex(final, ",p=")
	if at < 0 {
		return nil, nil, "", false
	}
	answer := xmpp_scram_fields(final)
	if answer["c"] != base64.StdEncoding.EncodeToString([]byte(header[0]+","+header[2]+",")) || answer["r"] != server {
		return nil, nil, "", false
	}
	proof, err := base64.StdEncoding.DecodeString(answer["p"])
	if err != nil {
		return nil, nil, "", false
	}
	message := bare + "," + reply + "," + final[:at]
	for _, c := range candidates {
		key := xmpp_hmac(hasher, c.salted, "Client Key")
		stored := hasher()
		stored.Write(key)
		signature := xmpp_hmac(hasher, stored.Sum(nil), message)
		if len(proof) != len(signature) {
			continue
		}
		recovered := make([]byte, len(proof))
		for i := range proof {
			recovered[i] = proof[i] ^ signature[i]
		}
		check := hasher()
		check.Write(recovered)
		if subtle.ConstantTimeCompare(check.Sum(nil), stored.Sum(nil)) != 1 {
			continue
		}
		user, account := account_by_address("xmpp", address)
		if user == nil {
			return nil, nil, "", false
		}
		password_used(c.token, user.UID)
		verifier := xmpp_hmac(hasher, xmpp_hmac(hasher, c.salted, "Server Key"), message)
		return user, account, "v=" + base64.StdEncoding.EncodeToString(verifier), false
	}
	return nil, nil, "", false
}

// xmpp_scram_cut splits a SCRAM client's first message into its GS2 header -
// the binding flag, the identity to act as, and what the flag carries - and
// the message proper.
func xmpp_scram_cut(text string) ([3]string, string, bool) {
	parts := strings.SplitN(text, ",", 3)
	if len(parts) != 3 || parts[0] == "" {
		return [3]string{}, "", false
	}
	identity := ""
	if parts[1] != "" {
		value, found := strings.CutPrefix(parts[1], "a=")
		if !found {
			return [3]string{}, "", false
		}
		identity = strings.ReplaceAll(strings.ReplaceAll(value, "=2C", ","), "=3D", "=")
	}
	return [3]string{parts[0][:1], identity, parts[1]}, parts[2], true
}

// xmpp_scram_fields reads a SCRAM message's attributes.
func xmpp_scram_fields(text string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(text, ",") {
		if key, value, found := strings.Cut(part, "="); found && len(key) == 1 {
			if _, seen := out[key]; !seen {
				out[key] = value
			}
		}
	}
	return out
}

// xmpp_scram_candidates are the salt, iterations and minted passwords a SCRAM
// sign-in for an address is checked against: those sharing the salt most of
// the account's passwords have, or for an address with none, a salt made from
// the address so that it is the same each time.
func xmpp_scram_candidates(address string, algorithm string) ([]byte, int, []password_salted) {
	forms := password_scram("xmpp", address, algorithm)
	counts := map[string]int{}
	best := ""
	for _, f := range forms {
		key := string(f.salt) + "|" + strconv.Itoa(f.iterations)
		counts[key]++
		if counts[key] > counts[best] {
			best = key
		}
	}
	var out []password_salted
	for _, f := range forms {
		if string(f.salt)+"|"+strconv.Itoa(f.iterations) == best {
			out = append(out, f)
		}
	}
	if len(out) > 0 {
		return out[0].salt, out[0].iterations, out
	}
	return xmpp_hmac(sha256.New, []byte(xmpp_secret()), "salt "+strings.ToLower(address))[:16], password_scram_iterations, nil
}

// xmpp_hmac is an HMAC of text under a key.
func xmpp_hmac(hasher func() hash.Hash, key []byte, text string) []byte {
	m := hmac.New(hasher, key)
	m.Write([]byte(text))
	return m.Sum(nil)
}
