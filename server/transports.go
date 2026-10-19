// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP transports
//
// A client's stream as negotiation and routing see it, whatever carries it
// (the federation plan, phase 26): a connection on the client ports
// (stanzas.go), or for a client in a browser a WebSocket (RFC 7395) or BOSH
// (XEP-0124, XEP-0206). The two web transports are paths of the engine on the
// web server, /xmpp/websocket and /xmpp/bosh, which the host-meta document
// names (XEP-0156); the upload service's files are served beside them
// (uploads.go). Both are secured by the web server's TLS, so they offer no
// STARTTLS, and a client signs in on them as on a connection.
//
// A WebSocket carries one element in each message: the stream opens and ends
// with the framing namespace's open and close, and a stream restart is a new
// open. BOSH carries a session over HTTP requests: each request's body holds
// what the client sends, and is held until something is sent the other way or
// the session's wait passes; the session ends when it is told to, or when no
// request has come for its inactivity.

package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"nhooyr.io/websocket"
)

const (
	xmpp_ns_framing = "urn:ietf:params:xml:ns:xmpp-framing"
	xmpp_ns_bosh    = "http://jabber.org/protocol/httpbind"
	xmpp_ns_xbosh   = "urn:xmpp:xbosh"
)

// An xmpp_stream is one client's stream: read stanza by stanza, written as
// text, opened again after the sign-in, and ended with an error, cleanly, or
// dropped as though it broke. Its end is io.EOF where the client ended it,
// and another error where it broke. deadline bounds how long the next read
// may wait; a zero time lifts it.
type xmpp_stream interface {
	header() (map[string]string, error)
	open(space string, from string, to string, id string) error
	next() (*xmpp_stanza, error)
	write(text string) error
	fail(condition string)
	close()
	abort()
	restart()
	deadline(t time.Time)
}

// deadline bounds the connection's reads and writes.
func (x *xmpp_conn) deadline(t time.Time) {
	x.conn.SetDeadline(t)
}

// abort drops the connection with no word to the client, as one that broke.
func (x *xmpp_conn) abort() {
	x.conn.Close()
}

// xmpp_stream_features is the stream features element, declaring the stream prefix
// itself, since a web transport's message has no stream header around it.
func xmpp_stream_features(inner string) string {
	return `<stream:features xmlns:stream="` + xmpp_ns_stream + `">` + inner + `</stream:features>`
}

// xmpp_engine answers the web transports and the upload service's files.
func xmpp_engine() *engine {
	return &engine{
		name:     "xmpp",
		prefixes: []string{"/xmpp/"},
		setting:  "xmpp",
		body:     int64(media_maximum),
		handler:  xmpp_web_route,
	}
}

// xmpp_web_route dispatches a request on the engine's paths.
func xmpp_web_route(c *gin.Context) bool {
	path := c.Request.URL.Path
	switch {
	case path == "/xmpp/websocket":
		xmpp_websocket_serve(c)
	case path == "/xmpp/bosh":
		xmpp_bosh_serve(c)
	case strings.HasPrefix(path, "/xmpp/upload/"):
		xmpp_upload_serve(c)
	default:
		respond_error(c, http.StatusNotFound, "not_found", "errors.not_found", nil)
	}
	return true
}

// xmpp_host_links are the links the host-meta document gives for a host's
// XMPP accounts (XEP-0156): its WebSocket and BOSH addresses, where it is a
// domain this server answers for on XMPP.
func xmpp_host_links(host string) [][2]string {
	if !xmpp_local(host) || xmpp_room_base(host) != "" {
		return nil
	}
	return [][2]string{
		{"urn:xmpp:alt-connections:websocket", "wss://" + host + "/xmpp/websocket"},
		{"urn:xmpp:alt-connections:xbosh", "https://" + host + "/xmpp/bosh"},
	}
}

// === WebSocket ===

// An xmpp_websocket is a stream carried by a WebSocket, one element to a
// message.
type xmpp_websocket struct {
	conn  *websocket.Conn
	ctx   context.Context
	lock  sync.Mutex
	until time.Time
}

// xmpp_dropped is a WebSocket closed with no close of the stream: a broken
// connection, which a client may resume from.
var xmpp_dropped = errors.New("the connection closed without the stream")

// xmpp_websocket_serve takes a WebSocket a client opens for the xmpp
// subprotocol, from any origin: its sign-in is the stream's, never the
// browser's cookies.
func xmpp_websocket_serve(c *gin.Context) {
	conn, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{Subprotocols: []string{"xmpp"}, InsecureSkipVerify: true})
	if err != nil {
		return
	}
	if conn.Subprotocol() != "xmpp" {
		conn.Close(websocket.StatusPolicyViolation, "the xmpp subprotocol is required")
		return
	}
	conn.SetReadLimit(xmpp_stanza_client)
	w := &xmpp_websocket{conn: conn, ctx: c.Request.Context()}
	w.until = time.Now().Add(xmpp_negotiate_timeout)
	xmpp_client_negotiate(w, c.ClientIP(), true, nil)
	conn.Close(websocket.StatusNormalClosure, "")
}

// read is the next element a message carries.
func (w *xmpp_websocket) read() (*xmpp_stanza, error) {
	for {
		ctx := w.ctx
		if !w.until.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(w.ctx, w.until)
			defer cancel()
		}
		kind, data, err := w.conn.Read(ctx)
		if err != nil {
			switch status := websocket.CloseStatus(err); {
			case status == websocket.StatusMessageTooBig || strings.Contains(err.Error(), "read limited"):
				return nil, xmpp_too_large
			case status != -1:
				return nil, xmpp_dropped
			}
			return nil, err
		}
		if kind != websocket.MessageText {
			return nil, fmt.Errorf("a binary message")
		}
		if strings.TrimSpace(string(data)) == "" {
			continue
		}
		return xmpp_parse(string(data))
	}
}

func (w *xmpp_websocket) header() (map[string]string, error) {
	s, err := w.read()
	if err != nil {
		return nil, err
	}
	if s.space != xmpp_ns_framing || s.name != "open" {
		return nil, fmt.Errorf("not a stream")
	}
	attrs := map[string]string{"xmlns": xmpp_ns_client}
	for _, a := range s.attrs {
		if a.Name.Space == "" {
			attrs[a.Name.Local] = a.Value
		}
	}
	return attrs, nil
}

func (w *xmpp_websocket) open(space string, from string, to string, id string) error {
	text := `<open xmlns="` + xmpp_ns_framing + `"`
	if from != "" {
		text += ` from="` + xmpp_escape(from) + `"`
	}
	if to != "" {
		text += ` to="` + xmpp_escape(to) + `"`
	}
	if id != "" {
		text += ` id="` + xmpp_escape(id) + `"`
	}
	return w.write(text + ` version="1.0" xml:lang="en"/>`)
}

func (w *xmpp_websocket) next() (*xmpp_stanza, error) {
	s, err := w.read()
	if err != nil {
		return nil, err
	}
	if s.space == xmpp_ns_framing {
		if s.name == "close" {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("a stream opened within the stream")
	}
	// Each message is an element of its own, so one without a namespace is
	// in the stream's.
	if s.space == "" {
		s.space = xmpp_ns_client
	}
	return s, nil
}

func (w *xmpp_websocket) write(text string) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return w.conn.Write(ctx, websocket.MessageText, []byte(text))
}

func (w *xmpp_websocket) fail(condition string) {
	w.write(`<stream:error xmlns:stream="` + xmpp_ns_stream + `"><` + condition + ` xmlns="` + xmpp_ns_streams + `"/></stream:error>`)
	w.close()
}

func (w *xmpp_websocket) close() {
	w.write(`<close xmlns="` + xmpp_ns_framing + `"/>`)
	w.conn.Close(websocket.StatusNormalClosure, "")
}

func (w *xmpp_websocket) abort() {
	w.conn.Close(websocket.StatusGoingAway, "")
}

func (w *xmpp_websocket) restart() {}

func (w *xmpp_websocket) deadline(t time.Time) {
	w.until = t
}

// === BOSH ===

// How long a BOSH request is held at most, and how long a session lasts with
// no request held.
var (
	xmpp_bosh_wait       = 60 * time.Second
	xmpp_bosh_inactivity = 60 * time.Second
)

// An xmpp_bosh_item is what a request brings the stream: a stanza, the
// session's start or a restart with its attributes, or the session's end.
type xmpp_bosh_item struct {
	stanza *xmpp_stanza
	attrs  map[string]string
	end    error
}

// xmpp_bosh_inactive is a session's end for want of requests: a broken
// connection, which a client may resume from.
var xmpp_bosh_inactive = errors.New("no request came for the session's inactivity")

// An xmpp_bosh is a BOSH session and the stream it carries: what the
// requests brought, waiting to be read; what waits to be sent; the request
// held for it; and the answers to the latest requests, for a client that asks
// again.
type xmpp_bosh struct {
	sid       string
	lock      sync.Mutex
	inbound   chan xmpp_bosh_item
	outbound  []string
	changed   chan struct{}
	held      chan struct{}
	rid       int64
	wait      time.Duration
	hold      int
	from      string
	id        string
	started   bool
	ended     bool
	condition string
	answers   map[int64]string
	until     time.Time
	idle      *time.Timer
}

var (
	xmpp_bosh_sessions      = map[string]*xmpp_bosh{}
	xmpp_bosh_sessions_lock sync.Mutex
)

// An xmpp_bosh_body is a request's body: its attributes, and the elements it
// carries, each in its own namespace.
type xmpp_bosh_body struct {
	attrs   map[string]string
	stanzas []*xmpp_stanza
	restart bool
	end     bool
}

// xmpp_bosh_read reads a request's body.
func xmpp_bosh_read(text string) (*xmpp_bosh_body, error) {
	d := xml.NewDecoder(strings.NewReader(text))
	d.Strict = true
	b := &xmpp_bosh_body{attrs: map[string]string{}}
	depth := 0
	for {
		t, err := d.Token()
		if err == io.EOF {
			if depth != 0 || len(b.attrs) == 0 && !b.end {
				return nil, fmt.Errorf("not a body")
			}
			return b, nil
		}
		if err != nil {
			return nil, err
		}
		switch t := t.(type) {
		case xml.StartElement:
			if depth == 0 {
				if t.Name.Space != xmpp_ns_bosh || t.Name.Local != "body" {
					return nil, fmt.Errorf("not a body")
				}
				for _, a := range t.Attr {
					switch {
					case a.Name.Space == "":
						b.attrs[a.Name.Local] = a.Value
					case a.Name.Space == xmpp_ns_xbosh:
						b.attrs["xmpp:"+a.Name.Local] = a.Value
					case a.Name.Space == xmpp_ns_xml:
						b.attrs["xml:"+a.Name.Local] = a.Value
					}
				}
				b.restart = b.attrs["xmpp:restart"] == "true"
				b.end = b.attrs["type"] == "terminate"
				depth++
				continue
			}
			var body struct {
				Inner []byte `xml:",innerxml"`
			}
			if err := d.DecodeElement(&body, &t); err != nil {
				return nil, err
			}
			s := &xmpp_stanza{space: t.Name.Space, name: t.Name.Local, inner: string(body.Inner)}
			// An element the body's own namespace qualifies is in the
			// stream's.
			if s.space == xmpp_ns_bosh || s.space == "" {
				s.space = xmpp_ns_client
			}
			for _, a := range t.Attr {
				if a.Name.Space == "" && a.Name.Local == "xmlns" {
					continue
				}
				s.attrs = append(s.attrs, a)
			}
			b.stanzas = append(b.stanzas, s)
		case xml.EndElement:
			depth--
		case xml.CharData:
		default:
			return nil, fmt.Errorf("restricted XML in a body")
		}
	}
}

// xmpp_bosh_answer writes a body to a request.
func xmpp_bosh_answer(c *gin.Context, text string) {
	c.Header("Content-Type", "text/xml; charset=utf-8")
	c.String(http.StatusOK, text)
}

// xmpp_bosh_ended is the body ending a session, with the reason where there
// is one.
func xmpp_bosh_ended(condition string) string {
	text := `<body xmlns="` + xmpp_ns_bosh + `" type="terminate"`
	if condition != "" {
		text += ` condition="` + condition + `"`
	}
	return text + `/>`
}

// xmpp_bosh_serve takes a BOSH request: one making a session, or one of a
// session's, from any origin.
func xmpp_bosh_serve(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	if c.Request.Method == http.MethodOptions {
		c.Header("Access-Control-Allow-Methods", "POST, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		c.Header("Access-Control-Max-Age", "86400")
		c.AbortWithStatus(http.StatusNoContent)
		return
	}
	if c.Request.Method != http.MethodPost {
		c.AbortWithStatus(http.StatusMethodNotAllowed)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, xmpp_stanza_client+4096))
	if err != nil {
		xmpp_bosh_answer(c, xmpp_bosh_ended("bad-request"))
		return
	}
	b, err := xmpp_bosh_read(string(data))
	if err != nil {
		xmpp_bosh_answer(c, xmpp_bosh_ended("bad-request"))
		return
	}
	rid, err := strconv.ParseInt(b.attrs["rid"], 10, 64)
	if err != nil || rid <= 0 {
		xmpp_bosh_answer(c, xmpp_bosh_ended("bad-request"))
		return
	}
	sid := b.attrs["sid"]
	if sid == "" {
		xmpp_bosh_start(c, b, rid)
		return
	}
	xmpp_bosh_sessions_lock.Lock()
	session := xmpp_bosh_sessions[sid]
	xmpp_bosh_sessions_lock.Unlock()
	if session == nil {
		xmpp_bosh_answer(c, xmpp_bosh_ended("item-not-found"))
		return
	}
	session.request(c, b, rid)
}

// xmpp_bosh_start makes a session for a stream to a domain of this server's,
// its stream negotiated as any client's.
func xmpp_bosh_start(c *gin.Context, b *xmpp_bosh_body, rid int64) {
	to := strings.ToLower(b.attrs["to"])
	if !xmpp_local(to) || xmpp_room_base(to) != "" {
		xmpp_bosh_answer(c, xmpp_bosh_ended("host-unknown"))
		return
	}
	wait := xmpp_bosh_wait
	if asked, err := strconv.Atoi(b.attrs["wait"]); err == nil && asked >= 0 && time.Duration(asked)*time.Second < wait {
		wait = time.Duration(asked) * time.Second
	}
	hold := 1
	if b.attrs["hold"] == "0" {
		hold = 0
	}
	session := &xmpp_bosh{sid: random_alphanumeric(32), inbound: make(chan xmpp_bosh_item, 64), changed: make(chan struct{}), rid: rid, wait: wait, hold: hold, answers: map[int64]string{}}
	session.until = time.Now().Add(xmpp_negotiate_timeout)
	session.idle = time.AfterFunc(xmpp_bosh_inactivity, session.expire)
	xmpp_bosh_sessions_lock.Lock()
	xmpp_bosh_sessions[session.sid] = session
	xmpp_bosh_sessions_lock.Unlock()
	attrs := map[string]string{"xmlns": xmpp_ns_client, "to": to, "version": b.attrs["xmpp:version"]}
	if lang := b.attrs["xml:lang"]; lang != "" {
		attrs["lang"] = lang
	}
	session.inbound <- xmpp_bosh_item{attrs: attrs}
	ip := c.ClientIP()
	go func() {
		xmpp_client_negotiate(session, ip, true, nil)
		session.finish("", io.EOF)
	}()
	session.hold_request(c, rid)
}

// request takes a session's request in order: its elements handed to the
// stream, then held for what the stream sends. A request asked again is
// answered as before; one out of order waits for those before it.
func (b *xmpp_bosh) request(c *gin.Context, body *xmpp_bosh_body, rid int64) {
	deadline := time.Now().Add(b.wait + 5*time.Second)
	b.lock.Lock()
	for rid > b.rid+1 && time.Now().Before(deadline) && !b.ended {
		changed := b.changed
		b.lock.Unlock()
		select {
		case <-changed:
		case <-time.After(time.Second):
		}
		b.lock.Lock()
	}
	if rid <= b.rid {
		answer, found := b.answers[rid]
		b.lock.Unlock()
		if !found {
			xmpp_bosh_answer(c, xmpp_bosh_ended("item-not-found"))
			return
		}
		xmpp_bosh_answer(c, answer)
		return
	}
	if rid != b.rid+1 {
		b.lock.Unlock()
		xmpp_bosh_answer(c, xmpp_bosh_ended("item-not-found"))
		b.finish("item-not-found", xmpp_bosh_inactive)
		return
	}
	b.rid = rid
	b.signal()
	ended := b.ended
	b.lock.Unlock()
	if ended {
		xmpp_bosh_answer(c, xmpp_bosh_ended(b.condition))
		return
	}
	if body.restart {
		attrs := map[string]string{"xmlns": xmpp_ns_client, "to": strings.ToLower(body.attrs["to"]), "version": "1.0"}
		b.inbound <- xmpp_bosh_item{attrs: attrs}
	}
	for _, s := range body.stanzas {
		select {
		case b.inbound <- xmpp_bosh_item{stanza: s}:
		case <-time.After(b.wait):
			xmpp_bosh_answer(c, xmpp_bosh_ended("policy-violation"))
			b.finish("policy-violation", xmpp_bosh_inactive)
			return
		}
	}
	if body.end {
		b.lock.Lock()
		b.answers[rid] = xmpp_bosh_ended("")
		b.lock.Unlock()
		xmpp_bosh_answer(c, xmpp_bosh_ended(""))
		b.finish("", io.EOF)
		return
	}
	b.hold_request(c, rid)
}

// hold_request holds a request until the stream sends something, the wait
// passes, a later request takes its place, or the session ends; then answers
// it with what waits.
func (b *xmpp_bosh) hold_request(c *gin.Context, rid int64) {
	b.lock.Lock()
	// The request held before is answered now, so the client always has one
	// to send on.
	if b.held != nil {
		close(b.held)
	}
	held := make(chan struct{})
	b.held = held
	b.idle.Stop()
	b.lock.Unlock()
	timer := time.NewTimer(b.wait)
	defer timer.Stop()
	for {
		b.lock.Lock()
		if len(b.outbound) > 0 || b.ended || b.hold == 0 {
			break
		}
		changed := b.changed
		b.lock.Unlock()
		select {
		case <-changed:
			continue
		case <-held:
		case <-timer.C:
		case <-c.Request.Context().Done():
		}
		b.lock.Lock()
		break
	}
	text := `<body xmlns="` + xmpp_ns_bosh + `" xmlns:stream="` + xmpp_ns_stream + `"`
	if !b.started {
		b.started = true
		text += ` sid="` + b.sid + `" wait="` + strconv.Itoa(int(b.wait/time.Second)) + `" inactivity="` + strconv.Itoa(int(xmpp_bosh_inactivity/time.Second)) + `" polling="2" requests="2" hold="` + strconv.Itoa(b.hold) + `" ver="1.11" maxpause="60" xmlns:xmpp="` + xmpp_ns_xbosh + `" xmpp:version="1.0" xmpp:restartlogic="true"`
		if b.from != "" {
			text += ` from="` + xmpp_escape(b.from) + `"`
		}
		if b.id != "" {
			text += ` authid="` + xmpp_escape(b.id) + `"`
		}
	}
	if b.ended && len(b.outbound) == 0 {
		text = xmpp_bosh_ended(b.condition)
	} else {
		if b.ended {
			text += ` type="terminate"`
			if b.condition != "" {
				text += ` condition="` + b.condition + `"`
			}
		}
		text += `>` + strings.Join(b.outbound, "") + `</body>`
		b.outbound = nil
	}
	b.answers[rid] = text
	delete(b.answers, rid-2)
	if b.held == held {
		b.held = nil
		if !b.ended {
			b.idle.Reset(xmpp_bosh_inactivity)
		}
	}
	ended := b.ended
	b.lock.Unlock()
	xmpp_bosh_answer(c, text)
	if ended {
		b.forget()
	}
}

// expire ends a session no request has come for in its inactivity.
func (b *xmpp_bosh) expire() {
	b.lock.Lock()
	idle := b.held == nil
	b.lock.Unlock()
	if idle {
		b.finish("", xmpp_bosh_inactive)
		b.forget()
	}
}

// finish ends the session: the stream reads its end - io.EOF where the client
// ended it, else the error it broke with - and the request held is answered.
func (b *xmpp_bosh) finish(condition string, end error) {
	b.lock.Lock()
	if b.ended {
		b.lock.Unlock()
		return
	}
	b.ended = true
	if b.condition == "" {
		b.condition = condition
	}
	b.signal()
	b.lock.Unlock()
	select {
	case b.inbound <- xmpp_bosh_item{end: end}:
	default:
	}
}

// forget drops an ended session.
func (b *xmpp_bosh) forget() {
	xmpp_bosh_sessions_lock.Lock()
	if xmpp_bosh_sessions[b.sid] == b {
		delete(xmpp_bosh_sessions, b.sid)
	}
	xmpp_bosh_sessions_lock.Unlock()
}

// signal wakes whatever waits on the session; the lock is held.
func (b *xmpp_bosh) signal() {
	close(b.changed)
	b.changed = make(chan struct{})
}

// item is the next thing the requests brought, or an error once the
// deadline passes.
func (b *xmpp_bosh) item() (xmpp_bosh_item, error) {
	b.lock.Lock()
	until := b.until
	b.lock.Unlock()
	var expired <-chan time.Time
	if !until.IsZero() {
		timer := time.NewTimer(time.Until(until))
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case item := <-b.inbound:
		return item, nil
	case <-expired:
		return xmpp_bosh_item{}, fmt.Errorf("the session was silent too long")
	}
}

func (b *xmpp_bosh) header() (map[string]string, error) {
	item, err := b.item()
	if err != nil {
		return nil, err
	}
	if item.end != nil {
		return nil, item.end
	}
	if item.attrs == nil {
		return nil, fmt.Errorf("not a stream")
	}
	return item.attrs, nil
}

func (b *xmpp_bosh) open(space string, from string, to string, id string) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if !b.started {
		b.from, b.id = from, id
	}
	return nil
}

func (b *xmpp_bosh) next() (*xmpp_stanza, error) {
	item, err := b.item()
	if err != nil {
		return nil, err
	}
	if item.end != nil {
		return nil, item.end
	}
	if item.stanza == nil {
		return nil, fmt.Errorf("a stream opened within the stream")
	}
	return item.stanza, nil
}

func (b *xmpp_bosh) write(text string) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.ended {
		return io.ErrClosedPipe
	}
	b.outbound = append(b.outbound, text)
	b.signal()
	return nil
}

func (b *xmpp_bosh) fail(condition string) {
	b.write(`<stream:error><` + condition + ` xmlns="` + xmpp_ns_streams + `"/></stream:error>`)
	b.finish("remote-stream-error", io.EOF)
}

func (b *xmpp_bosh) close() {
	b.finish("", io.EOF)
}

func (b *xmpp_bosh) abort() {
	b.finish("", xmpp_bosh_inactive)
}

func (b *xmpp_bosh) restart() {}

func (b *xmpp_bosh) deadline(t time.Time) {
	b.lock.Lock()
	b.until = t
	b.lock.Unlock()
}
