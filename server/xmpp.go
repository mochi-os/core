// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP client
//
// A linked XMPP account, signed in with the user's address and password. The
// protocol has no token to keep, so the password is kept, and the manager
// holds one connection per linked account for as long as it is linked: the
// server is found by its SRV records, the stream secured by TLS, the account
// authenticated by SCRAM where the server offers it and PLAIN otherwise.
//
// While it runs, the session takes copies of what the account's other clients
// send and receive (carbons), reads from the server's archive what arrived
// since it last ran, joins the rooms the account has bookmarked, and hands
// everything to the chat functions as conversation events (conversations.go):
// messages and their corrections and retractions, typing, delivery receipts and
// read markers, presence, and room invitations. It sends the same kinds the
// other way, shows the user's presence as they share it, and answers the
// receipts other clients ask for, which only say a message arrived.

package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/dial"
	"mellium.im/xmpp/jid"
)

// xmpp_connect_timeout bounds finding the server and negotiating the stream.
const xmpp_connect_timeout = 30 * time.Second

// xmpp_query_timeout bounds a request to the server or a room. A variable so
// tests can lower it.
var xmpp_query_timeout = 20 * time.Second

// xmpp_pause is how long after the last keystroke the other side is told the
// user stopped typing, since chat says only that they are typing.
var xmpp_pause = 10 * time.Second

// How much of the archive a session reads: the latest messages when the
// account is first linked, and in pages up to a bound after that.
const (
	xmpp_archive_first = 100
	xmpp_archive_page  = 50
	xmpp_archive_pages = 20
)

// The client's identity and features, as service discovery and the presence's
// capabilities hash announce them, so that others ask for receipts and markers.
const xmpp_node = "https://mochi-os.org"

var xmpp_features = []string{
	"http://jabber.org/protocol/caps",
	"http://jabber.org/protocol/chatstates",
	"http://jabber.org/protocol/disco#info",
	"jabber:x:conference",
	"jabber:x:oob",
	"eu.siacs.conversations.axolotl.devicelist+notify",
	"urn:xmpp:chat-markers:0",
	"urn:xmpp:message-correct:0",
	"urn:xmpp:message-retract:1",
	"urn:xmpp:ping",
	"urn:xmpp:receipts",
	"urn:xmpp:omemo:2:devices+notify",
	"urn:xmpp:reply:0",
}

// xmpp_dial opens the connection to an address's server: the SRV records
// decide the host and whether TLS is immediate, and the dialer's control hook
// refuses a private address as every outbound request does. A variable so
// tests can hand over a connection of their own.
var xmpp_dial = func(ctx context.Context, address jid.JID) (net.Conn, error) {
	dialer := dial.Dialer{
		Dialer: net.Dialer{
			Timeout: 10 * time.Second,
			Control: func(network string, address string, _ syscall.RawConn) error {
				return url_address_allowed(address)
			},
		},
		TLSConfig: &tls.Config{ServerName: address.Domainpart()},
	}
	return dialer.Dial(ctx, "tcp", address)
}

// xmpp_address reads the address a user typed as a bare JID.
func xmpp_address(text string) (jid.JID, error) {
	text = strings.TrimPrefix(strings.TrimSpace(text), "xmpp:")
	address, err := jid.Parse(text)
	if err != nil || address.Localpart() == "" || address.Domainpart() == "" {
		return jid.JID{}, fmt.Errorf("invalid address")
	}
	return address.Bare(), nil
}

// xmpp_session opens an authenticated session for an account.
func xmpp_session(ctx context.Context, address jid.JID, password string) (*xmpp.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, xmpp_connect_timeout)
	defer cancel()
	conn, err := xmpp_dial(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", linked_unreachable, err)
	}
	session, err := xmpp.NewClientSession(ctx, address, conn,
		xmpp.StartTLS(&tls.Config{ServerName: address.Domainpart()}),
		xmpp.SASL("", password, sasl.ScramSha256Plus, sasl.ScramSha1Plus, sasl.ScramSha256, sasl.ScramSha1, sasl.Plain),
		xmpp.BindResource(),
	)
	if err != nil {
		conn.Close()
		if xmpp_refused(err) {
			return nil, linked_signed_out
		}
		return nil, fmt.Errorf("the session could not be started: %w", err)
	}
	return session, nil
}

// xmpp_refused reports whether a negotiation error is the server refusing
// the credential.
func xmpp_refused(err error) bool {
	text := err.Error()
	for _, condition := range []string{"not-authorized", "credentials-expired", "account-disabled", "invalid-authzid"} {
		if strings.Contains(text, condition) {
			return true
		}
	}
	return false
}

// xmpp_close ends a session and its connection, giving the stream five
// seconds to close cleanly.
func xmpp_close(session *xmpp.Session) {
	session.SetCloseDeadline(time.Now().Add(5 * time.Second))
	session.Close()
	if conn := session.Conn(); conn != nil {
		conn.Close()
	}
}

// xmpp_link signs in once with the fields of the form - the address and the
// password - and answers the account and what to store, the password among it.
func xmpp_link(ctx context.Context, user *User, fields map[string]string) (*linked_profile, map[string]any, error) {
	address, err := xmpp_address(fields["address"])
	if err != nil {
		return nil, nil, err
	}
	if err := host_allowed(user, "", address.Domainpart()); err != nil {
		return nil, nil, err
	}
	session, err := xmpp_session(ctx, address, fields["password"])
	if err != nil {
		if err == linked_signed_out {
			return nil, nil, fmt.Errorf("the server did not accept the sign-in")
		}
		return nil, nil, err
	}
	go xmpp_close(session)
	bare := address.String()
	return &linked_profile{
			ID:      "xmpp:" + bare,
			Address: bare,
			Name:    address.Localpart(),
			URL:     "xmpp:" + bare,
			Server:  address.Domainpart(),
		}, map[string]any{
			"address":  bare,
			"password": fields["password"],
		}, nil
}

// xmpp_verify signs in once to check the credential.
func xmpp_verify(ctx context.Context, l *linked) error {
	address, err := xmpp_address(l.get("address"))
	if err != nil {
		return err
	}
	session, err := xmpp_session(ctx, address, l.get("password"))
	if err != nil {
		return err
	}
	go xmpp_close(session)
	return nil
}

// === Writing stanzas ===

// xmpp_escape writes text for an attribute or an element's content.
func xmpp_escape(text string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(text))
	return b.String()
}

// xmpp_raw reads a stanza written as text exactly as it is written, so that
// each namespace is declared once where it is written; Go's resolving reader
// would repeat every declaration.
type xmpp_raw struct{ d *xml.Decoder }

func (r xmpp_raw) Token() (xml.Token, error) {
	t, err := r.d.RawToken()
	if err != nil {
		return nil, err
	}
	return xml.CopyToken(t), nil
}

func xmpp_reader(text string) xml.TokenReader {
	return xmpp_raw{d: xml.NewDecoder(strings.NewReader(text))}
}

// xmpp_text writes what a reader holds back as text, for an answer read
// whole: a decoder reading tokens leaves a field of its inner XML empty. Each
// element's namespace is written from its name, so the declarations it came
// with are dropped rather than repeated.
func xmpp_text(r xml.TokenReader) ([]byte, error) {
	var b bytes.Buffer
	e := xml.NewEncoder(&b)
	for {
		token, err := r.Token()
		if start, ok := token.(xml.StartElement); ok {
			start.Attr = slices.DeleteFunc(slices.Clone(start.Attr), func(a xml.Attr) bool {
				return a.Name.Space == "xmlns" || a.Name.Space == "" && a.Name.Local == "xmlns"
			})
			token = start
		}
		if token != nil {
			if failure := e.EncodeToken(token); failure != nil {
				return nil, failure
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if err := e.Flush(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// xmpp_write writes a stanza on a handler's stream, which is how a handler
// answers: the session's own send methods wait for the handler to return.
func xmpp_write(w xmlstream.TokenWriter, text string) error {
	_, err := xmlstream.Copy(w, xmpp_reader(text))
	return err
}

// xmpp_caps is the capabilities hash of the client's identity and features.
func xmpp_caps() string {
	features := slices.Clone(xmpp_features)
	slices.Sort(features)
	text := "client/pc//Mochi<" + strings.Join(features, "<") + "<"
	sum := sha1.Sum([]byte(text))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// === The stanzas read ===

type xmpp_reference struct {
	ID string `xml:"id,attr"`
}

type xmpp_delay struct {
	Stamp string `xml:"stamp,attr"`
}

type xmpp_forwarded struct {
	Delay   *xmpp_delay   `xml:"urn:xmpp:delay delay"`
	Message *xmpp_message `xml:"message"`
}

type xmpp_identifier struct {
	ID string `xml:"id,attr"`
	By string `xml:"by,attr"`
}

type xmpp_fallback struct {
	For    string `xml:"for,attr"`
	Bodies []struct {
		Start *int `xml:"start,attr"`
		End   *int `xml:"end,attr"`
	} `xml:"body"`
}

// An xmpp_message is a message with every part the session reads.
type xmpp_message struct {
	From    string  `xml:"from,attr"`
	To      string  `xml:"to,attr"`
	ID      string  `xml:"id,attr"`
	Type    string  `xml:"type,attr"`
	Body    string  `xml:"body"`
	Subject *string `xml:"subject"`

	Active    *struct{} `xml:"http://jabber.org/protocol/chatstates active"`
	Composing *struct{} `xml:"http://jabber.org/protocol/chatstates composing"`
	Paused    *struct{} `xml:"http://jabber.org/protocol/chatstates paused"`
	Inactive  *struct{} `xml:"http://jabber.org/protocol/chatstates inactive"`
	Gone      *struct{} `xml:"http://jabber.org/protocol/chatstates gone"`

	Request      *struct{}       `xml:"urn:xmpp:receipts request"`
	Received     *xmpp_reference `xml:"urn:xmpp:receipts received"`
	Markable     *struct{}       `xml:"urn:xmpp:chat-markers:0 markable"`
	Marked       *xmpp_reference `xml:"urn:xmpp:chat-markers:0 received"`
	Displayed    *xmpp_reference `xml:"urn:xmpp:chat-markers:0 displayed"`
	Acknowledged *xmpp_reference `xml:"urn:xmpp:chat-markers:0 acknowledged"`

	Replace *xmpp_reference `xml:"urn:xmpp:message-correct:0 replace"`
	Retract *xmpp_reference `xml:"urn:xmpp:message-retract:1 retract"`
	Fasten  *struct {
		ID      string    `xml:"id,attr"`
		Retract *struct{} `xml:"urn:xmpp:message-retract:0 retract"`
	} `xml:"urn:xmpp:fasten:0 apply-to"`
	Reply     *xmpp_reference `xml:"urn:xmpp:reply:0 reply"`
	Reactions *struct {
		ID    string   `xml:"id,attr"`
		Items []string `xml:"reaction"`
	} `xml:"urn:xmpp:reactions:0 reactions"`
	Nick      string            `xml:"http://jabber.org/protocol/nick nick"`
	Fallbacks []xmpp_fallback   `xml:"urn:xmpp:fallback:0 fallback"`
	Delay     *xmpp_delay       `xml:"urn:xmpp:delay delay"`
	Stanzas   []xmpp_identifier `xml:"urn:xmpp:sid:0 stanza-id"`
	Origin    *xmpp_reference   `xml:"urn:xmpp:sid:0 origin-id"`
	Links     []struct {
		URL string `xml:"url"`
	} `xml:"jabber:x:oob x"`

	Carbons []struct {
		XMLName   xml.Name
		Forwarded xmpp_forwarded `xml:"urn:xmpp:forward:0 forwarded"`
	} `xml:"urn:xmpp:carbons:2 received"`
	Sent []struct {
		Forwarded xmpp_forwarded `xml:"urn:xmpp:forward:0 forwarded"`
	} `xml:"urn:xmpp:carbons:2 sent"`
	Result *struct {
		Query     string         `xml:"queryid,attr"`
		ID        string         `xml:"id,attr"`
		Forwarded xmpp_forwarded `xml:"urn:xmpp:forward:0 forwarded"`
	} `xml:"urn:xmpp:mam:2 result"`

	Invitation *struct {
		Invite *struct {
			From string `xml:"from,attr"`
		} `xml:"invite"`
	} `xml:"http://jabber.org/protocol/muc#user x"`
	Conference *struct {
		JID string `xml:"jid,attr"`
	} `xml:"jabber:x:conference x"`

	// The elements an encrypted message is carried in, any of them.
	OMEMO   *omemo_element `xml:"eu.siacs.conversations.axolotl encrypted"`
	OMEMO2  *omemo_element `xml:"urn:xmpp:omemo:2 encrypted"`
	OpenPGP *struct{}      `xml:"urn:xmpp:openpgp:0 openpgp"`
	Legacy  *struct{}      `xml:"jabber:x:encrypted x"`

	// A device list someone's server tells of as it changes.
	Event *struct {
		Items struct {
			Node string       `xml:"node,attr"`
			Item []omemo_item `xml:"item"`
		} `xml:"items"`
	} `xml:"http://jabber.org/protocol/pubsub#event event"`

	// What the account's OMEMO device read of an encrypted message: its
	// body, or that it carried only keys (omemo.go).
	opened    *string
	transport bool
}

// encrypted reports whether a message's content is encrypted: by OMEMO, its
// successor, OpenPGP for XMPP or the legacy form. Such a message's body is
// only a fallback saying so.
func (m *xmpp_message) encrypted() bool {
	return m.OMEMO != nil || m.OMEMO2 != nil || m.OpenPGP != nil || m.Legacy != nil
}

// archived is the id a service gave the message in its archive: the
// account's own archive for a conversation, the room's for a room.
func (m *xmpp_message) archived(by string) string {
	for _, s := range m.Stanzas {
		if s.By == by {
			return s.ID
		}
	}
	return ""
}

// text is the message's body without the parts a fallback marks as being for
// clients that do not understand a reply: the quoted message.
func (m *xmpp_message) text() string {
	body := m.Body
	if m.opened != nil {
		body = *m.opened
	}
	for _, f := range m.Fallbacks {
		if f.For != "urn:xmpp:reply:0" {
			continue
		}
		for _, b := range f.Bodies {
			if b.Start == nil || b.End == nil {
				continue
			}
			runes := []rune(body)
			start, end := *b.Start, *b.End
			if start < 0 || end > len(runes) || start >= end {
				continue
			}
			body = string(runes[:start]) + string(runes[end:])
		}
	}
	return strings.TrimSpace(body)
}

type xmpp_presence struct {
	From   string `xml:"from,attr"`
	Type   string `xml:"type,attr"`
	Show   string `xml:"show"`
	Status string `xml:"status"`
	User   *struct {
		Items    []xmpp_occupant `xml:"item"`
		Statuses []struct {
			Code string `xml:"code,attr"`
		} `xml:"status"`
	} `xml:"http://jabber.org/protocol/muc#user x"`
	Error *struct {
		Inner string `xml:",innerxml"`
	} `xml:"error"`
}

// An xmpp_occupant is what a room says of someone in it: their address where
// it shows it, their affiliation and role, and a new nick.
type xmpp_occupant struct {
	JID         string `xml:"jid,attr"`
	Affiliation string `xml:"affiliation,attr"`
	Role        string `xml:"role,attr"`
	Nick        string `xml:"nick,attr"`
}

// codes are the status codes a room's presence carries.
func (p *xmpp_presence) codes() map[string]bool {
	out := map[string]bool{}
	if p.User != nil {
		for _, s := range p.User.Statuses {
			out[s.Code] = true
		}
	}
	return out
}

// item is what a room's presence says of the occupant, or nil.
func (p *xmpp_presence) item() *xmpp_occupant {
	if p.User == nil || len(p.User.Items) == 0 {
		return nil
	}
	return &p.User.Items[0]
}

type xmpp_request struct {
	ID    string    `xml:"id,attr"`
	From  string    `xml:"from,attr"`
	Type  string    `xml:"type,attr"`
	Ping  *struct{} `xml:"urn:xmpp:ping ping"`
	Disco *struct {
		Node string `xml:"node,attr"`
	} `xml:"http://jabber.org/protocol/disco#info query"`
}

// An xmpp_head is the part of an answer every request reads: whether it
// failed, and how.
type xmpp_head struct {
	Type  string `xml:"type,attr"`
	Error *struct {
		Conditions []xml.Name `xml:",any"`
	} `xml:"error"`
}

func (h *xmpp_head) head() *xmpp_head { return h }

type xmpp_headed interface{ head() *xmpp_head }

// === The session ===

// An xmpp_client is a running session and what it keeps while it runs: the
// rooms joined and the nick in each, the ids it sent to rooms (a room echoes a
// message to its sender), the last markable message of each conversation, the
// pauses due after typing, the archive queries waiting for their results and
// the joins waiting for the room's answer. What it hears goes to the app in
// order through a queue, so the stream is read while the app works.
type xmpp_client struct {
	l        *linked
	session  *xmpp.Session
	address  jid.JID
	lock     sync.Mutex
	rooms    map[string]string
	sent     map[string]int64
	markable map[string]string
	typing   map[string]*time.Timer
	queries  map[string]*xmpp_query
	joins    map[string]chan error
	services map[string]string
	queue    chan []map[string]any
	shown    string
}

// An xmpp_query is an archive query collecting its results.
type xmpp_query struct {
	events []map[string]any
}

// xmpp_run is the account's connection: a session is opened, the app told,
// presence sent, and every stanza handled until the stream ends. It returns
// when the connection fails, to be run again, or with linked_signed_out when
// the server refuses the credential.
func xmpp_run(ctx context.Context, l *linked, s *linked_session) error {
	address, err := xmpp_address(l.get("address"))
	if err != nil {
		return linked_signed_out
	}
	session, err := xmpp_session(ctx, address, l.get("password"))
	if err != nil {
		return err
	}
	c := &xmpp_client{l: l, session: session, address: address, rooms: map[string]string{}, sent: map[string]int64{}, markable: map[string]string{}, typing: map[string]*time.Timer{}, queries: map[string]*xmpp_query{}, joins: map[string]chan error{}, services: map[string]string{}, queue: make(chan []map[string]any, 256)}
	done := make(chan struct{})
	// What the session starts runs while it does, and ends before it does.
	var started sync.WaitGroup
	defer started.Wait()
	running, stop := context.WithCancel(ctx)
	defer stop()
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			xmpp_close(session)
		case <-done:
		}
	}()
	started.Go(func() { c.deliver(done) })
	defer c.stop()
	// The app hears that the session began and says whether the user's
	// presence is shared before any is sent.
	linked_inbound(l.user, l.id, "xmpp", []map[string]any{{"kind": "session"}})
	served := make(chan error, 1)
	go func() {
		served <- session.Serve(xmpp.HandlerFunc(c.handle))
	}()
	if err := c.announce(ctx, true); err != nil {
		xmpp_close(session)
		return err
	}
	s.client.Store(c)
	defer s.client.Store(nil)
	s.connected.Store(true)
	s.outage.Store(nil)
	started.Go(func() { c.begin(running) })
	for {
		select {
		case err := <-served:
			xmpp_close(session)
			if ctx.Err() != nil {
				return nil
			}
			if err == nil || err == io.EOF {
				return linked_closed
			}
			return fmt.Errorf("%w: %v", linked_closed, err)
		case <-s.presence:
			c.announce(ctx, false)
		}
	}
}

// deliver hands the session's events to the app in the order they came.
func (c *xmpp_client) deliver(done chan struct{}) {
	for {
		select {
		case events := <-c.queue:
			linked_inbound(c.l.user, c.l.id, "xmpp", events)
		case <-done:
			return
		}
	}
}

// emit queues events for the app.
func (c *xmpp_client) emit(events []map[string]any) {
	if len(events) > 0 {
		c.queue <- events
	}
}

// stop ends the pauses still due.
func (c *xmpp_client) stop() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, timer := range c.typing {
		timer.Stop()
	}
}

// begin runs what follows signing in: copies of the account's other clients'
// traffic, what the archive holds since the session last ran, and the rooms.
func (c *xmpp_client) begin(ctx context.Context) {
	omemo_start(c.l.user, c.l.id)
	if err := c.query(ctx, `<iq xmlns="jabber:client" type="set"><enable xmlns="urn:xmpp:carbons:2"/></iq>`, &xmpp_head{}); err != nil {
		debug("XMPP account %s: carbons not enabled: %v", c.l.id, err)
	}
	c.catchup(ctx)
	c.rejoin(ctx)
}

// send writes a stanza from outside the handler. mellium is given a context
// it never sees cancelled: it watches the one a send is given, and one
// cancelled just after the send returns, as an action's is when it ends,
// can put the connection's write deadline in the past while another write
// is under way, failing it and, under TLS, every write after it.
func (c *xmpp_client) send(ctx context.Context, text string) error {
	return c.session.Send(context.WithoutCancel(ctx), xmpp_reader(text))
}

// query sends a request and reads its answer into into, failing on an error
// answer.
func (c *xmpp_client) query(ctx context.Context, text string, into xmpp_headed) error {
	ctx, cancel := context.WithTimeout(ctx, xmpp_query_timeout)
	defer cancel()
	response, err := c.session.SendIQ(ctx, xmpp_reader(text))
	if err != nil {
		return err
	}
	defer response.Close()
	answer, err := xmpp_text(response)
	if err != nil {
		return err
	}
	if err := xml.Unmarshal(answer, into); err != nil {
		return err
	}
	if head := into.head(); head.Type == "error" {
		condition := "error"
		if head.Error != nil && len(head.Error.Conditions) > 0 {
			condition = head.Error.Conditions[0].Local
		}
		return fmt.Errorf("the server answered %s", condition)
	}
	return nil
}

// handle reads one stanza. It runs with the stream held, so it answers on the
// handler's own stream and hands everything else on.
func (c *xmpp_client) handle(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	d := xml.NewTokenDecoder(xmlstream.MultiReader(xmlstream.Token(*start), t))
	switch start.Name.Local {
	case "message":
		var m xmpp_message
		if err := d.Decode(&m); err != nil && err != io.EOF {
			return nil
		}
		c.message(t, &m)
	case "presence":
		var p xmpp_presence
		if err := d.Decode(&p); err != nil && err != io.EOF {
			return nil
		}
		c.presence(&p)
	case "iq":
		var q xmpp_request
		if err := d.Decode(&q); err != nil && err != io.EOF {
			return nil
		}
		c.answer(t, &q)
	}
	return nil
}

// answer replies to a request: a ping, a question about the client's features,
// and anything else is not offered here.
func (c *xmpp_client) answer(t xmlstream.TokenWriter, q *xmpp_request) {
	if q.Type != "get" && q.Type != "set" {
		return
	}
	to := ""
	if q.From != "" {
		to = ` to="` + xmpp_escape(q.From) + `"`
	}
	head := `<iq xmlns="jabber:client" id="` + xmpp_escape(q.ID) + `"` + to
	switch {
	case q.Type == "get" && q.Ping != nil:
		xmpp_write(t, head+` type="result"/>`)
	case q.Type == "get" && q.Disco != nil:
		var b strings.Builder
		b.WriteString(head + ` type="result"><query xmlns="http://jabber.org/protocol/disco#info"`)
		if q.Disco.Node != "" {
			b.WriteString(` node="` + xmpp_escape(q.Disco.Node) + `"`)
		}
		b.WriteString(`><identity category="client" type="pc" name="Mochi"/>`)
		for _, feature := range xmpp_features {
			b.WriteString(`<feature var="` + feature + `"/>`)
		}
		b.WriteString(`</query></iq>`)
		xmpp_write(t, b.String())
	default:
		xmpp_write(t, head+` type="error"><error type="cancel"><service-unavailable xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></iq>`)
	}
}

// message reads a message: copies from the account's other clients, a result
// of an archive query, or one that arrived for the account.
func (c *xmpp_client) message(t xmlstream.TokenWriter, m *xmpp_message) {
	from, err := jid.Parse(m.From)
	if m.From != "" && err != nil {
		return
	}
	own := m.From == "" || from.Equal(c.address)
	// A carbon or an archive result comes from the account's own address
	// only; from anyone else it is a forgery.
	if len(m.Carbons) > 0 || len(m.Sent) > 0 {
		if !own {
			return
		}
		for _, carbon := range m.Carbons {
			if inner := carbon.Forwarded.Message; inner != nil {
				c.emit(c.read(inner, carbon.Forwarded.Delay, false))
			}
		}
		for _, carbon := range m.Sent {
			if inner := carbon.Forwarded.Message; inner != nil {
				c.emit(c.read(inner, carbon.Forwarded.Delay, true))
			}
		}
		return
	}
	if m.Result != nil {
		c.result(m)
		return
	}
	if m.Type == "error" {
		return
	}
	// Someone's OMEMO device list as their server tells of it changing.
	if m.Event != nil && len(m.Event.Items.Item) > 0 {
		if d, err := omemo_device_of(c.l.user, c.l.id); err == nil {
			d.heard(context.Background(), from, m.Event.Items.Node, m.Event.Items.Item[0].Inner)
		}
		return
	}
	events := c.read(m, m.Delay, false)
	c.emit(events)
	// A receipt asked for says the message arrived; it is not a read marker,
	// so the user's switches do not cover it.
	if m.Request != nil && m.ID != "" && m.Type != "groupchat" && !from.Bare().Equal(c.address) {
		for _, event := range events {
			if event["kind"] == "message" {
				xmpp_write(t, `<message xmlns="jabber:client" to="`+xmpp_escape(m.From)+`" id="`+xmpp_escape(uid())+`"><received xmlns="urn:xmpp:receipts" id="`+xmpp_escape(m.ID)+`"/></message>`)
				break
			}
		}
	}
}

// result files an archive result with the query that asked for it.
func (c *xmpp_client) result(m *xmpp_message) {
	c.lock.Lock()
	q := c.queries[m.Result.Query]
	c.lock.Unlock()
	if q == nil || m.Result.Forwarded.Message == nil {
		return
	}
	// The account's archive answers from the account's own address.
	if from, _ := jid.Parse(m.From); m.From != "" && !from.Equal(c.address) {
		return
	}
	events := c.read(m.Result.Forwarded.Message, m.Result.Forwarded.Delay, false)
	for _, event := range events {
		if event["kind"] == "message" && event["archive"] == "" {
			event["archive"] = m.Result.ID
		}
	}
	c.lock.Lock()
	q.events = append(q.events, events...)
	c.lock.Unlock()
}

// read turns a message into the events it carries. sent marks one the
// account sent from another client.
func (c *xmpp_client) read(m *xmpp_message, delay *xmpp_delay, sent bool) []map[string]any {
	from, err := jid.Parse(m.From)
	if err != nil {
		return nil
	}
	to, _ := jid.Parse(m.To)
	peer := from
	if sent || from.Bare().Equal(c.address) {
		sent = true
		peer = to
	}
	bare := peer.Bare().String()
	if bare == "" {
		return nil
	}
	c.lock.Lock()
	nick, room := c.rooms[bare]
	c.lock.Unlock()

	var conversation, sender, name, archive string
	own := sent
	if m.Type == "groupchat" {
		if !room {
			return nil
		}
		conversation = "xmpp:" + bare
		sender = "xmpp:" + from.String()
		name = from.Resourcepart()
		own = from.Resourcepart() == nick
		archive = m.archived(bare)
		if own {
			c.lock.Lock()
			_, echo := c.sent[m.ID]
			c.lock.Unlock()
			if echo {
				// The room's copy of what this session sent: only the id the
				// room gave it is news.
				if archive == "" {
					return nil
				}
				return []map[string]any{{"kind": "sent", "conversation": conversation, "id": m.ID, "archive": archive}}
			}
		}
	} else {
		conversation = "xmpp:" + bare
		if room {
			// A private message from someone in a room is had with them
			// as they are known there.
			conversation = "xmpp:" + peer.String()
		}
		sender = conversation
		if own {
			sender = "xmpp:" + c.address.String()
		}
		name = peer.Localpart()
		if room {
			name = peer.Resourcepart()
		}
		archive = m.archived(c.address.String())
	}
	if own {
		name = ""
	}
	// An encrypted message is read by the account's device: from the person
	// a room shows the sender to be, from the account itself where it sent
	// it from another client.
	if m.encrypted() {
		author, room_address := peer.Bare(), ""
		switch {
		case own:
			author = c.address
		case m.Type == "groupchat":
			room_address = bare
			author, _ = omemo_sender(c.l.id, bare, from.Resourcepart())
		}
		if m.Type != "groupchat" || author.String() != "" {
			omemo_read(c.l.user, c.l.id, m, author, room_address)
		}
	}
	events := xmpp_events(m, delay, from, conversation, sender, name, archive, own)
	if len(events) == 0 || events[len(events)-1]["kind"] != "message" {
		return events
	}
	event := events[len(events)-1]
	if m.Markable != nil && !own && m.Type != "groupchat" {
		c.lock.Lock()
		c.markable[conversation] = event["id"].(string)
		c.lock.Unlock()
	}
	if archive != "" && m.Type != "groupchat" && delay == nil {
		c.l.store("archive", archive)
	}
	if m.Type == "groupchat" {
		c.seen(bare, event["created"].(int64))
	}
	return events
}

// xmpp_events turns a message into the conversation events it carries, once
// its conversation, its sender and the name they go by are known, whether it
// was the account's own and the id an archive gave it: receipts, typing, a
// reaction, a withdrawal, a correction, an invitation to a room, or the
// message itself, which comes last.
func xmpp_events(m *xmpp_message, delay *xmpp_delay, from jid.JID, conversation string, sender string, name string, archive string, own bool) []map[string]any {
	created := now()
	if delay != nil {
		if stamp, err := time.Parse(time.RFC3339, delay.Stamp); err == nil {
			created = stamp.Unix()
		}
	}
	id := m.ID
	if m.Origin != nil && m.Origin.ID != "" {
		id = m.Origin.ID
	}
	if id == "" {
		id = archive
	}
	var events []map[string]any
	if !own {
		for _, receipt := range []struct {
			ref       *xmpp_reference
			displayed bool
		}{{m.Received, false}, {m.Marked, false}, {m.Displayed, true}, {m.Acknowledged, true}} {
			if receipt.ref != nil && receipt.ref.ID != "" {
				events = append(events, map[string]any{"kind": "receipt", "conversation": conversation, "from": sender, "id": receipt.ref.ID, "displayed": receipt.displayed})
			}
		}
		if delay == nil {
			switch {
			case m.Composing != nil:
				events = append(events, map[string]any{"kind": "typing", "conversation": conversation, "from": sender, "typing": true})
			case m.Paused != nil || m.Active != nil || m.Inactive != nil || m.Gone != nil:
				events = append(events, map[string]any{"kind": "typing", "conversation": conversation, "from": sender, "typing": false})
			}
		}
	}
	retracted := ""
	if m.Retract != nil {
		retracted = m.Retract.ID
	} else if m.Fasten != nil && m.Fasten.Retract != nil {
		retracted = m.Fasten.ID
	}
	switch {
	case m.Reactions != nil && m.Reactions.ID != "":
		// A person's reactions to a message come whole (XEP-0444): the first
		// is the one kept, none withdrawing theirs.
		reaction := ""
		for _, r := range m.Reactions.Items {
			if r = strings.TrimSpace(r); r != "" {
				reaction = r
				break
			}
		}
		return append(events, map[string]any{"kind": "reaction", "conversation": conversation, "from": sender, "id": m.Reactions.ID, "reaction": reaction, "event": xmpp_reaction_prefix + m.Reactions.ID, "own": own})
	case retracted != "":
		return append(events, map[string]any{"kind": "delete", "conversation": conversation, "from": sender, "id": retracted, "own": own})
	case m.transport:
		// Only keys, completing a session of the account's device.
		return events
	case m.Replace != nil && m.Replace.ID != "":
		if m.encrypted() && m.opened == nil {
			return events
		}
		return append(events, map[string]any{"kind": "edit", "conversation": conversation, "from": sender, "id": m.Replace.ID, "body": m.text(), "own": own})
	case m.Invitation != nil && m.Invitation.Invite != nil && !own:
		inviter, _ := jid.Parse(m.Invitation.Invite.From)
		return append(events, map[string]any{"kind": "invite", "conversation": "xmpp:" + from.Bare().String(), "from": "xmpp:" + inviter.Bare().String(), "name": from.Localpart()})
	case m.Conference != nil && m.Conference.JID != "" && !own:
		target, err := jid.Parse(m.Conference.JID)
		if err != nil {
			return events
		}
		return append(events, map[string]any{"kind": "invite", "conversation": "xmpp:" + target.Bare().String(), "from": sender, "name": target.Localpart()})
	}
	body := m.text()
	var files []any
	for _, link := range m.Links {
		address := strings.TrimSpace(link.URL)
		if !strings.HasPrefix(address, "https://") && !strings.HasPrefix(address, "http://") {
			continue
		}
		files = append(files, map[string]any{"url": address, "name": xmpp_file_name(address)})
		if body == address {
			body = ""
		}
	}
	// An encrypted message's body is what the account's device read of it,
	// none where it could not.
	encrypted := m.encrypted()
	if encrypted && m.opened == nil {
		body = ""
	}
	if body == "" && len(files) == 0 && !encrypted {
		return events
	}
	event := map[string]any{"kind": "message", "conversation": conversation, "room": m.Type == "groupchat", "from": sender, "name": name, "id": id, "archive": archive, "body": body, "created": created, "own": own, "encrypted": encrypted}
	if len(files) > 0 {
		event["files"] = files
	}
	if m.Reply != nil && m.Reply.ID != "" {
		event["reply"] = m.Reply.ID
	}
	return append(events, event)
}

// xmpp_file_name is the name a shared file's address ends in.
func xmpp_file_name(address string) string {
	name := address
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	if i := strings.IndexAny(name, "?#"); i >= 0 {
		name = name[:i]
	}
	if decoded, err := url.PathUnescape(name); err == nil {
		name = decoded
	}
	if name == "" || !utf8.ValidString(name) {
		return "file"
	}
	return name
}

// presence reads a presence: someone's own, or a room's word on who is in it
// and whether a join worked.
func (c *xmpp_client) presence(p *xmpp_presence) {
	from, err := jid.Parse(p.From)
	if err != nil || from.Bare().Equal(c.address) {
		return
	}
	bare := from.Bare().String()
	c.lock.Lock()
	nick, room := c.rooms[bare]
	waiting := c.joins[bare]
	c.lock.Unlock()
	if room || waiting != nil {
		self := false
		if p.User != nil {
			for _, s := range p.User.Statuses {
				if s.Code == "110" {
					self = true
				}
			}
		}
		if from.Resourcepart() == nick {
			self = true
		}
		switch {
		case p.Type == "error" && waiting != nil:
			c.joined(bare, fmt.Errorf("the room refused the join"))
		case self && waiting != nil:
			c.joined(bare, nil)
		case !self && from.Resourcepart() != "" && p.Type != "error":
			event := map[string]any{"kind": "member", "conversation": "xmpp:" + bare, "id": "xmpp:" + from.String(), "name": from.Resourcepart(), "left": p.Type == "unavailable"}
			address := ""
			if item := p.item(); item != nil {
				event["role"], event["affiliation"] = item.Role, item.Affiliation
				address = item.JID
			}
			// Who is in the room, by the address it shows, which OMEMO
			// encrypts to.
			omemo_occupant(c.l.id, bare, from.Resourcepart(), address, p.Type == "unavailable")
			c.emit([]map[string]any{event})
		}
		return
	}
	if p.Type != "" && p.Type != "unavailable" {
		return
	}
	status := "online"
	switch {
	case p.Type == "unavailable":
		status = "offline"
	case p.Show == "away" || p.Show == "xa":
		status = "away"
	case p.Show == "dnd":
		status = "busy"
	}
	c.emit([]map[string]any{{"kind": "presence", "from": "xmpp:" + bare, "status": status, "message": p.Status}})
}

// joined settles a join waiting for the room's answer.
func (c *xmpp_client) joined(room string, err error) {
	c.lock.Lock()
	waiting := c.joins[room]
	delete(c.joins, room)
	c.lock.Unlock()
	if waiting != nil {
		waiting <- err
	}
}

// === Presence ===

// xmpp_show is how an XMPP account shows a status of Mochi's. Offline is
// extended away, since a session that said it was unavailable would be sent
// nothing.
func xmpp_show(status string) string {
	switch status {
	case "away":
		return "away"
	case "busy":
		return "dnd"
	case "offline":
		return "xa"
	}
	return ""
}

// announce sends the user's presence as the account shows it, to the server
// and to every room joined: at the start, and after whenever what it shows
// changes.
func (c *xmpp_client) announce(ctx context.Context, start bool) error {
	status, message := linked_presence(c.l)
	c.lock.Lock()
	unchanged := c.shown == status+"\n"+message
	c.shown = status + "\n" + message
	c.lock.Unlock()
	if unchanged && !start {
		return nil
	}
	inner := ""
	if show := xmpp_show(status); show != "" {
		inner += `<show>` + show + `</show>`
	}
	if message != "" {
		inner += `<status>` + xmpp_escape(message) + `</status>`
	}
	inner += `<c xmlns="http://jabber.org/protocol/caps" hash="sha-1" node="` + xmpp_node + `" ver="` + xmpp_caps() + `"/>`
	if err := c.send(ctx, `<presence xmlns="jabber:client">`+inner+`</presence>`); err != nil {
		return err
	}
	c.lock.Lock()
	rooms := maps_copy(c.rooms)
	c.lock.Unlock()
	for room, nick := range rooms {
		c.send(ctx, `<presence xmlns="jabber:client" to="`+xmpp_escape(room+"/"+nick)+`">`+inner+`</presence>`)
	}
	return nil
}

// maps_copy copies a map of strings.
func maps_copy(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// === The archive ===

// archive reads one page of the account's archive: after a position, or the
// latest page. It answers the events, the position of the last result and
// whether the archive has no more.
func (c *xmpp_client) archive(ctx context.Context, after string, maximum int) ([]map[string]any, string, bool, error) {
	id := uid()
	c.lock.Lock()
	q := &xmpp_query{}
	c.queries[id] = q
	c.lock.Unlock()
	defer func() {
		c.lock.Lock()
		delete(c.queries, id)
		c.lock.Unlock()
	}()
	page := `<before/>`
	if after != "" {
		page = `<after>` + xmpp_escape(after) + `</after>`
	}
	var answer struct {
		xmpp_head
		Fin struct {
			Complete string `xml:"complete,attr"`
			Set      struct {
				Last string `xml:"last"`
			} `xml:"http://jabber.org/protocol/rsm set"`
		} `xml:"urn:xmpp:mam:2 fin"`
	}
	request := `<iq xmlns="jabber:client" type="set"><query xmlns="urn:xmpp:mam:2" queryid="` + id + `"><x xmlns="jabber:x:data" type="submit"><field var="FORM_TYPE" type="hidden"><value>urn:xmpp:mam:2</value></field></x><set xmlns="http://jabber.org/protocol/rsm"><max>` + strconv.Itoa(maximum) + `</max>` + page + `</set></query></iq>`
	if err := c.query(ctx, request, &answer); err != nil {
		return nil, "", false, err
	}
	c.lock.Lock()
	events := q.events
	c.lock.Unlock()
	return events, answer.Fin.Set.Last, answer.Fin.Complete == "true", nil
}

// catchup reads from the account's archive what arrived since the session
// last ran, page by page; on the first run, the latest messages, so that the
// conversations the account already has appear.
func (c *xmpp_client) catchup(ctx context.Context) {
	position := c.l.get("archive")
	if position == "" {
		events, last, _, err := c.archive(ctx, "", xmpp_archive_first)
		if err != nil {
			debug("XMPP account %s: archive not read: %v", c.l.id, err)
			return
		}
		c.emit(events)
		if last != "" {
			c.l.store("archive", last)
		}
		return
	}
	for range xmpp_archive_pages {
		events, last, complete, err := c.archive(ctx, position, xmpp_archive_page)
		if err != nil {
			debug("XMPP account %s: archive not read: %v", c.l.id, err)
			return
		}
		c.emit(events)
		if last == "" || last == position {
			return
		}
		position = last
		c.l.store("archive", position)
		if complete {
			return
		}
	}
}

// === Rooms ===

// An xmpp_bookmark is a room the account keeps (XEP-0402).
type xmpp_bookmark struct {
	Room     string
	Name     string
	Nick     string
	Autojoin bool
}

// bookmarks reads the rooms the account keeps.
func (c *xmpp_client) bookmarks(ctx context.Context) []xmpp_bookmark {
	var answer struct {
		xmpp_head
		Items []struct {
			ID         string `xml:"id,attr"`
			Conference struct {
				Name     string `xml:"name,attr"`
				Autojoin string `xml:"autojoin,attr"`
				Nick     string `xml:"nick"`
			} `xml:"urn:xmpp:bookmarks:1 conference"`
		} `xml:"http://jabber.org/protocol/pubsub pubsub>items>item"`
	}
	if err := c.query(ctx, `<iq xmlns="jabber:client" type="get"><pubsub xmlns="http://jabber.org/protocol/pubsub"><items node="urn:xmpp:bookmarks:1"/></pubsub></iq>`, &answer); err != nil {
		return nil
	}
	var out []xmpp_bookmark
	for _, item := range answer.Items {
		room, err := jid.Parse(item.ID)
		if err != nil || room.Localpart() == "" {
			continue
		}
		out = append(out, xmpp_bookmark{Room: room.Bare().String(), Name: item.Conference.Name, Nick: item.Conference.Nick, Autojoin: item.Conference.Autojoin == "true" || item.Conference.Autojoin == "1"})
	}
	return out
}

// bookmark keeps a room for the account's clients to join.
func (c *xmpp_client) bookmark(ctx context.Context, room string, name string, nick string) {
	options := `<publish-options><x xmlns="jabber:x:data" type="submit"><field var="FORM_TYPE" type="hidden"><value>http://jabber.org/protocol/pubsub#publish-options</value></field><field var="pubsub#persist_items"><value>true</value></field><field var="pubsub#max_items"><value>max</value></field><field var="pubsub#send_last_published_item"><value>never</value></field><field var="pubsub#access_model"><value>whitelist</value></field></x></publish-options>`
	item := `<item id="` + xmpp_escape(room) + `"><conference xmlns="urn:xmpp:bookmarks:1" name="` + xmpp_escape(name) + `" autojoin="true"><nick>` + xmpp_escape(nick) + `</nick></conference></item>`
	if err := c.query(ctx, `<iq xmlns="jabber:client" type="set"><pubsub xmlns="http://jabber.org/protocol/pubsub"><publish node="urn:xmpp:bookmarks:1">`+item+`</publish>`+options+`</pubsub></iq>`, &xmpp_head{}); err != nil {
		debug("XMPP account %s: room %s not bookmarked: %v", c.l.id, room, err)
	}
}

// unbookmark stops keeping a room.
func (c *xmpp_client) unbookmark(ctx context.Context, room string) {
	c.query(ctx, `<iq xmlns="jabber:client" type="set"><pubsub xmlns="http://jabber.org/protocol/pubsub"><retract node="urn:xmpp:bookmarks:1" notify="true"><item id="`+xmpp_escape(room)+`"/></retract></pubsub></iq>`, &xmpp_head{})
}

// xmpp_rooms reads the rooms an account's data keeps, a copy to change: each
// room's nick, its name and when its last message came.
func xmpp_rooms(l *linked) map[string]map[string]any {
	out := map[string]map[string]any{}
	json.Unmarshal([]byte(json_encode(l.value("rooms"))), &out)
	if out == nil {
		out = map[string]map[string]any{}
	}
	return out
}

// seen keeps when a room's latest message came, from which the next join
// asks for history.
func (c *xmpp_client) seen(room string, created int64) {
	rooms := xmpp_rooms(c.l)
	entry := rooms[room]
	if entry == nil {
		return
	}
	if as_int64(entry["seen"]) >= created {
		return
	}
	entry["seen"] = created
	c.l.store("rooms", rooms)
}

// rejoin joins every room the account keeps, its bookmarks marked to join and
// the rooms joined from here, and tells the app of each.
func (c *xmpp_client) rejoin(ctx context.Context) {
	kept := xmpp_rooms(c.l)
	names := map[string]string{}
	nicks := map[string]string{}
	for _, b := range c.bookmarks(ctx) {
		if b.Autojoin {
			names[b.Room], nicks[b.Room] = b.Name, b.Nick
		}
	}
	for room, entry := range kept {
		if _, found := nicks[room]; !found {
			nick, _ := entry["nick"].(string)
			name, _ := entry["name"].(string)
			names[room], nicks[room] = name, nick
		}
	}
	for room, nick := range nicks {
		if nick == "" {
			nick = c.address.Localpart()
		}
		var since int64
		if entry := kept[room]; entry != nil {
			since = as_int64(entry["seen"])
		}
		if err := c.join(ctx, room, nick, since); err != nil {
			debug("XMPP account %s: room %s not joined: %v", c.l.id, room, err)
			continue
		}
		name := names[room]
		if name == "" {
			name = c.room_name(ctx, room)
		}
		c.keep(room, nick, name)
		c.emit([]map[string]any{{"kind": "room", "conversation": "xmpp:" + room, "room": true, "name": name}})
	}
}

// keep records a room joined in the account's data.
func (c *xmpp_client) keep(room string, nick string, name string) {
	rooms := xmpp_rooms(c.l)
	entry := rooms[room]
	if entry == nil {
		entry = map[string]any{}
		rooms[room] = entry
	}
	entry["nick"] = nick
	if name != "" {
		entry["name"] = name
	}
	c.l.store("rooms", rooms)
}

// join enters a room under a nick and waits for the room to confirm, asking
// for the history since a moment, or for the latest messages the first time.
func (c *xmpp_client) join(ctx context.Context, room string, nick string, since int64) error {
	history := `<history maxstanzas="20"/>`
	if since > 0 {
		history = `<history since="` + time.Unix(since, 0).UTC().Format(time.RFC3339) + `"/>`
	}
	waiting := make(chan error, 1)
	c.lock.Lock()
	c.joins[room] = waiting
	c.rooms[room] = nick
	c.lock.Unlock()
	status, message := linked_presence(c.l)
	inner := ""
	if show := xmpp_show(status); show != "" {
		inner += `<show>` + show + `</show>`
	}
	if message != "" {
		inner += `<status>` + xmpp_escape(message) + `</status>`
	}
	if err := c.send(ctx, `<presence xmlns="jabber:client" to="`+xmpp_escape(room+"/"+nick)+`"><x xmlns="http://jabber.org/protocol/muc">`+history+`</x>`+inner+`</presence>`); err != nil {
		c.forget(room)
		return err
	}
	select {
	case err := <-waiting:
		if err != nil {
			c.forget(room)
		}
		return err
	case <-time.After(xmpp_query_timeout):
		c.forget(room)
		return fmt.Errorf("the room did not answer")
	case <-ctx.Done():
		c.forget(room)
		return ctx.Err()
	}
}

// forget drops a room from the session.
func (c *xmpp_client) forget(room string) {
	c.lock.Lock()
	delete(c.rooms, room)
	delete(c.joins, room)
	c.lock.Unlock()
}

// disco asks an address what it is: its identities and features.
func (c *xmpp_client) disco(ctx context.Context, to string) ([]struct{ Category, Type, Name string }, []string, error) {
	var answer struct {
		xmpp_head
		Identities []struct {
			Category string `xml:"category,attr"`
			Type     string `xml:"type,attr"`
			Name     string `xml:"name,attr"`
		} `xml:"http://jabber.org/protocol/disco#info query>identity"`
		Features []struct {
			Var string `xml:"var,attr"`
		} `xml:"http://jabber.org/protocol/disco#info query>feature"`
	}
	if err := c.query(ctx, `<iq xmlns="jabber:client" type="get" to="`+xmpp_escape(to)+`"><query xmlns="http://jabber.org/protocol/disco#info"/></iq>`, &answer); err != nil {
		return nil, nil, err
	}
	var identities []struct{ Category, Type, Name string }
	for _, i := range answer.Identities {
		identities = append(identities, struct{ Category, Type, Name string }{i.Category, i.Type, i.Name})
	}
	var features []string
	for _, f := range answer.Features {
		features = append(features, f.Var)
	}
	return identities, features, nil
}

// room_name is a room's name as it gives it, else its local part.
func (c *xmpp_client) room_name(ctx context.Context, room string) string {
	identities, _, err := c.disco(ctx, room)
	if err == nil {
		for _, i := range identities {
			if i.Category == "conference" && i.Name != "" {
				return i.Name
			}
		}
	}
	address, _ := jid.Parse(room)
	return address.Localpart()
}

// service finds the account's server's service of a kind: a room service, or
// the file upload service, by asking each of the server's items.
func (c *xmpp_client) service(ctx context.Context, kind string) (string, error) {
	c.lock.Lock()
	found := c.services[kind]
	c.lock.Unlock()
	if found != "" {
		return found, nil
	}
	var items struct {
		xmpp_head
		Items []struct {
			JID string `xml:"jid,attr"`
		} `xml:"http://jabber.org/protocol/disco#items query>item"`
	}
	domain := c.address.Domainpart()
	if err := c.query(ctx, `<iq xmlns="jabber:client" type="get" to="`+xmpp_escape(domain)+`"><query xmlns="http://jabber.org/protocol/disco#items"/></iq>`, &items); err != nil {
		return "", err
	}
	candidates := []string{domain}
	for _, item := range items.Items {
		candidates = append(candidates, item.JID)
	}
	for _, candidate := range candidates {
		identities, features, err := c.disco(ctx, candidate)
		if err != nil {
			continue
		}
		match := false
		switch kind {
		case "rooms":
			for _, i := range identities {
				match = match || (i.Category == "conference" && i.Type == "text")
			}
		case "upload":
			match = slices.Contains(features, "urn:xmpp:http:upload:0")
		}
		if match {
			c.lock.Lock()
			c.services[kind] = candidate
			c.lock.Unlock()
			return candidate, nil
		}
	}
	return "", fmt.Errorf("the server has no %s service", kind)
}

// room_join joins a room by its address and keeps it, answering its name. An
// address that is not a room is refused.
func (c *xmpp_client) room_join(ctx context.Context, address string) (string, string, error) {
	room, err := xmpp_address(address)
	if err != nil {
		return "", "", err
	}
	identities, _, err := c.disco(ctx, room.String())
	if err != nil {
		return "", "", fmt.Errorf("the room could not be reached: %v", err)
	}
	name := ""
	conference := false
	for _, i := range identities {
		if i.Category == "conference" {
			conference = true
			name = i.Name
		}
	}
	if !conference {
		return "", "", fmt.Errorf("not a room")
	}
	if name == "" {
		name = room.Localpart()
	}
	nick := c.address.Localpart()
	if err := c.join(ctx, room.String(), nick, 0); err != nil {
		return "", "", err
	}
	c.keep(room.String(), nick, name)
	c.bookmark(ctx, room.String(), name, nick)
	return room.String(), name, nil
}

// room_create makes a room on the account's server for the people given:
// persistent, members only, with each person's address shown to the others,
// each made a member and invited.
func (c *xmpp_client) room_create(ctx context.Context, people []string, name string) (string, error) {
	service, err := c.service(ctx, "rooms")
	if err != nil {
		return "", err
	}
	room := strings.ToLower(uid()) + "@" + service
	nick := c.address.Localpart()
	if err := c.join(ctx, room, nick, 0); err != nil {
		return "", err
	}
	form := `<x xmlns="jabber:x:data" type="submit"><field var="FORM_TYPE"><value>http://jabber.org/protocol/muc#roomconfig</value></field><field var="muc#roomconfig_roomname"><value>` + xmpp_escape(name) + `</value></field><field var="muc#roomconfig_persistentroom"><value>1</value></field><field var="muc#roomconfig_membersonly"><value>1</value></field><field var="muc#roomconfig_publicroom"><value>0</value></field><field var="muc#roomconfig_whois"><value>anyone</value></field></x>`
	if err := c.query(ctx, `<iq xmlns="jabber:client" type="set" to="`+xmpp_escape(room)+`"><query xmlns="http://jabber.org/protocol/muc#owner">`+form+`</query></iq>`, &xmpp_head{}); err != nil {
		c.room_leave(ctx, room)
		return "", fmt.Errorf("the room could not be set up: %v", err)
	}
	var items strings.Builder
	for _, person := range people {
		items.WriteString(`<item affiliation="member" jid="` + xmpp_escape(person) + `"/>`)
	}
	if err := c.query(ctx, `<iq xmlns="jabber:client" type="set" to="`+xmpp_escape(room)+`"><query xmlns="http://jabber.org/protocol/muc#admin">`+items.String()+`</query></iq>`, &xmpp_head{}); err != nil {
		debug("XMPP account %s: members of %s not set: %v", c.l.id, room, err)
	}
	for _, person := range people {
		c.send(ctx, `<message xmlns="jabber:client" to="`+xmpp_escape(person)+`" id="`+xmpp_escape(uid())+`"><x xmlns="jabber:x:conference" jid="`+xmpp_escape(room)+`"/></message>`)
	}
	c.keep(room, nick, name)
	c.bookmark(ctx, room, name, nick)
	return room, nil
}

// room_leave leaves a room and stops keeping it.
func (c *xmpp_client) room_leave(ctx context.Context, room string) {
	c.lock.Lock()
	nick := c.rooms[room]
	c.lock.Unlock()
	if nick != "" {
		c.send(ctx, `<presence xmlns="jabber:client" type="unavailable" to="`+xmpp_escape(room+"/"+nick)+`"/>`)
	}
	c.forget(room)
	rooms := xmpp_rooms(c.l)
	if _, kept := rooms[room]; kept {
		delete(rooms, room)
		c.l.store("rooms", rooms)
	}
	c.unbookmark(ctx, room)
}

// === Sending ===

// xmpp_target reads a conversation's address: a person or a room, with the
// prefix chat keeps it under.
func xmpp_target(conversation string) (jid.JID, error) {
	address, err := jid.Parse(strings.TrimPrefix(conversation, "xmpp:"))
	if err != nil || address.Domainpart() == "" {
		return jid.JID{}, fmt.Errorf("invalid conversation")
	}
	return address, nil
}

// kind is the message type a conversation takes: a room's or a person's.
func (c *xmpp_client) kind(address jid.JID) string {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, room := c.rooms[address.String()]; room && address.Resourcepart() == "" {
		return "groupchat"
	}
	return "chat"
}

// message_send sends a message of a conversation: its text, or a file by its
// address, a reply to another, or a correction of an earlier one. The id is
// the app's, so the room's echo is known.
func (c *xmpp_client) message_send(ctx context.Context, conversation string, id string, body string, file string, reply string, correct string) error {
	to, err := xmpp_target(conversation)
	if err != nil {
		return err
	}
	kind := c.kind(to)
	// A conversation whose people have OMEMO devices is encrypted.
	sealed, err := omemo_seal_message(ctx, c.l.user, c.l.id, to, kind == "groupchat", body, file)
	if err != nil {
		return err
	}
	text := xmpp_message_text(to.String(), kind, id, body, file, reply, correct, sealed)
	c.lock.Lock()
	if timer := c.typing[conversation]; timer != nil {
		timer.Stop()
		delete(c.typing, conversation)
	}
	if kind == "groupchat" {
		c.sent[id] = now()
		for sent, at := range c.sent {
			if at < now()-86400 {
				delete(c.sent, sent)
			}
		}
	}
	c.lock.Unlock()
	return c.send(ctx, text)
}

// xmpp_message_text writes a message of a conversation: its text, or a file by
// its address, a reply to another, or a correction of an earlier one, asking
// to be marked as read and, between two people, for a receipt.
func xmpp_message_text(to string, kind string, id string, body string, file string, reply string, correct string, sealed string) string {
	var b strings.Builder
	b.WriteString(`<message xmlns="jabber:client" to="` + xmpp_escape(to) + `" type="` + kind + `" id="` + xmpp_escape(id) + `">`)
	if file != "" {
		body = file
	}
	switch {
	case sealed != "":
		// Encrypted by the account's OMEMO device, with the text clients
		// that cannot read it show.
		b.WriteString(`<body>` + xmpp_escape(omemo_fallback) + `</body>` + sealed + `<encryption xmlns="` + omemo_ns_eme + `" namespace="` + omemo_legacy + `" name="OMEMO"/><store xmlns="` + omemo_ns_hints + `"/>`)
	case file != "":
		b.WriteString(`<body>` + xmpp_escape(body) + `</body><x xmlns="jabber:x:oob"><url>` + xmpp_escape(file) + `</url></x>`)
	default:
		b.WriteString(`<body>` + xmpp_escape(body) + `</body>`)
	}
	if correct != "" {
		b.WriteString(`<replace xmlns="urn:xmpp:message-correct:0" id="` + xmpp_escape(correct) + `"/>`)
	} else {
		b.WriteString(`<markable xmlns="urn:xmpp:chat-markers:0"/>`)
		if kind == "chat" {
			b.WriteString(`<request xmlns="urn:xmpp:receipts"/>`)
		}
	}
	if reply != "" {
		b.WriteString(`<reply xmlns="urn:xmpp:reply:0" id="` + xmpp_escape(reply) + `"/>`)
	}
	b.WriteString(`<active xmlns="http://jabber.org/protocol/chatstates"/><origin-id xmlns="urn:xmpp:sid:0" id="` + xmpp_escape(id) + `"/></message>`)
	return b.String()
}

// retract withdraws a message the account sent, by the id it has in the
// conversation: the room's for a room.
func (c *xmpp_client) retract(ctx context.Context, conversation string, id string) error {
	if message, found := strings.CutPrefix(id, xmpp_reaction_prefix); found {
		return c.react(ctx, conversation, message, "")
	}
	to, err := xmpp_target(conversation)
	if err != nil {
		return err
	}
	return c.send(ctx, `<message xmlns="jabber:client" to="`+xmpp_escape(to.String())+`" type="`+c.kind(to)+`" id="`+xmpp_escape(uid())+`"><retract xmlns="urn:xmpp:message-retract:1" id="`+xmpp_escape(id)+`"/><store xmlns="urn:xmpp:hints"/></message>`)
}

// react sends the user's reaction to a message, replacing the one before; an
// empty one withdraws it (XEP-0444). In a room the message is named by the id
// the room gave it.
func (c *xmpp_client) react(ctx context.Context, conversation string, message string, emoji string) error {
	to, err := xmpp_target(conversation)
	if err != nil {
		return err
	}
	return c.send(ctx, xmpp_make("message", xmpp_reactions(message, emoji), "to", to.String(), "type", c.kind(to), "id", uid()).text(xmpp_ns_client))
}

// type_state tells a conversation the user is typing, and that they stopped
// once xmpp_pause passes with no more.
func (c *xmpp_client) type_state(ctx context.Context, conversation string, typing bool) error {
	to, err := xmpp_target(conversation)
	if err != nil {
		return err
	}
	kind := c.kind(to)
	state := "paused"
	if typing {
		state = "composing"
	}
	c.lock.Lock()
	if timer := c.typing[conversation]; timer != nil {
		timer.Stop()
		delete(c.typing, conversation)
	}
	if typing {
		c.typing[conversation] = time.AfterFunc(xmpp_pause, func() {
			c.lock.Lock()
			delete(c.typing, conversation)
			c.lock.Unlock()
			c.send(context.Background(), `<message xmlns="jabber:client" to="`+xmpp_escape(to.String())+`" type="`+kind+`"><paused xmlns="http://jabber.org/protocol/chatstates"/><no-store xmlns="urn:xmpp:hints"/></message>`)
		})
	}
	c.lock.Unlock()
	return c.send(ctx, `<message xmlns="jabber:client" to="`+xmpp_escape(to.String())+`" type="`+kind+`"><`+state+` xmlns="http://jabber.org/protocol/chatstates"/><no-store xmlns="urn:xmpp:hints"/></message>`)
}

// display tells a person the user has read their messages up to one: only
// the one they marked as wanting it, as the markers ask, and not in a room.
func (c *xmpp_client) display(ctx context.Context, conversation string, id string) error {
	to, err := xmpp_target(conversation)
	if err != nil {
		return err
	}
	if c.kind(to) == "groupchat" {
		return nil
	}
	c.lock.Lock()
	marked := c.markable[conversation]
	c.lock.Unlock()
	if marked == "" || marked != id {
		return nil
	}
	return c.send(ctx, `<message xmlns="jabber:client" to="`+xmpp_escape(to.String())+`" type="chat" id="`+xmpp_escape(uid())+`"><displayed xmlns="urn:xmpp:chat-markers:0" id="`+xmpp_escape(id)+`"/><store xmlns="urn:xmpp:hints"/></message>`)
}

// upload puts a file on the server's upload service and answers where it can
// be fetched, for a message to name.
func (c *xmpp_client) upload(ctx context.Context, content []byte, kind string, name string) (map[string]any, error) {
	service, err := c.service(ctx, "upload")
	if err != nil {
		return nil, err
	}
	if kind == "" {
		kind = "application/octet-stream"
	}
	var slot struct {
		xmpp_head
		Put struct {
			URL     string `xml:"url,attr"`
			Headers []struct {
				Name  string `xml:"name,attr"`
				Value string `xml:",chardata"`
			} `xml:"header"`
		} `xml:"urn:xmpp:http:upload:0 slot>put"`
		Get struct {
			URL string `xml:"url,attr"`
		} `xml:"urn:xmpp:http:upload:0 slot>get"`
	}
	request := `<iq xmlns="jabber:client" type="get" to="` + xmpp_escape(service) + `"><request xmlns="urn:xmpp:http:upload:0" filename="` + xmpp_escape(name) + `" size="` + strconv.Itoa(len(content)) + `" content-type="` + xmpp_escape(kind) + `"/></iq>`
	if err := c.query(ctx, request, &slot); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(slot.Put.URL, "https://") || !strings.HasPrefix(slot.Get.URL, "https://") {
		return nil, fmt.Errorf("the upload service gave no secure address")
	}
	put, err := http.NewRequestWithContext(ctx, "PUT", slot.Put.URL, bytes.NewReader(content))
	if err != nil {
		return nil, err
	}
	put.Header.Set("Content-Type", kind)
	for _, h := range slot.Put.Headers {
		// Only these may be set by the slot (XEP-0363).
		switch strings.ToLower(h.Name) {
		case "authorization", "cookie", "expires":
			put.Header.Set(h.Name, strings.TrimSpace(h.Value))
		}
	}
	response, err := network_send(ctx, c.l.user, "", strings.ToLower(put.URL.Hostname())+"|"+c.l.id, put, 0)
	if err != nil {
		return nil, err
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, fmt.Errorf("the upload service answered %d", response.StatusCode)
	}
	return map[string]any{"url": slot.Get.URL, "name": name, "type": kind, "size": len(content)}, nil
}
