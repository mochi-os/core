// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP sessions across connections
//
// What keeps a client's session whole when its connection is not (the
// federation plan, phase 26). A client bound to a resource is written to by a
// goroutine of its own, so no one waits on a slow client. A client that
// manages its stream (XEP-0198) acknowledges what it has handled, and what it
// has not is kept; where it asked to resume, a connection that breaks leaves
// the resource waiting five minutes for the client to take it up on a new
// stream, with what it missed written again. Its presence stands meanwhile,
// and what comes for it waits. A resource that ends with messages unwritten
// keeps them for the account's next client where no other is online.
//
// A client that says it is inactive (XEP-0352) is spared presence and typing
// until it is active again, and given then the latest presence of each
// contact. A client that registers with a push service (XEP-0357) has its app
// woken, with no content, when a message comes and none of the account's
// clients has a connection to take it.

package main

import (
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mellium.im/xmpp/jid"
)

const (
	xmpp_ns_management = "urn:xmpp:sm:3"
	xmpp_ns_state      = "urn:xmpp:csi:0"
	xmpp_ns_push       = "urn:xmpp:push:0"
)

// How long a broken connection's resource waits to be resumed.
var xmpp_resume_timeout = 5 * time.Minute

// The most stanzas waiting for a client, written or not, before it is taken
// to have fallen behind; how many unacknowledged stanzas prompt the server to
// ask for an acknowledgement; and the most presence an inactive client is
// spared before the oldest goes.
const (
	xmpp_queue_maximum = 1000
	xmpp_ask_after     = 5
	xmpp_held_maximum  = 500
)

// An xmpp_resource is a client signed in to an account and bound to a
// resource: what it last said of its presence and to whom it sent presence of
// its own; what waits to be written to it; whether it takes copies of the
// account's other messages (carbons); and, where it manages its stream, the
// counts both ways, what it has not acknowledged and the token it resumes
// with. Its stream is nil while it waits to be resumed.
type xmpp_resource struct {
	user       *User
	account    string
	address    jid.JID
	full       jid.JID
	lock       sync.Mutex
	stream     xmpp_stream
	queue      []xmpp_queued
	wake       chan struct{}
	done       chan struct{}
	ended      bool
	once       sync.Once
	available  bool
	priority   int
	presence   string
	connection string
	directed   map[string]bool
	carbons    bool
	counting   bool
	managed    bool
	resumable  bool
	token      string
	handled    uint32
	sent       uint32
	acked      uint32
	unacked    []string
	asked      bool
	expiry     *time.Timer
	inactive   bool
	held       []xmpp_held
}

// An xmpp_queued is text waiting for a client: a stanza, which stream
// management counts, or an element of the stream itself, one of them the
// answer enabling it, from which the counting starts.
type xmpp_queued struct {
	text   string
	stanza bool
	enable bool
}

// An xmpp_held is presence an inactive client is spared, by whom it is from.
type xmpp_held struct {
	from string
	text string
}

// The bound resources, by account address and resource.
var (
	xmpp_resources      = map[string]map[string]*xmpp_resource{}
	xmpp_resources_lock sync.Mutex
)

// xmpp_resource_new is a resource of an account, not yet bound.
func xmpp_resource_new(user *User, account string, address jid.JID, full jid.JID) *xmpp_resource {
	return &xmpp_resource{user: user, account: account, address: address, full: full, wake: make(chan struct{}, 1), done: make(chan struct{}), connection: "xmpp-" + uid(), directed: map[string]bool{}}
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

// xmpp_live reports whether any of an account's clients has a connection.
func xmpp_live(a *xmpp_account) bool {
	for _, r := range xmpp_resources_of(a.address.String()) {
		r.lock.Lock()
		live := r.stream != nil && !r.ended
		r.lock.Unlock()
		if live {
			return true
		}
	}
	return false
}

// === Writing ===

// send queues a stanza for the client, ending a resource that has let too
// much wait. An inactive client is spared what it can do without.
func (r *xmpp_resource) send(text string) {
	r.lock.Lock()
	if r.ended {
		r.lock.Unlock()
		return
	}
	if r.inactive && r.spare(text) {
		r.lock.Unlock()
		return
	}
	if len(r.queue)+len(r.unacked) >= xmpp_queue_maximum {
		r.lock.Unlock()
		info("XMPP client %s fell behind; closing its stream", r.full)
		r.end("policy-violation")
		return
	}
	r.queue = append(r.queue, xmpp_queued{text: text, stanza: true})
	r.lock.Unlock()
	r.poke()
}

// control queues an element of the stream, in order with the stanzas.
func (r *xmpp_resource) control(text string, enable bool) {
	r.lock.Lock()
	if !r.ended {
		r.queue = append(r.queue, xmpp_queued{text: text, enable: enable})
	}
	r.lock.Unlock()
	r.poke()
}

// poke wakes the writer.
func (r *xmpp_resource) poke() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// writer writes what is queued for the client on one stream until the stream
// breaks or the resource ends. Where the client manages its stream, each
// stanza written is kept until it is acknowledged, and an acknowledgement is
// asked for once enough wait.
func (r *xmpp_resource) writer(stream xmpp_stream, broken chan struct{}) {
	for {
		r.lock.Lock()
		if r.stream != stream || r.ended {
			r.lock.Unlock()
			return
		}
		queue := r.queue
		r.queue = nil
		r.lock.Unlock()
		for i, q := range queue {
			if err := stream.write(q.text); err != nil {
				r.lock.Lock()
				r.queue = append(append([]xmpp_queued{}, queue[i:]...), r.queue...)
				r.lock.Unlock()
				stream.abort()
				return
			}
			r.lock.Lock()
			if q.enable {
				r.managed = true
				r.sent, r.acked, r.unacked = 0, 0, nil
			}
			ask := false
			if q.stanza && r.managed {
				r.unacked = append(r.unacked, q.text)
				r.sent++
				ask = !r.asked && len(r.unacked) >= xmpp_ask_after
				r.asked = r.asked || ask
			}
			r.lock.Unlock()
			if ask {
				stream.write(`<r xmlns="` + xmpp_ns_management + `"/>`)
			}
		}
		select {
		case <-r.wake:
		case <-broken:
			return
		case <-r.done:
			return
		}
	}
}

// === A resource's streams ===

// xmpp_client_run reads a bound client's stream until it ends: the stanzas
// it routes, and what it says of its stream's management and its state. A
// stream that breaks leaves a resumable resource waiting; one that ends
// otherwise ends the resource.
func xmpp_client_run(r *xmpp_resource, stream xmpp_stream) {
	broken := make(chan struct{})
	r.lock.Lock()
	if r.expiry != nil {
		r.expiry.Stop()
		r.expiry = nil
	}
	r.stream = stream
	r.lock.Unlock()
	go r.writer(stream, broken)
	r.poke()
	clean := false
	for {
		stream.deadline(time.Now().Add(xmpp_idle_timeout))
		s, err := stream.next()
		if err != nil {
			if err == xmpp_too_large {
				close(broken)
				r.end("policy-violation")
				return
			}
			clean = err == io.EOF
			break
		}
		switch s.space {
		case xmpp_ns_management:
			r.manage(stream, s)
		case xmpp_ns_state:
			r.state(s.name)
		case xmpp_ns_client:
			switch s.name {
			case "message", "presence", "iq":
				r.lock.Lock()
				if r.counting {
					r.handled++
				}
				r.lock.Unlock()
				xmpp_from_client(r, s)
			}
		}
	}
	close(broken)
	r.detach(stream, clean)
}

// detach is a stream's end: the resource waits to be resumed where the client
// asked for that and the stream broke, and ends otherwise. A stream a newer
// one has taken over from changes nothing.
func (r *xmpp_resource) detach(stream xmpp_stream, clean bool) {
	r.lock.Lock()
	if r.stream != stream || r.ended {
		r.lock.Unlock()
		return
	}
	if clean || !r.resumable {
		r.lock.Unlock()
		r.end("")
		return
	}
	r.stream = nil
	r.expiry = time.AfterFunc(xmpp_resume_timeout, func() { r.end("") })
	r.lock.Unlock()
	stream.abort()
	debug("XMPP client %s waits to be resumed", r.full)
}

// end ends the resource, once: its stream with an error where one is given,
// its presence, and what waited for it, kept for the account's next client.
func (r *xmpp_resource) end(condition string) {
	r.once.Do(func() {
		r.lock.Lock()
		r.ended = true
		stream := r.stream
		r.stream = nil
		if r.expiry != nil {
			r.expiry.Stop()
		}
		pending := append([]string{}, r.unacked...)
		for _, q := range r.queue {
			if q.stanza {
				pending = append(pending, q.text)
			}
		}
		r.unacked, r.queue = nil, nil
		r.lock.Unlock()
		close(r.done)
		// Forgotten before its stream is closed, which may wait on the client.
		xmpp_resource_end(r, pending)
		if stream != nil {
			if condition != "" {
				stream.fail(condition)
			} else {
				stream.close()
			}
		}
	})
}

// xmpp_resource_end forgets a client that has ended: its presence goes, to
// the account's contacts and to wherever it sent presence of its own. The
// messages it was not given are kept for the account's next client where no
// other is online, and those for it alone go to the others that did not take
// a copy.
func xmpp_resource_end(r *xmpp_resource, pending []string) {
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
	directed := make([]string, 0, len(r.directed))
	for to := range r.directed {
		directed = append(directed, to)
	}
	r.lock.Unlock()
	if available {
		xmpp_presence_gone(r)
	}
	for _, to := range directed {
		if address, err := jid.Parse(to); err == nil {
			xmpp_route(xmpp_make("presence", "", "type", "unavailable"), r.full, address)
		}
	}
	if len(pending) == 0 {
		return
	}
	a := &xmpp_account{user: r.user, id: r.account, address: r.address}
	var others []*xmpp_resource
	for _, other := range xmpp_resources_of(r.address.String()) {
		other.lock.Lock()
		online, copies := other.available, other.carbons
		other.lock.Unlock()
		if online && !copies {
			others = append(others, other)
		} else if online {
			others = append(others, nil)
		}
	}
	for _, text := range pending {
		s, err := xmpp_parse(text)
		if err != nil || s.name != "message" || !xmpp_keepable(s) {
			continue
		}
		if len(others) == 0 {
			xmpp_offline_keep(a, s)
			continue
		}
		// One for this client alone goes to the others online, but for those
		// that took a copy; one for the account they had already.
		to, err := jid.Parse(s.get("to"))
		if err != nil || to.Resourcepart() != r.full.Resourcepart() {
			continue
		}
		for _, other := range others {
			if other != nil {
				other.send(text)
			}
		}
	}
}

// xmpp_keepable reports whether a message is one kept for a client that
// missed it: one of a conversation, with content.
func xmpp_keepable(s *xmpp_stanza) bool {
	if kind := s.get("type"); kind != "" && kind != "chat" && kind != "normal" {
		return false
	}
	var m xmpp_message
	if s.decode(&m) != nil || len(m.Carbons) > 0 || len(m.Sent) > 0 || m.Result != nil {
		return false
	}
	return strings.TrimSpace(m.Body) != "" || m.encrypted()
}

// === Stream management ===

// manage takes what a client says of its stream's management: to enable it,
// asking for an acknowledgement, or acknowledging.
func (r *xmpp_resource) manage(stream xmpp_stream, s *xmpp_stanza) {
	switch s.name {
	case "enable":
		r.lock.Lock()
		if r.counting {
			r.lock.Unlock()
			stream.write(`<failed xmlns="` + xmpp_ns_management + `"><unexpected-request xmlns="` + xmpp_ns_stanzas + `"/></failed>`)
			return
		}
		r.counting, r.handled = true, 0
		text := `<enabled xmlns="` + xmpp_ns_management + `"`
		if resume := s.get("resume"); resume == "true" || resume == "1" {
			r.resumable = true
			r.token = random_alphanumeric(24)
			text += ` id="` + r.token + `" resume="true" max="` + strconv.Itoa(int(xmpp_resume_timeout/time.Second)) + `"`
		}
		r.lock.Unlock()
		r.control(text+`/>`, true)
	case "r":
		r.lock.Lock()
		counting, handled := r.counting, r.handled
		r.lock.Unlock()
		if counting {
			stream.write(`<a xmlns="` + xmpp_ns_management + `" h="` + strconv.FormatUint(uint64(handled), 10) + `"/>`)
		}
	case "a":
		h, err := strconv.ParseUint(s.get("h"), 10, 32)
		if err != nil {
			return
		}
		r.lock.Lock()
		count := int(uint32(h) - r.acked)
		if count > len(r.unacked) {
			r.lock.Unlock()
			r.end("undefined-condition")
			return
		}
		r.unacked = r.unacked[count:]
		r.acked = uint32(h)
		r.asked = false
		r.lock.Unlock()
	}
}

// xmpp_resume takes up a resource waiting to be resumed on a new stream, for
// the account that signed in on it, and answers with what the client last
// handled; what it did not acknowledge is written again. A resource that
// cannot be resumed is refused, and the client binds a new one.
func xmpp_resume(x xmpp_stream, user *User, address jid.JID, s *xmpp_stanza) *xmpp_resource {
	token := s.get("previd")
	h, err := strconv.ParseUint(s.get("h"), 10, 32)
	var found *xmpp_resource
	for _, r := range xmpp_resources_of(address.String()) {
		r.lock.Lock()
		match := token != "" && r.token == token && r.resumable && !r.ended && r.user.UID == user.UID
		r.lock.Unlock()
		if match {
			found = r
		}
	}
	if found == nil || err != nil {
		x.write(`<failed xmlns="` + xmpp_ns_management + `"><item-not-found xmlns="` + xmpp_ns_stanzas + `"/></failed>`)
		return nil
	}
	r := found
	r.lock.Lock()
	count := int(uint32(h) - r.acked)
	if count > len(r.unacked) || r.ended {
		r.lock.Unlock()
		x.write(`<failed xmlns="` + xmpp_ns_management + `"><undefined-condition xmlns="` + xmpp_ns_stanzas + `"/></failed>`)
		return nil
	}
	again := make([]xmpp_queued, 0, len(r.unacked)-count)
	for _, text := range r.unacked[count:] {
		again = append(again, xmpp_queued{text: text, stanza: true})
	}
	r.queue = append(again, r.queue...)
	r.unacked = nil
	r.acked, r.sent = uint32(h), uint32(h)
	r.asked = false
	old := r.stream
	r.stream = nil
	handled := r.handled
	r.lock.Unlock()
	if old != nil {
		old.abort()
	}
	x.write(`<resumed xmlns="` + xmpp_ns_management + `" previd="` + xmpp_escape(token) + `" h="` + strconv.FormatUint(uint64(handled), 10) + `"/>`)
	return r
}

// === Client state ===

// state takes a client saying it is active or inactive. Becoming active, it
// is given the presence it was spared.
func (r *xmpp_resource) state(name string) {
	switch name {
	case "inactive":
		r.lock.Lock()
		r.inactive = true
		r.lock.Unlock()
	case "active":
		r.lock.Lock()
		r.inactive = false
		held := r.held
		r.held = nil
		r.lock.Unlock()
		for _, h := range held {
			r.send(h.text)
		}
	}
}

// spare reports whether an inactive client can do without a stanza for now:
// presence, of which only each contact's latest is kept for later, and
// typing, which is dropped. The lock is held.
func (r *xmpp_resource) spare(text string) bool {
	if !strings.HasPrefix(text, "<presence") && !strings.HasPrefix(text, "<message") {
		return false
	}
	s, err := xmpp_parse(text)
	if err != nil {
		return false
	}
	if s.name == "presence" {
		if kind := s.get("type"); kind != "" && kind != "unavailable" {
			return false
		}
		from := s.get("from")
		for i, h := range r.held {
			if h.from == from {
				r.held = append(r.held[:i], r.held[i+1:]...)
				break
			}
		}
		r.held = append(r.held, xmpp_held{from: from, text: text})
		if len(r.held) > xmpp_held_maximum {
			r.held = r.held[1:]
		}
		return true
	}
	var m xmpp_message
	if s.decode(&m) != nil {
		return false
	}
	states := m.Composing != nil || m.Paused != nil || m.Active != nil || m.Inactive != nil || m.Gone != nil
	other := strings.TrimSpace(m.Body) != "" || m.encrypted() || m.Received != nil || m.Marked != nil || m.Displayed != nil || m.Acknowledged != nil || m.Retract != nil || m.Replace != nil || m.Reactions != nil || len(m.Carbons) > 0 || len(m.Sent) > 0 || m.Result != nil || m.Subject != nil || m.Invitation != nil || m.Conference != nil || len(m.Links) > 0
	return states && !other
}

// === Push ===

// The pushes waiting for their service's answer, by id, and when each
// registration was last woken.
var (
	xmpp_pushes      = map[string]xmpp_push{}
	xmpp_pushed      = map[string]time.Time{}
	xmpp_pushes_lock sync.Mutex
)

// The least time between two wakings of one registration.
const xmpp_push_interval = 2 * time.Second

// An xmpp_push is a client's registration with its push service: the service
// and the node it names the client by.
type xmpp_push struct {
	account string
	service string
	node    string
}

// xmpp_push_iq answers a client enabling or disabling push (XEP-0357): the
// service and node kept for the account, with the options its pushes carry,
// or those dropped.
func xmpp_push_iq(a *xmpp_account, s *xmpp_stanza, name string) *xmpp_stanza {
	if s.get("type") != "set" {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	var request struct {
		Enable *struct {
			JID     string `xml:"jid,attr"`
			Node    string `xml:"node,attr"`
			Options *struct {
				Inner string `xml:",innerxml"`
				Type  string `xml:"type,attr"`
			} `xml:"jabber:x:data x"`
		} `xml:"urn:xmpp:push:0 enable"`
		Disable *struct {
			JID  string `xml:"jid,attr"`
			Node string `xml:"node,attr"`
		} `xml:"urn:xmpp:push:0 disable"`
	}
	if s.decode(&request) != nil {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	db := xmpp_database()
	switch {
	case name == "enable" && request.Enable != nil:
		service, err := jid.Parse(request.Enable.JID)
		node := request.Enable.Node
		if err != nil || node == "" || len(node) > 1024 {
			return xmpp_failure(s, "bad-request", "modify", "")
		}
		options := ""
		if o := request.Enable.Options; o != nil {
			if len(o.Inner) > 4096 || o.Type != "submit" {
				return xmpp_failure(s, "bad-request", "modify", "")
			}
			options = `<x xmlns="jabber:x:data" type="submit">` + o.Inner + `</x>`
			if _, err := xmpp_parse(options); err != nil {
				return xmpp_failure(s, "bad-request", "modify", "")
			}
		}
		db.exec("insert into pushes (account, service, node, options, created) values (?, ?, ?, ?, ?) on conflict(account, service, node) do update set options=excluded.options", a.id, service.String(), node, options, now())
	case name == "disable" && request.Disable != nil:
		service, err := jid.Parse(request.Disable.JID)
		if err != nil {
			return xmpp_failure(s, "bad-request", "modify", "")
		}
		if request.Disable.Node != "" {
			db.exec("delete from pushes where account=? and service=? and node=?", a.id, service.String(), request.Disable.Node)
		} else {
			db.exec("delete from pushes where account=? and service=?", a.id, service.String())
		}
	default:
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	return xmpp_result(s, "")
}

// xmpp_push_wake wakes the apps of an account's clients registered for push,
// none of which has a connection: a notification with no content but that a
// message came, from the account's own address to each service.
func xmpp_push_wake(a *xmpp_account) {
	rows, _ := xmpp_database().rows("select service, node, options from pushes where account=?", a.id)
	for _, row := range rows {
		p := xmpp_push{account: a.id, service: row_string(row, "service"), node: row_string(row, "node")}
		key := p.account + " " + p.service + " " + p.node
		xmpp_pushes_lock.Lock()
		if time.Since(xmpp_pushed[key]) < xmpp_push_interval {
			xmpp_pushes_lock.Unlock()
			continue
		}
		xmpp_pushed[key] = time.Now()
		id := "push-" + uid()
		xmpp_pushes[id] = p
		xmpp_pushes_lock.Unlock()
		service, err := jid.Parse(p.service)
		if err != nil {
			continue
		}
		options := ""
		if text := row_string(row, "options"); text != "" {
			options = `<publish-options>` + text + `</publish-options>`
		}
		inner := `<pubsub xmlns="http://jabber.org/protocol/pubsub"><publish node="` + xmpp_escape(p.node) + `"><item><notification xmlns="` + xmpp_ns_push + `"><x xmlns="jabber:x:data" type="form"><field var="FORM_TYPE"><value>urn:xmpp:push:summary</value></field><field var="message-count"><value>1</value></field></x></notification></item></publish>` + options + `</pubsub>`
		xmpp_route(xmpp_make("iq", inner, "type", "set", "id", id), a.address, service)
	}
}

// xmpp_push_answered takes a service's answer to a push: an error stops the
// registration, so a service that no longer knows the client is not woken
// again. It reports whether the stanza was such an answer.
func xmpp_push_answered(a *xmpp_account, s *xmpp_stanza) bool {
	id := s.get("id")
	xmpp_pushes_lock.Lock()
	p, found := xmpp_pushes[id]
	delete(xmpp_pushes, id)
	xmpp_pushes_lock.Unlock()
	if !found || p.account != a.id {
		return false
	}
	if s.get("type") == "error" {
		info("XMPP push to %s for %s refused; stopping it", p.service, a.address)
		xmpp_database().exec("delete from pushes where account=? and service=? and node=?", p.account, p.service, p.node)
	}
	return true
}

// xmpp_push_prune drops the pushes no answer came for.
func xmpp_push_prune() {
	xmpp_pushes_lock.Lock()
	defer xmpp_pushes_lock.Unlock()
	for id := range xmpp_pushes {
		delete(xmpp_pushes, id)
	}
	for key, at := range xmpp_pushed {
		if time.Since(at) > time.Hour {
			delete(xmpp_pushed, key)
		}
	}
}
