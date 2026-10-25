// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: XMPP routing
//
// What a hosted account's clients send goes to its address, here or over a
// server link, and what comes for the account goes to its clients and to the
// apps declaring linked/receive, as the conversation events a linked account's
// session hands them (conversations.go): one conversation with a person shows
// the same in an XMPP client and in Mochi's own. A message that finds no
// client available waits for the next to come online.
//
// Chat's acceptance rule applies to whoever writes to the account: someone
// not on its roster is asked of the apps declaring linked/accept, and a
// message the rule does not take is passed over unseen. The user's block list
// is the account's (XEP-0191), people by their address and domains; what a
// blocked sender sends is refused, and nothing goes to them.
//
// The account's presence is each client's as it says, and, while the user is
// in Mochi and shares their presence, that of Mochi's own sessions under the
// resource "mochi". Contacts' presence goes to the clients and to the apps.
// The server answers for the account its vCard - the user's name and avatar -
// service discovery and pings.

package main

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"mellium.im/xmpp/jid"
)

// xmpp_mochi_resource is the resource Mochi's own sessions show the
// account's presence under.
const xmpp_mochi_resource = "mochi"

// xmpp_notices_name is the name the server's notices come from on each of its
// XMPP domains (notices.go). No account can take it.
const xmpp_notices_name = "notices"

// The most messages kept for an account's clients while none is online.
const xmpp_offline_maximum = 500

// What the server offers, as service discovery tells.
var xmpp_server_features = []string{
	"http://jabber.org/protocol/disco#info",
	"http://jabber.org/protocol/disco#items",
	"msgoffline",
	"urn:xmpp:blocking",
	"urn:xmpp:carbons:2",
	"urn:xmpp:http:upload:0",
	"urn:xmpp:ping",
	"vcard-temp",
}

const (
	xmpp_ns_roster   = "jabber:iq:roster"
	xmpp_ns_blocking = "urn:xmpp:blocking"
	xmpp_ns_vcard    = "vcard-temp"
	xmpp_ns_info     = "http://jabber.org/protocol/disco#info"
	xmpp_ns_items    = "http://jabber.org/protocol/disco#items"
	xmpp_ns_ping     = "urn:xmpp:ping"
)

// An xmpp_account is a hosted XMPP account: its user, its id and its address.
type xmpp_account struct {
	user    *User
	id      string
	address jid.JID
}

// xmpp_account_of is the hosted account at an address, or nil.
func xmpp_account_of(address jid.JID) *xmpp_account {
	if address.Localpart() == "" {
		return nil
	}
	user, row := account_by_address("xmpp", address.Bare().String())
	if user == nil || user_quiet(user) {
		return nil
	}
	return &xmpp_account{user: user, id: row_string(row, "id"), address: address.Bare()}
}

// xmpp_hosted_of is an account row's hosted XMPP account, or nil.
func xmpp_hosted_of(user *User, row map[string]any) *xmpp_account {
	if row == nil || !account_hosted_flag(row) || row["type"] != "xmpp" {
		return nil
	}
	address, err := jid.Parse(row_string(row, "identifier"))
	if err != nil || address.Localpart() == "" {
		return nil
	}
	return &xmpp_account{user: user, id: row_string(row, "id"), address: address.Bare()}
}

// mochi is the address of Mochi's own sessions on the account.
func (a *xmpp_account) mochi() jid.JID {
	full, _ := a.address.WithResource(xmpp_mochi_resource)
	return full
}

// === Routing ===

// xmpp_route sends a stanza from an address to another: to an account here,
// or over the link to the other server.
func xmpp_route(s *xmpp_stanza, from jid.JID, to jid.JID) {
	s.set("from", from.String())
	s.set("to", to.String())
	if xmpp_local(to.Domainpart()) {
		xmpp_deliver(s, from, to)
		return
	}
	xmpp_send_remote(s, from, to)
}

// xmpp_answer routes the answer to a stanza back to where it came from.
func xmpp_answer(s *xmpp_stanza, reply *xmpp_stanza) {
	if reply == nil {
		return
	}
	from, err := jid.Parse(s.get("to"))
	to, err2 := jid.Parse(s.get("from"))
	if err != nil || err2 != nil {
		return
	}
	xmpp_route(reply, from, to)
}

// xmpp_result is an IQ's answer carrying content.
func xmpp_result(s *xmpp_stanza, inner string) *xmpp_stanza {
	return xmpp_make("iq", inner, "id", s.get("id"), "type", "result")
}

// xmpp_from_server takes a stanza another server sent over a link.
func xmpp_from_server(s *xmpp_stanza, from jid.JID, to jid.JID) {
	s.space = xmpp_ns_client
	if !xmpp_local(to.Domainpart()) {
		return
	}
	xmpp_deliver(s, from, to)
}

// xmpp_deliver takes a stanza for an address here: the room service's, the
// server's, the notices address, or an account's, which its user's block list
// guards.
func xmpp_deliver(s *xmpp_stanza, from jid.JID, to jid.JID) {
	if xmpp_room_base(to.Domainpart()) != "" {
		xmpp_conference_deliver(s, from, to)
		return
	}
	if to.Localpart() == "" {
		if s.name == "iq" {
			xmpp_answer(s, xmpp_iq_server(s))
		}
		return
	}
	if strings.EqualFold(to.Localpart(), xmpp_notices_name) {
		if s.name == "iq" {
			xmpp_answer(s, xmpp_iq_notices(s))
		} else if s.name == "message" {
			xmpp_answer(s, xmpp_failure(s, "service-unavailable", "cancel", ""))
		}
		return
	}
	a := xmpp_account_of(to)
	if a == nil {
		if s.name != "presence" {
			xmpp_answer(s, xmpp_failure(s, "service-unavailable", "cancel", ""))
		}
		return
	}
	own := from.Bare().Equal(a.address)
	if !own && xmpp_blocked(a.user, from) {
		if s.name != "presence" {
			xmpp_answer(s, xmpp_failure(s, "service-unavailable", "cancel", ""))
		}
		return
	}
	switch s.name {
	case "message":
		xmpp_message_in(a, s, from, to, own)
	case "presence":
		xmpp_presence_in(a, s, from, to, own)
	case "iq":
		xmpp_iq_in(a, s, from, to)
	}
}

// xmpp_from_client routes what a client sends, from its own address: its
// presence, its messages, and its requests, answered by the server where
// they are the account's own.
func xmpp_from_client(r *xmpp_resource, s *xmpp_stanza) {
	s = s.copy()
	s.set("from", r.full.String())
	a := &xmpp_account{user: r.user, id: r.account, address: r.address}
	to := r.address
	given := s.get("to") != ""
	if given {
		parsed, err := jid.Parse(s.get("to"))
		if err != nil {
			if reply := xmpp_failure(s, "jid-malformed", "modify", ""); reply != nil {
				reply.set("to", r.full.String())
				r.send(reply.text(xmpp_ns_client))
			}
			return
		}
		to = parsed
	}
	switch s.name {
	case "message":
		xmpp_client_message(a, r, s, to)
	case "presence":
		xmpp_client_presence(a, r, s, to, given)
	case "iq":
		xmpp_client_iq(a, r, s, to)
	}
}

// === Messages ===

// xmpp_client_message sends a client's message, copies it to the account's
// other clients that take copies, and hands a message of a conversation to
// the apps as the account's own, with the id the account's archive gives it.
// What a client says in a room it is in, or privately to someone there, is
// the client's: Mochi's own sessions hear the room for themselves.
func xmpp_client_message(a *xmpp_account, r *xmpp_resource, s *xmpp_stanza, to jid.JID) {
	self := to.Bare().Equal(a.address)
	if !self && xmpp_blocked(a.user, to) {
		s.set("to", to.String())
		if reply := xmpp_failure(s, "not-acceptable", "cancel", `<blocked xmlns="urn:xmpp:blocking:errors"/>`); reply != nil {
			reply.set("from", to.String())
			r.send(reply.text(xmpp_ns_client))
		}
		return
	}
	kind := s.get("type")
	var m xmpp_message
	decoded := s.decode(&m) == nil
	room := kind == "groupchat" || r.joined(to.Bare())
	archive := ""
	if decoded && !self && !room && to.Localpart() != "" && xmpp_archivable(kind, &m) {
		archive = uid()
	}
	xmpp_route(s, r.full, to)
	if !self && xmpp_carbon_eligible(s) {
		sent := s.copy()
		if archive != "" {
			xmpp_stanza_id(sent, a.address.String(), archive)
		}
		xmpp_carbons(a, sent, "sent", r)
	}
	if self || room || to.Localpart() == "" || !decoded || (kind != "" && kind != "chat" && kind != "normal") {
		return
	}
	xmpp_accepted_forget(a, to.Bare())
	conversation := "xmpp:" + to.Bare().String()
	// What one of the account's clients encrypted is encrypted to its
	// device here too.
	omemo_read(a.user, a.id, &m, a.address, "")
	events := xmpp_events(&m, nil, r.full, conversation, "xmpp:"+a.address.String(), "", archive, true)
	xmpp_payload_add(events, s)
	xmpp_tell(a, events)
}

// xmpp_in_room reports whether one of the account's clients is in a room.
func xmpp_in_room(a *xmpp_account, room jid.JID) bool {
	for _, r := range xmpp_resources_of(a.address.String()) {
		if r.joined(room) {
			return true
		}
	}
	return false
}

// joined reports whether the client has sent presence to a room: a room's
// address it has joined under some nick.
func (r *xmpp_resource) joined(room jid.JID) bool {
	prefix := room.Bare().String() + "/"
	r.lock.Lock()
	defer r.lock.Unlock()
	for to := range r.directed {
		if strings.HasPrefix(to, prefix) {
			return true
		}
	}
	return false
}

// xmpp_payload_add gives a message event the encrypted content its stanza
// carries, which chat keeps for the archive.
func xmpp_payload_add(events []map[string]any, s *xmpp_stanza) {
	if len(events) == 0 || events[len(events)-1]["kind"] != "message" || events[len(events)-1]["encrypted"] != true {
		return
	}
	if payload := xmpp_payload(s); payload != "" {
		events[len(events)-1]["payload"] = payload
	}
}

// xmpp_message_in takes a message for an account: to the client a full
// address names, else to every client available, copied to those that take
// copies, and to the apps, unless the acceptance rule passes it over. A
// message of a conversation is given its id in the account's archive; one
// no client takes waits for the next, and where no client has a connection
// their push services are told. What a room sends is for the client in it,
// or for Mochi's own sessions.
func xmpp_message_in(a *xmpp_account, s *xmpp_stanza, from jid.JID, to jid.JID, own bool) {
	kind := s.get("type")
	if to.Resourcepart() == xmpp_mochi_resource && (xmpp_joining_of(a, from.Bare().String()) != nil || xmpp_joined_of(a, from.Bare().String()) != nil) {
		xmpp_room_in(a, s, from)
		return
	}
	if kind == "groupchat" {
		// A room's message for a client not online is dropped (RFC 6121).
		if r := xmpp_resource_get(a.address.String(), to.Resourcepart()); r != nil && to.Resourcepart() != "" {
			r.send(s.text(xmpp_ns_client))
		}
		return
	}
	// What a room one of the account's clients is in sends that client - a
	// private message from someone there, the room's archive - is the
	// client's, as is the archive's answer to a query the client asked.
	if to.Resourcepart() != "" && (xmpp_in_room(a, from.Bare()) || xmpp_archive_result(s)) {
		xmpp_message_deliver(a, to, s, true)
		return
	}
	var m xmpp_message
	decoded := s.decode(&m) == nil
	// Someone's OMEMO device list as their server tells of it changing, an
	// account's own or one it reads.
	if decoded && m.Event != nil && len(m.Event.Items.Item) > 0 {
		if d, err := omemo_device_of(a.user, a.id); err == nil {
			d.heard(context.Background(), from, m.Event.Items.Node, m.Event.Items.Item[0].Inner)
		}
	}
	// A room's invitation is the inviter's, whom the acceptance rule asks of.
	sender := from.Bare()
	if decoded && m.Invitation != nil && m.Invitation.Invite != nil {
		if inviter, err := jid.Parse(m.Invitation.Invite.From); err == nil {
			sender = inviter.Bare()
		}
	}
	if !own && kind != "error" && !xmpp_accepted(a, sender) {
		debug("XMPP message from %s to %s passed over: the acceptance rule does not take it", sender, a.address)
		return
	}
	archive := ""
	if decoded && !own && xmpp_archivable(kind, &m) {
		archive = uid()
		s = s.copy()
		xmpp_stanza_id(s, a.address.String(), archive)
	}
	delivered := xmpp_message_deliver(a, to, s, kind == "error" || kind == "headline")
	if own || kind == "error" || kind == "headline" || !decoded {
		return
	}
	if !delivered && (strings.TrimSpace(m.Body) != "" || m.encrypted()) {
		xmpp_offline_keep(a, s)
	}
	if archive != "" && !xmpp_live(a) {
		xmpp_push_wake(a)
	}
	conversation := "xmpp:" + from.Bare().String()
	name := from.Localpart()
	if nick := strings.TrimSpace(m.Nick); nick != "" && len(nick) <= 200 {
		name = nick
	}
	omemo_read(a.user, a.id, &m, from.Bare(), "")
	events := xmpp_events(&m, m.Delay, from, conversation, conversation, name, archive, false)
	if len(events) > 0 && events[len(events)-1]["kind"] == "message" && m.Markable != nil {
		xmpp_markable_set(a, conversation, events[len(events)-1]["id"].(string))
	}
	xmpp_payload_add(events, s)
	xmpp_tell(a, events)
}

// xmpp_message_deliver hands a message to an account's clients: the one a
// full address names where it is bound, else each available with a priority
// not below zero, and a copy to each other client that takes copies where it
// is one of a conversation. A message only for the resource named goes
// nowhere where it is not bound. It answers whether any client took it.
func xmpp_message_deliver(a *xmpp_account, to jid.JID, s *xmpp_stanza, only bool) bool {
	text := s.text(xmpp_ns_client)
	var took []*xmpp_resource
	if resource := to.Resourcepart(); resource != "" {
		if r := xmpp_resource_get(a.address.String(), resource); r != nil {
			r.send(text)
			took = append(took, r)
		} else if only {
			return false
		}
	}
	if len(took) == 0 {
		for _, r := range xmpp_resources_of(a.address.String()) {
			r.lock.Lock()
			ok := r.available && r.priority >= 0
			r.lock.Unlock()
			if ok {
				r.send(text)
				took = append(took, r)
			}
		}
	}
	if xmpp_carbon_eligible(s) {
		xmpp_carbons(a, s, "received", took...)
	}
	return len(took) > 0
}

// xmpp_resources_deliver hands a stanza to an account's clients: the one a
// full address names where it is bound, else each available with a priority
// not below zero. A stanza only for the resource named goes nowhere where it
// is not bound. It answers whether any client took it.
func xmpp_resources_deliver(a *xmpp_account, to jid.JID, text string, only bool) bool {
	if resource := to.Resourcepart(); resource != "" {
		if r := xmpp_resource_get(a.address.String(), resource); r != nil {
			r.send(text)
			return true
		}
		if only {
			return false
		}
	}
	delivered := false
	for _, r := range xmpp_resources_of(a.address.String()) {
		r.lock.Lock()
		ok := r.available && r.priority >= 0
		r.lock.Unlock()
		if ok {
			r.send(text)
			delivered = true
		}
	}
	return delivered
}

// === Carbons ===

const xmpp_ns_carbons = "urn:xmpp:carbons:2"

// xmpp_carbon_eligible reports whether a message is copied to the account's
// other clients (XEP-0280): a chat message, or a normal one with a body, that
// is not marked private, a copy already, or a room's to one of its occupants.
func xmpp_carbon_eligible(s *xmpp_stanza) bool {
	kind := s.get("type")
	body := false
	for _, c := range s.children() {
		switch {
		case c.space == xmpp_ns_carbons:
			return false
		case c.space == "urn:xmpp:hints" && c.name == "no-copy":
			return false
		case c.space == "http://jabber.org/protocol/muc#user":
			return false
		case c.space == xmpp_ns_client && c.name == "body":
			body = true
		}
	}
	return kind == "chat" || ((kind == "" || kind == "normal") && body)
}

// xmpp_carbons copies a message to the account's clients that take copies,
// but for those given: as received where it came to the account, as sent
// where it went from it.
func xmpp_carbons(a *xmpp_account, s *xmpp_stanza, direction string, except ...*xmpp_resource) {
	text := s.text(xmpp_ns_client)
	for _, r := range xmpp_resources_of(a.address.String()) {
		if slices.Contains(except, r) {
			continue
		}
		r.lock.Lock()
		ok := r.carbons && r.available
		r.lock.Unlock()
		if ok {
			r.send(xmpp_make("message", `<`+direction+` xmlns="`+xmpp_ns_carbons+`"><forwarded xmlns="`+xmpp_ns_forward+`">`+text+`</forwarded></`+direction+`>`, "from", a.address.String(), "to", r.full.String(), "type", s.get("type")).text(xmpp_ns_client))
		}
	}
}

// xmpp_carbons_iq answers a client enabling or disabling copies.
func xmpp_carbons_iq(r *xmpp_resource, s *xmpp_stanza, name string) *xmpp_stanza {
	if s.get("type") != "set" || (name != "enable" && name != "disable") {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	r.lock.Lock()
	r.carbons = name == "enable"
	r.lock.Unlock()
	return xmpp_result(s, "")
}

// xmpp_offline_keep keeps a message for the account's next client, stamped
// with when it came; the oldest go past the limit.
func xmpp_offline_keep(a *xmpp_account, s *xmpp_stanza) {
	kept := s.copy()
	kept.inner += `<delay xmlns="urn:xmpp:delay" from="` + xmpp_escape(a.address.Domainpart()) + `" stamp="` + time.Now().UTC().Format(time.RFC3339) + `"/>`
	db := xmpp_database()
	db.exec("insert into offline (account, stanza, created) values (?, ?, ?)", a.id, kept.text(xmpp_ns_client), now())
	db.exec("delete from offline where account=? and id not in (select id from offline where account=? order by id desc limit ?)", a.id, a.id, xmpp_offline_maximum)
}

// xmpp_offline_send gives a client that has come online the messages kept
// for the account.
func xmpp_offline_send(a *xmpp_account, r *xmpp_resource) {
	db := xmpp_database()
	rows, _ := db.rows("select id, stanza from offline where account=? order by id", a.id)
	for _, row := range rows {
		r.send(row_string(row, "stanza"))
		db.exec("delete from offline where id=?", row["id"])
	}
}

// === Telling the apps ===

// An xmpp_batch is work for a user's queue: what an account heard, for the
// apps, or a change to its roster, for people.
type xmpp_batch struct {
	work func()
}

// Each user's queue to the apps, which keeps what they are told in order and
// keeps the streams from waiting on them.
var (
	xmpp_queues      = map[string]chan xmpp_batch{}
	xmpp_queues_lock sync.Mutex
)

// xmpp_tell hands events of an account to the apps declaring linked/receive,
// in order.
func xmpp_tell(a *xmpp_account, events []map[string]any) {
	if len(events) == 0 {
		return
	}
	user, account := a.user, a.id
	xmpp_queue(user, func() {
		linked_inbound(user, account, "xmpp", events)
	})
}

// xmpp_teller does the work on a user's queue, ending once it has been empty
// for a minute.
func xmpp_teller(uid string, queue chan xmpp_batch) {
	for {
		select {
		case b := <-queue:
			b.work()
			xmpp_running.Done()
		case <-time.After(time.Minute):
			xmpp_queues_lock.Lock()
			if len(queue) == 0 {
				delete(xmpp_queues, uid)
				xmpp_queues_lock.Unlock()
				return
			}
			xmpp_queues_lock.Unlock()
		}
	}
}

// === The acceptance rule ===

type xmpp_acceptance struct {
	accepted bool
	until    time.Time
}

// The apps' answers for a minute, so a burst of typing asks once.
var (
	xmpp_acceptances      = map[string]xmpp_acceptance{}
	xmpp_acceptances_lock sync.Mutex
)

// xmpp_accepted reports whether someone may write to an account: someone on
// its roster, or whom an app declaring linked/accept takes - under chat's
// rule, someone the user knows, or anyone where their policy is anyone.
func xmpp_accepted(a *xmpp_account, sender jid.JID) bool {
	sender = sender.Bare()
	if item := xmpp_roster_item(a, sender.String()); item != nil && (item.subscription != "none" || item.ask) {
		return true
	}
	key := a.id + " " + sender.String()
	xmpp_acceptances_lock.Lock()
	cached, found := xmpp_acceptances[key]
	xmpp_acceptances_lock.Unlock()
	if found && time.Now().Before(cached.until) {
		return cached.accepted
	}
	accepted := false
	for _, app := range apps_for_function(a.user, "linked/accept") {
		result, err := activitypub_call(a.user, app, "linked/accept", Map{"account": a.id, "network": "xmpp", "person": "xmpp:" + sender.String()})
		if err != nil {
			info("Linked accept from %s: %v", app.id, err)
			continue
		}
		if yes, _ := result.(bool); yes {
			accepted = true
			break
		}
	}
	xmpp_acceptances_lock.Lock()
	xmpp_acceptances[key] = xmpp_acceptance{accepted: accepted, until: time.Now().Add(time.Minute)}
	xmpp_acceptances_lock.Unlock()
	return accepted
}

// xmpp_accepted_forget drops the answer kept for someone, whom the user has
// just written to.
func xmpp_accepted_forget(a *xmpp_account, other jid.JID) {
	xmpp_acceptances_lock.Lock()
	delete(xmpp_acceptances, a.id+" "+other.Bare().String())
	xmpp_acceptances_lock.Unlock()
}

// xmpp_accepted_prune drops the answers past their minute.
func xmpp_accepted_prune() {
	xmpp_acceptances_lock.Lock()
	defer xmpp_acceptances_lock.Unlock()
	for key, entry := range xmpp_acceptances {
		if time.Now().After(entry.until) {
			delete(xmpp_acceptances, key)
		}
	}
}

// === Blocking ===

// xmpp_blocked reports whether the user blocks an address: its person, or a
// domain it is on or under.
func xmpp_blocked(user *User, other jid.JID) bool {
	if other.Localpart() != "" && actor_blocked(user, "xmpp:"+strings.ToLower(other.Bare().String())) {
		return true
	}
	return blocked_by(db_user(user, "user"), strings.ToLower(other.Domainpart()))
}

// xmpp_blocklist lists what the user blocks, as XMPP addresses: people on
// XMPP, and domains.
func xmpp_blocklist(user *User) []string {
	db := db_user(user, "user")
	var out []string
	rows, _ := db.rows("select actor from blocked where actor like 'xmpp:%' order by created")
	for _, row := range rows {
		out = append(out, strings.TrimPrefix(row_string(row, "actor"), "xmpp:"))
	}
	domains, _ := db.rows("select domain from blocks order by created")
	for _, row := range domains {
		out = append(out, row_string(row, "domain"))
	}
	return out
}

// xmpp_blocking_iq answers a client's blocking command (XEP-0191): the list,
// or addresses blocked or unblocked, which every client of the account is
// told of. Blocking someone takes the account's presence from them.
func xmpp_blocking_iq(a *xmpp_account, s *xmpp_stanza, name string) *xmpp_stanza {
	kind := s.get("type")
	if name == "blocklist" && kind == "get" {
		var items strings.Builder
		for _, address := range xmpp_blocklist(a.user) {
			items.WriteString(`<item jid="` + xmpp_escape(address) + `"/>`)
		}
		return xmpp_result(s, `<blocklist xmlns="`+xmpp_ns_blocking+`">`+items.String()+`</blocklist>`)
	}
	if kind != "set" || (name != "block" && name != "unblock") {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	var request struct {
		Block []struct {
			JID string `xml:"jid,attr"`
		} `xml:"block>item"`
		Unblock []struct {
			JID string `xml:"jid,attr"`
		} `xml:"unblock>item"`
	}
	s.decode(&request)
	entries := request.Block
	if name == "unblock" {
		entries = request.Unblock
	}
	var addresses []jid.JID
	for _, item := range entries {
		address, err := jid.Parse(item.JID)
		if err != nil {
			return xmpp_failure(s, "jid-malformed", "modify", "")
		}
		addresses = append(addresses, address.Bare())
	}
	if name == "block" && len(addresses) == 0 {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	db := db_user(a.user, "user")
	if name == "unblock" && len(addresses) == 0 {
		for _, address := range xmpp_blocklist(a.user) {
			if parsed, err := jid.Parse(address); err == nil {
				addresses = append(addresses, parsed)
			}
		}
	}
	var items strings.Builder
	for _, address := range addresses {
		items.WriteString(`<item jid="` + xmpp_escape(address.String()) + `"/>`)
		switch {
		case name == "block" && address.Localpart() == "":
			block_add(db, address.Domainpart())
		case name == "block":
			blocked_add(a.user, "xmpp:"+strings.ToLower(address.String()))
			for _, r := range xmpp_resources_of(a.address.String()) {
				r.lock.Lock()
				available := r.available
				r.lock.Unlock()
				if available {
					xmpp_route(xmpp_make("presence", "", "type", "unavailable"), r.full, address)
				}
			}
		case address.Localpart() == "":
			block_remove(db, address.Domainpart())
		default:
			blocked_remove(a.user, "xmpp:"+strings.ToLower(address.String()))
		}
	}
	push := `<` + name + ` xmlns="` + xmpp_ns_blocking + `">` + items.String() + `</` + name + `>`
	for _, r := range xmpp_resources_of(a.address.String()) {
		r.send(xmpp_make("iq", push, "type", "set", "id", "push-"+uid(), "to", r.full.String()).text(xmpp_ns_client))
	}
	return xmpp_result(s, "")
}

// === Presence ===

// xmpp_status is the Mochi status an XMPP show is.
func xmpp_status(show string) string {
	switch show {
	case "away", "xa":
		return "away"
	case "dnd":
		return "busy"
	}
	return "online"
}

// xmpp_client_presence takes a client's presence: broadcast to the account's
// contacts and its other clients, the first time with the contacts' asked
// for and what waited for it handed over; or a subscription request or
// answer; or presence sent to one address.
func xmpp_client_presence(a *xmpp_account, r *xmpp_resource, s *xmpp_stanza, to jid.JID, given bool) {
	kind := s.get("type")
	if given {
		switch kind {
		case "subscribe", "subscribed", "unsubscribe", "unsubscribed":
			xmpp_subscription_out(a, kind, to.Bare())
		case "", "unavailable":
			if !xmpp_blocked(a.user, to) {
				// Where the client sent presence of its own is told it is gone
				// when it goes: a room it joined, above all.
				r.lock.Lock()
				if kind == "" && len(r.directed) < xmpp_directed_maximum {
					r.directed[to.String()] = true
				} else if kind == "unavailable" {
					delete(r.directed, to.String())
				}
				r.lock.Unlock()
				xmpp_route(s, r.full, to)
			}
		}
		return
	}
	switch kind {
	case "":
		var p struct {
			Priority string `xml:"priority"`
			Show     string `xml:"show"`
			Status   string `xml:"status"`
		}
		s.decode(&p)
		r.lock.Lock()
		first := !r.available
		r.available = true
		r.priority = xmpp_priority(p.Priority)
		r.presence = s.inner
		r.lock.Unlock()
		xmpp_broadcast(a, r.full, s.inner, "")
		if first {
			presence_connect(a.user.UID, r.connection, "", "xmpp", "")
			xmpp_arrived(a, r)
		}
		presence_set(a.user.UID, "", r.connection, xmpp_status(p.Show), strings.TrimSpace(p.Status))
	case "unavailable":
		r.lock.Lock()
		available := r.available
		r.available = false
		directed := make([]string, 0, len(r.directed))
		for address := range r.directed {
			directed = append(directed, address)
		}
		r.directed = map[string]bool{}
		r.lock.Unlock()
		if available {
			xmpp_broadcast(a, r.full, s.inner, "unavailable")
			presence_disconnect(a.user.UID, r.connection)
		}
		for _, address := range directed {
			if target, err := jid.Parse(address); err == nil {
				xmpp_route(xmpp_make("presence", "", "type", "unavailable"), r.full, target)
			}
		}
	}
}

// The most addresses one client's presence of its own is kept for.
const xmpp_directed_maximum = 1000

// xmpp_presence_gone tells an account's contacts that a client whose stream
// ended is unavailable.
func xmpp_presence_gone(r *xmpp_resource) {
	a := &xmpp_account{user: r.user, id: r.account, address: r.address}
	xmpp_broadcast(a, r.full, "", "unavailable")
	presence_disconnect(a.user.UID, r.connection)
}

// xmpp_broadcast sends presence from one of an account's resources to each
// contact allowed to see it and to the account's other clients.
func xmpp_broadcast(a *xmpp_account, from jid.JID, inner string, kind string) {
	for _, item := range xmpp_roster(a) {
		if item.subscription != "from" && item.subscription != "both" {
			continue
		}
		to, err := jid.Parse(item.address)
		if err != nil || xmpp_blocked(a.user, to) {
			continue
		}
		xmpp_route(xmpp_make("presence", inner, "type", kind), from, to)
	}
	text := xmpp_make("presence", inner, "type", kind, "from", from.String(), "to", a.address.String()).text(xmpp_ns_client)
	for _, r := range xmpp_resources_of(a.address.String()) {
		if r.full.Equal(from) {
			continue
		}
		r.lock.Lock()
		available := r.available
		r.lock.Unlock()
		if available {
			r.send(text)
		}
	}
}

// xmpp_arrived gives a client that has just come online what it should know:
// the presence of its contacts, asked for; that of the account's other
// clients and Mochi's own; the subscription requests waiting; and the
// messages kept for it.
func xmpp_arrived(a *xmpp_account, r *xmpp_resource) {
	xmpp_probes(a)
	for _, other := range xmpp_resources_of(a.address.String()) {
		if other == r {
			continue
		}
		other.lock.Lock()
		available, inner := other.available, other.presence
		other.lock.Unlock()
		if available {
			r.send(xmpp_make("presence", inner, "from", other.full.String(), "to", r.full.String()).text(xmpp_ns_client))
		}
	}
	if inner, shown := xmpp_mochi_presence(a); shown {
		r.send(xmpp_make("presence", inner, "from", a.mochi().String(), "to", r.full.String()).text(xmpp_ns_client))
	}
	rows, _ := xmpp_database().rows("select address from roster where account=? and pending=1", a.id)
	for _, row := range rows {
		r.send(xmpp_make("presence", "", "type", "subscribe", "from", row_string(row, "address"), "to", a.address.String()).text(xmpp_ns_client))
	}
	xmpp_offline_send(a, r)
}

// xmpp_probes asks the account's contacts whose presence it may see for it.
func xmpp_probes(a *xmpp_account) {
	for _, item := range xmpp_roster(a) {
		if item.subscription != "to" && item.subscription != "both" {
			continue
		}
		if to, err := jid.Parse(item.address); err == nil && !xmpp_blocked(a.user, to) {
			xmpp_route(xmpp_make("presence", "", "type", "probe"), a.address, to)
		}
	}
}

// xmpp_presence_in takes presence for an account: a contact's, which its
// clients and the apps are told where the account may see it; a probe; a
// subscription request or answer; or presence sent to one of its clients.
func xmpp_presence_in(a *xmpp_account, s *xmpp_stanza, from jid.JID, to jid.JID, own bool) {
	kind := s.get("type")
	if to.Resourcepart() == xmpp_mochi_resource && xmpp_room_heard(a, s, from) {
		return
	}
	switch kind {
	case "", "unavailable":
		if to.Resourcepart() != "" {
			if r := xmpp_resource_get(a.address.String(), to.Resourcepart()); r != nil {
				r.send(s.text(xmpp_ns_client))
			}
			return
		}
		if own {
			return
		}
		item := xmpp_roster_item(a, from.Bare().String())
		if item == nil || (item.subscription != "to" && item.subscription != "both") {
			return
		}
		xmpp_resources_deliver(a, to, s.text(xmpp_ns_client), false)
		var p struct {
			Show   string `xml:"show"`
			Status string `xml:"status"`
		}
		s.decode(&p)
		xmpp_contact_presence(a, from, kind == "unavailable", p.Show, p.Status)
	case "probe":
		if !own {
			xmpp_probe_answer(a, from.Bare())
		}
	case "subscribe", "subscribed", "unsubscribe", "unsubscribed":
		if !own {
			xmpp_subscription_in(a, kind, from.Bare(), s)
		}
	case "error":
		// An address that refused a client's presence, a room refusing a
		// join above all, did not take it.
		if to.Resourcepart() != "" {
			if r := xmpp_resource_get(a.address.String(), to.Resourcepart()); r != nil {
				r.lock.Lock()
				delete(r.directed, from.String())
				r.lock.Unlock()
			}
		}
		xmpp_resources_deliver(a, to, s.text(xmpp_ns_client), true)
	}
}

// xmpp_probe_answer answers a contact asking for the account's presence:
// each client's and Mochi's, or unavailable where none is online, or, to
// someone not allowed to see it, that they are not subscribed.
func xmpp_probe_answer(a *xmpp_account, prober jid.JID) {
	item := xmpp_roster_item(a, prober.String())
	if item == nil || (item.subscription != "from" && item.subscription != "both") {
		xmpp_route(xmpp_make("presence", "", "type", "unsubscribed"), a.address, prober)
		return
	}
	sent := false
	for _, r := range xmpp_resources_of(a.address.String()) {
		r.lock.Lock()
		available, inner := r.available, r.presence
		r.lock.Unlock()
		if available {
			xmpp_route(xmpp_make("presence", inner), r.full, prober)
			sent = true
		}
	}
	if inner, shown := xmpp_mochi_presence(a); shown {
		xmpp_route(xmpp_make("presence", inner), a.mochi(), prober)
		sent = true
	}
	if !sent {
		xmpp_route(xmpp_make("presence", "", "type", "unavailable"), a.address, prober)
	}
}

// What each account has heard of its contacts' presence, by contact and
// resource, and what the apps were last told of each contact.
var (
	xmpp_heard      = map[string]map[string]map[string][2]string{}
	xmpp_told       = map[string]string{}
	xmpp_heard_lock sync.Mutex
)

// xmpp_contact_presence keeps a contact's presence on one resource and tells
// the apps the contact's presence over all of them where it changed: the
// most present, online over busy over away.
func xmpp_contact_presence(a *xmpp_account, from jid.JID, gone bool, show string, status string) {
	bare := from.Bare().String()
	xmpp_heard_lock.Lock()
	if xmpp_heard[a.id] == nil {
		xmpp_heard[a.id] = map[string]map[string][2]string{}
	}
	resources := xmpp_heard[a.id][bare]
	if resources == nil {
		resources = map[string][2]string{}
		xmpp_heard[a.id][bare] = resources
	}
	if gone {
		delete(resources, from.Resourcepart())
	} else {
		resources[from.Resourcepart()] = [2]string{xmpp_status(show), strings.TrimSpace(status)}
	}
	best, message := "offline", ""
	rank := map[string]int{"offline": 0, "away": 1, "busy": 2, "online": 3}
	for _, heard := range resources {
		if rank[heard[0]] > rank[best] {
			best, message = heard[0], heard[1]
		}
	}
	key := a.id + " " + bare
	changed := xmpp_told[key] != best+"\n"+message
	xmpp_told[key] = best + "\n" + message
	xmpp_heard_lock.Unlock()
	if changed {
		xmpp_tell(a, []map[string]any{{"kind": "presence", "from": "xmpp:" + bare, "status": best, "message": message}})
	}
}

// === Mochi's own presence ===

// What each account last showed of Mochi's own sessions.
var (
	xmpp_shown      = map[string]string{}
	xmpp_shown_lock sync.Mutex
)

// xmpp_mochi_presence is the presence Mochi's own sessions show on an
// account, and whether they show any: the user's status in Mochi over their
// sessions other than XMPP clients, while they are in Mochi and share it.
func xmpp_mochi_presence(a *xmpp_account) (string, bool) {
	hidden := false
	if row := account_hosted_get(a.user, a.id); row != nil {
		data, _ := row["data"].(map[string]any)
		hidden, _ = data["hidden"].(bool)
	}
	status, message := presence_without(a.user.UID, "xmpp")
	if hidden || status == "offline" {
		return "", false
	}
	inner := ""
	switch status {
	case "away":
		inner += `<show>away</show>`
	case "busy":
		inner += `<show>dnd</show>`
	}
	if message != "" {
		inner += `<status>` + xmpp_escape(message) + `</status>`
	}
	inner += `<c xmlns="http://jabber.org/protocol/caps" hash="sha-1" node="` + xmpp_node + `" ver="` + xmpp_caps() + `"/>`
	if _, photo := xmpp_vcard(a); photo != "" {
		inner += `<x xmlns="vcard-temp:x:update"><photo>` + photo + `</photo></x>`
	}
	return inner, true
}

// xmpp_mochi_update shows each of a user's hosted XMPP accounts' contacts the
// presence of Mochi's own sessions where it changed, and on first showing it
// asks for theirs, so the apps hear of them.
func xmpp_mochi_update(uid string) {
	if !network_enabled("xmpp") {
		return
	}
	user := user_by_uid(uid)
	if user == nil {
		return
	}
	for _, row := range accounts_hosted(user, "xmpp") {
		a := xmpp_hosted_of(user, row)
		if a == nil {
			continue
		}
		inner, shown := xmpp_mochi_presence(a)
		xmpp_shown_lock.Lock()
		last, known := xmpp_shown[a.id]
		state := ""
		if shown {
			state = "shown\n" + inner
		}
		changed := !known || last != state
		xmpp_shown[a.id] = state
		xmpp_shown_lock.Unlock()
		switch {
		case !changed:
		case shown:
			xmpp_broadcast(a, a.mochi(), inner, "")
			if !strings.HasPrefix(last, "shown") {
				xmpp_probes(a)
			}
		case known:
			xmpp_broadcast(a, a.mochi(), "", "unavailable")
		}
		if changed {
			xmpp_rooms_announce(a)
		}
	}
}

// === Requests ===

// xmpp_client_iq takes a client's request: the account's own and the
// server's answered here, any other sent on.
func xmpp_client_iq(a *xmpp_account, r *xmpp_resource, s *xmpp_stanza, to jid.JID) {
	kind := s.get("type")
	switch {
	case to.Bare().Equal(a.address) && to.Resourcepart() == "":
		if kind == "result" || kind == "error" {
			return
		}
		reply := xmpp_iq_own(a, r, s)
		if reply != nil {
			reply.set("to", r.full.String())
			r.send(reply.text(xmpp_ns_client))
		}
	case to.Localpart() == "" && xmpp_local(to.Domainpart()):
		xmpp_route(s, r.full, to)
	default:
		if xmpp_blocked(a.user, to) {
			s.set("to", to.String())
			if reply := xmpp_failure(s, "not-acceptable", "cancel", `<blocked xmlns="urn:xmpp:blocking:errors"/>`); reply != nil {
				reply.set("from", to.String())
				r.send(reply.text(xmpp_ns_client))
			}
			return
		}
		xmpp_route(s, r.full, to)
	}
}

// xmpp_iq_own answers a client's request about its own account: the roster,
// the block list, its vCard, a session, copies of its messages, its archive,
// push, what it publishes (PEP), discovery and pings.
func xmpp_iq_own(a *xmpp_account, r *xmpp_resource, s *xmpp_stanza) *xmpp_stanza {
	space, name := s.child()
	kind := s.get("type")
	switch {
	case space == xmpp_ns_roster && name == "query":
		return xmpp_roster_iq(a, r, s)
	case space == xmpp_ns_blocking:
		return xmpp_blocking_iq(a, s, name)
	case space == xmpp_ns_vcard && name == "vCard":
		if kind == "get" {
			card, _ := xmpp_vcard(a)
			return xmpp_result(s, card)
		}
		return xmpp_failure(s, "not-allowed", "cancel", "")
	case space == xmpp_ns_session && name == "session":
		return xmpp_result(s, "")
	case space == xmpp_ns_carbons:
		return xmpp_carbons_iq(r, s, name)
	case space == xmpp_ns_archive && name == "query":
		return xmpp_archive_iq(a, r, s)
	case space == xmpp_ns_push:
		return xmpp_push_iq(a, s, name)
	case (space == xmpp_ns_pubsub || space == xmpp_ns_pubsub_owner) && name == "pubsub":
		return xmpp_pep_iq(a, s, true)
	case space == xmpp_ns_info && name == "query" && kind == "get":
		return xmpp_account_info(s, true)
	}
	return xmpp_iq_account(s, space, name)
}

// xmpp_iq_in takes a request for an account: one for a client goes to it,
// one for the account is answered for it.
func xmpp_iq_in(a *xmpp_account, s *xmpp_stanza, from jid.JID, to jid.JID) {
	kind := s.get("type")
	if resource := to.Resourcepart(); resource != "" {
		if r := xmpp_resource_get(a.address.String(), resource); r != nil {
			r.send(s.text(xmpp_ns_client))
			return
		}
		if kind != "get" && kind != "set" {
			if resource == xmpp_mochi_resource {
				xmpp_answered(s)
			}
			return
		}
		if resource == xmpp_mochi_resource {
			xmpp_answer(s, xmpp_iq_mochi(s))
			return
		}
		xmpp_answer(s, xmpp_failure(s, "service-unavailable", "cancel", ""))
		return
	}
	if kind != "get" && kind != "set" {
		if !xmpp_push_answered(a, s) {
			xmpp_answered(s)
		}
		return
	}
	space, name := s.child()
	switch {
	case space == xmpp_ns_vcard && name == "vCard" && kind == "get":
		card, _ := xmpp_vcard(a)
		xmpp_answer(s, xmpp_result(s, card))
		return
	case space == xmpp_ns_pubsub && name == "pubsub":
		xmpp_answer(s, xmpp_pep_iq_from(a, s, from))
		return
	case space == xmpp_ns_info && name == "query" && kind == "get":
		xmpp_answer(s, xmpp_account_info(s, false))
		return
	}
	xmpp_answer(s, xmpp_iq_account(s, space, name))
}

// xmpp_account_info is what discovery shows of an account: that it is one,
// that it publishes (PEP), and to its own clients what it offers them.
func xmpp_account_info(s *xmpp_stanza, own bool) *xmpp_stanza {
	features := []string{xmpp_ns_info}
	if own {
		features = append(features, xmpp_ns_archive, xmpp_ns_push, "http://jabber.org/protocol/pubsub#publish", "http://jabber.org/protocol/pubsub#publish-options", "http://jabber.org/protocol/pubsub#retract-items", "http://jabber.org/protocol/pubsub#delete-nodes", "urn:xmpp:bookmarks:1#compat")
	}
	var text strings.Builder
	text.WriteString(`<query xmlns="` + xmpp_ns_info + `"><identity category="account" type="registered"/><identity category="pubsub" type="pep"/>`)
	for _, feature := range features {
		text.WriteString(`<feature var="` + feature + `"/>`)
	}
	text.WriteString(`</query>`)
	return xmpp_result(s, text.String())
}

// xmpp_iq_account answers what the server answers for any account:
// discovery, which shows an account, and pings.
func xmpp_iq_account(s *xmpp_stanza, space string, name string) *xmpp_stanza {
	kind := s.get("type")
	switch {
	case space == xmpp_ns_info && name == "query" && kind == "get":
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_info+`"><identity category="account" type="registered"/><feature var="`+xmpp_ns_info+`"/></query>`)
	case space == xmpp_ns_items && name == "query" && kind == "get":
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_items+`"/>`)
	case space == xmpp_ns_ping && name == "ping" && kind == "get":
		return xmpp_result(s, "")
	}
	return xmpp_failure(s, "service-unavailable", "cancel", "")
}

// xmpp_iq_server answers a request to the server: discovery, pings and the
// upload service's slots.
func xmpp_iq_server(s *xmpp_stanza) *xmpp_stanza {
	kind := s.get("type")
	if kind != "get" && kind != "set" {
		return nil
	}
	space, name := s.child()
	switch {
	case space == xmpp_ns_info && name == "query" && kind == "get":
		var features strings.Builder
		for _, feature := range xmpp_server_features {
			features.WriteString(`<feature var="` + feature + `"/>`)
		}
		upload := `<x xmlns="` + xmpp_ns_forms + `" type="result"><field var="FORM_TYPE" type="hidden"><value>` + xmpp_ns_upload + `</value></field><field var="max-file-size"><value>` + strconv.Itoa(media_maximum) + `</value></field></x>`
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_info+`"><identity category="server" type="im" name="Mochi"/><identity category="store" type="file" name="Files"/>`+features.String()+upload+`</query>`)
	case space == xmpp_ns_items && name == "query" && kind == "get":
		// The room service under the domain.
		service := "rooms." + strings.ToLower(s.get("to"))
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_items+`"><item jid="`+xmpp_escape(service)+`" name="Rooms"/></query>`)
	case space == xmpp_ns_ping && name == "ping" && kind == "get":
		return xmpp_result(s, "")
	case space == xmpp_ns_upload && name == "request" && kind == "get":
		return xmpp_upload_request(s)
	}
	return xmpp_failure(s, "service-unavailable", "cancel", "")
}

// xmpp_iq_mochi answers for Mochi's own sessions as a client: what they
// support, as their capabilities name, and pings.
func xmpp_iq_mochi(s *xmpp_stanza) *xmpp_stanza {
	space, name := s.child()
	switch {
	case space == xmpp_ns_info && name == "query" && s.get("type") == "get":
		var features strings.Builder
		for _, feature := range xmpp_features {
			features.WriteString(`<feature var="` + feature + `"/>`)
		}
		var request struct {
			Node string `xml:"query>node,attr"`
		}
		s.decode(&request)
		node := ""
		if request.Node != "" {
			node = ` node="` + xmpp_escape(request.Node) + `"`
		}
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_info+`"`+node+`><identity category="client" type="pc" name="Mochi"/>`+features.String()+`</query>`)
	case space == xmpp_ns_ping && name == "ping":
		return xmpp_result(s, "")
	}
	return xmpp_failure(s, "service-unavailable", "cancel", "")
}

// xmpp_iq_notices answers for the notices address: its name, and that it is
// the server's.
func xmpp_iq_notices(s *xmpp_stanza) *xmpp_stanza {
	space, name := s.child()
	if space == xmpp_ns_vcard && name == "vCard" && s.get("type") == "get" {
		return xmpp_result(s, `<vCard xmlns="vcard-temp"><FN>`+notices_name+`</FN></vCard>`)
	}
	return xmpp_iq_account(s, space, name)
}

// === vCards ===

type xmpp_card struct {
	text  string
	photo string
	until time.Time
}

// Each account's vCard for ten minutes, so presence carries its photo's hash
// without asking for the avatar each time.
var (
	xmpp_cards      = map[string]xmpp_card{}
	xmpp_cards_lock sync.Mutex
)

// xmpp_vcard is an account's vCard (XEP-0054) - the name it goes by and the
// user's avatar - and the avatar's hash, as presence names it (XEP-0153).
func xmpp_vcard(a *xmpp_account) (string, string) {
	xmpp_cards_lock.Lock()
	cached, found := xmpp_cards[a.id]
	xmpp_cards_lock.Unlock()
	if found && time.Now().Before(cached.until) {
		return cached.text, cached.photo
	}
	name := rooms_display(a.user, a.id)
	text := `<vCard xmlns="vcard-temp"><FN>` + xmpp_escape(name) + `</FN><NICKNAME>` + xmpp_escape(name) + `</NICKNAME>`
	photo := ""
	if avatar, _ := atproto_profile_media(a.user)["avatar"].(map[string]any); avatar != nil {
		data := repository_bytes(avatar["data"])
		kind, _ := avatar["type"].(string)
		if len(data) > 0 && strings.HasPrefix(kind, "image/") {
			text += `<PHOTO><TYPE>` + xmpp_escape(kind) + `</TYPE><BINVAL>` + base64.StdEncoding.EncodeToString(data) + `</BINVAL></PHOTO>`
			sum := sha1.Sum(data)
			photo = hex.EncodeToString(sum[:])
		}
	}
	text += `</vCard>`
	xmpp_cards_lock.Lock()
	xmpp_cards[a.id] = xmpp_card{text: text, photo: photo, until: time.Now().Add(10 * time.Minute)}
	xmpp_cards_lock.Unlock()
	return text, photo
}

// === What the apps send ===

// Messages each account's conversations last asked a read marker for, and
// the pauses due after the user stopped typing.
var (
	xmpp_markable      = map[string]string{}
	xmpp_pauses        = map[string]*time.Timer{}
	xmpp_markable_lock sync.Mutex
)

// xmpp_markable_set keeps the latest message of a conversation that asked to
// be marked as read.
func xmpp_markable_set(a *xmpp_account, conversation string, id string) {
	xmpp_markable_lock.Lock()
	xmpp_markable[a.id+" "+conversation] = id
	xmpp_markable_lock.Unlock()
}

// An xmpp_target is where a conversation of a hosted account goes: the person,
// or a room Mochi's own sessions are in, with the nick they go by there.
type xmpp_destination struct {
	to   jid.JID
	room bool
	nick string
}

// xmpp_hosted_target is where a conversation of a hosted account goes,
// refusing a person the user blocks.
func xmpp_hosted_target(a *xmpp_account, conversation string) (*xmpp_destination, error) {
	to, err := xmpp_target(conversation)
	if err != nil || to.Localpart() == "" {
		return nil, fmt.Errorf("invalid conversation")
	}
	if j := xmpp_joined_of(a, to.Bare().String()); j != nil && to.Resourcepart() == "" {
		return &xmpp_destination{to: to.Bare(), room: true, nick: j.nick}, nil
	}
	if to.Resourcepart() == "" || xmpp_joined_of(a, to.Bare().String()) == nil {
		to = to.Bare()
	}
	if xmpp_blocked(a.user, to) {
		return nil, fmt.Errorf("the person is blocked")
	}
	return &xmpp_destination{to: to}, nil
}

// kind is the type a conversation's messages go as.
func (t *xmpp_destination) kind() string {
	if t.room {
		return "groupchat"
	}
	return "chat"
}

// xmpp_hosted_route sends a message from Mochi's own sessions on an account:
// to a room, kept to know its echo by, or to a person, copied to the
// account's clients that take copies with the id the account's archive gives
// it, where it is one the archive keeps.
func xmpp_hosted_route(a *xmpp_account, t *xmpp_destination, s *xmpp_stanza) string {
	if t.room {
		xmpp_room_sent(a, s.get("id"))
		xmpp_route(s, a.mochi(), t.to)
		return ""
	}
	archive := ""
	var m xmpp_message
	if s.decode(&m) == nil && xmpp_archivable(s.get("type"), &m) {
		archive = uid()
	}
	xmpp_route(s, a.mochi(), t.to)
	if xmpp_carbon_eligible(s) {
		sent := s.copy()
		if archive != "" {
			xmpp_stanza_id(sent, a.address.String(), archive)
		}
		xmpp_carbons(a, sent, "sent")
	}
	return archive
}

// xmpp_hosted_send sends a message from Mochi's own sessions on an account:
// its text or a file by its address, a reply to another, or a correction of
// an earlier one. It answers the id the account's archive gave it, and the
// encrypted elements it went with where it was encrypted, which the archive
// gives the account's clients.
func xmpp_hosted_send(a *xmpp_account, conversation string, id string, body string, file string, reply string, correct string) (string, string, error) {
	t, err := xmpp_hosted_target(a, conversation)
	if err != nil {
		return "", "", err
	}
	// A conversation whose people have OMEMO devices is encrypted.
	sealed, err := omemo_seal_message(context.Background(), a.user, a.id, t.to, t.room, body, file)
	if err != nil {
		return "", "", err
	}
	s, err := xmpp_parse(xmpp_message_text(t.to.String(), t.kind(), id, body, file, reply, correct, sealed))
	if err != nil {
		return "", "", err
	}
	xmpp_markable_lock.Lock()
	key := a.id + " " + conversation
	if timer := xmpp_pauses[key]; timer != nil {
		timer.Stop()
		delete(xmpp_pauses, key)
	}
	xmpp_markable_lock.Unlock()
	if !t.room {
		xmpp_accepted_forget(a, t.to)
	}
	return xmpp_hosted_route(a, t, s), sealed, nil
}

// xmpp_object_create sends a message of a conversation from Mochi's own
// sessions: its text, perhaps a reply; a file by the address its upload
// answered; or a reaction to a message. The app's key is its id, so the same
// message is known again; the answer carries the id the account's archive
// gave it.
func xmpp_object_create(a *xmpp_account, object map[string]any, key string) (map[string]any, error) {
	conversation, _ := object["conversation"].(string)
	text, _ := object["content"].(string)
	reply, _ := object["reply"].(string)
	if emoji, found := object["reaction"].(string); found {
		message, _ := object["message"].(string)
		if message == "" {
			return nil, fmt.Errorf("a reaction needs its message")
		}
		if err := xmpp_hosted_react(a, conversation, message, emoji); err != nil {
			return nil, err
		}
		return map[string]any{"id": xmpp_reaction_prefix + message, "url": "", "data": map[string]any{}}, nil
	}
	file := ""
	if f, _ := object["file"].(map[string]any); f != nil {
		file, _ = f["url"].(string)
	}
	if text == "" && file == "" {
		return nil, fmt.Errorf("a message needs content or a file")
	}
	id := key
	if id == "" {
		id = uid()
	}
	archive, sealed, err := xmpp_hosted_send(a, conversation, id, text, file, reply, "")
	if err != nil {
		return nil, err
	}
	answer := map[string]any{"id": id, "archive": archive, "url": "", "data": map[string]any{}}
	if sealed != "" {
		answer["encrypted"], answer["payload"] = true, sealed
	}
	return answer, nil
}

// xmpp_reaction_prefix begins the id a reaction is known by: the message it
// is to, since a person's reactions to a message are replaced as a whole.
const xmpp_reaction_prefix = "reaction|"

// xmpp_reactions is a message reacting to another (XEP-0444): the emoji the
// user reacts with, none withdrawing their reaction.
func xmpp_reactions(message string, emoji string) string {
	inner := `<reactions xmlns="urn:xmpp:reactions:0" id="` + xmpp_escape(message) + `">`
	if emoji != "" {
		inner += `<reaction>` + xmpp_escape(emoji) + `</reaction>`
	}
	return inner + `</reactions><store xmlns="urn:xmpp:hints"/>`
}

// xmpp_hosted_react sends the user's reaction to a message, replacing the one
// before; an empty one withdraws it. In a room the message is named by the
// id the room gave it.
func xmpp_hosted_react(a *xmpp_account, conversation string, message string, emoji string) error {
	t, err := xmpp_hosted_target(a, conversation)
	if err != nil {
		return err
	}
	xmpp_hosted_route(a, t, xmpp_make("message", xmpp_reactions(message, emoji), "type", t.kind(), "id", uid()))
	return nil
}

// xmpp_hosted_retract withdraws a message the account sent, or its reaction
// to one.
func xmpp_hosted_retract(a *xmpp_account, conversation string, id string) error {
	if message, found := strings.CutPrefix(id, xmpp_reaction_prefix); found {
		return xmpp_hosted_react(a, conversation, message, "")
	}
	t, err := xmpp_hosted_target(a, conversation)
	if err != nil {
		return err
	}
	xmpp_hosted_route(a, t, xmpp_make("message", `<retract xmlns="urn:xmpp:message-retract:1" id="`+xmpp_escape(id)+`"/><store xmlns="urn:xmpp:hints"/>`, "type", t.kind(), "id", uid()))
	return nil
}

// xmpp_hosted_typing tells a conversation the user is typing, or stopped,
// and that they stopped once xmpp_pause passes with no more.
func xmpp_hosted_typing(a *xmpp_account, conversation string, typing bool) error {
	t, err := xmpp_hosted_target(a, conversation)
	if err != nil {
		return err
	}
	state := "paused"
	if typing {
		state = "composing"
	}
	key := a.id + " " + conversation
	xmpp_markable_lock.Lock()
	if timer := xmpp_pauses[key]; timer != nil {
		timer.Stop()
		delete(xmpp_pauses, key)
	}
	if typing {
		xmpp_pauses[key] = time.AfterFunc(xmpp_pause, func() {
			xmpp_markable_lock.Lock()
			delete(xmpp_pauses, key)
			xmpp_markable_lock.Unlock()
			xmpp_hosted_route(a, t, xmpp_make("message", `<paused xmlns="http://jabber.org/protocol/chatstates"/><no-store xmlns="urn:xmpp:hints"/>`, "type", t.kind(), "id", uid()))
		})
	}
	xmpp_markable_lock.Unlock()
	xmpp_hosted_route(a, t, xmpp_make("message", `<`+state+` xmlns="http://jabber.org/protocol/chatstates"/><no-store xmlns="urn:xmpp:hints"/>`, "type", t.kind(), "id", uid()))
	return nil
}

// xmpp_hosted_read tells a person the user has read their messages up to
// one, where it is the one they asked to be told of. The account's other
// clients are told by their copy.
func xmpp_hosted_read(a *xmpp_account, conversation string, id string) error {
	t, err := xmpp_hosted_target(a, conversation)
	if err != nil {
		return err
	}
	if t.room {
		return nil
	}
	xmpp_markable_lock.Lock()
	marked := xmpp_markable[a.id+" "+conversation]
	xmpp_markable_lock.Unlock()
	if marked == "" || marked != id {
		return nil
	}
	xmpp_hosted_route(a, t, xmpp_make("message", `<displayed xmlns="urn:xmpp:chat-markers:0" id="`+xmpp_escape(id)+`"/><store xmlns="urn:xmpp:hints"/>`, "type", "chat", "id", uid()))
	return nil
}

// xmpp_hosted_share keeps whether the account shows the user's presence, and
// shows its contacts what that now makes it.
func xmpp_hosted_share(a *xmpp_account, shared bool) {
	db := db_user(a.user, "user")
	if row := account_hosted_get(a.user, a.id); row != nil {
		data, _ := row["data"].(map[string]any)
		if data == nil {
			data = map[string]any{}
		}
		if shared {
			delete(data, "hidden")
		} else {
			data["hidden"] = true
		}
		db.account_set(a.id, map[string]any{"data": json_encode(data)})
	}
	xmpp_mochi_update(a.user.UID)
}

// xmpp_priority reads a presence's priority.
func xmpp_priority(text string) int {
	value, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil {
		return 0
	}
	return max(-128, min(127, value))
}
