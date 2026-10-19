// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: the XMPP room service
//
// Rooms (XEP-0045) at rooms.<domain> for each domain this server answers for
// on XMPP (the federation plan, phase 26): a fixed name with no setting, its
// certificate asked for as the domain's own (domains.go). A room is made by an
// account of this server's joining it - locked until its owner configures it
// or takes it as it is - or by chat, for a conversation of a hosted account
// with several people and for a group with people from XMPP (relays.go). Its
// owners, admins, members and outcasts, its configuration and its history are
// kept in xmpp.db; who is in it is kept in memory, each occupant by their nick
// with their sessions and their role.
//
// A room is persistent or goes once empty, open or members only, moderated or
// not, shows its occupants' addresses to everyone or to moderators alone, and
// is listed or hidden. Its messages are given the room's own ids (XEP-0359)
// and kept, a new occupant given the latest of them, and its archive is
// answered to its occupants and members (XEP-0313). An occupant asking whether
// they are still in it is answered (XEP-0410). A Mochi member of a relayed
// group is an occupant with no address, present for as long as the room is
// relayed.

package main

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mellium.im/xmpp/jid"
)

const (
	xmpp_ns_muc        = "http://jabber.org/protocol/muc"
	xmpp_ns_muc_user   = "http://jabber.org/protocol/muc#user"
	xmpp_ns_muc_owner  = "http://jabber.org/protocol/muc#owner"
	xmpp_ns_muc_admin  = "http://jabber.org/protocol/muc#admin"
	xmpp_ns_muc_unique = "http://jabber.org/protocol/muc#unique"
)

// The most messages a room keeps, and the most a new occupant is given.
const (
	conference_kept    = 5000
	conference_history = 20
)

// xmpp_room_base is the domain a room service's host serves, or "" for a host
// that is not one: rooms.<domain>, for a domain this server answers for on
// XMPP, where the host is not itself a domain with accounts.
func xmpp_room_base(host string) string {
	host = strings.ToLower(host)
	base, found := strings.CutPrefix(host, "rooms.")
	if !found || base == "" || !network_enabled("xmpp") {
		return ""
	}
	if !xmpp_served(base) || xmpp_served(host) {
		return ""
	}
	return base
}

// === Reaching the service ===

// How a room service's host is looked up: its addresses, and its server's
// SRV records. Variables so tests can answer for names of their own.
var (
	xmpp_lookup_host = net.DefaultResolver.LookupHost
	xmpp_lookup_srv  = net.DefaultResolver.LookupSRV
)

// The room services' states, for five minutes.
var (
	xmpp_reached      = map[string]xmpp_reach{}
	xmpp_reached_lock sync.Mutex
)

type xmpp_reach struct {
	reason string
	until  time.Time
}

// xmpp_rooms_state is each XMPP domain's room service as the Status page
// shows it: working where its host leads to this server, else unavailable
// with the reason.
func xmpp_rooms_state(ctx context.Context) []any {
	domains := map[string]bool{}
	if domain := server_domain(); domain != "" && xmpp_served(domain) {
		domains[domain] = true
	}
	rows, _ := networks_database().rows("select distinct domain from actors where network='xmpp' and deleted=0")
	for _, row := range rows {
		domains[strings.ToLower(row_string(row, "domain"))] = true
	}
	names := make([]string, 0, len(domains))
	for domain := range domains {
		names = append(names, domain)
	}
	sort.Strings(names)
	out := []any{}
	for _, domain := range names {
		reason := xmpp_room_reach(ctx, domain)
		state := "working"
		if reason != "" {
			state = "unavailable"
		}
		out = append(out, map[string]any{"host": "rooms." + domain, "state": state, "reason": reason})
	}
	return out
}

// xmpp_room_reach says why a domain's room service cannot be reached, or ""
// where it can: its host, found as other servers find it - by its SRV
// records, else by its name - must lead to an address the domain itself is
// found at, the domain's being known to reach this server.
func xmpp_room_reach(ctx context.Context, domain string) string {
	xmpp_reached_lock.Lock()
	cached, found := xmpp_reached[domain]
	xmpp_reached_lock.Unlock()
	if found && time.Now().Before(cached.until) {
		return cached.reason
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	addresses := func(name string) map[string]bool {
		out := map[string]bool{}
		hosts := []string{name}
		if _, records, err := xmpp_lookup_srv(ctx, "xmpp-server", "tcp", name); err == nil && len(records) > 0 {
			hosts = nil
			for _, r := range records {
				hosts = append(hosts, strings.TrimSuffix(r.Target, "."))
			}
		}
		for _, host := range hosts {
			found, _ := xmpp_lookup_host(ctx, host)
			for _, address := range found {
				out[address] = true
			}
		}
		return out
	}
	ours := addresses(domain)
	reason := "dns"
	for address := range addresses("rooms." + domain) {
		if ours[address] {
			reason = ""
		}
	}
	xmpp_reached_lock.Lock()
	xmpp_reached[domain] = xmpp_reach{reason: reason, until: time.Now().Add(5 * time.Minute)}
	xmpp_reached_lock.Unlock()
	return reason
}

// === Rooms ===

// A conference_config is how a room is set up.
type conference_config struct {
	name        string
	description string
	subject     string
	subjector   string
	persistent  bool
	members     bool
	public      bool
	moderated   bool
	open        bool
	invite      bool
	whois       string
	history     int
	locked      bool
}

// A conference is a room: its address and configuration, the affiliations
// and nicks its people have, who is in it, and the user whose server relays
// it for a group, if any.
type conference struct {
	address      string
	lock         sync.Mutex
	config       conference_config
	affiliations map[string]string
	nicks        map[string]string
	occupants    map[string]*conference_occupant
	relay        string
	created      bool
}

// A conference_occupant is someone in a room: their nick, their address
// (empty for a Mochi member of a relayed group, whom the member names), each
// of their sessions with the presence it shows, and their role.
type conference_occupant struct {
	nick     string
	bare     string
	member   string
	sessions map[string]string
	latest   string
	role     string
}

var (
	conferences      = map[string]*conference{}
	conferences_lock sync.Mutex
)

// conference_of is a room by its address, loaded on first use, or nil.
func conference_of(address string) *conference {
	address = strings.ToLower(address)
	conferences_lock.Lock()
	defer conferences_lock.Unlock()
	if c := conferences[address]; c != nil {
		return c
	}
	db := xmpp_database()
	row, _ := db.row("select * from rooms where address=?", address)
	if row == nil {
		return nil
	}
	c := &conference{address: address, affiliations: map[string]string{}, nicks: map[string]string{}, occupants: map[string]*conference_occupant{}}
	c.config = conference_config{
		name:        row_string(row, "name"),
		description: row_string(row, "description"),
		subject:     row_string(row, "subject"),
		subjector:   row_string(row, "subjector"),
		persistent:  row_int(row, "persistent") == 1,
		members:     row_int(row, "members") == 1,
		public:      row_int(row, "public") == 1,
		moderated:   row_int(row, "moderated") == 1,
		open:        row_int(row, "open") == 1,
		invite:      row_int(row, "invite") == 1,
		whois:       row_string(row, "whois"),
		history:     int(row_int(row, "history")),
		locked:      row_int(row, "locked") == 1,
	}
	rows, _ := db.rows("select address, affiliation, nick from affiliations where room=?", address)
	for _, r := range rows {
		who := row_string(r, "address")
		c.affiliations[who] = row_string(r, "affiliation")
		if nick := row_string(r, "nick"); nick != "" {
			c.nicks[who] = nick
		}
		// A Mochi member of a relayed group is in the room for as long as it
		// is relayed.
		if member, found := strings.CutPrefix(who, "mochi:"); found {
			c.occupants[row_string(r, "nick")] = &conference_occupant{nick: row_string(r, "nick"), member: member, sessions: map[string]string{}, role: "participant"}
		}
	}
	if b := bridge_of("xmpp:" + address); b != nil {
		c.relay = b.user
	}
	conferences[address] = c
	return c
}

// save keeps the room's configuration.
func (c *conference) save() {
	flag := func(value bool) int {
		if value {
			return 1
		}
		return 0
	}
	g := c.config
	xmpp_database().exec("insert into rooms (address, name, description, subject, subjector, persistent, members, public, moderated, open, invite, whois, history, locked, created, updated) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on conflict(address) do update set name=excluded.name, description=excluded.description, subject=excluded.subject, subjector=excluded.subjector, persistent=excluded.persistent, members=excluded.members, public=excluded.public, moderated=excluded.moderated, open=excluded.open, invite=excluded.invite, whois=excluded.whois, history=excluded.history, locked=excluded.locked, updated=excluded.updated", c.address, g.name, g.description, g.subject, g.subjector, flag(g.persistent), flag(g.members), flag(g.public), flag(g.moderated), flag(g.open), flag(g.invite), g.whois, g.history, flag(g.locked), now(), now())
}

// affiliate gives someone an affiliation with the room, or "none" to take it
// away, keeping the nick they reserve.
func (c *conference) affiliate(who string, affiliation string, nick string) {
	db := xmpp_database()
	if affiliation == "none" || affiliation == "" {
		delete(c.affiliations, who)
		delete(c.nicks, who)
		db.exec("delete from affiliations where room=? and address=?", c.address, who)
		return
	}
	c.affiliations[who] = affiliation
	if nick != "" {
		c.nicks[who] = nick
	}
	db.exec("insert into affiliations (room, address, affiliation, nick) values (?, ?, ?, ?) on conflict(room, address) do update set affiliation=excluded.affiliation, nick=case when excluded.nick='' then nick else excluded.nick end", c.address, who, affiliation, nick)
}

// affiliation is someone's affiliation with the room.
func (c *conference) affiliation(bare string) string {
	if a := c.affiliations[strings.ToLower(bare)]; a != "" {
		return a
	}
	return "none"
}

// conference_rank orders affiliations, outcasts lowest; 0 is none of them.
func conference_rank(affiliation string) int {
	switch affiliation {
	case "owner":
		return 5
	case "admin":
		return 4
	case "member":
		return 3
	case "none":
		return 2
	case "outcast":
		return 1
	}
	return 0
}

// conference_roles are the roles an occupant may be given; none puts them out.
var conference_roles = []string{"moderator", "participant", "visitor", "none"}

// role is the role an affiliation brings on joining.
func (c *conference) role(affiliation string) string {
	switch {
	case affiliation == "owner" || affiliation == "admin":
		return "moderator"
	case c.config.moderated && affiliation == "none":
		return "visitor"
	}
	return "participant"
}

// conference_open makes a room an account of this server's joins where none
// is: locked until its owner configures it, the account its owner.
func conference_open(address string, by jid.JID) *conference {
	if xmpp_account_of(by.Bare()) == nil {
		return nil
	}
	room, err := jid.Parse(address)
	if err != nil || room.Localpart() == "" || room.Resourcepart() != "" {
		return nil
	}
	c := &conference{address: strings.ToLower(address), affiliations: map[string]string{}, nicks: map[string]string{}, occupants: map[string]*conference_occupant{}, created: true}
	c.config = conference_config{name: room.Localpart(), public: true, whois: "moderators", history: conference_history, locked: true}
	conferences_lock.Lock()
	if existing := conferences[c.address]; existing != nil {
		conferences_lock.Unlock()
		return existing
	}
	conferences[c.address] = c
	conferences_lock.Unlock()
	c.save()
	c.affiliate(by.Bare().String(), "owner", "")
	return c
}

// conference_make makes a room for a conversation of a hosted account or a
// relayed group: named, persistent, members only, each address shown to all,
// the account its owner where there is one. Its people are made members by
// their invitations.
func conference_make(address string, name string, owner jid.JID) *conference {
	c := &conference{address: strings.ToLower(address), affiliations: map[string]string{}, nicks: map[string]string{}, occupants: map[string]*conference_occupant{}}
	c.config = conference_config{name: name, persistent: true, members: true, whois: "anyone", history: conference_history}
	conferences_lock.Lock()
	conferences[c.address] = c
	conferences_lock.Unlock()
	c.lock.Lock()
	defer c.lock.Unlock()
	c.save()
	if owner.Localpart() != "" {
		c.affiliate(owner.Bare().String(), "owner", "")
	}
	return c
}

// conference_invite has a room invite someone on another's behalf, making
// them a member of a room only members may join.
func conference_invite(address string, inviter jid.JID, person jid.JID, reason string) {
	c := conference_of(address)
	if c == nil {
		return
	}
	var out conference_outbox
	c.lock.Lock()
	c.invite_send(inviter.Bare().String(), person, reason, &out)
	c.lock.Unlock()
	out.flush()
}

// forget drops a room that has gone: its configuration, affiliations and
// history.
func (c *conference) forget() {
	db := xmpp_database()
	db.exec("delete from rooms where address=?", c.address)
	db.exec("delete from affiliations where room=?", c.address)
	db.exec("delete from chatlog where room=?", c.address)
	conferences_lock.Lock()
	if conferences[c.address] == c {
		delete(conferences, c.address)
	}
	conferences_lock.Unlock()
}

// === What a room sends ===

// A conference_outbox is what a room sends, routed once the room's lock is
// let go, and what the apps relaying it are told.
type conference_outbox struct {
	stanzas []conference_send
	relay   string
	room    string
	events  []map[string]any
}

type conference_send struct {
	s    *xmpp_stanza
	from jid.JID
	to   jid.JID
}

// add queues a stanza from an address to another.
func (o *conference_outbox) add(s *xmpp_stanza, from string, to string) {
	f, err := jid.Parse(from)
	t, err2 := jid.Parse(to)
	if err == nil && err2 == nil {
		o.stanzas = append(o.stanzas, conference_send{s: s, from: f, to: t})
	}
}

// flush routes what was queued and tells the apps relaying the room.
func (o *conference_outbox) flush() {
	for _, send := range o.stanzas {
		xmpp_route(send.s, send.from, send.to)
	}
	if o.relay != "" && len(o.events) > 0 {
		if user := user_by_uid(o.relay); user != nil {
			room, events := o.room, o.events
			xmpp_queue(user, func() { bridge_inbound(user, room, events) })
		}
	}
}

// occupant_address is an occupant's address in the room.
func (c *conference) occupant_address(nick string) string {
	return c.address + "/" + nick
}

// session_of is the occupant one of whose sessions an address is.
func (c *conference) session_of(from jid.JID) *conference_occupant {
	for _, o := range c.occupants {
		if _, found := o.sessions[from.String()]; found {
			return o
		}
	}
	return nil
}

// moderator reports whether an occupant is a moderator.
func (o *conference_occupant) moderator() bool {
	return o != nil && o.role == "moderator"
}

// item is the muc#user item describing an occupant to someone: their
// affiliation and role, and their address where the room shows it to them.
func (c *conference) item(o *conference_occupant, to *conference_occupant, role string) string {
	affiliation := "member"
	if o.bare != "" {
		affiliation = c.affiliation(o.bare)
	}
	text := `<item affiliation="` + affiliation + `" role="` + role + `"`
	if o.latest != "" && (c.config.whois == "anyone" || to.moderator() || to == o) {
		text += ` jid="` + xmpp_escape(o.latest) + `"`
	}
	return text + `/>`
}

// presence is an occupant's presence as one occupant is shown it, with the
// status codes given.
func (c *conference) presence(o *conference_occupant, to *conference_occupant, kind string, codes ...string) *xmpp_stanza {
	inner := ""
	if kind == "" {
		inner = o.sessions[o.latest]
	}
	role := o.role
	if kind == "unavailable" {
		role = "none"
	}
	x := `<x xmlns="` + xmpp_ns_muc_user + `">` + c.item(o, to, role)
	for _, code := range codes {
		x += `<status code="` + code + `"/>`
	}
	return xmpp_make("presence", inner+x+`</x>`, "type", kind)
}

// broadcast sends an occupant's presence to everyone in the room, with the
// codes given, the occupant themselves told too with 110.
func (c *conference) broadcast(o *conference_occupant, kind string, out *conference_outbox, codes ...string) {
	for _, other := range c.occupants {
		for session := range other.sessions {
			these := codes
			if other == o {
				these = append(slices.Clone(codes), "110")
			}
			out.add(c.presence(o, other, kind, these...), c.occupant_address(o.nick), session)
		}
	}
}

// statuses are the codes saying what the room is to someone joining it.
func (c *conference) statuses() []string {
	codes := []string{"170"}
	if c.config.whois == "anyone" {
		codes = append(codes, "100")
	}
	return codes
}

// === Presence ===

// xmpp_conference_deliver takes a stanza for the room service: the service's
// own, or a room's, made where an account here joins a room that is not.
func xmpp_conference_deliver(s *xmpp_stanza, from jid.JID, to jid.JID) {
	s.space = xmpp_ns_client
	if to.Localpart() == "" {
		if s.name == "iq" {
			xmpp_answer(s, conference_service_iq(s, to))
		}
		return
	}
	c := conference_of(to.Bare().String())
	if c == nil && s.name == "presence" && to.Resourcepart() != "" && s.get("type") == "" {
		c = conference_open(to.Bare().String(), from)
	}
	if c == nil {
		if s.name != "presence" || s.get("type") == "" {
			xmpp_answer(s, xmpp_failure(s, "item-not-found", "cancel", ""))
		}
		return
	}
	out := &conference_outbox{relay: c.relay, room: "xmpp:" + c.address}
	c.lock.Lock()
	switch s.name {
	case "presence":
		c.presence_in(s, from, to, out)
	case "message":
		c.message_in(s, from, to, out)
	case "iq":
		c.iq_in(s, from, to, out)
	}
	// A room that does not persist goes once the last has left.
	gone := !c.config.persistent && len(c.occupants) == 0 && !c.created
	c.created = false
	c.lock.Unlock()
	if gone {
		c.forget()
	}
	out.flush()
}

// refuse answers a stanza with an error, from the address it went to.
func (c *conference) refuse(s *xmpp_stanza, from jid.JID, to jid.JID, condition string, kind string, out *conference_outbox) {
	if reply := xmpp_failure(s, condition, kind, ""); reply != nil {
		out.add(reply, to.String(), from.String())
	}
}

// presence_in takes presence for the room: joining, changing one's presence
// or nick, leaving, or an error saying a session is gone.
func (c *conference) presence_in(s *xmpp_stanza, from jid.JID, to jid.JID, out *conference_outbox) {
	o := c.session_of(from)
	nick := to.Resourcepart()
	switch s.get("type") {
	case "":
		switch {
		case nick == "":
			c.refuse(s, from, to, "jid-malformed", "modify", out)
		case o == nil:
			c.join(s, from, nick, out)
		case o.nick != nick:
			c.rename(o, from, nick, out)
		default:
			o.sessions[from.String()] = conference_shown(s)
			o.latest = from.String()
			c.broadcast(o, "", out)
		}
	case "unavailable":
		if o != nil {
			c.depart(o, from.String(), out)
		}
	case "error":
		if o != nil {
			c.depart(o, from.String(), out)
		}
	}
}

// conference_shown is what a presence shows, without what it says to the
// room.
func conference_shown(s *xmpp_stanza) string {
	shown := s.copy()
	shown.strip(func(child *xmpp_child) bool {
		return child.space == xmpp_ns_muc || child.space == xmpp_ns_muc_user
	})
	return shown.inner
}

// join takes someone joining the room under a nick: refused where the room
// is locked to them, they are outcast, it is for members and they are not
// one, or the nick is someone else's. They are told who is in the room, the
// room is told of them, and they are given its history and subject.
func (c *conference) join(s *xmpp_stanza, from jid.JID, nick string, out *conference_outbox) {
	bare := from.Bare().String()
	affiliation := c.affiliation(bare)
	to, _ := jid.Parse(c.occupant_address(nick))
	switch {
	case c.config.locked && affiliation != "owner":
		c.refuse(s, from, to, "item-not-found", "cancel", out)
		return
	case affiliation == "outcast":
		c.refuse(s, from, to, "forbidden", "auth", out)
		return
	case c.config.members && conference_rank(affiliation) < conference_rank("member"):
		c.refuse(s, from, to, "registration-required", "auth", out)
		return
	case len(nick) > 200:
		c.refuse(s, from, to, "jid-malformed", "modify", out)
		return
	}
	existing := c.occupants[nick]
	if existing != nil && existing.bare != bare {
		c.refuse(s, from, to, "conflict", "cancel", out)
		return
	}
	for who, reserved := range c.nicks {
		if reserved == nick && who != bare {
			c.refuse(s, from, to, "conflict", "cancel", out)
			return
		}
	}
	o := existing
	if o == nil {
		o = &conference_occupant{nick: nick, bare: bare, sessions: map[string]string{}, role: c.role(affiliation)}
	}
	o.sessions[from.String()] = conference_shown(s)
	o.latest = from.String()
	viewer := o
	for _, other := range c.occupants {
		if other != o {
			out.add(c.presence(other, viewer, ""), c.occupant_address(other.nick), from.String())
		}
	}
	codes := c.statuses()
	if c.config.locked {
		codes = append(codes, "201")
	}
	if existing == nil {
		c.occupants[nick] = o
		for _, other := range c.occupants {
			if other == o {
				continue
			}
			for session := range other.sessions {
				out.add(c.presence(o, other, ""), c.occupant_address(nick), session)
			}
		}
	}
	out.add(c.presence(o, o, "", append(codes, "110")...), c.occupant_address(nick), from.String())
	c.history_send(s, from.String(), out)
	subject := xmpp_make("message", `<subject>`+xmpp_escape(c.config.subject)+`</subject>`, "type", "groupchat", "id", uid())
	sender := c.address
	if c.config.subjector != "" {
		sender = c.occupant_address(c.config.subjector)
	}
	out.add(subject, sender, from.String())
}

// history_send gives someone joining the room's latest messages, as many as
// they asked for, or since a moment.
func (c *conference) history_send(s *xmpp_stanza, to string, out *conference_outbox) {
	var request struct {
		History *struct {
			Stanzas *int    `xml:"maxstanzas,attr"`
			Seconds *int    `xml:"seconds,attr"`
			Since   *string `xml:"since,attr"`
			Chars   *int    `xml:"maxchars,attr"`
		} `xml:"http://jabber.org/protocol/muc x>history"`
	}
	s.decode(&request)
	limit := c.config.history
	since := int64(0)
	if h := request.History; h != nil {
		if h.Stanzas != nil {
			limit = min(limit, max(*h.Stanzas, 0))
		}
		if h.Chars != nil && *h.Chars == 0 {
			limit = 0
		}
		if h.Seconds != nil {
			since = now() - int64(max(*h.Seconds, 0))
		}
		if h.Since != nil {
			if stamp, err := time.Parse(time.RFC3339, *h.Since); err == nil {
				since = max(since, stamp.Unix())
			}
		}
	}
	if limit <= 0 {
		return
	}
	rows, _ := xmpp_database().rows("select nick, stanza, created from chatlog where room=? and created>=? order by id desc limit ?", c.address, since, limit)
	for i := len(rows) - 1; i >= 0; i-- {
		message, err := xmpp_parse(row_string(rows[i], "stanza"))
		if err != nil {
			continue
		}
		message.inner += `<delay xmlns="` + xmpp_ns_delay + `" from="` + xmpp_escape(c.address) + `" stamp="` + time.Unix(row_int(rows[i], "created"), 0).UTC().Format(time.RFC3339) + `"/>`
		out.add(message, c.occupant_address(row_string(rows[i], "nick")), to)
	}
}

// rename takes an occupant changing their nick: refused where it is someone
// else's, or where they are in the room in more than one session.
func (c *conference) rename(o *conference_occupant, from jid.JID, nick string, out *conference_outbox) {
	to, _ := jid.Parse(c.occupant_address(nick))
	s := xmpp_make("presence", "", "from", from.String(), "to", to.String())
	taken := c.occupants[nick] != nil
	for who, reserved := range c.nicks {
		taken = taken || (reserved == nick && who != o.bare)
	}
	if taken || len(o.sessions) > 1 || len(nick) > 200 {
		c.refuse(s, from, to, "conflict", "cancel", out)
		return
	}
	for _, other := range c.occupants {
		for session := range other.sessions {
			p := c.presence(o, other, "unavailable", "303")
			p.inner = strings.Replace(p.inner, `<item `, `<item nick="`+xmpp_escape(nick)+`" `, 1)
			if other == o {
				p.inner = strings.Replace(p.inner, `</x>`, `<status code="110"/></x>`, 1)
			}
			out.add(p, c.occupant_address(o.nick), session)
		}
	}
	delete(c.occupants, o.nick)
	o.nick = nick
	c.occupants[nick] = o
	c.broadcast(o, "", out)
}

// depart takes a session leaving the room; the occupant leaves with their
// last.
func (c *conference) depart(o *conference_occupant, session string, out *conference_outbox, codes ...string) {
	if len(o.sessions) > 1 {
		delete(o.sessions, session)
		out.add(c.presence(o, o, "unavailable", append(codes, "110")...), c.occupant_address(o.nick), session)
		if o.latest == session {
			for other := range o.sessions {
				o.latest = other
			}
		}
		return
	}
	c.broadcast(o, "unavailable", out, codes...)
	delete(c.occupants, o.nick)
}

// remove puts an occupant out of the room for a reason the codes give: put
// out (307), banned (301), no longer a member (321), the room for members
// only (322), or its end (332).
func (c *conference) remove(o *conference_occupant, out *conference_outbox, codes ...string) {
	c.broadcast(o, "unavailable", out, codes...)
	delete(c.occupants, o.nick)
}

// === Messages ===

// message_in takes a message for the room: one to everyone, the subject, an
// invitation, a private message to one occupant, or an error saying a
// session is gone.
func (c *conference) message_in(s *xmpp_stanza, from jid.JID, to jid.JID, out *conference_outbox) {
	o := c.session_of(from)
	kind := s.get("type")
	if kind == "error" {
		if o != nil {
			c.depart(o, from.String(), out)
		}
		return
	}
	if nick := to.Resourcepart(); nick != "" {
		target := c.occupants[nick]
		switch {
		case o == nil:
			c.refuse(s, from, to, "not-acceptable", "modify", out)
		case kind == "groupchat":
			c.refuse(s, from, to, "bad-request", "modify", out)
		case target == nil:
			c.refuse(s, from, to, "item-not-found", "cancel", out)
		case target.bare == "":
			c.refuse(s, from, to, "service-unavailable", "cancel", out)
		default:
			private := s.copy()
			private.inner += `<x xmlns="` + xmpp_ns_muc_user + `"/>`
			for session := range target.sessions {
				out.add(private.copy(), c.occupant_address(o.nick), session)
			}
		}
		return
	}
	if kind != "groupchat" {
		c.invitation_in(s, from, to, out)
		return
	}
	switch {
	case o == nil:
		c.refuse(s, from, to, "not-acceptable", "modify", out)
		return
	case o.role == "visitor":
		c.refuse(s, from, to, "forbidden", "auth", out)
		return
	}
	var m xmpp_message
	if s.decode(&m) != nil {
		return
	}
	if m.Subject != nil && strings.TrimSpace(m.Body) == "" {
		if !o.moderator() && !c.config.open {
			c.refuse(s, from, to, "forbidden", "auth", out)
			return
		}
		c.config.subject = strings.TrimSpace(*m.Subject)
		if len(c.config.subject) > 1000 {
			c.config.subject = c.config.subject[:1000]
		}
		c.config.subjector = o.nick
		c.save()
		for _, other := range c.occupants {
			for session := range other.sessions {
				out.add(xmpp_make("message", `<subject>`+xmpp_escape(c.config.subject)+`</subject>`, "type", "groupchat", "id", uid()), c.occupant_address(o.nick), session)
			}
		}
		return
	}
	c.say(o, s, &m, out)
}

// say sends an occupant's message to everyone in the room, themselves too,
// with the id the room gives it, keeping it where it is one the archive
// keeps; a relayed room's apps are told what someone from XMPP said.
func (c *conference) say(o *conference_occupant, s *xmpp_stanza, m *xmpp_message, out *conference_outbox) string {
	archive := uid()
	message := s.copy()
	message.set("from", "")
	message.set("to", "")
	xmpp_stanza_id(message, c.address, archive)
	if conference_keeps(m) {
		db := xmpp_database()
		db.exec("insert into chatlog (room, archive, nick, sender, stanza, created) values (?, ?, ?, ?, ?, ?)", c.address, archive, o.nick, o.bare, message.text(xmpp_ns_client), now())
		db.exec("delete from chatlog where room=? and id<=(select id from chatlog where room=? order by id desc limit 1 offset ?)", c.address, c.address, conference_kept)
	}
	for _, other := range c.occupants {
		for session := range other.sessions {
			out.add(message.copy(), c.occupant_address(o.nick), session)
		}
	}
	if c.relay != "" && o.bare != "" {
		sender, _ := jid.Parse(c.occupant_address(o.nick))
		for _, event := range xmpp_events(m, nil, sender, "xmpp:"+c.address, "xmpp:"+o.bare, o.nick, archive, false) {
			event["from"] = "xmpp:" + o.bare
			out.events = append(out.events, event)
		}
	}
	return archive
}

// conference_keeps reports whether the room keeps a message: one with
// content, or one changing another - a correction, a withdrawal, a reaction.
func conference_keeps(m *xmpp_message) bool {
	return strings.TrimSpace(m.Body) != "" || m.encrypted() || len(m.Links) > 0 || m.Replace != nil || m.Retract != nil || m.Fasten != nil || m.Reactions != nil
}

// invitation_in takes a message to the room itself: an invitation an
// occupant sends through it, made a member where the room is for members, or
// someone declining one, passed to who invited them.
func (c *conference) invitation_in(s *xmpp_stanza, from jid.JID, to jid.JID, out *conference_outbox) {
	var request struct {
		User *struct {
			Invites []struct {
				To     string `xml:"to,attr"`
				Reason string `xml:"reason"`
			} `xml:"invite"`
			Decline *struct {
				To     string `xml:"to,attr"`
				Reason string `xml:"reason"`
			} `xml:"decline"`
		} `xml:"http://jabber.org/protocol/muc#user x"`
	}
	if s.decode(&request) != nil || request.User == nil {
		return
	}
	if d := request.User.Decline; d != nil {
		inviter, err := jid.Parse(d.To)
		if err != nil {
			return
		}
		out.add(xmpp_make("message", `<x xmlns="`+xmpp_ns_muc_user+`"><decline from="`+xmpp_escape(from.Bare().String())+`"><reason>`+xmpp_escape(d.Reason)+`</reason></decline></x>`, "id", uid()), c.address, inviter.String())
		return
	}
	o := c.session_of(from)
	if o == nil {
		c.refuse(s, from, to, "not-acceptable", "modify", out)
		return
	}
	affiliation := c.affiliation(o.bare)
	if c.config.members && conference_rank(affiliation) < conference_rank("admin") && !c.config.invite {
		c.refuse(s, from, to, "forbidden", "auth", out)
		return
	}
	for _, invite := range request.User.Invites {
		person, err := jid.Parse(invite.To)
		if err != nil {
			continue
		}
		c.invite_send(o.bare, person, invite.Reason, out)
	}
}

// invite_send invites someone to the room on another's behalf, making them a
// member where it is for members.
func (c *conference) invite_send(inviter string, person jid.JID, reason string, out *conference_outbox) {
	bare := person.Bare().String()
	if c.config.members && conference_rank(c.affiliation(bare)) < conference_rank("member") {
		c.affiliate(bare, "member", "")
	}
	inner := `<x xmlns="` + xmpp_ns_muc_user + `"><invite from="` + xmpp_escape(inviter) + `">`
	if reason != "" {
		inner += `<reason>` + xmpp_escape(reason) + `</reason>`
	}
	inner += `</invite></x><x xmlns="jabber:x:conference" jid="` + xmpp_escape(c.address) + `"/>`
	out.add(xmpp_make("message", inner, "id", uid()), c.address, bare)
}

// === Requests ===

// iq_in takes a request for the room, or for one of its occupants, which
// goes to them as from the asker's nick: but an occupant asking after
// themselves is answered (XEP-0410), and a Mochi member's card is the room's
// to give.
func (c *conference) iq_in(s *xmpp_stanza, from jid.JID, to jid.JID, out *conference_outbox) {
	kind := s.get("type")
	o := c.session_of(from)
	space, name := s.child()
	if nick := to.Resourcepart(); nick != "" {
		target := c.occupants[nick]
		if kind == "result" || kind == "error" {
			// An occupant's answer to another's request.
			if o != nil && target != nil {
				for session := range target.sessions {
					out.add(s.copy(), c.occupant_address(o.nick), session)
					break
				}
			}
			return
		}
		switch {
		case o == nil:
			c.refuse(s, from, to, "not-acceptable", "cancel", out)
		case target == nil:
			c.refuse(s, from, to, "item-not-found", "cancel", out)
		case target == o && space == xmpp_ns_ping:
			out.add(xmpp_result(s, ""), to.String(), from.String())
		case target.bare == "" && space == xmpp_ns_vcard && kind == "get":
			out.add(xmpp_result(s, `<vCard xmlns="vcard-temp"><FN>`+xmpp_escape(target.nick)+`</FN><NICKNAME>`+xmpp_escape(target.nick)+`</NICKNAME></vCard>`), to.String(), from.String())
		case target.bare == "":
			c.refuse(s, from, to, "service-unavailable", "cancel", out)
		default:
			for session := range target.sessions {
				out.add(s.copy(), c.occupant_address(o.nick), session)
				break
			}
		}
		return
	}
	if kind != "get" && kind != "set" {
		return
	}
	var reply *xmpp_stanza
	switch {
	case space == xmpp_ns_info && name == "query" && kind == "get":
		reply = c.info(s)
	case space == xmpp_ns_items && name == "query" && kind == "get":
		var items strings.Builder
		if c.config.public || o != nil {
			for _, other := range c.sorted() {
				items.WriteString(`<item jid="` + xmpp_escape(c.occupant_address(other.nick)) + `" name="` + xmpp_escape(other.nick) + `"/>`)
			}
		}
		reply = xmpp_result(s, `<query xmlns="`+xmpp_ns_items+`">`+items.String()+`</query>`)
	case space == xmpp_ns_muc_owner && name == "query":
		reply = c.owner_iq(s, from, kind, out)
	case space == xmpp_ns_muc_admin && name == "query":
		reply = c.admin_iq(s, from, o, kind, out)
	case space == xmpp_ns_archive && name == "query":
		reply = c.archive_iq(s, from, o, kind, out)
	case space == xmpp_ns_ping && name == "ping":
		reply = xmpp_result(s, "")
	case space == xmpp_ns_vcard && name == "vCard" && kind == "get":
		reply = xmpp_result(s, `<vCard xmlns="vcard-temp"/>`)
	default:
		reply = xmpp_failure(s, "service-unavailable", "cancel", "")
	}
	if reply != nil {
		out.add(reply, c.address, from.String())
	}
}

// sorted lists the occupants by nick.
func (c *conference) sorted() []*conference_occupant {
	list := make([]*conference_occupant, 0, len(c.occupants))
	for _, o := range c.occupants {
		list = append(list, o)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].nick < list[j].nick })
	return list
}

// info is what discovery shows of the room: its name and the features its
// configuration makes it, with its description, subject and how many are in
// it.
func (c *conference) info(s *xmpp_stanza) *xmpp_stanza {
	g := c.config
	pick := func(yes bool, a string, b string) string {
		if yes {
			return a
		}
		return b
	}
	features := []string{xmpp_ns_info, xmpp_ns_muc, xmpp_ns_archive, xmpp_ns_sid, "http://jabber.org/protocol/muc#stable_id", "http://jabber.org/protocol/muc#self-ping-optimization", "muc_unsecured",
		pick(g.persistent, "muc_persistent", "muc_temporary"), pick(g.members, "muc_membersonly", "muc_open"), pick(g.moderated, "muc_moderated", "muc_unmoderated"),
		pick(g.whois == "anyone", "muc_nonanonymous", "muc_semianonymous"), pick(g.public, "muc_public", "muc_hidden")}
	var text strings.Builder
	text.WriteString(`<query xmlns="` + xmpp_ns_info + `"><identity category="conference" type="text" name="` + xmpp_escape(g.name) + `"/>`)
	for _, feature := range features {
		text.WriteString(`<feature var="` + feature + `"/>`)
	}
	text.WriteString(`<x xmlns="` + xmpp_ns_forms + `" type="result"><field var="FORM_TYPE" type="hidden"><value>http://jabber.org/protocol/muc#roominfo</value></field>`)
	text.WriteString(`<field var="muc#roomconfig_roomname"><value>` + xmpp_escape(g.name) + `</value></field>`)
	text.WriteString(`<field var="muc#roominfo_description"><value>` + xmpp_escape(g.description) + `</value></field>`)
	text.WriteString(`<field var="muc#roominfo_subject"><value>` + xmpp_escape(g.subject) + `</value></field>`)
	text.WriteString(`<field var="muc#roominfo_occupants"><value>` + strconv.Itoa(len(c.occupants)) + `</value></field>`)
	text.WriteString(`</x></query>`)
	return xmpp_result(s, text.String())
}

// conference_service_iq answers a request to the room service itself:
// discovery, which lists the rooms it lists publicly, a name for a new room
// (muc#unique), and pings.
func conference_service_iq(s *xmpp_stanza, to jid.JID) *xmpp_stanza {
	kind := s.get("type")
	if kind != "get" && kind != "set" {
		return nil
	}
	space, name := s.child()
	service := strings.ToLower(to.Domainpart())
	switch {
	case space == xmpp_ns_info && name == "query" && kind == "get":
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_info+`"><identity category="conference" type="text" name="Rooms"/><feature var="`+xmpp_ns_info+`"/><feature var="`+xmpp_ns_items+`"/><feature var="`+xmpp_ns_muc+`"/><feature var="`+xmpp_ns_muc_unique+`"/></query>`)
	case space == xmpp_ns_items && name == "query" && kind == "get":
		rows, _ := xmpp_database().rows("select address, name from rooms where public=1 and persistent=1 and locked=0 and address like ? order by created", "%@"+service)
		var items strings.Builder
		for _, row := range rows {
			items.WriteString(`<item jid="` + xmpp_escape(row_string(row, "address")) + `" name="` + xmpp_escape(row_string(row, "name")) + `"/>`)
		}
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_items+`">`+items.String()+`</query>`)
	case space == xmpp_ns_muc_unique && name == "unique" && kind == "get":
		return xmpp_result(s, `<unique xmlns="`+xmpp_ns_muc_unique+`">`+strings.ToLower(uid())+"@"+xmpp_escape(service)+`</unique>`)
	case space == xmpp_ns_ping && name == "ping":
		return xmpp_result(s, "")
	}
	return xmpp_failure(s, "service-unavailable", "cancel", "")
}

// === Configuration ===

// owner_iq answers an owner configuring the room or ending it: the form, its
// answer - which a locked room is taken as it is by, or ended by cancelling -
// or the room's end.
func (c *conference) owner_iq(s *xmpp_stanza, from jid.JID, kind string, out *conference_outbox) *xmpp_stanza {
	if c.affiliation(from.Bare().String()) != "owner" {
		return xmpp_failure(s, "forbidden", "auth", "")
	}
	if kind == "get" {
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_muc_owner+`">`+c.form()+`</query>`)
	}
	var request struct {
		Query struct {
			Form *struct {
				Type   string `xml:"type,attr"`
				Fields []struct {
					Var    string   `xml:"var,attr"`
					Values []string `xml:"value"`
				} `xml:"field"`
			} `xml:"jabber:x:data x"`
			Destroy *struct {
				Reason string `xml:"reason"`
			} `xml:"destroy"`
		} `xml:"http://jabber.org/protocol/muc#owner query"`
	}
	if s.decode(&request) != nil {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	if d := request.Query.Destroy; d != nil {
		c.destroy(d.Reason, out)
		return xmpp_result(s, "")
	}
	form := request.Query.Form
	if form == nil {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	if form.Type == "cancel" {
		if c.config.locked {
			c.destroy("", out)
		}
		return xmpp_result(s, "")
	}
	if form.Type != "submit" {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	before := c.config
	for _, f := range form.Fields {
		value := ""
		if len(f.Values) > 0 {
			value = strings.TrimSpace(f.Values[0])
		}
		yes := value == "1" || value == "true"
		switch f.Var {
		case "muc#roomconfig_roomname":
			c.config.name = conference_cut(value, 200)
		case "muc#roomconfig_roomdesc":
			c.config.description = conference_cut(value, 1000)
		case "muc#roomconfig_persistentroom":
			c.config.persistent = yes
		case "muc#roomconfig_membersonly":
			c.config.members = yes
		case "muc#roomconfig_publicroom":
			c.config.public = yes
		case "muc#roomconfig_moderatedroom":
			c.config.moderated = yes
		case "muc#roomconfig_changesubject":
			c.config.open = yes
		case "muc#roomconfig_allowinvites":
			c.config.invite = yes
		case "muc#roomconfig_whois":
			if value == "anyone" || value == "moderators" {
				c.config.whois = value
			}
		case "muc#maxhistoryfetch", "muc#roomconfig_historylength":
			if n, err := strconv.Atoi(value); err == nil && n >= 0 {
				c.config.history = min(n, 200)
			}
		}
	}
	c.config.locked = false
	c.save()
	// A room become for members puts out those who are not.
	if c.config.members && !before.members {
		for _, o := range c.sorted() {
			if o.bare != "" && conference_rank(c.affiliation(o.bare)) < conference_rank("member") {
				c.remove(o, out, "322")
			}
		}
	}
	codes := `<status code="104"/>`
	if before.whois != c.config.whois {
		if c.config.whois == "anyone" {
			codes += `<status code="172"/>`
		} else {
			codes += `<status code="173"/>`
		}
	}
	for _, o := range c.occupants {
		for session := range o.sessions {
			out.add(xmpp_make("message", `<x xmlns="`+xmpp_ns_muc_user+`">`+codes+`</x>`, "type", "groupchat", "id", uid()), c.address, session)
		}
	}
	return xmpp_result(s, "")
}

// conference_cut shortens text to a number of bytes, whole characters only.
func conference_cut(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	cut := []rune(text[:limit])
	if len(cut) > 0 && cut[len(cut)-1] == '�' {
		cut = cut[:len(cut)-1]
	}
	return string(cut)
}

// form is the room's configuration as a form to fill.
func (c *conference) form() string {
	g := c.config
	flag := func(name string, label string, value bool) string {
		v := "0"
		if value {
			v = "1"
		}
		return `<field var="` + name + `" type="boolean" label="` + label + `"><value>` + v + `</value></field>`
	}
	text := `<x xmlns="` + xmpp_ns_forms + `" type="form"><field var="FORM_TYPE" type="hidden"><value>http://jabber.org/protocol/muc#roomconfig</value></field>`
	text += `<field var="muc#roomconfig_roomname" type="text-single" label="Name"><value>` + xmpp_escape(g.name) + `</value></field>`
	text += `<field var="muc#roomconfig_roomdesc" type="text-single" label="Description"><value>` + xmpp_escape(g.description) + `</value></field>`
	text += flag("muc#roomconfig_persistentroom", "Persistent", g.persistent)
	text += flag("muc#roomconfig_publicroom", "Listed", g.public)
	text += flag("muc#roomconfig_membersonly", "Members only", g.members)
	text += flag("muc#roomconfig_moderatedroom", "Moderated", g.moderated)
	text += flag("muc#roomconfig_changesubject", "Anyone may change the subject", g.open)
	text += flag("muc#roomconfig_allowinvites", "Members may invite", g.invite)
	text += `<field var="muc#roomconfig_whois" type="list-single" label="Addresses shown to"><value>` + g.whois + `</value><option label="Moderators"><value>moderators</value></option><option label="Anyone"><value>anyone</value></option></field>`
	text += `<field var="muc#maxhistoryfetch" type="text-single" label="Messages given on joining"><value>` + strconv.Itoa(g.history) + `</value></field>`
	return text + `</x>`
}

// destroy ends the room: everyone in it is told, and it goes.
func (c *conference) destroy(reason string, out *conference_outbox) {
	for _, o := range c.sorted() {
		for session := range o.sessions {
			x := `<x xmlns="` + xmpp_ns_muc_user + `"><item affiliation="none" role="none"/><destroy>`
			if reason != "" {
				x += `<reason>` + xmpp_escape(reason) + `</reason>`
			}
			x += `</destroy><status code="110"/></x>`
			out.add(xmpp_make("presence", x, "type", "unavailable"), c.occupant_address(o.nick), session)
		}
	}
	c.occupants = map[string]*conference_occupant{}
	c.config.persistent = false
	c.forget()
}

// === Affiliations and roles ===

// admin_iq answers lists of the room's people by affiliation or role, and
// changes to them: an affiliation to who may make it - owners any, admins
// those below them - and a role to moderators, over those below admin.
// Members of a room for members see its members, so they can write to all of
// them.
func (c *conference) admin_iq(s *xmpp_stanza, from jid.JID, o *conference_occupant, kind string, out *conference_outbox) *xmpp_stanza {
	var request struct {
		Items []struct {
			Affiliation string `xml:"affiliation,attr"`
			Role        string `xml:"role,attr"`
			JID         string `xml:"jid,attr"`
			Nick        string `xml:"nick,attr"`
			Reason      string `xml:"reason"`
		} `xml:"http://jabber.org/protocol/muc#admin query>item"`
	}
	if s.decode(&request) != nil || len(request.Items) == 0 {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	mine := c.affiliation(from.Bare().String())
	if kind == "get" {
		item := request.Items[0]
		var text strings.Builder
		switch {
		case item.Affiliation != "":
			allowed := conference_rank(mine) >= conference_rank("admin") || (c.config.members && conference_rank(mine) >= conference_rank("member") && item.Affiliation != "outcast")
			if !allowed {
				return xmpp_failure(s, "forbidden", "auth", "")
			}
			var who []string
			for address, affiliation := range c.affiliations {
				if affiliation == item.Affiliation && !strings.HasPrefix(address, "mochi:") {
					who = append(who, address)
				}
			}
			sort.Strings(who)
			for _, address := range who {
				text.WriteString(`<item affiliation="` + item.Affiliation + `" jid="` + xmpp_escape(address) + `"`)
				if nick := c.nicks[address]; nick != "" {
					text.WriteString(` nick="` + xmpp_escape(nick) + `"`)
				}
				text.WriteString(`/>`)
			}
		case item.Role != "":
			if !o.moderator() {
				return xmpp_failure(s, "forbidden", "auth", "")
			}
			for _, other := range c.sorted() {
				if other.role == item.Role {
					text.WriteString(c.item(other, o, other.role))
				}
			}
		default:
			return xmpp_failure(s, "bad-request", "modify", "")
		}
		return xmpp_result(s, `<query xmlns="`+xmpp_ns_muc_admin+`">`+text.String()+`</query>`)
	}
	for _, item := range request.Items {
		switch {
		case item.Affiliation != "":
			target, err := jid.Parse(item.JID)
			if err != nil && item.Nick != "" {
				if found := c.occupants[item.Nick]; found != nil && found.bare != "" {
					target, err = jid.Parse(found.bare)
				}
			}
			if err != nil || conference_rank(item.Affiliation) == 0 {
				return xmpp_failure(s, "bad-request", "modify", "")
			}
			bare := target.Bare().String()
			theirs := c.affiliation(bare)
			allowed := mine == "owner" || (mine == "admin" && conference_rank(theirs) < conference_rank("admin") && conference_rank(item.Affiliation) < conference_rank("admin"))
			if !allowed {
				return xmpp_failure(s, "not-allowed", "cancel", "")
			}
			if theirs == "owner" && item.Affiliation != "owner" && c.owners() == 1 {
				return xmpp_failure(s, "conflict", "cancel", "")
			}
			c.affiliate(bare, item.Affiliation, "")
			c.affiliated(bare, item.Affiliation, out)
		case item.Role != "":
			target := c.occupants[item.Nick]
			if target == nil {
				return xmpp_failure(s, "item-not-found", "cancel", "")
			}
			theirs := "member"
			if target.bare != "" {
				theirs = c.affiliation(target.bare)
			}
			allowed := o.moderator() && conference_rank(theirs) < conference_rank("admin") && (item.Role != "moderator" || conference_rank(mine) >= conference_rank("admin"))
			if !allowed || !slices.Contains(conference_roles, item.Role) {
				return xmpp_failure(s, "not-allowed", "cancel", "")
			}
			if item.Role == "none" {
				c.remove(target, out, "307")
				continue
			}
			target.role = item.Role
			c.broadcast(target, "", out)
		default:
			return xmpp_failure(s, "bad-request", "modify", "")
		}
	}
	return xmpp_result(s, "")
}

// owners counts the room's owners.
func (c *conference) owners() int {
	count := 0
	for _, affiliation := range c.affiliations {
		if affiliation == "owner" {
			count++
		}
	}
	return count
}

// affiliated carries a change of someone's affiliation to who is in the
// room: an outcast is put out, as is someone no longer a member of a room
// for members; anyone else in it is shown with their new affiliation and
// the role it brings.
func (c *conference) affiliated(bare string, affiliation string, out *conference_outbox) {
	for _, o := range c.sorted() {
		if o.bare != bare {
			continue
		}
		switch {
		case affiliation == "outcast":
			c.remove(o, out, "301")
		case c.config.members && conference_rank(affiliation) < conference_rank("member"):
			c.remove(o, out, "321")
		default:
			o.role = c.role(affiliation)
			c.broadcast(o, "", out)
		}
	}
}

// === The archive ===

// archive_iq answers a query of the room's archive, from someone in it or a
// member: each message sent to them, then the answer saying whether the page
// is the last.
func (c *conference) archive_iq(s *xmpp_stanza, from jid.JID, o *conference_occupant, kind string, out *conference_outbox) *xmpp_stanza {
	if kind == "get" {
		return xmpp_archive_form(s)
	}
	if o == nil && conference_rank(c.affiliation(from.Bare().String())) < conference_rank("member") {
		return xmpp_failure(s, "forbidden", "auth", "")
	}
	q, ok := xmpp_archive_query(s)
	if !ok {
		return xmpp_failure(s, "bad-request", "modify", "")
	}
	db := xmpp_database()
	where := "room=?"
	args := []any{c.address}
	if q.start > 0 {
		where += " and created>=?"
		args = append(args, q.start)
	}
	if q.end > 0 {
		where += " and created<=?"
		args = append(args, q.end)
	}
	for _, anchor := range []struct {
		id string
		op string
	}{{q.after, ">"}, {q.before, "<"}} {
		if anchor.id == "" {
			continue
		}
		row, _ := db.row("select id from chatlog where room=? and archive=?", c.address, anchor.id)
		if row == nil {
			return xmpp_failure(s, "item-not-found", "cancel", "")
		}
		where += " and id" + anchor.op + "?"
		args = append(args, row_int(row, "id"))
	}
	order := "asc"
	if q.before != "" || q.last {
		order = "desc"
	}
	rows, _ := db.rows("select archive, nick, stanza, created from chatlog where "+where+" order by id "+order+" limit ?", append(args, q.maximum+1)...)
	complete := len(rows) <= q.maximum
	if !complete {
		rows = rows[:q.maximum]
	}
	if order == "desc" {
		slices.Reverse(rows)
	}
	var items []xmpp_entry
	for _, row := range rows {
		message, err := xmpp_parse(row_string(row, "stanza"))
		if err != nil {
			continue
		}
		message.set("from", c.occupant_address(row_string(row, "nick")))
		items = append(items, xmpp_entry{archive: row_string(row, "archive"), created: row_int(row, "created"), text: message.text(xmpp_ns_client)})
	}
	for _, item := range items {
		out.add(xmpp_make("message", `<result xmlns="`+xmpp_ns_archive+`" queryid="`+xmpp_escape(q.id)+`" id="`+xmpp_escape(item.archive)+`"><forwarded xmlns="`+xmpp_ns_forward+`"><delay xmlns="`+xmpp_ns_delay+`" stamp="`+time.Unix(item.created, 0).UTC().Format(time.RFC3339)+`"/>`+item.text+`</forwarded></result>`), c.address, from.String())
	}
	return xmpp_result(s, xmpp_archive_fin(items, complete))
}

// === Mochi members of a relayed group ===

// seat puts a Mochi member in the room under a nick of their name, the same
// as before where they have one, and answers it; the room is told of them.
func (c *conference) seat(member string, name string, out *conference_outbox) string {
	key := "mochi:" + member
	if nick := c.nicks[key]; nick != "" && c.occupants[nick] != nil {
		return nick
	}
	nick := strings.TrimSpace(name)
	if nick == "" || len(nick) > 200 {
		nick = fingerprint(member)
	}
	if other := c.occupants[nick]; other != nil && other.member != member {
		nick = nick + " (" + fingerprint(member)[:4] + ")"
	}
	c.affiliate(key, "member", nick)
	o := &conference_occupant{nick: nick, member: member, sessions: map[string]string{}, role: "participant"}
	c.occupants[nick] = o
	for _, other := range c.occupants {
		for session := range other.sessions {
			out.add(c.presence(o, other, ""), c.occupant_address(nick), session)
		}
	}
	return nick
}

// seated is a Mochi member's occupant, or nil.
func (c *conference) seated(member string) *conference_occupant {
	if nick := c.nicks["mochi:"+member]; nick != "" {
		if o := c.occupants[nick]; o != nil && o.member == member {
			return o
		}
	}
	return nil
}

// unseat takes a Mochi member out of the room.
func (c *conference) unseat(member string, out *conference_outbox) {
	if o := c.seated(member); o != nil {
		c.broadcast(o, "unavailable", out)
		delete(c.occupants, o.nick)
	}
	c.affiliate("mochi:"+member, "none", "")
}

// speak sends a Mochi member's message to the room, as they are known in it:
// to everyone there from XMPP, with the id the room gives it, which is the
// message's own id too.
func (c *conference) speak(member string, inner string, out *conference_outbox) (string, error) {
	o := c.seated(member)
	if o == nil {
		return "", fmt.Errorf("the member is not in the room")
	}
	s := xmpp_make("message", inner, "type", "groupchat")
	var m xmpp_message
	if s.decode(&m) != nil {
		return "", fmt.Errorf("invalid message")
	}
	archive := uid()
	s.set("id", archive)
	message := s.copy()
	xmpp_stanza_id(message, c.address, archive)
	if conference_keeps(&m) {
		xmpp_database().exec("insert into chatlog (room, archive, nick, sender, stanza, created) values (?, ?, ?, ?, ?, ?)", c.address, archive, o.nick, "", message.text(xmpp_ns_client), now())
	}
	for _, other := range c.occupants {
		for session := range other.sessions {
			out.add(message.copy(), c.occupant_address(o.nick), session)
		}
	}
	return archive, nil
}
