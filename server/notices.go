// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Notices
//
// Notifications to a user's linked Matrix or XMPP account (the federation
// plan, phases 24 and 25): a message from the server's notices account on the
// server's own domain, else on the domain of the user's hosted account on that
// network. On Matrix it is @notices, a stand-in standing for no member, which
// speaks for the server and takes no invitations, in a room made with the
// account the first time and kept; someone who leaves the room, or refuses it,
// is told nothing more there. On XMPP it is notices@, a message to the
// account's address.
//
// What the user's linked sessions hear of the notices is not handed to the
// apps: the notification is already the user's, and a chat message about it
// would be notified in turn.

package main

import (
	"context"
	"strings"

	"github.com/matrix-org/gomatrixserverlib/spec"
	"mellium.im/xmpp/jid"
)

// notices_name is the notices account's name on Matrix.
const notices_name = "Mochi"

// notices_tables makes the table of each linked account's notices room.
func notices_tables(db *DB) {
	db.exec("create table if not exists notices (user text not null, account text not null, room text not null, created integer not null, primary key (user, account))")
}

// notices_sender is the notices account on a domain, made on first use.
func notices_sender(domain string) string {
	id := "@notices:" + domain
	federation_database().exec("insert or ignore into standins (id, domain, entity, name, user, created) values (?, ?, '', ?, '', ?)", id, domain, notices_name, now())
	return id
}

// notices_send tells a user's linked Matrix account a notification, and
// answers whether it went.
func notices_send(user *User, account string, address string, title string, body string, link string) bool {
	domain := bridge_domain(user)
	if domain == "" || !strings.HasPrefix(address, "@") {
		return false
	}
	sender := notices_sender(domain)
	ctx, cancel := context.WithTimeout(context.Background(), federation_timeout)
	defer cancel()
	db := federation_database()
	room := ""
	if row, _ := db.row("select room from notices where user=? and account=?", user.UID, account); row != nil {
		room = row_string(row, "room")
		switch rooms_membership(room, address) {
		case spec.Join, spec.Invite:
		default:
			// Left, or refused: nothing more is said there.
			return false
		}
	}
	if room == "" {
		made, err := rooms_create(ctx, sender, []string{address}, notices_name, true)
		if err != nil {
			info("Matrix notices to %s not sent: %v", address, err)
			return false
		}
		room = made
		db.exec("insert into notices (user, account, room, created) values (?, ?, ?, ?) on conflict(user, account) do update set room=excluded.room, created=excluded.created", user.UID, account, room, now())
	}
	if _, err := rooms_message(ctx, "notices", sender, room, "m.room.message", map[string]any{"msgtype": "m.text", "body": notices_text(title, body, link)}, ""); err != nil {
		info("Matrix notices to %s not sent: %v", address, err)
		return false
	}
	return true
}

// notices_text is a notification as a message: its title, its body, and the
// link to it, made absolute.
func notices_text(title string, body string, link string) string {
	text := title
	if body != "" {
		text += "\n" + body
	}
	if strings.HasPrefix(link, "/") {
		if base := server_public_url(); base != "" {
			link = strings.TrimSuffix(base, "/") + link
		} else {
			link = ""
		}
	}
	if link != "" {
		text += "\n" + link
	}
	return text
}

// notices_xmpp tells a user's linked XMPP account a notification, from
// notices@ on the server's domain, else on the domain of the user's hosted
// XMPP account, and answers whether it went.
func notices_xmpp(user *User, address string, title string, body string, link string) bool {
	if !network_enabled("xmpp") {
		return false
	}
	domain := server_domain()
	if domain == "" {
		row, _ := networks_database().row("select domain from actors where user=? and network='xmpp' and deleted=0 order by created limit 1", user.UID)
		domain = row_string(row, "domain")
	}
	to, err := xmpp_address(address)
	if domain == "" || err != nil {
		return false
	}
	from, err := jid.New(xmpp_notices_name, domain, "")
	if err != nil {
		return false
	}
	s := xmpp_make("message", `<body>`+xmpp_escape(notices_text(title, body, link))+`</body><nick xmlns="http://jabber.org/protocol/nick">`+notices_name+`</nick>`, "type", "chat", "id", uid())
	xmpp_route(s, from, to)
	return true
}

// notices_heard reports whether an event a linked session heard is this
// server's own notices: from its notices account on Matrix or XMPP, or in a
// notices room.
func notices_heard(event map[string]any) bool {
	from, _ := event["from"].(string)
	conversation, _ := event["conversation"].(string)
	for _, value := range []string{from, conversation} {
		if domain, found := strings.CutPrefix(value, "@notices:"); found && federation_local(spec.ServerName(strings.ToLower(domain))) {
			return true
		}
		if rest, found := strings.CutPrefix(value, "xmpp:"+xmpp_notices_name+"@"); found {
			domain, _, _ := strings.Cut(rest, "/")
			if xmpp_local(domain) {
				return true
			}
		}
	}
	if strings.HasPrefix(conversation, "!") {
		found, _ := federation_database().exists("select 1 from notices where room=?", conversation)
		return found
	}
	return false
}
