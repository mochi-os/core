// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Mochi server: Matrix notices
//
// Notifications to a user's linked Matrix account (the federation plan, phase
// 24): a message from the server's notices account, @notices on the server's
// own domain, else on the domain of the user's hosted Matrix account, in a
// room made with the account the first time and kept. The notices account is
// a stand-in standing for no member: it speaks for the server, and takes no
// invitations. Someone who leaves the room, or refuses it, is told nothing
// more there.

package main

import (
	"context"
	"strings"

	"github.com/matrix-org/gomatrixserverlib/spec"
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
	if _, err := rooms_message(ctx, "notices", sender, room, "m.room.message", map[string]any{"msgtype": "m.text", "body": text}, ""); err != nil {
		info("Matrix notices to %s not sent: %v", address, err)
		return false
	}
	return true
}
