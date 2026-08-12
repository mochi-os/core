// Mochi server: Outbound sender authorization
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// sender_thread builds a Starlark thread routed to the given entity, which is
// what /<app>/<entity>/... produces (web.go sets route_entity from the URL
// segment). Pass "" for a class-level action, which sets no route entity.
func sender_thread(route string) *sl.Thread {
	t := &sl.Thread{Name: "test"}
	if route != "" {
		t.SetLocal("route_entity", route)
	}
	return t
}

// setup_sender_test creates a users.db holding two users, each owning one
// entity, so a send can be attempted as one's own entity and as the other's.
func setup_sender_test(t *testing.T) (*User, *User, string, string, func()) {
	cleanup := create_test_users_db(t)

	db := db_open("db/users.db")
	db.exec("create table entities (id text not null primary key, private text not null, fingerprint text not null, user text not null, parent text not null default '', class text not null, name text not null, privacy text not null default 'public', data text not null default '', published integer not null default 0)")

	mine, theirs := "entity-mine", "entity-theirs"
	me := &User{UID: "user-me"}
	them := &User{UID: "user-them"}
	db.exec("insert into users (uid, username) values (?, ?)", me.UID, "me@example.com")
	db.exec("insert into users (uid, username) values (?, ?)", them.UID, "them@example.com")
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, 'k', 'fp1', ?, 'wiki', 'Mine')", mine, me.UID)
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, 'k', 'fp2', ?, 'wiki', 'Theirs')", theirs, them.UID)

	return me, them, mine, theirs, cleanup
}

// TestSenderCheckAllowsOwnedEntity — the ordinary case. Ownership is what
// authorizes a send, and it must keep working whether or not the request was
// routed to that entity.
func TestSenderCheckAllowsOwnedEntity(t *testing.T) {
	me, _, mine, _, cleanup := setup_sender_test(t)
	defer cleanup()

	for name, thread := range map[string]*sl.Thread{
		"routed to it":             sender_thread(mine),
		"class action":             sender_thread(""),
		"routed to another entity": sender_thread("entity-theirs"),
	} {
		t.Run(name, func(t *testing.T) {
			allowed, err := sender_check(thread, me, mine, "test")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !allowed {
				t.Error("a user was refused permission to send as their own entity")
			}
		})
	}
}

// TestSenderCheckRefusesRoutedEntity — the defect. Reaching
// /<app>/<entity>/... used to be accepted as licence to speak as that entity:
// the entity segment is resolved globally with no check that the caller owns
// it, so any user could pair any app with any entity and have claim_sign
// produce a genuine signature by that entity's key.
func TestSenderCheckRefusesRoutedEntity(t *testing.T) {
	me, _, _, theirs, cleanup := setup_sender_test(t)
	defer cleanup()

	allowed, err := sender_check(sender_thread(theirs), me, theirs, "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("routing to an entity authorized speaking as it, so a signature by another user's key could be obtained from a URL")
	}
}

// TestSenderCheckRefusesUnroutedForeignEntity — the same refusal without any
// routing involved, so the test above is known to be testing the route path
// rather than an unrelated blanket refusal.
func TestSenderCheckRefusesUnroutedForeignEntity(t *testing.T) {
	me, _, _, theirs, cleanup := setup_sender_test(t)
	defer cleanup()

	allowed, err := sender_check(sender_thread(""), me, theirs, "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("a user sent as an entity they do not own")
	}
}

// TestSenderCheckRefusesDeletedEntity — the case that was actually relying on
// the old grant: wikis' unsubscribe deleted its replica entity and then sent a
// farewell as it. Ownership cannot be established for a row that no longer
// exists, and the entity's key is gone too, so such a send could never have
// been signed - it only appeared to work.
func TestSenderCheckRefusesDeletedEntity(t *testing.T) {
	me, _, mine, _, cleanup := setup_sender_test(t)
	defer cleanup()

	db := db_open("db/users.db")
	db.exec("delete from entities where id=?", mine)

	allowed, err := sender_check(sender_thread(mine), me, mine, "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("a send was authorized as an entity that has been deleted")
	}
}

// TestSenderCheckRefusesUnknownEntity — an id that never existed must not be
// admitted by a missing row reading as anything other than "not yours".
func TestSenderCheckRefusesUnknownEntity(t *testing.T) {
	me, _, _, _, cleanup := setup_sender_test(t)
	defer cleanup()

	allowed, err := sender_check(sender_thread("entity-nonexistent"), me, "entity-nonexistent", "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("an entity that does not exist was accepted as a sender")
	}
}
