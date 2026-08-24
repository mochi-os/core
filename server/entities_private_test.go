// Mochi server: mochi.entity.info does not describe a private local entity to a
// caller belonging to another user.
//
// mochi.remote.peer already refuses to confirm such an entity exists. info
// answers a different question about the same id and returned strictly more -
// name, class, parent, privacy, and the owner's own identity as creator - so
// the ping guard was bypassed by asking for the description instead of the
// reachability.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// entity_info_row calls mochi.entity.info and reports whether a row came back,
// plus the fields that carry the disclosure.
func entity_info_row(t *testing.T, thread *sl.Thread, id string) (bool, string, string) {
	t.Helper()
	value, err := api_entity_info(thread, sl.NewBuiltin("mochi.entity.info", api_entity_info), sl.Tuple{sl.String(id)}, nil)
	if err != nil {
		t.Fatalf("api_entity_info returned %v", err)
	}
	if value == sl.None {
		return false, "", ""
	}
	row, ok := value.(*sl.Dict)
	if !ok {
		t.Fatalf("api_entity_info returned %T, want a dict or None", value)
	}
	text := func(key string) string {
		v, found, _ := row.Get(sl.String(key))
		if !found {
			return ""
		}
		s, _ := v.(sl.String)
		return string(s)
	}
	return true, text("name"), text("creator")
}

// entities_private_setup builds a users.db holding one private and one public
// entity for 'owner', plus a person entity for 'owner' and one for 'other', and
// returns their ids.
func entities_private_setup(t *testing.T) (private string, public string, owner string, other string) {
	t.Helper()
	create_test_users_db(t)
	db := db_open("db/users.db")
	db.exec("create table if not exists entities (id text not null primary key, private text not null default '', fingerprint text not null default '', user text not null, parent text not null default '', class text not null default '', name text not null default '', privacy text not null default 'public', data text not null default '', published integer not null default 0)")

	private = strings.Repeat("a", 50)
	public = strings.Repeat("b", 50)
	owner = strings.Repeat("c", 50)
	other = strings.Repeat("d", 50)
	db.exec("insert into entities (id, user, class, name, privacy) values (?, 'owner', 'feed', 'Secret feed', 'private')", private)
	db.exec("insert into entities (id, user, class, name, privacy) values (?, 'owner', 'feed', 'Open feed', 'public')", public)
	db.exec("insert into entities (id, user, class, name, privacy) values (?, 'owner', 'person', 'Owner', 'public')", owner)
	db.exec("insert into entities (id, user, class, name, privacy) values (?, 'other', 'person', 'Other', 'public')", other)
	return
}

// entities_private_thread is entity_get_thread with the caller's identity set,
// which is what principal_caller hands the gate.
func entities_private_thread(identity string) *sl.Thread {
	owner := &User{UID: "owner"}
	user := &User{UID: "caller"}
	if identity != "" {
		user.Identity = &Entity{ID: identity}
	}
	return entity_get_thread(owner, user, "")
}

func TestEntityInfoHidesAPrivateEntityFromAnotherUser(t *testing.T) {
	private, _, _, other := entities_private_setup(t)

	found, name, creator := entity_info_row(t, entities_private_thread(other), private)
	if found {
		t.Errorf("mochi.entity.info described a private entity to an unrelated caller: name=%q creator=%q", name, creator)
	}
}

func TestEntityInfoStillDescribesAPrivateEntityToItsOwner(t *testing.T) {
	private, _, owner, _ := entities_private_setup(t)

	found, name, _ := entity_info_row(t, entities_private_thread(owner), private)
	if !found {
		t.Fatal("a private entity must still be described to its own owner; the gate is about foreign callers")
	}
	if name != "Secret feed" {
		t.Errorf("owner read name %q, want %q", name, "Secret feed")
	}
}

func TestEntityInfoStillDescribesAPublicEntityToAnyone(t *testing.T) {
	_, public, _, other := entities_private_setup(t)

	found, name, _ := entity_info_row(t, entities_private_thread(other), public)
	if !found {
		t.Fatal("a public entity must be described to any caller; only privacy gates this")
	}
	if name != "Open feed" {
		t.Errorf("read name %q, want %q", name, "Open feed")
	}
}

func TestEntityInfoHidesAPrivateEntityFromAnAnonymousCaller(t *testing.T) {
	private, _, _, _ := entities_private_setup(t)

	// No identity: entity_private_local_foreign treats an empty caller as
	// foreign, the same reading mochi.remote.peer applies.
	found, name, _ := entity_info_row(t, entities_private_thread(""), private)
	if found {
		t.Errorf("mochi.entity.info described a private entity to an anonymous caller: name=%q", name)
	}
}

// The refusal must be the same answer as a genuinely absent entity, or info is
// still an existence oracle for exactly the ids the guard is meant to protect.
func TestEntityInfoRefusalIsIndistinguishableFromAbsent(t *testing.T) {
	private, _, _, other := entities_private_setup(t)
	absent := strings.Repeat("f", 50)

	refused, _, _ := entity_info_row(t, entities_private_thread(other), private)
	missing, _, _ := entity_info_row(t, entities_private_thread(other), absent)
	if refused != missing {
		t.Errorf("private-entity refusal returned found=%v but an absent id returned found=%v; the two must be identical", refused, missing)
	}
}

// The guard reads principal_caller's identity, so a caller with no Identity is
// treated as anonymous and refused. That is correct for a genuinely anonymous
// request, but it would be a regression for app event handlers: events.go sets
// the "user" local from user_owning_entity, and a handler routinely calls
// mochi.entity.info about its own user's private entity.
//
// What keeps that working is that both user constructors refuse to return a
// user whose identity is missing, rather than returning one with a nil
// Identity. If that ever relaxes, this guard starts answering None inside every
// event handler, so the invariant is pinned here rather than assumed.
func TestUserConstructorsNeverYieldANilIdentity(t *testing.T) {
	body, err := os.ReadFile("users.go")
	if err != nil {
		t.Fatalf("read users.go: %v", err)
	}
	source := string(body)

	for _, name := range []string{"func user_by_uid(", "func user_owning_entity("} {
		start := strings.Index(source, name)
		if start < 0 {
			t.Errorf("%s no longer exists in users.go; the identity invariant this guard relies on cannot be checked", name)
			continue
		}
		fn := source[start:]
		if end := strings.Index(fn, "\n}\n"); end > 0 {
			fn = fn[:end]
		}
		if !strings.Contains(fn, "u.Identity = u.identity()") {
			t.Errorf("%s no longer populates u.Identity; mochi.entity.info would treat every event handler as anonymous", name)
		}
		if !strings.Contains(fn, "if u.Identity == nil {") {
			t.Errorf("%s no longer refuses a user with no identity; mochi.entity.info would hide private entities from their own owner's handlers", name)
		}
	}
}
