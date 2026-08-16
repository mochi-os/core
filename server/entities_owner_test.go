// Mochi server: mochi.entity.get resolves the caller, not the database owner.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// entity_get_thread builds a thread as an action would: an owner whose data is
// being served, an authenticated caller, and optionally a domain route carrying
// a context (which is what makes db_user_for_thread substitute the owner).
func entity_get_thread(owner *User, user *User, context string) *sl.Thread {
	t := &sl.Thread{Name: "test"}
	t.SetLocal("owner", owner)
	if user != nil {
		t.SetLocal("user", user)
	}

	// mochi.entity.get is behind entity/read, so the thread needs a calling app
	// holding it - this test is about which USER the call resolves for, and the
	// permission must not be what decides the outcome.
	app := create_external_app("profile")
	t.SetLocal("app", app)
	for _, u := range []*User{owner, user} {
		if u == nil {
			continue
		}
		db := db_user(u, "user")
		db.permissions_setup()
		db.permissions_upsert(app.id, "entity/read", "", 1)
	}
	if context != "" {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/people/x/-/profile/set", nil)
		t.SetLocal("action", &Action{
			web:    c,
			domain: &DomainInfo{route: &DomainRouteInfo{context: context}},
		})
	}
	return t
}

func entity_get_ids(t *testing.T, thread *sl.Thread, id string) []string {
	t.Helper()
	value, err := api_entity_get(thread, sl.NewBuiltin("mochi.entity.get", api_entity_get), sl.Tuple{sl.String(id)}, nil)
	if err != nil {
		t.Fatalf("api_entity_get returned %v", err)
	}
	// sl_encode yields a Tuple for a row list; accept either sequence form.
	list, ok := value.(sl.Indexable)
	if !ok {
		t.Fatalf("api_entity_get returned %T, want an indexable sequence", value)
	}
	var out []string
	for i := 0; i < list.Len(); i++ {
		row, ok := list.Index(i).(*sl.Dict)
		if !ok {
			continue
		}
		if v, found, _ := row.Get(sl.String("id")); found {
			out = append(out, string(v.(sl.String)))
		}
	}
	return out
}

// TestEntityGetResolvesCallerNotOwner is the regression guard for the class
// where six apps read "does the caller own this" off a function that answers
// "which database do I open". On a domain route carrying a context the two
// diverge, and every logged-in visitor was reported as owning the owner's
// entities - which gated profile writes, wiki comment deletion and publisher's
// access check.
func TestEntityGetResolvesCallerNotOwner(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	owner := create_permission_test_user(t, "owneruser")
	visitor := create_permission_test_user(t, "visitoruser")

	db := db_open("db/users.db")
	db.exec("create table entities (id text primary key, private text, fingerprint text, user text not null default '', parent text default '', class text, name text, privacy text default 'public', data text default '', published integer default 0)")
	db.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, ?, ?, ?, ?, ?)",
		"12L4xgroenaPKg4XZxhW6i5VAsEjvVu9oWZifUmbvjRMDCe3SuC", "priv", "ownerfp01", owner.UID, "person", "Owner person")

	// The owner, on an ordinary route, still sees their own entity.
	if got := entity_get_ids(t, entity_get_thread(owner, owner, ""), "12L4xgroenaPKg4XZxhW6i5VAsEjvVu9oWZifUmbvjRMDCe3SuC"); len(got) != 1 {
		t.Errorf("owner on a plain route got %v, want their own entity", got)
	}

	// The owner, on a contexted domain route, still sees it - the fix must not
	// take ownership away from the person who actually owns the thing.
	if got := entity_get_ids(t, entity_get_thread(owner, owner, "site"), "12L4xgroenaPKg4XZxhW6i5VAsEjvVu9oWZifUmbvjRMDCe3SuC"); len(got) != 1 {
		t.Errorf("owner on a domain route got %v, want their own entity", got)
	}

	// A different logged-in visitor must NOT be told they own it, on either
	// route. Before the fix the domain-routed case returned the owner's entity.
	if got := entity_get_ids(t, entity_get_thread(owner, visitor, ""), "12L4xgroenaPKg4XZxhW6i5VAsEjvVu9oWZifUmbvjRMDCe3SuC"); len(got) != 0 {
		t.Errorf("visitor on a plain route got %v, want nothing", got)
	}
	if got := entity_get_ids(t, entity_get_thread(owner, visitor, "site"), "12L4xgroenaPKg4XZxhW6i5VAsEjvVu9oWZifUmbvjRMDCe3SuC"); len(got) != 0 {
		t.Errorf("visitor on a DOMAIN ROUTE got %v, want nothing - this is the escalation", got)
	}

	// An anonymous caller owns nothing rather than owning the owner's entities.
	if got := entity_get_ids(t, entity_get_thread(owner, nil, ""), "12L4xgroenaPKg4XZxhW6i5VAsEjvVu9oWZifUmbvjRMDCe3SuC"); len(got) != 0 {
		t.Errorf("anonymous caller got %v, want nothing", got)
	}

	// Fingerprint addressing keeps working: the person routes are linked by
	// fingerprint, and an ownership check that stopped matching them would 403
	// every such request.
	if got := entity_get_ids(t, entity_get_thread(owner, owner, ""), "ownerfp01"); len(got) != 1 {
		t.Errorf("owner by fingerprint got %v, want their own entity", got)
	}

	// Guard against the whole test passing for the wrong reason. The visitor
	// cases above are only meaningful because the storage selector genuinely
	// does resolve to someone else here - that divergence IS the bug. If
	// db_user_for_thread ever stopped substituting, these assertions would still
	// pass while proving nothing, so pin the thing they depend on.
	storage, err := db_user_for_thread(entity_get_thread(owner, visitor, "site"))
	if err != nil || storage == nil || storage.UID != owner.UID {
		t.Errorf("db_user_for_thread on a contexted route = %v, want the owner - "+
			"without that substitution the visitor assertions prove nothing", storage)
	}
	if plain, err := db_user_for_thread(entity_get_thread(owner, visitor, "")); err != nil || plain == nil || plain.UID != visitor.UID {
		t.Errorf("db_user_for_thread on a plain route = %v, want the visitor", plain)
	}
}
