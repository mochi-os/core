// Mochi server: interests belong to the caller, not to the storage account.
//
// principal_storage answers with the route owner for every request to a domain
// route carrying a context. That is right for the databases a hosted site
// serves from and wrong for a per-person profile: it made a signed-in visitor
// read and write the site owner's interests using their own grant.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"

	sl "go.starlark.net/starlark"
)

// interests_grant gives the thread's app the interests permissions for each
// user, so the assertions below turn on which account is resolved rather than
// on a missing grant.
func interests_grant(t *testing.T, thread *sl.Thread, users ...*User) {
	t.Helper()
	app := principal_app(thread)
	if app == nil {
		t.Fatal("the thread carries no app, so require_permission would refuse for the wrong reason")
	}
	for _, u := range users {
		db := db_user(u, "user")
		db.permissions_setup()
		db.permissions_upsert(app.id, "interests/read", "", 1)
		db.permissions_upsert(app.id, "interests/write", "", 1)
	}
}

// interests_weight reads a user's stored weight for qid, or 0 for no row.
func interests_weight(u *User, qid string) int {
	return db_user(u, "user").integer("select weight from interests where qid=?", qid)
}

// TestInterestsResolveTheCaller pins the resolution both APIs sit on. The
// end-to-end assertions below would pass for the wrong reason if
// principal_storage ever stopped substituting the owner, so this measures the
// divergence directly.
func TestInterestsResolveTheCaller(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	owner := create_permission_test_user(t, "owneruser")
	visitor := create_permission_test_user(t, "visitoruser")

	// The substitution this is all about: on a contexted route the storage
	// account is the owner even though the visitor is signed in.
	storage, err := principal_storage(entity_get_thread(owner, visitor, "site"))
	if err != nil || storage == nil || storage.UID != owner.UID {
		t.Fatalf("principal_storage on a contexted route = %v, want the owner - without that "+
			"substitution nothing below proves anything", storage)
	}

	// A write always belongs to whoever is signed in, on either route.
	if got := interests_writer(entity_get_thread(owner, visitor, "site")); got == nil || got.UID != visitor.UID {
		t.Errorf("a visitor's write on a DOMAIN ROUTE resolved %v, want the visitor - "+
			"resolving the owner spends the visitor's grant on the owner's profile", got)
	}
	if got := interests_writer(entity_get_thread(owner, visitor, "")); got == nil || got.UID != visitor.UID {
		t.Errorf("a visitor's write on a plain route resolved %v, want the visitor", got)
	}
	if got := interests_writer(entity_get_thread(owner, owner, "site")); got == nil || got.UID != owner.UID {
		t.Errorf("the owner's own write resolved %v, want the owner", got)
	}
	// An anonymous write belongs to nobody rather than to the route owner.
	if got := interests_writer(entity_get_thread(owner, nil, "site")); got != nil {
		t.Errorf("an anonymous write resolved %v, want nobody - no account asked for it", got)
	}

	// A read by someone other than the storage account is refused rather than
	// answered with that account's profile.
	if got := interests_reader(entity_get_thread(owner, visitor, "site")); got != nil {
		t.Errorf("a visitor's read on a DOMAIN ROUTE resolved %v, want nobody - the summary and "+
			"the ranking are the owner's personal profile", got)
	}
	// The two cases that must survive: the account's own read, and the
	// anonymous read that renders a hosted site.
	if got := interests_reader(entity_get_thread(owner, owner, "site")); got == nil || got.UID != owner.UID {
		t.Errorf("the owner's own read on a domain route resolved %v, want the owner", got)
	}
	if got := interests_reader(entity_get_thread(owner, nil, "site")); got == nil || got.UID != owner.UID {
		t.Errorf("an anonymous read on a domain route resolved %v, want the route owner - "+
			"that is what renders the hosted site", got)
	}
	if got := interests_reader(entity_get_thread(owner, visitor, "")); got == nil || got.UID != visitor.UID {
		t.Errorf("a visitor's read on a plain route resolved %v, want the visitor", got)
	}
}

// TestInterestsAdjustWritesToTheCallersProfile drives the real API: a visitor
// browsing a hosted site makes the app call mochi.interests.adjust on their
// behalf, and the weight must land in their own database.
func TestInterestsAdjustWritesToTheCallersProfile(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	owner := create_permission_test_user(t, "owneruser")
	visitor := create_permission_test_user(t, "visitoruser")

	thread := entity_get_thread(owner, visitor, "site")
	interests_grant(t, thread, owner, visitor)

	adjust := sl.NewBuiltin("mochi.interests.adjust", api_interests_adjust)
	if _, err := api_interests_adjust(thread, adjust,
		sl.Tuple{sl.String("Q11660"), sl.MakeInt(7)}, nil); err != nil {
		t.Fatalf("adjust refused the visitor's own write: %v", err)
	}

	if got := interests_weight(visitor, "Q11660"); got != 7 {
		t.Errorf("the visitor's own weight is %d, want 7", got)
	}
	if got := interests_weight(owner, "Q11660"); got != 0 {
		t.Errorf("the site owner's weight is %d, want 0 - a visitor browsing a hosted site "+
			"must not poison the owner's personalisation", got)
	}
}

// TestInterestsTopRefusesAnotherAccountsProfile. list/top/bottom/summary all
// resolve the same way; top stands for them.
func TestInterestsTopRefusesAnotherAccountsProfile(t *testing.T) {
	setup_test_data_dir(t)
	defer cleanup_test_data_dir(t)

	owner := create_permission_test_user(t, "owneruser")
	visitor := create_permission_test_user(t, "visitoruser")

	thread := entity_get_thread(owner, visitor, "site")
	interests_grant(t, thread, owner, visitor)

	db_user(owner, "user").exec(
		"insert into interests (qid, weight, updated) values (?, ?, ?)", "Q11660", 90, now())

	top := sl.NewBuiltin("mochi.interests.top", api_interests_top)
	if _, err := api_interests_top(thread, top, sl.Tuple{sl.MakeInt(10)}, nil); err == nil {
		t.Error("a signed-in visitor to a hosted site read the site owner's interest profile")
	}

	// The owner's own read still works, so the refusal is about whose profile it
	// is and not about the domain route.
	own := entity_get_thread(owner, owner, "site")
	interests_grant(t, own, owner)
	if _, err := api_interests_top(own, top, sl.Tuple{sl.MakeInt(10)}, nil); err != nil {
		t.Errorf("the owner could not read their own interests on a domain route: %v", err)
	}
}
