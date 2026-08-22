// Mochi server: entity creation from an app is rate limited.
//
// entity_create mints a keypair, signs an announcement, writes two databases
// and floods the mesh - where every peer verifies and writes its own row - plus
// a durable queue.db row acked only on peers_sufficient(). One call is N remote
// writes.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// entity_limit_thread builds a thread whose app declares the "widget" class,
// so entity_class_allowed passes and the rate limit is what decides.
func entity_limit_thread(t *testing.T, uid string) *sl.Thread {
	t.Helper()
	user := &User{UID: uid, Username: uid + "@example.com"}
	db := db_open("db/users.db")
	db.exec("insert or replace into users (uid, username) values (?, ?)", user.UID, user.Username)

	// Classes live on the active version. An internal app resolves its version
	// directly, where an external one runs the per-user/track SQL that is not
	// what these tests are about.
	app := &App{id: "widgets", internal: &AppVersion{Version: "1.0", Classes: []string{"widget"}}}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", app)
	rate_limit_entity_create.reset(uid)
	return thread
}

func entity_limit_create(thread *sl.Thread, name string) error {
	fn := sl.NewBuiltin("mochi.entity.create", api_entity_create)
	_, err := api_entity_create(thread, fn, sl.Tuple{
		sl.String("widget"), sl.String(name), sl.String("private"),
	}, nil)
	return err
}

// TestEntityCreateIsRateLimited is the finding: creation was unthrottled while
// only withdrawal was limited.
func TestEntityCreateIsRateLimited(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()

	thread := entity_limit_thread(t, "u-limit")
	defer rate_limit_entity_create.reset("u-limit")

	created, refusal := 0, error(nil)
	for i := 0; i < rate_limit_entity_create.limit+5; i++ {
		if err := entity_limit_create(thread, fmt.Sprintf("Widget %d", i)); err != nil {
			refusal = err
			break
		}
		created++
	}

	if refusal == nil {
		t.Fatalf("all %d creations were accepted; nothing bounds the mesh publish", created)
	}
	if created > rate_limit_entity_create.limit {
		t.Errorf("%d creations accepted, above the budget of %d", created, rate_limit_entity_create.limit)
	}
	if created == 0 {
		t.Error("the first creation was refused; the limit must admit ordinary use")
	}
	if !strings.Contains(refusal.Error(), "rate limit") {
		t.Errorf("refused with %q, want the rate limit error", refusal)
	}
}

// TestEntityCreateLimitIsPerUser. Keyed on the user so one account looping does
// not deny creation to every other account on the server - a shared bucket
// would turn one misbehaving app into an outage for everyone.
func TestEntityCreateLimitIsPerUser(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()

	flooder := entity_limit_thread(t, "u-flood")
	defer rate_limit_entity_create.reset("u-flood")
	for i := 0; i < rate_limit_entity_create.limit+5; i++ {
		if err := entity_limit_create(flooder, fmt.Sprintf("Flood %d", i)); err != nil {
			break
		}
	}

	other := entity_limit_thread(t, "u-other")
	defer rate_limit_entity_create.reset("u-other")
	if err := entity_limit_create(other, "Innocent"); err != nil {
		t.Errorf("an unrelated account was refused because another flooded: %v", err)
	}
}

// TestEntityCreateLimitLeavesSignupAlone. The charge sits in the Starlark
// builtin, not in entity_create, so the identity web.go mints at signup does
// not draw on an app's budget - an account cannot be prevented from existing by
// an app that has been busy.
func TestEntityCreateLimitLeavesSignupAlone(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()

	user := &User{UID: "u-signup", Username: "signup@example.com"}
	db := db_open("db/users.db")
	db.exec("insert into users (uid, username) values (?, ?)", user.UID, user.Username)

	// Budget fully spent for this account.
	for i := 0; i < rate_limit_entity_create.limit+5; i++ {
		rate_limit_entity_create.allow(user.UID)
	}
	defer rate_limit_entity_create.reset(user.UID)

	// The signup path calls entity_create directly, which must still work.
	if _, err := entity_create(user, "person", "Signup", "private", ""); err != nil {
		t.Errorf("signup identity creation was blocked by an app's rate limit: %v", err)
	}
}

// TestEntityCreateChargedAfterTheClassCheck. A call refused for lacking the
// class must not spend budget: an app with a manifest bug would otherwise burn
// the user's allowance and block the apps that are behaving.
func TestEntityCreateChargedAfterTheClassCheck(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()

	thread := entity_limit_thread(t, "u-refused")
	defer rate_limit_entity_create.reset("u-refused")

	fn := sl.NewBuiltin("mochi.entity.create", api_entity_create)
	for i := 0; i < rate_limit_entity_create.limit+5; i++ {
		// "elsewhere" is a class this app does not declare.
		_, err := api_entity_create(thread, fn, sl.Tuple{
			sl.String("elsewhere"), sl.String("Nope"), sl.String("private"),
		}, nil)
		if err == nil {
			t.Fatal("a class the app does not declare was accepted")
		}
		if strings.Contains(err.Error(), "rate limit") {
			t.Fatalf("refused calls consumed the budget: hit the limit after %d attempts", i)
		}
	}

	// The budget is intact, so a legitimate creation still goes through.
	if err := entity_limit_create(thread, "Allowed"); err != nil {
		t.Errorf("a valid creation was refused after class-check failures: %v", err)
	}
}
