// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"os"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// app_declaring builds an external app whose active version declares classes.
// External, not internal: require_permission waives internal Go apps, so an
// internal app would pass the gate for the wrong reason.
func app_declaring(id string, classes ...string) *App {
	av := &AppVersion{Version: "1.0", Classes: classes}
	return &App{id: id, versions: map[string]*AppVersion{"1.0": av}, latest: av}
}

func grant(t *testing.T, user *User, app string, permission string) {
	t.Helper()
	db := db_user(user, "user")
	db.permissions_setup()
	db.permissions_upsert(app, permission, "", 1)
}

// TestIdentityClassNeedsTheIdentityPermission. app_declares_class asks the
// calling app's OWN manifest whether it may act on a class, so any app buys
// authority over the account's login identity with one line of its app.json:
// "classes": ["person"]. Nothing else gated entity.create/update/delete, so a
// third-party app could delete the identity and its private key, rename it, or
// mint fresh ones until it captured the account's primary. mochi.user.identity
// .update already required user/identity/write for the very same mutation on
// the very same row; the entity APIs now require it too.
func TestIdentityClassNeedsTheIdentityPermission(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

	user := &User{UID: "u1", Username: "user1@example.com"}
	fn := sl.NewBuiltin("mochi.entity.update", nil)

	claimer := app_declaring("claimer", entity_class_identity, "feed")
	apps[claimer.id] = claimer
	thread := create_test_thread(user, claimer)

	// Declaring the class is no longer enough for the login identity.
	err := entity_class_allowed(thread, fn, claimer, user, entity_class_identity)
	if err == nil {
		t.Fatal("an app that merely declares the identity class was allowed to act on it")
	}
	var denied *PermissionError
	if !errors.As(err, &denied) {
		t.Fatalf("refusal is %T (%v), want a *PermissionError so callers can tell it apart", err, err)
	}
	if denied.Permission != "user/identity/write" {
		t.Errorf("refused on %q, want user/identity/write - the permission the equivalent user API already requires", denied.Permission)
	}

	// The same app is unaffected on a class that is not the login identity,
	// which is what keeps every other app working.
	if err := entity_class_allowed(thread, fn, claimer, user, "feed"); err != nil {
		t.Errorf("a non-identity class was refused: %v", err)
	}

	// With the grant the app proceeds, so the legitimate holders still work.
	grant(t, user, claimer.id, "user/identity/write")
	if err := entity_class_allowed(thread, fn, claimer, user, entity_class_identity); err != nil {
		t.Errorf("a granted app was refused: %v", err)
	}

	// The grant does not replace the manifest check - it is additional to it.
	stranger := app_declaring("stranger", "feed")
	apps[stranger.id] = stranger
	grant(t, user, stranger.id, "user/identity/write")
	if err := entity_class_allowed(create_test_thread(user, stranger), fn, stranger, user, entity_class_identity); err == nil {
		t.Error("an app holding the grant but not declaring the class was allowed through")
	}
}

// TestIdentityClassGateCoversEveryEntityMutator: create, delete and update all
// reach the class decision through the one helper, so none can be hardened or
// missed on its own. app_declares_class is the self-assertion being wrapped -
// a second caller would be a second way around the permission.
func TestIdentityClassGateCoversEveryEntityMutator(t *testing.T) {
	source, err := os.ReadFile("entities.go")
	if err != nil {
		t.Fatalf("read entities.go: %v", err)
	}
	text := string(source)

	if n := strings.Count(text, "entity_class_allowed(t, fn, app, user,"); n != 3 {
		t.Errorf("entity_class_allowed is called %d times in entities.go, want 3 - create, delete and update", n)
	}
	if n := strings.Count(text, "app_declares_class(app, user,"); n != 1 {
		t.Errorf("app_declares_class is called %d times in entities.go, want 1 - only inside entity_class_allowed", n)
	}
}

// TestIdentityWritersKeepTheirDefaultGrant guards what the gate could break.
// user/identity/write is the permission People uses to rename the login
// identity and Settings to manage it; both reach entity.update through the new
// check, and losing the apps_default grant would leave those flows refused.
func TestIdentityWritersKeepTheirDefaultGrant(t *testing.T) {
	for _, name := range []string{"People", "Settings"} {
		found := false
		for _, app := range apps_default {
			if app.Name != name {
				continue
			}
			for _, g := range app.Permissions {
				if g.Permission == "user/identity/write" {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("default app %q has no user/identity/write grant; the identity-class gate would refuse its entity.update calls", name)
		}
	}
}
