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
// calling app's own manifest, so "classes": ["person"] alone would buy an app
// authority over the account's login identity; the entity mutators need
// user/identity/write.
func TestIdentityClassNeedsTheIdentityPermission(t *testing.T) {
	create_test_routing_env(t)

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
// reach the class decision, so none can be missed on its own. Checked per
// mutator across both helpers - counting one name would pass with a mutator
// sitting on nothing.
func TestIdentityClassGateCoversEveryEntityMutator(t *testing.T) {
	source, err := os.ReadFile("entities.go")
	if err != nil {
		t.Fatalf("read entities.go: %v", err)
	}
	text := string(source)

	for _, mutator := range []string{"func api_entity_create", "func api_entity_delete", "func api_entity_update"} {
		start := strings.Index(text, mutator)
		if start < 0 {
			t.Fatalf("%s not found", mutator)
		}
		length := strings.Index(text[start+1:], "\nfunc ")
		if length < 0 {
			length = len(text) - start - 1
		}
		body := text[start : start+1+length]
		if !strings.Contains(body, "entity_class_allowed(t, fn, app, user,") &&
			!strings.Contains(body, "entity_class_owned(t, fn, app, user,") &&
			!strings.Contains(body, "entity_class_shared(t, fn, app, user,") {
			t.Errorf("%s reaches neither class check; the identity gate does not cover it", mutator)
		}
	}

	// Both stricter helpers must go through the permissive one, or the identity
	// permission stops applying to the mutators that use them.
	for _, helper := range []string{"func entity_class_owned", "func entity_class_shared"} {
		at := strings.Index(text, helper)
		if at < 0 {
			t.Fatalf("%s not found", helper)
		}
		length := strings.Index(text[at+1:], "\nfunc ")
		if !strings.Contains(text[at:at+1+length], "entity_class_allowed(t, fn, app, user, class)") {
			t.Errorf("%s does not call entity_class_allowed; its mutators no longer reach the user/identity/write gate", helper)
		}
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
