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

// TestEntitySignNeedsAPermission. mochi.entity.sign checked only that the user
// owned the entity, which every app in that user's session satisfies, so any
// installed app could mint an ed25519 signature over caller-supplied bytes
// under the account's identity key - a key core also signs export manifests
// and pubsub frames with.
func TestEntitySignNeedsAPermission(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

	user := &User{UID: "u1", Username: "user1@example.com"}
	signer := create_external_app("signer")
	apps[signer.id] = signer
	thread := create_test_thread(user, signer)
	fn := sl.NewBuiltin("mochi.entity.sign", nil)

	_, err := api_entity_sign(thread, fn, sl.Tuple{sl.String(strings.Repeat("1", 44)), sl.String("payload")}, nil)
	if err == nil {
		t.Fatal("an app with no grant was allowed to sign")
	}
	var denied *PermissionError
	if !errors.As(err, &denied) {
		t.Fatalf("refusal is %T (%v), want a *PermissionError", err, err)
	}
	if denied.Permission != "entity/sign" {
		t.Errorf("refused on %q, want entity/sign", denied.Permission)
	}

	// The gate must run before the arguments are read, so an app cannot learn
	// whether an entity exists by watching which error it gets.
	_, err = api_entity_sign(thread, fn, sl.Tuple{sl.String("not-an-entity")}, nil)
	if !errors.As(err, &denied) {
		t.Errorf("a malformed call reported %v rather than the permission refusal, so the gate runs after argument parsing", err)
	}
}

// TestEntitySignIsRestricted. A consent dialog cannot grant a restricted
// permission, which is the point: signing as the user is not something an app
// should be able to ask for at a moment of its choosing. Wikis therefore has
// to arrive holding it, and its apps_default entry is the only way it can.
func TestEntitySignIsRestricted(t *testing.T) {
	if !permission_restricted("entity/sign") {
		t.Error("entity/sign is not restricted, so any app can obtain it from a consent dialog")
	}
	if permission_administrator("entity/sign") {
		t.Error("entity/sign is administrator-only; wikis signs comments for ordinary users")
	}

	found := false
	for _, app := range apps_default {
		if app.Name != "Wikis" {
			continue
		}
		for _, g := range app.Permissions {
			if g.Permission == "entity/sign" {
				found = true
			}
		}
	}
	if !found {
		t.Error("Wikis has no entity/sign default grant; with the permission restricted its comment signing is dead and the user cannot re-grant it")
	}
}

// TestEntitySignHasALabelEverywhere. The permission is offered to the user by
// name in the Apps permissions tab, so an untranslated key shows English there.
// en-us is the deliberate 19-key sparse overlay and falls back to en.
func TestEntitySignHasALabelEverywhere(t *testing.T) {
	files, err := os.ReadDir("labels")
	if err != nil {
		t.Fatalf("read labels: %v", err)
	}
	checked := 0
	for _, f := range files {
		locale := strings.TrimSuffix(f.Name(), ".conf")
		if !strings.HasSuffix(f.Name(), ".conf") || locale == "en-us" {
			continue
		}
		body, err := os.ReadFile("labels/" + f.Name())
		if err != nil {
			t.Fatalf("read %s: %v", f.Name(), err)
		}
		checked++
		found := false
		for _, line := range strings.Split(string(body), "\n") {
			if value, ok := strings.CutPrefix(line, "permissions.entity.sign = "); ok && strings.TrimSpace(value) != "" {
				found = true
			}
		}
		if !found {
			t.Errorf("%s has no permissions.entity.sign label", locale)
		}
	}
	if checked < 90 {
		t.Errorf("only %d locales checked; the labels directory is not being read", checked)
	}
}
