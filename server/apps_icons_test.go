// Mochi server: the icon list shows one icon for a development app and the
// published app it was published as - the one their shared path opens.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"sort"
	"testing"

	sl "go.starlark.net/starlark"
)

// icons_app registers an app with one icon, the way the loaders do. A
// published app runs through app_resolve_paths first, so one declaring a path
// an app already registered holds lands on its fingerprint.
func icons_app(t *testing.T, id string, development bool, paths ...string) *App {
	t.Helper()
	av := &AppVersion{Version: "1.0", Paths: paths, Icons: []Icon{{Label: "app.name", File: "images/icon.svg"}}}
	if !development {
		app_resolve_paths(av, id)
	}
	a := &App{id: id, development: development, versions: map[string]*AppVersion{"1.0": av}, latest: av}
	apps_lock.Lock()
	apps[id] = a
	apps_lock.Unlock()
	resolution_invalidate()
	return a
}

// icons_listed calls mochi.app.icons as user and returns the ids it lists.
func icons_listed(t *testing.T, user *User) []string {
	t.Helper()
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	fn := sl.NewBuiltin("mochi.app.icons", api_app_icons)
	value, err := api_app_icons(thread, fn, nil, nil)
	if err != nil {
		t.Fatalf("mochi.app.icons: %v", err)
	}
	result, _ := sl_decode(value).(map[string]any)
	var ids []string
	switch icons := result["icons"].(type) {
	case []any:
		for _, icon := range icons {
			ids = append(ids, icon.(map[string]any)["id"].(string))
		}
	case []map[string]any:
		for _, icon := range icons {
			ids = append(ids, icon["id"].(string))
		}
	default:
		t.Fatalf("icons has type %T", result["icons"])
	}
	sort.Strings(ids)
	return ids
}

func icons_equal(t *testing.T, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("icons %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("icons %v, want %v", got, want)
		}
	}
}

// A development checkout holds the path, so the published copy is demoted to
// its fingerprint and drops out of the list; an app with no twin stays.
func TestIconsListTheDevelopmentTwin(t *testing.T) {
	create_test_routing_env(t)
	user := &User{UID: "u1", Username: "user1@example.com"}

	icons_app(t, "air", true, "air")
	published := icons_app(t, "1PublishedAirAppEntityIdentifier00000000000000000", false, "air")
	icons_app(t, "solo", false, "solo")

	if got := published.latest.Paths; len(got) != 1 || got[0] != fingerprint(published.id) {
		t.Fatalf("published twin serves %v, want its fingerprint", got)
	}
	if got := published.latest.declared; len(got) != 1 || got[0] != "air" {
		t.Fatalf("published twin declared %v, want [air] kept", got)
	}
	icons_equal(t, icons_listed(t, user), "air", "solo")
}

// The home screen is served at the root path, which is still a path.
func TestIconsListTheDevelopmentTwinAtTheRoot(t *testing.T) {
	create_test_routing_env(t)
	user := &User{UID: "u1", Username: "user1@example.com"}

	icons_app(t, "home", true, "")
	icons_app(t, "1PublishedHomeAppEntityIdentifier0000000000000000", false, "")

	icons_equal(t, icons_listed(t, user), "home")
}

// A user who binds the path to the published app gets that one, and the
// development twin gives way instead.
func TestIconsFollowAPathBinding(t *testing.T) {
	create_test_routing_env(t)
	user := &User{UID: "u1", Username: "user1@example.com"}

	icons_app(t, "air", true, "air")
	published := icons_app(t, "1PublishedAirAppEntityIdentifier00000000000000000", false, "air")
	user.set_path_app("air", published.id)
	resolution_invalidate()

	icons_equal(t, icons_listed(t, user), published.id)
}

// Two published apps contesting a path are different apps: the one demoted to
// its fingerprint must stay listed, or an installed app would vanish.
func TestIconsKeepContestingPublishedApps(t *testing.T) {
	create_test_routing_env(t)
	user := &User{UID: "u1", Username: "user1@example.com"}

	first := icons_app(t, "1FirstChatAppEntityIdentifier000000000000000000000", false, "chat")
	second := icons_app(t, "1SecondChatAppEntityIdentifier00000000000000000000", false, "chat")

	if got := second.latest.declared; len(got) != 1 || got[0] != "chat" {
		t.Fatalf("second app declared %v, want [chat] kept", got)
	}
	icons_equal(t, icons_listed(t, user), first.id, second.id)
}
