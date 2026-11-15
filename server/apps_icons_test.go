// Mochi server: the icon list shows one icon for a development app and the
// published app it was published as - the one their shared path opens.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	av.app = a
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

// icons_served registers a development app at path whose icon is file, served
// from its images route as an installed app's is, with files written under its
// directory by their path there.
func icons_served(t *testing.T, id string, path string, file string, files map[string]string) *AppVersion {
	t.Helper()
	base := t.TempDir()
	for name, content := range files {
		target := filepath.Join(base, name)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	av := &AppVersion{Version: "1.0", Paths: []string{path}, base: base,
		Icons:   []Icon{{Label: "app.name", File: file}},
		Actions: map[string]AppAction{"images": {Files: "web/dist/images", Public: true}, "": {Function: "action_page"}}}
	a := &App{id: id, development: true, versions: map[string]*AppVersion{"1.0": av}, latest: av}
	av.app = a
	apps_lock.Lock()
	apps[id] = a
	apps_lock.Unlock()
	resolution_invalidate()
	return av
}

// icons_answered calls mochi.app.icons as user, with data when asked, and
// returns its icons by id.
func icons_answered(t *testing.T, user *User, data bool) map[string]map[string]any {
	t.Helper()
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	fn := sl.NewBuiltin("mochi.app.icons", api_app_icons)
	var kwargs []sl.Tuple
	if data {
		kwargs = []sl.Tuple{{sl.String("data"), sl.True}}
	}
	value, err := api_app_icons(thread, fn, nil, kwargs)
	if err != nil {
		t.Fatalf("mochi.app.icons: %v", err)
	}
	result, _ := sl_decode(value).(map[string]any)
	out := map[string]map[string]any{}
	switch icons := result["icons"].(type) {
	case []any:
		for _, icon := range icons {
			m := icon.(map[string]any)
			out[m["id"].(string)] = m
		}
	case []map[string]any:
		for _, m := range icons {
			out[m["id"].(string)] = m
		}
	}
	return out
}

// icon_svg decodes an icon's data: URL, failing the test if it is not one.
func icon_svg(t *testing.T, icon map[string]any) string {
	t.Helper()
	value, _ := icon["data"].(string)
	encoded, found := strings.CutPrefix(value, "data:image/svg+xml;base64,")
	if !found {
		t.Fatalf("icon %v has no SVG data: URL: %q", icon["id"], value)
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("icon %v data: %v", icon["id"], err)
	}
	return string(decoded)
}

const icons_glyph = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M2 2h20v20H2z"/></svg>`

// With data, an icon carries the SVG its path serves, sanitised as it is
// served, so the home screen draws every icon from one answer; without it,
// as the menu calls it, the answer is as it was.
func TestIconsCarryTheSvgTheirPathServes(t *testing.T) {
	create_test_routing_env(t)
	user := &User{UID: "u1", Username: "user1@example.com"}
	hostile := `<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script><path d="M0 0" onload="alert(2)"/></svg>`
	icons_served(t, "feeds", "feeds", "images/icon.svg", map[string]string{"web/dist/images/icon.svg": icons_glyph})
	icons_served(t, "forums", "forums", "images/icon.svg", map[string]string{"web/dist/images/icon.svg": hostile})

	plain := icons_answered(t, user, false)
	if _, found := plain["feeds"]["data"]; found {
		t.Error("an icon carries data though the caller did not ask for it")
	}
	answered := icons_answered(t, user, true)
	if got := icon_svg(t, answered["feeds"]); got != icons_glyph {
		t.Errorf("THE POINT: the icon's data is %q, want the file its path serves", got)
	}
	got := icon_svg(t, answered["forums"])
	if got != string(svg_sanitize([]byte(hostile))) || strings.Contains(got, "<script") || strings.Contains(got, "onload") {
		t.Errorf("the data is not sanitised as the served file is: %q", got)
	}
	if answered["feeds"]["path"] != "feeds" || answered["feeds"]["file"] != "images/icon.svg" {
		t.Errorf("the path and file a page falls back to changed: %v", answered["feeds"])
	}
}

// Only an SVG the icon's own path serves, and of a sane size, is inlined: any
// other icon keeps its path alone, and the page loads it from there.
func TestIconDataIsOnlyAServedSvg(t *testing.T) {
	create_test_routing_env(t)
	user := &User{UID: "u1", Username: "user1@example.com"}
	icons_served(t, "png", "png", "images/icon.png", map[string]string{"web/dist/images/icon.png": "\x89PNG"})
	icons_served(t, "large", "large", "images/icon.svg", map[string]string{"web/dist/images/icon.svg": "<svg>" + strings.Repeat(" ", app_icon_data_maximum) + "</svg>"})
	icons_served(t, "unrouted", "unrouted", "pictures/icon.svg", map[string]string{"pictures/icon.svg": icons_glyph})
	icons_served(t, "missing", "missing", "images/gone.svg", nil)
	icons_served(t, "escape", "escape", "images/../../../secret.svg", map[string]string{"secret.svg": icons_glyph, "web/dist/images/icon.svg": icons_glyph})
	// A feature route answers its path itself, from no file.
	feature := icons_served(t, "feature", "feature", "repository/icon.svg", map[string]string{"icon.svg": icons_glyph})
	feature.Actions["repository"] = AppAction{Feature: "git"}
	// An action's own file is what its path serves, not its directory's.
	own := icons_served(t, "own", "own", "images/icon.svg", map[string]string{"web/dist/images/icon.svg": icons_glyph, "web/other.svg": icons_glyph})
	own.Actions["images"] = AppAction{File: "web/other.svg", Files: "web/dist/images", Public: true}
	resolution_invalidate()

	answered := icons_answered(t, user, true)
	for _, id := range []string{"png", "large", "unrouted", "missing", "escape", "feature", "own"} {
		icon, listed := answered[id]
		if !listed {
			t.Fatalf("icon %s is not listed", id)
		}
		if value, found := icon["data"]; found {
			t.Errorf("THE POINT: icon %s carries data %.60q", id, value)
		}
	}
}

// A theme's own icon for an app is the one inlined, read from the theme's
// icons route as its path serves it.
func TestIconDataFollowsTheTheme(t *testing.T) {
	create_test_routing_env(t)
	user := &User{UID: "u1", Username: "user1@example.com", Preferences: map[string]string{"theme": "themes:mono"}}
	themed := strings.Replace(icons_glyph, "M2 2", "M4 4", 1)
	icons_served(t, "feeds", "feeds", "images/icon.svg", map[string]string{"web/dist/images/icon.svg": icons_glyph})
	theme := icons_served(t, "themes", "themes", "images/icon.svg", map[string]string{"web/dist/images/icon.svg": icons_glyph, "icons/feeds.svg": themed})
	theme.Actions["icons"] = AppAction{Files: "icons", Public: true}
	theme.Themes = []AppTheme{{ID: "mono", Icons: map[string]string{"feeds": "feeds.svg"}}}
	resolution_invalidate()

	icon := icons_answered(t, user, true)["feeds"]
	if icon["path"] != "themes/icons" || icon["file"] != "feeds.svg" {
		t.Fatalf("the theme's icon is not the one listed: %v", icon)
	}
	if got := icon_svg(t, icon); got != themed {
		t.Errorf("THE POINT: the themed icon's data is %q, want the theme's file", got)
	}
}

// An icon's data is what its address serves for this user: a path the user
// binds to another app is answered from that app's files, so the data is
// read from there too.
func TestIconDataFollowsAPathBinding(t *testing.T) {
	create_test_routing_env(t)
	user := &User{UID: "u1", Username: "user1@example.com"}
	bound := strings.Replace(icons_glyph, "M2 2", "M6 6", 1)
	feeds := icons_served(t, "feeds", "feeds", "images/icon.svg", map[string]string{"web/dist/images/icon.svg": icons_glyph})
	other := icons_served(t, "other", "other", "images/icon.svg", map[string]string{"web/dist/images/icon.svg": bound})
	// Two published apps, so neither is the other's development twin and the
	// bound-away one stays listed.
	feeds.app.development = false
	other.app.development = false
	user.set_path_app("feeds", "other")
	resolution_invalidate()

	icon := icons_answered(t, user, true)["feeds"]
	if icon["path"] != "feeds" {
		t.Fatalf("the icon's address changed: %v", icon)
	}
	if got := icon_svg(t, icon); got != bound {
		t.Errorf("THE POINT: the data is %q, want the bound app's file, which /feeds/images/icon.svg serves", got)
	}
}
