// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// The feeds layout: a catch-all page, a class-level RSS action, a per-feed RSS
// action on a plain segment, the SPA page routes, a post view on the API side
// that matches any bare name by parameter, and one exact -/ API action.
func web_action_find_version() *AppVersion {
	av := &AppVersion{Actions: map[string]AppAction{
		"":                    {File: "web/dist/index.html"},
		"rss":                 {Function: "action_rss_all", Public: true},
		":feed":               {File: "web/dist/index.html", Public: true},
		":feed/rss":           {Function: "action_rss", Public: true},
		":feed/:post":         {File: "web/dist/index.html", Public: true},
		":feed/-/:post":       {File: "web/dist/index.html", Function: "action_view", Public: true},
		":feed/-/information": {Function: "action_information", Public: true},
	}}
	// A miss logs the app's name, so the version needs its app.
	av.app = &App{id: "findtest", latest: av, versions: map[string]*AppVersion{"1.0": av}}
	return av
}

func web_action_find_check(t *testing.T, av *AppVersion, e *Entity, name string, path string, prefer_html bool, want string) {
	t.Helper()
	aa := web_action_find(av, path, e, prefer_html)
	if aa == nil {
		t.Errorf("%s: %q resolved to nothing; want %q", name, path, want)
		return
	}
	if aa.name != want {
		t.Errorf("%s: %q resolved to %q; want %q", name, path, aa.name, want)
	}
}

func TestActionFindDomainEntity(t *testing.T) {
	av := web_action_find_version()
	id := test_entity_id('f')
	e := &Entity{ID: id, Fingerprint: fingerprint(id), Class: "feed"}

	cases := []struct {
		name        string
		path        string
		prefer_html bool
		want        string
	}{
		// The case the bare-name weighing exists for: a reader fetching /rss
		// on the feed's own domain must get that feed. The API pattern
		// ":feed/-/:post" matches "rss" by parameter, and used to win.
		{"reader rss", "rss", false, ":feed/rss"},
		{"browser rss", "rss", true, ":feed/rss"},
		// A -/ API path resolves against the entity whatever the client, and
		// one the entity has no action for stays on the catch-all rather than
		// reaching a class-level action through the entity's domain.
		{"api path", "-/information", false, ":feed/-/information"},
		{"missing api path", "-/rss/token", false, ""},
		// A bare name a -/ action declares exactly: the API action for a
		// non-browser, the page for a browser, so a page whose slug is an
		// action name still opens.
		{"reader api name", "information", false, ":feed/-/information"},
		{"browser api name", "information", true, ":feed/:post"},
		// A bare name both sides match only by parameter: the client's
		// preference decides.
		{"reader page name", "somepost", false, ":feed/-/:post"},
		{"browser page name", "somepost", true, ":feed/:post"},
		// The root and the entity's own fingerprint are the entity page.
		{"root", "", false, ":feed"},
		{"fingerprint", fingerprint(id), false, ":feed"},
	}
	for _, tt := range cases {
		web_action_find_check(t, av, e, tt.name, tt.path, tt.prefer_html, tt.want)
	}
}

// Without the parameterised post view, a bare name that only the page side
// matches reaches the page for any client, and a name with no route at all
// stays on the catch-all.
func TestActionFindDomainEntityPageOnly(t *testing.T) {
	av := web_action_find_version()
	delete(av.Actions, ":feed/-/:post")
	id := test_entity_id('f')
	e := &Entity{ID: id, Fingerprint: fingerprint(id), Class: "feed"}

	web_action_find_check(t, av, e, "reader page name", "somepost", false, ":feed/:post")
	web_action_find_check(t, av, e, "reader rss", "rss", false, ":feed/rss")
	delete(av.Actions, ":feed/:post")
	web_action_find_check(t, av, e, "reader unrouted name", "somepost", false, "")
	web_action_find_check(t, av, e, "browser unrouted name", "somepost", true, "")
}

func TestActionFindWithoutEntity(t *testing.T) {
	av := web_action_find_version()
	id := test_entity_id('f')

	cases := []struct {
		name string
		path string
		want string
	}{
		// Class-level and main-site paths resolve as given: "rss" is the
		// aggregate, a fingerprint-prefixed path is the per-feed route, the
		// old -/rss path is now the post view for a post called "rss", and
		// anything else is the catch-all page.
		{"class rss", "rss", "rss"},
		{"entity rss", fingerprint(id) + "/rss", ":feed/rss"},
		{"entity post", fingerprint(id) + "/1a2b3c4d5", ":feed/:post"},
		{"entity api", fingerprint(id) + "/-/information", ":feed/-/information"},
		{"old entity rss", fingerprint(id) + "/-/rss", ":feed/-/:post"},
		{"unmatched", "missing/path/here", ""},
	}
	for _, tt := range cases {
		web_action_find_check(t, av, nil, tt.name, tt.path, false, tt.want)
	}
}

// An app with no catch-all page reports a miss as nil, on both paths.
func TestActionFindWithoutCatchAll(t *testing.T) {
	av := web_action_find_version()
	delete(av.Actions, "")
	id := test_entity_id('f')
	e := &Entity{ID: id, Fingerprint: fingerprint(id), Class: "feed"}

	web_action_find_check(t, av, e, "reader rss", "rss", false, ":feed/rss")
	if aa := web_action_find(av, "-/rss/token", e, false); aa != nil {
		t.Errorf("entity path with no action resolved to %q; want nothing", aa.name)
	}
	if aa := web_action_find(av, "missing/path/here", nil, false); aa != nil {
		t.Errorf("unmatched path resolved to %q; want nothing", aa.name)
	}
}
