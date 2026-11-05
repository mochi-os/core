// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

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
		// A -/ API path resolves against the entity whatever the client.
		{"api path", "-/information", false, ":feed/-/information"},
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
	// One the entity has no action for is a miss: neither the page nor a
	// class-level action reached through the entity's domain, which would run
	// against the owner's storage outside the entity's access rules.
	if aa := web_action_find(av, "-/rss/token", e, false); aa != nil {
		t.Errorf("missing api path resolved to %q; want a miss", aa.name)
	}
	// Without the post view that takes any -/ name as a post, a class-level
	// action of the name is the only other match, and it must not be reached.
	delete(av.Actions, ":feed/-/:post")
	av.Actions["-/list"] = AppAction{Function: "action_list"}
	if aa := web_action_find(av, "-/list", e, false); aa != nil {
		t.Errorf("THE POINT: the class-level %q was reached through the entity's domain; want a miss", aa.name)
	}
	// Off the domain the class-level action is reached as itself.
	web_action_find_check(t, av, nil, "class list", "-/list", false, "-/list")
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

// TestActionFindSeparator: the - segment separates an entity or class from its
// action, so a path holding it asks for an action. A parameter never takes it
// as a value, and a path no action matches is a miss, never the app's page,
// which a client asking for data would read as a web page with no reason. A
// page path without it still falls to the page, and a route that names the
// separator itself still matches.
func TestActionFindSeparator(t *testing.T) {
	av := web_action_find_version()
	for _, path := range []string{
		"-/nosuchaction",                 // ":feed/:post" would take "-" as the feed
		"https:/lemmy.ml/c/test/-/posts", // a community's address in the path, as #217 sent
		"-",
	} {
		if aa := av.find_action(path); aa != nil {
			t.Errorf("THE POINT: %q resolved to %q; want a miss", path, aa.name)
		}
	}
	web_action_find_check(t, av, nil, "page path", "no/such/page", false, "")
	web_action_find_check(t, av, nil, "post page", "somefeed/somepost", false, ":feed/:post")
	web_action_find_check(t, av, nil, "declared separator", "somefeed/-/somepost", false, ":feed/-/:post")
	delete(av.Actions, ":feed/-/:post")
	if aa := av.find_action("somefeed/-/nosuchaction"); aa != nil {
		t.Errorf("THE POINT: an entity action path no route declares resolved to %q; want a miss", aa.name)
	}

	// A file route by its parameter, and a parameter after a wildcard, refuse
	// the separator as well.
	av.Actions[":feed/assets"] = AppAction{Files: "web/dist/assets"}
	av.Actions["docs/*path/:page"] = AppAction{Function: "action_docs"}
	web_action_find_check(t, av, nil, "feed asset", "somefeed/assets/app.js", false, ":feed/assets")
	web_action_find_check(t, av, nil, "document page", "docs/a/b/intro", false, "docs/*path/:page")
	for _, path := range []string{"-/assets/app.js", "docs/a/b/-"} {
		if aa := av.find_action(path); aa != nil {
			t.Errorf("THE POINT: %q resolved to %q; want a miss", path, aa.name)
		}
	}
}

// TestWebActionMissAnswers: a route with nowhere else to go answers a path no
// action matches as a miss, in JSON naming the path, rather than leaving the
// router's bare 404 or the app's page.
func TestWebActionMissAnswers(t *testing.T) {
	load_core_labels()
	gin.SetMode(gin.TestMode)
	av := web_action_find_version()
	a := &App{id: "findtest", internal: av}
	av.app = a
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "http://example.com/feeds/-/nosuchaction", nil)
	c.Request.Header.Set("Accept", "application/json")
	web_action_answer(c, a, "-/nosuchaction", nil, routing_class)
	if w.Code != http.StatusNotFound || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("THE POINT: a missed action answered %d %q: %s", w.Code, w.Header().Get("Content-Type"), w.Body.String())
	}
	if body := w.Body.String(); !strings.Contains(body, `"action_not_found"`) || !strings.Contains(body, "No action answers /feeds/-/nosuchaction") {
		t.Errorf("THE POINT: the miss does not name the path: %s", body)
	}
}
