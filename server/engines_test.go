// Mochi server: Protocol engine routing tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// engines_test_reset empties the registry for a test and restores it after.
func engines_test_reset(t *testing.T) {
	t.Helper()
	saved := engines
	engines = nil
	t.Cleanup(func() { engines = saved })
}

// TestEnginePrefixMatches: a prefix takes the path itself and paths below it,
// a prefix ending in "/" or a sigil takes everything beginning with it, and a
// longer segment that merely starts with the prefix is not matched.
func TestEnginePrefixMatches(t *testing.T) {
	cases := []struct {
		path, prefix string
		want         bool
	}{
		{"/xrpc/com.atproto.repo.listRecords", "/xrpc/", true},
		{"/xrpc", "/xrpc/", false},
		{"/.well-known/carddav", "/.well-known/carddav", true},
		{"/.well-known/carddav/", "/.well-known/carddav", true},
		{"/.well-known/carddavx", "/.well-known/carddav", false},
		{"/.well-known/webfinger", "/.well-known/carddav", false},
		{"/~k3f9x2ab", "/~", true},
		{"/~k3f9x2ab/inbox", "/~", true},
		{"/@alice", "/@", true},
		{"/_matrix/key/v2/server", "/_matrix", true},
		{"/_matrixx", "/_matrix", false},
		{"/apiary/x", "/api", false},
	}
	for _, c := range cases {
		if got := engine_prefix_matches(c.path, c.prefix); got != c.want {
			t.Errorf("engine_prefix_matches(%q, %q) = %v, want %v", c.path, c.prefix, got, c.want)
		}
	}
}

// TestEngineForLongestPrefix: the engine with the longest matching prefix
// answers, and a path no engine claims gets nil.
func TestEngineForLongestPrefix(t *testing.T) {
	engines_test_reset(t)
	wide := &engine{name: "wide", prefixes: []string{"/.well-known/"}}
	narrow := &engine{name: "narrow", prefixes: []string{"/.well-known/carddav"}}
	engine_register(wide)
	engine_register(narrow)

	if e := engine_for("/.well-known/carddav"); e != narrow {
		t.Errorf("engine_for carddav = %v, want narrow", e)
	}
	if e := engine_for("/.well-known/webfinger"); e != wide {
		t.Errorf("engine_for webfinger = %v, want wide", e)
	}
	if e := engine_for("/feeds/abc"); e != nil {
		t.Errorf("engine_for an app path = %v, want nil", e)
	}
}

// TestEngineRoute: a request under a registered prefix reaches the engine's
// handler and is reported answered; a reserved segment with no engine behind
// it falls through, so routing stays as it was before any engine existed.
func TestEngineRoute(t *testing.T) {
	engines_test_reset(t)
	gin.SetMode(gin.TestMode)

	called := ""
	engine_register(&engine{
		name:     "test",
		prefixes: []string{"/xrpc/"},
		handler: func(c *gin.Context) bool {
			called = c.Request.URL.Path
			c.String(http.StatusOK, "engine")
			return true
		},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/xrpc/com.atproto.server.describeServer", nil)
	if !engine_route(c) {
		t.Fatal("engine_route should answer a path under a registered prefix")
	}
	if called != "/xrpc/com.atproto.server.describeServer" || w.Code != http.StatusOK {
		t.Errorf("handler saw %q, status %d", called, w.Code)
	}

	called = ""
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/_matrix/key/v2/server", nil)
	if engine_route(c) {
		t.Fatal("engine_route should fall through on a reserved segment no engine answers")
	}
	if called != "" {
		t.Errorf("handler was called for %q", called)
	}
}

// TestEngineSetting: an engine behind a network setting answers only while the
// development switch is on and the owner has not switched the network off.
func TestEngineSetting(t *testing.T) {
	create_domains_test_env(t)
	engines_test_reset(t)
	gin.SetMode(gin.TestMode)
	saved := networks_development
	t.Cleanup(func() { networks_development = saved })

	called := 0
	engine_register(&engine{
		name:     "test",
		prefixes: []string{"/xrpc/"},
		setting:  "bluesky",
		handler: func(c *gin.Context) bool {
			called++
			c.String(http.StatusOK, "engine")
			return true
		},
	})
	probe := func() int {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/xrpc/com.atproto.server.describeServer", nil)
		if !engine_route(c) {
			t.Fatal("a registered prefix is always answered, by the engine or by not found")
		}
		return w.Code
	}

	networks_development = false
	if code := probe(); code != http.StatusNotFound || called != 0 {
		t.Errorf("with the development switch off: status %d, handler calls %d", code, called)
	}
	networks_development = true
	if code := probe(); code != http.StatusOK || called != 1 {
		t.Errorf("with the switch on and the setting at its default: status %d, handler calls %d", code, called)
	}
	setting_set("bluesky", "false")
	if code := probe(); code != http.StatusNotFound || called != 1 {
		t.Errorf("with the network switched off: status %d, handler calls %d", code, called)
	}
}

// TestEngineWellKnownDav: the DAV engine core ships answers its two discovery
// paths through the registry and nothing else under /.well-known/.
func TestEngineWellKnownDav(t *testing.T) {
	engines_test_reset(t)
	engine_register(dav_engine())

	if e := engine_for("/.well-known/carddav"); e == nil || e.name != "dav" {
		t.Errorf("carddav discovery is not routed to the dav engine: %v", e)
	}
	if e := engine_for("/.well-known/caldav"); e == nil || e.name != "dav" {
		t.Errorf("caldav discovery is not routed to the dav engine: %v", e)
	}
	if e := engine_for("/.well-known/webfinger"); e != nil {
		t.Errorf("webfinger has no engine yet and should not be claimed: %v", e)
	}
}

// TestEngineBodyLimit: web_body_limit applies the cap an engine declares on
// its paths and the general cap everywhere else.
func TestEngineBodyLimit(t *testing.T) {
	engines_test_reset(t)
	gin.SetMode(gin.TestMode)
	engine_register(&engine{name: "upload", prefixes: []string{"/xrpc/"}, body: 4 << 20})

	r := gin.New()
	r.Use(web_body_limit)
	r.POST("/*path", func(c *gin.Context) {
		if _, err := io.ReadAll(c.Request.Body); err != nil {
			c.String(http.StatusRequestEntityTooLarge, "too large")
			return
		}
		c.String(http.StatusOK, "ok")
	})

	body := bytes.Repeat([]byte("x"), 2<<20)
	for _, c := range []struct {
		path string
		want int
	}{
		{"/xrpc/com.atproto.repo.uploadBlob", http.StatusOK},
		{"/feeds/-/post", http.StatusRequestEntityTooLarge},
	} {
		req := httptest.NewRequest("POST", c.path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/octet-stream")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != c.want {
			t.Errorf("POST %s with a 2MB body: status %d, want %d", c.path, w.Code, c.want)
		}
	}
}

// TestEnginePathReserved: the segments engines answer cannot be an app's path,
// whole, and the manifest validator refuses one.
func TestEnginePathReserved(t *testing.T) {
	for _, path := range []string{"api", "nodeinfo", "oauth", "pictrs", "webmention", "websub", "xrpc"} {
		if !engine_path_reserved(path) {
			t.Errorf("engine_path_reserved(%q) should be true", path)
		}
	}
	for _, path := range []string{"apiary", "feeds", "x"} {
		if engine_path_reserved(path) {
			t.Errorf("engine_path_reserved(%q) should be false", path)
		}
	}
	av := &AppVersion{Version: "1.0", Label: "test", Paths: []string{"api"}}
	err := manifest_validate(av)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Errorf("manifest_validate should refuse the reserved path, got %v", err)
	}
}

// TestAppsForFunction: an engine asks which apps declare a function and gets
// them ordered as app_select_best would choose, development apps first.
func TestAppsForFunction(t *testing.T) {
	create_test_apps_db(t)
	receive := map[string]AppFunction{"activitypub/receive": {Function: "function_receive"}}
	apps_lock.Lock()
	saved := apps
	apps = map[string]*App{
		"zeta":  {id: "zeta", internal: &AppVersion{Functions: receive}},
		"alpha": {id: "alpha", development: true, internal: &AppVersion{Functions: receive}},
		"none":  {id: "none", internal: &AppVersion{}},
	}
	apps_lock.Unlock()
	t.Cleanup(func() {
		apps_lock.Lock()
		apps = saved
		apps_lock.Unlock()
	})

	found := apps_for_function(nil, "activitypub/receive")
	if len(found) != 2 || found[0].id != "alpha" || found[1].id != "zeta" {
		ids := []string{}
		for _, a := range found {
			ids = append(ids, a.id)
		}
		t.Errorf("apps_for_function = %v, want [alpha zeta]", ids)
	}
	if n := len(apps_for_function(nil, "mastodon/status")); n != 0 {
		t.Errorf("apps_for_function for an undeclared function returned %d apps", n)
	}
}

// TestNetworksDormant: with the development switch off, every path the
// networks' engines answer is not found, as before any of them existed, its
// preflight too; and with the switch on, a network the owner has switched off
// answers nothing on its paths while the others still do.
func TestNetworksDormant(t *testing.T) {
	create_domains_test_env(t)
	engines_test_reset(t)
	gin.SetMode(gin.TestMode)
	saved := networks_development
	t.Cleanup(func() { networks_development = saved })
	engines_start()
	paths := map[string][]string{
		"fediverse": {"/.well-known/nodeinfo", "/.well-known/webfinger?resource=acct:alice@example.com", "/nodeinfo/2.1", "/@alice", "/api/v1/instance", "/api/v3/site"},
		"bluesky":   {"/xrpc/com.atproto.server.describeServer", "/.well-known/atproto-did"},
		"matrix":    {"/_matrix/federation/v1/version", "/.well-known/matrix/server", "/_matrix/client/versions", "/.well-known/matrix/client"},
		"xmpp":      {"/xmpp/websocket"},
		"":          {"/oauth/authorize", "/.well-known/oauth-authorization-server", "/api/v1/apps", "/oauth/client-metadata.json", "/websub", "/webmention/x", "/.well-known/host-meta"},
	}
	probe := func(method string, path string) int {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(method, "http://example.com"+path, nil)
		c.Request.Host = "example.com"
		if !engine_route(c) {
			return 0
		}
		return w.Code
	}
	networks_development = false
	for _, list := range paths {
		for _, path := range list {
			for _, method := range []string{"GET", "OPTIONS"} {
				if code := probe(method, path); code != 0 && code != http.StatusNotFound {
					t.Errorf("THE POINT: with the development switch off, %s %s = %d", method, path, code)
				}
			}
		}
	}
	networks_development = true
	for _, network := range network_names {
		setting_set(network, "false")
		for _, path := range paths[network] {
			if code := probe("GET", path); code != http.StatusNotFound {
				t.Errorf("THE POINT: with %s switched off, GET %s = %d", network, path, code)
			}
		}
		for _, other := range network_names {
			if other == network || len(paths[other]) == 0 {
				continue
			}
			if code := probe("GET", paths[other][0]); code == http.StatusNotFound && other != "xmpp" {
				t.Errorf("with %s switched off, %s's %s = %d", network, other, paths[other][0], code)
			}
		}
		setting_set(network, "true")
	}
}
