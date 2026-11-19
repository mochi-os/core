// Mochi server: the actions a read may run
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Routing ignores the method and a page builds request paths from data, so a
// crafted id in an image's address could make its GET a call to a writing
// action, with inputs from the query string. A GET or HEAD runs only the
// functions an app marked read, and only a read takes inputs from the query
// string.

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	sl "go.starlark.net/starlark"
)

// read_inputs is what an action saw of its input "x": a.input's answer and
// a.inputs' list.
type read_inputs struct {
	single string
	plural []string
}

// read_app builds an app with one action of each kind a read meets, each
// counting its calls: a writing action, one marked read, an asset, a file, a
// files directory with and without a function, a page that serves its file to
// a browser and its function to anyone else, the same page marked read, and an
// action that records the input "x" it is given.
func read_app(t *testing.T) (*App, map[string]int, *read_inputs) {
	t.Helper()
	create_web_test_env(t)

	base := t.TempDir()
	files := map[string]string{
		"page.html":        "<html><head></head><body>the page</body></html>",
		"style.css":        "the style",
		"assets/script.js": "the script",
	}
	for name, content := range files {
		path := filepath.Join(base, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	calls := map[string]int{}
	count := func(name string) func(*Action) {
		return func(a *Action) { calls[name]++ }
	}
	seen := &read_inputs{}
	record := func(a *Action) {
		calls["echo"]++
		input := &ActionInput{action: a}
		single, err := input.CallInternal(&sl.Thread{}, sl.Tuple{sl.String("x"), sl.String("")}, nil)
		if err != nil {
			t.Errorf("a.input: %v", err)
			return
		}
		if text, _ := sl.AsString(single); text != a.input("x") {
			t.Errorf("a.input(\"x\") = %q but Action.input = %q; the two must agree", text, a.input("x"))
		}
		seen.single, _ = sl.AsString(single)
		plural, err := a.sl_inputs(&sl.Thread{}, sl.NewBuiltin("inputs", a.sl_inputs), sl.Tuple{sl.String("x")}, nil)
		if err != nil {
			t.Errorf("a.inputs: %v", err)
			return
		}
		list := plural.(*sl.List)
		seen.plural = []string{}
		for i := 0; i < list.Len(); i++ {
			text, _ := sl.AsString(list.Index(i))
			seen.plural = append(seen.plural, text)
		}
	}

	version := &AppVersion{base: base, Actions: map[string]AppAction{
		"-/write":  {Function: "action_write", Public: true, internal_function: count("write")},
		"-/read":   {Function: "action_read", Public: true, Read: true, internal_function: count("read")},
		"-/asset":  {Function: "action_asset", Public: true, Asset: true, internal_function: count("asset")},
		"-/style":  {File: "style.css", Public: true},
		"-/assets": {Files: "assets", Public: true},
		"-/bundle": {Files: "assets", Function: "action_bundle", Public: true, internal_function: count("bundle")},
		"-/page":   {File: "page.html", Function: "action_page", OpenGraph: "page_opengraph", Public: true, internal_function: count("page")},
		"-/view":   {File: "page.html", Function: "action_view", OpenGraph: "page_opengraph", Public: true, Read: true, internal_function: count("view")},
		"-/echo":   {Function: "action_echo", Public: true, Read: true, internal_function: record},
	}}
	app := &App{id: "readtest", internal: version}
	version.app = app
	return app, calls, seen
}

// read_request runs one request through web_action. A body is sent with the
// content type given.
func read_request(t *testing.T, app *App, method string, target string, accept string, content string, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, "/"+app.id+"/"+target, strings.NewReader(body))
	c.Request.Header.Set("Accept", accept)
	if content != "" {
		c.Request.Header.Set("Content-Type", content)
	}
	action, _, _ := strings.Cut(target, "?")
	if !web_action(c, app, action, nil, routing_class) {
		t.Fatalf("web_action did not handle %s %s", method, target)
	}
	return w
}

// A GET or HEAD of an action whose function the app did not mark read is
// refused before the function runs, naming the method it accepts; every other
// method runs it as before.
func TestReadRefusesAnUnmarkedAction(t *testing.T) {
	app, calls, _ := read_app(t)

	for _, method := range []string{"GET", "HEAD"} {
		w := read_request(t, app, method, "-/write", "application/json", "", "")
		if w.Code != http.StatusMethodNotAllowed || !strings.Contains(w.Body.String(), "method_not_allowed") {
			t.Errorf("%s of an unmarked action got %d %s; want 405 method_not_allowed", method, w.Code, w.Body.String())
		}
		if allow := w.Header().Get("Allow"); allow != "POST" {
			t.Errorf("%s of an unmarked action answered Allow %q; want POST", method, allow)
		}
	}
	if calls["write"] != 0 {
		t.Fatalf("THE POINT: a read ran an unmarked action's function %d times", calls["write"])
	}

	for _, method := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		before := calls["write"]
		w := read_request(t, app, method, "-/write", "application/json", "", "")
		if w.Code != http.StatusOK || calls["write"] != before+1 {
			t.Errorf("%s of an unmarked action got %d %s and ran it %d times; want it run once", method, w.Code, w.Body.String(), calls["write"]-before)
		}
	}
}

// An action marked read runs on a GET or HEAD, as does an asset action, which
// is what an <img> reads.
func TestReadRunsAMarkedAction(t *testing.T) {
	app, calls, _ := read_app(t)

	for _, name := range []string{"read", "asset"} {
		for _, method := range []string{"GET", "HEAD"} {
			before := calls[name]
			w := read_request(t, app, method, "-/"+name, "application/json", "", "")
			if w.Code != http.StatusOK || calls[name] != before+1 {
				t.Errorf("%s of -/%s got %d %s and ran it %d times; want it run once", method, name, w.Code, w.Body.String(), calls[name]-before)
			}
		}
	}
}

// A declared file or files directory answers a read with no function run. A
// page declaring a file, a function and an Open Graph function serves its file
// to a browser on any read, and its function only where the app marked it read.
func TestReadServesFiles(t *testing.T) {
	app, calls, _ := read_app(t)

	files := []struct {
		target string
		want   string
	}{
		{"-/style", "the style"},
		{"-/assets/script.js", "the script"},
		// A files directory answers before a function it also declares.
		{"-/bundle/script.js", "the script"},
	}
	for _, file := range files {
		w := read_request(t, app, "GET", file.target, "*/*", "", "")
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), file.want) {
			t.Errorf("GET of %s got %d %s; want 200 with %q", file.target, w.Code, w.Body.String(), file.want)
		}
	}

	for _, name := range []string{"page", "view"} {
		w := read_request(t, app, "GET", "-/"+name, "text/html", "", "")
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "the page") {
			t.Errorf("a browser's GET of -/%s got %d %s; want the page's file", name, w.Code, w.Body.String())
		}
	}
	if calls["page"] != 0 || calls["view"] != 0 || calls["bundle"] != 0 {
		t.Errorf("a GET answered with a file ran a function: page %d, view %d, bundle %d", calls["page"], calls["view"], calls["bundle"])
	}

	w := read_request(t, app, "GET", "-/page", "application/json", "", "")
	if w.Code != http.StatusMethodNotAllowed || calls["page"] != 0 {
		t.Errorf("THE POINT: an API GET of a page not marked read got %d %s and ran its function %d times; want 405 and none", w.Code, w.Body.String(), calls["page"])
	}
	w = read_request(t, app, "POST", "-/page", "application/json", "", "")
	if w.Code != http.StatusOK || calls["page"] != 1 {
		t.Errorf("an API POST of a page got %d %s and ran its function %d times; want it run once", w.Code, w.Body.String(), calls["page"])
	}
	w = read_request(t, app, "GET", "-/view", "application/json", "", "")
	if w.Code != http.StatusOK || calls["view"] != 1 {
		t.Errorf("an API GET of a page marked read got %d %s and ran its function %d times; want it run once", w.Code, w.Body.String(), calls["view"])
	}
}

// The engines that answer their own methods, and git under a repository's
// domain, are not the method gate's to refuse.
func TestReadLeavesEnginesTheirMethods(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name   string
		action AppAction
		git    bool
	}{
		{"carddav", AppAction{Feature: "carddav", Function: "action_dav"}, false},
		{"caldav", AppAction{Feature: "caldav", Function: "action_dav"}, false},
		{"git feature", AppAction{Feature: "git", Function: "action_git"}, false},
		{"git route", AppAction{Function: "action_page"}, true},
	}
	for _, tt := range cases {
		for _, method := range []string{"GET", "HEAD"} {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(method, "/", nil)
			if web_read_refused(c, &tt.action, tt.git) {
				t.Errorf("%s: %s refused; the engine answers its own methods", tt.name, method)
			}
		}
	}

	// Control: the same function with no engine is refused.
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/", nil)
	if !web_read_refused(c, &AppAction{Function: "action_page"}, false) {
		t.Error("CONTROL: a GET of an unmarked function was not refused")
	}
}

// Only a read takes its inputs from the query string. Any other method takes
// them from the route, the body and its form, so a request steered to a path a
// page built from data cannot also choose its inputs.
func TestReadInputsFromTheQuery(t *testing.T) {
	app, calls, seen := read_app(t)
	form := "application/x-www-form-urlencoded"

	cases := []struct {
		name    string
		method  string
		target  string
		content string
		body    string
		single  string
		plural  []string
	}{
		{"GET with the query", "GET", "-/echo?x=1", "", "", "1", []string{"1"}},
		{"HEAD with the query", "HEAD", "-/echo?x=1", "", "", "1", []string{"1"}},
		{"THE POINT: POST with only the query", "POST", "-/echo?x=1", "", "", "", []string{}},
		{"PUT with only the query", "PUT", "-/echo?x=1", "", "", "", []string{}},
		{"POST with the query and a form", "POST", "-/echo?x=1", form, "x=2", "2", []string{"2"}},
		{"POST with the query and JSON", "POST", "-/echo?x=1", "application/json", `{"x":"3"}`, "3", []string{}},
		{"POST with a form", "POST", "-/echo", form, "x=4", "4", []string{"4"}},
	}
	for _, tt := range cases {
		before := calls["echo"]
		*seen = read_inputs{}
		w := read_request(t, app, tt.method, tt.target, "application/json", tt.content, tt.body)
		if w.Code != http.StatusOK || calls["echo"] != before+1 {
			t.Errorf("%s: got %d %s; want the action run", tt.name, w.Code, w.Body.String())
			continue
		}
		if seen.single != tt.single {
			t.Errorf("%s: a.input(\"x\") = %q, want %q", tt.name, seen.single, tt.single)
		}
		if !slices.Equal(seen.plural, tt.plural) {
			t.Errorf("%s: a.inputs(\"x\") = %q, want %q", tt.name, seen.plural, tt.plural)
		}
	}
}
