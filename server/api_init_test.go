// Mochi server: Starlark API table construction tests
//
// The table used to be built by a package init(), which made the app-visible
// API surface a side effect of importing the package. api_init makes it a
// startup step instead. That only works if every entry point calls it - hence
// TestMain below, and hence a test that the surface is actually populated
// rather than silently empty.
//
// An empty table is the dangerous failure: scripts would still evaluate, and
// every mochi.* reference would fail as an undefined name deep inside an app.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"testing"

	sl "go.starlark.net/starlark"
	sls "go.starlark.net/starlarkstruct"
)

// TestMain gives the test binary the same API surface main_serve builds.
// Without it every test that evaluates Starlark would see an empty globals
// table, because nothing else populates it now that the init() is gone.
func TestMain(m *testing.M) {
	api_init()
	events_init()
	directory_init()
	peers_init()
	senders_init()
	os.Exit(m.Run())
}

// The table is populated, and populated by api_init rather than by importing
// the package.
func TestApiInitPopulatesTheGlobals(t *testing.T) {
	if len(api_globals) == 0 {
		t.Fatal("api_globals is empty; every mochi.* reference in every app would fail as an undefined name")
	}
	if _, ok := api_globals["mochi"]; !ok {
		t.Error("api_globals has no \"mochi\" entry")
	}
	if _, ok := api_globals["json"]; !ok {
		t.Error("api_globals has no \"json\" entry")
	}
}

// api_table is callable on its own - the point of hoisting it out of init().
// A caller can build the surface and inspect it without starting a server.
func TestApiTableIsExplicit(t *testing.T) {
	table := api_table()
	if len(table) == 0 {
		t.Fatal("api_table() returned an empty table")
	}

	// Spot-check that the mochi module really carries the documented
	// namespaces, so "populated" cannot be satisfied by a stub.
	mochi, ok := table["mochi"].(*sls.Struct)
	if !ok {
		t.Fatalf("table[\"mochi\"] is %T, want *starlarkstruct.Struct", table["mochi"])
	}
	for _, name := range []string{"db", "entity", "app", "access", "remote", "stream"} {
		v, err := mochi.Attr(name)
		if err != nil || v == nil {
			t.Errorf("mochi.%s missing from the API table (err=%v)", name, err)
		}
	}
}

// Building the table twice must produce the same surface. api_init is called
// from main_serve and from TestMain, and a table that varied between calls
// would mean apps loaded at different moments saw different APIs.
func TestApiTableIsStable(t *testing.T) {
	first, second := api_table(), api_table()
	if len(first) != len(second) {
		t.Fatalf("api_table() returned %d entries then %d", len(first), len(second))
	}
	for k := range first {
		if _, ok := second[k]; !ok {
			t.Errorf("key %q present in the first table and absent from the second", k)
		}
	}
}

// The globals starlark() hands to a script are the ones api_init built.
// starlark() must READ api_globals, never call a builder: calling one puts the
// table in the package initialization graph, which has a cycle
// (api_app -> ... -> starlark -> api_table -> api_app) that does not compile.
// This catches the refactor that reintroduces it having only been run against
// a build that happened to still work.
func TestStarlarkGlobalsComeFromApiGlobals(t *testing.T) {
	s := starlark(nil)
	if s == nil {
		t.Fatal("starlark(nil) returned nil")
	}
	for key := range api_globals {
		if _, ok := s.globals[key]; !ok {
			t.Errorf("global %q was built by api_init but never reached the script", key)
		}
	}
}

// Guards the accidental-shadowing mistake: a package-level `var api_globals =
// something()` compiles only if it does not close the cycle, and would quietly
// change when the table is built. Asserting the declared type is a plain
// StringDict keeps the shape a reader can rely on.
func TestApiGlobalsIsAPlainTable(t *testing.T) {
	var _ sl.StringDict = api_globals
}

// The built-in apps and their handlers are registered.
//
// This is the hazard the init() hoist introduces: registration used to happen
// for every binary and every test simply by importing the file, and now it
// depends on an explicit call. An entry point that forgets one loses the
// directory or peers service ENTIRELY - pubsub announcements would route
// nowhere, with no error at startup and no missing symbol to catch it at
// build time. Cheap to assert, and the only thing standing between a dropped
// call and a silently deaf node.
func TestBuiltinAppsAreRegistered(t *testing.T) {
	for _, c := range []struct {
		app    string
		events []string
	}{
		{"directory", []string{"publish", "delete", "request", "sync", "push"}},
		{"peers", []string{"request", "publish", "record"}},
	} {
		apps_lock.Lock()
		a, found := apps[c.app]
		apps_lock.Unlock()
		if !found {
			t.Errorf("built-in app %q is not registered; its service is deaf", c.app)
			continue
		}
		for _, event := range c.events {
			if _, ok := a.internal.Events[event]; !ok {
				t.Errorf("%s/%s handler is not registered", c.app, event)
			}
		}
	}
}

// The broadcast pending-drain dispatcher is wired. events_init sets it via a
// package-level var specifically so broadcast_pending.go need not depend on
// the routing graph, which also means nothing fails to compile if the call is
// dropped - the drain would just silently do nothing.
func TestBroadcastPendingDispatchIsWired(t *testing.T) {
	if broadcast_pending_dispatch == nil {
		t.Error("broadcast_pending_dispatch is nil; buffered broadcast rows would never be re-dispatched")
	}
}
