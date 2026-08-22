// Mochi server: an anonymous caller is nobody, not the owner.
//
// The account whose data is read and the identity a request carries are
// separate questions; principal_storage answers the first. These tests pin the
// binding and the permission gates a public action can reach, which must
// tolerate a nil caller.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	sl "go.starlark.net/starlark"
)

// anonymous_reachable is the audited set: every permission-gated API a
// public: true action can reach, by walking each app's Starlark call graph
// from its public entry points. Each must tolerate a nil caller, because a
// crawler or a webhook reaches all of them with no identity at all.
var anonymous_reachable = []string{
	"api_access_check",
	"api_account_list",
	"api_ai_prompt",
	"api_entity_owned",
	"api_interests_bottom",
	"api_interests_list",
	"api_interests_summary",
	"api_interests_top",
}

// TestAnonymousCallerIsNotTheOwner is the finding. Neither dispatcher may
// substitute the owner for a caller who never authenticated.
func TestAnonymousCallerIsNotTheOwner(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("read web.go: %v", err)
	}
	text := string(source)

	if strings.Contains(text, "effective = owner") {
		t.Error("a dispatcher still substitutes the owner for an anonymous caller: apps are told a stranger is the owner")
	}
	for _, fn := range []string{"web_action", "web_serve_file_with_opengraph"} {
		body := function_source(t, text, fn)
		if !strings.Contains(body, `s.set("user", user)`) {
			t.Errorf("%s does not bind the real caller", fn)
		}
	}
}

// Strict require_permission returns "no user context" before it looks at a
// grant, so any of these left strict turns its public route into a 500.
func TestReachableGatesTolerateAnAnonymousCaller(t *testing.T) {
	sources := map[string]string{}
	for _, name := range []string{"access.go", "accounts.go", "ai.go", "entities.go", "interests.go"} {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		sources[name] = string(body)
	}

	for _, fn := range anonymous_reachable {
		found := false
		for name, text := range sources {
			if !strings.Contains(text, "func "+fn+"(") {
				continue
			}
			found = true
			body := function_source(t, text, fn)
			switch {
			case strings.Contains(body, "require_permission(t, fn,"):
				t.Errorf("%s (%s) uses strict require_permission: a public action reaching it gets 500 \"no user context\" instead of an answer", fn, name)
			case !strings.Contains(body, "require_permission_acting(t, fn,"):
				t.Errorf("%s (%s) has no permission check at all", fn, name)
			}
		}
		if !found {
			t.Errorf("%s not found: the audited set has drifted from the code", fn)
		}
	}
}

// TestAnonymousCallerStillReachesTheOwnersData. Removing the substitution must
// not cost a public page its data: principal_storage answers the storage
// question separately, and falls back to the owner when nobody is bound.
func TestAnonymousCallerStillReachesTheOwnersData(t *testing.T) {
	owner := &User{UID: "u-owner", Username: "owner@example.com"}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("owner", owner)
	// No "user" local: exactly what a public action now sees anonymously.

	if caller := principal_caller(thread); caller != nil {
		t.Errorf("caller resolved to %v, want nil - anonymous must be representable", caller)
	}
	storage, err := principal_storage(thread)
	if err != nil {
		t.Fatalf("principal_storage refused an anonymous public request: %v", err)
	}
	if storage.UID != owner.UID {
		t.Errorf("reads resolved to %q, want the owner %q - the public page would render empty", storage.UID, owner.UID)
	}
}

// TestActingGatesResolveAgainstStorage confirms what _acting buys: with no
// caller, the grant consulted is the storage account's. A gate that resolved
// the caller instead would refuse, and one that ignored storage would consult
// the wrong account.
func TestActingGatesResolveAgainstStorage(t *testing.T) {
	create_test_routing_env(t)

	owner := &User{UID: "u-owner", Username: "owner@example.com"}
	app := create_external_app("public-app")
	apps[app.id] = app
	t.Cleanup(func() { delete(apps, app.id) })

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("app", app)
	thread.SetLocal("owner", owner)

	// Ungranted: a refusal naming the permission, NOT "no user context".
	err := require_permission_acting(thread, sl.NewBuiltin("probe", nil), "interests/read")
	if err == nil {
		t.Fatal("an ungranted app was allowed")
	}
	if strings.Contains(err.Error(), "no user context") {
		t.Errorf("refused with %v - the gate never reached the grant lookup, so the public route 500s", err)
	}

	// Granted to the OWNER, and the anonymous call is admitted on it.
	db := db_user(owner, "user")
	db.permissions_setup()
	db.permissions_upsert(app.id, "interests/read", "", 1)
	if err := require_permission_acting(thread, sl.NewBuiltin("probe", nil), "interests/read"); err != nil {
		t.Errorf("the owner's grant did not admit the anonymous call: %v", err)
	}
}

// TestAuthenticatedCallerIsUnaffected. The change is about anonymous requests;
// a caller who authenticated must still be themselves, and must still be the
// account their own permissions are checked against.
func TestAuthenticatedCallerIsUnaffected(t *testing.T) {
	owner := &User{UID: "u-owner", Username: "owner@example.com"}
	caller := &User{UID: "u-caller", Username: "caller@example.com"}

	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("owner", owner)
	thread.SetLocal("user", caller)

	if got := principal_caller(thread); got == nil || got.UID != caller.UID {
		t.Errorf("caller resolved to %v, want %q", got, caller.UID)
	}
	storage, err := principal_storage(thread)
	if err != nil {
		t.Fatalf("principal_storage: %v", err)
	}
	if storage.UID != caller.UID {
		t.Errorf("storage resolved to %q, want the caller %q when no storage is pinned", storage.UID, caller.UID)
	}
}

// function_source returns one Go function's body from a file's text.
func function_source(t *testing.T, text, name string) string {
	t.Helper()
	start := strings.Index(text, "\nfunc "+name+"(")
	if start < 0 {
		t.Fatalf("function %s not found", name)
	}
	rest := text[start+1:]
	end := regexp.MustCompile(`(?m)^\}$`).FindStringIndex(rest)
	if end == nil {
		t.Fatalf("could not find the end of %s", name)
	}
	return rest[:end[1]]
}
