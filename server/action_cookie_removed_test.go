// Mochi server: apps have no cookie API.
//
// a.cookie.get(name) read any request cookie by caller-supplied name with no filter
// and no permission check, and web_auth authenticates on the session cookie alone -
// so it handed any installed app the user's bearer credential. set and unset were the
// same hole in the other direction. Apps keep per-user state in their own database
// and browser state in the shell's storage proxy; neither needs cookies.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"strings"
	"testing"
)

// TestActionHasNoCookieAttribute is the regression: resolving a.cookie must not
// hand back an object. Starlark reads (nil, nil) as "no such attribute", which
// is what an app calling a.cookie.get() now gets.
func TestActionHasNoCookieAttribute(t *testing.T) {
	value, err := (&Action{}).Attr("cookie")
	if err != nil {
		t.Fatalf("a.cookie returned an error rather than an absent attribute: %v", err)
	}
	if value != nil {
		t.Errorf("a.cookie resolved to %T; apps must have no cookie API at all", value)
	}
}

// TestActionAttrNamesOmitsCookie. AttrNames drives introspection and error
// messages, so a name left there advertises an API that no longer answers.
func TestActionAttrNamesOmitsCookie(t *testing.T) {
	for _, name := range (&Action{}).AttrNames() {
		if name == "cookie" {
			t.Error("AttrNames still advertises \"cookie\"")
		}
	}
}

// TestNoCookieTypeRemains guards against a partial removal: the arm could be
// deleted while the type and its builtins stay behind, leaving the credential
// reachable if anything re-registers them.
func TestNoCookieTypeRemains(t *testing.T) {
	source, err := os.ReadFile("actions.go")
	if err != nil {
		t.Fatalf("reading actions.go: %v", err)
	}
	if strings.Contains(string(source), "ActionCookie") {
		t.Error("ActionCookie still exists in actions.go; the type and its get/set/unset builtins must go with the attribute")
	}
}

// TestCoreCookieHelpersSurvive is the other direction. Removing the app-facing
// API must not touch core's own cookie handling, which is what authentication,
// OAuth and the shell run on.
func TestCoreCookieHelpersSurvive(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("reading web.go: %v", err)
	}
	text := string(source)
	for _, want := range []string{"func web_cookie_get(", "func web_cookie_set(", "func web_cookie_unset("} {
		if !strings.Contains(text, want) {
			t.Errorf("core lost %s; authentication and OAuth depend on it", want)
		}
	}
}
