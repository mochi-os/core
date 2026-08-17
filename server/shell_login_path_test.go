// Mochi server: the shell's login exemption follows the login_app setting.
//
// shell_wrap_candidate matched the literal "/login", but the login app's path
// is the login_app setting, and a literal is wrong in both directions. Rename
// the login app and its interstitials start being shell-wrapped, which loads
// them into the sandboxed iframe with their cookies stripped and loops - the
// #414 class, which app_is_login already carries a comment about. Bind some
// other app to the path `login` and it stops being wrapped, handing it exactly
// what the wrap denies: its bundle running top-level, same-origin and
// cookie-bearing, where POST /_/token mints a JWT for every installed app.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// shell_wraps reports whether a top-level GET for path would be shell-wrapped.
// The headers are a plain browser navigation, so the only thing under test is
// the path decision.
func shell_wraps(path string) bool {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", path, nil)
	c.Request.Header.Set("Sec-Fetch-Dest", "document")
	c.Request.Header.Set("Accept", "text/html")
	return shell_wrap_candidate(c)
}

// shell_login_app_set points the login_app setting at a path, as an
// administrator would. Mirrors the override the app_login_owns tests use.
func shell_login_app_set(t *testing.T, path string) {
	t.Helper()
	db := db_open("db/settings.db")
	db.exec("create table if not exists settings (name text primary key, value text not null)")
	db.exec("replace into settings (name, value) values ('login_app', ?)", path)
}

// TestShellExemptsTheDefaultLoginPath is the control: with the setting at its
// default the behaviour is unchanged, so the two tests below are measuring the
// setting rather than a blanket change.
func TestShellExemptsTheDefaultLoginPath(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()
	shell_login_app_set(t, "login")

	if shell_wraps("/login") {
		t.Error("/login must not be shell-wrapped: its interstitials need their cookies")
	}
	if shell_wraps("/login/identity") {
		t.Error("/login/identity must not be shell-wrapped")
	}
	if !shell_wraps("/feeds") {
		t.Error("an ordinary app must still be shell-wrapped")
	}
}

// TestShellExemptionFollowsARenamedLoginApp is the first failure direction. An
// operator who repoints login_app must not have the new login app wrapped.
func TestShellExemptionFollowsARenamedLoginApp(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()
	shell_login_app_set(t, "welcome")

	if shell_wraps("/welcome") {
		t.Error("the renamed login app is shell-wrapped: its interstitials load in the sandboxed iframe with cookies stripped, and loop")
	}
	if shell_wraps("/welcome/identity") {
		t.Error("the renamed login app's sub-paths are shell-wrapped")
	}
}

// TestShellWrapsAnAppMerelyBoundToLogin is the second, sharper direction. Once
// login_app points elsewhere, an app sitting on the path `login` is an
// ordinary app and must be wrapped like any other - the literal let it opt out
// of the sandbox by choosing its path.
func TestShellWrapsAnAppMerelyBoundToLogin(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()
	shell_login_app_set(t, "welcome")

	if !shell_wraps("/login") {
		t.Error("an app on the path `login` escaped the shell after login_app moved away: its bundle would run top-level, same-origin and cookie-bearing")
	}
	if !shell_wraps("/login/anything") {
		t.Error("sub-paths of a non-login app on `login` escaped the shell")
	}
}

// TestShellAndClosingGateAgreeOnTheLoginApp. Two shell-level gates ask which
// paths belong to the login app; asking it the same way is what stops them
// disagreeing, the same argument shell_wrap_candidate already makes for
// sharing shell_resource_path with web_resource_guard.
func TestShellAndClosingGateAgreeOnTheLoginApp(t *testing.T) {
	cleanup := create_test_routing_env(t)
	defer cleanup()

	for _, bound := range []string{"login", "welcome"} {
		shell_login_app_set(t, bound)
		for _, path := range []string{"/login", "/login/identity", "/welcome", "/welcome/identity", "/feeds"} {
			owned := app_login_owns(trim_slashes(path))
			if shell_wraps(path) == owned {
				t.Errorf("login_app=%q path=%q: shell wrap and login ownership disagree (owned=%v)", bound, path, owned)
			}
		}
	}
}

// trim_slashes matches how the closing gate in web.go normalises a request
// path before asking app_login_owns.
func trim_slashes(path string) string {
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}
	return path
}
