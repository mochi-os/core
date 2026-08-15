// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func serves_file(accept string, aa *AppAction) bool {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/app/-/page", nil)
	if accept != "" {
		c.Request.Header.Set("Accept", accept)
	}
	return web_serves_file(c, aa)
}

// TestServesFileDecidesTheAuthBypass. shell_static skips three checks - the
// authentication gate, the app-token match and user_allowed - and it used to
// be true whenever an action DECLARED a file. Two requests took that bypass
// without ever reaching a file: an action carrying file+function+opengraph
// reached its FUNCTION on a non-HTML Accept, and a files action with no
// suffix skipped the checks on its way to a 400. Both now answer false here.
func TestServesFileDecidesTheAuthBypass(t *testing.T) {
	plain := &AppAction{File: "index.html"}
	negotiated := &AppAction{File: "index.html", Function: "action_post", OpenGraph: "og_post"}
	functionOnly := &AppAction{Function: "action_post"}

	for _, test := range []struct {
		name   string
		accept string
		action *AppAction
		want   bool
	}{
		{"a plain file action always serves the file", "application/json", plain, true},
		{"a plain file action, no Accept", "", plain, true},

		// The latent hole the finding names. Only two actions in the app tree
		// carry all three today and both are public, so nothing is exposed
		// yet; a third one without public would have been.
		{"negotiated, HTML - serves the file", "text/html", negotiated, true},
		{"negotiated, JSON - runs the FUNCTION, so no bypass", "application/json", negotiated, false},
		{"negotiated, wildcard - runs the function", "*/*", negotiated, false},
		{"negotiated, no Accept - runs the function", "", negotiated, false},
		{"negotiated, HTML and JSON both - runs the function", "text/html,application/json", negotiated, false},

		{"an action with no file never serves one", "text/html", functionOnly, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := serves_file(test.accept, test.action); got != test.want {
				t.Errorf("web_serves_file(Accept=%q) = %v, want %v", test.accept, got, test.want)
			}
		})
	}
}

// TestServesFileMatchesTheServingBranch: the bypass and the branch that
// actually writes the file ask the same function, so they cannot disagree
// about which of file-or-function an action is about to do. This is the
// property that makes the narrowing safe - if they diverged, either the
// bypass would be too wide again or a legitimate file would 403.
func TestServesFileMatchesTheServingBranch(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("read web.go: %v", err)
	}
	text := string(source)

	if n := strings.Count(text, "web_serves_file(c, aa)"); n != 2 {
		t.Errorf("web_serves_file is called %d times in web.go, want exactly 2 - the shell_static bypass and the serving branch", n)
	}
	if strings.Count(text, "shell_static := web_serves_file(c, aa)") != 1 {
		t.Error("shell_static no longer derives from web_serves_file; the bypass and the serving branch can now disagree")
	}
}
