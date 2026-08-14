// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for the resource-route response guard.
//
// The shell exempts attachment and git URLs from wrapping, decided from the raw
// path — which apps author. An app that names an action so the path matches has
// its own HTML served top-level, same-origin and cookie-bearing, where
// POST /_/token mints app JWTs. The guard sandboxes any resource-route response
// that arrives as an executable document, so the path an app chose no longer
// decides whether its script runs with the user's session.

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// resource_guard_response runs the guard around a handler that answers with the
// given content type, and returns the recorded response.
func resource_guard_response(path, content_type, body string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	router := gin.New()
	router.Use(web_resource_guard)
	router.GET("/*any", func(c *gin.Context) {
		if content_type != "" {
			c.Header("Content-Type", content_type)
		}
		c.String(http.StatusOK, body)
	})
	request := httptest.NewRequest("GET", path, nil)
	request.Header.Set("Sec-Fetch-Dest", "document")
	request.Header.Set("Accept", "text/html")
	router.ServeHTTP(w, request)
	return w
}

// TestResourceGuardSandboxesHTMLOnGitPath: the exploit. An app declares an
// action whose path carries "/git/", so the shell does not wrap it; serving its
// HTML with a real origin is what the guard has to prevent.
func TestResourceGuardSandboxesHTMLOnGitPath(t *testing.T) {
	w := resource_guard_response("/evil/git/page", "text/html; charset=utf-8", "<html>payload</html>")

	if policy := w.Header().Get("Content-Security-Policy"); policy != shell_resource_policy {
		t.Errorf("Content-Security-Policy = %q, want %q — an app's HTML on a resource path would run top-level, same-origin and cookie-bearing", policy, shell_resource_policy)
	}
}

// TestResourceGuardSandboxesHTMLOnAttachmentsPath: the other exempt substring,
// reachable the same way.
func TestResourceGuardSandboxesHTMLOnAttachmentsPath(t *testing.T) {
	w := resource_guard_response("/evil/-/attachments/page", "text/html", "<html>payload</html>")

	if policy := w.Header().Get("Content-Security-Policy"); policy != shell_resource_policy {
		t.Errorf("Content-Security-Policy = %q, want %q", policy, shell_resource_policy)
	}
}

// TestResourceGuardSandboxesSVG: SVG carries script, which is why
// content_type_inline already refuses to serve it inline.
func TestResourceGuardSandboxesSVG(t *testing.T) {
	w := resource_guard_response("/repo/git/logo", "image/svg+xml", "<svg/>")

	if policy := w.Header().Get("Content-Security-Policy"); policy != shell_resource_policy {
		t.Errorf("Content-Security-Policy = %q, want %q for SVG", policy, shell_resource_policy)
	}
}

// TestResourceGuardLeavesRealResourcesAlone is the regression that protects the
// reason the exemption exists: a PDF needs a real origin or Chrome's viewer
// fails with "Sandbox access violation". A blanket sandbox would re-break it.
func TestResourceGuardLeavesRealResourcesAlone(t *testing.T) {
	for _, content_type := range []string{"application/pdf", "image/png", "video/mp4", "application/x-git-upload-pack-result"} {
		w := resource_guard_response("/repo/-/attachments/file", content_type, "bytes")
		if policy := w.Header().Get("Content-Security-Policy"); policy != "" {
			t.Errorf("content type %q got Content-Security-Policy %q, want none — sandboxing a real resource is what the shell exemption exists to avoid", content_type, policy)
		}
	}
}

// TestResourceGuardIgnoresOrdinaryPaths: an app path that takes no exemption is
// shell-wrapped by the normal route and must not pick up the policy.
func TestResourceGuardIgnoresOrdinaryPaths(t *testing.T) {
	w := resource_guard_response("/feeds/abcdef123", "text/html", "<html>app</html>")

	if policy := w.Header().Get("Content-Security-Policy"); policy != "" {
		t.Errorf("ordinary app path got Content-Security-Policy %q, want none", policy)
	}
}

// TestResourceGuardSandboxesARedirect covers the ordering that the unit tests
// above cannot: c.Redirect calls c.Status(302) first, and http.Redirect sets
// Content-Type only afterwards. Applying the policy on WriteHeader therefore
// read an empty content type and latched, and the redirect — a text/html body
// with a link in it — went out with no policy at all. Found against the running
// server, not here, which is why it now has its own case.
func TestResourceGuardSandboxesARedirect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	router := gin.New()
	router.Use(web_resource_guard)
	router.GET("/*any", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "/")
	})
	router.ServeHTTP(w, httptest.NewRequest("GET", "/evil/git/payload", nil))

	if content_type := w.Header().Get("Content-Type"); !shell_resource_executable(content_type) {
		t.Fatalf("redirect Content-Type = %q; the case this test exists for is an HTML-bodied response", content_type)
	}
	if policy := w.Header().Get("Content-Security-Policy"); policy != shell_resource_policy {
		t.Errorf("Content-Security-Policy = %q, want %q — the policy was applied before the renderer set the content type", policy, shell_resource_policy)
	}
}

// TestResourceGuardIsRegistered checks the middleware is actually in the
// server's chain. The tests above build their own router, so every one of them
// would still pass if the r.Use call were dropped and the guard never ran in
// production. web_start binds its ports as it builds the engine, so there is no
// engine to exercise from a test; asserting on the registration line is the
// cheap way to keep the wiring honest.
func TestResourceGuardIsRegistered(t *testing.T) {
	source, err := os.ReadFile("web.go")
	if err != nil {
		t.Fatalf("reading web.go: %v", err)
	}
	if !strings.Contains(string(source), "r.Use(web_resource_guard)") {
		t.Error("web.go does not register web_resource_guard; the guard's tests would still pass but no request would ever reach it")
	}
}

// TestResourceGuardTracksTheShellExemption pins the pairing the fix depends on:
// every path that skips the shell wrap is one the guard covers. They now share
// shell_resource_path, so this holds by construction — but the property is what
// matters, not the sharing, and re-inlining the list in either place fails
// here.
func TestResourceGuardTracksTheShellExemption(t *testing.T) {
	gin.SetMode(gin.TestMode)
	paths := []string{
		"/evil/git/page",
		"/evil/-/attachments/page",
		"/repositories/abc/git/info/refs",
		"/feeds/abcdef123",
		"/feeds/abcdef123/-/posts",
		"/",
	}
	for _, path := range paths {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", path, nil)
		c.Request.Header.Set("Accept", "text/html")
		c.Request.Header.Set("Sec-Fetch-Dest", "document")

		// A candidate is a path the shell would wrap; the resource exemption is
		// the only reason any of these paths is refused, so the two must be
		// exact opposites across this set.
		candidate := shell_wrap_candidate(c)
		exempt := shell_resource_path(path)
		if candidate == exempt {
			t.Errorf("path %q: shell_wrap_candidate=%v and shell_resource_path=%v; the guard's copy of the exempt list has drifted from shell.go", path, candidate, exempt)
		}
	}
}
