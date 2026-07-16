// Mochi server: sticky-session middleware tests
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

// run_sticky_middleware sets up a minimal gin context, runs the
// middleware, and returns the recorder. The caller can inspect
// Set-Cookie headers via w.Header().Values("Set-Cookie").
func run_sticky_middleware(t *testing.T, cookie string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	orig_p2p_id := net_id
	net_id = "self-peer"
	t.Cleanup(func() { net_id = orig_p2p_id })

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/", nil)
	if cookie != "" {
		c.Request.AddCookie(&http.Cookie{Name: sticky_session_cookie, Value: cookie})
	}
	// Set a destination handler so Next() has somewhere to go.
	c.Handler()
	web_sticky_session(c)
	return w
}

// TestStickySessionStampsCookieWhenAbsent: no cookie → middleware
// stamps our peer-id.
func TestStickySessionStampsCookieWhenAbsent(t *testing.T) {
	w := run_sticky_middleware(t, "")

	cookies := w.Header().Values("Set-Cookie")
	found := false
	for _, c := range cookies {
		if strings.HasPrefix(c, sticky_session_cookie+"=self-peer;") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected Set-Cookie with %s=self-peer; got %v", sticky_session_cookie, cookies)
	}
}

// TestStickySessionPreservesMatchingCookie: cookie already names this
// peer → middleware is a no-op (no Set-Cookie header).
func TestStickySessionPreservesMatchingCookie(t *testing.T) {
	w := run_sticky_middleware(t, "self-peer")

	cookies := w.Header().Values("Set-Cookie")
	for _, c := range cookies {
		if strings.HasPrefix(c, sticky_session_cookie+"=") {
			t.Errorf("middleware should not re-stamp matching cookie; got %s", c)
		}
	}
}

// TestStickySessionReplacesMismatchedCookie: cookie names a different
// peer → middleware replaces it with our peer-id (the request reached
// us, so the cookie was either stale or not honoured).
func TestStickySessionReplacesMismatchedCookie(t *testing.T) {
	w := run_sticky_middleware(t, "some-other-peer")

	cookies := w.Header().Values("Set-Cookie")
	found := false
	for _, c := range cookies {
		if strings.HasPrefix(c, sticky_session_cookie+"=self-peer;") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected middleware to replace stale cookie with self-peer; got %v", cookies)
	}
}
