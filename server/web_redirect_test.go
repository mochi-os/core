// Mochi server: plain-HTTP listener tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/crypto/acme/autocert"
)

// TestRedirectHTTPS pins that the plain listener sends callers to HTTPS with
// the path and query intact — a redirect that drops either silently breaks
// every bookmarked deep link.
func TestRedirectHTTPS(t *testing.T) {
	// Targets are origin-form, as a server receives them. Passing an absolute
	// URL to httptest.NewRequest would set RequestURI to the whole URL, which
	// no real request carries, and the redirect builds its Location from Host
	// plus RequestURI.
	tests := []struct {
		target string
		want   string
	}{
		{"/", "https://mochi-os.org/"},
		{"/some/path", "https://mochi-os.org/some/path"},
		{"/some/path?q=1&r=2", "https://mochi-os.org/some/path?q=1&r=2"},
	}
	for _, test := range tests {
		request := httptest.NewRequest("GET", test.target, nil)
		request.Host = "mochi-os.org"
		recorder := httptest.NewRecorder()
		web_redirect_https(recorder, request)

		if recorder.Code != http.StatusMovedPermanently {
			t.Errorf("%s: status %d, want %d", test.target, recorder.Code, http.StatusMovedPermanently)
		}
		if got := recorder.Header().Get("Location"); got != test.want {
			t.Errorf("%s: Location %q, want %q", test.target, got, test.want)
		}
	}
}

// TestACMEChallengeIsNotRedirected pins the composition the :80 listener
// serves: autocert's handler in front of the HTTPS redirect.
//
// Order is the whole point. HTTP-01 validation fetches
// /.well-known/acme-challenge/<token> over plain HTTP, so if the redirect saw
// that path first it would answer 301 and validation could never complete —
// no certificate would ever be issued. Everything else must still redirect.
func TestACMEChallengeIsNotRedirected(t *testing.T) {
	manager := &autocert.Manager{
		Prompt: autocert.AcceptTOS,
		Cache:  autocert.DirCache(t.TempDir()),
	}
	handler := manager.HTTPHandler(http.HandlerFunc(web_redirect_https))

	// A challenge path must be answered by autocert, not redirected. With no
	// challenge outstanding it has nothing to serve, so anything other than a
	// redirect means the request reached the manager.
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, challenge_request(t, "/.well-known/acme-challenge/probe"))
	if recorder.Code == http.StatusMovedPermanently {
		t.Errorf("an ACME challenge was redirected to HTTPS (%d %q); HTTP-01 validation could never complete",
			recorder.Code, recorder.Header().Get("Location"))
	}

	// Everything else still goes to HTTPS.
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, challenge_request(t, "/feeds/"))
	if recorder.Code != http.StatusMovedPermanently {
		t.Errorf("an ordinary request returned %d, want a redirect to HTTPS", recorder.Code)
	}
	if got := recorder.Header().Get("Location"); got != "https://mochi-os.org/feeds/" {
		t.Errorf("Location %q, want %q", got, "https://mochi-os.org/feeds/")
	}
}

// challenge_request builds an origin-form request for mochi-os.org, matching
// what the listener actually receives.
func challenge_request(t *testing.T, target string) *http.Request {
	t.Helper()
	request := httptest.NewRequest("GET", target, nil)
	request.Host = "mochi-os.org"
	return request
}
