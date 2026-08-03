// Mochi server: plain-HTTP listener tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/crypto/acme/autocert"
)

// acme_manager gives the package a certificate manager for the duration of the
// test. web_https_serves follows the resolver all the way to the manager, so
// with none configured there is nothing that could issue a certificate and no
// host without a manual one is redirected.
func acme_manager(t *testing.T) {
	t.Helper()
	original := domains_acme_manager
	domains_acme_manager = &autocert.Manager{
		Prompt: autocert.AcceptTOS,
		Cache:  autocert.DirCache(t.TempDir()),
	}
	t.Cleanup(func() { domains_acme_manager = original })
}

// serving_domain registers domains in the state an in-service domain is in:
// verified, with automatic certificates on and a manager to issue them. A bare
// domain_register leaves verified=0, which under the default verification
// policy describes a domain that cannot yet serve HTTPS.
func serving_domain(t *testing.T, names ...string) {
	t.Helper()
	acme_manager(t)
	for _, name := range names {
		if _, err := domain_register(name); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
		db_open("db/domains.db").exec("update domains set verified=1 where domain=?", name)
	}
}

// TestRedirectHTTPS pins that the plain listener sends callers to HTTPS with
// the path and query intact — a redirect that drops either silently breaks
// every bookmarked deep link.
func TestRedirectHTTPS(t *testing.T) {
	cleanup := create_domains_test_env(t)
	defer cleanup()
	serving_domain(t, "mochi-os.org")

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
	cleanup := create_domains_test_env(t)
	defer cleanup()
	serving_domain(t, "mochi-os.org")

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

// TestRedirectHTTPSHost pins that the redirect target is validated rather than
// reflected. Host is supplied by the caller, and domains_get_certificate serves
// only names the domains table holds, so an unconfigured host must not become a
// permanent redirect this server issues in its own name.
func TestRedirectHTTPSHost(t *testing.T) {
	cleanup := create_domains_test_env(t)
	defer cleanup()
	serving_domain(t, "mochi-os.org", "*.example.com")

	tests := []struct {
		host   string
		status int
		want   string // the Location, empty when none should be sent
	}{
		{"mochi-os.org", http.StatusMovedPermanently, "https://mochi-os.org/some/path"},
		// A wildcard row covers the subdomain, and the Location carries the
		// REQUESTED name: the row's own domain is the pattern itself, which
		// would be no use as a destination.
		{"foo.example.com", http.StatusMovedPermanently, "https://foo.example.com/some/path"},
		// The port is dropped, so a configured domain cannot be borrowed to
		// bounce a caller onto an arbitrary port.
		{"mochi-os.org:8080", http.StatusMovedPermanently, "https://mochi-os.org/some/path"},
		// An unconfigured host is refused outright rather than reflected.
		{"attacker.example", http.StatusNotFound, ""},
		// The wildcard covers subdomains, not the apex.
		{"example.com", http.StatusNotFound, ""},
	}
	for _, test := range tests {
		request := httptest.NewRequest("GET", "/some/path", nil)
		request.Host = test.host
		recorder := httptest.NewRecorder()
		web_redirect_https(recorder, request)

		if recorder.Code != test.status {
			t.Errorf("%s: status %d, want %d", test.host, recorder.Code, test.status)
		}
		if got := recorder.Header().Get("Location"); got != test.want {
			t.Errorf("%s: Location %q, want %q", test.host, got, test.want)
		}
	}
}

// TestRedirectHTTPSIssuanceBlocked pins the two conditions that stop a
// certificate being issued for an otherwise well-formed domain. Both sit
// behind the ACME manager rather than in the domains table, so a predicate
// that reads the table alone reports HTTPS as available and sends callers to a
// handshake that is refused.
func TestRedirectHTTPSIssuanceBlocked(t *testing.T) {
	cleanup := create_domains_test_env(t)
	defer cleanup()

	// Unverified while the verification policy is on. This is the DEFAULT state
	// of a freshly registered domain, not an exotic one: domain_register writes
	// verified=0 and domains_verification defaults to true.
	acme_manager(t)
	if _, err := domain_register("unverified.example"); err != nil {
		t.Fatalf("register: %v", err)
	}
	setting_set("domains_verification", "true")

	request := httptest.NewRequest("GET", "/some/path", nil)
	request.Host = "unverified.example"
	recorder := httptest.NewRecorder()
	web_redirect_https(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Errorf("unverified domain: status %d, want %d", recorder.Code, http.StatusNotFound)
	}

	// Verifying it is the only thing that changes, and it now redirects.
	db_open("db/domains.db").exec("update domains set verified=1 where domain=?", "unverified.example")
	recorder = httptest.NewRecorder()
	web_redirect_https(recorder, request)
	if recorder.Code != http.StatusMovedPermanently {
		t.Errorf("verified domain: status %d, want %d", recorder.Code, http.StatusMovedPermanently)
	}

	// No manager configured at all: nothing can issue, so nothing is promised.
	original := domains_acme_manager
	domains_acme_manager = nil
	defer func() { domains_acme_manager = original }()
	recorder = httptest.NewRecorder()
	web_redirect_https(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Errorf("no ACME manager: status %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

// TestRedirectHTTPSManualCertificate pins that a manually certificated host is
// still redirected even with no row in the domains table. domains_get_certificate
// tries the manual map first, so HTTPS serves such a host; refusing to send
// callers there would strand it on plain HTTP. Wildcard certificates make this
// ordinary rather than exotic — ACME issues none over TLS-ALPN-01 or HTTP-01,
// so every wildcard is manual and its subdomains need no rows of their own.
func TestRedirectHTTPSManualCertificate(t *testing.T) {
	cleanup := create_domains_test_env(t)
	defer cleanup()

	original := domains_certs
	domains_certs = map[string]*tls.Certificate{"*.manual.example": {}}
	defer func() { domains_certs = original }()

	tests := []struct {
		host   string
		status int
		want   string
	}{
		{"host.manual.example", http.StatusMovedPermanently, "https://host.manual.example/some/path"},
		// Nothing covers this one: no certificate and no row.
		{"other.example", http.StatusNotFound, ""},
	}
	for _, test := range tests {
		request := httptest.NewRequest("GET", "/some/path", nil)
		request.Host = test.host
		recorder := httptest.NewRecorder()
		web_redirect_https(recorder, request)

		if recorder.Code != test.status {
			t.Errorf("%s: status %d, want %d", test.host, recorder.Code, test.status)
		}
		if got := recorder.Header().Get("Location"); got != test.want {
			t.Errorf("%s: Location %q, want %q", test.host, got, test.want)
		}
	}
}

// TestRedirectHTTPSAutomaticCertificatesOff pins what the domains table's tls
// column does to the redirect. Switching a domain off means no ACME issuance,
// so unless a certificate was installed by hand there is nothing for the
// handshake to present and sending callers to HTTPS strands them. A manual
// certificate is checked first and overrides the column, exactly as
// domains_get_certificate does.
func TestRedirectHTTPSAutomaticCertificatesOff(t *testing.T) {
	cleanup := create_domains_test_env(t)
	defer cleanup()
	serving_domain(t, "bare.example", "certificated.example")
	db_open("db/domains.db").exec("update domains set tls=0")

	original := domains_certs
	domains_certs = map[string]*tls.Certificate{"certificated.example": {}}
	defer func() { domains_certs = original }()

	tests := []struct {
		host   string
		status int
		want   string
	}{
		// Registered, but nothing can answer the handshake.
		{"bare.example", http.StatusNotFound, ""},
		// The hand-installed certificate serves it regardless of the column.
		{"certificated.example", http.StatusMovedPermanently, "https://certificated.example/some/path"},
	}
	for _, test := range tests {
		request := httptest.NewRequest("GET", "/some/path", nil)
		request.Host = test.host
		recorder := httptest.NewRecorder()
		web_redirect_https(recorder, request)

		if recorder.Code != test.status {
			t.Errorf("%s: status %d, want %d", test.host, recorder.Code, test.status)
		}
		if got := recorder.Header().Get("Location"); got != test.want {
			t.Errorf("%s: Location %q, want %q", test.host, got, test.want)
		}
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
