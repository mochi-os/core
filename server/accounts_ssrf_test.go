// Mochi server: account providers with caller-supplied addresses
//
// A notification account names the address the server will contact. That makes
// every such provider a request the caller can aim, issued from wherever the
// server sits - so it has to go through url_transport, whose dialer checks the
// resolved address on every hop, rather than a string test on the URL.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

// with_private_allowed runs f with the guard off, so a case can show that a
// destination is reachable but for the guard, and restores it immediately
// afterwards - t.Cleanup would not, since it fires at the end of the test
// rather than the end of the block.
//
// Idle connections are dropped on both edges. url_transport pools them and the
// dialer's Control hook only runs on a fresh dial, so a connection opened while
// the guard was off would otherwise be reused by the very request that is meant
// to be refused.
func with_private_allowed(t *testing.T, f func()) {
	t.Helper()
	previous := url_allow_private
	url_allow_private = true
	url_transport.CloseIdleConnections()
	f()
	url_allow_private = previous
	url_transport.CloseIdleConnections()
}

// private_endpoints_allowed does the same for a whole test, for delivery tests
// that stand up a stub server on 127.0.0.1 and are exercising the delivery
// mechanics rather than the guard.
//
// They need it explicitly: a blocked request fails, and a test asserting that
// delivery FAILS (on 410 Gone, say) would keep passing while no longer testing
// anything - the guard would be refusing before the stub was ever reached.
func private_endpoints_allowed(t *testing.T) {
	t.Helper()
	previous := url_allow_private
	url_allow_private = true
	url_transport.CloseIdleConnections()
	t.Cleanup(func() {
		url_allow_private = previous
		url_transport.CloseIdleConnections()
	})
}

// TestAccountUrlRefusesInternalAddress — the SSRF itself. The same loopback
// destination is reachable with the guard off and refused with it on, which is
// what shows the guard is doing the refusing rather than the server being
// absent or the port closed.
func TestAccountUrlRefusesInternalAddress(t *testing.T) {
	load_core_labels()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	with_private_allowed(t, func() {
		if result := account_test_url(server.URL, "", "en"); !result.Success {
			t.Fatalf("the destination is not reachable even with the guard off, so the case below proves nothing: %q", result.Message)
		}
	})

	// Guard on: the same URL must now be refused.
	if result := account_test_url(server.URL, "", "en"); result.Success {
		t.Error("a loopback address supplied by the caller was contacted")
	}
}

// TestAccountNtfyRefusesInternalAddress — ntfy builds its URL from a
// caller-supplied server, so it needs the same treatment as the raw URL
// provider and is a separate call site that could drift.
func TestAccountNtfyRefusesInternalAddress(t *testing.T) {
	load_core_labels()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	with_private_allowed(t, func() {
		if result := account_test_ntfy(server.URL, "topic", "", "en", "label"); !result.Success {
			t.Fatalf("unreachable with the guard off: %q", result.Message)
		}
	})

	if result := account_test_ntfy(server.URL, "topic", "", "en", "label"); result.Success {
		t.Error("a loopback ntfy server supplied by the caller was contacted")
	}
}

// TestAccountMcpRefusesInternalAddress — the third caller-supplied address.
func TestAccountMcpRefusesInternalAddress(t *testing.T) {
	load_core_labels()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}`))
	}))
	defer server.Close()

	with_private_allowed(t, func() {
		if result := account_test_mcp(server.URL, "", "en"); !result.Success {
			t.Fatalf("unreachable with the guard off: %q", result.Message)
		}
	})

	if result := account_test_mcp(server.URL, "", "en"); result.Success {
		t.Error("a loopback MCP server supplied by the caller was contacted")
	}
}

// TestAccountFailureCarriesNoDetail — the oracle half. The message returned to
// the caller must not distinguish a refused connection from a filtered one or
// from a name that does not resolve; repeated over a range, that difference is
// what maps a network the caller cannot otherwise see.
func TestAccountFailureCarriesNoDetail(t *testing.T) {
	load_core_labels()
	plain := resolve_core_label("en", "accounts.test.connection_failed", nil)

	for _, test := range []struct {
		name   string
		result AccountTestResult
	}{
		// Closed port, unroutable address and unresolvable name are three
		// distinguishable failures; all three must read identically.
		{"closed port", account_test_url("http://127.0.0.1:1/", "", "en")},
		{"private range", account_test_url("http://10.255.255.1/", "", "en")},
		{"no such host", account_test_url("http://nonexistent.invalid/", "", "en")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.result.Success {
				t.Fatal("expected failure")
			}
			if test.result.Message != plain {
				t.Errorf("message = %q, want the plain %q - a detail here separates one failure mode from another", test.result.Message, plain)
			}
			for _, leak := range []string{"127.0.0.1", "10.255.255.1", "nonexistent", "refused", "no such host", "timeout", "dial"} {
				if strings.Contains(strings.ToLower(test.result.Message), leak) {
					t.Errorf("message leaks %q: %q", leak, test.result.Message)
				}
			}
		})
	}
}

// TestAccountProvidersUseGuardedTransport — the regression test for the defect
// itself, and the reason it is worth reading the source: seven call sites each
// built their own http.Client, five behind a substring test for two literals
// and two behind nothing at all. A bare client makes exactly the same requests
// as a guarded one to every destination that is allowed, so nothing observable
// distinguishes them until someone points one inward.
//
// A provider whose address the user supplies must use account_client.
func TestAccountProvidersUseGuardedTransport(t *testing.T) {
	source, err := os.ReadFile("accounts.go")
	if err != nil {
		t.Fatalf("read accounts.go: %v", err)
	}
	text := string(source)

	// Every provider that takes an address from the account rather than a
	// fixed vendor endpoint.
	for _, name := range []string{
		"account_test_mcp",
		"account_test_ntfy",
		"account_test_url",
		"account_test_browser",
		"account_test_unifiedpush",
		"account_deliver_ntfy",
		"account_deliver_url",
		"account_deliver_unifiedpush",
	} {
		t.Run(name, func(t *testing.T) {
			start := strings.Index(text, "func "+name+"(")
			if start < 0 {
				t.Fatalf("%s not found; rename it here too", name)
			}
			end := strings.Index(text[start+1:], "\nfunc ")
			if end < 0 {
				t.Fatalf("could not find the end of %s", name)
			}
			body := text[start : start+1+end]

			if bare := regexp.MustCompile(`&http\.Client\{`).FindString(body); bare != "" {
				t.Errorf("builds its own http.Client; a caller-supplied address must go through account_client so the dialer checks it")
			}
		})
	}
}

// TestFixedVendorProvidersKeepTheirDetail — the other half of the split, so a
// later cleanup does not strip diagnostics that cost nothing. These reach a
// hardcoded vendor host, so the error describes an endpoint the caller could
// have contacted directly and tells them nothing about this network.
func TestFixedVendorProvidersKeepTheirDetail(t *testing.T) {
	source, err := os.ReadFile("accounts.go")
	if err != nil {
		t.Fatalf("read accounts.go: %v", err)
	}
	text := string(source)

	for _, name := range []string{"account_test_pushbullet", "account_test_claude", "account_test_openai"} {
		start := strings.Index(text, "func "+name+"(")
		if start < 0 {
			continue // renamed or removed; the guarded set is what this file pins
		}
		end := strings.Index(text[start+1:], "\nfunc ")
		body := text[start : start+1+end]
		if !strings.Contains(body, "_detail") {
			t.Errorf("%s no longer reports the underlying error; for a fixed vendor endpoint that is lost diagnostics, not a closed oracle", name)
		}
	}
}
