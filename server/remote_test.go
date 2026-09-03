// Mochi server: Remote entity communication unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	sl "go.starlark.net/starlark"
)

// Test peer_connect_url HTTP request and JSON parsing
// Note: These tests verify the HTTP/JSON handling but can't test actual Net connection
// peer_connect_tls stands up an https test server and points url_transport at a
// transport that trusts its self-signed certificate for the duration of the
// test. peer_connect_url is https-only (#593) because the addresses it learns
// are then dialled by libp2p, so its tests need a TLS harness; the production
// transport keeps full verification and is restored on cleanup.
func peer_connect_tls(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)

	saved := url_transport
	t.Cleanup(func() { url_transport = saved })
	url_transport = &http.Transport{
		DialContext:     saved.DialContext,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // test certificate // nolint
	}
	return server
}

func TestPeerConnectUrlHttpHandling(t *testing.T) {
	// Serves from httptest on 127.0.0.1; url_request blocks non-public
	// destinations by default.
	allow_private_for_test(t)
	tests := []struct {
		name             string
		handler          http.HandlerFunc
		expect_error_nil bool
		expect_contains  string
	}{
		{
			name: "server returns 404",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(404)
			},
			expect_error_nil: false,
			expect_contains:  "server returned status 404",
		},
		{
			name: "server returns 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(500)
			},
			expect_error_nil: false,
			expect_contains:  "server returned status 500",
		},
		{
			name: "server returns invalid JSON",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("not json"))
			},
			expect_error_nil: false,
			expect_contains:  "failed to parse net info",
		},
		{
			name: "server returns empty peer",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer":      "",
					"addresses": []string{"/ip4/127.0.0.1/tcp/1443"},
				})
			},
			expect_error_nil: false,
			expect_contains:  "invalid net info: missing peer or addresses",
		},
		{
			name: "server returns empty addresses",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer":      "12D3KooWTestPeerIdMockValue123456789",
					"addresses": []string{},
				})
			},
			expect_error_nil: false,
			expect_contains:  "invalid net info: missing peer or addresses",
		},
		{
			name: "server returns null addresses",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer": "12D3KooWTestPeerIdMockValue123456789",
				})
			},
			expect_error_nil: false,
			expect_contains:  "invalid net info: missing peer or addresses",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := peer_connect_tls(t, tt.handler)
			defer server.Close()

			_, err := peer_connect_url(server.URL)
			if tt.expect_error_nil && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if !tt.expect_error_nil && err == nil {
				t.Errorf("expected error containing %q, got nil", tt.expect_contains)
			}
			if !tt.expect_error_nil && err != nil && !strings.Contains(err.Error(), tt.expect_contains) {
				t.Errorf("expected error containing %q, got %q", tt.expect_contains, err.Error())
			}
		})
	}
}

// Test that peer_connect_url correctly constructs the info URL
func TestPeerConnectUrlPath(t *testing.T) {
	// Serves from httptest on 127.0.0.1; url_request blocks non-public
	// destinations by default.
	allow_private_for_test(t)
	var requested_path string
	server := peer_connect_tls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested_path = r.URL.Path
		w.WriteHeader(500) // Return error to stop further processing
	}))

	peer_connect_url(server.URL)

	if requested_path != "/_/p2p/info" {
		t.Errorf("expected request path /_/p2p/info, got %q", requested_path)
	}
}

// Test URL normalization (adding https:// when no scheme present)
func TestPeerConnectUrlNormalizesScheme(t *testing.T) {
	// Serves from httptest on 127.0.0.1; url_request blocks non-public
	// destinations by default.
	allow_private_for_test(t)
	// We can't easily test the https normalization without a real HTTPS server,
	// but we can verify the logic by checking that http:// URLs work
	var received_host string
	server := peer_connect_tls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received_host = r.Host
		w.WriteHeader(500)
	}))

	// Extract host:port from server URL
	host_port := strings.TrimPrefix(server.URL, "https://")

	peer_connect_url(server.URL)
	if received_host != host_port {
		t.Errorf("with full URL, expected host %q, got %q", host_port, received_host)
	}
}

// Test remote_connect logic without peer (directory lookup path)
func TestRemoteConnectDirectoryLookup(t *testing.T) {
	// Skip this test if database subsystem isn't initialized
	// (entity_peer requires database access)
	t.Skip("requires database subsystem initialization")

	// When no peer is provided, remote_connect should look up in directory
	// With a non-existent entity, entity_peer returns empty string

	valid_entity_id := strings.Repeat("a", 50) // 50-char entity ID
	_, err := remote_connect("", valid_entity_id, "")

	if err == nil {
		t.Error("expected error for non-existent entity in directory")
		return
	}
	if !strings.Contains(err.Error(), "entity not found in directory") {
		t.Errorf("expected 'entity not found in directory' error, got %q", err.Error())
	}
}

// Test remote_connect with invalid peer
func TestRemoteConnectInvalidPeer(t *testing.T) {
	// Skip this test if Net subsystem isn't initialized
	// (peer_connect will panic on nil config)
	t.Skip("requires Net subsystem initialization")

	valid_entity_id := strings.Repeat("a", 50)
	_, err := remote_connect("", valid_entity_id, "invalid-peer-id")

	if err == nil {
		t.Error("expected error for invalid peer")
	}
}

// Benchmark JSON parsing in peer_connect_url
func BenchmarkPeerConnectUrlJsonParsing(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return valid JSON but will fail at peer_connect
		json.NewEncoder(w).Encode(map[string]any{
			"peer":      "",
			"addresses": []string{},
		})
	}))
	defer server.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		peer_connect_url(server.URL)
	}
}

// TestPeerConnectUrlRequiresHttps is #593's transport half. This fetch learns a
// peer's addresses and libp2p then dials them, so it must not be a plain-text
// hop that anyone on the path can answer. The permission half - that
// api_remote_peer now calls require_permission_url - is not reachable without a
// Starlark thread, and is covered by the build: the call is unconditional for
// any URL that is not the p2p/ form.
func TestPeerConnectUrlRequiresHttps(t *testing.T) {
	allow_private_for_test(t)

	// A live server, so "refused" is distinguishable from "could not connect":
	// an unreachable host errors either way and would prove nothing.
	var hit atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Store(true)
		w.Write([]byte(`{"peer":"x","addresses":["/ip4/127.0.0.1/tcp/1"]}`))
	}))
	t.Cleanup(server.Close)

	if _, err := peer_connect_url(server.URL); err == nil {
		t.Error("plain http was accepted: the addresses this fetch returns are dialled, so the hop must be authenticated")
	}
	if hit.Load() {
		t.Error("the plain-http fetch was made: the scheme must be refused before anything leaves the host")
	}

	// The scheme is what is bounded, not the form of the name: a bare host
	// still normalises to https and gets as far as the request.
	hit.Store(false)
	host := strings.TrimPrefix(server.URL, "http://")
	if _, err := peer_connect_url("ftp://" + host); err == nil {
		t.Error("a non-http scheme was accepted")
	}
	if hit.Load() {
		t.Error("the ftp:// form still reached the server")
	}
}

// remote_thread is the Starlark context an external app calls
// mochi.remote.peer under: a user, an app, and no url: grant.
func remote_thread(t *testing.T) *sl.Thread {
	t.Helper()
	user := create_test_user(t)
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", user)
	thread.SetLocal("app", create_external_app("stranger"))
	db_user(user, "user").permissions_setup()
	return thread
}

// TestRemotePeerAnswersNoneForWhatItCannotFetch is the permission half of
// #593, pinned after it broke the repositories app (#1238). The url: gate
// applies to a value that would cause an HTTP fetch and to nothing else: a
// bare peer id is the p2p/ form and is connected directly, a value that is no
// URL answers None as it always did, and only an https host the app has no
// grant for is refused with an error.
func TestRemotePeerAnswersNoneForWhatItCannotFetch(t *testing.T) {
	thread := remote_thread(t)
	fn := sl.NewBuiltin("mochi.remote.peer", nil)
	peer := func(value string) (sl.Value, error) {
		return api_remote_peer(thread, fn, sl.Tuple{sl.String(value)}, nil)
	}

	// The repositories app stores the directory location with its p2p/ prefix
	// stripped and hands the bare id back here. Unknown to the peer table it
	// cannot connect, but it must never reach the gate.
	if v, err := peer("12D3KooWLYEsrR6rziEnpLYJXseH3W1DBQGdKn5qnmQcvgsKcwCH"); err != nil || v != sl.None {
		t.Errorf("bare peer id: value %v, error %v; want None with no error", v, err)
	}
	if v, err := peer("not a url"); err != nil || v != sl.None {
		t.Errorf("non-URL value: value %v, error %v; want None with no error", v, err)
	}
	if _, err := peer("https://127.0.0.1:9/"); err == nil || !strings.Contains(err.Error(), "permission") {
		t.Errorf("an ungranted https host was not refused by the url: gate: %v", err)
	}
}
