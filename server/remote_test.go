// Mochi server: Remote entity communication unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Test peer_connect_url HTTP request and JSON parsing
// Note: These tests verify the HTTP/JSON handling but can't test actual P2P connection
func TestPeerConnectUrlHttpHandling(t *testing.T) {
	tests := []struct {
		name           string
		handler        http.HandlerFunc
		expectErrorNil bool
		expectContains string
	}{
		{
			name: "server returns 404",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(404)
			},
			expectErrorNil: false,
			expectContains: "server returned status 404",
		},
		{
			name: "server returns 500",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(500)
			},
			expectErrorNil: false,
			expectContains: "server returned status 500",
		},
		{
			name: "server returns invalid JSON",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("not json"))
			},
			expectErrorNil: false,
			expectContains: "failed to parse p2p info",
		},
		{
			name: "server returns empty peer",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer":      "",
					"addresses": []string{"/ip4/127.0.0.1/tcp/1443"},
				})
			},
			expectErrorNil: false,
			expectContains: "invalid p2p info: missing peer or addresses",
		},
		{
			name: "server returns empty addresses",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer":      "12D3KooWTestPeerIdMockValue123456789",
					"addresses": []string{},
				})
			},
			expectErrorNil: false,
			expectContains: "invalid p2p info: missing peer or addresses",
		},
		{
			name: "server returns null addresses",
			handler: func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"peer": "12D3KooWTestPeerIdMockValue123456789",
				})
			},
			expectErrorNil: false,
			expectContains: "invalid p2p info: missing peer or addresses",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			_, err := peer_connect_url(server.URL)
			if tt.expectErrorNil && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if !tt.expectErrorNil && err == nil {
				t.Errorf("expected error containing %q, got nil", tt.expectContains)
			}
			if !tt.expectErrorNil && err != nil && !strings.Contains(err.Error(), tt.expectContains) {
				t.Errorf("expected error containing %q, got %q", tt.expectContains, err.Error())
			}
		})
	}
}

// Test that peer_connect_url correctly constructs the info URL
func TestPeerConnectUrlPath(t *testing.T) {
	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		w.WriteHeader(500) // Return error to stop further processing
	}))
	defer server.Close()

	peer_connect_url(server.URL)

	if requestedPath != "/_/p2p/info" {
		t.Errorf("expected request path /_/p2p/info, got %q", requestedPath)
	}
}

// Test URL normalization (adding https:// when no scheme present)
func TestPeerConnectUrlNormalizesScheme(t *testing.T) {
	// We can't easily test the https normalization without a real HTTPS server,
	// but we can verify the logic by checking that http:// URLs work
	var receivedHost string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHost = r.Host
		w.WriteHeader(500)
	}))
	defer server.Close()

	// Extract host:port from server URL
	hostPort := strings.TrimPrefix(server.URL, "http://")

	peer_connect_url(server.URL)
	if receivedHost != hostPort {
		t.Errorf("with full URL, expected host %q, got %q", hostPort, receivedHost)
	}
}

// Test remote_connect logic without peer (directory lookup path)
func TestRemoteConnectDirectoryLookup(t *testing.T) {
	// Skip this test if database subsystem isn't initialized
	// (entity_peer requires database access)
	t.Skip("requires database subsystem initialization")

	// When no peer is provided, remote_connect should look up in directory
	// With a non-existent entity, entity_peer returns empty string

	validEntityId := strings.Repeat("a", 50) // 50-char entity ID
	_, err := remote_connect(validEntityId, "")

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
	// Skip this test if P2P subsystem isn't initialized
	// (peer_connect will panic on nil config)
	t.Skip("requires P2P subsystem initialization")

	validEntityId := strings.Repeat("a", 50)
	_, err := remote_connect(validEntityId, "invalid-peer-id")

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
