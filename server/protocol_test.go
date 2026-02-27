// Mochi server: Protocol unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"testing"
)

// Test Headers.valid() basic validation
func TestHeadersValid(t *testing.T) {
	longID := make([]byte, 65)
	for i := range longID {
		longID[i] = 'x'
	}

	tests := []struct {
		name     string
		headers  Headers
		expected bool
	}{
		{"empty headers", Headers{}, true},
		{"msg type", Headers{Type: "msg"}, true},
		{"ack type without AckID", Headers{Type: "ack"}, false},
		{"ack type with AckID", Headers{Type: "ack", AckID: "test123"}, true},
		{"nack type without AckID", Headers{Type: "nack"}, false},
		{"nack type with AckID", Headers{Type: "nack", AckID: "test123"}, true},
		{"invalid type", Headers{Type: "invalid"}, false},
		{"ID at max length", Headers{ID: string(longID[:64])}, true},
		{"ID too long", Headers{ID: string(longID)}, false},
		{"AckID at max length", Headers{Type: "ack", AckID: string(longID[:64])}, true},
		{"AckID too long", Headers{Type: "ack", AckID: string(longID)}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.headers.valid()
			if result != tt.expected {
				t.Errorf("Headers.valid() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test Headers.msg_type() defaults to "msg"
func TestHeadersMsgType(t *testing.T) {
	tests := []struct {
		name     string
		headers  Headers
		expected string
	}{
		{"empty type", Headers{}, "msg"},
		{"msg type", Headers{Type: "msg"}, "msg"},
		{"ack type", Headers{Type: "ack"}, "ack"},
		{"nack type", Headers{Type: "nack"}, "nack"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.headers.msg_type()
			if result != tt.expected {
				t.Errorf("Headers.msg_type() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// Test signable_headers produces consistent output
func TestSignableHeaders(t *testing.T) {
	challenge := []byte("test_challenge_16")

	// Same inputs should produce same output
	h1 := signable_headers("msg", "from", "to", "service", "event", "", "id", "", challenge)
	h2 := signable_headers("msg", "from", "to", "service", "event", "", "id", "", challenge)

	if string(h1) != string(h2) {
		t.Error("signable_headers should be deterministic")
	}

	// Different inputs should produce different output
	h3 := signable_headers("msg", "from", "to", "service", "other", "", "id", "", challenge)
	if string(h1) == string(h3) {
		t.Error("Different inputs should produce different signable headers")
	}

	// Different challenge should produce different output
	h4 := signable_headers("msg", "from", "to", "service", "event", "", "id", "", []byte("other_challenge_16"))
	if string(h1) == string(h4) {
		t.Error("Different challenge should produce different signable headers")
	}

	// Phase 1: App is intentionally not included in signable headers yet
	// Phase 2: uncomment this test when App is included in signature
	// h5 := signable_headers("msg", "from", "to", "service", "event", "myapp", "id", "", challenge)
	// if string(h1) == string(h5) {
	// 	t.Error("Different app should produce different signable headers")
	// }
}

// Test Headers.verify() with nil challenge (broadcast case)
func TestHeadersVerifyBroadcast(t *testing.T) {
	// Unsigned message should verify with nil challenge
	h := Headers{Type: "msg", Service: "test", Event: "test"}
	if !h.verify(nil) {
		t.Error("Unsigned message should verify with nil challenge")
	}
}

// Benchmark signable_headers
func BenchmarkSignableHeaders(b *testing.B) {
	challenge := []byte("benchmark_challen")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		signable_headers("msg", "from_entity", "to_entity", "service", "event", "", "message_id", "", challenge)
	}
}

// Benchmark Headers.valid
func BenchmarkHeadersValid(b *testing.B) {
	h := Headers{Type: "msg", From: "test", To: "test", Service: "test", Event: "test"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.valid()
	}
}
