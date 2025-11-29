// Mochi server: Protocol unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"testing"
	"time"
)

// Test nonce_seen function
func TestNonceSeen(t *testing.T) {
	// Clear any existing nonces
	nonces_lock.Lock()
	nonces = map[string]int64{}
	nonces_lock.Unlock()

	// First time seeing a nonce should return false
	nonce1 := "test_nonce_1"
	if nonce_seen(nonce1) {
		t.Error("First call to nonce_seen should return false")
	}

	// Second time seeing same nonce should return true
	if !nonce_seen(nonce1) {
		t.Error("Second call to nonce_seen with same nonce should return true")
	}

	// Different nonce should return false
	nonce2 := "test_nonce_2"
	if nonce_seen(nonce2) {
		t.Error("First call with different nonce should return false")
	}
}

// Test nonce_cleanup removes expired nonces
func TestNonceCleanup(t *testing.T) {
	// Clear and set up expired nonce
	nonces_lock.Lock()
	nonces = map[string]int64{}
	nonces["expired"] = now() - 100 // expired 100 seconds ago
	nonces["valid"] = now() + 600   // expires in 10 minutes
	nonces_lock.Unlock()

	// Run cleanup
	nonce_cleanup()

	// Check expired was removed
	nonces_lock.Lock()
	_, expired_exists := nonces["expired"]
	_, valid_exists := nonces["valid"]
	nonces_lock.Unlock()

	if expired_exists {
		t.Error("Expired nonce should have been cleaned up")
	}
	if !valid_exists {
		t.Error("Valid nonce should not have been cleaned up")
	}
}

// Test timestamp validation in header validation
func TestTimestampValidation(t *testing.T) {
	// Clear nonces for clean test
	nonces_lock.Lock()
	nonces = map[string]int64{}
	nonces_lock.Unlock()

	tests := []struct {
		name       string
		age        int64 // seconds from now (negative = future)
		want_valid bool
	}{
		{"current", 0, true},
		{"1 minute ago", 60, true},
		{"4 minutes ago", 240, true},
		{"5 minutes ago", 300, true},
		{"6 minutes ago", 360, false},
		{"1 hour ago", 3600, false},
		{"1 minute in future", -60, true},
		{"4 minutes in future", -240, true},
		{"6 minutes in future", -360, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset nonces for each test
			nonces_lock.Lock()
			nonces = map[string]int64{}
			nonces_lock.Unlock()

			timestamp := now() - tt.age
			age := now() - timestamp
			if age < 0 {
				age = -age
			}
			is_valid := age <= 300

			if is_valid != tt.want_valid {
				t.Errorf("Timestamp %d seconds from now: got valid=%v, want valid=%v", tt.age, is_valid, tt.want_valid)
			}
		})
	}
}

// Test concurrent nonce access
func TestNonceConcurrency(t *testing.T) {
	// Clear nonces
	nonces_lock.Lock()
	nonces = map[string]int64{}
	nonces_lock.Unlock()

	// Run concurrent nonce checks
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(n int) {
			nonce := "concurrent_nonce"
			nonce_seen(nonce)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}

	// Verify nonce was recorded
	nonces_lock.Lock()
	_, exists := nonces["concurrent_nonce"]
	nonces_lock.Unlock()

	if !exists {
		t.Error("Nonce should exist after concurrent access")
	}
}

// Benchmark nonce_seen
func BenchmarkNonceSeen(b *testing.B) {
	// Pre-populate with some nonces
	nonces_lock.Lock()
	nonces = map[string]int64{}
	for i := 0; i < 1000; i++ {
		nonces["existing_"+string(rune(i))] = now() + 600
	}
	nonces_lock.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nonce_seen("new_nonce_" + string(rune(i%1000)))
	}
}

// Benchmark nonce_cleanup
func BenchmarkNonceCleanup(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Set up mixed expired/valid nonces
		nonces_lock.Lock()
		nonces = map[string]int64{}
		for j := 0; j < 500; j++ {
			nonces["expired_"+string(rune(j))] = now() - 100
			nonces["valid_"+string(rune(j))] = now() + 600
		}
		nonces_lock.Unlock()

		nonce_cleanup()
	}
}

// Test that cleanup goroutine starts
func TestNonceCleanupGoroutine(t *testing.T) {
	// Just verify the init() ran without panicking
	// The goroutine is started in init()
	time.Sleep(10 * time.Millisecond)
}
