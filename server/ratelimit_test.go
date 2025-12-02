// Mochi server: Rate limiting unit tests
// Copyright Alistair Cunningham 2025

package main

import (
	"sync"
	"testing"
)

// Test rate_limiter.allow basic functionality
func TestRateLimiterAllow(t *testing.T) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   3,
		window:  60,
	}

	key := "test-client"

	// First 3 requests should be allowed
	for i := 1; i <= 3; i++ {
		if !limiter.allow(key) {
			t.Errorf("Request %d should be allowed", i)
		}
	}

	// 4th request should be denied
	if limiter.allow(key) {
		t.Error("Request 4 should be denied (over limit)")
	}

	// 5th request should also be denied
	if limiter.allow(key) {
		t.Error("Request 5 should be denied (over limit)")
	}
}

// Test rate_limiter.allow with different keys
func TestRateLimiterAllowDifferentKeys(t *testing.T) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   2,
		window:  60,
	}

	// Exhaust limit for key1
	limiter.allow("key1")
	limiter.allow("key1")
	if limiter.allow("key1") {
		t.Error("key1 should be rate limited")
	}

	// key2 should still be allowed
	if !limiter.allow("key2") {
		t.Error("key2 should be allowed (independent of key1)")
	}
	if !limiter.allow("key2") {
		t.Error("key2 second request should be allowed")
	}
	if limiter.allow("key2") {
		t.Error("key2 third request should be denied")
	}
}

// Test rate_limiter.reset functionality
func TestRateLimiterReset(t *testing.T) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   2,
		window:  60,
	}

	key := "test-client"

	// Use up the limit
	limiter.allow(key)
	limiter.allow(key)
	if limiter.allow(key) {
		t.Error("Should be rate limited before reset")
	}

	// Reset the key
	limiter.reset(key)

	// Should be allowed again
	if !limiter.allow(key) {
		t.Error("Should be allowed after reset")
	}
}

// Test rate_limiter.cleanup functionality
func TestRateLimiterCleanup(t *testing.T) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   10,
		window:  1, // 1 second window for testing
	}

	// Add some entries
	limiter.allow("key1")
	limiter.allow("key2")
	limiter.allow("key3")

	if len(limiter.entries) != 3 {
		t.Errorf("Should have 3 entries, got %d", len(limiter.entries))
	}

	// Manually set one entry to be expired
	limiter.lock.Lock()
	limiter.entries["key1"].reset = now() - 10 // expired
	limiter.entries["key2"].reset = now() - 5  // expired
	limiter.entries["key3"].reset = now() + 60 // not expired
	limiter.lock.Unlock()

	// Run cleanup
	limiter.cleanup()

	if len(limiter.entries) != 1 {
		t.Errorf("Should have 1 entry after cleanup, got %d", len(limiter.entries))
	}

	if limiter.entries["key3"] == nil {
		t.Error("key3 should still exist after cleanup")
	}
}

// Test rate_limiter.reset on non-existent key (should not panic)
func TestRateLimiterResetNonExistent(t *testing.T) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   10,
		window:  60,
	}

	// Should not panic
	limiter.reset("non-existent-key")
}

// Test rate_limiter with limit of 1
func TestRateLimiterSingleRequest(t *testing.T) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   1,
		window:  60,
	}

	key := "single-test"

	if !limiter.allow(key) {
		t.Error("First request should be allowed")
	}

	if limiter.allow(key) {
		t.Error("Second request should be denied")
	}
}

// Test concurrent access to rate_limiter
func TestRateLimiterConcurrent(t *testing.T) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   100,
		window:  60,
	}

	key := "concurrent-test"
	var wg sync.WaitGroup
	allowed := 0
	var mu sync.Mutex

	// Launch 200 goroutines, each making a request
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.allow(key) {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Exactly 100 should be allowed
	if allowed != 100 {
		t.Errorf("Expected 100 allowed requests, got %d", allowed)
	}
}

// Test that window reset works
func TestRateLimiterWindowReset(t *testing.T) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   2,
		window:  1, // 1 second window
	}

	key := "window-test"

	// Use up the limit
	limiter.allow(key)
	limiter.allow(key)
	if limiter.allow(key) {
		t.Error("Should be rate limited")
	}

	// Manually expire the window
	limiter.lock.Lock()
	limiter.entries[key].reset = now() - 1
	limiter.lock.Unlock()

	// Should be allowed again
	if !limiter.allow(key) {
		t.Error("Should be allowed after window expires")
	}
}

// Benchmark rate_limiter.allow
func BenchmarkRateLimiterAllow(b *testing.B) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   1000000, // high limit so we don't get blocked
		window:  60,
	}

	key := "benchmark-key"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.allow(key)
	}
}

// Benchmark rate_limiter.allow with many different keys
func BenchmarkRateLimiterAllowManyKeys(b *testing.B) {
	limiter := &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   100,
		window:  60,
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = random_alphanumeric(16)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.allow(keys[i%len(keys)])
	}
}

// Test rate_limit_url configuration
func TestRateLimitURLConfig(t *testing.T) {
	if rate_limit_url.limit != 100 {
		t.Errorf("rate_limit_url.limit = %d, want 100", rate_limit_url.limit)
	}
	if rate_limit_url.window != 60 {
		t.Errorf("rate_limit_url.window = %d, want 60", rate_limit_url.window)
	}
}

// Test rate_limit_p2p_send configuration
func TestRateLimitP2PSendConfig(t *testing.T) {
	if rate_limit_p2p_send.limit != 20 {
		t.Errorf("rate_limit_p2p_send.limit = %d, want 20", rate_limit_p2p_send.limit)
	}
	if rate_limit_p2p_send.window != 1 {
		t.Errorf("rate_limit_p2p_send.window = %d, want 1", rate_limit_p2p_send.window)
	}
}
