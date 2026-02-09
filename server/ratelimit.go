// Mochi server: Rate limiting
// Copyright Alistair Cunningham 2025

package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type rate_limit_entry struct {
	count int
	reset int64
}

type rate_limiter struct {
	entries map[string]*rate_limit_entry
	lock    sync.Mutex
	limit   int
	window  int64 // seconds
}

var (
	// General API rate limiter: 1000 requests per minute
	rate_limit_api = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   1000,
		window:  60,
	}

	// Login rate limiter: 20 attempts per 5 minutes
	rate_limit_login = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20,
		window:  300,
	}

	// P2P stream rate limiter: 100 per second per peer
	rate_limit_p2p = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   100,
		window:  1,
	}

	// Pubsub outbound rate limiter: 20 per second global
	rate_limit_pubsub_out = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20,
		window:  1,
	}

	// Pubsub inbound rate limiter: 20 per second per peer
	rate_limit_pubsub_in = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20,
		window:  1,
	}

	// URL request rate limiter: 100 requests per minute per app
	rate_limit_url = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   100,
		window:  60,
	}

	// Direct P2P message rate limiter: 100 per second per app
	rate_limit_p2p_send = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   100,
		window:  1,
	}
)

// Check if request is allowed; returns true if allowed, false if rate limited
func (r *rate_limiter) allow(key string) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	now := now()
	entry := r.entries[key]

	if entry == nil {
		r.entries[key] = &rate_limit_entry{count: 1, reset: now + r.window}
		return true
	}

	// Window expired, reset counter
	if now >= entry.reset {
		entry.count = 1
		entry.reset = now + r.window
		return true
	}

	// Within window, check limit
	if entry.count >= r.limit {
		return false
	}

	entry.count++
	return true
}

// Reset counter for a key (e.g., on successful login)
func (r *rate_limiter) reset(key string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.entries, key)
}

// Clean up expired entries
func (r *rate_limiter) cleanup() {
	r.lock.Lock()
	defer r.lock.Unlock()

	now := now()
	for key, entry := range r.entries {
		if now >= entry.reset {
			delete(r.entries, key)
		}
	}
}

// Get client IP, respecting X-Forwarded-For if behind proxy
func rate_limit_client_ip(c *gin.Context) string {
	// Check X-Forwarded-For header (first IP is the client)
	xff := c.GetHeader("X-Forwarded-For")
	if xff != "" {
		// Take first IP in the chain
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}

	// Check X-Real-IP header
	xri := c.GetHeader("X-Real-IP")
	if xri != "" {
		return xri
	}

	// Fall back to direct client IP
	return c.ClientIP()
}

// Middleware for general API rate limiting
func rate_limit_api_middleware(c *gin.Context) {
	ip := rate_limit_client_ip(c)

	if !rate_limit_api.allow(ip) {
		audit_rate_limit(ip, "api")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "Rate limit exceeded. Please try again later."})
		c.Abort()
		return
	}

	c.Next()
}

// Middleware for login rate limiting (stricter)
func rate_limit_login_middleware(c *gin.Context) {
	ip := rate_limit_client_ip(c)

	if !rate_limit_login.allow(ip) {
		audit_rate_limit(ip, "login")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "Too many login attempts. Please try again later."})
		c.Abort()
		return
	}

	c.Next()
}

// Background cleanup goroutine for expired rate limit entries
func ratelimit_manager() {
	for range time.Tick(time.Minute) {
		rate_limit_api.cleanup()
		rate_limit_login.cleanup()
		rate_limit_p2p.cleanup()
		rate_limit_pubsub_out.cleanup()
		rate_limit_pubsub_in.cleanup()
		rate_limit_url.cleanup()
		rate_limit_p2p_send.cleanup()
	}
}
