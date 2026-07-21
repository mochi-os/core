// Mochi server: Rate limiting
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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

	// Per-account failed-attempt counter for the guessable login factors
	// (authenticator, MFA, recovery codes), keyed by user uid so the throttle
	// follows the account across rotating source addresses (the per-IP limiter
	// alone does not). It is deliberately NOT a hard gate: rejecting before
	// verification would let anyone lock a known account out for the whole
	// window with a few bad guesses, since /_/auth/begin identifies accounts.
	// Instead each accumulated failure adds a progressive delay before the
	// next verification (account_backoff); a correct credential always
	// verifies and clears the count, so a legitimate user is at most briefly
	// delayed, never locked out, while an attacker's guess rate collapses.
	// The limit only bounds how high the count climbs (the delay caps sooner).
	rate_limit_account = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   12,
		window:  900,
	}

	rate_limit_login = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20,
		window:  300,
	}

	// Net stream rate limiter: 100 per second per peer
	rate_limit_p2p = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   100,
		window:  1,
	}

	// Pubsub inbound rate limiter: 20 per second per peer
	rate_limit_pubsub_in = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20,
		window:  1,
	}

	// Peer address-request rate limiter: 1 broadcast per minute per
	// target peer. The queue retries unreachable peers every tick;
	// without this each retry would re-flood a peers/request.
	rate_limit_peer_request = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   1,
		window:  60,
	}

	// Peer record-relay rate limiter: 1 relayed answer per minute per
	// target peer. A peers/request is a broadcast every holder of the
	// target's record could answer; this bounds how often any one of
	// them does so for the same target.
	rate_limit_record_relay = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   1,
		window:  60,
	}

	// Directory ghost-withdrawal rate limiter: 1 broadcast per hour per
	// entity. Until a withdrawal propagates, every directory sync echoes
	// the same ghost row back (5-minute cadence); this bounds the
	// duplicate delete broadcasts entry_store would otherwise answer
	// each echo with.
	rate_limit_entry_withdraw = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   1,
		window:  3600,
	}

	// URL request rate limiter: 100 requests per minute per app
	rate_limit_url = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   100,
		window:  60,
	}

	// Direct Net message rate limiter: 1000 per second per app
	rate_limit_net_send = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   1000,
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

// count returns the current count for a key, or 0 if absent/expired. Unlike
// allow it neither increments nor enforces the limit — used to size the
// per-account login backoff.
func (r *rate_limiter) count(key string) int {
	r.lock.Lock()
	defer r.lock.Unlock()
	entry := r.entries[key]
	if entry == nil || now() >= entry.reset {
		return 0
	}
	return entry.count
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

// Get client IP from the direct connection
func rate_limit_client_ip(c *gin.Context) string {
	return c.RemoteIP()
}

// Middleware for general API rate limiting
func rate_limit_api_middleware(c *gin.Context) {
	ip := rate_limit_client_ip(c)

	if !rate_limit_api.allow(ip) {
		audit_rate_limit(ip, "api")
		respond_error(c, http.StatusTooManyRequests, "rate_limit_exceeded_please_try_again_later", "errors.rate_limit_exceeded", nil)
		c.Abort()
		return
	}

	c.Next()
}

// The first few failures are free (honest fat-fingering must not be punished);
// beyond that the delay doubles per failure up to a cap. Vars, not consts, so
// tests can neutralise the sleep.
var (
	account_backoff_free = 3
	account_backoff_cap  = 8 * time.Second
)

// account_backoff is the delay owed before the next guess given the account's
// accumulated failure count.
func account_backoff(failures int) time.Duration {
	steps := failures - account_backoff_free
	if steps <= 0 {
		return 0
	}
	if steps > 20 { // guard the shift below from overflowing
		return account_backoff_cap
	}
	delay := time.Second << (steps - 1) // 1s, 2s, 4s, ...
	if delay <= 0 || delay > account_backoff_cap {
		return account_backoff_cap
	}
	return delay
}

// rate_limit_account_throttle sleeps for the backoff currently owed by the
// account before a guessable factor is verified, so repeated wrong guesses
// slow to a crawl. A correct credential is verified after the same (bounded)
// delay and still succeeds — this throttles, it never locks out.
func rate_limit_account_throttle(user *User) {
	if delay := account_backoff(rate_limit_account.count(user.UID)); delay > 0 {
		time.Sleep(delay)
	}
}

// rate_limit_account_fail records one failed verification of a guessable
// factor, raising the backoff owed on the next attempt.
func rate_limit_account_fail(user *User) {
	rate_limit_account.allow(user.UID)
}

// Middleware for login rate limiting (stricter)
func rate_limit_login_middleware(c *gin.Context) {
	ip := rate_limit_client_ip(c)

	if !rate_limit_login.allow(ip) {
		audit_rate_limit(ip, "login")
		audit_repeated_failures(ip, rate_limit_login.limit, "login")
		respond_error(c, http.StatusTooManyRequests, "too_many_login_attempts_please_try_again_later", "errors.too_many_logins", nil)
		c.Abort()
		return
	}

	c.Next()
}

// Background cleanup goroutine for expired rate limit entries
func ratelimit_manager() {
	for range time.Tick(time.Minute) {
		rate_limit_api.cleanup()
		rate_limit_account.cleanup()
		rate_limit_login.cleanup()
		rate_limit_p2p.cleanup()
		rate_limit_pubsub_in.cleanup()
		rate_limit_peer_request.cleanup()
		rate_limit_record_relay.cleanup()
		rate_limit_entry_withdraw.cleanup()
		rate_limit_url.cleanup()
		rate_limit_net_send.cleanup()
	}
}
