// Mochi server: Rate limiting
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"net/http"
	"strconv"
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

	// Pubsub inbound rate limiter: 20 per second per peer. Applies to
	// application traffic — directory announcements and lookups — whose
	// volume follows user activity.
	rate_limit_pubsub_in = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20,
		window:  1,
	}

	// Pubsub inbound control-plane rate limiter: 10 per second per peer,
	// for the peers service only. Separate from rate_limit_pubsub_in so
	// application traffic cannot starve the messages hosts use to learn
	// each other's addresses — a synchronous remote request blocks on one
	// of those answers, and losing it reports an online peer as unreachable.
	//
	// A peer's legitimate control traffic is a few messages per minute, not
	// per second: senders self-limit to one address request and one relayed
	// record per minute per target (rate_limit_peer_request,
	// rate_limit_record_relay) and re-announce hourly. 10 per second is
	// therefore orders of magnitude of headroom for a burst while still
	// bounding what a flooder can push through this path.
	rate_limit_pubsub_control = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   10,
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

// Per-account login throttle for the guessable factors (authenticator, MFA,
// recovery codes), keyed by user uid so it follows the account across rotating
// source addresses (the per-IP limiter alone does not).
//
// It is a reservation gate, NOT a read-then-sleep: each attempt atomically
// claims the next verification slot under the lock, so concurrent guesses
// against one account are serialised into distinct future slots and cannot all
// slip through the free tier at once (the bug a plain "read count, sleep,
// verify" has — every concurrent request observes the same pre-failure count).
// It is also NOT a hard window: a correct credential submitted in a quiet
// period reserves an immediate slot, verifies, and clears the account, so a
// legitimate user is never locked out. Only when an account's reserved queue
// already stretches past account_wait_max is a request refused (429) rather
// than held — which both bounds the guess rate per account and caps how many
// handler goroutines ever sleep at once.
type account_gate_entry struct {
	failures int
	pending  int   // reservations issued but not yet settled (in-flight attempts)
	next     int64 // unix seconds: earliest the next attempt may verify
	seen     int64 // last activity, for cleanup
}

type account_gate struct {
	lock    sync.Mutex
	entries map[string]*account_gate_entry
}

var account_login = &account_gate{entries: make(map[string]*account_gate_entry)}

// Tunables (vars, not consts, so tests can adjust them). The first few
// failures reserve at the floor spacing; beyond that the spacing doubles up to
// account_wait_max, which is also the deepest queue a request will wait in
// before being refused outright.
var (
	account_gate_free  = 3
	account_gate_floor int64 = 1    // seconds between consecutive slots at minimum
	account_wait_max   int64 = 8    // seconds: refuse rather than wait/hold longer
	account_gate_ttl   int64 = 900  // seconds idle before an entry is dropped
)

// account_gate_spacing is the gap (seconds) reserved between consecutive
// verification slots given the failures seen so far — always at least the
// floor, so even a burst with no failures yet is serialised.
func account_gate_spacing(failures int) int64 {
	steps := failures - account_gate_free
	if steps <= 0 {
		return account_gate_floor
	}
	if steps > 20 { // guard the shift below from overflowing
		return account_wait_max
	}
	gap := int64(1) << (steps - 1) // 1, 2, 4, 8, ...
	if gap > account_wait_max {
		return account_wait_max
	}
	if gap < account_gate_floor {
		return account_gate_floor
	}
	return gap
}

// reserve atomically assigns this attempt a verification slot. It returns the
// seconds the caller must wait before verifying, and false when the account's
// reserved queue is already deeper than account_wait_max — in which case the
// caller rejects (429) instead of holding a goroutine. Serialising the slot
// assignment under the lock is what stops parallel guesses bypassing the
// throttle.
func (g *account_gate) reserve(uid string) (int64, bool) {
	g.lock.Lock()
	defer g.lock.Unlock()
	now := now()
	entry := g.entries[uid]
	if entry == nil {
		entry = &account_gate_entry{}
		g.entries[uid] = entry
	}
	entry.seen = now
	start := entry.next
	if start < now {
		start = now
	}
	wait := start - now
	// Accept only when this attempt's whole slot (its wait plus the spacing it
	// reserves) fits inside the window, so entry.next never climbs past
	// now+account_wait_max. That bounds recovery after a flood to at most
	// account_wait_max (a slot that started at the window edge cannot push next
	// a further spacing beyond it) and stops next ratcheting away under
	// sustained load. A front-of-queue attempt (wait 0) always fits, so a
	// correct credential on a quiet account is never refused.
	gap := account_gate_spacing(entry.failures)
	if wait+gap > account_wait_max {
		return wait, false
	}
	entry.next = start + gap
	entry.pending++
	return wait, true
}

// done settles a reservation from reserve. ok reports whether the credential
// verified. A wrong guess widens the spacing owed on later attempts. A correct
// credential clears the accumulated penalty — but only drops the whole entry
// (rewinding the slot timeline) when this was the last in-flight attempt;
// while other reservations are still sleeping it keeps their reserved slots so
// a mid-flight success cannot rewind next and let new requests overlap them.
func (g *account_gate) done(uid string, ok bool) {
	g.lock.Lock()
	defer g.lock.Unlock()
	entry := g.entries[uid]
	if entry == nil {
		return
	}
	if entry.pending > 0 {
		entry.pending--
	}
	entry.seen = now()
	if !ok {
		entry.failures++
		return
	}
	if entry.pending == 0 {
		delete(g.entries, uid)
		return
	}
	entry.failures = 0
}

// reset drops an account's throttle unconditionally (test cleanup only —
// handlers settle through done so in-flight reservations are respected).
func (g *account_gate) reset(uid string) {
	g.lock.Lock()
	defer g.lock.Unlock()
	delete(g.entries, uid)
}

func (g *account_gate) cleanup() {
	g.lock.Lock()
	defer g.lock.Unlock()
	now := now()
	for uid, entry := range g.entries {
		if entry.pending == 0 && now-entry.seen > account_gate_ttl && entry.next <= now {
			delete(g.entries, uid)
		}
	}
}

// account_gate_guard reserves a slot for a guessable-factor verification and
// waits out the (bounded) delay. It returns false after answering 429 when the
// account's queue is already too deep; the caller just returns. On the normal
// path the caller MUST settle the reservation exactly once with
// account_login.done(uid, verified) — a deferred done(uid, false) is the safe
// pattern, flipped to true once the credential verifies.
func account_gate_guard(c *gin.Context, uid string) bool {
	wait, ok := account_login.reserve(uid)
	if !ok {
		audit_rate_limit(rate_limit_client_ip(c), "account")
		c.Header("Retry-After", strconv.FormatInt(wait, 10))
		respond_error(c, http.StatusTooManyRequests, "too_many_login_attempts_please_try_again_later", "errors.too_many_logins", nil)
		return false
	}
	if wait > 0 {
		time.Sleep(time.Duration(wait) * time.Second)
	}
	return true
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
		account_login.cleanup()
		rate_limit_login.cleanup()
		rate_limit_p2p.cleanup()
		rate_limit_pubsub_in.cleanup()
		rate_limit_pubsub_control.cleanup()
		rate_limit_peer_request.cleanup()
		rate_limit_record_relay.cleanup()
		rate_limit_entry_withdraw.cleanup()
		rate_limit_url.cleanup()
		rate_limit_net_send.cleanup()
	}
}
