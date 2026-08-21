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
	sl "go.starlark.net/starlark"
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

	// Login-code sends, keyed on the account rather than the IP: 5 per 15 minutes.
	// code_send is reachable from /_/auth/code and from mochi.user.code.send(), so
	// the limit lives on the shared function. Each send leaves an hour-long code
	// valid.
	rate_limit_code = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   5,
		window:  900,
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

	// Pubsub inbound control plane: 10 per second per peer, peers service only.
	// Separate from rate_limit_pubsub_in so application traffic cannot starve
	// address learning; legitimate control traffic is a few messages per minute.
	rate_limit_pubsub_control = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   10,
		window:  1,
	}

	// Directory sync serving: 6 per minute per requesting peer. Answering streams
	// every row at or after the requester's watermark, so capping the row count
	// would break bootstrap; capping how often it may be asked does not.
	rate_limit_directory_sync = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   6,
		window:  60,
	}

	// Directory push receiving: 6 per minute per pushing peer. The sibling of
	// rate_limit_directory_sync and costlier: each row the peer chooses to send
	// costs validators, SQLite queries and an ed25519 verification.
	rate_limit_directory_push = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   6,
		window:  60,
	}

	// Entity creation from an app: 30 per minute per user. entity_create mints a
	// keypair, writes users.db and directory.db and floods the mesh, so one call
	// is N remote writes. Keyed on the user so one app's loop cannot bind other
	// accounts.
	rate_limit_entity_create = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   30,
		window:  60,
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

	// Directory ghost-withdrawal: 1 broadcast per hour per entity. Until a
	// withdrawal propagates every 5-minute directory sync echoes the ghost row
	// back, and this bounds the duplicate deletes entry_store would answer with.
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

	// Broadcast fan-out, charged one per RECIPIENT: one mochi.broadcast.send
	// becomes N wire messages and N queue rows. Separate from rate_limit_net_send
	// so a fan-out cannot exhaust the app's direct-send budget; the minute window
	// absorbs a backfill.
	rate_limit_broadcast = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20000,
		window:  60,
	}

	// Outbound remote request/stream/ping/peer, per app AND target. Keyed on the
	// target too, so a flood against one entity cannot exhaust the budget every
	// other user of that app shares. Sized to clear the apps update sweep, which
	// costs one call per catalogue app against a single publisher.
	rate_limit_remote_entity = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   600,
		window:  60,
	}

	// Bytes relayed out of a.write.stream, per app and CLIENT, 2GB per minute. In
	// kilobytes: rate_limit_entry.count is an int and a byte count would overflow
	// it. Keyed on the client, not the target, so it does not meter popularity.
	// Sized to clear one object_maximum transfer, and held there by
	// TestStorageLimitsAgree.
	rate_limit_stream_client = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   10 * 1024 * 1024, // kilobytes: one object_maximum transfer
		window:  60,
	}

	// Same accounting per app, as a circuit breaker against a flood spread across
	// many addresses. Far above any plausible honest minute: a binding limit would
	// refuse every user of an app because one is being abused.
	rate_limit_stream_app = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   80 * 1024 * 1024, // kilobytes: eight per-client budgets
		window:  60,
	}

	// Outbound stream bytes, per app. Separate from the relay pair above, which
	// meters what comes IN from a peer: the 600 streams/minimum per target cap says
	// nothing about how much each stream carries, so an app could hold one open
	// and write forever. Sized like the per-client relay budget.
	rate_limit_stream_outbound = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   10 * 1024 * 1024, // kilobytes
		window:  60,
	}

	// Not a budget: a once-per-minute-per-app gate on the log line written when a
	// refusal is turned into a 429. A limit of 1 used this way is the same trick
	// rate_limit_entry_withdraw uses to log an event at most once per window.
	rate_limit_refusal_log = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   1,
		window:  60,
	}

	// Aggregate backstop for the same path: 3000 per minute per app. Per-target
	// limiting bounds hammering ONE entity but not fanning out across every
	// entity the directory knows, so this bounds the total. High enough that a
	// legitimate list view fetching many different avatars never reaches it.
	rate_limit_remote = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   3000,
		window:  60,
	}

	// Account verification, in two buckets: per recipient bounds what one mailbox
	// receives, per sender bounds a spray across many addresses. Separate from
	// rate_limit_code so verification cannot lock an address out of login codes.
	rate_limit_verification = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   5,
		window:  900,
	}

	rate_limit_verification_sender = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20,
		window:  900,
	}
)

// RateLimitError is a refusal the HTTP layer turns into a 429 rather than the
// generic 500 every other builtin error becomes: a 500 reads as a server fault,
// so clients retry and each retry recharges the budget. Retry is seconds until
// reset (0 = unknown, no header); detail names the budget and reaches the log
// only.
type RateLimitError struct {
	Retry  int
	detail string
}

func (e *RateLimitError) Error() string {
	if e.detail == "" {
		return "rate limit exceeded"
	}
	return "rate limit exceeded (" + e.detail + ")"
}

// retry reports seconds until key's window resets, for Retry-After. Zero means
// no live window, which the caller reports as unknown rather than as retry now.
func (r *rate_limiter) retry(key string) int {
	r.lock.Lock()
	defer r.lock.Unlock()

	entry := r.entries[key]
	if entry == nil {
		return 0
	}
	remaining := entry.reset - now()
	if remaining <= 0 {
		return 0
	}
	return int(remaining)
}

// remote_rate_limit charges one outbound mochi.remote.* call against both the
// per-target and per-app budgets, returning a *RateLimitError so the caller
// answers
// 429. The target is an entity id, or a URL for peer - whatever is about to be dialled.
func remote_rate_limit(t *sl.Thread, target string) error {
	app, _ := t.Local("app").(*App)
	if app == nil {
		// No app in the thread: an internal call, not app-attributable. There is
		// no budget to charge and no untrusted caller to bound.
		return nil
	}
	entity_key := app.id + "/" + target
	if !rate_limit_remote_entity.allow(entity_key) {
		return rate_limit_refuse(rate_limit_remote_entity, entity_key,
			"remote calls per minute per target")
	}
	if !rate_limit_remote.allow(app.id) {
		return rate_limit_refuse(rate_limit_remote, app.id, "remote calls per minute")
	}
	return nil
}

// spend charges n units against key's budget, for limiters metering a quantity
// rather than counting events. False means the budget was already gone before
// this charge, which still lands, so an overshoot is not forgiven.
func (r *rate_limiter) spend(key string, n int) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	now := now()
	entry := r.entries[key]
	if entry == nil || now >= entry.reset {
		r.entries[key] = &rate_limit_entry{count: n, reset: now + r.window}
		return true
	}

	open := entry.count < r.limit
	entry.count += n
	return open
}

// exhausted reports whether key's budget is gone, without charging it. Used to
// refuse before a relay starts, so the caller gets a clean 429 rather than a body
// that stops partway.
func (r *rate_limiter) exhausted(key string) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	entry := r.entries[key]
	if entry == nil || now() >= entry.reset {
		return false
	}
	return entry.count >= r.limit
}

// stream_bytes_charge meters one read against the per-client and per-app budgets.
// Rounds up to whole kilobytes: a read of a few bytes still costs 1, so a peer
// dribbling single bytes cannot relay indefinitely for free.
func stream_bytes_charge(client, app string, bytes int) {
	kilobytes := (bytes + 1023) / 1024
	rate_limit_stream_client.spend(client, kilobytes)
	rate_limit_stream_app.spend(app, kilobytes)
}

// stream_bytes_refusal returns the refusal for a relay whose byte budget is
// already spent, or nil to proceed.
func stream_bytes_refusal(client, app string) error {
	if rate_limit_stream_client.exhausted(client) {
		return rate_limit_refuse(rate_limit_stream_client, client, "kilobytes relayed per minute per client")
	}
	if rate_limit_stream_app.exhausted(app) {
		return rate_limit_refuse(rate_limit_stream_app, app, "kilobytes relayed per minute")
	}
	return nil
}

// rate_limit_refuse builds the refusal for a limiter that has just declined key.
// Kept in one place so every limiter reports Retry-After from its own window
// rather than each call site deriving it, and so the log line always names which
// budget was hit while the client response never does.
func rate_limit_refuse(limiter *rate_limiter, key, what string) error {
	return &RateLimitError{
		Retry:  limiter.retry(key),
		detail: strconv.Itoa(limiter.limit) + " " + what,
	}
}

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

// since reports how long ago key's current window began, or a large sentinel
// when no window is live, so "no recent action" needs no special case.
// remote_reach uses it to bound the wait for an already-sent address request.
func (r *rate_limiter) since(key string) time.Duration {
	r.lock.Lock()
	defer r.lock.Unlock()

	entry := r.entries[key]
	if entry == nil || now() >= entry.reset {
		return time.Duration(1<<62) * time.Nanosecond
	}
	// The window began at reset - r.window; its age is now minus that.
	began := entry.reset - r.window
	return time.Duration(now()-began) * time.Second
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

// Per-account login throttle for the guessable factors, keyed by uid so it
// follows the account across rotating addresses. A reservation gate, not a
// read-then-sleep: each attempt claims the next slot under the lock, so
// concurrent guesses cannot all slip through on one pre-failure count. Past
// account_wait_maximum it refuses (429).
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
// account_wait_maximum, which is also the deepest queue a request will wait in
// before being refused outright.
var (
	account_gate_free          = 3
	account_gate_floor   int64 = 1   // seconds between consecutive slots at minimum
	account_wait_maximum int64 = 8   // seconds: refuse rather than wait/hold longer
	account_gate_ttl     int64 = 900 // seconds idle before an entry is dropped
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
		return account_wait_maximum
	}
	gap := int64(1) << (steps - 1) // 1, 2, 4, 8, ...
	if gap > account_wait_maximum {
		return account_wait_maximum
	}
	if gap < account_gate_floor {
		return account_gate_floor
	}
	return gap
}

// reserve atomically assigns this attempt a verification slot, returning the
// seconds to wait and false when the queue is deeper than account_wait_maximum
// - in which case the caller answers 429 instead of holding a goroutine.
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
	// Accept only when the whole slot (wait plus the spacing it reserves) fits
	// inside the window, so entry.next never climbs past now+account_wait_maximum
	// under sustained load. A front-of-queue attempt always fits.
	gap := account_gate_spacing(entry.failures)
	if wait+gap > account_wait_maximum {
		return wait, false
	}
	entry.next = start + gap
	entry.pending++
	return wait, true
}

// done settles a reservation from reserve. A wrong guess widens the spacing; a
// correct one clears the penalty but drops the entry only when no other
// reservation is still sleeping, so a mid-flight success cannot rewind their
// slots.
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

// account_gate_guard reserves a slot and waits out the bounded delay. False
// means it already answered 429 and the caller should return. Otherwise the
// caller MUST settle exactly once with account_login.done(uid, verified).
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
		rate_limit_code.cleanup()
		rate_limit_verification.cleanup()
		rate_limit_verification_sender.cleanup()
		rate_limit_p2p.cleanup()
		rate_limit_pubsub_in.cleanup()
		rate_limit_pubsub_control.cleanup()
		rate_limit_peer_request.cleanup()
		rate_limit_record_relay.cleanup()
		rate_limit_entry_withdraw.cleanup()
		rate_limit_url.cleanup()
		rate_limit_net_send.cleanup()
		rate_limit_broadcast.cleanup()
		// Keyed on values a caller chooses, so the map grows from remote
		// input until swept. rate_limit_stream_client is keyed on the app
		// AND the client address, so its ceiling is the address space.
		rate_limit_directory_sync.cleanup()
		rate_limit_directory_push.cleanup()
		rate_limit_entity_create.cleanup()
		rate_limit_remote_entity.cleanup()
		rate_limit_stream_client.cleanup()
		rate_limit_stream_outbound.cleanup()
		// Keyed on an app id, so bounded by what is installed. Swept anyway:
		// a list that covers most limiters invites the next reader to assume
		// each omission was deliberate.
		rate_limit_refusal_log.cleanup()
		rate_limit_remote.cleanup()
		rate_limit_stream_app.cleanup()
	}
}

// account_stepup is the guessing gate for step-up re-authentication, keyed on
// the account like account_login and with the same spacing. Separate on
// purpose: a shared bucket would let a step-up attacker exhaust the user's
// LOGIN budget.
var account_stepup = &account_gate{entries: make(map[string]*account_gate_entry)}

// stepup_gate_reserve sleeps out the spacing this account has earned, reporting
// false when the queue is deeper than account_wait_maximum. Settle every
// reservation with stepup_gate_done - a wrong guess is what widens the spacing.
func stepup_gate_reserve(uid string) bool {
	if uid == "" {
		return false
	}
	wait, ok := account_stepup.reserve(uid)
	if !ok {
		return false
	}
	if wait > 0 {
		time.Sleep(time.Duration(wait) * time.Second)
	}
	return true
}

// stepup_gate_done settles a reservation from stepup_gate_reserve.
func stepup_gate_done(uid string, verified bool) {
	account_stepup.done(uid, verified)
}

// stream_outbound_charge meters bytes an app has written to a peer.
func stream_outbound_charge(app string, bytes int) {
	rate_limit_stream_outbound.spend(app, (bytes+1023)/1024)
}

// stream_outbound_refusal reports whether the app has spent its outbound budget.
func stream_outbound_refusal(app string) error {
	if rate_limit_stream_outbound.exhausted(app) {
		return rate_limit_refuse(rate_limit_stream_outbound, app, "kilobytes written per minute")
	}
	return nil
}
