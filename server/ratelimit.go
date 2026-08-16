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

	// Login-code sends, keyed on the account rather than the IP: 5 per 15
	// minutes. code_send is reachable both from /_/auth/code, which the login
	// middleware covers, and from mochi.user.code.send(), which any app
	// holding user/export can call — so the limit lives on the function every
	// caller shares. Each send also leaves another hour-long code valid (the
	// codes table keys on the code, not the account), so this bounds how many
	// can be outstanding at once as well as the mail volume.
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

	// Directory sync serving: 6 per minute per requesting peer.
	//
	// A sync request is one small anonymous frame, and answering it reads and
	// streams every directory row at or after the requester's watermark - a
	// request with no watermark means the whole table. The asymmetry is the
	// problem, not the size: the rows are public, the node already stores
	// them all, and a joining peer legitimately needs every one, so capping
	// how MANY rows go back would break bootstrap. What has no legitimate
	// form is asking again immediately.
	//
	// directory_sync runs on a 5-minute tick, so 6 per minute is two orders
	// of magnitude of headroom for a peer that restarts, reconnects and
	// re-syncs in quick succession.
	rate_limit_directory_sync = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   6,
		window:  60,
	}

	// Directory push receiving: 6 per minute per pushing peer.
	//
	// The sibling of rate_limit_directory_sync, and the same reasoning applies
	// harder. Sync is anonymous and costs a read; push is anonymous and costs
	// a WRITE per row the peer chooses to send - four validators, up to three
	// SQLite queries and an ed25519 verification each - and the peer decides
	// how many rows there are. It had no limiter at all.
	//
	// directory_sync drives one push per 5-minute tick at most, and the
	// sender's watermark makes steady state one per hourly re-attest cycle, so
	// 6 per minute is two orders of magnitude of headroom for a peer that
	// restarts, reconnects and re-pushes in quick succession.
	rate_limit_directory_push = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   6,
		window:  60,
	}

	// Entity creation from an app: 30 per minute per user.
	//
	// A public entity is not a local row. entity_create mints a keypair, signs
	// an announcement, writes users.db and directory.db, and then floods the
	// mesh - where every peer verifies the signature and writes its own row. One
	// call is therefore N remote writes, and nothing bounded how many calls an
	// app could make. Only WITHDRAWAL was limited (rate_limit_entry_withdraw),
	// which is the wrong way round for a resource.
	//
	// Keyed on the user rather than the app, so a busy server's other accounts
	// are unaffected by one app looping for one of them, and so the bound
	// follows the account the entities belong to. Every app call site creates
	// one entity per user action - none is inside a loop - so 30 a minute is far
	// above hand-driven creation while turning an unbounded loop into one every
	// two seconds.
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

	// Broadcast fan-out, charged one per RECIPIENT: mochi.broadcast.send turns
	// one call into N wire messages and N queue.db rows, and had no limiter.
	//
	// Separate from rate_limit_net_send for the reason rate_limit_verification
	// is separate from rate_limit_code - a shared bucket would let one fan-out
	// exhaust the budget the app's direct sends need - and cannot reuse its
	// size, because charging per recipient changes the unit: feeds broadcasts
	// once per imported RSS item to every subscriber, so a backfill of a few
	// hundred items to a few dozen subscribers legitimately exceeds 1000 in a
	// burst. The minute window absorbs such a burst and bounds sustained volume
	// instead. Per app, so it bounds each app's contribution to the shared
	// queue rather than the total.
	rate_limit_broadcast = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   20000,
		window:  60,
	}

	// Outbound remote request/stream/ping/peer, per app AND target.
	// mochi.remote.* was the one outbound primitive with no limit, while
	// mochi.url.* and mochi.message.send have had one for a long time. Apps
	// proxy a remote entity's assets through public actions - a person's avatar,
	// a feed's image - so an anonymous caller can make this server re-fetch
	// someone else's bytes as fast as it will go.
	//
	// Keyed on the TARGET as well as the app, not on the app alone like its
	// siblings. Keying on the app alone would let a flood against one entity
	// exhaust the budget every other user of that app shares, turning a
	// bandwidth nuisance into a denial of service against ourselves.
	//
	// The ceiling is set by the largest legitimate fan-out at a SINGLE target,
	// which is the apps update sweep: apps.star queries one publisher entity
	// once per app in the catalogue, so a cold-cache sweep costs as many calls
	// as there are apps (27 today). A refusal there does not degrade - Starlark
	// has no try/except, so the builtin error aborts the whole action and the
	// updates page 500s - so this has to clear a catalogue several times larger
	// than today's, not merely today's. 600/minute leaves room to ~600 apps
	// while still turning an unbounded loop into 10/second.
	//
	// It bounds the REQUEST rate, not bytes: 600 banner fetches a minute is
	// still a lot of traffic. Byte accounting is a separate mechanism this does
	// not attempt.
	rate_limit_remote_entity = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   600,
		window:  60,
	}

	// Bytes relayed out of a.write.stream, per app and CLIENT, 2GB per minute.
	// Measured in kilobytes: rate_limit_entry.count is an int, and a byte count
	// this size would overflow it on a 32-bit build.
	//
	// This is the half the per-call cap cannot do. The cap bounds ONE relay; this
	// bounds how many a caller may induce, which is what turns 600 fetches of a
	// 10MB banner from 6GB of traffic into a bounded figure.
	//
	// Keyed on the CLIENT, deliberately not on the target entity like its
	// call-counting sibling. A per-target byte budget is shared by everyone
	// viewing that entity, so a much-visited profile would exhaust it and start
	// refusing legitimate viewers - it would meter popularity rather than abuse.
	// The client is who induces the cost, so the client is what to bound.
	//
	// Sized to clear one transfer of the largest object the platform stores, and
	// held there by TestStorageLimitsAgree. A budget below that does not stop
	// abuse, it truncates honest traffic part-way: the largest real transfers -
	// a repository archive, a market asset download, a video attachment - are
	// public routes an anonymous caller uses honestly, and a fast one completes
	// well inside a single window. It is a ceiling on one client, not a tight
	// quota, and it is the reason object_maximum cannot be raised on its own.
	rate_limit_stream_client = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   10 * 1024 * 1024, // kilobytes: one object_maximum transfer
		window:  60,
	}

	// Same accounting per app, as a circuit breaker against the same flood spread
	// across many addresses, which per-client keying cannot see. Set far above any
	// plausible honest minute so ordinary load never reaches it: a limit low
	// enough to bind in normal use would be shared fate, refusing every user of an
	// app because one of them is being abused.
	rate_limit_stream_app = &rate_limiter{
		entries: make(map[string]*rate_limit_entry),
		limit:   80 * 1024 * 1024, // kilobytes: eight per-client budgets
		window:  60,
	}

	// Outbound stream bytes, per app. Separate from the relay pair above, which
	// meters what comes IN from a peer: the 600 streams/min per target cap says
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

	// Account verification, in two buckets. Per recipient bounds what one
	// mailbox receives; per sender bounds a spray across many addresses, which
	// the recipient bucket never sees because each victim gets only one.
	//
	// Separate from rate_limit_code rather than sharing it: a shared bucket
	// would let verification traffic aimed at an address exhaust the budget
	// that address needs to receive a login code, which is the lockout core
	// 266798d0 kept step-up out of the login bucket to avoid.
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

// RateLimitError is a refusal that the HTTP layer turns into a 429 rather than
// the generic 500 every other builtin error becomes.
//
// Starlark has no try/except, so a limiter refusing inside a builtin unwinds the
// whole action, and web_action_error saw only an opaque error: it answered 500
// with the error text, which named the internal function and the budget. A 500
// is not merely untidy - it reads as "the server faulted", so a correct client
// retries it, and each retry recharges the budget the limiter is trying to
// protect. Modelled on PermissionError, which already survives the unwind this
// way (sl_error wraps with %w precisely so errors.As keeps working).
//
// Retry is seconds until the window resets, for Retry-After. Zero means unknown,
// in which case no header is sent rather than a guessed one.
//
// detail names which budget was exhausted and how large it is. It reaches the log
// and never the client: the numbers are what the old 500 disclosed to anonymous
// callers, and an operator reading "which limit fired" is the only party who needs
// them.
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

// retry reports seconds until key's window resets, for Retry-After. Zero when no
// window is live, which the caller reports as "unknown" rather than as "retry
// now". reset and now() are both Unix seconds, so the difference is already whole
// seconds and is at least 1 whenever it is positive - there is no rounding to do,
// and no way to answer a misleading 0 while a window is still open.
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
// per-target and per-app budgets. Returns nil to proceed, or a *RateLimitError so
// the HTTP layer answers 429 with Retry-After instead of a generic 500.
// The target is an entity id for request/stream/ping and a URL for peer; either
// way it is the thing we are about to dial, which is what needs bounding.
//
// The detail (which budget, and how large) goes into the error text for the log
// only - the response body carries a translated label with no numbers in it. The
// numbers come from the limiters themselves rather than being written out here: a
// hardcoded figure drifts the moment a limit is retuned, and then names a budget
// that is not the one being enforced.
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

// spend charges n units against key's budget, for limiters that meter a quantity
// (kilobytes relayed) rather than counting events. Returns false when the budget
// was already gone BEFORE this charge.
//
// The charge still lands when it returns false, so an overshoot is recorded
// rather than discarded - a caller that blows the budget by 30MB should wait for
// that, not have it forgiven.
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

// since reports how long ago the current window for key began — i.e. how long
// ago the first (and, for a limit-1 limiter, only) allowed action in this
// window happened. Returns a large sentinel when there is no live window, so a
// caller treating "no recent action" as "long ago" needs no special case.
//
// Used by remote_reach: when an address request for a target was suppressed
// because one already went out this minute, this says how stale that request
// is, so the wait for its answer can be bounded by when it was actually sent
// rather than restarting a full window.
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
	account_gate_free        = 3
	account_gate_floor int64 = 1   // seconds between consecutive slots at minimum
	account_wait_max   int64 = 8   // seconds: refuse rather than wait/hold longer
	account_gate_ttl   int64 = 900 // seconds idle before an entry is dropped
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
// the account like account_login and with the same spacing.
//
// Separate from account_login on purpose. Sharing one bucket would stop an
// attacker refreshing their budget by alternating between the two, but it
// would also let a step-up attacker exhaust the legitimate user's LOGIN
// budget - locking them out of signing in, using only their address. The
// login path already carries that denial-of-service property; a second
// trigger for it is not worth the marginal gain, since the two credentials
// guard different things.
var account_stepup = &account_gate{entries: make(map[string]*account_gate_entry)}

// stepup_gate_reserve slows a step-up guess, sleeping out the spacing this
// account has earned. Reports false when the queue is deeper than
// account_wait_max, i.e. refuse rather than hold the caller any longer.
//
// The context-free half of account_gate_guard: the Starlark step-up builtins
// have no gin.Context to hang a Retry-After header on, and a gate an app has
// to remember to call is not a gate. Settle every reservation with
// stepup_gate_done - a wrong guess is what widens the spacing, so failing to
// report one leaves the gate open.
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
