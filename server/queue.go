// Mochi server: Message queue with reliable delivery
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"fmt"
	"math/rand"
	rd "runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
)

// queue_per_peer_concurrency caps in-flight sends to one peer per tick.
// Concurrent streams to a peer are safe: libp2p multiplexes, ACK dedup keys are
// per-message, and SQL ops apply by sequence number, so wire order is rebuilt.
const queue_per_peer_concurrency = 8

// File pushes stay serial per peer — one in-flight file at a time per
// peer. Parallel file pushes only divide the same bandwidth.
const queue_per_peer_file_concurrency = 1

// How long a row may stay claimed before the safety net assumes the sender
// holding it is gone. Long enough that a slow but live send is never taken
// away from its sender; short enough that a row lost to a dead process comes
// back without waiting for a restart.
const queue_claim_timeout = 60

// Message priority tiers, stored in queue.priority and used by
// queue_process to order delivery — higher is more urgent. Spaced by 10
// so a tier can be inserted between two existing ones (or below bulk)
// without renumbering, since the values are purely ordinal.
const (
	// 40 was priority_control and 10 was priority_bulk, both produced only by
	// queue_priority's replication branch. The wire still has all three tiers
	// (frame_priority_*), which is a receiver-side vocabulary and outlives the
	// queue lanes that fed it.
	priority_replay      = 30 // broadcast resync replies: jump live broadcast queue
	priority_interactive = 20 // normal app and entity messages (the default)
)

// queue_silent_defer is how far a row's next_retry moves when its target is in
// the silent-failure cache. queue_resurrect_peer pulls it back on reconnect.
const queue_silent_defer = 3600 // 1 hour

// queue_priority classifies an outbound message into a priority tier. Every
// message is interactive; the function stays as the seam any future tiering
// goes through. A caller that knows its lane uses queue_add_direct_priority
// instead.
func queue_priority(service, event string) int {
	return priority_interactive
}

// Queue entry for outgoing messages
type QueueEntry struct {
	ID           string `db:"id"`
	Type         string `db:"type"`
	Target       string `db:"target"`
	FromEntity   string `db:"from_entity"`
	ToEntity     string `db:"to_entity"`
	Service      string `db:"service"`
	Event        string `db:"event"`
	FromApp      string `db:"from_app"`
	FromServices string `db:"from_services"`
	Content      []byte `db:"content"`
	Data         []byte `db:"data"`
	File         string `db:"file"`
	Expires      int64  `db:"expires"`
	Status       string `db:"status"`
	Attempts     int    `db:"attempts"`
	NextRetry    int64  `db:"next_retry"`
	LastError    string `db:"last_error"`
	Created      int64  `db:"created"`
	Priority     int    `db:"priority"`
	// When the row was last marked 'sending'. Created is the enqueue time and
	// never changes, so it cannot answer "how long has a sender held this",
	// which is the only question the stuck-sending safety net is asking.
	Claimed int64 `db:"claimed"`
}

// queue_age_maximum is the retention floor for every queued message.
const queue_age_maximum = 7 * 86400 // 7 days

// queue_wake_ch nudges the queue manager to process immediately rather than
// wait for the next tick. Buffer of 1, so wakes between ticks coalesce into one
// pass.
var queue_wake_ch = make(chan struct{}, 1)

// self_loop_wake_ch nudges the self_loop_drain goroutine to claim
// pending self-loop rows immediately. Same buffer-1 coalescing as
// queue_wake_ch and Sender.wake — multiple wakes between drains
// collapse into a single pass.
var self_loop_wake_ch = make(chan struct{}, 1)

// queue_wake nudges the queue manager, the self_loop drain, AND every
// open /mochi/2/messages Sender. Non-blocking on all three —
// already-pending wakes are dropped. Each consumer drains the slice of
// queue.db it owns:
//
//   - Senders' pull_loop: direct rows with target == <its peer>
//   - self_loop_drain: direct rows with target == net_id
//   - queue_process: everything else (broadcasts, file pushes,
//     offline-peer fast-fails, empty-target rows)
func queue_wake() {
	select {
	case queue_wake_ch <- struct{}{}:
	default:
	}
	select {
	case self_loop_wake_ch <- struct{}{}:
	default:
	}
	senders_wake_all()
}

// senders_wake_all signals every open Sender's pull loop. Non-blocking
// per Sender — already-pending wakes are dropped. Cheap enough to call
// from the queue-add hot path because each Sender has a buffer-1
// wake channel.
func senders_wake_all() {
	senders_lock.Lock()
	defer senders_lock.Unlock()
	for _, s := range senders {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// queue_claim_for_peer atomically claims up to `limit` rows targeting `peer`,
// marking them 'sending' so queue_process will not double-pick them. File
// pushes are excluded: they ride /mochi/2/stream, not the Sender's persistent
// stream.
func queue_claim_for_peer(peer string, limit int) []QueueEntry {
	if peer == "" || limit <= 0 {
		return nil
	}
	db := db_open("db/queue.db")
	var rows []QueueEntry
	err := db.scans(&rows, `update queue set status='sending', claimed=?
		where id in (
			select id from queue
			where target=? and status='pending' and next_retry<=?
				and type='direct' and event != 'file/push'
			order by priority desc, next_retry asc
			limit ?
		)
		returning id, type, target, from_entity, to_entity, service, event,
			from_app, from_services, content, data, file, expires, status,
			attempts, next_retry, created, priority`,
		now(), peer, now(), limit)
	if err != nil {
		info("queue_claim_for_peer error peer=%q: %v", peer, err)
		return nil
	}
	return rows
}

// queue_claim_for_self is queue_claim_for_peer for target == net_id, claimed by
// self_loop_drain. File pushes are excluded: file/push to self is a no-op
// nobody emits, and queue_process picks one up if it ever appears.
func queue_claim_for_self(limit int) []QueueEntry {
	if net_id == "" || limit <= 0 {
		return nil
	}
	db := db_open("db/queue.db")
	var rows []QueueEntry
	err := db.scans(&rows, `update queue set status='sending', claimed=?
		where id in (
			select id from queue
			where target=? and status='pending' and next_retry<=?
				and type='direct' and event != 'file/push'
			order by priority desc, next_retry asc
			limit ?
		)
		returning id, type, target, from_entity, to_entity, service, event,
			from_app, from_services, content, data, file, expires, status,
			attempts, next_retry, created, priority`,
		now(), net_id, now(), limit)
	if err != nil {
		info("queue_claim_for_self error: %v", err)
		return nil
	}
	return rows
}

// Retry delays: 1m, 2m, 4m, 8m, 15m, 30m, 1h
var retry_delays = []int64{60, 120, 240, 480, 900, 1800, 3600}

// Calculate next retry time with exponential backoff and jitter
func queue_next_retry(attempts int) int64 {
	idx := attempts
	if idx >= len(retry_delays) {
		idx = len(retry_delays) - 1
	}
	delay := retry_delays[idx]
	jitter := rand.Int63n(delay / 4)
	return now() + delay + jitter
}

// Per-(target, service) backlog thresholds queue_watchdog warns past: a local
// wedge, whatever the destination's state. Age is deliberately not per-bucket -
// see queue_warn_stale_targets. var (not const) so tests can lower them.
var queue_warn_rows int64 = 10000
var queue_warn_age int64 = 2 * 86400
var queue_warn_attempts int64 = 100

// queue_warn_stale_targets is how many distinct destinations must hold a stale
// unclassified row before the age signal warns. One unreachable peer cannot be
// told from a local fault; several going stale together can.
var queue_warn_stale_targets int64 = 5

// queue_warn_silence is how long with no successful delivery to ANY
// destination, while stale rows are pending, before the age signal warns
// regardless of breadth: nothing delivering anywhere reads as this server being
// off the network.
var queue_warn_silence int64 = 6 * 3600

// queue_delivered is the unix time of the last successful delivery to any
// destination, stamped by both ack paths. Zero means nothing since start, which
// the silence check treats as no evidence rather than as silence.
var queue_delivered atomic.Int64

var queue_stale_warned int64 // last age-breadth warn unix

// queue_warn_repeat is the re-warn cadence: a bucket warns on the tick
// it first trips a threshold, then once per repeat window while the
// condition persists, instead of every tick.
var queue_warn_repeat int64 = 86400

var queue_warned sync.Map // target+"|"+service -> last warn unix

// queue_park_attempts is the retry budget before queue_fail parks a row
// (status='parked', outside every claim path). At an hourly backoff cap, 50
// attempts is about two days. var (not const) so tests can lower it.
var queue_park_attempts = 50

// queue_warn_suspended is how many recipients suspended WITHIN THE PAST DAY
// make queue_watchdog warn. The burst is the signal; older suspensions are
// excluded because handled residue accumulates forever on a long-lived server.
var queue_warn_suspended int64 = 10

// queue_suspended_warned is the last unix time the breadth warn fired.
// Only queue_watchdog's manager goroutine touches it.
var queue_suspended_warned int64

// Per-recipient delivery health: an exhausted retry budget with no
// contradicting success suspends a recipient, cutting broadcast fan-out (only)
// to a periodic probe and eventually evicting the subscriber. var, so tests can
// lower them.
var queue_denial_limit int64 = 3
var queue_probe_interval int64 = 3 * 86400
var queue_evict_age int64 = 30 * 86400

// health_success records evidence the recipient is alive — a delivered
// row, a broadcast ack, or inbound verified contact — clearing any
// failure streak and suspension. A bare update: healthy recipients
// carry no health row at all.
func health_success(recipient string) {
	if recipient == "" {
		return
	}
	// Any delivery anywhere is the evidence the watchdog's silence check
	// wants; stamped before the health-row test so the healthy common
	// case, which returns early, still counts.
	queue_delivered.Store(now())
	db := db_open("db/queue.db")
	// Point read first: the healthy common case has no health row, and this runs
	// on delivery hot paths where an unconditional write transaction per message
	// would cost. The read-to-write race is harmless; the next ack clears it.
	if ok, _ := db.exists("select 1 from health where recipient=?", recipient); !ok {
		return
	}
	db.exec_bg("health success", "update health set failures=0, denials=0, success=?, suspended=0 where recipient=?", now(), recipient)
}

// health_failure records a parked row - a whole retry budget burned - against
// the recipient. Suspends only when nothing from them landed since `created`: a
// success mid-window is a per-message problem, not a dead recipient.
func health_failure(recipient string, created int64) {
	if recipient == "" {
		return
	}
	db := db_open("db/queue.db")
	moment := now()
	db.exec_bg("health failure", "insert into health (recipient, failures, since) values (?, 1, ?) on conflict(recipient) do update set failures = health.failures + 1, since = case when health.failures = 0 then excluded.since else health.since end", recipient, moment)
	db.exec_bg("health suspend", "update health set suspended=? where recipient=? and suspended=0 and success < ?", moment, recipient, created)
}

// health_denial records an authoritative unknown_user answer: the
// recipient's host responded and stated the recipient does not exist
// there. Stronger than silence — queue_denial_limit consecutive denials
// suspend immediately.
func health_denial(recipient string) {
	if recipient == "" {
		return
	}
	db := db_open("db/queue.db")
	moment := now()
	db.exec_bg("health denial", "insert into health (recipient, denials, since) values (?, 1, ?) on conflict(recipient) do update set denials = health.denials + 1, since = case when health.denials = 0 and health.failures = 0 then excluded.since else health.since end", recipient, moment)
	db.exec_bg("health suspend on denial", "update health set suspended=? where recipient=? and suspended=0 and denials >= ?", moment, recipient, queue_denial_limit)
}

// health_gate is consulted by broadcast fan-out per subscriber. Suspended
// recipients are skipped except one probe per queue_probe_interval, which goes
// as a normal send: its ack unsuspends, its park re-confirms.
func health_gate(recipient string) (skip bool, evict bool) {
	db := db_open("db/queue.db")
	var h struct {
		Suspended int64 `db:"suspended"`
		Probed    int64 `db:"probed"`
	}
	if !db.scan(&h, "select suspended, probed from health where recipient=?", recipient) {
		return false, false
	}
	if h.Suspended == 0 {
		return false, false
	}
	moment := now()
	if moment-h.Suspended > queue_evict_age {
		return true, true
	}
	if moment-h.Probed > queue_probe_interval {
		db.exec_bg("health probe", "update health set probed=? where recipient=?", moment, recipient)
		return false, false
	}
	return true, false
}

// health_evict_record throttles the eviction dispatch to once per day per (app,
// recipient). In-memory, so a restart costs one extra dispatch. The overdue
// clock lives in health.evicted instead: restarts outpace health_evict_overdue.
type health_evict_record struct {
	last   int64
	warned bool
}

var health_evict_state sync.Map // app.id+"|"+recipient -> health_evict_record

// health_evict_overdue is how long an (app, recipient) pair may keep receiving
// daily eviction dispatches before the operator is warned. A handling app drops
// the subscriber on the first one, so still being here means it has no handler.
var health_evict_overdue int64 = 7 * 86400

// health_evict_dispatch tells the owning app - once per day per (app,
// recipient) - that a subscriber has been unreachable past queue_evict_age.
// Fired from the fan-out gate, where the cost recurs and app context exists; no
// handler, no-op.
func health_evict_dispatch(user *User, app *App, service, recipient string) {
	key := app.id + "|" + recipient
	moment := now()
	record := health_evict_record{}
	if v, ok := health_evict_state.Load(key); ok {
		record = v.(health_evict_record)
	}
	if record.last != 0 && moment-record.last < 86400 {
		return
	}
	record.last = moment
	db := db_open("db/queue.db")
	var h struct {
		Since     int64 `db:"since"`
		Suspended int64 `db:"suspended"`
		Evicted   int64 `db:"evicted"`
	}
	_ = db.scan(&h, "select since, suspended, evicted from health where recipient=?", recipient)
	// Stamp the first dispatch: this is what lets queue_cleanup delete the row. An
	// unstamped row keeps gating, because forgetting it would let the subscriber
	// recycle a fresh retry ladder with its subscriber row in place.
	if h.Evicted == 0 {
		h.Evicted = moment
		db.exec("update health set evicted=? where recipient=? and evicted=0", moment, recipient)
	}
	if !record.warned && moment-h.Evicted >= health_evict_overdue {
		record.warned = true
		warn("App %q has been dispatched %s for subscriber %q daily for %d days and has not dropped it; the app is probably missing a handler for that event.", app.id, error_code_subscriber_unreachable, recipient, (moment-h.Evicted)/86400)
	}
	health_evict_state.Store(key, record)
	target := recipient
	subscriber_dispatch(user, app, error_code_subscriber_unreachable, "unreachable", service, target, nil, func() map[string]any {
		return map[string]any{"subscriber": target, "since": h.Since, "suspended": h.Suspended}
	})
}

// subscriber_dispatch is error_dispatch behind a var so tests can
// capture eviction dispatches without standing up an app registry.
var subscriber_dispatch = error_dispatch

// queue_watchdog runs every db_manager tick and warns when a (target, service)
// bucket is not draining. Only rows health has not classified count. Rows and
// attempts warn per bucket; age warns only across buckets (breadth or silence).
func queue_watchdog() {
	db := db_open("db/queue.db")
	if db == nil {
		return
	}
	var buckets []struct {
		Target   string `db:"target"`
		Service  string `db:"service"`
		Total    int64  `db:"total"`
		Oldest   int64  `db:"oldest"`
		Attempts int64  `db:"attempts"`
	}
	err := db.scans(&buckets, "select target, service, count(*) as total, min(created) as oldest, max(attempts) as attempts from queue where status = 'pending' and to_entity not in (select recipient from health where suspended != 0) group by target, service")
	if err != nil {
		return
	}
	now := now()
	unhealthy := map[string]bool{}
	stale := map[string]bool{} // distinct targets holding a row past queue_warn_age
	var oldest int64
	for _, bucket := range buckets {
		key := bucket.Target + "|" + bucket.Service
		age := now - bucket.Oldest
		if age >= queue_warn_age {
			stale[bucket.Target] = true
			if oldest == 0 || bucket.Oldest < oldest {
				oldest = bucket.Oldest
			}
		}
		if bucket.Total < queue_warn_rows && bucket.Attempts < queue_warn_attempts {
			queue_warned.Delete(key)
			continue
		}
		unhealthy[key] = true
		if v, ok := queue_warned.Load(key); ok && now-v.(int64) < queue_warn_repeat {
			continue
		}
		queue_warned.Store(key, now)
		warn("Queue backlog: %d pending rows for (target=%q, service=%q), oldest %.1f days old, attempts up to %d; deliveries to this destination are not draining.", bucket.Total, bucket.Target, bucket.Service, float64(age)/86400, bucket.Attempts)
	}
	// Buckets that drained entirely no longer appear in the query; drop
	// their re-warn tracking so a future recurrence warns fresh.
	queue_warned.Range(func(key, _ any) bool {
		if !unhealthy[key.(string)] {
			queue_warned.Delete(key)
		}
		return true
	})

	// Age, across buckets: enough stale destinations to implicate this side, or
	// nothing delivered anywhere for queue_warn_silence. A zero delivery stamp is
	// no evidence, and silence only counts while something is stale.
	delivered := queue_delivered.Load()
	silent := len(stale) > 0 && delivered != 0 && now-delivered >= queue_warn_silence
	if int64(len(stale)) < queue_warn_stale_targets && !silent {
		queue_stale_warned = 0
	} else if queue_stale_warned == 0 || now-queue_stale_warned >= queue_warn_repeat {
		queue_stale_warned = now
		if silent {
			warn("Queue health: %d destination(s) have held undelivered rows for over %.1f days and nothing has been delivered to any destination for %.1f hours; this server may be unable to resolve or reach peers.", len(stale), float64(now-oldest)/86400, float64(now-delivered)/3600)
		} else {
			warn("Queue health: %d distinct destinations have held undelivered rows for over %.1f days (threshold %d). One or two is a departed peer; this many together suggests directory resolution or connectivity is broken here.", len(stale), float64(now-oldest)/86400, queue_warn_stale_targets)
		}
	}

	// Breadth: each suspension is routine on its own, so crossing the
	// threshold is the first moment anything emails about them at all.
	// Only the past day's suspensions count — see queue_warn_suspended.
	suspended := int64(db.integer("select count(*) from health where suspended >= ?", now-86400))
	if suspended < queue_warn_suspended {
		queue_suspended_warned = 0
		return
	}
	if queue_suspended_warned != 0 && now-queue_suspended_warned < queue_warn_repeat {
		return
	}
	queue_suspended_warned = now
	warn("Queue health: %d recipients were suspended as unreachable within the past day (threshold %d). A few is the normal residue of departed peers; this many at once suggests directory resolution or connectivity is broken for recipients that may be fine.", suspended, queue_warn_suspended)
}

// Add a direct message to the queue. Caller can override the default
// (service+event)-derived priority by calling queue_add_direct_priority
// instead — used by broadcast_resync to ship replies in the priority_replay
// lane so they overtake the live-broadcast backlog.
func queue_add_direct(id, target, from_entity, to_entity, service, event, from_app string, services []string, content, data []byte, file string, expires int64) {
	queue_add_direct_priority(id, target, from_entity, to_entity, service, event, from_app, services, content, data, file, expires, queue_priority(service, event))
}

// queue_add_direct_priority is queue_add_direct with an explicit priority
// override. Callers that know the message deserves a different tier
// (currently only broadcast_resync, which marks replies priority_replay)
// pass it directly; the (service, event) default is bypassed.
func queue_add_direct_priority(id, target, from_entity, to_entity, service, event, from_app string, services []string, content, data []byte, file string, expires int64, priority int) {
	db := db_open("db/queue.db")
	from_services := strings.Join(services, ",")
	db.exec(`insert or replace into queue
		(id, type, target, from_entity, to_entity, service, event, from_app, from_services, content, data, file, expires, status, attempts, next_retry, created, priority)
		values (?, 'direct', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)`,
		id, target, from_entity, to_entity, service, event, from_app, from_services, content, data, file, expires, now(), now(), priority)
}

// Add a broadcast message to the queue
func queue_add_broadcast(id, from_entity, to_entity, service, event, from_app string, services []string, content, data []byte, expires int64) {
	db := db_open("db/queue.db")
	from_services := strings.Join(services, ",")
	db.exec(`insert or replace into queue
		(id, type, target, from_entity, to_entity, service, event, from_app, from_services, content, data, file, expires, status, attempts, next_retry, created, priority)
		values (?, 'broadcast', 'pubsub', ?, ?, ?, ?, ?, ?, ?, ?, '', ?, 'pending', 0, ?, ?, ?)`,
		id, from_entity, to_entity, service, event, from_app, from_services, content, data, expires, now(), now(), queue_priority(service, event))
}

// Mark a message as acknowledged (remove from queue). A successful delivery
// also confirms the target peer in the learned directory; the batch ack-flush
// path skips that, and partial coverage is fine since learned rows never age
// out.
func queue_ack(id string) {
	db := db_open("db/queue.db")
	var q QueueEntry
	if db.scan(&q, "select from_entity, to_entity, target from queue where id = ?", id) {
		health_success(q.ToEntity)
		if q.Target != "" {
			if user := user_owning_entity(q.FromEntity); user != nil {
				directory_user_confirm(user, q.ToEntity, q.Target)
			}
		}
	}
	db.exec_bg("queue ack delete", "delete from queue where id = ?", id)
	//debug("Queue ACK received for %q", id)
}

// queue_ack_ch buffers IDs from the worker pool and Sender read loops;
// queue_ack_batcher collapses them into one DELETE per batch. Capacity is
// generous so an ack burst does not fall through to the synchronous fallback.
var queue_ack_ch = make(chan string, 4096)

// queue_ack_batch caps a single DELETE's IN-list size; SQLite's default
// is 999 host parameters. Stay well under that to leave room for any
// driver-side prepared-statement overhead.
const queue_ack_batch = 256

// queue_ack_interval is the maximum time a worker's ack can sit in the
// buffer before being flushed even if the batch isn't full. Short
// enough that low-traffic acks aren't visibly delayed; long enough to
// amortise tx overhead under load.
const queue_ack_interval = 20 * time.Millisecond

// queue_ack_async pushes id onto queue_ack_ch for batched deletion.
// Non-blocking: a full channel falls back to synchronous queue_ack so progress
// is never lost.
func queue_ack_async(id string) {
	if id == "" {
		return
	}
	select {
	case queue_ack_ch <- id:
	default:
		queue_ack(id)
	}
}

// queue_ack_batcher drains queue_ack_ch into one DELETE per flush, saving a
// SQLite transaction per ack. An ID buffered when the process dies replays
// after restart: the row stays 'sending' until the timeout, and message_seen
// dedups it.
func queue_ack_batcher() {
	batch := make([]string, 0, queue_ack_batch)
	timer := time.NewTimer(queue_ack_interval)
	defer timer.Stop()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		queue_ack_flush(batch)
		batch = batch[:0]
	}
	for {
		select {
		case id := <-queue_ack_ch:
			batch = append(batch, id)
			if len(batch) >= queue_ack_batch {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(queue_ack_interval)
			}
		case <-timer.C:
			flush()
			timer.Reset(queue_ack_interval)
		}
	}
}

// queue_ack_drain pulls every queued ack from queue_ack_ch and
// flushes them synchronously. Used by tests that verify queue state
// after an ack — production has queue_ack_batcher draining the
// channel, but tests don't start that goroutine.
func queue_ack_drain() {
	batch := make([]string, 0, queue_ack_batch)
	for {
		select {
		case id := <-queue_ack_ch:
			batch = append(batch, id)
		default:
			queue_ack_flush(batch)
			return
		}
	}
}

// queue_ack_flush issues one DELETE for the given IDs. Caller must
// hold no locks; this opens db/queue.db via the cached handle.
func queue_ack_flush(ids []string) {
	if len(ids) == 0 {
		return
	}
	db := db_open("db/queue.db")
	placeholders := make([]byte, 0, len(ids)*2)
	args := make([]any, len(ids))
	for i, id := range ids {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args[i] = id
	}
	// Delivery success clears any failure streak for these recipients
	// before the rows disappear — one set-based update; a no-op for
	// recipients with no health row (the healthy common case).
	queue_delivered.Store(now())
	db.exec_bg("health success flush", "update health set failures=0, denials=0, success=?, suspended=0 where recipient in (select to_entity from queue where id in ("+string(placeholders)+"))", append([]any{now()}, args...)...)
	db.exec_bg("queue ack flush", "delete from queue where id in ("+string(placeholders)+")", args...)
}

// queue_drain_entity waits up to `wait` for every queued message from `entity`
// to leave the queue. Teardown deletes the signing key, after which unsent rows
// can never be claimed; on timeout the farewell is lost and receivers
// self-heal.
func queue_drain_entity(entity string, wait time.Duration) {
	if entity == "" {
		return
	}
	db := db_open("db/queue.db")
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		exists, _ := db.exists("select 1 from queue where from_entity=? limit 1", entity)
		if !exists {
			return
		}
		queue_wake()
		time.Sleep(50 * time.Millisecond)
	}
	info("Queue drain timeout: farewell messages from entity %q still queued at teardown", entity)
}

// queue_drop removes a queue row without scheduling a retry. Use when the
// receiver's NACK reason says another attempt would fail identically (e.g.
// "broadcast-gap", already being caught up by resync). queue_fail is the
// default.
func queue_drop(id, reason string) {
	db := db_open("db/queue.db")
	var q QueueEntry
	have := db.scan(&q, "select * from queue where id = ?", id)
	db.exec_bg("queue drop on nack", "delete from queue where id = ?", id)
	debug("Queue dropping message %q on NACK reason %q (no retry)", id, reason)
	// Surface a terminal NACK to the sending app — after the delete, so the
	// handler never runs while the row is being removed. unknown/rejected
	// map to a code; fail_dedup and unmapped reasons dispatch nothing.
	if have {
		if reason == fail_unknown_user {
			// The recipient's host answered and said they don't exist
			// there — authoritative evidence for the health record.
			health_denial(q.ToEntity)
		}
		if code, error_reason, ok := error_code_for_nack(reason); ok {
			queue_error_dispatch(&q, code, error_reason)
		}
	}
}

// queue_error_dispatch surfaces a terminal send failure as a core error
// event to the app that queued the row. Indirected through a var so tests
// can capture the call sites (reason->code mapping, dedup) without standing
// up an app/user. Call AFTER the row is removed from queue.db.
var queue_error_dispatch = queue_error_dispatch_real

// queue_error_dispatch_real resolves the row's owning user and app, then
// dispatches; cheap when that app declares no handler (the entity_peers
// lookup for detail.locations runs only when a handler exists, via
// error_dispatch's thunk).
func queue_error_dispatch_real(q *QueueEntry, code, reason string) {
	if q.FromApp == "" || q.FromEntity == "" {
		return
	}
	user := user_owning_entity(q.FromEntity)
	if user == nil {
		return
	}
	app := app_by_id(q.FromApp)
	if app == nil {
		return
	}
	original := map[string]any{
		"service": q.Service,
		"event":   q.Event,
		"message": q.ID,
	}
	var detail func() map[string]any
	if code == error_code_message_unknown || code == error_code_message_timeout {
		to := q.ToEntity
		detail = func() map[string]any {
			if _, ok := entity_local(to); !ok {
				// The ownership check itself failed; "unknown" must not
				// read as "gone" — locations == 0 tells apps the entity
				// has no host left (feeds deletes the subscriber on it).
				return map[string]any{"locations": int64(1)}
			}
			return map[string]any{"locations": int64(len(entity_peers(to)))}
		}
	}
	error_dispatch(user, app, code, reason, q.Service, q.ToEntity, original, detail)
}

// Mark a message as being sent (prevents other processors from picking it up)
func queue_sending(id string) {
	db := db_open("db/queue.db")
	db.exec_bg("queue mark sending", "update queue set status='sending', claimed=? where id=?", now(), id)
}

// queue_unsending rolls back queue_sending when the async send path
// fails before the row enters its inflight tracking (e.g. peer_send
// returns error_sender_unreachable). Returns the row to 'pending' so the
// next queue_select picks it up.
func queue_unsending(id string) {
	db := db_open("db/queue.db")
	db.exec_bg("queue unsending rollback", "update queue set status='pending' where id=? and status='sending'", id)
}

// queue_is_inflight returns true when the row is currently owned by
// the /mochi/2 async resolver (status='sending'). queue_process uses
// this to skip queue_fail for rows the resolver will resolve itself.
func queue_is_inflight(id string) bool {
	db := db_open("db/queue.db")
	row, err := db.row("select status from queue where id=?", id)
	if err != nil || row == nil {
		return false
	}
	s, _ := row["status"].(string)
	return s == "sending"
}

// queue_defer pushes a row's next_retry forward without incrementing attempts:
// a deliberately skipped row is not failing, so the backoff must not escalate.
func queue_defer(id string, delay int64) {
	db := db_open("db/queue.db")
	db.exec_bg("queue defer", "update queue set next_retry = ? where id = ?", now()+delay, id)
}

// queue_defer_target pushes every pending row for a target forward in one
// UPDATE, so queue_select stops re-scanning a silent peer's backlog. Row-by-row
// deferral walks the whole backlog once per tick. Idempotent; only rows due
// before `until`.
func queue_defer_target(target string, until int64) {
	if target == "" {
		return
	}
	db := db_open("db/queue.db")
	db.exec_bg("queue defer target", "update queue set next_retry = ? where target = ? and status = 'pending' and next_retry < ?", until, target, until)
}

// queue_resurrect_peer brings every deferred row for a peer back into the ready
// set, called from peer_connect so a reviving peer's backlog drains at once
// instead of waiting out the deferred next_retry.
func queue_resurrect_peer(target string) {
	if target == "" {
		return
	}
	db := db_open("db/queue.db")
	t := now()
	db.exec_bg("queue resurrect peer", "update queue set next_retry = ? where target = ? and status = 'pending' and next_retry > ?", t, target, t)
	// Parked rows (retry budget spent while the peer was away) revive on
	// reconnect: the peer coming back is exactly the condition parking
	// waited for. Attempts stay — if the peer is back but deliveries
	// still fail, the first failure re-parks instead of re-grinding.
	db.exec_bg("queue resurrect parked", "update queue set status = 'pending', next_retry = ? where target = ? and status = 'parked'", t, target)
	queue_wake()
}

// Mark a message as failed and schedule retry or drop
func queue_fail(id string, err string) {
	db := db_open("db/queue.db")

	var q QueueEntry
	if !db.scan(&q, "select * from queue where id = ?", id) {
		return
	}

	attempts := q.Attempts + 1
	age := time.Now().Unix() - q.Created

	if age > queue_age_maximum {
		//warn("Queue dropping message after %d attempts: id=%q type=%q from=%q to=%q service=%q event=%q error=%q", attempts, q.ID, q.Type, q.FromEntity, q.ToEntity, q.Service, q.Event, err)
		db.exec_bg("queue fail drop aged", "delete from queue where id = ?", id)
		// The retry budget is exhausted: the learned route (if any) is
		// proven dead, not merely old — evict it so future sends surface
		// undeliverable immediately instead of burning another budget.
		if q.Target != "" {
			if user := user_owning_entity(q.FromEntity); user != nil {
				directory_user_forget(user, q.ToEntity, q.Target)
			}
		}
		queue_error_dispatch(&q, error_code_message_timeout, "timeout")
	} else if queue_retarget(db, &q) {
		// The failure was our routing, not the peer: the row is now aimed at a
		// live route with a fresh budget. Checked before parking, so a row does
		// not spend the rest of its age waiting on a peer that has moved.
	} else if attempts >= queue_park_attempts {
		// Retry budget spent while still inside the age budget: park rather than
		// grind hourly retries for the remaining days. Parked rows keep their data,
		// revive on queue_resurrect_peer, and age out through the status-blind sweep.
		db.exec_bg("queue fail park", "update queue set status = 'parked', attempts = ?, last_error = ? where id = ?", attempts, err, id)
		// A parked row is a full retry budget burned against this recipient, so feed
		// the health record. The park is the machinery working: log for forensics, no
		// admin email.
		health_failure(q.ToEntity, q.Created)
		if q.Target == "" {
			// No peer to reconnect: the recipient entity never resolved
			// to any host, so these rows only age out.
			info("Queue parked a delivery for (service=%q) with no resolvable recipient after %d failed attempts (latest: %s); rows keep their data and are reaped after %d days.", q.Service, attempts, err, queue_age_maximum/86400)
		} else {
			info("Queue parked a delivery for (target=%q, service=%q) after %d failed attempts (latest: %s); rows keep their data, revive if the peer reconnects, and are reaped after %d days.", q.Target, q.Service, attempts, err, queue_age_maximum/86400)
		}
	} else {
		// Schedule retry
		next := queue_next_retry(attempts)
		db.exec_bg("queue fail retry reschedule", "update queue set status = 'pending', attempts = ?, next_retry = ?, last_error = ? where id = ?", attempts, next, err, id)
		//debug("Queue message %q scheduled for retry %d at %d: %s", id, attempts, next, err)
	}
}

// queue_expand_empty_target is the retry-time fan-out: a row with an empty
// target clones (N-1) siblings for the extra peers entity_peers now finds and
// returns the first for this attempt. Empty string when there is still no peer.
func queue_expand_empty_target(q *QueueEntry) string {
	peers := entity_peers_for(q.FromEntity, q.ToEntity)
	if len(peers) == 0 {
		return ""
	}
	for i := 1; i < len(peers); i++ {
		queue_add_direct(uid(), peers[i], q.FromEntity, q.ToEntity, q.Service, q.Event, q.FromApp,
			strings.Split(q.FromServices, ","), q.Content, q.Data, q.File, q.Expires)
	}
	return peers[0]
}

// queue_retarget re-resolves a row whose pinned peer is no longer among the
// routes entity_peers_for returns, and reports whether it did. Narrow on
// purpose - "this address is gone", not "not answering", which is
// queue_resurrect_peer's job. Attempts reset because the budget was spent on a
// different destination.
func queue_retarget(db *DB, q *QueueEntry) bool {
	if q.Type != "direct" || q.Target == "" {
		return false
	}
	peers := entity_peers_for(q.FromEntity, q.ToEntity)
	if len(peers) == 0 {
		return false
	}
	if slices.Contains(peers, q.Target) {
		return false
	}
	for i := 1; i < len(peers); i++ {
		queue_add_direct(uid(), peers[i], q.FromEntity, q.ToEntity, q.Service, q.Event, q.FromApp,
			strings.Split(q.FromServices, ","), q.Content, q.Data, q.File, q.Expires)
	}
	db.exec_bg("queue retarget", "update queue set target = ?, status = 'pending', attempts = 0, next_retry = ? where id = ?",
		peers[0], now(), q.ID)
	info("Queue retargeting %q for %q: peer %q is no longer a route, %q is", q.ID, q.ToEntity, q.Target, peers[0])
	return true
}

// queue_retarget_parked re-resolves parked rows, which never reach queue_fail
// and so are never retargeted by it. Grouped per destination, not per row: this
// queue has held rows by the million and must not be loaded wholesale to decide
// nothing.
func queue_retarget_parked() {
	db := db_open("db/queue.db")
	destinations, err := db.rows("select distinct from_entity, to_entity, target from queue where status = 'parked' and type = 'direct' and target != ''")
	if err != nil {
		warn("Database error loading parked queue destinations: %v", err)
		return
	}
	moved := 0
	for _, d := range destinations {
		from, _ := d["from_entity"].(string)
		to, _ := d["to_entity"].(string)
		target, _ := d["target"].(string)
		peers := entity_peers_for(from, to)
		if len(peers) == 0 || slices.Contains(peers, target) {
			continue
		}
		moved++
		if len(peers) == 1 {
			// One route, so there are no siblings to clone and queue_retarget's
			// fan-out loop would do nothing: every row for this destination
			// moves in a single statement, with no row read at all.
			db.exec_bg("queue retarget parked", "update queue set target = ?, status = 'pending', attempts = 0, next_retry = ? where status = 'parked' and type = 'direct' and from_entity = ? and to_entity = ? and target = ?",
				peers[0], now(), from, to, target)
			info("Queue retargeting parked rows for %q: peer %q is no longer a route, %q is", to, target, peers[0])
			continue
		}
		// Several routes: each row needs a sibling per remaining peer, so this
		// destination's rows are read and passed through queue_retarget - the
		// same path queue_fail uses, so the fan-out cannot diverge between them.
		var parked []QueueEntry
		if err := db.scans(&parked, "select * from queue where status = 'parked' and type = 'direct' and from_entity = ? and to_entity = ? and target = ?", from, to, target); err != nil {
			warn("Database error loading parked queue entries for %q: %v", to, err)
			continue
		}
		for i := range parked {
			queue_retarget(db, &parked[i])
		}
	}
	if moved > 0 {
		queue_wake()
	}
}

// Send a queued direct message (reads challenge before sending, waits for ACK).
// An empty target is expanded to the recipient's live peers first.
func queue_send_direct(q *QueueEntry) bool {
	peer := q.Target
	if peer == "" {
		peer = queue_expand_empty_target(q)
	}
	if peer == "" {
		return false
	}

	// Self-loop fast path: the wire envelope is wasted when the receiver is this
	// process. Every queue_add_* call site validates from_entity against the
	// writing user, so the row itself is the proof. File sends still need the slow
	// path.
	if peer == net_id {
		return queue_send_self_loop_fast(q)
	}

	// /mochi/2/messages path: the Sender owns claim, framing, ack matching and
	// resolving the row. Return false either way - on success the async resolver
	// owns it; on a stream-open failure it rolls back to pending for a later tick.
	f, err := frame_for_queue(q)
	if err != nil {
		queue_drop(q.ID, fmt.Sprintf("frame build failed: %v", err))
		return false
	}
	// Mark in-flight BEFORE handing off, so queue_process's post-call
	// status check sees 'sending' and doesn't queue_fail an in-flight row.
	queue_sending(q.ID)
	if send_error := peer_send(peer, q.ID, f); send_error != nil {
		// peer_send failed before queueing. Roll back 'sending' so
		// queue_process re-pends the row for a later retry.
		queue_unsending(q.ID)
	}
	return false
}

// queue_send_self_loop_fast bypasses the wire envelope when delivering to
// ourselves, via the per-(user, app) worker pool so self-loop frames serialise
// with remote ones. True means enqueued, not handled; queue_reply resolves the
// row.
func queue_send_self_loop_fast(q *QueueEntry) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			warn("Queue self-loop fast path: dispatch panic for %q: %v\n%s",
				q.ID, r, rd.Stack())
			ok = false
		}
	}()

	var content map[string]any
	if len(q.Content) > 0 {
		if err := cbor.Unmarshal(q.Content, &content); err != nil {
			info("Queue self-loop fast path: content decode failed for %q: %v", q.ID, err)
			return false
		}
	} else {
		content = map[string]any{}
	}

	var services []string
	if q.FromServices != "" {
		services = strings.Split(q.FromServices, ",")
	}

	// Resolve user from To (or accept "" if no To — anonymous self-loop
	// is a corner case the worker key copes with).
	to := q.ToEntity
	if to != "" && valid(to, "fingerprint") {
		if ent := entity_by_any(to); ent != nil {
			to = ent.ID
		}
	}
	user := ""
	if to != "" {
		if u := user_owning_entity(to); u != nil {
			user = u.UID
		}
	}

	f := &Frame{
		Type:     frame_type_message,
		ID:       q.ID,
		From:     q.FromEntity,
		To:       to,
		Service:  q.Service,
		Event:    q.Event,
		FromApp:  q.FromApp,
		Services: services,
		Priority: frame_priority_for(q.Priority),
		Content:  content,
		Data:     q.Data,
	}

	// Mark sending so queue_process knows the resolver owns this row.
	// Return false (NOT true) so queue_process doesn't delete the row
	// — the worker's queue_reply will queue_ack on success or
	// queue_fail/drop on failure.
	queue_sending(q.ID)

	worker_dispatch(user, q.Service, &worker_frame{
		frame: f,
		peer:  net_id, // self-loop: originating peer is us
		reply: queue_reply{id: q.ID},
	})
	return false
}

// Send a queued broadcast message (no challenge for broadcasts)
func queue_send_broadcast(q *QueueEntry) bool {
	if !peers_sufficient() {
		return false
	}

	pubsub_publish(q.FromEntity, q.Service, q.Event, q.ID, q.Content)
	return true
}

// queue_select pulls the next batch of due rows: one row per distinct target
// peer (queue_pick_direct_limit), plus broadcasts and empty-target rows by
// priority (queue_pick_other_limit). Pick-by-peer stops one peer's backlog
// filling the budget and starving every other peer.
const (
	queue_pick_direct_limit = 50
	queue_pick_other_limit  = 20
)

func queue_select(db *DB) []QueueEntry {
	ts := now()

	// Direct rows: one row per distinct target peer.
	var direct []QueueEntry
	err := db.scans(&direct, `
		with ranked as (
			select id, type, target, from_entity, to_entity, service, event,
				from_app, from_services, content, data, file, expires,
				status, attempts, next_retry, last_error, created, priority,
				row_number() over (partition by target order by priority desc, next_retry asc) as rn
			from queue
			where status = 'pending' and next_retry <= ?
				and type = 'direct' and target != ''
		)
		select id, type, target, from_entity, to_entity, service, event,
			from_app, from_services, content, data, file, expires,
			status, attempts, next_retry, last_error, created, priority
		from ranked
		where rn = 1
		order by priority desc, next_retry asc
		limit ?`, ts, queue_pick_direct_limit)
	if err != nil {
		info("Queue select (direct pick-by-peer) error: %v", err)
	}

	// Broadcasts (target='pubsub') and empty-target rows.
	var other []QueueEntry
	if err := db.scans(&other, `select id, type, target, from_entity, to_entity, service, event,
			from_app, from_services, content, data, file, expires,
			status, attempts, next_retry, last_error, created, priority
		from queue
		where status = 'pending' and next_retry <= ?
			and (type != 'direct' or target = '')
		order by priority desc, next_retry asc
		limit ?`, ts, queue_pick_other_limit); err != nil {
		info("Queue select (broadcast/empty-target) error: %v", err)
	}

	if len(other) == 0 {
		return direct
	}
	return append(direct, other...)
}

// Process pending queue entries. Returns the count of rows acted on
// (dispatched, silent-deferred, or pre-filtered to deletion) so the
// caller's drain loop can decide whether to immediately re-enter or
// sleep on the heartbeat tick.
func queue_process() int {
	db := db_open("db/queue.db")

	entries := queue_select(db)

	udb := db_open("db/users.db")
	processed := 0

	// Pre-filter: drop expired and from-deleted-entity rows serially.
	// Cheap, no network. The remaining `valid` slice goes through the
	// parallel send path below.
	valid := entries[:0]
	for _, q := range entries {
		if q.Expires > 0 && q.Expires < now() {
			debug("Queue message %q expired", q.ID)
			db.exec_bg("queue gc expired delete", "delete from queue where id = ?", q.ID)
			processed++
			continue
		}
		if q.FromEntity != "" {
			if exists, _ := udb.exists("select 1 from entities where id=?", q.FromEntity); !exists {
				info("Queue dropping message %q from deleted entity %q", q.ID, q.FromEntity)
				db.exec_bg("queue gc deleted-entity delete", "delete from queue where id = ?", q.ID)
				processed++
				continue
			}
		}
		// Silent-peer pre-filter: defer rows for a peer known unreachable so they do
		// not waste bucket slots; peer_connect resurrects them eagerly. Broadcasts
		// have no specific target, so this applies to direct rows only.
		if q.Type != "broadcast" && q.Target != "" && peer_is_silent(q.Target) {
			queue_defer_target(q.Target, now()+queue_silent_defer)
			processed++
			continue
		}
		// Stalled-peer pre-filter: the target opens a stream but never
		// acks (peer_progress.go).
		// Park its whole backlog until the trial window reopens, so the
		// manager stops re-scanning an undeliverable pile every tick.
		if q.Type != "broadcast" && q.Target != "" && peer_is_stalled(q.Target) {
			until := peer_stall_until(q.Target)
			if until <= now() {
				until = now() + peer_stall_window
			}
			queue_defer_target(q.Target, until)
			processed++
			continue
		}
		// Sender pre-filter: pull_loop owns direct rows for a peer with an active
		// Sender. Competing for the same outbox blocks peer_send for
		// sender_send_timeout and drags out the tick. File pushes ride their own
		// stream.
		if q.Type == "direct" && q.Event != "file/push" && q.Target != "" && senders_has(q.Target) {
			// Don't increment processed — the row isn't drained or
			// deferred, just routed to a different mechanism.
			continue
		}
		// Self-loop pre-filter: self_loop_drain owns rows targeting net_id. File
		// pushes to self are a no-op nobody emits; queue_process handles one if it
		// appears.
		if q.Type == "direct" && q.Event != "file/push" && q.Target != "" && q.Target == net_id {
			continue
		}
		valid = append(valid, q)
	}

	if len(valid) == 0 {
		return processed
	}

	// Per-peer semaphore: at most N in-flight sends per target peer, allocated
	// lazily and GC'd when this function returns. Broadcasts share one bucket;
	// file pushes use concurrency 1, since parallel pushes only divide bandwidth.
	semaphores := map[string]chan struct{}{}
	var semaphore_lock sync.Mutex
	get_semaphore := func(peer string, cap int) chan struct{} {
		semaphore_lock.Lock()
		defer semaphore_lock.Unlock()
		s, ok := semaphores[peer]
		if !ok {
			s = make(chan struct{}, cap)
			semaphores[peer] = s
		}
		return s
	}

	var wg sync.WaitGroup
	for _, q := range valid {
		wg.Add(1)
		// Bucketing key + concurrency cap per send type.
		var bucket string
		cap := queue_per_peer_concurrency
		switch {
		case q.Type == "broadcast":
			bucket = "\x00broadcast\x00"
		case q.Event == "file/push":
			bucket = "\x00file\x00" + q.Target
			cap = queue_per_peer_file_concurrency
		default:
			// Serialise per (target peer, from-entity) so one user's ops apply in order
			// on the receiver: otherwise a child row can land before its parent and fail
			// the foreign key. Different users on the same peer still parallelise.
			bucket = "\x00direct\x00" + q.Target + "\x00" + q.FromEntity
			cap = 1
		}
		semaphore := get_semaphore(bucket, cap)
		semaphore <- struct{}{}
		go func(q QueueEntry, semaphore chan struct{}) {
			defer wg.Done()
			defer func() { <-semaphore }()

			var ok bool
			switch {
			case q.Type == "broadcast":
				ok = queue_send_broadcast(&q)
			default:
				ok = queue_send_direct(&q)
			}

			if ok {
				db.exec_bg("queue process sent delete", "delete from queue where id = ?", q.ID)
			} else if !queue_is_inflight(q.ID) {
				// /mochi/2 paths set status='sending' and return
				// false; the async resolver (sender_read /
				// queue_reply) will queue_ack / queue_fail when the
				// receiver replies. Don't touch in-flight rows here.
				queue_fail(q.ID, "send failed")
			}
		}(q, semaphore)
	}
	wg.Wait()
	return processed + len(valid)
}

// Check for sent messages that haven't received ACK (timeout)
func queue_check_ack_timeout() {
	db := db_open("db/queue.db")
	// Rows still claimed 60 seconds on: a sender that died mid-flight. Keyed on
	// `claimed`, not `created` - created is the enqueue time and is never
	// rewritten, so any retried row would be swept the instant its sender claimed
	// it.
	stuck := now() - queue_claim_timeout
	db.exec_bg("queue stuck-sending requeue", "update queue set status = 'pending', next_retry = ? where status = 'sending' and claimed < ?",
		queue_next_retry(0), stuck)
}

// queue_check_entity is called when an entity's location is discovered. Nudge
// the queue manager rather than scanning here: concurrent discovery events each
// cloning every row's content blob pinned gigabytes.
func queue_check_entity(entity string) {
	queue_wake()
}

// queue_check_peer is called when a peer is discovered. Same design
// as queue_check_entity — nudge the queue manager, don't fan out.
func queue_check_peer(peer string) {
	queue_wake()
}

// Clean up old entries
func queue_cleanup() {
	db := db_open("db/queue.db")
	aged := "created < ?"
	cutoff := now() - queue_age_maximum

	// Log and delete expired messages
	var old []QueueEntry
	err := db.scans(&old, "select * from queue where "+aged, cutoff)
	if err != nil {
		warn("Database error loading expired queue entries: %v", err)
		return
	}
	db.exec_bg("queue cleanup", "delete from queue where "+aged, cutoff)

	// Surface each aged-out send as message/timeout to its sending app,
	// deduped per sweep by (from_entity, from_app, to_entity): fan-out makes
	// one row per (recipient, host), so a gone recipient yields many rows.
	seen := map[string]bool{}
	for i := range old {
		q := &old[i]
		key := q.FromEntity + "|" + q.FromApp + "|" + q.ToEntity
		if seen[key] {
			continue
		}
		seen[key] = true
		queue_error_dispatch(q, error_code_message_timeout, "timeout")
	}

	// A row that burned its whole retention undelivered is the ONLY failure signal
	// for recipients whose attempts never climb (a ghost peer with no addresses
	// short-circuits before queue_fail), so feed health here too.
	reaped := map[string]int64{}
	for i := range old {
		q := &old[i]
		if q.Created > reaped[q.ToEntity] {
			reaped[q.ToEntity] = q.Created
		}
	}
	for recipient, created := range reaped {
		health_failure(recipient, created)
	}

	// Health residue: delete a suspended row only once its app has been told to
	// drop the subscriber. On age alone the row reads healthy again while the
	// subscriber row is still in place, and the next post burns a fresh retry
	// ladder.
	db.exec_bg("health cleanup", "delete from health where suspended != 0 and suspended < ? and evicted != 0", now()-2*queue_evict_age)
}

// queue_drain waits, up to timeout, for the rows actually in flight - status
// 'sending'. Not 'pending': on a busy server new rows arrive continuously, so
// waiting for an empty pending set would always burn the whole timeout.
func queue_drain(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	db := db_open("db/queue.db")

	for time.Now().Before(deadline) {
		count := db.integer("select count(*) from queue where status = 'sending'")
		if count == 0 {
			info("Queue drained")
			return
		}
		info("Waiting for %d message(s) still sending...", count)
		time.Sleep(time.Second)
	}

	// The in-flight count, not every queued row: a queue holding a week of
	// undeliverable rows would otherwise report thousands and make a clean
	// shutdown read as a failed drain.
	remaining := db.integer("select count(*) from queue where status = 'sending'")
	if remaining > 0 {
		info("Queue drain timeout, %d message(s) still sending", remaining)
	}
}

// self_loop_drain owns queue.db's self-loop slice (direct rows targeting
// net_id), symmetric with Sender.pull_loop. Dedicated goroutine because
// queue_process waits for its whole batch at end of tick and one offline-peer
// dial would stall it. Batch mirrors queue_select's 50, so worker back-pressure
// shows next iteration.
const self_loop_batch = 50

func self_loop_drain() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		rows := queue_claim_for_self(self_loop_batch)
		for i := range rows {
			// Worker dispatch is blocking; if the worker inbox is full
			// we wait here. That's the backpressure path — visible as
			// queue depth growing rather than as an invisible stall.
			queue_send_self_loop_fast(&rows[i])
		}
		if len(rows) >= self_loop_batch {
			// Saturated batch — likely more rows are due. Don't sleep,
			// loop immediately (matches queue_manager's drain shape).
			continue
		}
		select {
		case <-tick.C:
		case <-self_loop_wake_ch:
		}
	}
}

// Queue manager goroutine. One processing loop owns every outbound send, so
// fan-out to a peer is serialised. While queue_process keeps finding rows the
// loop re-enters with no wait; the tick is only a heartbeat for the idle case.
func queue_manager() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	go func() {
		for {
			n := queue_process()
			queue_check_ack_timeout()
			if n > 0 {
				// Acted on at least one row. Loop straight back in
				// to pick up the next batch — no tick-interval cap.
				continue
			}
			// Nothing ready right now. Wait for the tick (heartbeat)
			// or a wake event (new enqueue / peer reconnect / etc.).
			select {
			case <-tick.C:
			case <-queue_wake_ch:
			}
		}
	}()

	// Cleanup runs less frequently
	for range time.Tick(time.Hour) {
		queue_cleanup()
		queue_retarget_parked()
		message_seen_cleanup()
	}
}
