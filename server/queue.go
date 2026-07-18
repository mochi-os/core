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
	"strings"
	"sync"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
)

// queue_per_peer_concurrency caps the number of in-flight sends to a
// single target peer per tick. Each in-flight send opens its own
// libp2p stream and waits for its ACK; allowing multiple sends per
// peer in parallel lets one tick drain ~Nx faster than the strict
// serial pattern. Receivers handle multiple concurrent streams from
// the same peer fine (libp2p multiplexes; ACK dedup keys are
// per-message ID), and SQL ops apply by sequence number on the
// receiver — out-of-order arrival on the wire is rebuilt at apply.
//
// 8 is conservative: enough to overcome per-message ACK latency
// (localhost ~50ms × 8 ≈ 6.25ms/op effective), well under any
// per-peer rate limits in tests.
const queue_per_peer_concurrency = 8

// File pushes stay serial per peer — one in-flight file at a time per
// peer. Parallel file pushes only divide the same bandwidth.
const queue_per_peer_file_concurrency = 1

// Message priority tiers, stored in queue.priority and used by
// queue_process to order delivery — higher is more urgent. Spaced by 10
// so a tier can be inserted between two existing ones (or below bulk)
// without renumbering, since the values are purely ordinal.
const (
	priority_control     = 40 // replication coordination: link/*, membership, keys/transfer
	priority_replay      = 30 // broadcast resync replies: jump live broadcast queue
	priority_interactive = 20 // normal app and entity messages (the default)
	priority_bulk        = 10 // replication data: sql/op, system/set, system/row
)

// queue_silent_defer is how long to push a row's next_retry forward
// when the target peer is in the silent-failure cache. Recovery is via
// queue_resurrect_peer when the peer reconnects. With pick-by-peer +
// durable silent-cache, silenced peers don't recycle through the
// picker anyway — the defer is belt-and-suspenders so a row that
// slipped through (e.g. silenced after the picker but before the
// goroutine fired) doesn't immediately re-appear at the front of the
// next tick.
const queue_silent_defer = 3600 // 1 hour

// queue_priority classifies an outbound message into a priority tier
// from its service and event. Replication coordination jumps ahead of
// everything so an approval is never stuck behind a sync; replication's
// bulk data sits below normal app traffic so a large sync cannot delay
// interactive messages. Everything else is interactive.
func queue_priority(service, event string) int {
	if service == "replication" {
		switch event {
		case "sql/op", "system/set", "system/row":
			return priority_bulk
		case "link/request", "link/approved", "link/denied",
			"join/request", "join/approved", "join/denied",
			"membership/join", "membership/assert", "membership/leave",
			"membership/evict", "pair/membership/change",
			"keys/transfer", "bootstrap/scope/done":
			return priority_control
		}
	}
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
}

const (
	queue_max_age = 7 * 86400 // 7 days

	// replication_op_retention is the retention floor for replication ops
	// specifically (service = "replication"): a peer offline up to this
	// long can still replay its missed ops from queue.db and converge
	// losslessly — the T_forget "host-gone" budget. Other message classes
	// use queue_max_age. Invariant (asserted in retention_test.go):
	// replication_op_retention >= queue_max_age.
	replication_op_retention = 30 * 86400 // 30 days (T_forget)
)

// queue_wake_ch is a buffered channel used by send_peer to nudge the
// queue manager into processing the queue immediately rather than
// waiting for the next tick. Buffer-of-1 means multiple wakes between
// ticks coalesce into a single processing pass — no work for the
// manager to do beyond what queue_process already handles.
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

// queue_claim_for_peer atomically pulls up to `limit` rows targeting
// `peer` from queue.db, marking them status='sending' in the same
// statement so queue_process won't double-pick them. Used by
// /mochi/2/messages Senders' pull_loop. Returns claimed rows in
// (priority desc, next_retry asc) order — same order as the global
// queue_select.
//
// File pushes are excluded — they use /mochi/2/stream with a fresh
// libp2p stream per file, not the Sender's persistent stream, so the
// queue_send_file_push code path in queue_process handles them.
// Broadcasts are implicitly excluded by the type='direct' filter.
func queue_claim_for_peer(peer string, limit int) []QueueEntry {
	if peer == "" || limit <= 0 {
		return nil
	}
	db := db_open("db/queue.db")
	var rows []QueueEntry
	err := db.scans(&rows, `update queue set status='sending'
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
		peer, now(), limit)
	if err != nil {
		info("queue_claim_for_peer error peer=%q: %v", peer, err)
		return nil
	}
	return rows
}

// queue_claim_for_self atomically claims up to `limit` direct rows
// whose target is net_id, marking them status='sending' in the same
// statement so queue_process won't double-pick them. Used by
// self_loop_drain (the symmetric counterpart to Sender.pull_loop).
// Same SQL shape as queue_claim_for_peer; the queue_target_priority_retry
// index handles both equally well.
//
// File pushes are excluded — file/push to self is a no-op nobody
// emits; if one ever appears, queue_process picks it up.
func queue_claim_for_self(limit int) []QueueEntry {
	if net_id == "" || limit <= 0 {
		return nil
	}
	db := db_open("db/queue.db")
	var rows []QueueEntry
	err := db.scans(&rows, `update queue set status='sending'
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
		net_id, now(), limit)
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

// queue_warn_rows / queue_warn_age / queue_warn_attempts are the
// pending-backlog thresholds past which queue_watchdog warns for a
// (target, service) bucket. The News feed self-loop wedge (2026-07-06
// to 2026-07-15) accumulated 1.4M undeliverable rows over a week with
// the WAL watchdog firing as the only, indirect signal; any one of
// these thresholds surfaces that class within hours of onset. The age
// threshold sits well below queue_max_age (7d): warning only as the
// reaper starts deleting the rows is too late to act on (the first
// live age warns fired at exactly 7.0 days, for buckets already being
// reaped). var (not const) so tests can lower them.
var queue_warn_rows int64 = 10000
var queue_warn_age int64 = 2 * 86400
var queue_warn_attempts int64 = 100

// queue_warn_repeat is the re-warn cadence: a bucket warns on the tick
// it first trips a threshold, then once per repeat window while the
// condition persists, instead of every tick.
var queue_warn_repeat int64 = 86400

var queue_warned sync.Map // target+"|"+service -> last warn unix

// queue_park_attempts is the retry budget before queue_fail parks a row
// (status='parked', outside every claim path) instead of rescheduling
// it. With the backoff ladder capped at an hour, 50 attempts is roughly
// two days of failures — long past transient, days before the age
// budget deletes the data. var (not const) so tests can lower it.
var queue_park_attempts = 50

var queue_park_warned sync.Map // target+"|"+service -> last park warn unix

// Per-recipient delivery health. The queue's rows each re-learn a dead
// recipient from scratch — fifty dials per event, forever, until the
// directory forgets the recipient's host (30 days, and a re-announcing
// ghost resets that clock). The health table remembers per RECIPIENT:
// exhausting a full retry budget with no contradicting success suspends
// them, suspension stops broadcast fan-out enqueueing anything beyond a
// periodic probe, and after queue_evict_age the owning app is told to
// drop the subscriber. Scope: the gate applies ONLY to broadcast-class
// fan-out (api_broadcast_send), where the resync/floor catch-up makes
// skipped events recoverable — direct correspondence (chat invites,
// interactive requests) always queues normally.
//
// queue_denial_limit is the fast path: an authoritative unknown_user
// answer means the host is alive and says the recipient does not exist
// there — three of those beat fifty timeouts. queue_probe_interval
// exceeds the ~2 days a probe row takes to burn its ladder, so probes
// never overlap. var (not const) so tests can lower them.
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
	db := db_open("db/queue.db")
	// Point read before the write: the healthy common case has no
	// health row, and this runs on delivery hot paths (queue_ack, the
	// self-loop dispatch) where an unconditional write transaction
	// per message would be a real cost. The read-to-write race is
	// harmless — a row inserted in between records a fresh failure
	// the next ack clears.
	if ok, _ := db.exists("select 1 from health where recipient=?", recipient); !ok {
		return
	}
	db.exec_bg("health success", "update health set failures=0, denials=0, success=?, suspended=0 where recipient=?", now(), recipient)
}

// health_failure records a row that exhausted its whole retry budget
// (parked) against the recipient. Suspends when the ladder ran with no
// contradicting success: the row burned every backoff step since
// `created` and nothing from this recipient landed in that window. A
// success mid-window (success >= created) blocks suspension — mixed
// outcomes are a per-message problem, not a dead recipient.
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

// health_gate is consulted by broadcast fan-out per subscriber. Healthy
// recipients (no row, or not suspended) pass. Suspended recipients are
// skipped — their streams catch up via resync when they return — except
// one probe row per queue_probe_interval, which passes through as a
// normal send: its ack unsuspends, its park re-confirms. Past
// queue_evict_age the caller should stop probing and tell the owning
// app to drop the subscriber instead.
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

var health_evict_warned sync.Map // app.id+"|"+recipient -> last dispatch unix

// health_evict_dispatch tells the owning app — once per day per (app,
// recipient) — that a subscriber has been unreachable past
// queue_evict_age, so it can drop the subscriber row. Fired lazily from
// the fan-out gate: exactly where the cost recurs and where app context
// exists, so no scheduler is involved. Apps without a handler no-op.
func health_evict_dispatch(user *User, app *App, service, recipient string) {
	key := app.id + "|" + recipient
	moment := now()
	if v, ok := health_evict_warned.Load(key); ok && moment-v.(int64) < 86400 {
		return
	}
	health_evict_warned.Store(key, moment)
	db := db_open("db/queue.db")
	var h struct {
		Since     int64 `db:"since"`
		Suspended int64 `db:"suspended"`
	}
	_ = db.scan(&h, "select since, suspended from health where recipient=?", recipient)
	target := recipient
	subscriber_dispatch(user, app, error_code_subscriber_unreachable, "unreachable", service, target, nil, func() map[string]any {
		return map[string]any{"subscriber": target, "since": h.Since, "suspended": h.Suspended}
	})
}

// subscriber_dispatch is error_dispatch behind a var so tests can
// capture eviction dispatches without standing up an app registry.
var subscriber_dispatch = error_dispatch

// queue_watchdog runs every db_manager tick. It groups pending queue
// rows by (target, service) and warns when a bucket's row count,
// oldest-row age, or attempt count says deliveries to that destination
// are not draining. Retries make a transient outage invisible, which
// also makes a permanent failure invisible — each individual retry is
// routine, so undeliverable rows accumulate with no signal until
// something downstream (disk, WAL churn) breaks. This is the direct
// signal.
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
	err := db.scans(&buckets, "select target, service, count(*) as total, min(created) as oldest, max(attempts) as attempts from queue where status in ('pending', 'parked') group by target, service")
	if err != nil {
		return
	}
	now := now()
	unhealthy := map[string]bool{}
	for _, bucket := range buckets {
		key := bucket.Target + "|" + bucket.Service
		age := now - bucket.Oldest
		if bucket.Total < queue_warn_rows && age < queue_warn_age && bucket.Attempts < queue_warn_attempts {
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
}

// Add a direct message to the queue. Caller can override the default
// (service+event)-derived priority by calling queue_add_direct_priority
// instead — used by broadcast_resync to ship replies in the priority_replay
// lane so they overtake the live-broadcast backlog (task #96).
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

// Mark a message as acknowledged (remove from queue). A successful
// delivery also confirms the target peer in the sender's learned
// directory (directory_user_confirm is throttled and cheap; the batch
// ack-flush path skips this — partial confirm coverage is fine, the
// learned rows never age out).
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

// queue_ack_ch buffers IDs successfully handled by the worker pool or
// resolved by /mochi/2 Sender read loops. queue_ack_batcher drains it
// and collapses the deletes into one DELETE ... WHERE id IN (...) per
// batch. Capacity is generous so a brief acks-burst from the worker
// pool doesn't fall through to the synchronous fallback.
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
// Non-blocking: if the channel is full (very high sustained ack rate),
// falls back to the synchronous queue_ack so progress is never lost.
// Used by queue_reply.ack() in the worker pool and by sender_read's
// ack-frame handler — the two hot-path ack sources.
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

// queue_ack_batcher drains queue_ack_ch, batching IDs into a single
// DELETE per flush. Saves a SQLite transaction (and the writer-mutex
// contention behind it) per ack vs the per-row queue_ack path.
//
// Flush triggers: batch fills (queue_ack_batch=256), or
// queue_ack_interval (20ms) elapses with a non-empty batch.
//
// Crash-loss window: an ID sitting in the buffer when the process
// dies will replay on next startup (the row stays 'sending' in
// queue.db until the timeout, then queue_check_ack_timeout re-pends
// it). message_seen dedup catches the replay on the receiver. The
// 20ms ceiling keeps the window small.
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
	db.exec_bg("health success flush", "update health set failures=0, denials=0, success=?, suspended=0 where recipient in (select to_entity from queue where id in ("+string(placeholders)+"))", append([]any{now()}, args...)...)
	db.exec_bg("queue ack flush", "delete from queue where id in ("+string(placeholders)+")", args...)
}

// queue_drain_entity waits up to `wait` for every queued message from
// `entity` to leave the queue (sent and resolved, or dropped). Used by
// account teardown: farewell messages (membership departs, user/purge)
// are signed with the user's identity key, which the caller is about to
// delete — once the key is gone, unsent rows can no longer be claimed
// and are silently dropped. Draining first lets the normal send complete;
// on timeout (peer offline) teardown proceeds and the farewell is lost,
// which receivers self-heal from (their own closure tick re-derives an
// account-gone purge; stream traffic at a departed host fails visibly).
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

// queue_drop removes a queue row without scheduling a retry. Use when
// the receiver's NACK carries a Reason hint that further attempts
// would deterministically NACK with the same outcome - e.g.
// "broadcast-gap" means the subscriber is already requesting catch-up
// via its own resync path and re-sending the same in-order live event
// is wasted work that just floods the queue. queue_fail is the
// default for unspecified failures (network blip, peer offline);
// queue_drop is the explicit-give-up path keyed off a known reason.
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
		if code, errReason, ok := error_code_for_nack(reason); ok {
			queue_error_dispatch(&q, code, errReason)
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

// nack_should_drop returns true when a NACK's Reason hint means
// retrying is pointless and the queue row should be dropped instead
// of scheduling another attempt. Falls back to "" -> retry which
// preserves the legacy behaviour for older receivers that don't set
// a reason at all.
func nack_should_drop(reason string) bool {
	switch reason {
	case nack_reason_broadcast_gap, nack_reason_decode_failed:
		return true
	}
	return false
}

// Mark a message as being sent (prevents other processors from picking it up)
func queue_sending(id string) {
	db := db_open("db/queue.db")
	db.exec_bg("queue mark sending", "update queue set status='sending' where id=?", id)
}

// queue_unsending rolls back queue_sending when the async send path
// fails before the row enters its inflight tracking (e.g. peer_send
// returns errSenderUnreachable). Returns the row to 'pending' so the
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

// queue_defer pushes a row's next_retry forward without incrementing
// attempts. Use when a row was deliberately skipped (target peer is
// in the silent-failure cache) - we want it to drop out of the ready
// set for a while, but the row isn't actually "failing" so the
// attempts counter / retry-backoff escalation shouldn't escalate.
func queue_defer(id string, delay int64) {
	db := db_open("db/queue.db")
	db.exec_bg("queue defer", "update queue set next_retry = ? where id = ?", now()+delay, id)
}

// queue_defer_target pushes every pending row for a target forward to
// `until` in one UPDATE. Used to park a silent or stalled peer's entire
// backlog so queue_select stops re-scanning it — deferring row-by-row
// instead walks the whole backlog (one defer per tick), which is the
// O(n^2) spin behind the 2026-06-02 incident. Idempotent: only rows due
// before `until` are moved. Resurrected by queue_resurrect_peer when the
// peer recovers.
func queue_defer_target(target string, until int64) {
	if target == "" {
		return
	}
	db := db_open("db/queue.db")
	db.exec_bg("queue defer target", "update queue set next_retry = ? where target = ? and status = 'pending' and next_retry < ?", until, target, until)
}

// queue_resurrect_peer brings every deferred row for a peer back into
// the ready set. Called from peer_connect's success path so a reviving
// peer's backlog drains immediately instead of waiting out the deferred
// next_retry timer set by queue_process's silent-peer pre-filter. No-op
// if there are no rows in the future for that peer.
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

	if age > queue_max_age {
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
	} else if attempts >= queue_park_attempts {
		// Retry budget exhausted while the row is still inside its age
		// budget: park it instead of grinding hourly retries for the
		// remaining days (1.4M wedged rows at attempts up to 157 were
		// the write churn that starved queue.db's WAL checkpoint,
		// 2026-07-15). Parked rows keep their data — they revive when
		// the target peer reconnects (queue_resurrect_peer) and age out
		// through the queue_cleanup sweep, which is status-blind.
		db.exec_bg("queue fail park", "update queue set status = 'parked', attempts = ?, last_error = ? where id = ?", attempts, err, id)
		// A parked row is a full retry budget burned against this
		// recipient — feed the per-recipient health record.
		health_failure(q.ToEntity, q.Created)
		key := q.Target + "|" + q.Service
		now := now()
		if v, ok := queue_park_warned.Load(key); !ok || now-v.(int64) >= queue_warn_repeat {
			queue_park_warned.Store(key, now)
			if q.Target == "" {
				// No peer to reconnect: the recipient entity never resolved
				// to any host, so these rows only age out.
				warn("Queue parking deliveries for (service=%q) with no resolvable recipient after %d failed attempts (latest: %s); rows keep their data and are reaped after %d days.", q.Service, attempts, err, queue_max_age/86400)
			} else {
				warn("Queue parking deliveries for (target=%q, service=%q) after %d failed attempts (latest: %s); rows keep their data, revive if the peer reconnects, and are reaped after %d days.", q.Target, q.Service, attempts, err, queue_max_age/86400)
			}
		}
	} else {
		// Schedule retry
		next := queue_next_retry(attempts)
		db.exec_bg("queue fail retry reschedule", "update queue set status = 'pending', attempts = ?, next_retry = ?, last_error = ? where id = ?", attempts, next, err, id)
		//debug("Queue message %q scheduled for retry %d at %d: %s", id, attempts, next, err)
	}
}

// queue_expand_empty_target is the retry-time fan-out: if a row has
// an empty target (entity_peers returned nothing at enqueue) and
// entity_peers now finds N live locations, clone (N-1) sibling rows
// targeting the additional peers and return the first peer for this
// attempt. Returns the empty string if entity_peers is still empty
// (caller should fail the row for retry later).
//
// Split out from queue_send_direct so the expansion logic is unit-
// testable without dragging in libp2p.
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

// Send a queued direct message (reads challenge before sending, waits for ACK)
//
// Multi-host fan-out: if the row was enqueued with an empty target
// (entity_peers returned nothing at the time, so send_work couldn't
// fan out) and entity_peers now finds multiple live locations, expand
// this row to N peers by inserting (N-1) sibling rows with fresh IDs.
// Send the primary copy on this attempt to whichever peer comes first.
func queue_send_direct(q *QueueEntry) bool {
	peer := q.Target
	if peer == "" {
		peer = queue_expand_empty_target(q)
	}
	if peer == "" {
		return false
	}

	// Self-loop fast path. The wire envelope (CBOR encode + sign + pipe
	// transit + verify + decode + ACK round-trip) costs ~1-5ms per row,
	// and every byte of it is wasted ceremony when the receiver is this
	// process. The queue table is a trusted store: every queue_add_*
	// call site validates the row's from_entity against the writing
	// user (messages.go api_message_send line 365 is the canonical
	// check; internal callers use server-controlled values). So we don't
	// need to re-prove identity to ourselves via the signature - the
	// presence of the row in queue.db IS the proof. File sends still
	// need the slow path to push bytes through the stream API.
	if peer == net_id {
		return queue_send_self_loop_fast(q)
	}

	// /mochi/2/messages path: build a Frame and hand to peer_send. The
	// Sender handles claim, codec, framing, ack matching, and updates
	// the queue row itself (queue_ack / queue_fail) via the inflight
	// resolver. Return false either way: on success the async resolver
	// owns the row (status 'sending'); on a stream-open failure we roll
	// back to 'pending' and queue_process retries on a later tick.
	f, err := frame_for_queue(q)
	if err != nil {
		queue_drop(q.ID, fmt.Sprintf("frame build failed: %v", err))
		return false
	}
	// Mark in-flight BEFORE handing off, so queue_process's post-call
	// status check sees 'sending' and doesn't queue_fail an in-flight row.
	queue_sending(q.ID)
	if send_err := peer_send(peer, q.ID, f); send_err != nil {
		// peer_send failed before queueing. Roll back 'sending' so
		// queue_process re-pends the row for a later retry.
		queue_unsending(q.ID)
	}
	return false
}

// queue_send_self_loop_fast bypasses the wire envelope when delivering
// to ourselves. Routes through the per-(user, app) worker pool (same
// path remote /mochi/2/messages frames take) so self-loop frames
// serialise with remote frames for the same handler — preserves the
// "handler invocations for the same (user, app) never overlap"
// guarantee across both sources.
//
// Differences from the pre-/mochi/2 version (which ran e.route()
// inline):
//   - Temporal: the call returns after enqueueing, not after the
//     handler runs. The queue row is resolved later by queue_reply
//     when the worker finishes (queue_ack / queue_fail / queue_drop).
//   - Serial guarantee: self-loop now serialises with remote sends
//     for the same (user, app).
//   - Panic isolation: now lives in the worker's handle() rather than
//     here. The defer recover guards only the dispatch path (resolve
//     user from To, decode Content) — the handler proper runs on the
//     worker goroutine which has its own recover.
//
// Returns true on successful enqueue (the worker will resolve the
// queue row), false only if the row can't be enqueued at all (decode
// fails). queue_process's caller treats false the same way as a
// failed remote send: queue_fail with standard backoff.
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

	pubsub_publish(q.FromEntity, q.ToEntity, q.Service, q.Event, q.ID, q.Content, q.Data)
	return true
}

// queue_select pulls the next batch of due rows for queue_process to
// dispatch. Two sub-batches, both filtered to status='pending' AND
// next_retry<=now:
//
//  1. Direct rows with a target peer: ONE row per target peer, picked
//     as the highest-priority earliest-next_retry row for that peer.
//     Up to queue_pick_direct_limit (50) distinct peers per tick.
//
//  2. Broadcasts (target='pubsub') and empty-target rows (target=”):
//     picked normally by priority+next_retry, up to
//     queue_pick_other_limit (20) per tick. Each is independent of
//     any specific peer so the per-peer dedup doesn't apply.
//
// Why pick-by-peer? Without it, queue_select's 50-row budget was
// dominated by whichever peer had the largest backlog — at wasabi
// scale, an offline peer with 150k queued rows fills nearly every
// pick, leaving online peers waiting many ticks for their first slot.
// With pick-by-peer, every peer with due work gets a fair shot at
// every tick; queue_process's tick latency is bounded by the slowest
// single goroutine rather than scaling with backlog imbalance. Once
// a peer has a Sender, pull_loop takes over and queue_process's
// pre-filter skips that peer entirely (senders_has fast path), so
// queue_select's job for that peer is just "bootstrap the Sender on
// the first row".
//
// Why no bulk-floor lane? The old model needed a reserved floor
// because urgent traffic could fill all 50 slots and starve bulk
// (replication) work. With pick-by-peer, every peer gets at most one
// slot per tick regardless of priority — a peer with only bulk rows
// gets its slot just the same as a peer with urgent rows. No
// starvation possible at the picker layer.
//
// SQLite cost: the PARTITION BY target ROW_NUMBER uses the
// queue_target_priority_retry index (target, priority desc, next_retry)
// without sorting — SQLite streams the index and emits the first row
// per partition.
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
		// Silent-peer pre-filter: defer rows whose target is in the
		// in-memory silent-failure cache (peer_is_silent) so they
		// don't waste bucket slots on a peer we know is unreachable.
		// Defer for queue_silent_defer (1h); resurrected eagerly on
		// peer_connect via queue_resurrect_peer. Broadcast type has
		// no specific target (pubsub fan-out), so the check only
		// applies to direct + file/push.
		if q.Type != "broadcast" && q.Target != "" && peer_is_silent(q.Target) {
			queue_defer_target(q.Target, now()+queue_silent_defer)
			processed++
			continue
		}
		// Stalled-peer pre-filter: the target opens a stream but never
		// acks (peer_progress.go) — e.g. a wiped/unbootstrapped replica.
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
		// Sender pull_loop pre-filter: skip direct rows whose target
		// has an active /mochi/2/messages Sender — pull_loop is
		// claiming them atomically and feeding them onto the Sender's
		// outbox directly. queue_process attempting the same row would
		// race for the same outbox slot and block on peer_send for
		// sender_send_timeout when pull_loop has it full, dragging
		// out the whole tick and starving self-loop / offline-peer
		// work in the same batch. Skipping here leaves
		// the row pending; pull_loop's next tick (≤1s) will claim it.
		// File pushes don't ride the Sender pipeline (separate
		// /mochi/2/stream per file), so they stay with queue_process.
		// Broadcasts have no specific target.
		if q.Type == "direct" && q.Event != "file/push" && q.Target != "" && senders_has(q.Target) {
			// Don't increment processed — the row isn't drained or
			// deferred, just routed to a different mechanism.
			continue
		}
		// Self-loop pre-filter: same logic for target == net_id.
		// self_loop_drain claims these rows atomically and dispatches
		// them straight to the per-(user, app) worker via
		// queue_send_self_loop_fast — no need (and no benefit) for
		// queue_process to compete. File pushes to self are a no-op
		// nobody emits; if one ever appears, queue_process handles it.
		if q.Type == "direct" && q.Event != "file/push" && q.Target != "" && q.Target == net_id {
			continue
		}
		valid = append(valid, q)
	}

	if len(valid) == 0 {
		return processed
	}

	// Per-peer semaphore: at most N in-flight sends per target peer.
	// Different peers proceed in parallel. The semaphore is allocated
	// lazily per peer; a single tick's worth of goroutines share these
	// channels. After this function returns, the semaphores are GC'd.
	//
	// Broadcasts share one bucket (no specific target). File pushes
	// use the same per-peer mechanism but with concurrency=1 — one
	// large file at a time per peer (parallel pushes would just
	// divide bandwidth).
	semaphores := map[string]chan struct{}{}
	var semLock sync.Mutex
	get_sem := func(peer string, cap int) chan struct{} {
		semLock.Lock()
		defer semLock.Unlock()
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
			// Serialise per (target peer, from-entity) so SQL ops for
			// the same user to the same peer apply in order on the
			// receiver. Without this, FK-dependent ops can arrive
			// before their parents (e.g. subscribers INSERT landing
			// before the parent feeds row INSERT) and fail with FK
			// violations. Different users on the same peer still
			// parallelise, retaining most of the throughput win.
			bucket = "\x00direct\x00" + q.Target + "\x00" + q.FromEntity
			cap = 1
		}
		sem := get_sem(bucket, cap)
		sem <- struct{}{}
		go func(q QueueEntry, sem chan struct{}) {
			defer wg.Done()
			defer func() { <-sem }()

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
		}(q, sem)
	}
	wg.Wait()
	return processed + len(valid)
}

// Check for sent messages that haven't received ACK (timeout)
func queue_check_ack_timeout() {
	db := db_open("db/queue.db")
	// Messages sent more than 30 seconds ago without ACK
	timeout := now() - 30
	db.exec_bg("queue ack-timeout requeue", "update queue set status = 'pending', next_retry = ? where status = 'sent' and created < ?",
		queue_next_retry(0), timeout)
	// Messages stuck in 'sending' for more than 60 seconds (safety net)
	stuck := now() - 60
	db.exec_bg("queue stuck-sending requeue", "update queue set status = 'pending', next_retry = ? where status = 'sending' and created < ?",
		queue_next_retry(0), stuck)
}

// queue_check_entity is called when an entity's location is discovered.
// Nudges the queue manager — the single processing goroutine will pick
// up any rows targeted at the entity in its next pass.
//
// Earlier versions ran this in a fresh goroutine that re-scanned the
// queue and re-sent rows itself. That meant multiple discovery events
// fired concurrent SELECT * FROM queue scans, each cloning every row's
// content/data blob via sqlx → bytes.Clone (the live capture showed
// 4.7 GB pinned across 8 stacked goroutines after the source emitted a
// 3.7 MB manifest-result for the 21,612-entry apps scope). Funnelling
// through queue_wake removes that fan-out: one goroutine, one scan.
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
	// Per-class retention: replication ops keep replication_op_retention
	// (30d / T_forget) so an offline replica can still replay and merge;
	// every other message class keeps queue_max_age (7d). One sweep, keyed
	// off service so it covers every replication emit path.
	gen_cutoff := now() - queue_max_age
	repl_cutoff := now() - replication_op_retention
	aged := "((service = 'replication' and created < ?) or (service != 'replication' and created < ?))"

	// Log and delete expired messages
	var old []QueueEntry
	err := db.scans(&old, "select * from queue where "+aged, repl_cutoff, gen_cutoff)
	if err != nil {
		warn("Database error loading expired queue entries: %v", err)
		return
	}
	db.exec_bg("queue cleanup", "delete from queue where "+aged, repl_cutoff, gen_cutoff)

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

	// Health residue: a recipient suspended past twice the evict age has
	// had a month of eviction dispatches — every owning app has dropped
	// them, so no fan-out consults the row again. If the host ever
	// returns, inbound contact rebuilds state from scratch anyway.
	db.exec_bg("health cleanup", "delete from health where suspended != 0 and suspended < ?", now()-2*queue_evict_age)
}

// Drain queue before shutdown (wait for pending sends to complete)
func queue_drain(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	db := db_open("db/queue.db")

	for time.Now().Before(deadline) {
		count := db.integer("select count(*) from queue where status = 'sent'")
		if count == 0 {
			info("Queue drained")
			return
		}
		info("Waiting for %d pending messages...", count)
		time.Sleep(time.Second)
	}

	remaining := db.integer("select count(*) from queue")
	if remaining > 0 {
		info("Queue drain timeout, %d messages still pending", remaining)
	}
}

// self_loop_drain owns queue.db's self-loop slice (direct rows with
// target == net_id). Symmetric with Sender.pull_loop, which owns the
// per-peer slice. Wakes on a 1-second tick (heartbeat) or a queue_wake
// nudge; claims a batch via queue_claim_for_self; dispatches each row
// through queue_send_self_loop_fast (which decodes content, resolves
// (user, app), and enqueues onto the worker's inbox). The worker's
// reply target (queue_reply) resolves the row via queue_ack / queue_fail
// after the handler runs.
//
// Why a dedicated goroutine instead of folding into queue_process:
//
//   - queue_process's WaitGroup.Wait at end-of-tick blocks until every
//     dispatched goroutine returns. When the batch includes a slow
//     offline-peer connect timeout (libp2p dial), the tick drags
//     out to sender_send_timeout (~5s), starving everything else in
//     the next batch. A dedicated drain only ever handles self-loop
//     rows — nothing slow can hold it up.
//   - Backpressure visibility: when the worker pool saturates,
//     worker_dispatch blocks self_loop_drain and the queue.db depth
//     for self-loop rows visibly rises (mochictl queue-length /
//     pipelining status). With the queue_process path the same
//     backpressure shows up as opaque goroutine stalls.
//   - Symmetric with pull_loop, so the architecture is "every queue
//     consumer has its own reader".
//
// Batch size mirrors queue_select's 50: large enough to amortise the
// claim cost, small enough that worker_dispatch back-pressure shows up
// promptly on the next iteration.
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

// Queue manager goroutine. Single processing loop owns every outbound
// send so that fan-out to a peer is serialised — multiple send_peer()
// callers don't race each other onto the wire.
//
// Drain shape: while queue_process is finding rows to act on, the loop
// re-enters immediately with no wait, so a 1.7M-row backlog drains at
// the SQL+send speed rather than the tick interval. The tick is just a
// heartbeat safety net for the idle case: if no row enqueue or peer
// reconnect fires queue_wake_ch, the heartbeat still fires every second
// so the manager picks up any rows whose next_retry has come due since
// the last pass. Worst-case latency between send_peer and the wire is
// the wake-pickup roundtrip (sub-millisecond) for new rows; up to one
// second for newly-due retries during a fully-idle period.
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
		message_seen_cleanup()
	}
}
