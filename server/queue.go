// Mochi server: Message queue with reliable delivery
// Copyright Alistair Cunningham 2025-2026

package main

import (
	"math/rand"
	"strings"
	"sync"
	"time"
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
	priority_control     = 30 // replication coordination: link/*, membership, keys/transfer
	priority_interactive = 20 // normal app and entity messages (the default)
	priority_bulk        = 10 // replication data: sql/op, system/set, system/row
)

// queue_bulk_floor is the number of slots queue_process reserves each
// tick for the bulk tier. A sustained flood of higher-priority traffic
// can therefore never starve replication — a permanently-behind replica
// would defeat the point of replicating.
const queue_bulk_floor = 10

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
			"host/membership/change", "pair/membership/change",
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
)

// queue_wake_ch is a buffered channel used by send_peer to nudge the
// queue manager into processing the queue immediately rather than
// waiting for the next tick. Buffer-of-1 means multiple wakes between
// ticks coalesce into a single processing pass — no work for the
// manager to do beyond what queue_process already handles.
var queue_wake_ch = make(chan struct{}, 1)

// queue_wake nudges the queue manager. Non-blocking; if a wake is
// already pending, the additional signal is dropped (the manager will
// pick up new rows when it processes).
func queue_wake() {
	select {
	case queue_wake_ch <- struct{}{}:
	default:
	}
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

// Add a direct message to the queue
func queue_add_direct(id, target, from_entity, to_entity, service, event, from_app string, services []string, content, data []byte, file string, expires int64) {
	db := db_open("db/queue.db")
	from_services := strings.Join(services, ",")
	db.exec(`insert or replace into queue
		(id, type, target, from_entity, to_entity, service, event, from_app, from_services, content, data, file, expires, status, attempts, next_retry, created, priority)
		values (?, 'direct', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)`,
		id, target, from_entity, to_entity, service, event, from_app, from_services, content, data, file, expires, now(), now(), queue_priority(service, event))
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

// Mark a message as acknowledged (remove from queue)
func queue_ack(id string) {
	db := db_open("db/queue.db")
	db.exec("delete from queue where id = ?", id)
	//debug("Queue ACK received for %q", id)
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
	db.exec("delete from queue where id = ?", id)
	debug("Queue dropping message %q on NACK reason %q (no retry)", id, reason)
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
	db.exec("update queue set status='sending' where id=?", id)
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
		db.exec("delete from queue where id = ?", id)
	} else {
		// Schedule retry
		next := queue_next_retry(attempts)
		db.exec("update queue set status = 'pending', attempts = ?, next_retry = ?, last_error = ? where id = ?", attempts, next, err, id)
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
	peers := entity_peers(q.ToEntity)
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

	s := peer_stream(peer)
	if s == nil {
		return false
	}
	defer s.close()

	// Read challenge from receiver
	challenge, err := s.read_challenge()
	if err != nil {
		return false
	}

	var services []string
	if q.FromServices != "" {
		services = strings.Split(q.FromServices, ",")
	}

	signature := entity_sign(q.FromEntity, string(signable_headers("msg", q.FromEntity, q.ToEntity, q.Service, q.Event, q.FromApp, q.ID, "", "", services, challenge)))

	headers := cbor_encode(Headers{
		Type: "msg", From: q.FromEntity, To: q.ToEntity, Service: q.Service, Event: q.Event,
		FromApp: q.FromApp, Services: services, ID: q.ID, Signature: signature,
	})

	// Batch headers + content + data into single write
	data := headers
	if len(q.Content) > 0 {
		data = append(data, q.Content...)
	}
	if len(q.Data) > 0 {
		data = append(data, q.Data...)
	}

	if s.write_raw(data) != nil {
		return false
	}
	if q.File != "" {
		_, err := s.write_file(q.File)
		if err != nil {
			return false
		}
	}

	// Close write direction to signal we're done sending (keeps read open for ACK)
	s.close_write()

	// Read ACK from stream
	var h Headers
	if s.read_headers(&h) != nil {
		debug("Queue direct %q failed to read ACK", q.ID)
		return false
	}

	if h.msg_type() == "ack" && h.AckID == q.ID {
		//debug("Queue direct %q received ACK", q.ID)
		return true
	}

	if h.msg_type() == "nack" && h.AckID == q.ID {
		// Reason-aware NACK handling: a "broadcast-gap" NACK means
		// the subscriber is already requesting catch-up via its own
		// resync path, so retrying the same in-order live event for
		// 7 days just floods the queue. Drop the row instead and
		// return true so the caller's delete-on-ack is the visible
		// outcome (idempotent - row's gone).
		if nack_should_drop(h.Reason) {
			queue_drop(q.ID, h.Reason)
			return true
		}
		debug("Queue direct %q received NACK reason=%q", q.ID, h.Reason)
		return false
	}

	debug("Queue direct %q received unexpected response type=%q ack=%q", q.ID, h.msg_type(), h.AckID)
	return false
}

// Send a queued broadcast message (no challenge for broadcasts)
func queue_send_broadcast(q *QueueEntry) bool {
	if !peers_sufficient() {
		return false
	}

	var services []string
	if q.FromServices != "" {
		services = strings.Split(q.FromServices, ",")
	}

	signature := entity_sign(q.FromEntity, string(signable_headers("msg", q.FromEntity, q.ToEntity, q.Service, q.Event, q.FromApp, q.ID, "", "", services, nil)))

	msg := Message{
		ID: q.ID, From: q.FromEntity, To: q.ToEntity, Service: q.Service, Event: q.Event,
		FromApp: q.FromApp, Services: services, Signature: signature,
	}
	data := cbor_encode(msg)
	if len(q.Content) > 0 {
		data = append(data, q.Content...)
	}

	p2p_pubsub_1.Publish(p2p_context, data)
	return true
}

// queue_select pulls the next batch of due messages, ordered so urgent
// traffic is delivered first. Lane A is the 50 most-urgent due messages
// (priority, then next_retry). Lane B is a reserved floor of bulk-tier
// slots so a sustained flood of higher-priority traffic can never
// starve replication. The lanes are merged and de-duplicated on id (a
// bulk row can appear in both when there is little urgent traffic).
func queue_select(db *DB) []QueueEntry {
	ts := now()

	var urgent []QueueEntry
	if err := db.scans(&urgent, "select * from queue where status = 'pending' and next_retry <= ? order by priority desc, next_retry limit 50", ts); err != nil {
		info("Queue select error: %v", err)
	}

	var bulk []QueueEntry
	if err := db.scans(&bulk, "select * from queue where status = 'pending' and next_retry <= ? and priority <= ? order by next_retry limit ?", ts, priority_bulk, queue_bulk_floor); err != nil {
		info("Queue select error (bulk floor): %v", err)
	}

	seen := make(map[string]bool, len(urgent)+len(bulk))
	entries := make([]QueueEntry, 0, len(urgent)+len(bulk))
	for _, q := range urgent {
		seen[q.ID] = true
		entries = append(entries, q)
	}
	for _, q := range bulk {
		if !seen[q.ID] {
			entries = append(entries, q)
		}
	}
	return entries
}

// Process pending queue entries
func queue_process() {
	db := db_open("db/queue.db")

	entries := queue_select(db)

	udb := db_open("db/users.db")

	// Pre-filter: drop expired and from-deleted-entity rows serially.
	// Cheap, no network. The remaining `valid` slice goes through the
	// parallel send path below.
	valid := entries[:0]
	for _, q := range entries {
		if q.Expires > 0 && q.Expires < now() {
			debug("Queue message %q expired", q.ID)
			db.exec("delete from queue where id = ?", q.ID)
			continue
		}
		if q.FromEntity != "" {
			if exists, _ := udb.exists("select 1 from entities where id=?", q.FromEntity); !exists {
				info("Queue dropping message %q from deleted entity %q", q.ID, q.FromEntity)
				db.exec("delete from queue where id = ?", q.ID)
				continue
			}
		}
		valid = append(valid, q)
	}

	if len(valid) == 0 {
		return
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
			case q.Event == "file/push":
				ok = queue_send_file_push(&q)
			default:
				ok = queue_send_direct(&q)
			}

			if ok {
				db.exec("delete from queue where id = ?", q.ID)
			} else {
				queue_fail(q.ID, "send failed")
			}
		}(q, sem)
	}
	wg.Wait()
}

// Check for sent messages that haven't received ACK (timeout)
func queue_check_ack_timeout() {
	db := db_open("db/queue.db")
	// Messages sent more than 30 seconds ago without ACK
	timeout := now() - 30
	db.exec("update queue set status = 'pending', next_retry = ? where status = 'sent' and created < ?",
		queue_next_retry(0), timeout)
	// Messages stuck in 'sending' for more than 60 seconds (safety net)
	stuck := now() - 60
	db.exec("update queue set status = 'pending', next_retry = ? where status = 'sending' and created < ?",
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
	cutoff := now() - queue_max_age

	// Log and delete expired messages
	var old []QueueEntry
	err := db.scans(&old, "select * from queue where created < ?", cutoff)
	if err != nil {
		warn("Database error loading expired queue entries: %v", err)
		return
	}
	// for _, q := range old {
	// 	warn("Queue dropping expired message: id=%q type=%q from=%q to=%q service=%q event=%q attempts=%d", q.ID, q.Type, q.FromEntity, q.ToEntity, q.Service, q.Event, q.Attempts)
	// }
	db.exec("delete from queue where created < ?", cutoff)
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

// Queue manager goroutine. Single processing loop owns every outbound
// send so that fan-out to a peer is serialised — multiple send_peer()
// callers don't race each other onto the wire. Worst-case latency
// between send_peer and the wire write is queue_tick_interval (1s) if
// no wake fires; with wake, it's the wake-pickup roundtrip
// (sub-millisecond).
func queue_manager() {
	// Periodic process tick. Picked up to 50 entries per pass; long-
	// running pair-backfill scenarios (hundreds of system-set ops at
	// once) drain in a few ticks rather than hammering N concurrent
	// goroutines.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	go func() {
		for {
			select {
			case <-tick.C:
			case <-queue_wake_ch:
			}
			queue_process()
			queue_check_ack_timeout()
		}
	}()

	// Cleanup runs less frequently
	for range time.Tick(time.Hour) {
		queue_cleanup()
		message_seen_cleanup()
	}
}
