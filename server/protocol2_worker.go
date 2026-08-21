// Mochi server: Protocol 2 — per-host (user, app) worker pool.
//
// Every inbound /mochi/2/messages frame ends up on the inbox of the
// worker for its `(user, app)` pair. The worker runs handlers serially
// — the receiver's "handler invocations for the same (user, app) never
// overlap" guarantee.
//
// Workers are per-host (not per-stream): multiple streams from
// different senders for the same (user, app) all dispatch into the
// same worker. The self-loop fast path (#102) also routes through the
// same worker via queue_reply, so local writes and remote writes
// serialise against each other for the same handler.
//
// Idle workers reap after worker_idle_default (5 minimum) of no activity.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	rd "runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
)

const (
	// worker_inbox_default — per-(user, app) channel depth. Smaller
	// values propagate back-pressure into libp2p faster; larger values
	// give more head-of-line tolerance.
	worker_inbox_default = 32

	// worker_idle_default — worker goroutine reaped after this many
	// seconds of no frames picked up (AND empty inbox).
	worker_idle_default = 300

	// worker_reaper_tick — how often the reaper checks for idle workers.
	worker_reaper_tick = 60 * time.Second
)

// reply_target abstracts where a worker's handler-result reply goes:
// stream_reply routes back to the source Receiver's batched acks; queue_reply
// and local_reply answer self-loop frames.
type reply_target interface {
	// ack signals the handler succeeded.
	ack()
	// fail signals the handler returned an error; reason is one of
	// the failure-reasons vocabulary.
	fail(reason string)
}

// worker_frame pairs a Frame with its reply target and the originating peer.
// `peer` travels explicitly because the worker registry is per-host, so origin
// cannot be inferred from the worker; self-loop frames pass net_id.
type worker_frame struct {
	frame *Frame
	peer  string
	reply reply_target
}

// user_app_key keys the per-host worker registry. Both fields are
// strings (UID and app id) so the zero value isn't ambiguous.
type user_app_key struct {
	user string
	app  string
}

// app_worker owns one goroutine that serialises handler invocations
// for the (user, app) pair. Lazy-created on first dispatch from any
// source (remote stream or self-loop), reaped after worker_idle of
// no activity.
type app_worker struct {
	user      string
	app       string
	inbox     chan *worker_frame
	last_used atomic.Int64
	in_flight atomic.Int32 // 1 while a handler is running; 0 otherwise. Used by workers_drain_test only.
}

var (
	app_workers_lock sync.RWMutex
	app_workers      = map[user_app_key]*app_worker{}
)

// worker_dispatch finds-or-creates the worker for (user, app) and pushes wf
// onto its inbox. Blocks when the inbox is full: that is what propagates
// back-pressure into libp2p flow control and serialises self-loop writes
// against remote frames.
func worker_dispatch(user, app string, wf *worker_frame) {
	key := user_app_key{user: user, app: app}

	app_workers_lock.RLock()
	w, ok := app_workers[key]
	app_workers_lock.RUnlock()

	if !ok {
		w = worker_create(key)
	}
	w.last_used.Store(now())
	w.inbox <- wf
}

// worker_inbox_offer is a try-once non-blocking enqueue into the (user, app)
// worker's inbox, creating the worker if absent; false means full. Non-blocking
// by design: the caller is in a send path, and spilling to queue.db keeps the
// backlog visible as queue depth.
func worker_inbox_offer(user, app string, wf *worker_frame) bool {
	key := user_app_key{user: user, app: app}
	app_workers_lock.RLock()
	w, ok := app_workers[key]
	app_workers_lock.RUnlock()
	if !ok || w == nil {
		w = worker_create(key)
	}
	w.last_used.Store(now())
	select {
	case w.inbox <- wf:
		return true
	default:
		return false
	}
}

// worker_create installs a new app_worker into the registry under
// app_workers_lock. Safe to race with the reaper — the reaper holds
// the write lock and re-verifies last_used before reaping.
func worker_create(key user_app_key) *app_worker {
	app_workers_lock.Lock()
	defer app_workers_lock.Unlock()

	if w, ok := app_workers[key]; ok {
		// Lost the race; another goroutine created it.
		return w
	}
	w := &app_worker{
		user:  key.user,
		app:   key.app,
		inbox: make(chan *worker_frame, peer_worker_inbox()),
	}
	w.last_used.Store(now())
	app_workers[key] = w
	go w.run()
	return w
}

// run is the worker goroutine. Tight loop: pick frame → decompress →
// run handler → ack or fail. Exits when inbox is closed (by the
// reaper). Handler panics are recovered so one buggy app can't take
// down the whole worker.
func (w *app_worker) run() {
	for wf := range w.inbox {
		w.last_used.Store(now())
		w.in_flight.Store(1)
		w.handle(wf)
		w.in_flight.Store(0)
	}
}

// frame_segment_stream wraps the CBOR segments a sender packed into Frame.Data
// so e.segment() can take them one at a time. Keyed on length, not nil: Data is
// `omitempty`, so a sender's empty slice arrives back as nil.
func frame_segment_stream(data []byte) *Stream {
	if len(data) == 0 {
		return nil
	}
	return stream_rw(io.NopCloser(bytes.NewReader(data)), nil)
}

// handle runs a single frame end-to-end: decompresses the body,
// decodes the Event from frame fields, routes
// it via e.route(), and signals completion via wf.reply.
func (w *app_worker) handle(wf *worker_frame) {
	defer func() {
		if r := recover(); r != nil {
			warn("Worker (%s,%s): handler panic for %q: %v\n%s",
				w.user, w.app, wf.frame.ID, r, rd.Stack())
			worker_fail(wf, fail_handler_panic)
		}
	}()

	f := wf.frame

	// Decompress content/data per Codec. For self-loop frames the
	// queue stored already-decoded content (Codec=0) so this is a
	// no-op; remote frames may carry zstd payloads.
	content := f.Content
	if content == nil {
		content = map[string]any{}
	}

	e := &Event{
		id:              event_id(),
		message:         f.ID,
		from:            f.From,
		to:              f.To,
		service:         f.Service,
		event:           f.Event,
		sender_app:      f.FromApp,
		sender_services: f.Services,
		peer:            wf.peer, // originating peer, NOT net_id
		content:         content,
		stream:          frame_segment_stream(f.Data),
	}

	if err := e.route(); err != nil {
		worker_fail(wf, worker_failure_reason(err))
		return
	}
	wf.reply.ack()
}

// worker_fail answers a frame with a failure reason, first clearing the dedup
// mark when the reason is one the sender retries: the mark is set before
// dispatch, so without this the retry is coalesced away and the message lost.
func worker_fail(wf *worker_frame, reason string) {
	if wf.frame != nil && wf.frame.ID != "" && fail_retryable(reason) {
		message_seen_clear(wf.frame.ID)
	}
	wf.reply.fail(reason)
}

// fail_retryable reports whether a failure reason leaves the sender still
// expecting to deliver this message. The drop set mirrors the one in
// Sender.resolve_fail and queue_reply.fail; an empty or unrecognised
// reason is transient, matching how both of those treat it.
func fail_retryable(reason string) bool {
	switch reason {
	case fail_unsupported, fail_unknown_user, fail_expired,
		fail_dedup, fail_signature_invalid:
		return false
	}
	return true
}

// worker_failure_reason maps an Event.route() error to a wire failure reason:
// unknown user / no handler / no service drop, everything else is transient.
func worker_failure_reason(err error) string {
	if err == nil {
		return ""
	}
	// Before the prefix matching: this one is asserted by the receiver, not
	// guessed from wording, and dropping it loses a broadcast event.
	if errors.Is(err, ErrBroadcastPendingFull) {
		return fail_transient
	}
	message := err.Error()
	switch {
	case strings.HasPrefix(message, "unknown user"):
		return fail_unknown_user
	case strings.HasPrefix(message, "unknown service"),
		strings.HasPrefix(message, "unknown event"),
		strings.HasPrefix(message, "no handler"),
		// The app is registered but none of its versions loaded. Fixed until
		// the operator repairs or removes it, so retrying is 50 deliveries of
		// the same failure.
		strings.HasPrefix(message, "no active version"),
		// Deterministic authorization rejections: the sender's declared
		// services are fixed in the message, so retrying can never change
		// the verdict. Drop instead of retrying forever (this is what wedged
		// the stuck _attachment/* self-loop rows at ~62 retries).
		strings.HasPrefix(message, "sender does not handle service"),
		// The app declares a handler name its Starlark globals do not
		// define. Fixed until the app is changed, so retrying is 50
		// deliveries of the same failure.
		strings.HasPrefix(message, "Starlark app function"):
		return fail_unsupported
	case strings.HasPrefix(message, "handler panic"),
		// Starlark.call's own recover, for a panic raised inside a Go
		// builtin rather than in the interpreter.
		strings.HasPrefix(message, "Starlark call "):
		return fail_handler_panic
	}
	return fail_transient
}

// worker_reaper runs once per worker_reaper_tick and removes any
// worker whose last_used is older than worker_idle AND whose inbox is
// empty. The closed inbox channel signals the worker goroutine to
// exit.
func worker_reaper() {
	for range time.Tick(worker_reaper_tick) {
		cutoff := now() - int64(peer_worker_idle_seconds())
		var doomed []user_app_key

		app_workers_lock.RLock()
		for key, w := range app_workers {
			if w.last_used.Load() < cutoff && len(w.inbox) == 0 {
				doomed = append(doomed, key)
			}
		}
		app_workers_lock.RUnlock()

		if len(doomed) == 0 {
			continue
		}

		app_workers_lock.Lock()
		cutoff = now() - int64(peer_worker_idle_seconds())
		for _, key := range doomed {
			w := app_workers[key]
			if w == nil {
				continue
			}
			// Re-verify under the write lock; another goroutine may
			// have dispatched into this worker in the gap.
			if w.last_used.Load() >= cutoff || len(w.inbox) > 0 {
				continue
			}
			close(w.inbox)
			delete(app_workers, key)
		}
		app_workers_lock.Unlock()
	}
}

// worker_count returns the current number of pending frames
// across all workers, for observability. O(n) over the worker map; fine
// for the ~50–200 typical worker population.
func worker_count() (workers, pending int) {
	app_workers_lock.RLock()
	defer app_workers_lock.RUnlock()
	for _, w := range app_workers {
		workers++
		pending += len(w.inbox)
	}
	return workers, pending
}

// workers_drain_test blocks until every worker's inbox is empty, so a test's
// tmp_dir and mutated globals are not torn down mid-frame. Tests only, and
// bounded by timeout so a stuck worker cannot wedge the suite.
func workers_drain_test(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// Check both inbox depth AND any worker mid-handle. The
		// in_flight counter catches the gap between "worker pulled
		// the frame from inbox" and "worker called reply.ack/fail".
		app_workers_lock.RLock()
		quiet := true
		for _, w := range app_workers {
			if len(w.inbox) > 0 || w.in_flight.Load() != 0 {
				quiet = false
				break
			}
		}
		app_workers_lock.RUnlock()
		if quiet {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stream_reply implements reply_target for frames received over a
// /mochi/2/messages stream. The worker posts to the Receiver's
// replies channel, where receiver_reply batches acks into one frame
// per drain.
type stream_reply struct {
	receiver *Receiver
	message  string // the message id being replied to
}

func (s stream_reply) ack() {
	if s.receiver == nil || s.receiver.closed.Load() {
		// Stream gone; sender's sweeper will time the inflight out
		// and retry. Receiver's message_mark_seen catches the dedup.
		return
	}
	select {
	case s.receiver.replies <- &Frame{Type: frame_type_ack, Replies: []string{s.message}}:
	default:
		// replies channel full — receiver_reply hasn't drained yet.
		// Drop the ack; same recovery path as a dropped stream.
		debug("Worker: dropping ack for %q (replies channel full)", s.message)
	}
}

func (s stream_reply) fail(reason string) {
	if s.receiver == nil || s.receiver.closed.Load() {
		return
	}
	if reason == "" {
		reason = fail_transient
	}
	f := &Frame{Type: frame_type_fail, Replies: []string{s.message}, Reason: reason}
	select {
	case s.receiver.replies <- f:
	default:
		debug("Worker: dropping fail for %q (replies channel full)", s.message)
	}
}

// queue_reply implements reply_target for self-loop frames: ack and fail map
// straight to queue_ack / queue_fail / queue_drop. Both recover() because they
// run on the worker goroutine, where a torn-down queue.db would panic it.
type queue_reply struct {
	id string
}

func (q queue_reply) ack() {
	defer func() {
		if r := recover(); r != nil {
			debug("queue_reply: ack panic for %q: %v", q.id, r)
		}
	}()
	queue_ack_async(q.id)
}

func (q queue_reply) fail(reason string) {
	defer func() {
		if r := recover(); r != nil {
			debug("queue_reply: fail panic for %q: %v", q.id, r)
		}
	}()
	if reason == "" {
		reason = fail_transient
	}
	// Map the v2 reason vocabulary to the queue's existing drop / fail
	// distinction. Reasons that say "retrying won't help" → drop;
	// everything else → fail (retry with backoff).
	switch reason {
	case fail_unsupported, fail_unknown_user, fail_expired,
		fail_dedup, fail_signature_invalid:
		queue_drop(q.id, reason)
	default:
		queue_fail(q.id, fmt.Sprintf("self-loop fast path: %s", reason))
	}
}

// local_reply implements reply_target for self-loop frames that never passed
// through queue.db: no row to ack, so ack() is a no-op and fail() only logs. A
// self-loop failure is a code error, so retrying the same input would fail
// again.
type local_reply struct {
	message string
	service string
	event   string
	to      string
}

// ack has no queue row to delete, but an in-process delivery is still a
// delivery success: clear any health streak for the recipient (a
// suspended recipient whose account moved onto this host must not stay
// gated). health_success is a no-op point read for healthy recipients.
func (l local_reply) ack() {
	health_success(l.to)
}

func (l local_reply) fail(reason string) {
	if reason == "" {
		reason = fail_transient
	}
	switch reason {
	case fail_dedup:
		// Common and benign — same message dispatched twice (e.g. app
		// retry on its own initiative). Debug only.
		debug("Self-loop direct dispatch: %s/%s id=%q dedup", l.service, l.event, l.message)
	case fail_signature_invalid:
		warn("Self-loop direct dispatch: %s/%s id=%q signature_invalid — local bug",
			l.service, l.event, l.message)
	default:
		info("Self-loop direct dispatch: %s/%s id=%q failed: %s",
			l.service, l.event, l.message, reason)
	}
}

// message_self_loop_dispatch enqueues m onto the local (user, app) worker
// inbox, bypassing queue.db. Returns false - caller uses the normal queue path
// - when m.target is not net_id, m has a file payload, the recipient is not
// local, or the inbox is full. `content` is the caller's already-CBOR-encoded
// body. May overtake queue.db rows for the same (user, app), which is benign:
// self-loop messages carry no cross-message ordering.
func message_self_loop_dispatch(m *Message, content []byte) bool {
	if m.target != net_id || net_id == "" {
		return false
	}
	if m.file != "" {
		return false
	}

	var body map[string]any
	if len(content) > 0 {
		if err := cbor.Unmarshal(content, &body); err != nil {
			debug("Self-loop direct dispatch: %s/%s id=%q content decode failed: %v",
				m.Service, m.Event, m.ID, err)
			return false
		}
	} else {
		body = map[string]any{}
	}

	to := m.To
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
	if user == "" {
		// Recipient did not resolve to a local user. queue.db owns
		// resolution and failure handling for that case; creating a
		// worker keyed on an empty user would strand the frame here.
		return false
	}

	wf := &worker_frame{
		frame: &Frame{
			Type:     frame_type_message,
			ID:       m.ID,
			From:     m.From,
			To:       to,
			Service:  m.Service,
			Event:    m.Event,
			FromApp:  m.FromApp,
			Services: m.Services,
			Priority: frame_priority_for(queue_priority(m.Service, m.Event)),
			Content:  body,
			Data:     m.data,
		},
		peer:  net_id,
		reply: local_reply{message: m.ID, service: m.Service, event: m.Event, to: to},
	}

	// Non-blocking, creating the worker on first use. A full inbox falls back to
	// queue.db, where the back-pressure shows up as queue depth.
	return worker_inbox_offer(user, m.Service, wf)
}
