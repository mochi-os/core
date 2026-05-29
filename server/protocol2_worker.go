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
// Idle workers reap after worker_idle_default (5 min) of no activity.
//
// Copyright Alistair Cunningham 2026

package main

import (
	"bytes"
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

// reply_target abstracts where a worker's handler-result reply goes.
// Two implementations exist:
//
//   stream_reply — for frames from a remote sender via /mochi/2/messages.
//                  Routes back to the source Receiver's replies channel,
//                  where receiver_reply batches them into ack frames.
//
//   queue_reply  — for frames from the self-loop fast path. Bypasses
//                  the wire entirely; ack/fail map straight to
//                  queue_ack / queue_fail / queue_drop.
type reply_target interface {
	// ack signals the handler succeeded.
	ack()
	// fail signals the handler returned an error; reason is one of
	// the failure-reasons vocabulary.
	fail(reason string)
}

// worker_frame pairs a Frame with its reply target plus the
// originating peer. Sent to the per-(user, app) worker via its inbox.
//
// `peer` is the libp2p peer ID of the sender — needed because the
// worker registry is per-host (not per-Receiver), so a frame's origin
// can't be inferred from the worker alone. Self-loop frames pass
// net_id here.
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

// worker_dispatch finds-or-creates the worker for the given (user, app)
// and pushes wf onto its inbox. Blocks if the inbox is full —
// receiver_read uses this to propagate back-pressure into libp2p's flow
// control. Self-loop callers also block, which serialises their writes
// against in-flight remote frames for the same (user, app).
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

// worker_inbox_offer is a try-once non-blocking enqueue into an
// already-existing worker's inbox. Returns:
//
//	true  — frame accepted; worker will process it
//	false — no worker for (user, app), OR inbox full
//
// Used by message_self_loop_dispatch. Does NOT create a worker, because
// the worker-create path (worker_create → go w.run() → ...) statically
// reaches the replication emit chain, which would form an init-cycle
// through `var replication_emit_to = replication_emit_to_real`. Falling
// through to the queue.db path on cache miss is fine — the first
// queue_select pick uses worker_dispatch (which DOES create) and from
// then on the worker is hot, so direct-dispatch hits on every
// subsequent send.
func worker_inbox_offer(user, app string, wf *worker_frame) bool {
	key := user_app_key{user: user, app: app}
	app_workers_lock.RLock()
	w, ok := app_workers[key]
	app_workers_lock.RUnlock()
	if !ok || w == nil {
		return false
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

// handle runs a single frame end-to-end: decompresses the body,
// decodes the Event from frame fields, routes
// it via e.route(), and signals completion via wf.reply.
func (w *app_worker) handle(wf *worker_frame) {
	defer func() {
		if r := recover(); r != nil {
			warn("Worker (%s,%s): handler panic for %q: %v\n%s",
				w.user, w.app, wf.frame.ID, r, rd.Stack())
			wf.reply.fail(fail_handler_panic)
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

	// e.stream carries any additional CBOR segments the sender packed
	// after the content map: /mochi/2 packs them all into Frame.Data as
	// a single []byte. We wrap that byte buffer in a Stream so
	// e.segment() / handlers using e.stream.read() can decode segments
	// one at a time. When Frame.Data is empty the stream's reader is a
	// 0-byte buffer; handlers that don't call e.segment() never notice.
	var event_stream *Stream
	if len(f.Data) > 0 || f.Data != nil {
		event_stream = stream_rw(io.NopCloser(bytes.NewReader(f.Data)), nil)
	}

	e := &Event{
		id:              event_id(),
		msg_id:          f.ID,
		from:            f.From,
		to:              f.To,
		service:         f.Service,
		event:           f.Event,
		sender_app:      f.FromApp,
		sender_services: f.Services,
		peer:            wf.peer, // originating peer, NOT net_id
		content:         content,
		stream:          event_stream,
	}

	if err := e.route(); err != nil {
		reason := worker_failure_reason(err)
		wf.reply.fail(reason)
		return
	}
	wf.reply.ack()
}

// worker_failure_reason maps an Event.route() error to a wire failure
// reason. Routing errors fall into one of three buckets:
//
//   • "unknown user / no handler / no service" → drop, retry will
//     never succeed. unsupported / unknown_user.
//   • Anything matching nack_reason_from_error's existing vocabulary
//     (broadcast-gap, pending-full, decode-failed) → translate to a
//     compatible v2 reason. We currently map all of those to
//     transient — the sender's resolver handles retry-backoff.
//   • Default → transient (the catch-all retry-later disposition).
func worker_failure_reason(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "unknown user"):
		return fail_unknown_user
	case strings.HasPrefix(msg, "unknown service"),
		strings.HasPrefix(msg, "unknown event"),
		strings.HasPrefix(msg, "no handler"):
		return fail_unsupported
	case strings.HasPrefix(msg, "handler panic"):
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

// worker_inbox_count returns the current number of pending frames
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

// workers_drain_test blocks until every worker's inbox is empty. Used
// by unit-test cleanups so the test's tmp_dir / mutated globals
// (data_dir, net_id) aren't torn down while a background worker is
// mid-processing a frame that references them.
//
// Safe to call from tests only — production code has no reason to
// drain workers, and a stuck worker holding its inbox open would block
// indefinitely. Bounded by timeout so a broken handler doesn't wedge
// the whole test suite.
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
	id       string // the message id being replied to
}

func (s stream_reply) ack() {
	if s.receiver == nil || s.receiver.closed.Load() {
		// Stream gone; sender's sweeper will time the inflight out
		// and retry. Receiver's message_mark_seen catches the dedup.
		return
	}
	select {
	case s.receiver.replies <- &Frame{Type: frame_type_ack, Replies: []string{s.id}}:
	default:
		// replies channel full — receiver_reply hasn't drained yet.
		// Drop the ack; same recovery path as a dropped stream.
		debug("Worker: dropping ack for %q (replies channel full)", s.id)
	}
}

func (s stream_reply) fail(reason string) {
	if s.receiver == nil || s.receiver.closed.Load() {
		return
	}
	if reason == "" {
		reason = fail_transient
	}
	f := &Frame{Type: frame_type_fail, Replies: []string{s.id}, Reason: reason}
	select {
	case s.receiver.replies <- f:
	default:
		debug("Worker: dropping fail for %q (replies channel full)", s.id)
	}
}

// queue_reply implements reply_target for frames originating in the
// self-loop fast path. Bypasses the wire — ack and fail map directly
// to queue_ack / queue_fail / queue_drop on the local queue.db row.
//
// Both ack() and fail() wrap their DB writes in a recover() because
// they run on the worker goroutine — a torn-down queue.db (e.g. unit
// tests cleaning up the tmp_dir before the worker drains) would
// otherwise panic the worker.
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

// local_reply implements reply_target for self-loop frames that never
// passed through queue.db (message_self_loop_dispatch hot path). No
// queue row to ack/delete; ack() is a no-op, fail() just logs at the
// matching severity for the failure-reason class.
//
// Why not retry on fail? Self-loop has no network unreliability — the
// handler runs in-process. A failure is a code error (handler returned
// fail, panicked, decoded badly): retrying the same code with the same
// input would deterministically fail again. Better to log loudly and
// move on than to recycle the row through queue.db's exponential
// backoff.
type local_reply struct {
	id      string
	service string
	event   string
}

func (l local_reply) ack() {}

func (l local_reply) fail(reason string) {
	if reason == "" {
		reason = fail_transient
	}
	switch reason {
	case fail_dedup:
		// Common and benign — same message dispatched twice (e.g. app
		// retry on its own initiative). Debug only.
		debug("Self-loop direct dispatch: %s/%s id=%q dedup", l.service, l.event, l.id)
	case fail_signature_invalid:
		warn("Self-loop direct dispatch: %s/%s id=%q signature_invalid — local bug",
			l.service, l.event, l.id)
	default:
		info("Self-loop direct dispatch: %s/%s id=%q failed: %s",
			l.service, l.event, l.id, reason)
	}
}

// message_self_loop_dispatch tries to enqueue m onto the local
// per-(user, app) worker inbox without going through queue.db. Returns
// true on success. Returns false when:
//
//   - m.target is not net_id (caller should use the normal queue path)
//   - m has a file payload (queue.db owns large-payload tracking)
//   - the worker inbox is full (caller falls back to queue.db so the
//     row gets the usual at-least-once retry semantics)
//
// `content` is the CBOR-encoded body produced by the caller's
// `cbor_encode(m.content)`; we decode it once here for the worker
// frame rather than have the worker re-decode it.
//
// Why bypass queue.db at all? Self-loop is in-process — there's no
// network to fail, no peer to retry against. Insert-into-queue +
// queue_select pick-up + queue_send_self_loop_fast dispatch +
// queue_ack-on-success is four SQLite round-trips for a message that
// never leaves the process. Direct dispatch is zero. At 80k+ self-loop
// rows backlogged on wasabi this matters; even at idle it shaves the
// happy-path send latency from "next queue tick" (≤1s) to "next
// scheduler slice" (μs).
//
// Older self-loop rows already in queue.db keep draining via
// queue_send_self_loop_fast. Worker order: the direct-dispatch path
// may jump ahead of those queue.db rows for the same (user, app)
// because both feed the same worker inbox. Self-loop has no FK chain
// across messages, so this re-ordering is benign — and once the
// backlog drains, the queue.db source goes silent and direct-dispatch
// is the only path.
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
		reply: local_reply{id: m.ID, service: m.Service, event: m.Event},
	}

	// Lookup-only non-blocking enqueue. Two miss cases both fall back
	// to queue.db:
	//
	//   1. No worker yet for (user, app). The queue.db path will create
	//      one on first pick; subsequent sends hit this hot path.
	//   2. Inbox full. The worker is saturated; back-pressure shows up
	//      as queue depth (visible via `mochictl pipelining status` and
	//      queue length metrics) rather than as a blocked send call.
	return worker_inbox_offer(user, m.Service, wf)
}
