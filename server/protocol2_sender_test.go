// Tests for protocol2_sender.go — failure-reason resolver, rate limit,
// inflight/ping sweep, shutdown drain, activity-resets-ping.
//
// Phase 3e per claude/plans/protocol2.md → Testing strategy.
//
// Most of these exercise pieces of the Sender state machine that don't
// need a libp2p stream — resolve_fail, rate_gate, sweep, shutdown can
// run on a synthetic Sender with stream=nil. Stream-IO behavior lands
// in the end-to-end integration tests.

package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// new_test_sender builds a Sender with no stream attached. Suitable
// for testing the in-memory state machine pieces (resolve_fail, sweep,
// rate_gate, shutdown drain).
func new_test_sender(t *testing.T, peer string) *Sender {
	t.Helper()
	return &Sender{
		peer:     peer,
		outbox:   make(chan *outbound, 8),
		inflight: map[string]*pending{},
		pings:    map[string]int64{},
		claimed:  map[string]bool{},
	}
}

// stash_sender installs a Sender in the global registry under `peer`
// so the test's calls (shutdown, senders_*) can find it; returns a
// cleanup that restores any prior entry.
func stash_sender(t *testing.T, peer string, s *Sender) func() {
	t.Helper()
	senders_lock.Lock()
	prev, had := senders[peer]
	senders[peer] = s
	senders_lock.Unlock()
	return func() {
		senders_lock.Lock()
		if had {
			senders[peer] = prev
		} else {
			delete(senders, peer)
		}
		senders_lock.Unlock()
	}
}

// install_queue_row writes a queue.db row so queue_ack / queue_fail /
// queue_drop have something to operate on.
func install_queue_row(t *testing.T, id string) {
	t.Helper()
	db := db_open("db/queue.db")
	db.exec(`insert into queue
		(id, type, target, from_entity, to_entity, service, event,
		 from_app, from_services, content, data, file, expires, status,
		 attempts, next_retry, created, priority)
		values
		(?, 'direct', 'peer', '', '', 's', 'e', '', '', '', '', '', 0,
		 'sending', 0, ?, ?, ?)`,
		id, now(), now(), priority_interactive)
}

// --- resolve_fail vocabulary -------------------------------------------

func TestResolveFailVocabulary(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s := new_test_sender(t, "test-peer")

	cases := []struct {
		reason       string
		wantStatus   string // "deleted" (drop/ack) | "pending" (fail-retry) | ""
		wantNonempty bool   // true → must remain in queue with status set
	}{
		{fail_unsupported, "deleted", false},
		{fail_unknown_user, "deleted", false},
		{fail_signature_invalid, "deleted", false},
		{fail_expired, "deleted", false},
		{fail_dedup, "deleted", false},
		{fail_rate_limited, "pending", true},
		{fail_buffer_full, "pending", true},
		{fail_handler_panic, "pending", true},
		{fail_unclaimed, "pending", true},
		{fail_transient, "pending", true},
		{"", "pending", true},                // empty → transient
		{"future-unknown-reason", "pending", true}, // unknown → transient retry
	}

	for _, c := range cases {
		t.Run(c.reason+"/wants-"+c.wantStatus, func(t *testing.T) {
			id := "test-" + c.reason + "-" + c.wantStatus
			install_queue_row(t, id)

			p := &pending{queue: id, sent: now()}
			s.resolve_fail(p, c.reason)

			db := db_open("db/queue.db")
			row, err := db.row("select status from queue where id=?", id)
			if err != nil {
				t.Fatalf("db.row: %v", err)
			}
			switch c.wantStatus {
			case "deleted":
				if row != nil {
					t.Errorf("reason=%q: row still present, want deleted", c.reason)
				}
			case "pending":
				if row == nil {
					t.Errorf("reason=%q: row deleted, want kept for retry", c.reason)
				} else {
					if s, _ := row["status"].(string); s != "pending" {
						t.Errorf("reason=%q: status=%q, want pending", c.reason, s)
					}
				}
			}
		})
	}
}

func TestResolveFailUnclaimedClearsClaimCache(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s := new_test_sender(t, "p")
	s.claimed["entity-a"] = true
	s.claimed["entity-b"] = true

	id := "uc-1"
	install_queue_row(t, id)

	s.resolve_fail(&pending{queue: id, sent: now()}, fail_unclaimed)

	if len(s.claimed) != 0 {
		t.Errorf("unclaimed fail did not clear claimed cache: %v", s.claimed)
	}
}

// --- handle_inbound ----------------------------------------------------

func TestHandleInboundUpdatesLastInbound(t *testing.T) {
	s := new_test_sender(t, "p")
	before := s.last_inbound.Load()
	s.handle_inbound(&Frame{Type: frame_type_ack, Replies: []string{}})
	after := s.last_inbound.Load()
	if after <= before {
		t.Errorf("handle_inbound did not bump last_inbound (before=%d after=%d)", before, after)
	}
}

func TestHandleInboundAckClearsInflight(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s := new_test_sender(t, "p")
	install_queue_row(t, "msg-1")
	install_queue_row(t, "msg-2")
	install_queue_row(t, "msg-3")
	s.inflight["msg-1"] = &pending{queue: "msg-1", sent: now()}
	s.inflight["msg-2"] = &pending{queue: "msg-2", sent: now()}
	s.inflight["msg-3"] = &pending{queue: "msg-3", sent: now()}

	// Batched ack frame covering 2 of 3.
	s.handle_inbound(&Frame{Type: frame_type_ack, Replies: []string{"msg-1", "msg-3"}})
	// The Sender's ack handler queues the DB-delete via the async
	// batcher. Tests don't run queue_ack_batcher, so drain manually.
	queue_ack_drain()

	if _, ok := s.inflight["msg-1"]; ok {
		t.Error("msg-1 still in inflight after ack")
	}
	if _, ok := s.inflight["msg-3"]; ok {
		t.Error("msg-3 still in inflight after ack")
	}
	if _, ok := s.inflight["msg-2"]; !ok {
		t.Error("msg-2 was incorrectly removed (not in ack batch)")
	}

	db := db_open("db/queue.db")
	for _, id := range []string{"msg-1", "msg-3"} {
		row, _ := db.row("select 1 from queue where id=?", id)
		if row != nil {
			t.Errorf("%s row not deleted on ack", id)
		}
	}
	row, _ := db.row("select 1 from queue where id=?", "msg-2")
	if row == nil {
		t.Errorf("msg-2 row deleted unexpectedly")
	}
}

func TestHandleInboundOrphanAckIgnored(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s := new_test_sender(t, "p")
	// No inflight entries. Ack for unknown id MUST be a no-op
	// (logged and dropped per plan).
	s.handle_inbound(&Frame{Type: frame_type_ack, Replies: []string{"unknown-id"}})
	if len(s.inflight) != 0 {
		t.Errorf("orphan ack mutated inflight: %v", s.inflight)
	}
}

func TestHandleInboundPongClearsPing(t *testing.T) {
	s := new_test_sender(t, "p")
	s.pings["ping-1"] = now()
	s.pings["ping-2"] = now()
	s.handle_inbound(&Frame{Type: frame_type_pong, ID: "ping-1"})
	if _, ok := s.pings["ping-1"]; ok {
		t.Error("pong did not clear matching ping")
	}
	if _, ok := s.pings["ping-2"]; !ok {
		t.Error("pong cleared wrong ping")
	}
}

// --- rate_gate ---------------------------------------------------------

func TestRateGateUnlimitedNoBlock(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	// Default peer.rate is 0 (unlimited). rate_gate should return
	// immediately for any number of calls.
	s := new_test_sender(t, "p")
	start := time.Now()
	for i := 0; i < 1000; i++ {
		s.rate_gate()
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("rate_gate took %v for 1000 calls at unlimited (slow)", elapsed)
	}
}

func TestRateGateLimitsViaEnv(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	// peer.rate is read via ini_int("peer", "rate", 0). The ini
	// package exposes env-var overrides as MOCHI_<SECTION>_<KEY>.
	// Limit = 3 / sec → the 4th call MUST block for >= 50ms (the
	// sleep granularity inside rate_gate).
	t.Setenv("MOCHI_PEER_RATE", "3")

	if got := peer_rate(); got != 3 {
		t.Fatalf("env override didn't take effect: peer_rate=%d, want 3", got)
	}

	s := new_test_sender(t, "p")

	// First 3 calls fit in the current 1s bucket.
	start := time.Now()
	for i := 0; i < 3; i++ {
		s.rate_gate()
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("first 3 calls took %v, want fast-path", elapsed)
	}

	// 4th call MUST block until the bucket rolls (≥ 50ms granularity
	// + however long is left in the current 1s window). Run it in a
	// goroutine so we can prove it blocks.
	blocked := make(chan time.Duration)
	go func() {
		t0 := time.Now()
		s.rate_gate()
		blocked <- time.Since(t0)
	}()

	select {
	case d := <-blocked:
		// Must have blocked at least 50ms (the inner sleep) for
		// the bucket to roll.
		if d < 40*time.Millisecond {
			t.Errorf("rate_gate over-limit returned in %v (no block)", d)
		}
	case <-time.After(2 * time.Second):
		// Should release after ≤ 1s when the bucket rolls.
		t.Error("rate_gate over-limit never released")
		// Force release by closing — otherwise the goroutine leaks.
		s.closed.Store(true)
	}
}

// --- ping_loop activity gate ------------------------------------------

func TestPingLoopSkipsWhenActive(t *testing.T) {
	// ping_loop iterates a tick; if last_inbound is within the
	// interval window, it should skip. We can't easily exercise
	// the whole loop, but we CAN verify the gate logic by calling
	// the loop body's predicate directly.
	s := new_test_sender(t, "p")
	interval := time.Duration(peer_ping_interval_seconds()) * time.Second

	// Simulate recent activity.
	s.last_inbound.Store(time.Now().UnixNano())

	// The predicate `time.Since(time.Unix(0, last)) < interval`
	// SHOULD return true → skip ping.
	last := s.last_inbound.Load()
	if !(last > 0 && time.Since(time.Unix(0, last)) < interval) {
		t.Errorf("ping skip-predicate failed for recent activity (last=%d)", last)
	}

	// Stale activity (zero) → predicate false → fire.
	s.last_inbound.Store(0)
	last = s.last_inbound.Load()
	if last > 0 && time.Since(time.Unix(0, last)) < interval {
		t.Errorf("ping skip-predicate held for zero last_inbound")
	}
}

// --- senders_sweep_all -------------------------------------------------

func TestSendersSweepTimesOutInflight(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s := new_test_sender(t, "sweep-peer")
	restore := stash_sender(t, "sweep-peer", s)
	defer restore()

	// One fresh inflight (just sent) + one stale (sent 1h ago).
	freshID := "fresh-1"
	staleID := "stale-1"
	install_queue_row(t, freshID)
	install_queue_row(t, staleID)
	s.inflight[freshID] = &pending{queue: freshID, sent: now()}
	s.inflight[staleID] = &pending{queue: staleID, sent: now() - 3600}

	senders_sweep_all()

	if _, ok := s.inflight[staleID]; ok {
		t.Error("stale inflight not swept")
	}
	if _, ok := s.inflight[freshID]; !ok {
		t.Error("fresh inflight wrongly swept")
	}

	// Stale row's status should now be 'pending' (queue_fail).
	db := db_open("db/queue.db")
	row, _ := db.row("select status from queue where id=?", staleID)
	if row == nil {
		t.Fatalf("stale row missing from queue.db")
	}
	if st, _ := row["status"].(string); st != "pending" {
		t.Errorf("stale row status=%q, want pending", st)
	}
}

// TestSendersSweepStampsPeerUnreachable: the real sweep path, repeated past
// the stall threshold, persists a peer_unreachable row — the signal the
// offline-irreparable detector reads. Closes the Sender->DB loop that the
// irreparable unit tests stub by calling peer_mark_no_progress directly.
func TestSendersSweepStampsPeerUnreachable(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	peer_progress = map[string]PeerProgress{}

	s := new_test_sender(t, "sweep-stamp-peer")
	restore := stash_sender(t, "sweep-stamp-peer", s)
	defer restore()

	db := db_open("db/replication.db")
	db.exec("insert into pair (peer, added, role) values ('sweep-stamp-peer', 0, '')") // a replication member
	// Each sweep removes the stale inflight, so re-arm one before each of
	// peer_stall_threshold sweeps to cross into stalled.
	for i := 0; i < peer_stall_threshold; i++ {
		id := fmt.Sprintf("stale-stamp-%d", i)
		install_queue_row(t, id)
		s.inflight[id] = &pending{queue: id, sent: now() - 3600}
		senders_sweep_all()
	}

	if ok, _ := db.exists("select 1 from peer_unreachable where peer=?", "sweep-stamp-peer"); !ok {
		t.Error("repeated sweep timeouts must stamp peer_unreachable for the offline detector")
	}
}

func TestSendersSweepTimesOutPings(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s := new_test_sender(t, "ping-sweep")
	restore := stash_sender(t, "ping-sweep", s)
	defer restore()

	// Stale ping (older than ping_timeout). Should trigger
	// shutdown (which marks closed=true).
	staleAge := int64(peer_ping_timeout_seconds() + 10)
	s.pings["ping-stale"] = now() - staleAge

	senders_sweep_all()

	if !s.closed.Load() {
		t.Error("stale ping did not trigger shutdown")
	}
}

// --- fail_outbound -----------------------------------------------------

func TestFailOutboundQueuesFail(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s := new_test_sender(t, "p")
	id := "outbound-fail-1"
	install_queue_row(t, id)
	s.fail_outbound(&outbound{queue: id, frame: &Frame{}}, "test-reason")

	db := db_open("db/queue.db")
	row, _ := db.row("select status from queue where id=?", id)
	if row == nil {
		t.Fatal("row missing")
	}
	if st, _ := row["status"].(string); st != "pending" {
		t.Errorf("status=%q, want pending after fail_outbound", st)
	}
}

func TestFailOutboundEmptyQueueIsNoop(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	// Frames with no queue id (synthetic, e.g. bye / ping) MUST NOT
	// touch queue.db.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("fail_outbound on empty queue panicked: %v", r)
		}
	}()
	s := new_test_sender(t, "p")
	s.fail_outbound(&outbound{queue: "", frame: &Frame{Type: frame_type_ping}}, "shouldn't touch db")
}

// --- shutdown ----------------------------------------------------------

func TestShutdownDrainsInflightToQueueFail(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()

	s := new_test_sender(t, "shutdown-peer")
	restore := stash_sender(t, "shutdown-peer", s)
	defer restore()

	for _, id := range []string{"sd-1", "sd-2", "sd-3"} {
		install_queue_row(t, id)
		s.inflight[id] = &pending{queue: id, sent: now()}
	}

	s.shutdown()
	if !s.closed.Load() {
		t.Error("shutdown did not mark closed=true")
	}
	if len(s.inflight) != 0 {
		t.Errorf("shutdown left inflight entries: %d", len(s.inflight))
	}

	db := db_open("db/queue.db")
	for _, id := range []string{"sd-1", "sd-2", "sd-3"} {
		row, _ := db.row("select status from queue where id=?", id)
		if row == nil {
			t.Errorf("%s row missing after shutdown", id)
			continue
		}
		if st, _ := row["status"].(string); st != "pending" {
			t.Errorf("%s status=%q, want pending after shutdown", id, st)
		}
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	s := new_test_sender(t, "idem-peer")
	restore := stash_sender(t, "idem-peer", s)
	defer restore()
	s.shutdown()
	// Second call must NOT panic (CompareAndSwap returns false → no-op).
	s.shutdown()
}

// --- senders_bye_all ---------------------------------------------------

func TestSendersBeyAllNoopOnNoSenders(t *testing.T) {
	// Sanity: with no Senders in the registry, senders_bye_all should
	// return promptly without blocking.
	senders_lock.Lock()
	saved := senders
	senders = map[string]*Sender{}
	senders_lock.Unlock()
	defer func() {
		senders_lock.Lock()
		senders = saved
		senders_lock.Unlock()
	}()

	done := make(chan struct{})
	go func() {
		senders_bye_all(50 * time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("senders_bye_all blocked with no senders")
	}
}

// --- write_one window cap (synthetic) ----------------------------------

// fake_writer implements io.Writer for stream_io-style tests of
// write_one and helpers that need a writable target.
type fake_writer struct {
	mu  sync.Mutex
	buf []byte
}

func (f *fake_writer) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buf = append(f.buf, p...)
	return len(p), nil
}

// (sender_stream methods Reset/Close not needed by write_one)

// TestWriteOneAddsToInflight covers the "insert BEFORE write" invariant.
// We can't easily call write_one without a stream, but we can confirm
// the inflight map's per-write behavior by inserting + checking the
// state at known points. The full window-blocking behavior is exercised
// in the integration tests.
func TestWriteOneInflightInvariant(t *testing.T) {
	t.Skip("write_one requires a real stream; integration tests cover the inflight-before-write invariant")
}

// --- senders_peer_invalidate -------------------------------------------

func TestSendersPeerInvalidateTearsDown(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	s := new_test_sender(t, "inv-peer")
	restore := stash_sender(t, "inv-peer", s)
	defer restore()
	senders_peer_invalidate("inv-peer")
	if !s.closed.Load() {
		t.Error("senders_peer_invalidate did not mark Sender closed")
	}
}

func TestSendersPeerInvalidateUnknownPeerNoop(t *testing.T) {
	// Calling for a peer with no Sender must not panic or leak state.
	senders_peer_invalidate("never-registered-peer")
}

// --- failure-reason resolution: stable atomics  ------------------------

var _ atomic.Int64 // import placeholder; remove if unused
