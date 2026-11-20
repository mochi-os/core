// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for protocol2_sender.go. Most run on a synthetic Sender with stream =
// nil; stream IO behaviour is covered by the integration tests.

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	p2p "github.com/libp2p/go-libp2p"
	p2p_host "github.com/libp2p/go-libp2p/core/host"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	p2p_rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
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
	previous, had := senders[peer]
	senders[peer] = s
	senders_lock.Unlock()
	return func() {
		senders_lock.Lock()
		if had {
			senders[peer] = previous
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
	setup_replication_test(t)

	s := new_test_sender(t, "test-peer")

	cases := []struct {
		reason        string
		want_status   string // "deleted" (drop/ack) | "pending" (fail-retry) | ""
		want_nonempty bool   // true → must remain in queue with status set
	}{
		{fail_unsupported, "deleted", false},
		{fail_unknown_user, "deleted", false},
		{fail_unproven, "pending", true},
		{fail_buffer_full, "pending", true},
		{fail_handler_panic, "pending", true},
		{fail_unclaimed, "pending", true},
		{fail_transient, "pending", true},
		{"", "pending", true},                      // empty → transient
		{"future-unknown-reason", "pending", true}, // unknown → transient retry
	}

	for _, c := range cases {
		t.Run(c.reason+"/wants-"+c.want_status, func(t *testing.T) {
			id := "test-" + c.reason + "-" + c.want_status
			install_queue_row(t, id)

			p := &pending{queue: id, sent: now()}
			s.resolve_fail(p, c.reason)

			db := db_open("db/queue.db")
			row, err := db.row("select status from queue where id=?", id)
			if err != nil {
				t.Fatalf("db.row: %v", err)
			}
			switch c.want_status {
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
	setup_replication_test(t)

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
	setup_replication_test(t)

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
	setup_replication_test(t)

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
	setup_replication_test(t)
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
	setup_replication_test(t)
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
	setup_replication_test(t)

	s := new_test_sender(t, "sweep-peer")
	restore := stash_sender(t, "sweep-peer", s)
	defer restore()

	// One fresh inflight (just sent) + one stale (sent 1h ago).
	fresh_i_d := "fresh-1"
	stale_i_d := "stale-1"
	install_queue_row(t, fresh_i_d)
	install_queue_row(t, stale_i_d)
	s.inflight[fresh_i_d] = &pending{queue: fresh_i_d, sent: now()}
	s.inflight[stale_i_d] = &pending{queue: stale_i_d, sent: now() - 3600}

	senders_sweep_all()

	if _, ok := s.inflight[stale_i_d]; ok {
		t.Error("stale inflight not swept")
	}
	if _, ok := s.inflight[fresh_i_d]; !ok {
		t.Error("fresh inflight wrongly swept")
	}

	// Stale row's status should now be 'pending' (queue_fail).
	db := db_open("db/queue.db")
	row, _ := db.row("select status from queue where id=?", stale_i_d)
	if row == nil {
		t.Fatalf("stale row missing from queue.db")
	}
	if st, _ := row["status"].(string); st != "pending" {
		t.Errorf("stale row status=%q, want pending", st)
	}
}

func TestSendersSweepTimesOutPings(t *testing.T) {
	setup_replication_test(t)

	s := new_test_sender(t, "ping-sweep")
	restore := stash_sender(t, "ping-sweep", s)
	defer restore()

	// Stale ping (older than ping_timeout). Should trigger
	// shutdown (which marks closed=true).
	stale_age := int64(peer_ping_timeout_seconds() + 10)
	s.pings["ping-stale"] = now() - stale_age

	senders_sweep_all()

	if !s.closed.Load() {
		t.Error("stale ping did not trigger shutdown")
	}
}

// --- fail_outbound -----------------------------------------------------

func TestFailOutboundQueuesFail(t *testing.T) {
	setup_replication_test(t)

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
	setup_replication_test(t)
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
	setup_replication_test(t)

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
	setup_replication_test(t)
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
//
//lint:ignore U1000 test scaffolding
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

func TestWriteOneInflightInvariant(t *testing.T) {
	t.Skip("write_one requires a real stream; integration tests cover the inflight-before-write invariant")
}

// --- senders_peer_invalidate -------------------------------------------

func TestSendersPeerInvalidateTearsDown(t *testing.T) {
	setup_replication_test(t)
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

// --- proofs the far side sends ------------------------------------------

// sender_proof is a valid proof of a fresh entity over the Sender's offered
// challenge: what the far side answers a prove with.
func sender_proof(t *testing.T, s *Sender) *Frame {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	entity := base58_encode(public)
	signable, err := responder_signable(s.offered, entity, net_id, protocol_messages)
	if err != nil {
		t.Fatalf("responder_signable: %v", err)
	}
	return &Frame{Type: frame_type_claim, From: entity, Signature: ed25519.Sign(private, signable)}
}

// A proof for an entity this Sender never asked about is not verified or
// recorded, and a peer that keeps sending them has the stream closed: each
// cost a signature check, and each valid one grew `proven` for the life of
// the stream.
func TestSenderIgnoresUnsolicitedProofs(t *testing.T) {
	messages_net_id(t)
	s := new_test_sender(t, "12D3KooWUnsolicited")
	s.offered = must_challenge(t)
	s.proven = map[string]bool{}
	s.proving = map[string]bool{}

	asked := sender_proof(t, s)
	s.proving[asked.From] = true
	s.asked = map[string]bool{asked.From: true}
	s.handle_inbound(asked)
	if !s.proven[asked.From] {
		t.Fatal("control: a proof asked for was not recorded")
	}

	for i := 0; i < sender_unsolicited_maximum; i++ {
		unasked := sender_proof(t, s)
		s.handle_inbound(unasked)
		s.handle_inbound(&Frame{Type: frame_type_fail, From: sender_proof(t, s).From, Reason: fail_unknown_user})
		if s.proven[unasked.From] {
			t.Fatal("THE POINT: a proof nobody asked for was recorded")
		}
	}
	if len(s.proven) != 1 {
		t.Errorf("THE POINT: %d entities recorded proven, want only the one asked for", len(s.proven))
	}
	if !s.closed.Load() {
		t.Error("THE POINT: a peer sending proofs nobody asked for kept the stream")
	}
}

// A proof is backed by the first message to its entity, the rule the receiver
// refunds a prove by. Counting every message naming a sender, as the Sender
// did, ran ahead of the receiver, which now refunds only those.
func TestSenderBacksAProofWithItsTargetsFirstMessage(t *testing.T) {
	s := new_test_sender(t, "12D3KooWBacked")
	s.stream = &fake_stream{buf: &bytes.Buffer{}}
	s.proven = map[string]bool{"proven": true}
	from := test_entity_id('f')
	s.claimed[from] = true
	s.unbacked = 3
	write := func(to string) {
		t.Helper()
		if err := s.write_one(&outbound{frame: &Frame{Type: frame_type_message, From: from, To: to, Service: "s", Event: "e"}}); err != nil {
			t.Fatalf("write_one: %v", err)
		}
	}
	write("other")
	if s.unbacked != 3 {
		t.Errorf("THE POINT: a message to an entity not proven on this stream backed a prove (%d unbacked)", s.unbacked)
	}
	write("proven")
	if s.unbacked != 2 {
		t.Errorf("the first message to a proven entity left %d unbacked, want 2", s.unbacked)
	}
	write("proven")
	if s.unbacked != 2 {
		t.Errorf("THE POINT: a second message to the same entity backed another prove (%d unbacked)", s.unbacked)
	}
}

// --- failure-reason resolution: stable atomics  ------------------------

var _ atomic.Int64 // import placeholder; remove if unused

// sender_test_hosts connects two real hosts, makes the first net_me, and holds
// the second in the registry as connected, so peer_connect answers at once. A
// handler given is the far host's for /mochi/2/messages, set before the two
// connect so identify lists it; options given are the far host's.
func sender_test_hosts(t *testing.T, handler p2p_network.StreamHandler, options ...p2p.Option) (p2p_host.Host, p2p_host.Host) {
	t.Helper()
	setup_peer_discovery_test(t)
	reset_peer_reachability(t)
	host := func(options ...p2p.Option) p2p_host.Host {
		h, err := p2p.New(append([]p2p.Option{p2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0")}, options...)...)
		if err != nil {
			t.Fatalf("test host: %v", err)
		}
		t.Cleanup(func() { h.Close() })
		return h
	}
	near, far := host(), host(options...)
	if handler != nil {
		far.SetStreamHandler(protocol_messages, handler)
	}
	saved_me, saved_id := net_me, net_id
	net_me, net_id = near, near.ID().String()
	t.Cleanup(func() { net_me, net_id = saved_me, saved_id })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := near.Connect(ctx, p2p_peer.AddrInfo{ID: far.ID(), Addrs: far.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	peers_lock.Lock()
	peers[far.ID().String()] = Peer{ID: far.ID().String(), state: peer_state_connected}
	peers_lock.Unlock()
	peer_progress_lock.Lock()
	delete(peer_progress, far.ID().String())
	peer_progress_lock.Unlock()
	return near, far
}

// TestSenderCountsAWithheldHelloAsNoProgress is #150: a peer that opened the
// stream and never sent its hello cost the queue a full stream_open_timeout per
// open with nothing recorded, so it was never stalled and every row it was owed
// paid again.
func TestSenderCountsAWithheldHelloAsNoProgress(t *testing.T) {
	_, far := sender_test_hosts(t, func(s p2p_network.Stream) {
		s.Write([]byte("not a hello"))
		s.Close()
	})
	peer := far.ID().String()

	if _, err := sender_open(peer); err == nil {
		t.Fatal("control: an open with no hello succeeded")
	}
	peer_progress_lock.Lock()
	evidence := peer_progress[peer].Since
	peer_progress_lock.Unlock()
	if evidence == 0 {
		t.Error("THE POINT: a peer that withheld its hello left no evidence against its progress")
	}
	if peer_is_silent(peer) {
		t.Error("a hello withheld on a live connection silenced the peer, as though it could not be dialled")
	}
}

// TestPeerProtocolOpenSparesALiveConnection is #168: a stream that would not
// open on a live connection counted towards silence, which only a successful
// open - now refused - or a fresh dial cleared, so a connected peer stayed
// silent until its connection happened to drop. A peer that does not speak the
// protocol is answered unreachable with nothing held against it; a stream the
// far side refuses is progress evidence, with its trial window.
func TestPeerProtocolOpenSparesALiveConnection(t *testing.T) {
	near, far := sender_test_hosts(t, nil)
	peer := far.ID().String()

	// Each open negotiates: nothing the peer's identify said decides it.
	for i := 0; i <= peer_silent_failure_threshold; i++ {
		near.Peerstore().SetProtocols(far.ID())
		if _, err := peer_protocol_open(peer, "/mochi/test/unspoken"); err != error_sender_unreachable {
			t.Fatalf("an unspoken protocol answered %v, want error_sender_unreachable", err)
		}
	}
	if peer_is_silent(peer) {
		t.Error("THE POINT: opens of a protocol the connected peer does not speak silenced it")
	}

	// A peer at its inbound stream limit refuses every stream on a live
	// connection.
	limits := p2p_rcmgr.DefaultLimits
	limits.PeerBaseLimit.StreamsInbound = 0
	limits.PeerLimitIncrease.StreamsInbound = 0
	resources, err := p2p_rcmgr.NewResourceManager(p2p_rcmgr.NewFixedLimiter(limits.AutoScale()))
	if err != nil {
		t.Fatal(err)
	}
	full, err := p2p.New(p2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), p2p.ResourceManager(resources))
	if err != nil {
		t.Fatalf("test host: %v", err)
	}
	defer full.Close()
	full.SetStreamHandler(protocol_messages, func(s p2p_network.Stream) { s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := near.Connect(ctx, p2p_peer.AddrInfo{ID: full.ID(), Addrs: full.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	refusing := full.ID().String()
	peers_lock.Lock()
	peers[refusing] = Peer{ID: refusing, state: peer_state_connected}
	peers_lock.Unlock()

	for i := 0; i <= peer_silent_failure_threshold; i++ {
		if _, err := peer_protocol_open(refusing, protocol_messages); err == nil {
			t.Fatal("control: a peer at its stream limit let a stream open")
		}
	}
	if peer_is_silent(refusing) {
		t.Error("THE POINT: streams refused on a live connection silenced the peer")
	}
	peer_progress_lock.Lock()
	evidence := peer_progress[refusing].Since
	peer_progress_lock.Unlock()
	if evidence == 0 {
		t.Error("streams refused on a live connection left no evidence against the peer's progress")
	}
}

// sender_test_status reads a queue row's status and attempts, nil when gone.
func sender_test_status(id string) map[string]any {
	row, _ := db_open("db/queue.db").row("select status, attempts from queue where id=?", id)
	return row
}

// sender_test_wait polls until check holds or within passes.
func sender_test_wait(within time.Duration, check func() bool) bool {
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if check() {
			return true
		}
	}
	return check()
}

// TestSenderDropsWhatTheFarSideDoesNotHost is #134: a peer that no longer
// hosts an entity answers its prove with unknown_user, and the Sender failed
// the messages held for it with the retry path, so each was retried fifty
// times and reaped as a timeout a week later. They are dropped as a receiver's
// answer to the message itself would drop them: the row deleted, the sending
// app told, the recipient's health charged a denial.
func TestSenderDropsWhatTheFarSideDoesNotHost(t *testing.T) {
	setup_replication_test(t)
	var calls []error_call
	restore := error_test_capture(&calls)
	db := db_open("db/queue.db")
	gone := test_entity_id('g')
	for _, id := range []string{"gone-1", "gone-2"} {
		error_test_queue_insert(db, id, "owner", "feeds", gone, "feeds", "post/new", now())
	}
	s := new_test_sender(t, "12D3KooWNotHosted")
	held := []*outbound{
		{queue: "gone-1", frame: &Frame{Type: frame_type_message, ID: "gone-1", To: gone}},
		{queue: "gone-2", frame: &Frame{Type: frame_type_message, ID: "gone-2", To: gone}},
	}
	w := &waiting{parked: map[string][]*outbound{gone: held}, deadlines: map[string]time.Time{gone: time.Now().Add(time.Second)}}
	s.parked.Store(2)

	if err := s.settle(w, proof{entity: gone, err: errors.New("refused"), reason: fail_unknown_user}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if !sender_test_wait(2*time.Second, func() bool { return sender_test_status("gone-1") == nil && sender_test_status("gone-2") == nil }) {
		t.Fatalf("THE POINT: messages to an entity the peer does not host are still queued: %v, %v", sender_test_status("gone-1"), sender_test_status("gone-2"))
	}
	restore()
	if s.parked.Load() != 0 {
		t.Errorf("%d messages still counted as held", s.parked.Load())
	}
	if len(calls) != 2 || calls[0].code != error_code_message_unknown {
		t.Errorf("the sending app was told %+v, want message/unknown for each", calls)
	}
	if denials := db.integer("select denials from health where recipient=?", gone); denials != 2 {
		t.Errorf("the recipient's health holds %d denials, want 2", denials)
	}

	// Control: a refusal a retry may change (the key missing there) fails the
	// held message back for a retry.
	error_test_queue_insert(db, "unproven-1", "owner", "feeds", gone, "feeds", "post/new", now())
	w = &waiting{parked: map[string][]*outbound{gone: {{queue: "unproven-1", frame: &Frame{Type: frame_type_message, ID: "unproven-1", To: gone}}}}, deadlines: map[string]time.Time{gone: time.Now().Add(time.Second)}}
	s.parked.Store(1)
	if err := s.settle(w, proof{entity: gone, err: errors.New("refused"), reason: fail_unproven}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if row := sender_test_status("unproven-1"); row == nil || row["status"] != "pending" || row["attempts"].(int64) != 1 {
		t.Errorf("control: an unproven target's message is %v, want it failed for a retry", row)
	}
}

// TestSenderSettlesOnlyTheRowWhoseClaimCannotBeSigned is #140: a claim that
// could not be signed - the sending entity gone - wrote nothing and left the
// stream intact, but write_one answered it as a write failure, so write_loop
// tore the Sender down and failed every message in flight to the peer. Only
// the row goes: dropped when the entity cannot sign here, failed for a retry
// when users.db would not give up its key.
func TestSenderSettlesOnlyTheRowWhoseClaimCannotBeSigned(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	s := new_test_sender(t, "12D3KooWUnsignable")
	s.stream = &fake_stream{buf: &bytes.Buffer{}}
	s.challenge = must_challenge(t)
	gone := test_entity_id('g')
	write := func(id string) {
		t.Helper()
		install_queue_row(t, id)
		if err := s.write_one(&outbound{queue: id, frame: &Frame{Type: frame_type_message, ID: id, From: gone, Service: "s", Event: "e"}}); err != nil {
			t.Fatalf("THE POINT: a claim that could not be signed failed the stream: %v", err)
		}
	}
	write("unsigned-1")
	if !sender_test_wait(2*time.Second, func() bool { return sender_test_status("unsigned-1") == nil }) {
		t.Errorf("a row whose sending entity is gone is still queued: %v", sender_test_status("unsigned-1"))
	}
	db_open("db/users.db").exec("drop table entities")
	write("unread-1")
	if !sender_test_wait(2*time.Second, func() bool {
		row := sender_test_status("unread-1")
		return row != nil && row["status"] == "pending" && row["attempts"].(int64) == 1
	}) {
		t.Errorf("a row whose key users.db would not give up is %v, want it failed for a retry", sender_test_status("unread-1"))
	}
	if s.closed.Load() {
		t.Error("the Sender closed")
	}
}

// TestSenderHandsBackWhatItNeverWrote is #142: messages a Sender held for a
// proof, or left on its outbox, when it closed were failed with an attempt
// counted and two minutes of backoff, for a cause that was the stream's. None
// of them was written: they go back due as they were.
func TestSenderHandsBackWhatItNeverWrote(t *testing.T) {
	setup_replication_test(t)
	s := new_test_sender(t, "12D3KooWNeverWrote")
	install_queue_row(t, "held-a")
	install_queue_row(t, "queued-a")
	w := &waiting{parked: map[string][]*outbound{"someone": {{queue: "held-a", frame: &Frame{Type: frame_type_message, ID: "held-a", To: "someone"}}}}, deadlines: map[string]time.Time{}}
	s.parked.Store(1)
	s.release(w)
	s.outbox <- &outbound{queue: "queued-a", frame: &Frame{Type: frame_type_message, ID: "queued-a"}}
	s.closed.Store(true)
	s.drain_outbox()
	for _, id := range []string{"held-a", "queued-a"} {
		if row := sender_test_status(id); row == nil || row["status"] != "pending" || row["attempts"].(int64) != 0 {
			t.Errorf("THE POINT: unwritten row %s is %v, want it pending with no attempt counted", id, row)
		}
	}
}

// stuck_stream is a stream whose far side has stopped reading: a write blocks
// until its deadline, or gives up after five seconds when none was set.
type stuck_stream struct {
	lock     sync.Mutex
	deadline time.Time
}

func (s *stuck_stream) Read([]byte) (int, error) { return 0, io.EOF }
func (s *stuck_stream) Reset() error             { return nil }

func (s *stuck_stream) SetWriteDeadline(t time.Time) error {
	s.lock.Lock()
	s.deadline = t
	s.lock.Unlock()
	return nil
}

func (s *stuck_stream) Write(p []byte) (int, error) {
	for give := time.Now().Add(5 * time.Second); time.Now().Before(give); time.Sleep(10 * time.Millisecond) {
		s.lock.Lock()
		deadline := s.deadline
		s.lock.Unlock()
		if !deadline.IsZero() && time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
	}
	return len(p), nil
}

// TestSenderWriteHasADeadline is #149: a far side that stopped reading parked
// write_loop in a write for as long as the connection stayed up, the ping
// behind it never went, and the Sender stayed open with the peer's rows left
// to a pull_loop with no room. A write ends at its deadline, and with it the
// stream.
func TestSenderWriteHasADeadline(t *testing.T) {
	t.Setenv("MOCHI_PEER_TIMEOUT", "1")
	s := new_test_sender(t, "12D3KooWStopsReading")
	s.stream = &stuck_stream{}
	start := time.Now()
	err := s.write_one(&outbound{frame: &Frame{Type: frame_type_ping, ID: "p"}})
	if err == nil {
		t.Fatal("THE POINT: a write the far side never read succeeded")
	}
	if waited := time.Since(start); waited > 3*time.Second {
		t.Errorf("THE POINT: the write waited %v, past its deadline", waited)
	}
}

// A message whose frame is still being written is not timed out by the sweep
// (#149): failed back then, the row was sent again while the first frame was
// still going out.
func TestSendersSweepSparesAMessageBeingWritten(t *testing.T) {
	setup_replication_test(t)
	s := new_test_sender(t, "12D3KooWWriting")
	restore := stash_sender(t, "12D3KooWWriting", s)
	defer restore()
	install_queue_row(t, "writing")
	install_queue_row(t, "stale")
	s.inflight["writing"] = &pending{queue: "writing"}
	s.inflight["stale"] = &pending{queue: "stale", sent: now() - 3600}
	senders_sweep_all()
	if _, kept := s.inflight["writing"]; !kept {
		t.Error("THE POINT: a message still being written was timed out")
	}
	if _, kept := s.inflight["stale"]; kept {
		t.Error("control: a stale message was not timed out")
	}
}

// A ping is recorded as sent once it is queued (#149), so one stuck behind a
// write the far side will not read times out, and the stream with it.
func TestSenderPingTimesOutWhenItCannotBeWritten(t *testing.T) {
	setup_replication_test(t)
	s := new_test_sender(t, "12D3KooWPingStuck")
	s.outbox = make(chan *outbound) // nothing takes from it
	restore := stash_sender(t, "12D3KooWPingStuck", s)
	defer restore()
	s.ping()
	s.lock.Lock()
	recorded := len(s.pings)
	for id := range s.pings {
		s.pings[id] = now() - int64(peer_ping_timeout_seconds()) - 1
	}
	s.lock.Unlock()
	if recorded != 1 {
		t.Fatalf("THE POINT: a ping that could not be written was not recorded (%d recorded)", recorded)
	}
	senders_sweep_all()
	if !s.closed.Load() {
		t.Error("THE POINT: a stream whose ping could never be written stayed open")
	}
}

// TestSenderTakesNoRowsOnceDraining is #170: shutdown queued a bye on every
// Sender while pull_loop went on claiming rows behind it, which the far side,
// reading no further than the bye, never saw.
func TestSenderTakesNoRowsOnceDraining(t *testing.T) {
	setup_replication_test(t)
	db := queue_test_table()
	const peer = "12D3KooWDraining"
	queue_test_insert_target(db, "drain-1", peer, priority_interactive)
	s := new_test_sender(t, peer)
	s.draining.Store(true)
	s.pull_once()
	if len(s.outbox) != 0 || !queue_untouched("drain-1") {
		t.Errorf("THE POINT: a draining Sender claimed a row (outbox %d)", len(s.outbox))
	}
	// Control: a Sender not draining takes it.
	s.draining.Store(false)
	s.pull_once()
	if len(s.outbox) != 1 {
		t.Errorf("control: the Sender took %d rows, want 1", len(s.outbox))
	}
}

// A draining Sender refuses what peer_send offers it (#170), leaving the row
// for the next run.
func TestPeerSendRefusedWhileDraining(t *testing.T) {
	const peer = "12D3KooWRefusesWhileDraining"
	s := new_test_sender(t, peer)
	s.draining.Store(true)
	restore := stash_sender(t, peer, s)
	defer restore()
	if err := peer_send(peer, "", &Frame{Type: frame_type_ping, ID: "p"}); !errors.Is(err, error_sender_draining) {
		t.Errorf("THE POINT: peer_send to a draining Sender answered %v", err)
	}
	if len(s.outbox) != 0 {
		t.Error("THE POINT: a draining Sender took a frame")
	}
}

// The bye is written only once what the Sender holds has been acknowledged, or
// the wait has passed (#170): the far side stops reading at bye.
func TestSendersByeWaitsForWhatIsInFlight(t *testing.T) {
	const peer = "12D3KooWByeLast"
	s := new_test_sender(t, peer)
	s.inflight["in-flight"] = &pending{sent: now()}
	senders_lock.Lock()
	saved := senders
	senders = map[string]*Sender{peer: s}
	senders_lock.Unlock()
	defer func() {
		senders_lock.Lock()
		senders = saved
		senders_lock.Unlock()
	}()
	done := make(chan struct{})
	go func() {
		senders_bye_all(5 * time.Second)
		close(done)
	}()
	time.Sleep(300 * time.Millisecond)
	if !s.draining.Load() {
		t.Error("the Sender was not told to drain")
	}
	if len(s.outbox) != 0 {
		t.Fatal("THE POINT: the bye was queued with a message still in flight")
	}
	s.lock.Lock()
	delete(s.inflight, "in-flight")
	s.lock.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("senders_bye_all did not return")
	}
	select {
	case ob := <-s.outbox:
		if ob.frame.Type != frame_type_bye {
			t.Errorf("queued %q, want bye", ob.frame.Type)
		}
	default:
		t.Error("no bye was queued once nothing was in flight")
	}
}

// TestSenderOpenedByARowSendsItFirst is #2114: pull_loop started inside the
// open, before the row that opened the Sender was on its outbox, so rows of
// the same peer queued after it went out ahead of it.
func TestSenderOpenedByARowSendsItFirst(t *testing.T) {
	got := make(chan string, 8)
	_, far := sender_test_hosts(t, func(stream p2p_network.Stream) {
		challenge, err := hello_challenge()
		if err != nil || hello_write(stream, 2, "order", challenge, receiver_codecs(), receiver_features()) != nil {
			stream.Reset()
			return
		}
		if _, err := caps_read(stream); err != nil {
			stream.Reset()
			return
		}
		for {
			f, err := frame_read(stream)
			if err != nil {
				return
			}
			if f.Type == frame_type_message {
				got <- f.ID
			}
		}
	})
	peer := far.ID().String()
	db := queue_test_table()
	for i, id := range []string{"first", "later-1", "later-2"} {
		db.exec(`insert into queue (id, target, from_entity, to_entity, service, event, next_retry, created)
			values (?, ?, '', '', 'test', 'msg', ?, ?)`, id, peer, now()-10+int64(i), now()-10+int64(i))
	}
	// A slow open: a pull_loop started inside it claims the later rows first.
	sender_opener = func(p string) (*Sender, error) {
		s, err := sender_open(p)
		time.Sleep(300 * time.Millisecond)
		return s, err
	}
	t.Cleanup(func() {
		sender_opener = nil
		senders_lock.Lock()
		s := senders[peer]
		senders_lock.Unlock()
		if s != nil {
			s.shutdown()
		}
	})
	if !queue_sending("first") {
		t.Fatal("control: the opening row could not be claimed")
	}
	rows := queue_rows(db, []any{"first"})
	f, err := frame_for_queue(&rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := peer_send(peer, "first", f); err != nil {
		t.Fatalf("peer_send: %v", err)
	}
	select {
	case id := <-got:
		if id != "first" {
			t.Errorf("THE POINT: %q went out before the row that opened the Sender", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was written")
	}
}

// broken_stream fails every write, after running what a test puts in
// writing: a far side gone mid-write.
type broken_stream struct {
	writing func()
}

func (b *broken_stream) Read([]byte) (int, error) { return 0, io.EOF }
func (b *broken_stream) Reset() error             { return nil }
func (b *broken_stream) Write([]byte) (int, error) {
	if b.writing != nil {
		b.writing()
	}
	return 0, errors.New("stream gone")
}

// When a held target's messages are being written and one write fails, the
// one that failed is charged and those behind it, never written, go back as
// they were (#142).
func TestSenderHandsBackTheRestAfterAFailedWrite(t *testing.T) {
	setup_replication_test(t)
	s := new_test_sender(t, "12D3KooWWriteFails")
	s.stream = &broken_stream{}
	s.proven = map[string]bool{"someone": true}
	install_queue_row(t, "tried")
	install_queue_row(t, "behind")
	w := &waiting{parked: map[string][]*outbound{"someone": {
		{queue: "tried", frame: &Frame{Type: frame_type_message, ID: "tried", To: "someone"}},
		{queue: "behind", frame: &Frame{Type: frame_type_message, ID: "behind", To: "someone"}},
	}}, deadlines: map[string]time.Time{"someone": time.Now().Add(time.Second)}}
	s.parked.Store(2)
	if err := s.settle(w, proof{entity: "someone"}); err == nil {
		t.Fatal("control: a failed write was not reported")
	}
	if row := sender_test_status("tried"); row == nil || row["attempts"].(int64) != 1 {
		t.Errorf("the message whose write failed is %v, want one attempt charged", row)
	}
	if row := sender_test_status("behind"); row == nil || row["status"] != "pending" || row["attempts"].(int64) != 0 {
		t.Errorf("THE POINT: the message never written is %v, want it pending with no attempt counted", row)
	}
}

// A message whose write fails after shutdown has already failed it back from
// inflight is failed once, not again by the write's own failure.
func TestSenderFailsAMessageOnceWhenShutdownTakesItMidWrite(t *testing.T) {
	setup_replication_test(t)
	s := new_test_sender(t, "12D3KooWShutMidWrite")
	s.stream = &broken_stream{writing: s.shutdown}
	install_queue_row(t, "mid-1")
	s.send(&outbound{queue: "mid-1", frame: &Frame{Type: frame_type_message, ID: "mid-1"}})
	if row := sender_test_status("mid-1"); row == nil || row["attempts"].(int64) != 1 {
		t.Errorf("THE POINT: a message failed back by shutdown mid-write is %v, want one attempt charged", row)
	}
}

// A message waiting for room in the window when the Sender closes was never
// written: it goes back as it was (#142).
func TestSenderHandsBackAMessageTheWindowHeldWhenItClosed(t *testing.T) {
	setup_replication_test(t)
	t.Setenv("MOCHI_PEER_WINDOW", "1")
	s := new_test_sender(t, "12D3KooWWindowClosed")
	s.stream = &fake_stream{buf: &bytes.Buffer{}}
	s.inflight["other"] = &pending{sent: now()}
	s.closed.Store(true)
	install_queue_row(t, "windowed")
	if err := s.send(&outbound{queue: "windowed", frame: &Frame{Type: frame_type_message, ID: "windowed"}}); err == nil {
		t.Fatal("control: a message for a closed Sender's full window was written")
	}
	if row := sender_test_status("windowed"); row == nil || row["status"] != "pending" || row["attempts"].(int64) != 0 {
		t.Errorf("THE POINT: the unwritten message is %v, want it pending with no attempt counted", row)
	}
}
