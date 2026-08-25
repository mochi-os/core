// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for protocol2_messages.go. Receivers are built with stream = nil (only
// the reply writer touches it); end-to-end stream tests live in
// protocol2_integration_test.go.

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fake_stream implements receiver_stream for tests — backs onto a
// bytes.Buffer for Read/Write and counts Reset() calls.
type fake_stream struct {
	mu          sync.Mutex
	buf         *bytes.Buffer
	reset_count *atomic.Int32
}

func (f *fake_stream) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Read(p)
}

func (f *fake_stream) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Write(p)
}

func (f *fake_stream) Reset() error {
	if f.reset_count != nil {
		f.reset_count.Add(1)
	}
	return nil
}

// --- coalesce_one: acks batch, fails flush -----------------------------

func TestCoalesceOneAcksAccumulate(t *testing.T) {
	r := &Receiver{}
	pending := make([]string, 0, 8)
	r.coalesce_one(&Frame{Type: frame_type_ack, Replies: []string{"a"}}, &pending)
	r.coalesce_one(&Frame{Type: frame_type_ack, Replies: []string{"b", "c"}}, &pending)
	r.coalesce_one(&Frame{Type: frame_type_ack, Replies: []string{"d"}}, &pending)

	if len(pending) != 4 {
		t.Fatalf("pending acks: got %d, want 4", len(pending))
	}
	want := []string{"a", "b", "c", "d"}
	for i, id := range want {
		if pending[i] != id {
			t.Errorf("pending[%d]: got %q, want %q", i, pending[i], id)
		}
	}
}

// --- write_replies drain-and-batch -------------------------------------

func TestWriteRepliesBatchesAcksInOneFrame(t *testing.T) {
	// Two acks pushed back-to-back should coalesce into one ack frame
	// on the wire (drain-and-batch). Push a handful, close the
	// channel, read the resulting frames from the buffer.
	var buf bytes.Buffer
	r := &Receiver{
		stream:  &fake_stream{buf: &buf},
		replies: make(chan *Frame, 8),
	}

	// Pre-load the channel before starting the writer so the writer's
	// non-blocking drain catches them all in one batch.
	r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"id-1"}}
	r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"id-2"}}
	r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"id-3"}}
	close(r.replies)

	r.write_replies() // returns when channel closes

	// Read frames from the buffer; should be exactly one ack with
	// Replies=[id-1, id-2, id-3].
	f, err := frame_read(&buf)
	if err != nil {
		t.Fatalf("frame_read: %v", err)
	}
	if f.Type != frame_type_ack {
		t.Errorf("Type: %q want %q", f.Type, frame_type_ack)
	}
	if len(f.Replies) != 3 {
		t.Errorf("Replies: got %d, want 3 (batched)", len(f.Replies))
	}
	// No further frames.
	if extra, err := frame_read(&buf); err == nil {
		t.Errorf("extra frame after batched ack: %+v", extra)
	}
}

func TestWriteRepliesFailFlushesAcksFirst(t *testing.T) {
	// A fail with accumulated acks ahead of it MUST flush the acks
	// before writing the fail standalone.
	var buf bytes.Buffer
	r := &Receiver{
		stream:  &fake_stream{buf: &buf},
		replies: make(chan *Frame, 8),
	}

	r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"a"}}
	r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"b"}}
	r.replies <- &Frame{Type: frame_type_fail, Replies: []string{"c"}, Reason: fail_unsupported}
	r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"d"}}
	close(r.replies)

	r.write_replies()

	// Expect: ack[a,b], fail[c], ack[d]
	first, err := frame_read(&buf)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if first.Type != frame_type_ack || len(first.Replies) != 2 {
		t.Errorf("first: got %v, want ack[a,b]", first)
	}
	second, err := frame_read(&buf)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if second.Type != frame_type_fail || second.Reason != fail_unsupported {
		t.Errorf("second: got %v, want fail/unsupported", second)
	}
	third, err := frame_read(&buf)
	if err != nil {
		t.Fatalf("third read: %v", err)
	}
	if third.Type != frame_type_ack || len(third.Replies) != 1 || third.Replies[0] != "d" {
		t.Errorf("third: got %v, want ack[d]", third)
	}
}

func TestWriteRepliesPongStandalone(t *testing.T) {
	// Pong is rare and not batchable — ship standalone.
	var buf bytes.Buffer
	r := &Receiver{
		stream:  &fake_stream{buf: &buf},
		replies: make(chan *Frame, 8),
	}
	r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"a"}}
	r.replies <- &Frame{Type: frame_type_pong, ID: "ping-1"}
	close(r.replies)

	r.write_replies()

	a, err := frame_read(&buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if a.Type != frame_type_ack {
		t.Errorf("first: %v, want ack", a)
	}
	p, err := frame_read(&buf)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if p.Type != frame_type_pong || p.ID != "ping-1" {
		t.Errorf("pong: got %v", p)
	}
}

// --- reply gating -----------------------------------------------------

func TestReplyDropsWhenClosed(t *testing.T) {
	r := &Receiver{
		replies: make(chan *Frame, 1),
	}
	r.closed.Store(true)
	r.reply(&Frame{Type: frame_type_ack, Replies: []string{"x"}})
	// Should not have been written to the channel.
	select {
	case f := <-r.replies:
		t.Errorf("reply written despite closed=true: %+v", f)
	default:
	}
}

func TestReplyDropsWhenFull(t *testing.T) {
	r := &Receiver{
		replies: make(chan *Frame, 1),
	}
	r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"first"}} // fill
	// Second reply must NOT block; the channel is full, so it's
	// dropped (debug-logged). Test passes if this returns quickly.
	done := make(chan struct{})
	go func() {
		r.reply(&Frame{Type: frame_type_ack, Replies: []string{"second"}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Error("reply() blocked when channel was full — should drop instead")
	}
}

// --- dispatch_message ---------------------------------------------------

func TestDispatchMessageUnclaimedFails(t *testing.T) {
	// Message from an unclaimed entity → fail{unclaimed} reply.
	reset_workers(t)
	defer reset_workers(t)

	r := &Receiver{
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	r.dispatch_message(&Frame{
		Type:    frame_type_message,
		ID:      "msg-1",
		From:    test_entity_id('u'),
		Service: "svc",
	})
	select {
	case got := <-r.replies:
		if got.Type != frame_type_fail || got.Reason != fail_unclaimed {
			t.Errorf("reply: got %+v, want fail/unclaimed", got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("no reply within timeout")
	}
}

func TestDispatchMessageAnonymousAllowed(t *testing.T) {
	// Plan: message gating is `claimed[From]` AND `From != ""`. An
	// anonymous frame (From="") bypasses the gate — it dispatches to
	// the worker without a claim check, as anonymous events like
	// directory_publish require.
	reset_workers(t)
	defer reset_workers(t)

	r := &Receiver{
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	r.dispatch_message(&Frame{
		Type:    frame_type_message,
		ID:      "msg-anon",
		From:    "", // anonymous
		Service: "no-such-service",
	})
	// Worker will run, fail with unsupported. Wait for reply.
	deadline := time.After(time.Second)
	for {
		select {
		case got := <-r.replies:
			if got.Reason == fail_unclaimed {
				t.Errorf("anonymous frame got fail{unclaimed}: %+v", got)
			}
			return
		case <-deadline:
			t.Fatal("no reply within timeout")
		}
	}
}

func TestDispatchMessageDedupsViaMessageSeen(t *testing.T) {
	// A duplicate ID arriving twice should ack immediately on the
	// second call without dispatching to the worker.
	reset_workers(t)
	defer reset_workers(t)

	// Mark the ID as seen first.
	const id = "dup-msg-id-test"
	seen_messages_lock.Lock()
	seen_messages[id] = now()
	seen_messages_lock.Unlock()
	defer func() {
		seen_messages_lock.Lock()
		delete(seen_messages, id)
		seen_messages_lock.Unlock()
	}()

	r := &Receiver{
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	r.dispatch_message(&Frame{
		Type:    frame_type_message,
		ID:      id,
		From:    test_entity_id('a'),
		Service: "any-service",
	})

	// Expect an immediate ack (not a fail/unclaimed) and no worker
	// dispatch.
	select {
	case got := <-r.replies:
		if got.Type != frame_type_ack {
			t.Errorf("dedup reply: got %+v, want ack", got)
		}
		if len(got.Replies) != 1 || got.Replies[0] != id {
			t.Errorf("dedup ack Replies: got %v, want [%s]", got.Replies, id)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("no ack reply for duplicate id within timeout")
	}

	// Worker count should still be 0 — dispatch was short-circuited.
	workers, _ := worker_count()
	if workers != 0 {
		t.Errorf("dedup'd frame still created a worker (count=%d)", workers)
	}
}

// --- handle() frame-type dispatch -------------------------------------

func TestHandleBye(t *testing.T) {
	r := &Receiver{
		stream:  &fake_stream{buf: &bytes.Buffer{}, reset_count: new(atomic.Int32)},
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	if r.handle(&Frame{Type: frame_type_bye}) {
		t.Error("handle(bye) returned true — should terminate the read loop")
	}
}

func TestHandlePingEchoesPong(t *testing.T) {
	r := &Receiver{
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	if !r.handle(&Frame{Type: frame_type_ping, ID: "p-1"}) {
		t.Error("handle(ping) returned false — should keep reading")
	}
	select {
	case got := <-r.replies:
		if got.Type != frame_type_pong || got.ID != "p-1" {
			t.Errorf("ping echo: got %+v, want pong id=p-1", got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("no pong echo within timeout")
	}
}

func TestHandleSecondCapsClosesStream(t *testing.T) {
	// Receiver's read_loop sets caps_seen=true after the first caps;
	// any subsequent caps frame hits handle() and must close the
	// stream. We check by observing the synthetic Reset() count.
	reset_count := new(atomic.Int32)
	r := &Receiver{
		stream:  &fake_stream{buf: &bytes.Buffer{}, reset_count: reset_count},
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	r.caps_seen.Store(true) // simulate first caps already done
	if r.handle(&Frame{Type: frame_type_caps}) {
		t.Error("handle(caps) on caps-already-seen returned true; should close stream")
	}
	if reset_count.Load() != 1 {
		t.Errorf("expected stream.Reset() called once, got %d", reset_count.Load())
	}
}

func TestHandleHelloOnMessagesStreamClosed(t *testing.T) {
	// hello after handshake is a protocol violation.
	reset_count := new(atomic.Int32)
	r := &Receiver{
		stream:  &fake_stream{buf: &bytes.Buffer{}, reset_count: reset_count},
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	r.caps_seen.Store(true)
	if r.handle(&Frame{Type: frame_type_hello}) {
		t.Error("handle(hello) on messages stream returned true")
	}
	if reset_count.Load() != 1 {
		t.Errorf("expected stream.Reset, got %d", reset_count.Load())
	}
}

// TestCoalesceOneShipsClaimFrames: a responder proof travels as a claim frame,
// and a frame type absent from write_replies' switch is discarded silently.
func TestCoalesceOneShipsClaimFrames(t *testing.T) {
	stream, peer_side := new_stream_pair()
	r := &Receiver{peer: "12D3KooWCoalesceTest", stream: stream}

	// The pipe is unbuffered, so the reader has to be waiting before the
	// write: coalesce_one blocks until someone takes the bytes.
	done := make(chan *Frame, 1)
	go func() {
		f, err := frame_read(peer_side)
		if err != nil {
			done <- nil
			return
		}
		done <- f
	}()

	acks := make([]string, 0)
	go func() {
		r.coalesce_one(&Frame{Type: frame_type_claim, From: test_entity_id('c'), Signature: []byte("sig")}, &acks)
	}()

	select {
	case f := <-done:
		if f == nil {
			t.Fatal("claim frame was not written to the stream")
		}
		if f.Type != frame_type_claim {
			t.Errorf("wrote type %q, want claim", f.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("claim frame was dropped rather than written")
	}
}

// --- budgets on the messages receiver ----------------------------------
//
// /mochi/2/stream bounds its pre-open phase on count and time; /mochi/2/messages
// did neither, and answered a claim that failed to verify by keeping the stream
// open. Each of these frames costs this host real work before the peer has sent
// anything: a verify per claim, a lookup and a signature per prove.

// deadline_stream is fake_stream plus the SetReadDeadline a real libp2p stream
// carries, so the receiver's optional assertion finds it.
type deadline_stream struct {
	fake_stream
	deadline atomic.Int64 // unix nanos, 0 when cleared
	sets     atomic.Int32
}

func (d *deadline_stream) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		d.deadline.Store(0)
	} else {
		d.deadline.Store(t.UnixNano())
	}
	d.sets.Add(1)
	return nil
}

// messages_net_id gives claim_signable and claim_verify a receiver to bind to;
// a bare unit test leaves net_id empty, which fails the verify for the wrong
// reason and would make a cap test pass without a cap.
func messages_net_id(t *testing.T) {
	t.Helper()
	previous := net_id
	net_id = "self"
	t.Cleanup(func() { net_id = previous })
}

// messages_claim mints a claim frame that really verifies against challenge on
// the messages protocol, so the handler does the full ed25519 work per frame and
// only a count cap can stop it.
func messages_claim(t *testing.T, challenge []byte) *Frame {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	entity := base58_encode(public)
	signable, err := claim_signable(challenge, entity, net_id, protocol_messages)
	if err != nil {
		t.Fatalf("claim_signable: %v", err)
	}
	return &Frame{Type: frame_type_claim, From: entity, Signature: ed25519.Sign(private, signable)}
}

func messages_receiver(reset *atomic.Int32) *Receiver {
	return &Receiver{
		stream:    &fake_stream{buf: &bytes.Buffer{}, reset_count: reset},
		replies:   make(chan *Frame, 1024),
		claimed:   map[string]bool{},
		challenge: []byte("0123456789abcdef0123456789abcdef"),
	}
}

// A claim that does not verify is not a retry, it is a peer spending our
// signature checks on nothing. The stream path resets; this one used to log and
// carry on, so the cost of a bad claim was zero to the sender.
func TestMessagesFailedClaimResetsStream(t *testing.T) {
	reset := new(atomic.Int32)
	r := messages_receiver(reset)
	r.caps_seen.Store(true)

	// From "" is a valid envelope and an invalid claim: claim_verify rejects an
	// empty entity, so this reaches the failure branch without needing a key.
	if r.handle(&Frame{Type: frame_type_claim, From: ""}) {
		t.Error("handle(bad claim) returned true; the stream stays open and the next bad claim costs another verify")
	}
	if reset.Load() != 1 {
		t.Errorf("expected the stream reset once, got %d", reset.Load())
	}
}

func TestMessagesCapsClaims(t *testing.T) {
	messages_net_id(t)
	reset := new(atomic.Int32)
	r := messages_receiver(reset)
	r.caps_seen.Store(true)

	for i := 0; i < messages_claims_maximum; i++ {
		if !r.handle(messages_claim(t, r.challenge)) {
			t.Fatalf("claim %d of %d was refused; the cap is too tight for a legitimate sender", i+1, messages_claims_maximum)
		}
	}
	if reset.Load() != 0 {
		t.Fatalf("reset before the cap was reached (%d)", reset.Load())
	}
	if r.handle(messages_claim(t, r.challenge)) {
		t.Errorf("claim %d was accepted; verifiable claims are unbounded", messages_claims_maximum+1)
	}
	if reset.Load() != 1 {
		t.Errorf("expected the stream reset once past the cap, got %d", reset.Load())
	}
}

// The cap above bounds claims the peer has not backed with traffic. A sender
// must claim once per entity it speaks for, so a peer hosting more entities
// than the cap legitimately sends more than that many claims over a session -
// counting the lifetime total cuts its delivery off part-way through, which is
// not a limit this host gets to impose on how many users the far side has.
func TestMessagesClaimBudgetReturnsOnDeliveredMessages(t *testing.T) {
	messages_net_id(t)
	// dispatch_message ends in worker_dispatch, and a worker outlives the test
	// unless its inbox is closed.
	reset_workers(t)
	defer reset_workers(t)

	reset := new(atomic.Int32)
	r := messages_receiver(reset)
	r.caps_seen.Store(true)

	// Well past the cap, each claim backed by the message it exists to carry.
	for i := 0; i < messages_claims_maximum*3; i++ {
		claim := messages_claim(t, r.challenge)
		if !r.handle(claim) {
			t.Fatalf("claim %d was refused after %d delivered messages; a peer speaking for more entities than the cap loses delivery mid-session", i+1, i)
		}
		if !r.handle(&Frame{Type: frame_type_message, From: claim.From}) {
			t.Fatalf("the message backing claim %d closed the stream", i+1)
		}
	}
	if reset.Load() != 0 {
		t.Errorf("stream reset %d times; a peer backing every claim with a message must never be cut off", reset.Load())
	}
}

func TestMessagesCapsProves(t *testing.T) {
	reset := new(atomic.Int32)
	r := messages_receiver(reset)
	r.caps_seen.Store(true)

	// To "" takes prove's cheap refusal branch, so this measures the budget
	// rather than the signing path it guards.
	for i := 0; i < messages_proves_maximum; i++ {
		if !r.handle(&Frame{Type: frame_type_prove}) {
			t.Fatalf("prove %d of %d was refused", i+1, messages_proves_maximum)
		}
	}
	if r.handle(&Frame{Type: frame_type_prove}) {
		t.Errorf("prove %d was accepted; an unauthenticated peer signs without limit", messages_proves_maximum+1)
	}
	if reset.Load() != 1 {
		t.Errorf("expected the stream reset once past the cap, got %d", reset.Load())
	}
}

// The read deadline bounds the phase before the first message and is cleared
// once one arrives, because a messages stream carrying traffic is long-lived.
func TestMessagesDeadlineClearedOnFirstMessage(t *testing.T) {
	// handle() dispatches the message to an app worker, and a worker lives until
	// the reaper closes its inbox 300s later. Without the teardown it outlives
	// this test and races later tests on the globals a worker reads.
	reset_workers(t)
	defer reset_workers(t)

	d := &deadline_stream{fake_stream: fake_stream{buf: &bytes.Buffer{}, reset_count: new(atomic.Int32)}}
	r := &Receiver{stream: d, replies: make(chan *Frame, 8), claimed: map[string]bool{}}
	r.caps_seen.Store(true)

	r.deadline(time.Now().Add(messages_ready_timeout))
	if d.deadline.Load() == 0 {
		t.Fatal("no read deadline set: a peer can hold the stream open indefinitely before sending anything")
	}

	r.handle(&Frame{Type: frame_type_message, From: "", Service: "test", Event: "test", ID: "1"})
	if d.deadline.Load() != 0 {
		t.Error("the deadline survived the first message; a legitimate long-lived stream would be cut")
	}
	if !r.ready {
		t.Error("receiver did not record that a message arrived")
	}
}
