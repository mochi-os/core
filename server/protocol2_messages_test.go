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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
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
	dedup_isolate(t)
	message_seen_mark(id, "earlier")

	from := test_entity_id('a')
	r := &Receiver{
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{from: true},
	}
	r.dispatch_message(&Frame{
		Type:    frame_type_message,
		ID:      id,
		From:    from,
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

// A frame from no authenticated sender takes no dedup record: its id may be
// anyone's, and a record for it would let a peer with no credentials pre-empt
// an honest sender's message or flush the records honest senders rely on.
func TestDispatchMessageMarksNothingForAnUnsignedFrame(t *testing.T) {
	setup_replication_test(t)
	reset_workers(t)
	defer reset_workers(t)
	dedup_isolate(t)
	delivered := make(chan string, 4)
	test_service(t, "unsigned", "arrive", func(e *Event) { delivered <- e.message })

	r := &Receiver{peer: "12D3KooWUnsigned", replies: make(chan *Frame, 4), claimed: map[string]bool{}}
	for range 2 {
		r.dispatch_message(&Frame{Type: frame_type_message, ID: "unsigned-1", Service: "unsigned", Event: "arrive"})
	}
	for range 2 {
		select {
		case <-delivered:
		case <-time.After(2 * time.Second):
			t.Fatal("an unsigned frame was not delivered; with no record its copy runs as well")
		}
	}
	if dedup_held("unsigned-1") {
		t.Error("THE POINT: an unsigned frame took a dedup record")
	}
}

// A copy that arrives while the first is still with its handler waits for the
// first's outcome. Acknowledged at once, as it was, the sender deleted its row,
// and when the first then failed for a reason it retries the message was lost.
func TestDispatchMessageCopyWaitsWhileTheFirstRuns(t *testing.T) {
	setup_replication_test(t)
	reset_workers(t)
	defer reset_workers(t)
	dedup_isolate(t)
	gate := make(chan struct{})
	// Released before reset_workers waits on the worker, however the test ends.
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	started := make(chan struct{}, 4)
	test_service(t, "copies", "arrive", func(e *Event) {
		started <- struct{}{}
		<-gate
		panic("the first copy fails")
	})

	from := test_entity_id('c')
	r := &Receiver{peer: "12D3KooWCopies", replies: make(chan *Frame, 8), claimed: map[string]bool{from: true}}
	frame := func() *Frame {
		return &Frame{Type: frame_type_message, ID: "copy-1", From: from, Service: "copies", Event: "arrive"}
	}
	r.dispatch_message(frame())
	<-started
	r.dispatch_message(frame())
	select {
	case f := <-r.replies:
		t.Fatalf("THE POINT: the copy was answered %s %v while the first still ran", f.Type, f.Replies)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	for i := 0; i < 2; i++ {
		select {
		case f := <-r.replies:
			if f.Type != frame_type_fail || f.Reason != fail_handler_panic {
				t.Errorf("reply %d was %s %q, want the first's retryable failure", i, f.Type, f.Reason)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("reply %d never came", i)
		}
	}
	// The retry the failure asks for runs.
	r.dispatch_message(frame())
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Error("the retry after a retryable failure did not run")
	}
}

// A prove comes back to the budget only for the first message to an entity
// this stream proved: any message used to return one, so a peer alternating a
// prove with an empty message had this host sign with its users' keys at wire
// speed.
func TestMessagesProveBudgetReturnsOnlyForAProvenTarget(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	messages_net_id(t)
	reset_workers(t)
	defer reset_workers(t)
	proved, _ := new_entity_keys(t)

	reset := new(atomic.Int32)
	r := messages_receiver(reset)
	r.peer = "12D3KooWProves"
	r.offered = must_challenge(t)
	r.caps_seen.Store(true)

	r.handle(&Frame{Type: frame_type_prove, To: proved})
	for i := 1; i < messages_proves_maximum; i++ {
		r.handle(&Frame{Type: frame_type_prove})
	}
	// Junk to no proven entity backs nothing; the first message to the proven
	// one backs its proof, and a second to it nothing more.
	r.handle(&Frame{Type: frame_type_message, Service: "no-such-service"})
	r.handle(&Frame{Type: frame_type_message, To: proved, Service: "no-such-service"})
	if !r.handle(&Frame{Type: frame_type_prove}) {
		t.Fatal("control: the first message to the proven entity did not back its proof")
	}
	r.handle(&Frame{Type: frame_type_message, To: proved, Service: "no-such-service"})
	if r.handle(&Frame{Type: frame_type_prove}) {
		t.Error("THE POINT: a second message to an entity already backed returned another prove")
	}
	if reset.Load() != 1 {
		t.Errorf("stream reset %d times, want once past the budget", reset.Load())
	}
}

// A peer needs one messages stream; past receiver_streams_maximum its oldest is
// reset, so a handful of identities cannot hold every slot of the protocol.
func TestReceiversKeepAFewStreamsPerPeer(t *testing.T) {
	receivers_lock.Lock()
	saved := receivers
	receivers = map[string][]*Receiver{}
	receivers_lock.Unlock()
	t.Cleanup(func() {
		receivers_lock.Lock()
		receivers = saved
		receivers_lock.Unlock()
	})

	open := func(peer string) (*Receiver, *atomic.Int32) {
		reset := new(atomic.Int32)
		r := &Receiver{peer: peer, stream: &fake_stream{buf: &bytes.Buffer{}, reset_count: reset}}
		receivers_add(r)
		return r, reset
	}
	first, first_reset := open("12D3KooWMany")
	second, second_reset := open("12D3KooWMany")
	_, other_reset := open("12D3KooWOther")
	if first_reset.Load()+second_reset.Load()+other_reset.Load() != 0 {
		t.Fatal("control: streams within the limit were reset")
	}
	third, _ := open("12D3KooWMany")
	if first_reset.Load() != 1 || second_reset.Load() != 0 {
		t.Errorf("THE POINT: past the limit the oldest was reset %d times and the next %d", first_reset.Load(), second_reset.Load())
	}
	if other_reset.Load() != 0 {
		t.Error("another peer's stream was reset")
	}
	receivers_remove(first)
	receivers_remove(second)
	receivers_remove(third)
	receivers_lock.Lock()
	left := len(receivers["12D3KooWMany"])
	receivers_lock.Unlock()
	if left != 0 {
		t.Errorf("%d streams recorded after all ended", left)
	}
}

// A sender says bye to collect the replies to what it has sent. The receiver
// stopped at bye and closed at once, so the acks of messages still with their
// workers were dropped and the sender's drain ran out.
func TestReceiverRepliesBeforeClosingOnBye(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	gate := make(chan struct{})
	test_service(t, "farewell", "arrive", func(*Event) { <-gate })

	s := new_claims_stream_on("12D3KooWBye-1")
	// A write that takes a while, so the stream would close under the writer
	// sending the last ack if the receiver did not wait for it.
	s.delay = 100 * time.Millisecond
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_messages_guarded(s)
	}()
	claims_hello(t, s)
	claims_caps(t, s)
	claims_send(t, s, &Frame{Type: frame_type_message, ID: "farewell-1", Service: "farewell", Event: "arrive"})
	claims_send(t, s, &Frame{Type: frame_type_bye})

	time.Sleep(200 * time.Millisecond)
	close(gate)
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("the receiver did not end after bye")
	}
	close(s.outbound.chunks)
	acked := false
	for {
		f, err := frame_read(&opener_reader{f: s.outbound})
		if err != nil {
			break
		}
		if f.Type == frame_type_ack && len(f.Replies) == 1 && f.Replies[0] == "farewell-1" {
			acked = true
		}
	}
	if !acked {
		t.Error("THE POINT: the ack of a message still with its worker at bye was not written")
	}
}

// Replies queued when the stream ends are written, not left in the channel:
// write_replies chose at random between a queued reply and the closed `done`.
func TestWriteRepliesDrainsOnDone(t *testing.T) {
	for round := 0; round < 32; round++ {
		var buf bytes.Buffer
		r := &Receiver{
			stream:  &fake_stream{buf: &buf},
			replies: make(chan *Frame, 8),
			done:    make(chan struct{}),
			written: make(chan struct{}),
		}
		r.replies <- &Frame{Type: frame_type_ack, Replies: []string{"queued"}}
		close(r.done)
		r.write_replies()
		select {
		case <-r.written:
		default:
			t.Fatal("write_replies did not say it had finished")
		}
		f, err := frame_read(&buf)
		if err != nil || f.Type != frame_type_ack || len(f.Replies) != 1 {
			t.Fatalf("THE POINT: round %d: the reply queued at the end was not written (%v, %v)", round, f, err)
		}
	}
}

// An ack batch is split well inside the size the far side's frame_read holds
// every frame but a message to.
func TestWriteRepliesSplitsLargeAckBatches(t *testing.T) {
	var buf bytes.Buffer
	r := &Receiver{stream: &fake_stream{buf: &buf}, replies: make(chan *Frame, 2048)}
	for i := 0; i < 2000; i++ {
		r.replies <- &Frame{Type: frame_type_ack, Replies: []string{strings.Repeat("i", id_length_maximum-4) + i64toa(int64(1000+i))}}
	}
	close(r.replies)
	r.write_replies()
	total := 0
	for {
		f, err := frame_read(&buf)
		if err != nil {
			break
		}
		total += len(f.Replies)
	}
	if total != 2000 {
		t.Errorf("THE POINT: %d of 2000 acks read back; a batch past the control size is refused by the far side", total)
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
	test_service(t, "claimbudget", "arrive", func(*Event) {})

	reset := new(atomic.Int32)
	r := messages_receiver(reset)
	r.caps_seen.Store(true)

	// Well past the cap, each claim backed by the message it exists to carry.
	for i := 0; i < messages_claims_maximum*3; i++ {
		claim := messages_claim(t, r.challenge)
		if !r.handle(claim) {
			t.Fatalf("claim %d was refused after %d delivered messages; a peer speaking for more entities than the cap loses delivery mid-session", i+1, i)
		}
		if !r.handle(&Frame{Type: frame_type_message, From: claim.From, Service: "claimbudget", Event: "arrive"}) {
			t.Fatalf("the message backing claim %d closed the stream", i+1)
		}
	}
	if reset.Load() != 0 {
		t.Errorf("stream reset %d times; a peer backing every claim with a message must never be cut off", reset.Load())
	}
}

// A message no handler takes returns no claim: a peer alternating a claim for
// a freshly minted entity with junk from it grew `claimed` by one entity a pair
// for the life of the stream.
func TestMessagesClaimBudgetIgnoresUndeliveredMessages(t *testing.T) {
	messages_net_id(t)
	reset_workers(t)
	defer reset_workers(t)

	reset := new(atomic.Int32)
	r := messages_receiver(reset)
	r.caps_seen.Store(true)

	refused := 0
	for i := 0; i <= messages_claims_maximum; i++ {
		claim := messages_claim(t, r.challenge)
		if !r.handle(claim) {
			break
		}
		r.handle(&Frame{Type: frame_type_message, ID: uid(), From: claim.From, Service: "no-such-service", Event: "junk"})
		refused++
	}
	if reset.Load() != 1 {
		t.Errorf("stream reset %d times after %d claims each backed only by junk; want it cut off at the cap of %d", reset.Load(), refused, messages_claims_maximum)
	}
	if workers, _ := worker_count(); workers != 0 {
		t.Errorf("junk messages created %d workers", workers)
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
func TestMessagesDeadlineRollsOnFirstMessage(t *testing.T) {
	// handle() dispatches the message to an app worker, and a worker lives until
	// the reaper retires it 300s later. Without the teardown it outlives
	// this test and races later tests on the globals a worker reads.
	reset_workers(t)
	defer reset_workers(t)

	d := &deadline_stream{fake_stream: fake_stream{buf: &bytes.Buffer{}, reset_count: new(atomic.Int32)}}
	r := &Receiver{stream: d, replies: make(chan *Frame, 8), claimed: map[string]bool{}}
	r.caps_seen.Store(true)

	r.deadline(time.Now().Add(messages_ready_timeout))
	before := d.deadline.Load()
	if before == 0 {
		t.Fatal("no read deadline set: a peer can hold the stream open indefinitely before sending anything")
	}

	r.handle(&Frame{Type: frame_type_message, From: "", Service: "test", Event: "test", ID: "1"})

	// The first message used to clear the deadline outright, which bought the
	// peer the stream - and its two goroutines and replies channel - for the
	// life of the connection. It now rolls to an idle window instead.
	after := d.deadline.Load()
	if after == 0 {
		t.Error("the first message cleared the read deadline: an established stream is then unbounded, and a silent peer holds it open indefinitely")
	}
	if after == before {
		t.Error("the deadline did not move on the first message, so it is still the pre-message window rather than an idle bound")
	}
	if after <= time.Now().UnixNano() {
		t.Error("the rolled deadline is already in the past, so a healthy stream would trip it immediately")
	}
	if !r.ready {
		t.Error("receiver did not record that a message arrived")
	}
}

// TestReceiverRegistersAPeerOnlyOnEvidence is #154 and the receiving half of
// #135. The messages handler registered the remote peer and its address on
// every exit after its hello, whatever the peer sent, so any libp2p node joined
// the registry - and with it the relay's admission list - by opening one stream
// and closing it. A completed hello now records the greeting the relay admits
// on; the registry entry waits for a claim that verifies or a message a handler
// takes.
func TestReceiverRegistersAPeerOnlyOnEvidence(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_users_test_schema()
	relay_test_greetings(t)
	messages_net_id(t)
	peer := p2p_peer.ID("12D3KooWClaimsFloodTestPeer").String()

	run := func(connection string, frames func(challenge []byte, s *claims_stream)) {
		s := new_claims_stream_on(connection)
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			receive_messages_guarded(s)
		}()
		challenge := claims_hello(t, s)
		claims_caps(t, s)
		frames(challenge, s)
		close(s.inbound.chunks)
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Fatal("the receiver did not end")
		}
	}

	// Hello and caps, then nothing: a greeting, no registry entry.
	run("12D3KooWEvidence-1", func([]byte, *claims_stream) {})
	if !peer_greeted(peer) {
		t.Error("a completed hello was not recorded as a greeting")
	}
	if peer_held(peer) {
		t.Error("THE POINT: a peer that sent nothing after its hello was registered")
	}

	// A claim that verifies: registered at the address the connection came from.
	run("12D3KooWEvidence-2", func(challenge []byte, s *claims_stream) {
		claims_send(t, s, messages_claim(t, challenge))
	})
	addresses := peer_stored_addresses(peer)
	if len(addresses) != 1 || addresses[0] != "/ip4/192.0.2.1/tcp/1443/p2p/"+peer {
		t.Errorf("a peer that proved a claim was registered at %v", addresses)
	}
}
