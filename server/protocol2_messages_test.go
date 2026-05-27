// Tests for protocol2_messages.go — Receiver drain-and-batch acks,
// dispatch gating, ping/pong/bye behavior.
//
// Phase 3d per claude/plans/protocol2.md → Testing strategy.
//
// We avoid spinning up a real libp2p stream by constructing a Receiver
// with `stream = nil` (only the reply writer touches it; tests that
// exercise the writer install a `fake_writer` instead). End-to-end
// stream tests live in protocol2_integration_test.go.

package main

import (
	"bytes"
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
	// the worker without a claim check (matches /mochi/1 behaviour
	// for anonymous events like directory_publish).
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
	message_mark_seen(id)
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
	resetCount := new(atomic.Int32)
	r := &Receiver{
		stream:  &fake_stream{buf: &bytes.Buffer{}, reset_count: resetCount},
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	r.caps_seen.Store(true) // simulate first caps already done
	if r.handle(&Frame{Type: frame_type_caps}) {
		t.Error("handle(caps) on caps-already-seen returned true; should close stream")
	}
	if resetCount.Load() != 1 {
		t.Errorf("expected stream.Reset() called once, got %d", resetCount.Load())
	}
}

func TestHandleHelloOnMessagesStreamClosed(t *testing.T) {
	// hello after handshake is a protocol violation.
	resetCount := new(atomic.Int32)
	r := &Receiver{
		stream:  &fake_stream{buf: &bytes.Buffer{}, reset_count: resetCount},
		replies: make(chan *Frame, 4),
		claimed: map[string]bool{},
	}
	r.caps_seen.Store(true)
	if r.handle(&Frame{Type: frame_type_hello}) {
		t.Error("handle(hello) on messages stream returned true")
	}
	if resetCount.Load() != 1 {
		t.Errorf("expected stream.Reset, got %d", resetCount.Load())
	}
}
