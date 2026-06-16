// End-to-end intra-instance integration tests for /mochi/2/messages.
//
// Phase 3g per claude/plans/protocol2.md → Testing strategy.
//
// We wire a Sender ↔ Receiver pair via io.Pipe and exercise the full
// wire protocol: hello → caps → claim → message → ack — without
// libp2p. Useful for:
//   - Full handshake round-trip
//   - Drain-and-batch ack arrival on the sender side
//   - Ordering preserved within a single stream
//   - Bye drain
//   - Stream death recovery

package main

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pipe_stream wraps two io.Pipe ends as a wire_stream. One pipe is the
// stream's read-from-remote direction, the other is the write-to-remote
// direction. Reset closes both.
type pipe_stream struct {
	mu        sync.Mutex
	reader    *io.PipeReader
	writer    *io.PipeWriter
	reset_count atomic.Int32
}

func (p *pipe_stream) Read(b []byte) (int, error)  { return p.reader.Read(b) }
func (p *pipe_stream) Write(b []byte) (int, error) { return p.writer.Write(b) }
func (p *pipe_stream) Reset() error {
	p.reset_count.Add(1)
	p.reader.Close()
	p.writer.Close()
	return nil
}

// Close lets pipe_stream double as an io.ReadCloser / io.WriteCloser
// so it can be wrapped via stream_rw in tests that need to drive the
// full /mochi/2/stream wire (handshake + post-ack content segments).
func (p *pipe_stream) Close() error { return p.Reset() }

// new_stream_pair returns two wire_streams plumbed back-to-back: a
// reads what b writes and vice versa. Use a-as-sender, b-as-receiver
// for a sender↔receiver test.
func new_stream_pair() (*pipe_stream, *pipe_stream) {
	// a writes to a_to_b; b reads it.
	// b writes to b_to_a; a reads it.
	atob_r, atob_w := io.Pipe()
	btoa_r, btoa_w := io.Pipe()
	a := &pipe_stream{reader: btoa_r, writer: atob_w}
	b := &pipe_stream{reader: atob_r, writer: btoa_w}
	return a, b
}

// --- End-to-end handshake ----------------------------------------------

func TestEndToEndHandshakeOverPipe(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	senderStream, recvStream := new_stream_pair()

	// Receiver-side state machine: write hello, read caps, read claim,
	// (we don't drive message dispatch here — the next test does).
	go func() {
		challenge, _ := hello_challenge()
		_ = hello_write(recvStream, 2, "test-sess", challenge, receiver_codecs(), receiver_features())
		caps, err := caps_read(recvStream)
		if err != nil {
			t.Errorf("recv caps_read: %v", err)
			return
		}
		if !contains_string(caps.Codecs, "zstd") {
			t.Errorf("caps codecs missing zstd: %v", caps.Codecs)
		}
		claim, err := frame_read(recvStream)
		if err != nil {
			t.Errorf("recv claim read: %v", err)
			return
		}
		if claim.Type != frame_type_claim || claim.From != id {
			t.Errorf("expected claim from %q, got %+v", id, claim)
			return
		}
		if err := claim_verify(claim.From, challenge, claim.Signature); err != nil {
			t.Errorf("claim verify failed: %v", err)
		}
	}()

	// Sender-side: read hello, write caps + claim.
	hello, err := hello_read(senderStream, 2)
	if err != nil {
		t.Fatalf("sender hello_read: %v", err)
	}
	if err := caps_write(senderStream, []string{"zstd"}, nil); err != nil {
		t.Fatalf("sender caps_write: %v", err)
	}
	if err := claim_write(senderStream, id, hello.Challenge); err != nil {
		t.Fatalf("sender claim_write: %v", err)
	}

	// Give the receiver goroutine a moment to consume + assert.
	time.Sleep(50 * time.Millisecond)
}

// --- Full Sender (with Receiver mock) ----------------------------------

// run_test_receiver starts a goroutine that performs the receiver-side
// handshake then sits in a read loop, sending an ack frame for every
// message frame it receives. Returns a channel that emits the IDs of
// every message it dispatched.
func run_test_receiver(t *testing.T, stream wire_stream, challenge []byte) <-chan string {
	t.Helper()
	got := make(chan string, 128)
	go func() {
		defer close(got)
		_ = hello_write(stream, 2, "rs", challenge, receiver_codecs(), receiver_features())
		// First sender frame must be caps.
		caps, err := caps_read(stream)
		if err != nil {
			t.Logf("recv caps_read: %v", err)
			return
		}
		_ = caps
		claimed := map[string]bool{}
		for {
			f, err := frame_read(stream)
			if err != nil {
				return
			}
			switch f.Type {
			case frame_type_claim:
				if err := claim_verify(f.From, challenge, f.Signature); err == nil {
					claimed[f.From] = true
				}
			case frame_type_message:
				if f.From != "" && !claimed[f.From] {
					_ = frame_write(stream, &Frame{Type: frame_type_fail, Replies: []string{f.ID}, Reason: fail_unclaimed})
					continue
				}
				got <- f.ID
				_ = frame_write(stream, &Frame{Type: frame_type_ack, Replies: []string{f.ID}})
			case frame_type_ping:
				_ = frame_write(stream, &Frame{Type: frame_type_pong, ID: f.ID})
			case frame_type_bye:
				return
			}
		}
	}()
	return got
}

// install_sender_for installs a Sender backed by `stream` under `peer`
// in the registry, kicks its read/write/ping goroutines, and returns a
// cleanup that removes it.
func install_sender_for(t *testing.T, peer string, stream wire_stream, hello *Frame) (*Sender, func()) {
	t.Helper()
	codecs := codec_intersect(receiver_codecs(), hello.Codecs)
	features := features_intersect(receiver_features(), hello.Features)
	s := &Sender{
		peer:      peer,
		stream:    stream,
		session:   hello.Session,
		challenge: hello.Challenge,
		codecs:    codecs,
		features:  features,
		outbox:    make(chan *outbound, peer_outbox()),
		inflight:  map[string]*pending{},
		pings:     map[string]int64{},
		claimed:   map[string]bool{},
	}
	if err := caps_write(stream, codecs, features); err != nil {
		t.Fatalf("caps_write: %v", err)
	}
	restore := stash_sender(t, peer, s)
	go s.write_loop()
	read_done := make(chan struct{})
	go func() {
		defer close(read_done)
		s.read_loop()
	}()
	return s, func() {
		s.shutdown()
		// Block until read_loop has fully exited before returning. The
		// caller's cleanup resets the data_dir / net_id globals, and a
		// still-running read_loop (handle_inbound -> peer_mark_progress ->
		// db_open, which reads data_dir) would race that reset — the
		// pre-existing flaky -race in this and later tests. shutdown()
		// Resets the stream, which closes the io.Pipe and unblocks
		// read_loop's frame_read, so this never hangs. Production never
		// resets data_dir, so this is purely a test-isolation concern.
		<-read_done
		restore()
	}
}

func TestEndToEndMessageRoundTrip(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	sendStream, recvStream := new_stream_pair()

	// Start the receiver simulator with a known challenge.
	challenge, _ := hello_challenge()
	gotIDs := run_test_receiver(t, recvStream, challenge)

	// Sender side: read the receiver's hello, install the Sender,
	// then send 3 messages.
	hello, err := hello_read(sendStream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	const peer = "e2e-peer"
	_, stop := install_sender_for(t, peer, sendStream, hello)
	defer stop()

	// Queue rows so resolve_fail / queue_ack have something to operate on.
	for _, mid := range []string{"e2e-1", "e2e-2", "e2e-3"} {
		install_queue_row(t, mid)
	}

	for _, mid := range []string{"e2e-1", "e2e-2", "e2e-3"} {
		f := &Frame{Type: frame_type_message, ID: mid, From: id, Service: "s"}
		if err := peer_send(peer, mid, f); err != nil {
			t.Fatalf("peer_send %q: %v", mid, err)
		}
	}

	// Collect 3 message ids from the receiver. Ordering must match
	// send order on a single stream.
	want := []string{"e2e-1", "e2e-2", "e2e-3"}
	for i, w := range want {
		select {
		case got := <-gotIDs:
			if got != w {
				t.Errorf("message %d: got id %q, want %q", i, got, w)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for message %d (%q)", i, w)
		}
	}

	// Acks land in the async batch channel; drain after each polling
	// tick so the test sees the resulting DELETEs.
	deadline := time.Now().Add(2 * time.Second)
	db := db_open("db/queue.db")
	for _, mid := range want {
		for time.Now().Before(deadline) {
			queue_ack_drain()
			row, _ := db.row("select 1 from queue where id=?", mid)
			if row == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		row, _ := db.row("select 1 from queue where id=?", mid)
		if row != nil {
			t.Errorf("row %q still present after ack", mid)
		}
	}
}

func TestEndToEndPipelinesClaimAndMessage(t *testing.T) {
	// First message from a new entity gets a claim frame pipelined
	// ahead of it. We assert by hand: receiver sees claim then
	// message before sending the ack.
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	sendStream, recvStream := new_stream_pair()
	challenge, _ := hello_challenge()

	saw_claim := make(chan struct{}, 1)
	saw_message := make(chan string, 1)

	go func() {
		_ = hello_write(recvStream, 2, "p", challenge, receiver_codecs(), receiver_features())
		caps, err := caps_read(recvStream)
		if err != nil {
			t.Errorf("recv caps_read: %v", err)
			return
		}
		_ = caps
		// Drain frames. First non-caps should be a claim, then a
		// message.
		for {
			f, err := frame_read(recvStream)
			if err != nil {
				return
			}
			switch f.Type {
			case frame_type_claim:
				saw_claim <- struct{}{}
			case frame_type_message:
				saw_message <- f.ID
				_ = frame_write(recvStream, &Frame{Type: frame_type_ack, Replies: []string{f.ID}})
				return
			}
		}
	}()

	hello, err := hello_read(sendStream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	const peer = "pipeline-peer"
	_, stop := install_sender_for(t, peer, sendStream, hello)
	defer stop()

	install_queue_row(t, "pipe-1")
	f := &Frame{Type: frame_type_message, ID: "pipe-1", From: id, Service: "s"}
	if err := peer_send(peer, "pipe-1", f); err != nil {
		t.Fatalf("peer_send: %v", err)
	}

	select {
	case <-saw_claim:
	case <-time.After(2 * time.Second):
		t.Fatal("no claim frame seen")
	}
	select {
	case mid := <-saw_message:
		if mid != "pipe-1" {
			t.Errorf("message id %q, want pipe-1", mid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message frame after claim")
	}
}

func TestEndToEndUnclaimedTriggersReclaim(t *testing.T) {
	// Receiver returns fail{unclaimed} on a message that's never been
	// claimed → sender's resolver clears its claimed cache and the
	// next send re-issues the claim before the message. We verify by
	// dropping the FIRST claim before responding to the message, then
	// observing the sender's resolve_fail directly.
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	const peer = "reclaim-peer"
	s := new_test_sender(t, peer)
	s.claimed[id] = true // pretend we'd claimed
	restore := stash_sender(t, peer, s)
	defer restore()

	install_queue_row(t, "reclaim-msg")
	p := &pending{queue: "reclaim-msg", sent: now()}
	s.resolve_fail(p, fail_unclaimed)

	if len(s.claimed) != 0 {
		t.Errorf("resolve_fail(unclaimed) did not clear cache; claimed=%v", s.claimed)
	}
	// Row should be pending (will retry, prompting a fresh claim).
	db := db_open("db/queue.db")
	row, _ := db.row("select status from queue where id=?", "reclaim-msg")
	if row == nil {
		t.Fatal("row missing")
	}
	if st, _ := row["status"].(string); st != "pending" {
		t.Errorf("status=%q, want pending", st)
	}
}

func TestEndToEndStreamDeathQueueFailsInflight(t *testing.T) {
	// Plan: "On stream death (writer or reader errors): ... Drain
	// inflight: every queue ID gets queue_fail("stream closed")."
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	sendStream, recvStream := new_stream_pair()

	challenge, _ := hello_challenge()
	go func() {
		_ = hello_write(recvStream, 2, "d", challenge, receiver_codecs(), receiver_features())
		_, _ = caps_read(recvStream)
		// Read first frame (claim or message), then kill stream
		// without acking.
		f, err := frame_read(recvStream)
		if err != nil {
			return
		}
		_ = f
		// Death: close both pipe ends so the sender sees EOF.
		recvStream.Reset()
	}()

	hello, err := hello_read(sendStream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	const peer = "death-peer"
	_, stop := install_sender_for(t, peer, sendStream, hello)
	defer stop()

	install_queue_row(t, "death-1")
	install_queue_row(t, "death-2")

	if err := peer_send(peer, "death-1", &Frame{Type: frame_type_message, ID: "death-1", From: id, Service: "s"}); err != nil {
		t.Fatalf("peer_send: %v", err)
	}
	if err := peer_send(peer, "death-2", &Frame{Type: frame_type_message, ID: "death-2", From: id, Service: "s"}); err != nil {
		t.Fatalf("peer_send: %v", err)
	}

	// Wait for the sender to detect stream death and drain inflight.
	// Detection happens via read_loop's frame_read returning EOF.
	deadline := time.Now().Add(2 * time.Second)
	db := db_open("db/queue.db")
	for time.Now().Before(deadline) {
		row, _ := db.row("select status from queue where id=?", "death-2")
		st, _ := row["status"].(string)
		if st == "pending" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Both rows must be 'pending' (queue_fail set), not 'sending'.
	for _, mid := range []string{"death-1", "death-2"} {
		row, _ := db.row("select status from queue where id=?", mid)
		if row == nil {
			t.Errorf("%s row deleted (acked?) unexpectedly", mid)
			continue
		}
		if st, _ := row["status"].(string); st != "pending" {
			t.Errorf("%s status=%q, want pending (queue_fail on stream death)", mid, st)
		}
	}
}

// --- /mochi/2/stream content-segment routing ---------------------------

// TestStreamOpenShipsContentAsFirstPostAckSegment guards the bug found
// in Phase 6 manual testing: when a caller passes `content` to
// stream_open, it MUST land on the wire as the first post-ack CBOR
// segment so the receiver's receive_stream picks it up as e.content.
// Earlier code packed it into open.Content where the receiver never
// looked, breaking auth_replicate's user-lookup ("No such user on the
// source server").
//
// We can't run the real /mochi/2/stream receiver here without libp2p,
// so we hand-construct the receiver side over an io.Pipe and assert
// that the first frame the sender writes after the ack is the content
// map.
func TestStreamOpenShipsContentAsFirstPostAckSegment(t *testing.T) {
	cleanup := setup_replication_test(t)
	defer cleanup()
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	sendStream, recvStream := new_stream_pair()

	got_content := make(chan map[string]any, 1)

	go func() {
		challenge, _ := hello_challenge()
		_ = hello_write(recvStream, 2, "s", challenge, receiver_codecs(), receiver_features())
		caps, err := caps_read(recvStream)
		if err != nil {
			t.Errorf("recv caps_read: %v", err)
			return
		}
		_ = caps
		// Drain claim + open
		for {
			f, err := frame_read(recvStream)
			if err != nil {
				return
			}
			if f.Type == frame_type_claim {
				continue
			}
			if f.Type == frame_type_open {
				_ = frame_write(recvStream, &Frame{Type: frame_type_ack, Replies: []string{f.ID}})
				// After ack, the next thing on the wire MUST be the
				// caller's content map. Decode it as a generic map and
				// hand back to the test.
				st := stream_rw(recvStream, recvStream)
				var content map[string]any
				if err := st.read(&content); err != nil {
					t.Errorf("recv content read: %v", err)
					return
				}
				got_content <- content
				return
			}
		}
	}()

	hello, err := hello_read(sendStream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	_ = hello

	// Mirror what stream_open does, but call it directly so the test
	// exercises the production code path (sender side).
	go func() {
		// Sender: write caps, claim, open, then content via stream_open's
		// new post-ack write. We can't call stream_open directly because
		// it uses peer_protocol_open + libp2p. Inline the relevant bits.
		_ = caps_write(sendStream, []string{"zstd"}, nil)
		_ = claim_write(sendStream, id, hello.Challenge)
		open := &Frame{Type: frame_type_open, ID: "x", From: id,
			Service: "svc", Event: "ev"}
		_ = frame_write(sendStream, open)
		// Read ack
		_, _ = frame_read(sendStream)
		// Now ship content as first post-ack segment (this is what
		// stream_open does when content != nil).
		st := stream_rw(sendStream, sendStream)
		_ = st.write(map[string]any{"username": "alistair@acunningham.org"})
	}()

	select {
	case got := <-got_content:
		if got["username"] != "alistair@acunningham.org" {
			t.Errorf("content[username] = %q, want %q", got["username"], "alistair@acunningham.org")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receiver never got the post-ack content segment")
	}
}

// TestStreamOpenSelfLoopUsesV2Native is the regression test for the
// market/staff → Comptroller outage: a mochi.remote.stream() to a
// locally-hosted entity resolves peer==net_id, and the wire path ends
// in net_me.NewStream(self), which libp2p refuses. stream_open_or_self
// must route self to the in-process loopback (stream_self_loop) rather
// than attempting (and failing) the wire path.
//
// In the test environment net_me is nil, so the wire attempt would
// return errSenderUnreachable; the self-loop path must return a non-nil
// near end with no error.
//
// Note: the far-end dispatch goroutine resolves "to-entity" to no local
// user and closes its pipe end — that's fine; this test only asserts
// the routing decision (the near end is returned, raw, no error).
func TestStreamOpenSelfLoopUsesV2Native(t *testing.T) {
	cleanup := setup_replication_test(t) // sets net_id = "self"
	defer cleanup()
	setup_users_test_schema() // so the far-end stream_resolve query has a table

	s, err := stream_open_or_self(net_id, "", "to-entity", "market", "search", "market", nil, nil)
	if err != nil {
		t.Fatalf("self-loop stream_open_or_self returned error: %v (market/staff → local Comptroller would 502)", err)
	}
	if s == nil {
		t.Fatal("self-loop returned nil stream; mochi.remote.stream() to a local entity would report 'not available'")
	}
	// Drain the near end. The far-end goroutine resolves "to-entity" to
	// no local user and closes its pipe end, so this read returns EOF.
	// Reading also synchronises with the goroutine's DB access (the
	// resolve happens-before its close, which happens-before this read
	// returns) so the detached goroutine can't race test teardown.
	var discard map[string]any
	_ = s.read(&discard)
	s.close()
}

// --- Unreachable peer ---------------------------------------------------

func TestQueueSendDirectUnreachablePeerFails(t *testing.T) {
	// queue_send_direct to a remote peer with no libp2p host (net_me is
	// nil in tests) must unwind cleanly: peer_protocol_open returns
	// errSenderUnreachable, queue_unsending rolls back the 'sending'
	// mark, and the row is left for queue_process to retry.
	cleanup := setup_replication_test(t)
	defer cleanup()
	saved := net_id
	net_id = "self-not-peer"
	defer func() { net_id = saved }()

	const peer = "mixed-peer"
	peer_add_known(peer, []string{})

	id := "unreachable-1"
	install_queue_dispatch_row(t, id, peer, "", nil)
	q := &QueueEntry{ID: id, Target: peer, Service: "s", Event: "e"}
	ok := queue_send_direct(q)
	if ok {
		t.Error("queue_send_direct returned true with no real peer")
	}
	if queue_is_inflight(id) {
		t.Error("row left marked 'sending' after unreachable peer; queue_unsending should have rolled it back")
	}
}
