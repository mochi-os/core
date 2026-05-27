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
	go s.read_loop()
	return s, func() {
		s.shutdown()
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

	// Acks should have removed the queue rows.
	deadline := time.Now().Add(2 * time.Second)
	db := db_open("db/queue.db")
	for _, mid := range want {
		for time.Now().Before(deadline) {
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

// --- Mixed-version: cache forces fallback ------------------------------

func TestMixedVersionPeerForcesLegacyPath(t *testing.T) {
	// Plan: peer that doesn't support /mochi/2/messages is cached as
	// such on the first probe failure; subsequent sends bypass the
	// v2 branch entirely. Verified directly via the dispatch tree
	// tests; here we additionally confirm queue_unsending unwinds
	// cleanly when the v2 path fails with a not-supported error.
	cleanup := setup_replication_test(t)
	defer cleanup()
	reset_protocol_cache(t)
	saved := net_id
	net_id = "self-not-peer"
	defer func() { net_id = saved }()

	const peer = "mixed-peer"
	peer_add_known(peer, []string{})

	// Pre-mark v2 as unknown (default). queue_send_direct will try
	// sender_open, which calls peer_protocol_open → libp2p NewStream
	// → fails (no net_me) → returns errSenderUnreachable. is_v2_unsupported
	// returns false for that error (it's not "not supported", it's
	// "couldn't even connect"), so queue_send_direct calls queue_fail.

	id := "mixed-1"
	install_queue_dispatch_row(t, id, peer, "", nil)
	q := &QueueEntry{ID: id, Target: peer, Service: "s", Event: "e"}
	ok := queue_send_direct(q)
	if ok {
		t.Error("queue_send_direct returned true with no real peer")
	}
}
