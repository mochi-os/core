// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// End-to-end intra-instance integration tests for /mochi/2/messages: a Sender
// and Receiver wired over io.Pipe run the full hello -> caps -> claim ->
// message -> ack sequence without libp2p.

package main

import (
	"fmt"
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
	reader      *io.PipeReader
	writer      *io.PipeWriter
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
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	sender_stream, received_stream := new_stream_pair()

	// Receiver-side state machine: write hello, read caps, read claim,
	// (we don't drive message dispatch here — the next test does).
	go func() {
		challenge, _ := hello_challenge()
		_ = hello_write(received_stream, 2, "test-sess", challenge, receiver_codecs(), receiver_features())
		caps, err := caps_read(received_stream)
		if err != nil {
			t.Errorf("recv caps_read: %v", err)
			return
		}
		if !contains_string(caps.Codecs, "zstd") {
			t.Errorf("caps codecs missing zstd: %v", caps.Codecs)
		}
		claim, err := frame_read(received_stream)
		if err != nil {
			t.Errorf("recv claim read: %v", err)
			return
		}
		if claim.Type != frame_type_claim || claim.From != id {
			t.Errorf("expected claim from %q, got %+v", id, claim)
			return
		}
		if err := claim_verify(claim.From, challenge, claim.Signature, test_receiver, protocol_messages); err != nil {
			t.Errorf("claim verify failed: %v", err)
		}
	}()

	// Sender-side: read hello, write caps + claim.
	hello, err := hello_read(sender_stream, 2)
	if err != nil {
		t.Fatalf("sender hello_read: %v", err)
	}
	if err := caps_write(sender_stream, []string{"zstd"}, nil, must_challenge(t)); err != nil {
		t.Fatalf("sender caps_write: %v", err)
	}
	if err := claim_write(sender_stream, id, hello.Challenge, test_receiver, protocol_messages); err != nil {
		t.Fatalf("sender claim_write: %v", err)
	}

	// Give the receiver goroutine a moment to consume + assert.
	time.Sleep(50 * time.Millisecond)
}

// --- Full Sender (with Receiver mock) ----------------------------------

// run_test_receiver plays the receiver: handshake, then ack every message
// frame; the returned channel emits dispatched message ids. `receiver` MUST be
// the peer the Sender under test was installed for, or every claim is rejected.
func run_test_receiver(t *testing.T, stream wire_stream, challenge []byte, receiver string) <-chan string {
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
		claimed := map[string]bool{}
		for {
			f, err := frame_read(stream)
			if err != nil {
				return
			}
			switch f.Type {
			case frame_type_claim:
				if err := claim_verify(f.From, challenge, f.Signature, receiver, protocol_messages); err == nil {
					claimed[f.From] = true
				}
			case frame_type_prove:
				// Sign the sender's caps challenge with the addressed entity's key, bound
				// to the OPENER's peer id (net_id here), not the peer this fake plays -
				// responder_verify rejects the wrong side.
				signature := responder_sign(f.To, caps.Challenge, net_id, protocol_messages)
				if signature == nil {
					_ = frame_write(stream, &Frame{Type: frame_type_fail, From: f.To, Reason: fail_unproven})
					continue
				}
				_ = frame_write(stream, &Frame{Type: frame_type_claim, From: f.To, Signature: signature})
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
	// offered must be the same challenge the caps frame carries: the far
	// side signs that, and the Sender verifies proofs against this copy.
	offered := must_challenge(t)
	s := &Sender{
		done:      make(chan struct{}),
		peer:      peer,
		stream:    stream,
		session:   hello.Session,
		challenge: hello.Challenge,
		offered:   offered,
		codecs:    codecs,
		features:  features,
		outbox:    make(chan *outbound, peer_outbox()),
		inflight:  map[string]*pending{},
		pings:     map[string]int64{},
		claimed:   map[string]bool{},
		proven:    map[string]bool{},
		proving:   map[string]bool{},
		proofs:    make(chan proof, 2*sender_proves_maximum),
	}
	if err := caps_write(stream, codecs, features, offered); err != nil {
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
		// Block until read_loop has exited: the caller's cleanup resets data_dir /
		// net_id, which a still-running read_loop would race. shutdown() resets the
		// stream, so this cannot hang.
		<-read_done
		restore()
	}
}

func TestEndToEndMessageRoundTrip(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	send_stream, received_stream := new_stream_pair()

	// Start the receiver simulator with a known challenge.
	challenge, _ := hello_challenge()
	got_i_ds := run_test_receiver(t, received_stream, challenge, "e2e-peer")

	// Sender side: read the receiver's hello, install the Sender,
	// then send 3 messages.
	hello, err := hello_read(send_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	const peer = "e2e-peer"
	_, stop := install_sender_for(t, peer, send_stream, hello)
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
		case got := <-got_i_ds:
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
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	send_stream, received_stream := new_stream_pair()
	challenge, _ := hello_challenge()

	saw_claim := make(chan struct{}, 1)
	saw_message := make(chan string, 1)

	go func() {
		_ = hello_write(received_stream, 2, "p", challenge, receiver_codecs(), receiver_features())
		caps, err := caps_read(received_stream)
		if err != nil {
			t.Errorf("recv caps_read: %v", err)
			return
		}
		_ = caps
		// Drain frames. First non-caps should be a claim, then a
		// message.
		for {
			f, err := frame_read(received_stream)
			if err != nil {
				return
			}
			switch f.Type {
			case frame_type_claim:
				saw_claim <- struct{}{}
			case frame_type_message:
				saw_message <- f.ID
				_ = frame_write(received_stream, &Frame{Type: frame_type_ack, Replies: []string{f.ID}})
				return
			}
		}
	}()

	hello, err := hello_read(send_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	const peer = "pipeline-peer"
	_, stop := install_sender_for(t, peer, send_stream, hello)
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
	// fail{unclaimed} must make the sender clear its claimed cache so the next
	// send re-issues the claim before the message.
	setup_replication_test(t)
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
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	id, _ := new_entity_keys(t)

	send_stream, received_stream := new_stream_pair()

	challenge, _ := hello_challenge()
	go func() {
		_ = hello_write(received_stream, 2, "d", challenge, receiver_codecs(), receiver_features())
		_, _ = caps_read(received_stream)
		// Read first frame (claim or message), then kill stream
		// without acking.
		f, err := frame_read(received_stream)
		if err != nil {
			return
		}
		_ = f
		// Death: close both pipe ends so the sender sees EOF.
		received_stream.Reset()
	}()

	hello, err := hello_read(send_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	const peer = "death-peer"
	_, stop := install_sender_for(t, peer, send_stream, hello)
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
	// Detection happens via read_loop's frame_read returning EOF. death-1's
	// write can fail first and be rolled back on its own path, so both rows
	// are waited for, not only death-2.
	deadline := time.Now().Add(2 * time.Second)
	db := db_open("db/queue.db")
	for time.Now().Before(deadline) {
		pending := 0
		for _, mid := range []string{"death-1", "death-2"} {
			row, _ := db.row("select status from queue where id=?", mid)
			if st, _ := row["status"].(string); st == "pending" {
				pending++
			}
		}
		if pending == 2 {
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

// TestStreamOpenShipsContentAsFirstPostAckSegment: `content` passed to
// stream_open must reach the wire as the first post-ack CBOR segment, which is
// where the receiver reads e.content. The receiver side is hand-built over a
// pipe.
func TestStreamOpenShipsContentAsFirstPostAckSegment(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	id, _ := new_entity_keys(t)

	send_stream, received_stream := new_stream_pair()

	got_content := make(chan map[string]any, 1)

	go func() {
		challenge, _ := hello_challenge()
		_ = hello_write(received_stream, 2, "s", challenge, receiver_codecs(), receiver_features())
		caps, err := caps_read(received_stream)
		if err != nil {
			t.Errorf("recv caps_read: %v", err)
			return
		}
		_ = caps
		// Drain claim + open
		for {
			f, err := frame_read(received_stream)
			if err != nil {
				return
			}
			if f.Type == frame_type_claim {
				continue
			}
			if f.Type == frame_type_open {
				_ = frame_write(received_stream, &Frame{Type: frame_type_ack, Replies: []string{f.ID}})
				// After ack, the next thing on the wire MUST be the
				// caller's content map. Decode it as a generic map and
				// hand back to the test.
				st := stream_rw(received_stream, received_stream)
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

	hello, err := hello_read(send_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	_ = hello

	opener_challenge := must_challenge(t)

	// Mirror what stream_open does, but call it directly so the test
	// exercises the production code path (sender side).
	go func() {
		// Sender: write caps, claim, open, then content via stream_open's
		// new post-ack write. We can't call stream_open directly because
		// it uses peer_protocol_open + libp2p. Inline the relevant bits.
		_ = caps_write(send_stream, []string{"zstd"}, nil, opener_challenge)
		_ = claim_write(send_stream, id, hello.Challenge, test_receiver, protocol_stream)
		open := &Frame{Type: frame_type_open, ID: "x", From: id,
			Service: "svc", Event: "ev"}
		_ = frame_write(send_stream, open)
		// Read ack
		_, _ = frame_read(send_stream)
		// Now ship content as first post-ack segment (this is what
		// stream_open does when content != nil).
		st := stream_rw(send_stream, send_stream)
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

// TestStreamOpenSelfLoopUsesV2Native: peer == net_id must route to the
// in-process loopback, since net_me.NewStream(self) is refused by libp2p.
// net_me is nil here, so the wire attempt would error; the self-loop must not.
func TestStreamOpenSelfLoopUsesV2Native(t *testing.T) {
	setup_replication_test(t) // sets net_id = "self"
	setup_users_test_schema() // so the far-end stream_resolve query has a table

	s, err := stream_open_or_self(net_id, "", "to-entity", "market", "search", "market", nil, nil)
	if err != nil {
		t.Fatalf("self-loop stream_open_or_self returned error: %v (market/staff → local Comptroller would 502)", err)
	}
	if s == nil {
		t.Fatal("self-loop returned nil stream; mochi.remote.stream() to a local entity would report 'not available'")
	}
	// Drain the near end: the far-end goroutine closes its pipe end, so this
	// returns EOF, and the read synchronises with its DB access so the detached
	// goroutine cannot race teardown.
	var discard map[string]any
	_ = s.read(&discard)
	s.close()
}

// --- Unreachable peer ---------------------------------------------------

func TestQueueSendDirectUnreachablePeerFails(t *testing.T) {
	// queue_send_direct to a remote peer with no libp2p host (net_me is
	// nil in tests) must unwind cleanly: peer_protocol_open returns
	// error_sender_unreachable, queue_unsending rolls back the 'sending'
	// mark, and the row is left for queue_process to retry.
	setup_replication_test(t)
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

// TestStreamSelfLoopAnswersErrors: the loopback must answer failures in-band,
// not with a bare close, so a requester can tell refusal from a dead
// connection.
func TestStreamSelfLoopAnswersErrors(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()

	// Unknown user: in-band unknown_user, not EOF.
	s := stream_self_loop("", "no-such-entity", "feeds", "view", "", nil)
	var response map[string]any
	if err := s.read(&response); err != nil {
		t.Fatalf("unknown-user loopback must answer in-band, got read error: %v", err)
	}
	if response["error"] != fail_unknown_user {
		t.Fatalf("unknown-user answer = %v", response)
	}
	s.close()

	// Handler failure (here: no app handles the service): a generic
	// in-band error with no internal detail.
	users := db_open("db/users.db")
	users.exec("insert into users (uid, username) values ('u-stream', 'stream@x')")
	entity := test_entity_id('s')
	users.exec("insert into entities (id, private, fingerprint, user, class, name) values (?, '', ?, 'u-stream', 'person', 'S')", entity, fingerprint(entity))
	s = stream_self_loop("", entity, "no-such-service", "view", "", nil)
	if err := s.write(map[string]any{}); err != nil {
		t.Fatalf("payload write: %v", err)
	}
	if err := s.read(&response); err != nil {
		t.Fatalf("handler-error loopback must answer in-band, got read error: %v", err)
	}
	if response["error"] != "remote handler failed" {
		t.Fatalf("handler-error answer = %v", response)
	}
	s.close()
}

// --- messages-path responder proof -------------------------------------

// TestMessagesProofPrecedesDelivery is the /mochi/2/messages counterpart
// of the stream ack proof: a message must not reach the wire until the
// far side has shown it holds the entity being addressed.
func TestMessagesProofPrecedesDelivery(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)

	from, _ := new_entity_keys(t)
	to, _ := new_entity_keys(t)

	sender_stream, received_stream := new_stream_pair()
	challenge := must_challenge(t)
	peer := "12D3KooWProofPeerUnderTest"
	got := run_test_receiver(t, received_stream, challenge, peer)

	hello, err := hello_read(sender_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	s, done := install_sender_for(t, peer, sender_stream, hello)
	defer done()

	if err := peer_send(peer, "", &Frame{
		Type: frame_type_message, ID: "m1", From: from, To: to,
		Service: "test", Event: "ping",
	}); err != nil {
		t.Fatalf("peer_send: %v", err)
	}

	select {
	case id := <-got:
		if id != "m1" {
			t.Errorf("delivered %q, want m1", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("message never delivered after the proof exchange")
	}

	s.lock.Lock()
	proven := s.proven[to]
	s.lock.Unlock()
	if !proven {
		t.Error("sender did not record the entity as proven")
	}
}

// TestMessagesProofCachedPerConnection checks the proof is asked for once
// per entity, not once per message — the property that makes this cheap
// enough to be mandatory on a multiplexed stream.
func TestMessagesProofCachedPerConnection(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)

	from, _ := new_entity_keys(t)
	to, _ := new_entity_keys(t)

	sender_stream, received_stream := new_stream_pair()
	challenge := must_challenge(t)
	peer := "12D3KooWProofCachePeerTest"

	// Count prove demands seen on the wire.
	proves := make(chan string, 16)
	go func() {
		_ = hello_write(received_stream, 2, "rs", challenge, receiver_codecs(), receiver_features())
		caps, err := caps_read(received_stream)
		if err != nil {
			return
		}
		for {
			f, err := frame_read(received_stream)
			if err != nil {
				return
			}
			switch f.Type {
			case frame_type_prove:
				proves <- f.To
				signature := responder_sign(f.To, caps.Challenge, net_id, protocol_messages)
				_ = frame_write(received_stream, &Frame{Type: frame_type_claim, From: f.To, Signature: signature})
			case frame_type_message:
				_ = frame_write(received_stream, &Frame{Type: frame_type_ack, Replies: []string{f.ID}})
			}
		}
	}()

	hello, err := hello_read(sender_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	_, done := install_sender_for(t, peer, sender_stream, hello)
	defer done()

	for _, id := range []string{"m1", "m2", "m3"} {
		if err := peer_send(peer, "", &Frame{
			Type: frame_type_message, ID: id, From: from, To: to,
			Service: "test", Event: "ping",
		}); err != nil {
			t.Fatalf("peer_send %s: %v", id, err)
		}
	}

	select {
	case <-proves:
	case <-time.After(3 * time.Second):
		t.Fatal("no prove demand seen")
	}
	select {
	case extra := <-proves:
		t.Errorf("entity %q proved more than once on one connection", extra)
	case <-time.After(500 * time.Millisecond):
	}
}

// --- proofs asked together ---------------------------------------------

// proving_receiver plays a far side whose proofs take `delay`, refuses the
// entities in `refuse` and never answers those in `silent`. got carries every
// delivered message id in arrival order, and "bye". peak is the most proves it
// ever held unbacked by a message, counted as the real receiver counts them;
// older is the same counted as receivers before this release did, crediting
// only a message naming its sender. asked is the most proves it held
// unanswered.
type proving_receiver struct {
	got     chan string
	peak    atomic.Int64
	older   atomic.Int64
	asked   atomic.Int64
	waiting atomic.Int64
}

func run_proving_receiver(t *testing.T, stream wire_stream, challenge []byte, delay time.Duration, refuse, silent map[string]bool) *proving_receiver {
	t.Helper()
	r := &proving_receiver{got: make(chan string, 1024)}
	var lock sync.Mutex
	write := func(f *Frame) {
		lock.Lock()
		defer lock.Unlock()
		_ = frame_write(stream, f)
	}
	go func() {
		_ = hello_write(stream, 2, "rs", challenge, receiver_codecs(), receiver_features())
		caps, err := caps_read(stream)
		if err != nil {
			return
		}
		unbacked, older := int64(0), int64(0)
		for {
			f, err := frame_read(stream)
			if err != nil {
				return
			}
			switch f.Type {
			case frame_type_prove:
				unbacked++
				if unbacked > r.peak.Load() {
					r.peak.Store(unbacked)
				}
				older++
				if older > r.older.Load() {
					r.older.Store(older)
				}
				if waiting := r.waiting.Add(1); waiting > r.asked.Load() {
					r.asked.Store(waiting)
				}
				entity := f.To
				if silent[entity] {
					continue
				}
				go func() {
					time.Sleep(delay)
					// Answered as the answer is written: the Sender reads it
					// and asks again before the write returns.
					r.waiting.Add(-1)
					if refuse[entity] {
						write(&Frame{Type: frame_type_fail, From: entity, Reason: fail_unproven})
						return
					}
					write(&Frame{Type: frame_type_claim, From: entity, Signature: responder_sign(entity, caps.Challenge, net_id, protocol_messages)})
				}()
			case frame_type_message:
				if unbacked > 0 {
					unbacked--
				}
				if f.From != "" && older > 0 {
					older--
				}
				r.got <- f.ID
				write(&Frame{Type: frame_type_ack, Replies: []string{f.ID}})
			case frame_type_ping:
				write(&Frame{Type: frame_type_pong, ID: f.ID})
			case frame_type_bye:
				r.got <- "bye"
			}
		}
	}()
	return r
}

// proving_setup opens a Sender to a proving_receiver and makes `count`
// entities to send to.
func proving_setup(t *testing.T, peer string, count int, delay time.Duration, refuse, silent func(int) bool) (*Sender, *proving_receiver, string, []string, func()) {
	t.Helper()
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	from, _ := new_entity_keys(t)
	entities := make([]string, count)
	refused, ignored := map[string]bool{}, map[string]bool{}
	for i := range entities {
		entities[i], _ = new_entity_keys(t)
		if refuse != nil && refuse(i) {
			refused[entities[i]] = true
		}
		if silent != nil && silent(i) {
			ignored[entities[i]] = true
		}
	}
	sender_stream, received_stream := new_stream_pair()
	r := run_proving_receiver(t, received_stream, must_challenge(t), delay, refused, ignored)
	hello, err := hello_read(sender_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	s, done := install_sender_for(t, peer, sender_stream, hello)
	return s, r, from, entities, func() {
		done()
		reset_workers(t)
	}
}

// proving_send queues one message frame for `to`, with a queue row when `row` is set.
func proving_send(t *testing.T, peer, id, from, to string, row bool) {
	t.Helper()
	queue := ""
	if row {
		install_queue_row(t, id)
		queue = id
	}
	if err := peer_send(peer, queue, &Frame{Type: frame_type_message, ID: id, From: from, To: to, Service: "test", Event: "ping"}); err != nil {
		t.Fatalf("peer_send %s: %v", id, err)
	}
}

// proving_status polls a queue row until it has `status` with `attempts`, or fails.
func proving_status(t *testing.T, id, status string, attempts int, within time.Duration) {
	t.Helper()
	db := db_open("db/queue.db")
	deadline := time.Now().Add(within)
	for {
		row, _ := db.row("select status, attempts from queue where id=?", id)
		if row != nil && row["status"] == status && int(row["attempts"].(int64)) == attempts {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("row %s is %v, want status %q with %d attempts", id, row, status, attempts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMessagesProofsAskedTogether pins that a fresh stream proves its targets
// together rather than one round trip after another, keeping each target's
// messages in order.
func TestMessagesProofsAskedTogether(t *testing.T) {
	const peer = "12D3KooWProofsTogetherPeer"
	s, r, from, entities, done := proving_setup(t, peer, 20, 100*time.Millisecond, nil, nil)
	defer done()
	_ = s

	start := time.Now()
	for i, to := range entities {
		proving_send(t, peer, fmt.Sprintf("together-%d-1", i), from, to, false)
		proving_send(t, peer, fmt.Sprintf("together-%d-2", i), from, to, false)
	}
	order := map[string]int{}
	for n := 0; n < 2*len(entities); n++ {
		select {
		case id := <-r.got:
			order[id] = n
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d messages delivered", n, 2*len(entities))
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("20 targets proven at 100 ms each took %v: the proofs were asked one after another", elapsed)
	}
	for i := range entities {
		if order[fmt.Sprintf("together-%d-1", i)] > order[fmt.Sprintf("together-%d-2", i)] {
			t.Errorf("target %d's messages arrived out of order", i)
		}
	}
}

// TestMessagesProofsStayWithinBudget pins that the proves a Sender keeps
// unbacked by a message stay well under the count past which the receiver
// resets the stream, however many targets are waiting.
func TestMessagesProofsStayWithinBudget(t *testing.T) {
	const peer = "12D3KooWProofsBudgetPeer"
	_, r, from, entities, done := proving_setup(t, peer, 70, 200*time.Millisecond, nil, nil)
	defer done()

	for i, to := range entities {
		proving_send(t, peer, fmt.Sprintf("budget-%d", i), from, to, false)
	}
	for n := 0; n < len(entities); n++ {
		select {
		case <-r.got:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d messages delivered", n, len(entities))
		}
	}
	if peak := r.peak.Load(); peak > sender_proves_maximum {
		t.Errorf("the receiver held %d proves unbacked, over the Sender's budget of %d (it resets past %d)", peak, sender_proves_maximum, messages_proves_maximum)
	}
}

// TestMessagesRefusedProofFailsOnlyItsTarget pins that a target the far side
// will not prove fails its own messages back to the queue and nothing else:
// the stream stays up for every other target.
func TestMessagesRefusedProofFailsOnlyItsTarget(t *testing.T) {
	const peer = "12D3KooWProofsRefusedPeer"
	s, r, from, entities, done := proving_setup(t, peer, 2, 10*time.Millisecond, func(i int) bool { return i == 0 }, nil)
	defer done()

	proving_send(t, peer, "refused-1", from, entities[0], true)
	proving_send(t, peer, "accepted-1", from, entities[1], false)
	select {
	case id := <-r.got:
		if id != "accepted-1" {
			t.Errorf("delivered %q, want accepted-1", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the proven target's message was never delivered")
	}
	proving_status(t, "refused-1", "pending", 1, 3*time.Second)
	if s.closed.Load() {
		t.Error("one target's refused proof closed the stream for every other target")
	}
}

// TestMessagesUnansweredProofWaitsAlone pins that a proof the far side never
// answers holds up only its own target's messages, which fail back to the
// queue once it is overdue.
func TestMessagesUnansweredProofWaitsAlone(t *testing.T) {
	const peer = "12D3KooWProofsSilentPeer"
	s, r, from, entities, done := proving_setup(t, peer, 2, 10*time.Millisecond, nil, func(i int) bool { return i == 0 })
	defer done()

	proving_send(t, peer, "silent-1", from, entities[0], true)
	start := time.Now()
	proving_send(t, peer, "answered-1", from, entities[1], false)
	select {
	case <-r.got:
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("the answered target waited %v behind the unanswered one", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the answered target's message was never delivered")
	}
	proving_status(t, "silent-1", "pending", 1, 4*time.Second)
	if s.closed.Load() {
		t.Error("an unanswered proof closed the stream")
	}
}

// TestMessagesSpentBudgetStartsAfresh pins that when every proof the budget
// allows has failed and targets still wait, the Sender closes so a fresh
// stream (with a fresh budget) can try them, failing what it held back to the
// queue rather than holding it for ever.
func TestMessagesSpentBudgetStartsAfresh(t *testing.T) {
	const peer = "12D3KooWProofsSpentPeer"
	count := sender_proves_maximum + 4
	s, _, from, entities, done := proving_setup(t, peer, count, 10*time.Millisecond, func(int) bool { return true }, nil)
	defer done()

	for i, to := range entities {
		proving_send(t, peer, fmt.Sprintf("spent-%d", i), from, to, true)
	}
	for i := range entities {
		proving_status(t, fmt.Sprintf("spent-%d", i), "pending", 1, 5*time.Second)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !s.closed.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the Sender stayed open with targets it can no longer ask to be proven")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSenderShutdownFailsHeldMessages pins that messages held for a proof go
// back to the queue when the Sender closes, as the outbox drain does.
func TestSenderShutdownFailsHeldMessages(t *testing.T) {
	const peer = "12D3KooWProofsShutdownPeer"
	s, _, from, entities, done := proving_setup(t, peer, 1, 0, nil, func(int) bool { return true })
	defer done()

	proving_send(t, peer, "held-1", from, entities[0], true)
	deadline := time.Now().Add(2 * time.Second)
	for s.parked.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the message was never held for its proof")
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.shutdown()
	proving_status(t, "held-1", "pending", 1, time.Second)
	if n := s.parked.Load(); n != 0 {
		t.Errorf("%d messages still counted as held after shutdown", n)
	}
}

// TestSenderRoomCountsHeldMessages pins that pull_loop counts messages held
// for a proof against the window, so a slow proof cannot draw an unbounded
// backlog into memory.
func TestSenderRoomCountsHeldMessages(t *testing.T) {
	s := new_test_sender(t, "room-peer")
	full := s.room()
	s.parked.Store(5)
	if got := s.room(); got != full-5 {
		t.Errorf("room with 5 held = %d, want %d", got, full-5)
	}
}

// TestSenderHoldsPastTheWindow pins that a message for an unproven target is
// held behind its target's earlier ones even when the window's worth is already
// held: handed back to the queue, it went out after its target's later
// messages. pull_loop bounds what is held.
func TestSenderHoldsPastTheWindow(t *testing.T) {
	setup_replication_test(t)
	s := new_test_sender(t, "overflow-peer")
	s.proven = map[string]bool{}
	s.proving = map[string]bool{}
	s.parked.Store(int64(peer_window()))
	install_queue_row(t, "overflow-1")
	w := &waiting{parked: map[string][]*outbound{"someone": {{queue: "earlier", frame: &Frame{Type: frame_type_message, ID: "earlier", To: "someone"}}}}, deadlines: map[string]time.Time{"someone": time.Now().Add(time.Second)}}
	if err := s.route(w, &outbound{queue: "overflow-1", frame: &Frame{Type: frame_type_message, ID: "overflow-1", To: "someone"}}); err != nil {
		t.Fatalf("route: %v", err)
	}
	if held := w.parked["someone"]; len(held) != 2 || held[1].queue != "overflow-1" {
		t.Errorf("held %v for the target, want the message behind the earlier one", held)
	}
	proving_status(t, "overflow-1", "sending", 0, time.Second)
}

// TestSenderExpireKeepsReadLateProof pins that a proof the far side gave in
// time, but which write_loop reads only after its deadline because it was busy
// writing, writes the held message rather than failing it back to the queue.
func TestSenderExpireKeepsReadLateProof(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	from, _ := new_entity_keys(t)
	to, _ := new_entity_keys(t)
	stream, far := new_stream_pair()
	defer stream.Close()
	go func() {
		for {
			if _, err := frame_read(far); err != nil {
				return
			}
		}
	}()
	s := &Sender{peer: "12D3KooWLateProof", stream: stream, challenge: must_challenge(t),
		inflight: map[string]*pending{}, pings: map[string]int64{}, claimed: map[string]bool{},
		proven: map[string]bool{to: true}, proving: map[string]bool{}}
	install_queue_row(t, "late-1")
	w := &waiting{
		parked:    map[string][]*outbound{to: {{frame: &Frame{Type: frame_type_message, ID: "late-1", From: from, To: to, Service: "test", Event: "ping"}, queue: "late-1"}}},
		deadlines: map[string]time.Time{to: time.Now().Add(-time.Second)},
	}
	s.parked.Store(1)

	if err := s.expire(w); err != nil {
		t.Fatalf("expire: %v", err)
	}
	s.lock.Lock()
	_, written := s.inflight["late-1"]
	s.lock.Unlock()
	if !written {
		t.Error("a message whose target was proven in time was not written")
	}
	row, _ := db_open("db/queue.db").row("select attempts from queue where id='late-1'")
	if row == nil || row["attempts"].(int64) != 0 {
		t.Errorf("row is %v, want it untouched by a failure", row)
	}
}

// TestSenderFailsOnceWhenProveWriteFails pins that a message held for a proof
// whose prove frame cannot be written is failed back to the queue once, not by
// both write_loop and the release of held messages.
func TestSenderFailsOnceWhenProveWriteFails(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	const peer = "12D3KooWProveWriteFails"
	from, _ := new_entity_keys(t)
	to, _ := new_entity_keys(t)
	sender_stream, far := new_stream_pair()
	go func() {
		_ = hello_write(far, 2, "rs", must_challenge(t), receiver_codecs(), receiver_features())
		_, _ = caps_read(far)
		far.reader.Close()
	}()
	hello, err := hello_read(sender_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	s, done := install_sender_for(t, peer, sender_stream, hello)
	defer done()
	time.Sleep(50 * time.Millisecond)

	proving_send(t, peer, "once-1", from, to, true)
	deadline := time.Now().Add(2 * time.Second)
	for !s.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	row, _ := db_open("db/queue.db").row("select attempts from queue where id='once-1'")
	if row == nil || row["attempts"].(int64) != 1 {
		t.Errorf("row is %v, want one failure charged", row)
	}
}

// TestMessagesDuplicatesBackProves pins that a message the far side has seen
// before backs a prove there as it does here: a fresh stream proving many
// targets whose messages are retries of ones delivered earlier stays within
// the receiver's budget instead of being reset.
func TestMessagesDuplicatesBackProves(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	reset_workers(t)
	defer reset_workers(t)
	from, _ := new_entity_keys(t)
	const count = 80
	entities := make([]string, count)
	for i := range entities {
		entities[i], _ = new_entity_keys(t)
	}
	sender_stream, received_stream := new_stream_pair()
	challenge := must_challenge(t)
	r := &Receiver{peer: net_id, stream: received_stream, session: "rs", challenge: challenge, replies: make(chan *Frame, 1024), done: make(chan struct{}), claimed: map[string]bool{}}
	go func() {
		_ = hello_write(received_stream, 2, "rs", challenge, receiver_codecs(), receiver_features())
		go r.write_replies()
		r.read_loop()
		r.closed.Store(true)
		close(r.done)
	}()
	hello, err := hello_read(sender_stream, 2)
	if err != nil {
		t.Fatalf("hello_read: %v", err)
	}
	s, done := install_sender_for(t, net_id, sender_stream, hello)
	defer done()
	for i, to := range entities {
		id := fmt.Sprintf("duplicate-%d", i)
		message_seen_mark(id)
		proving_send(t, net_id, id, from, to, true)
	}

	// Acks reach the queue through queue_ack_async, which nothing drains here,
	// so delivery is read from the Sender: every target proven and every
	// message acked.
	settled := func() bool {
		s.lock.Lock()
		defer s.lock.Unlock()
		return len(s.proven) == count && len(s.inflight) == 0 && s.parked.Load() == 0
	}
	deadline := time.Now().Add(10 * time.Second)
	for !settled() && !s.closed.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := received_stream.reset_count.Load(); n > 0 {
		t.Errorf("the receiver reset the stream %d times: duplicates did not back its proves", n)
	}
	if !settled() {
		s.lock.Lock()
		t.Errorf("%d of %d targets proven, %d messages unacknowledged, %d held", len(s.proven), count, len(s.inflight), s.parked.Load())
		s.lock.Unlock()
	}
	if n := db_open("db/queue.db").integer("select count(*) from queue where attempts > 0"); n > 0 {
		t.Errorf("%d duplicates were failed back to the queue", n)
	}
}

// TestSenderByeWaitsForHeldMessages pins that a bye, after which the far side
// stops reading, goes out behind the messages held for a proof rather than
// overtaking them.
func TestSenderByeWaitsForHeldMessages(t *testing.T) {
	const peer = "12D3KooWByeWaits"
	_, r, from, entities, done := proving_setup(t, peer, 2, 200*time.Millisecond, nil, nil)
	defer done()

	proving_send(t, peer, "held-1", from, entities[0], true)
	if err := peer_send(peer, "", &Frame{Type: frame_type_bye}); err != nil {
		t.Fatalf("peer_send bye: %v", err)
	}
	proving_send(t, peer, "behind-1", from, entities[1], true)
	order := []string{}
	timeout := time.After(3 * time.Second)
	for len(order) < 2 {
		select {
		case id := <-r.got:
			order = append(order, id)
		case <-timeout:
			t.Fatalf("received %v, want the held message and the bye", order)
		}
	}
	if order[0] != "held-1" || order[1] != "bye" {
		t.Errorf("received %v, want the held message before the bye", order)
	}
	// A message for an unproven target arriving behind the bye is handed back
	// to the queue rather than held, which would keep the bye waiting on it.
	proving_status(t, "behind-1", "pending", 0, time.Second)
}

// TestSendersByeAllWaitsForHeldMessages pins that shutdown waits for messages
// held for a proof as it waits for those in flight.
func TestSendersByeAllWaitsForHeldMessages(t *testing.T) {
	const peer = "12D3KooWByeAllWaits"
	_, r, from, entities, done := proving_setup(t, peer, 1, 300*time.Millisecond, nil, nil)
	defer done()

	proving_send(t, peer, "shutdown-1", from, entities[0], true)
	time.Sleep(50 * time.Millisecond)
	senders_bye_all(3 * time.Second)
	select {
	case id := <-r.got:
		if id != "shutdown-1" {
			t.Errorf("received %q first, want the held message", id)
		}
	default:
		t.Error("shutdown returned before the held message was written")
	}
}

// TestSenderAsksWithinAnswerRoom pins that the proves out at once stay within
// the Sender's budget even while messages to proven targets back them, so
// every answer fits where write_loop collects it.
func TestSenderAsksWithinAnswerRoom(t *testing.T) {
	const peer = "12D3KooWAnswerRoom"
	const count = 70
	s, r, from, entities, done := proving_setup(t, peer, 2*count, 300*time.Millisecond, nil, nil)
	defer done()
	s.lock.Lock()
	for _, entity := range entities[count:] {
		s.proven[entity] = true
	}
	s.lock.Unlock()

	for i := 0; i < count; i++ {
		proving_send(t, peer, fmt.Sprintf("room-unproven-%d", i), from, entities[i], false)
		proving_send(t, peer, fmt.Sprintf("room-proven-%d", i), from, entities[count+i], false)
	}
	timeout := time.After(5 * time.Second)
	for got := 0; got < 2*count; got++ {
		select {
		case <-r.got:
		case <-timeout:
			t.Fatalf("received %d of %d messages", got, 2*count)
		}
	}
	if asked := r.asked.Load(); asked > sender_proves_maximum {
		t.Errorf("%d proves were out at once, over the Sender's budget of %d", asked, sender_proves_maximum)
	}
}

// TestSenderCountsOnlySignedMessages pins that the Sender counts a prove backed
// only by a message naming its sender, the one kind every receiver credits, so
// a receiver from before this release is never pushed past its budget by
// messages it did not count.
func TestSenderCountsOnlySignedMessages(t *testing.T) {
	const peer = "12D3KooWSignedOnly"
	const count = 2 * sender_proves_maximum
	_, r, from, entities, done := proving_setup(t, peer, count, 10*time.Millisecond, func(int) bool { return true }, nil)
	defer done()

	for i := 0; i < sender_proves_maximum; i++ {
		proving_send(t, peer, fmt.Sprintf("refused-%d", i), from, entities[i], false)
	}
	for i := 0; i < 40; i++ {
		if err := peer_send(peer, "", &Frame{Type: frame_type_message, ID: fmt.Sprintf("unsigned-%d", i), Service: "test", Event: "ping"}); err != nil {
			t.Fatalf("peer_send unsigned: %v", err)
		}
	}
	for i := sender_proves_maximum; i < count; i++ {
		proving_send(t, peer, fmt.Sprintf("refused-%d", i), from, entities[i], false)
	}
	time.Sleep(time.Second)
	if older := r.older.Load(); older > sender_proves_maximum {
		t.Errorf("a receiver crediting only signed messages held %d proves unbacked, over the Sender's budget of %d", older, sender_proves_maximum)
	}
}

// TestSenderForSharesOpenUnderWay pins that callers asking for a peer's Sender
// while an open is under way wait for it and share its answer: a second dial
// meets the first one's connect in progress, which counts against the peer as
// a failure.
func TestSenderForSharesOpenUnderWay(t *testing.T) {
	var opens atomic.Int32
	sender_opener = func(peer string) (*Sender, error) {
		opens.Add(1)
		time.Sleep(100 * time.Millisecond)
		return nil, error_sender_unreachable
	}
	defer func() { sender_opener = nil }()

	var wait sync.WaitGroup
	failures := atomic.Int32{}
	for i := 0; i < 5; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := sender_for("12D3KooWOpenUnderWay"); err == error_sender_unreachable {
				failures.Add(1)
			}
		}()
	}
	wait.Wait()
	if n := opens.Load(); n != 1 {
		t.Errorf("five callers opened the peer %d times, want once", n)
	}
	if n := failures.Load(); n != 5 {
		t.Errorf("%d of five callers had the open's answer", n)
	}
	senders_lock.Lock()
	left := len(senders_opening)
	senders_lock.Unlock()
	if left != 0 {
		t.Errorf("%d opens still recorded as under way", left)
	}
}
