// Mochi server: the /mochi/2/stream pre-open phase is bounded on both count and
// time. libp2p's 128 inbound streams per peer bounds concurrency, not work per
// stream, and a peer identity is free to mint.//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// feed is a one-writer one-reader byte channel that honours a read deadline,
// which io.Pipe does not. Frames arrive whole, so a chunk per frame with a
// remainder is enough.
type feed struct {
	chunks chan []byte
	rest   []byte
	expire chan struct{} // closed to fire the read deadline at once
}

func new_feed() *feed {
	return &feed{chunks: make(chan []byte, 256), expire: make(chan struct{})}
}

func (f *feed) write(b []byte) {
	c := make([]byte, len(b))
	copy(c, b)
	f.chunks <- c
}

func (f *feed) read(p []byte, deadline time.Time) (int, error) {
	for len(f.rest) == 0 {
		var expiry <-chan time.Time
		if !deadline.IsZero() {
			expiry = time.After(time.Until(deadline))
		}
		select {
		case c, ok := <-f.chunks:
			if !ok {
				return 0, io.EOF
			}
			f.rest = c
		case <-f.expire:
			return 0, os.ErrDeadlineExceeded
		case <-expiry:
			return 0, os.ErrDeadlineExceeded
		}
	}
	n := copy(p, f.rest)
	f.rest = f.rest[n:]
	return n, nil
}

// claims_local is the receiving host of every claims_conn: the receiver a claim
// to it binds.
var claims_local = p2p_peer.ID("12D3KooWClaimsLocalTestPeer")

// claims_conn answers what receive_stream_guarded asks of the connection: who
// is at either end, and which connection it is, for its challenge.
type claims_conn struct {
	p2p_network.Conn
	id string
}

func (c *claims_conn) RemotePeer() p2p_peer.ID { return p2p_peer.ID("12D3KooWClaimsFloodTestPeer") }
func (c *claims_conn) LocalPeer() p2p_peer.ID  { return claims_local }
func (c *claims_conn) ID() string              { return c.id }
func (c *claims_conn) RemoteMultiaddr() multiaddr.Multiaddr {
	return multiaddr.StringCast("/ip4/192.0.2.1/tcp/1443")
}

// claims_stream is the far side of a /mochi/2/stream handshake: the handler
// reads what the opener writes, and the opener reads what the handler writes.
type claims_stream struct {
	p2p_network.Stream
	conn *claims_conn

	inbound  *feed // opener -> handler
	outbound *feed // handler -> opener

	lock     sync.Mutex
	deadline time.Time
	reset    bool
	closed   bool
	delay    time.Duration // how long each write takes
}

func new_claims_stream() *claims_stream {
	return new_claims_stream_on("12D3KooWCl-1")
}

// new_claims_stream_on is a stream on the connection with this ID.
func new_claims_stream_on(connection string) *claims_stream {
	return &claims_stream{conn: &claims_conn{id: connection}, inbound: new_feed(), outbound: new_feed()}
}

func (s *claims_stream) Conn() p2p_network.Conn { return s.conn }

func (s *claims_stream) Read(p []byte) (int, error) {
	s.lock.Lock()
	deadline := s.deadline
	s.lock.Unlock()
	return s.inbound.read(p, deadline)
}

func (s *claims_stream) Write(p []byte) (int, error) {
	// As a real stream: a write takes its time, and nothing still being
	// written when the stream is closed or reset arrives.
	time.Sleep(s.delay)
	if s.done() {
		return 0, io.ErrClosedPipe
	}
	s.outbound.write(p)
	return len(p), nil
}

// SetWriteDeadline is accepted and ignored: writes to the feed never block.
func (s *claims_stream) SetWriteDeadline(time.Time) error { return nil }

func (s *claims_stream) SetReadDeadline(t time.Time) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.deadline = t
	return nil
}

func (s *claims_stream) Reset() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.reset = true
	return nil
}

func (s *claims_stream) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closed = true
	return nil
}

func (s *claims_stream) done() bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.reset || s.closed
}

func (s *claims_stream) deadline_set() time.Time {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.deadline
}

// opener_reader adapts the handler's writes into an io.Reader the test can
// frame_read, so the opener can learn the challenge the handler chose.
type opener_reader struct{ f *feed }

func (r *opener_reader) Read(p []byte) (int, error) { return r.f.read(p, time.Time{}) }

// claims_hello waits for the handler's hello and returns its challenge - the
// value a genuine claim must be signed over, and the reason claims cannot be
// scripted in advance.
func claims_hello(t *testing.T, s *claims_stream) []byte {
	t.Helper()
	f, err := frame_read(&opener_reader{f: s.outbound})
	if err != nil {
		t.Fatalf("reading the handler's hello: %v", err)
	}
	if f.Type != frame_type_hello {
		t.Fatalf("first frame from the handler was %q, want hello", f.Type)
	}
	return f.Challenge
}

// claims_caps sends the opener's caps with its own well-formed challenge, so
// the handshake reaches the claim loop.
func claims_caps(t *testing.T, s *claims_stream) {
	t.Helper()
	challenge := make([]byte, challenge_size_v2)
	if _, err := rand.Read(challenge); err != nil {
		t.Fatalf("challenge: %v", err)
	}
	claims_send(t, s, &Frame{Type: frame_type_caps, Challenge: challenge})
}

func claims_send(t *testing.T, s *claims_stream, f *Frame) {
	t.Helper()
	var buffer bytes.Buffer
	if err := frame_write(&buffer, f); err != nil {
		t.Fatalf("encoding %s: %v", f.Type, err)
	}
	s.inbound.write(buffer.Bytes())
}

// claims_signed builds a claim that genuinely verifies: a fresh entity key
// signing the handler's own challenge. These are the expensive ones - the
// handler must do the full ed25519 verify before it can reject anything.
func claims_signed(t *testing.T, challenge []byte) *Frame {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	entity := base58_encode(public)
	signable, err := claim_signable(challenge, entity, claims_local.String(), protocol_stream)
	if err != nil {
		t.Fatalf("claim_signable: %v", err)
	}
	return &Frame{Type: frame_type_claim, From: entity, Signature: ed25519.Sign(private, signable)}
}

// TestStreamCapsClaimsBeforeOpen is the count half, driven with claims that
// really verify - so the handler is doing the full work per frame, and only a
// count cap can stop it.
func TestStreamCapsClaimsBeforeOpen(t *testing.T) {
	s := new_claims_stream()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_stream_guarded(s)
	}()

	challenge := claims_hello(t, s)
	claims_caps(t, s)
	for i := 0; i < stream_claims_maximum+10; i++ {
		claims_send(t, s, claims_signed(t, challenge))
	}
	// Deliberately no open frame: the peer never completes the handshake.

	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler never returned: a peer holds a goroutine and a stream by sending verifiable claims and never opening")
	}
	if !s.done() {
		t.Error("the handler returned without resetting or closing the stream")
	}
}

// TestStreamAcceptsALegitimateOpener is the control. The cap must not be so
// tight that an ordinary opener - a few claims, then open - trips it, or the
// test above would pass for the wrong reason.
func TestStreamAcceptsALegitimateOpener(t *testing.T) {
	// Past the ack the handler records the opener's address.
	setup_peer_discovery_test(t)
	test_service(t, "claimstest", "open", test_answer)
	s := new_claims_stream()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_stream_guarded(s)
	}()

	challenge := claims_hello(t, s)
	claims_caps(t, s)
	claim := claims_signed(t, challenge)
	claims_send(t, s, claim)
	// An open naming the claimed entity: past the claim loop, into resolution,
	// and acknowledged - an empty To is a host-to-host stream.
	claims_send(t, s, &Frame{Type: frame_type_open, ID: "open-1", From: claim.From, To: "", Service: "claimstest", Event: "open"})

	replies := make(chan *Frame, 1)
	go func() {
		if f, err := frame_read(&opener_reader{f: s.outbound}); err == nil {
			replies <- f
		}
	}()
	select {
	case f := <-replies:
		if f.Type != frame_type_ack {
			t.Errorf("a legitimate opener was answered %q, not an ack", f.Type)
		}
	case <-finished:
		t.Fatal("a legitimate opener was not served: one claim then open must pass the cap")
	case <-time.After(20 * time.Second):
		t.Fatal("a legitimate opener was not answered")
	}
	// The handler now waits for the request; end it.
	close(s.inbound.expire)
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler did not end when its read deadline fired")
	}
	close(s.outbound.chunks)
	if !s.done() {
		t.Error("the handler left the stream neither reset nor closed")
	}
}

// TestStreamDeadlinesTheSlowDrip is the time half, and the one no count can
// cover. A peer that completes caps and then goes silent costs nothing to
// sustain; without a read deadline the handler waits on it indefinitely.
func TestStreamDeadlinesTheSlowDrip(t *testing.T) {
	s := new_claims_stream()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_stream_guarded(s)
	}()

	claims_hello(t, s)
	claims_caps(t, s)
	// And nothing further, ever.

	// The deadline must be set before the handler blocks; poll briefly.
	var deadline time.Time
	for attempt := 0; attempt < 100; attempt++ {
		if deadline = s.deadline_set(); !deadline.IsZero() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if deadline.IsZero() {
		t.Fatal("no read deadline was set on the pre-open phase: a peer that stops sending parks the handler forever")
	}
	if until := time.Until(deadline); until > stream_open_timeout+time.Second {
		t.Errorf("read deadline is %v away, want at most %v", until, stream_open_timeout)
	}

	// The deadline is real and bounded, asserted above. Firing it now rather
	// than sleeping through it keeps a 30-second handshake timeout from
	// costing the suite 30 seconds on every run.
	close(s.inbound.expire)

	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler did not give up when its read deadline expired")
	}
	if !s.done() {
		t.Error("the handler timed out without resetting the stream")
	}
}

// TestStreamDeadlinesTheCapsPhase: the pre-open bound covers caps too. A peer
// that opens the stream and sends nothing at all held the handler and the slot
// for the life of the connection.
func TestStreamDeadlinesTheCapsPhase(t *testing.T) {
	s := new_claims_stream()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_stream_guarded(s)
	}()
	claims_hello(t, s)
	// And not even caps.

	var deadline time.Time
	for attempt := 0; attempt < 100; attempt++ {
		if deadline = s.deadline_set(); !deadline.IsZero() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if deadline.IsZero() {
		t.Fatal("THE POINT: no read deadline while waiting for caps; a silent peer parks the handler forever")
	}
	close(s.inbound.expire)
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler did not give up when its read deadline expired")
	}
}

// stream_admission runs an open through the receiver and returns its answer,
// and whether anything after the open was read.
func stream_admission(t *testing.T, open *Frame, request any) (*Frame, bool) {
	t.Helper()
	s := new_claims_stream()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_stream_guarded(s)
	}()
	challenge := claims_hello(t, s)
	claims_caps(t, s)
	if open.From != "" {
		claim := claims_signed(t, challenge)
		open.From = claim.From
		claims_send(t, s, claim)
	}
	claims_send(t, s, open)
	var buffer bytes.Buffer
	if err := cbor.NewEncoder(&buffer).Encode(request); err != nil {
		t.Fatalf("encoding the request: %v", err)
	}
	s.inbound.write(buffer.Bytes())

	reply, err := frame_read(&opener_reader{f: s.outbound})
	if err != nil {
		t.Fatalf("reading the answer to the open: %v", err)
	}
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler did not end")
	}
	close(s.outbound.chunks)
	return reply, len(s.inbound.chunks) == 0 && len(s.inbound.rest) == 0
}

// An open no app takes, or from no sender to an event that wants one, is
// refused before the ack and before any of its request is read: the request
// used to be read first, up to 100 MB of it, whoever sent it.
func TestStreamRefusesAnOpenBeforeReadingIt(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_users_test_schema()
	delivered := make(chan string, 4)
	a := app("signed")
	a.service("signed")
	a.internal.Events["arrive"] = AppEvent{internal_function: func(e *Event) { delivered <- e.message }}
	t.Cleanup(func() {
		apps_lock.Lock()
		delete(apps, "signed")
		delete(internal_services, "signed")
		apps_lock.Unlock()
		resolution_invalidate()
	})
	resolution_invalidate()

	for name, open := range map[string]*Frame{
		"no app":          {Type: frame_type_open, ID: "open-1", Service: "no-such-service", Event: "arrive"},
		"no such event":   {Type: frame_type_open, ID: "open-2", Service: "signed", Event: "no-such-event"},
		"unsigned sender": {Type: frame_type_open, ID: "open-3", Service: "signed", Event: "arrive"},
	} {
		reply, read := stream_admission(t, open, map[string]any{"n": 1})
		if reply.Type != frame_type_fail || reply.Reason != fail_unsupported {
			t.Errorf("THE POINT: %s: the open was answered %s %q, want fail %q", name, reply.Type, reply.Reason, fail_unsupported)
		}
		if read {
			t.Errorf("THE POINT: %s: the request of a refused open was read", name)
		}
	}

	// Control: a claimed sender to the same event is served.
	reply, _ := stream_admission(t, &Frame{Type: frame_type_open, ID: "open-4", From: "claimed", Service: "signed", Event: "arrive"}, map[string]any{"n": 1})
	if reply.Type != frame_type_ack {
		t.Fatalf("control: a claimed sender's open was answered %s %q", reply.Type, reply.Reason)
	}
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Error("control: the admitted open's handler did not run")
	}
}

// The request read before any handler runs is held to stream_content_maximum,
// the handler's own reads taking the stream's whole budget after it.
func TestStreamHoldsTheRequestToItsCap(t *testing.T) {
	setup_peer_discovery_test(t)
	setup_users_test_schema()
	delivered := make(chan string, 4)
	test_service(t, "capped", "arrive", func(e *Event) { delivered <- e.message })

	reply, _ := stream_admission(t, &Frame{Type: frame_type_open, ID: "open-big", Service: "capped", Event: "arrive"},
		map[string]any{"data": make([]byte, stream_content_maximum)})
	if reply.Type != frame_type_ack {
		t.Fatalf("control: the open was answered %s %q", reply.Type, reply.Reason)
	}
	select {
	case id := <-delivered:
		t.Errorf("THE POINT: a request past the cap reached the handler (%q)", id)
	case <-time.After(500 * time.Millisecond):
	}

	reply, _ = stream_admission(t, &Frame{Type: frame_type_open, ID: "open-small", Service: "capped", Event: "arrive"},
		map[string]any{"data": make([]byte, stream_content_maximum/2)})
	if reply.Type != frame_type_ack {
		t.Fatalf("control: the open was answered %s %q", reply.Type, reply.Reason)
	}
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Error("control: a request within the cap did not reach the handler")
	}
}

// TestStreamSelfLoopIsGuarded: the loopback's dispatch runs on a goroutine of
// its own, and a panic at the top of one ends the process; it is guarded as
// receive_stream is.
func TestStreamSelfLoopIsGuarded(t *testing.T) {
	function := function_body(t, "protocol2_stream.go", "func stream_self_loop(")
	if !strings.Contains(function, `go guard("stream_self_loop", func() { far.reset(); far.close() }, func() {`) {
		t.Error("THE POINT: stream_self_loop's dispatch goroutine is not guarded, or not reset and closed when it faults")
	}
	if strings.Contains(function, "go func()") {
		t.Error("stream_self_loop starts an unguarded goroutine")
	}
}

// TestStreamAnswerErrorIsGuarded is #2111: stream_answer_error's writer was the
// one transport goroutine with no guard, so a panic there ended the process.
// Nothing reaches a panic in it from a test, so the guard is pinned.
func TestStreamAnswerErrorIsGuarded(t *testing.T) {
	function := function_body(t, "protocol2_stream.go", "func stream_answer_error(")
	if !strings.Contains(function, `go guard("stream_answer_error", nil, func() {`) {
		t.Error("THE POINT: stream_answer_error's writer goroutine is not guarded")
	}
	if strings.Contains(function, "go func()") {
		t.Error("stream_answer_error starts an unguarded goroutine")
	}
}

func TestStreamRejectsUnverifiableClaims(t *testing.T) {
	s := new_claims_stream()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_stream_guarded(s)
	}()

	claims_hello(t, s)
	claims_caps(t, s)
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	claims_send(t, s, &Frame{
		Type:      frame_type_claim,
		From:      base58_encode(public),
		Signature: make([]byte, ed25519.SignatureSize),
	})

	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("an unverifiable claim did not end the stream: the handler is still waiting for an open that need never come")
	}
	if !s.done() {
		t.Error("an unverifiable claim left the stream open")
	}
}

// TestStreamPreOpenBoundsArePinned. The bounds have no output of their own and
// the behavioural tests would still pass if a later change loosened them to
// something useless, so the shape is pinned at the source.
func TestStreamPreOpenBoundsArePinned(t *testing.T) {
	if stream_claims_maximum <= 0 || stream_claims_maximum > 1024 {
		t.Errorf("stream_claims_maximum = %d: a real opener claims a handful", stream_claims_maximum)
	}
	if stream_open_timeout <= 0 || stream_open_timeout > 5*time.Minute {
		t.Errorf("stream_open_timeout = %v: the pre-open phase is a handshake, not a session", stream_open_timeout)
	}

	source, err := os.ReadFile("protocol2_stream.go")
	if err != nil {
		t.Fatalf("read protocol2_stream.go: %v", err)
	}
	body := string(source)
	set := strings.Index(body, "SetReadDeadline(time.Now().Add(stream_open_timeout))")
	clear := strings.Index(body, "SetReadDeadline(time.Time{})")
	if set < 0 {
		t.Error("the pre-open phase no longer sets a read deadline")
	}
	// Cleared before the handler runs: an app stream is long-lived by design
	// and must not inherit a handshake timeout.
	if clear < 0 {
		t.Error("the pre-open deadline is never cleared; a long-lived app stream would inherit the handshake timeout")
	}
	if set >= 0 && clear >= 0 && clear < set {
		t.Error("the deadline is cleared before it is set")
	}
}
