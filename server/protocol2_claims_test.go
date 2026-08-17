// Mochi server: the /mochi/2/stream pre-open phase is bounded.
//
// receive_stream_guarded drained claim frames until the peer chose to send
// open. Nothing capped how many, nothing capped how slowly they arrived, and a
// claim that failed verification only `continue`d - so an unauthenticated peer
// could hold a goroutine and a stream indefinitely while spending one ed25519
// verify per frame and one map entry per distinct entity id, which are free to
// mint. libp2p's 128 inbound streams per peer bounds concurrency, not work per
// stream, and a peer identity is free to mint too.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

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

	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
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

// claims_conn answers the one question receive_stream_guarded asks of the
// connection.
type claims_conn struct{ p2p_network.Conn }

func (c *claims_conn) RemotePeer() p2p_peer.ID { return p2p_peer.ID("12D3KooWClaimsFloodTestPeer") }

// claims_stream is the far side of a /mochi/2/stream handshake: the handler
// reads what the opener writes, and the opener reads what the handler writes.
type claims_stream struct {
	p2p_network.Stream

	inbound  *feed // opener -> handler
	outbound *feed // handler -> opener

	lock     sync.Mutex
	deadline time.Time
	reset    bool
	closed   bool
}

func new_claims_stream() *claims_stream {
	return &claims_stream{inbound: new_feed(), outbound: new_feed()}
}

func (s *claims_stream) Conn() p2p_network.Conn { return &claims_conn{} }

func (s *claims_stream) Read(p []byte) (int, error) {
	s.lock.Lock()
	deadline := s.deadline
	s.lock.Unlock()
	return s.inbound.read(p, deadline)
}

func (s *claims_stream) Write(p []byte) (int, error) {
	s.outbound.write(p)
	return len(p), nil
}

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
	signable, err := claim_signable(challenge, entity, net_id, protocol_stream)
	if err != nil {
		t.Fatalf("claim_signable: %v", err)
	}
	return &Frame{Type: frame_type_claim, From: entity, Signature: ed25519.Sign(private, signable)}
}

// claims_run starts the handler and reports whether it finished within grace.
func claims_run(t *testing.T, s *claims_stream, grace time.Duration) bool {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		receive_stream_guarded(s)
	}()
	select {
	case <-finished:
		return true
	case <-time.After(grace):
		return false
	}
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
	// An open naming the claimed entity: past the claim loop, into resolution.
	claims_send(t, s, &Frame{Type: frame_type_open, ID: "open-1", From: claim.From, To: ""})

	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("a legitimate opener was not served: one claim then open must pass the cap")
	}
	// It gets past the claim loop; where it lands after that is stream
	// resolution's business, not this bound's.
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

// TestStreamRejectsUnverifiableClaims. A claim that fails verification used to
// `continue`, so invalid claims were unlimited as well - and they are the
// cheap ones to produce, costing the peer nothing and this host a verify each.
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
