// Mochi server: Protocol 2 — /mochi/2/stream handler + sender.
//
// One libp2p stream per mochi.stream(...) call: handshake + open, then
// raw bytes for the lifetime of the stream. Used by:
//
//   • mochi.stream(...) / mochi.stream.peer(...)  (Starlark)
//   • directory_download_from_peer
//   • file-push (queue_send_file_push)
//
// Architecturally simpler than /mochi/2/messages — no persistent
// sender, no inflight tracking, no worker pool.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	p2p_network "github.com/libp2p/go-libp2p/core/network"
)

// receive_stream is the libp2p stream handler registered for /mochi/2/stream in
// net_start.
//
// Wire sequence (after libp2p accepts the protocol):
//  1. Write hello with the connection's challenge.
//  2. Read caps (first sender frame).
//  3. Read one or more claim frames; verify each signature over the
//     connection's challenge and cache claimed[From] = true.
//  4. Read open: dispatch to the app handler with the raw stream as e.stream.
//
// The hello is written before anything is read, and the content is read, if at
// all, only after the ack, so an opener that knows the connection may send its
// caps, claims, open and content at once. Both bindings of the peers come from
// the authenticated connection.
//
// Guarded: libp2p runs each handler on its own goroutine, and the dispatch
// below routes broadcast events outside run_handler's recover.
func receive_stream(s p2p_network.Stream) {
	guard("receive_stream", func() { s.Reset() }, func() { receive_stream_guarded(s) })
}

func receive_stream_guarded(s p2p_network.Stream) {
	peer := s.Conn().RemotePeer().String()

	if !peer_is_bootstrap(peer) && !rate_limit_p2p.allow(peer) {
		debug("Stream rate limited peer %q", peer)
		s.Reset()
		return
	}

	challenge, err := hello_connection_challenge(s.Conn())
	if err != nil {
		warn("Stream: challenge failed peer %q: %v", peer, err)
		s.Reset()
		return
	}
	session := session_id()
	// debug("Stream: open peer=%q session=%s", peer, session)

	if err := hello_write(s, 2, session, challenge, receiver_codecs(), receiver_features()); err != nil {
		info("Stream: hello write failed peer=%q session=%s: %v", peer, session, err)
		s.Reset()
		return
	}

	caps, err := caps_read(s)
	if err != nil {
		info("Stream: caps read failed peer=%q session=%s: %v", peer, session, err)
		s.Reset()
		return
	}
	// The opener's challenge is mandatory: it is what this host signs to
	// prove it holds the entity being addressed. Rejecting a missing or
	// malformed one here is what makes the proof unskippable — an opener
	// cannot opt out of being given it.
	if err := frame_reject_challenge(caps.Challenge); err != nil {
		info("Stream: caps challenge rejected peer=%q session=%s: %v", peer, session, err)
		s.Reset()
		return
	}

	claimed := map[string]bool{}
	var open *Frame

	// Bound the pre-open phase on count and time: until open arrives the peer has
	// spent nothing and this host a verify per claim. The deadline is cleared
	// before the handler runs - an app stream is long-lived by design.
	_ = s.SetReadDeadline(time.Now().Add(stream_open_timeout))
	for count := 0; ; count++ {
		if count >= stream_claims_maximum {
			info("Stream: too many claims before open peer=%q session=%s — over %d", peer, session, stream_claims_maximum)
			s.Reset()
			return
		}
		f, err := frame_read(s)
		if err != nil {
			info("Stream: framing error peer=%q session=%s: %v", peer, session, err)
			s.Reset()
			return
		}
		switch f.Type {
		case frame_type_claim:
			// A claim that does not verify is not a retry, it is a peer
			// spending our signature checks on nothing. It used to continue.
			if err := claim_verify(f.From, challenge, f.Signature, s.Conn().LocalPeer().String(), protocol_stream); err != nil {
				info("Stream: claim verify failed peer=%q entity=%q: %v", peer, f.From, err)
				s.Reset()
				return
			}
			claimed[f.From] = true
		case frame_type_open:
			open = f
		default:
			info("Stream: protocol violation peer=%q session=%s — %q before open", peer, session, f.Type)
			s.Reset()
			return
		}
		if open != nil {
			break
		}
	}
	_ = s.SetReadDeadline(time.Time{})

	// The messages receiver validates its envelope before anything reads it;
	// this path did not, so Service, Event, ID, FromApp and Services reached
	// Event.route, the logs and the resolution cache as arbitrary strings
	// bounded only by the frame size.
	if !envelope_valid(open.From, open.Service, open.Event, open.ID) || !envelope_target_valid(open.To) {
		info("Stream: invalid envelope peer=%q session=%s", peer, session)
		s.Reset()
		return
	}
	if open.FromApp != "" && !valid(open.FromApp, "constant") {
		info("Stream: invalid sending app peer=%q session=%s", peer, session)
		s.Reset()
		return
	}
	for _, offered := range open.Services {
		if offered != "" && !valid(offered, "constant") {
			info("Stream: invalid offered service peer=%q session=%s", peer, session)
			s.Reset()
			return
		}
	}

	if open.From != "" && !claimed[open.From] {
		_ = frame_write(s, &Frame{Type: frame_type_fail, Replies: []string{open.ID}, Reason: fail_unclaimed})
		s.Close()
		return
	}

	// Resolve target entity (may be a fingerprint) and the owning user.
	to, user, ok := stream_resolve(open.To)
	if ok && user != nil {
		// Same learning as the messages path: open.From passed the claim
		// check above, so record its verified location for the recipient.
		directory_user_learn(user, open.From, peer)
	}
	if !ok {
		_ = frame_write(s, &Frame{Type: frame_type_fail, Replies: []string{open.ID}, Reason: fail_unknown_user})
		s.Close()
		return
	}

	// Prove to the opener that this host holds the entity it addressed, by signing
	// the caps challenge. Only when an entity was addressed: an empty To is a
	// host-to-host stream libp2p has already authenticated.
	ack := &Frame{Type: frame_type_ack, Replies: []string{open.ID}}
	if to != "" {
		proof := responder_sign(to, caps.Challenge, peer, protocol_stream)
		if proof == nil {
			info("Stream: cannot prove entity %q to peer=%q session=%s", to, peer, session)
			_ = frame_write(s, &Frame{Type: frame_type_fail, Replies: []string{open.ID}, Reason: fail_unproven})
			s.Close()
			return
		}
		ack.Signature = proof
		// The opener may have addressed a fingerprint; it cannot verify a
		// signature against a key it only holds the fingerprint of, so
		// return the resolved id for it to check the fingerprint against.
		ack.To = to
	}
	if err := frame_write(s, ack); err != nil {
		info("Stream: ack write failed peer=%q session=%s: %v", peer, session, err)
		s.Reset()
		return
	}

	st := stream_rw(s, s)
	st.remote = s.Conn().RemoteMultiaddr().String()

	// Hand off to the shared post-handshake dispatch (reads the first
	// content segment, builds the Event, routes it, closes).
	stream_dispatch(st, open, user, to, peer)

	peer_discovered_address(peer, s.Conn().RemoteMultiaddr().String()+"/p2p/"+peer)
	// debug("Stream: closed peer=%q session=%s", peer, session)
}

// stream_resolve maps an open target to its entity id and owning user. ok is
// false when a non-empty target has no local owner; an empty target resolves to
// ("", nil, true). Shared by the wire receiver and the self-loop so they cannot
// drift.
func stream_resolve(to string) (string, *User, bool) {
	if to == "" {
		return "", nil, true
	}
	if valid(to, "fingerprint") {
		if ent := entity_by_any(to); ent != nil {
			to = ent.ID
		}
	}
	user := user_owning_entity(to)
	if user == nil {
		return to, nil, false
	}
	return to, user, true
}

// stream_dispatch runs the post-handshake half of a /mochi/2/stream session:
// read the first post-ack CBOR segment as e.content, route the Event with the
// stream as e.stream, close. Shared by receive_stream and stream_self_loop.
// st.read() lazy-creates one decoder, so later e.segment() calls resume from
// it.
func stream_dispatch(st *Stream, open *Frame, user *User, to, peer string) {
	content := map[string]any{}
	if err := st.read(&content); err != nil {
		info("Stream dispatch: content read failed from=%q service=%q event=%q: %v",
			open.From, open.Service, open.Event, err)
		st.close()
		return
	}

	e := &Event{
		id:              event_id(),
		message:         open.ID,
		from:            open.From,
		to:              to,
		service:         open.Service,
		event:           open.Event,
		sender_app:      open.FromApp,
		sender_services: open.Services,
		peer:            peer,
		content:         content,
		stream:          st,
		user:            user,
	}

	if err := e.route(); err != nil {
		info("Stream dispatch: handler error service=%q event=%q: %v",
			open.Service, open.Event, err)
		// Answer with a generic error segment rather than a bare close: an EOF cannot
		// be told from a dead connection. No internal detail crosses the host
		// boundary.
		stream_answer_error(st, map[string]any{"error": "remote handler failed", "code": 500, "transport": true})
	}
	st.close()
}

// stream_answer_error best-effort writes a final error segment. The write must
// never block the dispatch: on a self-loop pipe a sender that only writes is
// not reading, so a bare st.write deadlocks. Goroutine with a short grace.
func stream_answer_error(st *Stream, answer map[string]any) {
	done := make(chan struct{})
	go func() {
		st.write(answer)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

// stream_self_loop is the in-process /mochi/2/stream loopback for peer ==
// net_id, since libp2p refuses to dial self: two io.Pipes crosswise, the
// handler running on the far end via stream_dispatch. The handshake is skipped
// - the same-process boundary is trusted. Target resolution stays on the far
// end so an unhosted entity fails exactly as it would over the wire.
func stream_self_loop(from, to, service, event, from_app string, services []string) *Stream {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	far := stream_rw(&pipe_reader{PipeReader: r1}, &pipe_writer{PipeWriter: w2})
	near := stream_rw(&pipe_reader{PipeReader: r2}, &pipe_writer{PipeWriter: w1})

	open := &Frame{
		Type: frame_type_open, ID: uid(), From: from, To: to,
		Service: service, Event: event, FromApp: from_app, Services: services,
	}

	go func() {
		resolved, user, ok := stream_resolve(open.To)
		if !ok {
			debug("Stream self-loop: unknown user for to=%q service=%q event=%q",
				open.To, open.Service, open.Event)
			// The wire receiver answers this with a fail_unknown_user
			// frame at the open handshake; the loopback skips the
			// handshake, so answer in-band instead of a bare close.
			stream_answer_error(far, map[string]any{"error": fail_unknown_user, "code": 404, "transport": true})
			far.close()
			return
		}
		stream_dispatch(far, open, user, resolved, net_id)
	}()

	return near
}

// error_stream_sent marks a failed open whose request had already gone with
// the open: the far side may have run it, so it must not be sent again
// elsewhere.
var error_stream_sent = errors.New("stream: the request may have been delivered")

// stream_open is the sender side of /mochi/2/stream: handshake, open frame,
// ack, then the raw stream wrapped in a *Stream, with content (when not nil)
// already sent as its first segment. Also returns the session id.
//
// On a connection recorded by connection_learn the hello is not waited for: the
// claim is signed over the challenge the connection's earlier hello gave, and
// caps, claim and open go at once. The content goes with them when the far side
// has already proved `to` on this connection, or there is no `to`; otherwise
// only once this stream's ack has proved it. Once ctx ends nothing more is
// sent and no stream is returned; an error wrapping error_stream_sent means
// the request had already gone. See claude/plans/protocol2.md App streams →
// Connection reuse.
func stream_open(ctx context.Context, peer, from, to, service, event, from_app string,
	services []string, content any) (*Stream, string, error) {

	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	rawstream, err := peer_protocol_open(peer, protocol_stream)
	if err != nil {
		return nil, "", err
	}
	if rawstream == nil {
		return nil, "", error_sender_unreachable
	}

	// Bound the handshake, the mirror of the deadline receive_stream puts on its
	// own pre-open phase. Without it a peer that accepts the stream and then
	// stays silent holds this goroutine forever. Cleared at the ack, below: an
	// app stream is long-lived by design.
	_ = rawstream.SetReadDeadline(time.Now().Add(stream_open_timeout))

	conn := rawstream.Conn()
	record, proven, known := connection_recall(conn, to)

	var hello *Frame
	if !known {
		hello, err = stream_hello(rawstream, peer)
		if err != nil {
			return nil, "", err
		}
		connection_learn(conn, hello)
		record = connection_record{
			challenge: hello.Challenge,
			codecs:    codec_intersect(receiver_codecs(), hello.Codecs),
			features:  features_intersect(receiver_features(), hello.Features),
		}
	}

	// Our own challenge, for the far side to sign with the key of the entity we
	// are addressing. Fresh per stream: a reused one would let a proof from an
	// earlier stream be replayed on this one.
	challenge, err := hello_challenge()
	if err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: challenge entropy failed peer=%q: %w", peer, err)
	}

	if err := ctx.Err(); err != nil {
		rawstream.Reset()
		return nil, "", err
	}

	if err := caps_write(rawstream, record.codecs, record.features, challenge); err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: caps write failed peer=%q: %w", peer, err)
	}

	if from != "" {
		if err := claim_write(rawstream, from, record.challenge, peer, protocol_stream); err != nil {
			rawstream.Reset()
			return nil, "", fmt.Errorf("stream: claim write failed peer=%q: %w", peer, err)
		}
	}

	id := uid()
	// The open frame carries routing only; per-call content ships as the FIRST
	// segment after it, which is what the receiver reads as e.content, and the
	// receiver reads it only after its ack. A caller passing nil writes its own
	// first segment.
	open := &Frame{
		Type:     frame_type_open,
		ID:       id,
		From:     from,
		To:       to,
		Service:  service,
		Event:    event,
		FromApp:  from_app,
		Services: services,
	}
	if err := frame_write(rawstream, open); err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: open write failed peer=%q: %w", peer, err)
	}

	// Raw bytes from here on; metered before the content, which is the app's.
	st := stream_rw(io.ReadCloser(rawstream), io.WriteCloser(rawstream))
	st.meter(from_app)

	// The content may leave before this stream's proof only for a peer that
	// already proved `to` on this connection, or when nothing is addressed: an
	// empty To is a host-to-host stream libp2p has already authenticated. It is
	// written beside the reads below, not before them: a receiver that fails the
	// open reads none of it, and over yamux a first segment larger than the
	// stream's window would otherwise hold this call until its write deadline.
	early := content != nil && (to == "" || proven)
	var sent chan error
	if early {
		sent = make(chan error, 1)
		go func() {
			err := errors.New("stream: content write panicked")
			defer func() { sent <- err }()
			guard("stream_open", nil, func() { err = st.write(content) })
		}()
	}
	// end gives up on the stream. Once the content has gone with the open the
	// far side may have run the request, and the error says so: a caller must
	// not send it again elsewhere.
	end := func(session string, err error) (*Stream, string, error) {
		rawstream.Reset()
		if sent != nil {
			<-sent
			err = fmt.Errorf("%w: %w", error_stream_sent, err)
		}
		return nil, session, err
	}

	if hello == nil {
		hello, err = hello_read(rawstream, 2)
		if err != nil {
			if deadline_exceeded(err) {
				return end("", error_sender_unreachable)
			}
			return end("", fmt.Errorf("stream: hello read failed peer=%q: %w", peer, err))
		}
		// The claim went over the challenge recorded for this connection; a
		// receiver that gives another has broken the promise its feature made.
		if !bytes.Equal(hello.Challenge, record.challenge) || !contains_string(hello.Features, feature_connection) {
			connection_forget(conn)
			return end(hello.Session, fmt.Errorf("stream: peer=%q changed its connection challenge", peer))
		}
	}

	reply, err := frame_read(rawstream)
	if err != nil {
		if deadline_exceeded(err) {
			return end(hello.Session, error_sender_unreachable)
		}
		return end("", fmt.Errorf("stream: reply read failed peer=%q: %w", peer, err))
	}
	// The handshake is over either way; what follows is the app's own traffic,
	// which sets its own deadlines.
	_ = rawstream.SetReadDeadline(time.Time{})
	switch reply.Type {
	case frame_type_ack:
		// Until this passes we know only which HOST answered, not that it may speak
		// for `to` - which a caller-supplied peer would otherwise decide for us.
		if to != "" {
			resolved := reply.To
			if resolved == "" {
				resolved = to
			}
			// We may have addressed a fingerprint; the proof is over the
			// full id, so bind the two before trusting the id it named.
			if resolved != to && fingerprint(resolved) != to {
				connection_doubt(conn, to)
				return end(hello.Session, fmt.Errorf("stream: peer=%q answered for entity %q, not %q", peer, resolved, to))
			}
			if err := responder_verify(resolved, challenge, reply.Signature, conn.LocalPeer().String(), protocol_stream); err != nil {
				connection_doubt(conn, to)
				return end(hello.Session, fmt.Errorf("stream: peer=%q failed to prove entity %q: %w", peer, resolved, err))
			}
			connection_prove(conn, to)
		}

		if sent != nil {
			// A write that failed sent no whole segment, which the far side
			// cannot read as a request.
			if err := <-sent; err != nil {
				st.close()
				return nil, hello.Session, fmt.Errorf("stream: content write failed peer=%q: %w", peer, err)
			}
		}
		// A caller that has given up gets no stream, and content held for the
		// proof is not sent: without it the far end runs no handler.
		if err := ctx.Err(); err != nil {
			st.close()
			if early {
				err = fmt.Errorf("%w: %w", error_stream_sent, err)
			}
			return nil, hello.Session, err
		}
		if content != nil && !early {
			if err := st.write(content); err != nil {
				st.close()
				return nil, hello.Session,
					fmt.Errorf("stream: content write failed peer=%q: %w", peer, err)
			}
		}
		return st, hello.Session, nil
	case frame_type_fail:
		// A receiver fails an open before it reads any content, so nothing was
		// delivered. Reset rather than close when content is still being
		// written: the receiver reads none of it.
		connection_doubt(conn, to)
		if sent != nil {
			rawstream.Reset()
			<-sent
		} else {
			rawstream.Close()
		}
		return nil, hello.Session,
			fmt.Errorf("stream: dispatch failed peer=%q reason=%q", peer, reply.Reason)
	}
	connection_doubt(conn, to)
	return end(hello.Session, fmt.Errorf("stream: unexpected reply type %q peer=%q", reply.Type, peer))
}

// stream_hello reads the receiver's hello, resetting the stream if it fails.
func stream_hello(rawstream p2p_network.Stream, peer string) (*Frame, error) {
	hello, err := hello_read(rawstream, 2)
	if err != nil {
		rawstream.Reset()
		// A silent peer is unreachable, not a hard failure.
		if deadline_exceeded(err) {
			return nil, error_sender_unreachable
		}
		return nil, fmt.Errorf("stream: hello read failed peer=%q: %w", peer, err)
	}
	return hello, nil
}

// stream_open_or_self opens a /mochi/2/stream to peer, routing peer == net_id
// to the in-process loopback (libp2p refuses to dial self). The returned stream
// is raw: the open frame and any `content` are already shipped and acked.
func stream_open_or_self(ctx context.Context, peer, from, to, service, event, from_app string,
	services []string, content any) (*Stream, error) {

	if peer == net_id {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s := stream_self_loop(from, to, service, event, from_app, services)
		s.meter(from_app)
		if content != nil {
			if err := s.write(content); err != nil {
				s.close()
				return nil, err
			}
		}
		return s, nil
	}
	s, _, err := stream_open(ctx, peer, from, to, service, event, from_app, services, content)
	return s, err
}
