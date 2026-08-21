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
	"fmt"
	"io"
	"time"

	p2p_network "github.com/libp2p/go-libp2p/core/network"
)

// receive_stream is the libp2p stream handler registered for /mochi/2/stream in
// net_start.
//
// Wire sequence (after libp2p accepts the protocol):
//  1. Write hello with a fresh challenge.
//  2. Read caps (first sender frame).
//  3. Read one or more claim frames; verify per-(stream, entity) signatures
//     and cache claimed[From] = true.
//  4. Read open: dispatch to the app handler with the raw stream as e.stream.
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

	challenge, err := hello_challenge()
	if err != nil {
		warn("Stream: challenge entropy failed peer %q: %v", peer, err)
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
			if err := claim_verify(f.From, challenge, f.Signature, net_id, protocol_stream); err != nil {
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

// stream_open is the sender side of /mochi/2/stream: handshake, open frame,
// ack, then the raw stream wrapped in a *Stream. Also returns the session id.
func stream_open(peer, from, to, service, event, from_app string,
	services []string, content map[string]any) (*Stream, string, error) {

	rawstream, err := peer_protocol_open(peer, protocol_stream)
	if err != nil {
		return nil, "", err
	}
	if rawstream == nil {
		return nil, "", error_sender_unreachable
	}

	hello, err := hello_read(rawstream, 2)
	if err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: hello read failed peer=%q: %w", peer, err)
	}

	codecs := codec_intersect(receiver_codecs(), hello.Codecs)
	features := features_intersect(receiver_features(), hello.Features)

	// Our own challenge, for the far side to sign with the key of the
	// entity we are addressing. Fresh per stream, exactly as the hello
	// challenge is: a reused one would let a proof from an earlier
	// stream be replayed on this one.
	challenge, err := hello_challenge()
	if err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: challenge entropy failed peer=%q: %w", peer, err)
	}

	if err := caps_write(rawstream, codecs, features, challenge); err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: caps write failed peer=%q: %w", peer, err)
	}

	if from != "" {
		if err := claim_write(rawstream, from, hello.Challenge, peer, protocol_stream); err != nil {
			rawstream.Reset()
			return nil, "", fmt.Errorf("stream: claim write failed peer=%q: %w", peer, err)
		}
	}

	id := uid()
	// The open frame carries routing only; per-call content ships as the FIRST
	// post-ack CBOR segment, which is what the receiver reads as e.content. A
	// caller passing nil writes its own first segment.
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

	reply, err := frame_read(rawstream)
	if err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: reply read failed peer=%q: %w", peer, err)
	}
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
				rawstream.Reset()
				return nil, hello.Session,
					fmt.Errorf("stream: peer=%q answered for entity %q, not %q", peer, resolved, to)
			}
			if err := responder_verify(resolved, challenge, reply.Signature, net_id, protocol_stream); err != nil {
				rawstream.Reset()
				return nil, hello.Session,
					fmt.Errorf("stream: peer=%q failed to prove entity %q: %w", peer, resolved, err)
			}
		}

		// Handshake complete; raw bytes from here on.
		st := stream_rw(io.ReadCloser(rawstream), io.WriteCloser(rawstream))
		// If the caller passed a content map, ship it as the first
		// post-ack segment so receive_stream's read picks it up as
		// e.content. nil-content callers (stream_to_peer) write their
		// own first segment after the call returns.
		if content != nil {
			if err := st.write(content); err != nil {
				st.close()
				return nil, hello.Session,
					fmt.Errorf("stream: content write failed peer=%q: %w", peer, err)
			}
		}
		return st, hello.Session, nil
	case frame_type_fail:
		rawstream.Close()
		return nil, hello.Session,
			fmt.Errorf("stream: dispatch failed peer=%q reason=%q", peer, reply.Reason)
	}
	rawstream.Reset()
	return nil, hello.Session,
		fmt.Errorf("stream: unexpected reply type %q peer=%q", reply.Type, peer)
}

// stream_open_or_self opens a /mochi/2/stream to peer, routing peer == net_id
// to the in-process loopback (libp2p refuses to dial self). The returned stream
// is raw: the open frame and any `content` are already shipped and acked.
func stream_open_or_self(peer, from, to, service, event, from_app string,
	services []string, content map[string]any) (*Stream, error) {

	if peer == net_id {
		s := stream_self_loop(from, to, service, event, from_app, services)
		s.meter(from_app)
		return s, nil
	}
	s, _, err := stream_open(peer, from, to, service, event, from_app, services, content)
	if err == nil {
		s.meter(from_app)
	}
	return s, err
}
