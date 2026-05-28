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
// Copyright Alistair Cunningham 2026

package main

import (
	"fmt"
	"io"

	p2p_network "github.com/libp2p/go-libp2p/core/network"
)

// receive_stream is the libp2p stream handler registered for
// /mochi/2/stream in net_start.
//
// Wire sequence (after libp2p accepts the protocol):
//   1. Write hello with a fresh challenge.
//   2. Read caps (first sender frame).
//   3. Read one or more claim frames; verify per-(stream, entity)
//      signatures and cache claimed[From] = true.
//   4. Read open: the sender's app-stream request. Dispatch to the
//      app handler with the raw libp2p stream as e.stream; the
//      handler reads/writes raw bytes for the rest of the session.
func receive_stream(s p2p_network.Stream) {
	peer := s.Conn().RemotePeer().String()

	if !peer_is_bootstrap(peer) && !peer_is_pair(peer) && !rate_limit_p2p.allow(peer) {
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
	debug("Stream: open peer=%q session=%s", peer, session)

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
	_ = caps // sender features not used by app-stream handlers today

	claimed := map[string]bool{}
	var open *Frame

	// Drain claim frames until we see the open. Anything else before
	// open is a protocol violation.
	for {
		f, err := frame_read(s)
		if err != nil {
			info("Stream: framing error peer=%q session=%s: %v", peer, session, err)
			s.Reset()
			return
		}
		switch f.Type {
		case frame_type_claim:
			if err := claim_verify(f.From, challenge, f.Signature); err != nil {
				info("Stream: claim verify failed peer=%q entity=%q: %v", peer, f.From, err)
				continue
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

	if open.From != "" && !claimed[open.From] {
		_ = frame_write(s, &Frame{Type: frame_type_fail, Replies: []string{open.ID}, Reason: fail_unclaimed})
		s.Close()
		return
	}

	// Resolve target entity (may be a fingerprint) and the owning user.
	to, user, ok := stream_resolve(open.To)
	if !ok {
		_ = frame_write(s, &Frame{Type: frame_type_fail, Replies: []string{open.ID}, Reason: fail_unknown_user})
		s.Close()
		return
	}

	// Acknowledge the open; transition to raw mode. After this we hand
	// the libp2p stream to the app handler as the Event's stream — no
	// more framing.
	if err := frame_write(s, &Frame{Type: frame_type_ack, Replies: []string{open.ID}}); err != nil {
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
	debug("Stream: closed peer=%q session=%s", peer, session)
}

// stream_resolve maps a /mochi/2/stream open target to its resolved
// entity id and owning user. ok is false when a non-empty target has
// no local owner: the wire receiver answers fail_unknown_user, the
// self-loop just closes the pipe. An empty target resolves to
// ("", nil, true) — an anonymous stream the handler may still accept.
//
// Shared by receive_stream (wire) and stream_self_loop (in-process) so
// the two paths can never drift on how a target becomes a (user, app).
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

// stream_dispatch runs the post-handshake half of a /mochi/2/stream
// session: read the caller's first post-ack CBOR segment as e.content,
// build the Event with the stream as e.stream, route it to the app
// handler, then close.
//
// Shared by receive_stream (after its hello/caps/claim/ack handshake)
// and stream_self_loop (which skips the handshake — the same-process
// boundary is trusted, exactly as message_self_loop_dispatch skips the
// /mochi/2/messages envelope + signature). `to` is the resolved entity
// id; `user` its owner (may be nil for an anonymous target).
//
// st.read() lazy-creates a CBOR decoder backed by the stream, so
// handler-side e.segment(&v) / e.stream.read(&v) calls afterwards reuse
// the same decoder and pick up subsequent segments correctly.
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
		msg_id:          open.ID,
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
		debug("Stream dispatch: handler error service=%q event=%q: %v",
			open.Service, open.Event, err)
	}
	st.close()
}

// stream_self_loop is the in-process /mochi/2/stream loopback for
// peer == net_id. libp2p refuses to dial self, so instead of a wire
// stream we io.Pipe two ends crosswise: the caller reads/writes the
// near end; the app handler runs on the far end via stream_dispatch.
// The hello/caps/claim/open/ack handshake is skipped entirely — the
// same-process boundary is trusted, mirroring how the /mochi/2/messages
// self-send (message_self_loop_dispatch) skips the envelope + signature.
//
// Always returns a usable near end. Target resolution and the
// unknown-user case are handled on the far end exactly as the wire
// receiver does, so a self-loop to an unhosted entity fails the same
// way an over-the-wire send would (the handler never runs and the near
// end sees the closed pipe), rather than being decided differently at
// the sender.
//
// This is the v2-native replacement for the old peer_stream(net_id)
// self-loop, which ran the /mochi/1 receiver on the far end. Once
// Phase 8 removes /mochi/1 (and peer_stream with it), this is what
// keeps mochi.remote.stream() to a locally-hosted entity (market/staff
// → Comptroller) working.
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
			far.close()
			return
		}
		stream_dispatch(far, open, user, resolved, net_id)
	}()

	return near
}

// stream_open is the sender side of /mochi/2/stream. Establishes the
// libp2p stream, runs the handshake, writes the open frame, waits for
// ack, then returns the raw stream wrapped in a *Stream so the caller
// reads/writes bytes directly.
//
// Returns the wrapped Stream and the negotiated session ID (logged by
// the caller for log correlation).
func stream_open(peer, from, to, service, event, from_app string,
	services []string, content map[string]any) (*Stream, string, error) {

	rawstream, err := peer_protocol_open(peer, protocol_stream)
	if err != nil {
		return nil, "", err
	}
	if rawstream == nil {
		return nil, "", errSenderUnreachable
	}

	hello, err := hello_read(rawstream, 2)
	if err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: hello read failed peer=%q: %w", peer, err)
	}

	codecs := codec_intersect(receiver_codecs(), hello.Codecs)
	features := features_intersect(receiver_features(), hello.Features)

	if err := caps_write(rawstream, codecs, features); err != nil {
		rawstream.Reset()
		return nil, "", fmt.Errorf("stream: caps write failed peer=%q: %w", peer, err)
	}

	if from != "" {
		if err := claim_write(rawstream, from, hello.Challenge); err != nil {
			rawstream.Reset()
			return nil, "", fmt.Errorf("stream: claim write failed peer=%q: %w", peer, err)
		}
	}

	id := uid()
	// The open frame carries routing only. Per-call content is shipped
	// as the FIRST post-ack CBOR segment below, matching /mochi/1's
	// "headers then content" wire pattern. The receiver's
	// receive_stream reads exactly one CBOR segment after the ack as
	// e.content before dispatching, so any caller that passes
	// `content` here is wire-compatible with handlers that already
	// access e.content; callers that pass nil are responsible for
	// writing their own first segment via s.write after this returns.
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

// stream_open_v2_or_legacy is the migration helper for callers that
// can use /mochi/2/stream when available and need to fall back to the
// /mochi/1 peer_stream + read_challenge path for older peers. Returns
// (v2 Stream, true) or (legacy Stream, false). Caller writes the
// per-protocol message frame after this returns. During the rollout
// window every caller uses this; Phase 8 drops the legacy branch.
func stream_open_v2_or_legacy(peer, from, to, service, event, from_app string,
	services []string, content map[string]any) (st *Stream, v2 bool, err error) {

	// Self-loop streams (peer == net_id) can't use the wire v2 path:
	// peer_protocol_open ends in net_me.NewStream(self), which libp2p
	// refuses (a host can't dial itself). Route self to the v2-native
	// in-process loopback (stream_self_loop), which io.Pipes the two
	// ends and runs the /mochi/2/stream dispatch on the far end,
	// skipping the handshake. Returned v2=true: the near end is already
	// in raw mode, so the caller writes content directly rather than
	// taking the legacy challenge+Headers path. Without this, every
	// mochi.remote.stream() to a locally-hosted entity (market/staff →
	// Comptroller) fails.
	if peer == net_id {
		return stream_self_loop(from, to, service, event, from_app, services), true, nil
	}

	// Try v2 first unless the cache says it's not supported.
	switch protocol_known_get(peer, protocol_stream) {
	case protocol_state_unsupported:
		// Fall through to legacy.
	default:
		st, _, err = stream_open(peer, from, to, service, event, from_app, services, content)
		if err == nil {
			return st, true, nil
		}
		if is_protocol_not_supported(err) || protocol_known_get(peer, protocol_stream) == protocol_state_unsupported {
			// Cache already updated by peer_protocol_open; fall through.
		} else {
			// Real failure (e.g. peer unreachable, handshake error) —
			// don't fall back, that'd just retry the same path.
			return nil, false, err
		}
	}

	legacy := peer_stream(peer)
	if legacy == nil {
		return nil, false, errSenderUnreachable
	}
	return legacy, false, nil
}
