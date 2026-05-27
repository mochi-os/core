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
	to := open.To
	if to != "" && valid(to, "fingerprint") {
		if ent := entity_by_any(to); ent != nil {
			to = ent.ID
		}
	}
	var user *User
	if to != "" {
		user = user_owning_entity(to)
		if user == nil {
			_ = frame_write(s, &Frame{Type: frame_type_fail, Replies: []string{open.ID}, Reason: fail_unknown_user})
			s.Close()
			return
		}
	}

	// Acknowledge the open; transition to raw mode. After this we hand
	// the libp2p stream to the app handler as the Event's stream — no
	// more framing.
	if err := frame_write(s, &Frame{Type: frame_type_ack, Replies: []string{open.ID}}); err != nil {
		info("Stream: ack write failed peer=%q session=%s: %v", peer, session, err)
		s.Reset()
		return
	}

	content := open.Content
	if content == nil {
		content = map[string]any{}
	}

	st := stream_rw(s, s)
	st.remote = s.Conn().RemoteMultiaddr().String()

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
		debug("Stream: handler error peer=%q service=%q event=%q: %v",
			peer, open.Service, open.Event, err)
	}
	st.close()
	peer_discovered_address(peer, s.Conn().RemoteMultiaddr().String()+"/p2p/"+peer)
	debug("Stream: closed peer=%q session=%s", peer, session)
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
	open := &Frame{
		Type:     frame_type_open,
		ID:       id,
		From:     from,
		To:       to,
		Service:  service,
		Event:    event,
		FromApp:  from_app,
		Services: services,
		Content:  content,
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
