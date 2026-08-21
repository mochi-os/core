// Mochi server: Protocol 2 — /mochi/2/messages receiver.
//
// One libp2p stream per (peer, direction); many messages multiplexed
// over the lifetime of the stream. The receiver:
//
//   1. Writes hello (challenge + version + session + codecs + features).
//   2. Reads caps (mandatory first sender frame).
//   3. Reads message / claim / ping / bye frames, dispatching to the
//      per-host (user, app) worker pool.
//   4. Replies via the drain-and-batch ack writer.
//
// See claude/plans/protocol2.md → Receiver architecture.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	p2p_network "github.com/libp2p/go-libp2p/core/network"
)

const (
	// Capability flags this build advertises in hello.Features /
	// caps.Features. Empty in v2 baseline — every feature gets added
	// here and gated by intersection checks.
	//lint:ignore U1000 the capability-flag baseline: empty in v2, and every future feature is added here and gated by intersection
	receiver_features_default = ""

	// receiver_replies_buffer — depth of the per-stream replies
	// channel. Drain-and-batch will coalesce whatever's queued into
	// one ack frame; smaller depth = smaller batches but less risk of
	// stalling workers when the writer is busy.
	receiver_replies_buffer = 256
)

// receiver_codecs returns the codecs this build supports as a receiver
// for the hello advertisement. zstd is always implicit (every receiver
// MUST decode it) but listing it explicitly keeps the intersection
// logic uniform.
func receiver_codecs() []string {
	return []string{"zstd"}
}

// receiver_features returns the capability flags this build supports.
// Empty for now; future features get added here.
func receiver_features() []string {
	return nil
}

// wire_stream is the subset of p2p_network.Stream the /mochi/2
// state machines (Sender + Receiver) actually need. Production
// callers pass a real libp2p stream; tests pass an in-memory shim.
type wire_stream interface {
	io.Reader
	io.Writer
	Reset() error
}

// Receiver owns one inbound /mochi/2/messages stream. Reader,
// reply-writer, and the per-host worker pool all share this struct via
// reply_target callbacks.
type Receiver struct {
	peer      string
	stream    wire_stream
	session   string
	challenge []byte   // ours, in hello; the sender signs it to claim its entities
	offered   []byte   // the sender's, from caps; we sign it to prove ours
	codecs    []string // sender's advertised codecs from caps
	features  []string // sender's advertised features from caps
	caps_seen atomic.Bool
	replies   chan *Frame
	claimed   map[string]bool
	lock      sync.Mutex // guards claimed
	closed    atomic.Bool
}

// receive_messages is the libp2p stream handler for /mochi/2/messages,
// registered in net_start and run on a fresh goroutine per stream. Guarded: a
// panic below here reaches the top of a libp2p goroutine and ends the process.
func receive_messages(s p2p_network.Stream) {
	guard("receive_messages", func() { s.Reset() }, func() { receive_messages_guarded(s) })
}

func receive_messages_guarded(s p2p_network.Stream) {
	peer := s.Conn().RemotePeer().String()

	// Rate limit incoming streams per peer (skip bootstrap peers — trusted
	// infrastructure, not anonymous senders).
	if !peer_is_bootstrap(peer) && !rate_limit_p2p.allow(peer) {
		debug("Messages rate limited peer %q", peer)
		s.Reset()
		return
	}

	challenge, err := hello_challenge()
	if err != nil {
		warn("Messages: challenge entropy failed for peer %q: %v", peer, err)
		s.Reset()
		return
	}

	session := session_id()
	r := &Receiver{
		peer:      peer,
		stream:    s,
		session:   session,
		challenge: challenge,
		replies:   make(chan *Frame, receiver_replies_buffer),
		claimed:   map[string]bool{},
	}

	if err := hello_write(s, 2, session, challenge, receiver_codecs(), receiver_features()); err != nil {
		info("Messages: hello write failed for peer %q session=%s: %v", peer, session, err)
		s.Reset()
		return
	}
	debug("Messages: stream open peer=%q session=%s", peer, session)

	// Spawn the reply writer; it lives until the replies channel
	// closes (the reader signals end-of-stream by closing replies).
	go r.write_replies()

	// Reader runs inline; on return we close the stream and let the
	// reply writer drain whatever remains.
	r.read_loop()

	r.closed.Store(true)
	close(r.replies)
	s.Close()
	debug("Messages: stream closed peer=%q session=%s", peer, session)

	peer_discovered_address(peer, s.Conn().RemoteMultiaddr().String()+"/p2p/"+peer)
}

// read_loop reads frames from the stream until end-of-stream or a
// fatal framing error. Caps MUST be the first frame from the sender;
// non-caps before caps closes the stream.
func (r *Receiver) read_loop() {
	for {
		f, err := frame_read(r.stream)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			info("Messages: framing error peer=%q session=%s: %v", r.peer, r.session, err)
			r.stream.Reset()
			return
		}
		if !r.caps_seen.Load() {
			if f.Type != frame_type_caps {
				info("Messages: protocol violation peer=%q session=%s — first frame was %q, want caps", r.peer, r.session, f.Type)
				r.stream.Reset()
				return
			}
			r.codecs = f.Codecs
			r.features = f.Features
			// The opener's challenge, which this host signs to prove it
			// holds an entity the sender addresses. Mandatory, same as on
			// /mochi/2/stream — a sender must not be able to skip being
			// offered the proof.
			if err := frame_reject_challenge(f.Challenge); err != nil {
				info("Messages: caps challenge rejected peer=%q session=%s: %v", r.peer, r.session, err)
				r.stream.Reset()
				return
			}
			r.offered = f.Challenge
			r.caps_seen.Store(true)
			continue
		}
		if !r.handle(f) {
			return
		}
	}
}

// handle dispatches one inbound frame. Returns false to terminate the
// read loop (used by `bye` and protocol violations).
func (r *Receiver) handle(f *Frame) bool {
	// Checked before anything keys a map on Frame.Service or Frame.ID. A peer
	// sending a malformed envelope gets the stream closed, not a frame failure.
	if !envelope_valid(f.From, f.Service, f.Event, f.ID) {
		info("Messages: protocol violation peer=%q session=%s — malformed envelope (service=%d bytes, id=%d bytes)",
			r.peer, r.session, len(f.Service), len(f.ID))
		r.stream.Reset()
		return false
	}

	switch f.Type {
	case frame_type_caps:
		// Second caps frame mid-stream is a protocol violation.
		info("Messages: protocol violation peer=%q session=%s — second caps frame", r.peer, r.session)
		r.stream.Reset()
		return false

	case frame_type_claim:
		if err := claim_verify(f.From, r.challenge, f.Signature, net_id, protocol_messages); err != nil {
			// Don't fail the claim explicitly — the next message from
			// the unclaimed entity will fail naturally with unclaimed
			// and the sender re-issues. Logging is enough.
			info("Messages: claim verify failed peer=%q session=%s entity=%q: %v",
				r.peer, r.session, f.From, err)
			return true
		}
		r.lock.Lock()
		r.claimed[f.From] = true
		r.lock.Unlock()
		return true

	case frame_type_prove:
		// Mirror of the claim above: the sender proves who it speaks for, this host
		// proves who it answers for before the sender will send.
		r.prove(f.To)
		return true

	case frame_type_message:
		r.dispatch_message(f)
		return true

	case frame_type_ping:
		r.reply(&Frame{Type: frame_type_pong, ID: f.ID})
		return true

	case frame_type_pong:
		// Receiver-side pong: we don't currently send pings inbound
		// from the receiver, but if a future feature does, the sender
		// echo arrives here. Silently ignore.
		return true

	case frame_type_bye:
		debug("Messages: bye peer=%q session=%s", r.peer, r.session)
		return false

	case frame_type_ack, frame_type_fail:
		// Receiver doesn't track inflight; orphan acks are senders'
		// problem. Log + drop.
		debug("Messages: unexpected %q frame from peer=%q session=%s", f.Type, r.peer, r.session)
		return true

	case frame_type_hello, frame_type_open:
		info("Messages: protocol violation peer=%q session=%s — %q frame on messages stream", r.peer, r.session, f.Type)
		r.stream.Reset()
		return false
	}
	// frame_type_known() already filtered unknowns at frame_read time.
	return true
}

// dispatch_message decodes one message frame, checks per-(stream,
// entity) claim, decompresses content/data, and pushes onto the
// per-(user, app) worker.
func (r *Receiver) dispatch_message(f *Frame) {
	// Fast path: a known duplicate acks without touching the claim or codec
	// checks below. Read-only - the authoritative check-and-mark happens
	// atomically at the dispatch commit point.
	if f.ID != "" && message_seen(f.ID) {
		debug("Messages: duplicate message %q, ack only peer=%q", f.ID, r.peer)
		r.reply(&Frame{Type: frame_type_ack, Replies: []string{f.ID}})
		return
	}

	r.lock.Lock()
	claimed := r.claimed[f.From]
	r.lock.Unlock()
	if !claimed && f.From != "" {
		r.reply(&Frame{Type: frame_type_fail, Replies: []string{f.ID}, Reason: fail_unclaimed})
		return
	}

	// Decompress data/content per Codec. Decoded content is already
	// in f.Content (CBOR-decoded by frame_read); only the optional
	// blob in f.Data needs codec handling.
	if f.Codec != codec_none {
		if !frame_codec_supported(f.Codec) {
			r.reply(&Frame{Type: frame_type_fail, Replies: []string{f.ID}, Reason: fail_unsupported})
			return
		}
		if len(f.Data) > 0 {
			plain, err := frame_decompress(f.Data, f.Codec)
			if err != nil {
				info("Messages: decompress failed peer=%q id=%q: %v", r.peer, f.ID, err)
				r.reply(&Frame{Type: frame_type_fail, Replies: []string{f.ID}, Reason: fail_unsupported})
				return
			}
			f.Data = plain
		}
	}

	// The user we serve owns f.To. The worker key is the service name, not the app
	// e.route() picks, so frames for the same logical app serialise.
	to := f.To
	if to != "" && valid(to, "fingerprint") {
		if ent := entity_by_any(to); ent != nil {
			to = ent.ID
		}
	}
	user := ""
	if to != "" {
		if u := user_owning_entity(to); u != nil {
			user = u.UID
			// Learn the claim-verified (sender entity, peer) pair into the
			// recipient's directory — the routing memory that makes private
			// entities deliverable (directory_user.go). The claim check
			// above is the gate: f.From is proven, not just asserted.
			directory_user_learn(u, f.From, r.peer)
		}
	}

	// Check and mark in one step immediately before handing off: worker_dispatch
	// blocks on a full inbox, and a retry arriving in that gap applies twice. Must
	// stay after the claim and codec checks, whose failures ask for a retry.
	if f.ID != "" && message_seen_mark(f.ID) {
		debug("Messages: duplicate message %q, ack only peer=%q", f.ID, r.peer)
		r.reply(&Frame{Type: frame_type_ack, Replies: []string{f.ID}})
		return
	}

	worker_dispatch(user, f.Service, &worker_frame{
		frame: f,
		peer:  r.peer, // sender's libp2p peer ID
		reply: stream_reply{receiver: r, message: f.ID},
	})
}

// reply posts a frame onto the per-stream replies channel, dropping it if the
// channel is full - the sender's sweeper times the inflight out and retries.
//
// prove answers a `prove` demand for `entity`. fail_unknown_user means it is
// not hosted here; fail_unproven means it is but the key is missing, which must
// not be downgraded into serving the entity unauthenticated.
func (r *Receiver) prove(entity string) {
	if entity == "" || !valid(entity, "entity") {
		r.reply(&Frame{Type: frame_type_fail, From: entity, Reason: fail_unknown_user})
		return
	}
	if user_owning_entity(entity) == nil {
		r.reply(&Frame{Type: frame_type_fail, From: entity, Reason: fail_unknown_user})
		return
	}
	signature := responder_sign(entity, r.offered, r.peer, protocol_messages)
	if signature == nil {
		info("Messages: cannot prove entity %q to peer=%q session=%s", entity, r.peer, r.session)
		r.reply(&Frame{Type: frame_type_fail, From: entity, Reason: fail_unproven})
		return
	}
	r.reply(&Frame{Type: frame_type_claim, From: entity, Signature: signature})
}

func (r *Receiver) reply(f *Frame) {
	if r.closed.Load() {
		return
	}
	select {
	case r.replies <- f:
	default:
		debug("Messages: dropping reply type=%q peer=%q (channel full)", f.Type, r.peer)
	}
}

// write_replies is the per-stream reply writer: block on the first frame, then
// drain whatever is ready, coalescing acks into one frame. No timer - batches
// grow naturally under load.
func (r *Receiver) write_replies() {
	pending_acks := make([]string, 0, 64)

	flush_acks := func() {
		if len(pending_acks) == 0 {
			return
		}
		f := &Frame{Type: frame_type_ack, Replies: pending_acks}
		if err := frame_write(r.stream, f); err != nil {
			debug("Messages: ack write failed peer=%q: %v", r.peer, err)
		}
		pending_acks = pending_acks[:0]
	}

	for first := range r.replies {
		r.coalesce_one(first, &pending_acks)
		// Drain whatever else is immediately ready.
	drain:
		for {
			select {
			case extra, ok := <-r.replies:
				if !ok {
					flush_acks()
					return
				}
				r.coalesce_one(extra, &pending_acks)
			default:
				break drain
			}
		}
		flush_acks()
	}
}

// coalesce_one merges ack frames into pending_acks; any other frame flushes the
// acks first, then ships standalone, so a sender always sees an ack before
// anything queued after it. A frame type missing from the switch is dropped.
func (r *Receiver) coalesce_one(f *Frame, pending_acks *[]string) {
	if f.Type == frame_type_ack {
		for _, id := range f.Replies {
			*pending_acks = append(*pending_acks, id)
		}
		return
	}
	// Non-ack: flush whatever's queued so the sender sees the acks
	// before the subsequent frame.
	if len(*pending_acks) > 0 {
		ack := &Frame{Type: frame_type_ack, Replies: *pending_acks}
		if err := frame_write(r.stream, ack); err != nil {
			debug("Messages: ack write failed peer=%q: %v", r.peer, err)
		}
		*pending_acks = (*pending_acks)[:0]
	}
	switch f.Type {
	case frame_type_fail, frame_type_pong, frame_type_claim:
		if err := frame_write(r.stream, f); err != nil {
			debug("Messages: %q write failed peer=%q: %v", f.Type, r.peer, err)
		}
	default:
		// Shouldn't see other types here (reader doesn't enqueue them)
		// but be defensive.
		debug("Messages: unexpected reply type=%q peer=%q", f.Type, r.peer)
	}
}
