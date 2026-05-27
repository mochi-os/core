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
// Copyright Alistair Cunningham 2026

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
	challenge []byte
	codecs    []string // sender's advertised codecs from caps
	features  []string // sender's advertised features from caps
	caps_seen atomic.Bool
	replies   chan *Frame
	claimed   map[string]bool
	lock      sync.Mutex // guards claimed
	closed    atomic.Bool
}

// receive_messages is the libp2p stream handler registered for
// /mochi/2/messages in net_start. Runs in a fresh goroutine per stream.
func receive_messages(s p2p_network.Stream) {
	peer := s.Conn().RemotePeer().String()

	// Rate limit incoming streams per peer (skip bootstrap and paired
	// peers — both are trusted infrastructure, not anonymous senders).
	// Same gate as net_receive_1's /mochi/1 handler.
	if !peer_is_bootstrap(peer) && !peer_is_pair(peer) && !rate_limit_p2p.allow(peer) {
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
	switch f.Type {
	case frame_type_caps:
		// Second caps frame mid-stream is a protocol violation.
		info("Messages: protocol violation peer=%q session=%s — second caps frame", r.peer, r.session)
		r.stream.Reset()
		return false

	case frame_type_claim:
		if err := claim_verify(f.From, r.challenge, f.Signature); err != nil {
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
	// Dedup against the sticky message_seen cache. ID == "" frames
	// can't dedup; they ack/fail as normal but might double-apply on
	// a retry — current /mochi/1 has the same property.
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

	// Resolve (user, app). The user we serve owns f.To. The handler
	// dispatch (e.route()) picks the right app — we use the service
	// name as the worker key so frames addressed to the same logical
	// app serialise even if a different (sub-)app would also have
	// answered.
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
		}
	}

	worker_dispatch(user, f.Service, &worker_frame{
		frame: f,
		peer:  r.peer, // sender's libp2p peer ID
		reply: stream_reply{receiver: r, id: f.ID},
	})

	if f.ID != "" {
		// Mark as seen the moment we hand it off. The handler may
		// fail and we'll ack/fail accordingly — but a duplicate arriving
		// while the first copy is mid-flight gets the dedup ack rather
		// than racing the worker.
		message_mark_seen(f.ID)
	}
}

// reply posts a frame onto the per-stream replies channel. Drops the
// frame (with a debug log) if the channel is full — same recovery
// path as a dropped stream: sender's sweeper times the inflight out,
// retry, receiver_mark_seen catches the dup.
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

// write_replies is the per-stream reply writer. Implements the
// drain-and-batch policy: block on the first frame, then non-blockingly
// pull every other frame ready right now; coalesce all acks into one
// ack frame; flush any fail (carries a single Reason, so they don't
// batch) on its own. No timer-based waiting — batches grow naturally
// under load, look identical to per-message acks at low load.
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

// coalesce_one is the per-frame branch inside write_replies' drain
// loop: ack frames merge into pending_acks (one ack frame per drain
// batch); any other frame (fail, pong) flushes the accumulated acks
// first then ships standalone. Preserves wire ordering — a sender
// observing an ack always sees it before any frame queued after the
// ack, so inflight-resolution stays consistent with arrival order.
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
	case frame_type_fail, frame_type_pong:
		if err := frame_write(r.stream, f); err != nil {
			debug("Messages: %q write failed peer=%q: %v", f.Type, r.peer, err)
		}
	default:
		// Shouldn't see other types here (reader doesn't enqueue them)
		// but be defensive.
		debug("Messages: unexpected reply type=%q peer=%q", f.Type, r.peer)
	}
}
