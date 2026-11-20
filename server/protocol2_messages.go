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
	"time"

	p2p_network "github.com/libp2p/go-libp2p/core/network"
)

const (
	// receiver_replies_buffer — depth of the per-stream replies
	// channel. Drain-and-batch will coalesce whatever's queued into
	// one ack frame; smaller depth = smaller batches but less risk of
	// stalling workers when the writer is busy.
	receiver_replies_buffer = 256

	// receiver_acks_maximum — ids in one ack frame. Batches are split here so
	// an ack stays well inside frame_control_maximum, which the far side's
	// frame_read holds every frame but a message to.
	receiver_acks_maximum = 256

	// receiver_streams_maximum — inbound messages streams kept per peer. A
	// sender runs one; the second covers it reconnecting before its old stream
	// is noticed dead. A stream past it resets the peer's oldest. libp2p is
	// held one above (net_start), so the stream that resets the oldest arrives.
	receiver_streams_maximum = 2

	// receiver_drain_timeout — how long a Receiver told bye waits for the
	// replies of the messages it has handed to workers, and for the writer to
	// send what is queued, before it closes.
	receiver_drain_timeout = 5 * time.Second
)

// receivers holds each peer's open inbound messages streams, oldest first.
var (
	receivers      = map[string][]*Receiver{}
	receivers_lock sync.Mutex
)

// receivers_add records r as one of its peer's streams, resetting the peer's
// oldest when that takes it past receiver_streams_maximum.
func receivers_add(r *Receiver) {
	receivers_lock.Lock()
	open := append(receivers[r.peer], r)
	var oldest *Receiver
	if len(open) > receiver_streams_maximum {
		oldest = open[0]
		open = open[1:]
	}
	receivers[r.peer] = open
	receivers_lock.Unlock()
	if oldest != nil {
		info("Messages: peer=%q has over %d streams open; resetting its oldest, session=%s", r.peer, receiver_streams_maximum, oldest.session)
		oldest.stream.Reset()
	}
}

// receivers_remove forgets r once its stream has ended.
func receivers_remove(r *Receiver) {
	receivers_lock.Lock()
	defer receivers_lock.Unlock()
	open := receivers[r.peer]
	for i, other := range open {
		if other == r {
			open = append(open[:i:i], open[i+1:]...)
			break
		}
	}
	if len(open) == 0 {
		delete(receivers, r.peer)
		return
	}
	receivers[r.peer] = open
}

// receiver_codecs returns the codecs this build supports as a receiver
// for the hello advertisement. zstd is always implicit (every receiver
// MUST decode it) but listing it explicitly keeps the intersection
// logic uniform.
func receiver_codecs() []string {
	return []string{"zstd"}
}

// receiver_features returns the capability flags this build supports.
func receiver_features() []string {
	return []string{feature_connection}
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
	challenge []byte // ours, in hello; the sender signs it to claim its entities
	offered   []byte // the sender's, from caps; we sign it to prove ours
	caps_seen atomic.Bool
	replies   chan *Frame
	claimed   map[string]bool
	lock      sync.Mutex // guards claimed
	closed    atomic.Bool
	done      chan struct{} // closed at end-of-stream; write_replies drains what is queued and exits on it
	written   chan struct{} // closed by write_replies as it exits; nil when nothing waits on it

	// outstanding counts the replies owed for messages handed to workers, or
	// waiting on another copy's outcome, and not yet answered: what a sender
	// that says bye is waiting for.
	outstanding atomic.Int64

	// Budgets for the frames that cost this host work before the sender has
	// sent anything. Touched only from the reader goroutine, so unguarded.
	claims      int
	proves      int
	proved      map[string]bool // entities this stream proved: true until a message to it backs the proof
	undecodable int             // message frames answered for not decoding (refuse)
	ready       bool            // a message frame has arrived; the read deadline rolls per frame
	delivered   bool            // a message has been handed to a worker
}

// wire_deadline is the part of a real libp2p stream that bounds a read. The
// in-memory test shim does not implement it, so the assertion below is optional.
type wire_deadline interface {
	SetReadDeadline(time.Time) error
}

// deadline bounds reads: a fixed window until the first message frame, then a
// rolling idle window refreshed on every frame. A live sender pings well
// inside that window, so only a genuinely silent stream trips it.
func (r *Receiver) deadline(t time.Time) {
	if s, ok := r.stream.(wire_deadline); ok {
		_ = s.SetReadDeadline(t)
	}
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

	// The connection's challenge, the one its /mochi/2/stream receiver gives
	// too: a claim binds the protocol, so it cannot move between the two.
	challenge, err := hello_connection_challenge(s.Conn())
	if err != nil {
		warn("Messages: challenge failed for peer %q: %v", peer, err)
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
		done:      make(chan struct{}),
		written:   make(chan struct{}),
		claimed:   map[string]bool{},
	}

	// One sender needs one stream; a peer holding more is holding slots of a
	// protocol whose inbound limit every other server shares.
	receivers_add(r)
	defer receivers_remove(r)

	if err := hello_write(s, 2, session, challenge, receiver_codecs(), receiver_features()); err != nil {
		info("Messages: hello write failed for peer %q session=%s: %v", peer, session, err)
		s.Reset()
		return
	}
	debug("Messages: stream open peer=%q session=%s", peer, session)

	// Spawn the reply writer; it lives until `done` closes, then writes what is
	// queued and exits.
	go r.write_replies()

	// Bound the phase before the first message the way /mochi/2/stream bounds the
	// phase before open: until a message arrives the peer has spent nothing and
	// this host a verify per claim and a signature per prove.
	r.deadline(time.Now().Add(messages_ready_timeout))

	// Reader runs inline. A sender says bye to collect the replies to what it
	// has sent: those of messages still with their workers are waited for.
	if r.read_loop() {
		r.drain(receiver_drain_timeout)
	}

	r.closed.Store(true)
	// `done`, not `replies`. Workers check closed then select a send on
	// replies; closing it made that a send on a closed channel, which the
	// handler recover reported as fail_handler_panic - a warn with a stack,
	// which mails the admin, blaming an app that did nothing wrong.
	close(r.done)
	// The writer sends what is queued before the stream closes under it.
	select {
	case <-r.written:
	case <-time.After(receiver_drain_timeout):
	}
	s.Close()
	debug("Messages: stream closed peer=%q session=%s", peer, session)

	// A registry entry follows evidence of a Mochi server: a claim that
	// verified or a message a handler took. Any libp2p node can open a stream
	// and close it again.
	r.lock.Lock()
	claimed := len(r.claimed) > 0
	r.lock.Unlock()
	if claimed || r.delivered {
		peer_observed(peer, s.Conn())
	}
}

// drain waits, up to timeout, for every reply owed on the stream to have been
// answered (outstanding), the writer still running.
func (r *Receiver) drain(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for r.outstanding.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// awaiting is the reply target for a message this stream owes a reply,
// counted in outstanding until it is answered.
func (r *Receiver) awaiting(id string) stream_reply {
	r.outstanding.Add(1)
	return stream_reply{receiver: r, message: id}
}

// read_loop reads frames from the stream until end-of-stream or a
// fatal framing error, and reports whether it ended on the sender's bye.
// Caps MUST be the first frame from the sender; non-caps before caps closes
// the stream.
func (r *Receiver) read_loop() bool {
	for {
		f, err := frame_read(r.stream)
		if errors.Is(err, io.EOF) {
			return false
		}
		var undecodable *frame_undecodable
		if errors.As(err, &undecodable) && r.refuse(undecodable) {
			if r.ready {
				r.deadline(time.Now().Add(receiver_idle_timeout()))
			}
			continue
		}
		if err != nil {
			info("Messages: framing error peer=%q session=%s: %v", r.peer, r.session, err)
			r.stream.Reset()
			return false
		}
		// Rolling idle bound once the stream is established. Clearing the
		// deadline for good meant one message bought a stream - and its two
		// goroutines and replies channel - for the life of the connection.
		if r.ready {
			r.deadline(time.Now().Add(receiver_idle_timeout()))
		}
		if !r.caps_seen.Load() {
			if f.Type != frame_type_caps {
				info("Messages: protocol violation peer=%q session=%s — first frame was %q, want caps", r.peer, r.session, f.Type)
				r.stream.Reset()
				return false
			}
			// The opener's challenge, which this host signs to prove it
			// holds an entity the sender addresses. Mandatory, same as on
			// /mochi/2/stream — a sender must not be able to skip being
			// offered the proof.
			if err := frame_reject_challenge(f.Challenge); err != nil {
				info("Messages: caps challenge rejected peer=%q session=%s: %v", r.peer, r.session, err)
				r.stream.Reset()
				return false
			}
			r.offered = f.Challenge
			r.caps_seen.Store(true)
			peer_greet(r.peer)
			continue
		}
		if !r.handle(f) {
			return f.Type == frame_type_bye
		}
	}
}

// refuse answers a message frame whose body would not decode with a
// fail{unsupported} naming it, the reason every sender drops at once, and
// reports whether the stream carries on. The rest of the stream is unaffected:
// the frame's length bounded it and it has been read. Only a message is
// answered, only after caps, and at most messages_undecodable_maximum a
// stream; anything else resets the stream as any framing error does.
//
// Only a body this host will not decode is refused: one over a cap on array
// elements, map pairs, nesting or data items, or not CBOR at all. That is what
// a sender's frame_check stops, so no receiver will ever take it, and the
// sender may as well drop it now. A body that passes them but will not decode
// into a Frame - a field of a type this build does not expect - is version
// skew, not the message: the stream resets as before, the sender retries, and
// the message arrives once this side understands it.
//
// A refused message backs a prove as handle's messages do (Receiver.back), the
// rule the sender counts by. It is handed to no handler, so it returns no claim.
func (r *Receiver) refuse(failure *frame_undecodable) bool {
	if !r.caps_seen.Load() {
		return false
	}
	if cbor_check(failure.body) == nil {
		return false
	}
	head, ok := frame_identify(failure.body)
	if !ok || head.Type != frame_type_message || head.ID == "" || !envelope_valid("", "", "", head.ID, "", nil) || !envelope_target_valid(head.To) {
		return false
	}
	r.undecodable++
	if r.undecodable > messages_undecodable_maximum {
		info("Messages: too many undecodable messages peer=%q session=%s — over %d", r.peer, r.session, messages_undecodable_maximum)
		return false
	}
	r.back(head.To)
	// The read deadline then rolls to the idle window, in read_loop.
	r.ready = true
	info("Messages: refusing undecodable message %q peer=%q session=%s: %v", head.ID, r.peer, r.session, failure)
	r.reply(&Frame{Type: frame_type_fail, Replies: []string{head.ID}, Reason: fail_unsupported})
	return true
}

// back returns one prove to the budget when a message is the first to an
// entity this stream proved since that proof: what the proof was asked for. Any
// message at all used to, so a peer alternating a prove with an empty message
// had this host sign with its users' keys at wire speed. The sender counts by
// the same rule (Sender.write_one), so the two budgets stay in step.
func (r *Receiver) back(to string) {
	if to == "" || !r.proved[to] {
		return
	}
	r.proved[to] = false
	if r.proves > 0 {
		r.proves--
	}
}

// handle dispatches one inbound frame. Returns false to terminate the
// read loop (used by `bye` and protocol violations).
func (r *Receiver) handle(f *Frame) bool {
	// Checked before anything keys a map on Frame.Service or Frame.ID. A peer
	// sending a malformed envelope gets the stream closed, not a frame failure.
	if !envelope_valid(f.From, f.Service, f.Event, f.ID, f.FromApp, f.Services) || !envelope_target_valid(f.To) {
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
		r.claims++
		if r.claims > messages_claims_maximum {
			info("Messages: too many claims peer=%q session=%s — over %d", r.peer, r.session, messages_claims_maximum)
			r.stream.Reset()
			return false
		}
		if err := claim_verify(f.From, r.challenge, f.Signature, net_id, protocol_messages); err != nil {
			// A claim that does not verify is not a retry, it is a peer spending
			// our signature checks on nothing. Matches /mochi/2/stream, which used
			// to continue here too.
			info("Messages: claim verify failed peer=%q session=%s entity=%q: %v",
				r.peer, r.session, f.From, err)
			r.stream.Reset()
			return false
		}
		r.lock.Lock()
		r.claimed[f.From] = true
		r.lock.Unlock()
		return true

	case frame_type_prove:
		// Mirror of the claim above: the sender proves who it speaks for, this host
		// proves who it answers for before the sender will send. Budgeted, because
		// each one costs a lookup and a real entity signature.
		r.proves++
		if r.proves > messages_proves_maximum {
			info("Messages: too many proves peer=%q session=%s — over %d", r.peer, r.session, messages_proves_maximum)
			r.stream.Reset()
			return false
		}
		r.prove(f.To)
		return true

	case frame_type_message:
		// Backs a prove whatever becomes of the message: the sending side
		// counts each it writes and cannot know which this side delivers.
		r.back(f.To)
		if !r.ready {
			r.ready = true
			r.deadline(time.Now().Add(receiver_idle_timeout()))
		}
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

// credit returns one claim budget once a message shows the peer is doing the
// work that budget exists to gate: a message from the claimed entity that a
// handler takes, or a copy of one it took. Floored at zero - it is an allowance
// for claims the peer has not backed, not a running total. A message that goes
// nowhere returns nothing, or a peer alternating a claim for a minted entity
// with junk from it grows `claimed` without limit.
func (r *Receiver) credit() {
	if r.claims > 0 {
		r.claims--
	}
}

// dispatch_message checks one message frame's claim, decides whether it can be
// delivered (event_admit) and whether it is a copy of one already seen, and
// pushes it onto the worker of the (user, app) it resolved to. Nothing is
// marked, created or decompressed for a frame that cannot be delivered.
func (r *Receiver) dispatch_message(f *Frame) {
	r.lock.Lock()
	claimed := r.claimed[f.From]
	r.lock.Unlock()
	if !claimed && f.From != "" {
		r.reply(&Frame{Type: frame_type_fail, Replies: []string{f.ID}, Reason: fail_unclaimed})
		return
	}
	// Only a frame whose sender is proven takes or reads a dedup record: an id
	// from no one may be anyone's, and marking it lets a peer with no
	// credentials pre-empt or flush the records honest senders rely on.
	tracked := claimed && f.ID != ""

	// A copy of a message already settled is answered as the first was, even
	// were it no longer deliverable: it was delivered.
	if tracked {
		switch state, reason := message_state(f.ID); state {
		case message_applied:
			r.credit()
			r.reply(&Frame{Type: frame_type_ack, Replies: []string{f.ID}})
			return
		case message_refused:
			r.reply(&Frame{Type: frame_type_fail, Replies: []string{f.ID}, Reason: reason})
			return
		}
	}

	to, user, ok := target_resolve(f.To)
	if !ok {
		r.reply(&Frame{Type: frame_type_fail, Replies: []string{f.ID}, Reason: fail_unknown_user})
		return
	}
	f.To = to
	a, reason := event_admit(user, to, f.Service, f.Event, f.From)
	if reason != "" {
		r.reply(&Frame{Type: frame_type_fail, Replies: []string{f.ID}, Reason: reason})
		return
	}
	if user != nil && claimed {
		// Learn the claim-verified (sender entity, peer) pair into the
		// recipient's directory — the routing memory that makes private
		// entities deliverable (directory_user.go). The claim check
		// above is the gate: f.From is proven, not just asserted.
		directory_user_learn(user, f.From, r.peer)
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

	reply := r.awaiting(f.ID)
	// Check and mark in one step immediately before handing off: worker_dispatch
	// blocks on a full inbox, and a retry arriving in that gap applies twice. A
	// copy arriving while the first still runs waits for its outcome: acked now,
	// a later retryable failure of the first would lose the message.
	if tracked {
		switch state, reason := message_begin(f.ID, r.peer, reply); state {
		case message_running:
			if reason != "" {
				reply.fail(reason)
				return
			}
			debug("Messages: message %q is running, the copy waits peer=%q", f.ID, r.peer)
			r.credit()
			return
		case message_applied:
			r.credit()
			reply.ack()
			return
		case message_refused:
			reply.fail(reason)
			return
		}
	}

	key := ""
	if user != nil {
		key = user.UID
	}
	wf := &worker_frame{
		frame:  f,
		peer:   r.peer, // sender's libp2p peer ID
		user:   user,
		reply:  reply,
		marked: tracked,
	}
	if !worker_dispatch(key, a.id, wf) {
		worker_fail(wf, fail_buffer_full)
		return
	}
	r.delivered = true
	if claimed {
		// The claim did the thing a claim is for, so its budget comes back.
		// Without this a peer speaking for more entities than the cap loses
		// delivery part-way through a session.
		r.credit()
	}
}

// prove answers a `prove` demand for `entity`. fail_unknown_user means it is
// not hosted here; fail_unproven means it is but the key is missing, which must
// not be downgraded into serving the entity unauthenticated. A proof given is
// recorded, for the message to the entity that backs it (Receiver.back).
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
	if r.proved == nil {
		r.proved = map[string]bool{}
	}
	r.proved[entity] = true
	r.reply(&Frame{Type: frame_type_claim, From: entity, Signature: signature})
}

// reply posts a frame onto the per-stream replies channel, dropping it if the
// channel is full - the sender's sweeper times the inflight out and retries.
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
// grow naturally under load. Once `done` closes it writes what is still queued,
// so the replies a bye waited for are not left in the channel, and exits.
func (r *Receiver) write_replies() {
	if r.written != nil {
		defer close(r.written)
	}
	pending_acks := make([]string, 0, 64)

	for {
		var first *Frame
		var open bool
		select {
		case first, open = <-r.replies:
			// A closed replies channel is the older teardown signal; the
			// reader now closes `done` instead, but tests still close this one
			// and a closed channel yields a nil frame forever.
			if !open {
				r.flush(&pending_acks)
				return
			}
		case <-r.done:
			r.drain_replies(&pending_acks)
			r.flush(&pending_acks)
			return
		}
		r.coalesce_one(first, &pending_acks)
		if !r.drain_replies(&pending_acks) {
			r.flush(&pending_acks)
			return
		}
		r.flush(&pending_acks)
	}
}

// drain_replies coalesces whatever is queued now, and reports false once the
// replies channel has closed.
func (r *Receiver) drain_replies(pending_acks *[]string) bool {
	for {
		select {
		case extra, more := <-r.replies:
			if !more {
				return false
			}
			r.coalesce_one(extra, pending_acks)
		default:
			return true
		}
	}
}

// flush writes the acks gathered so far as one frame.
func (r *Receiver) flush(pending_acks *[]string) {
	if len(*pending_acks) == 0 {
		return
	}
	ack := &Frame{Type: frame_type_ack, Replies: *pending_acks}
	if err := frame_write(r.stream, ack); err != nil {
		debug("Messages: ack write failed peer=%q: %v", r.peer, err)
	}
	*pending_acks = (*pending_acks)[:0]
}

// coalesce_one merges ack frames into pending_acks, written once they reach
// receiver_acks_maximum; any other frame flushes the acks first, then ships
// standalone, so a sender always sees an ack before anything queued after it.
// A frame type missing from the switch is dropped.
func (r *Receiver) coalesce_one(f *Frame, pending_acks *[]string) {
	if f.Type == frame_type_ack {
		for _, id := range f.Replies {
			*pending_acks = append(*pending_acks, id)
			if len(*pending_acks) >= receiver_acks_maximum {
				r.flush(pending_acks)
			}
		}
		return
	}
	// Non-ack: flush whatever's queued so the sender sees the acks
	// before the subsequent frame.
	r.flush(pending_acks)
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
