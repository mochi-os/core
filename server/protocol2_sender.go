// Mochi server: Protocol 2 — /mochi/2/messages sender.
//
// One persistent Sender per (peer, outbound direction), lazily created
// on the first peer_send for that peer. Multiplexes many in-flight
// messages onto the same libp2p stream.
//
// Four goroutines per Sender:
//
//   sender_write  — pulls frames from outbox, writes caps before the
//                   first message, claims unclaimed entities, frames
//                   and ships the message frame.
//   sender_read   — reads ack / fail / ping / pong frames, resolves
//                   inflight, echoes pong on ping.
//   sender_sweep  — global ticker; times out stale inflight entries
//                   and unanswered pings.
//   sender_ping   — per-peer ticker; emits ping when the stream has
//                   been idle for peer_ping_interval.
//
// On any stream death the inflight map is drained to queue_fail and
// the Sender is torn down; next peer_send re-creates it.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_peer "github.com/libp2p/go-libp2p/core/peer"
	p2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	multistream "github.com/multiformats/go-multistream"

	cbor "github.com/fxamacker/cbor/v2"
)

// senders_init registers the /mochi/2 Sender invalidation hook. Called from
// main_serve, not init().
func senders_init() {
	// /mochi/2 owns per-peer Sender state in the Sender registry; it
	// needs invalidation on libp2p disconnect so the next reconnect
	// opens a fresh Sender. Register here so peers.go's peer_disconnected
	// stays decoupled from /mochi/2 internals.
	peer_register_disconnect_hook(senders_peer_invalidate)
}

const (
	// Sender outbox + inflight defaults. peer.window is the operator
	// tunable for the inflight cap; outbox is sized the same to absorb
	// short bursts without blocking queue_process.
	sender_window_default   = 256
	sender_outbox_default   = 256
	sender_send_timeout     = 2 * time.Second
	sender_inflight_timeout = 30
	sender_ping_interval    = 30 * time.Second
	sender_ping_timeout     = 90
)

// sender_proves_maximum is how many prove frames a Sender keeps unbacked by a
// message at once. The receiver resets the stream past
// messages_proves_maximum of them, counting each message as backing one, so
// half leaves room for the messages a proof is about to release.
const sender_proves_maximum = messages_proves_maximum / 2

// error_sender_proves is write_loop's answer when entities wait for a proof
// and the stream can ask for no more: its budget is spent on proofs that
// failed, and no message is left to back them. A fresh stream starts afresh.
var error_sender_proves = errors.New("sender: prove budget spent")

// peer_stream_open_timeout bounds net_me.NewStream so a stale "Connected" peer
// cannot hang the open (#47). A var, not a const, so tests can lower it.
var peer_stream_open_timeout = 10 * time.Second

// error_sender_unreachable is returned by peer_send when the target peer
// isn't reachable and the call can't even open a sender. queue_fail
// will re-queue with the usual backoff.
var error_sender_unreachable = errors.New("sender: peer unreachable")

// error_sender_full is returned by peer_send when the outbox channel is
// saturated past sender_send_timeout. Same recovery as a stream-open
// failure: queue_fail re-queues with exp backoff.
var error_sender_full = errors.New("sender: outbox full")

// pending tracks one outbound message awaiting ack. Keyed by message
// id on the Sender's inflight map.
type pending struct {
	queue string // queue.id this frame came from
	sent  int64  // unix seconds when written
}

// Sender owns one outbound /mochi/2/messages stream to a single peer.
// All fields except outbox are guarded by lock.
type Sender struct {
	peer      string
	stream    wire_stream
	session   string
	challenge []byte   // theirs, from hello; we sign it to claim our entities
	offered   []byte   // ours, in caps; they sign it to prove theirs
	codecs    []string // sender's effective codec set after intersection
	features  []string // sender's effective feature set after intersection
	outbox    chan *outbound
	inflight  map[string]*pending
	pings     map[string]int64
	claimed   map[string]bool
	// proven records entities the far side has shown it holds, for the life of
	// this connection - per entity, since a messages stream is multiplexed.
	// proving holds the entities asked and not yet answered; their answers
	// reach write_loop through proofs.
	proven       map[string]bool
	proving      map[string]bool
	proofs       chan proof
	parked       atomic.Int64 // messages write_loop holds for a proof; pull_loop counts them
	unbacked     int          // write_loop only: proves sent not yet backed by a message
	closed       atomic.Bool
	done         chan struct{} // closed by shutdown(); write_loop and peer_send exit on it
	last_inbound atomic.Int64  // unix ns of last inbound frame; reset by ping_loop
	wake         chan struct{} // queue_wake routes per-peer nudges here; pull_loop drains
	lock         sync.Mutex
	rate_lock    sync.Mutex
	rate_window  int64 // unix-second of current 1s bucket
	rate_count   int   // sends in the current bucket
}

// proof is the far side's answer to a prove frame: nil when it showed it holds
// entity.
type proof struct {
	entity string
	err    error
}

// waiting is write_loop's state for messages whose target is not yet proven:
// each such entity's messages in order, the entities asked with when their
// answer is due, and the entities that wait for the prove budget to ask.
type waiting struct {
	parked    map[string][]*outbound
	deadlines map[string]time.Time
	queued    []string
	bye       *outbound // held until the messages parked ahead of it are written
}

// outbound carries the original queue id alongside the wire frame so
// sender_write can register it in inflight before the write.
type outbound struct {
	frame *Frame
	queue string // queue.id; empty for synthetic frames like bye
}

var (
	senders         = map[string]*Sender{}
	senders_opening = map[string]*opening{}
	senders_lock    sync.Mutex
)

// senders_has reports whether an open Sender exists for `peer`. queue_process
// uses it to skip rows the Sender's pull_loop owns; competing for the outbox
// blocks peer_send for sender_send_timeout and drags out the whole tick.
func senders_has(peer string) bool {
	if peer == "" {
		return false
	}
	senders_lock.Lock()
	s := senders[peer]
	senders_lock.Unlock()
	return s != nil && !s.closed.Load()
}

// peer_send enqueues `frame` on the Sender for `peer`, creating it if needed.
// `queue` is the queue.id that originated the send; sender_read uses it to
// queue_ack / queue_fail / queue_drop when the receiver replies.
func peer_send(peer string, queue string, frame *Frame) error {
	s, err := sender_for(peer)
	if err != nil {
		return err
	}
	select {
	case s.outbox <- &outbound{frame: frame, queue: queue}:
		return nil
	case <-time.After(sender_send_timeout):
		return error_sender_full
	}
}

// sender_for finds-or-creates the Sender for peer. The open path is
// gated by senders_lock; the per-Sender state machine runs without it.
// Callers that find an open already under way wait for it and share its
// answer: a second dial meets the first one's connect in progress, and
// peer_connect counts that against the peer as a failure.
func sender_for(peer string) (*Sender, error) {
	senders_lock.Lock()
	s := senders[peer]
	if s != nil && !s.closed.Load() {
		senders_lock.Unlock()
		return s, nil
	}
	if o := senders_opening[peer]; o != nil {
		senders_lock.Unlock()
		<-o.done
		return o.sender, o.err
	}
	o := &opening{done: make(chan struct{})}
	senders_opening[peer] = o
	senders_lock.Unlock()

	open := sender_open
	if sender_opener != nil {
		open = sender_opener
	}
	o.sender, o.err = open(peer)
	senders_lock.Lock()
	delete(senders_opening, peer)
	senders_lock.Unlock()
	close(o.done)
	return o.sender, o.err
}

// senders_opening_has reports whether a Sender open for peer is under way.
func senders_opening_has(peer string) bool {
	senders_lock.Lock()
	defer senders_lock.Unlock()
	return senders_opening[peer] != nil
}

// opening is a Sender open under way, which other callers for the same peer
// wait on.
type opening struct {
	done   chan struct{}
	sender *Sender
	err    error
}

// sender_opener, when set, stands in for sender_open in sender_for.
var sender_opener func(string) (*Sender, error)

// sender_open establishes a new /mochi/2/messages stream to peer,
// runs the handshake (read hello → write caps), and starts the
// per-Sender goroutines. Returns error_sender_unreachable if the peer
// can't be reached or doesn't speak /mochi/2/messages.
func sender_open(peer string) (*Sender, error) {
	stream, err := peer_protocol_open(peer, protocol_messages)
	if err != nil {
		return nil, fmt.Errorf("sender: stream open failed: %w", err)
	}
	if stream == nil {
		return nil, error_sender_unreachable
	}

	// Bound the handshake, as the receiving side bounds its own pre-open phase.
	// Without this a peer that accepts the stream and then says nothing holds
	// this goroutine forever, and because sender_open runs under the outbound
	// queue that one peer stalls delivery to every other. Cleared once the
	// handshake completes: a messages stream is long-lived by design.
	_ = stream.SetReadDeadline(time.Now().Add(stream_open_timeout))

	hello, err := hello_read(stream, 2)
	if err != nil {
		stream.Reset()
		// A silent peer is unreachable, not a hard failure: report it as such so
		// the queue retries rather than discarding the message.
		if deadline_exceeded(err) {
			return nil, error_sender_unreachable
		}
		return nil, fmt.Errorf("sender: hello read failed peer=%q: %w", peer, err)
	}

	// Compute effective codec / feature sets.
	codecs := codec_intersect(receiver_codecs(), hello.Codecs)
	features := features_intersect(receiver_features(), hello.Features)
	// The connection's challenge serves its /mochi/2/stream opens too, which
	// can then skip their own hello.
	connection_learn(stream.Conn(), hello)

	// Every caps frame carries a challenge so the two /mochi/2 protocols share one
	// handshake. /mochi/2/stream consumes it at the ack; on the messages path the
	// per-target proof comes later and nothing reads this one yet.
	challenge, err := hello_challenge()
	if err != nil {
		stream.Reset()
		return nil, fmt.Errorf("sender: challenge entropy failed peer=%q: %w", peer, err)
	}

	if err := caps_write(stream, codecs, features, challenge); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("sender: caps write failed peer=%q: %w", peer, err)
	}
	_ = stream.SetReadDeadline(time.Time{})

	s := &Sender{
		done:      make(chan struct{}),
		peer:      peer,
		stream:    stream,
		session:   hello.Session,
		challenge: hello.Challenge,
		codecs:    codecs,
		features:  features,
		offered:   challenge,
		outbox:    make(chan *outbound, peer_outbox()),
		inflight:  map[string]*pending{},
		pings:     map[string]int64{},
		claimed:   map[string]bool{},
		proven:    map[string]bool{},
		proving:   map[string]bool{},
		proofs:    make(chan proof, 2*sender_proves_maximum),
		wake:      make(chan struct{}, 1),
	}

	senders_lock.Lock()
	// Race: another goroutine may have opened a Sender for the same
	// peer between our lookup and now. If so, prefer theirs.
	if existing := senders[peer]; existing != nil && !existing.closed.Load() {
		senders_lock.Unlock()
		stream.Reset()
		return existing, nil
	}
	senders[peer] = s
	senders_lock.Unlock()

	debug("Sender: stream open peer=%q session=%s codecs=%v features=%v",
		peer, s.session, codecs, features)

	go s.write_loop()
	go s.read_loop()
	go s.ping_loop()
	go s.pull_loop()

	return s, nil
}

// write_loop is the sender_write goroutine: pulls frames from outbox,
// pipelines claim before message, writes the framed message. Blocks
// the producer when len(inflight) >= sender_window_default — local
// memory cap; wire-level back-pressure rides on libp2p flow control.
//
// A message to an entity the far side has not proven waits, with that
// entity's later messages, while its prove frame is out: messages to proven
// entities go on meanwhile, so a fresh stream proves its targets together
// rather than one round trip after another.
func (s *Sender) write_loop() {
	w := &waiting{parked: map[string][]*outbound{}, deadlines: map[string]time.Time{}}
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		timer.Reset(w.next())
		var err error
		select {
		case ob := <-s.outbox:
			if s.closed.Load() {
				s.fail_outbound(ob, "sender closed")
				continue
			}
			err = s.route(w, ob)
		case p := <-s.proofs:
			err = s.settle(w, p.entity, p.err)
		case <-timer.C:
			err = s.expire(w)
		case <-s.done:
			s.release(w, "sender closed")
			return
		}
		if err == nil && w.bye != nil && len(w.parked) == 0 {
			bye := w.bye
			w.bye = nil
			err = s.send(bye)
		}
		if err != nil {
			debug("Sender: closing peer=%q: %v", s.peer, err)
			s.release(w, fmt.Sprintf("stream write: %v", err))
			s.shutdown()
			return
		}
	}
}

// next is how long until the earliest outstanding proof is due; an hour when
// none is.
func (w *waiting) next() time.Duration {
	wait := time.Hour
	for _, due := range w.deadlines {
		if left := time.Until(due); left < wait {
			wait = left
		}
	}
	if wait < 0 {
		wait = 0
	}
	return wait
}

// route writes ob, or holds it until its target is proven. A message it holds
// is the waiting list's to write or fail; one it writes, it fails itself when
// the write does.
func (s *Sender) route(w *waiting, ob *outbound) error {
	f := ob.frame
	if f.Type == frame_type_bye && len(w.parked) > 0 {
		// The far side stops reading at bye, so the messages held for proofs go
		// first, as they would have from the outbox.
		w.bye = ob
		return nil
	}
	if f.Type != frame_type_message || f.To == "" {
		return s.send(ob)
	}
	s.lock.Lock()
	proven := s.proven[f.To]
	s.lock.Unlock()
	if proven && len(w.parked[f.To]) == 0 {
		return s.send(ob)
	}
	// Nothing more waits behind a bye: the row goes back to the queue for the
	// next stream. Otherwise a message is always held, never handed back, which
	// would send it after its target's later messages: pull_loop bounds what is
	// held by counting it against the window.
	if w.bye != nil {
		if ob.queue != "" {
			queue_unsending(ob.queue)
		}
		return nil
	}
	_, held := w.parked[f.To]
	w.parked[f.To] = append(w.parked[f.To], ob)
	s.parked.Add(1)
	if held {
		return nil
	}
	if proven {
		return s.settle(w, f.To, nil)
	}
	w.queued = append(w.queued, f.To)
	return s.ask(w)
}

// send writes ob, failing it back to the queue when the write does not go
// through.
func (s *Sender) send(ob *outbound) error {
	if err := s.write_one(ob); err != nil {
		debug("Sender: write failed peer=%q queue=%q: %v", s.peer, ob.queue, err)
		s.fail_outbound(ob, fmt.Sprintf("stream write: %v", err))
		return err
	}
	return nil
}

// ask sends prove frames for the entities queued, as far as the budget goes.
// Outstanding asks are capped too, so every answer fits in proofs: messages to
// proven entities back proves while earlier ones are still out.
func (s *Sender) ask(w *waiting) error {
	for len(w.queued) > 0 && s.unbacked < sender_proves_maximum && len(w.deadlines) < sender_proves_maximum {
		entity := w.queued[0]
		w.queued = w.queued[1:]
		s.lock.Lock()
		s.proving[entity] = true
		s.lock.Unlock()
		if err := frame_write(s.stream, &Frame{Type: frame_type_prove, To: entity}); err != nil {
			return err
		}
		s.unbacked++
		w.deadlines[entity] = time.Now().Add(sender_send_timeout)
	}
	if len(w.queued) > 0 && len(w.deadlines) == 0 {
		return error_sender_proves
	}
	return nil
}

// settle releases an entity's messages once the far side has answered for
// it: written in order when it proved the entity, failed back to the queue
// when it did not. Then it asks for the entities waiting on the budget.
func (s *Sender) settle(w *waiting, entity string, err error) error {
	if _, asked := w.deadlines[entity]; !asked && err != nil {
		return nil
	}
	delete(w.deadlines, entity)
	held := w.parked[entity]
	delete(w.parked, entity)
	// Each message stops counting as held only once it is in flight or failed,
	// so room() never counts it in neither and lets pull_loop claim past the
	// window.
	for i, ob := range held {
		if err != nil {
			s.parked.Add(-1)
			s.fail_outbound(ob, fmt.Sprintf("proof: %v", err))
			continue
		}
		write := s.send(ob)
		s.parked.Add(-1)
		if write != nil {
			for _, rest := range held[i+1:] {
				s.parked.Add(-1)
				s.fail_outbound(rest, fmt.Sprintf("stream write: %v", write))
			}
			return write
		}
	}
	return s.ask(w)
}

// expire answers, as failures, the proofs that are overdue. One the far side
// proved in time but write_loop has not yet read, because it was busy writing,
// is answered as the proof it is.
func (s *Sender) expire(w *waiting) error {
	now := time.Now()
	for entity, due := range w.deadlines {
		if now.Before(due) {
			continue
		}
		s.lock.Lock()
		delete(s.proving, entity)
		proven := s.proven[entity]
		s.lock.Unlock()
		var answer error
		if !proven {
			answer = fmt.Errorf("peer %q did not prove entity %q", s.peer, entity)
		}
		if err := s.settle(w, entity, answer); err != nil {
			return err
		}
	}
	return nil
}

// release fails every message held for a proof, as the outbox drain fails
// what a closed Sender never sent.
func (s *Sender) release(w *waiting, reason string) {
	for entity, held := range w.parked {
		for _, ob := range held {
			s.fail_outbound(ob, reason)
		}
		s.parked.Add(-int64(len(held)))
		delete(w.parked, entity)
	}
}

// write_one pipelines (claim?, message) for a single outbound. Inserts
// into inflight BEFORE the write so an early ack from the reader
// goroutine always finds the entry.
func (s *Sender) write_one(ob *outbound) error {
	f := ob.frame

	// Outbound rate limit (peer.rate, default unlimited). Per-Sender
	// 1-second bucket. Ping frames also consume budget so a ping flood
	// from a misbehaving local app can't bypass it.
	s.rate_gate()

	// Apply per-frame codec selection: zstd if intersection allows
	// AND the payload is over the threshold AND there's a data blob to
	// compress.
	if f.Codec == codec_none && contains_string(s.codecs, "zstd") && len(f.Data) >= codec_threshold {
		codec, payload, err := frame_compress(f.Data, codec_zstd)
		if err == nil && codec == codec_zstd {
			f.Codec = codec_zstd
			f.Data = payload
		}
	}

	// Issue a claim for f.From if we haven't already done one on this
	// stream. Pipeline it ahead of the message — libp2p stream FIFO
	// guarantees the claim arrives first.
	s.lock.Lock()
	need_claim := f.Type == frame_type_message && f.From != "" && !s.claimed[f.From]
	if need_claim {
		s.claimed[f.From] = true
	}
	s.lock.Unlock()

	if need_claim {
		if err := claim_write(s.stream, f.From, s.challenge, s.peer, protocol_messages); err != nil {
			s.lock.Lock()
			delete(s.claimed, f.From)
			s.lock.Unlock()
			return err
		}
	}

	// The far side has proven it holds f.To before any of this message
	// crosses: route holds a message to an unproven target until it has.
	// Routing told us where to send; it did not establish that whoever is
	// there may act as f.To, and by the time a message frame is written the
	// content has already left.
	if f.Type == frame_type_message && f.ID != "" {
		// Window cap: block until inflight drops below peer.window. Back-pressure
		// path
		// - the outbox stays full and peer_send blocks new producers.
		window := peer_window()
		for {
			s.lock.Lock()
			if len(s.inflight) < window {
				s.inflight[f.ID] = &pending{queue: ob.queue, sent: now()}
				s.lock.Unlock()
				break
			}
			s.lock.Unlock()
			if s.closed.Load() {
				return fmt.Errorf("sender closed")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	if f.Type == frame_type_ping && f.ID != "" {
		s.lock.Lock()
		s.pings[f.ID] = now()
		s.lock.Unlock()
	}

	if err := frame_write(s.stream, f); err != nil {
		// Roll back the inflight insert; the caller will queue_fail.
		if f.Type == frame_type_message && f.ID != "" {
			s.lock.Lock()
			delete(s.inflight, f.ID)
			s.lock.Unlock()
		}
		// A refused message was never written, so the stream carries on and
		// only the message goes: failing the write would reset the stream and
		// send every message in flight on it back to the queue. Dropped off this
		// goroutine, which writes the stream and should not wait on queue.db.
		if errors.Is(err, frame_refused) && f.Type == frame_type_message {
			if ob.queue != "" {
				go frame_drop(ob.queue, s.peer, err)
			}
			return nil
		}
		return err
	}
	// Each message the far side receives backs one prove it answered. Counted
	// only for a message naming its sender, the one kind every receiver
	// credits, so this count never falls below the far side's.
	if f.Type == frame_type_message && f.From != "" && s.unbacked > 0 {
		s.unbacked--
	}
	return nil
}

// resolve_proof records the far side's answer for `entity` and hands it to
// write_loop when it was asked for. A nil error also records the proof for
// the rest of the connection's life.
func (s *Sender) resolve_proof(entity string, err error) {
	s.lock.Lock()
	asked := s.proving[entity]
	delete(s.proving, entity)
	if err == nil {
		s.proven[entity] = true
	}
	s.lock.Unlock()
	if asked && s.proofs != nil {
		// Never blocks: at most sender_proves_maximum are asked at once, and an
		// answer lost all the same is settled by its deadline from proven.
		select {
		case s.proofs <- proof{entity: entity, err: err}:
		default:
		}
	}
}

// fail_outbound is called when an outbound can't be written. Maps to
// queue_fail (or no-op for synthetic frames with empty queue id).
func (s *Sender) fail_outbound(ob *outbound, reason string) {
	if ob.queue == "" {
		return
	}
	queue_fail(ob.queue, reason)
}

// read_loop is the sender_read goroutine: reads ack / fail / ping /
// pong frames and resolves the inflight entries they reference.
func (s *Sender) read_loop() {
	for {
		f, err := frame_read(s.stream)
		if errors.Is(err, io.EOF) {
			s.shutdown()
			return
		}
		if err != nil {
			debug("Sender: framing error peer=%q session=%s: %v",
				s.peer, s.session, err)
			s.shutdown()
			return
		}
		s.handle_inbound(f)
	}
}

func (s *Sender) handle_inbound(f *Frame) {
	// Activity-on-stream: resets the ping idle timer in ping_loop.
	s.last_inbound.Store(time.Now().UnixNano())
	switch f.Type {
	case frame_type_ack:
		s.lock.Lock()
		acked := false
		acknowledged := []string{}
		for _, id := range f.Replies {
			p := s.inflight[id]
			if p == nil {
				continue
			}
			delete(s.inflight, id)
			acked = true
			if p.queue != "" {
				acknowledged = append(acknowledged, p.queue)
			}
		}
		s.lock.Unlock()
		// Acked outside the lock: queue_ack_async degrades to a synchronous
		// queue.db write plus a user.db write when its channel is full, and
		// write_one, pull_loop and the sweeper all contend for s.lock.
		for _, row := range acknowledged {
			queue_ack_async(row)
		}
		// The peer is applying and acking — clear any send-stall so its
		// deferred backlog resumes (peer_progress.go).
		if acked {
			peer_mark_progress(s.peer)
		}

	case frame_type_claim:
		// The far side answering a prove demand. Verified against the
		// challenge WE offered in caps and our own peer id, so a proof
		// gathered on some other connection cannot be relayed onto this
		// one.
		if err := responder_verify(f.From, s.offered, f.Signature, net_id, protocol_messages); err != nil {
			info("Sender: proof verify failed peer=%q session=%s entity=%q: %v",
				s.peer, s.session, f.From, err)
			s.resolve_proof(f.From, fmt.Errorf("proof verify failed: %w", err))
			return
		}
		s.resolve_proof(f.From, nil)

	case frame_type_fail:
		// A refused prove demand carries the entity rather than a message
		// id: it is answering a question about the peer, not about a
		// queued row.
		if len(f.Replies) == 0 && f.From != "" {
			s.resolve_proof(f.From, fmt.Errorf("peer refused to prove entity %q: %s", f.From, f.Reason))
			return
		}
		if len(f.Replies) == 0 {
			return
		}
		id := f.Replies[0]
		s.lock.Lock()
		p := s.inflight[id]
		if p != nil {
			delete(s.inflight, id)
		}
		s.lock.Unlock()
		if p == nil {
			return
		}
		s.resolve_fail(p, f.Reason)

	case frame_type_ping:
		// Echo pong with the same ID.
		_ = frame_write(s.stream, &Frame{Type: frame_type_pong, ID: f.ID})

	case frame_type_pong:
		s.lock.Lock()
		delete(s.pings, f.ID)
		s.lock.Unlock()

	default:
		debug("Sender: unexpected frame type=%q peer=%q session=%s",
			f.Type, s.peer, s.session)
	}
}

// resolve_fail maps the receiver's failure reason to the sender-side
// disposition (queue_ack/drop/fail/retry). The vocabulary table lives
// in claude/plans/protocol2.md → Failure reasons.
func (s *Sender) resolve_fail(p *pending, reason string) {
	switch reason {
	case fail_unsupported, fail_unknown_user, fail_expired, fail_dedup:
		if p.queue != "" {
			queue_drop(p.queue, reason)
		}
	case fail_signature_invalid:
		warn("Sender: signature_invalid fail from peer=%q queue=%q", s.peer, p.queue)
		if p.queue != "" {
			queue_drop(p.queue, reason)
		}
	case fail_rate_limited, fail_buffer_full, fail_handler_panic:
		if p.queue != "" {
			queue_fail(p.queue, fmt.Sprintf("retry-backoff: %s", reason))
		}
	case fail_unclaimed:
		// Clear our cached claim so the next send re-issues it on the
		// (new) stream, then re-queue for immediate retry.
		s.lock.Lock()
		for ent := range s.claimed {
			// Cheap: typically 1–2 entities per sender. Clearing all
			// is safe — re-claim on next send.
			delete(s.claimed, ent)
		}
		s.lock.Unlock()
		if p.queue != "" {
			queue_fail(p.queue, "retry-now: unclaimed")
		}
	case "", fail_transient:
		if p.queue != "" {
			queue_fail(p.queue, "retry-now: transient")
		}
	default:
		// Unknown reason from a newer peer; treat as transient.
		if p.queue != "" {
			queue_fail(p.queue, fmt.Sprintf("retry-now: unknown reason %q", reason))
		}
	}
}

// ping_loop emits a ping after peer.ping_interval of stream-side
// idleness. Active traffic resets the timer via last_inbound updates
// in handle_inbound, so busy streams never ping.
func (s *Sender) ping_loop() {
	interval := time.Duration(peer_ping_interval_seconds()) * time.Second
	// Tick more frequently than the interval so an early activity gap
	// doesn't make us wait a whole extra interval to ping.
	tick := interval / 3
	if tick < time.Second {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for range t.C {
		if s.closed.Load() {
			return
		}
		// Skip when the stream has received a frame within the
		// interval — receiver's liveness is implicitly confirmed.
		last := s.last_inbound.Load()
		if last > 0 && time.Since(time.Unix(0, last)) < interval {
			continue
		}
		id := uid()
		err := peer_send(s.peer, "", &Frame{Type: frame_type_ping, ID: id})
		if err != nil {
			debug("Sender: ping enqueue failed peer=%q: %v", s.peer, err)
		}
	}
}

// pull_loop drains queue.db rows targeting s.peer into s.outbox until s.closed.
// Batches are sized by remaining outbox capacity, and queue_claim_for_peer
// marks claimed rows 'sending' in the same statement so queue_process cannot
// double-pick. Wakes on a 1-second tick or a queue_wake nudge; on shutdown,
// claimed-but-unpushed rows roll back to 'pending' via queue_unsending.
func (s *Sender) pull_loop() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for !s.closed.Load() {
		capacity := s.room()
		if capacity > 0 {
			rows := queue_claim_for_peer(s.peer, capacity)
			for _, q := range rows {
				if s.closed.Load() {
					queue_unsending(q.ID)
					continue
				}
				if delivered_drop(&q) {
					continue
				}
				f, err := frame_for_queue(&q)
				if err != nil {
					frame_drop(q.ID, s.peer, err)
					continue
				}
				select {
				case s.outbox <- &outbound{frame: f, queue: q.ID}:
				case <-time.After(time.Second):
					// Outbox stuck — roll back the claim so the next
					// pull (or queue_process fallback) can retry.
					queue_unsending(q.ID)
				}
			}
		}
		select {
		case <-tick.C:
		case <-s.wake:
		}
	}
}

// room is how many more rows pull_loop may hand this Sender: its window less
// what is in flight, in the outbox, and held for a proof.
func (s *Sender) room() int {
	capacity := peer_window()
	s.lock.Lock()
	capacity -= len(s.inflight)
	s.lock.Unlock()
	return capacity - len(s.outbox) - int(s.parked.Load())
}

// rate_gate enforces peer.rate (messages/second per Sender). 0 =
// unlimited (no overhead beyond one int read). Otherwise: sleep until
// the next 1-second bucket if the current one is full.
func (s *Sender) rate_gate() {
	limit := peer_rate()
	if limit <= 0 {
		return
	}
	for {
		s.rate_lock.Lock()
		t := now()
		if t != s.rate_window {
			s.rate_window = t
			s.rate_count = 0
		}
		if s.rate_count < limit {
			s.rate_count++
			s.rate_lock.Unlock()
			return
		}
		s.rate_lock.Unlock()
		// Sleep until the next bucket boundary. Don't busy-wait.
		time.Sleep(50 * time.Millisecond)
		if s.closed.Load() {
			return
		}
	}
}

// shutdown marks the Sender closed, drains inflight to queue_fail,
// closes the libp2p stream, and removes the registry entry. Sibling
// goroutines notice via the closed atomic and exit.
func (s *Sender) shutdown() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	debug("Sender: shutdown peer=%q session=%s", s.peer, s.session)

	// Nothing ever closes outbox - peer_send may still hold a stale *Sender -
	// so write_loop needs its own exit signal. Without one it parked on the
	// channel receive forever, pinning the Sender and its maps: one leak per
	// disconnect, ping timeout, framing error or entity invalidation.
	// done is nil only in tests that install a synthetic Sender without going
	// through sender_open, the same exemption the stream check below makes.
	if s.done != nil {
		close(s.done)
	}

	senders_lock.Lock()
	if senders[s.peer] == s {
		delete(senders, s.peer)
	}
	senders_lock.Unlock()

	// stream is nil only in tests that install a synthetic Sender
	// without going through sender_open. Production always has one.
	if s.stream != nil {
		s.stream.Reset()
	}

	// Drain outbox into queue_fail so producers that arrive between
	// the CAS and the registry delete don't sit on a dead Sender.
	go func() {
		for {
			select {
			case ob := <-s.outbox:
				s.fail_outbound(ob, "sender closed")
			default:
				return
			}
		}
	}()

	s.lock.Lock()
	for id, p := range s.inflight {
		if p.queue != "" {
			queue_fail(p.queue, "stream closed")
		}
		delete(s.inflight, id)
	}
	s.lock.Unlock()
}

// senders_sweep_all walks every Sender and times out stale inflight
// entries + unanswered pings. Called from a single background goroutine
// (senders_sweep_manager); per-Sender locks are short-held.
func senders_sweep_all() {
	senders_lock.Lock()
	all := make([]*Sender, 0, len(senders))
	for _, s := range senders {
		all = append(all, s)
	}
	senders_lock.Unlock()

	t := now()
	inflight_timeout := int64(peer_inflight_timeout())
	ping_timeout := int64(peer_ping_timeout_seconds())
	for _, s := range all {
		var stale_inflight []string
		var stale_pings []string

		s.lock.Lock()
		for id, p := range s.inflight {
			if t-p.sent > inflight_timeout {
				stale_inflight = append(stale_inflight, id)
			}
		}
		for id, ts := range s.pings {
			if t-ts > ping_timeout {
				stale_pings = append(stale_pings, id)
			}
		}
		s.lock.Unlock()

		for _, id := range stale_inflight {
			s.lock.Lock()
			p := s.inflight[id]
			if p == nil {
				s.lock.Unlock()
				continue
			}
			delete(s.inflight, id)
			s.lock.Unlock()
			if p.queue != "" {
				queue_fail(p.queue, "inflight timeout")
			}
		}

		// Stream opened but frames timed out without an ack: an
		// app-level no-progress signal (distinct from a connect
		// failure). Repeated, this stalls the target so queue_process
		// parks its backlog instead of re-scanning it (peer_progress.go).
		if len(stale_inflight) > 0 {
			peer_mark_no_progress(s.peer)
		}

		if len(stale_pings) > 0 {
			debug("Sender: ping timeout peer=%q (no pongs for %v)",
				s.peer, stale_pings)
			s.shutdown()
		}
	}
}

// senders_sweep_manager runs senders_sweep_all once per second. One
// goroutine for the whole process; tracks every Sender via the global
// registry.
func senders_sweep_manager() {
	for range time.Tick(time.Second) {
		senders_sweep_all()
	}
}

// senders_bye_all walks every open Sender and writes a bye frame, then
// waits for inflight to drain (or sender_bye_drain_timeout). Called
// from the SIGTERM path; replaces the legacy peers_shutdown loop for
// peers that have an open /mochi/2/messages stream.
func senders_bye_all(timeout time.Duration) {
	senders_lock.Lock()
	all := make([]*Sender, 0, len(senders))
	for _, s := range senders {
		all = append(all, s)
	}
	senders_lock.Unlock()

	for _, s := range all {
		_ = peer_send(s.peer, "", &Frame{Type: frame_type_bye})
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		any_inflight := false
		senders_lock.Lock()
		for _, s := range senders {
			s.lock.Lock()
			if len(s.inflight) > 0 || s.parked.Load() > 0 {
				any_inflight = true
			}
			s.lock.Unlock()
			if any_inflight {
				break
			}
		}
		senders_lock.Unlock()
		if !any_inflight {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// senders_peer_invalidate is called from the libp2p disconnect handler
// to tear down any open Sender for the gone peer. Same shape as
// peer_disconnected for the rest of the per-peer state.
func senders_peer_invalidate(peer string) {
	senders_lock.Lock()
	s := senders[peer]
	senders_lock.Unlock()
	if s != nil {
		s.shutdown()
	}
}

// senders_entity_invalidate tears down any open Sender that has already claimed
// `entity`, so a receiver's cached claim against the old pubkey stops serving
// us after a key rotation. Mochi never rotates entity keys today.
func senders_entity_invalidate(entity string) {
	if entity == "" {
		return
	}
	senders_lock.Lock()
	doomed := make([]*Sender, 0, len(senders))
	for _, s := range senders {
		s.lock.Lock()
		if s.claimed[entity] {
			doomed = append(doomed, s)
		}
		s.lock.Unlock()
	}
	senders_lock.Unlock()
	for _, s := range doomed {
		debug("Sender: entity %q rotated; closing peer=%q session=%s",
			entity, s.peer, s.session)
		s.shutdown()
	}
}

// peer_protocol_open opens a libp2p stream to peer for `prefer`, connecting
// first if needed. Returns error_sender_unreachable when the peer is silent,
// cannot be connected, or does not speak `prefer` - the last silences the peer
// so it is not probed on every queue tick.
func peer_protocol_open(peer string, prefer string) (p2p_network.Stream, error) {
	if peer == "" || net_me == nil {
		return nil, error_sender_unreachable
	}
	if peer_is_silent(peer) {
		return nil, error_sender_unreachable
	}
	if peer != net_id && !peer_connect(peer) {
		// Unknown or stale-addressed peer: ask the mesh for its current
		// addresses (rate limited). A reply re-wakes the queue via
		// peer_discovered_address, so the failed row retries promptly.
		peer_request_addresses(peer)
		peer_mark_send_failed(peer)
		return nil, error_sender_unreachable
	}

	pid, err := p2p_peer.Decode(peer)
	if err != nil {
		return nil, fmt.Errorf("invalid peer id %q: %w", peer, err)
	}

	sctx, cancel := context.WithTimeout(net_context, peer_stream_open_timeout)
	defer cancel()
	s, err := net_me.NewStream(sctx, pid, p2p_protocol.ID(prefer))
	if err != nil {
		if is_protocol_not_supported(err) {
			debug("Protocol: peer %q does not support %q — treating as unreachable (peer never upgraded past /mochi/1?)", peer, prefer)
			peer_mark_send_failed(peer)
			return nil, error_sender_unreachable
		}
		peer_mark_send_failed(peer)
		return nil, fmt.Errorf("protocol: NewStream peer=%q proto=%q: %w", peer, prefer, err)
	}
	peer_mark_send_success(peer)
	return s, nil
}

// is_protocol_not_supported reports whether err is multistream's protocol
// rejection. The error is parameterised on the protocol-id type: libp2p's host
// returns the protocol.ID specialisation, tests the plain-string one.
func is_protocol_not_supported(err error) bool {
	if err == nil {
		return false
	}
	var es multistream.ErrNotSupported[string]
	if errors.As(err, &es) {
		return true
	}
	var ep multistream.ErrNotSupported[p2p_protocol.ID]
	if errors.As(err, &ep) {
		return true
	}
	// String fallback for any future libp2p wrapping that strips the
	// typed error. The phrase is stable across multistream versions.
	if message := err.Error(); strings.Contains(message, "protocols not supported") {
		return true
	}
	return false
}

// --- Frame adapters (queue / Message -> Frame) -----------------------
//
// Shared by queue_send_direct and message_attempt_send_real so the two callers
// build the same frame.

// frame_for_queue builds a v2 message Frame from a queue.db row. The row's
// content is already CBOR; decode it so the frame ships structured Content.
// An error is a frame no receiver would take (frame_check): the caller drops
// the row with frame_drop rather than sending it.
func frame_for_queue(q *QueueEntry) (*Frame, error) {
	content := map[string]any{}
	if len(q.Content) > 0 {
		if err := cbor.Unmarshal(q.Content, &content); err != nil {
			return nil, err
		}
	}
	var services []string
	if q.FromServices != "" {
		services = strings.Split(q.FromServices, ",")
	}
	return frame_checked(&Frame{
		Type:     frame_type_message,
		ID:       q.ID,
		From:     q.FromEntity,
		To:       q.ToEntity,
		Service:  q.Service,
		Event:    q.Event,
		FromApp:  q.FromApp,
		Services: services,
		Priority: frame_priority_for(q.Priority),
		Content:  content,
		Data:     q.Data,
	})
}

// frame_for_message builds a v2 message Frame from a *Message. Used
// by message_attempt_send_real's v2 branch. Errors as frame_for_queue.
func frame_for_message(m *Message, content []byte) (*Frame, error) {
	content_map := map[string]any{}
	if len(content) > 0 {
		if err := cbor.Unmarshal(content, &content_map); err != nil {
			return nil, err
		}
	}
	return frame_checked(&Frame{
		Type:     frame_type_message,
		ID:       m.ID,
		From:     m.From,
		To:       m.To,
		Service:  m.Service,
		Event:    m.Event,
		FromApp:  m.FromApp,
		Services: m.Services,
		Priority: frame_priority_for(queue_priority(m.Service, m.Event)),
		Content:  content_map,
		Data:     m.data,
	})
}

// frame_checked answers f, or frame_check's refusal of it.
func frame_checked(f *Frame) (*Frame, error) {
	if err := frame_check(f); err != nil {
		return nil, err
	}
	return f, nil
}

// frame_drop gives up a queued message whose frame cannot be sent, as the
// fail_unsupported its receiver would answer: the row goes at once and the
// sending app is told it was rejected. The app's handler runs on a goroutine
// of its own, since the callers are the send paths and a handler can take as
// long as it likes. Logged, not warned: the cause is the app's message, which
// the rejection reports to it.
func frame_drop(id, peer string, err error) {
	info("Sender: dropping message %q for peer=%q: %v", id, peer, err)
	if report := queue_drop_row(id, fail_unsupported); report != nil {
		go guard("message rejection", nil, report)
	}
}
