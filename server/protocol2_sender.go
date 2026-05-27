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
// Copyright Alistair Cunningham 2026

package main

import (
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

func init() {
	// /mochi/2 owns per-peer state in two places — the protocol-support
	// cache (protocol_known) and the Sender registry. Both need
	// invalidation on libp2p disconnect so the next reconnect re-probes
	// cleanly. Register here so peers.go's peer_disconnected stays
	// decoupled from /mochi/2 internals.
	peer_register_disconnect_hook(protocol_known_clear)
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

// errSenderUnreachable is returned by peer_send when the target peer
// isn't reachable and the call can't even open a sender. queue_fail
// will re-queue with the usual backoff.
var errSenderUnreachable = errors.New("sender: peer unreachable")

// errSenderFull is returned by peer_send when the outbox channel is
// saturated past sender_send_timeout. Same recovery as a stream-open
// failure: queue_fail re-queues with exp backoff.
var errSenderFull = errors.New("sender: outbox full")

// pending tracks one outbound message awaiting ack. Keyed by message
// id on the Sender's inflight map.
type pending struct {
	queue string // queue.id this frame came from
	sent  int64  // unix seconds when written
}

// Sender owns one outbound /mochi/2/messages stream to a single peer.
// All fields except outbox are guarded by lock.
type Sender struct {
	peer         string
	stream       wire_stream
	session      string
	challenge    []byte
	codecs       []string // sender's effective codec set after intersection
	features     []string // sender's effective feature set after intersection
	outbox       chan *outbound
	inflight     map[string]*pending
	pings        map[string]int64
	claimed      map[string]bool
	closed       atomic.Bool
	last_inbound atomic.Int64 // unix ns of last inbound frame; reset by ping_loop
	wake         chan struct{} // queue_wake routes per-peer nudges here; pull_loop drains
	lock         sync.Mutex
	rate_lock    sync.Mutex
	rate_window  int64 // unix-second of current 1s bucket
	rate_count   int   // sends in the current bucket
}

// outbound carries the original queue id alongside the wire frame so
// sender_write can register it in inflight before the write.
type outbound struct {
	frame *Frame
	queue string // queue.id; empty for synthetic frames like bye
}

var (
	senders      = map[string]*Sender{}
	senders_lock sync.Mutex
)

// senders_has reports whether an open Sender exists for `peer`. Used by
// queue_process to skip rows that the Sender's pull_loop owns, so the
// two paths don't compete for the same outbox slot (which manifests as
// queue_process tick latency: peer_send blocks for sender_send_timeout
// when pull_loop has saturated the outbox, dragging out the whole tick
// and starving self-loop / /mochi/1-only / offline-peer work).
func senders_has(peer string) bool {
	if peer == "" {
		return false
	}
	senders_lock.Lock()
	s := senders[peer]
	senders_lock.Unlock()
	return s != nil && !s.closed.Load()
}

// peer_send is the entry point for /mochi/2/messages outbound. Looks
// up (or creates) the Sender for `peer` and enqueues `frame` on its
// outbox. Returns errSenderUnreachable / errSenderFull on failure so
// queue_send_direct can map to queue_fail with the normal backoff.
//
// `queue` is the queue.id that originated this send — sender_read uses
// it to call queue_ack / queue_fail / queue_drop when the receiver
// replies.
func peer_send(peer string, queue string, frame *Frame) error {
	s, err := sender_for(peer)
	if err != nil {
		return err
	}
	select {
	case s.outbox <- &outbound{frame: frame, queue: queue}:
		return nil
	case <-time.After(sender_send_timeout):
		return errSenderFull
	}
}

// sender_for finds-or-creates the Sender for peer. The open path is
// gated by senders_lock; the per-Sender state machine runs without it.
func sender_for(peer string) (*Sender, error) {
	senders_lock.Lock()
	s := senders[peer]
	senders_lock.Unlock()
	if s != nil && !s.closed.Load() {
		return s, nil
	}
	return sender_open(peer)
}

// sender_open establishes a new /mochi/2/messages stream to peer,
// runs the handshake (read hello → write caps), and starts the
// per-Sender goroutines. Falls back to /mochi/1 if the peer doesn't
// support /mochi/2/messages — peer_protocol_open handles the cache.
func sender_open(peer string) (*Sender, error) {
	stream, err := peer_protocol_open(peer, protocol_messages)
	if err != nil {
		return nil, fmt.Errorf("sender: stream open failed: %w", err)
	}
	if stream == nil {
		return nil, errSenderUnreachable
	}

	hello, err := hello_read(stream, 2)
	if err != nil {
		stream.Reset()
		return nil, fmt.Errorf("sender: hello read failed peer=%q: %w", peer, err)
	}

	// Compute effective codec / feature sets.
	codecs := codec_intersect(receiver_codecs(), hello.Codecs)
	features := features_intersect(receiver_features(), hello.Features)

	if err := caps_write(stream, codecs, features); err != nil {
		stream.Reset()
		return nil, fmt.Errorf("sender: caps write failed peer=%q: %w", peer, err)
	}

	s := &Sender{
		peer:      peer,
		stream:    stream,
		session:   hello.Session,
		challenge: hello.Challenge,
		codecs:    codecs,
		features:  features,
		outbox:    make(chan *outbound, peer_outbox()),
		inflight:  map[string]*pending{},
		pings:     map[string]int64{},
		claimed:   map[string]bool{},
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
func (s *Sender) write_loop() {
	for ob := range s.outbox {
		if s.closed.Load() {
			s.fail_outbound(ob, "sender closed")
			continue
		}
		if err := s.write_one(ob); err != nil {
			debug("Sender: write failed peer=%q queue=%q: %v",
				s.peer, ob.queue, err)
			s.fail_outbound(ob, fmt.Sprintf("stream write: %v", err))
			s.shutdown()
			return
		}
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
		if err := claim_write(s.stream, f.From, s.challenge); err != nil {
			s.lock.Lock()
			delete(s.claimed, f.From)
			s.lock.Unlock()
			return err
		}
	}

	if f.Type == frame_type_message && f.ID != "" {
		// Window cap: block here when inflight has reached the
		// per-peer limit (peer.window). Back-pressure path —
		// outbox stays full while we wait, peer_send blocks new
		// producers. Wire-level back-pressure rides separately on
		// libp2p flow control.
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
		return err
	}
	return nil
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
		for _, id := range f.Replies {
			p := s.inflight[id]
			if p == nil {
				continue
			}
			delete(s.inflight, id)
			if p.queue != "" {
				queue_ack(p.queue)
			}
		}
		s.lock.Unlock()

	case frame_type_fail:
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

// pull_loop autonomously drains queue.db rows targeting s.peer into
// s.outbox, running until s.closed. Each pull batch is sized by
// remaining outbox capacity (peer_window minus current inflight minus
// queued outbox) so we never claim more than will fit. Atomic UPDATE
// RETURNING in queue_claim_for_peer marks claimed rows status='sending'
// in the same statement, so queue_process won't double-pick them.
//
// Wakes on either a 1-second tick (safety net) or a queue_wake nudge
// routed through s.wake. A busy producer pegs the pull loop to the
// rate at which the receiver acks free outbox slots; an idle producer
// just polls every second and finds nothing.
//
// On shutdown, any rows the loop has claimed but not yet pushed to
// outbox are rolled back to status='pending' via queue_unsending so
// the next Sender open (or queue_process fallback) picks them up.
func (s *Sender) pull_loop() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for !s.closed.Load() {
		capacity := peer_window()
		s.lock.Lock()
		capacity -= len(s.inflight)
		s.lock.Unlock()
		capacity -= len(s.outbox)
		if capacity > 0 {
			rows := queue_claim_for_peer(s.peer, capacity)
			for _, q := range rows {
				if s.closed.Load() {
					queue_unsending(q.ID)
					continue
				}
				f, err := frame_for_queue(&q)
				if err != nil {
					queue_drop(q.ID, fmt.Sprintf("frame build failed: %v", err))
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
			if len(s.inflight) > 0 {
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

// senders_entity_invalidate tears down any open Sender that has
// already claimed `entity` on its current stream. Called from an
// entity-key rotation handler so the receiver's pre-rotation cached
// claim (trusting the OLD pubkey) can't keep serving us. The next
// peer_send for the entity opens a fresh Sender and issues a new
// claim against the rotated key.
//
// No-op when entity rotation isn't wired (current Mochi never rotates
// entity keys); the hook is here so a future rotation path is one line
// of integration.
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

// peer_protocol_open opens a libp2p stream to peer for `prefer`,
// falling back to protocol_legacy if the peer doesn't support it. The
// per-(peer, protocol) cache (see protocol_known) makes the fallback
// stick for the lifetime of the libp2p connection.
//
// Returns nil stream + nil error in two cases:
//   • peer is silent (peer_is_silent caught it before stream-open)
//   • peer rejected both protocols
// Callers treat both as errSenderUnreachable.
func peer_protocol_open(peer string, prefer string) (p2p_network.Stream, error) {
	if peer == "" || net_me == nil {
		return nil, errSenderUnreachable
	}
	if peer_is_silent(peer) {
		return nil, errSenderUnreachable
	}
	if peer != net_id && !peer_connect(peer) {
		peer_mark_send_failed(peer)
		return nil, errSenderUnreachable
	}

	pid, err := p2p_peer.Decode(peer)
	if err != nil {
		return nil, fmt.Errorf("invalid peer id %q: %w", peer, err)
	}

	if protocol_known_get(peer, prefer) == protocol_state_unsupported {
		// We've already determined this peer can't speak `prefer`.
		// Hand back nil so caller falls back to legacy through
		// /mochi/1 (peer_stream + read/write headers).
		return nil, fmt.Errorf("protocol: peer %q does not support %q", peer, prefer)
	}

	s, err := net_me.NewStream(net_context, pid, p2p_protocol.ID(prefer))
	if err != nil {
		// libp2p returns multistream.ErrNotSupported when the peer
		// doesn't speak `prefer`. Cache and surface so caller falls back.
		if is_protocol_not_supported(err) {
			protocol_known_set(peer, prefer, protocol_state_unsupported)
			return nil, fmt.Errorf("protocol: peer %q does not support %q", peer, prefer)
		}
		peer_mark_send_failed(peer)
		return nil, fmt.Errorf("protocol: NewStream peer=%q proto=%q: %w", peer, prefer, err)
	}
	protocol_known_set(peer, prefer, protocol_state_supported)
	peer_mark_send_success(peer)
	return s, nil
}

// is_v2_unsupported reports whether err means "this peer doesn't speak
// /mochi/2/*", which is the signal callers use to fall back to
// /mochi/1. Wraps both the libp2p multistream error and the synthetic
// "protocol: peer ... does not support" we return from
// peer_protocol_open after a cache miss.
func is_v2_unsupported(err error) bool {
	if err == nil {
		return false
	}
	if is_protocol_not_supported(err) {
		return true
	}
	// peer_protocol_open's synthetic message after a cache hit.
	if msg := err.Error(); len(msg) > 0 {
		if strings.Contains(msg, "does not support") {
			return true
		}
	}
	return false
}

// --- Per-(peer, protocol) selection cache -----------------------------
//
// During the mixed-version rollout, the first stream-open attempt to
// each peer probes whether it supports the requested /mochi/2/* protocol.
// We cache the result for the lifetime of the libp2p connection, so
// subsequent sends skip the probe and either open /mochi/2/* directly
// or fall back to /mochi/1 without round-tripping.
//
// Cleared on peer disconnect (peer_disconnected in peers.go calls
// protocol_known_clear) — the peer may have upgraded or downgraded
// before reconnecting, so the next attempt re-probes.
//
// Per the operational context (< 10 production peers, mixed-version
// window measured in days), this is intentionally minimal — see
// claude/plans/protocol2.md → Protocol selection.
//
// Why not libp2p's Peerstore.SupportsProtocols / GetProtocols?
//
// Peerstore tracks protocols populated by the identify exchange.
// Identify is pull-only: the remote's advertised protocol set only
// becomes known after IdentifyConn runs on the connection. The first
// NewStream attempt for a fresh connection arrives before identify
// has finished, so Peerstore would return an empty list and we'd
// have to fall back to probing anyway. We also need a "negative"
// cache for the rollout window (peer answered ErrNotSupported once
// → don't reprobe this connection) — Peerstore has no concept of
// "this peer affirmatively does NOT support X", only "I don't know
// of it". Once /mochi/1 is gone (Phase 8), this entire cache can be
// retired in favour of always opening /mochi/2/* and treating
// ErrNotSupported as a hard error.

type protocol_state int

const (
	protocol_state_unknown protocol_state = iota
	protocol_state_supported
	protocol_state_unsupported
)

var (
	protocol_known      = map[string]map[string]protocol_state{}
	protocol_known_lock sync.RWMutex
)

// protocol_known_get returns the cached state for (peer, proto), or
// protocol_state_unknown if we haven't probed yet.
func protocol_known_get(peer, proto string) protocol_state {
	protocol_known_lock.RLock()
	defer protocol_known_lock.RUnlock()
	if m := protocol_known[peer]; m != nil {
		return m[proto]
	}
	return protocol_state_unknown
}

// protocol_known_set records the outcome of a probe.
func protocol_known_set(peer, proto string, state protocol_state) {
	protocol_known_lock.Lock()
	defer protocol_known_lock.Unlock()
	m := protocol_known[peer]
	if m == nil {
		m = map[string]protocol_state{}
		protocol_known[peer] = m
	}
	m[proto] = state
}

// protocol_known_clear drops every protocol entry for peer. Called
// from the libp2p disconnect handler — the peer may have upgraded or
// downgraded before next connect, so the cache must re-probe.
func protocol_known_clear(peer string) {
	protocol_known_lock.Lock()
	delete(protocol_known, peer)
	protocol_known_lock.Unlock()
}

// is_protocol_not_supported tests whether err came from libp2p's
// multistream-select rejecting the requested protocol.
//
// multistream.ErrNotSupported is parameterised on the protocol-id
// type. libp2p's host returns the protocol.ID specialisation; tests
// sometimes construct the plain-string one. We try both via errors.As.
//
// As a belt-and-braces fallback we also string-match the wrapped
// message — libp2p wraps the error in basic_host with "failed to
// negotiate protocol:" and earlier go-libp2p releases sometimes
// returned the wrapped form without the original error chain.
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
	if msg := err.Error(); strings.Contains(msg, "protocols not supported") {
		return true
	}
	return false
}

// --- Frame adapters (queue / Message -> Frame) -----------------------
//
// queue_send_direct + message_attempt_send_real both build a wire
// message from queue / Message fields; the helpers below factor out
// the common "build a Frame and ship it via peer_send" path so the
// two callers stay consistent.

// frame_for_queue builds a v2 message Frame from a queue.db row.
// Used by queue_send_direct's v2 branch. The queue row's content is
// already CBOR-encoded as a map; decode it back so the frame can ship
// it as a structured Content (and so the receiver doesn't have to
// repeat the work).
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
	return &Frame{
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
	}, nil
}

// frame_for_message builds a v2 message Frame from a *Message. Used
// by message_attempt_send_real's v2 branch.
func frame_for_message(m *Message, content []byte) (*Frame, error) {
	contentMap := map[string]any{}
	if len(content) > 0 {
		if err := cbor.Unmarshal(content, &contentMap); err != nil {
			return nil, err
		}
	}
	return &Frame{
		Type:     frame_type_message,
		ID:       m.ID,
		From:     m.From,
		To:       m.To,
		Service:  m.Service,
		Event:    m.Event,
		FromApp:  m.FromApp,
		Services: m.Services,
		Priority: frame_priority_for(queue_priority(m.Service, m.Event)),
		Content:  contentMap,
		Data:     m.data,
	}, nil
}
