// Mochi server: Streams
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
	cbor "github.com/fxamacker/cbor/v2"
	sl "go.starlark.net/starlark"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	cbor_maximum_size     = 100 * 1024 * 1024 // 100MB maximum message size
	cbor_maximum_depth    = 32                // Max nesting depth
	cbor_maximum_pairs    = 1000              // Max map pairs
	cbor_maximum_elements = 10000             // Max array elements
	content_maximum_key   = 256               // Max content key length
	content_maximum_value = 100 * 1024 * 1024 // 100MB maximum content value length

	// cbor_maximum_items bounds the data items in one decoded value: a frame, a
	// pubsub announcement or a stream segment. The caps above bound each array
	// and map but not their product, and an item costs some sixty bytes of heap
	// for each byte it takes on the wire, so a 16 MB frame of nested empty maps
	// decoded to a gigabyte. Sized to the largest real payloads, a wiki's full
	// dump among them, which the per-array cap keeps under it.
	cbor_maximum_items = 1000000
)

// cbor_walk follows one CBOR data item through its bytes as they arrive: where
// it ends, and how many data items it holds. Every head is read once however
// the item is split across reads, so a caller can hand it a growing buffer
// without the rescan of the whole prefix that cbor.Decoder makes on each read.
type cbor_walk struct {
	offset int     // the next head, counted from the item's first byte
	open   []int64 // items still owed to each open array, map or tag; cbor_walk_indefinite until its break
	items  int     // data items read
}

const cbor_walk_indefinite = -1

var (
	cbor_walk_malformed = errors.New("cbor: malformed data item")
	cbor_walk_items     = fmt.Errorf("cbor: more than %d data items", cbor_maximum_items)
	cbor_walk_depth     = errors.New("cbor: nested too deeply")
)

// step walks data, the item's bytes so far from its first, as far as whole
// heads go. It returns the item's length once the item is complete, and zero
// while it needs more bytes. Only the item budget and a nesting bound much
// looser than the decoder's are enforced here; the decode that follows applies
// the rest.
func (w *cbor_walk) step(data []byte) (int, error) {
	for w.offset < len(data) {
		major, information := data[w.offset]>>5, data[w.offset]&0x1f
		length := 1
		var argument uint64
		switch {
		case information < 24:
			argument = uint64(information)
		case information < 28:
			length += 1 << (information - 24)
			if len(data)-w.offset < length {
				return 0, nil
			}
			for _, b := range data[w.offset+1 : w.offset+length] {
				argument = argument<<8 | uint64(b)
			}
		case information == 31 && major == 7:
			// A break ends the innermost indefinite array, map or string.
			top := len(w.open) - 1
			if top < 0 || w.open[top] != cbor_walk_indefinite {
				return 0, cbor_walk_malformed
			}
			w.offset++
			w.open = w.open[:top]
			if w.complete() {
				return w.offset, nil
			}
			continue
		case information == 31 && major >= 2 && major <= 5:
		default:
			return 0, cbor_walk_malformed
		}
		// A string is taken whole or not at all, so its head is not counted twice.
		if (major == 2 || major == 3) && information != 31 {
			if argument > uint64(len(data)-w.offset-length) {
				return 0, nil
			}
			length += int(argument)
		}
		w.items++
		// An array or map owes at least one item per element, so one declaring
		// more than the budget cannot fit in it.
		if w.items > cbor_maximum_items || ((major == 4 || major == 5) && information != 31 && argument > cbor_maximum_items) {
			return 0, cbor_walk_items
		}
		w.offset += length
		switch {
		case information == 31:
			w.open = append(w.open, cbor_walk_indefinite)
		case major == 4 && argument > 0:
			w.open = append(w.open, int64(argument))
		case major == 5 && argument > 0:
			w.open = append(w.open, 2*int64(argument))
		case major == 6:
			w.open = append(w.open, 1)
		default:
			if w.complete() {
				return w.offset, nil
			}
			continue
		}
		// Tags count as a level here and only sometimes in the decoder.
		if len(w.open) > 2*cbor_maximum_depth {
			return 0, cbor_walk_depth
		}
	}
	return 0, nil
}

// complete counts an item just ended against the containers holding it,
// closing each it fills, and reports whether the outermost item has ended.
func (w *cbor_walk) complete() bool {
	for len(w.open) > 0 {
		top := len(w.open) - 1
		if w.open[top] == cbor_walk_indefinite {
			return false
		}
		w.open[top]--
		if w.open[top] > 0 {
			return false
		}
		w.open = w.open[:top]
	}
	return true
}

// cbor_items refuses a whole value whose data items run past
// cbor_maximum_items, before anything of it is decoded. Linear in its bytes, and
// allocates nothing but the walk's stack.
func cbor_items(data []byte) error {
	var w cbor_walk
	n, err := w.step(data)
	if err != nil {
		return err
	}
	if n == 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// cbor_check reports whether this host would decode a whole value: within the
// item budget and well formed under cbor_decode_mode's caps.
func cbor_check(data []byte) error {
	if err := cbor_items(data); err != nil {
		return err
	}
	return cbor_decode_mode.Wellformed(data)
}

// cbor_segments reads a stream's CBOR segments one whole data item at a time,
// walking each as its bytes arrive, so a segment over its size or the item
// budget is refused before any of it is decoded.
type cbor_segments struct {
	reader    io.Reader
	buffer    []byte // read and not yet returned: the next segment onwards
	remaining int64  // bytes the stream may still read through it
}

// next returns the next whole segment, of at most size bytes. The slice is the
// caller's: later reads never write over it.
func (c *cbor_segments) next(size int64) ([]byte, error) {
	var walk cbor_walk
	for {
		if len(c.buffer) > 0 {
			n, err := walk.step(c.buffer)
			if err != nil {
				return nil, err
			}
			if n > 0 {
				segment := c.buffer[:n:n]
				c.buffer = c.buffer[n:]
				return segment, nil
			}
		}
		want := size - int64(len(c.buffer))
		if want <= 0 {
			return nil, fmt.Errorf("cbor: segment over %d bytes", size)
		}
		if want > c.remaining {
			want = c.remaining
		}
		if want <= 0 {
			if len(c.buffer) == 0 {
				return nil, io.EOF
			}
			return nil, io.ErrUnexpectedEOF
		}
		// Grown into a fresh array, never compacted in place, so no segment
		// already returned is written over.
		if cap(c.buffer)-len(c.buffer) < 512 {
			grown := make([]byte, len(c.buffer), 2*cap(c.buffer)+512)
			copy(grown, c.buffer)
			c.buffer = grown
		}
		space := c.buffer[len(c.buffer):cap(c.buffer)]
		if int64(len(space)) > want {
			space = space[:want]
		}
		n, err := c.reader.Read(space)
		c.buffer = c.buffer[:len(c.buffer)+n]
		c.remaining -= int64(n)
		if n == 0 && err != nil {
			if errors.Is(err, io.EOF) {
				if len(c.buffer) == 0 {
					return nil, io.EOF
				}
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
}

var cbor_decode_mode cbor.DecMode

// Deliberately a package init(), not a call from main_serve: a nil
// cbor_decode_mode would decode with library defaults and no depth, map or
// element caps, so no path can be reachable before the limits exist.
func init() {
	cbor_decode_mode = must(cbor.DecOptions{
		MaxMapPairs:      cbor_maximum_pairs,
		MaxArrayElements: cbor_maximum_elements,
		MaxNestedLevels:  cbor_maximum_depth,
	}.DecMode())
}

type Stream struct {
	id       int64
	reader   io.ReadCloser
	writer   io.WriteCloser
	segments *cbor_segments
	encoder  *cbor.Encoder
	remote   string // Remote address (for incoming streams)
	timeout  struct {
		read  int
		write int
	}
	// maximum_bytes overrides the cumulative cap on the bytes segments are read
	// from (zero = cbor_maximum_size). Must be set BEFORE the first read: the
	// segment reader is constructed lazily.
	maximum_bytes int64
	// abandoned records that a write failed against the REMOTE end rather than the
	// local source, so the dispatcher logs instead of warn-emailing. Set only
	// where the failure is known to be the remote's; never inferred from an error
	// string.
	abandoned     bool
	on_close      func() // Called once when stream is closed (e.g. release semaphore)
	on_close_once sync.Once
	// requester marks a stream this side opened, whose far end answers; only
	// a requester's close may reset it (abandon). answered records that the
	// answer has begun - a segment read, or the body opened - since this side
	// last wrote, settled that a close has made the decision. Atomic, as close
	// can run on the goroutine a call's cancellation starts.
	requester bool
	answered  atomic.Bool
	settled   atomic.Bool
}

// stream_destination wraps the remote end of a copy so that a failure there
// can be told apart from a failure reading the source. io.Copy returns one
// error for both sides, so a vanished peer and an unreadable file are the same
// value to its caller; only this wrapper's own Write can record the remote's.
//
// With idle set, each piece (stream_piece) waits at most that long for the far
// end to take it, the pieces cut here because one Write may carry a whole
// upload, which one deadline would cut off while it was still moving.
type stream_destination struct {
	writer io.Writer
	idle   time.Duration
	failed error
}

// stream_piece is how much of a write waits under one deadline.
const stream_piece = 32 << 10

func (d *stream_destination) Write(p []byte) (int, error) {
	deadline, _ := d.writer.(interface{ SetWriteDeadline(time.Time) error })
	if d.idle <= 0 || deadline == nil {
		n, err := d.writer.Write(p)
		if err != nil {
			d.failed = err
		}
		return n, err
	}
	written := 0
	for written < len(p) {
		_ = deadline.SetWriteDeadline(time.Now().Add(d.idle))
		n, err := d.writer.Write(p[written:min(written+stream_piece, len(p))])
		written += n
		if err != nil {
			d.failed = err
			return written, err
		}
	}
	return written, nil
}

// send copies source to the remote, marking the stream abandoned if it was the
// remote that gave out. The error is returned unchanged either way: whether the
// transfer failed is the caller's business, and which end failed is the log's.
//
// A transfer that fails, at either end, resets the stream: the closing write
// its caller defers would otherwise end the body cleanly, and the far end would
// take what had arrived for the whole of it.
//
// A body served to a requester is sent with idle zero, no write deadline: a
// peer's stream gives the sender credit only after its reader has drained a
// large share of the window - half of up to 16 MB on yamux, a quarter of up to
// 10 MB on QUIC - so a deadline on each write times the gaps between credits,
// not whether the reader has stopped, and cuts off a relay feeding a slow
// browser that is still reading. The call bounds the transfer instead
// (stream_bound). An upload is sent with idle (write_raw).
func (s *Stream) send(source io.Reader, idle time.Duration) (int64, error) {
	if s == nil || s.writer == nil {
		return 0, fmt.Errorf("stream not open for writing")
	}
	s.answered.Store(false) // what is written now is the far end's to read (abandon)
	destination := &stream_destination{writer: s.writer, idle: idle}
	if idle > 0 {
		defer stream_deadline_clear(s.writer)
	}
	n, err := io.Copy(destination, source)
	if err != nil {
		if destination.failed != nil {
			s.abandoned = true
		}
		s.reset()
	}
	return n, err
}

// stream_deadline_clear lifts a write deadline, so it does not linger to cut a
// later write that sets none of its own.
func stream_deadline_clear(writer io.Writer) {
	if w, ok := writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = w.SetWriteDeadline(time.Time{})
	}
}

// error_stream_reset is what the far end of a self-loop reads from a stream
// reset part way.
var error_stream_reset = errors.New("stream reset")

// reset ends the stream so the far end reads an error rather than the end of
// the body. Safe while another goroutine is blocked writing to it, which it
// unblocks.
func (s *Stream) reset() {
	if s != nil && s.writer != nil {
		stream_reset(s.writer)
	}
}

// stream_reset resets a peer's stream (libp2p Reset), closes the self-loop's
// pipe with an error, and closes anything else.
func stream_reset(writer io.Writer) {
	switch w := writer.(type) {
	case *stream_writer:
		stream_reset(w.inner)
	case interface{ Reset() error }:
		_ = w.Reset()
	case interface{ CloseWithError(error) error }:
		_ = w.CloseWithError(error_stream_reset)
	case io.Closer:
		_ = w.Close()
	}
}

// stream_bound ties a builtin's bulk transfer to its call. The transfer takes
// the transfer bound, since it is moving bytes rather than computing, and at the
// call's time limit the stream is reset under it: a write blocked on a peer
// that stopped reading ignores the thread's cancellation, so without the reset
// the call is abandoned still blocked, and mailed to the operator as a failure.
// The caller defers the returned stop.
func stream_bound(t *sl.Thread, s *Stream) func() bool {
	starlark_transfer_set(t)
	return context.AfterFunc(starlark_context(t), s.reset)
}

var (
	streams_lock       = &sync.Mutex{}
	stream_next  int64 = 1
)

// Create a new stream over /mochi/2/stream (authenticated handshake via claim +
// open), with content as its first segment. When the recipient entity has
// several known locations, each is tried in entity_peers_failover order until
// one completes the handshake.
func stream(from string, to string, service string, event string, from_app string, services []string, content any) (*Stream, error) {
	peers := entity_peers_failover_for(from, to)
	if len(peers) == 0 {
		return nil, fmt.Errorf("stream unable to determine location of entity %q", to)
	}

	return stream_failover(peers, func(peer string) (*Stream, error) {
		return stream_to_peer(context.Background(), peer, from, to, service, event, from_app, services, content)
	})
}

// stream_failover opens a stream at each of peers in turn until one opens. It
// stops at an open whose request may have run (error_stream_sent): another
// location would run it twice.
func stream_failover(peers []string, open func(string) (*Stream, error)) (*Stream, error) {
	var last_error error
	for _, peer := range peers {
		s, err := open(peer)
		if err == nil {
			return s, nil
		}
		if errors.Is(err, error_stream_sent) {
			return nil, err
		}
		last_error = err
	}
	return nil, last_error
}

// Create a stream to a specific peer, without entity lookup; a self-target
// routes to the in-process loopback. The stream is in raw mode - the open frame
// and content, the request's first segment, are already shipped and acked, so
// the caller reads and writes bytes directly. Passing content rather than
// writing it afterwards lets it travel with the open (stream_open); once ctx
// ends nothing more is sent.
func stream_to_peer(ctx context.Context, peer string, from string, to string, service string, event string, from_app string, services []string, content any) (*Stream, error) {
	s, err := stream_open_or_self(ctx, peer, from, to, service, event, from_app, services, content)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, fmt.Errorf("stream unable to open to peer %q", peer)
	}
	return s, nil
}

// stream_content is a request's first segment as passed to stream_to_peer: an
// empty map for none, which the far end reads as e.content as it would the
// null a script's None encodes to. Only nil, to stream_to_peer, means the
// caller writes its own first segment.
func stream_content(v any) any {
	if v == nil {
		return map[string]any{}
	}
	return v
}

// Get next stream ID
func stream_id() int64 {
	streams_lock.Lock()
	id := stream_next
	stream_next = stream_next + 1
	streams_lock.Unlock()
	return id
}

// Create a new stream from an existing reader and writer
func stream_rw(r io.ReadCloser, w io.WriteCloser) *Stream {
	return &Stream{id: stream_id(), reader: r, writer: w}
}

// stream_writer meters everything an app writes to a peer, wrapping s.writer so
// a write path added later is metered too. CloseWrite and SetWriteDeadline are
// forwarded deliberately: the write builtins type-assert on s.writer for them.
type stream_writer struct {
	inner io.WriteCloser
	app   string
}

func (w *stream_writer) Write(p []byte) (int, error) {
	if err := stream_outbound_refusal(w.app); err != nil {
		return 0, err
	}
	n, err := w.inner.Write(p)
	if n > 0 {
		stream_outbound_charge(w.app, n)
	}
	return n, err
}

func (w *stream_writer) Close() error { return w.inner.Close() }

func (w *stream_writer) CloseWrite() error {
	if cw, ok := w.inner.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return w.inner.Close()
}

func (w *stream_writer) SetWriteDeadline(t time.Time) error {
	if d, ok := w.inner.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return d.SetWriteDeadline(t)
	}
	return nil
}

// meter wraps the stream's writer so an app's outbound bytes are counted. No-op
// for an unattributed stream, and never double-wraps.
func (s *Stream) meter(app string) {
	if s == nil || s.writer == nil || app == "" {
		return
	}
	if _, already := s.writer.(*stream_writer); already {
		return
	}
	s.writer = &stream_writer{inner: s.writer, app: app}
}

// Close only the write direction of a stream (if supported), otherwise close entirely
func (s *Stream) close_write() {
	if s.writer == nil {
		return
	}
	// Check if writer supports CloseWrite (libp2p streams do)
	if cw, ok := s.writer.(interface{ CloseWrite() error }); ok {
		cw.CloseWrite()
	} else {
		s.writer.Close()
	}
}

// abandon resets the stream when closing it gives up on an answer the far end
// may still be sending. Over yamux a close tells the sender nothing - it is a
// local CloseRead, and data arriving after it is dropped unread - so a responder
// writing a body the requester gave up on stays blocked until its call's limit;
// a reset stops it. A reset discards whatever the far end holds unread, so only
// a requester resets - a responder's far end holds the answer it has just
// written - and only once the answer has begun since it last wrote, before which
// the far end may hold its request or upload. And only while the answer has not
// ended, so a request that has had its whole answer closes quietly. Decided once,
// by the first close.
func (s *Stream) abandon() {
	if s == nil || !s.requester || s.settled.Swap(true) {
		return
	}
	if s.answered.Load() && !s.ended() {
		s.reset()
	}
}

// stream_end_wait is how long the check for the end of an answer waits: long
// enough for an end already on its way, not for one still to be sent.
const stream_end_wait = 10 * time.Millisecond

// ended reports whether the far end has finished sending: nothing is held
// unread, and a read finds the end at once. What a read here takes is being
// given up on anyway.
func (s *Stream) ended() bool {
	if s.reader == nil {
		return true
	}
	if s.segments != nil && len(s.segments.buffer) > 0 {
		return false
	}
	probe := make([]byte, 1)
	r, ok := s.reader.(interface{ SetReadDeadline(time.Time) error })
	if !ok {
		return false
	}
	_ = r.SetReadDeadline(time.Now().Add(stream_end_wait))
	n, err := s.reader.Read(probe)
	return n == 0 && errors.Is(err, io.EOF)
}

// Close only the read direction of a stream (if supported), otherwise close entirely
func (s *Stream) close_read() {
	if s == nil || s.reader == nil {
		return
	}
	s.abandon()
	if cr, ok := s.reader.(interface{ CloseRead() error }); ok {
		cr.CloseRead()
	} else {
		s.reader.Close()
	}
}

// close closes both the reader and writer of the stream
func (s *Stream) close() {
	s.abandon()
	if s.reader != nil {
		s.reader.Close()
	}
	if s.writer != nil {
		s.writer.Close()
	}
	if s.on_close != nil {
		s.on_close_once.Do(s.on_close)
	}
}

// idle bounds the wait for a raw read of a stream's body, as read_within bounds
// a segment's: a body is read in many pieces, and each may wait the stream's
// read timeout for its bytes, no longer. Without it a reader whose far end
// stalls holds its goroutine for as long as the far end keeps the stream open,
// past the call that started it. raw_reader applies it to every read.
func (s *Stream) idle() {
	if s == nil || s.reader == nil {
		return
	}
	timeout := time.Duration(s.timeout.read) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if r, ok := s.reader.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = r.SetReadDeadline(time.Now().Add(timeout))
	}
}

// Read a CBOR encoded segment from a stream
func (s *Stream) read(v any) error {
	return s.read_within(v, 0)
}

// read_within is read with its wait shortened to limit, when limit is positive
// and shorter than the stream's own: a script may shorten a read, never
// lengthen it.
func (s *Stream) read_within(v any, limit time.Duration) error {
	return s.read_segment(v, limit, 0)
}

// read_segment is read_within with this one segment held to size bytes, when
// size is positive, below what the stream may still read.
func (s *Stream) read_segment(v any, limit time.Duration, size int64) error {
	if s == nil || s.reader == nil {
		return fmt.Errorf("stream not open for reading")
	}

	timeout := time.Duration(s.timeout.read) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if limit > 0 && limit < timeout {
		timeout = limit
	}

	deadline := time.Now().Add(timeout)
	if r, ok := s.reader.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = r.SetReadDeadline(deadline)
		defer r.SetReadDeadline(time.Time{})
	}

	if s.segments == nil {
		s.segments = &cbor_segments{reader: s.reader, remaining: s.cbor_limit()}
	}
	if size <= 0 {
		size = s.segments.remaining + int64(len(s.segments.buffer))
	}
	segment, err := s.segments.next(size)
	if err == nil {
		err = cbor_decode_mode.Unmarshal(segment, v)
	}
	if err != nil {
		// %w so callers can errors.Is the EOF class (a far end that
		// closed without answering) apart from decode corruption.
		return fmt.Errorf("stream %d unable to read segment: %w", s.id, err)
	}
	s.answered.Store(true)

	// debug("Stream %d read segment: %+v", s.id, v)
	return nil
}

// cbor_limit returns the cumulative byte limit on what segments are read from.
// The default caps a stream's lifetime reads at 100 MB, which breaks bulk DB
// transfer; those streams set s.maximum_bytes before the first read.
func (s *Stream) cbor_limit() int64 {
	if s.maximum_bytes > 0 {
		return s.maximum_bytes
	}
	return int64(cbor_maximum_size)
}

// Read a content segment from a stream
func (s *Stream) read_content() (map[string]any, error) {
	content := map[string]any{}
	err := s.read(&content)
	if err != nil {
		return nil, err
	}

	// Validate key/value sizes
	for k, v := range content {
		if len(k) > content_maximum_key {
			return nil, fmt.Errorf("content key too long: %d > %d", len(k), content_maximum_key)
		}
		if str, ok := v.(string); ok {
			if len(str) > content_maximum_value {
				return nil, fmt.Errorf("content value too long: %d > %d", len(str), content_maximum_value)
			}
		}
	}

	return content, nil
}

// Get a reader for raw data after CBOR reads (includes any data the segment
// reader read past the last segment). Every read through it waits the stream's
// read timeout for its bytes, no longer (idle), so no caller can forget to: a
// far end that stalls part way through a body ends the read rather than holding
// its caller for as long as it keeps the stream open, and one that keeps
// sending at any speed is never cut off.
func (s *Stream) raw_reader() io.Reader {
	var reader io.Reader = s.reader
	if s.segments != nil && len(s.segments.buffer) > 0 {
		reader = io.MultiReader(bytes.NewReader(s.segments.buffer), s.reader)
	}
	s.answered.Store(true)
	return &stream_idle_reader{stream: s, reader: reader}
}

// stream_idle_reader bounds each read of a stream's raw body (raw_reader).
type stream_idle_reader struct {
	stream *Stream
	reader io.Reader
}

func (r *stream_idle_reader) Read(p []byte) (int, error) {
	r.stream.idle()
	return r.reader.Read(p)
}

// Write a CBOR encoded segment to a stream
func (s *Stream) write(v any) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("stream not open for writing")
	}
	// debug("Stream %d writing segment: %+v", s.id, v)
	s.answered.Store(false) // what is written now is the far end's to read (abandon)

	timeout := s.timeout.write
	if timeout <= 0 {
		timeout = 30
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if w, ok := s.writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = w.SetWriteDeadline(deadline)
		defer w.SetWriteDeadline(time.Time{})
	}

	if s.encoder == nil {
		s.encoder = cbor.NewEncoder(s.writer)
	}
	err := s.encoder.Encode(v)
	if err != nil {
		// Unconditional, unlike send: the encoder's only output is s.writer,
		// and its input is already a plain Go value from sl_decode, so there
		// is no local source here for the failure to have come from.
		s.abandoned = true
		return fmt.Errorf("stream error writing segment: %v", err)
	}

	return nil
}

// Write a file to a stream as raw bytes, returns bytes written
func (s *Stream) write_file(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("stream unable to read file %q", path)
	}
	defer f.Close()

	n, err := s.send(f, 0)
	if err != nil {
		return 0, fmt.Errorf("stream error sending file segment: %v", err)
	}

	return n, nil
}

// Write a raw, unencoded or pre-encoded, segment. It goes as a transfer,
// through send: data may be a whole upload (market's comptroller_upload sends a
// digital asset in one write.raw), which one write deadline cut off while it was
// still moving. Each piece waits at most the stream's write timeout. An upload's
// receiver writes it to storage, handing credit back as fast as bytes arrive,
// so a piece that waits that long means the receiver has stopped - as one that
// answers before reading does: over yamux its close drops the rest unread
// without telling the sender, who would otherwise wait out the whole call.
func (s *Stream) write_raw(data []byte) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("stream not open for writing")
	}
	// debug("Stream %d writing raw segment: %d bytes", s.id, len(data))

	timeout := s.timeout.write
	if timeout <= 0 {
		timeout = 30
	}
	if _, err := s.send(bytes.NewReader(data), time.Duration(timeout)*time.Second); err != nil {
		return fmt.Errorf("stream error writing raw segment: %w", err)
	}
	return nil
}

// Starlark methods
func (s *Stream) AttrNames() []string {
	return []string{"read", "write", "close"}
}

func (s *Stream) Attr(name string) (sl.Value, error) {
	switch name {
	case "read":
		return &StreamRead{stream: s}, nil
	case "write":
		return &StreamWrite{stream: s}, nil
	case "close":
		return sl.NewBuiltin("close", s.sl_close), nil
	default:
		return nil, nil
	}
}

// StreamRead is callable as s.read() and exposes s.read.file(path).
type StreamRead struct {
	stream *Stream
}

func (sr *StreamRead) String() string        { return "stream.read" }
func (sr *StreamRead) Type() string          { return "stream.read" }
func (sr *StreamRead) Freeze()               {}
func (sr *StreamRead) Truth() sl.Bool        { return true }
func (sr *StreamRead) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable: stream.read") }
func (sr *StreamRead) Name() string          { return "read" }

// Callable: s.read(timeout) -> dict | None: Read the next decoded segment, waiting at most timeout seconds (at most 30, the default)
func (sr *StreamRead) CallInternal(t *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sr.stream.sl_read(t, nil, args, kwargs)
}

func (sr *StreamRead) AttrNames() []string {
	return []string{"file"}
}

func (sr *StreamRead) Attr(name string) (sl.Value, error) {
	switch name {
	case "file":
		return sl.NewBuiltin("read.file", sr.stream.sl_read_file), nil
	}
	return nil, nil
}

// StreamWrite is callable as s.write(values...) and exposes s.write.{asset,
// cache, file, raw}.
type StreamWrite struct {
	stream *Stream
}

func (sw *StreamWrite) String() string        { return "stream.write" }
func (sw *StreamWrite) Type() string          { return "stream.write" }
func (sw *StreamWrite) Freeze()               {}
func (sw *StreamWrite) Truth() sl.Bool        { return true }
func (sw *StreamWrite) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable: stream.write") }
func (sw *StreamWrite) Name() string          { return "write" }

// Callable: s.write(values...) -> bool: Write one or more encoded segments
func (sw *StreamWrite) CallInternal(t *sl.Thread, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	return sw.stream.sl_write(t, nil, args, kwargs)
}

func (sw *StreamWrite) AttrNames() []string {
	return []string{"asset", "cache", "file", "raw"}
}

func (sw *StreamWrite) Attr(name string) (sl.Value, error) {
	switch name {
	case "asset":
		return sl.NewBuiltin("write.asset", sw.stream.sl_write_asset), nil
	case "cache":
		return sl.NewBuiltin("write.cache", sw.stream.sl_write_cache), nil
	case "file":
		return sl.NewBuiltin("write.file", sw.stream.sl_write_file), nil
	case "raw":
		return sl.NewBuiltin("write.raw", sw.stream.sl_write_raw), nil
	}
	return nil, nil
}

func (s *Stream) Hash() (uint32, error) {
	return sl.String(fmt.Sprintf("%d", s.id)).Hash()
}

func (s *Stream) Freeze() {}

func (s *Stream) String() string {
	return fmt.Sprintf("Stream %d", s.id)
}

func (s *Stream) Truth() sl.Bool {
	return sl.True
}

func (s *Stream) Type() string {
	return "Stream"
}

// s.read() -> any: Read and decode the next segment from the stream
// s.read(timeout) -> any | None: the next segment, or None when none came
// within timeout seconds, the stream ended or a segment would not decode. A
// read that fails ends the stream for reading, so every stream reads the same
// way after one: on a local loop the read cut off by its deadline leaves its
// reader still taking the bytes that follow, which a later read would never
// see whole, and closing the read side also lets that reader go. An event
// with no stream answers None, as before.
func (s *Stream) sl_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var timeout sl.Value = sl.None
	if err := sl.UnpackArgs("read", args, kwargs, "timeout?", &timeout); err != nil {
		return nil, err
	}
	var limit time.Duration
	if timeout != sl.None {
		seconds, ok := sl.AsFloat(timeout)
		if !ok || !(seconds > 0) {
			return nil, fmt.Errorf("read: timeout must be a positive number of seconds")
		}
		limit = max(time.Duration(min(seconds, 30)*float64(time.Second)), time.Nanosecond)
	}
	var v any
	if err := s.read_within(&v, limit); err != nil {
		s.close_read()
		return sl.None, nil
	}
	return sl_encode(v), nil
}

// s.read.file(path, maximum=0) -> int: Read raw bytes from the stream and write
// them to a per-user data file, returns bytes read. Writes to the same
// filesystem as mochi.file.*. With a positive maximum, at most maximum+1 bytes
// are written - the extra byte lets the caller distinguish a source that fits
// exactly from one that overran, delete the file, and refuse the transfer.
func (s *Stream) sl_read_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// debug("Stream %d reading rest of stream to file", s.id)

	var file string
	maximum := 0
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "maximum?", &maximum); err != nil {
		s.close_read()
		return nil, err
	}

	user := principal_caller(t)
	if user == nil {
		s.close_read()
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		s.close_read()
		return sl_error(fn, "no app")
	}

	if !valid(file, "filepath") {
		s.close_read()
		return sl_error(fn, "invalid file %q", file)
	}

	// Check storage limit and calculate remaining space (admins exempt)
	remaining, err := storage_remaining(storage_request_of(t), user)
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	if remaining <= 0 {
		s.close_read()
		return sl_error(fn, "storage limit exceeded")
	}

	// Ensure base directory exists and open root for traversal protection
	base := api_file_base(user, app)
	if err := os.MkdirAll(base, 0755); err != nil {
		s.close_read()
		return sl_error(fn, "unable to create files directory: %v", err)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to access files directory")
	}
	defer root.Close()

	// Create parent directories within the root if needed
	dir := filepath.Dir(file)
	if dir != "." && dir != "" {
		if err := root_mkdir_all(root, dir); err != nil {
			s.close_read()
			return sl_error(fn, "unable to create directory")
		}
	}

	// Open file within root for writing. The truncation frees what the file
	// held, and what is written is charged as it lands (storage_meter).
	f, err := storage_create(root, user.UID, file)
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to write file")
	}

	// Use raw_reader() to include any bytes buffered by the CBOR decoder
	// This is critical when read_to_file follows a read() call
	reader := s.raw_reader()

	// Limit reader to remaining storage space, tightened to the caller's
	// expectation when one was given (+1 so an overrun is visible to it).
	if maximum > 0 && int64(maximum)+1 < remaining {
		remaining = int64(maximum) + 1
	}
	limited := io.LimitReader(reader, remaining)

	// From here the call is moving bytes, not computing: the transfer bound,
	// as for write.file, so a large upload that is still arriving is not cut at
	// the compute bound. And the copy ends with its call - given up on at the
	// time limit, its stream is closed under it, as mochi.cache.write's is -
	// since a sender trickling bytes renews each read's deadline for ever.
	starlark_transfer_set(t)
	stop := context.AfterFunc(starlark_context(t), s.close)
	defer stop()
	n, err := io.Copy(&storage_meter{writer: f, owner: user.UID}, limited)
	f.Close()

	// Another write took the room this one measured: what it stored is removed
	// and given back.
	if errors.Is(err, storage_exceeded) {
		s.close_read()
		storage_remove(root, user.UID, file)
		return sl_error(fn, "storage limit exceeded")
	}
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to save file %q", file)
	}

	s.close_read()
	// debug("Stream %d read %d bytes to file", s.id, n)

	return sl.MakeInt64(n), nil
}

// s.write(values...) -> bool: Write one or more encoded segments to the stream; False when a write failed
func (s *Stream) sl_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	for _, a := range args {
		err := s.write(sl_decode(a))
		if err != nil {
			return sl.False, nil
		}
	}
	return sl.True, nil
}

// s.write.raw(data) -> None: Send raw bytes without CBOR encoding
func (s *Stream) sl_write_raw(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <data: bytes>")
	}

	data, ok := args[0].(sl.Bytes)
	if !ok {
		return sl_error(fn, "data must be bytes")
	}

	stop := stream_bound(t, s) // bulk bytes, not computation - see sl_write_file
	defer stop()
	err := s.write_raw([]byte(data))
	if err != nil {
		return sl_error(fn, err)
	}

	return sl.None, nil
}

// sl_write_file is the Go-level implementation behind both s.* and e.* file
// writers. Sends per-user data file contents as raw bytes; returns bytes written.
func (s *Stream) sl_write_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	defer s.close_write()
	var file string
	var offset int64
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "file", &file, "offset?", &offset); err != nil {
		return sl_error(fn, "syntax: <file: string>, [offset: integer]")
	}

	user := principal_caller(t)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	if !valid(file, "filepath") {
		return sl_error(fn, "invalid file %q", file)
	}

	// Open file using os.Root for traversal protection
	base := api_file_base(user, app)
	root, err := os.OpenRoot(base)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer root.Close()

	f, err := root.Open(file)
	if err != nil {
		return sl_error(fn, "file not found")
	}
	defer f.Close()

	// A resuming transfer starts mid-file. Bounds-checked against the file
	// itself, so a peer's stale belief about the size is refused here rather
	// than becoming a short read the receiver cannot tell from truncation.
	if offset < 0 {
		return sl_error(fn, "invalid offset %d", offset)
	}
	if offset > 0 {
		information, err := f.Stat()
		if err != nil {
			return sl_error(fn, "file not found")
		}
		if offset > information.Size() {
			return sl_error(fn, "offset %d beyond the file's %d bytes", offset, information.Size())
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return sl_error(fn, "unable to seek: %v", err)
		}
	}

	// From here the call is only moving bytes, so it takes the transfer bound, not
	// the compute one: a slow peer would otherwise hit the 90-second timeout
	// inside io.Copy, which ignores cancellation. And it ends with its call
	// (stream_bound).
	stop := stream_bound(t, s)
	defer stop()

	n, err := s.send(f, 0)
	if err != nil {
		// The cause is carried, not flattened: a peer that went away and a
		// disk that would not read reported the same fixed string, so the
		// operator email could not tell a client disconnect from real trouble.
		return sl_error(fn, "unable to send file: %v", err)
	}

	return sl.MakeInt64(n), nil
}

// s.write.cache(name) -> int or None: Send a cache entry's bytes over the
// stream, returning None on a cache miss so a responder can fill and retry.
// Used by byte-pull responders that keep a local cached copy.
func (s *Stream) sl_write_cache(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	defer s.close_write()
	var name string
	if err := sl.UnpackArgs(fn.Name(), args, kwargs, "name", &name); err != nil {
		return sl_error(fn, "syntax: write.cache(name)")
	}

	path, err := cache_serve_file(t, name)
	if err != nil {
		return sl.None, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return sl.None, nil
	}
	defer f.Close()

	stop := stream_bound(t, s) // bulk bytes, not computation - see sl_write_file
	defer stop()

	n, err := s.send(f, 0)
	if err != nil {
		return sl_error(fn, "unable to send cache entry: %v", err)
	}
	return sl.MakeInt64(n), nil
}

// s.write.asset(path) -> int: Send the contents of a bundled app asset as raw
// bytes, returns bytes written. Reads from the same filesystem as mochi.app.asset.*.
func (s *Stream) sl_write_asset(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	defer s.close_write()
	if len(args) != 1 {
		return sl_error(fn, "syntax: <path: string>")
	}

	path, ok := sl.AsString(args[0])
	if !ok || !valid(path, "filepath") {
		return sl_error(fn, "invalid path %q", path)
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	user := principal_caller(t)
	file := app_asset_path(app, user, path)
	if file == "" {
		return sl_error(fn, "file not found")
	}

	stop := stream_bound(t, s) // bulk bytes, not computation - see sl_write_file
	defer stop()

	n, err := s.write_file(file)
	if err != nil {
		return sl_error(fn, "unable to send file")
	}

	return sl.MakeInt64(n), nil
}

// s.close() -> None: Close the write side of the stream (signals EOF to reader)
func (s *Stream) sl_close(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	s.close_write()
	return sl.None, nil
}
