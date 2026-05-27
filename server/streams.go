// Mochi server: Streams
// Copyright Alistair Cunningham 2024-2026

package main

import (
	"crypto/rand"
	"fmt"
	cbor "github.com/fxamacker/cbor/v2"
	sl "go.starlark.net/starlark"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const peer_max_streams = 32 // Max concurrent outbound streams per peer

const (
	challenge_size    = 16
	cbor_max_size     = 100 * 1024 * 1024 // 100MB max message size
	cbor_max_depth    = 32                // Max nesting depth
	cbor_max_pairs    = 1000              // Max map pairs
	cbor_max_elements = 10000             // Max array elements
	content_max_key   = 256               // Max content key length
	content_max_value = 100 * 1024 * 1024 // 100MB max content value length
)

var cbor_decode_mode cbor.DecMode

func init() {
	cbor_decode_mode = must(cbor.DecOptions{
		MaxMapPairs:      cbor_max_pairs,
		MaxArrayElements: cbor_max_elements,
		MaxNestedLevels:  cbor_max_depth,
	}.DecMode())
}

type Stream struct {
	id        int64
	reader    io.ReadCloser
	writer    io.WriteCloser
	decoder   *cbor.Decoder
	encoder   *cbor.Encoder
	challenge []byte // For incoming streams: challenge we sent
	remote    string // Remote address (for incoming streams)
	timeout   struct {
		read  int
		write int
	}
	// max_bytes overrides the cumulative LimitReader cap used to
	// wrap the CBOR decoder. Zero = use cbor_max_size (100 MB total
	// for the stream's lifetime); set to a larger value before the
	// first read on streams that legitimately carry hundreds of MB
	// or more (bulk-bootstrap DB transfer). Must be set BEFORE the
	// first read or read_headers call, since the decoder + its
	// underlying LimitReader are constructed lazily.
	max_bytes     int64
	on_close      func() // Called once when stream is closed (e.g. release semaphore)
	on_close_once sync.Once
}

var (
	streams_lock       = &sync.Mutex{}
	stream_next  int64 = 1
)

// Generate a random challenge
func stream_challenge() ([]byte, error) {
	b := make([]byte, challenge_size)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// Create a new stream with specified headers. Prefers /mochi/2/stream
// (authenticated handshake via claim + open), falls back to /mochi/1's
// peer_stream + per-stream signed Headers for peers that haven't
// rolled out the new protocol.
//
// Multi-host failover: when the recipient entity has multiple known
// locations, try each in order until one accepts the stream. Order is
// from entity_peers_failover — active peers (seen within 2× republish
// interval) sorted oldest-seen first, then stale peers as a last
// resort. Stops at the first peer that completes the handshake.
func stream(from string, to string, service string, event string, from_app string, services []string) (*Stream, error) {
	peers := entity_peers_failover(to)
	if len(peers) == 0 {
		return nil, fmt.Errorf("stream unable to determine location of entity %q", to)
	}

	var last_err error
	for _, peer := range peers {
		s, err := stream_to_peer(peer, from, to, service, event, from_app, services)
		if err == nil {
			return s, nil
		}
		last_err = err
	}
	return nil, last_err
}

// Create a stream to a specific peer (without entity lookup).
func stream_to_peer(peer string, from string, to string, service string, event string, from_app string, services []string) (*Stream, error) {
	s, v2, err := stream_open_v2_or_legacy(peer, from, to, service, event, from_app, services, nil)
	if err != nil || s == nil {
		if err == nil {
			err = fmt.Errorf("stream unable to open to peer %q", peer)
		}
		return nil, err
	}
	if v2 {
		// v2 already shipped the open frame + got the ack; the
		// stream is now in raw mode for the caller.
		return s, nil
	}

	// Legacy /mochi/1 path: read challenge, write signed Headers.
	challenge, cerr := s.read_challenge()
	if cerr != nil {
		s.close()
		return nil, fmt.Errorf("stream unable to read challenge from peer %q: %v", peer, cerr)
	}

	id := uid()
	signature := entity_sign(from, string(signable_headers("msg", from, to, service, event, from_app, id, "", "", services, challenge)))
	if werr := s.write(Headers{Type: "msg", From: from, To: to, Service: service, Event: event, FromApp: from_app, Services: services, ID: id, Signature: signature}); werr != nil {
		s.close()
		return nil, werr
	}
	return s, nil
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

// Close only the read direction of a stream (if supported), otherwise close entirely
func (s *Stream) close_read() {
	if s.reader == nil {
		return
	}
	if cr, ok := s.reader.(interface{ CloseRead() error }); ok {
		cr.CloseRead()
	} else {
		s.reader.Close()
	}
}

// Close closes both the reader and writer of the stream
func (s *Stream) close() {
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

// Receive stream (send challenge first for direct streams)
func stream_receive(s *Stream, version int, peer string) {
	// Send challenge if this is a bidirectional stream (not pubsub)
	if s.writer != nil {
		var err error
		s.challenge, err = stream_challenge()
		if err != nil {
			info("Stream %d error generating challenge: %v", s.id, err)
			return
		}
		if err := s.write_raw(s.challenge); err != nil {
			info("Stream %d error sending challenge: %v", s.id, err)
			return
		}
	}

	// Get and verify message headers (limited to 4KB)
	var h Headers
	err := s.read_headers(&h)
	if err != nil {
		info("Stream %d error reading headers: %v", s.id, err)
		return
	}
	if !h.valid() {
		info("Stream %d received invalid headers", s.id)
		return
	}

	// Handle ACK/NACK messages
	msg_type := h.msg_type()
	if msg_type == "ack" {
		if !h.verify(s.challenge) {
			info("Stream %d ACK failed signature verification", s.id)
			return
		}
		//debug("Stream %d received ACK for ID %q", s.id, h.AckID)
		queue_ack(h.AckID)
		return
	}
	if msg_type == "nack" {
		if !h.verify(s.challenge) {
			info("Stream %d NACK failed signature verification", s.id)
			return
		}
		// debug("Stream %d received NACK for ID %q", s.id, h.AckID)
		queue_fail(h.AckID, "NACK received")
		return
	}

	// Verify signature — challenge is nil for pubsub, non-nil for direct streams.
	// For anonymous events, allow through with cleared From header - event handler checks Anonymous flag
	if !h.verify(s.challenge) {
		h.From = ""
	}

	// Deduplication check: skip if we've already processed this message
	// ID. ACK gate matches the success path below — anonymous server-to-
	// server events have From="" / To="" but still need ACKs, otherwise
	// the sender's queue retries forever (caught live: duplicate
	// bootstrap manifest-result messages logged "sending ACK only" but
	// the From/To gate silently skipped the actual send, leaving 30
	// queue rows stuck in retry-forever state on instance 1).
	if h.ID != "" && message_seen(h.ID) {
		debug("Stream %d duplicate message %q, sending ACK only", s.id, h.ID)
		if s.writer != nil {
			s.send_ack("ack", h.ID, h.To, h.From, "")
		}
		return
	}

	// Decode the content segment
	content, err := s.read_content()
	if err != nil {
		info("Stream %d error reading content: %v", s.id, err)
		// Same gate as the success path — anonymous server-to-server
		// events (From="") need NACKs too, otherwise the sender's queue
		// retries forever on a permanent decode error.
		if h.ID != "" && s.writer != nil {
			s.send_ack("nack", h.ID, h.To, h.From, nack_reason_decode_failed)
		}
		return
	}

	//debug("Stream %d from peer %q: from %q, to %q, service %q, event %q, content '%+v'", s.id, peer, h.From, h.To, h.Service, h.Event, content)

	// Create event and route to app
	e := Event{id: event_id(), msg_id: h.ID, from: h.From, to: h.To, service: h.Service, event: h.Event, sender_app: h.FromApp, sender_services: h.Services, peer: peer, content: content, stream: s}
	route_err := e.route()

	// Mark message as processed for deduplication
	if h.ID != "" {
		message_mark_seen(h.ID)
	}

	// Send ACK on success, NACK on failure. Any message with an ID
	// gets a reply on the stream — anonymous server-to-server events
	// (From="") need ACKs too, otherwise the sender's queue retries
	// indefinitely. Without this, a paired server emitting hundreds
	// of system-set / bootstrap-* / link-request ops accumulates
	// unbounded pending rows in queue.db (caught live: instance 1's
	// queue.db reached 3GB with 1100+ stuck bootstrap-db-chunks
	// before SQLite signalled "database or disk is full").
	if h.ID != "" && s.writer != nil {
		if route_err == nil {
			s.send_ack("ack", h.ID, h.To, h.From, "")
		} else {
			s.send_ack("nack", h.ID, h.To, h.From, nack_reason_from_error(route_err))
		}
	}
}

// Send ACK/NACK on existing stream (no challenge - TLS provides security)
// send_ack writes an ACK or NACK frame back to the sender. reason is
// a machine-readable hint used on NACKs (e.g. "broadcast-gap") so the
// sender can decide between retry and drop without parsing the
// (info-only) error string. Pass "" for ACKs and for unspecified
// NACKs - the wire field is omitempty and old peers ignore it.
func (s *Stream) send_ack(ack_type, ack_id, from, to, reason string) {
	signature := entity_sign(from, string(signable_headers(ack_type, from, to, "", "", "", "", ack_id, reason, nil, nil)))

	headers := cbor_encode(Headers{
		Type: ack_type, From: from, To: to, AckID: ack_id, Reason: reason, Signature: signature,
	})

	if s.write_raw(headers) == nil {
		// debug("Stream %d sent %s for ID %q (reason=%q)", s.id, ack_type, ack_id, reason)
	}

	if s.writer != nil {
		s.writer.Close()
	}
}

// Read challenge from stream
func (s *Stream) read_challenge() ([]byte, error) {
	if s == nil || s.reader == nil {
		return nil, fmt.Errorf("stream not open for reading")
	}

	timeout := s.timeout.read
	if timeout <= 0 {
		timeout = 30
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if r, ok := s.reader.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = r.SetReadDeadline(deadline)
		defer r.SetReadDeadline(time.Time{})
	}

	challenge := make([]byte, challenge_size)
	_, err := io.ReadFull(s.reader, challenge)
	if err != nil {
		return nil, err
	}
	return challenge, nil
}

// Read a CBOR encoded segment from a stream
func (s *Stream) read(v any) error {
	if s == nil || s.reader == nil {
		return fmt.Errorf("stream not open for reading")
	}

	timeout := s.timeout.read
	if timeout <= 0 {
		timeout = 30
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if r, ok := s.reader.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = r.SetReadDeadline(deadline)
		defer r.SetReadDeadline(time.Time{})
	}

	if s.decoder == nil {
		s.decoder = cbor_decode_mode.NewDecoder(io.LimitReader(s.reader, s.cbor_limit()))
	}
	err := s.decoder.Decode(v)
	if err != nil {
		return fmt.Errorf("stream %d unable to read segment: %v", s.id, err)
	}

	// debug("Stream %d read segment: %+v", s.id, v)
	return nil
}

// cbor_limit returns the cumulative byte limit applied to the CBOR
// decoder via io.LimitReader. The default (cbor_max_size) caps total
// decoder reads at 100 MB for a stream's lifetime, which is sufficient
// for normal app-message streams but breaks bulk-bootstrap DB transfer
// (a 948 MB feeds.db hits the cap at offset ~100 MB). Streams that
// legitimately carry more bytes set s.max_bytes before the first read.
func (s *Stream) cbor_limit() int64 {
	if s.max_bytes > 0 {
		return s.max_bytes
	}
	return int64(cbor_max_size)
}

// Read headers from a stream (limited to 4KB)
func (s *Stream) read_headers(h *Headers) error {
	if s == nil || s.reader == nil {
		return fmt.Errorf("stream not open for reading")
	}

	timeout := s.timeout.read
	if timeout <= 0 {
		timeout = 30
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if r, ok := s.reader.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = r.SetReadDeadline(deadline)
		defer r.SetReadDeadline(time.Time{})
	}

	// Use a size-limited decoder (must be called before read())
	// Use s.cbor_limit() since the same decoder is reused for content and segments
	if s.decoder != nil {
		return fmt.Errorf("stream %d: read_headers must be called before read", s.id)
	}
	s.decoder = cbor_decode_mode.NewDecoder(io.LimitReader(s.reader, s.cbor_limit()))

	err := s.decoder.Decode(h)
	if err != nil {
		return fmt.Errorf("stream %d unable to read headers: %v", s.id, err)
	}

	// Keep using the same decoder for subsequent reads
	// (the decoder may have buffered data that would be lost if we created a new one)

	// debug("Stream %d read headers: %+v", s.id, h)
	return nil
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
		if len(k) > content_max_key {
			return nil, fmt.Errorf("content key too long: %d > %d", len(k), content_max_key)
		}
		if str, ok := v.(string); ok {
			if len(str) > content_max_value {
				return nil, fmt.Errorf("content value too long: %d > %d", len(str), content_max_value)
			}
		}
	}

	return content, nil
}

// Get a reader for raw data after CBOR reads (includes any buffered data from decoder)
func (s *Stream) raw_reader() io.Reader {
	if s.decoder == nil {
		return s.reader
	}
	// Decoder's Buffered() returns any data read but not yet decoded
	buffered := s.decoder.Buffered()
	return io.MultiReader(buffered, s.reader)
}

// Write a CBOR encoded segment to a stream
func (s *Stream) write(v any) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("stream not open for writing")
	}
	// debug("Stream %d writing segment: %+v", s.id, v)

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
		return fmt.Errorf("stream error writing segment: %v", err)
	}

	return nil
}

// Write field/value pairs to a stream as a CBOR encoded segment
func (s *Stream) write_content(in ...string) error {
	content := map[string]string{}

	for {
		if len(in) < 2 {
			break
		}
		content[in[0]] = in[1]
		in = in[2:]
	}

	return s.write(content)
}

// Write a file to a stream as raw bytes, returns bytes written
func (s *Stream) write_file(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("stream unable to read file %q", path)
	}
	defer f.Close()

	n, err := io.Copy(s.writer, f)
	if err != nil {
		return 0, fmt.Errorf("stream error sending file segment: %v", err)
	}

	return n, nil
}

// Write a raw, unencoded or pre-encoded, segment
func (s *Stream) write_raw(data []byte) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("stream not open for writing")
	}
	// debug("Stream %d writing raw segment: %d bytes", s.id, len(data))

	timeout := s.timeout.write
	if timeout <= 0 {
		timeout = 30
	}

	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	if w, ok := s.writer.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = w.SetWriteDeadline(deadline)
		defer w.SetWriteDeadline(time.Time{})
	}

	_, err := s.writer.Write(data)
	if err != nil {
		return fmt.Errorf("stream error writing raw segment: %v", err)
	}

	// debug("Stream %d wrote raw segment", s.id)
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

// Callable: s.read() -> dict | None: Read the next decoded segment
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

// StreamWrite is callable as s.write(values...) and exposes s.write.{raw, asset}.
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
	return []string{"asset", "raw"}
}

func (sw *StreamWrite) Attr(name string) (sl.Value, error) {
	switch name {
	case "asset":
		return sl.NewBuiltin("write.asset", sw.stream.sl_write_asset), nil
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
func (s *Stream) sl_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var v any
	err := s.read(&v)
	if err != nil {
		return sl.None, nil
	}
	return sl_encode(v), nil
}

// s.read.file(path) -> int: Read raw bytes from the stream and write them to a
// per-user data file, returns bytes read. Writes to the same filesystem as mochi.file.*.
func (s *Stream) sl_read_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// debug("Stream %d reading rest of stream to file", s.id)

	if len(args) != 1 {
		s.close_read()
		return sl_error(fn, "syntax: <file: string>")
	}

	user := t.Local("user").(*User)
	if user == nil {
		s.close_read()
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		s.close_read()
		return sl_error(fn, "no app")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		s.close_read()
		return sl_error(fn, "invalid file %q", file)
	}

	// Check storage limit and calculate remaining space
	current, err := dir_size(user_storage_dir(user))
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to measure storage: %v", err)
	}
	remaining := file_max_storage - current
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

	// Open file within root for writing
	f, err := root.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to write file")
	}

	// Use raw_reader() to include any bytes buffered by the CBOR decoder
	// This is critical when read_to_file follows a read() call
	reader := s.raw_reader()

	// Limit reader to remaining storage space
	limited := io.LimitReader(reader, remaining)
	n, err := io.Copy(f, limited)
	f.Close()

	if err != nil {
		s.close_read()
		return sl_error(fn, "unable to save file %q", file)
	}

	s.close_read()
	// debug("Stream %d read %d bytes to file", s.id, n)
	return sl.MakeInt64(n), nil
}

// s.write(values...) -> None: Write one or more encoded segments to the stream
func (s *Stream) sl_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	for _, a := range args {
		err := s.write(sl_decode(a))
		if err != nil {
			return sl.False, nil
		}
	}
	return sl.True, nil
}

// s.write_raw(data) -> None: Send raw bytes without CBOR encoding
func (s *Stream) sl_write_raw(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	if len(args) != 1 {
		return sl_error(fn, "syntax: <data: bytes>")
	}

	data, ok := args[0].(sl.Bytes)
	if !ok {
		return sl_error(fn, "data must be bytes")
	}

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
	if len(args) != 1 {
		return sl_error(fn, "syntax: <file: string>")
	}

	user := t.Local("user").(*User)
	if user == nil {
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		return sl_error(fn, "no app")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
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

	n, err := io.Copy(s.writer, f)
	if err != nil {
		return sl_error(fn, "unable to send file")
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

	user, _ := t.Local("user").(*User)
	file := app_local_path(app, user, path)
	if file == "" {
		return sl_error(fn, "no active app version")
	}

	// Reject symlinks
	if file_is_symlink(file) {
		return sl_error(fn, "file not found")
	}

	if !file_exists(file) {
		return sl_error(fn, "file not found")
	}

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
