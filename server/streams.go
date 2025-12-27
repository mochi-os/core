// Mochi server: Streams
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"crypto/rand"
	"fmt"
	cbor "github.com/fxamacker/cbor/v2"
	sl "go.starlark.net/starlark"
	"io"
	"os"
	"sync"
	"time"
)

const (
	challenge_size    = 16
	cbor_max_size     = 100 * 1024 * 1024 // 100MB max message size
	cbor_max_depth    = 32                // Max nesting depth
	cbor_max_pairs    = 1000              // Max map pairs
	cbor_max_elements = 10000             // Max array elements
	headers_max_size  = 4 * 1024          // 4KB max headers size
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

// Create a new stream with specified headers (reads challenge, then sends)
func stream(from string, to string, service string, event string) (*Stream, error) {
	peer := entity_peer(to)
	if peer == "" {
		return nil, fmt.Errorf("stream unable to determine location of entity %q", to)
	}

	s := peer_stream(peer)
	if s == nil {
		return nil, fmt.Errorf("stream unable to open to peer %q for entity %q", peer, to)
	}

	// Read challenge from receiver
	challenge, err := s.read_challenge()
	if err != nil {
		return nil, fmt.Errorf("stream unable to read challenge: %v", err)
	}

	//debug("Stream %d open to peer %q: from %q, to %q, service %q, event %q", s.id, peer, from, to, service, event)

	id := uid()
	signature := entity_sign(from, string(signable_headers("msg", from, to, service, event, id, "", challenge)))
	err = s.write(Headers{Type: "msg", From: from, To: to, Service: service, Event: event, ID: id, Signature: signature})
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Create a stream to a specific peer (without entity lookup)
func stream_to_peer(peer string, from string, to string, service string, event string) (*Stream, error) {
	s := peer_stream(peer)
	if s == nil {
		return nil, fmt.Errorf("stream unable to open to peer %q", peer)
	}

	// Read challenge from receiver
	challenge, err := s.read_challenge()
	if err != nil {
		return nil, fmt.Errorf("stream unable to read challenge: %v", err)
	}

	//debug("Stream %d open to peer %q: from %q, to %q, service %q, event %q", s.id, peer, from, to, service, event)

	id := uid()
	signature := entity_sign(from, string(signable_headers("msg", from, to, service, event, id, "", challenge)))
	err = s.write(Headers{Type: "msg", From: from, To: to, Service: service, Event: event, ID: id, Signature: signature})
	if err != nil {
		return nil, err
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

// Close closes both the reader and writer of the stream
func (s *Stream) close() {
	if s.reader != nil {
		s.reader.Close()
	}
	if s.writer != nil {
		s.writer.Close()
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
		// debug("Stream %d received ACK for ID %q", s.id, h.AckID)
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

	// Verify signature (challenge is nil for pubsub, which allows unsigned broadcasts)
	// For anonymous events, allow through with cleared From header - event handler checks Anonymous flag
	if s.challenge != nil && !h.verify(s.challenge) {
		h.From = ""
	}

	// Deduplication check: skip if we've already processed this message ID
	if h.ID != "" && message_seen(h.ID) {
		debug("Stream %d duplicate message %q, sending ACK only", s.id, h.ID)
		if h.From != "" && h.To != "" && s.writer != nil {
			s.send_ack("ack", h.ID, h.To, h.From)
		}
		return
	}

	// Decode the content segment
	content, err := s.read_content()
	if err != nil {
		info("Stream %d error reading content: %v", s.id, err)
		if h.From != "" && h.To != "" && h.ID != "" && s.writer != nil {
			s.send_ack("nack", h.ID, h.To, h.From)
		}
		return
	}

	//debug("Stream %d from peer %q: from %q, to %q, service %q, event %q, content '%+v'", s.id, peer, h.From, h.To, h.Service, h.Event, content)

	// Create event and route to app
	e := Event{id: event_id(), msg_id: h.ID, from: h.From, to: h.To, service: h.Service, event: h.Event, peer: peer, content: content, stream: s}
	route_err := e.route()

	// Mark message as processed for deduplication
	if h.ID != "" {
		message_mark_seen(h.ID)
	}

	// Send ACK on success, NACK on failure (only for direct signed messages)
	// ACK is sent on same stream without challenge (TLS provides transport security)
	if h.From != "" && h.To != "" && h.ID != "" && s.writer != nil {
		if route_err == nil {
			s.send_ack("ack", h.ID, h.To, h.From)
		} else {
			s.send_ack("nack", h.ID, h.To, h.From)
		}
	}
}

// Send ACK/NACK on existing stream (no challenge - TLS provides security)
func (s *Stream) send_ack(ack_type, ack_id, from, to string) {
	signature := entity_sign(from, string(signable_headers(ack_type, from, to, "", "", "", ack_id, nil)))

	headers := cbor_encode(Headers{
		Type: ack_type, From: from, To: to, AckID: ack_id, Signature: signature,
	})

	if s.write_raw(headers) == nil {
		// debug("Stream %d sent %s for ID %q", s.id, ack_type, ack_id)
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
		s.decoder = cbor_decode_mode.NewDecoder(io.LimitReader(s.reader, cbor_max_size))
	}
	err := s.decoder.Decode(v)
	if err != nil {
		return fmt.Errorf("stream %d unable to read segment: %v", s.id, err)
	}

	// debug("Stream %d read segment: %+v", s.id, v)
	return nil
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
	// Use cbor_max_size since the same decoder is reused for content and segments
	if s.decoder != nil {
		return fmt.Errorf("stream %d: read_headers must be called before read", s.id)
	}
	s.decoder = cbor_decode_mode.NewDecoder(io.LimitReader(s.reader, cbor_max_size))

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
	return []string{"read", "read_to_file", "write", "write_raw", "write_from_file", "close"}
}

func (s *Stream) Attr(name string) (sl.Value, error) {
	switch name {
	case "read":
		return sl.NewBuiltin("read", s.sl_read), nil
	case "read_to_file":
		return sl.NewBuiltin("read_to_file", s.sl_read_to_file), nil
	case "write":
		return sl.NewBuiltin("write", s.sl_write), nil
	case "write_raw":
		return sl.NewBuiltin("write_raw", s.sl_write_raw), nil
	case "write_from_file":
		return sl.NewBuiltin("write_from_file", s.sl_write_from_file), nil
	case "close":
		return sl.NewBuiltin("close", s.sl_close), nil
	default:
		return nil, nil
	}
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
		return sl_error(fn, err)
	}
	return sl_encode(v), nil
}

// s.read_to_file(path) -> int: Read raw bytes from stream and write to file, returns bytes read
func (s *Stream) sl_read_to_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// debug("Stream %d reading rest of stream to file", s.id)

	if len(args) != 1 {
		s.reader.Close()
		return sl_error(fn, "syntax: <file: string>")
	}

	user := t.Local("user").(*User)
	if user == nil {
		s.reader.Close()
		return sl_error(fn, "no user")
	}

	app, ok := t.Local("app").(*App)
	if !ok || app == nil {
		s.reader.Close()
		return sl_error(fn, "no app")
	}

	file, ok := sl.AsString(args[0])
	if !ok || !valid(file, "filepath") {
		s.reader.Close()
		return sl_error(fn, "invalid file %q", file)
	}

	// Check storage limit and calculate remaining space
	current := dir_size(user_storage_dir(user))
	remaining := file_max_storage - current
	if remaining <= 0 {
		s.reader.Close()
		return sl_error(fn, "storage limit exceeded")
	}

	// Use raw_reader() to include any bytes buffered by the CBOR decoder
	// This is critical when read_to_file follows a read() call
	reader := s.raw_reader()

	// Limit reader to remaining storage space
	limited := io.LimitReader(reader, remaining)
	n, ok := file_write_from_reader_count(api_file_path(user, app, file), limited)
	if !ok {
		s.reader.Close()
		return sl_error(fn, "unable to save file %q", file)
	}

	s.reader.Close()
	// debug("Stream %d read %d bytes to file", s.id, n)
	return sl.MakeInt64(n), nil
}

// s.write(values...) -> None: Write one or more encoded segments to the stream
func (s *Stream) sl_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	for _, a := range args {
		err := s.write(sl_decode(a))
		if err != nil {
			return sl_error(fn, err)
		}
	}
	return sl.None, nil
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

// s.write_from_file(path) -> int: Send file contents as raw bytes, returns bytes written
func (s *Stream) sl_write_from_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	// debug("Stream %d writing from file", s.id)
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

	n, err := s.write_file(api_file_path(user, app, file))
	if err != nil {
		return sl_error(fn, "unable to send file")
	}

	// debug("Stream %d wrote %d bytes from file", s.id, n)
	return sl.MakeInt64(n), nil
}

// s.close() -> None: Close the write side of the stream (signals EOF to reader)
func (s *Stream) sl_close(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	s.close_write()
	return sl.None, nil
}
