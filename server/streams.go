// Mochi server: Streams
// Copyright Alistair Cunningham 2024-2025

package main

import (
	"fmt"
	cbor "github.com/fxamacker/cbor/v2"
	sl "go.starlark.net/starlark"
	"io"
	"os"
	"sync"
	"time"
)

type Stream struct {
	id      int64
	reader  io.ReadCloser
	writer  io.WriteCloser
	decoder *cbor.Decoder
	encoder *cbor.Encoder
	timeout struct {
		read  int
		write int
	}
}

var (
	streams_lock       = &sync.Mutex{}
	stream_next  int64 = 1
)

// Create a new stream with specified headers
func stream(from string, to string, service string, event string) (*Stream, error) {
	peer := entity_peer(to)
	if peer == "" {
		return nil, fmt.Errorf("Stream unable to determine location of entity %q", to)
	}

	s := peer_stream(peer)
	if s == nil {
		return nil, fmt.Errorf("Stream unable to open to peer %q for entity %q", peer, to)
	}
	debug("Stream %d open to peer %q: from %q, to %q, service %q, event %q", s.id, peer, from, to, service, event)

	err := s.write(Headers{From: from, To: to, Service: service, Event: event, Signature: entity_sign(from, from+to+service+event)})
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

// Receive stream
func stream_receive(s *Stream, version int, peer string) {
	// Get and verify message headers
	var h Headers
	err := s.read(&h)
	if err != nil {
		info("Stream %d error reading headers: %v", s.id, err)
	}
	if !h.valid() {
		info("Stream %d received invalid headers", s.id)
		return
	}

	// Decode the content segment
	content, err := s.read_content()
	if err != nil {
		info("Stream %d error reading content: %v", s.id, err)
		return
	}

	debug("Stream %d open from peer %q: from %q, to %q, service %q, event %q, content '%+v'", s.id, peer, h.From, h.To, h.Service, h.Event, content)

	// Create event, and route to app
	e := Event{id: event_id(), from: h.From, to: h.To, service: h.Service, event: h.Event, peer: peer, content: content, stream: s}
	e.route()
}

// Read a CBOR encoded segment from a stream
func (s *Stream) read(v any) error {
	if s == nil || s.reader == nil {
		return fmt.Errorf("Stream not open for reading")
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
		s.decoder = cbor.NewDecoder(s.reader)
	}
	err := s.decoder.Decode(v)
	if err != nil {
		return fmt.Errorf("Stream %d unable to read segment: %v", s.id, err)
	}

	debug("Stream %d read segment: %+v", s.id, v)
	return nil
}

// Read a content segment from a stream
func (s *Stream) read_content() (map[string]string, error) {
	content := map[string]string{}
	err := s.read(&content)
	if err != nil {
		return nil, err
	}
	return content, nil
}

// Write a CBOR encoded segment to a stream
func (s *Stream) write(v any) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("Stream not open for writing")
	}
	debug("Stream %d writing segment: %+v", s.id, v)

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
		return fmt.Errorf("Stream error writing segment: %v", err)
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

// Write a file to a stream as raw bytes
func (s *Stream) write_file(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("Stream unable to read file %q", path)
	}
	defer f.Close()

	_, err = io.Copy(s.writer, f)
	if err != nil {
		return fmt.Errorf("Stream error sending file segment: %v", err)
	}

	return nil
}

// Write a raw, unencoded or pre-encoded, segment
func (s *Stream) write_raw(data []byte) error {
	if s == nil || s.writer == nil {
		return fmt.Errorf("Stream not open for writing")
	}
	debug("Stream %d writing raw segment: %v", s.id, data)

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
		return fmt.Errorf("Stream error writing raw segment: %v", err)
	}

	debug("Stream %d wrote raw segment", s.id)
	return nil
}

// Starlark methods
func (s *Stream) AttrNames() []string {
	return []string{"read", "read_to_file", "write", "write_from_file"}
}

func (s *Stream) Attr(name string) (sl.Value, error) {
	switch name {
	case "read":
		return sl.NewBuiltin("read", s.sl_read), nil
	case "read_to_file":
		return sl.NewBuiltin("read_to_file", s.sl_read_to_file), nil
	case "write":
		return sl.NewBuiltin("write", s.sl_write), nil
	case "write_from_file":
		return sl.NewBuiltin("write_from_file", s.sl_write_from_file), nil
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

// Read a segment
func (s *Stream) sl_read(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	var v any
	err := s.read(&v)
	if err != nil {
		return sl_error(fn, err)
	}
	return sl_encode(v), nil
}

// Read the rest of the stream as raw bytes, and write to a file
func (s *Stream) sl_read_to_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	debug("Stream %d reading rest of stream to file", s.id)
	defer s.reader.Close()

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

	if !file_write_from_reader(api_file(user, app, file), s.reader) {
		return sl_error(fn, "unable to save file %q", file)
	}

	debug("Stream %d read to file", s.id)
	return sl.None, nil
}

// Write one or more segments
func (s *Stream) sl_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	for _, a := range args {
		err := s.write(sl_decode(a))
		if err != nil {
			return sl_error(fn, err)
		}
	}
	return sl.None, nil
}

// Send a file as raw bytes
func (s *Stream) sl_write_from_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	debug("Stream %d writing from file", s.id)
	defer s.writer.Close()
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

	if s.write_file(api_file(user, app, file)) != nil {
		return sl_error(fn, "unable to send file")
	}

	debug("Stream %d wrote from file", s.id)
	return sl.None, nil
}
