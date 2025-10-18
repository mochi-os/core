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
}

var (
	streams_lock       = &sync.Mutex{}
	stream_next  int64 = 1
)

//TODO Set timeouts

// Create a new stream with specified headers
func stream(from string, to string, service string, event string) *Stream {
	peer := entity_peer(to)

	s := peer_stream(peer)
	if s == nil {
		debug("Stream unable to open to peer '%s'", peer)
		return nil
	}
	debug("Stream %d open to peer '%s': from '%s', to '%s', service '%s', event '%s'", s.id, peer, from, to, service, event)

	if s.write(Headers{From: from, To: to, Service: service, Event: event, Signature: entity_sign(from, from+to+service+event)}) {
		return s
	}

	return nil
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
	if s.decoder == nil {
		s.decoder = cbor.NewDecoder(s.reader)
	}

	// Get and verify message headers
	var h Headers
	err := s.decoder.Decode(&h)
	if err != nil || !h.valid() {
		info("Stream closing due to bad headers")
		return
	}

	// Decode the content segment
	content := map[string]string{}
	err = s.decoder.Decode(&content)
	if err != nil {
		info("Stream closing due to bad content segment: %v", err)
		return
	}

	debug("Stream %d open from peer '%s': from '%s', to '%s', service '%s', event '%s', content '%#v'", s.id, peer, h.From, h.To, h.Service, h.Event, content)

	// Create event, and route to app
	e := Event{id: event_id(), from: h.From, to: h.To, service: h.Service, event: h.Event, peer: peer, content: content, stream: s}
	e.route()
}

// Read a content segment from a stream
func (s *Stream) read_content() map[string]string {
	debug("Stream %d reading content segment", s.id)
	//TODO Remove this delay once we figure out the partial CBOR problem
	time.Sleep(time.Millisecond)

	if s == nil {
		info("Stream %d not open", s.id)
		return nil
	}
	if s.decoder == nil {
		s.decoder = cbor.NewDecoder(s.reader)
	}

	var content map[string]string
	err := s.decoder.Decode(&content)
	if err != nil {
		info("Stream %d unable to read content segment: %v", s.id, err)
		return nil
	}
	debug("Stream %d read content segment: %#v", s.id, content)
	return content
}

// Read a CBOR encoded segment from a stream
func (s *Stream) read(v any) bool {
	debug("Stream %d reading segment type %T", s.id, v)
	//TODO Remove this delay once we figure out the partial CBOR problem
	time.Sleep(time.Millisecond)

	if s == nil {
		info("Stream %d not open", s.id)
		return false
	}
	if s.decoder == nil {
		s.decoder = cbor.NewDecoder(s.reader)
	}

	err := s.decoder.Decode(&v)
	if err != nil {
		info("Stream %d unable to read segment: %v", s.id, err)
		return false
	}
	debug("Stream %d read segment: %#v", s.id, v)
	return true
}

// Write a CBOR encoded segment to a stream
func (s *Stream) write(v any) bool {
	debug("Stream %d writing segment: %#v", s.id, v)
	if s == nil || s.writer == nil {
		info("Stream %d not open", s.id)
		return false
	}
	if s.encoder == nil {
		s.encoder = cbor.NewEncoder(s.writer)
	}

	err := s.encoder.Encode(v)
	if err != nil {
		warn("Stream %d error writing segment: %v", s.id, err)
		return false
	}

	debug("Stream %d wrote segment", s.id)
	return true
}

// Write field/value pairs to a stream as a CBOR encoded segment
func (s *Stream) write_content(in ...string) bool {
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
func (s *Stream) write_file(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		warn("Stream unable to read file '%s'", path)
		return false
	}
	defer f.Close()

	_, err = io.Copy(s.writer, f)
	if err != nil {
		debug("Stream error sending file segment: %v", err)
		return false
	}

	return true
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
	return sl.String(s.id).Hash()
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
	debug("Stream %d reading segment", s.id)
	//TODO Remove this delay once we figure out the partial CBOR problem
	time.Sleep(time.Millisecond)

	if s == nil {
		info("Stream %d not open", s.id)
		return sl_error(fn, "stream not open")
	}
	if s.decoder == nil {
		s.decoder = cbor.NewDecoder(s.reader)
	}

	var v any
	err := s.decoder.Decode(&v)
	if err != nil {
		info("Stream %d unable to decode segment: %v", s.id, err)
		return sl_error(fn, "unable to decode segment")
	}
	debug("Stream %d read segment: %#v", s.id, v)
	return sl_encode(v), nil
}

// Read the rest of the stream as raw bytes, and write to a file
func (s *Stream) sl_read_to_file(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	debug("Stream %d reading rest of stream to file", s.id)
	defer s.reader.Close()
	//TODO Remove this delay once we figure out the partial CBOR problem
	time.Sleep(time.Millisecond)

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
		return sl_error(fn, "invalid file '%s'", file)
	}

	if !file_write_from_reader(api_file(user, app, file), s.reader) {
		return sl_error(fn, "unable to save file '%s'", file)
	}

	debug("Stream %d read to file", s.id)
	return sl.None, nil
}

// Write one or more segments
func (s *Stream) sl_write(t *sl.Thread, fn *sl.Builtin, args sl.Tuple, kwargs []sl.Tuple) (sl.Value, error) {
	for _, a := range args {
		if !s.write(sl_decode(a)) {
			return sl_error(fn, "error writing stream")
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
		return sl_error(fn, "invalid file '%s'", file)
	}

	if !s.write_file(api_file(user, app, file)) {
		return sl_error(fn, "unable to send file")
	}

	debug("Stream %d wrote from file", s.id)
	return sl.None, nil
}
