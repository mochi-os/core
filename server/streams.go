// Mochi server: Streams
// Copyright Alistair Cunningham 2024-2025

package main

import (
	cbor "github.com/fxamacker/cbor/v2"
	"io"
	"os"
)

type Stream struct {
	reader  io.ReadCloser
	writer  io.WriteCloser
	decoder *cbor.Decoder
}

// Create a new stream with specified headers
func stream(from string, to string, service string, event string) *Stream {
	peer := entity_peer(to)
	debug("Stream opening from '%s', to '%s', service '%s', event '%s', peer '%s'", from, to, service, event, peer)

	s := peer_stream(peer)
	if s == nil {
		debug("Stream unable to open to peer '%s'", peer)
		return nil
	}

	h := Headers{From: from, To: to, Service: service, Event: event, Signature: entity_sign(from, from+to+service+event)}

	if !s.write_encode(h) {
		return nil
	}

	return s
}

// Create a new stream from an existing reader and writer
func stream_rw(r io.ReadCloser, w io.WriteCloser) *Stream {
	return &Stream{reader: r, writer: w}
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

	debug("Stream received from '%s', to '%s', service '%s', event '%s', content '%#v'", peer, h.From, h.To, h.Service, h.Event, content)

	// Create event, and route to app
	e := Event{from: h.From, to: h.To, service: h.Service, event: h.Event, peer: peer, content: content, stream: s}
	e.route()
}

// Close a stream
func (s *Stream) close() {
	if s.reader != nil {
		s.reader.Close()
	}

	if s.writer != nil {
		s.writer.Close()
	}
}

// Read a content segment from a stream
func (s *Stream) read_content() map[string]string {
	var content map[string]string
	s.read_decode(&content)
	return content
}

// Read a CBOR encoded segment from a stream
func (s *Stream) read_decode(v any) bool {
	if s.decoder == nil {
		s.decoder = cbor.NewDecoder(s.reader)
	}

	err := s.decoder.Decode(&v)
	if err != nil {
		info("Stream unable to decode segment: %v", err)
		return false
	}
	return true
}

// Write a segment to a stream
func (s *Stream) write(b []byte) bool {
	_, err := s.writer.Write(b)
	if err != nil {
		warn("Stream error sending segment: %v", err)
		return false
	}
	return true
}

// Write a content segment to a stream
func (s *Stream) write_content(in ...string) bool {
	content := map[string]string{}

	for {
		if len(in) < 2 {
			break
		}
		content[in[0]] = in[1]
		in = in[2:]
	}

	return s.write(cbor_encode(content))
}

// Write a CBOR encoded segment to a stream
func (s *Stream) write_encode(v any) bool {
	return s.write(cbor_encode(v))
}

// Write a file to a stream
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
