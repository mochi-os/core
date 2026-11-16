// Mochi server: a requester that gives up on a body tells the sender.
//
// Over yamux a stream's close is a local CloseRead - the remote is not told -
// and data arriving after it is dropped unread, so a responder writing a body
// the requester abandoned got neither credit nor a reset and stayed blocked
// until its call's limit. A requester's stream now resets when it gives up on a
// body; a responder's never does, since a reset discards the answer it holds.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	p2p_network "github.com/libp2p/go-libp2p/core/network"
	p2p_yamux "github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	sl "go.starlark.net/starlark"
)

// yamux_pair opens one stream over libp2p's yamux on a loopback TCP
// connection, returning the opener's end and the accepter's.
func yamux_pair(t *testing.T) (p2p_network.MuxedStream, p2p_network.MuxedStream) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	near, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	far := <-accepted
	listener.Close()
	t.Cleanup(func() { near.Close(); far.Close() })

	client, err := p2p_yamux.DefaultTransport.NewConn(near, false, nil)
	if err != nil {
		t.Fatalf("starting the client session: %v", err)
	}
	server, err := p2p_yamux.DefaultTransport.NewConn(far, true, nil)
	if err != nil {
		t.Fatalf("starting the server session: %v", err)
	}
	t.Cleanup(func() { client.Close(); server.Close() })

	opened, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("opening a stream: %v", err)
	}
	answering, err := server.AcceptStream()
	if err != nil {
		t.Fatalf("accepting the stream: %v", err)
	}
	return opened, answering
}

// stream_body_size is far past yamux's first receive window (256 KB), so a
// responder writing it waits on the requester's credit.
const stream_body_size = 4 << 20

// answer_body answers the requester as a responder does - a header segment,
// then a body - in the background, and reports how the body's write ended.
func answer_body(responder io.ReadWriteCloser) <-chan error {
	result := make(chan error, 1)
	go func() {
		if err := (&Stream{writer: responder}).write(map[string]any{"status": "200"}); err != nil {
			result <- err
			return
		}
		_, err := responder.Write(make([]byte, stream_body_size))
		result <- err
	}()
	return result
}

// read_header reads the header segment of the answer, as every requester does
// before it decides about the body.
func read_header(t *testing.T, s *Stream) {
	t.Helper()
	var header map[string]any
	if err := s.read(&header); err != nil {
		t.Fatalf("reading the answer's header: %v", err)
	}
}

// TestRequesterAbandoningABodyStopsTheResponder covers each way a requester
// gives up on a body: part read and the stream closed, as a relay does when the
// browser leaves or the cap is reached; the header read and the stream closed
// at the call's end, as an asset revalidation that matched its cache does; the
// read side closed unread, as s.read.file and mochi.cache.write do when they
// refuse; and a relay answered 304.
func TestRequesterAbandoningABodyStopsTheResponder(t *testing.T) {
	cases := map[string]func(t *testing.T, s *Stream){
		"part read, then closed": func(t *testing.T, s *Stream) {
			read_header(t, s)
			if _, err := io.ReadFull(s.raw_reader(), make([]byte, 4096)); err != nil {
				t.Fatalf("reading the start of the body: %v", err)
			}
			s.close()
		},
		"header read, then closed": func(t *testing.T, s *Stream) {
			read_header(t, s)
			s.close()
		},
		"header read, then read side closed": func(t *testing.T, s *Stream) {
			read_header(t, s)
			s.close_read()
		},
		"relay answered 304": func(t *testing.T, s *Stream) {
			read_header(t, s)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("GET", "/photo", nil)
			c.Request.Header.Set("If-None-Match", `"photo-1"`)
			a := &Action{web: c}
			a.web.Header("ETag", `"photo-1"`)
			if _, err := a.sl_write_stream(&sl.Thread{Name: "test"}, sl.NewBuiltin("write.stream", nil), sl.Tuple{s}, nil); err != nil {
				t.Fatalf("a.write.stream: %v", err)
			}
		},
	}
	for name, abandon := range cases {
		t.Run(name, func(t *testing.T) {
			opened, answering := yamux_pair(t)
			written := answer_body(answering)
			abandon(t, &Stream{reader: opened, writer: opened, requester: true})
			select {
			case err := <-written:
				if err == nil {
					t.Error("the responder's write of a body the requester abandoned succeeded")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the responder is still blocked writing 3s after the requester gave up on the body; over yamux it holds its call until the call's limit")
			}
		})
	}
}

// TestRequesterAbandoningAHeaderlessBodyStopsTheResponder: an answer that is
// all body, with no header segment before it, has begun once the body is
// opened.
func TestRequesterAbandoningAHeaderlessBodyStopsTheResponder(t *testing.T) {
	opened, answering := yamux_pair(t)
	written := make(chan error, 1)
	go func() {
		_, err := answering.Write(make([]byte, stream_body_size))
		written <- err
	}()
	s := &Stream{reader: opened, writer: opened, requester: true}
	if _, err := io.ReadFull(s.raw_reader(), make([]byte, 4096)); err != nil {
		t.Fatalf("reading the start of the body: %v", err)
	}
	s.close()
	select {
	case err := <-written:
		if err == nil {
			t.Error("the responder's write of a body the requester abandoned succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the responder is still blocked writing 3s after the requester gave up on a body with no header")
	}
}

// TestRequesterThatReadTheBodyClosesQuietly: a body read to its end is not
// abandoned, so the requester closes rather than resets, and the responder
// reads the end of the request rather than a reset - also when, as s.read.file
// does, the read side is closed first and the stream at the call's end, by
// which time a read of the closed side no longer finds the end.
func TestRequesterThatReadTheBodyClosesQuietly(t *testing.T) {
	for name, close := range map[string]func(*Stream){
		"closed":                        func(s *Stream) { s.close() },
		"read side closed, then closed": func(s *Stream) { s.close_read(); s.close() },
	} {
		t.Run(name, func(t *testing.T) {
			opened, answering := yamux_pair(t)
			go func() {
				answering.Write(make([]byte, 64<<10))
				answering.CloseWrite()
			}()
			s := &Stream{reader: opened, writer: opened, requester: true}
			if _, err := io.ReadAll(s.raw_reader()); err != nil {
				t.Fatalf("reading the whole body: %v", err)
			}
			close(s)
			if _, err := io.ReadAll(answering); err != nil {
				t.Errorf("the responder read %v after a requester that took the whole body closed, want the end of the request", err)
			}
		})
	}
}

// TestRequesterThatReadTheBodyClosesQuietlyOverTheSelfLoop: on the self-loop,
// as over QUIC, a read of a side already closed fails rather than finding the
// end, so the decision s.read.file's close_read made must stand at the call's
// closing of the stream, or a body read whole is reset after all.
func TestRequesterThatReadTheBodyClosesQuietlyOverTheSelfLoop(t *testing.T) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	t.Cleanup(func() { r1.Close(); w1.Close(); r2.Close(); w2.Close() })
	go func() {
		w2.Write(make([]byte, 64<<10))
		w2.Close()
	}()
	s := &Stream{reader: &pipe_reader{PipeReader: r2}, writer: &pipe_writer{PipeWriter: w1}, requester: true}
	if _, err := io.ReadAll(s.raw_reader()); err != nil {
		t.Fatalf("reading the whole body: %v", err)
	}
	s.close_read()
	s.close()
	if _, err := io.ReadAll(r1); err != nil {
		t.Errorf("the responder read %v after a requester that took the whole body closed, want the end of the request", err)
	}
}

// TestRequesterThatReadTheAnswerClosesQuietly: a requester that read its whole
// answer but not the end of it - one segment, then the stream closed at the
// call's end - has abandoned nothing, so it closes rather than resets.
func TestRequesterThatReadTheAnswerClosesQuietly(t *testing.T) {
	opened, answering := yamux_pair(t)
	if err := (&Stream{writer: answering}).write(map[string]any{"status": "200"}); err != nil {
		t.Fatalf("writing the answer: %v", err)
	}
	answering.CloseWrite()
	s := &Stream{reader: opened, writer: opened, requester: true}
	read_header(t, s)
	s.close()
	if _, err := io.ReadAll(answering); err != nil {
		t.Errorf("the responder read %v after a requester that had its whole answer closed, want the end of the request", err)
	}
}

// TestRequesterThatHasNoAnswerKeepsItsRequest: before any answer the responder
// may still hold the request or an upload unread, which a reset would discard -
// a requester that writes and closes without reading, and one whose wait for an
// answer timed out after an upload, close rather than reset.
func TestRequesterThatHasNoAnswerKeepsItsRequest(t *testing.T) {
	for name, close := range map[string]func(*Stream){
		"closed":           func(s *Stream) { s.close() },
		"read side closed": func(s *Stream) { s.close_read(); s.close() },
	} {
		t.Run(name, func(t *testing.T) {
			opened, answering := yamux_pair(t)
			s := &Stream{reader: opened, writer: opened, requester: true}
			if err := s.write_raw([]byte("an upload")); err != nil {
				t.Fatalf("writing the upload: %v", err)
			}
			close(s)
			time.Sleep(100 * time.Millisecond)
			upload, err := io.ReadAll(answering)
			if err != nil || string(upload) != "an upload" {
				t.Errorf("the responder read %q (%v), want the whole upload", upload, err)
			}
		})
	}
}

// TestRequesterWritingAfterTheAnswerKeepsWhatItWrote: what a requester writes
// after reading an answer is the far end's to read, so its close does not reset
// it away - the answer it read no longer counts. Either kind of write.
func TestRequesterWritingAfterTheAnswerKeepsWhatItWrote(t *testing.T) {
	for name, write := range map[string]func(*Stream) error{
		"segment": func(s *Stream) error { return s.write(map[string]any{"next": "more"}) },
		"raw":     func(s *Stream) error { return s.write_raw([]byte("more")) },
	} {
		t.Run(name, func(t *testing.T) {
			opened, answering := yamux_pair(t)
			if err := (&Stream{writer: answering}).write(map[string]any{"status": "200"}); err != nil {
				t.Fatalf("writing the answer: %v", err)
			}
			s := &Stream{reader: opened, writer: opened, requester: true}
			read_header(t, s)
			if err := write(s); err != nil {
				t.Fatalf("writing after the answer: %v", err)
			}
			s.close()
			time.Sleep(100 * time.Millisecond)
			received, err := io.ReadAll(answering)
			if err != nil || !strings.Contains(string(received), "more") {
				t.Errorf("the responder read %q (%v), want what the requester wrote after the answer", received, err)
			}
		})
	}
}

// TestResponderNeverResets: a responder that stops reading an upload part way -
// s.read.file refusing an overrun - still owes the requester an answer, and a
// reset would discard it from the requester's buffer unread.
func TestResponderNeverResets(t *testing.T) {
	opened, answering := yamux_pair(t)
	go opened.Write(make([]byte, stream_body_size))

	s := &Stream{reader: answering, writer: answering}
	if _, err := io.ReadFull(s.raw_reader(), make([]byte, 4096)); err != nil {
		t.Fatalf("reading the start of the upload: %v", err)
	}
	s.close_read()
	if _, err := s.writer.Write([]byte("too large")); err != nil {
		t.Fatalf("writing the answer: %v", err)
	}
	s.close()

	result := make(chan string, 1)
	go func() {
		answer, err := io.ReadAll(opened)
		if err != nil {
			answer = append(answer, []byte(" ("+err.Error()+")")...)
		}
		result <- string(answer)
	}()
	select {
	case answer := <-result:
		if answer != "too large" {
			t.Errorf("the requester read %q, want the responder's answer", answer)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the requester had not read the answer 3s after the responder closed")
	}
}

// TestOpenedStreamsAreRequesters pins the two places a stream is opened toward
// a far end that answers - over the wire, and the self-loop's near end - and
// that the ends that answer are not marked.
func TestOpenedStreamsAreRequesters(t *testing.T) {
	if !strings.Contains(function_body(t, "protocol2_stream.go", "func stream_open("), "st.requester = true") {
		t.Error("stream_open does not mark the stream it opens as a requester's, so abandoning a body over the wire leaves the responder blocked")
	}
	loop := function_body(t, "protocol2_stream.go", "func stream_self_loop(")
	if !strings.Contains(loop, "near.requester = true") {
		t.Error("stream_self_loop does not mark its near end as a requester's")
	}
	if strings.Contains(loop, "far.requester") || strings.Contains(function_body(t, "protocol2_stream.go", "func receive_stream_guarded("), "requester") {
		t.Error("an answering end is marked as a requester's; its close would reset away the answer it has written")
	}
}
