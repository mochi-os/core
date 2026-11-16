// Mochi server: a requester that goes away mid-transfer is not an operator
// problem, and one that stalls does not outlive the call.
//
// io.Copy returns one error whether the source or the destination gave out, so
// only the destination wrapper can tell a vanished peer from an unreadable
// disk, and only then can the dispatcher stay quiet about the first. A far end
// that stops sending ends a read at the stream's read timeout. One that stops
// reading holds a write until its call's limit, which resets the stream so the
// call ends rather than being abandoned, and a transfer that fails resets it
// too, so the far end reads an error rather than taking part of a body for the
// whole of it.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	sl "go.starlark.net/starlark"
)

// dead_writer is a remote that has gone away: every write fails.
type dead_writer struct{}

func (dead_writer) Write(p []byte) (int, error) { return 0, errors.New("stream closed") }
func (dead_writer) Close() error                { return nil }

// live_writer is a remote that is still there, so a failure under it can only
// have come from the source.
type live_writer struct{ bytes.Buffer }

func (*live_writer) Close() error { return nil }

// unreadable is a local source that fails, standing in for a disk error - the
// case that must still warn.
type unreadable struct{}

func (unreadable) Read(p []byte) (int, error) { return 0, errors.New("input/output error") }

// TestSendMarksTheStreamWhenTheRemoteFails is the signal the dispatcher reads.
func TestSendMarksTheStreamWhenTheRemoteFails(t *testing.T) {
	s := &Stream{writer: dead_writer{}}

	if _, err := s.send(strings.NewReader("some photo bytes"), 0); err == nil {
		t.Fatal("send reported success against a writer that fails every write")
	}
	if !s.abandoned {
		t.Error("the remote gave out and the stream is not marked abandoned, so the dispatcher will warn and mail the operator about a browser that closed a tab")
	}
}

// TestSendLeavesTheStreamUnmarkedWhenTheSourceFails is the other half, and the
// reason the destination is wrapped at all. Marking here would silence a real
// disk error - the opposite mistake, and a worse one.
func TestSendLeavesTheStreamUnmarkedWhenTheSourceFails(t *testing.T) {
	s := &Stream{writer: &live_writer{}}

	if _, err := s.send(unreadable{}, 0); err == nil {
		t.Fatal("send reported success against a source that fails every read")
	}
	if s.abandoned {
		t.Error("a local read failure marked the stream abandoned; a disk that cannot be read would then be logged as a client disconnect and never reach the operator")
	}
}

// TestSendSucceedsWithoutMarking keeps the marking from being unconditional.
func TestSendSucceedsWithoutMarking(t *testing.T) {
	destination := &live_writer{}
	s := &Stream{writer: destination}

	n, err := s.send(strings.NewReader("photo"), 0)
	if err != nil {
		t.Fatalf("send failed on a working stream: %v", err)
	}
	if n != 5 || destination.String() != "photo" {
		t.Errorf("send delivered %d bytes %q, want 5 bytes \"photo\"", n, destination.String())
	}
	if s.abandoned {
		t.Error("a transfer that completed marked the stream abandoned")
	}
}

// stream_test_piece is the size of the pieces the slow ends below move: the
// size send's io.Copy reads a file in, and stream_piece.
const stream_test_piece = 32 * 1024

// drain_slowly reads far a piece at a time, 300ms apart, until it ends, and
// reports how much arrived: a remote that is slow but never stops.
func drain_slowly(far net.Conn) <-chan int {
	received := make(chan int, 1)
	go func() {
		total := 0
		buffer := make([]byte, stream_test_piece)
		for {
			time.Sleep(300 * time.Millisecond)
			n, err := far.Read(buffer)
			total += n
			if err != nil {
				break
			}
		}
		received <- total
	}()
	return received
}

// TestWriteRawKeepsGoingWhileTheRemoteReads: write.raw sends a whole upload in
// one call - market's comptroller_upload sends a digital asset that way - and
// one write deadline over all of it cut off an upload that was still moving.
// Six pieces read 300ms apart take about 1.8s against a 1s write timeout.
func TestWriteRawKeepsGoingWhileTheRemoteReads(t *testing.T) {
	const size = stream_test_piece * 6
	near, far := net.Pipe()
	t.Cleanup(func() { near.Close(); far.Close() })
	received := drain_slowly(far)

	s := &Stream{writer: near}
	s.timeout.write = 1
	if err := s.write_raw(make([]byte, size)); err != nil {
		t.Fatalf("write.raw to a slow remote that kept reading failed: %v", err)
	}
	near.Close()
	if total := <-received; total != size {
		t.Errorf("the remote received %d bytes, want %d", total, size)
	}
}

// TestWriteRawGivesUpOnARemoteThatStopsReading: an upload's receiver may answer
// before reading it - comptroller's asset upload refuses a draft listing or a bad
// filename first - and over yamux its close drops the rest unread without
// telling the sender, so market's write.raw waited out the seller's whole call.
// Each piece now waits the stream's write timeout.
func TestWriteRawGivesUpOnARemoteThatStopsReading(t *testing.T) {
	for name, open := range transfer_writers(t) {
		t.Run(name, func(t *testing.T) {
			s := &Stream{writer: open().writer}
			s.timeout.write = 1
			result := make(chan error, 1)
			go func() { result <- s.write_raw(make([]byte, 256*1024)) }()
			select {
			case err := <-result:
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Errorf("write.raw to a remote that never reads ended with %v, want the write deadline", err)
				}
				if !s.abandoned {
					t.Error("a remote that stopped reading did not mark the stream abandoned")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("write.raw is still blocked 10s after a 1s write timeout against a remote that never reads")
			}
		})
	}
}

// resettable stands in for a libp2p stream: a pipe with Reset, which records
// that it was called and, as libp2p's does, unblocks a write in progress.
type resettable struct {
	net.Conn
	reset atomic.Bool
}

func (r *resettable) Reset() error {
	r.reset.Store(true)
	return r.Conn.Close()
}

// stream_ends is a stream's writer, its far end, and for a peer whether it was
// reset; the self-loop's far end reads the reset as an error instead.
type stream_ends struct {
	writer io.WriteCloser
	far    io.Reader
	reset  func() bool
}

// transfer_writers are the writers a stream really has: a peer's libp2p stream,
// the same metered, and the self-loop's pipe.
func transfer_writers(t *testing.T) map[string]func() stream_ends {
	peer := func() (*resettable, net.Conn) {
		near, far := net.Pipe()
		t.Cleanup(func() { near.Close(); far.Close() })
		return &resettable{Conn: near}, far
	}
	return map[string]func() stream_ends{
		"peer": func() stream_ends {
			r, far := peer()
			return stream_ends{writer: r, far: far, reset: r.reset.Load}
		},
		"metered peer": func() stream_ends {
			r, far := peer()
			return stream_ends{writer: &stream_writer{inner: r, app: "readtest"}, far: far, reset: r.reset.Load}
		},
		"self-loop": func() stream_ends {
			r, w := io.Pipe()
			t.Cleanup(func() { r.Close(); w.Close() })
			return stream_ends{writer: &pipe_writer{PipeWriter: w}, far: r}
		},
	}
}

// TestTransferEndsWithItsCall is the photos/get stall: a remote that stopped
// reading held e.write.file until the call's limit, the cancel could not reach
// the blocked write, and the call was abandoned still running and mailed. The
// call's end now resets the stream under the write, so the call ends, as the
// requester's abandonment, and the far end sees a reset.
func TestTransferEndsWithItsCall(t *testing.T) {
	builtins := map[string]func(*sl.Thread, *Stream) error{
		"write.file": func(thread *sl.Thread, s *Stream) error {
			_, err := s.sl_write_file(thread, sl.NewBuiltin("write.file", nil), sl.Tuple{sl.String("photo.bin")}, nil)
			return err
		},
		"write.raw": func(thread *sl.Thread, s *Stream) error {
			_, err := s.sl_write_raw(thread, sl.NewBuiltin("write.raw", nil), sl.Tuple{sl.Bytes(make([]byte, 64*1024))}, nil)
			return err
		},
	}
	for builtin, write := range builtins {
		for name, open := range transfer_writers(t) {
			t.Run(builtin+"/"+name, func(t *testing.T) {
				thread, cancel := read_file_thread(t)
				base := api_file_base(&User{UID: "reader"}, &App{id: "readtest"})
				if err := os.MkdirAll(base, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(base, "photo.bin"), make([]byte, 64*1024), 0o600); err != nil {
					t.Fatal(err)
				}
				ends := open()
				s := &Stream{writer: ends.writer}

				result := make(chan error, 1)
				go func() { result <- write(thread, s) }()
				time.Sleep(300 * time.Millisecond)
				cancel()
				select {
				case err := <-result:
					if err == nil {
						t.Error("a transfer the call gave up on reported success")
					}
				case <-time.After(3 * time.Second):
					t.Fatal("the transfer is still blocked 3s after its call ended; it is abandoned still running and mailed to the operator")
				}
				if !s.abandoned {
					t.Error("a transfer to a remote that never read is not marked abandoned, so the dispatcher warns and mails")
				}
				if !starlark_serving_get(thread) {
					t.Error("the transfer did not take the transfer bound")
				}
				if ends.reset != nil && !ends.reset() {
					t.Error("the peer's stream was not reset")
				}
				if ends.reset == nil {
					if _, err := ends.far.Read(make([]byte, 1)); !errors.Is(err, error_stream_reset) {
						t.Errorf("the far end read %v, want the reset", err)
					}
				}
			})
		}
	}
}

// TestFailedTransferResetsTheStream: the write builtins defer a closing write,
// which ends a body cleanly, so a transfer that failed part way - here the
// source - left the far end holding part of a photo or asset as the whole of it,
// served with a 200 and cached. A failed transfer now resets the stream.
func TestFailedTransferResetsTheStream(t *testing.T) {
	for name, open := range transfer_writers(t) {
		t.Run(name, func(t *testing.T) {
			ends := open()
			s := &Stream{writer: ends.writer}
			read := make(chan error, 1)
			go func() {
				_, err := io.ReadAll(ends.far)
				read <- err
			}()

			if _, err := s.send(io.MultiReader(strings.NewReader("part of a photo"), unreadable{}), 0); err == nil {
				t.Fatal("send reported success from a source that failed part way")
			}
			s.close_write()
			if ends.reset != nil && !ends.reset() {
				t.Error("the peer's stream was not reset, so the closing write ends the body cleanly")
			}
			if err := <-read; ends.reset == nil && err == nil {
				t.Error("the far end read a clean end of a body the transfer never finished")
			}
		})
	}
}

// TestBulkWritersEndWithTheirCall pins the binding on every builtin that moves
// a body to a peer, which the tests above drive for two of them.
func TestBulkWritersEndWithTheirCall(t *testing.T) {
	for _, writer := range []struct{ file, signature string }{
		{"streams.go", "func (s *Stream) sl_write_file("},
		{"streams.go", "func (s *Stream) sl_write_cache("},
		{"streams.go", "func (s *Stream) sl_write_asset("},
		{"streams.go", "func (s *Stream) sl_write_raw("},
		{"git.go", "func api_git_archive("},
	} {
		if !strings.Contains(function_body(t, writer.file, writer.signature), "stream_bound(") {
			t.Errorf("%s does not bind its transfer to its call (stream_bound), so a peer that stops reading holds it past its limit", writer.signature)
		}
	}
	// The archive writes to the stream itself rather than through send, so it
	// resets on failure itself.
	if !strings.Contains(function_body(t, "git.go", "func api_git_archive("), "stream.reset()") {
		t.Error("api_git_archive does not reset its stream when the archive fails, so the far end takes part of an archive for the whole of it")
	}
}

// TestSelfLoopTimedOutWriteKeepsItsBytes: a write to the self-loop that runs out
// its deadline goes on in the background, while the caller owns its buffer again
// the moment the write returns - a segment's encoder hands it back to a pool for
// the next encoding. The background write must deliver the bytes it was given,
// not whatever the buffer holds by the time a reader arrives.
func TestSelfLoopTimedOutWriteKeepsItsBytes(t *testing.T) {
	r, w := io.Pipe()
	t.Cleanup(func() { r.Close(); w.Close() })
	writer := &pipe_writer{PipeWriter: w}
	writer.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))

	buffer := []byte("the segment")
	if _, err := writer.Write(buffer); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a write nobody read ended with %v, want the deadline", err)
	}
	copy(buffer, "overwritten")
	received := make([]byte, len(buffer))
	if _, err := io.ReadFull(r, received); err != nil {
		t.Fatalf("reading what the timed-out write went on to deliver: %v", err)
	}
	if string(received) != "the segment" {
		t.Errorf("the reader received %q, want \"the segment\": the background write sent the caller's reused buffer", received)
	}
}

// TestRawReadsGiveUpOnASenderThatGoesQuiet is the read side of the photos/get
// stall: a far end that stops sending part way but keeps the stream open held
// s.read.file, mochi.cache.write and the bridge's avatar fetch for as long as
// it liked, because raw_reader handed out the bare stream. Every raw read now
// waits the stream's read timeout, whether the body follows a segment or not,
// on a peer's stream and on the self-loop's pipe alike.
func TestRawReadsGiveUpOnASenderThatGoesQuiet(t *testing.T) {
	streams := map[string]func(t *testing.T) *Stream{
		"peer": func(t *testing.T) *Stream {
			near, far := net.Pipe()
			t.Cleanup(func() { near.Close(); far.Close() })
			return &Stream{reader: near}
		},
		"self-loop": func(t *testing.T) *Stream {
			r, w := io.Pipe()
			t.Cleanup(func() { r.Close(); w.Close() })
			return &Stream{reader: &pipe_reader{PipeReader: r}}
		},
		"after a segment": func(t *testing.T) *Stream {
			near, far := net.Pipe()
			t.Cleanup(func() { near.Close(); far.Close() })
			segment, err := cbor.Marshal(map[string]any{"status": "200"})
			if err != nil {
				t.Fatalf("encoding the segment: %v", err)
			}
			go far.Write(segment)
			s := &Stream{reader: near}
			s.timeout.read = 1
			var header map[string]any
			if err := s.read(&header); err != nil {
				t.Fatalf("reading the segment before the body: %v", err)
			}
			return s
		},
	}

	for name, open := range streams {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			s.timeout.read = 1
			result := make(chan error, 1)
			go func() {
				_, err := io.ReadAll(s.raw_reader())
				result <- err
			}()
			select {
			case err := <-result:
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Errorf("a raw read from a sender that went quiet ended with %v, want the read deadline", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a raw read is still blocked 10s after a 1s read timeout against a sender that went quiet")
			}
		})
	}
}

// TestRawReadsKeepGoingWhileTheSenderSends: the read bound is per read, so a
// slow sender that keeps sending is never cut off.
func TestRawReadsKeepGoingWhileTheSenderSends(t *testing.T) {
	const size = stream_test_piece * 6
	near, far := net.Pipe()
	t.Cleanup(func() { near.Close(); far.Close() })
	go func() {
		for i := 0; i < size/stream_test_piece; i++ {
			time.Sleep(300 * time.Millisecond)
			if _, err := far.Write(make([]byte, stream_test_piece)); err != nil {
				return
			}
		}
		far.Close()
	}()

	s := &Stream{reader: near}
	s.timeout.read = 1
	data, err := io.ReadAll(s.raw_reader())
	if err != nil {
		t.Fatalf("a raw read from a slow sender that kept sending failed after %d bytes: %v; the read timeout must bound each read, not the body", len(data), err)
	}
	if len(data) != size {
		t.Errorf("read %d bytes, want %d", len(data), size)
	}
}

// read_file_thread is a call that may run s.read.file: an administrator, so
// the storage figure needs no database, in a scratch data directory, with the
// context the call's time limit cancels and the flag the transfer bound sets.
func read_file_thread(t *testing.T) (*sl.Thread, context.CancelFunc) {
	t.Helper()
	original := data_dir
	data_dir = t.TempDir()
	t.Cleanup(func() { data_dir = original })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	thread := &sl.Thread{Name: "test"}
	thread.SetLocal("user", &User{UID: "reader", Role: "administrator"})
	thread.SetLocal("app", &App{id: "readtest"})
	thread.SetLocal("context", ctx)
	thread.SetLocal("file_serving", &atomic.Bool{})
	return thread, cancel
}

// stream_read_file runs s.read.file("photo.bin") in the background.
func stream_read_file(thread *sl.Thread, s *Stream) <-chan error {
	result := make(chan error, 1)
	go func() {
		_, err := s.sl_read_file(thread, sl.NewBuiltin("read.file", nil), sl.Tuple{sl.String("photo.bin")}, nil)
		result <- err
	}()
	return result
}

// TestReadFileGivesUpOnASenderThatGoesQuiet drives the builtin that would have
// mailed: attachment_receive and comptroller's uploads end in s.read.file, and
// a sender that went quiet held the call to its compute bound, where it was
// abandoned still blocked in the read.
func TestReadFileGivesUpOnASenderThatGoesQuiet(t *testing.T) {
	thread, _ := read_file_thread(t)
	near, far := net.Pipe()
	t.Cleanup(func() { near.Close(); far.Close() })
	go far.Write([]byte("the first part of a photo"))
	s := &Stream{reader: near}
	s.timeout.read = 1

	select {
	case err := <-stream_read_file(thread, s):
		if err == nil {
			t.Error("s.read.file reported success for a body its sender never finished")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("s.read.file is still blocked 10s after a 1s read timeout against a sender that went quiet")
	}
}

// TestReadFileTakesTheTransferBound: an upload that is still arriving is moving
// bytes, not computing, so it gets the file bound as write.file does, rather
// than being cut at the compute bound.
func TestReadFileTakesTheTransferBound(t *testing.T) {
	thread, _ := read_file_thread(t)
	near, far := net.Pipe()
	t.Cleanup(func() { near.Close(); far.Close() })
	go func() {
		far.Write([]byte("photo"))
		far.Close()
	}()

	if err := <-stream_read_file(thread, &Stream{reader: near}); err != nil {
		t.Fatalf("s.read.file of a whole body failed: %v", err)
	}
	if !starlark_serving_get(thread) {
		t.Error("s.read.file did not take the transfer bound, so an upload longer than the compute bound is cut off while still arriving")
	}
	stored, err := os.ReadFile(filepath.Join(api_file_base(&User{UID: "reader"}, &App{id: "readtest"}), "photo.bin"))
	if err != nil || string(stored) != "photo" {
		t.Errorf("stored %q (%v), want \"photo\"", stored, err)
	}
}

// TestReadFileEndsWithItsCall: a sender trickling a byte at a time renews every
// read's deadline, so only the call's own limit can stop it, and the read
// ignores the thread's cancellation. The stream is closed under it instead.
func TestReadFileEndsWithItsCall(t *testing.T) {
	thread, cancel := read_file_thread(t)
	near, far := net.Pipe()
	t.Cleanup(func() { near.Close(); far.Close() })
	go func() {
		for {
			if _, err := far.Write([]byte("x")); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	s := &Stream{reader: near}
	s.timeout.read = 1

	result := stream_read_file(thread, s)
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("s.read.file is still reading 3s after its call was cancelled; a trickling sender holds it past its time limit")
	}
}

// TestCacheWriteGivesUpOnASenderThatGoesQuiet: mochi.cache.write from a stream
// is the remote attachment pull. It already ended with its call; the read bound
// now ends it at the stream's read timeout, not at the call's file bound.
func TestCacheWriteGivesUpOnASenderThatGoesQuiet(t *testing.T) {
	thread := cache_read_thread(t)
	near, far := net.Pipe()
	t.Cleanup(func() { near.Close(); far.Close() })
	go far.Write([]byte("the first part of an attachment"))
	s := &Stream{reader: near}
	s.timeout.read = 1

	result := make(chan sl.Value, 1)
	go func() {
		value, _ := api_cache_write(thread, sl.NewBuiltin("mochi.cache.write", api_cache_write), sl.Tuple{sl.String("pulled/entry"), s}, nil)
		result <- value
	}()
	select {
	case value := <-result:
		if value != sl.None {
			t.Errorf("mochi.cache.write of a body its sender never finished = %v, want None", value)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("mochi.cache.write is still blocked 10s after a 1s read timeout against a sender that went quiet")
	}
}

// TestEveryStreamWriterMarksAnAbandonedStream covers the paths an app reaches
// that do NOT go through send: the CBOR segment writer and the raw writer both
// write to s.writer and nothing else, so any failure there is the remote's.
// write_file is send's caller, behind e.write.asset.
func TestEveryStreamWriterMarksAnAbandonedStream(t *testing.T) {
	source := filepath.Join(t.TempDir(), "photo.bin")
	if err := os.WriteFile(source, []byte("bytes"), 0o600); err != nil {
		t.Fatalf("writing the test source file: %v", err)
	}

	writers := map[string]func(*Stream) error{
		"write":      func(s *Stream) error { return s.write(map[string]string{"status": "200"}) },
		"write_raw":  func(s *Stream) error { return s.write_raw([]byte("bytes")) },
		"write_file": func(s *Stream) error { _, err := s.write_file(source); return err },
	}

	for name, send := range writers {
		s := &Stream{writer: dead_writer{}}
		if err := send(s); err == nil {
			t.Errorf("%s reported success against a dead remote", name)
			continue
		}
		if !s.abandoned {
			t.Errorf("%s failed against a dead remote without marking the stream abandoned, so a handler that ends there still warns", name)
		}
	}
}

// TestAbandonedHandlerIsLoggedNotWarned drives the real dispatcher. The stream
// is marked, so the failure must be reported as an abandonment.
func TestAbandonedHandlerIsLoggedNotWarned(t *testing.T) {
	a, av := starlark_event_app(t)
	captured := capture_log(t)

	e := &Event{event: "photos/get", stream: &Stream{abandoned: true}}
	if err := e.run_handler(a, av, AppEvent{Function: "handler_boom"}); err == nil {
		t.Fatal("run_handler returned nil for a handler that raised; the error must still propagate unchanged")
	}

	out := captured.String()
	if !strings.Contains(out, "abandoned by the requester") {
		t.Errorf("a handler that failed on an abandoned stream logged %q; want the abandonment wording, not an operator warning", out)
	}
	if strings.Contains(out, `for "photos/get" failed:`) {
		t.Errorf("a handler that failed on an abandoned stream still logged the warn line: %q", out)
	}
}

// TestUnabandonedHandlerStillWarns is the guard against over-silencing: a
// handler that failed for its own reasons must still reach the operator.
func TestUnabandonedHandlerStillWarns(t *testing.T) {
	a, av := starlark_event_app(t)
	captured := capture_log(t)

	e := &Event{event: "photos/get", stream: &Stream{}}
	if err := e.run_handler(a, av, AppEvent{Function: "handler_boom"}); err == nil {
		t.Fatal("run_handler returned nil for a handler that raised")
	}

	out := captured.String()
	if !strings.Contains(out, `for "photos/get" failed:`) {
		t.Errorf("a genuine handler failure logged %q; the operator warning is gone", out)
	}
	if strings.Contains(out, "abandoned by the requester") {
		t.Errorf("a genuine handler failure was reported as an abandonment: %q", out)
	}
}

// TestNoStreamStillWarns: most events carry no stream at all (pubsub, queued
// delivery). A nil stream must not read as abandoned.
func TestNoStreamStillWarns(t *testing.T) {
	a, av := starlark_event_app(t)
	captured := capture_log(t)

	e := &Event{event: "posts/new"}
	if err := e.run_handler(a, av, AppEvent{Function: "handler_boom"}); err == nil {
		t.Fatal("run_handler returned nil for a handler that raised")
	}
	if !strings.Contains(captured.String(), `for "posts/new" failed:`) {
		t.Errorf("a streamless event's handler failure logged %q; want the operator warning", captured.String())
	}
}

// TestAbandonedBranchUsesInfoNotWarn pins the LEVEL, which the wording tests
// above cannot see: warn() and info() write the same shape of line, so
// renaming the message while leaving warn() in place would satisfy them and
// still mail the operator. warn() is the only one of the two that emails.
func TestAbandonedBranchUsesInfoNotWarn(t *testing.T) {
	body := function_body(t, "events.go", "func (e *Event) run_handler(")

	abandoned := strings.Index(body, "abandoned by the requester")
	if abandoned < 0 {
		t.Fatal("run_handler no longer reports an abandoned requester at all")
	}
	line_start := strings.LastIndex(body[:abandoned], "\n") + 1
	line := body[line_start:abandoned]
	if !strings.Contains(line, "info(") {
		t.Errorf("the abandoned-requester line is emitted by %q, not info(); warn() mails the operator, which is the whole defect", strings.TrimSpace(line))
	}
	if !strings.Contains(body, "warn(") {
		t.Error("run_handler no longer warns about anything, so a genuine handler failure is now silent to the operator")
	}
}

// TestAppFileWritersRouteThroughSend: the app-facing writers must use send, or
// their failures are invisible to the dispatcher again. sl_write_file is the
// path the yuzu email came down.
func TestAppFileWritersRouteThroughSend(t *testing.T) {
	for _, name := range []string{
		"func (s *Stream) sl_write_file(",
		"func (s *Stream) sl_write_cache(",
	} {
		body := function_body(t, "streams.go", name)
		if strings.Contains(body, "io.Copy(s.writer") {
			t.Errorf("%s copies straight to s.writer, so a remote that went away is indistinguishable from an unreadable file", name)
		}
		if !strings.Contains(body, "s.send(") {
			t.Errorf("%s does not route through s.send, so its failures never mark the stream abandoned", name)
		}
	}
}

// capture_log redirects the standard logger into a buffer for the duration of
// one test, and clears the repeat suppressor so an earlier test's lines cannot
// swallow this one's.
func capture_log(t *testing.T) *bytes.Buffer {
	t.Helper()
	log_repeat_mutex.Lock()
	log_repeat_state = map[string]*log_repeat_record{}
	log_repeat_mutex.Unlock()

	buffer := &bytes.Buffer{}
	previous_writer := log.Writer()
	previous_flags := log.Flags()
	log.SetOutput(buffer)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(previous_writer)
		log.SetFlags(previous_flags)
	})
	return buffer
}

// function_body returns one function's source with line comments stripped, so
// a scan cannot match the prose explaining the code it is checking. Two of
// these assertions matched their own comments when first written.
func function_body(t *testing.T, file, signature string) string {
	t.Helper()
	source, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("reading %s: %v", file, err)
	}
	start := strings.Index(string(source), signature)
	if start < 0 {
		t.Fatalf("%s no longer contains %q", file, signature)
	}
	rest := string(source)[start:]
	if end := strings.Index(rest, "\n}\n"); end > 0 {
		rest = rest[:end]
	}
	return regexp.MustCompile(`(?m)//.*$`).ReplaceAllString(rest, "")
}

var _ io.Reader = unreadable{}
