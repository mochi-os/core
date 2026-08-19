// Mochi server: a requester that goes away mid-transfer is not an operator problem.
//
// A browser navigating off a market listing abandons every image still in
// flight. The abandonment propagated back down the relay - HTTP/2 stream
// closed, so the market's a.write.stream() copy failed, so the Comptroller's
// e.write.file() copy failed - and surfaced at the event dispatcher, which
// warn()ed, and warn() mails the operator. The identical abandonment of a
// locally served file logged quietly, so whether the admin got an email
// depended on the transport rather than on anything being wrong.
//
// The discrimination io.Copy cannot do is the substance here: it returns one
// error whether the source or the destination gave out, so a vanished peer and
// an unreadable disk were the same value. Only the destination wrapper tells
// them apart.
//
// Copyright © 2026 Mochisoft OU
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
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

	if _, err := s.send(strings.NewReader("some photo bytes")); err == nil {
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

	if _, err := s.send(unreadable{}); err == nil {
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

	n, err := s.send(strings.NewReader("photo"))
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
