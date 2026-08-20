// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for the failure path of a Starlark event handler.
//
// run_handler used to discard Starlark.call's error and return nil, which
// the caller reads as "the handler ran": it acks to the sender and advances
// the broadcast watermark past the sequence. The event was then lost for
// good, because the retry it should have caused arrives at or below the
// watermark and is classed as a duplicate.
//
// Propagating the error is only half of it. The dedup mark is set before
// dispatch, so the retry a failure asks for is coalesced away unless the
// worker forgets the id — see message_seen_clear.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// starlark_event_app writes a one-file Starlark app exporting the handlers
// the tests below need, and returns the App/AppVersion pair to run them.
func starlark_event_app(t *testing.T) (*App, *AppVersion) {
	t.Helper()
	// main() sizes the concurrency semaphore at startup; in a test binary it
	// is nil, and every acquire then blocks for the full queue timeout and
	// returns "no concurrency slot available". That error is not nil, so a
	// test asserting only "an error came back" passes without the handler
	// ever running. Same guard the other Starlark tests use.
	if starlark_semaphore == nil {
		starlark_semaphore = make(chan struct{}, 4)
	}
	// Likewise the compute timeout, which is read from configuration at
	// startup and is zero here — time.After(0) fires at once, so every call
	// returns "cancelled: timeout" before the handler executes a step.
	original_timeout := starlark_default_timeout
	starlark_default_timeout = 30 * time.Second
	t.Cleanup(func() { starlark_default_timeout = original_timeout })

	path := filepath.Join(t.TempDir(), "handlers.star")
	source := `
def handler_ok(e):
    return None

def handler_boom(e):
    return 1 // 0
`
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("writing test Starlark app: %v", err)
	}
	av := &AppVersion{Execute: []string{path}}
	av.Architecture.Engine = "starlark"
	// Fail fast if the source itself did not load: starlark() logs and
	// continues on a parse error, which would otherwise show up here as a
	// misleading "function not found".
	if _, found := av.starlark().globals["handler_ok"]; !found {
		t.Fatal("test Starlark app did not load; handler_ok is not defined")
	}
	return &App{id: "test-app"}, av
}

// TestRunHandlerReturnsStarlarkError: a handler that raises must surface as
// an error. This is the defect itself — the error was dropped and the
// caller saw success.
func TestRunHandlerReturnsStarlarkError(t *testing.T) {
	a, av := starlark_event_app(t)
	e := &Event{event: "test/event"}

	err := e.run_handler(a, av, AppEvent{Function: "handler_boom"})
	if err == nil {
		t.Fatal("run_handler returned nil for a handler that raised; the caller will ack and advance the watermark past a sequence that was never applied")
	}
	// Pin the error to the division, not just to non-nil: an uninitialised
	// semaphore or a failed load also produces an error, and either would
	// let this test pass without the handler having run at all.
	if !strings.Contains(err.Error(), "division by zero") {
		t.Errorf("run_handler error = %v, want the handler's own runtime error; the call may not have reached the handler", err)
	}
}

// TestRunHandlerReturnsMissingFunctionError: a handler named in the manifest
// but absent from the app's globals is a permanent failure, and must not be
// reported as a successful apply.
func TestRunHandlerReturnsMissingFunctionError(t *testing.T) {
	a, av := starlark_event_app(t)
	e := &Event{event: "test/event"}

	err := e.run_handler(a, av, AppEvent{Function: "handler_absent"})
	if err == nil {
		t.Fatal("run_handler returned nil for a function that does not exist")
	}
	// The wire classification keys off this message, so pin the pairing
	// rather than just the presence of an error.
	if reason := worker_failure_reason(err); reason != fail_unsupported {
		t.Errorf("worker_failure_reason for a missing handler = %q, want %q — a retryable reason means 50 redeliveries of a failure that can never succeed", reason, fail_unsupported)
	}
}

// TestRunHandlerSucceedsQuietly: the happy path still returns nil, so the
// change does not turn working deliveries into NACKs.
func TestRunHandlerSucceedsQuietly(t *testing.T) {
	a, av := starlark_event_app(t)
	e := &Event{event: "test/event"}

	if err := e.run_handler(a, av, AppEvent{Function: "handler_ok"}); err != nil {
		t.Fatalf("run_handler returned %v for a handler that succeeded; want nil", err)
	}
}

// TestWorkerFailClearsDedupForRetryableReason: the retry a transient failure
// asks for has to be dispatched when it arrives. With the mark left in
// place the receiver acks it without running the handler and the sender
// deletes its queue row — silent loss, the same outcome the fix removed.
func TestWorkerFailClearsDedupForRetryableReason(t *testing.T) {
	const id = "message-retryable"
	if message_seen_mark(id) {
		t.Fatalf("id %q was already marked seen before the test", id)
	}

	wf := &worker_frame{
		frame: &Frame{Type: frame_type_message, ID: id},
		reply: local_reply{message: id},
	}
	worker_fail(wf, fail_transient)

	if message_seen(id) {
		t.Error("worker_fail left the dedup mark set for a transient failure; the sender's retry will be coalesced away as a duplicate and the message lost")
	}
}

// TestWorkerFailKeepsDedupForDropReason: the mirror. A reason the sender
// treats as final must leave the mark, so a stray redelivery of a message
// the receiver has deliberately given up on is still dropped.
func TestWorkerFailKeepsDedupForDropReason(t *testing.T) {
	const id = "message-dropped"
	if message_seen_mark(id) {
		t.Fatalf("id %q was already marked seen before the test", id)
	}

	wf := &worker_frame{
		frame: &Frame{Type: frame_type_message, ID: id},
		reply: local_reply{message: id},
	}
	worker_fail(wf, fail_unsupported)

	if !message_seen(id) {
		t.Error("worker_fail cleared the dedup mark for a drop reason; a redelivery would be applied after the receiver had rejected it")
	}
}

// TestFailRetryableMatchesSenderDisposition pins fail_retryable against the
// vocabulary Sender.resolve_fail and queue_reply.fail actually use. The
// three must agree: a reason the sender re-queues but the receiver counts
// as final leaves the mark set and loses the retry.
func TestFailRetryableMatchesSenderDisposition(t *testing.T) {
	drops := []string{
		fail_unsupported, fail_unknown_user, fail_expired,
		fail_dedup, fail_signature_invalid,
	}
	for _, reason := range drops {
		if fail_retryable(reason) {
			t.Errorf("fail_retryable(%q) = true, want false — the sender drops this", reason)
		}
	}

	retries := []string{
		fail_transient, fail_handler_panic, fail_rate_limited,
		fail_buffer_full, fail_unclaimed, "", "reason-from-a-newer-peer",
	}
	for _, reason := range retries {
		if !fail_retryable(reason) {
			t.Errorf("fail_retryable(%q) = false, want true — the sender re-queues this", reason)
		}
	}
}
