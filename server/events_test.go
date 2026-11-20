// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Tests for the failure path of a Starlark event handler.
//
// A discarded handler error reads as "the handler ran": the caller acks and
// advances the broadcast watermark, and the retry then arrives below it as a
// duplicate. Propagating the error is only half - the dedup record is taken
// before dispatch, so the retry is coalesced away unless the worker's failure
// leaves the record due a retry (message_end).

package main

import (
	sl "go.starlark.net/starlark"
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
	// main() sizes the concurrency semaphore at startup; in a test binary it is
	// nil and every acquire returns "no concurrency slot available", so a test
	// asserting only that an error came back passes without the handler running.
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
	dedup_isolate(t)
	const id = "message-retryable"
	if state, _ := message_begin(id, "peer", nil); state != message_new {
		t.Fatalf("id %q was already marked seen before the test", id)
	}

	wf := &worker_frame{
		frame:  &Frame{Type: frame_type_message, ID: id},
		reply:  local_reply{message: id},
		marked: true,
	}
	worker_fail(wf, fail_transient)

	if state, _ := message_begin(id, "peer", nil); state != message_new {
		t.Errorf("after a transient failure the retry was answered %d, not run; it will be coalesced away as a duplicate and the message lost", state)
	}
}

// TestWorkerFailKeepsDedupForDropReason: the mirror. A reason the sender
// treats as final must leave the mark, so a stray redelivery of a message
// the receiver has deliberately given up on is answered the same.
func TestWorkerFailKeepsDedupForDropReason(t *testing.T) {
	dedup_isolate(t)
	const id = "message-dropped"
	if state, _ := message_begin(id, "peer", nil); state != message_new {
		t.Fatalf("id %q was already marked seen before the test", id)
	}

	wf := &worker_frame{
		frame:  &Frame{Type: frame_type_message, ID: id},
		reply:  local_reply{message: id},
		marked: true,
	}
	worker_fail(wf, fail_unsupported)

	if state, reason := message_begin(id, "peer", nil); state != message_refused || reason != fail_unsupported {
		t.Errorf("a redelivery after a drop was answered %d %q, want the drop again; it would be applied after the receiver had rejected it", state, reason)
	}
}

// TestFailRetryableMatchesSenderDisposition pins fail_retryable, the one
// table Sender.resolve_fail, queue_reply.fail and the dedup record all read: a
// reason the sender re-queues but the receiver counts as final leaves the mark
// set and loses the retry.
func TestFailRetryableMatchesSenderDisposition(t *testing.T) {
	drops := []string{fail_unsupported, fail_unknown_user}
	for _, reason := range drops {
		if fail_retryable(reason) {
			t.Errorf("fail_retryable(%q) = true, want false — the sender drops this", reason)
		}
	}

	retries := []string{
		fail_transient, fail_handler_panic, fail_unproven,
		fail_buffer_full, fail_unclaimed, "", "reason-from-a-newer-peer",
	}
	for _, reason := range retries {
		if !fail_retryable(reason) {
			t.Errorf("fail_retryable(%q) = false, want true — the sender re-queues this", reason)
		}
	}
}

// Mochi server: e.header("local") unit tests
//
// "local" is true ONLY for an in-process self-loop (e.peer == net_id): it gates
// serving restricted apps to a host's own app-update loopback.
func event_header_local(t *testing.T, peer string) bool {
	t.Helper()
	e := &Event{peer: peer}
	v, err := e.sl_header(nil, sl.NewBuiltin("header", e.sl_header), sl.Tuple{sl.String("local")}, nil)
	if err != nil {
		t.Fatalf("sl_header(\"local\"): %v", err)
	}
	b, ok := v.(sl.Bool)
	if !ok {
		t.Fatalf("sl_header(\"local\") returned %T, want sl.Bool", v)
	}
	return bool(b)
}

func TestEventHeaderLocal(t *testing.T) {
	saved := net_id
	defer func() { net_id = saved }()

	net_id = "12D3KooWSelfPeerIdentityForTest"

	// In-process self-loop: peer == net_id -> local.
	if !event_header_local(t, net_id) {
		t.Errorf("self-loop (peer==net_id): local = false, want true")
	}

	// Remote peer: peer != net_id -> not local.
	if event_header_local(t, "12D3KooWRemotePeerIdentity") {
		t.Errorf("remote peer: local = true, want false")
	}

	// Empty peer (e.g. non-stream event) is not local.
	if event_header_local(t, "") {
		t.Errorf("empty peer: local = true, want false")
	}

	// Guard: when net_id is unset, nothing is local — even an empty peer must
	// not match an empty net_id.
	net_id = ""
	if event_header_local(t, "") {
		t.Errorf("empty net_id with empty peer: local = true, want false")
	}
}

// TestRouteTakesTheDispatchersResolution is #163: every dispatcher resolves a
// frame's target with target_resolve, and Event.route then looked it up a
// second time with a copy of its own. A caller that resolved the target hands
// route the entity and its owner, and route does not look again.
func TestRouteTakesTheDispatchersResolution(t *testing.T) {
	setup_replication_test(t)
	setup_users_test_schema()
	ran := make(chan *Event, 1)
	test_service(t, "resolvedapp", "arrive", func(e *Event) { ran <- e })
	// Resolved by the dispatcher; users.db does not know it.
	user := &User{UID: "resolved-user"}
	target := test_entity_id('q')
	e := &Event{id: event_id(), to: target, user: user, service: "resolvedapp", event: "arrive"}
	if err := e.route(); err != nil {
		t.Fatalf("THE POINT: route looked up a target its caller had resolved: %v", err)
	}
	select {
	case got := <-ran:
		if got.user != user || got.to != target {
			t.Errorf("the handler ran for user %v, target %q; want the dispatcher's", got.user, got.to)
		}
	default:
		t.Error("the handler did not run")
	}
	// Control: unresolved, the same target is refused for good.
	unresolved := &Event{id: event_id(), to: target, service: "resolvedapp", event: "arrive"}
	if reason := worker_failure_reason(unresolved.route()); reason != fail_unknown_user {
		t.Errorf("control: an unresolvable target was answered %q, want %q", reason, fail_unknown_user)
	}
}
