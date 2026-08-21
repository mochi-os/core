// Mochi server: serving a file over P2P gets the transfer budget, like HTTP.
//
// A call opts into the transfer bound (900s) over the compute one (90s) by
// flipping its file_serving atomic. The stream writers never did, so the same
// file served over P2P timed out inside io.Copy, which ignores cancellation -
// the call was abandoned rather than stopped.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	sl "go.starlark.net/starlark"
)

// transfer_thread returns a thread carrying the file_serving atomic that
// Starlark.call installs before every call, plus the atomic itself.
func transfer_thread() (*sl.Thread, *atomic.Bool) {
	thread := &sl.Thread{}
	serving := &atomic.Bool{}
	thread.SetLocal("file_serving", serving)
	return thread, serving
}

// TestTransferSetMarksTheThread is the mechanism the writers now use. It is
// stated separately because everything below depends on it: if the flag stops
// being what Starlark.call reads, the writers can call this all day and still
// get the compute budget.
func TestTransferSetMarksTheThread(t *testing.T) {
	thread, serving := transfer_thread()

	if starlark_serving_get(thread) {
		t.Fatal("a fresh thread reports itself as already serving")
	}
	starlark_transfer_set(thread)

	if !serving.Load() {
		t.Error("starlark_transfer_set did not set the file_serving atomic")
	}
	if !starlark_serving_get(thread) {
		t.Error("starlark_serving_get does not observe what starlark_transfer_set writes; Starlark.call reads this to choose the budget")
	}
}

// TestTransferSetToleratesAnUnmarkedThread: the writers call it unconditionally,
// and not every thread reaching them was created by Starlark.call.
func TestTransferSetToleratesAnUnmarkedThread(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("starlark_transfer_set panicked on a thread with no file_serving local: %v", r)
		}
	}()
	starlark_transfer_set(&sl.Thread{})
	if starlark_serving_get(&sl.Thread{}) {
		t.Error("a thread with no file_serving local reports itself as serving")
	}
}

// stream_writers are the three builtins that copy bytes out over a stream.
// Each is reachable as both s.write.* and e.write.*.
var stream_writers = []string{
	"sl_write_file",
	"sl_write_cache",
	"sl_write_asset",
}

// writer_body returns one stream writer's source. It leans on
// stream_abandon_test.go's function_body, which strips line comments - without
// that these scans would match the prose explaining the very call they check,
// which is how two assertions in that file first passed for the wrong reason.
func writer_body(t *testing.T, name string) string {
	t.Helper()
	return function_body(t, "streams.go", "func (s *Stream) "+name+"(")
}

// TestEveryStreamWriterTakesTheTransferBudget is the gate. Written over all
// three rather than the one that produced the email, because they share a
// namespace and a purpose: whichever one an app reaches for, it is serving a
// file to somebody whose link speed the server does not control.
func TestEveryStreamWriterTakesTheTransferBudget(t *testing.T) {
	for _, name := range stream_writers {
		body := writer_body(t, name)
		if !strings.Contains(body, "starlark_transfer_set(t)") {
			t.Errorf("%s does not mark its call as a transfer, so it runs on the 90s compute budget; a file large enough, or a receiver slow enough, times out inside io.Copy - which does not check for cancellation, so the call is abandoned rather than stopped", name)
		}
	}
}

// TestTheMarkComesAfterValidation: the point of two budgets is that thinking
// is bounded tightly. A writer that marked itself before deciding whether the
// file exists would hand the long budget to its argument checking too.
func TestTheMarkComesAfterValidation(t *testing.T) {
	for _, name := range stream_writers {
		body := writer_body(t, name)
		mark := strings.Index(body, "starlark_transfer_set(t)")
		if mark < 0 {
			continue // reported by the gate above
		}
		// Every error return before the mark is validation; there must be at
		// least one, or the mark is at the top of the function.
		if !strings.Contains(body[:mark], "sl_error(fn") && !strings.Contains(body[:mark], "return sl.None, nil") {
			t.Errorf("%s marks the transfer before any validation; argument and permission checks are computation and belong inside the compute budget", name)
		}
		// And the copy must come after it.
		rest := body[mark:]
		if !strings.Contains(rest, "s.send(") && !strings.Contains(rest, "s.write_file(") {
			t.Errorf("%s marks the transfer but does not copy anything afterwards", name)
		}
	}
}

// TestTransferBudgetIsLongerThanCompute pins the relationship the fix relies
// on. starlark_configure floors file_timeout at the compute timeout, so this
// cannot be inverted by configuration - but it can be flattened, and a reader
// should find out here rather than from a timeout.
func TestTransferBudgetIsLongerThanCompute(t *testing.T) {
	if starlark_file_default <= 90_000_000_000 { // 90s in ns
		t.Errorf("the default transfer budget (%s) is not longer than the default compute budget; marking a writer as a transfer then buys it nothing", starlark_file_default)
	}

	source, err := os.ReadFile("starlark.go")
	if err != nil {
		t.Fatalf("reading starlark.go: %v", err)
	}
	if !strings.Contains(string(source), "if file_secs < secs {") {
		t.Error("starlark_configure no longer floors file_timeout at the compute timeout; an operator could configure a transfer budget shorter than the compute one, which would make the transfer branch cut downloads off EARLIER than not marking them at all")
	}
}

// TestTimeoutBranchStillReadsTheFlag is the other end of the wiring. The
// writers mark the thread; Starlark.call is what has to act on it.
func TestTimeoutBranchStillReadsTheFlag(t *testing.T) {
	source, err := os.ReadFile("starlark.go")
	if err != nil {
		t.Fatalf("reading starlark.go: %v", err)
	}
	text := string(source)

	if !regexp.MustCompile(`if serving\.Load\(\)`).MatchString(text) {
		t.Fatal("Starlark.call no longer branches on the serving flag at the compute timeout; nothing consumes what the writers set")
	}
	if !strings.Contains(text, "starlark_file_timeout") {
		t.Error("Starlark.call no longer waits on starlark_file_timeout")
	}
}

// TestPublisherServesItsPackageThroughAStreamWriter ties the gate to the report
// that prompted it. If Publisher stops serving packages this way the coupling
// is gone and this test should be removed rather than quietly kept passing.
func TestPublisherServesItsPackageThroughAStreamWriter(t *testing.T) {
	source, err := os.ReadFile("../../apps/publisher/publisher.star")
	if err != nil {
		t.Skipf("publisher app not present in this tree: %v", err)
	}
	text := string(source)
	at := strings.Index(text, "def event_get(e):")
	if at < 0 {
		t.Skip("publisher no longer defines event_get")
	}
	body := text[at:]
	if end := strings.Index(body, "\ndef "); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "e.write.file(") {
		t.Error("publisher's event_get no longer serves the package with e.write.file; the transfer budget it now depends on is applied in sl_write_file")
	}
}
