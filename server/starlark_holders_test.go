// Mochi server: Starlark slot-holder reporting
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

// Saturation of the concurrency pool used to be undiagnosable after the fact:
// the error named whichever call lost the race for a slot, never what was
// occupying them, and the occupants had finished by the time anyone read the
// log. These cover the registry that answers the question.

package main

import (
	"strings"
	"testing"
	"time"

	sl "go.starlark.net/starlark"
)

// starlark_holders_reset clears the registry and the throttle so a test starts
// from a known pool. The registry is package state shared with every other
// test in the binary.
func starlark_holders_reset(t *testing.T) {
	t.Helper()
	starlark_holders.lock.Lock()
	starlark_holders.calls = map[int64]starlark_holder{}
	starlark_holders.reported = time.Time{}
	starlark_holders.lock.Unlock()
	t.Cleanup(func() {
		starlark_holders.lock.Lock()
		starlark_holders.calls = map[int64]starlark_holder{}
		starlark_holders.reported = time.Time{}
		starlark_holders.lock.Unlock()
	})
}

// TestHoldersReportNamesTheOccupants is the whole point of the registry: the
// report has to say which app and function filled the pool, and how many slots
// each took, because that is the part the failure message cannot carry.
func TestHoldersReportNamesTheOccupants(t *testing.T) {
	starlark_holders_reset(t)

	for i := 0; i < 3; i++ {
		starlark_holder_add("feeds", "event_attachment_fetch", "user-a")
	}
	starlark_holder_add("wikis", "action_page_view", "user-b")

	report := starlark_holders_report()

	if !strings.Contains(report, "4 slots held") {
		t.Errorf("report = %q, want the total slot count", report)
	}
	if !strings.Contains(report, "feeds:event_attachment_fetch x3") {
		t.Errorf("report = %q, want the dominant holder named with its count", report)
	}
	if !strings.Contains(report, "wikis:action_page_view x1") {
		t.Errorf("report = %q, want the minority holder named too", report)
	}
	// Ordered by how much of the pool each group holds: an operator reads the
	// first entry and stops.
	if strings.Index(report, "feeds:") > strings.Index(report, "wikis:") {
		t.Errorf("report = %q, want the largest holder first", report)
	}
	if !strings.Contains(report, "user-a") {
		t.Errorf("report = %q, want the account behind the oldest holder", report)
	}
}

// TestHoldersReportThrottled: one saturation stalls every queued call, so an
// unthrottled report would repeat itself once per victim - 200 times in the
// episode that prompted this.
func TestHoldersReportThrottled(t *testing.T) {
	starlark_holders_reset(t)
	starlark_holder_add("feeds", "schedule_sources_poll", "")

	if first := starlark_holders_report(); first == "" {
		t.Fatal("the first report was suppressed; nothing would ever be logged")
	}
	if second := starlark_holders_report(); second != "" {
		t.Errorf("a second report inside the window returned %q, want it suppressed", second)
	}
}

// TestHoldersReportSurvivesADrainedPool covers the race between the failed
// acquire and the sample: reporting an empty list as though it were the state
// during the wait would be a lie.
func TestHoldersReportSurvivesADrainedPool(t *testing.T) {
	starlark_holders_reset(t)

	report := starlark_holders_report()
	if !strings.Contains(report, "drained") {
		t.Errorf("report on an empty pool = %q, want it to say the pool drained before it could be sampled", report)
	}
}

// TestHolderRegisteredForTheLifeOfTheCall drives a real Starlark call and
// samples the registry from inside it. Registration is tied to the same defer
// that returns the slot, so this is what proves the two cannot diverge.
func TestHolderRegisteredForTheLifeOfTheCall(t *testing.T) {
	starlark_holders_reset(t)

	original := starlark_semaphore
	original_timeout := starlark_default_timeout
	starlark_semaphore = make(chan struct{}, 1)
	// A test binary never runs starlark_configure, so the compute bound is
	// zero and the call would be cancelled before it reached the builtin.
	starlark_default_timeout = 30 * time.Second
	t.Cleanup(func() {
		starlark_semaphore = original
		starlark_default_timeout = original_timeout
	})

	entered := make(chan struct{})
	release := make(chan struct{})
	block := sl.NewBuiltin("block", func(_ *sl.Thread, _ *sl.Builtin, _ sl.Tuple, _ []sl.Tuple) (sl.Value, error) {
		close(entered)
		<-release
		return sl.None, nil
	})

	s := &Starlark{thread: &sl.Thread{Name: "test"}, globals: sl.StringDict{"block": block}}
	globals, err := sl.ExecFile(s.thread, "held.star", "def held():\n    return block()\n", s.globals)
	if err != nil {
		t.Fatalf("loading the fixture: %v", err)
	}
	s.globals = globals
	s.thread.SetLocal("app", &App{id: "feeds"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.call("held", nil)
	}()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the call never reached the blocking builtin")
	}

	report := starlark_holders_report()
	if !strings.Contains(report, "feeds:held") {
		t.Errorf("report while the call held a slot = %q, want it to name feeds:held", report)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the call never returned")
	}

	// The registration must go back with the slot. A holder left behind would
	// make every later report name a call that finished long ago.
	starlark_holders.lock.Lock()
	remaining := len(starlark_holders.calls)
	starlark_holders.lock.Unlock()
	if remaining != 0 {
		t.Errorf("%d holders remain after the call returned, want 0 - a registration outlived its slot", remaining)
	}
}
