// Mochi server: log line flood suppression regression.
//
// The 2026-07 broadcast gap flood wrote ~60 debug lines/sec and cut yuzu's
// journald retention to ~35 minutes, evicting the evidence needed to diagnose
// the incident. log_repeat_allow suppresses a format past
// log_repeat_threshold lines per window, keyed by format string so distinct
// call sites never throttle each other.
//
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestLogRepeatAllow(t *testing.T) {
	threshold := log_repeat_threshold
	log_repeat_threshold = 3
	defer func() { log_repeat_threshold = threshold }()

	// Under the threshold: every line prints.
	for i := 0; i < 3; i++ {
		if !log_repeat_allow("format-a %d") {
			t.Fatalf("line %d under threshold must print", i+1)
		}
	}

	// Over the threshold inside the window: suppressed.
	if log_repeat_allow("format-a %d") {
		t.Fatal("line over threshold inside the window must be suppressed")
	}

	// A different format is keyed independently and unaffected.
	if !log_repeat_allow("format-b %s") {
		t.Fatal("distinct format must not share a throttle key")
	}

	// Window roll: printing resumes and the suppressed count resets.
	log_repeat_mutex.Lock()
	log_repeat_state["format-a %d"].start -= log_repeat_window + 1
	log_repeat_mutex.Unlock()
	if !log_repeat_allow("format-a %d") {
		t.Fatal("first line of a fresh window must print")
	}
	log_repeat_mutex.Lock()
	count := log_repeat_state["format-a %d"].count
	log_repeat_mutex.Unlock()
	if count != 1 {
		t.Fatalf("fresh window count = %d, want 1", count)
	}
}
