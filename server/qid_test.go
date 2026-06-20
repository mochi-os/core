// Tests for the Wikidata QID rate-limiter (#35).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

// TestQidRateWaitSkipsDuringBackoff: while a 429 backoff window is active,
// qid_rate_wait returns false immediately so callers skip the request rather
// than block a Starlark handler past the 90s watchdog (#35); once the window
// has elapsed it returns true.
func TestQidRateWaitSkipsDuringBackoff(t *testing.T) {
	orig_until := qid_backoff_until
	orig_last := qid_rate_last
	defer func() { qid_backoff_until = orig_until; qid_rate_last = orig_last }()

	// Active backoff window: must return false without blocking.
	qid_backoff_until = time.Now().Add(time.Hour)
	start := time.Now()
	if qid_rate_wait() {
		t.Error("qid_rate_wait returned true during an active backoff window")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("qid_rate_wait blocked %v during backoff; it must return immediately", elapsed)
	}

	// No active backoff: proceeds (true). Pre-age qid_rate_last so the
	// 1-request/second spacing doesn't add a real sleep.
	qid_backoff_until = time.Now().Add(-time.Hour)
	qid_rate_last = time.Now().Add(-time.Hour)
	if !qid_rate_wait() {
		t.Error("qid_rate_wait returned false with no active backoff window")
	}
}
