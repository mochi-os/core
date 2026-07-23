// Mochi server: remote address-wait bounding tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

// TestRateLimiterSince pins the timestamp remote_reach relies on: how long ago
// the current window for a key began. A limit-1 limiter has exactly one action
// per window, so this is how long ago that action happened.
func TestRateLimiterSince(t *testing.T) {
	limiter := &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 1, window: 60}

	// No entry: a large sentinel, so a caller reads it as "long ago" with no
	// special case.
	if age := limiter.since("absent"); age < time.Hour {
		t.Errorf("since on an unknown key = %s, want a large sentinel", age)
	}

	// A just-recorded action reads as roughly zero.
	limiter.allow("peer")
	if age := limiter.since("peer"); age > 2*time.Second {
		t.Errorf("since immediately after allow = %s, want near zero", age)
	}

	// An action whose window began 40s ago reads as ~40s. Reach into the entry
	// to set the window start rather than sleeping.
	limiter.entries["peer"].reset = now() + limiter.window - 40
	if age := limiter.since("peer"); age < 39*time.Second || age > 41*time.Second {
		t.Errorf("since for a 40s-old window = %s, want ~40s", age)
	}

	// An expired window reads as the sentinel: no live request to wait on.
	limiter.entries["peer"].reset = now() - 1
	if age := limiter.since("peer"); age < time.Hour {
		t.Errorf("since for an expired window = %s, want the sentinel", age)
	}
}

// TestAddressWaitBudget pins the rule remote_reach applies: a fresh request
// waits the full window for its answer, a request already several seconds old
// waits only the remainder, and a stale one is not waited on at all. This is
// the calculation inline in remote_reach, exercised directly so the intent is
// pinned even though the surrounding loop needs a live mesh.
func TestAddressWaitBudget(t *testing.T) {
	limiter := &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 1, window: 60}
	target := "12D3KooWTarget"

	budget := func(requested bool) time.Duration {
		b := remote_address_wait
		if !requested {
			freshest := remote_address_wait
			if age := limiter.since(target); age < freshest {
				freshest = age
			}
			b = remote_address_wait - freshest
			if b < 0 {
				b = 0
			}
		}
		return b
	}

	// We broadcast: full window regardless of any timestamp.
	if got := budget(true); got != remote_address_wait {
		t.Errorf("after broadcasting, budget = %s, want the full %s", got, remote_address_wait)
	}

	// Suppressed, request just went out: still essentially the full window, an
	// answer is plausibly in flight.
	limiter.allow(target)
	if got := budget(false); got < remote_address_wait-2*time.Second {
		t.Errorf("with a fresh suppressed request, budget = %s, want near the full window", got)
	}

	// Suppressed, request older than the window: no answer is coming, so do not
	// wait at all — the pointless hang this fixes.
	limiter.entries[target].reset = now() + limiter.window - int64(remote_address_wait/time.Second) - 5
	if got := budget(false); got != 0 {
		t.Errorf("with a stale suppressed request, budget = %s, want 0", got)
	}
}
