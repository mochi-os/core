// Mochi server: serving-side rate limit on directory sync requests.
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func directory_sync_limit_reset() {
	rate_limit_directory_sync.lock.Lock()
	rate_limit_directory_sync.entries = make(map[string]*rate_limit_entry)
	rate_limit_directory_sync.lock.Unlock()
}

// The asymmetry is the point. One small anonymous frame makes the server read
// and stream every directory row at or after the requester's watermark, and a
// request carrying no watermark means the whole table. Capping the ROWS would
// break bootstrap - a joining peer legitimately needs all of them - so what is
// bounded is how often a peer may ask.
func TestDirectorySyncLimitBoundsRepeatedRequests(t *testing.T) {
	directory_sync_limit_reset()

	peer := "12D3KooWFlooder"
	allowed := 0
	for i := 0; i < 100; i++ {
		if rate_limit_directory_sync.allow(peer) {
			allowed++
		}
	}
	if allowed != rate_limit_directory_sync.limit {
		t.Errorf("expected %d full-table reads to be served, got %d",
			rate_limit_directory_sync.limit, allowed)
	}
}

// A limit shared across peers would let one flooder starve the fleet's real
// sync traffic, which is the failure this must not trade for.
func TestDirectorySyncLimitIsPerPeer(t *testing.T) {
	directory_sync_limit_reset()

	flooder := "12D3KooWFlooder"
	for i := 0; i < 100; i++ {
		rate_limit_directory_sync.allow(flooder)
	}
	if rate_limit_directory_sync.allow(flooder) {
		t.Fatal("flooder should be refused after exhausting its own budget")
	}

	// An honest peer syncing for the first time is unaffected.
	if !rate_limit_directory_sync.allow("12D3KooWHonest") {
		t.Error("a different peer must have its own budget")
	}
}

// The tick is every 5 minutes, so a peer that restarts and re-syncs a few
// times in a row must never be refused. A limit tight enough to catch normal
// operation would be worse than none: it would break bootstrap intermittently
// and look like a network fault.
func TestDirectorySyncLimitLeavesRoomForRealSyncs(t *testing.T) {
	directory_sync_limit_reset()

	peer := "12D3KooWRestarting"
	// Three syncs inside one window models a restart, a reconnect and a
	// bootstrap retry landing together.
	for i := 0; i < 3; i++ {
		if !rate_limit_directory_sync.allow(peer) {
			t.Fatalf("legitimate sync %d refused; the limit is too tight", i+1)
		}
	}
	if rate_limit_directory_sync.window != 60 || rate_limit_directory_sync.limit < 3 {
		t.Errorf("limit %d per %ds leaves no headroom over the 5-minute sync tick",
			rate_limit_directory_sync.limit, rate_limit_directory_sync.window)
	}
}

// The wiring, not the policy. The three tests above exercise the limiter
// directly and pass even with the check deleted from the handler - so on their
// own they would let the whole fix be removed and stay green. This one calls
// the handler and asserts the limiter was consulted.
//
// Observed through the limiter's own bookkeeping: a handler that checks leaves
// an entry for the peer, one that does not leaves the map untouched. An
// earlier version of this test watched for a database panic instead, which
// never came - db_open succeeds here - so it passed with the check deleted.
func TestDirectorySyncEventConsultsTheLimit(t *testing.T) {
	directory_sync_limit_reset()

	peer := "12D3KooWFreshPeer"
	func() {
		// The handler streams to e.stream after the check; with no stream and
		// no rows that is a no-op, but recover keeps an empty-directory
		// assumption from turning into a flaky failure.
		defer func() { _ = recover() }()
		directory_sync_event(&Event{peer: peer, content: map[string]any{}})
	}()

	rate_limit_directory_sync.lock.Lock()
	entry := rate_limit_directory_sync.entries[peer]
	rate_limit_directory_sync.lock.Unlock()

	if entry == nil {
		t.Fatal("directory_sync_event served a request without consulting the rate limit")
	}
	if entry.count != 1 {
		t.Errorf("expected the request to charge exactly 1, got %d", entry.count)
	}
}
