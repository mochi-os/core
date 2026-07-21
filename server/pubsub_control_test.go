// Mochi server: pubsub control-plane rate limit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
)

// TestPubsubControlBudgetIsSeparate pins that application traffic cannot
// starve the peers service.
//
// One shared budget meant a host flooding directory announcements — whose
// volume follows user activity and is effectively unbounded — consumed the
// same allowance as the messages hosts use to learn each other's addresses.
// A synchronous remote request blocks for five seconds on one of those
// answers (remote_reach), so losing it reports a peer that is online and
// actively exchanging messages as unreachable.
func TestPubsubControlBudgetIsSeparate(t *testing.T) {
	peer := "12D3KooWTestPeerForControlBudget"

	original_in := rate_limit_pubsub_in
	original_control := rate_limit_pubsub_control
	t.Cleanup(func() {
		rate_limit_pubsub_in = original_in
		rate_limit_pubsub_control = original_control
	})
	// Fresh limiters so other tests' traffic cannot affect the counts.
	rate_limit_pubsub_in = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 3, window: 60}
	rate_limit_pubsub_control = &rate_limiter{entries: map[string]*rate_limit_entry{}, limit: 3, window: 60}

	// Exhaust the budget a directory flood is charged against, going through
	// the same routing the receive loop uses — asserting on the limiters
	// directly would pass even if the loop charged everything to one of them.
	for i := 0; i < 3; i++ {
		if !pubsub_limiter("directory").allow(peer) {
			t.Fatalf("application budget refused message %d, before its limit", i+1)
		}
	}
	if pubsub_limiter("directory").allow(peer) {
		t.Fatal("application budget did not stop at its limit")
	}

	// Peer control traffic must be unaffected by that flood.
	if !pubsub_limiter("peers").allow(peer) {
		t.Error("a directory flood exhausted the peers budget: an address announcement would be dropped, reporting an online peer as unreachable")
	}
}

// TestPubsubControlBudgetIsBounded pins that the separate budget is still a
// budget. Giving the control plane its own allowance must not hand a flooder
// an unmetered path into the receive loop.
func TestPubsubControlBudgetIsBounded(t *testing.T) {
	if rate_limit_pubsub_control.limit <= 0 || rate_limit_pubsub_control.window <= 0 {
		t.Fatalf("control limiter is unbounded: limit=%d window=%d",
			rate_limit_pubsub_control.limit, rate_limit_pubsub_control.window)
	}

	peer := "12D3KooWTestPeerForControlBound"
	original := rate_limit_pubsub_control
	t.Cleanup(func() { rate_limit_pubsub_control = original })
	rate_limit_pubsub_control = &rate_limiter{
		entries: map[string]*rate_limit_entry{},
		limit:   original.limit,
		window:  original.window,
	}

	for i := 0; i < original.limit; i++ {
		if !rate_limit_pubsub_control.allow(peer) {
			t.Fatalf("control budget refused message %d, before its limit of %d", i+1, original.limit)
		}
	}
	if rate_limit_pubsub_control.allow(peer) {
		t.Errorf("control budget did not stop at its limit of %d", original.limit)
	}
}
