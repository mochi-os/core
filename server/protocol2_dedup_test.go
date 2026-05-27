// Tests for the dedup window invariant from claude/plans/protocol2.md:
//
//   message_mark_seen's retention window must exceed the queue's max
//   retry interval by at least 2×. ... Add a test in queue_test.go that
//   asserts the relation.
//
// Phase 3h per claude/plans/protocol2.md → Testing strategy.

package main

import (
	"testing"
)

// TestDedupWindowExceedsMaxRetryInterval asserts the invariant called
// out by the plan: the dedup cache must outlive the longest retry gap
// by 2× so a late retry can't be misclassified as a fresh message
// after the dedup record expired.
//
// retry_delays is the per-attempt backoff schedule; the cap (final
// element) bounds the longest gap between two adjacent attempts. The
// dedup TTL must be at least twice that.
func TestDedupWindowExceedsMaxRetryInterval(t *testing.T) {
	if len(retry_delays) == 0 {
		t.Skip("retry_delays empty; invariant not applicable")
	}
	max_gap := retry_delays[0]
	for _, d := range retry_delays {
		if d > max_gap {
			max_gap = d
		}
	}
	required := 2 * max_gap
	if seen_messages_ttl < required {
		t.Errorf("dedup window invariant violated: seen_messages_ttl=%d, max retry gap=%d, required ≥ %d (2× max gap). "+
			"Bump seen_messages_ttl OR cap retry_delays so the relation holds.",
			seen_messages_ttl, max_gap, required)
	}
}
