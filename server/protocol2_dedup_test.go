// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
)

// TestDedupWindowExceedsMaxRetryInterval: seen_messages_ttl must be at least 2x
// the longest retry_delays gap, or a late retry reads as a fresh message.
func TestDedupWindowExceedsMaxRetryInterval(t *testing.T) {
	if len(retry_delays) == 0 {
		t.Skip("retry_delays empty; invariant not applicable")
	}
	gap_maximum := retry_delays[0]
	for _, d := range retry_delays {
		if d > gap_maximum {
			gap_maximum = d
		}
	}
	required := 2 * gap_maximum
	if seen_messages_ttl < required {
		t.Errorf("dedup window invariant violated: seen_messages_ttl=%d, max retry gap=%d, required ≥ %d (2× max gap). "+
			"Bump seen_messages_ttl OR cap retry_delays so the relation holds.",
			seen_messages_ttl, gap_maximum, required)
	}
}
