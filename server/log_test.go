// Mochi server: warn() admin-email rate-limit test.
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// TestWarnEmailAllow: the admin email for a warn format sends on first
// occurrence, is suppressed within the window, sends a different format on its
// own schedule, and after the window re-sends with the suppressed count.
func TestWarnEmailAllow(t *testing.T) {
	warn_email_state = map[string]warn_email_record{}

	// First occurrence of a format emails immediately.
	if send, sup := warn_email_allow("stream stalled peer=%q db=%q"); !send || sup != 0 {
		t.Errorf("first occurrence: send=%v suppressed=%d, want true/0", send, sup)
	}
	// Repeats within the window are suppressed.
	if send, _ := warn_email_allow("stream stalled peer=%q db=%q"); send {
		t.Error("first repeat must be suppressed")
	}
	if send, _ := warn_email_allow("stream stalled peer=%q db=%q"); send {
		t.Error("second repeat must be suppressed")
	}
	// A distinct format emails on its own schedule, unaffected.
	if send, _ := warn_email_allow("exec failed user=%q"); !send {
		t.Error("a distinct format must email immediately")
	}

	// After the window elapses, the first format re-sends and reports the two
	// suppressed occurrences.
	record := warn_email_state["stream stalled peer=%q db=%q"]
	record.last = now() - warn_email_window - 1
	warn_email_state["stream stalled peer=%q db=%q"] = record
	send, suppressed := warn_email_allow("stream stalled peer=%q db=%q")
	if !send || suppressed != 2 {
		t.Errorf("post-window: send=%v suppressed=%d, want true/2", send, suppressed)
	}
	// The rollup count resets after a send.
	if send, sup := warn_email_allow("stream stalled peer=%q db=%q"); send || sup != 0 {
		t.Errorf("immediately after re-send must suppress with reset count; send=%v suppressed=%d", send, sup)
	}
}
