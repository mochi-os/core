// Mochi server: email deliverability guard - never attempt delivery to the
// RFC 2606 / 6761 reserved domains, whose mail can only bounce.
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func TestEmailDeliverable(t *testing.T) {
	blocked := []string{
		"p2ptest-owner-123@example.com", "x@example.net", "y@example.org",
		"a@sub.example.com", "b@anything.test", "c@host.invalid",
		"d@foo.localhost", "e@bar.example", "nodomain",
	}
	for _, a := range blocked {
		if email_deliverable(a) {
			t.Errorf("email_deliverable(%q) = true, want false (reserved/undeliverable)", a)
		}
	}
	allowed := []string{
		"alistair@acunningham.org", "duc@gmail.com", "user@mochi-os.org",
		"a@example.company", // NOT example.com - a real-looking TLD
		"b@testcorp.com",    // contains 'test' but not the .test TLD
	}
	for _, a := range allowed {
		if !email_deliverable(a) {
			t.Errorf("email_deliverable(%q) = false, want true (real address)", a)
		}
	}
}
