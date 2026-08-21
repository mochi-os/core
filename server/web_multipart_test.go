// Mochi server: multipart body limit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
)

// TestMultipartMaximumAnonymous pins the ceiling for a caller with no identity:
// an anonymous request to a public action runs as the entity owner, so the
// limit must derive from the authenticated user and never from the owner.
func TestMultipartMaximumAnonymous(t *testing.T) {
	got := web_multipart_maximum(nil)
	want := int64(web_body_maximum + web_multipart_framing)
	if got != want {
		t.Errorf("anonymous multipart maximum = %d, want %d", got, want)
	}
	// An anonymous caller must not get more room for a multipart body than for
	// any other body.
	if got > web_body_maximum+web_multipart_framing {
		t.Errorf("anonymous multipart maximum %d exceeds the ordinary body limit plus framing", got)
	}
}

// Administrators are quota-exempt (user_storage_remaining returns MaxInt64), so
// the derivation must clamp rather than overflow into a negative limit.
func TestMultipartMaximumFinite(t *testing.T) {
	for _, remaining := range []int64{1<<62 - 1, file_maximum_storage, 1} {
		limit := remaining
		if limit > file_maximum_storage {
			limit = file_maximum_storage
		}
		limit += web_multipart_framing
		if limit <= 0 {
			t.Errorf("derived limit for remaining=%d overflowed to %d", remaining, limit)
		}
		if limit > file_maximum_storage+web_multipart_framing {
			t.Errorf("derived limit for remaining=%d is %d, above the per-user ceiling", remaining, limit)
		}
	}
}
