// Mochi server: multipart body limit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
)

// TestMultipartMaximumAnonymous pins the ceiling for callers with no
// authenticated identity.
//
// This is the case that matters: multipart is exempt from web_body_limit, and
// ParseMultipartForm spools the whole body before the handler runs — to
// os.TempDir(), which on a systemd host is usually a tmpfs, so an unbounded
// body is consumed as memory. An anonymous request to a public action runs as
// the entity owner, so the limit must be derived from the authenticated user
// and never from the owner, or every anonymous caller inherits the owner's
// upload budget.
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

// TestMultipartMaximumFinite pins that every caller class gets a finite
// ceiling. Administrators are quota-exempt (user_storage_remaining returns
// MaxInt64 for them), so the derivation has to clamp rather than add framing to
// MaxInt64 and overflow into a negative limit — which would reject every
// upload, or worse, be treated as unbounded.
func TestMultipartMaximumFinite(t *testing.T) {
	for _, remaining := range []int64{1<<62 - 1, file_max_storage, 1} {
		limit := remaining
		if limit > file_max_storage {
			limit = file_max_storage
		}
		limit += web_multipart_framing
		if limit <= 0 {
			t.Errorf("derived limit for remaining=%d overflowed to %d", remaining, limit)
		}
		if limit > file_max_storage+web_multipart_framing {
			t.Errorf("derived limit for remaining=%d is %d, above the per-user ceiling", remaining, limit)
		}
	}
}
