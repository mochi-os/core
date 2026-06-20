// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

func Test_web_log_redact(t *testing.T) {
	cases := []struct {
		path string
		want string
	}{
		{"/_/websocket?token=abc.def.ghi", "/_/websocket?token=redacted"},
		{"/feeds/x/-/file?token=eyJhbGci&thumbnail=1", "/feeds/x/-/file?token=redacted&thumbnail=1"},
		{"/feeds/x/-/file?thumbnail=1&token=eyJhbGci", "/feeds/x/-/file?thumbnail=1&token=redacted"},
		{"/feeds/x/-/list?cursor=10", "/feeds/x/-/list?cursor=10"},
		{"/_/health", "/_/health"},
	}
	for _, c := range cases {
		if got := web_log_redact(c.path); got != c.want {
			t.Errorf("web_log_redact(%q) = %q, want %q", c.path, got, c.want)
		}
	}
}
