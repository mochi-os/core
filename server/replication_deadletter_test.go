// Tests for forward-incompatible op quarantine (schema-skew dead-letter).
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"errors"
	"testing"
)

// TestReplicationExecForwardIncompatible: only schema-shape errors (the
// statement references a column/table this newer receiver lost) classify
// as forward-incompatible; transient / unrelated errors do not. The
// classification decides whether a failed replicated exec is quarantined
// (recorded + skipped, no email) or treated as a genuine fault (warn).
func TestReplicationExecForwardIncompatible(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("no such column: left"), true},
		{errors.New("no such table: chats"), true},
		{errors.New("table chats has no column named left"), true},
		{errors.New("FOREIGN KEY constraint failed"), false},
		{errors.New("database is locked"), false},
		{errors.New("UNIQUE constraint failed: chats.id"), false},
	}
	for _, c := range cases {
		if got := replication_exec_forward_incompatible(c.err); got != c.want {
			t.Errorf("replication_exec_forward_incompatible(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
