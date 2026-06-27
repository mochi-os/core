// Mochi server: #54 — bootstrap must seed inbound cursors for the per-event
// CORE sub-streams (links, notifications). Without the seed a fresh replica has
// no cursor for these streams and the first op stalls forever on the predecessor
// gap (the core:links / core:notifications missing-cursor stalls).
//
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import "testing"

// TestKeysTransferSeedsCoreSubStreamCursors: the source's core sub-stream tails
// are snapshotted into CoreSeeds and the receiver anchors its inbound cursor to
// each, so the next live op chains instead of buffering. A never-emitted stream
// (tail 0) is skipped — no spurious cursor row.
func TestKeysTransferSeedsCoreSubStreamCursors(t *testing.T) {
	defer setup_replication_test(t)()
	rdb := db_open("db/replication.db")
	const user = "u-core-seed"
	const peer = "peerSrc"

	// Source has emitted ops on the per-event core sub-streams.
	replication_tail_advance(user, repl_scope_app, repl_stream_key(repl_stream_class_core, "links"), 134)
	replication_tail_advance(user, repl_scope_app, repl_stream_key(repl_stream_class_core, "notifications"), 77)

	// The builder snapshots them into CoreSeeds under bare logical keys.
	seeds := replication_core_seeds(user)
	if seeds["links"] != 134 || seeds["notifications"] != 77 {
		t.Fatalf("core seeds = %v, want links=134 notifications=77", seeds)
	}

	// Before the seed the receiver has no inbound cursor — the stall condition.
	if _, anchored := replication_cursor(rdb, peer, repl_scope_app, user, repl_stream_key(repl_stream_class_core, "links")); anchored {
		t.Fatal("links cursor unexpectedly anchored before seeding")
	}

	// Apply: anchor the inbound cursors to the source's tail.
	replication_seed_core_cursors(rdb, peer, user, seeds)

	for stream, want := range map[string]int64{"links": 134, "notifications": 77} {
		seq, anchored := replication_cursor(rdb, peer, repl_scope_app, user, repl_stream_key(repl_stream_class_core, stream))
		if !anchored || seq != want {
			t.Fatalf("core:%s cursor = (%d, anchored=%v), want (%d, true)", stream, seq, anchored, want)
		}
	}

	// A stream the source never emitted (tail 0) is skipped, not seeded at 0.
	replication_seed_core_cursors(rdb, peer, "u-empty", map[string]int64{"links": 0})
	if _, anchored := replication_cursor(rdb, peer, repl_scope_app, "u-empty", repl_stream_key(repl_stream_class_core, "links")); anchored {
		t.Fatal("zero-tail stream should not seed a cursor")
	}

	// cursor_set is monotonic: re-applying an older seed must not rewind a
	// stream a live op has since advanced past.
	replication_cursor_set(rdb, peer, repl_scope_app, user, repl_stream_key(repl_stream_class_core, "links"), 200)
	replication_seed_core_cursors(rdb, peer, user, map[string]int64{"links": 134})
	if seq, _ := replication_cursor(rdb, peer, repl_scope_app, user, repl_stream_key(repl_stream_class_core, "links")); seq != 200 {
		t.Fatalf("re-seed rewound the cursor to %d, want 200 (monotonic)", seq)
	}
}
