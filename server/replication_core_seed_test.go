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

// TestKeysTransferSeedsAppSystemCursors: #61 — the source's app:<entity>/system
// sub-stream tails are snapshotted into AppSystemSeeds and the receiver anchors
// its inbound cursor to each, so the first live /system op chains instead of
// stalling. The /system DATA rides in the app DB file, but the file-bootstrap
// cursor seed only anchors the main app:<entity> stream (the app-DB analog of #54).
func TestKeysTransferSeedsAppSystemCursors(t *testing.T) {
	defer setup_replication_test(t)()
	rdb := db_open("db/replication.db")
	const user = "u-appsys-seed"
	const peer = "peerSrc"
	const sysA = "app:12abc/system"
	const sysB = "app:34def/system"

	// Source emitted /system ops on two apps, plus a MAIN app stream that must
	// NOT be picked up (the file bootstrap already seeds the main stream).
	replication_tail_advance(user, repl_scope_app, sysA, 412)
	replication_tail_advance(user, repl_scope_app, sysB, 77)
	replication_tail_advance(user, repl_scope_app, "app:12abc", 9000)

	seeds := replication_app_system_seeds(user)
	if seeds[sysA] != 412 || seeds[sysB] != 77 {
		t.Fatalf("app-system seeds = %v, want %s=412 %s=77", seeds, sysA, sysB)
	}
	if _, ok := seeds["app:12abc"]; ok {
		t.Fatalf("main app stream leaked into app-system seeds: %v", seeds)
	}

	if _, anchored := replication_cursor(rdb, peer, repl_scope_app, user, sysA); anchored {
		t.Fatal("sysA cursor unexpectedly anchored before seeding")
	}
	replication_seed_app_system_cursors(rdb, peer, user, seeds)
	for key, want := range map[string]int64{sysA: 412, sysB: 77} {
		seq, anchored := replication_cursor(rdb, peer, repl_scope_app, user, key)
		if !anchored || seq != want {
			t.Fatalf("%s cursor = (%d, anchored=%v), want (%d, true)", key, seq, anchored, want)
		}
	}

	// A never-emitted stream (tail 0) is skipped, not seeded at 0.
	replication_seed_app_system_cursors(rdb, peer, "u-empty", map[string]int64{"app:99zzz/system": 0})
	if _, anchored := replication_cursor(rdb, peer, repl_scope_app, "u-empty", "app:99zzz/system"); anchored {
		t.Fatal("zero-tail app-system stream should not seed a cursor")
	}

	// cursor_set is monotonic: re-applying an older seed must not rewind.
	replication_cursor_set(rdb, peer, repl_scope_app, user, sysA, 500)
	replication_seed_app_system_cursors(rdb, peer, user, map[string]int64{sysA: 412})
	if seq, _ := replication_cursor(rdb, peer, repl_scope_app, user, sysA); seq != 500 {
		t.Fatalf("re-seed rewound the cursor to %d, want 500 (monotonic)", seq)
	}
}

// TestKeysTransferCarriesLinkRows: #57 — the keys-transfer carries the user's
// links-table ROWS so a fresh replica gets the HISTORICAL links, not just the
// anchored core:links cursor (#54). Cursor seed alone leaves the rows missing.
func TestKeysTransferCarriesLinkRows(t *testing.T) {
	defer setup_replication_test(t)()
	rdb := db_open("db/replication.db")
	rdb.exec("create table if not exists links (user text not null, peer text not null, label text not null default '', placeholder text not null, received integer not null, expires integer not null, primary key (user, peer))")
	const user = "u-links"

	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values (?, 'peerA', 'lbl-a', 'ph-a', 100, 200)", user)
	rdb.exec("insert into links (user, peer, label, placeholder, received, expires) values (?, 'peerB', '', 'ph-b', 5, 9)", user)

	links := replication_user_links(user)
	if len(links) != 2 {
		t.Fatalf("user_links = %d, want 2", len(links))
	}

	// Apply onto a cleared receiver — the rows must round-trip via the transfer.
	rdb.exec("delete from links where user = ?", user)
	replication_seed_links(rdb, user, links)
	if n := rdb.integer("select count(*) from links where user = ?", user); n != 2 {
		t.Fatalf("seeded links = %d, want 2", n)
	}
	row, _ := rdb.row("select label, placeholder, expires from links where user = ? and peer = 'peerA'", user)
	if row == nil || row["label"] != "lbl-a" || row["placeholder"] != "ph-a" {
		t.Fatalf("link row round-trip failed: %v", row)
	}
}
