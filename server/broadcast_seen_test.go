// Mochi server: Broadcast seen / idle-resync (#165) tests
// Copyright Alistair Cunningham 2026

package main

import (
	"testing"
)

// TestBroadcastSeenStampedOnAdvance — every applied broadcast stamps seen=now
// for (sender, key), readable via broadcast_seen_get, without disturbing last.
func TestBroadcastSeenStampedOnAdvance(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	if s := broadcast_seen_get(db, "feed1"); s != 0 {
		t.Errorf("seen before any apply: got %d, want 0", s)
	}

	before := now()
	broadcast_advance_local(db, "peerA", "feed1", 3)
	after := now()

	seen := broadcast_seen_get(db, "feed1")
	if seen < before || seen > after {
		t.Errorf("seen after advance: got %d, want within [%d, %d]", seen, before, after)
	}
	if last := broadcast_received_get(db, "peerA", "feed1"); last != 3 {
		t.Errorf("received last after advance: got %d, want 3", last)
	}
}

// TestBroadcastSeenMaxOverPeers — seen is max across senders, so a paired owner
// (two peers, same key) reports the freshest apply; owner host-migration (new
// peer, same key) is the same shape.
func TestBroadcastSeenMaxOverPeers(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	broadcast_advance_local(db, "peerA", "feed1", 1)
	later := now() + 100
	db.exec("insert into received (sender, key, last, seen) values (?, ?, ?, ?)", "peerB", "feed1", 1, later)

	if s := broadcast_seen_get(db, "feed1"); s != later {
		t.Errorf("seen max-over-peers: got %d, want %d (peerB's later stamp)", s, later)
	}
}

// TestBroadcastTouchSeedsSeen — touch stamps seen for a key that never received
// a broadcast (empty source), via a sentinel sender='' row that does NOT leak
// into a real peer's position (the gap detector reads a specific (peer, key)),
// and whose last=0 never shadows a later real advance.
func TestBroadcastTouchSeedsSeen(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	before := now()
	broadcast_touch_local(db, "feed1")
	after := now()

	seen := broadcast_seen_get(db, "feed1")
	if seen < before || seen > after {
		t.Errorf("seen after touch: got %d, want within [%d, %d]", seen, before, after)
	}
	if last := broadcast_received_get(db, "somepeer", "feed1"); last != 0 {
		t.Errorf("touch leaked into a peer position: got last=%d, want 0", last)
	}

	broadcast_advance_local(db, "peerA", "feed1", 9)
	if last := broadcast_received_get(db, "peerA", "feed1"); last != 9 {
		t.Errorf("real advance after touch: got last=%d, want 9", last)
	}
}

// TestBroadcastSeenColumnMigration — an existing received table without the seen
// column (pre-#165 db) reads as seen=0, and the first advance migrates it in
// place and stamps seen without losing last.
func TestBroadcastSeenColumnMigration(t *testing.T) {
	db, cleanup := setup_broadcast_log_test(t)
	defer cleanup()

	// Old-schema table: no seen column.
	db.exec("create table received (sender text not null, key text not null, last integer not null default 0, primary key (sender, key))")
	db.exec("insert into received (sender, key, last) values (?, ?, ?)", "peerA", "feed1", 4)

	if s := broadcast_seen_get(db, "feed1"); s != 0 {
		t.Errorf("seen on pre-migration table: got %d, want 0", s)
	}

	before := now()
	broadcast_advance_local(db, "peerA", "feed1", 5)
	after := now()

	if last := broadcast_received_get(db, "peerA", "feed1"); last != 5 {
		t.Errorf("last after migrated advance: got %d, want 5", last)
	}
	seen := broadcast_seen_get(db, "feed1")
	if seen < before || seen > after {
		t.Errorf("seen after migrated advance: got %d, want within [%d, %d]", seen, before, after)
	}
}
