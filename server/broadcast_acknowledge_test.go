// Mochi server: Broadcast acknowledgement watermark tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
)

// setup_acknowledge_test gives a temp dir + DB scoped to the test. Separate
// from setup_broadcast_log_test only so the two files stay independent.
func setup_acknowledge_test(t *testing.T) *DB {
	t.Helper()
	test_data_directory(t)
	db := db_open("db/test.db")
	return db
}

// acknowledged_last reads a subscriber's recorded watermark, or -1 when the
// subscriber has no row at all.
func acknowledged_last(t *testing.T, db *DB, key, peer, subscriber string) int64 {
	t.Helper()
	row, _ := db.row("select last from acknowledged where key=? and peer=? and subscriber=?", key, peer, subscriber)
	if row == nil {
		return -1
	}
	last, _ := row["last"].(int64)
	return last
}

func log_count(t *testing.T, db *DB, key, peer string) int64 {
	t.Helper()
	row, _ := db.row("select count(*) as c from log where key=? and peer=?", key, peer)
	if row == nil {
		return 0
	}
	count, _ := row["c"].(int64)
	return count
}

// TestAcknowledgeClampsAboveHead - the sequence arrives over the network and
// feeds broadcast_log_ack_trim, so a watermark past the head is capped.
func TestAcknowledgeClampsAboveHead(t *testing.T) {
	db := setup_acknowledge_test(t)

	for i := 0; i < 3; i++ {
		broadcast_log_append(db, "k1", "peerA", "event/a", []byte(`{}`))
	}

	e := &Event{db: db, from: "subscriber1", content: map[string]any{
		"key": "k1", "peer": "peerA", "sequence": int64(999999),
	}}
	if err := e.broadcast_acknowledge(); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}

	if got := acknowledged_last(t, db, "k1", "peerA", "subscriber1"); got != 3 {
		t.Errorf("watermark: got %d, want 3 (the head)", got)
	}
}

// TestAcknowledgeHonoursHonestWatermark — the ordinary case is untouched: a
// subscriber acking within the log records exactly what it claimed, and the
// trim removes only what everyone has seen.
func TestAcknowledgeHonoursHonestWatermark(t *testing.T) {
	db := setup_acknowledge_test(t)

	for i := 0; i < 3; i++ {
		broadcast_log_append(db, "k1", "peerA", "event/a", []byte(`{}`))
	}

	e := &Event{db: db, from: "subscriber1", content: map[string]any{
		"key": "k1", "peer": "peerA", "sequence": int64(2),
	}}
	if err := e.broadcast_acknowledge(); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}

	if got := acknowledged_last(t, db, "k1", "peerA", "subscriber1"); got != 2 {
		t.Errorf("watermark: got %d, want 2", got)
	}
	// Sequence 1 is below the floor and goes; 2 and 3 stay.
	if got := log_count(t, db, "k1", "peerA"); got != 2 {
		t.Errorf("log rows after trim: got %d, want 2", got)
	}
}

// TestAcknowledgeIgnoredForUnknownStream — no sequence row means this host
// never originated the stream, so there is nothing to acknowledge. Recording a
// floor against a log we do not own would let any sender create rows for
// streams that are none of our business.
func TestAcknowledgeIgnoredForUnknownStream(t *testing.T) {
	db := setup_acknowledge_test(t)

	e := &Event{db: db, from: "stranger", content: map[string]any{
		"key": "never-seen", "peer": "peerA", "sequence": int64(5),
	}}
	if err := e.broadcast_acknowledge(); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}

	if got := acknowledged_last(t, db, "never-seen", "peerA", "stranger"); got != -1 {
		t.Errorf("watermark row was created for an unknown stream: got %d", got)
	}
}

// TestAcknowledgeStaysMonotonic — the clamp must not disturb the existing
// max() semantics: a later, lower ack (a retry, a reordered delivery) cannot
// walk a subscriber's floor backwards.
func TestAcknowledgeStaysMonotonic(t *testing.T) {
	db := setup_acknowledge_test(t)

	for i := 0; i < 4; i++ {
		broadcast_log_append(db, "k1", "peerA", "event/a", []byte(`{}`))
	}

	high := &Event{db: db, from: "subscriber1", content: map[string]any{
		"key": "k1", "peer": "peerA", "sequence": int64(3),
	}}
	if err := high.broadcast_acknowledge(); err != nil {
		t.Fatalf("acknowledge high: %v", err)
	}
	low := &Event{db: db, from: "subscriber1", content: map[string]any{
		"key": "k1", "peer": "peerA", "sequence": int64(1),
	}}
	if err := low.broadcast_acknowledge(); err != nil {
		t.Fatalf("acknowledge low: %v", err)
	}

	if got := acknowledged_last(t, db, "k1", "peerA", "subscriber1"); got != 3 {
		t.Errorf("watermark walked backwards: got %d, want 3", got)
	}
}

// TestAcknowledgeRejectsMalformed — the existing input contract is unchanged.
func TestAcknowledgeRejectsMalformed(t *testing.T) {
	db := setup_acknowledge_test(t)

	broadcast_log_append(db, "k1", "peerA", "event/a", []byte(`{}`))

	for _, content := range []map[string]any{
		{"key": "", "peer": "peerA", "sequence": int64(1)},
		{"key": "k1", "peer": "", "sequence": int64(1)},
		{"key": "k1", "peer": "peerA", "sequence": int64(0)},
		{"key": "k1", "peer": "peerA", "sequence": int64(-5)},
	} {
		e := &Event{db: db, from: "subscriber1", content: content}
		if err := e.broadcast_acknowledge(); err == nil {
			t.Errorf("accepted malformed content %v", content)
		}
	}
}
