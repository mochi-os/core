// Mochi server: broadcast subsystem unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.
//
// Tests targeting the NACK-reason wire hint plus the gap-error
// sentinel that the stream-receive NACK responder maps to it. The
// goal is to prove the wire-protocol extension does what's needed
// for the queue-side fix in task #80: a broadcast gap NACK becomes
// a drop on the sender, not another 7-day retry loop.

package main

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

// TestNackReasonFromBroadcastGap maps a route()-returned error
// wrapped around the ErrBroadcastGap sentinel to the wire reason
// string. The sender's queue uses this to decide drop vs retry.
func TestNackReasonFromBroadcastGap(t *testing.T) {
	err := fmt.Errorf("broadcast gap detected (peer=p, key=k, last=42, seq=99): %w", ErrBroadcastGap)
	if got := nack_reason_from_error(err); got != nack_reason_broadcast_gap {
		t.Errorf("wrapped sentinel: got reason %q, want %q", got, nack_reason_broadcast_gap)
	}

	// Plain non-sentinel error must map to empty (legacy retry path).
	plain := errors.New("something else broke")
	if got := nack_reason_from_error(plain); got != "" {
		t.Errorf("plain error: got reason %q, want empty (legacy retry)", got)
	}

	// Nil error returns empty - defensive; the caller should never
	// build a NACK from a nil error, but we don't want to panic.
	if got := nack_reason_from_error(nil); got != "" {
		t.Errorf("nil error: got reason %q, want empty", got)
	}
}

// TestNackReasonFromPendingFull confirms ErrBroadcastPendingFull
// wraps through to nack_reason_pending_full on the wire AND that the
// sender's nack_should_drop gate treats it as retry-not-drop. This
// pair is the load-bearing contract for the buffer-full fix: ACKing
// on overflow silently loses the event, so the receiver must signal
// and the sender must hold the row for retry.
func TestNackReasonFromPendingFull(t *testing.T) {
	err := fmt.Errorf("pending buffer full for (peer=p, key=k): %w", ErrBroadcastPendingFull)
	if got := nack_reason_from_error(err); got != nack_reason_pending_full {
		t.Errorf("wrapped sentinel: got reason %q, want %q", got, nack_reason_pending_full)
	}
	if nack_should_drop(nack_reason_pending_full) {
		t.Errorf("nack_should_drop(%q) returned true; want false (buffer-full is transient, sender must retry)", nack_reason_pending_full)
	}
}

// TestBroadcastResyncThrottleBurstDedup is the load-bearing property
// the original 60s throttle had: a burst of gap fires within ms must
// collapse to one outbound resync request. Repeats the call 50 times
// from a tight loop; only the first should return true.
func TestBroadcastResyncThrottleBurstDedup(t *testing.T) {
	broadcast_resync_lock.Lock()
	broadcast_resync_inflight = map[string]int64{}
	broadcast_resync_lock.Unlock()
	defer func() {
		broadcast_resync_lock.Lock()
		broadcast_resync_inflight = map[string]int64{}
		broadcast_resync_lock.Unlock()
	}()

	got := 0
	for i := 0; i < 50; i++ {
		if broadcast_resync_throttle("u1", "p1", "k1") {
			got++
		}
	}
	if got != 1 {
		t.Errorf("burst of 50 produced %d resync requests, want 1", got)
	}
}

// TestBroadcastResyncClearUnlocksImmediately is the new property: a
// successful resync (signalled by broadcast_advance_local clearing
// the in-flight flag) lets the next request fire WITHOUT waiting out
// any time window. Under the old 60s throttle this took 60s; under
// the new design it takes one clear call.
func TestBroadcastResyncClearUnlocksImmediately(t *testing.T) {
	broadcast_resync_lock.Lock()
	broadcast_resync_inflight = map[string]int64{}
	broadcast_resync_lock.Unlock()
	defer func() {
		broadcast_resync_lock.Lock()
		broadcast_resync_inflight = map[string]int64{}
		broadcast_resync_lock.Unlock()
	}()

	if !broadcast_resync_throttle("u1", "p1", "k1") {
		t.Fatal("first call must pass")
	}
	if broadcast_resync_throttle("u1", "p1", "k1") {
		t.Fatal("second call before clear must block")
	}
	broadcast_resync_clear("u1", "p1", "k1")
	if !broadcast_resync_throttle("u1", "p1", "k1") {
		t.Fatal("call after clear must pass immediately, not wait the timeout")
	}
}

// TestBroadcastResyncThrottleIndependentTags confirms the gate is
// per-(user, peer, key) - bursts on one stream don't block requests
// on another.
func TestBroadcastResyncThrottleIndependentTags(t *testing.T) {
	broadcast_resync_lock.Lock()
	broadcast_resync_inflight = map[string]int64{}
	broadcast_resync_lock.Unlock()
	defer func() {
		broadcast_resync_lock.Lock()
		broadcast_resync_inflight = map[string]int64{}
		broadcast_resync_lock.Unlock()
	}()

	if !broadcast_resync_throttle("u1", "p1", "k1") {
		t.Error("u1/p1/k1 first call must pass")
	}
	if !broadcast_resync_throttle("u1", "p1", "k2") {
		t.Error("different key must not be blocked by k1's in-flight")
	}
	if !broadcast_resync_throttle("u1", "p2", "k1") {
		t.Error("different peer must not be blocked")
	}
	if !broadcast_resync_throttle("u2", "p1", "k1") {
		t.Error("different user must not be blocked")
	}
}

// TestNackShouldDrop is the matching sender-side gate. Drop reasons
// route to queue_drop (delete row, no retry); everything else goes
// to queue_fail (schedule retry with backoff).
func TestNackShouldDrop(t *testing.T) {
	for _, reason := range []string{
		nack_reason_broadcast_gap,
		nack_reason_decode_failed,
	} {
		if !nack_should_drop(reason) {
			t.Errorf("reason %q: want drop=true, got false", reason)
		}
	}

	// Empty reason means a legacy receiver or an unspecified
	// failure. Must keep the retry semantics.
	if nack_should_drop("") {
		t.Error("empty reason: want drop=false, got true (would break legacy receivers)")
	}

	// An unknown reason from a future receiver also defaults to
	// retry - safer than dropping on something we don't recognise.
	if nack_should_drop("future-reason-we-dont-know") {
		t.Error("unknown reason: want drop=false, got true")
	}
}


// TestPriorityReplayAbovesInteractive locks in the relative ordering
// of the priority tiers (task #96). queue_select orders desc by
// priority, so for resync replies to overtake the live-broadcast
// backlog they MUST be strictly greater than priority_interactive.
// A future refactor that re-numbers the tiers without preserving
// the ordering would silently regress catch-up rate.
func TestPriorityReplayAbovesInteractive(t *testing.T) {
	if priority_replay <= priority_interactive {
		t.Errorf("priority_replay (%d) must be > priority_interactive (%d)", priority_replay, priority_interactive)
	}
	if priority_control <= priority_replay {
		t.Errorf("priority_control (%d) must be > priority_replay (%d)", priority_control, priority_replay)
	}
	if priority_interactive <= priority_bulk {
		t.Errorf("priority_interactive (%d) must be > priority_bulk (%d)", priority_interactive, priority_bulk)
	}
}

// TestQueueAddDirectPriorityOverride pins the wire-level invariant:
// queue_add_direct_priority writes its argument into the queue.priority
// column, NOT the (service, event)-derived default. Without this the
// resync-reply path would silently fall back to priority_interactive
// and the catch-up rate fix would be a no-op.
func TestQueueAddDirectPriorityOverride(t *testing.T) {
	tmp_dir, err := os.MkdirTemp("", "mochi_queue_prio")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmp_dir)
	orig := data_dir
	data_dir = tmp_dir
	defer func() { data_dir = orig }()

	// Initialise queue.db schema.
	q := db_open("db/queue.db")
	q.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20 )")

	// (service="feeds", event="post/create") would default to
	// priority_interactive (20). Override to priority_replay (30)
	// and read back from the priority column.
	queue_add_direct_priority("test-id", "peer-A", "from-entity", "to-entity", "feeds", "post/create", "", nil, nil, nil, "", 0, priority_replay)

	row, err := q.row("select priority from queue where id = ?", "test-id")
	if err != nil || row == nil {
		t.Fatalf("queue row missing: %v", err)
	}
	got, _ := row["priority"].(int64)
	if got != int64(priority_replay) {
		t.Errorf("priority override: got %d, want %d (priority_replay)", got, priority_replay)
	}
}

// TestBroadcastWireKeys pins the wire content-key literals. The sender
// (api_broadcast_send, broadcast_resync replay) and the receiver
// (events.go gap detection) share these constants, so they cannot
// diverge at compile time; this test additionally pins the underscore
// prefix itself, because a 2026-05-26 table-rename find/replace once
// rewrote the sender's "_sequence" to "sequence" and silently disabled
// dedup, gap buffering, and watermarks for six weeks.
func TestBroadcastWireKeys(t *testing.T) {
	if broadcast_content_key != "_key" {
		t.Errorf("broadcast_content_key = %q, want %q", broadcast_content_key, "_key")
	}
	if broadcast_content_sequence != "_sequence" {
		t.Errorf("broadcast_content_sequence = %q, want %q", broadcast_content_sequence, "_sequence")
	}
}

// TestBroadcastInboundClass covers the receiver's classification of an
// inbound sequenced event against the stream watermark, including the
// anchor-adoption rule: a stream with no watermark (last == 0) accepts
// its first event at any sequence instead of gap-buffering, because
// resync replay can never reach back past the sender's log trim, so a
// "gap" verdict on an unknown stream would wedge it permanently.
func TestBroadcastInboundClass(t *testing.T) {
	cases := []struct {
		name string
		last int64
		bseq int64
		want string
	}{
		{"fresh stream first event", 0, 1, "apply"},
		{"anchor adoption mid-stream", 0, 70, "apply"},
		{"in-order next", 5, 6, "apply"},
		{"exact duplicate", 5, 5, "duplicate"},
		{"stale retry below watermark", 5, 3, "duplicate"},
		{"gap on established stream", 5, 8, "gap"},
		{"gap of one", 5, 7, "gap"},
		{"duplicate of anchor", 70, 70, "duplicate"},
		{"next after anchor", 70, 71, "apply"},
		{"gap after anchor", 70, 80, "gap"},
	}
	for _, c := range cases {
		if got := broadcast_inbound_class(c.last, c.bseq); got != c.want {
			t.Errorf("%s: broadcast_inbound_class(%d, %d) = %q, want %q", c.name, c.last, c.bseq, got, c.want)
		}
	}
}
