// Mochi server: broadcast subsystem unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the Mochi
// Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
)

// TestPendingFullIsTransientBySentinel - a full pending buffer must make the
// sender retry: it deletes the queue row on ACK, so an ACK loses the event.
func TestPendingFullIsTransientBySentinel(t *testing.T) {
	err := fmt.Errorf("pending buffer full for (peer=p, key=k): %w", ErrBroadcastPendingFull)

	if reason := worker_failure_reason(err); reason != fail_transient {
		t.Errorf("worker_failure_reason = %q, want %q; the sender would drop the row and the event would be lost", reason, fail_transient)
	}
	if !fail_retryable(fail_transient) {
		t.Errorf("fail_retryable(%q) = false; buffer-full backpressure must be retried, not dropped", fail_transient)
	}
}

// TestPendingFullDoesNotDependOnItsWording proves the sentinel is what is
// consulted. The catch-all would answer transient for any unrecognised text,
// so a test using the real message cannot tell the two apart - this one uses
// a message that matches a DROP prefix and must still come back transient.
func TestPendingFullDoesNotDependOnItsWording(t *testing.T) {
	disguised := fmt.Errorf("unknown user in a rephrased overflow message: %w", ErrBroadcastPendingFull)

	if reason := worker_failure_reason(disguised); reason != fail_transient {
		t.Errorf("worker_failure_reason = %q for an error wrapping ErrBroadcastPendingFull, want %q. The sentinel must be checked before the prefixes, or rewording the fmt.Errorf at events.go silently turns retry into drop", reason, fail_transient)
	}
}

// TestUnrecognisedErrorsStayTransient: the catch-all is still the disposition
// for everything else, which is why the sentinel check had to be added rather
// than relied upon.
func TestUnrecognisedErrorsStayTransient(t *testing.T) {
	if reason := worker_failure_reason(errors.New("something else broke")); reason != fail_transient {
		t.Errorf("worker_failure_reason = %q for an unrecognised error, want %q", reason, fail_transient)
	}
	if reason := worker_failure_reason(nil); reason != "" {
		t.Errorf("worker_failure_reason(nil) = %q, want empty", reason)
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

// TestBroadcastResyncClearUnlocksImmediately - clearing the in-flight flag lets
// the next request fire without waiting out any time window.
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

// TestPriorityReplayAbovesInteractive - queue_select orders by priority desc,
// so resync replies overtake the live backlog only while replay > interactive.
func TestPriorityReplayAbovesInteractive(t *testing.T) {
	if priority_replay <= priority_interactive {
		t.Errorf("priority_replay (%d) must be > priority_interactive (%d)", priority_replay, priority_interactive)
	}
}

// TestQueueAddDirectPriorityOverride - queue_add_direct_priority must write its
// argument to queue.priority, not the (service, event)-derived default.
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
	q.exec("create table if not exists queue ( id text primary key, type text not null default 'direct', target text not null, from_entity text not null, to_entity text not null, service text not null, event text not null, from_app text not null default '', from_services text not null default '', content blob not null default '', data blob not null default '', file text not null default '', expires integer not null default 0, status text not null default 'pending', attempts integer not null default 0, next_retry integer not null, last_error text not null default '', created integer not null, priority integer not null default 20, claimed integer not null default 0 )")

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

// TestBroadcastWireKeys pins the wire content-key literals, including the
// underscore prefix that keeps app payload fields from colliding.
func TestBroadcastWireKeys(t *testing.T) {
	if broadcast_content_key != "_key" {
		t.Errorf("broadcast_content_key = %q, want %q", broadcast_content_key, "_key")
	}
	if broadcast_content_sequence != "_sequence" {
		t.Errorf("broadcast_content_sequence = %q, want %q", broadcast_content_sequence, "_sequence")
	}
}

// TestBroadcastInboundClass covers the watermark classification, including
// anchor adoption: an unknown stream applies its first event at any sequence.
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

// TestBroadcastPayloadDecodeKeepsIntegers - a replayed integer must not come
// back as a float64; apps validate such fields by pattern against str(value).
func TestBroadcastPayloadDecodeKeepsIntegers(t *testing.T) {
	raw := `{"created":1753400000,"edited":0,"body":"hello","score":1.5,` +
		`"nested":{"seen":1753400001},"list":[1753400002,2]}`

	var payload map[string]any
	if err := broadcast_payload_decode(raw, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}

	created, ok := payload["created"].(int64)
	if !ok {
		t.Fatalf("created should decode as int64, got %T (%v)", payload["created"], payload["created"])
	}
	if created != 1753400000 {
		t.Fatalf("created = %d, want 1753400000", created)
	}
	// The shape the apps actually check.
	if got := fmt.Sprintf("%v", created); got != "1753400000" {
		t.Fatalf("created renders as %q, want the plain integer", got)
	}
	// A plain Unmarshal is what used to happen - assert it really did differ,
	// so this test fails for the right reason if the helper is reverted.
	var legacy map[string]any
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		t.Fatalf("legacy decode: %v", err)
	}
	if _, was_float := legacy["created"].(float64); !was_float {
		t.Fatalf("expected the plain Unmarshal to yield float64, got %T", legacy["created"])
	}

	if zero, ok := payload["edited"].(int64); !ok || zero != 0 {
		t.Fatalf("edited = %v (%T), want int64 0", payload["edited"], payload["edited"])
	}
	if body, _ := payload["body"].(string); body != "hello" {
		t.Fatalf("body = %q, want hello", body)
	}
	// Genuinely fractional values stay float64.
	if score, ok := payload["score"].(float64); !ok || score != 1.5 {
		t.Fatalf("score = %v (%T), want float64 1.5", payload["score"], payload["score"])
	}
	// Nested maps and slices are walked too.
	nested, _ := payload["nested"].(map[string]any)
	if seen, ok := nested["seen"].(int64); !ok || seen != 1753400001 {
		t.Fatalf("nested.seen = %v (%T), want int64", nested["seen"], nested["seen"])
	}
	list, _ := payload["list"].([]any)
	if len(list) != 2 {
		t.Fatalf("list length %d, want 2", len(list))
	}
	if first, ok := list[0].(int64); !ok || first != 1753400002 {
		t.Fatalf("list[0] = %v (%T), want int64", list[0], list[0])
	}
}
