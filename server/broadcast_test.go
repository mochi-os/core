// Mochi server: broadcast subsystem unit tests
// Copyright Alistair Cunningham 2026
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

// TestBroadcastReceiverStateHostLocal is the regression test for
// task #91. The receiver-side helpers (broadcast_advance_local /
// broadcast_pending_insert / broadcast_pending_delete) must NOT
// pair-replicate their writes - each paired subscriber host applies
// inbound broadcasts independently and tracks its own apply state.
// Pre-fix, advance_local used exec_app_user and the partner host
// silently dedup'd events its handler had never actually run.
//
// Test stubs the package-level replication_emit_to so we can count
// every per-user-scope emit, exercises the three receiver-side
// helpers, asserts zero emits. Then exercises a sender-side helper
// (broadcast_log_append) and asserts at least one emit - the
// negative-test for the negative test, so a future refactor that
// silently disables ALL replication doesn't look like a pass.
func TestBroadcastReceiverStateHostLocal(t *testing.T) {
	cleanup, user_uid, app_id := setup_sql_replication_test(t)
	defer cleanup()

	original := replication_emit_to
	defer func() { replication_emit_to = original }()
	var emits int
	replication_emit_to = func(user string, op *ReplicationOp, peers []string) {
		emits++
	}

	u := &User{UID: user_uid}
	a := app_by_id(app_id)
	db := db_app_system(u, a)
	if db == nil {
		t.Fatal("db_app_system returned nil")
	}

	// --- receiver-side: zero emits expected ---

	emits = 0
	broadcast_advance_local(db, "peer-A", "key1", 5)
	if emits != 0 {
		t.Errorf("broadcast_advance_local fired %d replication emit(s); want 0", emits)
	}

	emits = 0
	broadcast_pending_insert(db, "peer-A", "key1", 7, "src", "dst", "svc", "ev", "msg", "app", "", []byte{1, 2})
	if emits != 0 {
		t.Errorf("broadcast_pending_insert fired %d replication emit(s); want 0", emits)
	}

	emits = 0
	broadcast_pending_delete(db, "peer-A", "key1", 7)
	if emits != 0 {
		t.Errorf("broadcast_pending_delete fired %d replication emit(s); want 0", emits)
	}

	// --- sender-side: replication still fires ---

	emits = 0
	broadcast_log_append(db, "key1", "peer-A", "object/update", []byte(`{"x":1}`))
	if emits == 0 {
		t.Error("broadcast_log_append fired 0 replication emits; want >0 (sender-side state MUST pair-replicate)")
	}
}
