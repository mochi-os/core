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
