// Mochi server: hole-punch tracer unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"

	holepunch "github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
)

// TestHolepunchTracer: only the End event's Success counts; a direct
// dial succeeding (no punch needed) and the start/attempt events do not.
func TestHolepunchTracer(t *testing.T) {
	holepunch_success.Store(0)
	holepunch_failure.Store(0)
	tr := holepunch_tracer{}

	tr.Trace(&holepunch.Event{Type: holepunch.EndHolePunchEvtT, Evt: &holepunch.EndHolePunchEvt{Success: true, EllapsedTime: time.Second}})
	tr.Trace(&holepunch.Event{Type: holepunch.EndHolePunchEvtT, Evt: &holepunch.EndHolePunchEvt{Success: false, EllapsedTime: time.Second, Error: "timeout"}})
	tr.Trace(&holepunch.Event{Type: holepunch.EndHolePunchEvtT, Evt: &holepunch.EndHolePunchEvt{Success: true}})
	// Non-counting events.
	tr.Trace(&holepunch.Event{Type: holepunch.DirectDialEvtT, Evt: &holepunch.DirectDialEvt{Success: true}})
	tr.Trace(&holepunch.Event{Type: holepunch.DirectDialEvtT, Evt: &holepunch.DirectDialEvt{Success: false, Error: "refused"}})
	tr.Trace(&holepunch.Event{Type: holepunch.StartHolePunchEvtT, Evt: &holepunch.StartHolePunchEvt{}})
	tr.Trace(&holepunch.Event{Type: holepunch.HolePunchAttemptEvtT, Evt: &holepunch.HolePunchAttemptEvt{Attempt: 1}})

	if got := holepunch_success.Load(); got != 2 {
		t.Errorf("success = %d, want 2", got)
	}
	if got := holepunch_failure.Load(); got != 1 {
		t.Errorf("failure = %d, want 1", got)
	}
}
