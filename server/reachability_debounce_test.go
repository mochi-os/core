// Mochi server: AutoNAT reachability debounce/hysteresis unit tests
// Copyright © 2026 Mochisoft OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

// TestReachabilityDebounceStep exercises the asymmetric hysteresis: a
// "public" verdict must hold for the (short) up-confirm window before the
// relay-gating verdict flips on; a transient drop that recovers before the
// (long) down-confirm window leaves it on (so the relay's reservations
// survive a flap); only a sustained drop past the down window flips it off.
func TestReachabilityDebounceStep(t *testing.T) {
	base := int64(1_000_000)
	up := int64(reachability_confirm_public / time.Second)
	down := int64(reachability_confirm_not_public / time.Second)

	// Starts not-public (relay off). Public doesn't take until it has held
	// for the up-confirm window.
	s := reachability_debounce_state{}
	s, flip := reachability_debounce_step(s, true, base)
	if flip {
		t.Fatal("public flipped immediately; want the up-confirm dwell")
	}
	s, flip = reachability_debounce_step(s, true, base+up-1)
	if flip {
		t.Fatal("public flipped one second before the up-confirm window")
	}
	s, flip = reachability_debounce_step(s, true, base+up)
	if !flip || !s.stable_public {
		t.Fatalf("public not confirmed at the window: flip=%v stable=%v", flip, s.stable_public)
	}

	// Now public/relaying. A transient drop that recovers before the
	// (much longer) down-confirm window must NOT flip — relay stays up.
	s, flip = reachability_debounce_step(s, false, base+up+10)
	if flip {
		t.Fatal("dropped immediately; want the long down-confirm dwell")
	}
	s, flip = reachability_debounce_step(s, false, base+up+10+down-1)
	if flip {
		t.Fatal("dropped one second before the down-confirm window")
	}
	s, flip = reachability_debounce_step(s, true, base+up+10+down+5)
	if flip || !s.stable_public {
		t.Fatalf("a transient drop flipped the stable verdict: flip=%v stable=%v", flip, s.stable_public)
	}
	if s.pending_since != 0 {
		t.Errorf("pending flip not cancelled on recovery: %+v", s)
	}

	// A sustained drop past the down-confirm window flips to not-public.
	s, _ = reachability_debounce_step(s, false, base+100_000)
	s, flip = reachability_debounce_step(s, false, base+100_000+down)
	if !flip || s.stable_public {
		t.Fatalf("sustained drop not confirmed: flip=%v stable=%v", flip, s.stable_public)
	}
}

// TestReachabilityDebounceUpWindowShorterThanDown locks in the asymmetry
// (start relaying promptly, leave reluctantly).
func TestReachabilityDebounceUpWindowShorterThanDown(t *testing.T) {
	if reachability_confirm_public >= reachability_confirm_not_public {
		t.Fatalf("up-confirm (%s) should be shorter than down-confirm (%s)",
			reachability_confirm_public, reachability_confirm_not_public)
	}
}
