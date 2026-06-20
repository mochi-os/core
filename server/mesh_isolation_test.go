// Mochi server: broadcast-mesh isolation auto-recovery unit tests
// Copyright © 2026 Mochi OÜ
// SPDX-License-Identifier: AGPL-3.0-only
// This file is part of Mochi, licensed under the GNU AGPL v3 with the
// Mochi Application Interface Exception - see license.txt and license-exception.md.

package main

import (
	"testing"
	"time"
)

// TestMeshIsolationStep walks the confirm -> remediate -> alert -> recover
// lifecycle of the pure isolation decision function: a single empty sample
// is treated as churn, the second confirms isolation and starts re-dialling,
// the alert fires exactly once after the threshold, and a returning peer
// resets all state and reports recovery.
func TestMeshIsolationStep(t *testing.T) {
	base := int64(1_000_000)
	alert_seconds := int64(mesh_isolation_alert_after / time.Second)

	// First empty sample: churn, not yet confirmed — no remediation, no alert.
	s, remediate, alert, recovered := mesh_isolation_step(mesh_isolation_state{}, 0, base)
	if remediate || alert || recovered {
		t.Fatalf("first empty sample: remediate=%v alert=%v recovered=%v, want all false", remediate, alert, recovered)
	}

	// Second consecutive empty sample confirms isolation: remediate, but the
	// alert clock only starts now.
	s, remediate, alert, _ = mesh_isolation_step(s, 0, base+15)
	if !remediate || alert {
		t.Fatalf("confirm sample: remediate=%v alert=%v, want true/false", remediate, alert)
	}
	if s.since != base+15 {
		t.Fatalf("isolation confirmed at %d, want %d", s.since, base+15)
	}

	// Still isolated, one second under the threshold: remediate, no alert yet.
	s, remediate, alert, _ = mesh_isolation_step(s, 0, base+15+alert_seconds-1)
	if !remediate || alert {
		t.Fatalf("pre-threshold: remediate=%v alert=%v, want true/false", remediate, alert)
	}

	// Crossing the threshold fires the alert exactly once.
	s, _, alert, _ = mesh_isolation_step(s, 0, base+15+alert_seconds)
	if !alert {
		t.Fatal("threshold crossed but no alert")
	}
	s, _, alert, _ = mesh_isolation_step(s, 0, base+15+alert_seconds+100)
	if alert {
		t.Fatal("alert fired a second time; want once per episode")
	}

	// A peer returns after we had alerted: state fully resets and recovery
	// is reported.
	next, remediate, alert, recovered := mesh_isolation_step(s, 1, base+15+alert_seconds+200)
	if remediate || alert || !recovered {
		t.Fatalf("recovery: remediate=%v alert=%v recovered=%v, want false/false/true", remediate, alert, recovered)
	}
	if next.since != 0 || next.empty != 0 || next.alerted {
		t.Fatalf("state not reset on recovery: %+v", next)
	}
}

// TestMeshIsolationStepBlipNoRecovery: a sub-threshold blip (one empty
// sample, then a peer) never confirms isolation, so it neither remediates
// nor logs a spurious recovery.
func TestMeshIsolationStepBlipNoRecovery(t *testing.T) {
	s, remediate, _, _ := mesh_isolation_step(mesh_isolation_state{}, 0, 5000)
	if remediate {
		t.Fatal("single empty sample should not remediate")
	}
	_, _, _, recovered := mesh_isolation_step(s, 1, 5015)
	if recovered {
		t.Fatal("a blip that never confirmed isolation should not log recovery")
	}
}

// TestMeshIsolationRemediateResetsBackoff: a remediation round pulls every
// known-peer reconnect forward to now, so peer_reconnect_manager retries
// them immediately instead of waiting out the per-peer exponential backoff.
func TestMeshIsolationRemediateResetsBackoff(t *testing.T) {
	// No bootstraps so remediate spawns no real dial goroutines.
	saved := peers_bootstrap
	peers_bootstrap = nil
	defer func() { peers_bootstrap = saved }()

	peer_reconnect_lock.Lock()
	peer_reconnects = map[string]PeerReconnect{
		"peerA": {NextRetry: now() + 250, Attempts: 5},
		"peerB": {NextRetry: now() + 40, Attempts: 2},
	}
	peer_reconnect_lock.Unlock()
	defer func() {
		peer_reconnect_lock.Lock()
		peer_reconnects = map[string]PeerReconnect{}
		peer_reconnect_lock.Unlock()
	}()

	mesh_isolation_remediate()

	peer_reconnect_lock.Lock()
	defer peer_reconnect_lock.Unlock()
	for id, r := range peer_reconnects {
		if r.NextRetry > now() {
			t.Errorf("peer %q NextRetry not reset: %d > %d", id, r.NextRetry, now())
		}
		if r.Attempts == 0 {
			t.Errorf("peer %q Attempts cleared; backoff history should be preserved", id)
		}
	}
}
